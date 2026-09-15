"""
rm-additive-prefetch  —  Lambda entry point
=============================================================================

The planner. Reads active sectors, splits into two lanes, routes them:

  1. Reads active sectors from qp_rm_master.active_sectors.
  2. For each sector, checks whether it has a cached history in S3.
  3. Splits:
       * NO cache / FORCE_BACKFILL / REFRESH  -> Glue fetch-only
         (direct start_job_run, chunked; Glue emits 1 SQS msg/sector after caching)
       * Cached (incremental)  -> one SQS message PER SECTOR to the process queue
         (AWS event-source-mapping batch size groups them into process Lambdas)
  4. Writes the run skeleton audit document to MongoDB.

Mapped sectors (N:1). Many sectors may map to one donor
(e.g. NMIDEL, NMIBLR, JAIBLR -> BOMDEL). Dependents are NOT dispatched here.
The donor is dispatched carrying its `dependent_sectors` list; after the donor
finishes, the process worker re-enqueues each dependent as its own 1-sector
message (carrying `mapped_sector` = donor) so it borrows the now-warm donor
history. This keeps SQS at 1 sector/message (batch size 5, concurrency 60).

Message contract (SQS -> rm-additive-process), one per sector:
{
  "modules": {"additive_process": 1},
  "overrides": {
    "sectors": ["BOMDEL"],
    "dependent_sectors": ["NMIDEL", "NMIBLR", "JAIBLR"],  # donor only
    "run_id": "a1b2c3d4-...",
    "batch_index": 3,
    "batch_total": 48,
    "sectors_expected": 48,
    "mode": "PROD"
  }
}
# A re-enqueued dependent instead carries "mapped_sector": "BOMDEL".
"""

import logging
import boto3
import json
import os
from datetime import datetime
from uuid import uuid4

from pymongo import MongoClient
import configsettings

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- AWS clients (created at module load, reused across warm invocations) ---
glue = boto3.client('glue')
sqs = boto3.client('sqs')
s3 = boto3.client('s3')

# --- MongoDB ---
mongo_client = MongoClient(
    host=configsettings.MONGO_URL, port=27017,
    username=configsettings.MONGO_USERNAME,
    password=configsettings.MONGO_PASSWORD,
    authSource=configsettings.MONGO_AUTH_SOURCE)

db_rmalloc = mongo_client['qp_dw_rmalloc']
db_master = mongo_client[configsettings.MONGO_MASTER_DB]

# --- Configuration ---
ENV               = os.environ.get('ENV', 'uat')
S3_BUCKET         = configsettings.ADDITIVE_MODEL_BUCKET
CACHE_PREFIX      = f"{configsettings.ADDITIVE_MODEL_FILE_PATH}_cache/hist_flown"
PROCESS_QUEUE_URL = configsettings.ADDITIVE_SQS_URL   # SQS queue -> process Lambda
GLUE_FETCH_JOB    = configsettings.ADDITIVE_GLUE_NAME
GLUE_BATCH_SIZE   = 10   # sectors per Glue fetch run

# --- Adhoc configuration (simulation) ---
ADHOC_PROCESS_QUEUE_URL = configsettings.ADDITIVE_ADHOC_SQS_URL
ADHOC_GLUE_FETCH_JOB    = configsettings.ADDITIVE_ADHOC_GLUE_NAME
ADHOC_CACHE_PREFIX       = 'additive_model_test/_cache/hist_flown'


# =============================================================================
# HELPERS
# =============================================================================

def _sector_has_cache(sector, cache_prefix):
    """True if at least one hist_flown partition exists for this sector in S3."""
    resp = s3.list_objects_v2(
        Bucket=S3_BUCKET, Prefix=f"{cache_prefix}/sector={sector}/", MaxKeys=1)
    return resp.get('KeyCount', 0) > 0


def _send_process_message(sector, run_id, batch_index, batch_total, sectors_expected,
                          mode, dependent_sectors=None, mapped_sector=None):
    """Send one SQS message for a SINGLE sector to the process queue.

    A donor sector may carry `dependent_sectors` (the sectors mapped to it). The
    worker processes the donor, then re-enqueues each dependent as its own
    1-sector message carrying `mapped_sector` = this donor, so the dependent
    borrows the (now warm) donor history. This keeps SQS at 1 sector/message
    (batch size 5, concurrency 60) with no upfront clustering.
    """
    overrides = {
        'sectors': [sector],
        'run_id': run_id,
        'batch_index': batch_index,
        'batch_total': batch_total,
        'sectors_expected': sectors_expected,
        'mode': mode,
    }
    if dependent_sectors:
        overrides['dependent_sectors'] = list(dependent_sectors)
    if mapped_sector:
        overrides['mapped_sector'] = mapped_sector
    message = {'modules': {'additive_process': 1}, 'overrides': overrides}
    sqs.send_message(QueueUrl=PROCESS_QUEUE_URL, MessageBody=json.dumps(message))


def _load_mapped_sectors():
    """{sector: mapped_sector} fallback donors from config_additive_mapped_sectors.

    N:1 is allowed — many sectors may map to the same donor
    (e.g. NMIDEL, NMIBLR, JAIBLR -> BOMDEL). Empty dict on any error (feature
    simply inactive)."""
    try:
        docs = db_rmalloc.config_additive_mapped_sectors.find(
            {}, {'_id': 0, 'sector': 1, 'mapped_sector': 1})
        out = {}
        for d in docs:
            s, m = d.get('sector'), d.get('mapped_sector')
            if s and m and s != m:
                out[str(s).strip()] = str(m).strip()
        return out
    except Exception as e:
        logger.warning(f"[prefetch] mapped-sectors load failed ({e.__class__.__name__}: {e}).")
        return {}


def _start_glue_fetch(sectors, run_id, force_backfill, refresh_spec,
                      sectors_expected=0, mode='PROD', dependent_map=None):
    """Start a Glue fetch-only job. Glue emits 1 SQS msg/sector after caching.

    `dependent_map` ({donor: [dependents]}) covers the edge case where a donor is
    itself uncached and routed through Glue: Glue should stamp `dependent_sectors`
    on that donor's emitted process message so the worker still re-enqueues the
    dependents. Passed as JSON on --DEPENDENT_MAP.
    """
    glue.start_job_run(
        JobName=GLUE_FETCH_JOB,
        Arguments={
            '--SECTORS': ','.join(sectors),
            '--RUN_ID': run_id,
            '--FORCE_BACKFILL': 'true' if force_backfill else 'false',
            '--REFRESH_SPEC': str(refresh_spec) if refresh_spec else 'none',
            '--SECTORS_EXPECTED': str(sectors_expected),
            '--MODE': mode,
            '--PROCESS_QUEUE_URL': PROCESS_QUEUE_URL,
            '--DEPENDENT_MAP': json.dumps(dependent_map or {}),
            '--ENV': ENV,
        })


def _write_audit_skeleton(run_id, snapshot_date, sectors_expected, planned, mode, analyst):
    """Write the run skeleton doc so process Lambdas can write into it."""
    db_rmalloc.additive_model_run_audit.insert_one({
        'run_id': run_id,
        'module': 'additive_pickup_model',
        'snapshot_date': snapshot_date,
        'status': 'IN_PROGRESS',
        'sectors_expected': sectors_expected,
        'sectors_completed': 0,
        'batches_seen': [],
        'start_time': datetime.utcnow(),
        'end_time': None,
        'sector_stats': {},
        'planned_bins': planned,
        'mode': mode,
        'triggered_by': analyst,
    })


# =============================================================================
# HANDLER
# =============================================================================

def lambda_handler(event, context):
    # --- parse body ---
    if "Records" in event:
        body = event["Records"][0]["body"]
    elif "body" in event:
        body = event["body"]
    else:
        body = json.dumps(event)

    data = json.loads(body)
    overrides = data.get("overrides", data)

    run_id         = overrides.get('run_id') or str(uuid4())
    mode           = overrides.get('mode', 'PROD').upper()
    force_backfill = str(overrides.get('force_backfill', 'false')).lower() == 'true'
    refresh_spec   = overrides.get('refresh_spec')
    analyst        = overrides.get('analyst', 'system')
    target_sectors = overrides.get('sectors')   # optional explicit subset

    # --- ADHOC SIMULATION ---
    # simulate_snapshot = a past model-run date ('DD-MM-YYYY' or 'YYYY-MM-DD').
    # When set: the whole pipeline runs "as of" that date against a separate S3
    # root (additive_model_test/). The cache-lane check reads that test root, so
    # the FIRST snapshot (empty test cache) routes to Glue to seed, and every
    # later snapshot (now cached) routes straight to the process Lambda, which
    # gap-fills. No audit skeleton is written (the process Lambda skips audit in
    # simulation, so a skeleton would hang IN_PROGRESS forever).
    simulate_snapshot = overrides.get('simulate_snapshot')
    simulate = bool(simulate_snapshot)
    cache_prefix = ADHOC_CACHE_PREFIX if simulate else CACHE_PREFIX

    # Select the correct SQS queue and Glue job based on simulation mode
    active_queue_url = ADHOC_PROCESS_QUEUE_URL if simulate else PROCESS_QUEUE_URL
    active_glue_job  = ADHOC_GLUE_FETCH_JOB if simulate else GLUE_FETCH_JOB

    # --- load active sectors ---
    if target_sectors:
        active = list(target_sectors)
    else:
        docs = db_master.active_sectors.find({}, {"_id": 0, "sector": 1})
        active = [d['sector'] for d in docs if d.get('sector')]

    active = sorted(set(active))
    logger.info(f"[prefetch] {len(active)} active sectors | run_id={run_id} | "
                f"mode={mode} | force={force_backfill} | refresh={refresh_spec}"
                + (f" | SIMULATE={simulate_snapshot}" if simulate else ""))

    if not active:
        logger.error("[prefetch] No active sectors found — aborting.")
        return {"statusCode": 400, "body": "No active sectors"}

    # --- mapped-sector fallback config (N:1 — many dependents -> one donor) ---
    mapping = _load_mapped_sectors()          # {dependent: donor}
    active_set = set(active)
    # Only mappings whose dependent is actually active this run matter.
    mapping = {s: m for s, m in mapping.items() if s in active_set}
    # Invert to {donor: [dependents]} so a donor carries its whole dependent list.
    donor_to_deps = {}
    for dep, donor in mapping.items():
        donor_to_deps.setdefault(donor, []).append(dep)
    for donor in donor_to_deps:
        donor_to_deps[donor].sort()
    mapped_targets = set(donor_to_deps)       # donor sectors
    logger.info(f"[prefetch] {len(mapping)} active mapped dependent(s) across "
                f"{len(mapped_targets)} donor(s): {donor_to_deps}")

    # --- cache status for every sector we might touch (active + donors) ---
    def _cached(s):
        return force_backfill is False and refresh_spec is None and _sector_has_cache(s)

    # --- routing ---
    # Dependents are NOT dispatched by prefetch. Their donor is dispatched with a
    # `dependent_sectors` list; the worker re-enqueues each dependent (1 msg each,
    # carrying mapped_sector=donor) only AFTER the donor has processed, so the
    # dependent borrows the warm donor history. This keeps SQS at 1 sector/message.
    #
    #   cached / donor          -> PROCESS now (donor carries dependent list)
    #   uncached + no mapping   -> GLUE fetch-only, then it self-enqueues to PROCESS
    # A dependent whose donor is inactive but needed & uncached: the donor is
    # backfilled via Glue and then processes (and re-enqueues its dependents).
    dependents_set = set(mapping)
    glue_sectors = []
    process_sectors = []          # sectors prefetch dispatches to PROCESS now
    for s in active:
        if s in dependents_set:
            continue              # dispatched later by its donor, not by prefetch
        if _cached(s) or s in mapped_targets:
            process_sectors.append(s)
        else:
            glue_sectors.append(s)

    # Donors not in the active list but needed & uncached -> Glue backfill first.
    # (Glue emits a process message for them; that message carries no dependent
    #  list, so we also record their dependents on the Glue arguments below.)
    inactive_uncached_donors = [m for m in mapped_targets
                                if m not in active_set and not _sector_has_cache(m)]
    glue_sectors.extend(inactive_uncached_donors)

    # sectors_expected must count EVERY sector that will write an audit stat:
    # the sectors prefetch dispatches now + every dependent (re-enqueued later) +
    # any donor sitting in the Glue lane. Dependents produce output, so include them.
    process_set = set(process_sectors)
    expected_set = process_set | set(glue_sectors) | dependents_set
    sectors_expected = len(expected_set)

    # --- uncached (no mapping) -> Glue fetch-only (chunked) ---
    glue_batches = [glue_sectors[i:i + GLUE_BATCH_SIZE]
                    for i in range(0, len(glue_sectors), GLUE_BATCH_SIZE)]

    # --- audit skeleton BEFORE dispatching ---
    snapshot_date = datetime.utcnow().strftime('%Y-%m-%d')
    planned = [{'batch_index': i, 'sectors': [s],
                'dependent_sectors': donor_to_deps.get(s, []),
                'lane': 'lambda'} for i, s in enumerate(process_sectors)]
    planned += [{'sectors': b, 'lane': 'glue_then_lambda'} for b in glue_batches]

    if sectors_expected > 0 and not simulate:
        _write_audit_skeleton(run_id, snapshot_date, sectors_expected,
                              planned, mode, analyst)

    # --- dispatch Glue jobs (inactive-donor backfills + unmapped uncached) ---
    glue_units = 0
    for batch in glue_batches:
        batch_dep_map = {d: donor_to_deps[d] for d in batch if d in donor_to_deps}
        _start_glue_fetch(batch, run_id, force_backfill, refresh_spec,
                          sectors_expected=sectors_expected, mode=mode,
                          dependent_map=batch_dep_map)
        glue_units += 1

    # --- dispatch PROCESS messages (donors first with their dependent lists) ---
    batch_total = len(process_sectors)
    lambda_units = 0
    for idx, s in enumerate(process_sectors):
        _send_process_message(
            s, run_id, idx, batch_total, sectors_expected, mode,
            dependent_sectors=donor_to_deps.get(s))   # None for a plain sector
        lambda_units += 1

    logger.info(f"[prefetch] DONE — {lambda_units} SQS message(s) now "
                f"({len(process_sectors)} sector(s); {len(mapping)} dependent(s) "
                f"re-enqueued by their donors), {glue_units} Glue job(s) | "
                f"run_id={run_id} | sectors_expected={sectors_expected}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "run_id": run_id,
            "lambda_units": lambda_units,
            "glue_units": glue_units,
            "sectors_total": len(active),
            "sectors_process_now": len(process_sectors),
            "sectors_dependent_deferred": len(mapping),
            "sectors_glue": len(glue_sectors),
            "sectors_expected": sectors_expected,
        })
    }