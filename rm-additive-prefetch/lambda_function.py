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

Message contract (SQS -> rm-additive-process), one per sector:
{
  "modules": {"additive_process": 1},
  "overrides": {
    "sectors": ["AUHBLR"],
    "run_id": "a1b2c3d4-...",
    "batch_index": 3,
    "batch_total": 48,
    "sectors_expected": 48,
    "mode": "PROD"
  }
}
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


# =============================================================================
# HELPERS
# =============================================================================

def _sector_has_cache(sector):
    """True if at least one hist_flown partition exists for this sector in S3."""
    resp = s3.list_objects_v2(
        Bucket=S3_BUCKET, Prefix=f"{CACHE_PREFIX}/sector={sector}/", MaxKeys=1)
    return resp.get('KeyCount', 0) > 0


def _send_process_message(sector, run_id, batch_index, batch_total, sectors_expected, mode):
    """Send one SQS message for a single sector to the process queue."""
    message = {
        'modules': {'additive_process': 1},
        'overrides': {
            'sectors': [sector],
            'run_id': run_id,
            'batch_index': batch_index,
            'batch_total': batch_total,
            'sectors_expected': sectors_expected,
            'mode': mode,
        }
    }
    sqs.send_message(QueueUrl=PROCESS_QUEUE_URL, MessageBody=json.dumps(message))


def _load_mapped_sectors():
    """{sector: mapped_sector} 1:1 fallback donors from config_additive_mapped_sectors.
    Empty dict on any error (feature simply inactive)."""
    try:
        docs = db_master.config_additive_mapped_sectors.find(
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


def _send_process_cluster(entries, run_id, batch_index, batch_total,
                          sectors_expected, mode):
    """Send one SQS message carrying a cluster of sector entries.

    Each entry: {'sector': str, 'priority': int, 'mapped_sector': str|None}.
    The process worker sorts by priority so a mapped-sector donor (priority 0) is
    fetched/cached before the dependent sector (priority 1) that borrows its data.
    """
    message = {
        'modules': {'additive_process': 1},
        'overrides': {
            'sectors': [e['sector'] for e in entries],   # back-compat flat list
            'sector_entries': entries,                    # priority + mapping
            'run_id': run_id,
            'batch_index': batch_index,
            'batch_total': batch_total,
            'sectors_expected': sectors_expected,
            'mode': mode,
        }
    }
    sqs.send_message(QueueUrl=PROCESS_QUEUE_URL, MessageBody=json.dumps(message))


def _start_glue_fetch(sectors, run_id, force_backfill, refresh_spec,
                      sectors_expected=0, mode='PROD'):
    """Start a Glue fetch-only job. Glue emits 1 SQS msg/sector after caching."""
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

    # --- load active sectors ---
    if target_sectors:
        active = list(target_sectors)
    else:
        docs = db_master.active_sectors.find({}, {"_id": 0, "sector": 1})
        active = [d['sector'] for d in docs if d.get('sector')]

    active = sorted(set(active))
    logger.info(f"[prefetch] {len(active)} active sectors | run_id={run_id} | "
                f"mode={mode} | force={force_backfill} | refresh={refresh_spec}")

    if not active:
        logger.error("[prefetch] No active sectors found — aborting.")
        return {"statusCode": 400, "body": "No active sectors"}

    # --- mapped-sector fallback config (1:1) ---
    mapping = _load_mapped_sectors()          # {sector: mapped_sector}
    active_set = set(active)
    # Only mappings whose dependent is actually active this run matter.
    mapping = {s: m for s, m in mapping.items() if s in active_set}
    mapped_targets = set(mapping.values())    # donor sectors (must run first)
    logger.info(f"[prefetch] {len(mapping)} active mapped-sector fallback(s); "
                f"{len(mapped_targets)} donor sector(s).")

    # --- cache status for every sector we might touch (active + donors) ---
    def _cached(s):
        return force_backfill is False and refresh_spec is None and _sector_has_cache(s)

    # --- routing ---
    #   uncached + has mapping  -> PROCESS (worker falls back to donor data)
    #   uncached + no mapping   -> GLUE fetch-only
    #   cached                  -> PROCESS
    # Donors that are themselves active are processed normally; a donor that is
    # NOT an active sector but is needed and uncached must be backfilled via Glue.
    glue_sectors = []
    process_sectors = []
    for s in active:
        if _cached(s):
            process_sectors.append(s)
        elif s in mapping:
            process_sectors.append(s)          # borrow donor data in-process
        else:
            glue_sectors.append(s)

    # Ensure donors that are active are in process_sectors (they are, if active).
    # Donors not in the active list but needed & uncached -> Glue backfill first.
    extra_donor_backfill = [m for m in mapped_targets
                            if m not in active_set and not _sector_has_cache(m)]
    glue_sectors.extend(extra_donor_backfill)

    sectors_expected = len(process_sectors)    # only PROCESS sectors produce output

    # --- build clusters (each becomes ONE SQS message, SQS batch size = 1) ---
    # Rule 1: a dependent sector rides in the SAME message as its donor, donor at
    #         priority 0, dependent at priority 1 (donor cached in-process first).
    # Rule 2: independent sectors are paired 2-per-message.
    # Rule 3: donor/dependent clusters are dispatched FIRST; plain sectors after.
    proc_set = set(process_sectors)
    donors_in_proc = [m for m in mapped_targets if m in proc_set]
    dependents = [s for s in process_sectors if s in mapping]
    plain = [s for s in process_sectors
             if s not in mapping and s not in mapped_targets]

    clusters = []          # each: list of {sector, priority, mapped_sector}

    # (a) donor + its dependents grouped together
    grouped = set()
    for donor in donors_in_proc:
        deps = [s for s in dependents if mapping[s] == donor]
        entry = [{'sector': donor, 'priority': 0, 'mapped_sector': None}]
        for d in deps:
            entry.append({'sector': d, 'priority': 1, 'mapped_sector': donor})
            grouped.add(d)
        grouped.add(donor)
        clusters.append(entry)

    # (b) any dependent whose donor is NOT in process (donor backfilled via Glue)
    #     still needs to run; it carries its mapped_sector so the worker can
    #     read the donor's cache once Glue has populated it.
    for d in dependents:
        if d in grouped:
            continue
        clusters.append([{'sector': d, 'priority': 1, 'mapped_sector': mapping[d]}])
        grouped.add(d)

    # (c) plain sectors -> 2 per message
    SECTORS_PER_MSG = 2
    for i in range(0, len(plain), SECTORS_PER_MSG):
        chunk = plain[i:i + SECTORS_PER_MSG]
        clusters.append([{'sector': s, 'priority': 0, 'mapped_sector': None}
                         for s in chunk])

    # --- uncached (no mapping) -> Glue fetch-only (chunked) ---
    glue_batches = [glue_sectors[i:i + GLUE_BATCH_SIZE]
                    for i in range(0, len(glue_sectors), GLUE_BATCH_SIZE)]

    # --- audit skeleton BEFORE dispatching ---
    snapshot_date = datetime.utcnow().strftime('%Y-%m-%d')
    planned = [{'batch_index': i,
                'sectors': [e['sector'] for e in c],
                'lane': 'lambda'} for i, c in enumerate(clusters)]
    planned += [{'sectors': b, 'lane': 'glue_then_lambda'} for b in glue_batches]

    if sectors_expected > 0:
        _write_audit_skeleton(run_id, snapshot_date, sectors_expected,
                              planned, mode, analyst)

    # --- dispatch Glue jobs (donor backfills + unmapped uncached) ---
    glue_units = 0
    for batch in glue_batches:
        _start_glue_fetch(batch, run_id, force_backfill, refresh_spec,
                          sectors_expected=sectors_expected, mode=mode)
        glue_units += 1

    # --- dispatch clusters: donor/dependent clusters first, then plain ---
    batch_total = len(clusters)
    lambda_units = 0
    for idx, cluster in enumerate(clusters):
        _send_process_cluster(cluster, run_id, idx, batch_total,
                              sectors_expected, mode)
        lambda_units += 1

    logger.info(f"[prefetch] DONE — {lambda_units} SQS message(s) "
                f"({len(process_sectors)} process sector(s)), "
                f"{glue_units} Glue job(s) (fetch-only) | run_id={run_id}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "run_id": run_id,
            "lambda_units": lambda_units,
            "glue_units": glue_units,
            "sectors_total": len(active),
            "sectors_process": len(process_sectors),
            "sectors_glue": len(glue_sectors),
            "mapped_fallbacks": len(mapping),
        })
    }