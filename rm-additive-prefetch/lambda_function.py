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

def _sector_has_cache(sector, cache_prefix):
    """True if at least one hist_flown partition exists for this sector in S3."""
    resp = s3.list_objects_v2(
        Bucket=S3_BUCKET, Prefix=f"{cache_prefix}/sector={sector}/", MaxKeys=1)
    return resp.get('KeyCount', 0) > 0


def _send_process_message(sector, run_id, batch_index, batch_total, sectors_expected, mode,
                          simulate_snapshot=None):
    """Send one SQS message for a single sector to the process queue."""
    overrides = {
        'sectors': [sector],
        'run_id': run_id,
        'batch_index': batch_index,
        'batch_total': batch_total,
        'sectors_expected': sectors_expected,
        'mode': mode,
    }
    if simulate_snapshot:
        overrides['simulate_snapshot'] = simulate_snapshot
    message = {
        'modules': {'additive_process': 1},
        'overrides': overrides,
    }
    sqs.send_message(QueueUrl=PROCESS_QUEUE_URL, MessageBody=json.dumps(message))


def _start_glue_fetch(sectors, run_id, force_backfill, refresh_spec,
                      sectors_expected=0, mode='PROD', simulate_snapshot=None):
    """Start a Glue fetch-only job. Glue emits 1 SQS msg/sector after caching."""
    arguments = {
        '--SECTORS': ','.join(sectors),
        '--RUN_ID': run_id,
        '--FORCE_BACKFILL': 'true' if force_backfill else 'false',
        '--REFRESH_SPEC': str(refresh_spec) if refresh_spec else 'none',
        '--SECTORS_EXPECTED': str(sectors_expected),
        '--MODE': mode,
        '--PROCESS_QUEUE_URL': PROCESS_QUEUE_URL,
        '--ENV': ENV,
    }
    # Only pass when simulating — the Glue job treats SIMULATE_SNAPSHOT as an
    # optional arg (present only if supplied on the command line).
    if simulate_snapshot:
        arguments['--SIMULATE_SNAPSHOT'] = simulate_snapshot
    glue.start_job_run(JobName=GLUE_FETCH_JOB, Arguments=arguments)


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
    cache_prefix = 'additive_model_test/_cache/hist_flown' if simulate else CACHE_PREFIX

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

    # --- lane split: cached vs uncached ---
    if force_backfill or refresh_spec:
        cached_sectors, uncached_sectors = [], active
    else:
        cached_sectors = [s for s in active if _sector_has_cache(s, cache_prefix)]
        uncached_set = set(cached_sectors)
        uncached_sectors = [s for s in active if s not in uncached_set]

    logger.info(f"[prefetch] cached={len(cached_sectors)}, uncached={len(uncached_sectors)}")

    sectors_expected = len(active)

    # --- uncached -> Glue fetch-only (chunked); Glue emits SQS per sector ---
    glue_batches = [uncached_sectors[i:i + GLUE_BATCH_SIZE]
                    for i in range(0, len(uncached_sectors), GLUE_BATCH_SIZE)]

    # --- cached -> one SQS message per sector (AWS batch size does the grouping) ---
    batch_total_cached = len(cached_sectors)

    # --- audit skeleton BEFORE dispatching ---
    snapshot_date = datetime.utcnow().strftime('%Y-%m-%d')
    planned = [{'batch_index': i, 'sectors': [s], 'lane': 'lambda'}
               for i, s in enumerate(cached_sectors)]
    planned += [{'sectors': b, 'lane': 'glue_then_lambda'} for b in glue_batches]

    if sectors_expected > 0 and not simulate:
        _write_audit_skeleton(run_id, snapshot_date, sectors_expected,
                              planned, mode, analyst)

    # --- dispatch Glue jobs ---
    glue_units = 0
    for batch in glue_batches:
        _start_glue_fetch(batch, run_id, force_backfill, refresh_spec,
                          sectors_expected=sectors_expected, mode=mode,
                          simulate_snapshot=simulate_snapshot)
        glue_units += 1

    # --- dispatch one SQS message per cached sector ---
    lambda_units = 0
    for idx, sector in enumerate(cached_sectors):
        _send_process_message(sector, run_id, idx, batch_total_cached,
                              sectors_expected, mode,
                              simulate_snapshot=simulate_snapshot)
        lambda_units += 1

    logger.info(f"[prefetch] DONE — {lambda_units} SQS messages (1/sector), "
                f"{glue_units} Glue jobs (fetch-only) | run_id={run_id}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "run_id": run_id,
            "lambda_units": lambda_units,
            "glue_units": glue_units,
            "sectors_total": len(active),
            "sectors_cached": len(cached_sectors),
            "sectors_uncached": len(uncached_sectors),
        })
    }