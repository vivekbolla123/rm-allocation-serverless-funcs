"""
rm-additive-prefetch  —  Lambda entry point
=============================================================================

The planner / load balancer. Sole job: build sector bins and route them.

  1. Reads active sectors from qp_rm_master.active_sectors.
  2. For each sector, checks whether it has a cached history in S3.
  3. Splits into two lanes:
       * NO cache (new/backfill) OR FORCE_BACKFILL/REFRESH  -> Glue fetch-only
         (direct start_job_run — one Glue run per batch of uncached sectors)
       * Cached (incremental)  -> Process Lambda via SQS
         (one SQS message per cost-balanced bin)
  4. Writes the run skeleton audit document to MongoDB.

Cost-based binning (LPT) uses the previous run's per-sector seconds from
additive_model_run_audit so no single Lambda exceeds TARGET_BIN_SECONDS.

Message contract (SQS -> rm-additive-process):
{
  "modules": {"additive_process": 1},
  "overrides": {
    "sectors": ["AUHBLR", "BOMDXB", ...],
    "run_id": "2026-07-03T02:00:05Z-ab12",
    "batch_index": 3,
    "batch_total": 10,
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
lambda_client = boto3.client('lambda')
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
ENV                = os.environ.get('ENV', 'uat')
S3_BUCKET          = configsettings.ADDITIVE_MODEL_BUCKET
CACHE_PREFIX       = f"{configsettings.ADDITIVE_MODEL_FILE_PATH}_cache/hist_flown"
PROCESS_QUEUE_URL  = configsettings.ADDITIVE_SQS_URL   # SQS queue -> process Lambda
PROCESS_LAMBDA     = configsettings.ADDITIVE_PROCESS_LAMBDA_NAME
GLUE_FETCH_JOB     = configsettings.ADDITIVE_GLUE_NAME

# Bin-packing parameters
TARGET_BIN_SECONDS = 250.0       # max wall-clock per process Lambda
MAX_PARALLEL       = 15          # cap on number of bins
DEFAULT_SECTOR_COST = 30.0       # nominal cost for sectors with no history
EWMA_ALPHA         = 0.7         # smoothing: 0.7*last + 0.3*prev


# =============================================================================
# HELPERS
# =============================================================================

def _sector_has_cache(sector):
    """True if at least one hist_flown partition exists for this sector in S3."""
    resp = s3.list_objects_v2(
        Bucket=S3_BUCKET, Prefix=f"{CACHE_PREFIX}/sector={sector}/", MaxKeys=1)
    return resp.get('KeyCount', 0) > 0


def _latest_sector_times():
    """
    Read per-sector processing seconds from the most recent completed audit doc.
    Returns dict: {sector: seconds}.
    """
    doc = db_rmalloc.additive_model_run_audit.find_one(
        {'module': 'additive_pickup_model', 'status': {'$in': ['SUCCESS', 'PARTIAL']}},
        sort=[('start_time', -1)],
        projection={'sector_stats': 1})
    if not doc:
        return {}
    return {
        s: v.get('seconds', DEFAULT_SECTOR_COST)
        for s, v in doc.get('sector_stats', {}).items()
        if v.get('status') == 'SUCCESS'
    }


def _lpt_bin_pack(sectors, times):
    """
    Longest-Processing-Time-first greedy bin-packing (min-makespan).

    1. Cost each sector from audit history; unknown -> high default.
    2. Choose bin count k = clamp(ceil(total_cost / TARGET), 1, MAX_PARALLEL).
    3. Sort sectors by cost descending.
    4. Greedy assign: place each sector into the currently-lightest bin.
    """
    costs = {s: times.get(s, DEFAULT_SECTOR_COST) for s in sectors}
    total_cost = sum(costs.values())

    k = max(1, min(MAX_PARALLEL, int(math.ceil(total_cost / TARGET_BIN_SECONDS))))

    # Sort descending by cost (LPT)
    ranked = sorted(sectors, key=lambda s: costs[s], reverse=True)

    # Greedy placement into lightest bin
    bins = [[] for _ in range(k)]
    bin_loads = [0.0] * k

    for s in ranked:
        lightest = min(range(k), key=lambda i: bin_loads[i])
        bins[lightest].append(s)
        bin_loads[lightest] += costs[s]

    # Remove empty bins (if k > len(sectors))
    bins = [b for b in bins if b]

    logger.info(f"[LPT] {len(sectors)} sectors -> {len(bins)} bins, "
                f"loads: {[round(l, 1) for l in sorted(bin_loads, reverse=True) if l > 0]}")
    return bins


import math  # noqa: E402 (already imported at top but kept for clarity)


def _send_process_message(bin_sectors, run_id, batch_index, batch_total, sectors_expected, mode):
    """Send one SQS message per bin to trigger a process Lambda."""
    message = {
        'modules': {'additive_process': 1},
        'overrides': {
            'sectors': bin_sectors,
            'run_id': run_id,
            'batch_index': batch_index,
            'batch_total': batch_total,
            'sectors_expected': sectors_expected,
            'mode': mode,
        }
    }
    sqs.send_message(
        QueueUrl=PROCESS_QUEUE_URL,
        MessageBody=json.dumps(message))


def _start_glue_fetch(sectors, run_id, force_backfill, refresh_spec):
    """Start a Glue fetch-only job directly (no SQS — Glue has its own retry)."""
    glue.start_job_run(
        JobName=GLUE_FETCH_JOB,
        Arguments={
            '--SECTORS': ','.join(sectors),
            '--RUN_ID': run_id,
            '--FETCH_ONLY': 'true',
            '--FORCE_BACKFILL': 'true' if force_backfill else 'false',
            '--REFRESH_SPEC': str(refresh_spec or ''),
            '--ENV': ENV,
        })


def _write_audit_skeleton(run_id, snapshot_date, sectors_expected, planned_bins, mode, analyst):
    """Write the run skeleton doc so process Lambdas can $inc into it."""
    db_rmalloc.additive_model_run_audit.insert_one({
        'run_id': run_id,
        'module': 'additive_pickup_model',
        'env': ENV,
        'snapshot_date': snapshot_date,
        'status': 'IN_PROGRESS',
        'sectors_expected': sectors_expected,
        'sectors_completed': 0,
        'batches_seen': [],
        'start_time': datetime.utcnow(),
        'end_time': None,
        'sector_stats': {},
        'planned_bins': planned_bins,
        'mode': mode,
        'triggered_by': analyst,
    })


# =============================================================================
# HANDLER
# =============================================================================

def lambda_handler(event, context):
    # --- parse body (same pattern as all RM lambdas) ---
    if "Records" in event:
        body = event["Records"][0]["body"]
    elif "body" in event:
        body = event["body"]
    else:
        body = json.dumps(event)

    data = json.loads(body)
    overrides = data.get("overrides", data)

    # --- resolve parameters ---
    run_id = overrides.get('run_id') or f"{datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}-{uuid4().hex[:4]}"
    mode = overrides.get('mode', 'PROD').upper()
    force_backfill = str(overrides.get('force_backfill', 'false')).lower() == 'true'
    refresh_spec = overrides.get('refresh_spec')        # int N or 'YYYY-MM,...'
    analyst = overrides.get('analyst', 'system')
    target_sectors = overrides.get('sectors')            # optional explicit list

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

    # --- lane split: cached vs uncached ---
    if force_backfill or refresh_spec:
        # Everything routes to Glue fetch-only (full refetch / month refresh)
        cached_sectors = []
        uncached_sectors = active
    else:
        cached_sectors = [s for s in active if _sector_has_cache(s)]
        uncached_sectors = [s for s in active if s not in set(cached_sectors)]

    logger.info(f"[prefetch] cached={len(cached_sectors)}, uncached={len(uncached_sectors)}")

    # --- route UNCACHED -> Glue fetch-only (direct start) ---
    glue_units = 0
    GLUE_BATCH_SIZE = 10  # sectors per Glue run (Glue can handle larger batches)

    for i in range(0, len(uncached_sectors), GLUE_BATCH_SIZE):
        batch = uncached_sectors[i:i + GLUE_BATCH_SIZE]
        _start_glue_fetch(batch, run_id, force_backfill, refresh_spec)
        glue_units += 1

    # --- route CACHED -> SQS -> process Lambdas (LPT bin-packed) ---
    lambda_units = 0
    bins = []

    if cached_sectors:
        times = _latest_sector_times()
        bins = _lpt_bin_pack(cached_sectors, times)

        sectors_expected = len(cached_sectors)
        batch_total = len(bins)

        for idx, bin_sectors in enumerate(bins):
            _send_process_message(
                bin_sectors, run_id, idx, batch_total, sectors_expected, mode)
            lambda_units += 1

        # --- write audit skeleton (only for process lane) ---
        snapshot_date = datetime.utcnow().strftime('%Y-%m-%d')
        _write_audit_skeleton(
            run_id, snapshot_date, sectors_expected,
            planned_bins=[{'batch_index': i, 'sectors': b} for i, b in enumerate(bins)],
            mode=mode, analyst=analyst)

    logger.info(f"[prefetch] DONE — {lambda_units} SQS messages (process), "
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


# --- Local testing ---
# messageBody = {
#     "overrides": {
#         "mode": "PROD",
#         "analyst": "Kenneth Menezes"
#     }
# }
# event = {'Records': [{'body': json.dumps(messageBody)}]}
# lambda_handler(event, type('ctx', (), {'aws_request_id': 'local-test-001'})())
