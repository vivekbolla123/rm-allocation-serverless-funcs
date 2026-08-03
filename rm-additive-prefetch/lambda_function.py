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

Override: sectors_per_bin
  If set to 1, bypasses LPT and sends exactly 1 sector per SQS message
  (1 Lambda per sector). Useful for debugging or when you want maximum
  parallelism regardless of cost balancing.

Message contract (SQS -> rm-additive-process):
{
  "modules": {"additive_process": 1},
  "overrides": {
    "sectors": ["AUHBLR", "BOMDXB", ...],
    "run_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "batch_index": 3,
    "batch_total": 10,
    "sectors_expected": 48,
    "mode": "PROD"
  }
}
"""

import logging
import math
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
ENV                = os.environ.get('ENV', 'uat')
S3_BUCKET          = configsettings.ADDITIVE_MODEL_BUCKET
CACHE_PREFIX       = f"{configsettings.ADDITIVE_MODEL_FILE_PATH}_cache/hist_flown"
PROCESS_QUEUE_URL  = configsettings.ADDITIVE_SQS_URL   # SQS queue -> process Lambda
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


def _lpt_bin_pack(sectors, times, target=TARGET_BIN_SECONDS, max_bins=MAX_PARALLEL,
                  default_cost=DEFAULT_SECTOR_COST):
    """
    Longest-Processing-Time-first greedy bin-packing (min-makespan).

    1. Cost each sector from audit history; unknown -> default_cost.
    2. Choose bin count k = clamp(ceil(total_cost / target), 1, max_bins).
    3. Sort sectors by cost descending.
    4. Greedy assign: place each sector into the currently-lightest bin.
    """
    costs = {s: times.get(s, default_cost) for s in sectors}
    total_cost = sum(costs.values())

    k = max(1, min(max_bins, int(math.ceil(total_cost / target))))

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

    logger.info(f"[LPT] {len(sectors)} sectors -> {len(bins)} bins "
                f"(target={target}s, max_bins={max_bins}), "
                f"loads: {[round(l, 1) for l in sorted(bin_loads, reverse=True) if l > 0]}")
    return bins


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


def _start_glue_fetch(sectors, run_id, force_backfill, refresh_spec,
                      sectors_expected=0, mode='PROD'):
    """
    Start a Glue job that does fetch-only (populates S3 cache).

    After caching completes, the Glue job sends 1 SQS message per sector
    to PROCESS_QUEUE_URL so process Lambdas pick them up. This keeps all
    processing logic in Lambdas.

    Glue arguments include the SQS URL, run_id, and sectors_expected so it
    can form the correct message contract.
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
    run_id = overrides.get('run_id') or str(uuid4())
    mode = overrides.get('mode', 'PROD').upper()
    force_backfill = str(overrides.get('force_backfill', 'false')).lower() == 'true'
    refresh_spec = overrides.get('refresh_spec')        # int N or 'YYYY-MM,...'
    analyst = overrides.get('analyst', 'system')
    target_sectors = overrides.get('sectors')            # optional explicit list
    sectors_per_bin = overrides.get('sectors_per_bin')   # int: bypass LPT, fixed N per message

    # Bin-packing tunables (override hardcoded defaults)
    target_bin_seconds = float(overrides.get('target_bin_seconds', TARGET_BIN_SECONDS))
    max_parallel = int(overrides.get('max_parallel', MAX_PARALLEL))
    default_sector_cost = float(overrides.get('default_sector_cost', DEFAULT_SECTOR_COST))

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

    # --- ALL sectors that will produce forecasts (both lanes) ---
    # Uncached sectors will be processed AFTER Glue populates their cache,
    # but we count them now so finalization knows the total.
    sectors_expected = len(active)

    # --- route UNCACHED -> Glue (fetch-only, populates cache) ---
    # After Glue finishes, it sends 1 SQS message per sector to the process
    # queue with the same run_id. Process Lambda then runs them like any other.
    glue_units = 0
    glue_batches = []
    GLUE_BATCH_SIZE = 10  # sectors per Glue run

    for i in range(0, len(uncached_sectors), GLUE_BATCH_SIZE):
        batch = uncached_sectors[i:i + GLUE_BATCH_SIZE]
        glue_batches.append(batch)

    # --- route CACHED -> SQS -> process Lambdas (LPT bin-packed or fixed-size) ---
    lambda_units = 0
    bins = []

    if cached_sectors:
        if sectors_per_bin:
            # Fixed-size bins: bypass LPT, send exactly N sectors per message
            n = int(sectors_per_bin)
            bins = [cached_sectors[i:i + n]
                    for i in range(0, len(cached_sectors), n)]
            logger.info(f"[prefetch] Fixed binning: {n} sector(s)/message -> {len(bins)} bins")
        else:
            # LPT cost-balanced binning
            times = _latest_sector_times()
            bins = _lpt_bin_pack(cached_sectors, times,
                                 target=target_bin_seconds,
                                 max_bins=max_parallel,
                                 default_cost=default_sector_cost)

    # --- Write audit skeleton BEFORE dispatching ---
    # batch_total for the cached lane only (Glue lane adds its own messages later)
    batch_total_cached = len(bins)
    snapshot_date = datetime.utcnow().strftime('%Y-%m-%d')

    planned = []
    for i, b in enumerate(bins):
        planned.append({'batch_index': i, 'sectors': b, 'lane': 'lambda'})
    for j, b in enumerate(glue_batches):
        planned.append({'sectors': b, 'lane': 'glue_then_lambda'})

    if sectors_expected > 0:
        _write_audit_skeleton(
            run_id, snapshot_date, sectors_expected,
            planned_bins=planned, mode=mode, analyst=analyst)

    # --- Dispatch Glue jobs (fetch-only — Glue sends SQS after caching) ---
    for j, batch in enumerate(glue_batches):
        _start_glue_fetch(batch, run_id, force_backfill, refresh_spec,
                          sectors_expected=sectors_expected, mode=mode)
        glue_units += 1

    # --- Dispatch SQS messages for cached sectors ---
    for idx, bin_sectors in enumerate(bins):
        _send_process_message(
            bin_sectors, run_id, idx, batch_total_cached, sectors_expected, mode)
        lambda_units += 1

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
