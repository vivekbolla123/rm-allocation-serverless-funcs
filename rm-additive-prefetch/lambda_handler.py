"""
rm-additive-prefetch  —  Lambda entry point (matches existing lambda pattern)
=============================================================================

Sole job: build sector batches and route them.
  - Reads active sectors.
  - For each sector, checks whether it has a cached history in S3.
  - Splits into groups of GROUP_SIZE (default 50).
  - For each group, decides the lane:
       * FORCE_BACKFILL / REFRESH_MONTHS / no-cache  -> GLUE fetch-only, 5 sec/job
       * otherwise (cached, incremental)             -> PROCESS lambda, cost-binned
  - Fans out via SQS (glue lane) or direct async invoke (lambda lane).

Cost-based binning uses the previous run's per-sector processing_seconds
(from additive_model_run_audit) so no lambda exceeds TARGET_BIN_SECONDS.
"""

import logging
import boto3
import json
import math
import os
from pymongo import MongoClient
import configsettings

logger = logging.getLogger()
logger.setLevel(logging.INFO)

lambda_client = boto3.client('lambda')
glue = boto3.client('glue')
sqs = boto3.client('sqs')
s3 = boto3.client('s3')

mongo_client = MongoClient(
    host=configsettings.MONGO_URL, port=27017,
    username=configsettings.MONGO_USERNAME, password=configsettings.MONGO_PASSWORD,
    authSource=configsettings.MONGO_AUTH_SOURCE)

# ---- config ----
ENV                = os.environ.get('ENV', 'uat')
S3_BUCKET          = configsettings.ADDITIVE_MODEL_BUCKET
CACHE_PREFIX       = f"{configsettings.ADDITIVE_MODEL_FILE_PATH}_cache/hist_flown"
GROUP_SIZE         = 50
SECTORS_PER_UNIT   = 5
TARGET_BIN_SECONDS = 250.0
PROCESS_LAMBDA     = configsettings.ADDITIVE_PROCESS_LAMBDA_NAME
GLUE_FETCH_JOB     = configsettings.ADDITIVE_GLUE_NAME
GLUE_QUEUE_URL     = os.environ.get('GLUE_QUEUE_URL', '')


def _sector_has_cache(sector):
    """True if any hist_flown partition exists for this sector in S3."""
    resp = s3.list_objects_v2(
        Bucket=S3_BUCKET, Prefix=f"{CACHE_PREFIX}/sector={sector}/", MaxKeys=1)
    return resp.get('KeyCount', 0) > 0


def _latest_sector_times():
    """Previous run's per-sector processing seconds, for load balancing."""
    doc = mongo_client['qp_dw_rmalloc'].additive_model_run_audit.find_one(
        {'env': ENV, 'sector_stats': {'$ne': {}}}, sort=[('started_at', -1)])
    if not doc:
        return {}
    return {s: v.get('processing_seconds', 30.0)
            for s, v in doc.get('sector_stats', {}).items()}


def _bin_by_cost(sectors, times, target=TARGET_BIN_SECONDS, cap=SECTORS_PER_UNIT):
    """
    Greedy bin-packing: sort sectors by cost desc, drop each into the lightest
    open bin, respecting both the time target and the per-bin sector cap.
    Sectors with no history default to a nominal cost.
    """
    ranked = sorted(sectors, key=lambda s: times.get(s, 30.0), reverse=True)
    bins = []            # list of {'sectors': [...], 'cost': float}
    for s in ranked:
        c = times.get(s, 30.0)
        placed = False
        for b in bins:
            if len(b['sectors']) < cap and (b['cost'] + c) <= target:
                b['sectors'].append(s); b['cost'] += c; placed = True; break
        if not placed:
            bins.append({'sectors': [s], 'cost': c})
    return [b['sectors'] for b in bins]


def _invoke_process(unit_sectors, run_uuid, group_idx):
    """Async-invoke the process lambda for one cost-balanced unit."""
    lambda_client.invoke(
        FunctionName=PROCESS_LAMBDA,
        InvocationType='Event',                       # async fan-out
        Payload=json.dumps({
            'modules': {'additive_process': 1},
            'overrides': {
                'sectors': unit_sectors,
                'run_uuid': run_uuid,
                'group_index': group_idx,
                'env': ENV,
            }}).encode())


def _dispatch_glue_fetch(unit_sectors, run_uuid, refresh_months, force):
    """Queue a glue fetch-only job for one unit (SQS gives back-pressure/retry)."""
    msg = {
        'sectors': unit_sectors, 'run_uuid': run_uuid,
        'fetch_only': True, 'force_backfill': force,
        'refresh_months': refresh_months, 'env': ENV,
    }
    if GLUE_QUEUE_URL:
        sqs.send_message(QueueUrl=GLUE_QUEUE_URL, MessageBody=json.dumps(msg))
    else:
        glue.start_job_run(JobName=GLUE_FETCH_JOB, Arguments={
            '--SECTORS': ','.join(unit_sectors),
            '--RUN_UUID': run_uuid,
            '--FETCH_ONLY': 'true',
            '--FORCE_BACKFILL': 'true' if force else 'false',
            '--BACKFILL_MONTHS': str(refresh_months or ''),
            '--ENV': ENV,
        })


def lambda_handler(event, context):
    # --- parse body in the established pattern ---
    if "Records" in event:
        body = event["Records"][0]["body"]
    elif "body" in event:
        body = event["body"]
    else:
        body = json.dumps(event)
    data = json.loads(body)
    overrides = data.get("overrides", data)

    run_uuid       = overrides.get('run_uuid') or context.aws_request_id
    force_backfill = str(overrides.get('force_backfill', 'false')).lower() == 'true'
    refresh_months = overrides.get('refresh_months')            # int or None
    target_sectors = overrides.get('sectors')                   # optional explicit list

    # --- active sectors ---
    if target_sectors:
        active = list(target_sectors)
    else:
        docs = mongo_client['qp_rm_master'].active_sectors.find(
            {"sector": "DELBOM"}, {"_id": 0, "sector": 1})
        active = [d['sector'] for d in docs if d.get('sector')]
    active = sorted(set(active))
    logger.info(f"[prefetch] {len(active)} active sectors, force={force_backfill}, "
                f"refresh_months={refresh_months}")

    # --- split into groups of GROUP_SIZE ---
    groups = [active[i:i + GROUP_SIZE] for i in range(0, len(active), GROUP_SIZE)]

    # --- route each group ---
    times = _latest_sector_times()
    glue_units, lambda_units = 0, 0

    for gi, group in enumerate(groups):
        if force_backfill or refresh_months:
            # whole group -> glue fetch-only
            for unit in [group[i:i+SECTORS_PER_UNIT]
                         for i in range(0, len(group), SECTORS_PER_UNIT)]:
                _dispatch_glue_fetch(unit, run_uuid, refresh_months, force_backfill)
                glue_units += 1
            continue

        # incremental: split cached vs uncached
        cached   = [s for s in group if _sector_has_cache(s)]
        uncached = [s for s in group if s not in cached]

        # uncached -> glue fetch-only (new sectors)
        for unit in [uncached[i:i+SECTORS_PER_UNIT]
                     for i in range(0, len(uncached), SECTORS_PER_UNIT)]:
            if unit:
                _dispatch_glue_fetch(unit, run_uuid, None, False)
                glue_units += 1

        # cached -> process lambdas, cost-balanced
        for unit in _bin_by_cost(cached, times):
            _invoke_process(unit, run_uuid, gi)
            lambda_units += 1

    logger.info(f"[prefetch] dispatched {lambda_units} process-lambdas, "
                f"{glue_units} glue-fetch units for run {run_uuid}.")
    return {"statusCode": 200,
            "body": json.dumps({"run_uuid": run_uuid,
                                "lambda_units": lambda_units,
                                "glue_units": glue_units})}


# --- local testing ---
# messageBody = {"overrides": {"env": "uat"}}
# event = {'Records': [{'body': json.dumps(messageBody)}]}
# lambda_handler(event, None)