import logging
import boto3
import traceback
from pymongo import MongoClient
import configsettings
import sys
import json

from datetime import datetime
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from constants_utils import ConstantsUtils
from additive_model import AdditivePickupModelProcessor

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ESB_DB = f"mysql+pymysql://{configsettings.ESB_DB_USERNAME}:{configsettings.ESB_DB_PASSWORD}@{configsettings.ESB_DB_WR_APPUSER_URL}/{configsettings.ESB_NAV_SCHEMA_NAME}"

try:
    rm_rd_esb_conn = create_engine(ESB_DB)
    mongo_client = MongoClient(
        host=configsettings.MONGO_URL, port=27017,
        username=configsettings.MONGO_USERNAME,
        password=configsettings.MONGO_PASSWORD,
        authSource=configsettings.MONGO_AUTH_SOURCE)
    db = mongo_client["qp_dw_rmalloc"]
    db_master = mongo_client[configsettings.MONGO_MASTER_DB]
    s3 = boto3.client("s3")
except Exception as e:
    logger.error(e)
    sys.exit()

module_map = {
    'additive_process': AdditivePickupModelProcessor,
}


def _run_modules(modules, overrides, context):
    """Run each requested module once for the given (possibly merged) bin."""
    params = {
        "rdEsbConn": rm_rd_esb_conn,
        "mongo": db,
        "mongo_master": db_master,
        "s3": s3,
        "constantutils": ConstantsUtils(db),
    }
    result = None
    for module_key in modules.keys():
        Module = module_map[module_key]
        result = Module(logger, params, overrides, context).run()
    return result


def lambda_handler(event, context):
    # --- SQS path: batch size > 1 delivers MULTIPLE 1-sector messages ---
    if "Records" in event:
        records = event["Records"]
        merged_sectors, sectors_by_msg = [], {}
        base_overrides, modules = None, None

        for rec in records:
            data = json.loads(rec["body"])
            if modules is None:
                modules = data.get("modules")
            ov = data.get("overrides", data)
            secs = list(ov.get("sectors") or [])
            sectors_by_msg[rec["messageId"]] = secs
            merged_sectors.extend(secs)
            if base_overrides is None:
                base_overrides = dict(ov)   # run_id/sectors_expected/mode identical across the batch

        if not modules:
            logger.error("'modules' key missing in all records")
            # nothing runnable -> redeliver everything so it isn't silently dropped
            return {"batchItemFailures": [{"itemIdentifier": m} for m in sectors_by_msg]}

        overrides = {**base_overrides, "sectors": sorted(set(merged_sectors))}
        logger.info(f"Batched {len(records)} message(s) -> "
                    f"{len(overrides['sectors'])} sector(s): {overrides['sectors']}")

        try:
            result = _run_modules(modules, overrides, context)
            failed = set(result.get("sectors_failed") or []) if result else set(overrides["sectors"])
        except Exception as e:
            logger.error(f"Batch run crashed: {e}\n{traceback.format_exc()}")
            failed = set(overrides["sectors"])          # redeliver the whole batch

        # a message redelivers only if one of ITS sectors failed (1:1 here)
        batch_failures = [
            {"itemIdentifier": mid}
            for mid, secs in sectors_by_msg.items()
            if failed.intersection(secs)
        ]
        if batch_failures:
            logger.warning(f"Reporting {len(batch_failures)} message(s) for redelivery")
        return {"batchItemFailures": batch_failures}

    # --- API Gateway / direct invocation: single payload, unchanged ---
    body = event["body"] if "body" in event else json.dumps(event)
    data = json.loads(body)
    logger.info(f"Received event: {json.dumps(data, default=str)}")
    if "modules" not in data:
        logger.error("'modules' key missing in body")
        return {"statusCode": 400, "body": "'modules' key missing"}
    overrides = data.get("overrides", data)
    result = _run_modules(data["modules"], overrides, context)
    return {"statusCode": 200,
            "body": json.dumps(result, default=str) if result else "Executed successfully."}

# Uncomment the following lines to test the lambda_handler function locally with a sample payload.

# event = {
#     "modules": {"additive_process": 1},
#     "overrides": {"mode": "TEST", "sectors": ["BOMDEL"], "analyst": "Kenneth Menezes"}
# }
# lambda_handler(event, None)