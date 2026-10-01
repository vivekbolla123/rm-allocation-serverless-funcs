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
DW_DB = f"mysql+pymysql://{configsettings.DW_DB_USER}:{quote_plus(configsettings.DW_DB_PASSWORD)}@{configsettings.DW_DB_HOST}/{configsettings.DW_DB_WRITE_SCHEMA}"

try:
    rm_rd_esb_conn = create_engine(ESB_DB)
    dw_conn = create_engine(DW_DB)
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
        "dwconn": dw_conn,
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
    # --- SQS path: batch size up to 5 delivers MULTIPLE 1-sector messages ---
    #
    # Contract (one sector per message):
    #   plain      -> {"sectors": ["DELBOM"]}
    #   donor      -> {"sectors": ["BOMDEL"], "dependent_sectors": ["NMIDEL", ...]}
    #   dependent  -> {"sectors": ["JAIBLR"], "mapped_sector": "BOMDEL"}
    #
    # Each SQS message is processed INDEPENDENTLY (not merged into one bin) so a
    # per-message failure only redelivers that message. A donor, after it finishes
    # processing, re-enqueues its dependents (see AdditivePickupModelProcessor.run).
    if "Records" in event:
        records = event["Records"]
        batch_failures = []
        for rec in records:
            mid = rec["messageId"]
            try:
                data = json.loads(rec["body"])
            except Exception as e:
                logger.error(f"[{mid}] bad JSON body: {e}")
                batch_failures.append({"itemIdentifier": mid})
                continue

            modules = data.get("modules")
            ov = data.get("overrides", data)
            secs = list(ov.get("sectors") or [])
            if not modules or not secs:
                logger.error(f"[{mid}] missing 'modules' or 'sectors' — redelivering")
                batch_failures.append({"itemIdentifier": mid})
                continue

            overrides = dict(ov)
            overrides["sectors"] = secs
            dep = overrides.get("dependent_sectors")
            mp = overrides.get("mapped_sector")
            logger.info(f"[{mid}] sector(s)={secs}"
                        + (f" donor->dependents={dep}" if dep else "")
                        + (f" dependent(mapped<-{mp})" if mp else ""))

            try:
                result = _run_modules(modules, overrides, context)
                failed = set(result.get("sectors_failed") or []) if result else set(secs)
            except Exception as e:
                logger.error(f"[{mid}] run crashed: {e}\n{traceback.format_exc()}")
                failed = set(secs)

            if failed:
                logger.warning(f"[{mid}] reporting for redelivery (failed: {sorted(failed)})")
                batch_failures.append({"itemIdentifier": mid})

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
#     "overrides": {"mode": "TEST", "sectors": ["DELBLR"], "analyst": "Kenneth Menezes"}
# }
# lambda_handler(event, None)