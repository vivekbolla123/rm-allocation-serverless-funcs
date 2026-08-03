import logging
import boto3
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
            host=configsettings.MONGO_URL,
            port=27017,
            username=configsettings.MONGO_USERNAME,
            password=configsettings.MONGO_PASSWORD,
            authSource=configsettings.MONGO_AUTH_SOURCE
        )
    db = mongo_client["qp_dw_rmalloc"]
    db_master = mongo_client[configsettings.MONGO_MASTER_DB]

    s3 = boto3.client("s3")

except Exception as e:
    logger.error(e)
    sys.exit()

module_map = {
    'additive_process': AdditivePickupModelProcessor,
}


def lambda_handler(event, context):
    if "Records" in event:
        # SQS event (from rm-additive-prefetch)
        records = event["Records"]
        for record in records:
            body = record["body"]
    elif "body" in event:
        # API Gateway event
        body = event["body"]
    else:
        # Direct invocation
        body = json.dumps(event)
        
    data = json.loads(body)
    logger.info(f"Received event: {json.dumps(data, default=str)}")

    if "modules" not in data:
        logger.error("'modules' key missing in body")
        return {"statusCode": 400, "body": "'modules' key missing"}

    modules = data["modules"]

    overrides = {}
    params = {}

    if "overrides" in data:
        # SQS message from rm-additive-prefetch OR direct invocation
        # Message contract: sectors, run_id, batch_index, batch_total,
        # sectors_expected, mode, force_backfill, refresh_spec, analyst
        overrides = data["overrides"]
    else:
        # Flat structure fallback
        overrides = {
            k: data[k] for k in ('sectors', 'run_id', 'batch_index',
                                  'batch_total', 'sectors_expected',
                                  'mode', 'force_backfill', 'refresh_spec',
                                  'analyst')
            if k in data
        }

    for module_key in modules.keys():
        params["rdEsbConn"] = rm_rd_esb_conn
        params["mongo"] = db
        params["mongo_master"] = db_master
        params['s3'] = s3
        params['constantutils'] = ConstantsUtils(db)
        
        Module = module_map[module_key]
        module_obj = Module(logger, params, overrides, context)
        result = module_obj.run()

    return {
        "statusCode": 200,
        "body": json.dumps(result, default=str) if result else "Executed successfully."
    }


# --- Local testing ---
# messageBody = {
#     "modules": {"additive_process": 1},
#     "overrides": {
#         "sectors": ["AUHBLR"],
#         "run_id": "test-local-001",
#         "batch_index": 0,
#         "batch_total": 1,
#         "sectors_expected": 1,
#         "mode": "TEST",
#         "analyst": "Kenneth Menezes"
#     }
# }
# event = {'Records': [{'body': json.dumps(messageBody)}]}
# lambda_handler(event, None)