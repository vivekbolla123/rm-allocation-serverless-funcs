import logging
import boto3
from pymongo import MongoClient
import configsettings
import sys
import json

from redis import Redis
from datetime import datetime
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from common_utils import instrument_engine
from constants_utils import ConstantsUtils
from additive_model import AdditivePickupModelProcessor

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ESB_DB = f"mysql+pymysql://{configsettings.ESB_DB_USERNAME}:{configsettings.ESB_DB_PASSWORD}@{configsettings.ESB_DB_WR_APPUSER_URL}/{configsettings.ESB_NAV_SCHEMA_NAME}"

try:

    rm_rd_esb_conn = create_engine(ESB_DB)

    NavitaireConnection = create_engine(
        f"mssql+pymssql://{configsettings.NAV_DB_USER}:"
        f"{quote_plus(configsettings.NAV_DB_PASSWORD)}@"
        f"{configsettings.NAV_DB_HOST}"
    )
    instrument_engine(NavitaireConnection)
    
    mongo_client = MongoClient(
            host=configsettings.MONGO_URL,
            port=27017,
            username=configsettings.MONGO_USERNAME,
            password=configsettings.MONGO_PASSWORD,
            authSource=configsettings.MONGO_AUTH_SOURCE
        )
    db = mongo_client["qp_dw_rmalloc"]
    db_master = mongo_client[configsettings.MONGO_MASTER_DB]

    sqs = boto3.client('sqs')
    
    s3 = boto3.client("s3")

except Exception as e:
    logger.error(e)
    sys.exit()

module_map = {
    'additive_process' : AdditivePickupModelProcessor,
}


def lambda_handler(event, context):
    events = boto3.client('events')
    if "Records" in event:
        # SQS event
        events = event["Records"]
        for i in events:
            body = i["body"]
    elif "body" in event:
        # API Gateway event
        body = event["body"]
    else:
        body = json.dumps(event)
        
    data = json.loads(body)
    print(data)

    if "modules" not in data:
        logger.info("Error! 'modules' key missing in body ")
        return False

    modules = data["modules"]

    overrides = {}
    params = {}

    if "overrides" in data:
        # Direct invocation / local testing — overrides nested under 'overrides' key
        overrides = data["overrides"]
    else:
        # SQS message from rm-pricing-fetch-inputs — flat structure
        # Build overrides from top-level keys
        overrides = {
            k: data[k] for k in ('route', 'analyst', 'run_id', 'batch_index',
                                  'batch_total', 'ladder_codes')
            if k in data
        }

    for module_key in modules.keys():
        params["rdEsbConn"] = rm_rd_esb_conn
        params["mongo"]=db
        params["mongo_master"] = db_master
        params['navconn'] = NavitaireConnection
        params['sqs'] = sqs
        params['s3'] = s3
        params['events'] = events 
        params['constantutils'] = ConstantsUtils(db)
        
        Module = module_map[module_key]
        Module_obj = Module(logger, params, overrides, context)
        Module_obj.run()

    return {
        "statusCode": 200, 
        "body": "Cron job executed successfully."
    }


# --- local testing ---
# messageBody = {
#     "modules": {"additive_process": 1},
#     "overrides": {"sectors": ["BOMDEL", "DELBOM"], "run_uuid": "test-123", "env": "uat"}
# }
# event = {'Records': [{'body': json.dumps(messageBody)}]}
# lambda_handler(event, None)