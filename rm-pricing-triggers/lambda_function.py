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
from actionableSectors import ActionSectors
from ladderGen import LadderGen
from salesFare import SalesFare
from fareFile import FareFiling
from tariffGen import TariffGen
from sectorMonitor import SectorMonitor
from fareMonitor import FareMonitor


logger = logging.getLogger()
logger.setLevel(logging.INFO)

ESB_DB = f"mysql+pymysql://{configsettings.ESB_DB_USERNAME}:{configsettings.ESB_DB_PASSWORD}@{configsettings.ESB_DB_WR_APPUSER_URL}/{configsettings.ESB_NAV_SCHEMA_NAME}"
DW_DB_CONN = f"mysql+pymysql://{configsettings.DW_DB_USER}:{configsettings.DW_DB_PASSWORD}@{configsettings.DW_DB_HOST}/{configsettings.DW_DB_WRITE_SCHEMA}"

try:

    rm_rd_esb_conn = create_engine(ESB_DB)
    dw_conn = create_engine(DW_DB_CONN)

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
    db = mongo_client["qp_dw_priaut"]
    db_fares = mongo_client["qp_dw_rmalloc"]
    db_master = mongo_client[configsettings.MONGO_MASTER_DB]

    sqs = boto3.client('sqs')
    
    s3 = boto3.client("s3")

except Exception as e:
    logger.error(e)
    sys.exit()

module_map = {
    'action_sect' : ActionSectors,
    'ladder_gen' : LadderGen,
    'sales_fare' : SalesFare,
    'fare_filing': FareFiling,
    'tariff_gen': TariffGen,
    'sector_monitor': SectorMonitor,
    'fare_monitor': FareMonitor
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
        params["dwconn"] = dw_conn
        params["mongo"]=db
        params["mongofares"] = db_fares
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


# DO COMMENT THE BELOW LINE AFTER TESTING

# --- Local testing: process ALL ladder codes for a route ---
# messageBody = {
#     "overrides": {
#         "route": "I",
#         "analyst": "Kenneth Menezes",
#     },
#     "modules": {"ladder_gen": 1}
# }

# messageBody = {
#     "overrides": {
#         "route": "I",
#         "analyst": "Kenneth Menezes"
#     },
#     "modules": {"action_sect": 1}
# }

# messageBody = {
#     "overrides": {
#         "analyst": "Kenneth Menezes"
#     },
#     "modules": {"sales_fare": 1}
# }

# messageBody = {
#     "overrides": {
#         "analyst": "Kenneth Menezes",
#         "type": "Others",
#         "route": "D"
#     },
#     "modules": {"fare_filing": 1}
# }

# --- Tariff Sheet Automation modules ---

# messageBody = {
#     "overrides": {
#         "analyst": "Kenneth Menezes"
#     },
#     "modules": {"tariff_gen": 1}
# }

# messageBody = {
#     "overrides": {
#         "analyst": "Kenneth Menezes"
#     },
#     "modules": {"sector_monitor": 1}
# }

# messageBody = {
#     "overrides": {
#         "analyst": "Kenneth Menezes",
#         "check_type": "daily"
#     },
#     "modules": {"fare_monitor": 1}
# }

# messageBody = {
#     "overrides": {
#         "analyst": "Kenneth Menezes",
#         "check_type": "weekly"
#     },
#     "modules": {"fare_monitor": 1}
# }
 
# event = {'Records':[{'body':json.dumps(messageBody)}]}
# lambda_handler(event,None)