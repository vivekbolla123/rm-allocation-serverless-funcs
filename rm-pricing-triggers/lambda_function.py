import logging
import boto3
import pymssql
import configsettings
import sys
import json

from redis import Redis
from datetime import datetime
from sqlalchemy import create_engine
from actionableSectors import ActionSectors
from constants_utils import ConstantsUtils


logger = logging.getLogger()
logger.setLevel(logging.INFO)

RM_ALLOC_DB = f"mysql+pymysql://{configsettings.RM_DB_APPUSER_USERNAME}:{configsettings.RM_DB_APPUSER_PASSWORD}@{configsettings.RM_DB_APPUSER_URL}/{configsettings.RM_DB_SCHEMA_NAME}"
RM_REPORTS_DB = f"mysql+pymysql://{configsettings.RM_DB_APPUSER_USERNAME}:{configsettings.RM_DB_APPUSER_PASSWORD}@{configsettings.RM_DB_WR_APPUSER_URL}/{configsettings.RM_DB_SCHEMA_NAME}"
ESB_DB = f"mysql+pymysql://{configsettings.ESB_DB_USERNAME}:{configsettings.ESB_DB_PASSWORD}@{configsettings.ESB_DB_WR_APPUSER_URL}/{configsettings.ESB_NAV_SCHEMA_NAME}"
DB_DW_WR_CONN_STRING = f"mysql+pymysql://{configsettings.DW_DB_USER}:{configsettings.DW_DB_PASSWORD}@{configsettings.DW_DB_HOST}/{configsettings.DW_DB_WRITE_SCHEMA}"

CACHE_ENDPOINT = configsettings.RM_CACHE_ENDPOINT
CACHE_PORT = configsettings.RM_CACHE_PORT
CACHE_USERNAME = configsettings.RM_CACHE_USERNAME
CACHE_PASSWORD = configsettings.RM_CACHE_PASSWORD
cache_client = None

try:
    rm_rd_conn = create_engine(RM_ALLOC_DB)
    rm_wr_conn = create_engine(RM_REPORTS_DB)
    rm_rd_esb_conn = create_engine(ESB_DB)
    dw_conn =  create_engine(DB_DW_WR_CONN_STRING)


    NavitaireConnection = pymssql.connect(
            host=configsettings.NAV_DB_HOST,
            user=configsettings.NAV_DB_USER,
            password=configsettings.NAV_DB_PASSWORD
        )

    
    cache_client = Redis(host=CACHE_ENDPOINT, port=CACHE_PORT, username=CACHE_USERNAME, password=CACHE_PASSWORD)
    # Initialize AWS resources
    sqs = boto3.client('sqs')
    
    s3 = boto3.client("s3")

except Exception as e:
    logger.error(e)
    sys.exit()

module_map = {
    'action_sect' : ActionSectors
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
        overrides = data["overrides"]

    for module_key in modules.keys():
        params["dwconn"] = dw_conn
        params["rdEsbConn"] = rm_rd_esb_conn
        params['rdconn'] = rm_rd_conn
        params['wrconn'] = rm_wr_conn
        params['navconn'] = NavitaireConnection
        params['sqs'] = sqs
        params['s3'] = s3
        params['cache_client'] = cache_client
        params['events'] = events 
        params['constantutils'] = ConstantsUtils(rm_rd_conn)
        
        Module = module_map[module_key]
        Module_obj = Module(logger, params, overrides, context)
        Module_obj.run()

    return {
        "statusCode": 200, 
        "body": "Cron job executed successfully."
    }


# DO COMMENT THE BELOW LINE AFTER TESTING
# messageBody = {
#     "overrides": {
#         "route": "I",
#         "analyst": "Kenneth Menezes"
#     },
#     "modules": {"action_sect": 1}
# }
 

# messageBody = {
#   "body": "{\"modules\":{\"action_sect\": 1}}"
# }

# event = {'Records':[{'body':json.dumps(messageBody)}]}
# lambda_handler(event,None)
