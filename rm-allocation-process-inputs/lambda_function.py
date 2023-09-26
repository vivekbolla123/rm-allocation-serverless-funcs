import sys
import logging
import configsettings
import json
from sqlalchemy import create_engine
import pymssql
import warnings
import boto3

import Constants
from runAlloc import DynAlloc

warnings.filterwarnings('ignore')

# sqs setting
sqs = boto3.client('sqs')  # client is required to interact with
s3=boto3.client('s3')
# DB settings
TABLE_MARKET_FARES = "market_fares"
WAIT_TIME = 30  # in seconds

LOGS_ON = False

# rds settings
# DB_CONN_STRING = "mysql+mysqldb://root:root@localhost/QP_DW_RMALLOC"
DB_WR_CONN_STRING = f"mysql+mysqldb://{configsettings.RM_DB_APPUSER_USERNAME}:{configsettings.RM_DB_APPUSER_PASSWORD}@{configsettings.RM_DB_APPUSER_URL}/{configsettings.RM_DB_SCHEMA_NAME}"
DB_RD_CONN_STRING = f"mysql+mysqldb://{configsettings.RM_DB_APPUSER_USERNAME}:{configsettings.RM_DB_APPUSER_PASSWORD}@{configsettings.RM_REPLICA_DB_APPUSER_URL}/{configsettings.RM_DB_SCHEMA_NAME}"

# Queue details
MARKET_LIST_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/525872668219/rm-allocation-queues_rm-allocation-market-list"
RBD_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/525872668219/rm-allocation-queues_rm-allocation-rbds"
AUCHUNKS_QUEUE_URL="https://sqs.ap-south-1.amazonaws.com/525872668219/rm-allocation-queues_rm-allocation-au-chunks"
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# create the database connection outside of the handler to allow connections to be
# re-used by subsequent function invocations.
try:
    wrconn = create_engine(DB_WR_CONN_STRING)
    rdconn = create_engine(DB_RD_CONN_STRING)

    NavitaireConnection = pymssql.connect(
        host=configsettings.NAV_DB_HOST,
        user=configsettings.NAV_DB_USER,
        password=configsettings.NAV_DB_PASSWORD
    )
except Exception as e:
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")



def lambda_handler(event, context):
    """
    This function fetches the market_list and inserts it into the queue
    """
    records = event['Records']

    input_data = {
        "debugMode" : False,
        "isUpdateNavitaire" : True,
        "currDate": None,
        "currDateTime": None,
        "MARKET_LIST_QUEUE_URL": MARKET_LIST_QUEUE_URL,
        "RBD_QUEUE_URL": RBD_QUEUE_URL,
        "AU_CHUNK_QUEUE_URL": AUCHUNKS_QUEUE_URL,
        "NAV_DB_NAME": configsettings.NAV_DB_NAME,
        Constants.CONST_INPUT_SOURCE : Constants.SERVER_INPUT,
        Constants.CONST_LOGS : Constants.SERVER_LOGS,
        Constants.CONST_OUTPUT : Constants.SERVER_OUTPUT,
        "latestCode": True
    }

    for record in records:
        content = record['body']
        # NavitaireUpdateMethod=record['attributes']['update_navitaire_method']
        data = json.loads(content)
        data["NavitaireConnection"] = NavitaireConnection
        data["dbconn"] = rdconn
        data["wrconn"] = wrconn
        data["sqs"] = sqs
        data["runSingleMarket"] = True
        data["s3"]=s3
        data["NavitaireUpdateMethod"]=data["update_navitaire_method"]
        DynAlloc(input_data, data)

    return {
        'statusCode': 200,
        'body': 'success'
    }

# DO COMMENT THE BELOW LINE AFTER TESTING
# messageBody = {"Origin":"BOM","Destin":"DEL","FlightNumber":1127,"PerType":"D","PerStart":"03/30/2024","PerEnd":"03/30/2024","DOW":1111111,"strategyFlag":"1","TimeWindowStart":"01:00","TimeWindowEnd":"23:30","CurveID":"DELBOM","CarrExlusion":"**","fareAnchor":"Strategy_2","fareOffset":0.0,"FirstRBDAlloc":-99,"OtherRBDAlloc":10,"B2BBackstop":4200,"B2CBackstop":4710,"B2BFactor":"0.8","SkippingFactor": 0,"runId":"a7831b2b-96ad-4b7b-9753-b774d8e19a6a"}
# event = {'Records':[{'attributes':{"update_navitaire_method":"sftp"}, 'body':json.dumps(messageBody)}]}
# lambda_handler(event,None)