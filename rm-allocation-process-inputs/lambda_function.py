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
WAIT_TIME = 30  # in seconds

LOGS_ON = False

# rds settings
# DB_CONN_STRING = "mysql+mysqldb://root:root@localhost/QP_DW_RMALLOC"
DB_WR_CONN_STRING = f"mysql+mysqldb://{configsettings.RM_DB_APPUSER_USERNAME}:{configsettings.RM_DB_APPUSER_PASSWORD}@{configsettings.RM_DB_APPUSER_URL}/{configsettings.RM_DB_SCHEMA_NAME}"
DB_RD_CONN_STRING = f"mysql+mysqldb://{configsettings.RM_DB_APPUSER_USERNAME_READ}:{configsettings.RM_DB_APPUSER_PASSWORD_READ}@{configsettings.RM_DB_APPUSER_URL_READ}/{configsettings.RM_DB_SCHEMA_NAME}"

# Queue details

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
        "MARKET_LIST_QUEUE_URL": configsettings.MARKET_LIST_QUEUE_URL,
        "RBD_QUEUE_URL": configsettings.RBD_QUEUE_URL,
        "AU_CHUNK_QUEUE_URL": configsettings.AUCHUNKS_QUEUE_URL,
        "NAV_DB_NAME": configsettings.NAV_DB_NAME,
        "API_KEY": configsettings.RM_GW_API_KEY,
        "API_URL": configsettings.NAV_API_URL,
        Constants.CONST_INPUT_SOURCE : Constants.SERVER_INPUT,
        Constants.CONST_LOGS : Constants.SERVER_LOGS,
        Constants.CONST_OUTPUT : Constants.SERVER_OUTPUT,
        "latestCode": True,
        "currentTime": False
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
        data["runTime"]=data["runTime"]
        DynAlloc(input_data, data)

    return {
        'statusCode': 200,
        'body': 'success'
    }

# DO COMMENT THE BELOW LINE AFTER TESTING
# messageBody = { "Origin": "CCU", "Destin": "BLR", "FlightNumber": 1541, "PerType": "D", "PerStart": "12/08/2023", "PerEnd": "12/08/2023", "DOW": 9999919, "strategyFlag": 1, "TimeWindowStart": "16:30", "TimeWindowEnd": "23:30", "CurveID": "CCUBLR_P", "CarrExlusion": "AI", "fareAnchor": "SC_Lean_MO", "fareOffset": "", "FirstRBDAlloc": 5, "OtherRBDAlloc": 3, "B2BBackstop": 5000, "B2CBackstop": 5700, "B2BFactor": "0.6", "SkippingFactor": 0, "OwnFareFlag": "N", "DaySpan": 0, "AutoTimeRangeFlag": 0, "analystName": "Srishti", "openingFares": "1", "runId": "e7ee97be-1887-4d5e-8039-703399f6c687", "update_navitaire_method": "skip" }
# event = {'Records':[{'attributes':{"update_navitaire_method":"sftp"}, 'body':json.dumps(messageBody)}]}
# lambda_handler(event,None)