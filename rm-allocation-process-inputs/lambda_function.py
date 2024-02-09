import sys
import logging
import configsettings
import json
from sqlalchemy import create_engine
import pymssql
import warnings
import boto3
from redis import Redis
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

CACHE_ENDPOINT = configsettings.RM_CACHE_ENDPOINT
CACHE_PORT = configsettings.RM_CACHE_PORT
CACHE_USERNAME = configsettings.RM_CACHE_USERNAME
CACHE_PASSWORD = configsettings.RM_CACHE_PASSWORD

cache_client = Redis(host=CACHE_ENDPOINT, port=CACHE_PORT, username=CACHE_USERNAME, password=CACHE_PASSWORD)
if cache_client.ping():
    print("Connected to Redis")

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
        "NAV_DB_DW_NAME": configsettings.NAV_DB_DW_NAME,
        "NAV_DB_WB_NAME": configsettings.NAV_DB_WB_NAME,
        "API_KEY": configsettings.RM_GW_API_KEY,
        "API_URL": configsettings.NAV_API_URL,
        "NAV_BASE_API_URL": configsettings.NAV_BASE_URL,
        "NAV_OCP_SUBSCRIPTION_KEY": configsettings.NAV_OCP_SUBSCRIPTION_KEY,
        "NAV_USERNAME": configsettings.NAV_USERNAME,
        "NAV_PWD": configsettings.NAV_PWD,
        "NAV_DOMAIN": configsettings.NAV_DOMAIN,
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
        data['cache_client']=cache_client
        data["NavitaireUpdateMethod"]=data["update_navitaire_method"]
        data["runTime"]=data["runTime"]
        DynAlloc(input_data, data)

    return {
        'statusCode': 200,
        'body': 'success'
    }

# DO COMMENT THE BELOW LINE AFTER TESTING
# messageBody= {   "Origin": "BOM",
#     "Destin": "BLR",
#     "FlightNumber": 1374,
#     "PerType": "D",
#     "PerStart": "01/30/2024",
#     "PerEnd": "01/30/2024",
#     "DOW": 1111111,
#     "strategyFlag": 1,
#     "TimeWindowStart": "10:45",
#     "TimeWindowEnd": "13:55",
#     "CurveID": "BOMBLR_P",
#     "CarrExlusion": "**",
#     "fareAnchor": "Strategy_NP_Weak",
#     "fareOffset": "",
#     "FirstRBDAlloc": 3,
#     "OtherRBDAlloc": 2,
#     "B2BBackstop": 2810,
#     "B2CBackstop": 3450,
#     "B2BFactor": "0.5",
#     "SkippingFactor": 0,
#     "OwnFareFlag": "N",
#     "DaySpan": 0,
#     "AutoTimeRangeFlag": 0,
#     "analystName": "Neha",
#     "openingFares": "0",
#     "OverBooking": "Y",
#     "runId": "6a918d7c-7ed6-47cd-9a41-e5e64fcebb3b",
#     "update_navitaire_method": "api",
#     "runTime": 1703021133338
# }

# event = {'Records':[{'attributes':{"update_navitaire_method":"sftp"}, 'body':json.dumps(messageBody)}]}
# lambda_handler(event,None)
