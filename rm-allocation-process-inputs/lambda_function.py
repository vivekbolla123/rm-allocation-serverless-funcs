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
from runAlloc_Connections import DynAllocConnections
from time_logger import TimeLogger

warnings.filterwarnings('ignore')

# sqs setting
sqs = boto3.client('sqs')  # client is required to interact with
s3 = boto3.client('s3')
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

# create the database connection outside the handler to allow connections to be
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

log_time = TimeLogger(wrconn)
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
        "NAV_DB_NAME": configsettings.NAV_DB_NAME,
        "NAV_DB_DW_NAME": configsettings.NAV_DB_DW_NAME,
        "NAV_DB_WB_NAME": configsettings.NAV_DB_WB_NAME,
        "NAV_DB_REF_NAME":configsettings.NAV_DB_REF_NAME,
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
#        NavitaireUpdateMethod=record['attributes']['update_navitaire_method']
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
        route_type = data["route_type"]
        if route_type == 'connections':
            DynAllocConnections(input_data, data)
        if route_type == 'direct':
            DynAlloc(input_data, data)
    
    log_time.send_time_log_db()
    return {
        'statusCode': 200,
        'body': 'success'
    }

# DO COMMENT THE BELOW LINE AFTER TESTING
messageBody = {"Sector1": "DOHBOM", "Flight1": 71, "Sector2": "BOMCOK", "Flight2": 1518, "Outbound_stop": 1, "PerType": "D", "DOW": 1111111, "Price_Strategy": 1, "Discount_Value": 3683, "FirstRBDAlloc": 10, "OtherRBDAlloc": 10, "B2BBackstop": 0, "B2CBackstop": 4000, "B2BFactor": 0.5, "SkippingFactor": 10, "Transit_time": "3", "Currency": "INR", "fareAnchor": "MAXO", "Offset": "1", "DiscountFlag": "0", "analystName": "VIKRAM", "PerStart": "08/15/2024", "PerEnd": "08/15/2024", "Flag": "1", "runId": "a2a9dbb6-4791-4307-8a2f-d900e84f0a6b", "update_navitaire_method": "sftp", "runTime": "2024-08-08T15:28:28.313680", "route_type": "connections", "queue": "https://sqs.ap-south-1.amazonaws.com/891377165721/test-rm-allocation-queues-rm-allocation-market-list"}


event = {'Records':[{'attributes':{"update_navitaire_method":"sftp"}, 'body':json.dumps(messageBody)}]}
lambda_handler(event,None)
