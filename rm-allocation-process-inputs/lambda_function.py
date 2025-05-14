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
from helper.rm_database_helper import RMDatabaseHelper
from runAlloc import DynAlloc
from runAlloc_Connections import DynAllocConnections
from time_logger import TimeLogger
from constants_utils import ConstantsUtils
import pandas as pd

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
DW_DB_CONN_STRING = f"mysql+mysqldb://{configsettings.DW_DB_USER}:{configsettings.DW_DB_PASSWORD}@{configsettings.DW_DB_HOST}/{configsettings.DW_DB_WRITE_SCHEMA}"

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
    dwconn = create_engine(DW_DB_CONN_STRING)
except Exception as e:
    logger.error(e)
    sys.exit()

constantsutils = ConstantsUtils(wrconn)
log_time = TimeLogger(wrconn, constantsutils.CHECK_THRESHOLD_PROCESS_INPUTS)
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
        data["timelogger"] = log_time
        data["dbconn"] = rdconn
        data["wrconn"] = wrconn
        data["dwconn"] = dwconn
        data["sqs"] = sqs
        data["runSingleMarket"] = True
        data["s3"]=s3
        data['cache_client']=cache_client
        data["NavitaireUpdateMethod"]=data["update_navitaire_method"]
        data["runTime"]=data["runTime"]
        route_type = data["route_type"]
        run_id = data["runId"]
        status_df = getRunStatus(run_id)
        if (not status_df.empty):
            if (status_df.iloc[0]['adhoc_run_status'] == 'DELETED'):
                logger.info("Skipping run, since run is deleted.")
                log_time.send_time_log_db()
                return {
                    'statusCode': 200,
                    'body': 'stopped'
                }
            elif (status_df.iloc[0]['adhoc_run_status'] == 'PAUSED'):
                logger.info("Pausing run.")
                redisKey = f"Paused:{run_id}"
                existing_data = cache_client.get(redisKey)
                dataArray = []
                if existing_data:
                    try:
                        dataArray = json.loads(existing_data)
                        if not isinstance(dataArray, list):
                            dataArray = []
                    except json.JSONDecodeError:
                        logger.error(f"Error parsing existing data for key {redisKey}")
                        dataArray = []
                
                dataArray.append(json.loads(content))
                cache_client.set(redisKey, json.dumps(dataArray), ex=6*60*60)

                logger.info(f"Successfully stored event in Redis under key {redisKey}. Total events: {len(dataArray)}")
                log_time.send_time_log_db()
                return {
                    'statusCode': 200,
                    'body': 'paused'
                }
        if route_type == 'connections':
            DynAllocConnections(input_data, data)
        if route_type == 'direct':
            DynAlloc(input_data, data)
    
    log_time.send_time_log_db()
    return {
        'statusCode': 200,
        'body': 'success'
    }

def getRunStatus(run_id):
    read_query = "SELECT adhoc_run_status FROM QP_DW_RMALLOC.allocation_run_audit WHERE run_id = '"+ run_id +"'"
    try:
        df = pd.read_sql(read_query, rdconn)
        return df
    except Exception as e:
        logger.info(e)

# DO COMMENT THE BELOW LINE AFTER TESTING
# messageBody = {"PerStart": "12/19/2024", "PerEnd": "12/19/2024", "Origin": "KWI", "Destin": "BOM", "FlightNumber": 572, "DOW": 9999199, "strategyFlag": 1, "TimeWindowStart": "0:01", "TimeWindowEnd": "23:59", "CurveID": "IGULF70_1", "CarrExlusionB2C": "**", "CarrExlusionB2B": "**", "fareAnchor": "STG1MIN", "fareOffset": "", "FirstRBDAlloc": 7, "OtherRBDAlloc": 5, "B2BBackstop": 23, "B2CBackstop": 23, "B2BFactor": "0", "SkippingFactor": 0, "OwnFareFlag": "0", "DaySpan": 0, "AutoTimeRangeFlag": 0, "analystName": "TANAY", "openingFares": 0, "OverBooking": "0", "profileFares": "0", "rbdPushFlag": "0", "B2BTolerance": "", "B2CTolerance": "", "distressInventoryFlag": "0", "seriesBlock": "0", "Flag": "1", "runId": "254be916-8985-483b-afaa-832eaa823099", "update_navitaire_method": "api", "runTime": "2024-09-04T06:20:07.329945", "route_type": "direct", "queue": "https://sqs.ap-south-1.amazonaws.com/471112573018/rm-allocation-queues_rm-allocation-market-list"}


# event = {'Records':[{'attributes':{"update_navitaire_method":"sftp"}, 'body':json.dumps(messageBody)}]}
# lambda_handler(event,None)
