import sys
import logging

from pymongo import MongoClient
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
mongoclient = MongoClient(
            host=configsettings.MONGO_URL,
            port=27017,
            username=configsettings.MONGO_USERNAME,
            password=configsettings.MONGO_PASSWORD,
            authSource=configsettings.MONGO_DB,
            # Connection pool settings
            maxPoolSize=1,          # Limit to 1 connection
            minPoolSize=0      # No minimum connections    
        )
mongodb=mongoclient.test_db
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
        data["mongodbclient"]=mongodb
        status_df = getRunStatus(run_id)
        if (not status_df.empty):
            if (status_df.iloc[0]['run_status'] == 'DELETED'):
                logger.info(f"Skipping run: {run_id}")
                log_time.send_time_log_db()
                return {
                    'statusCode': 200,
                    'body': 'stopped'
                }
            elif (status_df.iloc[0]['run_status'] == 'PAUSED'):
                insert_paused_run(run_id, content)
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
    read_query = "SELECT run_status FROM QP_DW_RMALLOC.allocation_run_audit WHERE run_id = '"+ run_id +"'"
    try:
        df = pd.read_sql(read_query, rdconn)
        return df
    except Exception as e:
        logger.info(e)

def insert_paused_run(run_id, content):
    """
    Insert paused run record into database
    """
    import uuid
    from datetime import datetime
    
    try:
        record_id = str(uuid.uuid4())
        create_at = datetime.now()

        query = """
        INSERT INTO QP_DW_RMALLOC.run_paused 
        (run_id, record_id, createAt, value) 
        VALUES (%s, %s, %s, %s)
        """
        
        wrconn.execute(query, (run_id, record_id, create_at, content))
    except Exception as e:
        logger.error(f"Error inserting paused run record: {str(e)}")
        raise

# DO COMMENT THE BELOW LINE AFTER TESTING
# messageBody = {"index": 642, "PerStart": "07/04/2025", "PerEnd": "07/04/2025", "Origin": "IXZ", "Destin": "MAA", "FlightNumber": 1305, "DOW": 1111111, "TimeWindowStart": "00:15", "TimeWindowEnd": "23:55", "CurveID": "IXZMAA_NP", "CarrExlusionB2C": "**", "CarrExlusionB2B": "**", "flightExclusionB2C": "", "flightExclusionB2B": "", "fareAnchor": "NP_General", "hardAnchor": "MIN", "plfthreshold": "88%", "fareOffset": "-1", "FirstRBDAlloc": 8, "OtherRBDAlloc": 3, "B2BBackstop": 5000, "B2CBackstop": 5100, "B2BFactor": "0.3", "obSeats": "", "obFare": "", "SkippingFactor": 10, "DaySpan": 0, "AutoTimeRangeFlag": 0, "analystName": "NEHA", "openingFares": 1, "OverBooking": "1", "ProfileFares": "1", "rbdPushFlag": "0", "B2BTolerance": "", "B2CTolerance": "", "distressInventoryFlag": "0", "seriesBlock": "0", "AutoGroup": "1", "TbfFlag": "1", "CugFlag": "0", "MktFareFlag": "1", "Autobackstopflag": "0", "Flag": "1", "w0_value": 0.0, "DepartureTime": "12:00", "bookedPax": 2.0, "capacity": 185.0, "adjustedCapacity": 185.0, "DepartureTimeUTC": "06:30", "tbf_bookings": 0.0, "WA_bookings": 0.0, "WB_bookings": 0.0, "WC_bookings": 0.0, "WD_bookings": 0.0, "WE_bookings": 0.0, "seats_sold_L": 0.0, "seats_sold_P": 0.0, "current_rbd_L": 0.0, "current_rbd_P": 0.0, "total_series_seats": 0, "NextFlight": 0.0, "OpeningRBDB2B": "Z9", "OpeningRBDB2C": "B", "BookingsInHour": 0.0, "BookingsCount": 2.0, "BookingToday": 0.0, "runId": "c470b2ed-441e-4e92-89fc-ecd33ffa6dbd", "update_navitaire_method": "api", "runTime": "2025-06-17T21:32:27.519504", "route_type": "direct", "queue": "https://sqs.ap-south-1.amazonaws.com/891377165721/test-rm-allocation-queues-rm-allocation-market-list", "cug_alloc": "0", "mongodb": "1", "saleinvflag": "0"}


# event = {'Records':[{'attributes':{"update_navitaire_method":"sftp"}, 'body':json.dumps(messageBody)}]}
# lambda_handler(event,None)
