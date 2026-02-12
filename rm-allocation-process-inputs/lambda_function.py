from datetime import datetime
import sys
import logging
import uuid

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
from runAlloc_SalesInv import DynAllocSalesInv
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
            authSource=configsettings.MONGO_AUTH_SOURCE
        )
mongodb=mongoclient[configsettings.MONGO_DB]
# DB settings
WAIT_TIME = 30  # in seconds

LOGS_ON = False

# rds settings
DW_DB_CONN_STRING = f"mysql+pymysql://{configsettings.DW_DB_USER}:{configsettings.DW_DB_PASSWORD}@{configsettings.DW_DB_HOST}/{configsettings.DW_DB_WRITE_SCHEMA}"

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
    dwconn = create_engine(DW_DB_CONN_STRING)
except Exception as e:
    logger.error(e)
    sys.exit()

constantsutils = ConstantsUtils(mongodb)
log_time = TimeLogger(mongodb, constantsutils.CHECK_THRESHOLD_PROCESS_INPUTS)
logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")

def lambda_handler(event, context):
    """
    This function fetches the market_list and inserts it into the queue.
    It now checks for saleinvflag to route to the appropriate allocation logic.
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
        data = json.loads(content)
        data["timelogger"] = log_time
        data["dwconn"] = dwconn
        data["sqs"] = sqs
        data["runSingleMarket"] = True
        data["s3"]=s3
        data['cache_client']=cache_client
        data["NavitaireUpdateMethod"]=data["update_navitaire_method"]
        data["runTime"]=data["runTime"]
        
        route_type = data.get("route_type")
        run_id = data.get("runId")
        is_sales_inventory = str(data.get("saleinvflag")) == "1"

        data["mongodbclient"]=mongodb
        status_df = getRunStatus(run_id)
        if (not status_df.empty):
            if (status_df.iloc[0]['run_status'] == 'DELETED'):
                logger.info(f"Skipping run: {run_id}")
                log_time.send_time_log_db()
                continue 
            elif (status_df.iloc[0]['run_status'] in ['PAUSED', 'AUTO-PAUSED']):
                insert_paused_run(run_id, content)
                log_time.send_time_log_db()
                continue

        # Route to the correct logic based on the message flags
        if is_sales_inventory:
            logger.info("Routing to Sales Inventory Allocation Logic")
            DynAllocSalesInv(input_data, data)
        elif route_type == 'connections':
            logger.info("Routing to Connections Allocation Logic")
            DynAllocConnections(input_data, data)
        elif route_type == 'direct':
            logger.info("Routing to Direct Allocation Logic")
            DynAlloc(input_data, data)
        else:
            logger.warning(f"No valid route found for run_id: {run_id}. Flags: saleinvflag='{data.get('saleinvflag')}', route_type='{route_type}'")


    log_time.send_time_log_db()
    return {
        'statusCode': 200,
        'body': 'success'
    }

def getRunStatus(run_id):
    """
    Get run status from MongoDB allocation_run_audit collection
    
    Args:
        mongo_db: MongoDB database instance
        run_id: The run ID to query
        
    Returns:
        DataFrame with run_status
    """
    try:
        collection = mongodb['allocation_run_audit']
        
        # Query MongoDB
        result = collection.find_one(
            {'run_id': run_id},
            {'_id': 0, 'run_status': 1}  # Only return run_status field, exclude _id
        )
        
        # Convert to DataFrame (similar to pd.read_sql behavior)
        if result:
            df = pd.DataFrame([result])
        else:
            # Return empty DataFrame with column if no result found
            df = pd.DataFrame(columns=['run_status'])
            
        return df
    except Exception as e:
        logger.info(e)
        return pd.DataFrame(columns=['run_status'])


def insert_paused_run( run_id, content):
    """
    Insert paused run record into MongoDB run_paused collection
    
    Args:
        mongo_db: MongoDB database instance
        run_id: The run ID
        content: The content/value to store
    """
    try:
        collection = mongodb['run_paused']
        
        record_id = str(uuid.uuid4())
        create_at = datetime.now()
        
        # Create document to insert
        document = {
            'run_id': run_id,
            'record_id': record_id,
            'createAt': create_at,
            'value': content
        }
        
        # Insert into MongoDB
        result = collection.insert_one(document)
        logger.info(f"Successfully inserted paused run record with id: {run_id}")
        
    except Exception as e:
        logger.error(f"Error inserting paused run record: {str(e)}")
        raise

# DO COMMENT THE BELOW LINE AFTER TESTING
# messageBody = {"index": 1533, "Origin": "VNS", "Destin": "BOM", "FlightNumber": 1497, "DOW": 9991999, "TimeWindowStart": "14:00", "TimeWindowEnd": "17:00", "CurveID": "C17440_20", "CarrExlusionB2C": "**", "CarrExlusionB2B": "**", "flightExclusionB2C": "", "flightExclusionB2B": "", "fareAnchor": "UP_Lean1_JM", "hardAnchor": "", "plfThreshold": "", "fareOffset": "", "FirstRBDAlloc": 5, "OtherRBDAlloc": 5, "B2BBackstop": 3500, "B2CBackstop": 3500, "B2BFactor": 0.0, "obSeats": "", "obFare": "", "SkippingFactor": 10, "DaySpan": 0, "AutoTimeRangeFlag": 0, "analystName": "JOGITA", "openingFares": 1, "OverBooking": 1, "profileFares": 1, "rbdPushFlag": 0, "B2BTolerance": "", "B2CTolerance": "", "distressInventoryFlag": 0, "seriesBlock": 0, "autoGroup": 0, "tbfFlag": 0, "CugFlag": 0, "MktFareFlag": 1, "autoBackstopFlag": 0, "directFlag": 1, "TBFEnhFlag": 0, "defaultMlFlag": 0, "b2bStrategy": "", "Marketfare_Upsell": 0, "Outbound_Stop": 0, "forecastAllocFlag": 0, "Spoilage_Flag": 0, "PerStart": "02/11/2026", "PerEnd": "02/11/2026", "w0_value": None, "forecastPLF": None, "forecast_variance": None, "DepartureTime": "16:05", "bookedPax": 0.0, "capacity": 186.0, "adjustedCapacity": 186.0, "OpSuffix": " ", "DepartureTimeUTC": "10:35", "group_bookings": 0.0, "tbf_bookings": 0.0, "WA_bookings": 0.0, "WB_bookings": 0.0, "WC_bookings": 0.0, "WD_bookings": 0.0, "WE_bookings": 0.0, "seats_sold_L": 0.0, "seats_sold_P": 0.0, "current_rbd_L": 0.0, "current_rbd_P": 0.0, "total_series_seats": None, "NextFlight": 0.0, "OpeningRBDB2B": None, "OpeningRBDB2C": "R6", "BookingsInHour": None, "BookingsCount": None, "BookingToday": None, "runId": "fd51cd07-37a8-44a4-9841-8a0e6b1db851", "update_navitaire_method": "api", "runTime": "2026-02-04T03:45:31.095894", "route_type": "direct", "queue": "https://sqs.ap-south-1.amazonaws.com/891377165721/test-rm-allocation-queues-rm-allocation-market-list", "cug_alloc": "0", "mongodb": "0", "saleinvflag": "0", "domestic_flag": "1"}


# event = {'Records':[{'attributes':{"update_navitaire_method":"sftp"}, 'body':json.dumps(messageBody)}]}
# lambda_handler(event,None)
