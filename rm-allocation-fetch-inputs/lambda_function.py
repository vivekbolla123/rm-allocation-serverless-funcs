import sys
import logging
import boto3
import json
import warnings
import configsettings
import pytz
from uuid import uuid4
from sqlalchemy import create_engine
from pandas import read_sql
from datetime import datetime
from datetime import timedelta
from datetime import date
from redis import Redis

warnings.filterwarnings('ignore')

# sqs setting
sqs = boto3.client('sqs')  # client is required to interact with

DB_RD_CONN_STRING = f"mysql+mysqldb://{configsettings.RM_DB_APPUSER_USERNAME_READ}:{configsettings.RM_DB_APPUSER_PASSWORD_READ}@{configsettings.RM_DB_APPUSER_URL_READ}/{configsettings.RM_DB_SCHEMA_NAME}"
DB_WR_CONN_STRING = f"mysql+mysqldb://{configsettings.RM_DB_APPUSER_USERNAME}:{configsettings.RM_DB_APPUSER_PASSWORD}@{configsettings.RM_DB_APPUSER_URL}/{configsettings.RM_DB_SCHEMA_NAME}"

USERNAME = "SYS2"

CACHE_ENDPOINT = configsettings.RM_CACHE_ENDPOINT
CACHE_PORT = configsettings.RM_CACHE_PORT
CACHE_USERNAME = configsettings.RM_CACHE_USERNAME
CACHE_PASSWORD = configsettings.RM_CACHE_PASSWORD

KEY_B2B_PENDING_COUNT = 'b2b_pending_count_'
KEY_B2C_PENDING_COUNT = 'b2c_pending_count_'
KEY_B2B_READY_COUNT = 'b2b_ready_count_'
KEY_B2C_READY_COUNT = 'b2c_ready_count_'
KEY_B2B_DONE_COUNT = 'b2b_done_count_'
KEY_B2C_DONE_COUNT = 'b2c_done_count_'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# create the database connection outside the handler to allow connections to be
# re-used by subsequent function invocations.
try:
    cache_client = Redis(host=CACHE_ENDPOINT, port=CACHE_PORT, username=CACHE_USERNAME, password=CACHE_PASSWORD)
    conn = create_engine(DB_RD_CONN_STRING)
    wrconn = create_engine(DB_WR_CONN_STRING)
except Exception as e:
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")
if cache_client.ping():
    logger.info("Connected to Redis")

def lambda_handler(event, context):
    """
    This function fetches the market_list and inserts it into the queue
    """
    # keys
    start_interval_key = "start_interval"
    end_interval_key = "end_interval"
    queue_key = "queue"
    market_list_key = "market_list_table_name"
    update_navitaire_method_key = "update_navitaire_method"
    region_key = "region_method"    
    username_key = "username"
    route_type_key = "route_type"
    run_type_key = "run_type"
    is_connections_required_key = "is_connections_required"

    body = event['body']
    # Parse the JSON body
    data = json.loads(body)

    # defaults
    start_interval = 30 #0
    end_interval = 90 #365
    market_list_table = "market_list"
    queue_name = configsettings.MARKET_LIST_QUEUE_URL
    update_navitaire_method="api"
    route_type = "direct"

    username = USERNAME
    type = "Adhoc"
    is_connections_required = "0"
    region = "Domestic"

    # overrides
    if(route_type_key in data): 
        route_type = data[route_type_key]

    if (queue_key in data):
        queue_name = data[queue_key]

    if (start_interval_key in data):
        start_interval = data[start_interval_key]

    if (end_interval_key in data):
        end_interval = data[end_interval_key]

    if (market_list_key in data):
        market_list_table = data[market_list_key]

    if(update_navitaire_method_key in data):
        update_navitaire_method=data[update_navitaire_method_key]

    if (username_key in data):
        username = data[username_key]

    if (run_type_key in data):
        type = data[run_type_key]

    if (region_key in data):
        region = data[region_key]

    if(not type == "Adhoc"):
        is_connections_required = '1'

    if(is_connections_required_key in data):
        is_connections_required = data[is_connections_required_key]

    # start program
    if type == "Adhoc":
        available_slot = deduct_available_slot()
        if not available_slot:
            logger.info("No slots available to run Adhoc API!")
            return False

    current_utc_time = datetime.utcnow()
    # Define the UTC time zone
    utc_timezone = pytz.timezone('UTC')
    # Set the time zone to IST (Indian Standard Time)
    ist_timezone = pytz.timezone('Asia/Kolkata')
    # Convert the UTC time to IST
    startTime = current_utc_time.replace(tzinfo=utc_timezone).astimezone(ist_timezone)
    curr_date = startTime.date()
    start_period = datetime.strftime(curr_date + timedelta(days=start_interval), "%Y-%m-%d")
    end_period = datetime.strftime(curr_date + timedelta(days=end_interval), "%Y-%m-%d")

    if route_type == "direct":
        market_list_query = getMarketListQuery(start_period, end_period, market_list_table)

    if route_type == "connections":
        update_navitaire_method = "sftp"
        is_connections_required = "0"
        region = 'Domestic'
        market_list_query = getConnectionMarketListQuery(start_period, end_period, market_list_table)

    mkt_list = read_sql(market_list_query, conn)
    markets_count = mkt_list.shape[0]
    logger.info(markets_count)

    start_time = datetime.now()
    run_id = str(uuid4())
    insert_run_start(start_time, run_id, start_interval, end_interval, market_list_table, username, type, is_connections_required, update_navitaire_method, region, markets_count)

    cache_client.set(KEY_B2B_PENDING_COUNT + run_id, int(markets_count), 86400)
    cache_client.set(KEY_B2C_PENDING_COUNT + run_id, int(markets_count), 86400)
    cache_client.set(KEY_B2B_READY_COUNT + run_id, 0, 86400)
    cache_client.set(KEY_B2C_READY_COUNT + run_id, 0, 86400)
    cache_client.set(KEY_B2B_DONE_COUNT + run_id, 0, 86400)
    cache_client.set(KEY_B2C_DONE_COUNT + run_id, 0, 86400)

    for mkt_index in range(markets_count):
        data = mkt_list.iloc[mkt_index]
        
        try:                
            data["runId"] = run_id
            data["update_navitaire_method"] = update_navitaire_method
            data["runTime"] = start_time
            data["route_type"] = route_type

            data_str = data.to_json()
            logger.info(data_str)
            print(data_str)
            
        except:
            logger.info("Error in Start or End Date at row: " + data)  
            logger.info("Run Id: " + run_id)  
            
        sqs.send_message(
            QueueUrl=queue_name,
            MessageBody=data_str
        )

    end_time = datetime.now()
    update_run_end(end_time, run_id)
    if(update_navitaire_method == 'S3'):
        insert_file_upload_s3(run_id, start_time)
    return {
        'statusCode': 200,
        'body': markets_count
    }

def insert_run_start(start_time, run_id, start_interval, end_interval, market_list_table, username, type, is_connections_required, update_navitaire_method, region, count):
    currVersionQuery = "SELECT tableName, curr_version FROM currentVersion"
    currVersionValues = read_sql(currVersionQuery, conn)

    table_versions = {}
    for index,currVersion in currVersionValues.iterrows():
        if market_list_table == currVersion['tableName']:
            table_versions['market_list'] = currVersion['curr_version']
            table_versions['market_list_name'] = currVersion['tableName']
        else:
            table_versions[currVersion['tableName']] = currVersion['curr_version']

    insert_query = " INSERT INTO allocation_run_audit (run_id, start_time, user, dtd_start, dtd_end, Curves, d1_d2_strategies, market_list, Fares, type, market_list_name, is_connections_required, update_navitaire_method, is_run_completed, region, marketListCount) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    try:
        wrconn.execute(insert_query,
                    (run_id, start_time.strftime('%Y-%m-%d %H:%M:%S'), username, start_interval, end_interval, table_versions['Curves'], table_versions['d1_d2_strategies'], table_versions['market_list'], table_versions['Fares'], type, table_versions['market_list_name'], is_connections_required, update_navitaire_method, '0', region, count))
    except Exception as e:
        logger.info(e)
    logger.info("STARTED ALLOCATION RUN")

def insert_file_upload_s3(run_id, createdAt):
    insert_query = " INSERT INTO s3_file_upload (runId, createdAt) VALUES (%s, %s)"
    try:
        wrconn.execute(insert_query, (run_id, createdAt))
    except Exception as e:
        logger.info(e)
    logger.info("S3 File Audited")


def update_run_end(end_time, run_id):
    update_query = " UPDATE allocation_run_audit SET end_time = %s WHERE run_id = %s "
    try:
        wrconn.execute(update_query,
                    (end_time.strftime('%Y-%m-%d %H:%M:%S'), run_id))
    except Exception as e:
        logger.info(e)
    logger.info("STOPPED ALLOCATION RUN")

def deduct_available_slot():
    """Get the available slots for queue to run Adhoc Api calls, if slot is available deduct one from available slots"""
    
    select_query = "SELECT available FROM adhoc_run_queue"
    count_avaiable_slot = conn.execute(select_query).fetchone()[0]
    logger.info(f"Slot Avaiable: {count_avaiable_slot}")
  
    if count_avaiable_slot == 0:
        logger.info("No Slot Avaiable !")
        return False
    
    new_avaiable_slot = count_avaiable_slot - 1
    update_query = f"UPDATE adhoc_run_queue SET available={new_avaiable_slot}"
    try:
        wrconn.execute(update_query)
    except Exception as e:
        logger.info(e)
    logger.info(f"Slot Avaiable after Adhoc run started: {new_avaiable_slot}")
    return True

def getMarketListQuery(start_period, end_period, market_list_table):
    query = f"""
    WITH RECURSIVE DateRange AS (
        SELECT
            str_to_date('{start_period}', '%%Y-%%m-%%d') AS Date
        UNION
        ALL
        SELECT
            Date + INTERVAL 1 DAY
        FROM
            DateRange
        WHERE
            Date < str_to_date('{end_period}', '%%Y-%%m-%%d')
    )
    SELECT
        *
    FROM
        (
            SELECT
                DATE_FORMAT(Date, '%%m/%%d/%%Y') as PerStart,
                DATE_FORMAT(Date, '%%m/%%d/%%Y') as PerEnd,
                Origin,Destin,FlightNumber,DOW,strategyFlag,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusion,fareAnchor,fareOffset,FirstRBDAlloc,OtherRBDAlloc,
                B2BBackstop,B2CBackstop,B2BFactor,SkippingFactor,OwnFareFlag,DaySpan,AutoTimeRangeFlag,analystName,openingFares,OverBooking,profileFares,rbdPushFlag,
                B2BTolerance,B2CTolerance,
                substring(dow, dayofweek(Date), 1) as Flag
            FROM
                DateRange
            CROSS JOIN 
                {market_list_table} as ml
            WHERE
                Date >= str_to_date(ml.PerStart, '%%m/%%d/%%Y')
                AND Date <= str_to_date(ml.PerEnd, '%%m/%%d/%%Y')
        ) A
    WHERE
        Flag = 1
    ORDER BY
        Origin, Destin, FlightNumber
    """
    return query

def getConnectionMarketListQuery(start_period, end_period, market_list_table):
    query = f"""
    WITH RECURSIVE DateRange AS (
        SELECT
            str_to_date('{start_period}', '%%Y-%%m-%%d') AS Date
        UNION
        ALL
        SELECT
            Date + INTERVAL 1 DAY
        FROM
            DateRange
        WHERE
            Date < str_to_date('{end_period}', '%%Y-%%m-%%d')
    )
    SELECT
        *
    FROM
        (
            SELECT
                Sector1,Flight1,Sector2,Flight2,Outbound_stop,PerType,DOW,Price_Strategy,Discount_Value,FirstRBDAlloc,OtherRBDAlloc,
                B2BBackstop,B2CBackstop,B2BFactor,SkippingFactor,analystName,
                DATE_FORMAT(Date, '%%m/%%d/%%Y') as PerStart,
                DATE_FORMAT(Date, '%%m/%%d/%%Y') as PerEnd,
                substring(dow, dayofweek(Date), 1) as Flag
            FROM
                DateRange
            CROSS JOIN 
                {market_list_table} as ml
            WHERE
                Date >= str_to_date(ml.PerStart, '%%m/%%d/%%Y')
                AND Date <= str_to_date(ml.PerEnd, '%%m/%%d/%%Y')
        ) A
    WHERE
        Flag = 1
    """
    return query

# DO COMMENT THE BELOW LINE AFTER TESTING
# lambda_handler({"body": "{\"start_interval\": 0, \"end_interval\": 7}"},None)
