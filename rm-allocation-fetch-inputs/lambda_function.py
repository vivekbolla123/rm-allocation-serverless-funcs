import sys
import logging
import boto3
import json
import warnings
import pymssql
import configsettings
import pytz
import time
import pandas as pd
from uuid import uuid4
from sqlalchemy import create_engine
from pandas import read_sql,Timestamp
from datetime import datetime, timedelta, date, timezone
from redis import Redis
from constants_utils import ConstantsUtils
from concurrent.futures import ThreadPoolExecutor
from time_logger import TimeLogger

warnings.filterwarnings('ignore')

run_log=[]

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
    
    NavitaireConnection = pymssql.connect(
        host=configsettings.NAV_DB_HOST,
        user=configsettings.NAV_DB_USER,
        password=configsettings.NAV_DB_PASSWORD
    )
except Exception as e:
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")
if cache_client.ping():
    logger.info("Connected to Redis")

constantsutils = ConstantsUtils(conn)

log_time= TimeLogger(wrconn, constantsutils.CHECK_THRESHOLD_FETCH_INPUTS)

def number_of_batches_required(total_records, adhoc_batch_size):
    if total_records < adhoc_batch_size:
        return [1, 0]
    if total_records % adhoc_batch_size != 0:
        return [total_records // adhoc_batch_size, total_records % adhoc_batch_size]
    else:
        return [total_records // adhoc_batch_size, 0]

def add_additional_fields(record, run_id, update_navitaire_method, start_time, route_type, queue_name):
    record["runId"] = run_id
    record["update_navitaire_method"] = update_navitaire_method
    record["runTime"] = start_time
    record["route_type"] = route_type
    record["queue"] = queue_name
    return record

def send_batch_to_sqs(batch, queue_url, additional_fields):
    entries = [
        {
            'Id': str(i),
            'MessageBody': json.dumps(add_additional_fields(record, **additional_fields))
        } for i, record in enumerate(batch)
    ]
    logger.info(entries)
    
    response = sqs.send_message_batch(QueueUrl=queue_url, Entries=entries)
    
    if 'Failed' in response:
        logger.error(f"Failed to send {len(response['Failed'])} messages")
        # Implement retry logic here if needed

def send_dataframe_to_sqs(df, queue_url, run_id, update_navitaire_method, route_type, queue_name, batch_size=10, max_workers=5):
    records = df.to_dict('records')
    batches = [records[i:i+batch_size] for i in range(0, len(records), batch_size)]
    
    start_time = datetime.now().isoformat()
    additional_fields = {
        'run_id': run_id,
        'update_navitaire_method': update_navitaire_method,
        'start_time': start_time,
        'route_type': route_type,
        'queue_name': queue_name
    }
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(lambda batch: send_batch_to_sqs(batch, queue_url, additional_fields), batches)

def lambda_handler(event, context):
    """
    This function fetches the market_list and inserts it into the queue
    """
    # keys
    start_interval_key = "start_interval"
    end_interval_key = "end_interval"
    unique_key = "uniqueKey"
    queue_key = "queue"
    market_list_key = "market_list_table_name"
    update_navitaire_method_key = "update_navitaire_method"
    region_key = "region_method"    
    username_key = "username"
    route_type_key = "route_type"
    run_type_key = "run_type"
    is_connections_required_key = "is_connections_required"

    adhoc_batch_size = constantsutils.ADHOC_BATCH_SIZE   #This is the batch size for Adhoc runs
    body = event['body']
    # Parse the JSON body
    data = json.loads(body)

    # defaults
    start_interval = 30
    end_interval = 90
    market_list_table = "market_list"
    queue_name = configsettings.MARKET_LIST_QUEUE_URL
    update_navitaire_method = "api"
    route_type = "direct"
    username = USERNAME
    type = "Adhoc"
    is_connections_required = "0"
    region = "Domestic"
    unique_key_value = ""
    # overrides
    if route_type_key in data: 
        route_type = data[route_type_key]
    if queue_key in data:
        queue_name = data[queue_key]
    if start_interval_key in data:
        start_interval = data[start_interval_key]
    if end_interval_key in data:
        end_interval = data[end_interval_key]
    if market_list_key in data:
        market_list_table = data[market_list_key]
    if update_navitaire_method_key in data:
        update_navitaire_method = data[update_navitaire_method_key]
    if username_key in data:
        username = data[username_key]
    if run_type_key in data:
        type = data[run_type_key]
    if region_key in data:
        region = data[region_key]
    if not type == "Adhoc":
        is_connections_required = '1'
    if is_connections_required_key in data:
        is_connections_required = data[is_connections_required_key]
    if unique_key in data: 
        unique_key_value = data[unique_key]

    # start program
    if type == "Adhoc":
        cache_client.set(unique_key_value, 0, 18000)
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
    elif route_type == "connections":
        update_navitaire_method = "sftp"
        is_connections_required = "0"
        region = 'Domestic'
        market_list_query = getConnectionMarketListQuery(start_period, end_period, market_list_table)
    
    w0_df= get_w0_rbd(start_period,end_period)
    mkt_list = read_sql(market_list_query, conn)
    logger.info(f"Original Market List {mkt_list}")
    mkt_list, failCount = clean_markets_optimized(mkt_list,w0_df)
    logger.info(f"Filtered Market List {mkt_list}")
    mkt_list = mkt_list.reset_index()
    markets_count = mkt_list.shape[0]
    logger.info(f"Total markets: {markets_count}")

    start_time = datetime.now()
    run_id = str(uuid4())
    log_time.update_run_id(run_id)
    
    insert_run_start(start_time, run_id, start_interval, end_interval, market_list_table, username, type, is_connections_required, update_navitaire_method, region, markets_count)

    cache_client.set(KEY_B2B_PENDING_COUNT + run_id, int(markets_count), 86400)
    cache_client.set(KEY_B2C_PENDING_COUNT + run_id, int(markets_count), 86400)
    cache_client.set(KEY_B2B_READY_COUNT + run_id, 0, 86400)
    cache_client.set(KEY_B2C_READY_COUNT + run_id, 0, 86400)
    cache_client.set(KEY_B2B_DONE_COUNT + run_id, 0, 86400)
    cache_client.set(KEY_B2C_DONE_COUNT + run_id, 0, 86400)

    if type != "Adhoc":
        send_dataframe_to_sqs(mkt_list, queue_name, run_id, update_navitaire_method, route_type, queue_name)
    else:
        batches_required = number_of_batches_required(markets_count, adhoc_batch_size)
        for batch_num in range(batches_required[0]):
            start_idx = batch_num * adhoc_batch_size
            end_idx = start_idx + adhoc_batch_size
            batch_df = mkt_list.iloc[start_idx:end_idx]
            batch_df['runId'] = run_id
            batch_df['update_navitaire_method'] = update_navitaire_method
            batch_df['runTime'] = start_time
            batch_df['route_type'] = route_type
            batch_df['queue'] = queue_name
            temp_name = f"adhoc_stack_{run_id}_batch_{batch_num + 1}"
            data_to_serialize = batch_df.to_dict(orient='records')
            my_list_json = json.dumps(data_to_serialize, default=custom_serializer)
            logger.info(my_list_json)
            cache_client.set(temp_name, my_list_json, 86400)
            allocation_run_batch_audit(run_id, temp_name)
        
        if batches_required[1] > 0:
            last_batch_df = mkt_list.iloc[-batches_required[1]:]
            last_batch_df['runId'] = run_id
            last_batch_df['update_navitaire_method'] = update_navitaire_method
            last_batch_df['runTime'] = start_time
            last_batch_df['route_type'] = route_type
            last_batch_df['queue'] = queue_name
            temp_name = f"adhoc_stack_{run_id}_batch_{batches_required[0] + 1}"
            data_to_serialize = last_batch_df.to_dict(orient='records')
            my_list_json = json.dumps(data_to_serialize, default=custom_serializer)
            logger.info(my_list_json)
            cache_client.set(temp_name, my_list_json, 86400)
            allocation_run_batch_audit(run_id, temp_name)
        
        update_run_with_batches(batches_required[0] + (1 if batches_required[1] > 0 else 0), run_id)

    end_time = datetime.now()
    
    logger.info(run_log)
    if type != "Adhoc":
        update_run_end(end_time, run_id)
        
    if update_navitaire_method == "S3":
        insert_file_upload_s3(run_id, start_time)
    
    log_time.send_time_log_db()
    return {
        "statusCode": 200, 
        "body": markets_count
    }

def custom_serializer(obj):
    if isinstance(obj, Timestamp):
        return str(obj.isoformat())
    return str(obj)# Convert Timestamp to ISO format string
    raise TypeError(f'Type {type(obj)} not serializable')

def insert_run_start(start_time, run_id, start_interval, end_interval, market_list_table, username, type, is_connections_required, update_navitaire_method, region, count):
    log_time.start()
    currVersionQuery = "SELECT tableName, curr_version FROM currentVersion"
    currVersionValues = read_sql(currVersionQuery, conn)

    table_versions = {}
    for index, currVersion in currVersionValues.iterrows():
        if market_list_table == currVersion["tableName"]:
            table_versions["market_list"] = currVersion["curr_version"]
            table_versions["market_list_name"] = currVersion["tableName"]
        else:
            table_versions[currVersion["tableName"]] = currVersion["curr_version"]

    insert_query = "INSERT INTO allocation_run_audit (run_id, start_time, user, dtd_start, dtd_end, Curves, d1_d2_strategies, market_list, Fares, type, market_list_name, is_connections_required, update_navitaire_method, is_run_completed, region, marketListCount) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    try:
        wrconn.execute(insert_query,
                    (run_id, start_time.strftime('%Y-%m-%d %H:%M:%S'), username, start_interval, end_interval, table_versions['Curves'], table_versions['d1_d2_strategies'], table_versions['market_list'], table_versions['Fares'], type, table_versions['market_list_name'], is_connections_required, update_navitaire_method, '0', region, count))
    except Exception as e:
        logger.info(e)
    finally:
        log_time.end("insert_run_start")
    logger.info("STARTED ALLOCATION RUN")

def insert_file_upload_s3(run_id, createdAt):
    log_time.start()
    insert_query = "INSERT INTO s3_file_upload (runId, createdAt) VALUES (%s, %s)"
    try:
        wrconn.execute(insert_query, (run_id, createdAt))
    except Exception as e:
        logger.info(e)
    finally:
        log_time.end("insert_file_upload_s3")
    logger.info("S3 File Audited")

def update_run_end(end_time, run_id):
    log_time.start()
    update_query = "UPDATE allocation_run_audit SET end_time = %s WHERE run_id = %s"
    try:
        wrconn.execute(update_query, (end_time.strftime('%Y-%m-%d %H:%M:%S'), run_id))
    except Exception as e:
        logger.info(e)
    finally:
        log_time.end("update_run_end")
    logger.info("STOPPED ALLOCATION RUN")

def update_run_with_batches(total_batches, run_id):
    log_time.start()
    update_query = "UPDATE allocation_run_audit SET total_batch_size = %s, batch_completed = %s, adhoc_run_status = %s WHERE run_id = %s"
    try:
        wrconn.execute(update_query, (total_batches, 0, "IN-QUEUE", run_id))
    except Exception as e:
        logger.info(e)
    finally:
        log_time.end("update_run_with_batches")

def allocation_run_batch_audit(run_id, batch_id):
    log_time.start()
    update_query = "INSERT INTO allocation_run_batch_audit (runId, batch_id) VALUES ( %s, %s)"
    try:
        wrconn.execute(update_query, (run_id, batch_id))
    except Exception as e:
        logger.info(e)
    finally:
        log_time.end("allocation_run_batch_audit")
        
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
                Origin,Destin,FlightNumber,DOW,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusionB2C,CarrExlusionB2B,flightExclusionB2C,flightExclusionB2B,fareAnchor,hardAnchor,plfthreshold,fareOffset,FirstRBDAlloc,OtherRBDAlloc,
                B2BBackstop,B2CBackstop,B2BFactor,obSeats,obFare,SkippingFactor,DaySpan,AutoTimeRangeFlag,analystName,openingFares,OverBooking,profileFares,rbdPushFlag,
                B2BTolerance,B2CTolerance,distressInventoryFlag,seriesBlock,autoGroup,tbfFlag,autoBackstopFlag,
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
    log_time.start()
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
                B2BBackstop,B2CBackstop,B2BFactor,SkippingFactor,Outbound_duration,Currency,fareAnchor,Offset,DiscountFlag,analystName,
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
    log_time.end("getConnectionMarketListQuery")
    return query

def create_flight_lookup_map(flights_df):
    flight_map = {}
    for _, flight in flights_df.iterrows():
        # Key format: FlightNumber_Origin_Destination_Date
        key = f"{flight['FlightNumber']}_{flight['Origin']}_{flight['Destination']}_{flight['DepartureDate']}"
        flight_map[key] = flight['DepartureTimeUTC']
    return flight_map

def getFltSectDeptTimeForCurrentDay():
    try:
        current_date = datetime.now(timezone.utc).date()
        NavitaireQuery = f"""
            SELECT  
                DepartureStation Origin,
                ArrivalStation Destination,
                TRIM(FlightNumber) as FlightNumber,
                DepartureDate,
                FORMAT(STDUTC, 'HH:mm') as DepartureTimeUTC
            FROM {configsettings.NAV_DB_NAME}rez.inventoryleg 
            WHERE DepartureDate = '{current_date.strftime("%Y-%m-%d")}'
        """
        
        
        return pd.read_sql(NavitaireQuery, NavitaireConnection)
            
    except Exception as e:
        logger.error(f"Error fetching flight data: {str(e)}")
        raise
    
def get_w0_rbd(start_date,end_date):
    try:
        NavitaireQuery = f"""SELECT classofservice,
       classau,
       CONVERT(VARCHAR(10), departuredate, 120) AS DepartureDate,
       IL.flightnumber,
       IL.departurestation,
       IL.arrivalstation
FROM   {configsettings.NAV_DB_NAME}[Rez].[inventorylegclass] ILC
       JOIN {configsettings.NAV_DB_NAME}[Rez].[inventoryleg] IL
         ON IL.[inventorylegid] = ILC.[inventorylegid]
WHERE  IL.departuredate >= '{start_date}'
       AND IL.departuredate <= '{end_date}'
       AND classofservice = 'W0'
ORDER  BY departuredate """
        
        return pd.read_sql(NavitaireQuery, NavitaireConnection)
            
    except Exception as e:
        logger.error(f"Error fetching flight data: {str(e)}")
        raise

def filter_market_list(market_list_df, ndo0_map):
    current_datetime = datetime.now(timezone.utc)    
    logger.info(f"Current time: {current_datetime}")
    logger.info(f"skipping time delta: {str(constantsutils.TIME_BEFORE_DEPARTURE)}")
    cutoff_time = (current_datetime + timedelta(minutes=constantsutils.TIME_BEFORE_DEPARTURE)).strftime("%H:%M")
    current_date = current_datetime.strftime("%Y-%m-%d")
    
    logger.info(f"Filtering markets. Current date: {current_date}, Cutoff time: {cutoff_time}")
    
    # Create mask for rows to keep
    keep_mask = pd.Series(False, index=market_list_df.index)
    skip_mask = pd.Series(False, index=market_list_df.index)
    
    for idx, market in market_list_df.iterrows():
        try:
            origin = market.get("Origin")
            destination = market.get("Destin")
            flight_number = market.get("FlightNumber")
            market_date = datetime.strptime(market.get("PerStart"), "%m/%d/%Y").strftime("%Y-%m-%d")
            
            if not all([origin, destination, flight_number, market_date]):
                logger.info(f"Skipping market due to missing data: {market}")
                continue
            
            # Only check departure time for today's flights
            if market_date == current_date:
                key = f"{flight_number}_{origin}_{destination}_{current_date}"
                
                if key in ndo0_map:
                    departure_time = ndo0_map[key]
                    
                    if departure_time > cutoff_time:
                        keep_mask[idx] = True
                        logger.info(f"Keeping today's flight {flight_number} {origin}-{destination} as departure time {departure_time} is after cutoff {cutoff_time}")
                    else:
                        skip_mask[idx] = True
                        logger.info(f"Skipping today's flight {flight_number} due to departure time {departure_time} before cutoff {cutoff_time}")
                else:
                    logger.info(f"Today's flight {flight_number} not found in Navitaire data - skipping")
                    skip_mask[idx] = True
            else:
                # Future dates - keep all flights
                keep_mask[idx] = True
                logger.info(f"Keeping future flight {flight_number} for date {market_date}")
                
        except Exception as e:
            logger.error(f"Error processing market {market}: {str(e)}")
            continue
    
    filtered_markets_df = market_list_df[keep_mask].copy()
    skipped_markets_df = market_list_df[skip_mask].copy()
    
    logger.info(f"Total markets processed: {len(market_list_df)}")
    logger.info(f"Markets kept: {len(filtered_markets_df)}")
    logger.info(f"Markets skipped: {len(skipped_markets_df)}")
    
    return filtered_markets_df, skipped_markets_df

def clean_markets_optimized(market_list_df,w0_df):
    try:
        try:
            current_flights_df = getFltSectDeptTimeForCurrentDay()
            if current_flights_df.empty:
                logger.info("No current day flights found in Navitaire")
                return market_list_df, 0
        except Exception as e:
            logger.error(f"Error fetching current flights: {str(e)}")
            return market_list_df, 0
            
        # Create lookup map with validation
        ndo0_map = create_flight_lookup_map(current_flights_df)
        if not ndo0_map:
            logger.info("Empty flight lookup map created")
            return market_list_df, 0
            
        # Filter markets while maintaining DataFrame structure
        filtered_markets_df, skipped_markets_df = filter_market_list(market_list_df, ndo0_map)
        w0_df["DepartureDate"] = pd.to_datetime(w0_df["DepartureDate"])  # Convert DepartureDate to datetime
        filtered_markets_df['PerStart'] = pd.to_datetime(filtered_markets_df['PerStart'])
        w0_df['flightnumber'] = w0_df['flightnumber'].astype('int64')
        filtered_markets_df['FlightNumber'] = filtered_markets_df['FlightNumber'].astype('int64')
        merged_df = pd.merge(
        filtered_markets_df,
        w0_df,
        left_on=['PerStart', 'Origin', 'Destin', 'FlightNumber'],
        right_on=['DepartureDate', 'departurestation', 'arrivalstation', 'flightnumber'],
        how='left'
    )
    
        # Rename classau column to wo_value
        merged_df = merged_df.rename(columns={'classau': 'w0_value'})
        
        # Convert PerStart back to normal date format (MM/DD/YYYY)
        merged_df['PerStart'] = merged_df['PerStart'].dt.strftime('%m/%d/%Y')
        merged_df.drop(columns=['DepartureDate', 'flightnumber', 'departurestation', 'arrivalstation','classofservice'], inplace=True)
        return merged_df, len(skipped_markets_df)
        
    except Exception as e:
        logger.error(f"Error in clean_markets_optimized: {str(e)}")
        return market_list_df, 0
    
# DO COMMENT THE BELOW LINE AFTER TESTING
# lambda_handler({
#   "body": "{\"start_interval\": 0, \"end_interval\": 365, \"run_type\": \"Adhoc\", \"update_navitaire_method\": \"api\", \"market_list_table_name\": \"market_list_adhoc_SV_Test\"}"
# },None)
