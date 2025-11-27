import sys
import logging
import boto3
import json
import warnings
import time
from functools import wraps

import pymssql
import configsettings
import pytz
import pandas as pd
import numpy as np
from uuid import uuid4
from sqlalchemy import create_engine, text, pool, event
from pandas import read_sql, Timestamp
from datetime import datetime, timedelta, date, timezone
from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError, TimeoutError as RedisTimeoutError
from constants_utils import ConstantsUtils
from concurrent.futures import ThreadPoolExecutor
from time_logger import TimeLogger
from nav_api_helper import NavAPIHelper
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError, AutoReconnect
from pymssql import Error as PymssqlError
from sqlalchemy.exc import OperationalError, DBAPIError
from sqlalchemy.engine.base import Engine
from urllib.parse import quote_plus

warnings.filterwarnings('ignore')

# Global variables and setup
run_log = []
sqs = boto3.client('sqs')

# Database connection strings
USERNAME = "SYS2"
DW_DB_CONN_STRING = f"mysql+pymysql://{configsettings.DW_DB_USER}:{configsettings.DW_DB_PASSWORD}@{configsettings.DW_DB_HOST}/{configsettings.DW_DB_WRITE_SCHEMA}"

# Cache settings
CACHE_ENDPOINT = configsettings.RM_CACHE_ENDPOINT
CACHE_PORT = configsettings.RM_CACHE_PORT
CACHE_USERNAME = configsettings.RM_CACHE_USERNAME
CACHE_PASSWORD = configsettings.RM_CACHE_PASSWORD

# Cache keys
KEY_B2B_PENDING_COUNT = 'b2b_pending_count_'
KEY_B2C_PENDING_COUNT = 'b2c_pending_count_'
KEY_B2B_READY_COUNT = 'b2b_ready_count_'
KEY_B2C_READY_COUNT = 'b2c_ready_count_'
KEY_B2B_DONE_COUNT = 'b2b_done_count_'
KEY_B2C_DONE_COUNT = 'b2c_done_count_'
KEY_SALES_PENDING_COUNT = 'sales_pending_count_'
KEY_SALES_READY_COUNT = 'sales_ready_count_'
KEY_SALES_DONE_COUNT = 'sales_done_count_'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ==========================
# RETRY DECORATOR
# ==========================

def retry_on_connection_error(max_retries=3, delay=2, backoff=2, exceptions=(Exception,)):
    """
    Retry decorator for handling connection errors
    
    Args:
        max_retries: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff: Multiplier for delay after each retry
        exceptions: Tuple of exceptions to catch and retry
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_retries:
                        logger.error(f"Failed after {max_retries} retries in {func.__name__}: {str(e)}")
                        raise
                    
                    logger.warning(
                        f"Connection error in {func.__name__} (attempt {attempt + 1}/{max_retries}): {str(e)}. "
                        f"Retrying in {current_delay} seconds..."
                    )
                    time.sleep(current_delay)
                    current_delay *= backoff
            return None
        return wrapper
    return decorator

# ==========================
# CONNECTION FUNCTIONS WITH RETRY
# ==========================

def create_mongo_connection():
    """Create MongoDB connection with retry logic"""
    try:
        mongo_client = MongoClient('mongodb://localhost:27017/')

        # Test connection
        mongo_client.admin.command('ping')
        logger.info("MongoDB connection established successfully")
        return mongo_client
    except Exception as e:
        logger.error(f"MongoDB connection error: {str(e)}")
        raise

@retry_on_connection_error(
    max_retries=3, 
    delay=2, 
    backoff=2,
    exceptions=(ConnectionFailure, ServerSelectionTimeoutError, AutoReconnect)
)
def get_mongo_connection():
    """Get MongoDB connection with automatic retry"""
    return create_mongo_connection()

@retry_on_connection_error(
    max_retries=3,
    delay=1,
    backoff=2,
    exceptions=(RedisConnectionError, RedisTimeoutError)
)
def create_redis_connection():
    """Create Redis connection with retry logic"""
    cache_client = Redis(
        host=CACHE_ENDPOINT,
        port=CACHE_PORT,
        username=CACHE_USERNAME,
        password=CACHE_PASSWORD,
        socket_connect_timeout=5,
        socket_timeout=5,
        socket_keepalive=True,
        health_check_interval=30,
        retry_on_timeout=True,
        decode_responses=False
    )
    # Test connection
    cache_client.ping()
    logger.info("Connected to Redis")
    return cache_client

def create_navitaire_connection_pool():
    """Create SQL Server connection pool with SQLAlchemy - RECOMMENDED APPROACH"""
    connection_string = (
        f"mssql+pymssql://{configsettings.NAV_DB_USER}:"
        f"{quote_plus(configsettings.NAV_DB_PASSWORD)}@"
        f"{configsettings.NAV_DB_HOST}"
    )
    
    engine = create_engine(
        connection_string,
        pool_pre_ping=True,  # Validates connections before use
        pool_recycle=3600,   # Recycle connections every hour
        pool_size=5,
        max_overflow=10,
        connect_args={
            'timeout': 0,      # Query timeout - increased from 30
            'login_timeout': 120  # Login timeout - increased from 30
        }
    )
    
    # Add event listener to handle disconnects
    @event.listens_for(engine, "engine_connect")
    def receive_engine_connect(conn, branch):
        if not branch:
            # Test connection with SELECT 1
            conn.execute(text("SELECT 1"))
    
    logger.info("Navitaire SQL Server connection pool established")
    return engine

@retry_on_connection_error(
    max_retries=3,
    delay=2,
    backoff=2,
    exceptions=(PymssqlError,)
)
def create_navitaire_connection():
    """Create SQL Server connection with retry logic - FALLBACK IF NOT USING POOL"""
    conn = pymssql.connect(
        host=configsettings.NAV_DB_HOST,
        user=configsettings.NAV_DB_USER,
        password=configsettings.NAV_DB_PASSWORD,
        timeout=0,          # Increased from 30 to 120 seconds
        login_timeout=120      # Increased from 30 to 60 seconds
    )
    logger.info("Navitaire SQL Server connection established")
    return conn

def create_dw_connection():
    """Create MySQL connection with retry and pooling"""
    engine = create_engine(
        DW_DB_CONN_STRING,
        pool_pre_ping=True,
        pool_recycle=3600,
        pool_size=10,
        max_overflow=20,
        connect_args={
            'connect_timeout': 10,
            'read_timeout': 30,
            'write_timeout': 30
        }
    )
    logger.info("Data Warehouse MySQL connection established")
    return engine

# Database and cache connections
try:
    cache_client = create_redis_connection()
    mongo_client = get_mongo_connection()
    mongo_db = mongo_client[configsettings.MONGO_DB]
    
    # OPTION 1: Use connection pool (RECOMMENDED)
    NavitaireConnection = create_navitaire_connection_pool()
    
    # OPTION 2: Use simple connection (if you can't use pool)
    # NavitaireConnection = create_navitaire_connection()
    
    dwconn = create_dw_connection()
    
    logger.info("SUCCESS: All database connections established")
except Exception as e:
    logger.error(f"Failed to establish connections: {str(e)}")
    sys.exit()

# Updated to pass mongo_db instead of SQL connections
constantsutils = ConstantsUtils(mongo_db)
log_time = TimeLogger(mongo_db, constantsutils.CHECK_THRESHOLD_FETCH_INPUTS)

# ==========================
# MongoDB Helper Functions with Retry
# ==========================

@retry_on_connection_error(
    max_retries=3,
    delay=1,
    exceptions=(ConnectionFailure, AutoReconnect)
)
def mongo_find_with_retry(collection_name, query_filter, projection=None):
    """MongoDB find operation with retry"""
    if projection:
        return list(mongo_db[collection_name].find(query_filter, projection))
    return list(mongo_db[collection_name].find(query_filter))

@retry_on_connection_error(
    max_retries=3,
    delay=1,
    exceptions=(ConnectionFailure, AutoReconnect)
)
def mongo_insert_with_retry(collection_name, document):
    """MongoDB insert operation with retry"""
    return mongo_db[collection_name].insert_one(document)

@retry_on_connection_error(
    max_retries=3,
    delay=1,
    exceptions=(ConnectionFailure, AutoReconnect)
)
def mongo_update_with_retry(collection_name, filter_query, update_doc):
    """MongoDB update operation with retry"""
    return mongo_db[collection_name].update_one(filter_query, update_doc)

# ==========================
# NavDatabaseHelper class with Enhanced Retry
# ==========================

class NavDatabaseHelper:
    def __init__(self, navconn, nav_db_name, nav_db_wb_name):
        self.NavitaireConnection = navconn
        self.NAV_DB_NAME = nav_db_name
        self.NAV_DB_WB_NAME = nav_db_wb_name
        self.logger = logging.getLogger()
        self.is_pool = isinstance(navconn, Engine)

    def _validate_connection(self):
        """Validate connection is alive with a simple query"""
        try:
            if self.is_pool:
                # For SQLAlchemy engine, test with a simple query
                with self.NavitaireConnection.connect() as conn:
                    conn.execute(text("SELECT 1"))
            else:
                # For pymssql connection
                cursor = self.NavitaireConnection.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                cursor.close()
            return True
        except Exception as e:
            self.logger.warning(f"Connection validation failed: {str(e)}")
            return False

    def _execute_query_with_retry(self, query, max_retries=3):
        """Execute query with automatic reconnection on failure - ENHANCED VERSION"""
        for attempt in range(max_retries):
            try:
                # If using connection pool, validation is automatic
                if self.is_pool:
                    return pd.read_sql(query, self.NavitaireConnection)
                
                # For direct connection, validate before use
                if not self._validate_connection():
                    self.logger.warning(f"Connection dead, recreating (attempt {attempt + 1})")
                    try:
                        self.NavitaireConnection.close()
                    except:
                        pass
                    time.sleep(2 ** attempt)
                    self.NavitaireConnection = create_navitaire_connection()
                
                return pd.read_sql(query, self.NavitaireConnection)
                
            except Exception as e:
                error_msg = str(e).lower()
                # Check for connection-related errors
                is_connection_error = any(keyword in error_msg for keyword in [
                    "connection", "dead", "not connected", "timeout", 
                    "20003", "20047", "not enabled", "timed out"
                ])
                
                if is_connection_error:
                    self.logger.warning(f"Connection error on attempt {attempt + 1}: {str(e)}")
                    if attempt < max_retries - 1:
                        # Recreate connection and retry
                        if not self.is_pool:
                            try:
                                self.NavitaireConnection.close()
                            except:
                                pass
                            time.sleep(3 ** attempt)  # Longer exponential backoff
                            self.NavitaireConnection = create_navitaire_connection()
                        else:
                            # For pool, just wait and retry
                            time.sleep(3 ** attempt)
                    else:
                        self.logger.error(f"Failed after {max_retries} retries")
                        raise
                else:
                    # Non-connection error, don't retry
                    self.logger.error(f"Non-connection error: {str(e)}")
                    raise
        
        raise Exception("Query execution failed after all retries")

    def getCurrentBookedLoads(self, startDate, endDate):
        NavitaireQuery = f""" EXEC {self.NAV_DB_WB_NAME}[dbo].[SP_Bookeddata]
                                @DepartureDate_from = N'{str(startDate)}',
                                @DepartureDate_to = N'{str(endDate)}'
                            """
        return self._execute_query_with_retry(NavitaireQuery)
    
    def get_last_bookings_count(self, departure_start_date, departure_end_date, comparison_date, comparison_datetime):
        """Execute SP_LastBookingsCount stored procedure"""
        query = f"""
                EXEC {self.NAV_DB_WB_NAME}[dbo].[SP_LastBookingsCount]
                    @DepartureStartDate = N'{departure_start_date}',
                    @DepartureEndDate = N'{departure_end_date}',
                    @ComparisonDate = N'{comparison_date}',
                    @ComparisonDateTime = N'{comparison_datetime}'
                """
        df = self._execute_query_with_retry(query)
        df.columns = ['FlightNumber', 'Origin', 'Destination', 
                     'DepartureDate', 'BookingsInHour', 'BookingsCount', 'BookingToday']
        return df

    def get_seats_sold_by_class(self, min_date, max_date):
        query = f'''
        SELECT DepartureDate, FlightNumber, DepartureStation as Origin, ArrivalStation as Destination,
            ILCS.[ClassOfService] as class_code, ILCS.[ClassSold] as seats_sold
        FROM {self.NAV_DB_NAME}[Rez].[InventoryLegVersion] ILV
        JOIN {self.NAV_DB_NAME}[Rez].[InventoryLegClassSold] ILCS ON ILV.InventoryLegID = ILCS.InventoryLegID
        WHERE ILV.DepartureDate >= '{min_date}'
            AND ILV.DepartureDate <= '{max_date}'
            AND ILCS.ClassOfService IN ('P', 'L')
        ORDER BY ILV.ModifiedUTC DESC
        '''
        df = self._execute_query_with_retry(query)
        pivot_df = df.pivot_table(
            index=['DepartureDate', 'FlightNumber', 'Origin', 'Destination'],
            columns='class_code',
            values='seats_sold',
            aggfunc='first'
        ).reset_index()
        pivot_df.columns = ['DepartureDate', 'FlightNumber',
                            'Origin', 'Destination', 'seats_sold_L', 'seats_sold_P']
        return pivot_df
    
    def get_seats_sold_by_w_class(self, min_date, max_date):
        query = f'''
        SELECT DepartureDate, FlightNumber, DepartureStation as Origin, ArrivalStation as Destination,
            ILCS.[ClassOfService] as class_code, ILCS.[ClassSold] as seats_sold
        FROM {self.NAV_DB_NAME}[Rez].[InventoryLegVersion] ILV
        JOIN {self.NAV_DB_NAME}[Rez].[InventoryLegClassSold] ILCS ON ILV.InventoryLegID = ILCS.InventoryLegID
        WHERE ILV.DepartureDate >= '{min_date}'
            AND ILV.DepartureDate <= '{max_date}'
            AND ILCS.ClassOfService IN ('W4', 'W5', 'W6')
        ORDER BY ILV.ModifiedUTC DESC
        '''
        df = self._execute_query_with_retry(query)
        pivot_df = df.pivot_table(
            index=['DepartureDate', 'FlightNumber', 'Origin', 'Destination'],
            columns='class_code',
            values='seats_sold',
            aggfunc='first'
        ).reset_index()
        pivot_df['sector'] = pivot_df['Origin'] + pivot_df['Destination']
        pivot_df.drop(['Origin', 'Destination'], axis=1, inplace=True)
        pivot_df.columns = ['departure_date', 'flight_number',
                            'seats_sold_W4', 'seats_sold_W5', 'seats_sold_W6', 'sector']
        return pivot_df

    def get_current_rbds_by_class(self, min_date, max_date):
        query = f"""
        SELECT DepartureDate, FlightNumber, DepartureStation as Origin, ArrivalStation as Destination,
            ILC.ClassOfService as class_code, ILC.ClassAu as current_availability
        FROM {self.NAV_DB_NAME}[Rez].[InventoryLegClass] ILC
        JOIN {self.NAV_DB_NAME}[Rez].[InventoryLeg] IL ON IL.[InventoryLegID] = ILC.[InventoryLegID]
        WHERE IL.DepartureDate >= '{min_date}'
            AND IL.DepartureDate <= '{max_date}'
            AND ILC.ClassOfService IN ('P', 'L')
        ORDER BY ILC.ModifiedUTC DESC
        """
        df = self._execute_query_with_retry(query)
        pivot_df = df.pivot_table(
            index=['DepartureDate', 'FlightNumber', 'Origin', 'Destination'],
            columns='class_code',
            values='current_availability',
            aggfunc='first'
        ).reset_index()
        pivot_df.columns = ['DepartureDate', 'FlightNumber',
                            'Origin', 'Destination', 'current_rbd_L', 'current_rbd_P']
        return pivot_df

    def get_series_seats(self, min_date, max_date):
        query = f"""
        SELECT Origin, Destination, TRIM(FlightNumber) as FlightNumber, DepartureDate,
            COUNT(PassengerID) as bookedPax
        FROM (
            SELECT BP.PassengerID, IL.DepartureStation as Origin, IL.ArrivalStation as Destination,
                IL.DepartureDate, IL.FlightNumber, PJS.SourceOrganizationCode, PJS.ClassOfService
            FROM {self.NAV_DB_NAME}[Rez].InventoryLeg IL
            LEFT OUTER JOIN {self.NAV_DB_NAME}[Rez].PassengerJourneyLeg PJL ON PJL.InventoryLegID = IL.InventoryLegID
            LEFT OUTER JOIN {self.NAV_DB_NAME}[Rez].PassengerJourneySegment PJS ON PJS.PassengerID = PJL.PassengerID
                AND PJS.SegmentID = PJL.SegmentID AND (RIGHT(PJS.FareBasis, 3) = 'SER')
            LEFT OUTER JOIN {self.NAV_DB_NAME}[Rez].BookingPassenger BP ON BP.PassengerID = PJS.PassengerID
            WHERE IL.Lid > 0 AND IL.Status NOT IN (2)
                AND IL.DepartureDate >= '{min_date}'
                AND IL.DepartureDate <= '{max_date}'
                AND PJS.SourceOrganizationCode IS NOT NULL AND PJS.ClassOfService = 'G'
        ) A
        GROUP BY Origin, Destination, FlightNumber, DepartureDate
        """
        df = self._execute_query_with_retry(query)
        df.columns = ['Origin', 'Destination', 'FlightNumber',
                      'DepartureDate', 'total_series_seats']
        return df

    def get_daily_flights(self, start_date, end_date):
        """Execute SP_DailyFlights stored procedure"""
        query = f"""EXEC {self.NAV_DB_WB_NAME}[dbo].[SP_DailyFlights]
                    @StartDate = N'{str(start_date)}',
                    @EndDate = N'{str(end_date)}'
                 """
        df = self._execute_query_with_retry(query)
        df.columns = ['Origin', 'Destination', 'FlightNumber', 
                     'DepartureDate', 'NextFlight']
        return df
        
    def get_opening_rbds(self, min_date, max_date):
        """Get opening RBDs for both B2B and B2C customers"""
        query = f"""
        WITH RankedInventory AS (
            SELECT 
                IL.FlightNumber,
                IL.DepartureStation AS Origin,
                IL.ArrivalStation AS Destination,
                IL.DepartureDate,
                ILC.ClassOfService,
                ROW_NUMBER() OVER (
                    PARTITION BY IL.FlightNumber, IL.DepartureStation, IL.ArrivalStation, IL.DepartureDate,
                                CASE WHEN Substring(ILC.ClassOfService,1,1) = 'Z' THEN 'B2B' ELSE 'B2C' END
                    ORDER BY ILC.ClassRank, ILC.ModifiedUTC DESC
                ) AS RowNum,
                CASE WHEN Substring(ILC.ClassOfService,1,1) = 'Z' THEN 'B2B' ELSE 'B2C' END AS CustomerType
            FROM 
                {self.NAV_DB_NAME}[Rez].[InventoryLegClass] ILC 
            JOIN 
                {self.NAV_DB_NAME}[Rez].[InventoryLeg] IL ON IL.[InventoryLegID] = ILC.[InventoryLegID]
            WHERE 
                IL.DepartureDate BETWEEN '{min_date}' AND '{max_date}'
                AND ILC.ClassType = ''
                AND ILC.ClassAu != '0'
        )
        SELECT 
            trim(FlightNumber) as FlightNumber,
            Origin,
            Destination,
            DepartureDate,
            MAX(CASE WHEN CustomerType = 'B2B' AND RowNum = 1 THEN ClassOfService ELSE NULL END) AS OpeningRBDB2B,
            MAX(CASE WHEN CustomerType = 'B2C' AND RowNum = 1 THEN ClassOfService ELSE NULL END) AS OpeningRBDB2C
        FROM 
            RankedInventory
        GROUP BY 
            FlightNumber,
            Origin,
            Destination,
            DepartureDate
        ORDER BY 
            DepartureDate,
            Origin,
            Destination,
            FlightNumber
        """
        return self._execute_query_with_retry(query)
    
    def get_w0_rbd(self, start_date, end_date):
        """Get W0 RBD data"""
        query = f"""SELECT classofservice,
           classau,
           CONVERT(VARCHAR(10), departuredate, 120) AS DepartureDate,
           IL.flightnumber,
           IL.departurestation,
           IL.arrivalstation
    FROM   {self.NAV_DB_NAME}[Rez].[inventorylegclass] ILC
           JOIN {self.NAV_DB_NAME}[Rez].[inventoryleg] IL
             ON IL.[inventorylegid] = ILC.[inventorylegid]
    WHERE  IL.departuredate >= '{start_date}'
           AND IL.departuredate <= '{end_date}'
           AND classofservice = 'W0'
    ORDER  BY departuredate """
        return self._execute_query_with_retry(query)
    
    def getFltSectDeptTimeForCurrentDay(self):
        
        current_date = datetime.now(timezone.utc).date()
        NavitaireQuery = f"""
            SELECT
                DepartureStation Origin,
                ArrivalStation Destination,
                TRIM(FlightNumber) as FlightNumber,
                DepartureDate,
                FORMAT(STDUTC, 'HH:mm') as DepartureTimeUTC
            FROM {self.NAV_DB_NAME}rez.inventoryleg
            WHERE DepartureDate = '{current_date.strftime("%Y-%m-%d")}'
        """
        # Use retry mechanism
        return self._execute_query_with_retry(NavitaireQuery)

# ==========================
# DW Query with Retry
# ==========================

@retry_on_connection_error(
    max_retries=3,
    delay=2,
    exceptions=(OperationalError, DBAPIError)
)
def execute_dw_query_with_retry(query, engine):
    """Execute DW query with retry"""
    return pd.read_sql(query, engine)

def get_forecast(min_date, max_date):
    query = f"""
    SELECT ROUND(forecastPLF, 1) AS forecastPLF,Variance as forecast_variance,Origin,Destination,FlightNumber, DepartureDate 
    FROM RM_DW_DATA.forecast_results
    WHERE DepartureDate BETWEEN '{min_date}' AND '{max_date}'
    """
    return query

# ==========================
# Utility functions
# ==========================

def number_of_batches_required(total_records, adhoc_batch_size):
    if total_records < adhoc_batch_size:
        return [1, 0]
    if total_records % adhoc_batch_size != 0:
        return [total_records // adhoc_batch_size, total_records % adhoc_batch_size]
    else:
        return [total_records // adhoc_batch_size, 0]

def add_additional_fields(record, run_id, update_navitaire_method, start_time, route_type, queue_name, cug_alloc, saleinvflag, mongodb):
    record["runId"] = run_id
    record["update_navitaire_method"] = update_navitaire_method
    record["runTime"] = start_time
    record["route_type"] = route_type
    record["queue"] = queue_name
    record['cug_alloc'] = cug_alloc
    record["mongodb"] = mongodb
    record["saleinvflag"] = saleinvflag
    return record

def send_batch_to_sqs(batch, queue_url, additional_fields):
    try:
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
    except Exception as e:
        print(e)

def send_dataframe_to_sqs(df, queue_url, run_id, update_navitaire_method, route_type, queue_name, cug_alloc, salesinv, mongodb, batch_size=10, max_workers=5):
    records = df.to_dict('records')
    batches = [records[i:i+batch_size] for i in range(0, len(records), batch_size)]
    start_time = datetime.now().isoformat()
    additional_fields = {
        'run_id': run_id,
        'update_navitaire_method': update_navitaire_method,
        'start_time': start_time,
        'route_type': route_type,
        'queue_name': queue_name,
        'cug_alloc': cug_alloc,
        'mongodb': mongodb,
        'saleinvflag': salesinv
    }
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(lambda batch: send_batch_to_sqs(batch, queue_url, additional_fields), batches)

def left_join_with_destination_mapping(left_df, right_df):
    right_df_copy = right_df.copy()
    right_df_copy['FlightNumber'] = right_df_copy['FlightNumber'].astype('int64')
    return left_df.merge(
        right_df_copy,
        how='left',
        left_on=['DepartureDate', 'FlightNumber', 'Origin', 'Destin'],
        right_on=['DepartureDate', 'FlightNumber', 'Origin', 'Destination']
    ).drop(columns=['Destination'], errors='ignore')

def quick_optimize(mkt_list, navapihelper):
    print(f"Optimizing {len(mkt_list)} rows...")
    
    unique_params = mkt_list[['Flight1', 'Flight2', 'Sector1', 'Sector2', 'DepartureDate']].drop_duplicates()
    print(f"Reduced to {len(unique_params)} unique API calls")
    
    def api_call(row):
        return navapihelper.get_second_flight_dep(
            row['Flight1'], row['Flight2'], 
            row['Sector1'][:3], row['Sector2'][3:],
            row['DepartureDate'].strftime('%Y-%m-%d')
        )
    
    with ThreadPoolExecutor(max_workers=100) as executor:
        results = list(executor.map(api_call, [row for _, row in unique_params.iterrows()]))
    
    unique_params['api_result'] = results
    mkt_list = mkt_list.merge(unique_params, on=['Flight1', 'Flight2', 'Sector1', 'Sector2', 'DepartureDate'])
    mkt_list['SecondFlightDepartureDate'] = mkt_list['api_result']
    
    return mkt_list.drop('api_result', axis=1)


def create_flight_lookup_map(flights_df):
    flight_map = {}
    for _, flight in flights_df.iterrows():
        key = f"{flight['FlightNumber']}_{flight['Origin']}_{flight['Destination']}_{flight['DepartureDate']}"
        flight_map[key] = flight['DepartureTimeUTC']
    return flight_map

def filter_market_list(market_list_df, ndo0_map):
    current_datetime = datetime.now(timezone.utc)
    logger.info(f"Current time: {current_datetime}")
    logger.info(f"skipping time delta: {str(constantsutils.TIME_BEFORE_DEPARTURE)}")
    cutoff_time = (current_datetime + timedelta(minutes=constantsutils.TIME_BEFORE_DEPARTURE)).strftime("%H:%M")
    current_date = current_datetime.strftime("%Y-%m-%d")

    logger.info(f"Filtering markets. Current date: {current_date}, Cutoff time: {cutoff_time}")

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

            if market_date == current_date:
                key = f"{flight_number}_{origin}_{destination}_{current_date}"

                if key in ndo0_map:
                    departure_time = ndo0_map[key]
                    if departure_time > cutoff_time:
                        keep_mask[idx] = True
                    else:
                        skip_mask[idx] = True
                else:
                    skip_mask[idx] = True
            else:
                keep_mask[idx] = True

        except Exception as e:
            logger.error(f"Error processing market {market}: {str(e)}")
            continue

    filtered_markets_df = market_list_df[keep_mask].copy()
    skipped_markets_df = market_list_df[skip_mask].copy()

    logger.info(f"Total markets processed: {len(market_list_df)}")
    logger.info(f"Markets kept: {len(filtered_markets_df)}")
    logger.info(f"Markets skipped: {len(skipped_markets_df)}")

    return filtered_markets_df, skipped_markets_df

def clean_markets_optimized(market_list_df, w0_df,nav_helper):
    try:
        current_flights_df = nav_helper.getFltSectDeptTimeForCurrentDay()
        if current_flights_df.empty:
            logger.info("No current day flights found in Navitaire")
            return market_list_df, 0

        ndo0_map = create_flight_lookup_map(current_flights_df)
        if not ndo0_map:
            logger.info("Empty flight lookup map created")
            return market_list_df, 0

        filtered_markets_df, skipped_markets_df = filter_market_list(market_list_df, ndo0_map)
        w0_df["DepartureDate"] = pd.to_datetime(w0_df["DepartureDate"])
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

        merged_df = merged_df.rename(columns={'classau': 'w0_value'})
        merged_df['PerStart'] = merged_df['PerStart'].dt.strftime('%m/%d/%Y')
        merged_df.drop(columns=['DepartureDate', 'flightnumber', 'departurestation', 'arrivalstation', 'classofservice'], inplace=True)
        
        return merged_df, len(skipped_markets_df)

    except Exception as e:
        logger.error(f"Error in clean_markets_optimized: {str(e)}")
        return market_list_df, 0

def fetch_and_process_market_list(start_period, end_period, market_list_table, query_type='standard'):
    log_time.start()
    try:
        # Fetch using retry mechanism
        market_list_filter = {}
        if market_list_table != "market_list" and query_type != 'connections':
            market_list_filter['defaultMlFlag'] = 0

        cursor = mongo_find_with_retry(market_list_table, market_list_filter)
        base_ml_df = pd.DataFrame(cursor)
        
        if base_ml_df.empty:
            logger.info(f"Market list {market_list_table} is empty.")
            return pd.DataFrame()

        # Define projection columns based on query type
        standard_cols = [
            'Origin','Destin','FlightNumber','DOW','TimeWindowStart','TimeWindowEnd','CurveID','CarrExlusionB2C',
            'CarrExlusionB2B','flightExclusionB2C','flightExclusionB2B','fareAnchor','hardAnchor','plfThreshold',
            'fareOffset','FirstRBDAlloc','OtherRBDAlloc','B2BBackstop','B2CBackstop','B2BFactor','obSeats',
            'obFare','SkippingFactor','DaySpan','AutoTimeRangeFlag','analystName','openingFares','OverBooking',
            'profileFares','rbdPushFlag','B2BTolerance','B2CTolerance','distressInventoryFlag','seriesBlock',
            'autoGroup','tbfFlag','CugFlag','MktFareFlag','autoBackstopFlag','directFlag','TBFEnhFlag',
            'defaultMlFlag','b2bStrategy','Marketfare_Upsell','Outbound_Stop','forecastAllocFlag','Spoilage_Flag'
        ]
        sales_cols = ['Origin','Destin','FlightNumber','DOW','CurveID','saleflag']
        connection_cols = [
            'Sector1','Flight1','Sector2','Flight2','Outbound_stop','PerType','DOW','Price_Strategy',
            'Discount_Value','carrierExclusionB2C','carrierExclusionB2B','FirstRBDAlloc','OtherRBDAlloc',
            'B2BBackstop','B2CBackstop','B2BFactor','SkippingFactor','Outbound_duration','Currency',
            'fareAnchor','Offset','DiscountFlag','sosFlag','analystName'
        ]

        projection_cols = []
        if query_type == 'standard':
            projection_cols = standard_cols
        elif query_type == 'sales':
            projection_cols = sales_cols
        elif query_type == 'connections':
            projection_cols = connection_cols

        existing_cols = [col for col in projection_cols if col in base_ml_df.columns]
        required_cols = existing_cols + ['PerStart', 'PerEnd', 'DOW']
        base_ml_df = base_ml_df[list(set(required_cols))]

        # Generate Date Range
        s_date = datetime.strptime(start_period, "%Y-%m-%d")
        e_date = datetime.strptime(end_period, "%Y-%m-%d")
        date_range = pd.date_range(start=s_date, end=e_date , freq='D')
        date_df = pd.DataFrame({'Date': date_range})

        # Cross Join
        base_ml_df['key'] = 1
        date_df['key'] = 1
        mkt_list = pd.merge(date_df, base_ml_df, on='key').drop("key", axis=1)

        # Apply Filters
        mkt_list['PerStart_dt'] = pd.to_datetime(mkt_list['PerStart'], format='%m/%d/%Y')
        mkt_list['PerEnd_dt'] = pd.to_datetime(mkt_list['PerEnd'], format='%m/%d/%Y')

        mkt_list = mkt_list[
            (mkt_list['Date'] >= mkt_list['PerStart_dt']) &
            (mkt_list['Date'] <= mkt_list['PerEnd_dt'])
        ]

        # Apply DOW Filter
        def check_dow(row):
            pandas_dow = row['Date'].dayofweek
            mysql_dow = (pandas_dow + 2)
            if mysql_dow > 7:
                mysql_dow = 1

            dow_string = row['DOW']
            try:
                flag = str(dow_string)[mysql_dow - 1]
                return flag == '1'
            except (IndexError, TypeError):
                return False

        mkt_list['Flag'] = mkt_list.apply(check_dow, axis=1)
        mkt_list = mkt_list[mkt_list['Flag'] == True]

        # Format Final Output
        mkt_list['PerStart'] = mkt_list['Date'].dt.strftime('%m/%d/%Y')
        mkt_list['PerEnd'] = mkt_list['Date'].dt.strftime('%m/%d/%Y')
        
        final_cols = existing_cols + ['PerStart', 'PerEnd']
        mkt_list = mkt_list[final_cols]

        if query_type != 'connections':
            mkt_list = mkt_list.sort_values(by=['Origin', 'Destin', 'FlightNumber'])
        
        return mkt_list

    except Exception as e:
        logger.error(f"Error in fetch_and_process_market_list: {str(e)}")
        raise
    finally:
        log_time.end("fetch_and_process_market_list")


def fetch_sales_list(start_date, end_date, sector_list):
    mktList = pd.DataFrame()

    pipeline = [
        {
            "$match": {
                "sector": {"$in": sector_list},
                "departure_date": {
                    "$gte": start_date,  
                    "$lte": end_date  
                }
            }
        },
        {
            "$lookup": {
                "from": "sales_sector_base_load",
                "localField": "sector",
                "foreignField": "sector",
                "as": "base_load_info"
            }
        },
        {
            "$unwind": "$base_load_info"
        },
        {
            "$project": {
                "_id": 0,
                "sector": 1,
                "departure_date": 1,
                "series_value": 1,
                "mapped_month": "$base_load_info.mapped_month",
                "target_base_load": "$base_load_info.target_base_load"
            }
        }
    ]
    try:
        logger.info("Executing sales inv aggregation pipeline")
        results = list(mongo_db.sales_flight_series.aggregate(pipeline))
        logger.info(f"Retrieved {len(results)} records from aggregation")
        
        if len(results) == 0:
            logger.warning("No results returned from aggregation pipeline")
        
        mktList = pd.DataFrame(results)
        
    except Exception as e:
        logger.error(f"Error in direct route aggregation: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
    
    return mktList

# def sector_hist_curr_plf_query(mapped_months, sector_list):
#     sector_in = ",".join(f"'{s}'" for s in sector_list)

#     return f"""
#         SELECT 
#             Sector AS sector,
#             DATE_FORMAT(DepartureDate, '%%Y-%%m') AS month,
#             ROUND(AVG(`0`), 2) AS hist_curr_plf
#         FROM RM_DW_DATA.booking_curves_plf
#         WHERE DATE_FORMAT(DepartureDate, '%%Y-%%m') IN ({', '.join(f"'{m}'" for m in mapped_months)})
#         AND Sector IN ({sector_in})
#         GROUP BY Sector, DATE_FORMAT(DepartureDate, '%%Y-%%m');
#     """

def sector_hist_curr_plf_query(mapped_months, sector_list):
    start_day = 0
    end_day = 91  # Changed to 366 to handle leap years

    # Build the CASE mapping
    case_when_inner = "\n".join([f"WHEN {i} THEN `{i}`" for i in range(start_day, end_day + 1)])  

    sector_in = ",".join(f"'{s}'" for s in sector_list)

    sql_query = f"""
    SELECT 
        Sector AS sector,
        DATE_FORMAT(DepartureDate, '%%Y-%%m') AS month,
        ROUND(AVG(
            CASE 
                WHEN DATEDIFF(
                    DepartureDate, DATE_SUB(CURDATE(), INTERVAL 1 YEAR)
                ) BETWEEN {start_day} AND {end_day}
                THEN 
                    CASE DATEDIFF(
                        DepartureDate,
                        DATE_SUB(CURDATE(), INTERVAL 1 YEAR)
                    )
                        {case_when_inner}
                    END
            END
        ), 2) AS hist_curr_plf
    FROM RM_DW_DATA.booking_curves_plf
    WHERE DATE_FORMAT(DepartureDate, '%%Y-%%m') IN ({', '.join(f"'{m}'" for m in mapped_months)})
    AND Sector IN ({sector_in})
    GROUP BY Sector, DATE_FORMAT(DepartureDate, '%%Y-%%m');
    """

    return sql_query

def sector_month_start_hist_base_plf_query(mapped_months, sector_list):
    start_day = 0
    end_day = 31

    # Build the CASE mapping
    case_when_inner = "\n".join([f"WHEN {i} THEN `{i}`" for i in range(start_day, end_day + 1)])  

    sector_in = ",".join(f"'{s}'" for s in sector_list)

    sql_query = f"""
    SELECT 
        Sector AS sector,
        DATE_FORMAT(DepartureDate, '%%Y-%%m') AS month,
        ROUND(AVG(
            CASE 
                WHEN DATEDIFF(
                    DepartureDate, 
                    DATE_SUB(DepartureDate, INTERVAL DAY(DepartureDate)-1 DAY)
                ) BETWEEN {start_day} AND {end_day}
                THEN 
                    CASE DATEDIFF(
                        DepartureDate,
                        DATE_SUB(DepartureDate, INTERVAL DAY(DepartureDate)-1 DAY)
                    )
                        {case_when_inner}
                    END
            END
        ), 2) AS hist_base_plf
    FROM RM_DW_DATA.booking_curves_plf
    WHERE DATE_FORMAT(DepartureDate, '%%Y-%%m') IN ({', '.join(f"'{m}'" for m in mapped_months)})
    AND Sector IN ({sector_in})
    GROUP BY Sector, DATE_FORMAT(DepartureDate, '%%Y-%%m');
    """

    return sql_query

def fetch_market_list_for_sales(start_date, end_date, sector_list):
    mktList = pd.DataFrame()

    pipeline = [
        {
            "$addFields": {
                "PerStartDate": {
                    "$dateFromString": {"dateString": "$PerStart", "format": "%m/%d/%Y"}
                },
                "sector": {"$concat": ["$Origin", "$Destin"]}
            }
        },
        {
            "$match": {
                "sector": {"$in": sector_list},
                "PerStartDate": {"$gte": start_date, "$lte": end_date}
            }
        },
        {
            "$project": {
                "_id": 0,
                "departure_date": "$PerStartDate", 
                "flight_number": "$FlightNumber",
                "sector": 1
            }
        }
    ]
    try:
        logger.info("Executing sales inv aggregation pipeline")
        results = list(mongo_db.market_list.aggregate(pipeline))
        logger.info(f"Retrieved {len(results)} records from aggregation")
        
        if len(results) == 0:
            logger.warning("No results returned from aggregation pipeline")
        
        mktList = pd.DataFrame(results)
        
    except Exception as e:
        logger.error(f"Error in direct route aggregation: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
    
    return mktList


def fetch_and_process_sales_market_list(start_date_str, end_date_str, sector_list):
    log_time.start()
    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

        nav_helper = NavDatabaseHelper(
                navconn=NavitaireConnection,
                nav_db_name=configsettings.NAV_DB_NAME,
                nav_db_wb_name=configsettings.NAV_DB_WB_NAME
            )

        sales_list = fetch_sales_list(start_date, end_date, sector_list)
        mkt_list = fetch_market_list_for_sales(start_date, end_date, sector_list)
        if not mkt_list.empty:
            mkt_list['flight_number'] = mkt_list['flight_number'].astype(str)

        w_bookload_list = nav_helper.get_seats_sold_by_w_class(start_date, end_date)
        w_bookload_list['departure_date'] = pd.to_datetime(w_bookload_list['departure_date'])

        bookload_list = nav_helper.getCurrentBookedLoads(start_date, end_date)
        bookload_list['DepartureDate'] = pd.to_datetime(bookload_list['DepartureDate'])
        bookload_list['sector'] = bookload_list['Origin'] + bookload_list['Destination']
        
        sales_list['mapped_month'] = sales_list['mapped_month'].astype(str).str[:7]
        mapped_months = sales_list['mapped_month'].unique().tolist()
        hist_curr_plf_query = sector_hist_curr_plf_query(mapped_months, sector_list)
        month_start_hist_base_plf_query = sector_month_start_hist_base_plf_query(mapped_months, sector_list)

        hist_curr_plf_list = execute_dw_query_with_retry(hist_curr_plf_query, dwconn)
        month_start_hist_base_plf_list = execute_dw_query_with_retry(month_start_hist_base_plf_query, dwconn)

        # Merge hist_curr_plf_list
        sales_list = sales_list.merge(
            hist_curr_plf_list[['sector', 'hist_curr_plf', 'month']],
            how='left',
            left_on=['sector', "mapped_month"],
            right_on=['sector', "month"],
            suffixes=('', '_drop')
        )
        sales_list.drop(columns=[col for col in ['month']] + 
            [col for col in sales_list.columns if col.endswith('_drop')], inplace=True)

        # Merge month_start_hist_base_plf_list
        sales_list = sales_list.merge(
            month_start_hist_base_plf_list[['sector', 'hist_base_plf', 'month']],
            how='left',
            left_on=['sector', "mapped_month"],
            right_on=['sector', "month"],
            suffixes=('', '_drop')
        )
        sales_list.drop(columns=[col for col in ['month']] + 
            [col for col in sales_list.columns if col.endswith('_drop')], inplace=True)

        # Merge mkt_list
        sales_list = sales_list.merge(
            mkt_list[['departure_date', 'flight_number', 'sector']],
            how='left',
            on=['departure_date', 'sector'],
            suffixes=('', '_drop')
        )
        sales_list.drop(columns=[col for col in sales_list.columns if col.endswith('_drop')], inplace=True)

        # Merge w_bookload_list
        sales_list = sales_list.merge(
            w_bookload_list[['departure_date', 'flight_number', 'sector', 'seats_sold_W4', 'seats_sold_W5', 'seats_sold_W6']],
            how='left',
            on=['departure_date', 'flight_number', 'sector'],
            suffixes=('', '_drop')
        )
        sales_list.drop(columns=[col for col in sales_list.columns if col.endswith('_drop')], inplace=True)

        # Merge bookload_list
        sales_list = sales_list.merge(
            bookload_list[['DepartureDate', 'FlightNumber', 'sector', 'bookedPax', 'adjustedCapacity']],
            how='left',
            left_on=['departure_date', 'sector', 'flight_number'],
            right_on=['DepartureDate', 'sector', 'FlightNumber'],
            suffixes=('', '_drop')
        )
        sales_list.drop(columns=[col for col in ['DepartureDate', 'FlightNumber']] + 
                        [col for col in sales_list.columns if col.endswith('_drop')], inplace=True)
        
        sales_list['departure_date'] = sales_list['departure_date'].astype(str)

        return sales_list

    except Exception as e:
        logger.error(f"Error in fetch_and_process_sales_market_list: {str(e)}")
        raise
    finally:
        log_time.end("fetch_and_process_sales_market_list")

# Helper functions with retry

def insert_run_start(start_time, run_id, start_interval, end_interval, market_list_table, username, type, is_connections_required, update_navitaire_method, region, count):
    log_time.start()
    cursor = mongo_find_with_retry('currentversion', {})
    currVersionValues = pd.DataFrame(cursor)

    table_versions = {}
    for index, currVersion in currVersionValues.iterrows():
        if market_list_table == currVersion["tableName"]:
            table_versions["market_list"] = currVersion["curr_version"]
            table_versions["market_list_name"] = currVersion["tableName"]
        else:
            table_versions[currVersion["tableName"]] = currVersion["curr_version"]

    try:
        document = {
            'run_id': run_id,
            'start_time': start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'user': username,
            'dtd_start': start_interval,
            'dtd_end': end_interval,
            'Curves': table_versions.get('Curves'),
            'd1_d2_strategies': table_versions.get('d1_d2_strategies'),
            'market_list': table_versions.get('market_list'),
            'Fares': table_versions.get('Fares'),
            'type': type,
            'market_list_name': table_versions.get('market_list_name'),
            'is_connections_required': is_connections_required,
            'update_navitaire_method': update_navitaire_method,
            'is_run_completed': '0',
            'region': region,
            'marketListCount': count
        }
        mongo_insert_with_retry('allocation_run_audit', document)
    except Exception as e:
        logger.info(e)
    finally:
        log_time.end("insert_run_start")
    logger.info("STARTED ALLOCATION RUN")

def get_run_operation_log_data(start_interval, end_interval, market_list_table, update_navitaire_method, region, type, run_id):
    try:
        current_time = datetime.now(timezone.utc)
        time_window = timedelta(minutes=15)
        start_time = current_time - time_window
        end_time = current_time + time_window

        query_filter = {
            'run_timestamp': {
                '$gte': start_time.strftime('%Y-%m-%d %H:%M:%S'),
                '$lte': end_time.strftime('%Y-%m-%d %H:%M:%S')
            },
            'dtd': f'{start_interval}-{end_interval}',
            'marketlist_name': market_list_table,
            'region': region,
            'type': type,
            'operation': 'DELETED',
            'run_id': None
        }
        
        cursor = mongo_find_with_retry('allocation_run_operation_log', query_filter)
        df = pd.DataFrame(cursor)

        if not df.empty:
            mongo_update_with_retry(
                'allocation_run_operation_log',
                {'operation_id': df.iloc[0]['operation_id']},
                {'$set': {'run_id': run_id}}
            )
        
        return df

    except Exception as e:
        logger.error(f"Error fetching run_operation_log data: {str(e)}")
        raise

def insert_deleted_run(start_time, run_id, start_interval, end_interval, market_list_table, username, type, update_navitaire_method, region, count):
    log_time.start()
    cursor = mongo_find_with_retry('currentVersion', {}, {'tableName': 1, 'curr_version': 1, '_id': 0})
    currVersionValues = pd.DataFrame(cursor)

    is_connections_required = 1 if type == 'Scheduled' else 0
    is_connections_started = 1 if type == 'Scheduled' else None

    table_versions = {}
    for index, currVersion in currVersionValues.iterrows():
        if market_list_table == currVersion["tableName"]:
            table_versions["market_list"] = currVersion["curr_version"]
            table_versions["market_list_name"] = currVersion["tableName"]
        else:
            table_versions[currVersion["tableName"]] = currVersion["curr_version"]

    try:
        document = {
            'run_id': run_id,
            'start_time': start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'end_time': start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'actual_end_time': start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'user': username,
            'dtd_start': start_interval,
            'dtd_end': end_interval,
            'Curves': table_versions.get('Curves'),
            'd1_d2_strategies': table_versions.get('d1_d2_strategies'),
            'market_list': table_versions.get('market_list'),
            'Fares': table_versions.get('Fares'),
            'type': type,
            'market_list_name': table_versions.get('market_list_name'),
            'is_connections_required': is_connections_required,
            'is_connections_started': is_connections_started,
            'update_navitaire_method': update_navitaire_method,
            'is_run_completed': '0',
            'region': region,
            'marketListCount': count,
            'run_status': 'DELETED'
        }
        mongo_insert_with_retry('allocation_run_audit', document)
    except Exception as e:
        logger.info(e)
    finally:
        log_time.end("insert_deleted_run")

def insert_file_upload_s3(run_id, createdAt):
    log_time.start()
    try:
        mongo_insert_with_retry('s3_file_upload', {'runId': run_id, 'createdAt': createdAt})
    except Exception as e:
        logger.info(e)
    finally:
        log_time.end("insert_file_upload_s3")
    logger.info("S3 File Audited")

def update_run_end(end_time, run_id):
    log_time.start()
    try:
        mongo_update_with_retry(
            'allocation_run_audit',
            {'run_id': run_id},
            {'$set': {'end_time': end_time.strftime('%Y-%m-%d %H:%M:%S')}}
        )
    except Exception as e:
        logger.info(e)
    finally:
        log_time.end("update_run_end")
    logger.info("STOPPED ALLOCATION RUN")

def update_run_with_batches(total_batches, run_id):
    log_time.start()
    try:
        mongo_update_with_retry(
            'allocation_run_audit',
            {'run_id': run_id},
            {'$set': {
                'total_batch_size': total_batches,
                'batch_completed': 0,
                'adhoc_run_status': "IN-QUEUE"
            }}
        )
    except Exception as e:
        logger.info(e)
    finally:
        log_time.end("update_run_with_batches")

def allocation_run_batch_audit(run_id, batch_id):
    log_time.start()
    try:
        mongo_insert_with_retry('allocation_run_batch_audit', {'runId': run_id, 'batch_id': batch_id})
    except Exception as e:
        logger.info(e)
    finally:
        log_time.end("allocation_run_batch_audit")

def deduct_available_slot():
    """Get the available slots for queue to run Adhoc API calls"""
    cursor = mongo_find_with_retry('adhoc_run_queue', {}, {'available': 1})
    adhoc_docs = list(cursor)
    adhoc_doc = adhoc_docs[0] if adhoc_docs else None
    count_available_slot = adhoc_doc['available'] if adhoc_doc else 0
    logger.info(f"Slot Available: {count_available_slot}")
  
    if count_available_slot == 0:
        logger.info("No Slot Available!")
        return False
    
    new_available_slot = count_available_slot - 1
    
    try:
        filter_query = {'_id': adhoc_doc['_id']} if adhoc_doc else {}
        mongo_update_with_retry(
            'adhoc_run_queue',
            filter_query,
            {'$set': {'available': new_available_slot}}
        )
    except Exception as e:
        logger.info(e)
    logger.info(f"Slot Available after Adhoc run started: {new_available_slot}")
    return True

def custom_serializer(obj):
    if isinstance(obj, Timestamp):
        return str(obj.isoformat())
    return str(obj)

# ==========================
# Main Lambda Handler
# ==========================

def lambda_handler(event, context):
    """
    Fetches the market list, integrates Navitaire data, and sends it to SQS.
    """
    # Input parsing
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
    cug_alloc_key = "cug_alloc"
    mongodb_key = "mongodb"
    sales_dtd_start_key = "sales_dtd_start"
    sales_dtd_end_key = "sales_dtd_end"
    sector_list_key = "sector_list"

    adhoc_batch_size = constantsutils.ADHOC_BATCH_SIZE
    body = event['body']
    data = json.loads(body)

    # Defaults
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
    cug_alloc = "0"
    mongodb = "0"
    salesinv = "0"
    sales_dtd_start = ""
    sales_dtd_end = ""
    sector_list = ""

    # Overrides
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
    if cug_alloc_key in data:
        cug_alloc = data[cug_alloc_key]
    if mongodb_key in data:
        mongodb = data[mongodb_key]
    if sales_dtd_start_key in data:
        sales_dtd_start = data[sales_dtd_start_key]
    if sales_dtd_end_key in data:
        sales_dtd_end = data[sales_dtd_end_key]
    if sector_list_key in data:
        sector_list = json.loads(data[sector_list_key])
        
    if type == "Adhoc_Sales":
        salesinv = "1"

    # Start program
    if salesinv == "0":
        if type == "Adhoc":
            cache_client.set(unique_key_value, 0, 18000)
            available_slot = deduct_available_slot()
            if not available_slot:
                logger.info("No slots available to run Adhoc API!")
                return False

        current_utc_time = datetime.utcnow()
        utc_timezone = pytz.timezone('UTC')
        ist_timezone = pytz.timezone('Asia/Kolkata')
        startTime = current_utc_time.replace(tzinfo=utc_timezone).astimezone(ist_timezone)
        curr_date = startTime.date()
        start_period = datetime.strftime(curr_date + timedelta(days=start_interval), "%Y-%m-%d")
        end_period = datetime.strftime(curr_date + timedelta(days=end_interval), "%Y-%m-%d")
        
        # Fetch market list
        if route_type == "direct":
            mkt_list = fetch_and_process_market_list(start_period, end_period, market_list_table)
            nav_helper = NavDatabaseHelper(
                navconn=NavitaireConnection,
                nav_db_name=configsettings.NAV_DB_NAME,
                nav_db_wb_name=configsettings.NAV_DB_WB_NAME
            )
            
            w0_df = nav_helper.get_w0_rbd(start_period, end_period)
            logger.info(f"Original Market List {mkt_list}")
            mkt_list, failCount = clean_markets_optimized(mkt_list, w0_df,nav_helper)
            logger.info(f"Filtered Market List {mkt_list}")
            mkt_list = mkt_list.reset_index()
            markets_count = mkt_list.shape[0]
            logger.info(f"Total markets: {markets_count}")

            # Initialize NavDatabaseHelper
        

            # Convert PerStart to datetime for joining
            mkt_list['DepartureDate'] = pd.to_datetime(mkt_list['PerStart'], format='%m/%d/%Y').dt.date
            min_date = mkt_list['DepartureDate'].min()
            max_date = mkt_list['DepartureDate'].max()
            ist = pytz.timezone('Asia/Kolkata')

            curr_date = datetime.now(ist).date()
            curr_datetime = datetime.now(ist) - timedelta(hours=1)
            curr_datetime_str = curr_datetime.strftime("%Y-%m-%d %H:%M:%S")
            
            # Fetch Navitaire data with retry
            booked_loads = nav_helper.getCurrentBookedLoads(startDate=min_date, endDate=max_date)
            seats_sold_pivot = nav_helper.get_seats_sold_by_class(min_date, max_date)
            rbd_pivot = nav_helper.get_current_rbds_by_class(min_date, max_date)
            series_seats_data = nav_helper.get_series_seats(min_date, max_date)
            daily_flights = nav_helper.get_daily_flights(min_date, max_date)
            opening_rbds = nav_helper.get_opening_rbds(min_date, max_date)
            bookings_count = nav_helper.get_last_bookings_count(
                departure_start_date=min_date,
                departure_end_date=max_date,
                comparison_date=curr_date,
                comparison_datetime=curr_datetime_str
            )
            forecast_plf_query = get_forecast(min_date, max_date)
            forecast_plf_list = execute_dw_query_with_retry(forecast_plf_query, dwconn)

            mkt_list = left_join_with_destination_mapping(mkt_list, forecast_plf_list)
            print(len(mkt_list), " forecast_plf_list")
            mkt_list = left_join_with_destination_mapping(mkt_list, booked_loads)
            print(len(mkt_list), " booked_loads")
            mkt_list = left_join_with_destination_mapping(mkt_list, seats_sold_pivot)
            print(len(mkt_list), " seats_sold_pivot")
            mkt_list = left_join_with_destination_mapping(mkt_list, rbd_pivot)
            print(len(mkt_list), " rbd_pivot")
            mkt_list = left_join_with_destination_mapping(mkt_list, series_seats_data)
            print(len(mkt_list), " series_seats_data")
            mkt_list = left_join_with_destination_mapping(mkt_list, daily_flights)
            print(len(mkt_list), " daily_flights")
            mkt_list = left_join_with_destination_mapping(mkt_list, opening_rbds)
            print(len(mkt_list), " opening_rbds")
            mkt_list = left_join_with_destination_mapping(mkt_list, bookings_count)
            print(len(mkt_list), " bookings_count")
            
        elif route_type == "connections":
            navapihelper = NavAPIHelper(configsettings.NAV_BASE_URL, configsettings.NAV_OCP_SUBSCRIPTION_KEY, configsettings.NAV_USERNAME, configsettings.NAV_PWD, configsettings.NAV_DOMAIN)
            update_navitaire_method = "sftp"
            is_connections_required = "0"
            region = 'Domestic'
            
            mkt_list = fetch_and_process_market_list(start_period, end_period, market_list_table, query_type='connections')

            nav_helper = NavDatabaseHelper(
                navconn=NavitaireConnection,
                nav_db_name=configsettings.NAV_DB_NAME,
                nav_db_wb_name=configsettings.NAV_DB_WB_NAME
            )

            mkt_list['DepartureDate'] = pd.to_datetime(mkt_list['PerStart'], format='%m/%d/%Y').dt.date
            min_date = mkt_list['DepartureDate'].min()
            max_date = mkt_list['DepartureDate'].max()
            
            mkt_list = quick_optimize(mkt_list, navapihelper)
            
            original_count = len(mkt_list)
            mkt_list = mkt_list[mkt_list['SecondFlightDepartureDate'] != ""]
            filtered_count = len(mkt_list)
            logger.info(f"Filtered connections: {original_count} -> {filtered_count} (removed {original_count - filtered_count} invalid connections)")
            
            if not mkt_list.empty:
                mkt_list['SecondFlightDepartureDate'] = pd.to_datetime(mkt_list['SecondFlightDepartureDate']).dt.date
                all_dates = pd.concat([mkt_list['DepartureDate'], mkt_list['SecondFlightDepartureDate']])
                min_date = all_dates.min()
                max_date = all_dates.max()
            
            booked_loads = nav_helper.getCurrentBookedLoads(startDate=min_date, endDate=max_date)
            forecast_plf_query = get_forecast(min_date, max_date)
            forecast_plf_list = execute_dw_query_with_retry(forecast_plf_query, dwconn)
            
            mkt_list['Sector1Origin'] = mkt_list['Sector1'].str[:3]
            mkt_list['Sector1Dest'] = mkt_list['Sector1'].str[3:]
            mkt_list['Sector2Origin'] = mkt_list['Sector2'].str[:3]
            mkt_list['Sector2Dest'] = mkt_list['Sector2'].str[3:]

            opening_rbds = nav_helper.get_opening_rbds(min_date, max_date)
            opening_rbds['FlightNumber'] = opening_rbds['FlightNumber'].astype('int64')

            booked_loads['FlightNumber'] = booked_loads['FlightNumber'].astype('int64')
            forecast_plf_list['FlightNumber'] = forecast_plf_list['FlightNumber'].astype('int64')
            
            # Merge operations for connections...
            mkt_list = mkt_list.merge(
                forecast_plf_list[['DepartureDate', 'FlightNumber', 'Origin', 'Destination', 'forecastPLF']],
                how='left',
                left_on=['DepartureDate', 'Flight1', 'Sector1Origin', 'Sector1Dest'],
                right_on=['DepartureDate', 'FlightNumber', 'Origin', 'Destination'],
                suffixes=('', '_drop')
            ).rename(columns={'forecastPLF': 'forecastPLF1'}).drop(
                columns=['Origin', 'Destination', 'FlightNumber', 'DepartureDate_drop'], errors='ignore')
            
            mkt_list = mkt_list.merge(
                forecast_plf_list[['DepartureDate', 'FlightNumber', 'Origin', 'Destination', 'forecastPLF']],
                how='left',
                left_on=['DepartureDate', 'Flight2', 'Sector2Origin', 'Sector2Dest'],
                right_on=['DepartureDate', 'FlightNumber', 'Origin', 'Destination'],
                suffixes=('', '_drop')
            ).rename(columns={'forecastPLF': 'forecastPLF2'}).drop(
                columns=['Origin', 'Destination', 'FlightNumber', 'DepartureDate_drop'], errors='ignore')
            
            mkt_list = mkt_list.merge(
                forecast_plf_list[['DepartureDate', 'FlightNumber', 'Origin', 'Destination', 'forecast_variance']],
                how='left',
                left_on=['DepartureDate', 'Flight1', 'Sector1Origin', 'Sector1Dest'],
                right_on=['DepartureDate', 'FlightNumber', 'Origin', 'Destination'],
                suffixes=('', '_drop')
            ).rename(columns={'forecast_variance': 'forecast_variance1'}).drop(
                columns=['Origin', 'Destination', 'FlightNumber', 'DepartureDate_drop'], errors='ignore')
            
            mkt_list = mkt_list.merge(
                forecast_plf_list[['DepartureDate', 'FlightNumber', 'Origin', 'Destination', 'forecast_variance']],
                how='left',
                left_on=['DepartureDate', 'Flight2', 'Sector2Origin', 'Sector2Dest'],
                right_on=['DepartureDate', 'FlightNumber', 'Origin', 'Destination'],
                suffixes=('', '_drop')
            ).rename(columns={'forecast_variance': 'forecast_variance2'}).drop(
                columns=['Origin', 'Destination', 'FlightNumber', 'DepartureDate_drop'], errors='ignore')
            
            mkt_list = mkt_list.merge(
                booked_loads[['DepartureDate', 'FlightNumber', 'Origin', 'Destination', 'bookedPax']],
                how='left',
                left_on=['DepartureDate', 'Flight1', 'Sector1Origin', 'Sector1Dest'],
                right_on=['DepartureDate', 'FlightNumber', 'Origin', 'Destination'],
                suffixes=('', '_drop')
            ).rename(columns={'bookedPax': 'BookedPax1'}).drop(
                columns=['Origin', 'Destination', 'FlightNumber', 'DepartureDate_drop'], errors='ignore')
            
            mkt_list = mkt_list.merge(
                booked_loads[['DepartureDate', 'FlightNumber', 'Origin', 'Destination', 'capacity']],
                how='left',
                left_on=['DepartureDate', 'Flight1', 'Sector1Origin', 'Sector1Dest'],
                right_on=['DepartureDate', 'FlightNumber', 'Origin', 'Destination'],
                suffixes=('', '_drop')
            ).rename(columns={'capacity': 'Lid1'}).drop(
                columns=['Origin', 'Destination', 'FlightNumber', 'DepartureDate_drop'], errors='ignore')

            mkt_list = mkt_list.merge(
                booked_loads[['DepartureDate', 'FlightNumber', 'Origin', 'Destination', 'bookedPax']],
                how='left',
                left_on=['SecondFlightDepartureDate', 'Flight2', 'Sector2Origin', 'Sector2Dest'],
                right_on=['DepartureDate', 'FlightNumber', 'Origin', 'Destination'],
                suffixes=('', '_drop')
            ).rename(columns={'bookedPax': 'BookedPax2'}).drop(
                columns=['Origin', 'Destination', 'FlightNumber', 'DepartureDate_drop'], errors='ignore')
            
            mkt_list = mkt_list.merge(
                booked_loads[['DepartureDate', 'FlightNumber', 'Origin', 'Destination', 'capacity']],
                how='left',
                left_on=['SecondFlightDepartureDate', 'Flight2', 'Sector2Origin', 'Sector2Dest'],
                right_on=['DepartureDate', 'FlightNumber', 'Origin', 'Destination'],
                suffixes=('', '_drop')
            ).rename(columns={'capacity': 'Lid2'}).drop(
                columns=['Origin', 'Destination', 'FlightNumber', 'DepartureDate_drop'], errors='ignore')

            mkt_list = mkt_list.merge(
                opening_rbds[['DepartureDate', 'FlightNumber', 'Origin', 'Destination', 'OpeningRBDB2B', 'OpeningRBDB2C']],
                how='left',
                left_on=['DepartureDate', 'Flight1', 'Sector1Origin', 'Sector1Dest'],
                right_on=['DepartureDate', 'FlightNumber', 'Origin', 'Destination'],
                suffixes=('', '_drop')
            ).rename(columns={'OpeningRBDB2B': 'OpeningRBDB2B1', 'OpeningRBDB2C': 'OpeningRBDB2C1'}).drop(
                columns=['Origin', 'Destination', 'FlightNumber', 'DepartureDate_drop'], errors='ignore')

            mkt_list = mkt_list.merge(
                opening_rbds[['DepartureDate', 'FlightNumber', 'Origin', 'Destination', 'OpeningRBDB2B', 'OpeningRBDB2C']],
                how='left',
                left_on=['SecondFlightDepartureDate', 'Flight2', 'Sector2Origin', 'Sector2Dest'],
                right_on=['DepartureDate', 'FlightNumber', 'Origin', 'Destination'],
                suffixes=('', '_drop')
            ).rename(columns={'OpeningRBDB2B': 'OpeningRBDB2B2', 'OpeningRBDB2C': 'OpeningRBDB2C2'}).drop(
                columns=['Origin', 'Destination', 'FlightNumber', 'DepartureDate_drop'], errors='ignore')
            
            markets_count = mkt_list.shape[0]
            mkt_list['SecondFlightDepartureDate'] = mkt_list['SecondFlightDepartureDate'].astype(str)
            mkt_list = mkt_list.drop(columns=['DepartureDate'], errors='ignore')
            
        mkt_list = mkt_list.drop(columns=['DepartureDate'], errors='ignore')
        
        # Proceed with SQS
        start_time = datetime.now()
        run_id = str(uuid4())
        log_time.update_run_id(run_id)

        del_stack_df = get_run_operation_log_data(start_interval, end_interval, market_list_table, update_navitaire_method, region, type, run_id)

        if not del_stack_df.empty:
            insert_deleted_run(start_time, run_id, start_interval, end_interval, market_list_table, username, type, update_navitaire_method, region, markets_count)
            logger.info(f"Skipping run: {run_id}")
            log_time.send_time_log_db()
            return {
                'statusCode': 200,
                'body': -1
            }

        insert_run_start(start_time, run_id, start_interval, end_interval, market_list_table,
                        username, type, "0", update_navitaire_method, region, markets_count)

        cache_client.set(KEY_B2B_PENDING_COUNT + run_id, int(markets_count), 86400)
        cache_client.set(KEY_B2C_PENDING_COUNT + run_id, int(markets_count), 86400)
        cache_client.set(KEY_B2B_READY_COUNT + run_id, 0, 86400)
        cache_client.set(KEY_B2C_READY_COUNT + run_id, 0, 86400)
        cache_client.set(KEY_B2B_DONE_COUNT + run_id, 0, 86400)
        cache_client.set(KEY_B2C_DONE_COUNT + run_id, 0, 86400)

        if type != "Adhoc":
            send_dataframe_to_sqs(mkt_list, queue_name, run_id,
                                update_navitaire_method, route_type, queue_name, cug_alloc, salesinv, mongodb)
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
                batch_df['cug_alloc'] = cug_alloc
                batch_df['mongodb'] = mongodb
                batch_df['saleinvflag'] = salesinv
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
                last_batch_df['cug_alloc'] = cug_alloc
                last_batch_df['mongodb'] = mongodb
                last_batch_df['saleinvflag'] = salesinv
                temp_name = f"adhoc_stack_{run_id}_batch_{batches_required[0] + 1}"
                data_to_serialize = last_batch_df.to_dict(orient='records')
                my_list_json = json.dumps(data_to_serialize, default=custom_serializer)
                logger.info(my_list_json)
                cache_client.set(temp_name, my_list_json, 86400)
                allocation_run_batch_audit(run_id, temp_name)

            update_run_with_batches(batches_required[0] + (1 if batches_required[1] > 0 else 0), run_id)
        
    elif salesinv == "1":
        sales_list = fetch_and_process_sales_market_list(sales_dtd_start, sales_dtd_end, sector_list)

        markets_count = sales_list.shape[0]
        logger.info(f"Total sales markets: {markets_count}")

        start_time = datetime.now()
        run_id = str(uuid4())
        log_time.update_run_id(run_id)

        insert_run_start(start_time, run_id, start_interval, end_interval, market_list_table,
                        username, type, is_connections_required, update_navitaire_method, region, markets_count)

        cache_client.set(KEY_SALES_PENDING_COUNT + run_id, int(markets_count), 86400)
        cache_client.set(KEY_SALES_READY_COUNT + run_id, 0, 86400)
        cache_client.set(KEY_SALES_DONE_COUNT + run_id, 0, 86400)
        
        send_dataframe_to_sqs(sales_list, queue_name, run_id,
                                update_navitaire_method, route_type, queue_name, cug_alloc, salesinv, mongodb)


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

if __name__ == "__main__":
    # For local testing; adjust event as needed
    # event = {
    #     "body": json.dumps({
    #         "start_interval": 0,
    #         "end_interval": 7,
    #         "market_list_table_name": "market_list",
    #         "run_type": "Scheduled",
    #         "cug_alloc":0
    #     })
    # }
    event = {
        "body": json.dumps({
            "sales_dtd_start": "2025-12-01",
            "sales_dtd_end": "2025-12-05",
            "sector_list": "[\"CCUAMD\", \"SXRBOM\", \"GOXPNQ\"]",
            "run_type": "Adhoc_Sales",
        })
    }
    context = {}
    result = lambda_handler(event, context)
    print(result)