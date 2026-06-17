import sys
import logging
from zoneinfo import ZoneInfo
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
from common_utils import instrument_engine
import tracemalloc
import gc

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

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def log_memory(label):
    import psutil, os
    process = psutil.Process(os.getpid())
    mb = process.memory_info().rss / 1024 / 1024
    logger.info(f"[MEM] {label}: {mb:.1f} MB")

# Then sprinkle throughout:
log_memory("after market list fetch")
log_memory("after booked_loads")
log_memory("after all merges")
log_memory("before SQS send")

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
        mongo_client = MongoClient(
            host=configsettings.MONGO_URL,
            port=27017,
            username=configsettings.MONGO_USERNAME,
            password=configsettings.MONGO_PASSWORD,
            authSource=configsettings.MONGO_AUTH_SOURCE,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=10000,
            socketTimeoutMS=10000,
            retryWrites=True,
            retryReads=True,
            maxPoolSize=50,
            minPoolSize=10
        )
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
    instrument_engine(engine)
    return engine

@retry_on_connection_error(
    max_retries=3,
    delay=2,
    backoff=2,
    exceptions=(PymssqlError,)
)
def create_navitaire_connection():
    """Create SQL Server connection with retry logic - FALLBACK IF NOT USING POOL"""
    connection_string = (
        f"mssql+pymssql://{configsettings.NAV_DB_USER}:"
        f"{quote_plus(configsettings.NAV_DB_PASSWORD)}@"
        f"{configsettings.NAV_DB_HOST}"
    )
    engine = create_engine(
        connection_string,
        connect_args={
            'timeout': 0,
            'login_timeout': 120
        }
    )
    instrument_engine(engine)
    logger.info("Navitaire SQL Server connection established")
    return engine

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
    mongo_master_db = mongo_client[configsettings.MONGO_MASTER_DB]
    
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
    
    def get_last_bookings_count(self, departure_start_date, departure_end_date):
        """Execute SP_LastBookingsCount stored procedure"""
        query = f"""
                EXEC {self.NAV_DB_WB_NAME}[dbo].[SP_LastBookingsCount]
                    @DepartureStartDate = N'{departure_start_date}',
                    @DepartureEndDate = N'{departure_end_date}'
                """
        df = self._execute_query_with_retry(query)
        df.columns = ['DepartureDate', 'FlightNumber', 'Origin', 'Destination', 'BookingToday']
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
        B2B_RBDS = ",".join(f"'{rbd.strip()}'" for rbd in constantsutils.B2B_RBD_VALUES.split(","))
        B2C_RBDS = ",".join(f"'{rbd.strip()}'" for rbd in constantsutils.B2C_RBD_VALUES.split(","))
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
                                CASE 
                                    WHEN ILC.ClassOfService IN ({B2B_RBDS}) THEN 'B2B'  
                                    WHEN ILC.ClassOfService IN ({B2C_RBDS}) THEN 'B2C'  
                                    ELSE 'OTHER'
                                END
                    ORDER BY ILC.ClassRank, ILC.ModifiedUTC DESC
                ) AS RowNum,
                CASE 
                    WHEN ILC.ClassOfService IN ({B2B_RBDS}) THEN 'B2B'
                    WHEN ILC.ClassOfService IN ({B2C_RBDS}) THEN 'B2C'
                    ELSE 'OTHER'
                END AS CustomerType
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
        
        current_date = datetime.now(ZoneInfo("Asia/Kolkata")).date()
        NavitaireQuery = f"""
            SELECT
                DepartureStation Origin,
                ArrivalStation Destination,
                TRIM(FlightNumber) as FlightNumber,
                DepartureDate,
                FORMAT(STD, 'HH:mm') as DepartureTime
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
        SELECT
            ROUND(fr.forecastPLF, 1) AS forecastPLF, ROUND(fr_ml.forecastPLF, 1) AS forecastPLF_ml, fr.Variance AS forecast_variance, ROUND(fr.AdjustedForecastPLF, 1) AS adjustedPLF, ROUND(fr_ml.AdjustedForecastPLF, 1) AS adjustedPLF_ml, fr.Origin, fr.Destination, fr.FlightNumber, fr.DepartureDate, fr.ALLIN_AF, fr.ForecastAF_Allin, fr.ALLIN_AF_NB_LM, fr.ALLIN_AF_NB_LY
        FROM RM_DW_DATA.forecast_results fr

        LEFT JOIN RM_DW_DATA.forecast_results_ml fr_ml
            ON fr.Origin = fr_ml.Origin AND fr.Destination = fr_ml.Destination AND fr.FlightNumber = fr_ml.FlightNumber AND fr.DepartureDate = fr_ml.DepartureDate
        WHERE fr.DepartureDate BETWEEN '{min_date}' AND '{max_date}'
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

def enrich_with_portfolio_siblings(mkt_list, start_period, end_period, region, route_type, salesinv, run_type, query_type):
    """
    For Adhoc/Scheduled_Thirty runs, fetches missing sibling flights from the main
    market list table so that portfolio aggregation is correct, but flags them with
    portfolio_only=1 so they don't produce downstream outputs.
    """
    if run_type not in ["Scheduled_Thirty", "Adhoc"] or route_type != "direct" or salesinv == "1":
        mkt_list['portfolio_only'] = 0
        return mkt_list

    if mkt_list.empty:
        mkt_list['portfolio_only'] = 0
        return mkt_list

    # 1. Main table logic based on region
    main_table = "market_list" if region == "Domestic" else "market_list_international"

    # 2. Extract unique (Origin, Destin, PerStart) pairs from adhoc mkt_list to filter the main table
    unique_pairs = mkt_list[['Origin', 'Destin', 'PerStart']].drop_duplicates()
    if unique_pairs.empty:
        mkt_list['portfolio_only'] = 0
        return mkt_list

    # Revise start_period and end_period based on actual dates present in mkt_list
    dates = pd.to_datetime(unique_pairs['PerStart'], format='%m/%d/%Y')
    actual_start = dates.min().strftime('%Y-%m-%d')
    actual_end = dates.max().strftime('%Y-%m-%d')

    # 3. Fetch from main table
    logger.info(f"Enriching portfolio with siblings from {main_table} for {len(unique_pairs)} origin/destin/date pairs ({actual_start} to {actual_end})")
    main_df = fetch_and_process_market_list(actual_start, actual_end, main_table, query_type)
    
    if main_df.empty:
        mkt_list['portfolio_only'] = 0
        return mkt_list

    # 4. Filter main_df to only those Origin-Destin-PerStart pairs that are in our adhoc list
    keys = ['Origin', 'Destin', 'PerStart']
    main_filtered = pd.merge(main_df, unique_pairs, on=keys, how='inner')

    # 5. Remove exact matches that already exist in the adhoc list
    adhoc_keys = mkt_list[['Origin', 'Destin', 'PerStart', 'FlightNumber']].copy()
    adhoc_keys['is_adhoc'] = 1

    main_joined = pd.merge(
        main_filtered, 
        adhoc_keys, 
        on=['Origin', 'Destin', 'PerStart', 'FlightNumber'], 
        how='left'
    )
    siblings = main_joined[main_joined['is_adhoc'].isnull()].drop(columns=['is_adhoc'])

    # 6. Flag and concat
    mkt_list['portfolio_only'] = 0
    if not siblings.empty:
        siblings['portfolio_only'] = 1
        mkt_list = pd.concat([mkt_list, siblings], ignore_index=True)
        logger.info(f"Added {len(siblings)} portfolio_only sibling flights")

    return mkt_list

def stamp_direct_route_portfolio_fields(mkt_list, route_type, salesinv):
    """Stamp ``pass`` and ``portfolio_count`` on direct-route messages (R13).

    For direct-route flights only (``route_type == "direct"`` and not a
    sales-inventory run), group the outbound messages by
    ``(Origin, Destin, PerStart)`` and set ``portfolio_count = len(group)``
    on every row of that group, plus ``pass = "anchor"``.  Connections and
    sales-inventory messages are returned unchanged so they keep today's
    body shape.

    Filtering (paused / deleted / missing-data flights) has already been
    applied to ``mkt_list`` upstream, so a group's size is exactly the
    number of flights that will be enqueued for that
    ``(sector, departure_date)`` — including single-flight portfolios,
    which still get ``portfolio_count == 1`` (R13.3).

    Returns ``(mkt_list, is_direct_route)``.
    """
    is_direct_route = (route_type == "direct" and salesinv != "1")
    if not is_direct_route or mkt_list.empty:
        return mkt_list, is_direct_route

    portfolio_sizes = (
        mkt_list.groupby(['Origin', 'Destin', 'PerStart']).size().to_dict()
    )
    # Build an object-dtype column of native python ints so the values stay
    # JSON-serialisable in both the scheduled (json.dumps without a default
    # serializer) and adhoc SQS paths.
    mkt_list['portfolio_count'] = pd.Series(
        [
            int(portfolio_sizes[(origin, destin, perstart)])
            for origin, destin, perstart in zip(
                mkt_list['Origin'], mkt_list['Destin'], mkt_list['PerStart']
            )
        ],
        index=mkt_list.index,
        dtype=object,
    )
    mkt_list['pass'] = 'anchor'
    return mkt_list, is_direct_route

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
    left_df['DepartureDate'] = pd.to_datetime(left_df['DepartureDate']).dt.normalize()
    right_df_copy['DepartureDate'] = pd.to_datetime(right_df_copy['DepartureDate']).dt.normalize()
    right_df_copy['FlightNumber'] = right_df_copy['FlightNumber'].astype('int64')
    return left_df.merge(
        right_df_copy,
        how='left',
        left_on=['DepartureDate', 'FlightNumber', 'Origin', 'Destin'],
        right_on=['DepartureDate', 'FlightNumber', 'Origin', 'Destination']
    ).drop(columns=['Destination'], errors='ignore')

def left_join_forecast(mkt_list, forecast_df, model_map):
    mkt = mkt_list.copy()
    fc = forecast_df.copy()
    model = model_map.copy()

    mkt['DepartureDate'] = pd.to_datetime(mkt['DepartureDate']).dt.normalize()
    fc['DepartureDate'] = pd.to_datetime(fc['DepartureDate']).dt.normalize()

    mkt['FlightNumber'] = mkt['FlightNumber'].astype('int64')
    fc['FlightNumber'] = fc['FlightNumber'].astype('int64')

    mkt = mkt.merge(
        model[['Origin', 'Destination', 'forecast_model', 'farecap']],
        how='left',
        left_on=['Origin', 'Destin'],
        right_on=['Origin', 'Destination'],
        suffixes=('', '_drop')
    )

    mkt = mkt.merge(
        fc,
        how='left',
        left_on=['DepartureDate', 'FlightNumber', 'Origin', 'Destin'],
        right_on=['DepartureDate', 'FlightNumber', 'Origin', 'Destination'],
        suffixes=('', '_drop')
    )

    mkt.loc[(mkt['forecast_model'] == 'ML') & (mkt['forecastPLF_ml'].notna()) & (mkt['forecastPLF_ml'].astype(str).str.strip() != ''), 'forecastPLF'] = mkt['forecastPLF_ml']

    mkt.loc[(mkt['forecast_model'] == 'ML') & (mkt['adjustedPLF_ml'].notna()) & (mkt['adjustedPLF_ml'].astype(str).str.strip() != ''), 'adjustedPLF'] = mkt['adjustedPLF_ml']

    mkt.drop(columns=['Destination', 'Destination_drop', 'forecastPLF_ml', 'adjustedPLF_ml', 'forecast_model'], inplace=True, errors='ignore')
    return mkt

def get_connection_departure_dates(nav_helper, min_date, max_date):
    """Fetch second flight departure dates using SP_GetNavitaireJourneys stored procedure.
    Includes both Connecting (2 segments) and Through (1 segment, 2 legs) flights."""
    query = f"""EXEC {nav_helper.NAV_DB_WB_NAME}[dbo].[SP_GetNavitaireJourneys]
                @StartDate = '{min_date}',
                @EndDate = '{max_date}'
            """
    conn_df = nav_helper._execute_query_with_retry(query)
    conn_df['FlightNumber1'] = conn_df['FlightNumber1'].astype(str).str.strip()
    conn_df['FlightNumber2'] = conn_df['FlightNumber2'].astype(str).str.strip()
    conn_df['Sector1'] = conn_df['Sector1'].astype(str).str.strip()
    conn_df['Sector2'] = conn_df['Sector2'].astype(str).str.strip()
    logger.info(f"Fetched {len(conn_df)} connection rows from ActiveJourneyView (Connecting + Through)")
    return conn_df


def quick_optimize(mkt_list, nav_helper):
    """Look up SecondFlightDepartureDate by joining market list against
    ActiveJourneyView query results instead of calling Navitaire API."""
    print(f"Optimizing {len(mkt_list)} rows...")

    min_date = mkt_list['DepartureDate'].min()
    max_date = mkt_list['DepartureDate'].max()

    conn_df = get_connection_departure_dates(nav_helper, min_date, max_date)

    # Prepare lookup: match on Flight1, Flight2, Sector1, Sector2, DepartureDate
    conn_df['Flight1'] = conn_df['FlightNumber1'].astype('int64')
    conn_df['Flight2'] = conn_df['FlightNumber2'].astype('int64')
    conn_df['DepartureDate'] = pd.to_datetime(conn_df['DepartureDate1']).dt.date
    conn_df['SecondFlightDepartureDate'] = pd.to_datetime(conn_df['DepartureDate2']).dt.date

    # Deduplicate — keep first match per key
    lookup = conn_df.drop_duplicates(subset=['Flight1', 'Flight2', 'Sector1', 'Sector2', 'DepartureDate'])
    lookup = lookup[['Flight1', 'Flight2', 'Sector1', 'Sector2', 'DepartureDate', 'SecondFlightDepartureDate']]

    mkt_list['Flight1'] = mkt_list['Flight1'].astype('int64')
    mkt_list['Flight2'] = mkt_list['Flight2'].astype('int64')

    mkt_list = mkt_list.merge(
        lookup,
        on=['Flight1', 'Flight2', 'Sector1', 'Sector2', 'DepartureDate'],
        how='left'
    )

    # Fill missing with empty string to match old API behavior
    mkt_list['SecondFlightDepartureDate'] = mkt_list['SecondFlightDepartureDate'].fillna('')

    print(f"Matched {(mkt_list['SecondFlightDepartureDate'] != '').sum()} / {len(mkt_list)} connections")
    return mkt_list


def create_flight_lookup_map(flights_df):
    flight_map = {}
    for _, flight in flights_df.iterrows():
        key = f"{flight['FlightNumber']}_{flight['Origin']}_{flight['Destination']}_{flight['DepartureDate']}"
        flight_map[key] = flight['DepartureTime']
    return flight_map

def filter_market_list(market_list_df, ndo0_map):
    current_datetime = datetime.now(ZoneInfo("Asia/Kolkata"))
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
                        logger.info(
                            f"[SKIP] key={key} | reason=departure_before_cutoff | "
                            f"departure_time={departure_time} <= cutoff_time={cutoff_time}"
                        )
                else:
                    skip_mask[idx] = True
                    logger.info(
                        f"[SKIP] key={key} | reason=key_not_found_in_ndo0_map | "
                        f"flight not present in today's Navitaire schedule"
                    )
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
        # Fetch using retry mechanism - filter by NDO date range overlap
        # Only fetch records where PerEnd >= start_period AND PerStart <= end_period
        # PerStart/PerEnd are stored as MM/DD/YYYY strings, so use $dateFromString for comparison
        market_list_filter = {
            '$expr': {
                '$and': [
                    {
                        '$gte': [
                            {'$dateFromString': {'dateString': '$PerEnd', 'format': '%m/%d/%Y'}},
                            {'$dateFromString': {'dateString': start_period, 'format': '%Y-%m-%d'}}
                        ]
                    },
                    {
                        '$lte': [
                            {'$dateFromString': {'dateString': '$PerStart', 'format': '%m/%d/%Y'}},
                            {'$dateFromString': {'dateString': end_period, 'format': '%Y-%m-%d'}}
                        ]
                    }
                ]
            }
        }
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
            'defaultMlFlag','b2bStrategy','Marketfare_Upsell','Outbound_Stop','forecastAllocFlag','Spoilage_Flag', 'CanAdjustedForecast', 'market_station_mapping','afForecastAllocFlag'
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
        update_doc = {
            '$setOnInsert': {
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
                'batch_completed': 0,
                'total_batch_size': 0
            },
            '$inc': {'marketListCount': count}
        }
        mongo_db['allocation_run_audit'].update_one(
            {'run_id': run_id},
            update_doc,
            upsert=True
        )
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
            {
                '$inc': {'total_batch_size': total_batches},
                '$set': {'adhoc_run_status': "IN-QUEUE"}
            }
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

def get_sector_forecast_model_map():
    projection = {"sector": 1, "forecast_model": 1, "farecap": 1, "_id": 0 }
    cursor = list(mongo_master_db['active_sectors'].find({}, projection))
    df = pd.DataFrame(cursor)

    if not df.empty:
        df[['Origin', 'Destination']] = df['sector'].str.extract(r'(.{3})(.{3})')

    return df

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

    adhoc_batch_size = constantsutils.ADHOC_BATCH_SIZE

    # SQS trigger from prefetchLambda vs direct API call (Adhoc)
    if 'Records' in event:
        # triggered by SQS from prefetchLambda
        body = event['Records'][0]['body']
        data = json.loads(json.loads(body)['body'])
    else:
        # direct API call (Adhoc) — unchanged
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
    if type not in ("Adhoc", "Equipment_Change"):
        is_connections_required = '1'
    if is_connections_required_key in data:
        is_connections_required = data[is_connections_required_key]
    if unique_key in data:
        unique_key_value = data[unique_key]
    if cug_alloc_key in data:
        cug_alloc = data[cug_alloc_key]
    if mongodb_key in data:
        mongodb = data[mongodb_key]
        
    if region == 'SalesInv':
        salesinv = "1"

    # Start program
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
        query_type = 'sales' if region == 'SalesInv' else 'standard'
        mkt_list = fetch_and_process_market_list(start_period, end_period, market_list_table, query_type)
        log_memory("after fetch_and_process_market_list")
        logger.info(f"mkt_list shape: {mkt_list.shape}")
        
        mkt_list = enrich_with_portfolio_siblings(mkt_list, start_period, end_period, region, route_type, salesinv, type, query_type)
        logger.info(f"mkt_list shape after enrichment: {mkt_list.shape}")

        nav_helper = NavDatabaseHelper(
            navconn=NavitaireConnection,
            nav_db_name=configsettings.NAV_DB_NAME,
            nav_db_wb_name=configsettings.NAV_DB_WB_NAME
        )

        w0_df = nav_helper.get_w0_rbd(start_period, end_period)
        log_memory("after get_w0_rbd")
        logger.info(f"w0_df shape: {w0_df.shape}")

        mkt_list, failCount = clean_markets_optimized(mkt_list, w0_df, nav_helper)
        log_memory("after clean_markets_optimized")
        logger.info(f"mkt_list after clean: {mkt_list.shape}")

        mkt_list = mkt_list.reset_index()
        markets_count = mkt_list.shape[0]
        logger.info(f"Total markets: {markets_count}")

        mkt_list['DepartureDate'] = pd.to_datetime(mkt_list['PerStart'], format='%m/%d/%Y').dt.date
        min_date = mkt_list['DepartureDate'].min()
        max_date = mkt_list['DepartureDate'].max()
        logger.info(f"Date range: {min_date} to {max_date}")

        ist = pytz.timezone('Asia/Kolkata')
        curr_date = datetime.now(ist).date()
        curr_datetime = datetime.now(ist) - timedelta(hours=1)
        curr_datetime_str = curr_datetime.strftime("%Y-%m-%d %H:%M:%S")

        booked_loads = nav_helper.getCurrentBookedLoads(startDate=min_date, endDate=max_date)
        log_memory("after getCurrentBookedLoads")
        logger.info(f"booked_loads shape: {booked_loads.shape}")

        seats_sold_pivot = nav_helper.get_seats_sold_by_class(min_date, max_date)
        log_memory("after get_seats_sold_by_class")
        logger.info(f"seats_sold_pivot shape: {seats_sold_pivot.shape}")

        rbd_pivot = nav_helper.get_current_rbds_by_class(min_date, max_date)
        log_memory("after get_current_rbds_by_class")
        logger.info(f"rbd_pivot shape: {rbd_pivot.shape}")

        series_seats_data = nav_helper.get_series_seats(min_date, max_date)
        log_memory("after get_series_seats")
        logger.info(f"series_seats_data shape: {series_seats_data.shape}")

        daily_flights = nav_helper.get_daily_flights(min_date, max_date)
        log_memory("after get_daily_flights")
        logger.info(f"daily_flights shape: {daily_flights.shape}")

        opening_rbds = nav_helper.get_opening_rbds(min_date, max_date)
        log_memory("after get_opening_rbds")
        logger.info(f"opening_rbds shape: {opening_rbds.shape}")

        bookings_count = nav_helper.get_last_bookings_count(
            departure_start_date=min_date,
            departure_end_date=max_date
        )
        log_memory("after get_last_bookings_count")
        logger.info(f"bookings_count shape: {bookings_count.shape}")

        forecast_plf_query = get_forecast(min_date, max_date)
        forecast_plf_list = execute_dw_query_with_retry(forecast_plf_query, dwconn)
        log_memory("after forecast query")
        logger.info(f"forecast_plf_list shape: {forecast_plf_list.shape}")

        sector_forecast_model_map = get_sector_forecast_model_map()
        log_memory("after get_sector_forecast_model_map")
        logger.info(f"sector_forecast_model_map shape: {sector_forecast_model_map.shape}")

        pre_filter_count = len(mkt_list)
        nav_flight_keys = set(
            zip(
                pd.to_datetime(booked_loads['DepartureDate']).dt.normalize(),
                booked_loads['FlightNumber'].astype('int64'),
                booked_loads['Origin'].str.strip(),
                booked_loads['Destination'].str.strip()
            )
        )
        mkt_list['_nav_key'] = list(zip(
            pd.to_datetime(mkt_list['DepartureDate']).dt.normalize() if not pd.api.types.is_datetime64_any_dtype(mkt_list['DepartureDate']) else pd.to_datetime(mkt_list['DepartureDate']),
            mkt_list['FlightNumber'].astype('int64'),
            mkt_list['Origin'].str.strip(),
            mkt_list['Destin'].str.strip()
        ))
        mkt_list = mkt_list[mkt_list['_nav_key'].isin(nav_flight_keys)].drop(columns=['_nav_key'])
        mkt_list = mkt_list.reset_index(drop=True)
        log_memory("after nav existence filter")
        logger.info(f"mkt_list after nav filter: {pre_filter_count} -> {len(mkt_list)} (removed {pre_filter_count - len(mkt_list)})")
        markets_count = mkt_list.shape[0]

        mkt_list = left_join_forecast(mkt_list, forecast_plf_list, sector_forecast_model_map)
        log_memory("after left_join_forecast")
        logger.info(f"mkt_list shape: {mkt_list.shape}")

        mkt_list = left_join_with_destination_mapping(mkt_list, booked_loads)
        log_memory("after merge booked_loads")
        logger.info(f"mkt_list shape: {mkt_list.shape}")

        mkt_list = left_join_with_destination_mapping(mkt_list, seats_sold_pivot)
        log_memory("after merge seats_sold_pivot")
        logger.info(f"mkt_list shape: {mkt_list.shape}")

        mkt_list = left_join_with_destination_mapping(mkt_list, rbd_pivot)
        log_memory("after merge rbd_pivot")
        logger.info(f"mkt_list shape: {mkt_list.shape}")

        mkt_list = left_join_with_destination_mapping(mkt_list, series_seats_data)
        log_memory("after merge series_seats_data")
        logger.info(f"mkt_list shape: {mkt_list.shape}")

        mkt_list = left_join_with_destination_mapping(mkt_list, daily_flights)
        log_memory("after merge daily_flights")
        logger.info(f"mkt_list shape: {mkt_list.shape}")

        mkt_list = left_join_with_destination_mapping(mkt_list, opening_rbds)
        log_memory("after merge opening_rbds")
        logger.info(f"mkt_list shape: {mkt_list.shape}")

        mkt_list = left_join_with_destination_mapping(mkt_list, bookings_count)
        mkt_list = mkt_list.drop_duplicates(subset=['Origin', 'Destin', 'FlightNumber', 'PerStart'])

        log_memory("after merge bookings_count")
        logger.info(f"FINAL mkt_list shape: {mkt_list.shape}")
        logger.info(f"FINAL mkt_list columns: {list(mkt_list.columns)}")
        
    elif route_type == "connections":
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
        
        mkt_list = quick_optimize(mkt_list, nav_helper)
        
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
        sector_forecast_model_map = get_sector_forecast_model_map()
        
        mkt_list['Sector1Origin'] = mkt_list['Sector1'].str[:3]
        mkt_list['Sector1Dest'] = mkt_list['Sector1'].str[3:]
        mkt_list['Sector2Origin'] = mkt_list['Sector2'].str[:3]
        mkt_list['Sector2Dest'] = mkt_list['Sector2'].str[3:]

        opening_rbds = nav_helper.get_opening_rbds(min_date, max_date)
        opening_rbds['FlightNumber'] = opening_rbds['FlightNumber'].astype('int64')

        booked_loads['FlightNumber'] = booked_loads['FlightNumber'].astype('int64')
        forecast_plf_list['FlightNumber'] = forecast_plf_list['FlightNumber'].astype('int64')
        forecast_plf_list['DepartureDate'] = pd.to_datetime(forecast_plf_list['DepartureDate']).dt.date
        
        # Merge operations for connections...
        mkt_list = mkt_list.merge(
            sector_forecast_model_map[['Origin', 'Destination', 'forecast_model']],
            how='left',
            left_on=['Sector1Origin', 'Sector1Dest'],
            right_on=['Origin', 'Destination']
        ).rename(columns={'forecast_model': 'forecast_model1'}) \
        .drop(columns=['Origin', 'Destination'], errors='ignore')

        mkt_list = mkt_list.merge(
            forecast_plf_list[['DepartureDate', 'FlightNumber', 'Origin', 'Destination', 'forecastPLF', 'forecastPLF_ml', 'adjustedPLF', 'adjustedPLF_ml']],
            how='left',
            left_on=['DepartureDate', 'Flight1', 'Sector1Origin', 'Sector1Dest'],
            right_on=['DepartureDate', 'FlightNumber', 'Origin', 'Destination'],
            suffixes=('', '_drop')
        ).rename(columns={'forecastPLF': 'forecastPLF1', 'forecastPLF_ml': 'forecastPLF_ml1', 'adjustedPLF': 'adjustedPLF1', 'adjustedPLF_ml': 'adjustedPLF_ml1'}).drop(
            columns=['Origin', 'Destination', 'FlightNumber', 'DepartureDate_drop'], errors='ignore')

        mkt_list.loc[(mkt_list['forecast_model1'] == 'ML') & (mkt_list['forecastPLF_ml1'].notna()) & (mkt_list['forecastPLF_ml1'].astype(str).str.strip() != ''),
            'forecastPLF1'
        ] = mkt_list['forecastPLF_ml1']

        mkt_list.loc[(mkt_list['forecast_model1'] == 'ML') & (mkt_list['adjustedPLF_ml1'].notna()) & (mkt_list['adjustedPLF_ml1'].astype(str).str.strip() != ''),
            'adjustedPLF1'
        ] = mkt_list['adjustedPLF_ml1']

        mkt_list = mkt_list.merge(
            sector_forecast_model_map[['Origin', 'Destination', 'forecast_model']],
            how='left',
            left_on=['Sector2Origin', 'Sector2Dest'],
            right_on=['Origin', 'Destination']
        ).rename(columns={'forecast_model': 'forecast_model2'}) \
        .drop(columns=['Origin', 'Destination'], errors='ignore')
        
        mkt_list = mkt_list.merge(
            forecast_plf_list[['DepartureDate', 'FlightNumber', 'Origin', 'Destination', 'forecastPLF', 'forecastPLF_ml', 'adjustedPLF', 'adjustedPLF_ml']],
            how='left',
            left_on=['SecondFlightDepartureDate', 'Flight2', 'Sector2Origin', 'Sector2Dest'],
            right_on=['DepartureDate', 'FlightNumber', 'Origin', 'Destination'],
            suffixes=('', '_drop')
        ).rename(columns={'forecastPLF': 'forecastPLF2', 'forecastPLF_ml': 'forecastPLF_ml2', 'adjustedPLF': 'adjustedPLF2', 'adjustedPLF_ml': 'adjustedPLF_ml2'}).drop(
            columns=['Origin', 'Destination', 'FlightNumber', 'DepartureDate_drop'], errors='ignore')
        
        mkt_list.loc[(mkt_list['forecast_model2'] == 'ML') & (mkt_list['forecastPLF_ml2'].notna()) & (mkt_list['forecastPLF_ml2'].astype(str).str.strip() != ''),
            'forecastPLF2'
        ] = mkt_list['forecastPLF_ml2']

        mkt_list.loc[(mkt_list['forecast_model2'] == 'ML') & (mkt_list['adjustedPLF_ml2'].notna()) & (mkt_list['adjustedPLF_ml2'].astype(str).str.strip() != ''),
            'adjustedPLF2'
        ] = mkt_list['adjustedPLF_ml2']

        mkt_list.drop(columns=['forecast_model1', 'forecastPLF_ml1', 'adjustedPLF_ml1', 'forecast_model2', 'forecastPLF_ml2', 'adjustedPLF_ml2'], inplace=True, errors='ignore')
        
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
            left_on=['SecondFlightDepartureDate', 'Flight2', 'Sector2Origin', 'Sector2Dest'],
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
        
        # --- Scenario 4: Direct Flight on Connection OD ---
        # For each connection, find direct flights on the same OD (Sector1Origin to Sector2Dest)
        # Only consider direct flights within +/- 6 hours of the connection's first flight departure time
        # and attach the best direct flight's forecast/PLF/fare data
        
        # Create connection OD columns
        mkt_list['ConnectionOrigin'] = mkt_list['Sector1Origin']
        mkt_list['ConnectionDest'] = mkt_list['Sector2Dest']
        
        # First, get the first flight's departure time for each connection
        # Merge booked_loads to get Flight1's departure time
        first_flight_times = booked_loads[['DepartureDate', 'FlightNumber', 'Origin', 'Destination', 'DepartureTime']].copy()
        first_flight_times = first_flight_times.rename(columns={
            'DepartureTime': 'FirstFlightDepTime'
        })
        
        # Merge to get first flight departure time for each connection
        mkt_list = mkt_list.merge(
            first_flight_times,
            how='left',
            left_on=['DepartureDate', 'Flight1', 'Sector1Origin', 'Sector1Dest'],
            right_on=['DepartureDate', 'FlightNumber', 'Origin', 'Destination'],
            suffixes=('', '_ff')
        ).drop(columns=['FlightNumber', 'Origin', 'Destination'], errors='ignore')
        
        # Filter booked_loads and forecast for direct flights on connection OD
        # We'll merge to get direct flight data for each connection
        
        # Get direct flight bookloads for connection OD
        direct_flight_bookloads = booked_loads[['DepartureDate', 'FlightNumber', 'Origin', 'Destination', 'bookedPax', 'capacity', 'DepartureTime']].copy()
        direct_flight_bookloads = direct_flight_bookloads.rename(columns={
            'FlightNumber': 'DirectFlightNumber',
            'bookedPax': 'DirectFlightBookedPax',
            'capacity': 'DirectFlightLid',
            'DepartureTime': 'DirectFlightDepTime'
        })
        
        # Get direct flight forecasts for connection OD
        direct_flight_forecasts = forecast_plf_list[['DepartureDate', 'FlightNumber', 'Origin', 'Destination', 'forecastPLF']].copy()
        direct_flight_forecasts = direct_flight_forecasts.rename(columns={
            'FlightNumber': 'DirectFlightNumber',
            'forecastPLF': 'DirectFlightForecast'
        })
        
        # Get direct flight opening RBDs for connection OD
        direct_flight_openings = opening_rbds[['DepartureDate', 'FlightNumber', 'Origin', 'Destination', 'OpeningRBDB2B', 'OpeningRBDB2C']].copy()
        direct_flight_openings = direct_flight_openings.rename(columns={
            'FlightNumber': 'DirectFlightNumber',
            'OpeningRBDB2B': 'DirectFlightOpeningRBDB2B',
            'OpeningRBDB2C': 'DirectFlightOpeningRBDB2C'
        })
        
        # Merge bookloads with forecasts
        direct_flights_data = direct_flight_bookloads.merge(
            direct_flight_forecasts,
            on=['DepartureDate', 'DirectFlightNumber', 'Origin', 'Destination'],
            how='left'
        )
        
        # Merge with opening RBDs
        direct_flights_data = direct_flights_data.merge(
            direct_flight_openings,
            on=['DepartureDate', 'DirectFlightNumber', 'Origin', 'Destination'],
            how='left'
        )
        
        # Calculate PLF for direct flights
        direct_flights_data['DirectFlightPLF'] = (
            direct_flights_data['DirectFlightBookedPax'] / direct_flights_data['DirectFlightLid'] * 100
        ).fillna(0)
        
        direct_flights_data['DirectFlightForecast'] = direct_flights_data['DirectFlightForecast'].fillna(0)
        
        # Now, for each connection, filter direct flights to only those within +/- 6 hours
        # of the connection's first flight departure time
        # We need to do this by merging and then filtering
        
        # First, merge all direct flights to each connection (cross-join on OD and Date)
        # Then filter by time window
        
        # Create a temporary key for merging
        mkt_list['merge_key'] = mkt_list['DepartureDate'].astype(str) + '_' + mkt_list['ConnectionOrigin'] + '_' + mkt_list['ConnectionDest']
        direct_flights_data['merge_key'] = direct_flights_data['DepartureDate'].astype(str) + '_' + direct_flights_data['Origin'] + '_' + direct_flights_data['Destination']
        
        # Helper function to filter by time window (+/- 6 hours)
        def filter_direct_flights_by_time(connection_group):
            first_flight_time_str = connection_group['FirstFlightDepTime'].iloc[0]
            merge_key = connection_group['merge_key'].iloc[0]
            
            if pd.isna(first_flight_time_str) or first_flight_time_str == '':
                # No first flight time available, return empty
                return pd.DataFrame()
            
            # Get all direct flights for this OD and date
            matching_direct = direct_flights_data[direct_flights_data['merge_key'] == merge_key].copy()
            
            if matching_direct.empty:
                return pd.DataFrame()
            
            try:
                # Parse first flight time (format: HH:mm)
                first_flight_minutes = int(first_flight_time_str.split(':')[0]) * 60 + int(first_flight_time_str.split(':')[1])
                
                # Filter direct flights within +/- 6 hours (360 minutes)
                def is_within_time_window(dep_time_str):
                    if pd.isna(dep_time_str) or dep_time_str == '':
                        return False
                    try:
                        direct_minutes = int(dep_time_str.split(':')[0]) * 60 + int(dep_time_str.split(':')[1])
                        time_diff = abs(direct_minutes - first_flight_minutes)
                        # Handle wrap-around at midnight
                        time_diff = min(time_diff, 1440 - time_diff)
                        return time_diff <= 360  # 6 hours = 360 minutes
                    except:
                        return False
                
                matching_direct = matching_direct[matching_direct['DirectFlightDepTime'].apply(is_within_time_window)]
                
                if matching_direct.empty:
                    return pd.DataFrame()
                
                # Return the direct flight with highest forecast
                best_direct = matching_direct.sort_values(by='DirectFlightForecast', ascending=False).iloc[0]
                return pd.DataFrame([best_direct])
            except Exception as e:
                logger.warning(f"Error filtering direct flights by time: {str(e)}")
                return pd.DataFrame()
        
        # Group connections by merge_key and filter direct flights
        best_direct_flights_list = []
        for merge_key, group in mkt_list.groupby('merge_key'):
            result = filter_direct_flights_by_time(group)
            if not result.empty:
                best_direct_flights_list.append(result)
        
        if best_direct_flights_list:
            best_direct_flights = pd.concat(best_direct_flights_list, ignore_index=True)
            best_direct_flights = best_direct_flights.drop_duplicates(subset=['merge_key'], keep='first')
            
            # Merge direct flight data to connection market list
            mkt_list = mkt_list.merge(
                best_direct_flights[[
                    'merge_key',
                    'DirectFlightNumber', 'DirectFlightBookedPax', 'DirectFlightLid',
                    'DirectFlightForecast', 'DirectFlightPLF', 'DirectFlightDepTime',
                    'DirectFlightOpeningRBDB2B', 'DirectFlightOpeningRBDB2C'
                ]],
                how='left',
                on='merge_key',
                suffixes=('', '_direct')
            )
        else:
            # No direct flights found within time window, add empty columns
            mkt_list['DirectFlightNumber'] = None
            mkt_list['DirectFlightBookedPax'] = None
            mkt_list['DirectFlightLid'] = None
            mkt_list['DirectFlightForecast'] = None
            mkt_list['DirectFlightPLF'] = None
            mkt_list['DirectFlightDepTime'] = None
            mkt_list['DirectFlightOpeningRBDB2B'] = None
            mkt_list['DirectFlightOpeningRBDB2C'] = None
        
        # Drop helper columns
        mkt_list = mkt_list.drop(columns=['ConnectionOrigin', 'ConnectionDest', 'merge_key'], errors='ignore')
        
        logger.info(f"Added direct flight data for {mkt_list['DirectFlightNumber'].notna().sum()} connections (filtered within +/-6 hours of first flight time)")
        
        markets_count = mkt_list.shape[0]
        mkt_list['SecondFlightDepartureDate'] = mkt_list['SecondFlightDepartureDate'].astype(str)
        mkt_list = mkt_list.drop(columns=['DepartureDate'], errors='ignore')
        
    mkt_list = mkt_list.drop(columns=['DepartureDate'], errors='ignore')

    if 'portfolio_only' in mkt_list.columns:
        actual_markets_count = len(mkt_list[mkt_list['portfolio_only'] == 0])
        markets_count = len(mkt_list)
    else:
        actual_markets_count = mkt_list.shape[0]
        markets_count = mkt_list.shape[0]

    # Portfolio coordination (R13): for direct-route flights only, group the
    # outbound messages by (Origin, Destin, PerStart) and stamp each message
    # with portfolio_count = len(group) and pass = "anchor".  Connections and
    # sales-inventory messages keep today's body shape.
    mkt_list, is_direct_route = stamp_direct_route_portfolio_fields(
        mkt_list, route_type, salesinv
    )
    if is_direct_route and not mkt_list.empty:
        logger.info(
            f"Stamped pass=anchor + portfolio_count on {len(mkt_list)} "
            f"direct-route messages"
        )

    # Proceed with SQS
    start_time = datetime.now()
    run_id = data.get('run_id') or str(uuid4())
    overall_start = data.get('overall_start', start_interval)
    overall_end = data.get('overall_end', end_interval)

    log_time.update_run_id(run_id)

    del_stack_df = get_run_operation_log_data(overall_start, overall_end, market_list_table, update_navitaire_method, region, type, run_id)

    if not del_stack_df.empty:
        insert_deleted_run(start_time, run_id, overall_start, overall_end, market_list_table, username, type, update_navitaire_method, region, actual_markets_count)
        logger.info(f"Skipping run: {run_id}")
        log_time.send_time_log_db()
        return {
            'statusCode': 200,
            'body': -1
        }
        
    logger.info(f"Total markets: {markets_count}, Actual Markets: {actual_markets_count})")
    
    insert_run_start(start_time, run_id, overall_start, overall_end, market_list_table,
                     username, type, is_connections_required, update_navitaire_method, region, actual_markets_count)

    cache_client.incrby(KEY_B2B_PENDING_COUNT + run_id, int(actual_markets_count))
    cache_client.expire(KEY_B2B_PENDING_COUNT + run_id, 86400)
    
    cache_client.incrby(KEY_B2C_PENDING_COUNT + run_id, int(actual_markets_count))
    cache_client.expire(KEY_B2C_PENDING_COUNT + run_id, 86400)
    
    cache_client.setnx(KEY_B2B_READY_COUNT + run_id, 0)
    cache_client.expire(KEY_B2B_READY_COUNT + run_id, 86400)
    
    cache_client.setnx(KEY_B2C_READY_COUNT + run_id, 0)
    cache_client.expire(KEY_B2C_READY_COUNT + run_id, 86400)
    
    cache_client.setnx(KEY_B2B_DONE_COUNT + run_id, 0)
    cache_client.expire(KEY_B2B_DONE_COUNT + run_id, 86400)
    
    cache_client.setnx(KEY_B2C_DONE_COUNT + run_id, 0)
    cache_client.expire(KEY_B2C_DONE_COUNT + run_id, 86400)

    if type != "Adhoc":
        send_dataframe_to_sqs(mkt_list, queue_name, run_id,
                              update_navitaire_method, route_type, queue_name, cug_alloc, salesinv, mongodb)
    else:
        batches_required = number_of_batches_required(markets_count, adhoc_batch_size)
        total_number_of_batches = batches_required[0] + (1 if batches_required[1] > 0 else 0)
        
        if total_number_of_batches > 0:
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
                
                # Atomically get sequential batch id across concurrent fetch-inputs invocations
                counter_key = f"adhoc_batch_counter_{run_id}"
                batch_id = cache_client.incr(counter_key)
                cache_client.expire(counter_key, 86400)
                
                temp_name = f"adhoc_stack_{run_id}_batch_{batch_id}"
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
                
                counter_key = f"adhoc_batch_counter_{run_id}"
                batch_id = cache_client.incr(counter_key)
                cache_client.expire(counter_key, 86400)
                
                temp_name = f"adhoc_stack_{run_id}_batch_{batch_id}"
                data_to_serialize = last_batch_df.to_dict(orient='records')
                my_list_json = json.dumps(data_to_serialize, default=custom_serializer)
                logger.info(my_list_json)
                cache_client.set(temp_name, my_list_json, 86400)
                allocation_run_batch_audit(run_id, temp_name)

            update_run_with_batches(total_number_of_batches, run_id)

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

# if __name__ == "__main__":
#     # For local testing; adjust event as needed
#     event = {
#         "body": json.dumps({
#             "start_interval": 0,
#             "end_interval": 7,
#             "market_list_table_name": "market_list",
#             "run_type": "Scheduled",
#             "cug_alloc":0
#         })
#     }
#     context = {}
#     result = lambda_handler(event, context)
#     print(result)