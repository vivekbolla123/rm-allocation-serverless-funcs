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
from sqlalchemy import create_engine, text
from pandas import read_sql, Timestamp
from datetime import datetime, timedelta, date, timezone
from redis import Redis
from constants_utils import ConstantsUtils
from concurrent.futures import ThreadPoolExecutor
from time_logger import TimeLogger
from nav_api_helper import NavAPIHelper

warnings.filterwarnings('ignore')

# Global variables and setup
run_log = []
sqs = boto3.client('sqs')

# Database connection strings
DB_RD_CONN_STRING = f"mysql+mysqldb://{configsettings.RM_DB_APPUSER_USERNAME_READ}:{configsettings.RM_DB_APPUSER_PASSWORD_READ}@{configsettings.RM_DB_APPUSER_URL_READ}/{configsettings.RM_DB_SCHEMA_NAME}"
DB_WR_CONN_STRING = f"mysql+mysqldb://{configsettings.RM_DB_APPUSER_USERNAME}:{configsettings.RM_DB_APPUSER_PASSWORD}@{configsettings.RM_DB_APPUSER_URL}/{configsettings.RM_DB_SCHEMA_NAME}"
USERNAME = "SYS2"

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

# Database and cache connections
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
if  cache_client.ping():
    logger.info("Connected to Redis")

constantsutils = ConstantsUtils(conn)
log_time = TimeLogger(wrconn, constantsutils.CHECK_THRESHOLD_FETCH_INPUTS)

# NavDatabaseHelper class


class NavDatabaseHelper:
    def __init__(self, navconn, nav_db_name, nav_db_wb_name):
        self.NavitaireConnection = navconn
        self.NAV_DB_NAME = nav_db_name
        self.NAV_DB_WB_NAME = nav_db_wb_name
        self.logger = logging.getLogger()

    def getCurrentBookedLoads(self, startDate, endDate):

        NavitaireQuery = f""" EXEC {self.NAV_DB_WB_NAME}[dbo].[SP_Bookeddata]
                                @DepartureDate_from = N'{str(startDate)}',
                                @DepartureDate_to = N'{str(endDate)}'
                            """
        bookedData = pd.read_sql(NavitaireQuery, self.NavitaireConnection)
        return bookedData
    
    def get_last_bookings_count(self, departure_start_date, departure_end_date, comparison_date, comparison_datetime):
        """
        Execute SP_LastBookingsCount2 stored procedure to get booking counts
        """
        query = f"""
                EXEC {self.NAV_DB_WB_NAME}[dbo].[SP_LastBookingsCount]
                    @DepartureStartDate = N'{departure_start_date}',
                    @DepartureEndDate = N'{departure_end_date}',
                    @ComparisonDate = N'{comparison_date}',
                    @ComparisonDateTime = N'{comparison_datetime}'
                """
        df = pd.read_sql(query, self.NavitaireConnection)
        print("get_last_bookings_count")
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
        df = pd.read_sql(query, self.NavitaireConnection)
        print("get_seats_sold_by_class")
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
        df = pd.read_sql(query, self.NavitaireConnection)
        print("get_current_rbds_by_class")
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
        df = pd.read_sql(query, self.NavitaireConnection)
        print("get_series_seats")
        df.columns = ['Origin', 'Destination', 'FlightNumber',
                      'DepartureDate', 'total_series_seats']
        return df

    def get_daily_flights(self, start_date, end_date):
        """
        Execute SP_DailyFlights2 stored procedure to get daily flight information
        """
        query = f"""EXEC {self.NAV_DB_WB_NAME}[dbo].[SP_DailyFlights]
                    @StartDate = N'{str(start_date)}',
                    @EndDate = N'{str(end_date)}'
                 """
        df = pd.read_sql(query, self.NavitaireConnection)
        print("get_daily_flights")
        df.columns = ['Origin', 'Destination', 'FlightNumber', 
                     'DepartureDate', 'NextFlight']
        return df
        
    def get_opening_rbds(self, min_date, max_date):
        """
        Get opening RBDs for both B2B and B2C customers
        """
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
        df = pd.read_sql(query, self.NavitaireConnection)
        print("get_opening_rbds")
        return df

# Utility functions


def number_of_batches_required(total_records, adhoc_batch_size):
    if total_records < adhoc_batch_size:
        return [1, 0]
    if total_records % adhoc_batch_size != 0:
        return [total_records // adhoc_batch_size, total_records % adhoc_batch_size]
    else:
        return [total_records // adhoc_batch_size, 0]


def add_additional_fields(record, run_id, update_navitaire_method, start_time, route_type, queue_name,cug_alloc,saleinvflag):
    record["runId"] = run_id
    record["update_navitaire_method"] = update_navitaire_method
    record["runTime"] = start_time
    record["route_type"] = route_type
    record["queue"] = queue_name
    record['cug_alloc'] = cug_alloc
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


def send_dataframe_to_sqs(df, queue_url, run_id, update_navitaire_method, route_type, queue_name,cug_alloc,salesinv, batch_size=10, max_workers=5):
    records = df.to_dict('records')
    batches = [records[i:i+batch_size]
               for i in range(0, len(records), batch_size)]
    start_time = datetime.now().isoformat()
    additional_fields = {
        'run_id': run_id,
        'update_navitaire_method': update_navitaire_method,
        'start_time': start_time,
        'route_type': route_type,
        'queue_name': queue_name,
        'cug_alloc':cug_alloc,
        'saleinvflag':salesinv
    }
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(lambda batch: send_batch_to_sqs(
            batch, queue_url, additional_fields), batches)

# Lambda handler
def left_join_with_destination_mapping(left_df, right_df):
    # Create a copy of right_df to avoid modifying the original
    right_df_copy = right_df.copy()
    
    # Convert FlightNumber in right_df to int64 to match left_df
    right_df_copy['FlightNumber'] = right_df_copy['FlightNumber'].astype('int64')
    return left_df.merge(
        right_df_copy,
        how='left',
        left_on=['DepartureDate', 'FlightNumber', 'Origin', 'Destin'],
        right_on=['DepartureDate', 'FlightNumber', 'Origin', 'Destination']
    ).drop(columns=['Destination'], errors='ignore')
    
def quick_optimize(mkt_list,navapihelper):
    print(f"Optimizing {len(mkt_list)} rows...")
    
    # Create unique parameter combinations only
    unique_params = mkt_list[['Flight1', 'Flight2', 'Sector1', 'Sector2', 'DepartureDate']].drop_duplicates()
    print(f"Reduced to {len(unique_params)} unique API calls")
    
    # Parallel API calls for unique combinations
    def api_call(row):
        return navapihelper.get_second_flight_dep(
            row['Flight1'], row['Flight2'], 
            row['Sector1'][:3], row['Sector2'][3:],
            row['DepartureDate'].strftime('%Y-%m-%d')
        )
    
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=100) as executor:
        results = list(executor.map(api_call, [row for _, row in unique_params.iterrows()]))
    
    # Map results back
    unique_params['api_result'] = results
    mkt_list = mkt_list.merge(unique_params, on=['Flight1', 'Flight2', 'Sector1', 'Sector2', 'DepartureDate'])
    mkt_list['SecondFlightDepartureDate'] = mkt_list['api_result']
    
    return mkt_list.drop('api_result', axis=1)
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
    if not type == "Adhoc":
        is_connections_required = '1'
    if is_connections_required_key in data:
        is_connections_required = data[is_connections_required_key]
    if unique_key in data:
        unique_key_value = data[unique_key]
    if cug_alloc_key in data:
        cug_alloc = data[cug_alloc_key]
        
    if region=='SalesInv':
        salesinv="1"

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
    startTime = current_utc_time.replace(
        tzinfo=utc_timezone).astimezone(ist_timezone)
    curr_date = startTime.date()
    start_period = datetime.strftime(
        curr_date + timedelta(days=start_interval), "%Y-%m-%d")
    end_period = datetime.strftime(
        curr_date + timedelta(days=end_interval), "%Y-%m-%d")
    
    # Fetch market list
    if route_type == "direct":
        market_list_query = getMarketListQuery(start_period, end_period, market_list_table)
        w0_df= get_w0_rbd(start_period,end_period)
        mkt_list = read_sql(market_list_query, conn)
        logger.info(f"Original Market List {mkt_list}")
        mkt_list, failCount = clean_markets_optimized(mkt_list,w0_df)
        logger.info(f"Filtered Market List {mkt_list}")
        mkt_list = mkt_list.reset_index()
        markets_count = mkt_list.shape[0]
        logger.info(f"Total markets: {markets_count}")

        # Initialize NavDatabaseHelper
        nav_helper = NavDatabaseHelper(
            navconn=NavitaireConnection,
            nav_db_name=configsettings.NAV_DB_NAME,
            nav_db_wb_name=configsettings.NAV_DB_WB_NAME
        )

        # Convert PerStart to datetime for joining
        mkt_list['DepartureDate'] = pd.to_datetime(
            mkt_list['PerStart'], format='%m/%d/%Y').dt.date
        min_date = mkt_list['DepartureDate'].min()
        max_date = mkt_list['DepartureDate'].max()
        curr_date = datetime.now(timezone.utc).date()
        curr_datetime = datetime.now(timezone.utc) - timedelta(hours=1)
        curr_datetime_str = curr_datetime.strftime("%Y-%m-%d %H:%M:%S")
        # Fetch Navitaire data
        booked_loads = nav_helper.getCurrentBookedLoads(
            startDate=min_date, endDate=max_date)
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

        mkt_list = left_join_with_destination_mapping(mkt_list, booked_loads)
        print(len(mkt_list)," booked_loads")
        mkt_list = left_join_with_destination_mapping(mkt_list, seats_sold_pivot)
        print(len(mkt_list)," seats_sold_pivot")
        mkt_list = left_join_with_destination_mapping(mkt_list, rbd_pivot)
        print(len(mkt_list)," rbd_pivot")
        mkt_list = left_join_with_destination_mapping(mkt_list, series_seats_data)
        print(len(mkt_list)," series_seats_data")
        mkt_list = left_join_with_destination_mapping(mkt_list, daily_flights)
        print(len(mkt_list)," daily_flights")
        mkt_list = left_join_with_destination_mapping(mkt_list, opening_rbds)
        print(len(mkt_list)," opening_rbds")
        mkt_list = left_join_with_destination_mapping(mkt_list, bookings_count)
        print(len(mkt_list)," bookings_count")
    elif route_type == "connections":
        navapihelper = NavAPIHelper(configsettings.NAV_BASE_URL,configsettings.NAV_OCP_SUBSCRIPTION_KEY,configsettings.NAV_USERNAME,configsettings.NAV_PWD,configsettings.NAV_DOMAIN)
        update_navitaire_method = "sftp"
        is_connections_required = "0"
        region = 'Domestic'
        market_list_query = getConnectionMarketListQuery(
            start_period, end_period, market_list_table)
        mkt_list = read_sql(market_list_query, conn)
        nav_helper = NavDatabaseHelper(
            navconn=NavitaireConnection,
            nav_db_name=configsettings.NAV_DB_NAME,
            nav_db_wb_name=configsettings.NAV_DB_WB_NAME
        )

        # Convert PerStart to datetime for joining
        mkt_list['DepartureDate'] = pd.to_datetime(
            mkt_list['PerStart'], format='%m/%d/%Y').dt.date
        min_date = mkt_list['DepartureDate'].min()
        max_date = mkt_list['DepartureDate'].max()
        
        # Add logic to get second flight departure dates
        mkt_list = quick_optimize(mkt_list,navapihelper)
        
        # Filter out rows where SecondFlightDepartureDate is empty
        original_count = len(mkt_list)
        mkt_list = mkt_list[mkt_list['SecondFlightDepartureDate'] != ""]
        filtered_count = len(mkt_list)
        logger.info(f"Filtered connections: {original_count} -> {filtered_count} (removed {original_count - filtered_count} invalid connections)")
        
        # Convert SecondFlightDepartureDate to datetime and update min/max dates if needed
        if not mkt_list.empty:
            mkt_list['SecondFlightDepartureDate'] = pd.to_datetime(mkt_list['SecondFlightDepartureDate']).dt.date
            # Update date range to include second flight dates
            all_dates = pd.concat([mkt_list['DepartureDate'], mkt_list['SecondFlightDepartureDate']])
            min_date = all_dates.min()
            max_date = all_dates.max()
        
        booked_loads = nav_helper.getCurrentBookedLoads(
            startDate=min_date, endDate=max_date)
        mkt_list['Sector1Origin'] = mkt_list['Sector1'].str[:3]
        mkt_list['Sector1Dest'] = mkt_list['Sector1'].str[3:]
        mkt_list['Sector2Origin'] = mkt_list['Sector2'].str[:3]
        mkt_list['Sector2Dest'] = mkt_list['Sector2'].str[3:]

        # Get opening RBDs for B2B and B2C
        opening_rbds = nav_helper.get_opening_rbds(min_date, max_date)
        opening_rbds['FlightNumber'] = opening_rbds['FlightNumber'].astype('int64')

        # Prepare booked loads for mapping
        booked_loads['FlightNumber'] = booked_loads['FlightNumber'].astype('int64')
        
        # Map BookedPax for Sector1 (using original DepartureDate)
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
        ).rename(columns={'capacity': 'Capacity1'}).drop(
            columns=['Origin', 'Destination', 'FlightNumber', 'DepartureDate_drop'], errors='ignore')

        # Map BookedPax for Sector2 (using SecondFlightDepartureDate)
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
        ).rename(columns={'capacity': 'Capacity2'}).drop(
            columns=['Origin', 'Destination', 'FlightNumber', 'DepartureDate_drop'], errors='ignore')

        # Map OpeningRBDB2B and OpeningRBDB2C for Sector1 (using original DepartureDate)
        mkt_list = mkt_list.merge(
            opening_rbds[['DepartureDate', 'FlightNumber', 'Origin', 'Destination', 'OpeningRBDB2B', 'OpeningRBDB2C']],
            how='left',
            left_on=['DepartureDate', 'Flight1', 'Sector1Origin', 'Sector1Dest'],
            right_on=['DepartureDate', 'FlightNumber', 'Origin', 'Destination'],
            suffixes=('', '_drop')
        ).rename(columns={'OpeningRBDB2B': 'OpeningRBDB2B1', 'OpeningRBDB2C': 'OpeningRBDB2C1'}).drop(
            columns=['Origin', 'Destination', 'FlightNumber', 'DepartureDate_drop'], errors='ignore')

        # Map OpeningRBDB2B and OpeningRBDB2C for Sector2 (using SecondFlightDepartureDate)
        mkt_list = mkt_list.merge(
            opening_rbds[['DepartureDate', 'FlightNumber', 'Origin', 'Destination', 'OpeningRBDB2B', 'OpeningRBDB2C']],
            how='left',
            left_on=['SecondFlightDepartureDate', 'Flight2', 'Sector2Origin', 'Sector2Dest'],
            right_on=['DepartureDate', 'FlightNumber', 'Origin', 'Destination'],
            suffixes=('', '_drop')
        ).rename(columns={'OpeningRBDB2B': 'OpeningRBDB2B2', 'OpeningRBDB2C': 'OpeningRBDB2C2'}).drop(
            columns=['Origin', 'Destination', 'FlightNumber', 'DepartureDate_drop'], errors='ignore')
        # mkt_list = mkt_list.rename(columns=lambda x: 'DepartureDate' if x.startswith('DepartureDate_') else x)
        # mkt_list = mkt_list.loc[:, ~mkt_list.columns.duplicated()]            
        markets_count = mkt_list.shape[0]
        mkt_list['SecondFlightDepartureDate'] = mkt_list['SecondFlightDepartureDate'].astype(str)
        mkt_list = mkt_list.drop(
            columns=['DepartureDate'], errors='ignore')
    mkt_list = mkt_list.drop(
            columns=['DepartureDate'], errors='ignore')
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
                     username, type, is_connections_required, update_navitaire_method, region, markets_count)

    cache_client.set(KEY_B2B_PENDING_COUNT + run_id, int(markets_count), 86400)
    cache_client.set(KEY_B2C_PENDING_COUNT + run_id, int(markets_count), 86400)
    cache_client.set(KEY_B2B_READY_COUNT + run_id, 0, 86400)
    cache_client.set(KEY_B2C_READY_COUNT + run_id, 0, 86400)
    cache_client.set(KEY_B2B_DONE_COUNT + run_id, 0, 86400)
    cache_client.set(KEY_B2C_DONE_COUNT + run_id, 0, 86400)

    if type != "Adhoc":
        send_dataframe_to_sqs(mkt_list, queue_name, run_id,
                              update_navitaire_method, route_type, queue_name,cug_alloc,salesinv)
    else:
        batches_required = number_of_batches_required(
            markets_count, adhoc_batch_size)
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
            batch_df['saleinvflag']=salesinv
            temp_name = f"adhoc_stack_{run_id}_batch_{batch_num + 1}"
            data_to_serialize = batch_df.to_dict(orient='records')
            my_list_json = json.dumps(
                data_to_serialize, default=custom_serializer)
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
            batch_df['saleinvflag']=salesinv
            temp_name = f"adhoc_stack_{run_id}_batch_{batches_required[0] + 1}"
            data_to_serialize = last_batch_df.to_dict(orient='records')
            my_list_json = json.dumps(
                data_to_serialize, default=custom_serializer)
            logger.info(my_list_json)
            cache_client.set(temp_name, my_list_json, 86400)
            allocation_run_batch_audit(run_id, temp_name)

        update_run_with_batches(
            batches_required[0] + (1 if batches_required[1] > 0 else 0), run_id)

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

# Helper functions


def custom_serializer(obj):
    if isinstance(obj, Timestamp):
        return str(obj.isoformat())
    return str(obj)


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
            table_versions[currVersion["tableName"]
                           ] = currVersion["curr_version"]

    insert_query = "INSERT INTO allocation_run_audit (run_id, start_time, user, dtd_start, dtd_end, Curves, d1_d2_strategies, market_list, Fares, type, market_list_name, is_connections_required, update_navitaire_method, is_run_completed, region, marketListCount) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    try:
        wrconn.execute(insert_query,
                       (run_id, start_time.strftime('%Y-%m-%d %H:%M:%S'), username, start_interval, end_interval, table_versions['Curves'], table_versions['d1_d2_strategies'], table_versions['market_list'], table_versions['Fares'], type, table_versions['market_list_name'], is_connections_required, update_navitaire_method, '0', region, count))
    except Exception as e:
        logger.info(e)
    finally:
        log_time.end("insert_run_start")
    logger.info("STARTED ALLOCATION RUN")

def get_run_operation_log_data(start_interval, end_interval, market_list_table, update_navitaire_method, region, type, run_id):
    try:
        current_time = datetime.now(timezone.utc)
        time_window = timedelta(minutes=15) # time window (±15 mins)
        start_time = current_time - time_window
        end_time = current_time + time_window

        read_query = f"""
            SELECT * FROM QP_DW_RMALLOC.allocation_run_operation_log
            WHERE
                run_timestamp BETWEEN '{start_time.strftime('%Y-%m-%d %H:%M:%S')}' AND '{end_time.strftime('%Y-%m-%d %H:%M:%S')}'
                AND dtd = '{start_interval}-{end_interval}'
                AND marketlist_name = '{market_list_table}'
                AND region = '{region}'
                AND `type` = '{type}'
                AND operation = 'DELETED'
                AND run_id IS NULL
        """
        df = pd.read_sql(read_query, conn)

        uodate_query = "UPDATE QP_DW_RMALLOC.allocation_run_operation_log SET run_id = %s WHERE operation_id=%s"
        if not df.empty:
            wrconn.execute(uodate_query, (run_id, df.iloc[0]['operation_id']))
        
        return df

    except Exception as e:
        logger.error(f"Error fetching run_operation_log data: {str(e)}")
        raise

def insert_deleted_run(start_time, run_id, start_interval, end_interval, market_list_table, username, type, update_navitaire_method, region, count):
    log_time.start()
    currVersionQuery = "SELECT tableName, curr_version FROM currentVersion"
    currVersionValues = read_sql(currVersionQuery, conn)

    is_connections_required = 1 if type == 'Scheduled' else 0
    is_connections_started = 1 if type == 'Scheduled' else None

    table_versions = {}
    for index, currVersion in currVersionValues.iterrows():
        if market_list_table == currVersion["tableName"]:
            table_versions["market_list"] = currVersion["curr_version"]
            table_versions["market_list_name"] = currVersion["tableName"]
        else:
            table_versions[currVersion["tableName"]] = currVersion["curr_version"]

    insert_query = "INSERT INTO allocation_run_audit (run_id, start_time, end_time, actual_end_time, user, dtd_start, dtd_end, Curves, d1_d2_strategies, market_list, Fares, type, market_list_name, is_connections_required, is_connections_started, update_navitaire_method, is_run_completed, region, marketListCount, run_status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    try:
        wrconn.execute(insert_query,
                    (run_id, start_time.strftime('%Y-%m-%d %H:%M:%S'), start_time.strftime('%Y-%m-%d %H:%M:%S'), start_time.strftime('%Y-%m-%d %H:%M:%S'), username, start_interval, end_interval, table_versions['Curves'], table_versions['d1_d2_strategies'], table_versions['market_list'], table_versions['Fares'], type, table_versions['market_list_name'], is_connections_required, is_connections_started, update_navitaire_method, '0', region, count, 'DELETED'))
    except Exception as e:
        logger.info(e)
    finally:
        log_time.end("insert_deleted_run")

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
        wrconn.execute(
            update_query, (end_time.strftime('%Y-%m-%d %H:%M:%S'), run_id))
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
    
    select_query = text("SELECT available FROM adhoc_run_queue")
    count_available_slot = conn.execute(select_query).fetchone()[0]
    logger.info(f"Slot Available: {count_available_slot}")
  
    if count_available_slot == 0:
        logger.info("No Slot Available!")
        return False
    
    new_available_slot = count_available_slot - 1
    update_query = text("UPDATE adhoc_run_queue SET available = :new_available_slot")
    try:
        wrconn.execute(update_query, {"new_available_slot": new_available_slot})
    except Exception as e:
        logger.info(e)
    logger.info(f"Slot Available after Adhoc run started: {new_available_slot}")
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
                B2BBackstop,B2CBackstop,B2BFactor,obSeats,obFare,SkippingFactor,DaySpan,AutoTimeRangeFlag,analystName,openingFares,OverBooking,ProfileFares,rbdPushFlag,
                B2BTolerance,B2CTolerance,distressInventoryFlag,seriesBlock,AutoGroup,TbfFlag,CugFlag,MktFareFlag,Autobackstopflag,
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
                Sector1,Flight1,Sector2,Flight2,Outbound_stop,PerType,DOW,Price_Strategy,Discount_Value,carrierExclusionB2C,carrierExclusionB2B,FirstRBDAlloc,OtherRBDAlloc,
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


def get_w0_rbd(start_date, end_date):
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
    logger.info(
        f"skipping time delta: {str(constantsutils.TIME_BEFORE_DEPARTURE)}")
    cutoff_time = (current_datetime +
                   timedelta(minutes=constantsutils.TIME_BEFORE_DEPARTURE)).strftime("%H:%M")
    current_date = current_datetime.strftime("%Y-%m-%d")

    logger.info(
        f"Filtering markets. Current date: {current_date}, Cutoff time: {cutoff_time}")

    keep_mask = pd.Series(False, index=market_list_df.index)
    skip_mask = pd.Series(False, index=market_list_df.index)

    for idx, market in market_list_df.iterrows():
        try:
            origin = market.get("Origin")
            destination = market.get("Destin")
            flight_number = market.get("FlightNumber")
            market_date = datetime.strptime(market.get(
                "PerStart"), "%m/%d/%Y").strftime("%Y-%m-%d")

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


def clean_markets_optimized(market_list_df, w0_df):
    try:
        current_flights_df = getFltSectDeptTimeForCurrentDay()
        if current_flights_df.empty:
            logger.info("No current day flights found in Navitaire")
            return market_list_df, 0

        ndo0_map = create_flight_lookup_map(current_flights_df)
        if not ndo0_map:
            logger.info("Empty flight lookup map created")
            return market_list_df, 0

        filtered_markets_df, skipped_markets_df = filter_market_list(
            market_list_df, ndo0_map)
        w0_df["DepartureDate"] = pd.to_datetime(w0_df["DepartureDate"])
        filtered_markets_df['PerStart'] = pd.to_datetime(
            filtered_markets_df['PerStart'])
        w0_df['flightnumber'] = w0_df['flightnumber'].astype('int64')
        filtered_markets_df['FlightNumber'] = filtered_markets_df['FlightNumber'].astype(
            'int64')
        merged_df = pd.merge(
            filtered_markets_df,
            w0_df,
            left_on=['PerStart', 'Origin', 'Destin', 'FlightNumber'],
            right_on=['DepartureDate', 'departurestation',
                      'arrivalstation', 'flightnumber'],
            how='left'
        )

        merged_df = merged_df.rename(columns={'classau': 'w0_value'})
        merged_df['PerStart'] = merged_df['PerStart'].dt.strftime('%m/%d/%Y')
        merged_df.drop(columns=['DepartureDate', 'flightnumber',
                       'departurestation', 'arrivalstation', 'classofservice'], inplace=True)
        return merged_df, len(skipped_markets_df)

    except Exception as e:
        logger.error(f"Error in clean_markets_optimized: {str(e)}")
        return market_list_df, 0


# if __name__ == "__main__":
#     # For local testing; adjust event as needed
#     event = {
#         "body": json.dumps({
#             "start_interval": 0,
#             "end_interval": 2,
#             "market_list_table_name": "market_list",
#             "route_type": "direct",
#             "run_type": "Scheduled",
#             "cug_alloc":0
#         })
#     }
#     context = {}
#     result = lambda_handler(event, context)
#     print(result)
