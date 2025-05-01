from datetime import datetime,timedelta
import json
import pandas as pd
import pytz
from ownlogger import OwnLogger
from Constants import *
from utilities.time_utils import TimeUtils 
from helper.redis_helper import RedisHelper
from utilities.table_cache_utils import TableCache
from sqlalchemy import text

class RMDatabaseHelper:
    def __init__(self,conn,wrconn,dwconn,runId,inputSource,debugmode,cache_client,currParams):
        self.conn=conn
        self.wrconn=wrconn
        self.runId=runId
        self.dwconn = dwconn
        self.logger= OwnLogger(runId, inputSource)
        self.currParams=currParams
        self.inputSource=inputSource
        self.debugMode=debugmode
        self.timeutils=TimeUtils(inputSource)
        self.redishelper=RedisHelper(self.runId,cache_client,inputSource)
        # Use TableCache for caching entire tables
        self.table_cache = TableCache(self.logger)
        # Preload frequently used tables
        self.preload_common_tables()

    def preload_common_tables(self):
        """Preload commonly used tables to avoid multiple queries"""
        try:
            # Get list of existing tables first
            table_query = "SHOW TABLES"
            try:
                tables_df = pd.read_sql(table_query, self.conn)
                existing_tables = set(tables_df.iloc[:, 0].tolist())
                self.logger.info(f"Found {len(existing_tables)} tables in database")
            except Exception as e:
                self.logger.warning(f"Could not get list of tables: {e}")
                existing_tables = set()  # Empty set if we can't get the tables
            
            # Load common reference tables that are unlikely to change
            common_tables = ["ndo_bands", "dplf_bands", "time_ranges"]
            for table in common_tables:
                if table in existing_tables:
                    self.table_cache.load_table(table, self.conn)
                    self.logger.info(f"Loaded {table} into cache")
                else:
                    self.logger.warning(f"Table {table} does not exist, skipping preload")
            
            # Additional tables to load if they exist
            tables_to_check = [
                "config_profile_fares", 
                "tbf_discount_grid", 
                "series_blocking", 
                "config_ownFares", 
                "config_determine_fares"
            ]
            
            for table in tables_to_check:
                if table in existing_tables:
                    self.table_cache.load_table(table, self.conn)
                    self.logger.info(f"Loaded {table} into cache")
                else:
                    self.logger.warning(f"Table {table} does not exist, skipping preload")
            
            # Try to load currency_conversion from DW connection
            try:
                self.table_cache.load_table("currency_conversion", self.dwconn)
                self.logger.info("Loaded currency_conversion into cache")
            except Exception as e:
                self.logger.warning(f"Could not load currency_conversion table: {e}")
            
            # Load frequently accessed tables if they're not too large and they exist
            if self.currParams and hasattr(self.currParams, 'flight_number'):
                flight_number = self.currParams.flight_number
                
                # Check if current_rbd table exists
                if "current_rbd" in existing_tables:
                    try:
                        self.table_cache.load_table("current_rbd", self.conn, 
                                                  condition=f"FlightNumber = '{flight_number}'")
                        self.logger.info(f"Loaded current_rbd for flight {flight_number} into cache")
                    except Exception as e:
                        self.logger.warning(f"Error loading current_rbd table: {e}")
                else:
                    self.logger.warning("Table current_rbd does not exist, skipping preload")
                
                # Check if series_booked table exists
                if "series_booked" in existing_tables:
                    try:
                        self.table_cache.load_table("series_booked", self.conn, 
                                                  condition=f"FlightNumber = '{flight_number}'")
                        self.logger.info(f"Loaded series_booked for flight {flight_number} into cache")
                    except Exception as e:
                        self.logger.warning(f"Error loading series_booked table: {e}")
                else:
                    self.logger.warning("Table series_booked does not exist, skipping preload")
                
                # If origin and destination are available, load fare data if available
                if hasattr(self.currParams, 'origin') and hasattr(self.currParams, 'destin'):
                    origin = self.currParams.origin
                    destin = self.currParams.destin
                    
                    # Check if Fares table exists
                    if "Fares" in existing_tables:
                        try:
                            self.table_cache.load_table("Fares", self.conn, 
                                                    condition=f"Org = '{origin}' AND Dst = '{destin}'")
                            self.logger.info(f"Loaded Fares for {origin}-{destin} into cache")
                        except Exception as e:
                            self.logger.warning(f"Error loading Fares table: {e}")
                    else:
                        self.logger.warning("Table Fares does not exist, skipping preload")
                    
                    # Check if booking_fare_data table exists
                    if "booking_fare_data" in existing_tables:
                        try:
                            self.table_cache.load_table("booking_fare_data", self.conn,
                                                    condition=f"Origin = '{origin}' AND Destin = '{destin}' AND FlightNumber = '{flight_number}'")
                            self.logger.info(f"Loaded booking_fare_data for {origin}-{destin}-{flight_number} into cache")
                        except Exception as e:
                            self.logger.warning(f"Error loading booking_fare_data table: {e}")
                    else:
                        self.logger.warning("Table booking_fare_data does not exist, skipping preload")
            
            # Log statistics about loaded tables
            stats = self.table_cache.get_cache_stats()
            self.logger.info(f"Preloaded {stats['total_tables']} tables with {stats['total_rows']} total rows")
        except Exception as e:
            self.logger.error(f"Error preloading tables: {e}")
            # Don't let preloading errors crash the application

    def getNdoBand(self, ndo):
        """Get NDO band using cached table rather than executing a query"""
        try:
            # Filter the cached ndo_bands table
            ndo_bands_df = self.table_cache.filter_table("ndo_bands")
            if ndo_bands_df is None:
                # If not cached, load it
                ndo_bands_df = self.table_cache.load_table("ndo_bands", self.conn)
                
            # Filter to find the band that contains this NDO value
            result = ndo_bands_df[(ndo_bands_df['start'] <= ndo) & (ndo_bands_df['end'] >= ndo)]
            if len(result) > 0:
                return result['ndo_band'].iloc[0]
            else:
                return 5  # Default value if no matching band is found
        except Exception as e:
            self.logger.error(f"Error in getNdoBand: {e}")
            return 5

    def getdplfBand(self, dPLF):
        """Get DPLF band using cached table rather than executing a query"""
        try:
            # Filter the cached dplf_bands table
            dplf_bands_df = self.table_cache.filter_table("dplf_bands")
            if dplf_bands_df is None:
                # If not cached, load it
                dplf_bands_df = self.table_cache.load_table("dplf_bands", self.conn)
                
            # Filter to find the band that contains this DPLF value
            result = dplf_bands_df[(dplf_bands_df['start'] <= dPLF) & (dplf_bands_df['end'] >= dPLF)]
            if len(result) > 0:
                return result['dplf_band'].iloc[0]
            else:
                return 3  # Default value if no matching band is found
        except Exception as e:
            self.logger.error(f"Error in getdplfBand: {e}")
            return 3

    def getTimeRange(self, time_range):
        """Get time range using cached table rather than executing a query"""
        try:
            # Get the cached time_ranges table
            time_ranges_df = self.table_cache.filter_table("time_ranges") 
            if time_ranges_df is None:
                # If not cached, load it
                time_ranges_df = self.table_cache.load_table("time_ranges", self.conn)
            
            # Filter for the specific time_range
            result = time_ranges_df[time_ranges_df['time_range'] == time_range]
            return result
        except Exception as e:
            self.logger.error(f"Error in getTimeRange: {e}")
            return pd.DataFrame()

    def fetch_all_conversion_data(self):
        """Get currency conversion data using cached table rather than executing a query"""
        try:
            # Get the cached currency_conversion table
            conversion_df = self.table_cache.filter_table("currency_conversion")
            if conversion_df is None:
                # If not cached, load it
                conversion_df = self.table_cache.load_table("currency_conversion", self.dwconn)
            
            return conversion_df
        except Exception as e:
            self.logger.error(f"Error in fetch_all_conversion_data: {e}")
            return pd.DataFrame()

    def get_current_rbds_by_class(self, departure_date, flight_number, sector):
        """Get current RBDs by class using cached table and filtering"""
        try:
            # Try to get from cache first
            current_rbd_df = self.table_cache.filter_table("current_rbd")
            if current_rbd_df is None:
                # Load relevant subset if not cached (scope by flight number to keep it manageable)
                current_rbd_df = self.table_cache.load_table("current_rbd", self.conn,
                                                          condition=f"FlightNumber = '{flight_number}'")
            
            # Filter the cached data
            filtered_df = current_rbd_df[
                (current_rbd_df['DepartureDate'] == departure_date) &
                (current_rbd_df['FlightNumber'] == str(flight_number)) &
                (current_rbd_df['Origin'] + current_rbd_df['Destin'] == str(sector)) &
                ((current_rbd_df['ClassOfService'] == 'P') | (current_rbd_df['ClassOfService'] == 'L'))
            ]
            
            if filtered_df.empty:
                return -1, -1
                
            # Extract values for L and P classes
            l_class_df = filtered_df[filtered_df['ClassOfService'] == 'L']
            p_class_df = filtered_df[filtered_df['ClassOfService'] == 'P']
            
            l_class_rbd = l_class_df['ClassAu'].values[0] if len(l_class_df) > 0 else -1
            p_class_rbd = p_class_df['ClassAu'].values[0] if len(p_class_df) > 0 else -1
            
            return l_class_rbd, p_class_rbd
        except Exception as e:
            self.logger.error(f"Error in get_current_rbds_by_class: {e}")
            return -1, -1

    def get_series_seats(self, departure_date, flight_number, sector):
        """Get series seats using cached table and filtering"""
        try:
            # Try to get from cache first
            series_booked_df = self.table_cache.filter_table("series_booked")
            if series_booked_df is None:
                # If not already cached, load the table or a subset of it
                series_booked_df = self.table_cache.load_table("series_booked", self.conn,
                                                            condition=f"FlightNumber = '{flight_number}'")
            
            # Filter the cached data
            filtered_df = series_booked_df[
                (series_booked_df['DepartureDate'] == departure_date) &
                (series_booked_df['FlightNumber'] == flight_number) &
                (series_booked_df['Origin'] + series_booked_df['Destination'] == sector)
            ]
            
            return filtered_df
        except Exception as e:
            self.logger.error(f"Error in get_series_seats: {e}")
            return pd.DataFrame()

    def getSeriesBlockedSeats(self, departure_date):
        """Get series blocked seats using cached table and filtering"""
        try:
            # Try to get from cache first
            series_blocking_df = self.table_cache.filter_table("series_blocking")
            if series_blocking_df is None:
                # If not already cached, load the table
                series_blocking_df = self.table_cache.load_table("series_blocking", self.conn)
            
            # Filter the cached data
            filtered_df = series_blocking_df[series_blocking_df['DepartureDate'] == departure_date]
            
            # Aggregate the results
            result = pd.DataFrame({'blocked': [filtered_df['seatsblocked'].sum()]})
            return result
        except Exception as e:
            self.logger.error(f"Error in getSeriesBlockedSeats: {e}")
            return pd.DataFrame({'blocked': [0]})

    def get_tbf_discount(self, month, sector):
        """Get TBF discount using cached table and filtering"""
        try:
            # Try to get from cache first
            tbf_discount_df = self.table_cache.filter_table("tbf_discount_grid")
            if tbf_discount_df is None:
                # If not already cached, load the table
                tbf_discount_df = self.table_cache.load_table("tbf_discount_grid", self.conn)
            
            # Filter for either the specific sector/month or the wildcard values
            filtered_df = tbf_discount_df[
                ((tbf_discount_df['Sector'] == sector) | (tbf_discount_df['Sector'] == '*')) &
                ((tbf_discount_df['Month'] == month) | (tbf_discount_df['Month'] == '*'))
            ]
            
            # Sort to prioritize specific values over wildcards
            sorted_df = filtered_df.assign(
                sector_priority=filtered_df['Sector'].apply(lambda x: 0 if x == sector else 1),
                month_priority=filtered_df['Month'].apply(lambda x: 0 if x == month else 1)
            ).sort_values(['sector_priority', 'month_priority'])
            
            # Return the first row (highest priority)
            return sorted_df[['TF', 'TE', 'TD', 'TC', 'TB', 'TY']].head(1)
        except Exception as e:
            self.logger.error(f"Error in get_tbf_discount: {e}")
            return pd.DataFrame()

    def execute_cached_query(self, query, connection, query_key=None):
        """
        Execute a query and cache the result using the TableCache.
        Wrapper method to maintain compatibility with existing code.
        """
        if query is None or connection is None:
            self.logger.warning("Query or connection is None, cannot execute query")
            return pd.DataFrame()
            
        try:
            # Extract table name from the query for table cache
            query_lower = query.lower()
            table_name = None
            
            if "select" in query_lower and "from" in query_lower:
                # Simple parsing to get the table name - not always accurate for complex queries
                from_idx = query_lower.index("from") + 4
                table_parts = query_lower[from_idx:].strip().split()
                if table_parts:
                    table_name = table_parts[0].strip().rstrip(";")
                    # Clean up any schema prefixes or aliases
                    if '.' in table_name:
                        table_name = table_name.split('.')[-1]
                    if ' ' in table_name:
                        table_name = table_name.split(' ')[0]
                    
                    # Check if we can use the table cache for this query
                    if hasattr(self, 'table_cache') and self.table_cache is not None:
                        # If the table is already cached, we can try to filter it
                        cached_table = self.table_cache.filter_table(table_name)
                        if cached_table is not None:
                            # Table is already cached, but we can't easily transform the SQL to pandas filtering
                            # For simple queries we could implement this, but for now just execute the query
                            self.logger.debug(f"Table {table_name} is cached, but using direct query execution")
            
            # Fall back to direct query execution
            self.logger.debug(f"Executing query: {query[:100]}...")  # Log first 100 chars of query
            return pd.read_sql(query, connection)
        except Exception as e:
            self.logger.error(f"Error executing query: {e}, query: {query[:100]}...")
            # Return empty DataFrame rather than raising exception
            return pd.DataFrame()

    def getLatestCodeVersion(self):
        """Get the latest code version from config_code_version table"""
        try:
            # First check if the table exists
            table_query = "SHOW TABLES LIKE 'config_code_version'"
            tables_df = pd.read_sql(table_query, self.conn)
            
            if tables_df.empty:
                self.logger.warning("Table config_code_version does not exist")
                return "UNKNOWN"
                
            # If table exists, proceed with the query
            lastestQuery = "SELECT code_version FROM config_code_version WHERE isActive = 1"
            inputsData = self.execute_cached_query(lastestQuery, self.conn, "latest_code_version")
            
            if inputsData is not None and not inputsData.empty:
                return inputsData['code_version'].iloc[0]
            else:
                self.logger.warning("No active code version found")
                return "UNKNOWN"
        except Exception as e:
            self.logger.error(f"Error fetching latest code version: {e}")
            return "UNKNOWN"  # Changed from "ERROR" to "UNKNOWN" for consistency

    def loadRunParams(self):
        # load the list of sectors

        if self.inputSource.input_source[CONST_MARKET_LIST] == FILE:
            if self.currParams.route_type=="direct":
                self.mktList = pd.read_csv(CONST_MARKET_LIST_FILE_PATH)
            else:
                self.mktList = pd.read_csv(CONST_MARKET_LIST_CONNECTIONS_FILE_PATH)
        else:
            if self.currParams.route_type=="direct":
                marketListQuery = """SELECT 
    ml.*,
    bfd.BookingsCount,
    bfd.BookingToday,
    bfd.BookingsInHour,
    bl.bookedPax,
    bl.DepartureTime,
    bl.Lid AS capacity,
    bl.adjustedCapacity,
    bl.tbf_bookings,
    SUM(CASE WHEN cr.ClassOfService = 'L' THEN cr.ClassSold ELSE 0 END) AS seats_sold_L,
    SUM(CASE WHEN cr.ClassOfService = 'P' THEN cr.ClassSold ELSE 0 END) AS seats_sold_P,
    SUM(CASE WHEN cr.ClassOfService = 'L' THEN cr.ClassAu ELSE 0 END) AS current_rbd_L,
    SUM(CASE WHEN cr.ClassOfService = 'P' THEN cr.ClassAu ELSE 0 END) AS current_rbd_P,
    SUM(sb.bookedPax) AS total_series_seats,
    IFNULL((
        SELECT 1
        FROM Bookload bl_next
        WHERE bl_next.Origin = bl.Origin
        AND bl_next.Destination = bl.Destination
        AND bl_next.DepartureDate = bl.DepartureDate
        AND bl_next.DepartureTime > bl.DepartureTime
        LIMIT 1
    ), 0) AS NextFlight
FROM 
    market_list ml
LEFT JOIN 
    booking_fare_data bfd ON ml.Origin = bfd.Origin 
    AND ml.Destin = bfd.Destin 
    AND ml.FlightNumber = bfd.FlightNumber
    AND STR_TO_DATE(ml.PerStart, '%%m/%%d/%%Y') = bfd.FlightDate
LEFT JOIN 
    Bookload bl ON ml.Origin = bl.Origin 
    AND ml.Destin = bl.Destination 
    AND ml.FlightNumber = bl.FlightNumber
    AND STR_TO_DATE(ml.PerStart, '%%m/%%d/%%Y') = bl.DepartureDate
LEFT JOIN 
    current_rbd cr ON ml.Origin = cr.Origin 
    AND ml.Destin = cr.Destin 
    AND ml.FlightNumber = cr.FlightNumber
    AND STR_TO_DATE(ml.PerStart, '%%m/%%d/%%Y') = cr.DepartureDate
LEFT JOIN 
    series_booked sb ON ml.Origin = sb.Origin 
    AND ml.Destin = sb.Destination 
    AND ml.FlightNumber = sb.FlightNumber
    AND STR_TO_DATE(ml.PerStart, '%%m/%%d/%%Y') = sb.DepartureDate
GROUP BY
    ml.Origin,
    ml.Destin,
    ml.FlightNumber,
    ml.PerType,
    ml.PerStart,
    ml.PerEnd,
    ml.DOW,
    ml.plfthreshold,
    ml.hardAnchor,
    ml.TimeWindowStart,
    ml.TimeWindowEnd,
    ml.CurveID,
    ml.CarrExlusionB2C,
    ml.CarrExlusionB2B,
    ml.flightExclusionB2C,
    ml.flightExclusionB2B,
    ml.fareAnchor,
    ml.fareOffset,
    ml.FirstRBDAlloc,
    ml.OtherRBDAlloc,
    ml.B2BBackstop,
    ml.B2CBackstop,
    ml.B2BFactor,
    ml.obSeats,
    ml.obFare,
    ml.SkippingFactor,
    ml.analystName,
    ml.DaySpan,
    ml.AutoTimeRangeFlag,
    ml.openingFares,
    ml.OverBooking,
    ml.profileFares,
    ml.rbdPushFlag,
    ml.B2BTolerance,
    ml.B2CTolerance,
    ml.distressInventoryFlag,
    ml.seriesBlock,
    ml.autoGroup,
    ml.tbfFlag,
    ml.autoBackstopFlag,
    bfd.BookingsCount,
    bfd.BookingToday, 
    bfd.BookingsInHour,
    bl.bookedPax,
    bl.DepartureTime,
    bl.DepartureDate,
    bl.Lid,
    bl.adjustedCapacity,
    bl.tbf_bookings"""
            else:
                marketListQuery = """SELECT 
                mlc.*,
                bl1.bookedPax AS BookedPax1,
                bl1.Lid AS Capacity1,
                bl2.bookedPax AS BookedPax2,
                bl2.Lid AS Capacity2,
                crb1_b2b.OpeningRBD AS OpeningRBDB2B1,
                crb1_b2c.OpeningRBD AS OpeningRBDB2C1,
                crb2_b2b.OpeningRBD AS OpeningRBDB2B2,
                crb2_b2c.OpeningRBD AS OpeningRBDB2C2
            FROM 
                market_list_connections mlc
            LEFT JOIN 
                Bookload bl1 ON 
                LEFT(mlc.Sector1, 3) = bl1.Origin AND
                RIGHT(mlc.Sector1, 3) = bl1.Destination AND
                mlc.Flight1 = bl1.FlightNumber AND
                STR_TO_DATE(mlc.PerStart, '%%m/%%d/%%Y') = bl1.DepartureDate
            LEFT JOIN 
                Bookload bl2 ON 
                LEFT(mlc.Sector2, 3) = bl2.Origin AND
                RIGHT(mlc.Sector2, 3) = bl2.Destination AND
                mlc.Flight2 = bl2.FlightNumber AND
                STR_TO_DATE(mlc.PerStart, '%%m/%%d/%%Y') = bl2.DepartureDate
            LEFT JOIN 
                current_opening_rbd crb1_b2b ON 
                LEFT(mlc.Sector1, 3) = crb1_b2b.Origin AND
                RIGHT(mlc.Sector1, 3) = crb1_b2b.Destination AND
                mlc.Flight1 = crb1_b2b.flightno AND
                STR_TO_DATE(mlc.PerStart, '%%m/%%d/%%Y') = crb1_b2b.DepartureDate AND
                crb1_b2b.Channel = 'B2B'
            LEFT JOIN 
                current_opening_rbd crb1_b2c ON 
                LEFT(mlc.Sector1, 3) = crb1_b2c.Origin AND
                RIGHT(mlc.Sector1, 3) = crb1_b2c.Destination AND
                mlc.Flight1 = crb1_b2c.flightno AND
                STR_TO_DATE(mlc.PerStart, '%%m/%%d/%%Y') = crb1_b2c.DepartureDate AND
                crb1_b2c.Channel = 'B2C'
            LEFT JOIN 
                current_opening_rbd crb2_b2b ON 
                LEFT(mlc.Sector2, 3) = crb2_b2b.Origin AND
                RIGHT(mlc.Sector2, 3) = crb2_b2b.Destination AND
                mlc.Flight2 = crb2_b2b.flightno AND
                STR_TO_DATE(mlc.PerStart, '%%m/%%d/%%Y') = crb2_b2b.DepartureDate AND
                crb2_b2b.Channel = 'B2B'
            LEFT JOIN 
                current_opening_rbd crb2_b2c ON 
                LEFT(mlc.Sector2, 3) = crb2_b2c.Origin AND
                RIGHT(mlc.Sector2, 3) = crb2_b2c.Destination AND
                mlc.Flight2 = crb2_b2c.flightno AND
                STR_TO_DATE(mlc.PerStart, '%%m/%%d/%%Y') = crb2_b2c.DepartureDate AND
                crb2_b2c.Channel = 'B2C'
                """
            try:
                cache_key = f"market_list_{self.currParams.route_type}"
                self.mktList = self.execute_cached_query(marketListQuery, self.conn, cache_key)
            except Exception as e:
                self.logger.error(e)

        if(self.debugMode):
            self.logger.info("Loaded Market List")
            self.logger.info(self.mktList)
        return self.mktList

    def insertRunStart(self, startTime, username):
        if self.inputSource.logs[CONST_RUN_AUDIT] == DB:
            currVersionQuery = "SELECT tableName, curr_version FROM currentVersion"
            try:
                currVersionValues = pd.read_sql(currVersionQuery, self.conn)
            except Exception as e:
                self.logger.error(e)

            table_versions = {}
            for index,currVersion in currVersionValues.iterrows():
                table_versions[currVersion['tableName']] = currVersion['curr_version']

            type = "Local Run"

            insert_query = " INSERT INTO allocation_run_audit (run_id, start_time, user, dtd_start, dtd_end, Curves, d1_d2_strategies, market_list, Fares, type, is_sftp_pushed, is_connections_required, update_navitaire_method) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            try:
                self.wrconn.execute(insert_query,
                            (self.runId, startTime.strftime(DATE_TIME_FORMAT), username, 0, 365, table_versions['Curves'], table_versions['d1_d2_strategies'], type, table_versions['Fares'], type, '0', '0', 'file'))
            except Exception as e:
                self.logger.error(e)

    def updateRunEnd(self, endTime):
        if self.inputSource.logs[CONST_RUN_AUDIT] == DB:
            updateQuery = " UPDATE allocation_run_audit SET end_time = %s WHERE run_id = %s "
            try:
                self.wrconn.execute(updateQuery, (endTime.strftime(DATE_TIME_FORMAT), self.runId))
            except Exception as e:
                self.logger.error(e)

    def areAllInputsReady(self):
        if self.inputSource.input_source[CONST_INPUT_STATUS] == DB:
            countQuery = f"""select count(*) as busyInputs from inputs_status 
            WHERE is_running = b'1' 
            AND name not like 'market_list%%' """
            try:
                inputsData = pd.read_sql(countQuery, self.conn)
            except Exception as e:
                self.logger.error(e)
            return inputsData['busyInputs'][0] == 0

    def insertSummaryRow(self, allocResults):
        if self.inputSource.logs[CONST_RUN_SUMMARY] == DB:
            latest_row = allocResults[-1]

            insertQuery = text(""" 
                INSERT INTO run_summary 
                (Id, Origin, Destin, FltNum, DepDate, BookedLoad, TgtLoad, MktFare_Min, OpenRBD, SellingFare, Channel, HowDetermined, RunId, CreatedAt, MktFare_Max, fareAnchor, fareOffset, TimeRange, dlfBand, ndoBand, criteria, AirlineCode_Max, AirlineCode_Min, FlightNumber_Max, FlightNumber_Min, backstop, autoBackstop, Variance, StrategyReference, analystName, bookedPlf, actualbookedPlf, startTime, endTime, allocationStatus, endOfDaydlfBand, endOfDayTgtLoad, strategyFare, lastSellingFare, lastSevenAvgFare, hardAnchorFare, upsell, bookingToday, bookingInHour, currentTgtLoad, currentLoadFactor, overBookingCount, recommendedActions, profileFare, status, w0Flag)  
                VALUES (
                    :Id, :Origin, :Destin, :FltNum, :DepDate, :BookedLoad, :TgtLoad, :MktFare_Min, :OpenRBD, :SellingFare, :Channel, :HowDetermined, :RunId, :CreatedAt, :MktFare_Max, :fareAnchor, :fareOffset, :TimeRange, :dlfBand, :ndoBand, :criteria, :AirlineCode_Max, :AirlineCode_Min, :FlightNumber_Max, :FlightNumber_Min, :backstop, :autoBackstop, :Variance, :StrategyReference, :analystName, :bookedPlf, :actualbookedPlf, :startTime, :endTime, :allocationStatus, :endOfDaydlfBand, :endOfDayTgtLoad, :strategyFare, :lastSellingFare, :lastSevenAvgFare, :hardAnchorFare, :upsell, :bookingToday, :bookingInHour, :currentTgtLoad, :currentLoadFactor, :overBookingCount, :recommendedActions, :profileFare, :status, :w0Flag
                )
            """)

            params = {
                "Id": latest_row.Id,
                "Origin": latest_row.Origin,
                "Destin": latest_row.Destin,
                "FltNum": latest_row.FltNum,
                "DepDate": latest_row.DepDate,
                "BookedLoad": latest_row.BookedLoad,
                "TgtLoad": latest_row.TgtLoad,
                "MktFare_Min": str(latest_row.MktFare_Min),
                "OpenRBD": latest_row.OpenRBD,
                "SellingFare": str(latest_row.SellingFare),
                "Channel": latest_row.Channel,
                "HowDetermined": latest_row.HowDetermined,
                "RunId": latest_row.RunId,
                "CreatedAt": self.timeutils.getCurrentDateTime().strftime(DATE_TIME_FORMAT),
                "MktFare_Max": str(latest_row.MktFare_Max),
                "fareAnchor": str(latest_row.anchorFare),
                "fareOffset": str(latest_row.fareOffset),
                "TimeRange": str(latest_row.TimeRange),
                "dlfBand": str(latest_row.dlfBand),
                "ndoBand": str(latest_row.ndoBand),
                "criteria": str(latest_row.criteria),
                "AirlineCode_Max": str(latest_row.AirlineCode_Max),
                "AirlineCode_Min": str(latest_row.AirlineCode_Min),
                "FlightNumber_Max": str(latest_row.FlightNumber_Max),
                "FlightNumber_Min": str(latest_row.FlightNumber_Min),
                "backstop": str(latest_row.Backstop),
                "autoBackstop": str(latest_row.autoBackstop),
                "Variance": str(latest_row.Variance),
                "StrategyReference": str(latest_row.StrategyReference),
                "analystName": str(latest_row.analystName),
                "bookedPlf": str(latest_row.bookedPlf),
                "actualbookedPlf": str(latest_row.actualbookedPlf),
                "startTime": str(latest_row.startTime),
                "endTime": str(latest_row.endTime),
                "allocationStatus": str(latest_row.allocationStatus),
                "endOfDaydlfBand": str(latest_row.endOfDaydlfBand),
                "endOfDayTgtLoad": str(latest_row.endOfDayTgtLoad),
                "strategyFare": str(latest_row.strategyFare),
                "lastSellingFare": str(latest_row.lastSellingFare),
                "lastSevenAvgFare": str(latest_row.lastSevenAvgFare),
                "hardAnchorFare": str(latest_row.hardAnchorFare),
                "upsell": str(latest_row.upsell),
                "bookingToday": str(latest_row.bookingToday),
                "bookingInHour": str(latest_row.bookingLastHour),
                "currentTgtLoad": str(latest_row.currentTgtLoad),
                "currentLoadFactor": str(latest_row.currentLoadFactor),
                "overBookingCount": str(latest_row.overBookingCount),
                "recommendedActions": str(latest_row.recommendedActions),
                "profileFare": str(latest_row.profileFare),
                "status": str(latest_row.status),
                "w0Flag": str(latest_row.w0Flag)
            }

            try:
                self.wrconn.execute(insertQuery, params)
            except Exception as e:
                self.logger.error(e)

    def getTargetBookedLoadFromBenchmarkCurve(self, ndocounter, curveID):
        tgtBookedLoad = []
        # Fetch the Target Booked Load from the curve
        if ndocounter<0:
            ndocounter=0
        bookingCurveQuery = "Select LF from Curves where NDO = " + str(ndocounter) + " and CurveID = '" + curveID + "'"
        try:
            benchmark = pd.read_sql(bookingCurveQuery, self.conn)
        except Exception as e:
            self.logger.error(e)
        if(benchmark.shape[0] > 0):
            tgtBookedLoad = benchmark

        return tgtBookedLoad

    def getPlfThreshhold(self, ndocounter, flightNumber):
        pflThresholdQuery = "SELECT W9_threshold,W8_threshold,W7_threshold,W6_threshold,W5_threshold,W4_threshold,W3_threshold,W2_threshold,W1_threshold,P_threshold,L_threshold,B2B_threshold FROM config_pfl_threshold WHERE flightNumber = '"+ str(flightNumber) +"' AND NDO = '"+ str(ndocounter) +"' UNION " 
        pflThresholdQuery = pflThresholdQuery+ " SELECT W9_threshold,W8_threshold,W7_threshold,W6_threshold,W5_threshold,W4_threshold,W3_threshold,W2_threshold,W1_threshold,P_threshold,L_threshold,B2B_threshold FROM config_pfl_threshold WHERE flightNumber = '*' " 
        pflThresholdQuery = pflThresholdQuery+ " AND NOT EXISTS (SELECT 1 FROM config_pfl_threshold WHERE flightNumber = '"+ str(flightNumber) +"' and NDO = '"+str(ndocounter)+"') AND NDO = '"+ str(ndocounter) +"' LIMIT 1" 
        try:
            pflThresholdList = pd.read_sql(pflThresholdQuery, self.conn)
        except Exception as e:
            self.logger.error(e)
        w9_threshold = pflThresholdList['W9_threshold']
        w8_threshold = pflThresholdList['W8_threshold']
        w7_threshold = pflThresholdList['W7_threshold']
        w6_threshold = pflThresholdList['W6_threshold']
        w5_threshold = pflThresholdList['W5_threshold']
        w4_threshold = pflThresholdList['W4_threshold']
        w3_threshold = pflThresholdList['W3_threshold']
        w2_threshold = pflThresholdList['W2_threshold']
        w1_threshold = pflThresholdList['W1_threshold']
        p_threshold = pflThresholdList['P_threshold']
        l_threshold = pflThresholdList['L_threshold']
        B2B_threshold = pflThresholdList['B2B_threshold']
        
        return w9_threshold,w8_threshold,w7_threshold,w6_threshold,w5_threshold,w4_threshold,w3_threshold,w2_threshold,w1_threshold,p_threshold,l_threshold,B2B_threshold

    def getDepartureTimeBands(self):
        timeBandsQuery = "SELECT start_time, end_time FROM departure_time_ranges ORDER BY priority"
        try:
            cache_key = "departure_time_bands"
            queryResult = self.execute_cached_query(timeBandsQuery, self.conn, cache_key)
            return queryResult
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()

    def getd1d2StrategyValue(self, strategy, dplfBand, ndoBand, channel):
        d1d2Query = f"SELECT {strategy} FROM d1_d2_strategies where dplf_band = {dplfBand} and ndo_band = {ndoBand}"
        try:
            cache_key = f"d1d2_strategy_{strategy}_{dplfBand}_{ndoBand}"
            return self.execute_cached_query(d1d2Query, self.conn, cache_key)[strategy][0]
        except Exception as e:
            self.logger.error(e)
            return ""

    def getTimeReferenceValue(self, currParams):
        timeRangeQuery = "SELECT ar_start, ar_end FROM allocation_acceptable_range_d1 where airports = '"+ currParams.origin +"'"
        try:
            rangeList = pd.read_sql(timeRangeQuery, self.conn)
        except Exception as e:
            self.logger.error(e)
        return rangeList

    def getMarketFareRange(self, channel, currParams,currFltDateStr, DaySpan, startTime, endtime, Day3Flag,hrs):
        currFltDate = datetime.strptime(currFltDateStr, DATE_FORMAT).date()
        updateDate=currFltDate+timedelta(days=int(DaySpan))
        if Day3Flag == False:
            startDateTime=f"{currFltDate} {startTime}"
            enddateTime=f"{currFltDate} {endtime}"
            updateDate=currFltDate+timedelta(days=int(DaySpan))
            if currFltDate>updateDate:
                startDateTime=f"{updateDate} {startTime}"
                enddateTime=f"{currFltDate} {endtime}"
            elif currFltDate<updateDate:
                startDateTime=f"{currFltDate} {startTime}"
                enddateTime=f"{updateDate} {endtime}"
        if channel==B2B_CHANNEL:
            carr_exclusion = currParams.carr_exclusion_b2b
            flightExclusion = currParams.flightExclusionB2B
        else:
            carr_exclusion = currParams.carr_exclusion_b2c
            flightExclusion = currParams.flightExclusionB2C
    
        carr_exclusion_str = str(tuple(carr_exclusion.split('/')))  
        if len(tuple(carr_exclusion.split('/'))) == 1:
            carr_exclusion_str=carr_exclusion_str.replace(",", "")
        origin = currParams.origin
        destination = currParams.destin
        routedSector = self.getRoutedDestination(origin, destination)

        if(len(routedSector) > 0):
            origin = routedSector['routedSector'][0][:3]
            destination = routedSector['routedSector'][0][3:]   

        mktLowestQuery = f"""Select gross_fare, outbound_airline_code, outbound_flight 
            from market_fares where source_airport = '{origin}' and destination_airport = '{destination}' 
            and channel = '{channel}' and outbound_airline_code NOT IN {carr_exclusion_str} AND outbound_stop = 0
            AND outbound_airline_code != 'QP' AND outbound_airline_code!='G8' AND outbound_flight!='{flightExclusion}'
        """
        if Day3Flag:
            mktLowestQuery = mktLowestQuery + f""" AND CAST(departure_date AS DATE) >= CAST('{currFltDate}' AS DATE)
                AND CAST(departure_date AS DATE) <= CAST('{updateDate}' AS DATE)
                AND CAST(outbound_departuretime AS TIME) >= CAST('{startTime}' AS TIME)
                AND CAST(outbound_departuretime AS TIME) <= CAST('{endtime}' AS TIME) """
        else:
            mktLowestQuery = mktLowestQuery + f""" AND CAST(CONCAT(departure_date, ' ', outbound_departuretime) AS DATETIME) <= CAST('{enddateTime}' AS DATETIME) 
            AND CAST(CONCAT(departure_date, ' ', outbound_departuretime) AS DATETIME) >= CAST('{startDateTime}' AS DATETIME)  """

        if(self.inputSource.input_source[CONST_MARKET_FARES] != TEST_DATA):
            mktLowestQuery = mktLowestQuery + f" AND created_at > NOW() - INTERVAL {hrs} HOUR "
        mktLowestQuery = mktLowestQuery + " ORDER BY gross_fare ASC, outbound_flight ASC"
        try:
            mktFareRange = pd.read_sql(mktLowestQuery, self.conn)
        except Exception as e:
            self.logger.error(e)

        return mktFareRange
    
    def getMarketFareRangeConnections(self, channel, currParams,currFltDateStr, outBoundStop,outboundDuration):
        
        currFltDate = currFltDateStr
        origin = currParams.sector1[:3]
        destination = currParams.sector2[3:]
        
        mktLowestQuery = f"""Select gross_fare, outbound_airline_code, outbound_flight ,currency
            from market_fares where source_airport = '{origin}' and destination_airport = '{destination}' 
            and channel = '{channel}' AND outbound_stop <= '{outBoundStop}' and outbound_duration <= '{outboundDuration}'
            AND outbound_airline_code != 'QP' AND outbound_airline_code!='G8' 
        """
        
        mktLowestQuery = mktLowestQuery + f""" AND departure_date  = '{currFltDate}' """

        if(self.inputSource.input_source[CONST_MARKET_FARES] != TEST_DATA):
            mktLowestQuery = mktLowestQuery + " AND created_at > NOW() - INTERVAL 2 DAY "
        mktLowestQuery = mktLowestQuery + " ORDER BY gross_fare ASC"
        try:
            mktFareRange = pd.read_sql(mktLowestQuery, self.conn)
        except Exception as e:
            self.logger.error(e)

        return mktFareRange

    def getCurrentTime(self, currentTimeFlag, currParams, currFltDate,starttime):
        # load the list of sectors
        if currentTimeFlag:
            currFltDate = datetime.strptime(currFltDate, DATE_FORMAT).date()
            query = f''' 
                SELECT runTime FROM config_runTime 
                    where FlightNumber = '{currParams.flight_number}' 
                    and Origin = '{currParams.origin}' 
                    and Destin = '{currParams.destin}' 
                    and FlightDate ='{currFltDate}'
            '''
            currTime = pd.read_sql(query, self.conn)
            self.currentTime = currTime["runTime"][0]
        else:
            # Convert UTC time to IST
            # Define the IST timezone
            ist_timezone = pytz.timezone('Asia/Kolkata')
            # Convert UTC time to IST
            cur_time = starttime.astimezone(ist_timezone).time().strftime("%H:%M")
            self.currentTime = cur_time
        return self.currentTime
    def get_qp_currency(self, origin, destin):
        query = f"SELECT Currency FROM Fares WHERE Org = '{origin}' and Dst = '{destin}' and Status = 'A' limit 1"
        try:
            cache_key = f"qp_currency_{origin}_{destin}"
            return self.execute_cached_query(query, self.conn, cache_key)
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()
    
    def get_qp_fares(self, origin, destin):
        query = f"SELECT RBD, total FROM Fares WHERE Org = '{origin}' and Dst = '{destin}' and Status = 'A'"
        try:
            cache_key = f"qp_fares_{origin}_{destin}"
            return self.execute_cached_query(query, self.conn, cache_key)
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()

    def get_TBF_fares(self, origin, destin):
        is_valid = False     
        query = f"""
            SELECT TOTAL 
            FROM Fares 
            WHERE Org = '{origin}' 
            AND Dst = '{destin}'
            AND RBD IN ({TBF_RBD_QUERY})
        """
        
        try:
            rbddata = pd.read_sql(query, self.conn)
            is_valid = (
                len(rbddata) >= TBF_COUNT 
                and not (rbddata['TOTAL'] <= 0).any() 
                and not rbddata['TOTAL'].isnull().any()
            )
            
        except Exception as e:
            self.logger.error(e)
        
        return is_valid
    
    def getValueWithOffset(self, origin, destin, anchorFare, offset, channel, openResult,route):
        self.logger.info("Anchor Fare is " + str(anchorFare) + " with an offset of " + str(offset))

        RBDQuery = "select RBD, Total from Fares where Org = '" + origin + "' and Dst = '" + destin + "' and route = '"+route+"'"
        try:
            rbddata = pd.read_sql(RBDQuery, self.conn)
        except Exception as e:
            self.logger.error(e)
        if(rbddata.shape[0] == 0):
            self.logger.info('QP Fare not available. Please validate Fares table')
            openResult = {'RBD':[CONST_NA], 'Fare':[CONST_NA], 'FareResult':['QP Fares not available'],'anchorFare': [anchorFare],'statusInd':[1], 'allocationStatus': [1]}
            self.redishelper.updateRunAudit()
            return openResult, rbddata
        exclude_rbd_list = TBF_RBD.split(',')
        exclude_rbd_list.extend(list(json.loads(GROUP_QUOTATION).values()))

    # Convert the list of RBDs to a comma-separated string for the SQL query
        exclude_rbd_str = ",".join(["'{}'".format(rbd) for rbd in exclude_rbd_list])
        RBDQuery += f" AND RBD NOT IN ({exclude_rbd_str})"
        
        if channel == 'B2C':
            RBDQuery = RBDQuery + " and LEFT(RBD,1) != 'Z'"
        if channel == 'B2B':
            RBDQuery = RBDQuery + " and LEFT(RBD,1) = 'Z'"

        sortorder = ' desc '

        if (int(offset) < 0):
            RBDQuery = RBDQuery + " and Total < " + str(anchorFare)
            sortorder = ' desc '
        if (int(offset) >= 0):
            anchorFare = float(anchorFare)
            if (anchorFare > 0):
                RBDQuery = RBDQuery + " and Total >= " + str(anchorFare)
            else:
                # Below condition forces zero records to be returned
                RBDQuery = RBDQuery + " and Total >= 1000000"
            sortorder = ' asc '

        RBDQuery = RBDQuery + " order by Total " + sortorder
        if(self.debugMode):
            self.logger.info(RBDQuery)

        try:
            rbddata = pd.read_sql(RBDQuery, self.conn)
        except Exception as e:
            self.logger.error(e)
        return openResult, rbddata

    def fetchExtremePublishedFare(self, origin, destin, channel, type, backstop,route):
        exclude_rbd_list = TBF_RBD.split(',')
        exclude_rbd_list.extend(list(json.loads(GROUP_QUOTATION).values()))
    # Convert the list of RBDs to a comma-separated string for the SQL query
        exclude_rbd_str = ",".join(["'{}'".format(rbd) for rbd in exclude_rbd_list])
        RBDQuery = "select RBD, Total from Fares where Org = '" + origin + "' and Dst = '" + destin + "' and route = '"+route+"'"
        RBDQuery += f" AND RBD NOT IN ({exclude_rbd_str})"
        
        if channel == 'B2C':
            if type == 'LOW':
                RBDQuery = RBDQuery + " and LEFT(RBD,1) != 'Z' and Total >= '" + str(backstop) + "' order by Total asc" 
            else:
                RBDQuery = RBDQuery + " and LEFT(RBD,1) != 'Z' and Total >= '" + str(backstop) + "' order by Total desc"
        else:
            if type == 'LOW':
                RBDQuery = RBDQuery + " and LEFT(RBD,1) = 'Z' and Total >= '" + str(backstop) + "' order by Total asc"
            else:
                RBDQuery = RBDQuery + " and LEFT(RBD,1) = 'Z' and Total >= '" + str(backstop) + "' order by Total desc"
        try:
            defFareData = pd.read_sql(RBDQuery, self.conn)
        except Exception as e:
            self.logger.error(e)

        return defFareData

    def getHourlyOwnFareData(self, ndo, hour):
        query = f''' SELECT value FROM config_ownfares_hourly where {ndo} between ndoStart and ndoEnd and hour = {int(hour)} '''
        try:
            hourlyData = pd.read_sql(query, self.conn)
        except Exception as e:
            self.logger.error(e)

        return hourlyData

    def getGridOwnFareData(self, ndoBand, dlfBand, channel):

        query = f''' SELECT value FROM config_ownfare_grid where ndoBand={ndoBand} and dlfBand={dlfBand} and channel='{channel}' '''
        try:
            gridData = pd.read_sql(query, self.conn)
        except Exception as e:
            self.logger.error(e)

        return gridData

    def getFareHistoryData(self, ndo, sector, month, dow, channel, currTime):

        query = f''' SELECT value FROM config_ndo_range where start <= {ndo} and end > {ndo} '''
        try:
            ndoRange = pd.read_sql(query, self.conn)
        except Exception as e:
            self.logger.error(e)

        currTime =  currTime + ':00'

        query = f''' 
                select `{ndoRange['value'][0]}` as price from (
                    SELECT *, CASE
                        WHEN TIME(startTime) = '02:30:00' AND TIME(endTime) = '07:30:00' THEN 0
                        WHEN TIME(startTime) = '07:30:00' AND TIME(endTime) = '11:00:00' THEN 1
                        WHEN TIME(startTime) = '11:00:00' AND TIME(endTime) = '14:30:00' THEN 2
                        WHEN TIME(startTime) = '14:30:00' AND TIME(endTime) = '18:00:00' THEN 3
                        WHEN TIME(startTime) = '18:00:00' AND TIME(endTime) = '21:00:00' THEN 4
                        ELSE 5 end AS Cluster
                    FROM config_ownFares
                    WHERE sector='{sector}' and Month='{month}' and channel='{channel}' and DOW = '{dow}' 
                ) A where A.Cluster = 
                CASE 
                    WHEN TIME('{currTime}') >= '02:30:00' AND TIME('{currTime}') < '07:30:00' THEN 0
                    WHEN TIME('{currTime}') >= '07:30:00' AND TIME('{currTime}') < '11:00:00' THEN 1
                    WHEN TIME('{currTime}') >= '11:00:00' AND TIME('{currTime}') < '14:30:00' THEN 2
                    WHEN TIME('{currTime}') >= '14:30:00' AND TIME('{currTime}') < '18:00:00' THEN 3
                    WHEN TIME('{currTime}') >= '18:00:00' AND TIME('{currTime}') < '21:00:00' THEN 4
                    ELSE 5 end 
                '''
        try:
            historicData = pd.read_sql(query, self.conn)
        except Exception as e:
            self.logger.error(e)

        return historicData

    def getProfileFares(self, sector, month, dow, currTime, percentile):
        query = f''' 
                select {percentile} as price from (
                    SELECT *
                    FROM config_profile_fares
                    WHERE sector='{sector}' and Month='{month}' and DOW = '{dow}' 
                ) A where Cluster = 
                CASE 
                    WHEN TIME('{currTime}') >= '02:30:00' AND TIME('{currTime}') < '07:30:00' THEN 0
                    WHEN TIME('{currTime}') >= '07:30:00' AND TIME('{currTime}') < '11:00:00' THEN 1
                    WHEN TIME('{currTime}') >= '11:00:00' AND TIME('{currTime}') < '14:30:00' THEN 2
                    WHEN TIME('{currTime}') >= '14:30:00' AND TIME('{currTime}') < '18:00:00' THEN 3
                    WHEN TIME('{currTime}') >= '18:00:00' AND TIME('{currTime}') < '21:00:00' THEN 4
                    ELSE 5 end 
                '''
        try:
            profileData = pd.read_sql(query, self.conn)
        except Exception as e:
            self.logger.error(e)
        return profileData

    def profileFaresExtract(self, origin, destin, month, dow, currTime):
        query = f''' 
                SELECT Q0,Q1,Q2,Q3,Q4,Q5,Q6,Q7,Q8,Q9,Q10,Q11,Q12,Q13,Q14,Q15,Q16,Q17,Q18,Q19,Q20
                FROM config_profile_fares
                WHERE sector = CONCAT('{origin}','{destin}') and Month = '{month}' and DOW = '{dow}'  
                AND Cluster = 
                CASE 
                    WHEN TIME('{currTime}') >= '02:30:00' AND TIME('{currTime}') < '07:30:00' THEN 0
                    WHEN TIME('{currTime}') >= '07:30:00' AND TIME('{currTime}') < '11:00:00' THEN 1
                    WHEN TIME('{currTime}') >= '11:00:00' AND TIME('{currTime}') < '14:30:00' THEN 2
                    WHEN TIME('{currTime}') >= '14:30:00' AND TIME('{currTime}') < '18:00:00' THEN 3
                    WHEN TIME('{currTime}') >= '18:00:00' AND TIME('{currTime}') < '21:00:00' THEN 4
                    ELSE 5 end 
                '''
        try:
            profileData = pd.read_sql(query, self.conn)
        except Exception as e:
            self.logger.error(e)
        return profileData

    def getMappedMonth(self, currFltDate):
        query = f''' SELECT mappedMonth FROM config_date_event where date = '{currFltDate}' '''
        return pd.read_sql(query, self.conn)

    def getUpsellValue(self, currFltDate, sector):
        query = f''' SELECT 
                        REPLACE(value,'%%','') AS percentage
                    FROM
                        config_profile_fare_station_upsell
                    WHERE
                        date = '{currFltDate}'
                        AND sector = '{sector}' '''

        return pd.read_sql(query, self.conn)

    def getArea(self, currentHour):
        query = f''' SELECT sum(shareOfBooking) as sum FROM config_share_of_booking where hours <= {currentHour} '''
        return pd.read_sql(query, self.conn)

    def getRDBTable(self, dplfBand, endOfDayDplfBand, currentTime, ndoBand):
        """Get RDB table data using cached table and filtering"""
        try:
            # Get the cached table
            config_determine_fares_df = self.table_cache.filter_table("config_determine_fares")
            if config_determine_fares_df is None:
                # If not cached, load it
                config_determine_fares_df = self.table_cache.load_table("config_determine_fares", self.conn)
            
            # This query uses complex JSON conditions - we need to implement equivalent filtering
            # For this specific case, we'll need to extract and process the JSON fields
            result = []
            
            # Process each row to check conditions
            for _, row in config_determine_fares_df.iterrows():
                # Extract JSON values - in a real implementation, you'd need proper JSON parsing
                conditions = row.get('conditions', '{}')
                if isinstance(conditions, str):
                    try:
                        conditions = json.loads(conditions)
                    except:
                        conditions = {}
                
                # Extract time values
                start_time = conditions.get('start_time', '00:00')
                end_time = conditions.get('end_time', '23:59')
                
                # Extract band values
                start_of_day_dlf_band = conditions.get('start_of_day_dlf_band', str(dplfBand))
                end_of_day_dlf_band = conditions.get('end_of_day_dlf_band', str(endOfDayDplfBand))
                ndo_band_value = conditions.get('ndo_band', str(ndoBand))
                
                # Check if this row matches our conditions
                if (start_time <= currentTime <= end_time and
                    str(dplfBand) in start_of_day_dlf_band.split(',') and
                    str(endOfDayDplfBand) in end_of_day_dlf_band.split(',') and
                    str(ndoBand) in ndo_band_value.split(',')):
                    result.append({
                        'B2CValue': row.get('B2CValue'),
                        'B2BValue': row.get('B2BValue')
                    })
            
            return pd.DataFrame(result)
        except Exception as e:
            self.logger.error(f"Error in getRDBTable: {e}")
            # If we can't process the cached data, fall back to direct query
            query = f'''
                SELECT B2CValue, B2BValue 
                FROM config_determine_fares 
                WHERE 1 = 1 
                AND IFNULL(TIME(JSON_UNQUOTE(JSON_EXTRACT(conditions, '$.start_time'))), '00:00') <= '{currentTime}' 
                AND IFNULL(TIME(JSON_UNQUOTE(JSON_EXTRACT(conditions, '$.end_time'))), '23:59') >= '{currentTime}' 
                AND FIND_IN_SET('{dplfBand}',IFNULL(JSON_UNQUOTE(JSON_EXTRACT(conditions, '$.start_of_day_dlf_band')), '{dplfBand}')) 
                AND FIND_IN_SET('{endOfDayDplfBand}',IFNULL(JSON_UNQUOTE(JSON_EXTRACT(conditions, '$.end_of_day_dlf_band')), '{endOfDayDplfBand}')) 
                AND IFNULL(FIND_IN_SET('{ndoBand}',JSON_UNQUOTE(JSON_EXTRACT(conditions, '$.ndo_band'))), '{ndoBand}')
            '''
            return pd.read_sql(query, self.conn)
    
    def fareExtract(self, origin, destination, channel, openingRBD, route):
        """Get fare information using cached table and filtering"""
        try:
            # Get the cached Fares table
            fares_df = self.table_cache.filter_table("Fares")
            if fares_df is None:
                # If not already cached, load it with a condition to limit size
                fares_df = self.table_cache.load_table("Fares", self.conn, 
                                                     condition=f"Org = '{origin}' AND Dst = '{destination}' AND Route = '{route}'")
            
            # Apply initial filters
            filtered_df = fares_df[
                (fares_df['Org'] == origin) &
                (fares_df['Dst'] == destination) &
                (fares_df['Route'] == route)
            ]
            
            # Apply channel filter
            if channel == 'B2C':
                filtered_df = filtered_df[~filtered_df['RBD'].str.startswith('Z')]
            elif channel == 'B2B':
                filtered_df = filtered_df[filtered_df['RBD'].str.startswith('Z')]
            
            # Apply RBD filter if specified
            if openingRBD:
                filtered_df = filtered_df[filtered_df['RBD'] == openingRBD]
            
            # Sort by Total
            filtered_df = filtered_df.sort_values('Total')
            
            # Add ROW_NUMBER
            filtered_df = filtered_df.reset_index(drop=True)
            filtered_df.index += 1  # 1-based index
            filtered_df = filtered_df.reset_index()
            filtered_df.rename(columns={'index': 'id'}, inplace=True)
            
            # Select only required columns
            result_df = filtered_df[['id', 'RBD', 'Total']]
            
            return result_df
        except Exception as e:
            self.logger.error(f"Error in fareExtract: {e}")
            # Fall back to direct query
            RBDQuery = "select ROW_NUMBER() OVER (ORDER BY TOTAL) AS id, RBD, Total from Fares where Org = '" + origin + "' and Dst = '" + destination + "' and Route= '"+route+"'"

            if channel == 'B2C':
                RBDQuery = RBDQuery + " and LEFT(RBD,1) != 'Z' "
            if channel == 'B2B':
                RBDQuery = RBDQuery + " and LEFT(RBD,1) = 'Z' "
            if openingRBD != '':
                RBDQuery = RBDQuery + " and RBD = '" + openingRBD + "'"

            RBDQuery = RBDQuery + " order by Total asc"
            return pd.read_sql(RBDQuery, self.conn)

    def insert_run_fltDate_audit(self, run_id, flightNumber, flightDate, result, b2cstatus, b2bstatus):
        insert_query = " INSERT INTO run_flight_date_audit (runId, flightNumber, flightDate, result, b2cstatus, b2bstatus ,createdAt) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        try:
            self.wrconn.execute(insert_query, (run_id, flightNumber, flightDate, result, b2cstatus, b2bstatus, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        except Exception as e:
            self.logger.info(e)

    def getCurrentBookedLoads(self, origin, destin, startDate, endDate):

        NavitaireQuery = f"""SELECT Origin, Destination, FlightNumber, DepartureDate, bookedPax, capacity ,DepartureTime, adjustedCapacity,tbf_bookings from bookload
            where DepartureDate >= '{str(startDate)}' and DepartureDate <= '{str(endDate)}'
            and Origin = '{origin}' and Destination = '{destin}' """
        bookedData = pd.read_sql(NavitaireQuery, self.conn)

        if(self.debugMode):
            self.logger.info(NavitaireQuery)

        return bookedData

    def getLid(self, origin, destin, startDate, endDate, FltNumber):

        NavitaireQuery = f"""SELECT Lid from bookload
            where DepartureDate >= '{str(startDate)}' and DepartureDate <= '{str(endDate)}'
            and Origin = '{origin}' and Destination = '{destin}' and FlightNumber='{FltNumber}'"""
        bookedData = pd.read_sql(NavitaireQuery, self.conn)

        if(self.debugMode):
            self.logger.info(NavitaireQuery)

        return bookedData
    

    def checkForNextFLight(self, currFltDateStr, currParams):

        currFltDate = datetime.strptime(currFltDateStr, DATE_FORMAT).date()	

        query=f'''
            select DepartureTime,Origin,Destination,FlightNumber from bookload where DepartureDate = '{currFltDate}' AND Origin = '{currParams.origin}' AND Destination = '{currParams.destin}' order by STR_TO_DATE(DepartureTime, '%%H:%%i:%%s') ASC
        '''
        conn=self.conn
        df = pd.read_sql(query, conn)

        df['FlightNumber'] = df['FlightNumber'].astype(str)
        row_index = df[((df['FlightNumber'])== str(currParams.flight_number)) &(df['Origin'] == currParams.origin) & (df['Destination'] == currParams.destin)].index

        return len(row_index) > 0 and row_index[0] < len(df) - 1

    def getLastSellingFare(self, currFltDateStr, currParams):
        """Get last selling fare using cached table and filtering"""
        try:
            # Convert string date to datetime.date
            currFltDate = datetime.strptime(currFltDateStr, DATE_FORMAT).date()
            
            # Get the cached booking_fare_data table
            booking_fare_data_df = self.table_cache.filter_table("booking_fare_data")
            if booking_fare_data_df is None:
                # Load if not cached
                booking_fare_data_df = self.table_cache.load_table("booking_fare_data", self.conn,
                                                                 condition=f"FlightNumber = '{currParams.flight_number}' AND Origin = '{currParams.origin}' AND Destin = '{currParams.destin}'")
            
            # Filter the data
            filtered_df = booking_fare_data_df[
                (booking_fare_data_df['FlightDate'] == currFltDate) &
                (booking_fare_data_df['FlightNumber'] == currParams.flight_number) &
                (booking_fare_data_df['Origin'] == currParams.origin) &
                (booking_fare_data_df['Destin'] == currParams.destin)
            ]
            
            # Select only needed columns
            result = filtered_df[['B2B_last3avg_fare', 'B2C_last3avg_fare', 'last7avg']]
            
            return result
        except Exception as e:
            self.logger.error(f"Error in getLastSellingFare: {e}")
            # Fall back to direct query
            query = f'''
                select B2B_last3avg_fare,B2C_last3avg_fare,last7avg from booking_fare_data 
                where FlightDate = '{currFltDate}' 
                AND FlightNumber = '{currParams.flight_number}' 
                AND Origin = '{currParams.origin}' 
                AND Destin = '{currParams.destin}'
            '''
            return pd.read_sql(query, self.conn)

    def getNumberOfBookings(self, currFltDateStr, currParams):
        """Get number of bookings using cached table and filtering"""
        try:
            # Convert string date to datetime.date
            currFltDate = datetime.strptime(currFltDateStr, DATE_FORMAT_2).date()
            
            # Get the cached booking_fare_data table
            booking_fare_data_df = self.table_cache.filter_table("booking_fare_data")
            if booking_fare_data_df is None:
                # Load if not cached
                booking_fare_data_df = self.table_cache.load_table("booking_fare_data", self.conn,
                                                                 condition=f"FlightNumber = '{currParams.flight_number}' AND Origin = '{currParams.origin}' AND Destin = '{currParams.destin}'")
            
            # Filter the data
            filtered_df = booking_fare_data_df[
                (booking_fare_data_df['FlightDate'] == currFltDate) &
                (booking_fare_data_df['FlightNumber'] == currParams.flight_number) &
                (booking_fare_data_df['Origin'] == currParams.origin) &
                (booking_fare_data_df['Destin'] == currParams.destin)
            ]
            
            # Select only needed columns
            result = filtered_df[['BookingsCount', 'STD', 'BookingToday', 'BookingsInHour']]
            
            return result
        except Exception as e:
            self.logger.error(f"Error in getNumberOfBookings: {e}")
            # Fall back to direct query
            query = f'''
                select BookingsCount,STD,BookingToday,BookingsInHour from booking_fare_data 
                where FlightDate = '{currFltDate}' 
                AND FlightNumber = '{currParams.flight_number}' 
                AND Origin = '{currParams.origin}' 
                AND Destin = '{currParams.destin}'
            '''
            return pd.read_sql(query, self.conn)

    def get_parameters(self):
        parameterValueQuery = "SELECT parameterKey,parameterValue FROM rm_parameter_values"
        try:
            parameterValue = pd.read_sql(parameterValueQuery, self.conn)
        except Exception as e:
            self.logger.error(e)

        return parameterValue

    def getRoutedDestination(self, origin, destination):
        query = f''' SELECT routedSector FROM config_marketFares_rerouting where sector = concat('{origin}','{destination}') '''
        try:
            routedData = pd.read_sql(query, self.conn)
        except Exception as e:
            self.logger.error(e)

        return routedData

    def getFinalPrice(self, rbd_sector1, rbd_sector2, sector1, sector2, channel):

        if channel == 'B2C':
            condition = " and LEFT(RBD,1) != 'Z'"
        if channel == 'B2B':
            condition = " and LEFT(RBD,1) = 'Z'"

        sector1_RBDquery = f"select Total ,currency from Fares where sector = '{sector1}' and RBD = '{rbd_sector1}' and route = 'L' {condition}"
        sector2_RBDquery = f"select Total ,currency from Fares where sector = '{sector2}' and RBD = '{rbd_sector2}' and route = 'L' {condition}"

        sector1_result = pd.read_sql(sector1_RBDquery, self.conn)
        sector2_result = pd.read_sql(sector2_RBDquery, self.conn)

        if (sector1_result.shape[0] == 0 or sector2_result.shape[0] == 0):
            total = 0
            sector1_total=0
            sector2_total=0
            currency1=""
            currency2=""
        else:
            sector1_total = sector1_result["Total"][0]
            sector2_total = sector2_result["Total"][0]
            currency1=sector1_result["currency"][0]
            currency2=sector2_result["currency"][0]
            total = sector1_total + sector2_total
        return total,sector1_total,sector2_total,currency1,currency2

    def getSellingPrice(self, rbd, sector, channel,route):

        if channel == 'B2C':
            condition = " and LEFT(RBD,1) != 'Z'"
        if channel == 'B2B':
            condition = " and LEFT(RBD,1) = 'Z'"

        sector1_RBDquery = f"select Total,currency from Fares where sector = '{sector}' and RBD = '{rbd}' and route = '{route}' {condition}"

        sector1_result = pd.read_sql(sector1_RBDquery, self.conn)

        if (sector1_result.shape[0] == 0):

            sector1_total=0
            currency=""

        else:
            sector1_total = sector1_result["Total"][0]
            currency = sector1_result["currency"][0]
        return sector1_total,currency
    
    def insertSummaryRow_connections(self, allocresults):
        if self.inputSource.logs[CONST_RUN_SUMMARY] == DB:
            latest_row = allocresults[-1]
            
            insertQuery = text(""" 
                INSERT INTO run_summary_connections 
                (Id, RunId, Sector1, Sector2, FltNum1, FltNum2, DepDate1, DepDate2, BookedLoad, BookedLoad1, SellingFare1, SellingFare2, BookedLoad2, OpenRBD1, OpenRBD2, Discount, anchorFare, OpenRBD, SellingFare, Channel, HowDetermined, allocationStatus, Backstop, CreatedAt, analystName, Variance1, Variance2, strategyFare, criteria, fareOffset, MktFare_Min, AirlineCode_Min, FlightNumber_Min, MktFare_Max, AirlineCode_Max, FlightNumber_Max, outBoundStop, outboundDuration, DiscountedFare, Currency, conversionRate) 
                VALUES (
                    :Id, :RunId, :Sector1, :Sector2, :FltNum1, :FltNum2, :DepDate1, :DepDate2, :BookedLoad, :BookedLoad1, :SellingFare1, :SellingFare2, :BookedLoad2, :OpenRBD1, :OpenRBD2, :Discount, :anchorFare, :OpenRBD, :SellingFare, :Channel, :HowDetermined, :allocationStatus, :Backstop, :CreatedAt, :analystName, :Variance1, :Variance2, :strategyFare, :criteria, :fareOffset, :MktFare_Min, :AirlineCode_Min, :FlightNumber_Min, :MktFare_Max, :AirlineCode_Max, :FlightNumber_Max, :outBoundStop, :outboundDuration, :DiscountedFare, :Currency, :conversionRate
                )
            """)

            params = {
                "Id": latest_row.Id,
                "RunId": latest_row.RunId,
                "Sector1": latest_row.Sector1,
                "Sector2": latest_row.Sector2,
                "FltNum1": str(latest_row.FltNum1),
                "FltNum2": str(latest_row.FltNum2),
                "DepDate1": latest_row.DepDate1,
                "DepDate2": latest_row.DepDate2,
                "BookedLoad": str(latest_row.BookedLoad),
                "BookedLoad1": str(latest_row.Bookedload1),
                "SellingFare1": str(latest_row.SellingFare1),
                "SellingFare2": str(latest_row.SellingFare2),
                "BookedLoad2": str(latest_row.Bookedload2),
                "OpenRBD1": str(latest_row.OpenRBD1),
                "OpenRBD2": str(latest_row.OpenRBD2),
                "Discount": str(latest_row.Discount),
                "anchorFare": latest_row.anchorFare,
                "OpenRBD": str(latest_row.OpenRBD),
                "SellingFare": str(latest_row.SellingFare),
                "Channel": latest_row.Channel,
                "HowDetermined": latest_row.HowDetermined,
                "allocationStatus": latest_row.allocationStatus,
                "Backstop": str(latest_row.Backstop),
                "CreatedAt": self.timeutils.getCurrentDateTime().strftime(DATE_TIME_FORMAT),
                "analystName": str(latest_row.analystName),
                "Variance1": str(latest_row.variance1),
                "Variance2": str(latest_row.variance2),
                "strategyFare": str(latest_row.strategyfare),
                "criteria": str(latest_row.criteria),
                "fareOffset": str(latest_row.fareOffset),
                "MktFare_Min": str(latest_row.MktFare_Min),
                "AirlineCode_Min": str(latest_row.AirlineCode_Min),
                "FlightNumber_Min": str(latest_row.FlightNumber_Min),
                "MktFare_Max": str(latest_row.MktFare_Max),
                "AirlineCode_Max": str(latest_row.AirlineCode_Max),
                "FlightNumber_Max": str(latest_row.FlightNumber_Max),
                "outBoundStop": str(latest_row.outBoundStop),
                "outboundDuration": str(latest_row.outboundDuration),
                "DiscountedFare": str(latest_row.discountedFare),
                "Currency": str(latest_row.currency),
                "conversionRate": str(latest_row.conversionRate)
            }

            try:
                self.wrconn.execute(insertQuery, params)
            except Exception as e:
                self.logger.error(e)

    def getcurrentRBD(self,current_date, flight, channel, sector,bookload):

        parameterValueQuery = f"""SELECT OpeningRBD as ClassOfService  FROM current_opening_rbd where DepartureDate = '{str(current_date)}' and concat(Origin,Destination)='{sector}' and flightno='{str(flight)}' and Channel='{channel}'"""
        try:
            parameterValue = pd.read_sql(parameterValueQuery, self.conn)
        except Exception as e:
            self.logger.error(e)

        return parameterValue

    def getDefaultAu(self, departure_date):
        month_name = datetime.strptime(departure_date, '%Y-%m-%d').strftime('%B')
        query = f"""
        SELECT default_au, au_cap
        FROM default_distress_inventory
        WHERE month='{str(month_name)}'
        """
        try:
            parameter_value = pd.read_sql(query, self.conn)
        except Exception as e:
            self.logger.error(e)
        return parameter_value["default_au"][0], parameter_value["au_cap"][0]

    def getDistressAu(self, ndo_band, dplf_band):
        query = f"""
        SELECT au_value
        FROM distress_inventory_strategy
        WHERE ndo_band='{str(ndo_band)}' AND dplf_band='{str(dplf_band)}'
        """
        try:
            parameter_value = pd.read_sql(query, self.conn)
        except Exception as e:
            self.logger.error(e)
        return parameter_value["au_value"][0]

    def get_seats_sold_by_class(self, flight_number, origin, destination, departure_date):
        query = f'''
        SELECT 
           ClassOfService as class_code,
           ClassSold as seats_sold
        FROM current_rbd
        WHERE
            FlightNumber = "{str(flight_number)}"
            AND DepartureDate = "{str(departure_date)}"
            AND Origin = "{origin}"
            AND Destin = "{destination}"
            AND (ClassOfService = 'P' or ClassOfService = 'L')
        '''
        df = pd.read_sql(query, self.conn)
        if df.empty:
            return -1, -1
        l_class_seats_sold = df[df['class_code'] == 'L']['seats_sold'].values[0]
        p_class_seats_sold = df[df['class_code'] == 'P']['seats_sold'].values[0]
        return l_class_seats_sold, p_class_seats_sold

    def get_curveids(self, sector1, sector2, fltno1, fltno2, dep1, dep2):
        query = f"SELECT * FROM market_list ml WHERE (CONCAT(ml.Origin,ml.Destin) = '{sector1}' AND STR_TO_DATE(ml.PerStart, '%%m/%%d/%%Y') = '{dep1}' AND ml.FlightNumber = '{fltno1}') OR (CONCAT(ml.Origin,ml.Destin) = '{sector2}' AND STR_TO_DATE(ml.PerStart, '%%m/%%d/%%Y') = '{dep2}' AND ml.FlightNumber = '{fltno2}') UNION SELECT * FROM market_list_international mli WHERE (CONCAT(mli.Origin, mli.Destin) = '{sector1}' AND STR_TO_DATE(mli.PerStart, '%%m/%%d/%%Y') = '{dep1}' AND mli.FlightNumber = '{fltno1}') OR (CONCAT(mli.Origin, mli.Destin) = '{sector2}' AND STR_TO_DATE(mli.PerStart, '%%m/%%d/%%Y') = '{dep2}' AND mli.FlightNumber = '{fltno2}');"
        
        cache_key = f"curveids_{sector1}_{sector2}_{fltno1}_{fltno2}_{dep1}_{dep2}"
        df = self.execute_cached_query(query, self.conn, cache_key)
        return df
    
    def get_second_flight_dep(self, flightno1, flightno2, org, destin, depDate):
        query = f"""select Departuredate2 from connections_details where FlightNo1='{flightno1}' and FlightNo2 = '{flightno2}' and Origin1='{org}' and Destnination2='{destin}' and Departuredate1='{depDate}'"""
        
        cache_key = f"second_flight_dep_{flightno1}_{flightno2}_{org}_{destin}_{depDate}"
        df = self.execute_cached_query(query, self.conn, cache_key)
        if len(df)==0:
            return ""
        return df['Departuredate2'][0]