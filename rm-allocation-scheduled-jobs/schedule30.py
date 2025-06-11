import pandas as pd
import logging
import os
from sqlalchemy import create_engine, text
import configsettings
from constants_utils import ConstantsUtils
from datetime import datetime, timedelta
import traceback
import json
import boto3
from uuid import uuid4
from os import getenv
import io

class ScheduleThirty:
    """Class to manage the flight booking threshold monitoring process with multiple threshold crossings."""
    
    def __init__(self, logger, params, overrides, context):
        """Initialize connections and settings."""
        self.logger = logger or logging.getLogger()
        self.rm_rd_conn = params['wrconn']
        self.nav_conn = params['navconn']
        self.dw_conn = params['dwconn']
        self.cache_client = params['cache_client']
        self.logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")
        self.nav_db_name = configsettings.NAV_DB_NAME
        self.nav_db_dw_name = configsettings.NAV_DB_DW_NAME
        self.constants = ConstantsUtils(self.rm_rd_conn)
        self.booking_threshold = self.constants.BOOKINGS_THRESHOLD
        
        # Redis cache key structure with folder
        self.flight_tracker_folder = "flight_tracker"
        self.flight_tracker_prefix = f"{self.flight_tracker_folder}:flight:"
        self.all_flights_key = f"{self.flight_tracker_folder}:all_flight_keys"  # Key to store list of all flight keys
    
    @staticmethod
    def generate_flight_key(row):
        """Generate a unique key for each flight."""
        # Ensure the flight number has no leading/trailing spaces
        flight_number = str(row['FlightNumber']).strip()
        
        # Format the departure date consistently (YYYY-MM-DD)
        if isinstance(row['DepartureDate'], str):
            departure_date = row['DepartureDate']
        else:
            # Format as string if it's a datetime object
            departure_date = row['DepartureDate'].strftime('%Y-%m-%d')
        
        # Create the flight key without any spaces
        return f"{row['DepartureStation']}{row['ArrivalStation']}_{flight_number}_{departure_date}"
    
    @staticmethod
    def parse_flight_key(flight_key):
        """
        Extract flight information from the flight key.
        
        Args:
            flight_key (str): Format is "DEPSTNARSTN_FLIGHTNUM_YYYY-MM-DD"
            
        Returns:
            dict: Dictionary with departure_station, arrival_station, flight_number, departure_date
        """
        try:
            # Split by underscore
            parts = flight_key.split('_')
            
            # First part contains departure and arrival stations
            stations = parts[0]
            departure_station = stations[:3]
            arrival_station = stations[3:] if len(stations) > 3 else ""
            
            # Second part is flight number
            flight_number = parts[1] if len(parts) > 1 else ""
            
            # Third part is date
            departure_date = parts[2] if len(parts) > 2 else ""
            
            return {
                "departure_station": departure_station,
                "arrival_station": arrival_station,
                "flight_number": flight_number,
                "departure_date": departure_date
            }
        except Exception as e:
            # Return empty dict on error
            return {
                "departure_station": "",
                "arrival_station": "",
                "flight_number": "",
                "departure_date": ""
            }

    def get_flights_with_bookings(self):
        """
        Get all flights and their booking data from the database for flights
        departing 30+ days in the future
        """
        today = datetime.now()
        departure_start_date = (today + timedelta(days=30)).strftime('%Y-%m-%d')
        departure_end_date = (today + timedelta(days=365)).strftime('%Y-%m-%d')
        
        self.logger.info(f"Fetching flight bookings for flights departing between {departure_start_date} and {departure_end_date}")
        
        # Query to get flight bookings (modified to not generate flight key in SQL)
        query = f"""
        WITH FlightBookings AS (
            SELECT 
                IL.FlightNumber,
                IL.DepartureStation,
                IL.ArrivalStation,
                IL.DepartureDate,
                COUNT(DISTINCT BP.PassengerID) AS TotalBookings
            FROM {self.nav_db_name}rez.Booking B 
            INNER JOIN {self.nav_db_name}rez.BookingPassenger BP ON BP.BookingID = B.BookingID 
            INNER JOIN {self.nav_db_name}rez.PassengerJourneySegment PJS ON PJS.PassengerID = BP.PassengerID 
                AND (PJS.FareBasis != 'SO7NBSOL' OR PJS.FareBasis IS NULL)
            INNER JOIN {self.nav_db_name}rez.PassengerJourneyLeg PJL ON PJL.PassengerID = PJS.PassengerID 
                AND PJL.SegmentID = PJS.SegmentID 
            INNER JOIN {self.nav_db_name}rez.InventoryLeg IL ON IL.InventoryLegId = PJL.InventoryLegID 
            WHERE IL.DepartureDate BETWEEN CAST('{departure_start_date}' AS DATE) 
                                    AND CAST('{departure_end_date}' AS DATE)
            GROUP BY 
                IL.FlightNumber,
                IL.DepartureStation,
                IL.ArrivalStation,
                IL.DepartureDate
        )
        SELECT
            FlightNumber,
            DepartureStation,
            ArrivalStation,
            DepartureDate,
            TotalBookings
        FROM FlightBookings
        ORDER BY DepartureDate;
        """
        
        try:
            df = pd.read_sql(query, self.nav_conn)
            # Generate flight keys using the improved method
            df['flight_key'] = df.apply(self.generate_flight_key, axis=1)
            
            self.logger.info(f"Retrieved booking data for {len(df)} flights")
            
            return df
        except Exception as e:
            error_msg = f"Error fetching flights with bookings: {str(e)}"
            self.logger.error(error_msg)
            traceback.print_exc()
            return pd.DataFrame()

    def track_booking_changes(self, flights_df):
        """
        Track booking changes and identify flights that cross the threshold.
        With the new design, this method returns flights that have received
        at least 'threshold' new bookings since the last time they crossed the threshold.
        Uses Redis cache instead of SQL database for tracking.
        """
        if flights_df.empty:
            return pd.DataFrame()
            
        current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.logger.info(f"Tracking booking changes with threshold = {self.booking_threshold}")
        
        # Get existing tracking data from Redis cache
        tracking_dict = self._get_all_flight_tracking_data()
        self.logger.info(f"Retrieved {len(tracking_dict)} existing flight tracking records from cache")
        
        # Process each flight and prepare cache operations
        threshold_crossings = []
        updates = []
        new_flights = []
        
        for _, flight in flights_df.iterrows():
            flight_key = flight['flight_key']
            total_bookings = int(flight['TotalBookings'])  # Ensure integer type
            
            # If flight is new to our tracking
            if flight_key not in tracking_dict:
                # New flight - add to tracking (optimized to store only necessary data)
                new_flight_data = {
                    'flight_key': flight_key,
                    'last_check_time': current_datetime,
                    'total_bookings': total_bookings,
                    'previous_bookings': 0,
                    'last_threshold_date': current_datetime if total_bookings >= self.booking_threshold else None,
                    'last_threshold_bookings': total_bookings if total_bookings >= self.booking_threshold else None
                }
                
                new_flights.append(new_flight_data)
                
                # If new flight already crosses threshold, add to threshold crossings
                if total_bookings >= self.booking_threshold:
                    # Extract flight info from key for threshold crossing record
                    flight_info = self.parse_flight_key(flight_key)
                    
                    threshold_crossings.append({
                        'flight_key': flight_key,
                        'departure_station': flight_info['departure_station'],
                        'arrival_station': flight_info['arrival_station'],
                        'flight_number': flight_info['flight_number'],
                        'departure_date': flight_info['departure_date'],
                        'total_bookings': total_bookings,
                        'new_bookings': total_bookings,  # All bookings are "new" for a new flight
                        'threshold_crossing_date': current_datetime
                    })
                    self.logger.info(f"NEW THRESHOLD CROSSING: {flight_key} with {total_bookings} bookings")
                
            else:
                # Existing flight - update tracking with robust error handling
                record = tracking_dict[flight_key]
                
                # Get previous values safely
                try:
                    previous_bookings = int(record.get('total_bookings', 0))
                except (ValueError, TypeError):
                    previous_bookings = 0
                    self.logger.info(f"Warning: Invalid total_bookings value for {flight_key}. Using 0.")
                
                # Get last threshold information
                last_threshold_date = record.get('last_threshold_date')
                
                try:
                    if record.get('last_threshold_bookings') is None:
                        last_threshold_bookings = 0
                    else:
                        last_threshold_bookings = int(record.get('last_threshold_bookings', 0))
                except (ValueError, TypeError):
                    last_threshold_bookings = 0
                    self.logger.info(f"Warning: Invalid last_threshold_bookings value for {flight_key}. Using 0.")
                
                # Calculate new bookings since last check and last threshold
                new_bookings_since_last_check = total_bookings - previous_bookings
                new_bookings_since_last_threshold = total_bookings - last_threshold_bookings if last_threshold_bookings is not None else total_bookings
                
                # Check if we've crossed the threshold again
                # Key change: We look for bookings exceeding the THRESHOLD from the last threshold point
                threshold_crossed_again = new_bookings_since_last_threshold >= self.booking_threshold
                
                # Update tracking record (optimized to store only necessary data)
                update_data = {
                    'flight_key': flight_key,
                    'last_check_time': current_datetime,
                    'total_bookings': total_bookings,
                    'previous_bookings': previous_bookings,
                    'last_threshold_date': last_threshold_date,
                    'last_threshold_bookings': last_threshold_bookings
                }
                
                # If threshold crossed again, update the last crossing info
                if threshold_crossed_again:
                    update_data['last_threshold_date'] = current_datetime
                    update_data['last_threshold_bookings'] = total_bookings
                    
                    # Extract flight info from key for threshold crossing record
                    flight_info = self.parse_flight_key(flight_key)
                    
                    threshold_crossings.append({
                        'flight_key': flight_key,
                        'departure_station': flight_info['departure_station'],
                        'arrival_station': flight_info['arrival_station'],
                        'flight_number': flight_info['flight_number'],
                        'departure_date': flight_info['departure_date'],
                        'total_bookings': total_bookings,
                        'new_bookings': new_bookings_since_last_threshold,
                        'threshold_crossing_date': current_datetime
                    })
                    self.logger.info(f"THRESHOLD CROSSED AGAIN: {flight_key} with {new_bookings_since_last_threshold} new bookings since last threshold (total: {total_bookings})")
                else:
                    # Keep the previous threshold info
                    update_data['last_threshold_date'] = last_threshold_date
                    update_data['last_threshold_bookings'] = last_threshold_bookings
                
                updates.append(update_data)
        
        # Execute Redis cache operations
        self._update_cache_flight_data(new_flights, updates)
        
        # Convert threshold crossings to DataFrame
        threshold_df = pd.DataFrame(threshold_crossings)
        
        self.logger.info(f"Processed {len(flights_df)} flights, found {len(threshold_crossings)} threshold crossings")
        
        return threshold_df
    
    def _get_all_flight_tracking_data(self):
        """
        Retrieve all flight tracking data from Redis cache under the flight_tracker folder.
        Returns a dictionary with flight_key as the key.
        """
        try:
            # Get all flight keys from the flight_tracker folder
            all_flight_keys = self.cache_client.smembers(self.all_flights_key)
            
            if not all_flight_keys:
                self.logger.info(f"No existing flight data found in {self.flight_tracker_folder} folder")
                return {}
            
            tracking_dict = {}
            
            # Get flight data for each key within the flight_tracker folder
            for flight_key in all_flight_keys:
                flight_key_str = flight_key.decode('utf-8') if isinstance(flight_key, bytes) else flight_key
                cache_key = f"{self.flight_tracker_prefix}{flight_key_str}"
                
                flight_data = self.cache_client.get(cache_key)
                if flight_data:
                    flight_data_str = flight_data.decode('utf-8') if isinstance(flight_data, bytes) else flight_data
                    flight_data_dict = json.loads(flight_data_str)
                    tracking_dict[flight_key_str] = flight_data_dict
            
            return tracking_dict
            
        except Exception as e:
            self.logger.error(f"Error retrieving flight tracking data from {self.flight_tracker_folder} folder: {str(e)}")
            traceback.print_exc()
            return {}
    
    def _update_cache_flight_data(self, new_flights, updates):
        """
        Update flight tracking data in Redis cache within the flight_tracker folder.
        Handles both new flights and updates to existing flights.
        Sets TTL based on departure date to automatically remove flights after they depart.
        """
        try:
            pipeline = self.cache_client.pipeline()
            
            # Process new flights
            for flight_data in new_flights:
                flight_key = flight_data['flight_key']
                cache_key = f"{self.flight_tracker_prefix}{flight_key}"
                
                # Add to set of all flight keys in the flight_tracker folder
                pipeline.sadd(self.all_flights_key, flight_key)
                
                # Clean up data for storage - handle None values
                clean_data = {}
                for key, value in flight_data.items():
                    if value is not None:
                        clean_data[key] = value
                    else:
                        clean_data[key] = ""
                
                # Store flight data as JSON in the flight_tracker folder
                pipeline.set(cache_key, json.dumps(clean_data))
                
                # Set TTL based on departure date
                flight_info = self.parse_flight_key(flight_key)
                self._set_flight_ttl(pipeline, cache_key, flight_info['departure_date'])
            
            # Process updates to existing flights
            for flight_data in updates:
                flight_key = flight_data['flight_key']
                cache_key = f"{self.flight_tracker_prefix}{flight_key}"
                
                # Get existing data and update it
                existing_data = self.cache_client.get(cache_key)
                if existing_data:
                    existing_data_str = existing_data.decode('utf-8') if isinstance(existing_data, bytes) else existing_data
                    existing_dict = json.loads(existing_data_str)
                    
                    # Update with new values
                    for key, value in flight_data.items():
                        if value is not None:
                            existing_dict[key] = value
                    
                    # Store updated data in the flight_tracker folder
                    pipeline.set(cache_key, json.dumps(existing_dict))
                    
                    # Reset TTL based on departure date
                    flight_info = self.parse_flight_key(flight_key)
                    self._set_flight_ttl(pipeline, cache_key, flight_info['departure_date'])
            
            # Execute all commands as a transaction
            pipeline.execute()
            self.logger.info(f"Successfully updated {self.flight_tracker_folder} folder with {len(new_flights)} new flights and {len(updates)} updates")
            
        except Exception as e:
            error_msg = f"Error updating flight tracking data in {self.flight_tracker_folder} folder: {str(e)}"
            self.logger.error(error_msg)
            traceback.print_exc()
            raise
            
    def _set_flight_ttl(self, pipeline, cache_key, departure_date_str):
        """
        Set TTL for a flight key in Redis based on its departure date.
        Flights will be automatically removed from Redis:
        - 1 day after departure for departed flights
        - 1 day after the 30-day threshold for future flights
        
        Args:
            pipeline: Redis pipeline
            cache_key: Redis key to set TTL for
            departure_date_str: Departure date in YYYY-MM-DD format
        """
        try:
            # Parse departure date
            departure_date = datetime.strptime(departure_date_str, '%Y-%m-%d').date()
            
            # Current date
            current_date = datetime.now().date()
            
            # Calculate days until departure
            days_until_departure = (departure_date - current_date).days
            
            # For flights that have departed or are within 30 days, set TTL to 1 day (86400 seconds)
            if days_until_departure <= 30:
                ttl_seconds = 86400  # 1 day in seconds
            else:
                # For future flights beyond 30 days, set TTL to expiry at 30 days before departure plus 1 day
                ttl_seconds = (days_until_departure - 30) * 86400 + 86400  # in seconds
            
            # Set the TTL
            pipeline.expire(cache_key, ttl_seconds)
            
        except Exception as e:
            # Log error but continue - TTL is a nice-to-have, not critical
            self.logger.warning(f"Error setting TTL for {cache_key}: {str(e)}. Flight will not auto-expire.")
            # Don't re-raise the exception to avoid disrupting the main process

    def get_combined_market_list(self):
        """
        Get and combine both domestic and international market lists,
        using explicit column selection to ensure consistent ordering.
        """
        self.logger.info("Getting combined market list (domestic + international)")
        
        try:
            # Define specific columns to select in the same order for both tables
            # This list is based on your provided column information
            specific_columns = [
                "Origin", "Destin", "FlightNumber", "PerType", "PerStart", "PerEnd", 
                "DOW", "TimeWindowStart", "TimeWindowEnd", "DaySpan", "CurveID",
                "CarrExlusionB2C", "CarrExlusionB2B", "flightExclusionB2C", "flightExclusionB2B",
                "fareAnchor", "hardAnchor", "plfThreshold", "fareOffset", "FirstRBDAlloc",
                "OtherRBDAlloc", "B2BBackstop", "B2CBackstop", "B2BFactor", "obSeats",
                "obFare","SkippingFactor", "UUID", "OwnFareFlag", "analystName", "AutoTimeRangeFlag",
                "OverBooking", "openingFares", "profileFares", "rbdPushFlag", "B2CTolerance",
                "B2BTolerance", "distressInventoryFlag", "seriesBlock", "autoGroup",
                "autoBackstopFlag", "tbfFlag", "CugFlag", "MktFareFlag"
            ]
            
            # Create a comma-separated list of columns
            columns_str = ", ".join(specific_columns)
            
            # Create a combined query using UNION ALL with the specific columns
            combined_query = text(f"""
                SELECT {columns_str}
                FROM QP_DW_RMALLOC.market_list
                WHERE STR_TO_DATE(PerStart, '%m/%d/%Y') > CURDATE() + INTERVAL 30 DAY
                
                UNION ALL
                
                SELECT {columns_str}
                FROM QP_DW_RMALLOC.market_list_international
                WHERE STR_TO_DATE(PerStart, '%m/%d/%Y') > CURDATE() + INTERVAL 30 DAY
                
                ORDER BY PerStart ASC
            """)
            
            # Execute the query
            with self.rm_rd_conn.connect() as connection:
                combined_result = connection.execute(combined_query)
                combined_df = pd.DataFrame(combined_result.fetchall(), columns=specific_columns)
            
            self.logger.info(f"Combined market list has {len(combined_df)} records")
            
            # Convert PerStart to datetime for later comparison
            combined_df['PerStart_DT'] = pd.to_datetime(combined_df['PerStart'])
                
            return combined_df
        
        except Exception as e:
            error_msg = f"Error getting combined market list: {str(e)}"
            self.logger.error(error_msg)
            traceback.print_exc()
            return pd.DataFrame()

    def match_flights_with_market_list(self, threshold_df, market_list_df):
        """
        Match threshold crossing flights with market list entries.
        Returns matched market list entries that should be added to market_list_thirty.
        """
        if threshold_df.empty:
            self.logger.info("No threshold-crossing flights to match with market list")
            return pd.DataFrame()
            
        if market_list_df.empty:
            self.logger.info("Empty market list - cannot match flights")
            return pd.DataFrame()
            
        self.logger.info("Matching threshold-crossing flights with market list")
        self.logger.info(f"Threshold-crossing flights to match: {len(threshold_df)}")
        
        # Ensure consistent data types for joining
        threshold_df['flight_number'] = threshold_df['flight_number'].astype(str).str.strip()
        market_list_df['FlightNumber'] = market_list_df['FlightNumber'].astype(str).str.strip()
        
        # Normalize flight numbers - remove spaces and standardize format
        threshold_df['flight_number_norm'] = threshold_df['flight_number'].str.replace(r'\s+|-', '', regex=True)
        market_list_df['FlightNumber_norm'] = market_list_df['FlightNumber'].str.replace(r'\s+|-', '', regex=True)
        
        # Convert departure_date to datetime for comparison
        threshold_df['departure_date_dt'] = pd.to_datetime(threshold_df['departure_date'])
        
        # Perform the matching - strict exact date matching only
        matches = []
        
        for _, flight in threshold_df.iterrows():
            flight_key = f"{flight['departure_station']}-{flight['arrival_station']} Flight {flight['flight_number']} on {flight['departure_date']}"
            
            # Find exact route, flight number, AND date match (exact day)
            exact_matches = market_list_df[
                (market_list_df['Origin'] == flight['departure_station']) &
                (market_list_df['Destin'] == flight['arrival_station']) &
                (market_list_df['FlightNumber_norm'] == flight['flight_number_norm']) &
                (market_list_df['PerStart_DT'].dt.date == flight['departure_date_dt'].date())
            ].copy()
            
            # If we found exact matches (including date), use them
            if not exact_matches.empty:
                self.logger.info(f"Found exact date match for {flight_key}")
                # Get the first exact match (should be only one, but just in case)
                exact_match = exact_matches.iloc[0].to_dict()
                
                # Remove utility columns
                exact_match.pop('PerStart_DT', None)
                exact_match.pop('FlightNumber_norm', None)
                
                matches.append(exact_match)
                
                self.logger.info(f"Exact match: {flight_key} with market list entry for {exact_match['Origin']}-{exact_match['Destin']} "
                      f"Flight {exact_match['FlightNumber']} on {exact_match['PerStart']}")
            else:
                # No exact match found - log and skip
                route_matches = market_list_df[
                    (market_list_df['Origin'] == flight['departure_station']) &
                    (market_list_df['Destin'] == flight['arrival_station']) &
                    (market_list_df['FlightNumber_norm'] == flight['flight_number_norm'])
                ]
                
                if route_matches.empty:
                    self.logger.info(f"No market list entry found for route and flight number: {flight_key}")
                else:
                    # There are matching routes but not the exact date
                    closest_dates = route_matches['PerStart'].tolist()
                    self.logger.info(f"No exact date match for {flight_key}. Available dates in market list: {closest_dates}")
        
        # Convert to DataFrame
        matches_df = pd.DataFrame(matches) if matches else pd.DataFrame()
        
        # Log matching results
        matched_count = len(matches_df)
        all_count = len(threshold_df)
        
        self.logger.info(f"Found {matched_count} exact matches out of {all_count} threshold-crossing flights")
        
        if matched_count < all_count:
            self.logger.info(f"{all_count - matched_count} flights did not have exact matches in market list and will be skipped")
        
        return matches_df

    def update_market_list_thirty(self, market_list_thirty_df):
        """
        Update the market_list_thirty table with newly matched flights.
        First truncates the table, then inserts all matches.
        """
        self.logger.info("Updating market_list_thirty table")
        
        # Truncate the table
        truncate_query = text("TRUNCATE TABLE QP_DW_RMALLOC.market_list_thirty")
        
        with self.rm_rd_conn.connect() as connection:
            # Start transaction
            trans = connection.begin()
            try:
                # Truncate first
                connection.execute(truncate_query)
                self.logger.info("Truncated market_list_thirty table")
                
                # Insert if we have matches
                if not market_list_thirty_df.empty:
                    # Drop utility columns used for matching
                    drop_cols = ['date_diff', 'PerStart_DT', 'FlightNumber_norm']
                    market_list_thirty_df = market_list_thirty_df.drop(
                        [col for col in drop_cols if col in market_list_thirty_df.columns], 
                        axis=1
                    )
                    
                    # Get column names from the dataframe
                    columns = market_list_thirty_df.columns.tolist()
                    
                    # Prepare parameterized query
                    param_names = ', '.join([':' + col for col in columns])
                    columns_str = ', '.join(columns)
                    
                    insert_query = text(f"""
                        INSERT INTO QP_DW_RMALLOC.market_list_thirty 
                        ({columns_str}) 
                        VALUES ({param_names})
                    """)
                    
                    # Convert DataFrame to list of dictionaries for batch insert
                    records = market_list_thirty_df.to_dict('records')
                    
                    connection.execute(insert_query, records)
                    
                    self.logger.info(f"Successfully inserted {len(market_list_thirty_df)} records to market_list_thirty")
                else:
                    self.logger.info("No records to insert into market_list_thirty")
                
                # Commit transaction
                trans.commit()
                
            except Exception as e:
                # Rollback on error
                trans.rollback()
                error_msg = f"Error updating market_list_thirty: {str(e)}"
                self.logger.error(error_msg)
                raise
    
    def update_current_version(self, version_id):
        """
        Update the currentVersion table with the new UUID for market_list_thirty
        
        Args:
            version_id (str): UUID to use as the current version
        """
        self.logger.info(f"Updating currentVersion table with new version: {version_id}")
        
        update_query = text("""
            UPDATE QP_DW_RMALLOC.currentVersion
            SET curr_version = :version_id
            WHERE tableName = 'market_list_thirty'
        """)
        
        # If no rows are updated, insert a new row
        insert_query = text("""
            INSERT INTO QP_DW_RMALLOC.currentVersion (tableName, curr_version)
            SELECT 'market_list_thirty', :version_id
            WHERE NOT EXISTS (
                SELECT 1 FROM QP_DW_RMALLOC.currentVersion 
                WHERE tableName = 'market_list_thirty'
            )
        """)
        
        with self.rm_rd_conn.connect() as connection:
            # Start transaction
            trans = connection.begin()
            try:
                # First try to update
                result = connection.execute(update_query, {"version_id": version_id})
                
                # If no rows were updated, insert a new row
                if result.rowcount == 0:
                    connection.execute(insert_query, {"version_id": version_id})
                
                # Commit transaction
                trans.commit()
                self.logger.info(f"Successfully updated currentVersion table")
                
            except Exception as e:
                # Rollback on error
                trans.rollback()
                error_msg = f"Error updating currentVersion table: {str(e)}"
                self.logger.error(error_msg)
                raise
    
    def upload_to_s3(self, df, version_id):
        """
        Upload market list thirty data to S3
        
        Args:
            df (DataFrame): The market list data to upload
            version_id (str): UUID to use in the file name
        
        Returns:
            bool: True if upload successful, False otherwise
        """
        try:
            self.logger.info(f"Preparing to upload market_list_thirty data to S3")
            
            # Create S3 client
            s3_client = boto3.client('s3')
            
            # Define bucket and file path
            bucket_name = configsettings.S3_BUCKET_NAME
            
            # Create the path with the market_list_thirty folder
            s3_folder_path = f"{configsettings.S3_FILE_PATH}rm-admin/market_list_thirty"
            file_name = f"{version_id}_market_list_thirty"
            s3_file_path = f"{s3_folder_path}/{file_name}"
            
            # Convert DataFrame to CSV
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_content = csv_buffer.getvalue()
            
            # Upload to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_file_path,
                Body=csv_content
            )
            
            self.logger.info(f"Successfully uploaded market_list_thirty data to S3: s3://{bucket_name}/{s3_file_path}")
            return True
            
        except Exception as e:
            error_msg = f"Error uploading market_list_thirty data to S3: {str(e)}"
            self.logger.error(error_msg)
            traceback.print_exc()
            return False

    def runScheduleThirty(self, run_id):
        """
        Invoke the FETCH_INPUTS_LAMBDA asynchronously with the provided run_id and other parameters.
        Uses asynchronous invocation to avoid timeout issues and prevent duplicate executions.
        """
        self.logger.info(f"Invoking Lambda function asynchronously for Schedule Thirty with run_id: {run_id}")
        
        try:
            # Initialize the Lambda client
            lambda_client = boto3.client('lambda')
            
            # Get the Lambda function name from config settings
            lambda_function_name = configsettings.FETCH_INPUTS_LAMBDA
            
            # Prepare the payload in the same format as the example
            payload = {
                "body": json.dumps({
                    "run_id": run_id,
                    "start_interval": 0,
                    "end_interval": 365,
                    "run_type": "Scheduled_Thirty",
                    "market_list_table_name": "market_list_thirty",
                    "route_type": "direct",
                    "update_navitaire_method": "api"
                })
            }
            
            # Invoke the Lambda function asynchronously
            self.logger.info(f"Invoking Lambda asynchronously: {lambda_function_name}")
            response = lambda_client.invoke(
                FunctionName=lambda_function_name,
                InvocationType='Event',  # Asynchronous execution
                Payload=json.dumps(payload)
            )
            
            # Log the response for debugging
            self.logger.info(f"Lambda async invocation response: {response}")
            
            # For asynchronous invocation, we only get a submission confirmation
            if response['StatusCode'] == 202:  # 202 = Accepted (for async invocations)
                self.logger.info(f"Lambda function {lambda_function_name} invoked successfully (async)")
                return {"status": "success", "message": "Lambda invoked asynchronously", "statusCode": 202}
            else:
                # Handle non-202 status codes
                error_msg = f"Lambda async invocation failed with status code: {response['StatusCode']}"
                self.logger.error(error_msg)
                return {"status": "error", "message": error_msg, "statusCode": response['StatusCode']}
            
        except Exception as e:
            error_msg = f"Error invoking Lambda function asynchronously: {str(e)}"
            self.logger.error(error_msg)
            traceback.print_exc()
            raise
    
    def run(self):
        """Main execution method for the ScheduleThirty process."""
        try:
            run_id = str(uuid4())
            self.logger.info(f"Starting Schedule Thirty process with run_id: {run_id}")

            flights_df = self.get_flights_with_bookings()

            threshold_df = self.track_booking_changes(flights_df)

            market_list_df = self.get_combined_market_list()
            
            market_list_thirty_df = self.match_flights_with_market_list(threshold_df, market_list_df)
            
            self.update_market_list_thirty(market_list_thirty_df)
            
            if not market_list_thirty_df.empty:
                # Generate a version UUID for this market list update
                version_id = str(uuid4())
                self.logger.info(f"Generated version ID for market_list_thirty: {version_id}")
                
                # Update the currentVersion table with the new UUID
                self.update_current_version(version_id)
                
                # Upload the market list data to S3
                self.upload_to_s3(market_list_thirty_df, version_id)
                
                # Invoke the Lambda function
                self.runScheduleThirty(run_id)
            
            self.logger.info("Schedule Thirty process completed successfully")
            
            return True
            
        except Exception as e:
            error_msg = f"Error in Schedule Thirty process: {str(e)}"
            self.logger.error(error_msg)
            traceback.print_exc()
            return False