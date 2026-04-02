import pandas as pd
import logging
import os
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
        self.mongoconn = params['mongoconn']  # Single MongoDB connection for application DB
        self.nav_conn = params['navconn']     # Keep for Navitaire SQL Server (external)
        self.dw_conn = params['dwconn']       # Keep for Data Warehouse SQL Server (external)
        self.cache_client = params['cache_client']
        
        # MongoDB database
        self.db = self.mongoconn["qp_dw_rmalloc"]  # or self.mongoconn['your_db_name']
        
        self.nav_db_name = configsettings.NAV_DB_NAME
        self.nav_db_dw_name = configsettings.NAV_DB_DW_NAME
        
        # Update ConstantsUtils to use MongoDB
        self.constants = ConstantsUtils(self.mongoconn)
        self.booking_threshold = self.constants.BOOKINGS_THRESHOLD
        
        # Redis cache key structure with folder
        self.flight_tracker_folder = "flight_tracker"
        self.flight_tracker_prefix = f"{self.flight_tracker_folder}:flight:"
        self.all_flights_key = f"{self.flight_tracker_folder}:all_flight_keys"
    
    @staticmethod
    def generate_flight_key(row):
        """Generate a unique key for each flight."""
        flight_number = str(row['FlightNumber']).strip()
        
        if isinstance(row['DepartureDate'], str):
            departure_date = row['DepartureDate']
        else:
            departure_date = row['DepartureDate'].strftime('%Y-%m-%d')
        
        return f"{row['DepartureStation']}{row['ArrivalStation']}_{flight_number}_{departure_date}"
    
    @staticmethod
    def parse_flight_key(flight_key):
        """Extract flight information from the flight key."""
        try:
            parts = flight_key.split('_')
            stations = parts[0]
            departure_station = stations[:3]
            arrival_station = stations[3:] if len(stations) > 3 else ""
            flight_number = parts[1] if len(parts) > 1 else ""
            departure_date = parts[2] if len(parts) > 2 else ""
            
            return {
                "departure_station": departure_station,
                "arrival_station": arrival_station,
                "flight_number": flight_number,
                "departure_date": departure_date
            }
        except Exception as e:
            return {
                "departure_station": "",
                "arrival_station": "",
                "flight_number": "",
                "departure_date": ""
            }

    def get_flights_with_bookings(self):
        """
        Get all flights and their booking data from Navitaire database
        Note: nav_conn remains SQL Server as it's an external system
        """
        today = datetime.now()
        departure_start_date = (today + timedelta(days=30)).strftime('%Y-%m-%d')
        departure_end_date = (today + timedelta(days=365)).strftime('%Y-%m-%d')
        
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
            df['flight_key'] = df.apply(self.generate_flight_key, axis=1)
            return df
        except Exception as e:
            error_msg = f"Error fetching flights with bookings: {str(e)}"
            self.logger.error(error_msg)
            traceback.print_exc()
            return pd.DataFrame()

    def track_booking_changes(self, flights_df):
        """Track booking changes using Redis cache."""
        if flights_df.empty:
            return pd.DataFrame()
            
        current_datetime = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        
        tracking_dict = self._get_all_flight_tracking_data()
        
        threshold_crossings = []
        updates = []
        new_flights = []
        
        for _, flight in flights_df.iterrows():
            flight_key = flight['flight_key']
            total_bookings = int(flight['TotalBookings'])
            
            if flight_key not in tracking_dict:
                new_flight_data = {
                    'flight_key': flight_key,
                    'last_check_time': current_datetime,
                    'total_bookings': total_bookings,
                    'previous_bookings': 0,
                    'last_threshold_date': current_datetime if total_bookings >= self.booking_threshold else None,
                    'last_threshold_bookings': total_bookings if total_bookings >= self.booking_threshold else None
                }
                
                new_flights.append(new_flight_data)
                
                if total_bookings >= self.booking_threshold:
                    flight_info = self.parse_flight_key(flight_key)
                    
                    threshold_crossings.append({
                        'flight_key': flight_key,
                        'departure_station': flight_info['departure_station'],
                        'arrival_station': flight_info['arrival_station'],
                        'flight_number': flight_info['flight_number'],
                        'departure_date': flight_info['departure_date'],
                        'total_bookings': total_bookings,
                        'new_bookings': total_bookings,
                        'threshold_crossing_date': current_datetime
                    })
                
            else:
                record = tracking_dict[flight_key]
                
                try:
                    previous_bookings = int(record.get('total_bookings', 0))
                except (ValueError, TypeError):
                    previous_bookings = 0
                
                last_threshold_date = record.get('last_threshold_date')
                
                try:
                    if record.get('last_threshold_bookings') is None:
                        last_threshold_bookings = 0
                    else:
                        last_threshold_bookings = int(record.get('last_threshold_bookings', 0))
                except (ValueError, TypeError):
                    last_threshold_bookings = 0
                
                new_bookings_since_last_check = total_bookings - previous_bookings
                new_bookings_since_last_threshold = total_bookings - last_threshold_bookings if last_threshold_bookings is not None else total_bookings
                
                threshold_crossed_again = new_bookings_since_last_threshold >= self.booking_threshold
                
                update_data = {
                    'flight_key': flight_key,
                    'last_check_time': current_datetime,
                    'total_bookings': total_bookings,
                    'previous_bookings': previous_bookings,
                    'last_threshold_date': last_threshold_date,
                    'last_threshold_bookings': last_threshold_bookings
                }
                
                if threshold_crossed_again:
                    update_data['last_threshold_date'] = current_datetime
                    update_data['last_threshold_bookings'] = total_bookings
                    
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
                else:
                    update_data['last_threshold_date'] = last_threshold_date
                    update_data['last_threshold_bookings'] = last_threshold_bookings
                
                updates.append(update_data)
        
        self._update_cache_flight_data(new_flights, updates)
        
        threshold_df = pd.DataFrame(threshold_crossings)
        
        self.logger.info(f"Processed {len(flights_df)} flights, found {len(threshold_crossings)} threshold crossings")
        
        return threshold_df
    
    def _get_all_flight_tracking_data(self):
        """Retrieve all flight tracking data from Redis cache."""
        try:
            all_flight_keys = self.cache_client.smembers(self.all_flights_key)
            
            if not all_flight_keys:
                self.logger.info(f"No existing flight data found in {self.flight_tracker_folder} folder")
                return {}
            
            tracking_dict = {}
            
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
            self.logger.error(f"Error retrieving flight tracking data: {str(e)}")
            traceback.print_exc()
            return {}
    
    def _update_cache_flight_data(self, new_flights, updates):
        """Update flight tracking data in Redis cache."""
        try:
            pipeline = self.cache_client.pipeline()
            
            for flight_data in new_flights:
                flight_key = flight_data['flight_key']
                cache_key = f"{self.flight_tracker_prefix}{flight_key}"
                
                pipeline.sadd(self.all_flights_key, flight_key)
                
                clean_data = {}
                for key, value in flight_data.items():
                    if value is not None:
                        clean_data[key] = value
                    else:
                        clean_data[key] = ""
                
                pipeline.set(cache_key, json.dumps(clean_data))
                
                flight_info = self.parse_flight_key(flight_key)
                self._set_flight_ttl(pipeline, cache_key, flight_info['departure_date'])
            
            for flight_data in updates:
                flight_key = flight_data['flight_key']
                cache_key = f"{self.flight_tracker_prefix}{flight_key}"
                
                existing_data = self.cache_client.get(cache_key)
                if existing_data:
                    existing_data_str = existing_data.decode('utf-8') if isinstance(existing_data, bytes) else existing_data
                    existing_dict = json.loads(existing_data_str)
                    
                    for key, value in flight_data.items():
                        if value is not None:
                            existing_dict[key] = value
                    
                    pipeline.set(cache_key, json.dumps(existing_dict))
                    
                    flight_info = self.parse_flight_key(flight_key)
                    self._set_flight_ttl(pipeline, cache_key, flight_info['departure_date'])
            
            pipeline.execute()
            self.logger.info(f"Successfully updated {len(new_flights)} new flights and {len(updates)} updates")
            
        except Exception as e:
            error_msg = f"Error updating flight tracking data: {str(e)}"
            self.logger.error(error_msg)
            traceback.print_exc()
            raise
            
    def _set_flight_ttl(self, pipeline, cache_key, departure_date_str):
        """Set TTL for a flight key in Redis based on departure date."""
        try:
            departure_date = datetime.strptime(departure_date_str, '%Y-%m-%d').date()
            current_date = datetime.now().date()
            days_until_departure = (departure_date - current_date).days
            
            if days_until_departure <= 30:
                ttl_seconds = 86400
            else:
                ttl_seconds = (days_until_departure - 30) * 86400 + 86400
            
            pipeline.expire(cache_key, ttl_seconds)
            
        except Exception as e:
            self.logger.warning(f"Error setting TTL for {cache_key}: {str(e)}")

    def get_combined_market_list(self):
        """
        Get combined market list from MongoDB collections
        """
        try:
            # First, fetch the column configuration from MongoDB
            config_collection = self.db['config_column_names']
            config_doc = config_collection.find_one(
                {'tableName': 'market_list_s3'},
                {'columns': 1, '_id': 0}
            )
            
            if not config_doc:
                # Fallback to hardcoded columns
                self.logger.warning("No column configuration found, using fallback columns")
                columns_list = [
                    'Origin', 'Destin', 'FlightNumber', 'PerType', 'PerStart', 'PerEnd',
                    'DOW', 'TimeWindowStart', 'TimeWindowEnd', 'DaySpan', 'CurveID',
                    'CarrExlusionB2C', 'CarrExlusionB2B', 'flightExclusionB2C', 'flightExclusionB2B',
                    'fareAnchor', 'hardAnchor', 'plfThreshold', 'fareOffset', 'FirstRBDAlloc',
                    'OtherRBDAlloc', 'B2BBackstop', 'B2CBackstop', 'B2BFactor', 'obSeats',
                    'obFare', 'SkippingFactor', 'UUID', 'OwnFareFlag', 'analystName', 'AutoTimeRangeFlag',
                    'OverBooking', 'openingFares', 'profileFares', 'rbdPushFlag', 'B2CTolerance',
                    'B2BTolerance', 'distressInventoryFlag', 'seriesBlock', 'autoGroup',
                    'autoBackstopFlag', 'tbfFlag', 'CugFlag', 'MktFareFlag', 'directFlag', 'TBFEnhFlag', 
                    'defaultMlFlag', 'b2bStrategy', 'Outbound_Stop', 'Marketfare_Upsell'
                ]
            else:
                columns_str = config_doc['columns']
                columns_list = [col.strip() for col in columns_str.split(',')]
                self.logger.info(f"Retrieved column configuration from config_column_names collection")
            
            # Calculate the date threshold (30 days from now)
            thirty_days_from_now = datetime.now() + timedelta(days=30)
            
            # Fetch from market_list collection
            market_list_collection = self.db['market_list']
            projection = {col: 1 for col in columns_list}
            projection['_id'] = 0
            
            cursor1 = market_list_collection.find({}, projection)
            df1 = pd.DataFrame(list(cursor1))
            
            # Fetch from market_list_international collection
            market_list_intl_collection = self.db['market_list_international']
            cursor2 = market_list_intl_collection.find({}, projection)
            df2 = pd.DataFrame(list(cursor2))
            
            # Combine the dataframes
            combined_df = pd.concat([df1, df2], ignore_index=True)
            
            if combined_df.empty:
                self.logger.warning("Combined market list is empty")
                return pd.DataFrame()
            
            # Convert PerStart to datetime and filter
            if 'PerStart' in combined_df.columns:
                combined_df['PerStart_DT'] = pd.to_datetime(combined_df['PerStart'], format='%m/%d/%Y', errors='coerce')
                combined_df = combined_df[combined_df['PerStart_DT'] > thirty_days_from_now]
                combined_df = combined_df.sort_values('PerStart_DT')
            else:
                self.logger.warning("PerStart column not found in result set")
            
            self.logger.info(f"Combined market list has {len(combined_df)} records")
            
            return combined_df
        
        except Exception as e:
            error_msg = f"Error getting combined market list: {str(e)}"
            self.logger.error(error_msg)
            traceback.print_exc()
            return pd.DataFrame()

    def match_flights_with_market_list(self, threshold_df, market_list_df):
        """Match threshold crossing flights with market list entries."""
        if threshold_df.empty:
            self.logger.info("No threshold-crossing flights to match")
            return pd.DataFrame()
            
        if market_list_df.empty:
            self.logger.info("Empty market list - cannot match flights")
            return pd.DataFrame()
        
        threshold_df['flight_number'] = threshold_df['flight_number'].astype(str).str.strip()
        market_list_df['FlightNumber'] = market_list_df['FlightNumber'].astype(str).str.strip()
        
        threshold_df['flight_number_norm'] = threshold_df['flight_number'].str.replace(r'\s+|-', '', regex=True)
        market_list_df['FlightNumber_norm'] = market_list_df['FlightNumber'].str.replace(r'\s+|-', '', regex=True)
        
        threshold_df['departure_date_dt'] = pd.to_datetime(threshold_df['departure_date'])
        
        matches = []
        
        for _, flight in threshold_df.iterrows():
            flight_key = f"{flight['departure_station']}-{flight['arrival_station']} Flight {flight['flight_number']} on {flight['departure_date']}"
            
            exact_matches = market_list_df[
                (market_list_df['Origin'] == flight['departure_station']) &
                (market_list_df['Destin'] == flight['arrival_station']) &
                (market_list_df['FlightNumber_norm'] == flight['flight_number_norm']) &
                (market_list_df['PerStart_DT'].dt.date == flight['departure_date_dt'].date())
            ].copy()
            
            if not exact_matches.empty:
                self.logger.info(f"Found exact date match for {flight_key}")
                exact_match = exact_matches.iloc[0].to_dict()
                exact_match.pop('PerStart_DT', None)
                exact_match.pop('FlightNumber_norm', None)
                matches.append(exact_match)
            else:
                route_matches = market_list_df[
                    (market_list_df['Origin'] == flight['departure_station']) &
                    (market_list_df['Destin'] == flight['arrival_station']) &
                    (market_list_df['FlightNumber_norm'] == flight['flight_number_norm'])
                ]
                
                if route_matches.empty:
                    self.logger.info(f"No market list entry found for: {flight_key}")
                else:
                    closest_dates = route_matches['PerStart'].tolist()
                    self.logger.info(f"No exact date match for {flight_key}. Available dates: {closest_dates}")
        
        matches_df = pd.DataFrame(matches) if matches else pd.DataFrame()
        
        matched_count = len(matches_df)
        all_count = len(threshold_df)
        
        if matched_count < all_count:
            self.logger.info(f"{all_count - matched_count} flights did not have exact matches")
        
        return matches_df

    def update_market_list_thirty(self, market_list_thirty_df):
        """
        Update market_list_thirty collection in MongoDB
        """
        try:
            collection = self.db['market_list_thirty']
            
            # Drop the entire collection (equivalent to TRUNCATE)
            collection.drop()
            
            # Insert if we have matches
            if not market_list_thirty_df.empty:
                # Drop utility columns
                drop_cols = ['date_diff', 'PerStart_DT', 'FlightNumber_norm']
                market_list_thirty_df = market_list_thirty_df.drop(
                    [col for col in drop_cols if col in market_list_thirty_df.columns],
                    axis=1
                )
                
                # Convert to records
                records = market_list_thirty_df.to_dict('records')
                
                # Insert documents
                collection.insert_many(records)
                
                self.logger.info(f"Successfully inserted {len(records)} records to market_list_thirty")
            else:
                self.logger.info("No records to insert into market_list_thirty")
                
        except Exception as e:
            error_msg = f"Error updating market_list_thirty: {str(e)}"
            self.logger.error(error_msg)
            raise
    
    def update_current_version(self, version_id):
        """
        Update currentVersion collection in MongoDB
        """
        try:
            collection = self.db['currentVersion']
            
            # Upsert operation - update if exists, insert if not
            result = collection.update_one(
                {'tableName': 'market_list_thirty'},
                {
                    '$set': {
                        'curr_version': version_id,
                        'updated_at': datetime.utcnow()
                    }
                },
                upsert=True
            )
            
            self.logger.info(f"Successfully updated currentVersion collection")
            
        except Exception as e:
            error_msg = f"Error updating currentVersion collection: {str(e)}"
            self.logger.error(error_msg)
            raise
    
    def upload_to_s3(self, df, version_id):
        """Upload market list data to S3."""
        try:
            s3_client = boto3.client('s3')
            bucket_name = configsettings.S3_BUCKET_NAME
            s3_folder_path = f"{configsettings.S3_FILE_PATH}rm-admin/market_list_thirty"
            file_name = f"{version_id}_market_list_thirty"
            s3_file_path = f"{s3_folder_path}/{file_name}"
            
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_content = csv_buffer.getvalue()
            
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_file_path,
                Body=csv_content
            )
            
            self.logger.info(f"Successfully uploaded to S3: s3://{bucket_name}/{s3_file_path}")
            return True
            
        except Exception as e:
            error_msg = f"Error uploading to S3: {str(e)}"
            self.logger.error(error_msg)
            traceback.print_exc()
            return False

    def runScheduleThirty(self, run_id):
        """Invoke Lambda function."""
        try:
            lambda_client = boto3.client('lambda')
            lambda_function_name = configsettings.PREFETCH_INPUTS_LAMBDA
            
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
            
            response = lambda_client.invoke(
                FunctionName=lambda_function_name,
                InvocationType='Event',
                Payload=json.dumps(payload)
            )
            
            if response['StatusCode'] == 202:
                return {"status": "success", "message": "Lambda invoked asynchronously", "statusCode": 202}
            else:
                error_msg = f"Lambda invocation failed with status: {response['StatusCode']}"
                self.logger.error(error_msg)
                return {"status": "error", "message": error_msg, "statusCode": response['StatusCode']}
            
        except Exception as e:
            error_msg = f"Error invoking Lambda function: {str(e)}"
            self.logger.error(error_msg)
            traceback.print_exc()
            raise
    
    def run(self):
        """Main execution method."""
        try:
            run_id = str(uuid4())
            self.logger.info(f"Starting Schedule Thirty process with run_id: {run_id}")

            flights_df = self.get_flights_with_bookings()
            threshold_df = self.track_booking_changes(flights_df)
            market_list_df = self.get_combined_market_list()
            market_list_thirty_df = self.match_flights_with_market_list(threshold_df, market_list_df)
            
            self.update_market_list_thirty(market_list_thirty_df)
            
            if not market_list_thirty_df.empty:
                version_id = str(uuid4())
                self.logger.info(f"Generated version ID: {version_id}")
                
                self.update_current_version(version_id)
                self.upload_to_s3(market_list_thirty_df, version_id)
                self.runScheduleThirty(run_id)
            
            self.logger.info("Schedule Thirty process completed successfully")
            return True
            
        except Exception as e:
            error_msg = f"Error in Schedule Thirty process: {str(e)}"
            self.logger.error(error_msg)
            traceback.print_exc()
            return False