import json
import pytz
import configsettings
import pandas as pd
from datetime import date, datetime, timedelta, time
from constants_utils import ConstantsUtils
import traceback
import requests
import numpy as np
import warnings
warnings.filterwarnings('ignore')


class update_imputed_forecast:
    def __init__(self, logger, params, overrides, context):
        self.context = context
        self.logger = logger
        
        # Keep DW connection as SQL
        self.dw_conn = params['dwconn']
        
        # MongoDB connection for RM data (replaces rdconn and wrconn)
        self.mongo_client = params['mongoconn']
        self.rm_db = self.mongo_client['qp_dw_rmalloc']
        
        # MongoDB Collections (replaces rdconn and wrconn queries)
        self.market_list_collection = self.rm_db['market_list']
        self.market_list_intl_collection = self.rm_db['market_list_international']
        self.curves_collection = self.rm_db['curves']
        self.share_of_booking_collection = self.rm_db['config_share_of_booking']
        self.share_of_booking = self.get_share_of_booking()
        
        # Keep Navitaire connection as is
        self.navconn = params['navconn']
        
        self.cache_client = params['cache_client']
        self.constants = ConstantsUtils(self.rm_db)
        self.DEDUCTION_PER = self.constants.IMP_FORECAST_DEDUCTION
        self.LIST_LENGTH = 6
        self.session = requests.Session()
        self.session.headers.update(
            {"Ocp-Apim-Subscription-Key": configsettings.NAV_OCP_SUBSCRIPTION_KEY}
        )

    def run(self):
        try:
            self.logger.info("Starting the run method.")

            # Keep this as SQL (dw_conn)
            forecast_list = self.get_forecast()

            if forecast_list.empty:
                raise ValueError("No flights found in Marketlist")

            min_date = min(forecast_list['DepartureDate'])
            max_date = max(forecast_list['DepartureDate'])

            # Convert to MongoDB (was rdconn)
            market_list = self.getMarketList(min_date, max_date)

            # Keep as SQL (navconn)
            booked_loads = self.getCurrentBookedLoads(min_date, max_date)

            if market_list.empty:
                raise ValueError("No market data found for the given date range")
            if booked_loads.empty:
                raise ValueError("No booked load data found for the given date range")

            merge_list = self.left_join(forecast_list, market_list)
            merge_list = self.left_join(merge_list, booked_loads)

            columns_to_check = ['CurveID', 'adjustedCapacity', 'bookedPax', 'DepartureTime']
            missing_columns = [col for col in columns_to_check if col not in merge_list.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")
            merge_list = merge_list.dropna(subset=columns_to_check)

            self.get_curve_ids(merge_list)

            merge_list['NDO_Curr'] = (merge_list['DepartureDate'] - datetime.now().date()).dt.days

            merge_list['PLF'] = np.where(merge_list['adjustedCapacity'] > 0, 
                                        (merge_list['bookedPax'] * 100.0 / merge_list['adjustedCapacity']), 0)

            merge_list = self.calculate_expected_plf_vectorized(merge_list)
           
            merge_list['Variance'] = merge_list['PLF'] - merge_list['Expected_PLF']

            merge_list['ImputedForecastPLF'] = merge_list['forecastPLF'] + merge_list['Variance'] - merge_list['forecast_variance'] - self.DEDUCTION_PER

            merge_list['ImputedForecastPLF'] = merge_list['ImputedForecastPLF'].round(2)

            updated_count = self.update_redis_forecast(merge_list)

            self.logger.info(f'Updated {updated_count}/{len(merge_list)} flights')
            
            self.logger.info("Run method completed.")
        except Exception as e:
            self.logger.error(f"An error occurred in the run method: {traceback.format_exc()}")
   
    def update_redis_forecast(self, merge_list: pd.DataFrame, batch_size: int = 1000):
        updated_count = 0
        current_time = datetime.now().replace(microsecond=0)
       
        # Build all Redis keys upfront
        redis_keys = []
        key_to_row = {}
       
        for index, row in merge_list.iterrows():
            redis_key = f"ImputedForecastPLF:{row['Origin']}{row['Destination']}_{row['FlightNumber']}_{row['DepartureDate']}"
            redis_keys.append(redis_key)
            key_to_row[redis_key] = row
       
        # Process in batches
        for i in range(0, len(redis_keys), batch_size):
            batch_keys = redis_keys[i:i + batch_size]
           
            try:
                # Get all existing values at once using mget
                existing_values = self.cache_client.mget(batch_keys)
               
                # Build the update dictionary
                updates = {}
               
                for redis_key, existing_json in zip(batch_keys, existing_values):
                    try:
                        row = key_to_row[redis_key]
                        current_plf = float(row['ImputedForecastPLF'])
                       
                        # Parse existing PLF list
                        existing_plf_list = []
                        try:
                            existing_plf_list = json.loads(existing_json) if existing_json else []
                        except (json.JSONDecodeError, TypeError) as e:
                            self.logger.error(f"Invalid JSON in Redis for key {redis_key}: {e}")
                            existing_plf_list = []
                       
                        # Build new PLF list with timestamps
                        plf_history_list = self.build_plf_list_with_timestamps(
                            existing_plf_list, current_time, current_plf
                        )
                       
                        updates[redis_key] = json.dumps(plf_history_list)
                       
                    except Exception as e:
                        self.logger.error(f'Error processing key {redis_key}: {e}')
               
                # Set all values at once using mset
                if updates:
                    try:
                        # Use pipeline for mset + expire operations
                        pipe = self.cache_client.pipeline()
                        pipe.mset(updates)
                       
                        # Set expiration for each key
                        for redis_key in updates.keys():
                            pipe.expire(redis_key, 172800)
                       
                        pipe.execute()
                        updated_count += len(updates)
                       
                    except Exception as e:
                        self.logger.error(f"Error setting batch in Redis: {e}")
                       
            except Exception as e:
                self.logger.error(f'Error processing batch {i}-{i+batch_size}: {e}')
       
        return updated_count
 
    def build_plf_list_with_timestamps(self, existing_plf_list, current_time, current_plf):
        cutoff_time = current_time - timedelta(hours=7)
        
        filtered_plf_list = []
        for record in existing_plf_list:
            if record is not None and 'timestamp' in record:
                try:
                    record_time = datetime.fromisoformat(record['timestamp'])
                    if record_time >= cutoff_time:
                        filtered_plf_list.append(record)
                except ValueError:
                    continue
        
        new_record = {
            'value': current_plf,
            'timestamp': current_time.isoformat()
        }
        
        filtered_plf_list.append(new_record)
        
        try:
            filtered_plf_list.sort(key=lambda x: datetime.fromisoformat(x['timestamp']))
        except (ValueError, KeyError) as e:
            self.logger.warning(f"Error sorting PLF records by timestamp: {e}")
            return filtered_plf_list
       
        return filtered_plf_list
   
    def get_forecast(self):
        """
        KEPT AS SQL - Using dw_conn (Data Warehouse)
        """
        query = """
        SELECT forecastPLF, Variance as forecast_variance, Origin, Destination, FlightNumber, DepartureDate
        FROM RM_DW_DATA.forecast_results
        WHERE DepartureDate >= DATE(CONVERT_TZ(NOW(), 'UTC', 'Asia/Kolkata'))
          AND forecastPLF IS NOT NULL AND Variance IS NOT NULL
        """
        forecast_list = pd.read_sql(query, self.dw_conn)
        return forecast_list
    
    def getMarketList(self, min_date, max_date):
        """
        CONVERTED TO MONGODB - Replaces rdconn query
        """
        try:
            # Query for domestic market list
            if isinstance(min_date, date) and not isinstance(min_date, datetime):
                min_date = datetime.combine(min_date, datetime.min.time())
            if isinstance(max_date, date) and not isinstance(max_date, datetime):
                max_date = datetime.combine(max_date, datetime.max.time())  # Changed to max.time()

            query = {
                '$expr': {
                    '$and': [
                        {
                            '$gte': [
                                {'$dateFromString': {
                                    'dateString': '$PerStart',
                                    'format': '%m/%d/%Y'
                                }},
                                min_date
                            ]
                        },
                        {
                            '$lte': [
                                {'$dateFromString': {
                                    'dateString': '$PerStart',
                                    'format': '%m/%d/%Y'
                                }},
                                max_date
                            ]
                        }
                    ]
                }
            }
           
            projection = {
                '_id': 0,
                'Origin': 1,
                'Destin': 1,
                'PerStart': 1,
                'FlightNumber': 1,
                'CurveID': 1
            }
           
            # Fetch domestic data
            domestic_cursor = self.market_list_collection.find(query, projection)
            domestic_df = pd.DataFrame(list(domestic_cursor))
           
            # Fetch international data
            intl_cursor = self.market_list_intl_collection.find(query, projection)
            intl_df = pd.DataFrame(list(intl_cursor))
           
            # Combine both datasets
            market_list = pd.concat([domestic_df, intl_df], ignore_index=True)
           
            if not market_list.empty:
                # Rename Destin to Destination
                market_list.rename(columns={'Destin': 'Destination'}, inplace=True)
               
                # Ensure PerStart is string before converting to datetime
                market_list['PerStart'] = market_list['PerStart'].astype(str)
               
                # Convert PerStart to date
                market_list['DepartureDate'] = pd.to_datetime(
                    market_list['PerStart'],
                    format='%m/%d/%Y',
                    errors='coerce'  # Handle any bad dates gracefully
                ).dt.date
               
                # Clean FlightNumber - FIXED: Convert to string first
                market_list['FlightNumber'] = market_list['FlightNumber'].astype(str).str.strip()
               
                # Drop the original PerStart column
                market_list.drop('PerStart', axis=1, inplace=True)
               
                # Optional: Remove rows where date conversion failed
                market_list = market_list.dropna(subset=['DepartureDate'])
           
            return market_list
           
        except Exception as e:
            self.logger.error(f"Error fetching market list from MongoDB: {e}")
            return pd.DataFrame()
   
    def get_curve_ids(self, market_list):
        """
        CONVERTED TO MONGODB - Replaces rdconn query
        """
        try:
            curve_ids = market_list['CurveID'].unique().tolist()
           
            if curve_ids:
                # MongoDB query
                query = {
                    'CurveID': {'$in': curve_ids},
                    'NDO': {'$ne': None, '$exists': True},
                    'LF': {'$ne': None, '$exists': True}
                }
               
                projection = {
                    '_id': 0,
                    'NDO': 1,
                    'CurveID': 1,
                    'LF': 1
                }
               
                cursor = self.curves_collection.find(query, projection)
                df_curves = pd.DataFrame(list(cursor))
               
                if df_curves.empty:
                    self.logger.warning("No curves data found for the market CurveIDs.")
                    self._curves = pd.DataFrame()
                else:
                    # Rename LF to LoadFactor
                    df_curves.rename(columns={'LF': 'LoadFactor'}, inplace=True)
                   
                    # Convert to numeric
                    df_curves['NDO'] = pd.to_numeric(df_curves['NDO'], errors='coerce')
                    df_curves['LoadFactor'] = pd.to_numeric(df_curves['LoadFactor'], errors='coerce')
                    df_curves = df_curves.dropna(subset=['NDO', 'LoadFactor'])
                   
                    self.logger.info(f"Fetched {len(df_curves)} curves records.")
                    self._curves = df_curves
            else:
                self.logger.warning("No valid CurveIDs found in market_list.")
                self._curves = pd.DataFrame()
               
        except Exception as e:
            self.logger.error(f"Error fetching curves data from MongoDB: {e}")
            self.logger.error(traceback.format_exc())
            self._curves = pd.DataFrame()
   
    def getCurrentBookedLoads(self, startDate, endDate):
        """
        KEPT AS SQL - Using navconn (Navitaire database)
        """
        NavitaireQuery = f""" EXEC {configsettings.NAV_DB_WB_NAME}[dbo].[SP_Bookeddata]
                                @DepartureDate_from = N'{str(startDate)}',
                                @DepartureDate_to = N'{str(endDate)}'
                            """
        bookedData = pd.read_sql(NavitaireQuery, self.navconn)
        return bookedData
   
    def left_join(self, left_df, right_df):
        right_df_copy = right_df.copy()
        for df in [left_df, right_df_copy]:
            df['FlightNumber'] = df['FlightNumber'].astype(str).str.strip()
            df['Origin'] = df['Origin'].astype(str).str.strip().str.upper()
            df['Destination'] = df['Destination'].astype(str).str.strip().str.upper()
            df['DepartureDate'] = pd.to_datetime(df['DepartureDate']).dt.date
 
        return left_df.merge(
            right_df_copy,
            how='left',
            left_on=['DepartureDate', 'FlightNumber', 'Origin', 'Destination'],
            right_on=['DepartureDate', 'FlightNumber', 'Origin', 'Destination']
        )
   
    def calculate_expected_plf_vectorized(self, merge_list):
        """Vectorized version of expected PLF calculation"""

        # Pre-merge curves data with market list for NDO >= 22
        high_ndo_mask = merge_list['NDO_Curr'] >= 22

        # Create lookup NDO column
        merge_list['lookup_ndo'] = merge_list['NDO_Curr'] + 1

        # Merge with curves for high NDO cases
        high_ndo_data = merge_list[high_ndo_mask].merge(
            self._curves[['CurveID', 'NDO', 'LoadFactor']],
            left_on=['CurveID', 'lookup_ndo'],
            right_on=['CurveID', 'NDO'],
            how='left',
            suffixes=('', '_curve')
        )

        # For missing matches, find closest NDO
        missing_mask = high_ndo_data['LoadFactor'].isna()
        if missing_mask.any():
            # Group curves by CurveID for vectorized closest match
            curves_grouped = self._curves.groupby('CurveID')

            for curve_id in high_ndo_data[missing_mask]['CurveID'].unique():
                if pd.isna(curve_id):
                    continue
                curve_data = curves_grouped.get_group(curve_id) if curve_id in curves_grouped.groups else pd.DataFrame()
                if curve_data.empty:
                    continue

                mask = (high_ndo_data['CurveID'] == curve_id) & missing_mask
                lookup_ndos = high_ndo_data.loc[mask, 'lookup_ndo']

                for idx, lookup_ndo in lookup_ndos.items():
                    closest_idx = (curve_data['NDO'] - lookup_ndo).abs().idxmin()
                    high_ndo_data.loc[idx, 'LoadFactor'] = curve_data.loc[closest_idx, 'LoadFactor']

        merge_list.loc[high_ndo_mask, 'Expected_PLF'] = high_ndo_data['LoadFactor'] * 100

        # For NDO 1-21 and < 1, still need row-by-row (complex time-based logic)
        # But pre-filter to reduce iterations
        low_ndo_mask = (merge_list['NDO_Curr'] >= 1) & (merge_list['NDO_Curr'] <= 21)
        same_day_mask = merge_list['NDO_Curr'] < 1

        # Only iterate over rows that need complex calculation
        for idx in merge_list[low_ndo_mask].index:
            row = merge_list.loc[idx]
            merge_list.loc[idx, 'Expected_PLF'] = self._calculate_area_based_expected_plf(
                row, row['CurveID'], row['adjustedCapacity'], int(row['NDO_Curr'])
            )

        for idx in merge_list[same_day_mask].index:
            row = merge_list.loc[idx]
            merge_list.loc[idx, 'Expected_PLF'] = self._calculate_same_day_expected_plf(
                row, row['CurveID'], row['adjustedCapacity'], row['DepartureTime']
            )

        return merge_list
 
    def _calculate_area_based_expected_plf(self, row, curve_id, capacity, ndo_curr):
        """
        Calculate expected PLF for NDO 1-21 using area-based calculation
        """
        try:
            start_curve_match = self._curves[
                (self._curves['CurveID'] == curve_id) &
                (self._curves['NDO'] == ndo_curr + 1)
            ]
           
            if start_curve_match.empty:
                start_curve_match = self._curves[
                    (self._curves['CurveID'] == curve_id) &
                    (self._curves['NDO'] == ndo_curr)
                ]
           
            if start_curve_match.empty:
                return np.nan
           
            start_of_day_book_load = start_curve_match.iloc[0]['LoadFactor']
            start_of_day_tgt_booked_load = start_of_day_book_load * capacity
           
            end_curve_match = self._curves[
                (self._curves['CurveID'] == curve_id) &
                (self._curves['NDO'] == ndo_curr)
            ]
           
            if end_curve_match.empty:
                return np.nan
           
            end_of_day_book_load = end_curve_match.iloc[0]['LoadFactor']
            end_of_day_tgt_booked_load = end_of_day_book_load * capacity
           
            current_time = datetime.now()
            area_at_current_time = self._get_area_for_hour(current_time.hour)
           
            tgt_at_current_hour = (end_of_day_tgt_booked_load - start_of_day_tgt_booked_load) * area_at_current_time + start_of_day_tgt_booked_load
           
            expected_plf = (tgt_at_current_hour / capacity) * 100
            return min(expected_plf, 100)
           
        except Exception as e:
            self.logger.debug(f"Error calculating area-based expected PLF for NDO {ndo_curr}: {e}")
            return np.nan
 
    def _calculate_same_day_expected_plf(self, row, curve_id, capacity, std_time):
        """
        Calculate expected PLF for same day flights (NDO < 1)
        """
        try:
            END_OF_DAY_LF_FACTOR = 0.95
           
            start_curve_match = self._curves[
                (self._curves['CurveID'] == curve_id) &
                (self._curves['NDO'] == 1)
            ]
           
            if start_curve_match.empty:
                start_curve_match = self._curves[
                    (self._curves['CurveID'] == curve_id) &
                    (self._curves['NDO'] == 0)
                ]
           
            if start_curve_match.empty:
                return np.nan
           
            end_curve_match = self._curves[
                (self._curves['CurveID'] == curve_id) &
                (self._curves['NDO'] == 0)
            ]
           
            start_of_day_book_load = start_curve_match.iloc[0]['LoadFactor']
            start_of_day_tgt_booked_load = start_of_day_book_load * capacity
           
            end_of_day_book_load = end_curve_match.iloc[0]['LoadFactor']
            end_of_day_tgt_booked_load = end_of_day_book_load * capacity
           
            end_of_day_tgt_booked_load = max(capacity * END_OF_DAY_LF_FACTOR, end_of_day_tgt_booked_load)
           
            current_time = self.get_current_ist_time()
            departure_time = self._parse_departure_time(std_time, row['DepartureDate'])
           
            if departure_time is None:
                return np.nan
           
            area_current_time = self._get_area_for_hour(current_time.hour)
           
            hours_2hrs_before_departure = max(0, (departure_time - timedelta(hours=2)).hour)
            area_2hrs_departure = self._get_area_for_hour(hours_2hrs_before_departure)
           
            if area_2hrs_departure > 0:
                area_ratio = area_current_time / area_2hrs_departure
                if area_current_time > area_2hrs_departure:
                    area_ratio = 1
            else:
                area_ratio = 1
           
            area_at_current_time = area_ratio
           
            tgt_at_current_hour = (end_of_day_tgt_booked_load - start_of_day_tgt_booked_load) * area_at_current_time + start_of_day_tgt_booked_load
           
            expected_plf = (tgt_at_current_hour / capacity) * 100
            return min(expected_plf, 100)
           
        except Exception as e:
            self.logger.debug(f"Error calculating same day expected PLF: {e}")
            return np.nan
 
    def get_current_ist_time(self):
        """Get current time in IST timezone"""
        ist = pytz.timezone('Asia/Kolkata')
        return datetime.now(ist)
 
    def _parse_departure_time(self, std_time, departure_date):
        """
        Parse departure time from STD field and combine with departure date
        """
        try:
            if pd.isna(std_time):
                return None
           
            if isinstance(departure_date, str):
                date_part = datetime.strptime(departure_date, '%Y-%m-%d').date()
            else:
                date_part = departure_date
           
            if isinstance(std_time, str):
                time_part = datetime.strptime(std_time, '%H:%M').time()
            elif isinstance(std_time, timedelta):
                time_part = (datetime.min + std_time).time()
            elif isinstance(std_time, time):
                time_part = std_time
            else:
                return None
           
            return datetime.combine(date_part, time_part)
           
        except Exception as e:
            self.logger.debug(f"Error parsing departure time: {e}")
            return None
   
    def get_share_of_booking(self):
        try:
            cursor = self.share_of_booking_collection.find({}, {'_id': 0, 'hours': 1, 'shareOfBooking': 1})
            df = pd.DataFrame(list(cursor))
            return df
        except Exception as e:
            self.logger.error(f"Error fetching share of booking from MongoDB: {e}")
            return pd.DataFrame()
 
    def _get_area_for_hour(self, hour):
        """
        CONVERTED TO MONGODB - Replaces rdconn query
        """
        try:
            sum = self.share_of_booking[self.share_of_booking['hours'] <= hour]['shareOfBooking'].sum()
            if sum:
                return round(sum / 100, 2)
            else:
                return 1.0
               
        except Exception as e:
            self.logger.debug(f"Error getting area for hour {hour}: {e}")
            return 1.0