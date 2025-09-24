import json

import pytz
import configsettings
import pandas as pd
from datetime import datetime, timedelta, time
from constants_utils import ConstantsUtils
import traceback
import requests
import numpy as np
from sqlalchemy import text
import warnings
warnings.filterwarnings('ignore')

class update_imputed_forecast:
    def __init__(self, logger, params, overrides, context):
      self.context = context
      self.logger = logger
      self.navconn = params['navconn']
      self.rm_rd_conn = params['rdconn']
      self.rm_wr_conn = params['wrconn']
      self.dw_conn = params['dwconn']
      self.cache_client = params['cache_client']
      self.constants = ConstantsUtils(self.rm_wr_conn)
      self.DEDUCTION_PER = self.constants.IMP_FORECAST_DEDUCTION
      self.LIST_LENGTH = 6
      self.session = requests.Session()
      self.session.headers.update(
          {"Ocp-Apim-Subscription-Key": configsettings.NAV_OCP_SUBSCRIPTION_KEY}
      )

    def run(self):
      try:
        self.logger.info("Starting the run method.")

        forecast_list = self.get_forecast()

        if forecast_list.empty:
          raise ValueError("No flights found in Marketlist")

        min_date = min(forecast_list['DepartureDate'])
        max_date = max(forecast_list['DepartureDate'])

        market_list = pd.read_sql(self.getMarketListQuery(min_date, max_date), self.rm_rd_conn)

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

        merge_list['Expected_PLF'] = merge_list.apply(self._get_expected_plf, axis=1)
        
        merge_list['Variance'] = merge_list['PLF'] - merge_list['Expected_PLF']

        merge_list['ImputedForecastPLF'] = merge_list['forecastPLF'] + merge_list['Variance'] - merge_list['forecast_variance'] - self.DEDUCTION_PER

        merge_list['ImputedForecastPLF'] = merge_list['ImputedForecastPLF'].round(2)

        updated_count = self.update_redis_forecast(merge_list)

        self.logger.info(f'Updated {updated_count}/{len(merge_list)} flights')
        
        self.logger.info("Run method completed.")
      except Exception as e:
        self.logger.error(f"An error occurred in the run method: {traceback.format_exc()}")
    
    def update_redis_forecast(self, merge_list: pd.DataFrame):
      updated_count = 0
      current_time = datetime.now().replace(microsecond=0)  # Trim milliseconds
      
      for index, row in merge_list.iterrows():
          try:
              redis_key = f"ImputedForecastPLF:{row['Origin']}{row['Destination']}_{row['FlightNumber']}_{row['DepartureDate']}"
              current_plf = float(row['ImputedForecastPLF'])
              
              existing_json = self.cache_client.get(redis_key)
              existing_plf_list = []
              
              try:
                  existing_json = self.cache_client.get(redis_key)
                  existing_plf_list = json.loads(existing_json) if existing_json else []
              except (json.JSONDecodeError, TypeError) as e:
                  self.logger.error(f"Invalid JSON in Redis for key {redis_key}: {e}")
                  existing_plf_list = []
              except Exception as e:
                  self.logger.error(f"Redis connection error for key {redis_key}: {e}")
                  continue
              
              plf_history_list = self.build_plf_list_with_timestamps(
                  existing_plf_list, current_time, current_plf
              )
              
              try:
                  redis_value = json.dumps(plf_history_list)
                  self.cache_client.set(redis_key, redis_value, ex=172800)   #ttl: 2 days
                  updated_count += 1
              except Exception as e:
                  self.logger.error(f"Error setting Redis key {redis_key}: {e}")
              
          except Exception as e:
              self.logger.error(f'Error updating redis: {e}')
      
      return updated_count

    def build_plf_list_with_timestamps(self, existing_plf_list, current_time, current_plf):
      # Remove records older than 7 hours
      cutoff_time = current_time - timedelta(hours=7)
      
      # Filter existing list to keep only records within 7 hours
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
      
      # Sort
      try:
          filtered_plf_list.sort(key=lambda x: datetime.fromisoformat(x['timestamp']))
      except (ValueError, KeyError) as e:
          self.logger.warning(f"Error sorting PLF records by timestamp: {e}")
          return filtered_plf_list
      
      return filtered_plf_list 
    
    def get_forecast(self):
      query = """
      SELECT forecastPLF, Variance as forecast_variance, Origin, Destination, FlightNumber, DepartureDate 
      FROM RM_DW_DATA.forecast_results
      WHERE DepartureDate >= DATE(CONVERT_TZ(NOW(), 'UTC', 'Asia/Kolkata'))
        AND forecastPLF IS NOT NULL AND Variance IS NOT NULL
      """
      forecast_list = pd.read_sql(query, self.dw_conn)
      return forecast_list
    
    def getMarketListQuery(self, min_date, max_date):
      query = f"""
                SELECT Origin, Destin AS Destination,
                    STR_TO_DATE(PerStart, '%%m/%%d/%%Y') as DepartureDate,
                    TRIM(FlightNumber) as FlightNumber, CurveID
                FROM QP_DW_RMALLOC.market_list
                WHERE STR_TO_DATE(PerStart, '%%m/%%d/%%Y') >= '{min_date}'
                    AND STR_TO_DATE(PerStart, '%%m/%%d/%%Y') <= '{max_date}'

                UNION

                SELECT Origin, Destin AS Destination,
                    STR_TO_DATE(PerStart, '%%m/%%d/%%Y') as DepartureDate,
                    TRIM(FlightNumber) as FlightNumber, CurveID
                FROM QP_DW_RMALLOC.market_list_international
                WHERE STR_TO_DATE(PerStart, '%%m/%%d/%%Y') >= '{min_date}'
                    AND STR_TO_DATE(PerStart, '%%m/%%d/%%Y') <= '{max_date}';
            """
      return query
    
    def get_curve_ids(self, market_list):
      curve_ids = market_list['CurveID'].unique().tolist()

      try:
        if curve_ids:
          # Create placeholders for the IN clause
          placeholders = ','.join([':curve_id_' + str(i) for i in range(len(curve_ids))])
          curves_query = f"""
          SELECT 
              NDO,
              CurveID,
              LF as LoadFactor
          FROM QP_DW_RMALLOC.Curves
          WHERE CurveID IN ({placeholders})
          AND NDO IS NOT NULL
          AND LF IS NOT NULL
          """
          
          # Create parameters dictionary
          params = {f'curve_id_{i}': curve_id for i, curve_id in enumerate(curve_ids)}
          
          df_curves = pd.read_sql(text(curves_query), self.rm_rd_conn, params=params)
          
          if df_curves.empty:
              self.logger.warning("No curves data found for the market CurveIDs.")
              self._curves = pd.DataFrame()
          else:
              df_curves['NDO'] = pd.to_numeric(df_curves['NDO'], errors='coerce')
              df_curves['LoadFactor'] = pd.to_numeric(df_curves['LoadFactor'], errors='coerce')
              df_curves = df_curves.dropna(subset=['NDO', 'LoadFactor'])
              self.logger.info(f"Fetched {len(df_curves)} curves records.")
              self._curves = df_curves
        else:
          self.logger.warning("No valid CurveIDs found in market_list.")
          self._curves = pd.DataFrame()
          
      except Exception as e:
          self.logger.error(f"Error fetching market curves data: {e}")
          self.logger.error(traceback.format_exc())
          self._curves = pd.DataFrame()
    
    def getCurrentBookedLoads(self, startDate, endDate):

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
    
    def _get_expected_plf(self, row):
        """
        Get expected PLF from curves data based on market_list mapping with updated NDO logic
        """
        try:
            if self._curves is None or self._curves.empty:
                return np.nan
            
            ndo_curr = row.get('NDO_Curr')
            capacity = row.get('adjustedCapacity', 0)
            std_time = row.get('DepartureTime')  # Departure time
            
            if pd.isna(ndo_curr):
                return np.nan
            
            # Get the first matching CurveID
            curve_id = row['CurveID']
            
            if pd.isna(curve_id) or curve_id == '':
                return np.nan
            
            # Determine which NDO to use based on the logic
            if ndo_curr >= 22:
                # For NDO >= 22, use NDO + 1
                lookup_ndo = int(ndo_curr) + 1
                
                # Find the corresponding LF from curves table
                curve_match = self._curves[
                    (self._curves['CurveID'] == curve_id) &
                    (self._curves['NDO'] == lookup_ndo)
                ]
                
                if curve_match.empty:
                    # If exact NDO match not found, try to find closest NDO
                    curve_for_id = self._curves[
                        self._curves['CurveID'] == curve_id
                    ]
                    if not curve_for_id.empty:
                        # Find closest NDO
                        closest_ndo = curve_for_id.iloc[
                            (curve_for_id['NDO'] - lookup_ndo).abs().argsort()
                        ].iloc[0]
                        return closest_ndo['LoadFactor'] * 100
                    return np.nan
                
                return curve_match.iloc[0]['LoadFactor'] * 100
                
            elif 1 <= ndo_curr <= 21:
                # For NDO 1-21, use the area-based calculation
                return self._calculate_area_based_expected_plf(row, curve_id, capacity, int(ndo_curr))
            else:
                # For NDO < 1 (same day), use special calculation
                return self._calculate_same_day_expected_plf(row, curve_id, capacity, std_time)
            
        except Exception as e:
            self.logger.debug(f"Error getting expected PLF for {row.get('Origin', 'N/A')}-{row.get('Destination', 'N/A')} flight {row.get('FlightNumber', 'N/A')}: {e}")
            return np.nan

    def _calculate_area_based_expected_plf(self, row, curve_id, capacity, ndo_curr):
        """
        Calculate expected PLF for NDO 1-21 using area-based calculation
        """
        try:
            # Get start of day target from curves (NDO + 1)
            start_curve_match = self._curves[
                (self._curves['CurveID'] == curve_id) &
                (self._curves['NDO'] == ndo_curr + 1)
            ]
            
            if start_curve_match.empty:
                # Fallback to current NDO if NDO + 1 not available
                start_curve_match = self._curves[
                    (self._curves['CurveID'] == curve_id) &
                    (self._curves['NDO'] == ndo_curr)
                ]
            
            if start_curve_match.empty:
                return np.nan
            
            start_of_day_book_load = start_curve_match.iloc[0]['LoadFactor']
            start_of_day_tgt_booked_load = start_of_day_book_load * capacity
            
            # Get end of day target from NDO 0
            end_curve_match = self._curves[
                (self._curves['CurveID'] == curve_id) &
                (self._curves['NDO'] == ndo_curr)
            ]
            
            if end_curve_match.empty:
                return np.nan
            
            end_of_day_book_load = end_curve_match.iloc[0]['LoadFactor']
            end_of_day_tgt_booked_load = end_of_day_book_load * capacity
            
            # Get area at current time
            current_time = datetime.now()
            area_at_current_time = self._get_area_for_hour(current_time.hour)
            
            # Calculate target at current hour using the formula
            tgt_at_current_hour = (end_of_day_tgt_booked_load - start_of_day_tgt_booked_load) * area_at_current_time + start_of_day_tgt_booked_load
            
            # Return as percentage
            expected_plf = (tgt_at_current_hour / capacity) * 100
            return min(expected_plf, 100)  # Cap at 100%
            
        except Exception as e:
            self.logger.debug(f"Error calculating area-based expected PLF for NDO {ndo_curr}: {e}")
            return np.nan

    def _calculate_same_day_expected_plf(self, row, curve_id, capacity, std_time):
        """
        Calculate expected PLF for same day flights (NDO < 1) using the complex logic
        """
        try:
            # Constants - you may need to adjust these based on your system
            END_OF_DAY_LF_FACTOR = 0.95  # Adjust as needed
            
            # Get start of day target from curves (NDO + 1, which would be NDO 1 for same day)
            start_curve_match = self._curves[
                (self._curves['CurveID'] == curve_id) &
                (self._curves['NDO'] == 1)
            ]
            
            if start_curve_match.empty:
                # Fallback to NDO 0 if NDO 1 not available
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
            
            # Get end of day target (this should come from your target booking factor)
            # For now, using a default calculation - you may need to adjust this
            end_of_day_tgt_booked_load = max(capacity * END_OF_DAY_LF_FACTOR, end_of_day_tgt_booked_load)
            
            # Calculate area at current time
            current_time = self.get_current_ist_time()
            departure_time = self._parse_departure_time(std_time, row['DepartureDate'])
            
            if departure_time is None:
                return np.nan
            
            # Get area values (you'll need to implement getArea equivalent)
            area_current_time = self._get_area_for_hour(current_time.hour)
            
            # For same day flights, calculate area differently
            hours_2hrs_before_departure = max(0, (departure_time - timedelta(hours=2)).hour)
            area_2hrs_departure = self._get_area_for_hour(hours_2hrs_before_departure)
            
            if area_2hrs_departure > 0:
                area_ratio = area_current_time / area_2hrs_departure
                if area_current_time > area_2hrs_departure:
                    area_ratio = 1
            else:
                area_ratio = 1
            
            area_at_current_time = area_ratio
            
            # Calculate target at current hour
            tgt_at_current_hour = (end_of_day_tgt_booked_load - start_of_day_tgt_booked_load) * area_at_current_time + start_of_day_tgt_booked_load
            
            # Return as percentage
            expected_plf = (tgt_at_current_hour / capacity) * 100
            return min(expected_plf, 100)  # Cap at 100%
            
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

    def _get_area_for_hour(self, hour):
        """
        Get area value for a given hour - equivalent to the getArea function
        Returns the cumulative share of booking up to the given hour
        """
        try:
            engine = self._get_rm_engine()
            query = "SELECT SUM(shareOfBooking) as sum FROM QP_DW_RMALLOC.config_share_of_booking WHERE hours <= :hour"
            
            with engine.connect() as connection:
                result = pd.read_sql(text(query), connection, params={'hour': hour})
            
            if not result.empty and not pd.isna(result['sum'].iloc[0]):
                # Return the sum divided by 100 to convert percentage to decimal
                return result['sum'].iloc[0] / 100
            else:
                return 1.0  # Default fallback
                
        except Exception as e:
            self.logger.debug(f"Error getting area for hour {hour}: {e}")
            return 1.0  # Default fallback