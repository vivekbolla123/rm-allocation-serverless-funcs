import json
import configsettings
import pandas as pd
from datetime import datetime, timezone, timedelta
from constants_utils import ConstantsUtils
import traceback
import requests
import uuid
import warnings
warnings.filterwarnings('ignore')

class defaultMarketlist:
    def __init__(self, logger, params, overrides, context):
        self.context = context
        self.logger = logger
        self.navconn = params['navconn']  # Keep SQL for Navitaire
        self.mongo_conn = params['mongoconn']  # Single MongoDB connection
        self.db = self.mongo_conn["qp_dw_rmalloc"]
        self.master_db = params['mongo_master_db']
        self.cache_client = params['cache_client']
        self.constants = ConstantsUtils(self.db)
        self.default_list = json.loads(self.constants.DEFAULT_MARKET_LIST)
        self.max_days = self.constants.DEFAULT_MARKET_LIST_DAYS
        self.session = requests.Session()
        self.session.headers.update(
            {"Ocp-Apim-Subscription-Key": configsettings.NAV_OCP_SUBSCRIPTION_KEY}
        )

    def run(self):
        try:
            self.logger.info("Starting the run method.")
            currDate = datetime.now(timezone.utc)
            endDate = currDate + timedelta(days=self.max_days)
            endDate = endDate.date()

            # Navitaire query remains SQL (different system)
            NavitaireQuery = f"""
                SELECT
                    IL.DepartureStation as Origin,
                    IL.ArrivalStation as Destination,
                    IL.DepartureDate,
                    TRIM(IL.FlightNumber) as FlightNumber,
                    FORMAT(IL.STD, 'HH:mm') as DepartureTime,
                    FORMAT(IL.STDUTC, 'HH:mm') as DepartureTimeUTC
                FROM
                    {configsettings.NAV_DB_NAME}[rez].InventoryLeg IL
                WHERE IL.Lid > 0
                    AND IL.Status NOT IN (2)
                    AND IL.DepartureDate >= CAST(GETDATE() AS DATE)
                    AND IL.DepartureDate <= '{endDate}'
            """
            navFlights = pd.read_sql(NavitaireQuery, self.navconn)

            if navFlights.empty or len(navFlights) == 0:
                raise ValueError("No flights found in NavODS")

            # Fetch market_list from MongoDB
            mlFlights = self._fetch_marketlist_flights(endDate)

            if mlFlights.empty or len(mlFlights) == 0:
                raise ValueError("No flights found in Marketlist")

            navFlights['DepartureDate'] = pd.to_datetime(navFlights['DepartureDate'])
            mlFlights['DepartureDate'] = pd.to_datetime(mlFlights['DepartureDate'])

            # Get unmatched records - Fix: Use indicator merge to properly identify unmatched records
            merge_indicator = navFlights.merge(
                mlFlights[['Origin', 'Destination', 'FlightNumber', 'DepartureDate']],
                on=['Origin', 'Destination', 'FlightNumber', 'DepartureDate'],
                how='left',
                indicator=True
            )
            
            # Filter for records that exist only in navFlights (left_only)
            unmatched_navFlights = merge_indicator[merge_indicator['_merge'] == 'left_only'].drop('_merge', axis=1)
            unmatched_navFlights['sector'] = unmatched_navFlights['Origin'] + unmatched_navFlights['Destination']
            
            # Fetch analyst sector map from MongoDB
            analyst_sector_map = self._fetch_analyst_sector_map()
            if analyst_sector_map.empty:
                raise ValueError("No columns found for market_list table")
            
            # Fetch station master from MongoDB
            station_master_map = self._fetch_station_master()
            if station_master_map.empty:
                raise ValueError("No columns found for market_list table")
            
            unmatched_navFlights = unmatched_navFlights.merge(
                analyst_sector_map[['sector', 'user1']],
                on=['sector'],
                how='left'
            )

            # Get default values
            default_list = self.default_list
            if len(default_list) == 0:
                raise ValueError("No default values found for market_list")
            column_names = list(default_list.keys())

            # Prepare data for insertion
            insert_data = []
            for _, row in unmatched_navFlights.iterrows():
                record = default_list.copy()
                record['Origin'] = row['Origin']
                record['Destin'] = row['Destination']
                record['PerStart'] = row['DepartureDate'].strftime('%m/%d/%Y')
                record['PerEnd'] = row['DepartureDate'].strftime('%m/%d/%Y')
                record['FlightNumber'] = row['FlightNumber']
                record['analystName'] = row['user1']
                record['UUID'] = str(uuid.uuid4())
                record['defaultMlFlag'] = "1"
                weekday_num = row['DepartureDate'].weekday()

                sunday_based_day = (weekday_num + 1) % 7
                dow_base = list('9999999')
                dow_base[sunday_based_day] = '1'
                record['DOW'] = ''.join(dow_base)
                insert_data.append([record.get(col, '') for col in column_names])

            # Create DataFrame for insertion
            insert_df = pd.DataFrame(insert_data, columns=column_names)

            origin_routes = insert_df.merge(
                station_master_map, 
                left_on='Origin', 
                right_on='station', 
                how='left'
            ).rename(columns={'route': 'origin_route'})

            final_df = origin_routes.merge(
                station_master_map, 
                left_on='Destin', 
                right_on='station', 
                how='left'
            ).rename(columns={'route': 'destin_route'})

            international_df = final_df[
                (final_df['origin_route'] == 'I') | (final_df['destin_route'] == 'I')
            ]

            # Domestic: rest of them (everything that's not international)
            domestic_df = final_df[
                ~((final_df['origin_route'] == 'I') | (final_df['destin_route'] == 'I'))
            ]

            international_df = international_df.drop(columns=['origin_route', 'destin_route', 'station_x', 'station_y'], errors='ignore')
            domestic_df = domestic_df.drop(columns=['origin_route', 'destin_route', 'station_x', 'station_y'], errors='ignore')

            # Insert into MongoDB collections
            self._insert_to_mongodb('market_list', domestic_df)
            self._insert_to_mongodb('market_list_international', international_df)

            self.logger.info(f"Inserted {len(domestic_df)} unmatched records into market_list.")
            self.logger.info(f"Inserted {len(international_df)} unmatched records into market_list_international.")
            self.logger.info("Run method completed.")
            
        except Exception as e:
            self.logger.error(f"An error occurred in the run method: {traceback.format_exc()}")

    def _fetch_marketlist_flights(self, endDate):
        """
        Fetch flights from market_list and market_list_international collections.
        Equivalent to UNION query in MySQL.
        """
        try:
            current_date = datetime.now().date()
            
            # Helper function to fetch from a collection
            def fetch_from_collection(collection_name):
                collection = self.db[collection_name]
                
                # MongoDB query
                # Note: Handling both string and datetime formats for PerStart
                cursor = collection.find(
                    {},
                    {
                        '_id': 0,
                        'Origin': 1,
                        'Destin': 1,
                        'PerStart': 1,
                        'FlightNumber': 1
                    }
                )
                
                df = pd.DataFrame(list(cursor))
                if df.empty:
                    return pd.DataFrame(columns=['Origin', 'Destination', 'DepartureDate', 'FlightNumber'])
                
                # Rename Destin to Destination for consistency
                df = df.rename(columns={'Destin': 'Destination'})
                
                # Convert PerStart to datetime (handle both string and datetime)
                df['DepartureDate'] = df['PerStart'].apply(
                    lambda x: pd.to_datetime(x, format='%m/%d/%Y', errors='coerce') 
                    if isinstance(x, str) else pd.to_datetime(x, errors='coerce')
                )
                
                # Filter by date range
                df = df[
                    (df['DepartureDate'].dt.date >= current_date) &
                    (df['DepartureDate'].dt.date <= endDate)
                ]
                
                # Clean FlightNumber (trim whitespace)
                df['FlightNumber'] = df['FlightNumber'].astype(str).str.strip()
                
                return df[['Origin', 'Destination', 'DepartureDate', 'FlightNumber']]
            
            # Fetch from both collections
            domestic_flights = fetch_from_collection('market_list')
            international_flights = fetch_from_collection('market_list_international')
            
            # Union (concatenate) the results
            mlFlights = pd.concat([domestic_flights, international_flights], ignore_index=True)
            
            # Remove duplicates
            mlFlights = mlFlights.drop_duplicates()
            
            self.logger.info(f"Fetched {len(mlFlights)} flights from market_list collections")
            return mlFlights
            
        except Exception as e:
            self.logger.error(f"Error fetching marketlist flights from MongoDB: {e}")
            return pd.DataFrame()

    def _fetch_analyst_sector_map(self):
        """Fetch analyst sector mapping from MongoDB."""
        try:
            collection = self.db['config_user_sector_map']
            cursor = collection.find({}, {'_id': 0, 'sector': 1, 'user1': 1})
            df = pd.DataFrame(list(cursor))
            
            self.logger.info(f"Fetched {len(df)} records from config_user_sector_map")
            return df
            
        except Exception as e:
            self.logger.error(f"Error fetching analyst sector map from MongoDB: {e}")
            return pd.DataFrame()

    def _fetch_station_master(self):
        """Fetch station master from MongoDB."""
        try:
            collection = self.master_db['config_station_master']
            cursor = collection.find({})
            df = pd.DataFrame(list(cursor))
            
            # Remove MongoDB _id field
            if '_id' in df.columns:
                df = df.drop('_id', axis=1)
            
            self.logger.info(f"Fetched {len(df)} records from config_station_master")
            return df
            
        except Exception as e:
            self.logger.error(f"Error fetching station master from MongoDB: {e}")
            return pd.DataFrame()

    def _insert_to_mongodb(self, collection_name, df):
        """
        Insert DataFrame records into MongoDB collection.
        Converts all values to appropriate types: int, float, or string.
        
        Args:
            collection_name: Name of the collection
            df: DataFrame to insert
        """
        try:
            if df.empty:
                self.logger.info(f"No records to insert into {collection_name}")
                return
            
            collection = self.db[collection_name]
            
            # Helper function to convert a value to int, float, or string
            def parse_value(val):
                """
                Parse value to int, float, or string in that order of preference.
                """
                # Handle None/NaN
                if pd.isna(val) or val is None:
                    return None
                
                # If already correct type, return as-is
                if isinstance(val, (int, float, bool)):
                    # Try to convert float to int if it's a whole number
                    if isinstance(val, float):
                        if val.is_integer():
                            return int(val)
                    return val
                
                # Convert to string first for uniform processing
                str_val = str(val).strip()
                
                # Empty string handling
                if str_val == '' or str_val.lower() in ['nan', 'none', 'null']:
                    return None
                
                # Try integer conversion first
                try:
                    # Check if string represents an integer
                    if '.' not in str_val and 'e' not in str_val.lower():
                        return int(str_val)
                    else:
                        # Check if it's a float that's actually an integer
                        float_val = float(str_val)
                        if float_val.is_integer():
                            return int(float_val)
                        return float_val
                except (ValueError, OverflowError):
                    pass
                
                # Try float conversion
                try:
                    return float(str_val)
                except (ValueError, OverflowError):
                    pass
                
                # Return as string if all conversions fail
                return str_val
            
            # Convert DataFrame to list of dictionaries with type conversion
            records = []
            for _, row in df.iterrows():
                record = {}
                for col in df.columns:
                    val = row[col]
                    
                    # Special handling for specific fields
                    if col in ['PerStart', 'PerEnd']:
                        # Keep date strings as-is for MongoDB
                        if isinstance(val, str):
                            record[col] = val
                        elif pd.notna(val):
                            record[col] = str(val)
                        else:
                            record[col] = None
                    
                    elif col == 'UUID':
                        # Ensure UUID is string
                        record[col] = str(val) if pd.notna(val) else None
                    
                    
                    elif col in ['analystName', 'Origin', 'Destin']:
                        # Keep certain fields as strings
                        record[col] = str(val).strip() if pd.notna(val) else None
                    
                    else:
                        # Apply general type conversion
                        record[col] = parse_value(val)
                
                records.append(record)
            
            # Bulk insert
            if records:
                result = collection.insert_many(records)
                self.logger.info(f"Inserted {len(result.inserted_ids)} records into {collection_name}")
            else:
                self.logger.info(f"No valid records to insert into {collection_name}")
            
        except Exception as e:
            self.logger.error(f"Error inserting into {collection_name}: {traceback.format_exc()}")
            raise
