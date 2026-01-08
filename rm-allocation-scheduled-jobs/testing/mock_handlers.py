"""
Mock handlers for testing - handles MongoDB operations, Redis cache, and SQL queries
Generates CSV output files for analysis
"""
import logging
import pandas as pd
import pymongo
import json
import os
import glob
from datetime import datetime

logger = logging.getLogger(__name__)

def setup_output_directory():
    """Create and return output directory path"""
    output_dir = os.path.join(os.path.dirname(__file__), "output")
    os.makedirs(output_dir, exist_ok=True)
    return output_dir

def clean_old_output_files(module_name, output_dir):
    """Delete old output files for the specified module"""
    try:
        # Define patterns for each module's output files
        patterns = {
            'default_ml': ['market_list_*.csv', 'market_list_international_*.csv'],
            'imputed_forecast': ['imputed_forecast_output*.csv'],
            'schedule_thirty': ['market_list_thirty_*.csv', 'flight_tracker*.csv'],
            'heuristic_calc': ['Holiday_Upsell_Forecast_*.csv']
        }
        
        if module_name in patterns:
            for pattern in patterns[module_name]:
                files_to_delete = glob.glob(os.path.join(output_dir, pattern))
                for file_path in files_to_delete:
                    try:
                        os.remove(file_path)
                        logger.info(f"Deleted old output file: {os.path.basename(file_path)}")
                    except Exception as e:
                        logger.warning(f"Could not delete {file_path}: {e}")
    except Exception as e:
        logger.error(f"Error cleaning old output files for {module_name}: {e}")

def patch_mongodb_operations():
    """Patch MongoDB operations to save results as CSV files for testing"""
    output_dir = setup_output_directory()
    
    def mock_insert_many(self, documents):
        """Mock insert_many to save data as CSV"""
        try:
            collection_name = self.name
            
            if not documents:
                logger.info(f"No documents to insert into {collection_name}")
                return None
                
            df = pd.DataFrame(documents)
            
            # Simple filename without timestamp
            filename = f"{collection_name}.csv"
            filepath = os.path.join(output_dir, filename)
            
            df.to_csv(filepath, index=False)
            logger.info(f"Saved {len(documents)} records to {filename}")
            
            # Mock result
            class MockResult:
                def __init__(self, count):
                    self.inserted_ids = [f"mock_id_{i}" for i in range(count)]
            return MockResult(len(documents))
                
        except Exception as e:
            logger.error(f"Error in mock_insert_many for {self.name}: {e}")
            return None
    
    def mock_insert_one(self, document):
        """Mock insert_one to save data as CSV"""
        try:
            collection_name = self.name
            df = pd.DataFrame([document])
            
            filename = f"{collection_name}_single.csv"
            filepath = os.path.join(output_dir, filename)
            
            df.to_csv(filepath, index=False)
            logger.info(f"Saved 1 record to {filename}")
            
            # Mock result
            class MockResult:
                def __init__(self):
                    self.inserted_id = "mock_id_1"
            return MockResult()
            
        except Exception as e:
            logger.error(f"Error in mock_insert_one for {self.name}: {e}")
            return None
    
    def mock_update_one(self, filter_dict, update_dict, **kwargs):
        """Mock update_one to save data as CSV"""
        try:
            collection_name = self.name
            
            # Skip saving currentVersion and config_profile_fare_station_upsell updates to CSV
            if collection_name in ['currentVersion', 'config_profile_fare_station_upsell']:
                # Mock result without saving to CSV
                class MockResult:
                    def __init__(self):
                        self.modified_count = 1
                        self.matched_count = 1
                return MockResult()
            
            # Combine filter and update for CSV output
            combined_data = {**filter_dict}
            if '$set' in update_dict:
                combined_data.update(update_dict['$set'])
            if '$inc' in update_dict:
                combined_data.update({f"{k}_inc": v for k, v in update_dict['$inc'].items()})
            
            df = pd.DataFrame([combined_data])
            
            filename = f"{collection_name}_update.csv"
            filepath = os.path.join(output_dir, filename)
            
            df.to_csv(filepath, index=False)
            logger.info(f"Saved update operation to {filename}")
            
            # Mock result
            class MockResult:
                def __init__(self):
                    self.modified_count = 1
                    self.matched_count = 1
            return MockResult()
            
        except Exception as e:
            logger.error(f"Error in mock_update_one for {self.name}: {e}")
            return None
    
    def mock_drop(self):
        """Mock drop to log the operation"""
        collection_name = self.name
        logger.info(f"Mock drop operation on collection: {collection_name}")
        return None
    
    # Apply patches to pymongo Collection class
    try:
        import pymongo.collection
        
        # Store original methods
        pymongo.collection.Collection._original_insert_many = pymongo.collection.Collection.insert_many
        pymongo.collection.Collection._original_insert_one = pymongo.collection.Collection.insert_one
        pymongo.collection.Collection._original_update_one = pymongo.collection.Collection.update_one
        pymongo.collection.Collection._original_drop = pymongo.collection.Collection.drop
        
        # Apply patches
        pymongo.collection.Collection.insert_many = mock_insert_many
        pymongo.collection.Collection.insert_one = mock_insert_one
        pymongo.collection.Collection.update_one = mock_update_one
        pymongo.collection.Collection.drop = mock_drop
        
        logger.info("Patched MongoDB operations to save CSV outputs")
        
    except Exception as e:
        logger.error(f"Failed to patch MongoDB operations: {e}")

class MockCacheClient:
    """Mock Redis cache client that saves forecast data to CSV"""
    
    def __init__(self, module_name=None):
        self.data = {}
        self.sets = {}  # Store Redis sets
        self.output_dir = setup_output_directory()
        self.module_name = module_name
        self.flight_tracker_data = self._load_flight_tracker_data()
    
    def _load_flight_tracker_data(self):
        """Load flight tracker data from input CSV file"""
        try:
            inputs_dir = os.path.join(os.path.dirname(__file__), "inputs")
            csv_path = os.path.join(inputs_dir, "flight_tracker_cache.csv")
            
            if os.path.exists(csv_path):
                df = pd.read_csv(csv_path)
                # Convert to dictionary keyed by flight_key
                flight_data = {}
                for _, row in df.iterrows():
                    flight_key = row['flight_key']
                    flight_data[flight_key] = {
                        'flight_key': flight_key,
                        'last_check_time': row['last_check_time'],
                        'total_bookings': int(row['total_bookings']) if pd.notna(row['total_bookings']) else 0,
                        'previous_bookings': int(row['previous_bookings']) if pd.notna(row['previous_bookings']) else 0,
                        'last_threshold_date': row['last_threshold_date'] if pd.notna(row['last_threshold_date']) else None,
                        'last_threshold_bookings': int(row['last_threshold_bookings']) if pd.notna(row['last_threshold_bookings']) else None
                    }
                logger.info(f"Loaded {len(flight_data)} flight tracking records from input CSV")
                return flight_data
            else:
                logger.warning(f"Flight tracker input file not found: {csv_path}")
                return {}
        except Exception as e:
            logger.error(f"Error loading flight tracker data: {e}")
            return {}
    
    def get(self, key):
        """Mock Redis GET with support for flight tracking data"""
        # Check if this is a flight tracking key
        if "flight_tracker:flight:" in key:
            # Extract flight key from cache key
            flight_key = key.replace("flight_tracker:flight:", "")
            
            # Return data from input CSV if available
            if flight_key in self.flight_tracker_data:
                import json
                return json.dumps(self.flight_tracker_data[flight_key])
            else:
                # Return None if flight not found in input data
                return None
        
        return self.data.get(key)
    
    def set(self, key, value, ex=None):
        self.data[key] = value
    
    def mget(self, keys):
        return [self.data.get(key) for key in keys]
    
    def mset(self, mapping):
        self.data.update(mapping)
        self._save_forecast_data(mapping)
    
    def _save_forecast_data(self, mapping):
        """Save forecast data in structured CSV format"""
        try:
            # Check if this is forecast data (keys contain "ImputedForecastPLF:")
            forecast_keys = [key for key in mapping.keys() if key.startswith("ImputedForecastPLF:")]
            
            if forecast_keys:
                # Parse forecast data into structured format
                forecast_records = []
                
                for key, value in mapping.items():
                    if key.startswith("ImputedForecastPLF:"):
                        try:
                            # Parse the key: "ImputedForecastPLF:DELAMD_1146_2026-01-15"
                            key_parts = key.replace("ImputedForecastPLF:", "").split("_")
                            if len(key_parts) >= 3:
                                route_flight = key_parts[0]  # e.g., "DELAMD"
                                flight_number = key_parts[1]  # e.g., "1146"
                                departure_date = key_parts[2]  # e.g., "2026-01-15"
                                
                                # Extract origin and destination from route
                                if len(route_flight) == 6:  # Assuming 3-letter airport codes
                                    origin = route_flight[:3]
                                    destination = route_flight[3:]
                                else:
                                    origin = "UNK"
                                    destination = "UNK"
                                
                                # Parse the JSON value to get PLF history
                                plf_history = json.loads(value) if value else []
                                
                                # Get the latest PLF value
                                latest_plf = None
                                latest_timestamp = None
                                
                                if plf_history and isinstance(plf_history, list):
                                    # Sort by timestamp and get the latest
                                    sorted_history = sorted(plf_history, 
                                                           key=lambda x: x.get('timestamp', ''), 
                                                           reverse=True)
                                    if sorted_history:
                                        latest_plf = sorted_history[0].get('value')
                                        latest_timestamp = sorted_history[0].get('timestamp')
                                
                                forecast_records.append({
                                    'Origin': origin,
                                    'Destination': destination,
                                    'FlightNumber': flight_number,
                                    'DepartureDate': departure_date,
                                    'ImputedForecastPLF': latest_plf,
                                    'Timestamp': latest_timestamp,
                                    'HistoryCount': len(plf_history)
                                })
                                
                        except Exception as e:
                            logger.debug(f"Error parsing forecast key {key}: {e}")
                
                if forecast_records:
                    # Save structured forecast data with simple filename
                    df = pd.DataFrame(forecast_records)
                    filename = "imputed_forecast_output.csv"
                    filepath = os.path.join(self.output_dir, filename)
                    
                    df.to_csv(filepath, index=False)
                    logger.info(f"Saved {len(forecast_records)} forecast records to {filename}")
                
        except Exception as e:
            logger.error(f"Error saving forecast data: {e}")
    
    def pipeline(self):
        return MockPipeline(self)
    
    def expire(self, key, seconds):
        pass  # Mock implementation
    
    def smembers(self, key):
        """Mock Redis SMEMBERS - return members of a set"""
        # For flight tracking, return flight keys from input CSV if this is the all_flights_key
        if "all_flight_keys" in key:
            # Return flight keys from loaded input data
            return set(self.flight_tracker_data.keys())
        return self.sets.get(key, set())
    
    def sadd(self, key, *values):
        """Mock Redis SADD - add members to a set"""
        if key not in self.sets:
            self.sets[key] = set()
        for value in values:
            self.sets[key].add(value)
        return len(values)  # Return number of elements added

class MockPipeline:
    """Mock Redis pipeline that captures batch operations"""
    
    def __init__(self, client):
        self.client = client
        self.commands = []
    
    def mset(self, mapping):
        self.commands.append(('mset', mapping))
        return self
    
    def expire(self, key, seconds):
        self.commands.append(('expire', key, seconds))
        return self
    
    def set(self, key, value):
        self.commands.append(('set', key, value))
        return self
    
    def sadd(self, key, *values):
        self.commands.append(('sadd', key, values))
        return self
    
    def execute(self):
        # Execute all commands and capture forecast data
        results = []
        
        for command in self.commands:
            if command[0] == 'mset':
                mapping = command[1]
                self.client.data.update(mapping)
                # Use the enhanced forecast data saving
                self.client._save_forecast_data(mapping)
                results.append(True)
                
            elif command[0] == 'set':
                key, value = command[1], command[2]
                self.client.data[key] = value
                # Check if this is forecast data
                if key.startswith("ImputedForecastPLF:"):
                    self.client._save_forecast_data({key: value})
                # Save flight tracking data for schedule30
                elif "flight_tracker:flight:" in key:
                    self._save_flight_tracking_data(key, value)
                results.append(True)
                
            elif command[0] == 'sadd':
                key, values = command[1], command[2]
                if key not in self.client.sets:
                    self.client.sets[key] = set()
                for value in values:
                    self.client.sets[key].add(value)
                results.append(len(values))
                
            elif command[0] == 'expire':
                # Mock expire - just log it
                results.append(True)
                
        return results
    
    def _save_flight_tracking_data(self, key, value):
        """Save flight tracking data to CSV for schedule30 module"""
        try:
            if self.client.module_name == 'schedule_thirty':
                import json
                import pandas as pd
                
                # Parse the flight data
                flight_data = json.loads(value) if isinstance(value, str) else value
                
                # Create a record for CSV
                flight_record = {
                    'cache_key': key,
                    'flight_key': flight_data.get('flight_key', ''),
                    'last_check_time': flight_data.get('last_check_time', ''),
                    'total_bookings': flight_data.get('total_bookings', 0),
                    'previous_bookings': flight_data.get('previous_bookings', 0),
                    'last_threshold_date': flight_data.get('last_threshold_date', ''),
                    'last_threshold_bookings': flight_data.get('last_threshold_bookings', 0)
                }
                
                # Save to CSV
                df = pd.DataFrame([flight_record])
                filename = "flight_tracker.csv"
                filepath = os.path.join(self.client.output_dir, filename)
                
                # Append to existing file or create new one
                if os.path.exists(filepath):
                    existing_df = pd.read_csv(filepath)
                    df = pd.concat([existing_df, df], ignore_index=True)
                
                df.to_csv(filepath, index=False)
                logger.info(f"Saved flight tracking data to {filename}")
                
        except Exception as e:
            logger.error(f"Error saving flight tracking data: {e}")

class MockS3Client:
    """Mock S3 client for testing"""
    
    def __init__(self, module_name=None):
        self.module_name = module_name
        self.output_dir = setup_output_directory()
    
    def put_object(self, Bucket, Key, Body, **kwargs):
        """Mock S3 put_object - save to local CSV file"""
        try:
            # Extract filename from S3 key
            filename = Key.split('/')[-1]
            if not filename.endswith('.csv'):
                filename += '.csv'
            
            # Save to output directory
            filepath = os.path.join(self.output_dir, filename)
            
            # Write the body content to file
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(Body)
            
            logger.info(f"Saved S3 upload to {filename}")
            
            # Mock successful response
            return {
                'ResponseMetadata': {
                    'HTTPStatusCode': 200
                }
            }
            
        except Exception as e:
            logger.error(f"Error in mock S3 put_object: {e}")
            raise

class MockMongoCollection:
    """Mock MongoDB collection for testing"""
    
    def __init__(self, collection_name, module_name=None):
        self.collection_name = collection_name
        self.module_name = module_name
        self.output_dir = setup_output_directory()
        self.data = self._load_mock_data()
    
    def _load_mock_data(self):
        """Load mock data for this collection"""
        df = load_mock_mongodb_data(self.collection_name)
        return df.to_dict('records') if not df.empty else []
    
    def find(self, query=None, projection=None):
        """Mock MongoDB find operation"""
        try:
            # For simplicity, return all data (in real implementation, would filter by query)
            results = self.data.copy()
            
            # Apply projection if specified
            if projection and results:
                projected_results = []
                for doc in results:
                    projected_doc = {}
                    for field, include in projection.items():
                        if include and field in doc:
                            projected_doc[field] = doc[field]
                    projected_results.append(projected_doc)
                results = projected_results
            
            return MockMongoCursor(results)
            
        except Exception as e:
            logger.error(f"Error in mock MongoDB find for {self.collection_name}: {e}")
            return MockMongoCursor([])
    
    def find_one(self, query=None, projection=None):
        """Mock MongoDB find_one operation"""
        try:
            if not self.data:
                return None
            
            # For simplicity, return first document (in real implementation, would filter by query)
            doc = self.data[0].copy()
            
            # Apply projection if specified
            if projection:
                projected_doc = {}
                for field, include in projection.items():
                    if include and field in doc:
                        projected_doc[field] = doc[field]
                doc = projected_doc
            
            return doc
            
        except Exception as e:
            logger.error(f"Error in mock MongoDB find_one for {self.collection_name}: {e}")
            return None
    
    def aggregate(self, pipeline):
        """Mock MongoDB aggregation"""
        try:
            # For simplicity, return all data (in real implementation, would process pipeline)
            results = self.data.copy()
            
            # Handle specific aggregation patterns
            if self.collection_name == 'config_station_master':
                # Handle the domestic sectors aggregation
                unique_stations = list(set(doc.get('station', '') for doc in results))
                results = [{'station': station} for station in unique_stations if station]
            
            elif self.collection_name == 'config_profile_fares':
                # Handle the profile fares aggregation (group by sector, month, dow and sum Q10)
                grouped = {}
                for doc in results:
                    key = (doc.get('sector', ''), doc.get('month', ''), doc.get('dow', ''))
                    if key not in grouped:
                        grouped[key] = {
                            'sector': doc.get('sector', ''),
                            'month': doc.get('month', ''),
                            'dow': doc.get('dow', ''),
                            'Q10': 0
                        }
                    grouped[key]['Q10'] += doc.get('Q10', 0)
                
                # Divide by 6 as per the aggregation pipeline
                for item in grouped.values():
                    item['Q10'] = item['Q10'] / 6
                
                results = list(grouped.values())
            
            return MockMongoCursor(results)
            
        except Exception as e:
            logger.error(f"Error in mock MongoDB aggregate for {self.collection_name}: {e}")
            return MockMongoCursor([])
    
    def bulk_write(self, operations, ordered=True):
        """Mock MongoDB bulk_write operation"""
        try:
            # Don't save bulk write operations to CSV for config_profile_fare_station_upsell
            if self.collection_name == 'config_profile_fare_station_upsell':
                logger.info(f"Skipping CSV creation for {self.collection_name} bulk operations")
                # Mock successful bulk write result
                class MockBulkWriteResult:
                    def __init__(self, modified_count, upserted_count):
                        self.modified_count = modified_count
                        self.upserted_count = upserted_count
                
                return MockBulkWriteResult(len(operations), 0)
            
            # For other collections, continue with normal mock behavior
            # Mock successful bulk write result
            class MockBulkWriteResult:
                def __init__(self, modified_count, upserted_count):
                    self.modified_count = modified_count
                    self.upserted_count = upserted_count
            
            return MockBulkWriteResult(len(operations), 0)
            
        except Exception as e:
            logger.error(f"Error in mock MongoDB bulk_write for {self.collection_name}: {e}")
            # Return empty result
            class MockBulkWriteResult:
                def __init__(self):
                    self.modified_count = 0
                    self.upserted_count = 0
            return MockBulkWriteResult()

class MockMongoCursor:
    """Mock MongoDB cursor for iteration"""
    
    def __init__(self, data):
        self.data = data
        self.index = 0
    
    def __iter__(self):
        return self
    
    def __next__(self):
        if self.index >= len(self.data):
            raise StopIteration
        result = self.data[self.index]
        self.index += 1
        return result
    
    def sort(self, field, direction=1):
        """Mock sort operation"""
        try:
            if isinstance(field, str):
                self.data.sort(key=lambda x: x.get(field, ''))
            return self
        except:
            return self
    
    def head(self, n):
        """Mock head operation"""
        return MockMongoCursor(self.data[:n])

class MockMongoDatabase:
    """Mock MongoDB database"""
    
    def __init__(self, module_name=None):
        self.module_name = module_name
        self.collections = {}
    
    def __getitem__(self, collection_name):
        """Get or create a mock collection"""
        if collection_name not in self.collections:
            self.collections[collection_name] = MockMongoCollection(collection_name, self.module_name)
        return self.collections[collection_name]

class MockMongoClient:
    """Mock MongoDB client"""
    
    def __init__(self, connection_string, module_name=None):
        self.module_name = module_name
        self.databases = {}
    
    def __getitem__(self, db_name):
        """Get or create a mock database"""
        if db_name not in self.databases:
            self.databases[db_name] = MockMongoDatabase(self.module_name)
        return self.databases[db_name]

class MockSQLConnection:
    """Mock SQL connection for testing"""
    
    def __init__(self, csv_file_path):
        self.csv_file_path = csv_file_path
    
    def execute(self, query):
        # Mock execute method
        pass

def load_mock_sql_data(csv_filename):
    """Load mock data from CSV file to simulate SQL query results"""
    inputs_dir = os.path.join(os.path.dirname(__file__), "inputs")
    csv_path = os.path.join(inputs_dir, csv_filename)
    
    if os.path.exists(csv_path):
        return pd.read_csv(csv_path)
    else:
        logger.warning(f"Input data file not found: {csv_path}")
        return pd.DataFrame()

def load_mock_mongodb_data(collection_name):
    """Load mock data from CSV file to simulate MongoDB query results"""
    inputs_dir = os.path.join(os.path.dirname(__file__), "inputs")
    csv_path = os.path.join(inputs_dir, f"{collection_name}.csv")
    
    if os.path.exists(csv_path):
        return pd.read_csv(csv_path)
    else:
        logger.warning(f"MongoDB mock data file not found: {csv_path}")
        return pd.DataFrame()

def setup_mock_sql():
    """Setup mock pandas read_sql function"""
    try:
        import pandas as pd
        original_read_sql = pd.read_sql
        
        def mock_read_sql(query, connection, **kwargs):
            """Mock pandas read_sql to return data from CSV files"""
            query_lower = query.lower()
            
            # Determine which mock data to return based on query content
            if 'navitaire' in str(connection).lower() or 'inventoryleg' in query_lower:
                # Check if this is the schedule30 flight bookings query
                if 'flightbookings' in query_lower or 'totalbookings' in query_lower or 'bookingpassenger' in query_lower:
                    df = load_mock_sql_data('flight_bookings.csv')
                    if not df.empty:
                        # Ensure FlightNumber is string type
                        if 'FlightNumber' in df.columns:
                            df['FlightNumber'] = df['FlightNumber'].astype(str)
                    return df
                else:
                    # This is the regular navitaire flights query
                    df = load_mock_sql_data('navitaire_flights.csv')
                    if not df.empty and 'FlightNumber' in df.columns:
                        df['FlightNumber'] = df['FlightNumber'].astype(str)
                    return df
            elif 'forecast_results' in query_lower:
                df = load_mock_sql_data('forecast_results.csv')
                if not df.empty and 'FlightNumber' in df.columns:
                    df['FlightNumber'] = df['FlightNumber'].astype(str)
                    # Convert DepartureDate to datetime then to date (like real SQL would return)
                    df['DepartureDate'] = pd.to_datetime(df['DepartureDate']).dt.date
                    # Rename Variance to forecast_variance to match SQL alias
                    if 'Variance' in df.columns:
                        df.rename(columns={'Variance': 'forecast_variance'}, inplace=True)
                return df
            elif 'sp_bookeddata' in query_lower or 'bookeddata' in query_lower:
                df = load_mock_sql_data('booked_loads.csv')
                if not df.empty and 'FlightNumber' in df.columns:
                    df['FlightNumber'] = df['FlightNumber'].astype(str)
                    # Convert DepartureDate to datetime then to date (like real SQL would return)
                    df['DepartureDate'] = pd.to_datetime(df['DepartureDate']).dt.date
                return df
            else:
                logger.warning(f"Unknown query pattern, returning empty DataFrame: {query[:100]}...")
                return pd.DataFrame()
        
        # Replace pandas read_sql with mock version
        pd.read_sql = mock_read_sql
        logger.info("Patched pandas read_sql function")
        
    except ImportError:
        logger.warning("pandas not available for mocking")