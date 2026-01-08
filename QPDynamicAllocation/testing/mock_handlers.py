"""Mock handlers for QPDynamicAllocation testing"""
import os
import json
import pandas as pd
import logging
from datetime import datetime
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

def setup_output_directory():
    """Setup output directory for test results"""
    output_dir = os.path.join(os.path.dirname(__file__), "output")
    os.makedirs(output_dir, exist_ok=True)
    return output_dir

def clean_old_output_files(module_name, output_dir):
    """Clean old output files for the specified module"""
    try:
        patterns = {
            'last_sold_fare': ['last_sold_fare_output*.csv', 'redis_operations*.csv']
        }
        
        if module_name in patterns:
            import glob
            for pattern in patterns[module_name]:
                files = glob.glob(os.path.join(output_dir, pattern))
                for file in files:
                    try:
                        os.remove(file)
                    except Exception as e:
                        pass
    except Exception as e:
        pass

def load_mock_sql_data(csv_filename):
    """Load mock data from CSV file to simulate SQL query results"""
    inputs_dir = os.path.join(os.path.dirname(__file__), "inputs")
    csv_path = os.path.join(inputs_dir, csv_filename)
    
    if os.path.exists(csv_path):
        return pd.read_csv(csv_path)
    else:
        return pd.DataFrame()

def setup_mock_sql():
    """Setup mock SQL data patching"""
    def mock_read_sql(query, connection, **kwargs):
        """Mock pandas read_sql to return data from CSV files"""
        
        # Return mock booking data for LastSoldFare
        df = load_mock_sql_data('last_sold_bookings.csv')
        if not df.empty:
            # Ensure proper data types
            if 'BookingUTC' in df.columns:
                df['BookingUTC'] = pd.to_datetime(df['BookingUTC'])
            if 'DepartureDate' in df.columns:
                df['DepartureDate'] = pd.to_datetime(df['DepartureDate']).dt.date
        return df
    
    # Patch pandas.read_sql
    import pandas as pd
    pd.read_sql = mock_read_sql

class MockRedisClient:
    """Mock Redis client that captures operations"""
    
    def __init__(self):
        self.data = {}
        self.execution_time_key = "LastSoldFare:executionTime"
        
    def ping(self):
        """Mock ping method"""
        return True
    
    def exists(self, key):
        """Mock exists method"""
        return key in self.data
    
    def get(self, key):
        """Mock get method"""
        value = self.data.get(key)
        if value is None:
            return None
        # Return as bytes to simulate Redis behavior
        return value.encode('utf-8') if isinstance(value, str) else value
    
    def set(self, key, value):
        """Mock set method"""
        if isinstance(value, bytes):
            value = value.decode('utf-8')
        self.data[key] = value
        return True

class MockSQLConnection:
    """Mock SQL connection for Navitaire database"""
    
    def __init__(self, csv_file_path):
        self.csv_file_path = csv_file_path
        self.query_count = 0
    
    def execute(self, query):
        """Mock execute method"""
        self.query_count += 1
        return True
    
    def close(self):
        """Mock close method"""
        pass

class MockMongoCollection:
    """Mock MongoDB collection"""
    
    def __init__(self, collection_name):
        self.collection_name = collection_name
        self.data = self._load_mock_data()
    
    def _load_mock_data(self):
        """Load mock data for this collection"""
        inputs_dir = os.path.join(os.path.dirname(__file__), "inputs")
        csv_path = os.path.join(inputs_dir, f"{self.collection_name}.csv")
        
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path)
            return df.to_dict('records')
        else:
            # Return default data for rm_parameter_values
            if self.collection_name == 'rm_parameter_values':
                return [
                    {
                        'parameterKey': 'fare_filter_allfares',
                        'parameterValue': json.dumps({
                            'excluded_class_prefixes': ['G'],
                            'excluded_classes': ['G7'],
                            'included_fare_basis': []
                        })
                    },
                    {
                        'parameterKey': 'fare_filter_b2bfares',
                        'parameterValue': json.dumps({
                            'excluded_class_prefixes': ['G'],
                            'excluded_classes': ['G7'],
                            'included_fare_basis': ['M', 'R']
                        })
                    },
                    {
                        'parameterKey': 'fare_filter_b2cfares',
                        'parameterValue': json.dumps({
                            'excluded_class_prefixes': ['G'],
                            'excluded_classes': ['G7'],
                            'included_fare_basis': ['C', 'V', 'Z']
                        })
                    }
                ]
            return []
    
    def find(self, query=None):
        """Mock MongoDB find operation"""
        results = self.data.copy()
        
        # Apply basic query filtering
        if query:
            filtered_results = []
            for doc in results:
                match = True
                for key, value in query.items():
                    if key == 'parameterKey' and isinstance(value, dict) and '$in' in value:
                        # Handle $in operator
                        if doc.get(key) not in value['$in']:
                            match = False
                            break
                    elif doc.get(key) != value:
                        match = False
                        break
                if match:
                    filtered_results.append(doc)
            results = filtered_results
        
        return MockMongoCursor(results)

class MockMongoCursor:
    """Mock MongoDB cursor"""
    
    def __init__(self, data):
        self.data = data
    
    def __iter__(self):
        return iter(self.data)
    
    def __next__(self):
        return next(iter(self.data))

class MockMongoDatabase:
    """Mock MongoDB database"""
    
    def __init__(self):
        self.collections = {}
    
    def __getitem__(self, collection_name):
        """Get or create a mock collection"""
        if collection_name not in self.collections:
            self.collections[collection_name] = MockMongoCollection(collection_name)
        return self.collections[collection_name]

class MockMongoClient:
    """Mock MongoDB client"""
    
    def __init__(self, connection_string):
        self.connection_string = connection_string
        self.databases = {}
    
    def __getitem__(self, db_name):
        """Get or create a mock database"""
        if db_name not in self.databases:
            self.databases[db_name] = MockMongoDatabase()
        return self.databases[db_name]
    
    def close(self):
        """Mock close method"""
        pass