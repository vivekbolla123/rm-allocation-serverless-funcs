"""
Test lambda function for local testing with MongoDB
Uses actual scripts with local MongoDB connection
"""
import logging
import pymongo
import sys
import json
import os
from datetime import datetime

# Ensure we're in the correct directory
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

# Get the current script directory and parent directory
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
root_dir = os.path.dirname(parent_dir)

# Add paths for actual configuration files and scripts
config_path = os.path.join(root_dir, 'rm-allocation-configurations')
sys.path.insert(0, config_path)
sys.path.insert(0, parent_dir)  # Add the scheduled jobs directory
sys.path.insert(0, root_dir)    # Add the root directory

print(f"Current directory: {current_dir}")
print(f"Parent directory: {parent_dir}")
print(f"Root directory: {root_dir}")
print(f"Config path: {config_path}")
print(f"Working directory: {os.getcwd()}")

# Check if files exist
config_files = ['Constants.py', 'configsettings.py', 'constants_utils.py']
for file in config_files:
    file_path = os.path.join(config_path, file)
    if os.path.exists(file_path):
        print(f"Found {file}")
    else:
        print(f"Missing {file} at {file_path}")

script_files = ['defaultMarketlist.py', 'updateImpForecast.py', 'schedule30.py']
for file in script_files:
    file_path = os.path.join(parent_dir, file)
    if os.path.exists(file_path):
        print(f"Found {file}")
    else:
        print(f"Missing {file} at {file_path}")

try:
    import Constants
    import configsettings
    from constants_utils import ConstantsUtils
    print("Successfully imported configuration modules")
except ImportError as e:
    print(f"Failed to import configuration modules: {e}")
    print("Available files in config directory:")
    if os.path.exists(config_path):
        for f in os.listdir(config_path):
            if f.endswith('.py'):
                print(f"  - {f}")
    sys.exit(1)

# Import mock handlers
from mock_handlers import (
    patch_mongodb_operations, 
    MockCacheClient, 
    MockSQLConnection, 
    setup_mock_sql,
    clean_old_output_files,
    setup_output_directory,
    MockS3Client,
    MockMongoClient
)

# Patch ConstantsUtils to avoid database dependency
def patch_constants_utils():
    """Patch ConstantsUtils to skip database queries in testing"""
    original_init = ConstantsUtils.__init__
    
    def mock_init(self, db_connection):
        """Mock ConstantsUtils initialization with default values"""
        self.db_connection = db_connection
        # Set default values for testing
        self.IMP_FORECAST_DEDUCTION = 2.0
        self.DEFAULT_MARKET_LIST = '{"Origin": "DEL", "Destin": "AMD", "FlightNumber": "1146", "DepartureDate": "2026-01-15"}'  # Test JSON object with Destin
        self.DEFAULT_MARKET_LIST_DAYS = 30  # Default days for market list
        self.BOOKINGS_THRESHOLD = 10  # Default booking threshold
        self.parameters_df = None
    
    def mock_get_parameters(self):
        """Return empty DataFrame to skip database parameter loading"""
        import pandas as pd
        return pd.DataFrame()
    
    ConstantsUtils.__init__ = mock_init
    ConstantsUtils.get_parameters = mock_get_parameters
    print("Patched ConstantsUtils.__init__ and get_parameters")

# Apply the patch
patch_constants_utils()

# Import actual modules
try:
    from defaultMarketlist import defaultMarketlist
    print("Successfully imported defaultMarketlist")
except ImportError as e:
    print(f"Failed to import defaultMarketlist: {e}")
    print("Available files in parent directory:")
    if os.path.exists(parent_dir):
        for f in os.listdir(parent_dir):
            if f.endswith('.py'):
                print(f"  - {f}")
    sys.exit(1)

try:
    from updateImpForecast import update_imputed_forecast
    print("Successfully imported update_imputed_forecast")
except ImportError as e:
    print(f"Failed to import update_imputed_forecast: {e}")
    sys.exit(1)

try:
    from schedule30 import ScheduleThirty
    print("Successfully imported ScheduleThirty")
except ImportError as e:
    print(f"Failed to import ScheduleThirty: {e}")
    sys.exit(1)

try:
    from heuristicCalc import FareStatisticalAnalysis
    print("Successfully imported FareStatisticalAnalysis")
except ImportError as e:
    print(f"Failed to import FareStatisticalAnalysis: {e}")
    sys.exit(1)

print("Successfully imported all modules")

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Setup console handler for logging
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Apply patches
patch_mongodb_operations()
setup_mock_sql()

try:
    mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
    # Test the connection
    mongo_client.admin.command('ping')
    logger.info("Connected to local MongoDB successfully")
except Exception as e:
    logger.error(f"Failed to connect to local MongoDB: {e}")
    sys.exit(1)

module_map = {
    "default_ml": defaultMarketlist,
    "imputed_forecast": update_imputed_forecast,
    "schedule_thirty": ScheduleThirty,
    "heuristic_calc": FareStatisticalAnalysis,
}

def test_lambda_handler(event, context=None):
    """Test version of lambda handler for local testing"""
    
    if "Records" in event:
        events = event["Records"]
        for i in events:
            body = i["body"]
    else:
        body = event["body"]

    data = json.loads(body)
    print(f"Processing event data: {data}")

    if "modules" not in data:
        logger.info("Error! 'modules' key missing in body ")
        return False

    modules = data["modules"]
    overrides = data.get("overrides", {})

    # Create output directory
    output_dir = setup_output_directory()

    for module_key in modules.keys():
        if module_key not in module_map:
            logger.warning(f"Module {module_key} not available in test environment")
            continue
            
        logger.info(f"Starting {module_key}")
        
        # Clean old output files for this module
        clean_old_output_files(module_key, output_dir)
        
        # Setup parameters for testing
        params = {
            "mongoconn": MockMongoClient("mongodb://localhost:27017/", module_key) if module_key == "heuristic_calc" else mongo_client,
            "dwconn": MockSQLConnection("forecast_results.csv"),  # Mock DW connection
            "navconn": MockSQLConnection("navitaire_flights.csv"),  # Mock Navitaire connection
            "rdEsbConn": MockSQLConnection("booked_loads.csv"),  # Mock ESB connection
            "cache_client": MockCacheClient(module_key),
            "s3": MockS3Client(module_key),  # Mock S3 client for heuristic_calc
            "testing_mode": True  # Flag to indicate this is a test run
        }
        
        try:
            Module = module_map[module_key]
            Module_obj = Module(logger, params, overrides, context)
            
            # Run the module
            result = Module_obj.run()
            
            logger.info(f"{module_key} completed successfully")
            
        except Exception as e:
            logger.error(f"{module_key} failed: {str(e)}")
            import traceback
            traceback.print_exc()

    return {
        "statusCode": 200, 
        "body": "Test execution completed."
    }

def run_individual_test(module_name):
    """Run individual module test"""
    logger.info(f"Running individual test for: {module_name}")
    
    event = {
        "body": json.dumps({
            "modules": {module_name: 1}
        })
    }
    
    return test_lambda_handler(event)

def run_all_tests():
    """Run all available tests"""
    logger.info("Running all available tests")
    
    # Comment out tests to be skipped
    event = {
        "body": json.dumps({
            "modules": {
                # "default_ml": 1,
                # "imputed_forecast": 1,
                "schedule_thirty": 1,
                # "heuristic_calc": 1
            }
        })
    }
    
    return test_lambda_handler(event)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Run tests for RM allocation scheduled jobs')
    parser.add_argument('--module', choices=['default_ml', 'imputed_forecast', 'schedule_thirty', 'heuristic_calc', 'all'],
                       default='all', help='Which module to test')
    
    args = parser.parse_args()
    
    logger.info("Starting RM Allocation Scheduled Jobs Testing")
    logger.info(f"Test started at: {datetime.now()}")
    
    try:
        if args.module == 'all':
            run_all_tests()
        else:
            run_individual_test(args.module)
            
        logger.info("Test execution completed!")
        
    except Exception as e:
        logger.error(f"Test execution failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        logger.info(f"Test completed at: {datetime.now()}")