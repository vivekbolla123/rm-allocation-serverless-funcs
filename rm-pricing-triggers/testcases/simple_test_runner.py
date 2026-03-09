"""
Simple Test Runner for Local Testing

Just change TEST_MODULE to test different modules.
This replaces database connections with CSV file reads automatically.
"""

import sys
import os
import logging
import importlib
import pandas as pd
from datetime import datetime
import traceback

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Mock the missing modules before importing anything else
def setup_mock_modules():
    """Setup mock modules to replace missing dependencies."""
    import types
    
    # Mock configsettings
    mock_configsettings = types.ModuleType('configsettings')
    mock_configsettings.NAV_DB_NAME = "[TEST_NAV]."
    mock_configsettings.NAV_DB_DW_NAME = "[TEST_DW]."
    mock_configsettings.NAV_DB_WB_NAME = "[TEST_WB]."
    mock_configsettings.NAV_DB_HOST = "localhost"
    mock_configsettings.NAV_DB_USER = "test_user"
    mock_configsettings.NAV_DB_PASSWORD = "test_password"
    mock_configsettings.MONGO_USERNAME = "test_mongo_user"
    mock_configsettings.MONGO_PASSWORD = "test_mongo_password"
    mock_configsettings.MONGO_URL = "localhost"
    mock_configsettings.MONGO_AUTH_SOURCE = "admin"
    mock_configsettings.ESB_DB_USERNAME = "test_esb_user"
    mock_configsettings.ESB_DB_PASSWORD = "test_esb_password"
    mock_configsettings.ESB_DB_WR_APPUSER_URL = "localhost"
    mock_configsettings.ESB_NAV_SCHEMA_NAME = "TEST_ESB_SCHEMA"
    mock_configsettings.RM_DB_SCHEMA_NAME = "TEST_RM_SCHEMA"
    mock_configsettings.PA_BUCKET_NAME = "test-bucket"
    mock_configsettings.PA_FILE_PATH = "test/PA/"
    mock_configsettings.env = "test"
    sys.modules['configsettings'] = mock_configsettings
    
    # Mock constants_utils
    from mock_constants_utils import ConstantsUtils
    mock_constants_utils = types.ModuleType('constants_utils')
    mock_constants_utils.ConstantsUtils = ConstantsUtils
    sys.modules['constants_utils'] = mock_constants_utils
    
    print("[TEST MODE] Mock modules setup complete")

# Setup mocks before importing other modules
setup_mock_modules()

from simple_mock_helper import SimpleMockHelper, TEST_MODULE, OUTPUTS_DIR

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Module configurations
MODULE_CONFIGS = {
    'sales_fare': {
        'module_file': 'salesFare',
        'class_name': 'SalesFare',
        'helper_class': 'SalesPAHelper',
        'helper_path': 'helpers.pa_db_helpers.sales_pa_helper',
        'overrides': {'analyst': 'Test User'}
    },
    'ladder_gen': {
        'module_file': 'ladderGen',
        'class_name': 'LadderGen', 
        'helper_class': 'LadderPAHelper',
        'helper_path': 'helpers.pa_db_helpers.ladder_pa_helper',
        'overrides': {'route': 'D', 'analyst': 'Test User', 'sector': 'BOMDEL'}
    },
    'action_sect': {
        'module_file': 'actionableSectors',
        'class_name': 'ActionSectors',
        'helper_class': 'ActionPAHelper', 
        'helper_path': 'helpers.pa_db_helpers.action_pa_helper',
        'overrides': {'route': 'D', 'analyst': 'Test User'}
    }
}


class SimpleTestRunner:
    """
    Generic test runner that works with any pricing module.
    Automatically replaces database helpers with CSV-based mock helpers.
    """
    
    def __init__(self, module_key):
        self.module_key = module_key
        self.logger = logger
        
        # Get module configuration
        if module_key not in MODULE_CONFIGS:
            raise ValueError(f"Module '{module_key}' not configured. Available: {list(MODULE_CONFIGS.keys())}")
        
        self.config = MODULE_CONFIGS[module_key]
        
        # Mock database connections (not used in test mode)
        self.mock_db = None
        self.mock_nav_conn = None
        
        # Create mock parameters
        self.params = {
            'mongo': self.mock_db,
            'mongofares': self.mock_db,
            'navconn': self.mock_nav_conn,
            'rdEsbConn': None,
            'sqs': None,
            's3': None,
            'events': None,
            'constantutils': None
        }
        
        self.overrides = self.config['overrides']
        self.context = {'test_mode': True}
    
    def inject_mock_helper(self):
        """
        Replace the real helper class with our mock helper.
        This is the key to making the test work without database connections.
        """
        try:
            # Import the helper module
            helper_module = importlib.import_module(self.config['helper_path'])
            
            # Get the original helper class
            original_helper_class = getattr(helper_module, self.config['helper_class'])
            
            # Create a mock version that inherits from SimpleMockHelper
            module_key = self.module_key  # Capture in closure
            class MockedHelper(SimpleMockHelper):
                def __init__(self, *args, **kwargs):
                    # Handle different constructor signatures
                    if len(args) >= 2:
                        db, logger = args[0], args[1] if len(args) > 1 else None
                    else:
                        db, logger = kwargs.get('db'), kwargs.get('logger')
                    
                    super().__init__(db, logger, module_key)
                    # Copy any constants from original class
                    if hasattr(original_helper_class, 'CONST_VALUE'):
                        self.CONST_VALUE = original_helper_class.CONST_VALUE
                    
                    # Store additional arguments for compatibility
                    self.extra_args = args[2:] if len(args) > 2 else []
                    self.extra_kwargs = kwargs
            
            # Replace the class in the module
            setattr(helper_module, self.config['helper_class'], MockedHelper)
            
            self.logger.info(f"✓ Injected mock helper for {self.config['helper_class']}")
            
            # Mock nav and esb helpers based on module
            if self.module_key == 'sales_fare':
                self._mock_nav_helper('sales_nav_helper', 'SalesNavHelper')
            elif self.module_key == 'action_sect':
                self._mock_nav_helper('action_nav_helper', 'ActionNavHelper')
                self._mock_esb_helper('ladder_esb_helper', 'LadderEsbHelper')
            elif self.module_key == 'ladder_gen':
                self._mock_nav_helper('ladder_nav_helper', 'LadderNavHelper')
                self._mock_esb_helper('ladder_esb_helper', 'LadderEsbHelper')
            
        except Exception as e:
            self.logger.error(f"Failed to inject mock helper: {str(e)}")
            raise
    
    def _mock_nav_helper(self, helper_file, helper_class):
        """Mock the nav helper for any module."""
        try:
            # Import nav helper module
            nav_helper_module = importlib.import_module(f'helpers.nav_db_helpers.{helper_file}')
            
            # Import INPUTS_DIR from simple_mock_helper
            from simple_mock_helper import INPUTS_DIR
            
            # Create mock nav helper class
            class MockNavHelper:
                def __init__(self, nav_conn, nav_db_name, nav_db_dw_name, nav_db_wb_name, logger):
                    self.nav_conn = nav_conn
                    self.nav_db_name = nav_db_name
                    self.nav_db_dw_name = nav_db_dw_name
                    self.nav_db_wb_name = nav_db_wb_name
                    self.logger = logger
                    self.failure_reason = None
                
                def collect_active_sectors(self):
                    """Mock method that reads from CSV instead of database."""
                    csv_path = os.path.join(INPUTS_DIR, 'active_sectors.csv')
                    if os.path.exists(csv_path):
                        df = pd.read_csv(csv_path)
                        self.logger.info(f"[TEST MODE] Loaded {len(df)} active sectors from CSV")
                        return df
                    else:
                        self.logger.warning(f"[TEST MODE] Active sectors CSV not found: {csv_path}")
                        return pd.DataFrame(columns=['DepartureStation', 'ArrivalStation'])
                
                def collect_booked_data(self, date_range_days=30):
                    """Mock method for booked data."""
                    csv_path = os.path.join(INPUTS_DIR, 'booked_data.csv')
                    if os.path.exists(csv_path):
                        df = pd.read_csv(csv_path)
                        # Parse date columns
                        if 'DepartureDate' in df.columns:
                            df['DepartureDate'] = pd.to_datetime(df['DepartureDate'], errors='coerce')
                        if 'DepartureTimeUTC' in df.columns:
                            df['DepartureTimeUTC'] = pd.to_datetime(df['DepartureTimeUTC'], errors='coerce')
                        self.logger.info(f"[TEST MODE] Loaded {len(df)} booked data records from CSV")
                        return df
                    else:
                        self.logger.warning(f"[TEST MODE] Booked data CSV not found: {csv_path}")
                        return pd.DataFrame()
                
                def collect_combined_booking_data(self, start_date, end_date, sector, channel):
                    """Mock method for combined booking data."""
                    csv_path = os.path.join(INPUTS_DIR, 'booked_data.csv')
                    if os.path.exists(csv_path):
                        df = pd.read_csv(csv_path)
                        # Filter by sector if provided
                        if sector:
                            df = df[df['Origin'] + df['Destination'] == sector]
                        # Parse date columns
                        if 'DepartureDate' in df.columns:
                            df['DepartureDate'] = pd.to_datetime(df['DepartureDate'], errors='coerce')
                        if 'DepartureTimeUTC' in df.columns:
                            df['DepartureTimeUTC'] = pd.to_datetime(df['DepartureTimeUTC'], errors='coerce')
                        self.logger.info(f"[TEST MODE] Loaded {len(df)} combined booking records from CSV")
                        return df
                    else:
                        self.logger.warning(f"[TEST MODE] Booking data CSV not found: {csv_path}")
                        return pd.DataFrame()
                
                def collect_city_pair_data(self, sector=None):
                    """Mock method for city pair data."""
                    csv_path = os.path.join(INPUTS_DIR, 'city_pairs.csv')
                    if os.path.exists(csv_path):
                        df = pd.read_csv(csv_path)
                        if sector:
                            # Filter for specific sector if provided
                            origin = sector[:3]
                            destination = sector[3:]
                            df = df[(df['DepartureStation'] == origin) & (df['ArrivalStation'] == destination)]
                        self.logger.info(f"[TEST MODE] Loaded {len(df)} city pair records from CSV")
                        return df
                    else:
                        self.logger.warning(f"[TEST MODE] City pairs CSV not found: {csv_path}")
                        return pd.DataFrame()
            
            # Replace the class
            setattr(nav_helper_module, helper_class, MockNavHelper)
            self.logger.info(f"✓ Injected mock nav helper for {helper_class}")
            
        except Exception as e:
            self.logger.error(f"Failed to inject mock nav helper: {str(e)}")
            # Don't raise - nav helper is optional for some tests
    
    def _mock_esb_helper(self, helper_file, helper_class):
        """Mock the ESB helper for any module."""
        try:
            # Import esb helper module
            esb_helper_module = importlib.import_module(f'helpers.esb_db_helpers.{helper_file}')
            
            # Import INPUTS_DIR from simple_mock_helper
            from simple_mock_helper import INPUTS_DIR
            
            # Create mock esb helper class
            module_key = self.module_key  # Capture in closure
            class MockEsbHelper:
                def __init__(self, db_fares, logger):
                    self.db_fares = db_fares
                    self.logger = logger
                    self.failure_reason = None
                    self.module_key = module_key  # Store module key
                
                def collect_combined_market_fare_data(self, ndo, sector, channel):
                    """Mock method for combined market fare data."""
                    # Use different CSV files based on the module being tested
                    if self.module_key == 'ladder_gen':
                        csv_path = os.path.join(INPUTS_DIR, 'booking_data.csv')  # Historical fare data
                    else:
                        csv_path = os.path.join(INPUTS_DIR, 'booked_data.csv')   # Flight capacity data
                    
                    if os.path.exists(csv_path):
                        df = pd.read_csv(csv_path)
                        # Filter by sector if provided
                        if sector:
                            origin = sector[:3]
                            destination = sector[3:]
                            if 'source_airport' in df.columns and 'destination_airport' in df.columns:
                                df = df[(df['source_airport'] == origin) & (df['destination_airport'] == destination)]
                            elif 'Origin' in df.columns and 'Destination' in df.columns:
                                df = df[df['Origin'] + df['Destination'] == sector]
                        # Rename gross_fare to Fare if needed
                        if 'gross_fare' in df.columns and 'Fare' not in df.columns:
                            df = df.rename(columns={'gross_fare': 'Fare'})
                        self.logger.info(f"[TEST MODE] Loaded {len(df)} market fare records from {csv_path} for NDO={ndo}")
                        return df
                    else:
                        self.logger.warning(f"[TEST MODE] Market fare data CSV not found: {csv_path}")
                        return pd.DataFrame()
            
            # Replace the class
            setattr(esb_helper_module, helper_class, MockEsbHelper)
            self.logger.info(f"✓ Injected mock esb helper for {helper_class}")
            
        except Exception as e:
            self.logger.error(f"Failed to inject mock esb helper: {str(e)}")
            # Don't raise - esb helper is optional for some tests
    
    def run_module(self):
        """
        Run the specified module with mock helpers.
        """
        try:
            self.logger.info(f"Starting test run for module: {self.module_key}")
            
            # Inject mock helper before importing the module
            self.inject_mock_helper()
            
            # Use unittest.mock to patch datetime and uuid
            from unittest.mock import patch
            from simple_mock_helper import FIXED_REFERENCE_TIME
            
            # Create mock UUID that returns a fixed string
            class MockUUID:
                def __str__(self):
                    return 'test-fixed-uuid-12345'
            
            with patch('datetime.datetime') as mock_datetime, \
                 patch('uuid.uuid4', return_value=MockUUID()):
                
                # Configure the mock datetime
                mock_datetime.now.return_value = FIXED_REFERENCE_TIME
                mock_datetime.side_effect = lambda *args, **kw: FIXED_REFERENCE_TIME
                
                # Import the module
                module = importlib.import_module(self.config['module_file'])
                
                # Get the class
                module_class = getattr(module, self.config['class_name'])
                
                # Create instance
                module_instance = module_class(
                    logger=self.logger,
                    params=self.params,
                    overrides=self.overrides,
                    context=self.context
                )
                
                # Run the module
                self.logger.info("Executing module.run()...")
                result = module_instance.run()
                
                self.logger.info("✓ Module execution completed successfully")
                
                return result
            
        except Exception as e:
            self.logger.error(f"✗ Module execution failed: {str(e)}")
            self.logger.error(traceback.format_exc())
            
            raise


def main():
    """
    Main function - runs the test for the configured module.
    """
    try:
        print(f"🚀 Starting Simple Test Runner")
        print(f"📋 Module: {TEST_MODULE}")
        print(f"📁 Output Directory: {OUTPUTS_DIR}")
        print("-" * 50)
        
        # Create and run test
        runner = SimpleTestRunner(TEST_MODULE)
        result = runner.run_module()
        
        print("-" * 50)
        print("✅ Test completed successfully!")
        
        if result is not None:
            if hasattr(result, 'empty') and not result.empty:  # DataFrame
                print(f"📊 Output rows: {len(result)}")
                print(f"📋 Columns: {list(result.columns)}")
                
                # Show first few rows
                print("\n📄 First 3 rows:")
                print(result.head(3).to_string())
            elif isinstance(result, dict):  # Dictionary
                print(f"📊 Dictionary keys: {len(result)}")
                print(f"📋 Keys: {list(result.keys())}")
            elif isinstance(result, list):  # List
                print(f"📊 List items: {len(result)}")
            else:  # Other types
                print(f"📊 Result type: {type(result)}")
        else:
            print("⚠️  No output data generated")
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        print("\n🔍 Full error:")
        print(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()