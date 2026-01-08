"""
Test framework for QPDynamicAllocation/update_last_sold_fare.py
Uses runtime patching to replace connections and capture output
"""
import logging
import pandas as pd
import sys
import json
import os
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

# Ensure we're in the correct directory
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

# Get the current script directory and parent directory
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
root_dir = os.path.dirname(parent_dir)

# Add paths
sys.path.insert(0, current_dir)   # Add current directory first
sys.path.insert(0, parent_dir)    # Add QPDynamicAllocation directory
sys.path.insert(0, root_dir)      # Add root directory

print(f"Current directory: {current_dir}")
print(f"Parent directory: {parent_dir}")
print(f"Root directory: {root_dir}")
print(f"Working directory: {os.getcwd()}")

# Import mock handlers
try:
    from mock_handlers import (
        MockRedisClient,
        MockSQLConnection,
        MockMongoClient,
        setup_mock_sql,
        setup_output_directory,
        clean_old_output_files,
        load_mock_sql_data
    )
    print("Successfully imported mock handlers")
except ImportError as e:
    print(f"Failed to import mock handlers: {e}")
    sys.exit(1)

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

def patch_last_sold_fare_connections():
    """Patch all connections in LastSoldFare class for testing"""
    
    # Mock AWS SSM client
    mock_ssm_client = MagicMock()
    mock_ssm_client.get_parameter.return_value = {
        'Parameter': {'Value': 'mock_password'}
    }
    
    # Mock pymssql connection
    mock_nav_connection = MockSQLConnection("last_sold_bookings.csv")
    
    # Mock Redis client
    mock_redis_client = MockRedisClient()
    
    # Mock MongoDB client
    mock_mongo_client = MockMongoClient("mongodb://localhost:27017/")
    
    # Apply patches - patch at module level after import
    patches = [
        patch('update_last_sold_fare.client', return_value=mock_ssm_client),
        patch('update_last_sold_fare.pymssql.connect', return_value=mock_nav_connection),
        patch('update_last_sold_fare.Redis', return_value=mock_redis_client),
        patch('update_last_sold_fare.MongoClient', return_value=mock_mongo_client),
    ]
    
    return patches, mock_redis_client, mock_nav_connection, mock_mongo_client

def test_last_sold_fare():
    """Test the LastSoldFare functionality"""
    logger.info("Starting LastSoldFare testing")
    
    # Setup output directory
    output_dir = setup_output_directory()
    clean_old_output_files('last_sold_fare', output_dir)
    
    # Setup mock SQL data first
    setup_mock_sql()
    
    # Apply patches
    patches, mock_redis, mock_nav, mock_mongo = patch_last_sold_fare_connections()
    
    # Start all patches
    for p in patches:
        p.start()
    
    try:
        # Check if the original script exists
        original_script = os.path.join(parent_dir, 'update_last_sold_fare.py')
        if not os.path.exists(original_script):
            logger.error(f"Original script not found: {original_script}")
            return False
        
        # Import and run the LastSoldFare class
        try:
            from update_last_sold_fare import LastSoldFare
            logger.info("Successfully imported LastSoldFare")
        except ImportError as e:
            logger.error(f"Failed to import LastSoldFare: {e}")
            return False
        
        # Create instance (this will run the entire process)
        logger.info("Creating LastSoldFare instance...")
        last_sold_fare = LastSoldFare()
        
        logger.info("LastSoldFare test completed successfully")
        
        # Save captured Redis data to CSV
        if mock_redis.data:
            redis_data = []
            for key, value in mock_redis.data.items():
                try:
                    if key == "LastSoldFare:executionTime":
                        # Skip execution time key
                        continue
                        
                    parsed_value = json.loads(value) if isinstance(value, str) else value
                    if isinstance(parsed_value, dict):
                        # Extract fare data
                        all_fares = parsed_value.get('allfares', [])
                        b2b_fares = parsed_value.get('b2bfares', [])
                        b2c_fares = parsed_value.get('b2cfares', [])
                        
                        redis_data.append({
                            'redis_key': key,
                            'all_fares_count': len(all_fares),
                            'b2b_fares_count': len(b2b_fares),
                            'b2c_fares_count': len(b2c_fares),
                            'all_fares_data': json.dumps(all_fares),
                            'b2b_fares_data': json.dumps(b2b_fares),
                            'b2c_fares_data': json.dumps(b2c_fares)
                        })
                except Exception as e:
                    redis_data.append({
                        'redis_key': key,
                        'raw_value': str(value)
                    })
            
            # Save to CSV
            if redis_data:
                df = pd.DataFrame(redis_data)
                output_file = os.path.join(output_dir, "last_sold_fare_output.csv")
                df.to_csv(output_file, index=False)
                logger.info(f"Saved {len(redis_data)} Redis operations to last_sold_fare_output.csv")
        
        return True
        
    except Exception as e:
        logger.error(f"LastSoldFare test failed: {e}")
        return False
        
    finally:
        # Stop all patches
        for p in patches:
            p.stop()

if __name__ == "__main__":
    logger.info("Starting QPDynamicAllocation LastSoldFare Testing")
    
    try:
        success = test_last_sold_fare()
        
        if success:
            logger.info("Test execution completed successfully")
            
            # Simple analysis of results
            output_dir = "output"
            output_file = os.path.join(output_dir, "last_sold_fare_output.csv")
            if os.path.exists(output_file):
                try:
                    df = pd.read_csv(output_file)
                    logger.info(f"Results Summary:")
                    logger.info(f"   Total Redis Keys: {len(df)}")
                    logger.info(f"   Total All Fares: {df['all_fares_count'].sum()}")
                    logger.info(f"   Total B2B Fares: {df['b2b_fares_count'].sum()}")
                    logger.info(f"   Total B2C Fares: {df['b2c_fares_count'].sum()}")
                except Exception as e:
                    pass
        else:
            logger.error("Test execution failed")
            
    except Exception as e:
        logger.error(f"Test execution failed: {e}")