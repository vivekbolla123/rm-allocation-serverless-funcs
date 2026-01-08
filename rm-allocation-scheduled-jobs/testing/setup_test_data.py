"""
Script to load mock data into local MongoDB for testing
Uses actual configuration files
"""
import pandas as pd
import pymongo
import os
import sys
from datetime import datetime

# Ensure we're in the correct directory
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

# Get the current script directory and parent directory
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
root_dir = os.path.dirname(parent_dir)

# Add paths for actual configuration files
config_path = os.path.join(root_dir, 'rm-allocation-configurations')
sys.path.insert(0, config_path)
sys.path.insert(0, parent_dir)
sys.path.insert(0, root_dir)

try:
    import Constants
    print("✅ Successfully imported Constants")
except ImportError as e:
    print(f"⚠️  Warning: Could not import Constants: {e}")
    print("Continuing without Constants (not required for data loading)")
    Constants = None

def setup_mongodb_data():
    """Load all mock CSV data into local MongoDB"""
    
    # Connect to local MongoDB
    try:
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        # Test connection
        client.admin.command('ping')
        print("✅ Connected to local MongoDB successfully")
    except Exception as e:
        print(f"❌ Failed to connect to local MongoDB: {e}")
        return False
    
    db = client["qp_dw_rmalloc"]
    
    # Get the directory containing this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    inputs_dir = os.path.join(script_dir, "inputs")
    
    print("Loading mock data into MongoDB...")
    
    # Collection mapping: CSV filename -> MongoDB collection name
    collections_map = {
        "market_list.csv": "market_list",
        "market_list_international.csv": "market_list_international",
        "config_user_sector_map.csv": "config_user_sector_map",
        "config_station_master.csv": "config_station_master",
        "curves.csv": "curves",
        "config_share_of_booking.csv": "config_share_of_booking",
        "config_column_names.csv": "config_column_names",
        "calendar_af.csv": "calendar_af",
        "config_holiday_list.csv": "config_holiday_list",
        "config_date_event.csv": "config_date_event",
        "config_profile_fares.csv": "config_profile_fares",
        "config_profile_fare_station_upsell.csv": "config_profile_fare_station_upsell"
    }
    
    total_records = 0
    
    for csv_file, collection_name in collections_map.items():
        csv_path = os.path.join(inputs_dir, csv_file)
        
        if not os.path.exists(csv_path):
            print(f"⚠️  Warning: {csv_file} not found, skipping...")
            continue
        
        try:
            # Load CSV data
            df = pd.read_csv(csv_path)
            
            # Get collection and clear existing data
            collection = db[collection_name]
            collection.drop()  # Clear existing data
            
            # Convert DataFrame to records and insert
            records = df.to_dict('records')
            if records:
                collection.insert_many(records)
                total_records += len(records)
                print(f"✅ Loaded {len(records)} records into {collection_name}")
            else:
                print(f"⚠️  No records found in {csv_file}")
                
        except Exception as e:
            print(f"❌ Error loading {csv_file}: {e}")
    
    # Create empty collections that will be populated by tests
    empty_collections = ["market_list_thirty", "currentVersion"]
    for collection_name in empty_collections:
        db[collection_name].drop()
        print(f"✅ Prepared empty collection: {collection_name}")
    
    print(f"\n🎉 Mock data setup complete!")
    print(f"📊 Total records loaded: {total_records}")
    print(f"🗄️  Database: {db.name}")
    print(f"📁 Collections created: {sorted(db.list_collection_names())}")
    
    return True

def verify_data():
    """Verify that data was loaded correctly"""
    try:
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = client["qp_dw_rmalloc"]
        
        print("\n🔍 Verifying loaded data:")
        
        collections_to_check = [
            "market_list",
            "market_list_international", 
            "config_user_sector_map",
            "config_station_master",
            "curves",
            "config_share_of_booking",
            "config_column_names",
            "calendar_af",
            "config_holiday_list",
            "config_date_event",
            "config_profile_fares",
            "config_profile_fare_station_upsell"
        ]
        
        for collection_name in collections_to_check:
            count = db[collection_name].count_documents({})
            print(f"   {collection_name}: {count} documents")
            
            # Show sample document
            if count > 0:
                sample = db[collection_name].find_one()
                sample_keys = list(sample.keys())[:5]  # Show first 5 keys
                print(f"     Sample keys: {sample_keys}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error verifying data: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Setting up test data for RM Allocation Scheduled Jobs")
    print(f"Started at: {datetime.now()}")
    
    success = setup_mongodb_data()
    
    if success:
        verify_data()
        print("\n✅ Setup completed successfully!")
        print("\nNext steps:")
        print("1. Run tests: python test_lambda_function.py")
        print("2. Or run individual tests: python test_lambda_function.py --module default_ml")
    else:
        print("\n❌ Setup failed!")
    
    print(f"Completed at: {datetime.now()}")