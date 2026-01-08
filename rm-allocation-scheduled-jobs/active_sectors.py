import logging
from datetime import datetime, timedelta
from constants_utils import ConstantsUtils

class ActiveSectors:
    def __init__(self, logger, params, overrides, context):
        """Initialize ActiveSectors with required connections and constants."""
        self.logger = logger
        # Get MongoDB client and access the database
        self.mongo_client = params['mongoconn']
        self.mongo_db = self.mongo_client['qp_dw_rmalloc']
        self.constants = ConstantsUtils(self.mongo_db)

    def fetch_existing_sectors(self):
        """Fetch all distinct sectors from the market_list and market_list_international collections."""
        self.logger.info("Fetching distinct sectors from market_list and market_list_international")
        try:
            sectors = set()
            
            # Fetch sectors from market_list collection
            market_list_pipeline = [
                {
                    "$project": {
                        "sector": {"$concat": ["$Origin", "$Destin"]},
                        "_id": 0
                    }
                },
                {
                    "$group": {
                        "_id": "$sector"
                    }
                }
            ]
            
            market_list_sectors = self.mongo_db.market_list.aggregate(market_list_pipeline)
            for doc in market_list_sectors:
                if doc['_id']:  # Ensure sector is not null/empty
                    sectors.add(doc['_id'])
            
            # Fetch sectors from market_list_international collection
            market_list_intl_pipeline = [
                {
                    "$project": {
                        "sector": {"$concat": ["$Origin", "$Destin"]},
                        "_id": 0
                    }
                },
                {
                    "$group": {
                        "_id": "$sector"
                    }
                }
            ]
            
            market_list_intl_sectors = self.mongo_db.market_list_international.aggregate(market_list_intl_pipeline)
            for doc in market_list_intl_sectors:
                if doc['_id']:  # Ensure sector is not null/empty
                    sectors.add(doc['_id'])
            
            sectors_list = list(sectors)
            self.logger.info(f"Found {len(sectors_list)} distinct sectors")
            return sectors_list
            
        except Exception as e:
            self.logger.error(f"Error fetching sectors: {str(e)}")
            raise

    def clear_active_sectors_collection(self):
        """Clear the active_sectors collection before inserting new data."""
        try:
            result = self.mongo_db.active_sectors.delete_many({})
            self.logger.info(f"Cleared {result.deleted_count} existing records from active_sectors collection")
            return result.deleted_count
        except Exception as e:
            self.logger.error(f"Error clearing active_sectors collection: {str(e)}")
            raise

    def insert_active_sectors(self, sectors):
        """Insert sectors into the active_sectors collection."""
        if not sectors:
            self.logger.warning("No sectors to insert")
            return 0
            
        try:
            # Create records with timestamp
            current_time = datetime.now()
            records = [
                {
                    'sector': sector,
                    'created_at': current_time,
                    'updated_at': current_time
                }
                for sector in sectors
            ]
            
            result = self.mongo_db.active_sectors.insert_many(records)
            inserted_count = len(result.inserted_ids)
            self.logger.info(f"Inserted {inserted_count} sectors into active_sectors collection")
            return inserted_count
            
        except Exception as e:
            self.logger.error(f"Error inserting sectors: {str(e)}")
            raise

    def run(self):
        """Main method to update active_sectors collection with unique sectors."""
        results = {
            "status": "not_attempted",
            "sectors_fetched": 0,
            "records_cleared": 0,
            "records_inserted": 0,
            "timestamp": datetime.now().isoformat()
        }

        try:
            # Fetch unique sectors from both collections
            sectors = self.fetch_existing_sectors()
            results["sectors_fetched"] = len(sectors)
            
            # Clear existing active_sectors collection
            cleared_count = self.clear_active_sectors_collection()
            results["records_cleared"] = cleared_count
            
            # Insert new sectors
            inserted_count = self.insert_active_sectors(sectors)
            results["records_inserted"] = inserted_count
            
            results["status"] = "success"
            self.logger.info(f"Successfully updated active_sectors collection with {inserted_count} sectors")
            
        except Exception as e:
            self.logger.error(f"Error in run method: {str(e)}")
            results["status"] = "error"
            results["error"] = str(e)

        return results