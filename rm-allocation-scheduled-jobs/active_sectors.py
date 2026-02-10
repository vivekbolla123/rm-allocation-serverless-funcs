import logging
from datetime import datetime, timedelta
from constants_utils import ConstantsUtils
import pandas as pd
import configsettings

class ActiveSectors:
    def __init__(self, logger, params, overrides, context):
        """Initialize ActiveSectors with required connections and constants."""
        self.logger = logger
        # Get MongoDB client and access the database
        self.mongo_client = params['mongoconn']
        self.mongo_db = self.mongo_client['qp_dw_rmalloc']
        self.nav_db_name = configsettings.NAV_DB_NAME
        self.nav_conn = params['navconn']
        self.constants = ConstantsUtils(self.mongo_db)

    def fetch_existing_sectors(self):
        """Collect active sectors from Navitaire."""
        try:
            query = f"""
            SELECT DISTINCT DepartureStation, ArrivalStation
            FROM {self.nav_db_name}rez.InventoryLeg
            WHERE DepartureDate >= '{datetime.now().date()}'
            AND CarrierCode='QP' AND FlightNumber < 7000
            """
            return pd.read_sql(query, self.nav_conn)
        except Exception as e:
            self.logger.error(f"Error collecting active sectors data: {e}")
            raise
    
    def clear_nav_sectors(self):
        """Clear only Nav source sectors from the active_sectors collection."""
        try:
            result = self.mongo_db.active_sectors.delete_many({'source': 'Nav'})
            self.logger.info(f"Cleared {result.deleted_count} Nav sectors from active_sectors collection")
            return result.deleted_count
        except Exception as e:
            self.logger.error(f"Error clearing Nav sectors: {str(e)}")
            raise
    
    def get_manual_sectors(self):
        """Get existing manual sectors to avoid duplicates."""
        try:
            manual_sectors = list(self.mongo_db.active_sectors.find(
                {'source': 'Manual'},
                {'sector': 1, '_id': 0}
            ))
            return {doc['sector'] for doc in manual_sectors}
        except Exception as e:
            self.logger.error(f"Error fetching manual sectors: {str(e)}")
            raise
    
    def insert_active_sectors(self, sectors):
        """Insert Nav sectors into the active_sectors collection."""
        if sectors.empty:
            self.logger.warning("No sectors to insert")
            return 0
            
        try:
            # Get existing manual sectors
            manual_sectors = self.get_manual_sectors()
            
            # Create records with timestamp and source
            current_time = datetime.now()
            
            # Convert DataFrame rows to sector strings (e.g., "DEL-BOM")
            sector_list = [
                f"{row['DepartureStation']}{row['ArrivalStation']}"
                for _, row in sectors.iterrows()
            ]
            
            # Filter out sectors that already exist as Manual
            new_sectors = [s for s in sector_list if s not in manual_sectors]
            
            if not new_sectors:
                self.logger.info("No new Nav sectors to insert (all exist as Manual)")
                return 0
            
            records = [
                {
                    'sector': sector,
                    'source': 'Nav',
                    'created_at': current_time,
                    'updated_at': current_time
                }
                for sector in new_sectors
            ]
            
            result = self.mongo_db.active_sectors.insert_many(records)
            inserted_count = len(result.inserted_ids)
            self.logger.info(f"Inserted {inserted_count} Nav sectors into active_sectors collection")
            
            if len(sector_list) != len(new_sectors):
                skipped = len(sector_list) - len(new_sectors)
                self.logger.info(f"Skipped {skipped} sectors that already exist as Manual")
            
            return inserted_count
            
        except Exception as e:
            self.logger.error(f"Error inserting sectors: {str(e)}")
            raise
    
    def run(self):
        """Main method to update active_sectors collection with unique sectors."""
        results = {
            "status": "not_attempted",
            "sectors_fetched": 0,
            "nav_sectors_cleared": 0,
            "nav_sectors_inserted": 0,
            "manual_sectors_preserved": 0,
            "timestamp": datetime.now().isoformat()
        }
        
        try:
            # Get count of manual sectors before operation
            manual_count = self.mongo_db.active_sectors.count_documents({'source': 'Manual'})
            results["manual_sectors_preserved"] = manual_count
            
            # Fetch unique sectors from Navitaire
            sectors = self.fetch_existing_sectors()
            results["sectors_fetched"] = len(sectors)
            
            # Clear only Nav sectors
            cleared_count = self.clear_nav_sectors()
            results["nav_sectors_cleared"] = cleared_count
            
            # Insert new Nav sectors
            inserted_count = self.insert_active_sectors(sectors)
            results["nav_sectors_inserted"] = inserted_count
            
            # Verify manual sectors are still there
            manual_count_after = self.mongo_db.active_sectors.count_documents({'source': 'Manual'})
            if manual_count != manual_count_after:
                self.logger.warning(f"Manual sector count changed from {manual_count} to {manual_count_after}")
            
            results["status"] = "success"
            self.logger.info(
                f"Successfully updated active_sectors collection: "
                f"{inserted_count} Nav sectors inserted, "
                f"{manual_count_after} Manual sectors preserved"
            )
            
        except Exception as e:
            self.logger.error(f"Error in run method: {str(e)}")
            results["status"] = "error"
            results["error"] = str(e)
        
        return results