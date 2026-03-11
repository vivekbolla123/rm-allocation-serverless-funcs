import logging
from datetime import datetime, timedelta
from constants_utils import ConstantsUtils
import pandas as pd
import configsettings
import json

# Source type constants
SOURCE_NAV = 'Nav'
SOURCE_MANUAL = 'Manual'

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
        
        # Default fields to add to new sector documents
        self.default_sector_fields = json.loads(self.constants.ACTIVE_SECTORS_DEFAULT_FIELDS)

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
    
    def get_existing_mongo_sectors(self):
        """Get all existing sectors from MongoDB (both Nav and Manual)."""
        try:
            all_sectors = list(self.mongo_db.active_sectors.find(
                {},
                {'sector': 1, 'source': 1, '_id': 0}
            ))
            
            nav_sectors = {doc['sector'] for doc in all_sectors if doc.get('source') == SOURCE_NAV}
            # Manual sectors: source='Manual' OR source is null OR source field doesn't exist
            manual_sectors = {
                doc['sector'] for doc in all_sectors 
                if doc.get('source') in [SOURCE_MANUAL, None] or 'source' not in doc
            }
            all_sectors_set = {doc['sector'] for doc in all_sectors}
            
            return {
                'nav': nav_sectors,
                'manual': manual_sectors,
                'all': all_sectors_set
            }
        except Exception as e:
            self.logger.error(f"Error fetching existing sectors: {str(e)}")
            raise
    
    def sync_nav_sectors(self, nav_sectors_df):
        """Sync Nav sectors with MongoDB - add new, remove obsolete, keep existing."""
        if nav_sectors_df.empty:
            self.logger.warning("No Nav sectors to sync")
            return {'added': 0, 'removed': 0, 'kept': 0, 'manual': 0}
            
        try:
            # Convert DataFrame to sector strings
            nav_sectors_from_db = {
                f"{row['DepartureStation']}{row['ArrivalStation']}"
                for _, row in nav_sectors_df.iterrows()
            }
            
            # Get existing sectors from MongoDB
            mongo_sectors = self.get_existing_mongo_sectors()
            
            # Determine what to add and remove
            sectors_to_add = nav_sectors_from_db - mongo_sectors['all']  # New sectors not in MongoDB at all
            sectors_to_remove = mongo_sectors['nav'] - nav_sectors_from_db  # Nav sectors no longer in Navitaire
            sectors_kept = mongo_sectors['nav'] & nav_sectors_from_db  # Nav sectors that exist in both
            
            current_time = datetime.now()
            added_count = 0
            removed_count = 0
            
            # Add new sectors
            if sectors_to_add:
                records = [
                    {
                        'sector': sector,
                        'source': SOURCE_NAV,
                        'created_at': current_time,
                        'updated_at': current_time,
                        **self.default_sector_fields  # Add default configurable fields
                    }
                    for sector in sectors_to_add
                ]
                result = self.mongo_db.active_sectors.insert_many(records)
                added_count = len(result.inserted_ids)
                self.logger.info(f"Added {added_count} new Nav sectors with default fields")
            
            # Remove obsolete Nav sectors
            if sectors_to_remove:
                result = self.mongo_db.active_sectors.delete_many({
                    'sector': {'$in': list(sectors_to_remove)},
                    'source': SOURCE_NAV
                })
                removed_count = result.deleted_count
                self.logger.info(f"Removed {removed_count} obsolete Nav sectors")
            
            return {
                'added': added_count,
                'removed': removed_count,
                'kept': len(sectors_kept),
                'manual': len(mongo_sectors['manual'])
            }
            
        except Exception as e:
            self.logger.error(f"Error syncing sectors: {str(e)}")
            raise
    
    def run(self):
        """Main method to sync active_sectors collection with Navitaire data."""
        
        try:
            # Fetch sectors from Navitaire
            sectors = self.fetch_existing_sectors()
            
            # Sync Nav sectors with MongoDB
            sync_results = self.sync_nav_sectors(sectors)
            
            self.logger.info(
                f"Successfully synced active_sectors: "
                f"{sync_results['added']} added, {sync_results['removed']} removed, "
                f"{sync_results['kept']} kept, {sync_results['manual']} manual preserved"
            )
            
        except Exception as e:
            self.logger.error(f"Error in run method: {str(e)}")