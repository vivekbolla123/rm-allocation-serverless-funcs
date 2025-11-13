import logging
from datetime import datetime, timedelta
from constants_utils import ConstantsUtils

class CalendarUpdate:
    def __init__(self, logger, params, overrides, context):
        """Initialize CalendarUpdate with required connections and constants."""
        self.logger = logger
        # Get MongoDB client and access the database
        self.mongo_client = params['mongoconn']
        self.mongo_db = self.mongo_client['qp_dw_rmalloc']
        self.constants = ConstantsUtils(self.mongo_client)
        self.upsell_value = self.constants.PF_UPSELL_VALUE

    def fetch_existing_sectors(self):
        """Fetch all distinct sectors from the config_profile_fare_station_upsell collection."""
        self.logger.info("Fetching distinct sectors")
        try:
            # Get distinct sectors from MongoDB collection
            sectors = self.mongo_db.config_profile_fare_station_upsell.distinct("sector")
            self.logger.info(f"Found {len(sectors)} distinct sectors")
            return sectors
        except Exception as e:
            self.logger.error(f"Error fetching sectors: {str(e)}")
            raise

    def check_existing_entries(self, date_str, collection_name):
        """Check if entries exist for the given date in specified collection."""
        try:
            count = self.mongo_db[collection_name].count_documents({"date": date_str})
            return count > 0
        except Exception as e:
            self.logger.error(f"Error checking existing entries in {collection_name}: {str(e)}")
            return False

    def insert_daily_upsell_records(self, date_str):
        """Insert records for the given date for all sectors in upsell collection."""
        if self.check_existing_entries(date_str, 'config_profile_fare_station_upsell'):
            self.logger.info(f"Upsell entries for {date_str} already exist")
            return 0

        sectors = self.fetch_existing_sectors()
        records = [
            {'date': date_str, 'sector': sector, 'value': self.upsell_value}
            for sector in sectors
        ]
        
        try:
            if records:
                result = self.mongo_db.config_profile_fare_station_upsell.insert_many(records)
                inserted_count = len(result.inserted_ids)
                self.logger.info(f"Inserted {inserted_count} upsell records for {date_str}")
                return inserted_count
            else:
                self.logger.warning("No sectors found to insert upsell records")
                return 0
        except Exception as e:
            self.logger.error(f"Error inserting upsell records: {str(e)}")
            raise

    def insert_daily_date_event_record(self, date_str):
        """Insert record for the given date in date_event collection."""
        if self.check_existing_entries(date_str, 'config_date_event'):
            self.logger.info(f"Date event entry for {date_str} already exists")
            return 0

        date_obj = datetime.strptime(date_str, '%m/%d/%Y')
        dow_map = {
            0: 'Mon', 1: 'Tue', 2: 'Wed', 3: 'Thu',
            4: 'Fri', 5: 'Sat', 6: 'Sun'
        }

        # Determine half-month indicator
        month_name = date_obj.strftime('%B')
        half_indicator = '_H1' if date_obj.day <= 15 else '_H2'
        mapped_month = month_name + half_indicator

        record = {
            'date': date_str,
            'DOW': dow_map[date_obj.weekday()],
            'mappedMonth': mapped_month,
            'event': ''
        }

        try:
            result = self.mongo_db.config_date_event.insert_one(record)
            if result.inserted_id:
                self.logger.info(f"Inserted date_event record for {date_str}")
                return 1
            else:
                self.logger.warning(f"Failed to insert date_event record for {date_str}")
                return 0
        except Exception as e:
            self.logger.error(f"Error inserting date_event record: {str(e)}")
            raise

    def delete_old_records(self):
        """Delete records older than current date - 2 days from both collections."""
        try:
            # Calculate cutoff date (current date - 2 days)
            cutoff_date = datetime.now() - timedelta(days=2)
            cutoff_date_str = cutoff_date.strftime('%m/%d/%Y')
            
            self.logger.info(f"Deleting records before {cutoff_date_str}")
            
            # Delete from config_profile_fare_station_upsell
            upsell_pipeline = [
                {
                    "$addFields": {
                        "date_obj": {
                            "$dateFromString": {
                                "dateString": "$date",
                                "format": "%m/%d/%Y"
                            }
                        }
                    }
                },
                {
                    "$match": {
                        "date_obj": {"$lt": cutoff_date}
                    }
                }
            ]
            
            # Get IDs to delete from upsell collection
            upsell_docs = list(self.mongo_db.config_profile_fare_station_upsell.aggregate(upsell_pipeline))
            upsell_ids = [doc['_id'] for doc in upsell_docs]
            
            upsell_deleted = 0
            if upsell_ids:
                upsell_result = self.mongo_db.config_profile_fare_station_upsell.delete_many(
                    {"_id": {"$in": upsell_ids}}
                )
                upsell_deleted = upsell_result.deleted_count
                self.logger.info(f"Deleted {upsell_deleted} records from config_profile_fare_station_upsell")
            
            # Delete from config_date_event
            date_event_pipeline = [
                {
                    "$addFields": {
                        "date_obj": {
                            "$dateFromString": {
                                "dateString": "$date",
                                "format": "%m/%d/%Y"
                            }
                        }
                    }
                },
                {
                    "$match": {
                        "date_obj": {"$lt": cutoff_date}
                    }
                }
            ]
            
            # Get IDs to delete from date_event collection
            date_event_docs = list(self.mongo_db.config_date_event.aggregate(date_event_pipeline))
            date_event_ids = [doc['_id'] for doc in date_event_docs]
            
            date_event_deleted = 0
            if date_event_ids:
                date_event_result = self.mongo_db.config_date_event.delete_many(
                    {"_id": {"$in": date_event_ids}}
                )
                date_event_deleted = date_event_result.deleted_count
                self.logger.info(f"Deleted {date_event_deleted} records from config_date_event")
            
            return {
                "upsell_deleted": upsell_deleted,
                "date_event_deleted": date_event_deleted,
                "cutoff_date": cutoff_date_str
            }
            
        except Exception as e:
            self.logger.error(f"Error deleting old records: {str(e)}")
            raise

    def get_next_date(self):
        """Get the next date to be processed."""
        try:
            # Find the document with the maximum date
            # First, get all dates and convert them to datetime objects for comparison
            pipeline = [
                {
                    "$addFields": {
                        "date_obj": {
                            "$dateFromString": {
                                "dateString": "$date",
                                "format": "%m/%d/%Y"
                            }
                        }
                    }
                },
                {
                    "$sort": {"date_obj": -1}
                },
                {
                    "$limit": 1
                },
                {
                    "$project": {
                        "date": 1,
                        "date_obj": 1,
                        "_id": 0
                    }
                }
            ]
            
            result = list(self.mongo_db.config_date_event.aggregate(pipeline))
            
            if not result:
                raise ValueError("No existing dates found in config_date_event collection")
            
            last_date = result[0]['date_obj']
            next_date = last_date + timedelta(days=1)
            return next_date.strftime('%m/%d/%Y')
            
        except Exception as e:
            self.logger.error(f"Error getting next date: {str(e)}")
            raise

    def run(self):
        """Main method to run daily updates for both collections."""
        results = {
            "date": None,
            "upsell_status": "not_attempted",
            "date_event_status": "not_attempted",
            "delete_status": "not_attempted"
        }

        try:
            # Delete old records first
            delete_results = self.delete_old_records()
            results["delete_status"] = "success"
            results["delete_results"] = delete_results

            # Get next date to insert
            next_date = self.get_next_date()
            results["date"] = next_date

            # Update upsell collection
            upsell_count = self.insert_daily_upsell_records(next_date)
            results["upsell_status"] = "success"
            results["upsell_records_inserted"] = upsell_count

            # Update date_event collection
            date_event_count = self.insert_daily_date_event_record(next_date)
            results["date_event_status"] = "success"
            results["date_event_records_inserted"] = date_event_count

        except Exception as e:
            self.logger.error(f"Error in run method: {str(e)}")
            if results["delete_status"] == "not_attempted":
                results["delete_status"] = "error"
                results["delete_error"] = str(e)
            if results["upsell_status"] == "not_attempted":
                results["upsell_status"] = "error"
                results["upsell_error"] = str(e)
            if results["date_event_status"] == "not_attempted":
                results["date_event_status"] = "error"
                results["date_event_error"] = str(e)

        return results