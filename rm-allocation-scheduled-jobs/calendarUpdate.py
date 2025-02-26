import logging
from datetime import datetime, timedelta
import pandas as pd
from constants_utils import ConstantsUtils

class CalendarUpdate:
    def __init__(self, logger, params, overrides, context):
        """Initialize CalendarUpdate with required connections and constants."""
        self.logger = logger
        self.rm_rd_conn = params['wrconn']
        self.constants = ConstantsUtils(self.rm_rd_conn)
        self.upsell_value = self.constants.PF_UPSELL_VALUE

    def fetch_existing_sectors(self):
        """Fetch all distinct sectors from the config_profile_fare_station_upsell table."""
        query = """
            SELECT DISTINCT sector 
            FROM QP_DW_RMALLOC.config_profile_fare_station_upsell
        """
        self.logger.info("Fetching distinct sectors")
        try:
            sectors_df = pd.read_sql(query, self.rm_rd_conn)
            self.logger.info(f"Found {len(sectors_df)} distinct sectors")
            return sectors_df['sector'].tolist()
        except Exception as e:
            self.logger.error(f"Error fetching sectors: {str(e)}")
            raise

    def check_existing_entries(self, date_str, table_name):
        """Check if entries exist for the given date in specified table."""
        query = f"""
            SELECT COUNT(*) as count 
            FROM QP_DW_RMALLOC.{table_name} 
            WHERE date = '{date_str}'
        """
        try:
            result = pd.read_sql(query, self.rm_rd_conn)
            return result['count'].iloc[0] > 0
        except Exception as e:
            self.logger.error(f"Error checking existing entries in {table_name}: {str(e)}")
            return False

    def insert_daily_upsell_records(self, date_str):
        """Insert records for the given date for all sectors in upsell table."""
        if self.check_existing_entries(date_str, 'config_profile_fare_station_upsell'):
            self.logger.info(f"Upsell entries for {date_str} already exist")
            return 0

        sectors = self.fetch_existing_sectors()
        records = [
            {'date': date_str, 'sector': sector, 'value': self.upsell_value}
            for sector in sectors
        ]
        
        df = pd.DataFrame(records)
        try:
            df.to_sql(
                'config_profile_fare_station_upsell',
                self.rm_rd_conn,
                schema='QP_DW_RMALLOC',
                if_exists='append',
                index=False
            )
            self.logger.info(f"Inserted {len(records)} upsell records for {date_str}")
            return len(records)
        except Exception as e:
            self.logger.error(f"Error inserting upsell records: {str(e)}")
            raise

    def insert_daily_date_event_record(self, date_str):
        """Insert record for the given date in date_event table."""
        if self.check_existing_entries(date_str, 'config_date_event'):
            self.logger.info(f"Date event entry for {date_str} already exists")
            return 0

        date_obj = datetime.strptime(date_str, '%m/%d/%Y')
        dow_map = {
            0: 'Mon', 1: 'Tue', 2: 'Wed', 3: 'Thu',
            4: 'Fri', 5: 'Sat', 6: 'Sun'
        }

        record = pd.DataFrame([{
            'date': date_str,
            'DOW': dow_map[date_obj.weekday()],
            'mappedMonth': date_obj.strftime('%B'),
            'event': ''
        }])

        try:
            record.to_sql(
                'config_date_event',
                self.rm_rd_conn,
                schema='QP_DW_RMALLOC',
                if_exists='append',
                index=False
            )
            self.logger.info(f"Inserted date_event record for {date_str}")
            return 1
        except Exception as e:
            self.logger.error(f"Error inserting date_event record: {str(e)}")
            raise

    def get_next_date(self):
        """Get the next date to be processed."""
        # Using text parameter instead of string formatting to handle % safely
        query = """
            SELECT MAX(STR_TO_DATE(date, '%%m/%%d/%%Y')) as last_date 
            FROM QP_DW_RMALLOC.config_date_event
        """
        try:
            result = pd.read_sql(query, self.rm_rd_conn)
            last_date = result['last_date'].iloc[0]
            
            if last_date is None:
                raise ValueError("No existing dates found in config_date_event table")
                
            next_date = last_date + timedelta(days=1)
            return next_date.strftime('%m/%d/%Y')
        except Exception as e:
            self.logger.error(f"Error getting next date: {str(e)}")
            raise

    def run(self):
        """Main method to run daily updates for both tables."""
        results = {
            "date": None,
            "upsell_status": "not_attempted",
            "date_event_status": "not_attempted"
        }

        try:
            next_date = self.get_next_date()
            results["date"] = next_date

            # Update upsell table
            upsell_count = self.insert_daily_upsell_records(next_date)
            results["upsell_status"] = "success"
            results["upsell_records_inserted"] = upsell_count

            # Update date_event table
            date_event_count = self.insert_daily_date_event_record(next_date)
            results["date_event_status"] = "success"
            results["date_event_records_inserted"] = date_event_count

        except Exception as e:
            self.logger.error(f"Error in run method: {str(e)}")
            if results["upsell_status"] == "not_attempted":
                results["upsell_status"] = "error"
                results["upsell_error"] = str(e)
            if results["date_event_status"] == "not_attempted":
                results["date_event_status"] = "error"
                results["date_event_error"] = str(e)

        return results