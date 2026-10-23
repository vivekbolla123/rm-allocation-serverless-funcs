from datetime import datetime, timedelta
import uuid
import pandas as pd
import pytz
import configsettings
from helpers.market_list_audit import stamp_and_delete, utc_now, SYSTEM_USER, SOURCE_ROLLING

class DailyRolling:
    # Collections purged of expired single-day market list rows
    TARGET_COLLECTIONS = ['market_list', 'market_list_international', 'market_list_connections']
    PERIOD_DATE_FORMAT = '%m/%d/%Y'

    def __init__(self, logger, params, overrides, context):
        self.context = context
        self.logger = logger
        self.mongo_conn = params['mongoconn']  # Single MongoDB connection
        self.db = self.mongo_conn["qp_dw_rmalloc"]
        self.master_db = params['mongo_master_db']
        self.nav_conn = params['navconn']  # Keep NAV SQL connection

    def run(self):
        """Delete expired single-day records (see should_delete_record) from each market list collection.

        Only the matching documents are deleted; all other documents are left untouched.
        """
        self.logger.info("Starting the run method.")
        current_date = self.get_current_date_ist()
        self.logger.info(f"Current date: {current_date}")

        results = {}
        for collection_name in self.TARGET_COLLECTIONS:
            results[collection_name] = self.delete_expired_records(collection_name, current_date)

        self.logger.info(f"Daily rolling complete: {results}")
        return results

    def delete_expired_records(self, collection_name, current_date):
        """Find documents matching should_delete_record and delete them by _id in batches.

        Documents whose PerStart/PerEnd cannot be parsed are skipped (never deleted).
        """
        try:
            collection = self.db[collection_name]
            cursor = collection.find({}, {'_id': 1, 'PerStart': 1, 'PerEnd': 1})

            ids_to_delete = []
            total_checked = 0
            unparseable = 0
            for doc in cursor:
                total_checked += 1
                start_date = self._parse_period_date(doc.get('PerStart'))
                end_date = self._parse_period_date(doc.get('PerEnd'))
                if start_date is None or end_date is None:
                    unparseable += 1
                    continue
                if self.should_delete_record(start_date, end_date, current_date):
                    ids_to_delete.append(doc['_id'])

            if unparseable:
                self.logger.warning(
                    f"{collection_name}: skipped {unparseable} documents with missing/invalid PerStart or PerEnd"
                )

            # Transactional stamp (_deleted + audit fields) then delete, in batches
            deleted_count = stamp_and_delete(
                collection, ids_to_delete,
                changed_by=SYSTEM_USER, change_source=SOURCE_ROLLING, changed_at=utc_now()
            )

            self.logger.info(
                f"{collection_name}: checked {total_checked}, deleted {deleted_count} expired single-day records"
            )
            return {'checked': total_checked, 'deleted': deleted_count, 'skipped_unparseable': unparseable}

        except Exception as e:
            self.logger.error(f"Error deleting expired records from {collection_name}: {e}")
            raise

    def _parse_period_date(self, value):
        """Parse a PerStart/PerEnd value (mm/dd/yyyy string or datetime) into a date. Returns None if invalid."""
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, str):
            try:
                return datetime.strptime(value.strip(), self.PERIOD_DATE_FORMAT).date()
            except ValueError:
                return None
        return None

    def get_current_date_ist(self):
        local_tz = pytz.timezone('Asia/Kolkata')  # IST timezone
        local_datetime = datetime.now(local_tz)
        return local_datetime.date()

    def fetch_sector_map(self):
        """Fetch all documents from config_user_sector_map collection."""
        try:
            collection = self.db['config_user_sector_map']
            self.logger.info("Fetching data from config_user_sector_map collection")
            
            cursor = collection.find({})
            df_sector_map = pd.DataFrame(list(cursor))
            
            # Remove MongoDB _id field if present
            if '_id' in df_sector_map.columns:
                df_sector_map = df_sector_map.drop('_id', axis=1)
            
            self.logger.info(f"Retrieved {len(df_sector_map)} records from config_user_sector_map")
            return df_sector_map
            
        except Exception as e:
            self.logger.error(f"Error fetching config_user_sector_map: {e}")
            return pd.DataFrame()

    def should_delete_record(self, start_date, end_date, current_date):
        c1 = (start_date < current_date and end_date < current_date)
        c2 = start_date == end_date
        # c3= (start_date-current_date).days==30
        return c1 and c2 

    def should_update_record(self, start_date, end_date, current_date):
        return start_date < current_date and end_date >= current_date

    def update_record(self, df_market_list, df_sector_map, row, index, start_date, end_date, current_date, new_rows):
        duration = (end_date - current_date).days
        self.logger.info(f"Updating PerStart for index {index} to current date")
        df_market_list.at[index, 'PerStart'] = current_date.strftime('%m/%d/%Y')
        sector = row['Origin'] + row['Destin']
        self.logger.info(f"Sector for index {index}: {sector}")
        sector_row = df_sector_map[df_sector_map['sector'] == sector].iloc[0]
        user1, user2 = sector_row['user1'], sector_row['user2']
        analyst_name = user1 if duration <= 30 else user2
        self.logger.info(f"Analyst name for index {index}: {analyst_name}")
        df_market_list.at[index, 'analystName'] = analyst_name

        if duration > 30 and (start_date - current_date).days <= 30:
            new_end_date = end_date - timedelta(days=duration-30)
            # new_row = self.create_new_row(row, current_date, new_end_date, user1,df_market_list)
            # new_rows.append(new_row)
            df_market_list.at[index, 'PerStart'] = (new_end_date + timedelta(days=1)).strftime('%m/%d/%Y')

        return df_market_list, new_rows

    def split_record(self, df_market_list, df_sector_map, row, index, start_date, end_date, current_date, new_rows):
        duration = (end_date - current_date).days
        sector = row['Origin'] + row['Destin']
        self.logger.info(f"Sector for index {index}: {sector}")
        sector_row = df_sector_map[df_sector_map['sector'] == sector].iloc[0]
        user1, user2 = sector_row['user1'], sector_row['user2']
        analyst_name = user1 if duration <= 30 else user2
        self.logger.info(f"Analyst name for index {index}: {analyst_name}")
        df_market_list.at[index, 'analystName'] = analyst_name

        if duration > 30 and (start_date - current_date).days <= 30:
            new_end_date = end_date - timedelta(days=duration-30)
            # new_row = self.create_new_row(row, start_date, new_end_date, user1,df_market_list)
            # new_rows.append(new_row)
            df_market_list.at[index, 'PerStart'] = (new_end_date + timedelta(days=1)).strftime('%m/%d/%Y')

        return df_market_list, new_rows

    def create_new_row(self, df_market_list, current_date, df_sector_map):
        tgt_date = current_date + timedelta(days=30)
        all_flights = self.get_nav_dump(tgt_date)

        # Create a base new_row with the static values set
        new_row_template = {
            'PerEnd': [tgt_date.strftime('%m/%d/%Y')],
            'PerStart': [tgt_date.strftime('%m/%d/%Y')],
            'UUID': [str(uuid.uuid4())],
            'DOW': [self.update_dow(tgt_date, tgt_date)]
        }

        for index, row in all_flights.iterrows():
            sector = row['Origin'] + row['Destination']
            self.logger.info(f"Sector for index {index}: {sector}")

            # Check if sector exists in df_sector_map
            sector_rows = df_sector_map[df_sector_map['sector'] == sector]
            if sector_rows.empty:
                continue

            sector_row = sector_rows.iloc[0]
            new_row_template['analystName'] = sector_row['user1']

            corresponding_row = self.lookup_corresponding_row(df_market_list, row['Origin'], row['Destination'], row['FlightNumber'], tgt_date)
            if corresponding_row is not None:
                self.logger.info(f"Updating new row with data from corresponding row: {corresponding_row}")

                # Create a new row with the data from corresponding_row and new_row_template
                new_row = {**new_row_template}
                new_row['UUID'] = [str(uuid.uuid4())]
                for column in df_market_list.columns:
                    if column not in new_row_template:
                        new_row[column] = [corresponding_row[column]]
                new_row = pd.DataFrame(new_row)
                # Concat the new row to the DataFrame
                df_market_list = pd.concat([df_market_list, new_row], ignore_index=True)

        return df_market_list
            
    def lookup_corresponding_row(self, df_market_list, origin, destin, flight_number, date):
        # Filter rows based on origin, destination, and flight number
        previous_rows = df_market_list[
            (df_market_list['Origin'] == origin) &
            (df_market_list['Destin'] == destin) &
            (df_market_list['FlightNumber'] == int(flight_number))
        ]
        self.logger.info(f"previous_rows: {previous_rows}")
        # Parse the date into a datetime object
        target_date = date
        
        # Further filter rows to ensure PerStart and PerEnd are before the target date
        previous_rows = previous_rows[
            (previous_rows['PerStart'].apply(lambda x: datetime.strptime(x, '%m/%d/%Y').date()) < target_date) &
            (previous_rows['PerEnd'].apply(lambda x: datetime.strptime(x, '%m/%d/%Y').date()) < target_date)
        ]
        if previous_rows.shape[0] != 0:
            # Convert 'PerStart' to datetime format for sorting
            previous_rows['PerStart'] = previous_rows['PerStart'].apply(lambda x: datetime.strptime(x, '%m/%d/%Y').date())

            # Sort by PerStart date in descending order
            previous_rows = previous_rows.sort_values(by='PerStart', ascending=False)

            # Iterate through the sorted rows
            for _, prev_row in previous_rows.iterrows():
                prev_start_date = prev_row['PerStart']
                prev_end_date = datetime.strptime(prev_row['PerEnd'], '%m/%d/%Y').date()
                if prev_start_date.weekday() <= target_date.weekday() <= prev_end_date.weekday():
                    # Convert 'PerStart' back to original format before returning
                    prev_row['PerStart'] = prev_row['PerStart'].strftime('%m/%d/%Y')
                    # prev_row['PerEnd'] = prev_row['PerEnd']
                    return prev_row

        return None
    
    def update_dow(self, start_date, end_date):
        # Initialize DOW string to all '9's indicating closed
        dow_list = ['9'] * 7

        # Calculate the difference in days and iterate through each day
        current_date = start_date
        while current_date <= end_date:
            day_of_week = current_date.weekday()  # Monday is 0, Sunday is 6
            if day_of_week == 6:  # Adjust for the DOW format starting with Sunday
                dow_list[0] = '1'
            else:
                dow_list[day_of_week + 1] = '1'
            current_date += timedelta(days=1)

        return ''.join(dow_list)
    
    def get_nav_dump(self, depDate):
        """
        Fetch navigation dump from SQL Server (NAV connection remains SQL).
        This method stays unchanged as it queries a different database.
        """
        # First get station list from MongoDB
        try:
            collection = self.master_db['config_station_master']
            station_cursor = collection.find({'route': 'I'}, {'station': 1, '_id': 0})
            station_master = pd.DataFrame(list(station_cursor))
        except Exception as e:
            self.logger.error(f"Error fetching station master from MongoDB: {e}")
            station_master = pd.DataFrame()
        
        if station_master.empty:
            int_stations = "()"
        else:
            int_stations = tuple(f"{val}" for val in station_master["station"])
            if len(int_stations) == 1:
                int_stations = f"('{int_stations[0]}')"
        
        # NAV query remains SQL Server query
        query = f"""SELECT IL.DepartureStation as Origin,IL.ArrivalStation as Destination, 
		IL.DepartureDate,
         TRIM(IL.FlightNumber) as FlightNumber 
         FROM 
         {configsettings.NAV_DB_NAME}[rez].InventoryLeg IL 
         WHERE 
         IL.Lid > 0 
         and IL.Status NOT IN (2) 
		 and LEN(TRIM(IL.FlightNumber))=4
         and IL.DepartureDate >= '{depDate.strftime('%Y-%m-%d')}' 
         and IL.DepartureDate <= '{depDate.strftime('%Y-%m-%d')}'
         and IL.DepartureStation NOT IN {int_stations} and IL.ArrivalStation NOT IN{int_stations};
         """
        dump = pd.read_sql(query, self.nav_conn)
        self.logger.info(f"dump length {len(dump)}")
        return dump