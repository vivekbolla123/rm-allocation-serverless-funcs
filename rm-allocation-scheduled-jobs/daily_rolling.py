from datetime import datetime, timedelta
import uuid
import pandas as pd
import pytz
import configsettings

class DailyRolling:
    def __init__(self, logger, params, overrides, context):
        self.context = context
        self.logger = logger
        self.mongo_conn = params['mongoconn']  # Single MongoDB connection
        self.db = self.mongo_conn["qp_dw_rmalloc"]
        self.nav_conn = params['navconn']  # Keep NAV SQL connection

    def run(self):
        self.logger.info("Starting the run method.")
        df_market_list = self.fetch_market_list()
        df_market_list_int = self.fetch_market_list_int()
        df_market_list_conn = self.fetch_market_list_conn()
        df_sector_map = self.fetch_sector_map()
        current_date = self.get_current_date_ist()
        self.logger.info(f"Current date: {current_date}")
        print(len(df_market_list))
        df_market_list = self.process_market_list(df_market_list, df_sector_map, current_date)
        df_market_list_int = self.process_market_list(df_market_list_int, df_sector_map, current_date)
        df_market_list_conn = self.process_market_list(df_market_list_conn, df_sector_map, current_date)
        
        print(len(df_market_list))
        # df_market_list=self.create_new_row(df_market_list,current_date,df_sector_map)
        print(len(df_market_list))
        self.logger.info("Writing modified DataFrame back to the database.")
        
        # Replace TRUNCATE and to_sql with MongoDB operations
        self._replace_collection('market_list', df_market_list)
        self._replace_collection('market_list_international', df_market_list_int)
        self._replace_collection('market_list_connections', df_market_list_conn)

    def _replace_collection(self, collection_name, df):
        """
        Replace entire collection with new data (equivalent to TRUNCATE + INSERT).
        
        Args:
            collection_name: Name of MongoDB collection
            df: DataFrame to write
        """
        try:
            collection = self.db[collection_name]
            
            # Delete all documents (equivalent to TRUNCATE)
            delete_result = collection.delete_many({})
            self.logger.info(f"Deleted {delete_result.deleted_count} documents from {collection_name}")
            
            # Insert new documents
            if not df.empty:
                if 'createdAt' not in df.columns:
                    df['createdAt'] = pd.Timestamp.now()
                else:
                    # Fill any NaT values in existing created_at column with current datetime
                    df['createdAt'] = df['createdAt'].fillna(pd.Timestamp.now())
                
                # Replace any remaining NaT values in other columns with None
                df = df.replace({pd.NaT: None})
                
                # Convert DataFrame to list of dictionaries
                records = df.to_dict('records')
                
                # Convert date columns to datetime objects if needed
                # for record in records:
                #     for key, value in record.items():
                #         # Handle date strings
                #         if isinstance(value, str) and '/' in value:
                #             try:
                #                 record[key] = datetime.strptime(value, '%m/%d/%Y')
                #             except:
                #                 pass  # Keep as string if not a date
                
                insert_result = collection.insert_many(records)
                self.logger.info(f"Inserted {len(insert_result.inserted_ids)} documents into {collection_name}")
            else:
                self.logger.info(f"No records to insert into {collection_name}")
                
        except Exception as e:
            self.logger.error(f"Error replacing collection {collection_name}: {e}")
            raise

    def get_current_date_ist(self):
        local_tz = pytz.timezone('Asia/Kolkata')  # IST timezone
        local_datetime = datetime.now(local_tz)
        return local_datetime.date()
    
    def fetch_market_list(self):
        """Fetch all documents from market_list collection."""
        try:
            collection = self.db['market_list']
            self.logger.info("Fetching data from market_list collection")
            
            cursor = collection.find({})
            df_market_list = pd.DataFrame(list(cursor))
            
            # Remove MongoDB _id field if present
            if '_id' in df_market_list.columns:
                df_market_list = df_market_list.drop('_id', axis=1)
            
            # Convert datetime fields back to string format if they were stored as datetime
            for col in ['PerStart', 'PerEnd']:
                if col in df_market_list.columns:
                    df_market_list[col] = df_market_list[col].apply(
                        lambda x: x.strftime('%m/%d/%Y') if isinstance(x, datetime) else x
                    )
            
            self.logger.info(f"Retrieved {len(df_market_list)} records from market_list")
            return df_market_list
            
        except Exception as e:
            self.logger.error(f"Error fetching market_list: {e}")
            return pd.DataFrame()
    
    def fetch_market_list_int(self):
        """Fetch all documents from market_list_international collection."""
        try:
            collection = self.db['market_list_international']
            self.logger.info("Fetching data from market_list_international collection")
            
            cursor = collection.find({})
            df_market_list = pd.DataFrame(list(cursor))
            
            # Remove MongoDB _id field if present
            if '_id' in df_market_list.columns:
                df_market_list = df_market_list.drop('_id', axis=1)
            
            # Convert datetime fields back to string format
            for col in ['PerStart', 'PerEnd']:
                if col in df_market_list.columns:
                    df_market_list[col] = df_market_list[col].apply(
                        lambda x: x.strftime('%m/%d/%Y') if isinstance(x, datetime) else x
                    )
            
            self.logger.info(f"Retrieved {len(df_market_list)} records from market_list_international")
            return df_market_list
            
        except Exception as e:
            self.logger.error(f"Error fetching market_list_international: {e}")
            return pd.DataFrame()
    
    def fetch_market_list_conn(self):
        """Fetch all documents from market_list_connections collection."""
        try:
            collection = self.db['market_list_connections']
            self.logger.info("Fetching data from market_list_connections collection")
            
            cursor = collection.find({})
            df_market_list = pd.DataFrame(list(cursor))
            
            # Remove MongoDB _id field if present
            if '_id' in df_market_list.columns:
                df_market_list = df_market_list.drop('_id', axis=1)
            
            # Convert datetime fields back to string format
            for col in ['PerStart', 'PerEnd']:
                if col in df_market_list.columns:
                    df_market_list[col] = df_market_list[col].apply(
                        lambda x: x.strftime('%m/%d/%Y') if isinstance(x, datetime) else x
                    )
            
            self.logger.info(f"Retrieved {len(df_market_list)} records from market_list_connections")
            return df_market_list
            
        except Exception as e:
            self.logger.error(f"Error fetching market_list_connections: {e}")
            return pd.DataFrame()

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

    def process_market_list(self, df_market_list, df_sector_map, current_date):
        new_rows = []
        for index, row in df_market_list.iterrows():
            start_date = datetime.strptime(row['PerStart'], '%m/%d/%Y').date()
            end_date = datetime.strptime(row['PerEnd'], '%m/%d/%Y').date()
            
            if self.should_delete_record(start_date, end_date, current_date):
                self.logger.info(f"Deleting record with index: {index}")
                df_market_list.drop(index, inplace=True)
            # elif self.should_update_record(start_date, end_date, current_date):
            #     df_market_list, new_rows = self.update_record(df_market_list, df_sector_map, row, index, start_date, end_date, current_date, new_rows)
            # else:
            #     df_market_list, new_rows = self.split_record(df_market_list, df_sector_map, row, index, start_date, end_date, current_date, new_rows)

        # if new_rows:
        #     df_market_list = df_market_list.append(new_rows, ignore_index=True)
        #     self.logger.info(f"Appended new rows: {new_rows}")

        return df_market_list

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
            collection = self.db['config_station_master']
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