from datetime import datetime, timedelta
import uuid
import pandas as pd
import pytz
import configsettings

class DailyRolling:
    def __init__(self, logger, params, overrides, context):
        self.context = context
        self.logger = logger
        self.rm_rd_conn = params['rdconn']
        self.rm_wr_conn = params['wrconn']
        self.nav_conn = params['navconn']

    def run(self):
        self.logger.info("Starting the run method.")
        df_market_list = self.fetch_market_list()
        df_market_list_int = self.fetch_market_list_int()
        df_sector_map = self.fetch_sector_map()
        current_date = self.get_current_date_ist()
        self.logger.info(f"Current date: {current_date}")
        print(len(df_market_list))
        df_market_list = self.process_market_list(df_market_list, df_sector_map, current_date)
        df_market_list_int = self.process_market_list(df_market_list_int, df_sector_map, current_date)
        
        print(len(df_market_list))
        # df_market_list=self.create_new_row(df_market_list,current_date,df_sector_map)
        print(len(df_market_list))
        self.logger.info("Writing modified DataFrame back to the database.")
        self.rm_wr_conn.execute("TRUNCATE TABLE market_list")
        self.rm_wr_conn.execute("TRUNCATE TABLE market_list_international")
        df_market_list.to_sql('market_list', self.rm_wr_conn, if_exists='append', index=False)
        df_market_list_int.to_sql('market_list_international', self.rm_wr_conn, if_exists='append', index=False)
        

    def get_current_date_ist(self):
        local_tz = pytz.timezone('Asia/Kolkata')  # IST timezone
        local_datetime = datetime.now(local_tz)
        return local_datetime.date()
    
    def fetch_market_list(self):
        query_market_list = "SELECT * FROM market_list"
        self.logger.info(f"Executing query_market_list: {query_market_list}")
        df_market_list = pd.read_sql(query_market_list, self.rm_wr_conn)
        self.logger.info("Retrieved data from market_list")
        return df_market_list
    
    def fetch_market_list_int(self):
        query_market_list = "SELECT * FROM market_list_international"
        self.logger.info(f"Executing query_market_list: {query_market_list}")
        df_market_list = pd.read_sql(query_market_list, self.rm_wr_conn)
        self.logger.info("Retrieved data from market_list_international")
        return df_market_list


    def fetch_sector_map(self):
        query_sector_map = "SELECT * FROM config_user_sector_map"
        self.logger.info(f"Executing query_sector_map: {query_sector_map}")
        df_sector_map = pd.read_sql(query_sector_map, self.rm_wr_conn)
        self.logger.info("Retrieved data from config_user_sector_map")
        return df_sector_map

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
        c1 =  (start_date < current_date and end_date < current_date)
        c2 = start_date==end_date
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
                df_market_list = pd.concat([df_market_list,new_row], ignore_index=True)

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
        if previous_rows.shape[0]!=0:
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
    
    def update_dow(self,start_date, end_date):
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
    
    def get_nav_dump(self,depDate):
        station_master_query="select station from config_station_master where route='I'"
        station_master=pd.read_sql(station_master_query,self.rm_rd_conn)
        int_stations=tuple(f"{val}" for val in station_master["station"])
        if (len(int_stations)==1):
            int_stations=f"('{int_stations[0]}')"
        query=f"""SELECT IL.DepartureStation as Origin,IL.ArrivalStation as Destination, 
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
        dump=pd.read_sql(query,self.nav_conn)
        self.logger.info(f"dump legth {len(dump)}")
        return dump