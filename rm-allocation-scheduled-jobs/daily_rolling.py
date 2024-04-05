from datetime import datetime, timedelta
import uuid
import pandas as pd
import pytz

class DailyRolling:
    def __init__(self, logger, params, overrides, context):
        self.context = context
        self.logger = logger
        self.rm_rd_conn = params['rdconn']
        self.rm_wr_conn = params['wrconn']

    def run(self):
        self.logger.info("Starting the run method.")
        df_market_list = self.fetch_market_list()
        df_sector_map = self.fetch_sector_map()
        current_date = self.get_current_date_ist()
        self.logger.info("Current date: ", current_date)

        df_market_list = self.process_market_list(df_market_list, df_sector_map, current_date)

        self.logger.info("Writing modified DataFrame back to the database.")
        df_market_list.to_sql('market_list', self.rm_wr_conn, if_exists='replace', index=False)
        self.logger.info("Finished writing to market_list table.")

    def get_current_date_ist(self):
        local_tz = pytz.timezone('Asia/Kolkata')  # IST timezone
        local_datetime = datetime.now(local_tz)
        return local_datetime.date()
    
    def fetch_market_list(self):
        query_market_list = "SELECT * FROM market_list"
        self.logger.info("Executing query_market_list: ", query_market_list)
        df_market_list = pd.read_sql(query_market_list, self.rm_wr_conn)
        self.logger.info("Retrieved data from market_list")
        return df_market_list

    def fetch_sector_map(self):
        query_sector_map = "SELECT * FROM config_user_sector_map"
        self.logger.info("Executing query_sector_map: ", query_sector_map)
        df_sector_map = pd.read_sql(query_sector_map, self.rm_wr_conn)
        self.logger.info("Retrieved data from config_user_sector_map")
        return df_sector_map

    def process_market_list(self, df_market_list, df_sector_map, current_date):
        new_rows = []
        for index, row in df_market_list.iterrows():
            start_date = datetime.strptime(row['PerStart'], '%m/%d/%Y').date()
            end_date = datetime.strptime(row['PerEnd'], '%m/%d/%Y').date()

            if self.should_delete_record(start_date, end_date, current_date):
                self.logger.info("Deleting record with index: ", index)
                df_market_list.drop(index, inplace=True)
            elif self.should_update_record(start_date, end_date, current_date):
                df_market_list, new_rows = self.update_record(df_market_list, df_sector_map, row, index, start_date, end_date, current_date, new_rows)
            else:
                df_market_list, new_rows = self.split_record(df_market_list, df_sector_map, row, index, start_date, end_date, current_date, new_rows)

        if new_rows:
            df_market_list = df_market_list.append(new_rows, ignore_index=True)
            self.logger.info("Appended new rows: %s", new_rows)

        return df_market_list

    def should_delete_record(self, start_date, end_date, current_date):
        return start_date < current_date and end_date < current_date

    def should_update_record(self, start_date, end_date, current_date):
        return start_date < current_date and end_date >= current_date

    def update_record(self, df_market_list, df_sector_map, row, index, start_date, end_date, current_date, new_rows):
        duration = (end_date - current_date).days
        self.logger.info("Updating PerStart for index %s to current date", index)
        df_market_list.at[index, 'PerStart'] = current_date.strftime('%m/%d/%Y')
        sector = row['Origin'] + row['Destin']
        self.logger.info("Sector for index %s: %s", index, sector)
        sector_row = df_sector_map[df_sector_map['sector'] == sector].iloc[0]
        user1, user2 = sector_row['user1'], sector_row['user2']
        analyst_name = user1 if duration <= 30 else user2
        self.logger.info("Analyst name for index %s: %s", index, analyst_name)
        df_market_list.at[index, 'analystName'] = analyst_name

        if duration > 30 and (start_date - current_date).days <= 30:
            new_end_date = end_date - timedelta(days=duration-30)
            new_row = self.create_new_row(row, current_date, new_end_date, user1)
            new_rows.append(new_row)
            df_market_list.at[index, 'PerStart'] = (new_end_date + timedelta(days=1)).strftime('%m/%d/%Y')

        return df_market_list, new_rows

    def split_record(self, df_market_list, df_sector_map, row, index, start_date, end_date, current_date, new_rows):
        duration = (end_date - current_date).days
        sector = row['Origin'] + row['Destin']
        self.logger.info("Sector for index %s: %s", index, sector)
        sector_row = df_sector_map[df_sector_map['sector'] == sector].iloc[0]
        user1, user2 = sector_row['user1'], sector_row['user2']
        analyst_name = user1 if duration <= 30 else user2
        self.logger.info("Analyst name for index %s: %s", index, analyst_name)
        df_market_list.at[index, 'analystName'] = analyst_name

        if duration > 30 and (start_date - current_date).days <= 30:
            new_end_date = end_date - timedelta(days=duration-30)
            new_row = self.create_new_row(row, start_date, new_end_date, user1)
            new_rows.append(new_row)
            df_market_list.at[index, 'PerStart'] = (new_end_date + timedelta(days=1)).strftime('%m/%d/%Y')

        return df_market_list, new_rows

    def create_new_row(self, row, start_date, end_date, user):
        new_row = row.copy()
        new_row['PerEnd'] = end_date.strftime('%m/%d/%Y')
        new_row['PerStart'] = start_date.strftime('%m/%d/%Y')
        new_row['UUID'] = str(uuid.uuid4())
        new_row['analystName'] = user
        return new_row