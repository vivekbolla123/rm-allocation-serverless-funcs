import sys
import pandas as pd
from sqlalchemy import create_engine


class Schema:
    def __init__(self, conn):
        # Establish a connection to the DataLake
        self.conn = conn

    def initializTables(self):
        self.load_generic_table("Curves", "Curves.csv")
        self.load_generic_table("Fares", "Fares.csv")
        self.load_generic_table("market_fares", "MarketFares.csv")
        self.load_generic_table("bookload", "Bookload.csv")
        self.load_generic_table("market_list", "MarketList.csv")
        self.load_generic_table("d1_d2_strategies", "d1d2Strategy.csv")
        self.load_generic_table("allocation_acceptable_range_d1", "acceptableRange.csv")
        self.load_generic_table("dplf_bands", "dplf_bands.csv")
        self.load_generic_table("ndo_bands", "ndo_bands.csv")
        self.load_generic_table("time_ranges", "time_ranges.csv")
        self.load_generic_table("rm_parameter_values", "rm_parameter_values.csv")
        self.load_generic_table("config_pfl_threshold","config_pfl_threshold.csv")
        self.load_generic_table("departure_time_ranges","departure_time_ranges.csv")
        self.alterTables()

    def load_generic_table(self, tableName, dataFile):
        # This will load a csv file into a DB Table
        rawdata = pd.read_csv(f"{sys.path[0]}/params/data/" + dataFile)

        self.dropTable(tableName)
        rawdata.to_sql(name=tableName, con=self.conn,
                       if_exists='replace', index=False)
        print("Loaded " + tableName)

    def dropTable(self, tablename):
        self.conn.execute("DROP TABLE if exists " + tablename)

    def alterTables(self):
        alterQuery = "ALTER TABLE bookload MODIFY COLUMN DepartureDate DATE,MODIFY COLUMN FlightNumber text"
        self.conn.execute(alterQuery)
        
        alterQuery = "ALTER TABLE d1_d2_strategies MODIFY COLUMN ndo_band INT, MODIFY COLUMN dplf_band INT, MODIFY COLUMN offset INT"
        self.conn.execute(alterQuery)

        alterQuery = "ALTER TABLE dplf_bands MODIFY COLUMN dplf_band INT, MODIFY COLUMN start INT, MODIFY COLUMN end INT"
        self.conn.execute(alterQuery)
        
        alterQuery = "ALTER TABLE ndo_bands MODIFY COLUMN ndo_band INT, MODIFY COLUMN start INT, MODIFY COLUMN end INT"
        self.conn.execute(alterQuery)

def __del__(self):
    # body of destructor
    self.conn.close()


if __name__ == "__main__":
    Schema()
