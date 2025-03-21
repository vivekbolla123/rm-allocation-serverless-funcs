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
        self.load_generic_table("market_list_connections", "MarketList_Connections.csv")
        self.load_generic_table("d1_d2_strategies", "d1d2Strategy.csv")
        self.load_generic_table("allocation_acceptable_range_d1", "acceptableRange.csv")
        self.load_generic_table("dplf_bands", "dplf_bands.csv")
        self.load_generic_table("ndo_bands", "ndo_bands.csv")
        self.load_generic_table("time_ranges", "time_ranges.csv")
        self.load_generic_table("rm_parameter_values", "rm_parameter_values.csv")
        self.load_generic_table("config_pfl_threshold","config_pfl_threshold.csv")
        self.load_generic_table("departure_time_ranges","departure_time_ranges.csv")
        self.load_generic_table("booking_fare_data","booking_fare_data.csv")
        self.load_generic_table("config_share_of_booking","config_share_of_booking.csv")
        self.load_generic_table("config_runTime","config_runTime.csv")
        self.load_generic_table("no_show_probabilities","no_show_probabilities.csv")
        self.load_generic_table("config_ndo_range","config_ndo_range.csv")
        self.load_generic_table("config_ownFares","config_ownFares.csv")
        self.load_generic_table("config_marketFares_rerouting","config_marketFares_rerouting.csv")
        self.load_generic_table("config_profile_fares","config_profile_fares.csv")
        self.load_generic_table("config_profile_fare_station_upsell","config_profile_fare_station_upsell.csv")
        self.load_generic_table("config_date_event","config_date_event.csv")
        self.load_generic_table("config_determine_fares","config_determine_fares.csv")
        self.load_generic_table("current_opening_rbd","current_opening_rbd.csv")
        self.load_generic_table("default_distress_inventory","default_distress_inventory.csv")
        self.load_generic_table("distress_inventory_strategy","distress_inventory_strategy.csv")
        self.load_generic_table("current_rbd","current_rbd.csv")
        self.load_generic_table("config_clusters","config_clusters.csv")
        self.load_generic_table("series_blocking","series_blocking.csv")
        self.load_generic_table("series_booked","series_booked.csv")
        self.load_generic_table("config_conversion_rate","config_conversion_rate.csv")
        self.load_generic_table("connections_details","connections_details.csv")
        self.load_generic_table("market_list_international","Marketlist_international.csv")
        self.load_generic_table("tbf_discount_grid","tbf_discount_grid.csv")
        
        
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
        alterQuery = "ALTER TABLE market_list MODIFY COLUMN B2BTolerance text,MODIFY COLUMN B2CTolerance text"
        self.conn.execute(alterQuery)
        
        alterQuery = "ALTER TABLE bookload MODIFY COLUMN DepartureDate DATE,MODIFY COLUMN FlightNumber text"
        self.conn.execute(alterQuery)
        
        alterQuery = "ALTER TABLE d1_d2_strategies MODIFY COLUMN ndo_band INT, MODIFY COLUMN dplf_band INT, MODIFY COLUMN offset INT"
        self.conn.execute(alterQuery)

        alterQuery = "ALTER TABLE dplf_bands MODIFY COLUMN dplf_band INT, MODIFY COLUMN start INT, MODIFY COLUMN end INT"
        self.conn.execute(alterQuery)
        
        alterQuery = "ALTER TABLE ndo_bands MODIFY COLUMN ndo_band INT, MODIFY COLUMN start INT, MODIFY COLUMN end INT"
        self.conn.execute(alterQuery)
        
        alterQuery = "ALTER TABLE config_ownFares MODIFY COLUMN startTime time, MODIFY COLUMN endTime time, MODIFY COLUMN `0` INT, MODIFY COLUMN `1` INT, MODIFY COLUMN `2` INT, MODIFY COLUMN `3` INT, MODIFY COLUMN `4` INT, MODIFY COLUMN `5` INT, MODIFY COLUMN `6` INT"
        self.conn.execute(alterQuery)

def __del__(self):
    # body of destructor
    self.conn.close()


if __name__ == "__main__":
    Schema()
