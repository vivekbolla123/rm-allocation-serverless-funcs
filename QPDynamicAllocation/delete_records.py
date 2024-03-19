from sqlalchemy import create_engine

class Schema:
    
    RM_ALLOC_DB_URL = "mysql+mysqldb://devAdmin:lb7GgQsnCLTlESR7ItOM@internal-dev.cd7glmg3mmed.ap-south-1.rds.amazonaws.com/QP_DW_RMALLOC"
    conn = create_engine(RM_ALLOC_DB_URL)
    
    def __init__(self, conn):
        # Establish a connection to the DataLake
        self.conn = conn

    def initializTables(self):
        self.deleteRecordsFromTable("d1_d2_strategies")
        self.deleteRecordsFromTable("Curves")
        self.deleteRecordsFromTable("Fares")
        self.deleteRecordsFromTable("market_list")
        self.deleteRecordsFromTable("market_list_adhoc_test")
        self.deleteRecordsFromTable("allocation_acceptable_range_d1")
        self.deleteRecordsFromTable("dplf_bands")
        self.deleteRecordsFromTable("ndo_bands")
        self.deleteRecordsFromTable("time_ranges")
        self.deleteRecordsFromTable("config_pfl_threshold")
        self.deleteRecordsFromTable("no_show_probabilities")
        self.deleteRecordsFromTable("config_ownFares")
        self.deleteRecordsFromTable("config_marketFares_rerouting")

    def deleteRecordsFromTable(self, tablename):
        self.conn.execute("DELETE FROM " + tablename)

def __del__(self):
    # body of destructor
    self.conn.close()


if __name__ == "__main__":
    Schema()
