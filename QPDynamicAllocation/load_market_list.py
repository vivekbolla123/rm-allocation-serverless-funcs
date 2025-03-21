import pandas as pd
from sqlalchemy import create_engine
import uuid
from uploadLogging import UploadLogging


class Schema:
    def __init__(self):

        # Establish a connection to the DataLake
        self.conn = create_engine("mysql+mysqldb://rmscriptuser:8BkNw9JEOHoxD6h@rm-prod-db.cd7glmg3mmed.ap-south-1.rds.amazonaws.com/QP_DW_RMALLOC")
        self.upload=UploadLogging("market_list",self.conn)
        self.upload.startDataLoading()

        # Load the Fare Ladder and Booking Curves
        self.load_generic_table("market_list", "MarketList.csv")

    def load_generic_table(self, tableName, dataFile):
        # This will load a csv file into a DB Table
        rawdata = pd.read_csv("params/" + dataFile)
        expected_columns=self.loadColumnNames(tableName)
        mismatched_columns = list(set(rawdata.columns) - set(expected_columns))
        if not mismatched_columns:
            rawdata = pd.DataFrame(rawdata)
            rawdata['UUID'] = [str(uuid.uuid4()) for _ in range(len(rawdata))]
            self.dropTable(tableName)
            rawdata.to_sql(name=tableName, con=self.conn,
                        if_exists='replace', index=False)
            print("Loaded " + tableName)
            self.upload.endDataLoading()
        else:
            print("Error: Column(s) mismatch. The following columns are not in the expected columns:")
            print(mismatched_columns)

    def dropTable(self, tablename):
        self.conn.execute("DROP TABLE if exists " + tablename)
    def loadColumnNames(self,tableName):
        query=f'''Select columns from config_column_names where tableName="{tableName}"'''
        data=pd.read_sql(query,self.conn)
        return (data["columns"][0]).split(",")

def __del__(self):
    # body of destructor
    self.conn.close()


if __name__ == "__main__":
    Schema()
