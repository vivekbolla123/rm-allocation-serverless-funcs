import pandas as pd
from sqlalchemy import create_engine
from uploadLogging import UploadLogging
class Schema:
    def __init__(self):

        # Establish a connection to the DataLake
        self.conn = create_engine("mysql+mysqldb://rmscriptuser:8BkNw9JEOHoxD6h@rm-prod-db.cd7glmg3mmed.ap-south-1.rds.amazonaws.com/QP_DW_RMALLOC")
        self.upload=UploadLogging("Curves",self.conn)
        self.upload.startDataLoading()

        # Load the Fare Ladder and Booking Curves
        self.load_generic_table("Curves", "Curves.csv")
        
    def loadColumnNames(self,tableName):
        query=f'''Select columns from config_column_names where tableName="{tableName}"'''
        data=pd.read_sql(query,self.conn)
        return (data["columns"][0]).split(",")
    
    
    def load_generic_table(self, tableName, dataFile):
        # This will load a csv file into a DB Table
        rawdata = pd.read_csv("data/" + dataFile)
        expected_columns = self.loadColumnNames(tableName)
        mismatched_columns = list(set(rawdata.columns) - set(expected_columns))
        if not mismatched_columns:
            self.dropTable(tableName)
            rawdata.to_sql(name=tableName, con=self.conn, if_exists='replace', index=False)
            self.indexTable(tableName)
            print("Loaded " + tableName)
            self.upload.endDataLoading()
        else:
            # Columns do not match, print the mismatched columns
            print("Error: Column(s) mismatch. The following columns are not in the expected columns:")
            print(mismatched_columns)

    def indexTable(self, tablename):
        len_query = f"SELECT MAX(LENGTH(CurveID)) as len FROM {tablename}"
        len_data = pd.read_sql(len_query, self.conn)

        update_len_query = f"ALTER TABLE {tablename} CHANGE COLUMN CurveID CurveID VARCHAR({len_data['len'][0]}) NULL DEFAULT NULL"
        self.conn.execute(update_len_query)

        update_indexes_query = f"ALTER TABLE {tablename} ADD INDEX indexNDO (NDO ASC) VISIBLE, ADD INDEX indexCurveID (CurveID ASC) VISIBLE"
        self.conn.execute(update_indexes_query)

    def dropTable(self, tablename):
        self.conn.execute("DROP TABLE if exists " + tablename)


def __del__(self):
    # body of destructor
    self.conn.close()


if __name__ == "__main__":
    Schema()
