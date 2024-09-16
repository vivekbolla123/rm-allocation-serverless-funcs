import logging
import pandas as pd
from sqlalchemy import text

class QCCheck:

    def __init__(self, logger, params, overrides, context):
        self.logger = logging.getLogger()
        self.rm_rd_conn = params['wrconn']
    
    def run(self):
        self.check_fare_ranges()
        self.check_profile_fare_ranges()
        
    def check_fare_ranges(self):
       with self.rm_rd_conn.connect() as connection:
            # Call the stored procedure
            connection.execution_options(autocommit=True).execute(text("CALL check_fare_ranges();"))
    
    def check_profile_fare_ranges(self):
        with self.rm_rd_conn.connect() as connection:
            # Call the stored procedure
            connection.execution_options(autocommit=True).execute(text("CALL check_profile_fare_ranges();"))