import uuid
from datetime import datetime
import os
class UploadLogging:
    
        def __init__(self, tableName,conn):
            self.tablename=tableName
            self.userName=os.getlogin()
            self.conn = conn
            self.curr_version=str(uuid.uuid4())
        def startDataLoading(self):
            start_time=str(datetime.now())
            query1=f'''INSERT INTO file_upload_audit (tableName, curr_version, start_time, userName) VALUE ('{self.tablename}','{self.curr_version}','{start_time}','{self.userName}')'''
            query2=f'''UPDATE inputs_status SET is_running = b'1' WHERE name = "{self.tablename}"'''
            self.conn.execute(query1)
            self.conn.execute(query2)

        def endDataLoading(self):
            end_time=str(datetime.now())
            query1=f'''UPDATE file_upload_audit SET end_time = '{end_time}' WHERE curr_version = '{self.curr_version}' AND tableName = "{self.tablename}"'''
            query2=f'''UPDATE inputs_status SET is_running = b'0' WHERE name = "{self.tablename}"'''
            query3=f'''UPDATE currentVersion SET curr_version = '{self.curr_version}' WHERE tableName = "{self.tablename}"'''
            self.conn.execute(query1)
            self.conn.execute(query2)
            self.conn.execute(query3)
            
            