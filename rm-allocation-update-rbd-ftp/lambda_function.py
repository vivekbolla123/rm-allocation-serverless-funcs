import json
import os
import sys
import traceback
import boto3
import warnings
import paramiko
import time
import logging
import configsettings
from datetime import datetime 
from sqlalchemy import create_engine
import pandas as pd
from redis import Redis

warnings.filterwarnings('ignore')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DB_WR_CONN_STRING = f"mysql+mysqldb://{configsettings.RM_DB_APPUSER_USERNAME}:{configsettings.RM_DB_APPUSER_PASSWORD}@{configsettings.RM_DB_APPUSER_URL}/{configsettings.RM_DB_SCHEMA_NAME}"

CACHE_ENDPOINT = configsettings.RM_CACHE_ENDPOINT
CACHE_PORT = configsettings.RM_CACHE_PORT
CACHE_USERNAME = configsettings.RM_CACHE_USERNAME
CACHE_PASSWORD = configsettings.RM_CACHE_PASSWORD

cache_client = Redis(host=CACHE_ENDPOINT, port=CACHE_PORT, username=CACHE_USERNAME, password=CACHE_PASSWORD)
if cache_client.ping():
    print("Connected to Redis")

KEY_B2B_PENDING_COUNT = 'b2b_pending_count_'
KEY_B2C_PENDING_COUNT = 'b2c_pending_count_'
KEY_B2B_DONE_COUNT = 'b2b_done_count_'
KEY_B2C_DONE_COUNT = 'b2c_done_count_'

try:
    conn = create_engine(DB_WR_CONN_STRING)
except Exception as e:
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")


def lambda_handler(event, context):
    data = {
        "starttime": datetime.now(),
        "refresh_time": 30,
        "chunk_size": 40 * 1024 * 1024,  # 4MB in bytes
        "timeout": 900,
        "userName": "SYS2",

    }
    records = event['Records']
    processor = DataProcessor(data,context)
    
    for record in records:
        content = record['body']
        data = json.loads(content)
        runid=data["run_id"]
        logger.info(f"started ftp upload: {runid}")
        processor.fetch_data(runid)
    return {
        'statusCode': 200,
        'body': 'success'
    }


class DataProcessor:
    def __init__(self, configData,context):
        # Define your instance variables here
        self.context=context
        self.starttime = configData["starttime"]
        self.refresh_time = configData["refresh_time"]
        self.chunk_size = configData["chunk_size"]
        self.sftp_host = configsettings.SFTP_HOST
        self.sftp_port = configsettings.SFTP_PORT
        self.sftp_username = configsettings.SFTP_USERNAME
        self.sftp_password = configsettings.SFTP_PASSWORD
        self.sftp_destination_dir = configsettings.SFTP_DES_DIR
        self.sftp_target_file = configsettings.SFTP_TARG_FILE
        self.AWS_SQS_QUEUE_URL = configsettings.AWS_SQS_QUEUE_URL
        self.timeout = configData["timeout"]
        self.userName = configData["userName"]

    def upload_to_sftp(self,local_file,runid):
        logger.info("Started SFTP upload.....")
        start=datetime.now()
        transport = paramiko.Transport((self.sftp_host, self.sftp_port))
        transport.connect(username=self.sftp_username, password=self.sftp_password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        remote_file_path = os.path.join(self.sftp_destination_dir, self.sftp_target_file)
        
        timeout = 0
        while timeout < self.timeout:
                # Poll the SQS queue for messages
                try:
                    # Check if the target file already exists on the SFTP server
                    try:
                        sftp.stat(remote_file_path)
                        print("File AUUPDATE already exists on the SFTP server. Waiting for deletion...")
                        logger.info(
                            f"File AUUPDATE already exists on the SFTP server. Waiting for deletion...")
                        # Wait for 30 seconds before checking again
                        time.sleep(self.refresh_time)
                        timeout = timeout + self.refresh_time
                        continue
                    except FileNotFoundError:
                        pass
                    sftp.put(local_file, remote_file_path)
                    sftp.chdir(self.sftp_destination_dir)
                    sftp.open("AUUPDATE.TAG", 'w').close()
                    elapsed_time = datetime.now() - start
                    minutes, remainder = divmod(elapsed_time.seconds, 60)
                    seconds = remainder

                    print(f"It took {minutes} minutes and {seconds} seconds. to upload file to SFTP")
                    all_done = self.is_all_allocation_done(runid)
                    
                    if(all_done == True):
                        actual_end_time = datetime.now()
                        conn.execute(f"UPDATE allocation_run_audit SET is_sftp_pushed = '1', actual_end_time ='{actual_end_time.strftime('%Y-%m-%d %H:%M:%S')}' WHERE run_id = '{runid}'")
                    sftp.close()
                    transport.close()
                    print(f"{local_file} successfully uploaded")
                    logger.info(f"{local_file} successfully uploaded")
                    break
                except Exception as e:
                    # Handle any exceptions or errors
                    traceback.print_exc()
                    logger.info(f"Error processing message: {str(e)}")
                    break
        if timeout == 900:
            logger.info("Time limit exceeded 13 minutes")
    # Check if the remote file already exists
    
        # Upload the local file to the SFTP server
    

    def pad_rbd_value(self,rbd_value):
        return f"{rbd_value:04}"

    def format_date(self,date_string):
        # Assuming date_string is in the format YYYY-MM-DD
        date_obj = datetime.strptime(date_string, "%Y-%m-%d")
        return date_obj.strftime("%Y%m%d")

    # Define a function to generate the file content
    def generate_file_content_conn(self,departurdate, flightno1, sector1, flightno2, sector2, rbds):
        file_content = ""
        for rbd_value in rbds:
            if int(rbds[rbd_value]) >= 0:
                rbd, rbd_value = rbd_value, self.pad_rbd_value(int(rbds[rbd_value]))
                final_date = self.format_date(departurdate)
                line = f"{final_date} QP   {flightno1.rjust(4, ' ')} {sector1} QP   {flightno2.rjust(4, ' ')} {sector2}{' ' * (24 - len(sector2))}"
                line += f"C {rbd}{' ' * (9 - len(rbd))}{rbd_value} {rbd_value}\n"
                file_content += line
        return file_content
    
    def generate_file_content_direct(self, departurdate, flightno, sector, rbds):
        file_content = ""
        for rbd_value in rbds:
            if int(rbds[rbd_value])>=0:
                rbd, rbd_value = rbd_value,self.pad_rbd_value(int(rbds[rbd_value]))
                final_date=self.format_date(departurdate)
                line = f"{final_date} QP   {flightno.rjust(4, ' ')} {sector}{' ' * (41 - len(sector))}"
                line += f"C {rbd}{' ' * (9 - len(rbd))}{rbd_value} {rbd_value}\n"
                file_content += line
        return file_content
    # Function to upload the file to SFTP
    def push_message_back_to_sqs(self, runid):
        # Check current retry count
        retry_query = f"SELECT sftp_retry_count FROM allocation_run_audit WHERE run_id = '{runid}'"
        retry_count_result = pd.read_sql(retry_query, conn)
        
        # Only push back to SQS if retry count is less than 2
        if int(retry_count_result['sftp_retry_count'].iloc[0]) < 2:
            # Increment retry count
            conn.execute(f"""
                UPDATE allocation_run_audit 
                SET sftp_retry_count = sftp_retry_count + 1 
                WHERE run_id = '{runid}'
            """)
            
            sqs_client = boto3.client('sqs')
            message_body = {
                "run_id": runid
            }
            sqs_client.send_message(
                QueueUrl=configsettings.AWS_SQS_QUEUE_URL,
                MessageBody=json.dumps(message_body)
            )
            logger.info(f"message with runid: {runid} sent back to Queue Successfully")
        else:
            logger.info(f"Maximum retry attempts reached for run_id: {runid}")
        
    def fetch_data(self,runid):
        retry_query = f"SELECT sftp_retry_count FROM allocation_run_audit WHERE run_id = '{runid}'"
        retry_count_result = pd.read_sql(retry_query, conn)
        
        # If retry count is already 2 or more, stop processing
        if int(retry_count_result['sftp_retry_count'].iloc[0]) >= 2:
            logger.info(f"Maximum retry attempts reached for run_id: {runid}")
            return
        data = conn.execute(f"SELECT result,flightNumber,flightDate FROM run_flight_date_audit WHERE runId = '{runid}' and (b2cstatus != 'Done' OR b2bstatus != 'Done')")
        # Generate and write data to the AUUPDATE file
        file_name = configsettings.TEMP_FILE_NAME  # Use Lambda's /tmp directory
        with open(file_name, 'w') as file:
            for row in data:
                file_size = os.path.getsize(file_name)
                if (self.context.get_remaining_time_in_millis() < configsettings.EXP_TIME or file_size>configsettings.MAX_FILE_SIZE):
                    logger.error("Lambda is about to time out, message pushed back to SQS.")
                    self.upload_to_sftp(file_name,runid)
                    self.push_message_back_to_sqs(runid)
                    return
                
                conn.execute(f"UPDATE run_flight_date_audit SET b2cstatus = 'Done', b2bstatus = 'Done' WHERE runId = '{runid}' and flightNumber='{row['flightNumber']}' and flightDate='{row['flightDate']}'")
                cache_client.incr(KEY_B2B_DONE_COUNT + runid, 1)
                cache_client.incr(KEY_B2C_DONE_COUNT + runid, 1)

                if(row["result"] is not None):
                    data = json.loads(row["result"])
                    if data["type"]=="R":
                        departurdate, flightno1, sector1, flightno2, sector2 = data["departureDate"], data["flightNo1"],data["sector1"], data["flightNo2"], data["sector2"]
                        rbds=data["auClasses"]
                        logger.info(f"started upload for {departurdate}, {flightno1}, {sector1}, {flightno2}, {sector2} ,{rbds}")
                        file_content = self.generate_file_content_conn(departurdate, flightno1, sector1, flightno2, sector2, rbds)
                    else:
                        departurdate, flightno, sector=data["departureDate"],data["flightNo1"],data["sector1"]
                        rbds=data["auClasses"]
                        logger.info(f"started upload for {departurdate}, {flightno}, {sector}, {rbds}")
                        file_content=self.generate_file_content_direct(departurdate,flightno,sector,rbds)
                    file.write(file_content)

        # Periodically check if the remote file exists (every 15 seconds)
        self.upload_to_sftp(file_name,runid)

    def is_all_allocation_done(self, run_id):
        # b2b_pending_count = cache_client.get(KEY_B2B_PENDING_COUNT + run_id)
        # b2c_pending_count = cache_client.get(KEY_B2C_PENDING_COUNT + run_id)
        # b2b_done_count = cache_client.get(KEY_B2B_DONE_COUNT + run_id)
        # b2c_done_count = cache_client.get(KEY_B2C_DONE_COUNT + run_id)
        
        # return int(b2b_pending_count) + int(b2c_pending_count) <= int(b2b_done_count) + int(b2c_done_count)
    
        query=f'''SELECT CASE WHEN COUNT(*) = SUM(b2cstatus = 'Done') AND COUNT(*) = SUM(b2bstatus = 'Done') THEN TRUE ELSE FALSE END AS all_done FROM QP_DW_RMALLOC.run_flight_date_audit WHERE runid = "{run_id}"'''
        all_done = pd.read_sql(query,conn)
        return all_done["all_done"][0] == 1
        
# messageBody = {"run_id":"dcae96e5-c754-4006-8895-b7dba03edab0"}
# event = {'Records':[{'body':json.dumps(messageBody)}]}
# lambda_handler(event, None)