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

warnings.filterwarnings('ignore')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DB_WR_CONN_STRING = f"mysql+mysqldb://{configsettings.RM_DB_APPUSER_USERNAME}:{configsettings.RM_DB_APPUSER_PASSWORD}@{configsettings.RM_DB_APPUSER_URL}/{configsettings.RM_DB_SCHEMA_NAME}"

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
    processor = DataProcessor(data)
    
    for record in records:
        content = record['body']
        data = json.loads(content)
        runid=data["run_id"]
        logger.info("started ftp upload: ", runid)
        processor.fetch_data(runid)
    return {
        'statusCode': 200,
        'body': 'success'
    }


class DataProcessor:
    def __init__(self, configData):
        # Define your instance variables here
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
                    conn.execute(f"UPDATE allocation_run_audit SET is_sftp_pushed = '1' WHERE run_id = '{runid}'")
                    conn.execute(f"UPDATE run_flight_date_audit SET status = 'Done' WHERE runId = '{runid}'")
                    
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
    
    
    def divide_file(self,file_path, run_id,chunk_size_mb=40):
        
        chunk_size = chunk_size_mb * 1024 * 1024  # Convert MB to bytes
        file_size = os.path.getsize(file_path)

        if file_size <= chunk_size:
            print(f"The file '{file_path}' is already smaller than {chunk_size_mb}MB.")
            self.upload_to_sftp(configsettings.TEMP_FILE_NAME,run_id)
            return

        with open(file_path, 'rb') as input_file:
            base_filename = os.path.splitext(file_path)[0]
            chunk_number = 1
            while True:
                chunk_data = input_file.read(chunk_size)
                if not chunk_data:
                    break
                
                # Check if the chunk ends with a newline character
                if chunk_data.endswith(b'\n'):
                    # If it does, write the chunk to the current file
                    with open(f"{base_filename}_part{chunk_number}", 'wb') as output_file:
                        output_file.write(chunk_data)
                else:
                    # If it doesn't, find the last newline character position
                    last_newline_pos = chunk_data.rfind(b'\n')
                    if last_newline_pos != -1:
                        # Write the chunk up to the last newline character
                        with open(f"{base_filename}_part{chunk_number}", 'wb') as output_file:
                            output_file.write(chunk_data[:last_newline_pos + 1])
                        
                        # Seek back to the position after the last newline character
                        input_file.seek(-(len(chunk_data) - last_newline_pos - 1), os.SEEK_CUR)
                    else:
                        # If there are no newline characters, write the whole chunk
                        with open(f"{base_filename}_part{chunk_number}", 'wb') as output_file:
                            output_file.write(chunk_data)
                self.upload_to_sftp(f"{base_filename}_part{chunk_number}",run_id)
                chunk_number += 1

            print(f"File '{file_path}' has been divided into {chunk_number - 1} chunks.")

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
            if int(rbds[rbd_value]) > 0:
                rbd, rbd_value = rbd_value, self.pad_rbd_value(int(rbds[rbd_value]))
                final_date = self.format_date(departurdate)
                line = f"{final_date} QP   {flightno1} {sector1} QP   {flightno2} {sector2}{' ' * (25 - len(sector2))}"
                line += f"C {rbd}{' ' * (9 - len(rbd))}{rbd_value} {rbd_value}\n"
                file_content += line
        return file_content
    
    def generate_file_content_direct(self, departurdate, flightno, sector, rbds):
        file_content = ""
        for rbd_value in rbds:
            if int(rbds[rbd_value])>0:
                rbd, rbd_value = rbd_value,self.pad_rbd_value(int(rbds[rbd_value]))
                final_date=self.format_date(departurdate)
                line = f"{final_date} QP   {flightno} {sector}{' ' * (41 - len(sector))}"
                line += f"C {rbd}{' ' * (9 - len(rbd))}{rbd_value} {rbd_value}\n"
                file_content += line
        return file_content
    # Function to upload the file to SFTP
        
    def fetch_data(self,runid):
        data = conn.execute(f"SELECT result FROM run_flight_date_audit WHERE runId = '{runid}'")
        # Generate and write data to the AUUPDATE file
        file_name = configsettings.TEMP_FILE_NAME  # Use Lambda's /tmp directory
        with open(file_name, 'w') as file:
            for row in data:
                if(row[0] is not None):
                    data = json.loads(row[0])
                                        
                    if data["type"]=="R":
                        departurdate, flightno1, sector1, flightno2, sector2 = data["departureDate"], data["flightNo1"],data["sector1"], data["flightNo2"], data["sector2"]
                        rbds=data["auClasses"]
                        logger.info(f"started upload for {departurdate}, {flightno1}, {sector1}, {flightno2}, {sector2} ,{rbds}")
                        file_content = self.generate_file_content_conn(departurdate, flightno1, sector1, flightno2, sector2, rbds)
                    else:
                        departurdate, flightno, sector=data["departureDate"],data["flightNo1"],data["sector1"]
                        rbds=data["auClasses"]
                        logger.info(f"started upload for {departurdate}, {flightno1}, {sector1}, {flightno2}, {sector2} ,{rbds}")
                        file_content=self.generate_file_content_direct(departurdate,flightno,sector,rbds)
                    file.write(file_content)

        # Periodically check if the remote file exists (every 15 seconds)
        self.divide_file(file_name,run_id=runid)
        
        
# messageBody = {"run_id":"dcae96e5-c754-4006-8895-b7dba03edab0"}
# event = {'Records':[{'body':json.dumps(messageBody)}]}
# lambda_handler(event, None)