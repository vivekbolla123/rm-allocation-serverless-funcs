import os
import sys
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
        "chunk_size": 4 * 1024 * 1024,  # 4MB in bytes
        "timeout": 900,
        "userName": "SYS2",

    }
    records = event['Records']
    processor = DataProcessor(data)
    for record in records:
        content = record['body']
        receiptHandle = record['receiptHandle']
        processor.sendsftp(content, receiptHandle, conn)

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
        self.sftp_destination_dir = configsettings.SFT_DES_DIR
        self.sftp_target_file = configsettings.SFTP_TARG_FILE
        self.AWS_BUCKET_NAME = configsettings.AWS_BUCKET_NAME
        self.QUEUE_NAME = configsettings.QUEUE_NAME
        self.AWS_SQS_QUEUE_URL = configsettings.AWS_SQS_QUEUE_URL
        self.CHUNKS_PATH=configsettings.AWS_BUCKET_CHUNKS_PATH
        self.timeout = configData["timeout"]
        self.userName = configData["userName"]

    def sendsftp(self, messageBody, receiptHandle, conn):
        sqs = boto3.client('sqs')
        s3 = boto3.client('s3')
        timeout = 0
        transport = paramiko.Transport((self.sftp_host, self.sftp_port))
        transport.connect(username=self.sftp_username,
                          password=self.sftp_password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        while timeout < self.timeout:
            # Poll the SQS queue for messages
            try:
                # Check if the target file already exists on the SFTP server
                try:
                    sftp.stat(os.path.join(
                        self.sftp_destination_dir, self.sftp_target_file))
                    logger.info(
                        f"File {self.sftp_target_file} already exists on the SFTP server. Waiting for deletion...")
                    # Wait for 30 seconds before checking again
                    time.sleep(self.refresh_time)
                    timeout = timeout + self.refresh_time
                    continue
                except FileNotFoundError:
                    pass

                # Retrieve file path and bucket name from message attributes
                file_path = messageBody

                # Download the file from S3
                file_name = os.path.basename(file_path)
                local_file_path = '/tmp/' + file_name
                s3.download_file(
                    self.AWS_BUCKET_NAME, f"{self.CHUNKS_PATH}/{file_name}", local_file_path)
                # Connect to the SFTP server and upload the file
                sftp.put(local_file_path, os.path.join(
                    self.sftp_destination_dir, self.sftp_target_file))
                logger.info(f"{file_name} successfully uploaded")
                sftp.close()
                transport.close()
                # If successful, delete the file from S3
                s3.delete_object(Bucket=self.AWS_BUCKET_NAME,
                                 Key=f"{self.CHUNKS_PATH}/{file_name}")

                # Delete the message from the SQS queue
                sqs.delete_message(
                    QueueUrl=self.AWS_SQS_QUEUE_URL, ReceiptHandle=receiptHandle)
                endtime = datetime.now()
                query = f'''INSERT INTO auchunks_sftp_audit (chunk_name,start_time, end_time,user) VALUE ("{messageBody}","{self.starttime}","{endtime}","{self.userName}")'''
                conn.execute(query)
                break
            except Exception as e:
                # Handle any exceptions or errors
                logger.info(f"Error processing message: {str(e)}")
                break

        if timeout == self.timeout:
            logger.info("Time limit exceeded 15 minutes")
