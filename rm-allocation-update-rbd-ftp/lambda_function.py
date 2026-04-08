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
from pymongo import MongoClient
import pandas as pd
from redis import Redis
from ddtrace import tracer

warnings.filterwarnings('ignore')

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def trace_span(name, resource=None, tags=None):
    """Create a safe Datadog trace span that never breaks business logic.
    
    Usage:
        span = trace_span("sftp.upload", resource="/path", tags={"run_id": "123"})
        try:
            # your code
        except Exception:
            if span:
                span.set_exc_info(*sys.exc_info())
            raise
        finally:
            finish_span(span)
    """
    try:
        span = tracer.trace(name, resource=resource)
        if tags:
            for k, v in tags.items():
                span.set_tag(k, v)
        span.set_tag("Department", "RM")
        return span
    except Exception:
        return None


def finish_span(span):
    """Safely finish a trace span."""
    if span:
        try:
            span.finish()
        except Exception:
            pass

# MongoDB connection string

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
    mongo_client = MongoClient(
            host=configsettings.MONGO_URL,
            port=27017,
            username=configsettings.MONGO_USERNAME,
            password=configsettings.MONGO_PASSWORD,
            authSource=configsettings.MONGO_AUTH_SOURCE
        )
    db = mongo_client["qp_dw_rmalloc"]
    
    # Collections
    allocation_run_audit_col = db['allocation_run_audit']
    run_flight_date_audit_col = db['run_flight_date_audit']
    
except Exception as e:
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to MongoDB instance succeeded")


def lambda_handler(event, context):
    data = {
        "starttime": datetime.now(),
        "refresh_time": 30,
        "chunk_size": 40 * 1024 * 1024,  # 4MB in bytes
        "timeout": 900,
        "userName": "SYS2",

    }
    records = event['Records']
    processor = DataProcessor(data, context)
    
    for record in records:
        content = record['body']
        data = json.loads(content)
        runid = data["run_id"]
        logger.info(f"started ftp upload: {runid}")
        processor.fetch_data(runid)
    return {
        'statusCode': 200,
        'body': 'success'
    }


class DataProcessor:
    def __init__(self, configData, context):
        # Define your instance variables here
        self.context = context
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

    def upload_to_sftp(self, local_file, runid):
        logger.info("Started SFTP upload.....")
        start = datetime.now()

        span = trace_span("sftp.connect", resource=self.sftp_host, service="au-sftp", tags={
            "sftp.host": self.sftp_host, "sftp.port": self.sftp_port, "run_id": runid
        })
        try:
            transport = paramiko.Transport((self.sftp_host, self.sftp_port))
            transport.connect(username=self.sftp_username, password=self.sftp_password)
            sftp = paramiko.SFTPClient.from_transport(transport)
        except Exception:
            if span:
                span.set_exc_info(*sys.exc_info())
            raise
        finally:
            finish_span(span)

        remote_file_path = os.path.join(self.sftp_destination_dir, self.sftp_target_file)
        
        timeout = 0
        while timeout < self.timeout:
            try:
                # Check if the target file already exists on the SFTP server
                stat_span = trace_span("sftp.stat", resource=remote_file_path, tags={"run_id": runid})
                try:
                    sftp.stat(remote_file_path)
                    finish_span(stat_span)
                    print("File AUUPDATE already exists on the SFTP server. Waiting for deletion...")
                    logger.info("File AUUPDATE already exists on the SFTP server. Waiting for deletion...")
                    time.sleep(self.refresh_time)
                    timeout += self.refresh_time
                    continue
                except FileNotFoundError:
                    finish_span(stat_span)
                except Exception:
                    if stat_span:
                        stat_span.set_exc_info(*sys.exc_info())
                    finish_span(stat_span)
                    raise
                
                file_size = os.path.getsize(local_file) if os.path.exists(local_file) else 0
                upload_span = trace_span("sftp.upload", resource=remote_file_path, tags={
                    "sftp.remote_path": remote_file_path, "run_id": runid, "sftp.file_size_bytes": file_size
                })
                try:
                    sftp.put(local_file, remote_file_path)
                    sftp.chdir(self.sftp_destination_dir)
                    sftp.open("AUUPDATE.TAG", 'w').close()
                except Exception:
                    if upload_span:
                        upload_span.set_exc_info(*sys.exc_info())
                    raise
                finally:
                    finish_span(upload_span)

                elapsed_time = datetime.now() - start
                minutes, remainder = divmod(elapsed_time.seconds, 60)
                seconds = remainder

                print(f"It took {minutes} minutes and {seconds} seconds to upload file to SFTP")
                all_done = self.is_all_allocation_done(runid)
                
                if all_done:
                    actual_end_time = datetime.now()
                    allocation_run_audit_col.update_one(
                        {"run_id": runid},
                        {"$set": {
                            "is_sftp_pushed": "1",
                            "actual_end_time": actual_end_time
                        }}
                    )
                
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

    def pad_rbd_value(self, rbd_value):
        return f"{rbd_value:04}"

    def format_date(self, date_string):
        # Assuming date_string is in the format YYYY-MM-DD
        date_obj = datetime.strptime(date_string, "%Y-%m-%d")
        return date_obj.strftime("%Y%m%d")

    # Define a function to generate the file content
    def generate_file_content_conn(self, departurdate, flightno1, sector1, flightno2, sector2, rbds):
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
            if int(rbds[rbd_value]) >= 0:
                rbd, rbd_value = rbd_value, self.pad_rbd_value(int(rbds[rbd_value]))
                final_date = self.format_date(departurdate)
                line = f"{final_date} QP   {flightno.rjust(4, ' ')} {sector}{' ' * (41 - len(sector))}"
                line += f"C {rbd}{' ' * (9 - len(rbd))}{rbd_value} {rbd_value}\n"
                file_content += line
        return file_content
    
    def generate_file_content_through(self, departurdate, flightno, sector1, sector2, rbds):
        """
        Generate file content for through flights (connections with same flight number)
        Sector format: first 3 chars of sector1 + last 3 chars of sector2
        """
        file_content = ""
        # Combine sectors: first 3 of sector1 + last 3 of sector2
        combined_sector = sector1[:3] + sector2[-3:]
        
        for rbd_value in rbds:
            if int(rbds[rbd_value]) >= 0:
                rbd, rbd_value = rbd_value, self.pad_rbd_value(int(rbds[rbd_value]))
                final_date = self.format_date(departurdate)
                line = f"{final_date} QP   {flightno.rjust(4, ' ')} {combined_sector}{' ' * (41 - len(combined_sector))}"
                line += f"C {rbd}{' ' * (9 - len(rbd))}{rbd_value} {rbd_value}\n"
                file_content += line
        return file_content

    
    # Function to upload the file to SFTP
    def push_message_back_to_sqs(self, runid):
        # Check current retry count
        retry_count_result = allocation_run_audit_col.find_one(
            {"run_id": runid},
            {"sftp_retry_count": 1}
        )
        
        # Only push back to SQS if retry count is less than 2
        if retry_count_result and int(retry_count_result.get('sftp_retry_count', 0)) < 2:
            # Increment retry count
            allocation_run_audit_col.update_one(
                {"run_id": runid},
                {"$inc": {"sftp_retry_count": 1}}
            )
            
            sqs_client = boto3.client('sqs')
            message_body = {
                "run_id": runid
            }
            sqs_client.send_message(
                QueueUrl=configsettings.AWS_SQS_QUEUE_URL,
                MessageBody=json.dumps(message_body)
            )
            logger.info(f"Message with runid: {runid} sent back to Queue Successfully")
        else:
            logger.info(f"Maximum retry attempts reached for run_id: {runid}")
        
    def fetch_data(self, runid):
        retry_count_result = allocation_run_audit_col.find_one(
            {"run_id": runid},
            {"sftp_retry_count": 1}
        )
        
        # If retry count is already 2 or more, stop processing
        if retry_count_result and int(retry_count_result.get('sftp_retry_count', 0)) >= 2:
            logger.info(f"Maximum retry attempts reached for run_id: {runid}")
            return
        
        # Fetch data from MongoDB
        data_cursor = run_flight_date_audit_col.find(
            {
                "runId": runid,
                "$or": [
                    {"b2cstatus": {"$ne": "Done"}},
                    {"b2bstatus": {"$ne": "Done"}}
                ]
            },
            {"result": 1, "flightNumber": 1, "flightDate": 1, "sector": 1}
        )
        
        # Generate and write data to the AUUPDATE file
        file_name = configsettings.TEMP_FILE_NAME  # Use Lambda's /tmp directory
        with open(file_name, 'w') as file:
            for row in data_cursor:
                file_size = os.path.getsize(file_name)
                if (self.context.get_remaining_time_in_millis() < configsettings.EXP_TIME or file_size > configsettings.MAX_FILE_SIZE):
                    logger.error("Lambda is about to time out, message pushed back to SQS.")
                    self.upload_to_sftp(file_name, runid)
                    self.push_message_back_to_sqs(runid)
                    return
                
                # Update the document
                run_flight_date_audit_col.update_one(
                    {
                        "runId": runid,
                        "flightNumber": row['flightNumber'],
                        "flightDate": row['flightDate'],
                        "sector": row['sector']
                    },
                    {"$set": {
                        "b2cstatus": "Done",
                        "b2bstatus": "Done"
                    }}
                )
                
                cache_client.incr(KEY_B2B_DONE_COUNT + runid, 1)
                cache_client.incr(KEY_B2C_DONE_COUNT + runid, 1)

                if row.get("result") is not None:
                    data = json.loads(row["result"])
                    if data["type"] == "R":
                        departurdate = data["departureDate"]
                        flightno1 = data["flightNo1"]
                        sector1 = data["sector1"]
                        flightno2 = data["flightNo2"]
                        sector2 = data["sector2"]
                        rbds = data["auClasses"]
                        
                        # Check if it's a through flight (same flight numbers)
                        if flightno1 == flightno2:
                            logger.info(f"started upload for through flight {departurdate}, {flightno1}, {sector1}, {sector2}, {rbds}")
                            file_content = self.generate_file_content_through(departurdate, flightno1, sector1, sector2, rbds)
                        else:
                            logger.info(f"started upload for connection {departurdate}, {flightno1}, {sector1}, {flightno2}, {sector2}, {rbds}")
                            file_content = self.generate_file_content_conn(departurdate, flightno1, sector1, flightno2, sector2, rbds)
                    else:
                        departurdate, flightno, sector = data["departureDate"], data["flightNo1"], data["sector1"]
                        rbds = data["auClasses"]
                        logger.info(f"started upload for direct {departurdate}, {flightno}, {sector}, {rbds}")
                        file_content = self.generate_file_content_direct(departurdate, flightno, sector, rbds)
                    file.write(file_content)


        # Periodically check if the remote file exists (every 15 seconds)
        self.upload_to_sftp(file_name, runid)

    def is_all_allocation_done(self, run_id):
        # Count total documents and documents with 'Done' status
        pipeline = [
            {"$match": {"runId": run_id}},
            {"$group": {
                "_id": None,
                "total_count": {"$sum": 1},
                "b2c_done_count": {
                    "$sum": {"$cond": [{"$eq": ["$b2cstatus", "Done"]}, 1, 0]}
                },
                "b2b_done_count": {
                    "$sum": {"$cond": [{"$eq": ["$b2bstatus", "Done"]}, 1, 0]}
                }
            }}
        ]
        
        result = list(run_flight_date_audit_col.aggregate(pipeline))
        
        if result:
            total = result[0]['total_count']
            b2c_done = result[0]['b2c_done_count']
            b2b_done = result[0]['b2b_done_count']
            return total == b2c_done and total == b2b_done
        
        return False
        
# messageBody = {"run_id":"c9e3ce37-d40f-4580-8a8c-f5609d897f06"}
# event = {'Records':[{'body':json.dumps(messageBody)}]}
# lambda_handler(event, None)