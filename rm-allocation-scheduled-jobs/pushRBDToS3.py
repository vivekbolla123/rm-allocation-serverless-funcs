import json
import configsettings
import pandas as pd
from io import StringIO
from boto3 import client
from datetime import datetime

KEY_B2B_PENDING_COUNT = 'b2b_pending_count_'
KEY_B2C_PENDING_COUNT = 'b2c_pending_count_'
KEY_B2B_DONE_COUNT = 'b2b_done_count_'
KEY_B2C_DONE_COUNT = 'b2c_done_count_'
KEY_B2B_READY_COUNT = 'b2b_ready_count_'
KEY_B2C_READY_COUNT = 'b2c_ready_count_'

class pushRBDToS3:

    def __init__(self, logger, params, overrides, context):
        self.context = context
        self.logger = logger
        self.rm_rd_conn = params['rdconn']
        self.sqs = params['sqs']
        self.cache_client = params['cache_client']

        self.S3_FILE_PATH = configsettings.S3_FILE_PATH
        self.S3_BUCKET_NAME = configsettings.S3_BUCKET_NAME

    def run(self):
        try:
            self.logger.info("Starting the run method.")
            query = "SELECT run_id FROM allocation_run_audit WHERE update_navitaire_method = 'S3' and is_s3_pushed IS NULL "
            rows = pd.read_sql(query, self.rm_rd_conn)
            for index, row in rows.iterrows():
                run_id = row["run_id"]
                self.fetch_data_AUGrid(run_id)
                self.fetch_data_runSummary(run_id)
                self.updateRecords(run_id)
            self.logger.info("Run method completed successfully.")
        except Exception as e:
            self.logger.error(f"An error occurred in the run method: {str(e)}")

    def updateRecords(self, run_id):
        try:
            self.logger.debug(f"Updating records for run_id: {run_id}")
            all_done = self.is_all_allocation_done(run_id)

            if all_done:
                b2bPendingCount = self.cache_client.get(KEY_B2B_PENDING_COUNT + run_id)
                b2cPendingCount = self.cache_client.get(KEY_B2C_PENDING_COUNT + run_id)
                self.cache_client.set(KEY_B2B_READY_COUNT + run_id, int(b2bPendingCount))
                self.cache_client.set(KEY_B2C_READY_COUNT + run_id, int(b2cPendingCount))

                actual_end_time = datetime.now()
                self.rm_rd_conn.execute(
                    f"UPDATE allocation_run_audit SET is_s3_pushed = '1', actual_end_time ='{actual_end_time.strftime('%Y-%m-%d %H:%M:%S')}' WHERE run_id = '{run_id}'"
                )
                self.logger.info(f"Records updated successfully for run_id: {run_id}")
        except Exception as e:
            self.logger.error(f"An error occurred in updateRecords: {str(e)}")

    def fetch_data_AUGrid(self, runid):
        try:
            self.logger.debug(f"Fetching AU Grid data for run_id: {runid}")
            data = self.rm_rd_conn.execute(
                f"SELECT result,flightNumber,flightDate FROM run_flight_date_audit WHERE runId = '{runid}' and (b2cstatus != 'Done' OR b2bstatus != 'Done')"
            )
            AUFrame = []
            AUHeaders = self.generateAUHeaders()

            for row in data:
                AUFrame.append(eval(row['result']))
                self.rm_rd_conn.execute(
                    f"UPDATE run_flight_date_audit SET b2cstatus = 'Done', b2bstatus = 'Done' WHERE runId = '{runid}' and flightNumber='{row['flightNumber']}' and flightDate='{row['flightDate']}'"
                )

            if not len(AUFrame) == 0:
                AUFrame = pd.DataFrame(AUFrame, columns=AUHeaders)
                self.appendToS3(AUFrame, runid, 'AUGrid')
                self.logger.info(f"AUGrid data fetched and uploaded to S3 for run_id: {runid}")
            else:
                self.logger.warning(f"No AU Grid data found for run_id: {runid}")
        except Exception as e:
            self.logger.error(f"An error occurred in fetch_data_AUGrid: {str(e)}")

    def fetch_data_runSummary(self, runid):
        try:
            self.logger.debug(f"Fetching runSummary data for run_id: {runid}")
            summaryData = self.generateSummaryData(runid)
            self.appendToS3(summaryData, runid, 'run_summary')
            self.logger.info(f"runSummary data fetched and uploaded to S3 for run_id: {runid}")
        except Exception as e:
            self.logger.error(f"An error occurred in fetch_data_runSummary: {str(e)}")

    def push_message_back_to_sqs(self):
        try:
            self.logger.debug("Pushing message back to SQS.")
            sqs_client = client('sqs')
            message_body = {
                "s3_push": 1
            }
            sqs_client.send_message(
                QueueUrl=configsettings.AWS_SQS_QUEUE_URL,
                MessageBody=json.dumps(message_body)
            )
            self.logger.info("Message sent back to the queue successfully")
        except Exception as e:
            self.logger.error(f"An error occurred in push_message_back_to_sqs: {str(e)}")

    def generateAUHeaders(self):
        self.logger.debug("Generating AU Headers.")
        # AU Grid
        AUlist = ['Type', 'Flight', 'StartDate', 'EndDate', 'DayOfWeek', 'Aircraft Type', 'Aircraft Suffix', 'Lid', 'Capacity', 'Status']
        length = 86
        for i in range(int(length)):
            AUlist.append(f'Class Of Service AU Class {i+1}')
            AUlist.append(f'Class Type AU Class {i+1}')
            AUlist.append(f'Class Nest AU Class {i+1}')
            AUlist.append(f'Class Rank AU Class {i+1}')
            AUlist.append(f'Class AU AU Class {i+1}')
            AUlist.append(f'Class Allotted AU Class {i+1}')
            AUlist.append(f'AP Restriction AU Class {i+1}')

        return AUlist

    def generateSummaryHeaders(self):
        try:
            self.logger.debug("Generating Summary Headers.")
            query = f"SELECT columns FROM QP_DW_RMALLOC.config_column_names where tableName = 'run_summary' "
            data = pd.read_sql(query, self.rm_rd_conn)
            return data['columns'][0]
        except Exception as e:
            self.logger.error(f"An error occurred in generateSummaryHeaders: {str(e)}")

    def generateSummaryData(self, runId):
        try:
            self.logger.debug(f"Generating Summary Data for run_id: {runId}")
            summaryHeaders = self.generateSummaryHeaders()
            query = f"SELECT {summaryHeaders} FROM QP_DW_RMALLOC.run_summary where RunId = '{runId}' "
            data = pd.read_sql(query, self.rm_rd_conn)
            return data
        except Exception as e:
            self.logger.error(f"An error occurred in generateSummaryData: {str(e)}")

    def appendToS3(self, AUFrame, runId, path):
        try:
            self.logger.debug(f"Appending data to S3 for run_id: {runId}, path: {path}")
            # Convert DataFrame to CSV format in-memory using StringIO
            csv_buffer = StringIO()
            AUFrame.to_csv(csv_buffer, index=False)

            # Reset the buffer position to the beginning before uploading
            csv_buffer.seek(0)

            # Upload the CSV content to S3
            bucket_name = self.S3_BUCKET_NAME
            key = f"{self.S3_FILE_PATH}{path}/{runId}.csv"
            s3_client = client('s3')
            s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=key)
            self.logger.info(f"Data uploaded to S3 for run_id: {runId}")
        except Exception as e:
            self.logger.error(f"An error occurred in appendToS3: {str(e)}")

    def is_all_allocation_done(self, run_id):
        try:
            self.logger.debug(f"Checking if all allocation is done for run_id: {run_id}")
            query = f'''SELECT CASE WHEN COUNT(*) = SUM(b2cstatus = 'Done') AND COUNT(*) = SUM(b2bstatus = 'Done') THEN TRUE ELSE FALSE END AS all_done FROM QP_DW_RMALLOC.run_flight_date_audit WHERE runid = "{run_id}"'''
            all_done = pd.read_sql(query, self.rm_rd_conn)
            return all_done["all_done"][0] == 1
        except Exception as e:
            self.logger.error(f"An error occurred in is_all_allocation_done: {str(e)}")
