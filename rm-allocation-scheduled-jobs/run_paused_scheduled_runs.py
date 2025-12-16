import sys
import json
import warnings
from datetime import datetime, timedelta, timezone
import croniter
from pyawscron import AWSCron
import pandas as pd
from collections import Counter
import configsettings
import traceback

KEY_B2B_PENDING_COUNT = 'b2b_pending_count_'
KEY_B2C_PENDING_COUNT = 'b2c_pending_count_'
KEY_B2B_READY_COUNT = 'b2b_ready_count_'
KEY_B2C_READY_COUNT = 'b2c_ready_count_'
KEY_B2B_DONE_COUNT = 'b2b_done_count_'
KEY_B2C_DONE_COUNT = 'b2c_done_count_'

class Run_Paused_Runs:
    def __init__(self, logger, params, overrides, context):
        warnings.filterwarnings('ignore')
        self.USERNAME = "SYS2"

        self.logger = logger
        self.mongoconn = params['mongoconn']  # Single MongoDB connection
        self.sqs = params['sqs']
        self.cache_client = params['cache_client']
        
        # MongoDB database
        self.db = self.mongoconn['qp_dw_rmalloc'] 
    
    def run(self):
        try:
            self.logger.info("SUCCESS: Connection to MongoDB instance succeeded")
            if self.cache_client.ping():
                self.logger.info("Connected to Redis")
            
            # check if any actual paused runs
            runs = self.get_runs_by_status('AUTO-PAUSED', 6)
            active_runs = [run for run in runs if self.is_run_active(run['run_id'], run['type'])]

            # check if a priority run is active
            if active_runs:
                pri_runs = self.get_runs_by_status('PRIORITY', 1)
                pri_active_runs = [pri_run for pri_run in pri_runs if self.is_run_active(pri_run['run_id'], pri_run['type'])]

                if pri_active_runs:
                    self.logger.info(f"Skipping since a priority run is active")

                else:
                    # If no priority run active, then load all paused runs into sqs
                    for run in active_runs:
                        run_id = run['run_id']
                        status = 'AUTO-PLAYED'
                        run_records = self.get_paused_run_records(run_id)

                        self.update_run_status(run_id, status)
                        self.add_operation_log(run_id, status, self.USERNAME)

                        if run_records:
                            is_sent = self.send_message_batches(run_records)
                            
                            if is_sent:
                                self.logger.info(f"Sent all messages to SQS for run {run_id}")
                                self.delete_paused_run_data(run_id)
                        else:
                            self.logger.info(f"No records found for run {run_id}")

            self.logger.info("Run method completed.")
        except Exception as e:
            self.logger.error(f"An error occurred in the run method: {traceback.format_exc()}")
    
    def get_runs_by_status(self, status, hours_back):
        collection = self.db['allocation_run_audit']

        # Get current time and one day ago in same string format as stored in DB
        now = datetime.now(timezone.utc)
        one_day_ago = now - timedelta(hours=hours_back)
        one_day_ago_str = one_day_ago.strftime("%Y-%m-%d %H:%M:%S")
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")

        cursor = collection.find(
            {
                'run_status': status,
                'start_time': {
                    '$gte': one_day_ago_str,
                    '$lte': now_str
                }
            },
            {'run_id': 1, 'type': 1, '_id': 0}
        )

        rows = list(cursor)
        return rows
    
    def get_paused_run_records(self, run_id):
        collection = self.db['run_paused']
        cursor = collection.find(
            {'run_id': run_id},
            {'value': 1, '_id': 0}
        )
        
        rows = list(cursor)
        records = [row['value'] for row in rows]
        return records
    
    def is_run_active(self, run_id, type):
        b2bPendingCount = self.cache_client.get(KEY_B2B_PENDING_COUNT + run_id)
        b2cPendingCount = self.cache_client.get(KEY_B2C_PENDING_COUNT + run_id)
        b2bDoneCount = self.cache_client.get(KEY_B2B_DONE_COUNT + run_id)
        b2cDoneCount = self.cache_client.get(KEY_B2C_DONE_COUNT + run_id)
        is_complete = False
        
        try:
            # for connections
            if type.lower().endswith('connections'):
                db_total_count, db_done_count = self.get_conn_run_counts(run_id)
                is_complete = db_done_count >= db_total_count

            # for direct
            else:
                is_complete = int(b2bDoneCount) + int(b2cDoneCount) >= int(b2bPendingCount) + int(b2cPendingCount)

        except Exception as e:
            self.logger.info(f"Run Id: {run_id} failed due to reason {e}")
            is_complete = False
        
        return not is_complete
    
    def send_message_batches(self, list_of_values):
        total_sent = 0
        batch_size = 10

        for i in range(0, len(list_of_values), batch_size):
            batch = list_of_values[i:i + batch_size]

            entries = [
                {
                    'Id': str(idx),  # must be unique within the batch
                    'MessageBody': record
                }
                for idx, record in enumerate(batch)
            ]

            response = self.sqs.send_message_batch(
                QueueUrl=configsettings.MARKET_LIST_QUEUE_URL,
                Entries=entries
            )

            successful = response.get('Successful', [])
            total_sent += len(successful)

        return total_sent == len(list_of_values)
    
    def add_operation_log(self, run_id, operation, user):
        try:
            collection = self.db['allocation_run_operation_log']
            operation_log = {
                'run_id': run_id,
                'operation': operation,
                'user': user,
                'timestamp': datetime.now(timezone.utc)
            }
            collection.insert_one(operation_log)
        except Exception as e:
            print(f"Error in add_operation_log: {str(e)}")

    def update_run_status(self, run_id, status):
        try:
            collection = self.db['allocation_run_audit']
            result = collection.update_one(
                {'run_id': run_id},
                {'$set': {'run_status': status}}
            )
        except Exception as e:
            print(f"Error in update_run_status: {str(e)}")

    def delete_paused_run_data(self, run_id):
        try:
            collection = self.db['run_paused']
            result = collection.delete_many({'run_id': run_id})
        except Exception as e:
            print(f"Error deleting paused run data for run {run_id}: {str(e)}")

    def get_conn_run_counts(self, run_id):
        total_count = done_count = 0

        try:
            collection = self.db['run_flight_date_audit']
            result = collection.aggregate([
                {"$match": {"runId": run_id}},
                {
                    "$group": {
                        "_id": None,
                        "total": {"$sum": 1},
                        "done": {
                            "$sum": {
                                "$cond": [
                                    {
                                        "$and": [
                                            {"$eq": ["$b2bstatus", "Done"]},
                                            {"$eq": ["$b2cstatus", "Done"]}
                                        ]
                                    },
                                    1,
                                    0
                                ]
                            }
                        }
                    }
                }
            ])

            # Extract result safely
            data = next(result, {"total": 0, "done": 0})
            total_count = data['total']
            done_count = data['done']
        except Exception as e:
            self.logger.error(f"Unable to get DB Done count for run {run_id}")

        return total_count, done_count