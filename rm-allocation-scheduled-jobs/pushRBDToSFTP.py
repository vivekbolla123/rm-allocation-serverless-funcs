import json
import configsettings
import pandas as pd

KEY_B2B_PENDING_COUNT = 'b2b_pending_count_'
KEY_B2C_PENDING_COUNT = 'b2c_pending_count_'
KEY_B2B_READY_COUNT = 'b2b_ready_count_'
KEY_B2C_READY_COUNT = 'b2c_ready_count_'
KEY_B2B_DONE_COUNT = 'b2b_done_count_'
KEY_B2C_DONE_COUNT = 'b2c_done_count_'

class PushRBDtoSFTP:

    def __init__(self, logger, params, overrides, context):
        self.logger = logger
        self.mongoconn = params['mongoconn']  # Single MongoDB connection
        self.sqs = params['sqs']
        self.cache_client = params['cache_client']
        
        # MongoDB database
        self.db = self.mongoconn["qp_dw_rmalloc"]  # or self.mongoconn['your_db_name']

    def run(self):
        # MongoDB query to find documents where update_navitaire_method is 'sftp' 
        # and is_sftp_started is NULL (or doesn't exist)
        collection = self.db['allocation_run_audit']
        cursor = collection.find(
            {
                'update_navitaire_method': 'sftp',
                '$or': [
                    {'is_sftp_started': {'$exists': False}},
                    {'is_sftp_started': None}
                ]
            },
            {'run_id': 1, '_id': 0}
        )
        
        rows = list(cursor)
        
        for row in rows:
            run_id = row["run_id"]
            self.pushRBDToSFTP(run_id)

    def pushRBDToSFTP(self, run_id):
        b2bPendingCount = self.cache_client.get(KEY_B2B_PENDING_COUNT + run_id)
        b2cPendingCount = self.cache_client.get(KEY_B2C_PENDING_COUNT + run_id)
        b2bReadyCount = self.cache_client.get(KEY_B2B_READY_COUNT + run_id)
        b2cReadyCount = self.cache_client.get(KEY_B2C_READY_COUNT + run_id)
        
        try:
            is_complete = int(b2bReadyCount) + int(b2cReadyCount) >= int(b2bPendingCount) + int(b2cPendingCount)
        except Exception as e:
            self.logger.info(f"Run Id: {run_id} failed due to reason {e}")
            is_complete = False
        
        if is_complete:
            # All flights are ready, push run_id to the update_rbd_ftp
            self.sqs.send_message(
                QueueUrl=configsettings.SFTP_RBD_QUEUE_URL,
                MessageBody=json.dumps({'run_id': run_id}),
            )
            self.logger.info(f"{run_id} is pushed into queue for sftp")
            
            # MongoDB update operation
            collection = self.db['allocation_run_audit']
            collection.update_one(
                {'run_id': run_id},
                {'$set': {'is_sftp_started': '1'}}
            )