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
        self.rm_rd_conn = params['rdconn']
        self.rm_wr_conn = params['wrconn']
        self.sqs = params['sqs']
        self.cache_client = params['cache_client']

    def run(self):
        query = "SELECT run_id FROM allocation_run_audit WHERE update_navitaire_method = 'sftp' and is_sftp_started IS NULL "
        rows = pd.read_sql(query, self.rm_rd_conn)
        for index,row in rows.iterrows():
            run_id = row["run_id"]
            self.pushRBDToSFTP(run_id)

    def pushRBDToSFTP(self, run_id):
        # query2 = f"SELECT COUNT(*) as total FROM run_flight_date_audit WHERE runId = '{run_id}' AND (b2cstatus <> 'Ready' OR b2bstatus <> 'Ready')"
        # count_ready = pd.read_sql(query2, self.rm_rd_conn)["total"][0]
        # self.logger.info(f"no of records are not ready  {count_ready}")
        # is_complete = count_ready == 0

        b2bPendingCount = self.cache_client.get(KEY_B2B_PENDING_COUNT + run_id)
        b2cPendingCount = self.cache_client.get(KEY_B2C_PENDING_COUNT + run_id)
        b2bReadyCount = self.cache_client.get(KEY_B2B_READY_COUNT + run_id)
        b2cReadyCount = self.cache_client.get(KEY_B2C_READY_COUNT + run_id)
        # is_complete = b2bReadyCount == b2bPendingCount and b2cReadyCount == b2cPendingCount
        try:
            is_complete = int(b2bReadyCount) + int(b2cReadyCount) >= int(b2bPendingCount) + int(b2cPendingCount)
        except Exception as e:
            self.logger.info(f"Run Id: {run_id} failed due to reason {e}")
            is_complete = False
        
        if is_complete:
                # All flights are ready, push run_id to the update_rbd_ftp
            self.sqs.send_message(
                    QueueUrl=configsettings.SFTP_RBD_QUEUE_URL,
                    MessageBody=json.dumps(
                        {
                            'run_id': run_id
                        }),
                )
            self.logger.info(f"{run_id} is pushed into queue for sftp")
            query = f"UPDATE allocation_run_audit SET is_sftp_started = '1' WHERE run_id='{run_id}'"
            self.rm_wr_conn.execute(query)
