import json
import configsettings
import pandas as pd

class PushRBDtoSFTP:

    def __init__(self, sqs, logger, rm_rd_conn):
        self.sqs = sqs
        self.logger = logger
        self.rm_rd_conn = rm_rd_conn

    def run(self):
        query = "SELECT run_id FROM allocation_run_audit WHERE update_navitaire_method = 'sftp' and is_sftp_pushed IS NULL "
        rows = pd.read_sql(query, self.rm_rd_conn)
        for index,row in rows.iterrows():
            run_id = row["run_id"]
            self.pushRBDToSFTP(run_id)

    def pushRBDToSFTP(self, run_id):
        query2 = f"SELECT COUNT(*) as total FROM run_flight_date_audit WHERE runId = '{run_id}' AND (b2cstatus <> 'Ready' OR b2bstatus <> 'Ready')"
        count_ready = pd.read_sql(query2, self.rm_rd_conn)["total"][0]
        self.logger.info(f"no of records are not ready  {count_ready}")
        if count_ready == 0:
                # All flights are ready, push run_id to the update_rbd_ftp
            self.sqs.send_message(
                    QueueUrl=configsettings.sftp_rbd_queue_url,
                    MessageBody=json.dumps(
                        {
                            'run_id': run_id
                        }),
                )
            self.logger.info(f"{run_id} is pushed into queue for sftp")