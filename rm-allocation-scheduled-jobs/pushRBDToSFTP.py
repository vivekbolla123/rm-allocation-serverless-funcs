import json
import configsettings
import pandas as pd

class PushRBDtoSFTP:

    def __init__(self, sqs, logger):
        self.sqs = sqs
        self.logger = logger

    def pushRBDToSFTP(self, conn, run_id):
        query2 = f"SELECT COUNT(*) as total FROM run_flight_date_audit WHERE runId = '{run_id}' AND (b2cstatus <> 'Ready' OR b2bstatus <> 'Ready')"
        count_ready = pd.read_sql(query2, conn)["total"][0]
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