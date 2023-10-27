import json
import pandas as pd
import configsettings

class RunConnections:

    def __init__(self, sqs, logger):
        self.sqs = sqs
        self.logger = logger

    def runConnentions(self, conn, run_id, dtd_start, dtd_end, is_connections_required):
        if(is_connections_required == 1):
                # Check run_flight_date_audit table if all are done for the current run_id
            query1 = f"SELECT COUNT(*) as total FROM run_flight_date_audit WHERE runId = '{run_id}' AND status <> 'Done'"
            count_done = pd.read_sql(query1, conn)["total"][0]
            if count_done == 0:
                    # All flights are done, push run_id to the connection_queue
                self.sqs.send_message(
                        QueueUrl=configsettings.connection_queue_url,
                        MessageBody=json.dumps(
                            {
                                'run_id': run_id, 
                                'start_interval': dtd_start, 
                                'end_interval': dtd_end, 
                                'run_type': "Scheduled_Connections", 
                                "market_list_table_name": "market_list_connections"
                            }),
                    )