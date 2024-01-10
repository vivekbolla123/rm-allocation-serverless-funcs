import json
import pandas as pd
import configsettings

KEY_B2B_PENDING_COUNT = 'b2b_pending_count_'
KEY_B2C_PENDING_COUNT = 'b2c_pending_count_'
KEY_B2B_READY_COUNT = 'b2b_ready_count_'
KEY_B2C_READY_COUNT = 'b2c_ready_count_'
KEY_B2B_DONE_COUNT = 'b2b_done_count_'
KEY_B2C_DONE_COUNT = 'b2c_done_count_'

class RunConnections:

    def __init__(self, logger, params, overrides, context):
        self.logger = logger
        self.sqs = params['sqs']
        self.rm_rd_conn = params['rdconn']
        self.cache_client = params['cache_client']

    def run(self):
        # Fetch runIds from allocation_run_audit table where is_connections_started is null
        query = "SELECT run_id,dtd_start,dtd_end,is_connections_required FROM allocation_run_audit WHERE is_connections_started IS NULL"
        rows = pd.read_sql(query, self.rm_rd_conn)
        for index,row in rows.iterrows():
            run_id = row["run_id"] 
            dtd_start = row["dtd_start"] 
            dtd_end = row["dtd_end"] 
            is_connections_required = row["is_connections_required"] 
            self.runConnentions(run_id, dtd_start, dtd_end, is_connections_required)

    def runConnentions(self, run_id, dtd_start, dtd_end, is_connections_required):
        if(is_connections_required == 1):
                # Check run_flight_date_audit table if all are done for the current run_id
            # query1 = f"SELECT COUNT(*) as total FROM run_flight_date_audit WHERE runId = '{run_id}' AND (b2cstatus <> 'Done' OR b2bstatus <> 'Done')"
            # count_done = pd.read_sql(query1, self.rm_rd_conn)["total"][0]
            # is_complete = count_done == 0

            b2bPendingCount = self.cache_client.get(KEY_B2B_PENDING_COUNT + run_id)
            b2cPendingCount = self.cache_client.get(KEY_B2C_PENDING_COUNT + run_id)
            b2bDoneCount = self.cache_client.get(KEY_B2B_DONE_COUNT + run_id)
            b2cDoneCount = self.cache_client.get(KEY_B2C_DONE_COUNT + run_id)
            # is_complete = b2bDoneCount == b2bPendingCount and b2cDoneCount == b2cPendingCount
            is_complete = int(b2bDoneCount) + int(b2cDoneCount) >= int(b2bPendingCount) + int(b2cPendingCount)

            if is_complete:
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