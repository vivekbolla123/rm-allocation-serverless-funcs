import pandas as pd

KEY_B2B_PENDING_COUNT = 'b2b_pending_count_'
KEY_B2C_PENDING_COUNT = 'b2c_pending_count_'
KEY_B2B_READY_COUNT = 'b2b_ready_count_'
KEY_B2C_READY_COUNT = 'b2c_ready_count_'
KEY_B2B_DONE_COUNT = 'b2b_done_count_'
KEY_B2C_DONE_COUNT = 'b2c_done_count_'

class MarkRunCompleted:

    def __init__(self, logger, params, overrides, context):
        self.logger = logger
        self.rm_rd_conn = params['rdconn']
        self.rm_wr_conn = params['wrconn']
        self.cache_client = params['cache_client']

    def run(self):
        query = "SELECT run_id, update_navitaire_method, type FROM allocation_run_audit WHERE is_run_completed = '0' "
        rows = pd.read_sql(query, self.rm_rd_conn)
        for index, row in rows.iterrows():
            run_id = row["run_id"]
            audit_type = row["type"]
            self.checkIfRunCompleted(run_id, audit_type)

    def checkIfRunCompleted(self, run_id, audit_type):
        # query = f"SELECT COUNT(*) as total FROM run_flight_date_audit WHERE runId = '{run_id}' AND (b2cstatus <> 'Done' OR b2bstatus <> 'Done')"
        
        # count_ready = pd.read_sql(query, self.rm_rd_conn)["total"][0]
        # is_complete = count_ready == 0
        # self.logger.info(f"no of records are not ready  {count_ready}")
        
        b2bPendingCount = self.cache_client.get(KEY_B2B_PENDING_COUNT + run_id)
        b2cPendingCount = self.cache_client.get(KEY_B2C_PENDING_COUNT + run_id)
        b2bDoneCount = self.cache_client.get(KEY_B2B_DONE_COUNT + run_id)
        b2cDoneCount = self.cache_client.get(KEY_B2C_DONE_COUNT + run_id)
        try:
            is_complete = int(b2bDoneCount) + int(b2cDoneCount) >= int(b2bPendingCount) + int(b2cPendingCount)
        except Exception as e:
            self.logger.info(f"Run Id: {run_id} failed due to reason {e}")
            is_complete = False
            
        if is_complete:
            # Update the value to 1 if all flight are ready
            update_query = "UPDATE allocation_run_audit SET is_run_completed = %s WHERE run_id = %s "
            if audit_type == 'Adhoc':
                self.logger.info(f"Adhoc run for run_id: {run_id}")
                self.add_available_slot()
                update_query = "UPDATE allocation_run_audit SET is_run_completed = %s WHERE run_id = %s "
            
            self.rm_wr_conn.execute(update_query, ('1', run_id))
            self.logger.info(
                f"Updated value for {run_id} since run is completed")

    def add_available_slot(self):
        """Add slot when run is completed & run_id is Adhoc"""

        select_query = "SELECT available FROM adhoc_run_queue"
        available_slot = self.rm_rd_conn.execute(select_query).fetchone()[0]

        add_slot = available_slot + 1
    
        update_query = f"UPDATE adhoc_run_queue SET available={add_slot}"
        self.rm_wr_conn.execute(update_query)
        
        self.logger.info(f"Slot Avaiable after Adhoc run is completed: {add_slot}")
