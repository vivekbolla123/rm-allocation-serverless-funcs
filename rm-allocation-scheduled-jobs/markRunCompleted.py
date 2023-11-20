import pandas as pd


class MarkRunCompleted:

    def __init__(self, logger, rdconn, wrconn):
        self.logger = logger
        self.rdconn = rdconn
        self.wrconn = wrconn

    def run(self):
        query = "SELECT run_id, update_navitaire_method, type FROM allocation_run_audit WHERE is_run_completed = '0' "
        rows = pd.read_sql(query, self.rdconn)
        for index, row in rows.iterrows():
            run_id = row["run_id"]
            audit_type = row["type"]
            self.checkIfRunCompleted(run_id, row["update_navitaire_method"], audit_type)

    def checkIfRunCompleted(self, run_id, method, audit_type):
        query = f"SELECT COUNT(*) as total FROM run_flight_date_audit WHERE runId = '{run_id}' AND (b2cstatus <> 'Done' OR b2bstatus <> 'Done')"
        
        count_ready = pd.read_sql(query, self.rdconn)["total"][0]
        self.logger.info(f"no of records are not ready  {count_ready}")
        if count_ready == 0:
            # Update the value to 1 if all flight are ready
            if audit_type == 'Adhoc':
                self.logger.info(f"Adhoc run for run_id: {run_id}")
                self.add_available_slot()
            
            update_query = "UPDATE allocation_run_audit SET is_run_completed = %s WHERE run_id = %s "
            self.wrconn.execute(update_query, ('1', run_id))
            self.logger.info(
                f"Updated value for {run_id} since run is completed")

    def add_available_slot(self):
        """Add slot when run is completed & run_id is Adhoc"""

        select_query = "SELECT available FROM adhoc_run_queue"
        available_slot = self.rdconn.execute(select_query).fetchone()[0]

        add_slot = available_slot + 1
    
        update_query = f"UPDATE adhoc_run_queue SET available={add_slot}"
        self.wrconn.execute(update_query)
        
        self.logger.info(f"Slot Avaiable after Adhoc run is completed: {add_slot}")
