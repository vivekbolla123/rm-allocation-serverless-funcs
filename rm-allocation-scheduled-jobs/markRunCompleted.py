import pandas as pd


class MarkRunCompleted:

    def __init__(self, logger, rdconn, wrconn):
        self.logger = logger
        self.rdconn = rdconn
        self.wrconn = wrconn

    def run(self):
        query = "SELECT run_id, update_navitaire_method FROM allocation_run_audit WHERE is_run_completed = '0' "
        rows = pd.read_sql(query, self.rdconn)
        for index, row in rows.iterrows():
            run_id = row["run_id"]
            self.checkIfRunCompleted(run_id, row["update_navitaire_method"])

    def checkIfRunCompleted(self, run_id, method):
        query = f"SELECT COUNT(*) as total FROM run_flight_date_audit WHERE runId = '{run_id}' AND (b2cstatus <> 'Done' OR b2bstatus <> 'Done')"
        
        count_ready = pd.read_sql(query, self.rdconn)["total"][0]
        self.logger.info(f"no of records are not ready  {count_ready}")
        if count_ready == 0:
            # Update the value to 1 if all flight are ready
            update_query = "UPDATE allocation_run_audit SET is_run_completed = %s WHERE run_id = %s "
            self.wrconn.execute(update_query, ('1', run_id))
            self.logger.info(
                f"Updated value for {run_id} since run is completed")
