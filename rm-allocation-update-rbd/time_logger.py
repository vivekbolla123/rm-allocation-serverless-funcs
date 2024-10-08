import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class TimeLogger:
    def __init__(self, wrconn, constutils, label="rm-allocation-update-rbd"):
        self.wrconn = wrconn
        self.constutils = constutils
        self.label = label
        self.run_log = []
        self.latest_start_time = None
        self.run_id = None

    def start(self):
        self.latest_start_time = datetime.now()

    def end(self, func_name):
        end_time = datetime.now()
        total_time = end_time - self.latest_start_time
        self.run_log.append({
            'start_time': self.latest_start_time.strftime('%Y-%m-%d %H:%M:%S.%f'),
            'end_time': end_time.strftime('%Y-%m-%d %H:%M:%S.%f'),
            'run_id': self.run_id,
            'label': self.label,
            'func': func_name,
            'total_time': total_time.total_seconds()
        })
        self.latest_start_time = None

    def send_time_log_db(self):

        log_query = "INSERT INTO time_audit (run_id, start_time, end_time, label, func) VALUES (%s, %s, %s, %s, %s)"
        try:
            with self.wrconn.begin() as transaction:
                for item in self.run_log:
                    if item["total_time"] >= self.constutils:
                        transaction.execute(log_query, (
                            item["run_id"],
                            item["start_time"],
                            item["end_time"],
                            item["label"],
                            item["func"]
                        ))
            logger.info(f"Successfully logged {len(self.run_log)} entries to time_audit table")
            self.run_log.clear()
        except Exception as e:
            logger.error(f"Error logging to time_audit table: {e}")
            
    def update_run_id(self, run_id):
        self.run_id=run_id