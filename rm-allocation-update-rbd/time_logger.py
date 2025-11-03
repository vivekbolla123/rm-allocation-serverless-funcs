import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class TimeLogger:
    def __init__(self, db, constutils, label="rm-allocation-update-rbd"):
        self.db = db
        self.time_audit_col = db['time_audit']
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
            'start_time': self.latest_start_time,
            'end_time': end_time,
            'run_id': self.run_id,
            'label': self.label,
            'func': func_name,
            'total_time': total_time.total_seconds()
        })
        self.latest_start_time = None

    def send_time_log_db(self):
        try:
            # Filter logs that meet the threshold and prepare documents
            documents_to_insert = []
            for item in self.run_log:
                if item["total_time"] >= self.constutils:
                    document = {
                        "run_id": item["run_id"],
                        "start_time": item["start_time"],
                        "end_time": item["end_time"],
                        "label": item["label"],
                        "func": item["func"],
                        "total_time": item["total_time"]
                    }
                    documents_to_insert.append(document)
            
            # Bulk insert if there are documents to insert
            if documents_to_insert:
                self.time_audit_col.insert_many(documents_to_insert)
                logger.info(f"Successfully logged {len(documents_to_insert)} entries to time_audit collection")
            else:
                logger.info("No entries met the threshold for logging")
            
            self.run_log.clear()
        except Exception as e:
            logger.error(f"Error logging to time_audit collection: {e}")
            
    def update_run_id(self, run_id):
        self.run_id = run_id