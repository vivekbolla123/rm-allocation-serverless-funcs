import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class TimeLogger:
    def __init__(self, mongo_db, constutils, label="rm-allocation-fetch-inputs"):
        self.mongo_db = mongo_db
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
            collection = self.mongo_db['time_audit']
            documents_to_insert = []
            
            for item in self.run_log:
                if item["total_time"] >= self.constutils:
                    documents_to_insert.append({
                        'run_id': item["run_id"],
                        'start_time': item["start_time"],
                        'end_time': item["end_time"],
                        'label': item["label"],
                        'func': item["func"]
                    })
            
            if documents_to_insert:
                collection.insert_many(documents_to_insert)
                
            logger.info(f"Successfully logged {len(documents_to_insert)} entries to time_audit collection")
            self.run_log.clear()
        except Exception as e:
            logger.error(f"Error logging to time_audit collection: {e}")
            
    def update_run_id(self, run_id):
        self.run_id = run_id