import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class TimeLogger:
    def __init__(self, mongo_db, constutils, label="rm-allocation-process-inputs(nav-helper)"):
        """
        Args:
            mongo_db: MongoDB database instance (e.g., MongoClient()['your_database'])
            constutils: Minimum time threshold in seconds
            label: Label for the time audit entry
        """
        self.mongo_db = mongo_db
        self.constutils = constutils
        self.label = label
        self.run_log = []
        self.latest_start_time = None
        self.run_id = None
        self.collection = mongo_db['time_audit']  # Collection name

    def start(self):
        self.latest_start_time = datetime.now()

    def end(self, func_name):
        end_time = datetime.now()
        total_time = end_time - self.latest_start_time
        self.run_log.append({
            'start_time': self.latest_start_time,  # Store as datetime object
            'end_time': end_time,  # Store as datetime object
            'run_id': self.run_id,
            'label': self.label,
            'func': func_name,
            'total_time': total_time.total_seconds()
        })
        self.latest_start_time = None

    def send_time_log_db(self):
        """
        Insert time log entries into MongoDB time_audit collection
        """
        try:
            # Filter entries that meet the time threshold
            entries_to_insert = [
                {
                    'run_id': item['run_id'],
                    'start_time': item['start_time'],
                    'end_time': item['end_time'],
                    'label': item['label'],
                    'func': item['func'],
                    'total_time': item['total_time']
                }
                for item in self.run_log
                if item['total_time'] >= self.constutils
            ]
            
            if entries_to_insert:
                # Insert multiple documents at once
                result = self.collection.insert_many(entries_to_insert)
                logger.info(f"Successfully logged {len(result.inserted_ids)} entries to time_audit collection")
            else:
                logger.info("No entries met the time threshold for logging")
            
            self.run_log.clear()
        except Exception as e:
            logger.error(f"Error logging to time_audit collection: {e}")
            
    def update_run_id(self, run_id):
        self.run_id = run_id

