import uuid
from datetime import datetime

class AuditScheduledJob:

    def __init__(self, mongo_conn, logger, module_name, start_time, database_name='qp_dw_rmalloc'):
        """
        Initialize audit job tracker with MongoDB connection.
        
        Args:
            mongo_conn: MongoDB client connection
            logger: Logger instance
            module_name: Name of the module being audited
            start_time: Job start time (datetime object or string)
            database_name: MongoDB database name (default from config)
        """
        self.mongo_conn = mongo_conn
        self.db = mongo_conn[database_name]
        self.collection = self.db['scheduled_job_run_audit']
        self.logger = logger
        self.module_name = module_name
        self.start_time = start_time
    
    
    def add_scheduled_job_audit(self, end_time):
        """
        Insert audit record for scheduled job run.
        
        Args:
            end_time: Job end time (datetime object or string)
        """
        try:
            uuid_code = str(uuid.uuid4())
            
            # Ensure dates are datetime objects
            if isinstance(self.start_time, str):
                start_time_dt = datetime.fromisoformat(self.start_time)
            else:
                start_time_dt = self.start_time
            
            if isinstance(end_time, str):
                end_time_dt = datetime.fromisoformat(end_time)
            else:
                end_time_dt = end_time
            
            # Create document
            audit_document = {
                'uuid': uuid_code,
                'module_name': self.module_name,
                'start_time': start_time_dt,
                'end_time': end_time_dt,
                'created_at': datetime.now()  # Optional: track when record was created
            }
            
            # Insert into MongoDB
            result = self.collection.insert_one(audit_document)
            
            self.logger.info(
                f"Schedule Job for module '{self.module_name}' inserted in scheduled_job_run_audit "
                f"(ID: {result.inserted_id}, UUID: {uuid_code})"
            )
            
            return uuid_code
            
        except Exception as e:
            self.logger.error(f"Error inserting audit record for module '{self.module_name}': {e}")
            raise