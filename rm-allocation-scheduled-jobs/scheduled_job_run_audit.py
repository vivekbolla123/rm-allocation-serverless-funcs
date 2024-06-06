import uuid

class AuditScheduledJob:

    def __init__(self, rm_wr_conn, logger, module_name, start_time):
        self.rm_wr_conn = rm_wr_conn
        self.logger = logger
        self.module_name = module_name
        self.start_time = start_time
    
    
    def add_scheduled_job_audit(self, end_time):
        uuid_code = str(uuid.uuid4())

        insert_query="""INSERT INTO scheduled_job_run_audit (uuid, module_name, start_time, end_time) 
                        VALUES (%s, %s, %s, %s)"""
        
        self.rm_wr_conn.execute(insert_query, (uuid_code, self.module_name, self.start_time, end_time))
        self.logger.info(f"Schedule Job for module '{self.module_name}' inserted in scheduled_job_run_audit")



    