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
        self.mongoconn = params['mongoconn']  # Single MongoDB connection
        self.cache_client = params['cache_client']
        
        # Assuming your MongoDB database and collection names
        self.db = self.mongoconn["qp_dw_rmalloc"]  # or self.mongoconn['your_db_name']

    def run(self):
        # MongoDB query to get documents where is_run_completed is '0'
        collection = self.db['allocation_run_audit']
        cursor = collection.find(
            {'is_run_completed': '0'},
            {'run_id': 1, 'update_navitaire_method': 1, 'type': 1, '_id': 0}
        )
        
        # Convert to list for iteration
        rows = list(cursor)
        
        for row in rows:
            run_id = row["run_id"]
            audit_type = row["type"]
            self.checkIfRunCompleted(run_id, audit_type)

    def checkIfRunCompleted(self, run_id, audit_type):
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
            # Update the document to mark run as completed
            collection = self.db['allocation_run_audit']
            
            if audit_type == 'Adhoc':
                self.logger.info(f"Adhoc run for run_id: {run_id}")
                self.add_available_slot()
            
            # MongoDB update operation
            collection.update_one(
                {'run_id': run_id},
                {'$set': {'is_run_completed': '1'}}
            )
            
            self.logger.info(
                f"Updated value for {run_id} since run is completed")

    def add_available_slot(self):
        """Add slot when run is completed & run_id is Adhoc"""
        
        collection = self.db['adhoc_run_queue']
        
        # Get current available slot count
        result = collection.find_one({}, {'available': 1, '_id': 0})
        available_slot = result['available'] if result else 0
        
        add_slot = available_slot + 1
        
        # Update available slot (assuming single document in adhoc_run_queue)
        # If you have multiple documents, add a filter condition
        collection.update_one(
            {},  # Add filter here if needed, e.g., {'_id': specific_id}
            {'$set': {'available': add_slot}}
        )
        
        self.logger.info(f"Slot Available after Adhoc run is completed: {add_slot}")