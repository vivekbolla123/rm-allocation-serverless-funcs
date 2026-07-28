import pandas as pd
from sqlalchemy import text
from datetime import datetime
from pymongo import UpdateOne

from common.utils.logger import logger
from common.constants.constants import UPLOAD_QUEUE_STATUS_IN_PROGRESS, DATABASE_ACCESS_ERROR_MESSAGE, UPLOAD_INFO_NOT_FOUND_ERROR_MESSAGE
from common.config.db_config import get_mongo_db, get_mongo_master_db, get_db_for_collection
from common.constants.query_constants import *
from common.constants.constants import RM_PARAMETER_VALUES_COLLECTION

class DbHelper:
    def add_errors_to_table(self, errors_list, obj_id):
        """Add errors to the inputs_file_upload collection"""
        if not errors_list:
            return
        
        # Join errors with comma separator
        errors_string = ", ".join(errors_list)
        
        try:
            with get_mongo_db() as db:
                db.inputs_file_upload.update_one(
                    {"obj_id": obj_id},
                    {"$set": {"errors": errors_string}}
                )
            
        except Exception as e:
            logger.error(f"{DATABASE_ACCESS_ERROR_MESSAGE}: {e}")
    
    def update_status_in_progress(self, obj_id):
        try:
            with get_mongo_db() as db:
                db.inputs_file_upload.update_one(
                    {"obj_id": obj_id},
                    {"$set": {
                        "status": UPLOAD_QUEUE_STATUS_IN_PROGRESS,
                        "inprogress_timestamp": datetime.now()
                    }}
                )
            
        except Exception as e:
            logger.error(f"Error updating status: {e}")

    def get_column_headers(self, table_name):
        """Get column headers for a table"""
        list_obj = [None, None]
        
        try:
            with get_mongo_db() as db:
                result = db.config_column_names.find_one(
                    {"table_name": table_name},
                    {"column_names": 1, "id_column_names": 1, "_id": 0}
                )
                
                if result:
                    list_obj = [
                        result.get("column_names"),
                        result.get("id_column_names")
                    ]
                    
        except Exception as e:
            logger.error(f"{DATABASE_ACCESS_ERROR_MESSAGE}: {e}")
        
        if list_obj[0] is None:
            logger.warning(f"Column headers not found for table: {table_name}. Returning empty string.")
            return ""
        
        return list_obj
    
    def copy_from_temp_to_main_table(self, table_name, temp_table_name, data):
        """
        Copy data from temporary collection to main collection.
        """
        try:
            with get_mongo_db() as db:
                # Get all documents from temp collection (temp is always in default db)
                temp_docs = list(db[temp_table_name].find())
                
                if temp_docs:
                    # Remove _id field to allow MongoDB to generate new ones
                    for doc in temp_docs:
                        if '_id' in doc:
                            del doc['_id']
                    
                    # Insert into main collection (may be in master db)
                    with get_db_for_collection(table_name)() as target_db:
                        target_db[table_name].insert_many(temp_docs)
                    logger.info(f"Inserted {len(temp_docs)} records into {table_name}.")
                else:
                    logger.info(f"No records to copy from {temp_table_name} to {table_name}.")
        
        except Exception as e:
            logger.error(f"Failed to copy data from {temp_table_name} to {table_name}: {e}")

    def insert_statement(self, csv_df, column_headers, table_name):
        try:
            if len(csv_df.columns) != len(column_headers):
                raise ValueError(
                    f"Mismatch between CSV columns ({len(csv_df.columns)}) "
                    f"and provided headers ({len(column_headers)})"
                )

            csv_df.columns = column_headers

            # Convert DataFrame to list of dictionaries
            records = csv_df.to_dict('records')
            
            # Handle NaN values - convert to None
            for record in records:
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None

            batch_size = 10000

            with get_db_for_collection(table_name)() as db:
                # Drop the collection if it exists (equivalent to replace)
                db[table_name].drop()
                
                # Insert in batches
                for i in range(0, len(records), batch_size):
                    batch = records[i:i + batch_size]
                    db[table_name].insert_many(batch)
                    logger.info(f"Inserted batch {i//batch_size + 1} with {len(batch)} records into {table_name}")

        except Exception as e:
            logger.error(f"Failed to execute MongoDB batch insert: {e}")
            raise

    def delete_values(self, table_name, temp_table_name, id_columns):
        try:
            with get_mongo_db() as db:
                if id_columns is None:
                    # Delete all documents from the target collection (may be in master db)
                    with get_db_for_collection(table_name)() as target_db:
                        result = target_db[table_name].delete_many({})
                    logger.info(f"Deleted {result.deleted_count} records from {table_name}.")
                else:
                    # Get the ID values from temp table (always in default db)
                    id_column_list = [col.strip() for col in id_columns.split(',')]
                    
                    # Get all documents from temp table
                    temp_docs = list(db[temp_table_name].find({}, {col: 1 for col in id_column_list}))
                    
                    if temp_docs:
                        with get_db_for_collection(table_name)() as target_db:
                            # Build delete criteria
                            if len(id_column_list) == 1:
                                # Single column - use $in
                                id_col = id_column_list[0]
                                id_values = [doc.get(id_col) for doc in temp_docs if doc.get(id_col) is not None]
                                result = target_db[table_name].delete_many({id_col: {"$in": id_values}})
                            else:
                                # Multiple columns - use $or with multiple conditions
                                delete_conditions = []
                                for doc in temp_docs:
                                    condition = {col: doc.get(col) for col in id_column_list if doc.get(col) is not None}
                                    if condition:
                                        delete_conditions.append(condition)
                                
                                if delete_conditions:
                                    result = target_db[table_name].delete_many({"$or": delete_conditions})
                                else:
                                    result = type('obj', (object,), {'deleted_count': 0})()
                        
                        logger.info(f"Deleted {result.deleted_count} records from {table_name}.")
                    else:
                        logger.info(f"No records to delete from {table_name}.")

        except Exception as e:
            logger.error(f"Error deleting values from table '{table_name}': {e}")
            raise
        
    def update_audit_statement(self, table_name: str, uuid: str):
        """
        Updates audit records and maintains current version tracking.
        """
        try:
            with get_mongo_db() as db:
                # Count current versions for this table
                count = db.current_table_version.count_documents({"table_name": table_name})

                # Update or insert current version
                if count == 1:
                    db.current_table_version.update_one(
                        {"table_name": table_name},
                        {"$set": {"curr_version": uuid}}
                    )
                else:
                    db.current_table_version.insert_one({
                        "curr_version": uuid,
                        "table_name": table_name
                    })

        except Exception as e:
            logger.error(f"Error updating audit for table '{table_name}': {e}")
            raise

    def delete_table(self, table_name: str):
        """
        Drops the given collection from the database.
        """
        try:
            with get_db_for_collection(table_name)() as db:
                db[table_name].drop()
                logger.info(f"Dropped collection '{table_name}'")

        except Exception as e:
            logger.error(f"Failed to delete collection '{table_name}': {e}")
            raise
        
    def get_upload_info(self, obj_id, list_of_errors, error_occurred):
        try:
            with get_mongo_db() as db:
                result = db.inputs_file_upload.find_one(
                    {"obj_id": obj_id},
                    {"table_name": 1, "role": 1, "username": 1, "init_timestamp": 1, "_id": 0}
                )
                
                upload_data = {}

                if result:
                    start_time = result.get("init_timestamp")
                    if isinstance(start_time, str):
                        start_time = datetime.fromisoformat(start_time)
                    
                    upload_data = {
                        "table": result.get("table_name"),
                        "role": result.get("role"),
                        "user_name": result.get("username"),
                        "start_time": start_time
                    }
                
                else:
                    error_occurred = 1
                    list_of_errors.append(UPLOAD_INFO_NOT_FOUND_ERROR_MESSAGE)
                    self.add_errors_to_table(list_of_errors, obj_id)
                    logger.error(UPLOAD_INFO_NOT_FOUND_ERROR_MESSAGE)

                return upload_data, list_of_errors, error_occurred
                    
        except Exception as e:
            error_occurred = 1
            list_of_errors.append(DATABASE_ACCESS_ERROR_MESSAGE)
            self.add_errors_to_table(list_of_errors, obj_id)
            logger.error(f"{DATABASE_ACCESS_ERROR_MESSAGE}: {e}")
            return None, list_of_errors, error_occurred
        
    def update_final_upload_info(self, status, end_time, num_of_rows, formatted_duration, obj_id, list_of_errors):
        try:
            update_data = {
                "status": status,
                "done_timestamp": end_time,
                "total_rows": num_of_rows,
                "total_time_taken": formatted_duration
            }
            
            # If status is FAILED and there are errors, add them to the update
            if status == "FAILED" and list_of_errors:
                errors_string = ", ".join(list_of_errors)
                update_data["errors"] = errors_string
            
            with get_mongo_db() as db:
                db.inputs_file_upload.update_one(
                    {"obj_id": obj_id},
                    {"$set": update_data}
                )
            
        except Exception as e:
            list_of_errors.append(DATABASE_ACCESS_ERROR_MESSAGE)
            logger.error(f"{DATABASE_ACCESS_ERROR_MESSAGE}: {e}")
        
        return list_of_errors
    
    def upsert_data(self, data_df, table_name, id_columns):
        """
        Upsert data into MongoDB collection based on ID columns.
        Updates existing documents and inserts new ones.
        
        Args:
            data_df: DataFrame with data to upsert
            table_name: Name of the collection
            id_columns: Comma-separated string of ID column names
        """
        try:
            # Convert DataFrame to list of dictionaries
            records = data_df.to_dict('records')
            
            # Handle NaN values
            for record in records:
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None
            
            # Parse ID columns
            id_column_list = [col.strip() for col in id_columns.split(',')]
            
            bulk_operations = []
            for record in records:
                # Build filter based on ID columns
                filter_dict = {col: record[col] for col in id_column_list if col in record}
                
                if filter_dict:
                    # Create upsert operation
                    bulk_operations.append(
                        UpdateOne(
                            filter_dict,
                            {"$set": record},
                            upsert=True
                        )
                    )
            
            if bulk_operations:
                with get_db_for_collection(table_name)() as db:
                    result = db[table_name].bulk_write(bulk_operations, ordered=False)
                    logger.info(
                        f"Upserted to {table_name}: "
                        f"{result.upserted_count} inserted, "
                        f"{result.modified_count} updated"
                    )
            else:
                logger.warning(f"No valid records to upsert into {table_name}")
                
        except Exception as e:
            logger.error(f"Failed to upsert data into {table_name}: {e}")
            raise

    def replace_tariff_sheet(self, tariff_doc, user_name):
        """
        Store the live tariff sheet as a single active document.
        Upserts on status='active' so only one active sheet exists at a time.
        """
        try:
            with get_mongo_db() as db:
                current_count = db.live_tariff_sheet.count_documents({'status': 'active'})
                new_version = (current_count or 0) + 1

                db.live_tariff_sheet.update_one(
                    {'status': 'active'},
                    {'$set': {
                        'status': 'active',
                        'fare_data': tariff_doc['fare_data'],
                        'uploaded_at': datetime.now(),
                        'uploaded_by': user_name,
                        'version': new_version
                    }},
                    upsert=True
                )
                logger.info(f"Live tariff sheet updated — version {new_version}, {len(tariff_doc['fare_data'])} sectors")

        except Exception as e:
            logger.error(f"Failed to replace live tariff sheet: {e}")
            raise

    def get_parameter_value(self, parameter_key):
        """
        Fetch a parameter value from the rm_parameter_values collection by key.
        
        Args:
            parameter_key: The parameterKey to look up (e.g. 'SINGLE_RBD_MAP')
            
        Returns:
            The parameterValue string if found, None otherwise.
        """
        try:
            with get_db_for_collection(RM_PARAMETER_VALUES_COLLECTION)() as db:
                result = db[RM_PARAMETER_VALUES_COLLECTION].find_one(
                    {"parameterKey": parameter_key},
                    {"parameterValue": 1, "_id": 0}
                )
                if result:
                    return result.get("parameterValue")
                else:
                    logger.warning(f"Parameter '{parameter_key}' not found in {RM_PARAMETER_VALUES_COLLECTION}")
                    return None
        except Exception as e:
            logger.error(f"Error fetching parameter '{parameter_key}': {e}")
            return None

    def replace_collection_data(self, data_df, table_name):
        """
        Replace all data in a MongoDB collection.
        Deletes all existing documents and inserts new ones.
        
        Args:
            data_df: DataFrame with data to insert
            table_name: Name of the collection
        """
        try:
            # Convert DataFrame to list of dictionaries
            records = data_df.to_dict('records')
            
            # Handle NaN values
            for record in records:
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None

            batch_size = 10000

            with get_db_for_collection(table_name)() as db:
                # Delete all existing documents
                delete_result = db[table_name].delete_many({})
                logger.info(f"Deleted {delete_result.deleted_count} existing records from {table_name}")
                
                # Insert in batches
                total_inserted = 0
                for i in range(0, len(records), batch_size):
                    batch = records[i:i + batch_size]
                    db[table_name].insert_many(batch)
                    total_inserted += len(batch)
                    logger.info(f"Inserted batch {i//batch_size + 1} with {len(batch)} records into {table_name}")
                
                logger.info(f"Total inserted into {table_name}: {total_inserted} records")

        except Exception as e:
            logger.error(f"Failed to replace collection data in {table_name}: {e}")
            raise