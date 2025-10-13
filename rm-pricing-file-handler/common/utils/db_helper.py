import pandas as pd
from sqlalchemy import text
from datetime import datetime

from common.utils.logger import logger
from common.constants.constants import UPLOAD_QUEUE_STATUS_IN_PROGRESS, DATABASE_ACCESS_ERROR_MESSAGE, UPLOAD_INFO_NOT_FOUND_ERROR_MESSAGE
from common.config.db_config import write_db
from common.constants.query_constants import *

class DbHelper:
    def add_errors_to_table(self, errors_list, obj_id):
        """Add errors to the inputs_file_upload table"""
        if not errors_list:
            return
        
        # Join errors with comma separator
        errors_string = ", ".join(errors_list)
        
        try:
            with write_db() as db:
                db.execute(
                    text(input_file_upload_error_occurred()),
                    {
                        "errors": errors_string,
                        "obj_id": obj_id
                    }
                )
            
        except Exception as e:
            logger.error(f"{DATABASE_ACCESS_ERROR_MESSAGE}: {e}")
    
    def update_status_in_progress(self, obj_id):
        try:
            with write_db() as db:
                db.execute(
                    text(status_in_progress()),
                    {
                        "status": UPLOAD_QUEUE_STATUS_IN_PROGRESS,
                        "inprogress_timestamp": datetime.now(),
                        "obj_id": obj_id
                    }
                )
            
        except Exception as e:
            logger.error(f"Error updating status: {e}")

    def get_column_headers(self, table_name):
        """Get column headers for a table"""
        list_obj = [None, None]
        
        try:
            with write_db() as db:  
                list_obj = db.execute(
                    text(get_column_headers_query()),
                    {"table_name": table_name}
                ).fetchone() 
                    
        except Exception as e:
            logger.error(f"{DATABASE_ACCESS_ERROR_MESSAGE}: {e}")
        
        if list_obj[0] is None:
            logger.warning(f"Column headers not found for table: {table_name}. Returning empty string.")
            return ""
        
        return list_obj
    
    def copy_from_temp_to_main_table(self, table_name, temp_table_name, data):
        """
        Copy data from temporary table to main table.
        INSERT INTO main_table SELECT * FROM temp_table;
        """
        try:
            with write_db() as db:
                db.execute(text(copy_values_table_to_table(table_name, temp_table_name)))

            logger.info(f"Inserted {len(data)} records into {table_name}.")
        
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

            batch_size = 10000

            with write_db() as db:
                csv_df.to_sql(
                    name=table_name,
                    con=db.get_bind(),  # get the engine bound to this session
                    if_exists="replace",  # replace if prev temp table exists
                    index=False,         # don’t insert pandas index as a column
                    chunksize=batch_size,
                    method="multi",      # faster multi-row inserts
                )

        except Exception as e:
            logger.error(f"Failed to execute pandas to_sql batch insert: {e}")
            raise

    def delete_values(self, table_name, temp_table_name, id_columns):
        try:
            with write_db() as db:
                if id_columns is None:
                    result = db.execute(text(delete_all_values_query(table_name)))
                else:
                    result = db.execute(text(delete_key_values_query(table_name, temp_table_name, id_columns)))
                
                logger.info(f"Deleted {result.rowcount} records from {table_name}.")

        except Exception as e:
            logger.error(f"Error updating audit for table '{table_name}': {e}")
            raise
        
    def update_audit_statement(self, table_name: str, uuid: str):
        """
        Updates audit records and maintains current version tracking.
        """
        try:
            with write_db() as db:
                # Count current versions for this table
                result = db.execute(
                    text(GET_CURRENT_VERSION_COUNT()), {"table_name": table_name}
                )
                count = result.scalar() or 0

                # Update or insert current version
                if count == 1:
                    db.execute(
                        text(UPDATE_CURRENT_VERSION()),
                        {"uuid": uuid, "table_name": table_name},
                    )
                else:
                    db.execute(
                        text(INSERT_CURRENT_VERSION()),
                        {"uuid": uuid, "table_name": table_name},
                    )

        except Exception as e:
            logger.error(f"Error updating audit for table '{table_name}': {e}")
            raise

    def delete_table(self, table_name: str):
        """
        Drops the given table from the database using a context manager.
        """
        try:
            with write_db() as db:
                sql = DELETE_TABLE(table_name)
                db.execute(text(sql))

        except Exception as e:
            logger.error(f"Failed to delete table '{table_name}': {e}")
            raise
        
    def get_upload_info(self, obj_id, list_of_errors, error_occurred):
        try:
            with write_db() as db:
                result = db.execute(text(get_input_file_upload()), {"obj_id": obj_id}).fetchone()
                upload_data = {}

                if result:
                    start_time = result[3]
                    if isinstance(start_time, str):
                        start_time = datetime.fromisoformat(start_time)
                    
                    upload_data = {
                        "table": result[0],
                        "role": result[1],
                        "user_name": result[2],
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
            logger.error(DATABASE_ACCESS_ERROR_MESSAGE)
            return None, list_of_errors, error_occurred
        
    def update_final_upload_info(self, status, end_time, num_of_rows, formatted_duration, obj_id, list_of_errors):
        try:
            with write_db() as db:
                db.execute(
                    text(update_upload_time_taken()),
                    {
                        "status": status,
                        "done_timestamp": end_time,
                        "total_rows": num_of_rows,
                        "total_time_taken": formatted_duration,
                        "obj_id": obj_id
                    }
                )
            
        except Exception as e:
            list_of_errors.append(DATABASE_ACCESS_ERROR_MESSAGE)
            logger.error(DATABASE_ACCESS_ERROR_MESSAGE)
        
        return list_of_errors