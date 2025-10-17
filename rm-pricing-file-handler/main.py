from datetime import datetime
import io
import pandas as pd

from common.utils.logger import logger
from common.constants.constants import *
from common.config.config_settings import S3_FILE_PATH_NAME, S3_PREFIX
from common.utils.common_helper import calculate_duration
from common.utils.s3_helper import S3Helper
from common.utils.db_helper import DbHelper
from common.processors.sale_discount_processor import pivot_sale_fares


class FileUploadWorker:
    """Handles individual file upload - each instance processes one file"""
    
    def __init__(self, file_key):
        self.file_key = file_key
        self.error_occurred = 0
        self.list_of_errors = []
        self.s3_helper = S3Helper(logger)
        self.db_helper = DbHelper()
        self.obj_id = None
        self.num_of_rows = 0

    def insert_file_data(self, data, table_name, user_name):
        uuid =  self.obj_id
        uuid_clean = uuid.replace("-", "_")
        temp_table_name = f"{table_name}_temp_{uuid_clean}"
        
        try:
            columns, id_columns = self.db_helper.get_column_headers(table_name)
            headers = [col.strip() for col in columns.split(COMMA)]
            
            self.db_helper.insert_statement(data, headers, temp_table_name)
            self.db_helper.delete_values(table_name, temp_table_name, id_columns)
            self.db_helper.copy_from_temp_to_main_table(table_name, temp_table_name, data)
            self.db_helper.update_audit_statement(table_name, uuid)
            
            # Delete temp table
            self.db_helper.delete_table(temp_table_name)
            
        except Exception as e:
            raise

    def validate_and_insert(self, table, user_name):
        try:
            self.db_helper.update_status_in_progress(self.obj_id)
            
            file_object = self.s3_helper.get_object(self.file_key)
            file_content = file_object.read().decode('utf-8')
            
            reader = io.StringIO(file_content)

            # Count rows (excluding header)
            lines = reader.readlines()
            row_count = len(lines) - 1 if len(lines) > 0 else 0
            self.num_of_rows = row_count

            reader.seek(0)
            csv_df = pd.read_csv(reader)
            
            if table == SALE_DISCOUNTS_TABLE:
                try:
                    sales_df, discounts_df = pivot_sale_fares(csv_df)
                    self.insert_file_data(sales_df, SALE_DATES_TABLE, user_name)
                    self.insert_file_data(discounts_df, SALE_DISCOUNTS_TABLE, user_name)
                except Exception as e:
                    self.list_of_errors.append("Error Uploading Strategy File to DB. Please check file content")
                    raise RuntimeError(f"Error processing strategy file content: {str(e)}")
            else:
                try:
                    self.insert_file_data(csv_df, table, user_name)
                    self.s3_helper.store_file(file_content.encode("utf-8"), table, self.obj_id)
                except Exception as e:
                    raise RuntimeError(f"Error processing file content: {str(e)}")
                    
        except Exception as e:
            self.list_of_errors.append("Failure During Validation")
            logger.error(f"Error in validate_and_insert: {str(e)}")
            raise

    def process_file(self):
        """Main method to process a single file"""
        final_key = self.file_key
        length_of_key = len(final_key)
        self.obj_id = final_key[len(S3_PREFIX) : length_of_key - 4]

        table = None
        role = None
        user_name = None
        start_time = None

        upload_info, self.list_of_errors, self.error_occurred = self.db_helper.get_upload_info(
            self.obj_id, self.list_of_errors, self.error_occurred
        )
        
        if upload_info:
            table = upload_info["table"]
            role = upload_info["role"]
            user_name = upload_info["user_name"]
            start_time = upload_info["start_time"]

        logger.info(f"[{self.obj_id}] Processing table: {table}")

        try:
            self.validate_and_insert(table, user_name)
        except Exception as e:
            self.error_occurred = 1
            if not self.list_of_errors:
                self.list_of_errors.append("Validation has failed while inserting data in table")
                self.db_helper.add_errors_to_table(self.list_of_errors, self.obj_id)
            logger.error(f"[{self.obj_id}] Error: {e}")

        if self.error_occurred == 0:
            status = UPLOAD_QUEUE_STATUS_DONE
        else:
            status = UPLOAD_QUEUE_STATUS_FAILED

        end_time = datetime.now()
        duration = calculate_duration(start_time, end_time)
        logger.info(f"[{self.obj_id}] Status: {status}")

        self.list_of_errors = self.db_helper.update_final_upload_info(
            status, end_time, self.num_of_rows, duration, self.obj_id, self.list_of_errors
        )

        local_test_dir = "locals-pricing"
        s3_path_len = len(S3_FILE_PATH_NAME)
        if final_key[s3_path_len:s3_path_len+len(local_test_dir)].lower() != local_test_dir:
            self.s3_helper.delete_object(final_key)

        return {
            'obj_id': self.obj_id,
            'status': status,
            'error_occurred': self.error_occurred,
            'errors': self.list_of_errors.copy()
        }