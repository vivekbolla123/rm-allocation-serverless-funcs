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
from common.processors.saver_processor import format_saver_file
from common.processors.product_upsell_processor import validate_product_upsell
from common.processors.market_list_processor import validate_market_list


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

    def normalize_dataframe_columns(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """
        Normalize uploaded CSV column names to canonical names from config_column_names.
        Matching is case-insensitive and whitespace/underscore-insensitive.

        Args:
            df: Raw DataFrame from CSV upload
            table_name: Name of the table/collection

        Returns:
            DataFrame with canonical column names in canonical order

        Raises:
            ValueError: If any canonical column cannot be matched in the upload
        """
        result = self.db_helper.get_column_headers(table_name)

        if not result or not result[0]:
            logger.warning(
                f"No config_column_names entry for '{table_name}', "
                "applying basic strip/lowercase only."
            )
            df.columns = df.columns.str.strip().str.lower()
            return df

        columns = result[0]
        canonical_cols = [col.strip() for col in columns.split(COMMA)]

        # Build lookup: stripped/lowercased/no-separators -> canonical name
        # e.g. "Rule Nbr", "rule_nbr", "RULE NBR", "RuleNbr" all -> "rulenbr" -> "rule_nbr"
        canonical_lookup = {
            col.lower().replace(" ", "").replace("_", ""): col
            for col in canonical_cols
        }

        rename_map = {}
        unmatched = []

        for uploaded_col in df.columns:
            normalized_key = uploaded_col.strip().lower().replace(" ", "").replace("_", "")
            if normalized_key in canonical_lookup:
                rename_map[uploaded_col] = canonical_lookup[normalized_key]
            else:
                unmatched.append(uploaded_col)

        matched_canonical = set(rename_map.values())
        missing_canonical = [col for col in canonical_cols if col not in matched_canonical]

        if missing_canonical:
            raise ValueError(
                f"Uploaded file for '{table_name}' is missing required columns: {missing_canonical}. "
                f"Unrecognized uploaded columns: {unmatched}"
            )

        df = df.rename(columns=rename_map)
        # Return only canonical columns in canonical order, drop any extras
        return df[canonical_cols]


    def insert_file_data(self, data, table_name, user_name, use_upsert=True):
        uuid = self.obj_id

        try:
            _, id_columns = self.db_helper.get_column_headers(table_name)

            if use_upsert and id_columns:
                self.db_helper.upsert_data(data, table_name, id_columns)
            else:
                self.db_helper.replace_collection_data(data, table_name)

            self.db_helper.update_audit_statement(table_name, uuid)

        except Exception as e:
            logger.error(f"Error in insert_file_data for {table_name}: {e}")
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

            if table == SALE_DATES_TABLE:
                try:
                    normalized_df = self.normalize_dataframe_columns(csv_df, SALE_DATES_TABLE)
                    sales_df, discounts_df = pivot_sale_fares(normalized_df)
                    self.insert_file_data(sales_df, SALE_DATES_TABLE, user_name, use_upsert=True)
                    self.insert_file_data(discounts_df, SALE_DISCOUNTS_TABLE, user_name, use_upsert=True)

                    self.s3_helper.store_file(file_content, SALE_DATES_TABLE, self.obj_id)
                    self.s3_helper.store_file(file_content, SALE_DISCOUNTS_TABLE, self.obj_id)
                except Exception as e:
                    self.list_of_errors.append("Error Uploading Sale_fare_discount to DB")
                    raise RuntimeError(f"Error processing Sale_fare_discount content: {str(e)}")
            elif table == SAVER_TABLE:
                try:
                    normalized_df = self.normalize_dataframe_columns(csv_df, SAVER_TABLE)
                    saver_df = format_saver_file(normalized_df)
                    self.insert_file_data(saver_df, SAVER_TABLE, user_name, use_upsert=True)

                    self.s3_helper.store_file(file_content, SAVER_TABLE, self.obj_id)
                except Exception as e:
                    self.list_of_errors.append("Error Uploading Saver to DB")
                    raise RuntimeError(f"Error processing Saver content: {str(e)}")
            elif table == PRODUCT_UPSELL_TABLE:
                try:
                    normalized_df = self.normalize_dataframe_columns(csv_df, PRODUCT_UPSELL_TABLE)
                    product_upsell_df = validate_product_upsell(normalized_df)
                    self.insert_file_data(product_upsell_df, PRODUCT_UPSELL_TABLE, user_name, use_upsert=True)

                    self.s3_helper.store_file(file_content, PRODUCT_UPSELL_TABLE, self.obj_id)
                except ValueError as ve:
                    self.list_of_errors.append(f"Product Upsell Validation Failed: {str(ve)}")
                    raise RuntimeError(f"Product Upsell validation error: {str(ve)}")
                except Exception as e:
                    self.list_of_errors.append("Error Uploading Product Upsell to DB")
                    raise RuntimeError(f"Error processing Product Upsell content: {str(e)}")

            elif table in MARKET_LIST_TABLES:
                try:
                    normalized_df = self.normalize_dataframe_columns(csv_df, table)
                    market_list_df = validate_market_list(normalized_df)
                    self.insert_file_data(market_list_df, table, user_name, use_upsert=True)
                    self.s3_helper.store_file(file_content, table, self.obj_id)
                except ValueError as ve:
                    self.list_of_errors.append(f"Market List Validation Failed: {str(ve)}")
                    raise RuntimeError(f"Market List validation error: {str(ve)}")
                except Exception as e:
                    self.list_of_errors.append(f"Error Uploading {table} to DB")
                    raise RuntimeError(f"Error processing Market List content: {str(e)}")
            else:
                try:
                    normalized_df = self.normalize_dataframe_columns(csv_df, table)
                    use_upsert = table not in FULL_REPLACE_TABLES
                    self.insert_file_data(normalized_df, table, user_name, use_upsert=use_upsert)
                    self.s3_helper.store_file(file_content, table, self.obj_id)
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
            # Always add errors to database when an exception occurs
            self.db_helper.add_errors_to_table(self.list_of_errors, self.obj_id)
            logger.error(f"[{self.obj_id}] Error: {e}")

        if self.error_occurred == 0:
            status = UPLOAD_QUEUE_STATUS_DONE
        else:
            status = UPLOAD_QUEUE_STATUS_FAILED
            if self.list_of_errors:
                self.db_helper.add_errors_to_table(self.list_of_errors, self.obj_id)

        end_time = datetime.now()
        duration = calculate_duration(start_time, end_time)
        logger.info(f"[{self.obj_id}] Status: {status}")

        self.list_of_errors = self.db_helper.update_final_upload_info(
            status, end_time, self.num_of_rows, duration, self.obj_id, self.list_of_errors
        )

        if self.list_of_errors and status == UPLOAD_QUEUE_STATUS_FAILED:
            self.db_helper.add_errors_to_table(self.list_of_errors, self.obj_id)

        local_test_dir = "locals"
        s3_path_len = len(S3_FILE_PATH_NAME)
        if final_key[s3_path_len:s3_path_len + len(local_test_dir)].lower() != local_test_dir:
            self.s3_helper.delete_object(final_key)

        return {
            'obj_id': self.obj_id,
            'status': status,
            'error_occurred': self.error_occurred,
            'errors': self.list_of_errors.copy()
        }