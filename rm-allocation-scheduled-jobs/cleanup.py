import configsettings
import pandas as pd
import awswrangler as wr
from datetime import datetime, timedelta

class CleanUpConfig:

    def __init__(self, logger, params, overrides, context):
        self.logger = logger
        # Get MongoDB client
        self.mongo_client = params['mongoconn']

    def run(self):
        """This method gets configurations from cleanup_config collection and performs Delete operation on specified criteria"""
        try:
            # Access the database that contains cleanup_config
            # Assuming it's in QP_DW_RMALLOC, adjust if needed
            mongo_db = self.mongo_client['qp_dw_rmalloc']
            
            # Get all cleanup configurations
            cleanup_configs = list(mongo_db.cleanup_config.find({}))
            
            if cleanup_configs:
                for config in cleanup_configs:
                    archive = False
                    database_name = config.get('schema_name').lower()
                    collection_name = config.get('table_name')
                    time_column = config.get('time_column')
                    max_days = config.get('max_days')
                    is_archive_db = config.get('is_archive_db', False)
                    is_archive_s3 = config.get('is_archive_s3', False)

                    # Archive data before deletion
                    archive = self.archive_before_delete(
                        database_name, 
                        collection_name, 
                        time_column, 
                        max_days, 
                        is_archive_db, 
                        is_archive_s3
                    )
                    
                    if archive:
                        # Calculate cutoff date
                        cutoff_date = (datetime.now() - timedelta(days=max_days))
                        
                        # Get the database and perform delete
                        target_db = self.mongo_client[database_name]
                        result = target_db[collection_name].delete_many({
    "$expr": {
        "$lt": [
            {
                "$cond": {
                    "if": {"$eq": [{"$type": f"${time_column}"}, "date"]},
                    "then": f"${time_column}",
                    "else": {
                        "$dateFromString": {
                            "dateString": {
                                "$replaceOne": {
                                    "input": f"${time_column}",
                                    "find": "T",
                                    "replacement": " "
                                }
                            },
                            "format": "%Y-%m-%d %H:%M:%S"
                        }
                    }
                }
            },
            cutoff_date
        ]
    }
})
                        
                        self.logger.info(f"{result.deleted_count} records deleted from {database_name}.{collection_name}!")
            else:
                self.logger.info("No collection configured for clean up!")
                
        except Exception as err:
            self.logger.error(f"Error in cleanup run: {err}")
            
    def _normalize_to_datetime(self, date_value):
        """Convert date value to datetime object, handling both datetime and string formats."""
        if isinstance(date_value, datetime):
            return date_value
        elif isinstance(date_value, str):
            try:
                # Try common datetime formats
                for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%Y/%m/%d', '%Y-%m-%dT%H:%M:%S']:
                    try:
                        return datetime.strptime(date_value, fmt)
                    except ValueError:
                        continue
                # If none of the formats work, try pandas to_datetime for flexible parsing
                return pd.to_datetime(date_value)
            except Exception as e:
                self.logger.warning(f"Could not parse date string '{date_value}': {e}")
                raise
        else:
            raise ValueError(f"Unsupported date type: {type(date_value)}")

    def archive_before_delete(self, database_name, collection_name, time_column, max_days, is_archive_db, is_archive_s3,date_format='%Y-%m-%d %H:%M:%S'):
        """Archive data to S3 and/or archive database before deletion"""
        try:
            if not is_archive_db and not is_archive_s3:
                self.logger.info("Archive Skipped as per configuration!")
                return True

            # Get the source database
            source_db = self.mongo_client[database_name]
            cutoff_date = datetime.now() - timedelta(days=max_days)
            cutoff_date_str = cutoff_date.strftime(date_format)
            # Find documents to archive
            query = {time_column: {"$lt": cutoff_date_str}}
            documents_to_archive = list(source_db[collection_name].find(query))
            
            if not documents_to_archive:
                self.logger.info("Archive skipped since no data to archive")
                return True

            if is_archive_s3:
                # Get min and max dates
                dates = [doc[time_column] for doc in documents_to_archive if time_column in doc]
                if not dates:
                    self.logger.info("No valid dates found for archiving")
                    return True
                    
                normalized_dates = [self._normalize_to_datetime(d) for d in dates]
                min_date = min(normalized_dates)
                max_date = max(normalized_dates)

                base_path = f"s3://{configsettings.S3_BUCKET_NAME}/{configsettings.S3_FILE_PATH}rm-admin/archives"

                # Convert to DataFrame for S3 operations
                df = pd.DataFrame(documents_to_archive)
                
                # Add archive_date column (date only, not datetime)
                df['archive_date'] = df[time_column].apply(lambda x: x.date() if isinstance(x, datetime) else x)

                if min_date.date() == max_date.date():
                    # Single date archive
                    year = min_date.year
                    month = str(min_date.month).zfill(2)
                    day = str(min_date.day).zfill(2)
                    s3_path = base_path + f"/collection={collection_name}/year={year}/month={month}/day={day}/"
    
                    # Remove MongoDB _id field if present
                    df_to_save = df.drop(columns=['archive_date', '_id'], errors='ignore')
                    
                    wr.s3.to_parquet(
                        df=df_to_save,
                        path=s3_path,
                        dataset=True,
                        mode='append',
                        compression='snappy',
                        filename_prefix=f"{collection_name}_{min_date.strftime('%Y%m%d')}_"
                    )
                    self.logger.info(f"Archived records for {min_date.date()} to S3 at {s3_path}")
                else:
                    # Multiple dates - group by date
                    grouped = df.groupby('archive_date')
                    for archive_date, group_df in grouped:
                        if isinstance(archive_date, str):
                            date_obj = datetime.strptime(archive_date, '%Y-%m-%d')
                        else:
                            date_obj = datetime.combine(archive_date, datetime.min.time())
                            
                        year = date_obj.year
                        month = str(date_obj.month).zfill(2)
                        day = str(date_obj.day).zfill(2)
                        s3_path = base_path + f"/collection={collection_name}/year={year}/month={month}/day={day}/"
        
                        # Remove MongoDB _id field if present
                        df_to_save = group_df.drop(columns=['archive_date', '_id'], errors='ignore')
                        
                        wr.s3.to_parquet(
                            df=df_to_save,
                            path=s3_path,
                            dataset=True,
                            mode='append',
                            compression='snappy',
                            filename_prefix=f"{collection_name}_{date_obj.strftime('%Y%m%d')}_"
                        )
                        self.logger.info(f"Archived records for {archive_date} to S3 at {s3_path}")

                self.logger.info(f"Successfully archived records from {database_name}.{collection_name} to S3")
            
            if is_archive_db:
                # Archive to archive database
                archive_db = self.mongo_client['qp_rm_archives']
                
                if documents_to_archive:
                    # Insert into archive collection
                    archive_db[collection_name].insert_many(documents_to_archive)
                    self.logger.info(f"Archived {len(documents_to_archive)} documents to QP_RM_ARCHIVES.{collection_name}")

            return True
            
        except Exception as error:
            self.logger.error(f"Error in archive_before_delete: {error}")
            return False