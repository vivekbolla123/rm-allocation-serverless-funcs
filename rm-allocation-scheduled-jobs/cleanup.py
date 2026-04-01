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

    def archive_before_delete(self, database_name, collection_name, time_column, max_days, is_archive_db, is_archive_s3, date_format='%Y-%m-%d %H:%M:%S', batch_size=1000):
        """Archive data to S3 and/or archive database before deletion with memory-optimized batching"""
        try:
            if not is_archive_db and not is_archive_s3:
                self.logger.info("Archive Skipped as per configuration!")
                return True

            source_db = self.mongo_client[database_name]
            cutoff_date = datetime.now() - timedelta(days=max_days)
            cutoff_date_str = cutoff_date.strftime(date_format)
            query = {time_column: {"$lt": cutoff_date_str}}
            
            # Check if any documents exist without loading them
            if source_db[collection_name].count_documents(query, limit=1) == 0:
                self.logger.info("Archive skipped since no data to archive")
                return True

            total_archived = 0
            archive_db = self.mongo_client['qp_rm_archives'] if is_archive_db else None
            
            # Process in batches using cursor
            cursor = source_db[collection_name].find(query).batch_size(batch_size)
            
            if is_archive_s3:
                # Memory-bounded: flush each date-bucket to S3 when it reaches batch_size
                date_batches = {}  # date_key -> list of docs (bounded by batch_size)
                db_batch = []  # buffer for bulk DB inserts
                
                for doc in cursor:
                    # Archive to DB if needed (bulk insert for efficiency)
                    if is_archive_db:
                        db_batch.append(doc)
                        if len(db_batch) >= batch_size:
                            try:
                                archive_db[collection_name].insert_many(db_batch, ordered=False)
                            except Exception as e:
                                self.logger.warning(f"Failed to archive batch to DB: {e}")
                            db_batch = []
                    
                    # Accumulate by date for S3, flush when batch is full
                    if time_column not in doc:
                        continue
                        
                    doc_date = self._normalize_to_datetime(doc[time_column])
                    date_key = doc_date.date()
                    
                    if date_key not in date_batches:
                        date_batches[date_key] = []
                    
                    date_batches[date_key].append(doc)
                    total_archived += 1
                    
                    # Flush this date's batch to S3 when it reaches batch_size
                    if len(date_batches[date_key]) >= batch_size:
                        self._write_batch_to_s3(date_batches[date_key], collection_name, date_key)
                        self.logger.info(f"Flushed {len(date_batches[date_key])} records for {date_key} to S3")
                        date_batches[date_key] = []  # free memory immediately
                
                # Flush remaining documents per date
                for date_key, documents in date_batches.items():
                    if documents:
                        self._write_batch_to_s3(documents, collection_name, date_key)
                        self.logger.info(f"Flushed final {len(documents)} records for {date_key} to S3")
                date_batches.clear()  # free memory
                
                # Flush remaining DB batch
                if is_archive_db and db_batch:
                    try:
                        archive_db[collection_name].insert_many(db_batch, ordered=False)
                    except Exception as e:
                        self.logger.warning(f"Failed to archive final batch to DB: {e}")
                    db_batch = []
                
                self.logger.info(f"Successfully archived {total_archived} records from {database_name}.{collection_name} to S3")
            
            elif is_archive_db:
                # Only archive to DB (no S3) - use bulk insert for efficiency
                batch = []
                for doc in cursor:
                    batch.append(doc)
                    total_archived += 1
                    
                    if len(batch) >= batch_size:
                        archive_db[collection_name].insert_many(batch, ordered=False)
                        self.logger.info(f"Archived batch of {len(batch)} documents")
                        batch = []
                
                # Insert remaining documents
                if batch:
                    archive_db[collection_name].insert_many(batch, ordered=False)
                    self.logger.info(f"Archived final batch of {len(batch)} documents")
                
                self.logger.info(f"Archived {total_archived} documents to QP_RM_ARCHIVES.{collection_name}")

            return True
            
        except Exception as error:
            self.logger.error(f"Error in archive_before_delete: {error}")
            return False

    def _write_batch_to_s3(self, documents, collection_name, date_key):
        """Helper method to write a batch of documents to S3"""
        try:
            base_path = f"s3://{configsettings.S3_BUCKET_NAME}/{configsettings.S3_FILE_PATH}rm-admin/archives"
            
            year = date_key.year
            month = str(date_key.month).zfill(2)
            day = str(date_key.day).zfill(2)
            s3_path = base_path + f"/collection={collection_name}/year={year}/month={month}/day={day}/"
            
            # Sanitize and convert to DataFrame in one pass (no intermediate list)
            df_to_save = pd.DataFrame(self._sanitize_document(doc) for doc in documents)
            
            # Convert all columns to consistent types
            for col in df_to_save.columns:
                col_dtype = df_to_save[col].dtype
                
                # Handle object dtype columns - convert to string for safety
                if col_dtype == 'object':
                    df_to_save[col] = df_to_save[col].astype(str)
                
                # Handle float columns that might have NaN
                elif 'float' in str(col_dtype):
                    df_to_save[col] = df_to_save[col].astype('float64')
            
            # Generate unique filename with timestamp
            import uuid
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            unique_id = str(uuid.uuid4())[:8]
            filename = f"{collection_name}_{date_key.strftime('%Y%m%d')}_{timestamp}_{unique_id}.parquet"
            full_s3_path = s3_path + filename
            
            # Write to S3
            wr.s3.to_parquet(
                df=df_to_save,
                path=full_s3_path,
                compression='snappy'
            )
            
            self.logger.info(f"Wrote batch of {len(documents)} records for {date_key} to S3")
            
        except Exception as e:
            self.logger.error(f"Error writing batch to S3: {e}")
            raise

    def _sanitize_document(self, doc):
        """Sanitize MongoDB document for parquet serialization - convert everything to basic types"""
        from bson import Decimal128, Int64, ObjectId
        import numpy as np
        
        sanitized = {}
        for key, value in doc.items():
            # Skip _id entirely
            if key == '_id':
                continue
                
            # Handle None/null
            if value is None:
                sanitized[key] = None
            # Handle MongoDB types
            elif isinstance(value, Decimal128):
                sanitized[key] = float(str(value))
            elif isinstance(value, Int64):
                sanitized[key] = int(value)
            elif isinstance(value, ObjectId):
                sanitized[key] = str(value)
            elif isinstance(value, bytes):
                sanitized[key] = value.hex()
            # Handle datetime
            elif isinstance(value, datetime):
                sanitized[key] = value.isoformat()
            # Handle numpy types
            elif isinstance(value, (np.integer, np.floating)):
                sanitized[key] = value.item()
            # Handle nested dict
            elif isinstance(value, dict):
                sanitized[key] = str(value)  # Convert nested dicts to string
            # Handle lists
            elif isinstance(value, list):
                # Convert complex lists to JSON strings
                sanitized[key] = str(value)
            # Handle everything else
            else:
                # Force convert to basic Python types
                try:
                    if isinstance(value, (int, float, str, bool)):
                        sanitized[key] = value
                    else:
                        sanitized[key] = str(value)
                except:
                    sanitized[key] = str(value)
        
        return sanitized