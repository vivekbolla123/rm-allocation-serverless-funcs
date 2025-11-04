import boto3
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta
import pytz
import os
import logging
import sys
from typing import List, Tuple
import traceback
import configsettings

class S3CsvCombiner:
    
    def __init__(self, logger, params, overrides, context):
        self.logger = logger or logging.getLogger()
        self.mongo_conn = params['mongoconn']  # Single MongoDB connection
        self.db = self.mongo_conn['qp_dw_rmalloc']

        # S3 Configuration from overrides or defaults
        self.bucket_name = configsettings.S3_BUCKET_NAME
        self.prefix_path = configsettings.S3_FILE_PATH + "rm-admin/audits/"
        
        self.min_date = datetime.now() - timedelta(days=30)
        
        # Database collection names
        self.raw_collection_name = "audit_raw_data"
        self.summary_collection_name = "audit_ml_summary"
        
        # Initialize S3 client
        self.s3_client = boto3.client('s3')
        
        # Store combined DataFrame in memory instead of temp file
        self.combined_df = None

    def normalize_prefix(self, prefix: str) -> str:
        return prefix.rstrip('/') + '/'

    def make_timezone_aware(self, date_obj: datetime) -> datetime:
        if date_obj.tzinfo is None:
            return pytz.utc.localize(date_obj)
        return date_obj

    def get_matching_csv_files(self, bucket: str, prefix: str, min_date: datetime) -> List[str]:
        paginator = self.s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        
        csv_files = []
        for page in pages:
            for obj in page.get('Contents', []):
                if obj['Key'].endswith('.csv'):
                    last_modified = obj['LastModified']
                    if last_modified > min_date:
                        csv_files.append(obj['Key'])
        
        self.logger.info(f"Found {len(csv_files)} CSV files from last 30 days")
        
        if not csv_files:
            error_msg = f"No CSV files found with last modified date after {min_date}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
        return csv_files

    def read_csv_files_from_s3(self, bucket: str, csv_files: List[str]) -> List[pd.DataFrame]:
        dfs = []
        for index, file_key in enumerate(csv_files, 1):
            try:
                response = self.s3_client.get_object(Bucket=bucket, Key=file_key)
                csv_content = response['Body'].read().decode('utf-8')
                df = pd.read_csv(StringIO(csv_content))
                dfs.append(df)
            except Exception as e:
                self.logger.error("Failed to read file %s: %s", file_key, str(e))
                raise
        
        return dfs

    def combine_dataframes(self, dfs: List[pd.DataFrame]) -> pd.DataFrame:
        result_df = pd.concat(dfs, ignore_index=True)
        self.logger.info(f"Combined {len(dfs)} files into {len(result_df)} total rows")
        return result_df

    def save_dataframe_to_mongodb(self, df: pd.DataFrame, collection_name: str, replace: bool = True) -> None:
        """
        Save DataFrame to MongoDB collection.
        
        Args:
            df: DataFrame to save
            collection_name: Name of the collection
            replace: If True, replace all documents (like SQL 'replace'). If False, append.
        """
        try:
            collection = self.db[collection_name]
            
            # Replace mode: delete all existing documents
            if replace:
                delete_result = collection.delete_many({})
                self.logger.info(f"Deleted {delete_result.deleted_count} existing documents from {collection_name}")
            
            # Convert DataFrame to records
            records = df.to_dict('records')
            
            # Convert date columns to datetime objects for MongoDB
            # for record in records:
            #     for key, value in record.items():
            #         # Handle pandas Timestamp objects
            #         if isinstance(value, pd.Timestamp):
            #             record[key] = value.to_pydatetime()
            #         # Handle date objects
            #         elif hasattr(value, 'date') and callable(getattr(value, 'date')):
            #             try:
            #                 record[key] = datetime.combine(value, datetime.min.time())
            #             except:
            #                 pass
            #         # Handle NaN and None values
            #         elif pd.isna(value):
            #             record[key] = None
            
            # Bulk insert
            if records:
                result = collection.insert_many(records)
                self.logger.info(f"Saved {len(result.inserted_ids)} rows to collection {collection_name}")
            else:
                self.logger.info(f"No records to save to collection {collection_name}")
                
        except Exception as e:
            self.logger.error("Failed to save to MongoDB collection %s: %s", collection_name, str(e))
            raise

    def combine_s3_csv_files(self) -> pd.DataFrame:
        # Normalize prefix and make date timezone-aware
        normalized_prefix = self.normalize_prefix(self.prefix_path)
        tz_aware_min_date = self.make_timezone_aware(self.min_date)
        
        # Get matching CSV files
        csv_files = self.get_matching_csv_files(self.bucket_name, normalized_prefix, tz_aware_min_date)
        
        # Read CSV files from S3
        dataframes = self.read_csv_files_from_s3(self.bucket_name, csv_files)
        
        # Combine all DataFrames
        combined_df = self.combine_dataframes(dataframes)
        
        # Store in memory for later processing
        self.combined_df = combined_df
        
        # Save combined DataFrame to MongoDB
        self.save_dataframe_to_mongodb(combined_df, self.raw_collection_name, replace=True)
        
        return combined_df

    def process_audit_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df['changedAt'] = pd.to_datetime(df['changedAt'])
        df['changedAtDate'] = df['changedAt'].dt.date

        # Group by date and userName for audit counts
        grouped_df = df.groupby(['changedAtDate', 'userName']).agg({
            'date': 'size'  # This counts the number of records
        }).reset_index()
        
        # Rename columns for clarity
        grouped_df.rename(columns={
            'changedAtDate': 'date',
            'userName': 'username',  # Standardize column name
            'date': 'AuditCounts'
        }, inplace=True)
        
        self.logger.info(f"Processed audit data into {len(grouped_df)} grouped records")
        return grouped_df

    def get_date_range_from_audit_data(self, grouped_df: pd.DataFrame) -> Tuple[str, str]:
        min_date = grouped_df['date'].min().strftime('%Y-%m-%d')
        max_date = grouped_df['date'].max().strftime('%Y-%m-%d')
        return min_date, max_date

    def fetch_ml_upload_counts(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Fetch ML upload counts from MongoDB inputs_file_upload collection.
        """
        try:
            collection = self.db['inputs_file_upload']
            
            # Convert date strings to datetime objects for MongoDB query
            start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
            end_datetime = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)
            
            start_datetime_str=start_datetime.strftime('%Y-%m-%d %H:%M:%S')# Include end date
            end_datetime_str=end_datetime.strftime('%Y-%m-%d %H:%M:%S')
            # MongoDB aggregation pipeline
            pipeline = [
    {
        "$addFields": {
            # Convert string to actual date object
            "init_timestamp_dt": {
                "$dateFromString": {
                    "dateString": "$init_timestamp",
                    "format": "%Y-%m-%dT%H:%M:%S"
                }
            }
        }
    },
    {
        "$match": {
            "init_timestamp_dt": {
                "$gte": start_datetime,
                "$lt": end_datetime
            },
            "tableName": {"$regex": "market_list", "$options": "i"}
        }
    },
    {
        "$project": {
            "date": "$init_timestamp_dt",
            "username": 1
        }
    },
    {
        "$group": {
            "_id": {
                "date": "$date",
                "username": "$username"
            },
            "MLUploadCounts": {"$sum": 1}
        }
    },
    {
        "$project": {
            "_id": 0,
            "date": "$_id.date",
            "username": "$_id.username",
            "MLUploadCounts": 1
        }
    },
    {
        "$sort": {"date": 1}
    }
]
            
            # Execute aggregation
            result = list(collection.aggregate(pipeline))
            ml_df = pd.DataFrame(result)
            
            if not ml_df.empty:
                # Convert date string back to date object
                ml_df['date'] = pd.to_datetime(ml_df['date']).dt.date
            
            self.logger.info(f"Fetched {len(ml_df)} ML upload records")
            return ml_df
            
        except Exception as e:
            self.logger.error("Failed to fetch ML upload counts from MongoDB: %s", str(e))
            # Fallback: try with string-based date matching if init_timestamp is stored as string
            try:
                self.logger.info("Attempting fallback query with string date matching")
                return self._fetch_ml_upload_counts_fallback(start_date, end_date)
            except:
                raise

    def _fetch_ml_upload_counts_fallback(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Fallback method if init_timestamp is stored as string in MongoDB.
        """
        try:
            collection = self.db['inputs_file_upload']
            
            # Fetch all matching documents and process in pandas
            cursor = collection.find({
                'tableName': {'$regex': 'market_list', '$options': 'i'}
            })
            
            df = pd.DataFrame(list(cursor))
            
            if df.empty:
                return pd.DataFrame(columns=['date', 'username', 'MLUploadCounts'])
            
            # Remove MongoDB _id
            if '_id' in df.columns:
                df = df.drop('_id', axis=1)
            
            # Convert init_timestamp to datetime
            df['init_timestamp'] = pd.to_datetime(df['init_timestamp'], errors='coerce')
            df['date'] = df['init_timestamp'].dt.date
            
            # Filter by date range
            start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
            end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
            df = df[(df['date'] >= start_dt) & (df['date'] <= end_dt)]
            
            # Group by date and username
            ml_df = df.groupby(['date', 'username']).size().reset_index(name='MLUploadCounts')
            
            self.logger.info(f"Fetched {len(ml_df)} ML upload records (fallback method)")
            return ml_df
            
        except Exception as e:
            self.logger.error("Fallback method also failed: %s", str(e))
            raise

    def join_audit_and_ml_data(self, audit_df: pd.DataFrame, ml_df: pd.DataFrame) -> pd.DataFrame:
        # Create a comprehensive date-username combination from both datasets
        audit_combinations = audit_df[['date', 'username']].copy()
        ml_combinations = ml_df[['date', 'username']].copy()
        
        # Combine all unique date-username combinations
        all_combinations = pd.concat([audit_combinations, ml_combinations]).drop_duplicates().reset_index(drop=True)
        
        # Left join audit data to get audit counts
        final_df = pd.merge(all_combinations, audit_df, on=['date', 'username'], how='left')
        
        # Left join ML data to get ML upload counts
        final_df = pd.merge(final_df, ml_df, on=['date', 'username'], how='left')
        
        # Fill missing values with 0
        final_df['AuditCounts'] = final_df['AuditCounts'].fillna(0).astype(int)
        final_df['MLUploadCounts'] = final_df['MLUploadCounts'].fillna(0).astype(int)
        
        self.logger.info(f"Joined data: {len(final_df)} total records")
        
        return final_df

    def add_default_ml_counts(self, audit_df: pd.DataFrame) -> pd.DataFrame:
        final_df = audit_df.copy()
        final_df['MLUploadCounts'] = 0
        return final_df

    def process_ml_data_integration(self, audit_df: pd.DataFrame) -> pd.DataFrame:
        try:
            min_date, max_date = self.get_date_range_from_audit_data(audit_df)
            ml_df = self.fetch_ml_upload_counts(min_date, max_date)
            return self.join_audit_and_ml_data(audit_df, ml_df)
            
        except Exception as e:
            self.logger.error("Failed to fetch ML upload counts, proceeding without: %s", str(e))
            return self.add_default_ml_counts(audit_df)

    def generate_final_report(self) -> pd.DataFrame:
        # Check if combined_df is available
        if self.combined_df is None:
            raise ValueError("No combined data available. Run combine_s3_csv_files() first.")
        
        # Process audit data to get grouped counts (using in-memory DataFrame)
        audit_df = self.process_audit_data(self.combined_df)
        
        # Integrate ML data with the audit counts
        final_df = self.process_ml_data_integration(audit_df)
        final_df['date'] = pd.to_datetime(final_df['date'])
        
        # Save final report to MongoDB
        self.save_dataframe_to_mongodb(final_df, self.summary_collection_name, replace=True)
        
        return final_df

    def run(self):
        try:
            self.logger.info("Starting S3 CSV combiner process")
            
            # Step 1: Combine S3 CSV files and save to MongoDB
            combined_df = self.combine_s3_csv_files()
            
            # Step 2: Generate final report with ML data and save to MongoDB
            final_df = self.generate_final_report()
            
            self.logger.info(f"Process completed successfully - Raw: {len(combined_df)} rows, Summary: {len(final_df)} rows")
            return True
            
        except Exception as e:
            self.logger.error(f"Error in S3 CSV combiner process: {str(e)}")
            traceback.print_exc()
            return False