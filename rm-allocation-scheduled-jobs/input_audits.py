import boto3
import pandas as pd
from io import StringIO, BytesIO
from datetime import datetime, timedelta
import pytz
import os
import logging
import sys
from typing import List, Tuple, Dict, Any
import traceback
import configsettings
import json
import re

# Add pyarrow for parquet support
try:
    import pyarrow.parquet as pq
except ImportError:
    # Fallback if pyarrow is not available
    pq = None

class S3AuditProcessor:
    
    def __init__(self, logger, params, overrides, context):
        self.logger = logger or logging.getLogger()
        self.mongo_conn = params['mongoconn']  # Single MongoDB connection
        self.db = self.mongo_conn['qp_dw_rmalloc']

        # S3 Configuration from overrides or defaults
        self.bucket_name = configsettings.S3_BUCKET_NAME
        self.ml_audits_prefix = configsettings.S3_FILE_PATH + "rm-admin/ml-audits/"
        self.wb_audits_prefix = configsettings.S3_FILE_PATH + "rm-admin/audits/"
        
        # Rolling window configuration
        self.rolling_days = 3
        self.min_days_required = 2
        
        # Calculate date ranges
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        self.target_date = yesterday  # Process yesterday's data
        self.window_start_date = today - timedelta(days=self.rolling_days)
        self.window_end_date = yesterday
        
        # Database collection names
        self.audit_reports_collection = "audit_reports"
        self.metadata_collection = "audit_metadata"
        
        # Initialize S3 client
        self.s3_client = boto3.client('s3')
        
        # Store processed data
        self.processed_data = []

    def get_last_processed_info(self) -> Dict[str, Any]:
        """Get information about the last processing run"""
        try:
            collection = self.db[self.metadata_collection]
            last_run = collection.find_one({"type": "last_run"}, sort=[("timestamp", -1)])
            
            if last_run:
                return {
                    "last_processed_date": last_run.get("last_processed_date"),
                    "last_run_timestamp": last_run.get("timestamp"),
                    "files_processed_count": last_run.get("files_processed_count", 0)
                }
            return {"last_processed_date": None, "last_run_timestamp": None, "files_processed_count": 0}
            
        except Exception as e:
            self.logger.error(f"Failed to get last processed info: {str(e)}")
            return {"last_processed_date": None, "last_run_timestamp": None, "files_processed_count": 0}

    def save_processing_metadata(self, processed_files_count: int, processed_date: str) -> None:
        """Save metadata about the current processing run"""
        try:
            collection = self.db[self.metadata_collection]
            
            metadata = {
                "type": "last_run",
                "timestamp": datetime.now(),
                "last_processed_date": processed_date,
                "files_processed_count": processed_files_count
            }
            
            collection.insert_one(metadata)
            self.logger.info(f"Saved processing metadata for {processed_date}: {processed_files_count} files")
            
        except Exception as e:
            self.logger.error(f"Failed to save processing metadata: {str(e)}")

    def is_first_run(self) -> bool:
        """Check if this is the first run by looking at existing data"""
        try:
            collection = self.db[self.audit_reports_collection]
            count = collection.count_documents({})
            return count == 0
        except Exception as e:
            self.logger.error(f"Failed to check if first run: {str(e)}")
            return True

    def cleanup_old_data(self, cutoff_date: str) -> None:
        """Remove data older than the cutoff date to maintain rolling window"""
        try:
            collection = self.db[self.audit_reports_collection]
            
            # Convert cutoff_date string to datetime object for comparison
            cutoff_datetime = datetime.strptime(cutoff_date, '%Y-%m-%d')
            
            # Delete records older than cutoff_date
            delete_result = collection.delete_many({
                "changeAtdate": {"$lt": cutoff_datetime}
            })
            
            self.logger.info(f"Cleaned up {delete_result.deleted_count} old records before {cutoff_date}")
            
        except Exception as e:
            self.logger.error(f"Failed to cleanup old data: {str(e)}")

    def get_existing_data_date_range(self) -> Tuple[datetime, datetime]:
        """Get the date range of existing data in the database"""
        try:
            collection = self.db[self.audit_reports_collection]
            
            # Get min and max dates
            pipeline = [
                {
                    "$group": {
                        "_id": None,
                        "min_date": {"$min": "$changeAtdate"},
                        "max_date": {"$max": "$changeAtdate"}
                    }
                }
            ]
            
            result = list(collection.aggregate(pipeline))
            if result and result[0]["min_date"] and result[0]["max_date"]:
                return result[0]["min_date"], result[0]["max_date"]
            
            return None, None
            
        except Exception as e:
            self.logger.error(f"Failed to get existing data date range: {str(e)}")
            return None, None

    def normalize_prefix(self, prefix: str) -> str:
        return prefix.rstrip('/') + '/'

    def make_timezone_aware(self, date_obj: datetime) -> datetime:
        if date_obj.tzinfo is None:
            return pytz.utc.localize(date_obj)
        return date_obj

    def get_ml_audit_files(self, is_first_run: bool = False) -> List[str]:
        """Get ML audit parquet files based on run type"""
        ml_files = []
        
        # Determine date filter based on run type
        if is_first_run:
            # First run: get last 30 days
            min_date = self.make_timezone_aware(datetime.combine(self.window_start_date, datetime.min.time()))
            print(f"First run: Looking for ML files from {self.window_start_date}")
        else:
            # Rolling run: get files modified since yesterday
            min_date = self.make_timezone_aware(datetime.combine(self.target_date, datetime.min.time()))
            print(f"Rolling run: Looking for ML files modified since {self.target_date}")
        
        # Check both direct and connections folders
        for audit_type in ['direct', 'connections']:
            prefix = f"{self.ml_audits_prefix}{audit_type}/"
            
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)
            
            for page in pages:
                for obj in page.get('Contents', []):
                    if obj['Key'].endswith('.parquet'):
                        last_modified = obj['LastModified']
                        
                        # For first run, check if file contains data within our window
                        # For rolling run, check if file was modified recently
                        if is_first_run:
                            # Extract date from path and check if it's in our window
                            if self.is_file_in_date_range(obj['Key'], self.window_start_date, self.window_end_date):
                                ml_files.append(obj['Key'])
                        else:
                            # Check if file was modified since target date
                            if last_modified >= min_date:
                                ml_files.append(obj['Key'])
        
        print(f"Found {len(ml_files)} ML audit parquet files")
        return ml_files

    def get_wb_audit_files(self, is_first_run: bool = False) -> List[str]:
        """Get Workbench audit CSV files based on run type"""
        wb_files = []
        
        # Determine date filter based on run type
        if is_first_run:
            min_date = self.make_timezone_aware(datetime.combine(self.window_start_date, datetime.min.time()))
            print(f"First run: Looking for WB files from {self.window_start_date}")
        else:
            min_date = self.make_timezone_aware(datetime.combine(self.target_date, datetime.min.time()))
            print(f"Rolling run: Looking for WB files modified since {self.target_date}")
        
        paginator = self.s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.bucket_name, Prefix=self.wb_audits_prefix)
        
        for page in pages:
            for obj in page.get('Contents', []):
                if obj['Key'].endswith('.csv'):
                    last_modified = obj['LastModified']
                    
                    if is_first_run:
                        # Extract date from path and check if it's in our window
                        if self.is_file_in_date_range(obj['Key'], self.window_start_date, self.window_end_date):
                            wb_files.append(obj['Key'])
                    else:
                        # Check if file was modified since target date
                        if last_modified >= min_date:
                            wb_files.append(obj['Key'])
        
        print(f"Found {len(wb_files)} Workbench audit CSV files")
        return wb_files

    def is_file_in_date_range(self, file_key: str, start_date: datetime.date, end_date: datetime.date) -> bool:
        """Check if a file contains data within the specified date range"""
        try:
            # For ML files: extract from changedAtDate=YYYY-MM-DD
            if 'changedAtDate=' in file_key:
                date_match = re.search(r'changedAtDate=(\d{4}-\d{2}-\d{2})', file_key)
                if date_match:
                    file_date = datetime.strptime(date_match.group(1), '%Y-%m-%d').date()
                    return start_date <= file_date <= end_date
            
            # For WB files: extract from date=MM-DD-YYYY and filename YYYY-MM-DD.csv
            elif '/date=' in file_key:
                # Extract departure date from path
                date_match = re.search(r'/date=(\d{2}-\d{2}-\d{4})/', file_key)
                if date_match:
                    dept_date_str = date_match.group(1)
                    month, day, year = dept_date_str.split('-')
                    dept_date = datetime(int(year), int(month), int(day)).date()
                    
                    # Also check filename for changed date
                    filename = file_key.split('/')[-1].replace('.csv', '')
                    try:
                        changed_date = datetime.strptime(filename, '%Y-%m-%d').date()
                        # File is relevant if either departure date or change date is in range
                        return (start_date <= dept_date <= end_date) or (start_date <= changed_date <= end_date)
                    except:
                        # If filename parsing fails, just use departure date
                        return start_date <= dept_date <= end_date
            
            return False
            
        except Exception as e:
            self.logger.warning(f"Could not parse date from file {file_key}: {str(e)}")
            return False

    def process_ml_audit_file(self, file_key: str) -> List[Dict[str, Any]]:
        """Process a single ML audit parquet file"""
        try:
            # Extract metadata from file path
            # Format: rm-admin/ml-audits/direct/changedAtDate=2026-01-06/part-xxx.parquet
            path_parts = file_key.split('/')
            audit_type = path_parts[2]  # 'direct' or 'connections'
            
            # Extract changedAtDate from path
            changed_at_date_part = [part for part in path_parts if part.startswith('changedAtDate=')]
            if not changed_at_date_part:
                self.logger.warning(f"Could not extract changedAtDate from {file_key}")
                return []
            
            changed_at_date = changed_at_date_part[0].split('=')[1]
            
            # Read parquet file from S3
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=file_key)
            parquet_content = response['Body'].read()
            
            # Read parquet data
            df = pd.read_parquet(BytesIO(parquet_content))
            
            processed_rows = []
            
            for _, row in df.iterrows():
                # Extract basic info
                sector = row.get('sector', '')
                flight_no = row.get('flightNo', '')
                dept_date = row.get('date', '')
                changed_at = row.get('changedAt', '')
                analyst = row.get('userName', '')
                
                # Convert dept_date from dd-mm-yyyy to yyyy-mm-dd if needed
                if dept_date and isinstance(dept_date, str):
                    try:
                        if '-' in dept_date and len(dept_date.split('-')[0]) == 2:
                            # Format is dd-mm-yyyy, convert to yyyy-mm-dd
                            day, month, year = dept_date.split('-')
                            dept_date = f"{year}-{month}-{day}"
                    except:
                        pass
                
                # Extract changed_at_date from timestamp
                if changed_at:
                    try:
                        changed_at_dt = pd.to_datetime(changed_at)
                        change_at_date = changed_at_dt.strftime('%Y-%m-%d')
                    except:
                        change_at_date = changed_at_date
                else:
                    change_at_date = changed_at_date
                
                # Process each column as a potential change
                for col_name, col_value in row.items():
                    # Skip metadata columns and null values
                    if col_name in ['sector', 'flightNo', 'date', 'changedAt', 'userName'] or pd.isna(col_value):
                        continue
                    
                    processed_rows.append({
                        'source': 'ML',
                        'sector': sector,
                        'fltno': flight_no,
                        'deptdate': dept_date,
                        'changeAtdate': datetime.strptime(change_at_date, '%Y-%m-%d') if change_at_date else None,
                        'analyst': analyst,
                        'key': col_name,
                        'value': str(col_value) if col_value is not None else ''
                    })
            
            self.logger.info(f"Processed ML audit file {file_key}: {len(processed_rows)} records")
            return processed_rows
            
        except Exception as e:
            self.logger.error(f"Failed to process ML audit file {file_key}: {str(e)}")
            return []

    def process_wb_audit_file(self, file_key: str) -> List[Dict[str, Any]]:
        """Process a single Workbench audit CSV file"""
        try:
            # Extract metadata from file path
            # Format: rm-admin/audits/sector=AMDBLR/flightNo=1332/date=01-01-2026/2025-10-29.csv
            path_parts = file_key.split('/')
            
            # Extract sector, flightNo, and date from path
            sector = ''
            flight_no = ''
            dept_date = ''
            
            for part in path_parts:
                if part.startswith('sector='):
                    sector = part.split('=')[1]
                elif part.startswith('flightNo='):
                    flight_no = part.split('=')[1]
                elif part.startswith('date='):
                    dept_date = part.split('=')[1]
            
            # Convert dept_date from mm-dd-yyyy to yyyy-mm-dd
            if dept_date and '-' in dept_date:
                try:
                    month, day, year = dept_date.split('-')
                    dept_date = f"{year}-{month}-{day}"
                except:
                    pass
            
            # Extract changed_at_date from filename (without .csv extension)
            filename = path_parts[-1].replace('.csv', '')
            change_at_date = filename  # Format: 2025-10-29
            
            # Read CSV file from S3
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=file_key)
            csv_content = response['Body'].read().decode('utf-8')
            df = pd.read_csv(StringIO(csv_content))
            
            processed_rows = []
            
            for _, row in df.iterrows():
                # Use values from CSV if available, otherwise use path values
                csv_sector = row.get('sector', sector)
                csv_flight_no = row.get('flightNo', flight_no)
                csv_dept_date = row.get('date', dept_date)
                analyst = row.get('userName', '')
                
                # Convert CSV dept_date if needed
                if csv_dept_date and isinstance(csv_dept_date, str):
                    try:
                        if '-' in csv_dept_date and len(csv_dept_date.split('-')[0]) == 2:
                            # Format is dd-mm-yyyy, convert to yyyy-mm-dd
                            day, month, year = csv_dept_date.split('-')
                            csv_dept_date = f"{year}-{month}-{day}"
                        elif '-' in csv_dept_date and len(csv_dept_date.split('-')[0]) == 1:
                            # Format is m-dd-yyyy, convert to yyyy-mm-dd
                            month, day, year = csv_dept_date.split('-')
                            csv_dept_date = f"{year}-{month.zfill(2)}-{day}"
                    except:
                        pass
                
                # Process each column as a potential change
                for col_name, col_value in row.items():
                    # Skip metadata columns and null values
                    if col_name in ['date', 'flightNo', 'sector', 'changeType', 'changedAt', 'userName'] or pd.isna(col_value):
                        continue
                    
                    processed_rows.append({
                        'source': 'WB',
                        'sector': csv_sector,
                        'fltno': csv_flight_no,
                        'deptdate': csv_dept_date,
                        'changeAtdate': datetime.strptime(change_at_date, '%Y-%m-%d') if change_at_date else None,
                        'analyst': analyst,
                        'key': col_name,
                        'value': str(col_value) if col_value is not None else ''
                    })
            
            self.logger.info(f"Processed WB audit file {file_key}: {len(processed_rows)} records")
            return processed_rows
            
        except Exception as e:
            self.logger.error(f"Failed to process WB audit file {file_key}: {str(e)}")
            return []

    def save_audit_reports_to_mongodb(self, records: List[Dict[str, Any]], replace: bool = False) -> None:
        """
        Save audit reports to MongoDB collection.
        
        Args:
            records: List of audit report records
            replace: If True, replace all documents. If False, append (default for rolling updates).
        """
        try:
            collection = self.db[self.audit_reports_collection]
            
            # Replace mode: delete all existing documents (only for first run)
            if replace:
                delete_result = collection.delete_many({})
                print(f"Deleted {delete_result.deleted_count} existing documents from {self.audit_reports_collection}")
            
            # Bulk insert
            if records:
                result = collection.insert_many(records)
                print(f"Saved {len(result.inserted_ids)} audit report records to MongoDB")
            else:
                print("No audit report records to save")
                
        except Exception as e:
            self.logger.error("Failed to save audit reports to MongoDB: %s", str(e))
            raise

    def process_all_audit_files(self, is_first_run: bool = False) -> List[Dict[str, Any]]:
        """Process both ML and WB audit files based on run type"""
        all_records = []
        processed_files = []
        
        # Process ML audit files
        ml_files = self.get_ml_audit_files(is_first_run)
        for file_key in ml_files:
            records = self.process_ml_audit_file(file_key)
            all_records.extend(records)
            processed_files.append(file_key)
        
        # Process WB audit files
        wb_files = self.get_wb_audit_files(is_first_run)
        for file_key in wb_files:
            records = self.process_wb_audit_file(file_key)
            all_records.extend(records)
            processed_files.append(file_key)
        
        print(f"Total processed records: {len(all_records)} from {len(processed_files)} files")
        return all_records, processed_files

    def run(self):
        try:
            print("Starting S3 audit processor")
            
            # Determine if this is the first run
            is_first_run = self.is_first_run()
            
            if is_first_run:
                print(f"First run detected - Processing last {self.rolling_days} days of data")
                print(f"Date range: {self.window_start_date} to {self.window_end_date}")
                
                # Process all audit files for the initial window
                all_records, processed_files = self.process_all_audit_files(is_first_run=True)
                
                if not all_records:
                    self.logger.warning("No audit records found for initial load")
                    return False
                
                # Save to MongoDB (replace mode for first run)
                self.save_audit_reports_to_mongodb(all_records, replace=True)
                
                # Save metadata
                self.save_processing_metadata(len(processed_files), self.target_date.strftime('%Y-%m-%d'))
                
            else:
                print(f"Rolling run - Processing data for {self.target_date}")
                
                # Get existing data range to ensure we maintain minimum days
                min_existing_date, max_existing_date = self.get_existing_data_date_range()
                
                if min_existing_date and max_existing_date:
                    self.logger.info(f"Existing data range: {min_existing_date} to {max_existing_date}")
                    
                    # Calculate how many days of data we currently have
                    # Convert datetime objects to date objects for calculation
                    if isinstance(min_existing_date, datetime):
                        min_date_obj = min_existing_date.date()
                    else:
                        min_date_obj = min_existing_date
                        
                    if isinstance(max_existing_date, datetime):
                        max_date_obj = max_existing_date.date()
                    else:
                        max_date_obj = max_existing_date
                    
                    current_days = (max_date_obj - min_date_obj).days + 1
                    self.logger.info(f"Current data spans {current_days} days")
                    
                    # Only cleanup if we have enough data
                    if current_days >= self.min_days_required:
                        cutoff_date = self.window_start_date.strftime('%Y-%m-%d')
                        self.cleanup_old_data(cutoff_date)
                    else:
                        self.logger.info(f"Keeping all existing data (only {current_days} days, need {self.min_days_required})")
                
                # Process new files (incremental)
                all_records, processed_files = self.process_all_audit_files(is_first_run=False)
                
                if all_records:
                    # Save to MongoDB (append mode for rolling updates)
                    self.save_audit_reports_to_mongodb(all_records, replace=False)
                    
                    # Save metadata
                    self.save_processing_metadata(len(processed_files), self.target_date.strftime('%Y-%m-%d'))
                else:
                    self.logger.info("No new audit records found for rolling update")
            
            # Final verification and summary
            final_min_date, final_max_date = self.get_existing_data_date_range()
            if final_min_date and final_max_date:
                if isinstance(final_min_date, datetime):
                    final_min_obj = final_min_date.date()
                else:
                    final_min_obj = final_min_date
                    
                if isinstance(final_max_date, datetime):
                    final_max_obj = final_max_date.date()
                else:
                    final_max_obj = final_max_date
                
                final_days = (final_max_obj - final_min_obj).days + 1
                self.logger.info(f"Final data range: {final_min_date} to {final_max_date} ({final_days} days)")
            
            # Validate data completeness
            is_complete = self.validate_data_completeness()
            
            # Get processing summary
            summary = self.get_processing_summary()
            self.logger.info(f"Processing summary: {summary}")
            
            self.logger.info("Process completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error in S3 audit processor: {str(e)}")
            traceback.print_exc()
            return False
    def validate_data_completeness(self) -> bool:
        """Validate that we have sufficient data coverage"""
        try:
            min_date, max_date = self.get_existing_data_date_range()
            
            if not min_date or not max_date:
                self.logger.warning("No existing data found")
                return False
            
            # Convert datetime objects to date objects for calculation
            if isinstance(min_date, datetime):
                min_date_obj = min_date.date()
            else:
                min_date_obj = min_date
                
            if isinstance(max_date, datetime):
                max_date_obj = max_date.date()
            else:
                max_date_obj = max_date
            
            # Calculate coverage
            days_covered = (max_date_obj - min_date_obj).days + 1
            
            # Check if we have minimum required coverage
            if days_covered >= self.min_days_required:
                print(f"Data completeness validated: {days_covered} days of data")
                return True
            else:
                self.logger.warning(f"Insufficient data coverage: {days_covered} days (need {self.min_days_required})")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to validate data completeness: {str(e)}")
            return False

    def get_processing_summary(self) -> Dict[str, Any]:
        """Get a summary of the current processing state"""
        try:
            min_date, max_date = self.get_existing_data_date_range()
            
            collection = self.db[self.audit_reports_collection]
            total_records = collection.count_documents({})
            
            # Get record counts by source
            ml_count = collection.count_documents({"source": "ML"})
            wb_count = collection.count_documents({"source": "WB"})
            
            return {
                "total_records": total_records,
                "ml_records": ml_count,
                "wb_records": wb_count,
                "date_range_start": min_date,
                "date_range_end": max_date,
                "target_date": self.target_date.strftime('%Y-%m-%d'),
                "window_start": self.window_start_date.strftime('%Y-%m-%d'),
                "window_end": self.window_end_date.strftime('%Y-%m-%d')
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get processing summary: {str(e)}")
            return {}
        """Validate that we have sufficient data coverage"""
        try:
            min_date, max_date = self.get_existing_data_date_range()
            
            if not min_date or not max_date:
                self.logger.warning("No existing data found")
                return False
            
            # Convert to date objects if they're strings
            if isinstance(min_date, str):
                min_date_obj = datetime.strptime(min_date, '%Y-%m-%d').date()
            else:
                min_date_obj = min_date
                
            if isinstance(max_date, str):
                max_date_obj = datetime.strptime(max_date, '%Y-%m-%d').date()
            else:
                max_date_obj = max_date
            
            # Calculate coverage
            days_covered = (max_date_obj - min_date_obj).days + 1
            
            # Check if we have minimum required coverage
            if days_covered >= self.min_days_required:
                print(f"Data completeness validated: {days_covered} days of data")
                return True
            else:
                self.logger.warning(f"Insufficient data coverage: {days_covered} days (need {self.min_days_required})")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to validate data completeness: {str(e)}")
            return False

    def get_processing_summary(self) -> Dict[str, Any]:
        """Get a summary of the current processing state"""
        try:
            min_date, max_date = self.get_existing_data_date_range()
            
            collection = self.db[self.audit_reports_collection]
            total_records = collection.count_documents({})
            
            # Get record counts by source
            ml_count = collection.count_documents({"source": "ML"})
            wb_count = collection.count_documents({"source": "WB"})
            
            return {
                "total_records": total_records,
                "ml_records": ml_count,
                "wb_records": wb_count,
                "date_range_start": min_date,
                "date_range_end": max_date,
                "date_range_start": min_date.strftime('%Y-%m-%d') if min_date else None,
                "date_range_end": max_date.strftime('%Y-%m-%d') if max_date else None,
                "target_date": self.target_date.strftime('%Y-%m-%d'),
                "window_start": self.window_start_date.strftime('%Y-%m-%d'),
                "window_end": self.window_end_date.strftime('%Y-%m-%d')
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get processing summary: {str(e)}")
            return {}


# Backward compatibility alias
S3CsvCombiner = S3AuditProcessor