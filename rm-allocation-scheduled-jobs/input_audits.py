import boto3
import pandas as pd
from io import StringIO, BytesIO
from datetime import datetime, timedelta
import pytz
import logging
import re
from typing import List, Tuple, Dict, Any
import traceback
import configsettings
from sqlalchemy import text
from concurrent.futures import ThreadPoolExecutor, as_completed

class S3AuditProcessor:
    
    def __init__(self, logger, params, overrides, context):
        self.logger = logger or logging.getLogger()
        self.dw_conn = params['dwconn']
        self.schema = "RM_DW_DATA"
        
        # S3 Configuration
        self.bucket = configsettings.S3_BUCKET_NAME
        self.ml_prefix = configsettings.S3_FILE_PATH + "rm-admin/ml-audits/"
        self.wb_prefix = configsettings.S3_FILE_PATH + "rm-admin/audits/"
        self.s3_client = boto3.client('s3')
        
        # Performance settings
        self.max_workers = 20
        self.batch_size = 1000
        
        # Rolling window settings
        self.rolling_days = 90
        self.min_days_required = 85
        
        # Date calculations
        today = datetime.now().date()
        self.target_date = today - timedelta(days=1)
        self.window_start = today - timedelta(days=self.rolling_days)
        self.window_end = self.target_date
        
        # Table names
        self.audit_table = "audit_reports"
        self.metadata_table = "audit_metadata"
        
        self.create_tables()

    def create_tables(self):
        """Create MySQL tables if not exist"""
        try:
            with self.dw_conn.connect() as conn:
                audit_table_sql = text(f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.{self.audit_table} (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    sector VARCHAR(50) NOT NULL,
                    flightNo VARCHAR(50) NOT NULL,
                    departureDate INT NOT NULL,
                    source TINYINT NOT NULL COMMENT '0=ML, 1=WB',
                    changedAtDate INT NOT NULL COMMENT 'YYYYMMDD',
                    changedAtTime INT NOT NULL COMMENT 'HHMM',
                    analystName VARCHAR(100),
                    colName VARCHAR(100),
                    value TEXT,
                    INDEX idx_sector_flight (sector, flightNo, departureDate),
                    INDEX idx_changed_date (changedAtDate),
                    INDEX idx_source (source)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)
                
                metadata_table_sql = text(f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.{self.metadata_table} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    type VARCHAR(50),
                    timestamp DATETIME,
                    last_processed_date INT,
                    files_processed_count INT,
                    INDEX idx_type_time (type, timestamp)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """)
                
                conn.execute(audit_table_sql)
                conn.execute(metadata_table_sql)
                self.logger.info("Tables created successfully")
                
        except Exception as e:
            self.logger.error(f"Failed to create tables: {e}")
            raise

    # ==================== DATE/TIME CONVERTERS ====================
    
    def to_date_int(self, date_str: str) -> int:
        """Convert YYYY-MM-DD to YYYYMMDD"""
        try:
            return int(datetime.strptime(date_str, '%Y-%m-%d').strftime('%Y%m%d'))
        except:
            return 0

    def to_time_int(self, iso_datetime: str) -> int:
        """
        Extract time from ISO datetime and convert to 1HHMM (5 digits with leading 1)
        Examples:
            2026-01-06T04:32:53 → 10432
            2026-01-06T14:26:53 → 11426
            2025-10-29T10:42:53 → 11042
            2026-01-08T00:00:00 → 10000
        """
        try:
            if not iso_datetime or 'T' not in iso_datetime:
                return 0
            
            time_part = iso_datetime.split('T')[1]
            hour, minute = time_part.split(':')[:2]
            
            # Format: 1HHMM (5 digits with leading 1)
            return int(f"1{hour.zfill(2)}{minute.zfill(2)}")
            
        except Exception as e:
            self.logger.warning(f"Failed to convert time from '{iso_datetime}': {e}")
            return 0

    def normalize_date(self, date_str: str) -> str:
        """
        Normalize MM-DD-YYYY format to YYYY-MM-DD
        Both WB and ML use MM-DD-YYYY format
        Examples:
            01-24-2026 → 2026-01-24
            10-27-2026 → 2026-10-27
        """
        if not date_str or not isinstance(date_str, str):
            return ""
        
        try:
            date_str = date_str.strip()
            parts = date_str.split('-')
            
            if len(parts) != 3:
                return ""
            
            # yyyy-mm-dd (already correct)
            if len(parts[0]) == 4:
                return date_str
            
            # mm-dd-yyyy format (standard for both WB and ML)
            if len(parts[2]) == 4:
                month, day, year = parts
                return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            
            return ""
        except Exception as e:
            self.logger.warning(f"Failed to normalize date '{date_str}': {e}")
            return ""

    # ==================== METADATA & HELPERS ====================
    
    def is_first_run(self) -> bool:
        """Check if database is empty"""
        try:
            with self.dw_conn.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {self.schema}.{self.audit_table}"))
                count = result.fetchone()[0]
                return count == 0
        except:
            return True

    def save_metadata(self, file_count: int):
        """Save processing run metadata"""
        try:
            with self.dw_conn.connect() as conn:
                query = text(f"""
                    INSERT INTO {self.schema}.{self.metadata_table}
                    (type, timestamp, last_processed_date, files_processed_count)
                    VALUES ('last_run', :timestamp, :target_date, :file_count)
                """)
                
                target_int = int(self.target_date.strftime('%Y%m%d'))
                conn.execute(query, {
                    'timestamp': datetime.now(),
                    'target_date': target_int,
                    'file_count': file_count
                })
                self.logger.info(f"Saved metadata: {file_count} files processed")
        except Exception as e:
            self.logger.error(f"Failed to save metadata: {e}")

    def get_date_range(self) -> Tuple[int, int]:
        """Get min/max dates from existing data"""
        try:
            with self.dw_conn.connect() as conn:
                query = text(f"""
                    SELECT 
                        MIN(changedAtDate) as min_date,
                        MAX(changedAtDate) as max_date
                    FROM {self.schema}.{self.audit_table}
                    WHERE changedAtDate >= 19000101
                """)
                result = conn.execute(query)
                row = result.fetchone()
                
                if row and row[0] and row[1]:
                    min_date = int(row[0])
                    max_date = int(row[1])
                    
                    # Validation: ensure 8-digit dates
                    if len(str(min_date)) == 8 and len(str(max_date)) == 8:
                        return min_date, max_date
                    else:
                        self.logger.warning(f"Invalid date format: min={min_date}, max={max_date}")
                        return None, None
                        
                return None, None
                
        except Exception as e:
            self.logger.error(f"Failed to get date range: {e}")
            return None, None

    def cleanup_old_data(self, cutoff_date_int: int):
        """Delete records older than cutoff date"""
        try:
            with self.dw_conn.connect() as conn:
                query = text(f"""
                    DELETE FROM {self.schema}.{self.audit_table} 
                    WHERE changedAtDate < :cutoff
                """)
                result = conn.execute(query, {'cutoff': cutoff_date_int})
                self.logger.info(f"Deleted {result.rowcount} old records before {cutoff_date_int}")
        except Exception as e:
            self.logger.error(f"Failed cleanup: {e}")

    # ==================== S3 FILE DISCOVERY ====================
    
    def make_timezone_aware(self, dt: datetime) -> datetime:
        """Ensure datetime is timezone-aware"""
        return pytz.utc.localize(dt) if dt.tzinfo is None else dt

    def file_in_range(self, file_key: str, start: datetime.date, end: datetime.date) -> bool:
        """Check if file's date is within range"""
        try:
            # ML files: changedAtDate=YYYY-MM-DD
            if 'changedAtDate=' in file_key:
                match = re.search(r'changedAtDate=(\d{4}-\d{2}-\d{2})', file_key)
                if match:
                    file_date = datetime.strptime(match.group(1), '%Y-%m-%d').date()
                    return start <= file_date <= end
            
            # WB files: date=MM-DD-YYYY and filename YYYY-MM-DD.csv
            elif '/date=' in file_key:
                match = re.search(r'/date=(\d{2}-\d{2}-\d{4})/', file_key)
                if match:
                    m, d, y = match.group(1).split('-')
                    dept_date = datetime(int(y), int(m), int(d)).date()
                    
                    filename = file_key.split('/')[-1].replace('.csv', '')
                    try:
                        changed_date = datetime.strptime(filename, '%Y-%m-%d').date()
                        return (start <= dept_date <= end) or (start <= changed_date <= end)
                    except:
                        return start <= dept_date <= end
            
            return False
        except:
            return False

    def get_ml_files(self, first_run: bool) -> List[str]:
        """Get ML audit parquet files - INCLUDING CONNECTIONS"""
        files = []
        min_date = self.make_timezone_aware(
            datetime.combine(self.window_start if first_run else self.target_date, datetime.min.time())
        )
        
        # Include both direct AND connections
        for audit_type in ['direct', 'connections']:
            prefix = f"{self.ml_prefix}{audit_type}/"
            paginator = self.s3_client.get_paginator('list_objects_v2')
            
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                for obj in page.get('Contents', []):
                    if obj['Key'].endswith('.parquet'):
                        if first_run:
                            if self.file_in_range(obj['Key'], self.window_start, self.window_end):
                                files.append(obj['Key'])
                        else:
                            if obj['LastModified'] >= min_date:
                                files.append(obj['Key'])
        
        print(f"Found {len(files)} ML files (direct + connections)")
        return files

    def get_wb_files(self, first_run: bool) -> List[str]:
        """Get Workbench audit CSV files"""
        files = []
        min_date = self.make_timezone_aware(
            datetime.combine(self.window_start if first_run else self.target_date, datetime.min.time())
        )
        
        paginator = self.s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self.wb_prefix):
            for obj in page.get('Contents', []):
                if obj['Key'].endswith('.csv'):
                    if first_run:
                        if self.file_in_range(obj['Key'], self.window_start, self.window_end):
                            files.append(obj['Key'])
                    else:
                        if obj['LastModified'] >= min_date:
                            files.append(obj['Key'])
        
        print(f"Found {len(files)} WB files")
        return files

    # ==================== FILE PROCESSING ====================
    
    def process_ml_file(self, file_key: str) -> List[Dict]:
        """Process single ML parquet file - HANDLES CONNECTIONS"""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=file_key)
            df = pd.read_parquet(BytesIO(response['Body'].read()))
            
            records = []
            for _, row in df.iterrows():
                # Get sector and flight info (could be connections like "619-1528")
                sector = str(row.get('sector', ''))[:50]
                flight_no = str(row.get('flightNo', ''))[:50]
                dept_date = str(row.get('date', ''))  # MM-DD-YYYY format
                changed_at = row.get('changedAt', '')
                analyst = str(row.get('userName', '') or row.get('analystName', ''))[:100]
                
                # Normalize date from MM-DD-YYYY to YYYY-MM-DD
                normalized_date = self.normalize_date(dept_date)
                dept_date_int = self.to_date_int(normalized_date)
                
                # Extract date and time separately
                changed_at_date_int = self.to_date_int(changed_at.split('T')[0]) if 'T' in changed_at else 0
                changed_at_time_int = self.to_time_int(changed_at)
                
                # FILTER: Only include audits where departureDate < changedAtDate
                # (i.e., the flight has already departed when the audit was created)
                if dept_date_int >= changed_at_date_int:
                    continue
                
                # Skip connection-specific metadata fields
                skip_columns = [
                    'sector', 'flightNo', 'date', 'changedAt', 'userName', 'analystName',
                    'sector1', 'sector2', 'flight1', 'flight2'
                ]
                
                # Process each column as a potential change
                for col, val in row.items():
                    if col in skip_columns:
                        continue
                    if pd.isna(val):
                        continue
                    
                    records.append({
                        'sector': sector,
                        'flightNo': flight_no,
                        'departureDate': dept_date_int,
                        'source': 0,
                        'changedAtDate': changed_at_date_int,
                        'changedAtTime': changed_at_time_int,
                        'analystName': analyst,
                        'colName': col,
                        'value': str(val) if val is not None else ''
                    })
            
            return records
            
        except Exception as e:
            self.logger.error(f"Failed ML file {file_key}: {e}")
            return []

    def process_wb_file(self, file_key: str) -> List[Dict]:
        """Process single WB CSV file"""
        try:
            # Extract metadata from path
            path_parts = file_key.split('/')
            sector = flight_no = dept_date = ''
            
            for part in path_parts:
                if part.startswith('sector='):
                    sector = part.split('=')[1]
                elif part.startswith('flightNo='):
                    flight_no = part.split('=')[1]
                elif part.startswith('date='):
                    dept_date = part.split('=')[1]  # MM-DD-YYYY format
            
            # Read CSV from S3
            response = self.s3_client.get_object(Bucket=self.bucket, Key=file_key)
            df = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))
            
            records = []
            for _, row in df.iterrows():
                csv_sector = str(row.get('sector', sector))[:50]
                csv_flight = str(row.get('flightNo', flight_no))[:50]
                csv_date = str(row.get('date', dept_date))  # MM-DD-YYYY format
                analyst = str(row.get('userName', ''))[:100]
                changed_at = row.get('changedAt', '')
                
                # Normalize date from MM-DD-YYYY to YYYY-MM-DD
                normalized_date = self.normalize_date(csv_date)
                dept_date_int = self.to_date_int(normalized_date)
                
                # Extract date and time separately
                changed_at_date_int = self.to_date_int(changed_at.split('T')[0]) if 'T' in changed_at else 0
                changed_at_time_int = self.to_time_int(changed_at)
                
                # FILTER: Only include audits where departureDate < changedAtDate
                # (i.e., the flight has already departed when the audit was created)
                if dept_date_int >= changed_at_date_int:
                    continue
                
                for col, val in row.items():
                    if col in ['date', 'flightNo', 'sector', 'changeType', 'changedAt', 'userName']:
                        continue
                    if pd.isna(val):
                        continue
                    
                    records.append({
                        'sector': csv_sector,
                        'flightNo': csv_flight,
                        'departureDate': dept_date_int,
                        'source': 1,
                        'changedAtDate': changed_at_date_int,
                        'changedAtTime': changed_at_time_int,
                        'analystName': analyst,
                        'colName': col,
                        'value': str(val) if val is not None else ''
                    })
            
            return records
            
        except Exception as e:
            self.logger.error(f"Failed WB file {file_key}: {e}")
            return []

    def process_all_files(self, first_run: bool) -> Tuple[List[Dict], int]:
        """Process all files in parallel"""
        all_records = []
        
        # Get file lists
        ml_files = self.get_ml_files(first_run)
        wb_files = self.get_wb_files(first_run)
        
        print(f"Processing {len(ml_files)} ML + {len(wb_files)} WB files in parallel...")
        
        # Process ML files in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            ml_futures = {executor.submit(self.process_ml_file, f): f for f in ml_files}
            
            completed = 0
            for future in as_completed(ml_futures):
                try:
                    records = future.result()
                    all_records.extend(records)
                    completed += 1
                    if completed % 50 == 0:
                        print(f"  Processed {completed}/{len(ml_files)} ML files...")
                except Exception as e:
                    self.logger.error(f"ML file processing failed: {e}")
        
        # Process WB files in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            wb_futures = {executor.submit(self.process_wb_file, f): f for f in wb_files}
            
            completed = 0
            for future in as_completed(wb_futures):
                try:
                    records = future.result()
                    all_records.extend(records)
                    completed += 1
                    if completed % 100 == 0:
                        print(f"  Processed {completed}/{len(wb_files)} WB files...")
                except Exception as e:
                    self.logger.error(f"WB file processing failed: {e}")
        
        file_count = len(ml_files) + len(wb_files)
        print(f"Total: {len(all_records)} records from {file_count} files")
        return all_records, file_count

    # ==================== DATABASE OPERATIONS ====================
    
    def save_records(self, records: List[Dict], replace: bool = False):
        """Save records to MySQL with batching"""
        try:
            with self.dw_conn.connect() as conn:
                if replace:
                    conn.execute(text(f"DELETE FROM {self.schema}.{self.audit_table}"))
                    print(f"Cleared {self.audit_table}")
                
                if records:
                    query = text(f"""
                        INSERT INTO {self.schema}.{self.audit_table}
                        (sector, flightNo, departureDate, source, changedAtDate, changedAtTime,
                        analystName, colName, value)
                        VALUES (:sector, :flightNo, :departureDate, :source, 
                                :changedAtDate, :changedAtTime, :analystName, :colName, :value)
                    """)
                    
                    # Batch insert for better performance
                    total_inserted = 0
                    for i in range(0, len(records), self.batch_size):
                        batch = records[i:i + self.batch_size]
                        conn.execute(query, batch)
                        total_inserted += len(batch)
                        if total_inserted % (self.batch_size * 5) == 0:
                            print(f"  Inserted {total_inserted}/{len(records)} records...")
                    
                    print(f"Saved {len(records)} records to MySQL")
                
        except Exception as e:
            self.logger.error(f"Failed to save records: {e}")
            raise

    def get_summary(self) -> Dict:
        """Get processing summary"""
        try:
            with self.dw_conn.connect() as conn:
                min_date, max_date = self.get_date_range()
                
                result = conn.execute(text(f"SELECT COUNT(*) FROM {self.schema}.{self.audit_table}"))
                total = result.fetchone()[0]
                
                result = conn.execute(text(f"SELECT COUNT(*) FROM {self.schema}.{self.audit_table} WHERE source = 0"))
                ml_count = result.fetchone()[0]
                
                result = conn.execute(text(f"SELECT COUNT(*) FROM {self.schema}.{self.audit_table} WHERE source = 1"))
                wb_count = result.fetchone()[0]
                
                # Count connections vs direct
                result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM {self.schema}.{self.audit_table} 
                    WHERE source = 0 AND flightNo LIKE '%-%'
                """))
                connection_count = result.fetchone()[0]
                
                result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM {self.schema}.{self.audit_table} 
                    WHERE source = 0 AND flightNo NOT LIKE '%-%'
                """))
                direct_count = result.fetchone()[0]
                
                return {
                    'total_records': total,
                    'ml_records': ml_count,
                    'ml_direct': direct_count,
                    'ml_connections': connection_count,
                    'wb_records': wb_count,
                    'date_range': f"{min_date} to {max_date}" if min_date else "No data",
                    'target_date': int(self.target_date.strftime('%Y%m%d'))
                }
        except:
            return {}

    # ==================== MAIN RUN ====================
    
    def run(self):
        """Main execution flow"""
        try:
            start_time = datetime.now()
            print("=" * 60)
            print("S3 AUDIT PROCESSOR (Direct + Connections)")
            print("=" * 60)
            
            first_run = self.is_first_run()
            
            if first_run:
                print(f"\n>>> FIRST RUN - Loading {self.rolling_days} days")
                print(f"Date range: {self.window_start} to {self.window_end}\n")
                
                records, file_count = self.process_all_files(first_run=True)
                
                if not records:
                    print("No records found for initial load")
                    return False
                
                self.save_records(records, replace=True)
                self.save_metadata(file_count)
                
            else:
                print(f"\n>>> ROLLING RUN - Processing {self.target_date}\n")
                
                min_date, max_date = self.get_date_range()
                if min_date and max_date:
                    # Validate date format before parsing
                    if len(str(min_date)) != 8 or len(str(max_date)) != 8:
                        self.logger.error(f"Invalid date format: min={min_date}, max={max_date}")
                        print(f"ERROR: Invalid data in database. Please run cleanup.")
                        return False
                    
                    min_obj = datetime.strptime(str(min_date), '%Y%m%d').date()
                    max_obj = datetime.strptime(str(max_date), '%Y%m%d').date()
                    days_span = (max_obj - min_obj).days + 1
                    
                    print(f"Existing: {min_date} to {max_date} ({days_span} days)")
                    
                    if days_span >= self.min_days_required:
                        cutoff = int(self.window_start.strftime('%Y%m%d'))
                        self.cleanup_old_data(cutoff)
                    else:
                        print(f"Keeping all data ({days_span} < {self.min_days_required} days)")
                
                records, file_count = self.process_all_files(first_run=False)
                
                if records:
                    self.save_records(records, replace=False)
                    self.save_metadata(file_count)
                else:
                    print("No new records found")
            
            print("\n" + "=" * 60)
            summary = self.get_summary()
            print("SUMMARY:")
            for key, val in summary.items():
                print(f"  {key}: {val}")
            
            elapsed = (datetime.now() - start_time).total_seconds()
            print(f"  processing_time: {elapsed:.2f} seconds")
            print("=" * 60)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Process failed: {e}")
            traceback.print_exc()
            return False