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
from mysql.connector import Error

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
        
        # Rolling window settings
        self.rolling_days = 3
        self.min_days_required = 2
        
        # Date calculations
        today = datetime.now().date()
        self.target_date = today - timedelta(days=1)  # Yesterday
        self.window_start = today - timedelta(days=self.rolling_days)
        self.window_end = self.target_date
        
        # Table names
        self.audit_table = "audit_reports"
        self.metadata_table = "audit_metadata"
        
        self.create_tables()

    def create_tables(self):
        """Create MySQL tables if not exist"""
        cursor = None
        try:
            cursor = self.dw_conn.cursor()
            
            audit_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.{self.audit_table} (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                sector VARCHAR(10) NOT NULL,
                flightNo INT NOT NULL,
                departureDate INT NOT NULL,
                source TINYINT NOT NULL COMMENT '0=ML, 1=WB',
                changedAtDateTime BIGINT NOT NULL COMMENT 'YYYYMMDDHHMM',
                analystName VARCHAR(100),
                `key` VARCHAR(100),
                value TEXT,
                INDEX idx_sector_flight (sector, flightNo, departureDate),
                INDEX idx_changed (changedAtDateTime),
                INDEX idx_source (source)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
            
            metadata_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.{self.metadata_table} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                type VARCHAR(50),
                timestamp DATETIME,
                last_processed_date INT,
                files_processed_count INT,
                INDEX idx_type_time (type, timestamp)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
            
            cursor.execute(audit_table_sql)
            cursor.execute(metadata_table_sql)
            self.dw_conn.commit()
            self.logger.info("Tables created successfully")
            
        except Error as e:
            self.logger.error(f"Failed to create tables: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    # ==================== DATE/TIME CONVERTERS ====================
    
    def to_date_int(self, date_str: str) -> int:
        """Convert YYYY-MM-DD to YYYYMMDD"""
        try:
            return int(datetime.strptime(date_str, '%Y-%m-%d').strftime('%Y%m%d'))
        except:
            return 0

    def to_datetime_int(self, iso_datetime: str) -> int:
        """Convert ISO datetime to YYYYMMDDHHMM"""
        try:
            if not iso_datetime or 'T' not in iso_datetime:
                return 0
            
            date_part, time_part = iso_datetime.split('T')
            year, month, day = date_part.split('-')
            hour, minute = time_part.split(':')[:2]
            
            return int(f"{year}{month}{day}{hour}{minute}")
        except:
            return 0

    def normalize_date(self, date_str: str) -> str:
        """Normalize various date formats to YYYY-MM-DD"""
        if not date_str or not isinstance(date_str, str):
            return ""
        
        try:
            parts = date_str.split('-')
            if len(parts) != 3:
                return ""
            
            # dd-mm-yyyy or d-mm-yyyy
            if len(parts[0]) <= 2:
                day, month, year = parts
                return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            
            # mm-dd-yyyy or m-dd-yyyy  
            if len(parts[1]) <= 2 and len(parts[2]) == 4:
                month, day, year = parts
                return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            
            return date_str
        except:
            return ""

    # ==================== METADATA & HELPERS ====================
    
    def is_first_run(self) -> bool:
        """Check if database is empty"""
        cursor = None
        try:
            cursor = self.dw_conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {self.schema}.{self.audit_table}")
            count = cursor.fetchone()[0]
            return count == 0
        except:
            return True
        finally:
            if cursor:
                cursor.close()

    def save_metadata(self, file_count: int):
        """Save processing run metadata"""
        cursor = None
        try:
            cursor = self.dw_conn.cursor()
            query = f"""
                INSERT INTO {self.schema}.{self.metadata_table}
                (type, timestamp, last_processed_date, files_processed_count)
                VALUES ('last_run', %s, %s, %s)
            """
            target_int = int(self.target_date.strftime('%Y%m%d'))
            cursor.execute(query, (datetime.now(), target_int, file_count))
            self.dw_conn.commit()
            self.logger.info(f"Saved metadata: {file_count} files processed")
        except Error as e:
            self.logger.error(f"Failed to save metadata: {e}")
        finally:
            if cursor:
                cursor.close()

    def get_date_range(self) -> Tuple[int, int]:
        """Get min/max dates from existing data"""
        cursor = None
        try:
            cursor = self.dw_conn.cursor()
            query = f"""
                SELECT 
                    FLOOR(MIN(changedAtDateTime) / 10000),
                    FLOOR(MAX(changedAtDateTime) / 10000)
                FROM {self.schema}.{self.audit_table}
            """
            cursor.execute(query)
            result = cursor.fetchone()
            if result and result[0] and result[1]:
                return int(result[0]), int(result[1])
            return None, None
        except:
            return None, None
        finally:
            if cursor:
                cursor.close()

    def cleanup_old_data(self, cutoff_date_int: int):
        """Delete records older than cutoff date"""
        cursor = None
        try:
            cursor = self.dw_conn.cursor()
            cutoff_datetime = cutoff_date_int * 10000  # YYYYMMDD to YYYYMMDD0000
            query = f"DELETE FROM {self.schema}.{self.audit_table} WHERE changedAtDateTime < %s"
            cursor.execute(query, (cutoff_datetime,))
            self.dw_conn.commit()
            self.logger.info(f"Deleted {cursor.rowcount} old records")
        except Error as e:
            self.logger.error(f"Failed cleanup: {e}")
        finally:
            if cursor:
                cursor.close()

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
        """Get ML audit parquet files"""
        files = []
        min_date = self.make_timezone_aware(
            datetime.combine(self.window_start if first_run else self.target_date, datetime.min.time())
        )
        
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
        
        print(f"Found {len(files)} ML files")
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
        """Process single ML parquet file"""
        try:
            # Read parquet from S3
            response = self.s3_client.get_object(Bucket=self.bucket, Key=file_key)
            df = pd.read_parquet(BytesIO(response['Body'].read()))
            
            records = []
            for _, row in df.iterrows():
                sector = row.get('sector', '')
                flight_no = int(row.get('flightNo', 0)) if row.get('flightNo') else 0
                dept_date = self.normalize_date(row.get('date', ''))
                changed_at = row.get('changedAt', '')
                analyst = row.get('userName', '') or row.get('analystName', '')
                
                dept_date_int = self.to_date_int(dept_date)
                changed_datetime_int = self.to_datetime_int(changed_at)
                
                # Each non-metadata column becomes a record
                for col, val in row.items():
                    if col in ['sector', 'flightNo', 'date', 'changedAt', 'userName', 'analystName']:
                        continue
                    if pd.isna(val):
                        continue
                    
                    records.append({
                        'sector': sector,
                        'flightNo': flight_no,
                        'departureDate': dept_date_int,
                        'source': 0,  # ML
                        'changedAtDateTime': changed_datetime_int,
                        'analystName': analyst,
                        'key': col,
                        'value': str(val) if val is not None else ''
                    })
            
            self.logger.info(f"Processed ML file {file_key}: {len(records)} records")
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
                    dept_date = part.split('=')[1]
            
            # Normalize dept_date from mm-dd-yyyy
            if dept_date:
                try:
                    m, d, y = dept_date.split('-')
                    dept_date = f"{y}-{m}-{d}"
                except:
                    pass
            
            # Read CSV from S3
            response = self.s3_client.get_object(Bucket=self.bucket, Key=file_key)
            df = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))
            
            records = []
            for _, row in df.iterrows():
                csv_sector = row.get('sector', sector)
                csv_flight = row.get('flightNo', flight_no)
                csv_date = self.normalize_date(row.get('date', dept_date))
                analyst = row.get('userName', '')
                changed_at = row.get('changedAt', '')
                
                flight_no_int = int(csv_flight) if csv_flight else 0
                dept_date_int = self.to_date_int(csv_date)
                changed_datetime_int = self.to_datetime_int(changed_at)
                
                # Each non-metadata column becomes a record
                for col, val in row.items():
                    if col in ['date', 'flightNo', 'sector', 'changeType', 'changedAt', 'userName']:
                        continue
                    if pd.isna(val):
                        continue
                    
                    records.append({
                        'sector': csv_sector,
                        'flightNo': flight_no_int,
                        'departureDate': dept_date_int,
                        'source': 1,  # WB
                        'changedAtDateTime': changed_datetime_int,
                        'analystName': analyst,
                        'key': col,
                        'value': str(val) if val is not None else ''
                    })
            
            self.logger.info(f"Processed WB file {file_key}: {len(records)} records")
            return records
            
        except Exception as e:
            self.logger.error(f"Failed WB file {file_key}: {e}")
            return []

    def process_all_files(self, first_run: bool) -> Tuple[List[Dict], int]:
        """Process all ML and WB files"""
        all_records = []
        file_count = 0
        
        # Process ML files
        for file_key in self.get_ml_files(first_run):
            records = self.process_ml_file(file_key)
            all_records.extend(records)
            file_count += 1
        
        # Process WB files
        for file_key in self.get_wb_files(first_run):
            records = self.process_wb_file(file_key)
            all_records.extend(records)
            file_count += 1
        
        print(f"Total: {len(all_records)} records from {file_count} files")
        return all_records, file_count

    # ==================== DATABASE OPERATIONS ====================
    
    def save_records(self, records: List[Dict], replace: bool = False):
        """Save records to MySQL"""
        cursor = None
        try:
            cursor = self.dw_conn.cursor()
            
            # Replace mode: truncate table
            if replace:
                cursor.execute(f"DELETE FROM {self.schema}.{self.audit_table}")
                print(f"Cleared {self.audit_table}")
            
            # Bulk insert
            if records:
                query = f"""
                    INSERT INTO {self.schema}.{self.audit_table}
                    (sector, flightNo, departureDate, source, changedAtDateTime, 
                     analystName, `key`, value)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                values = [
                    (r['sector'], r['flightNo'], r['departureDate'], r['source'],
                     r['changedAtDateTime'], r['analystName'], r['key'], r['value'])
                    for r in records
                ]
                
                cursor.executemany(query, values)
                self.dw_conn.commit()
                print(f"Saved {len(records)} records to MySQL")
            
        except Error as e:
            self.logger.error(f"Failed to save records: {e}")
            self.dw_conn.rollback()
            raise
        finally:
            if cursor:
                cursor.close()

    def get_summary(self) -> Dict:
        """Get processing summary"""
        cursor = None
        try:
            cursor = self.dw_conn.cursor()
            
            min_date, max_date = self.get_date_range()
            
            cursor.execute(f"SELECT COUNT(*) FROM {self.schema}.{self.audit_table}")
            total = cursor.fetchone()[0]
            
            cursor.execute(f"SELECT COUNT(*) FROM {self.schema}.{self.audit_table} WHERE source = 0")
            ml_count = cursor.fetchone()[0]
            
            cursor.execute(f"SELECT COUNT(*) FROM {self.schema}.{self.audit_table} WHERE source = 1")
            wb_count = cursor.fetchone()[0]
            
            return {
                'total_records': total,
                'ml_records': ml_count,
                'wb_records': wb_count,
                'date_range': f"{min_date} to {max_date}" if min_date else "No data",
                'target_date': int(self.target_date.strftime('%Y%m%d'))
            }
        except:
            return {}
        finally:
            if cursor:
                cursor.close()

    # ==================== MAIN RUN ====================
    
    def run(self):
        """Main execution flow"""
        try:
            print("=" * 60)
            print("S3 AUDIT PROCESSOR")
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
                
                # Check existing data
                min_date, max_date = self.get_date_range()
                if min_date and max_date:
                    min_obj = datetime.strptime(str(min_date), '%Y%m%d').date()
                    max_obj = datetime.strptime(str(max_date), '%Y%m%d').date()
                    days_span = (max_obj - min_obj).days + 1
                    
                    print(f"Existing: {min_date} to {max_date} ({days_span} days)")
                    
                    # Cleanup if we have enough data
                    if days_span >= self.min_days_required:
                        cutoff = int(self.window_start.strftime('%Y%m%d'))
                        self.cleanup_old_data(cutoff)
                    else:
                        print(f"Keeping all data ({days_span} < {self.min_days_required} days)")
                
                # Process new files
                records, file_count = self.process_all_files(first_run=False)
                
                if records:
                    self.save_records(records, replace=False)
                    self.save_metadata(file_count)
                else:
                    print("No new records found")
            
            # Final summary
            print("\n" + "=" * 60)
            summary = self.get_summary()
            print("SUMMARY:")
            for key, val in summary.items():
                print(f"  {key}: {val}")
            print("=" * 60)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Process failed: {e}")
            traceback.print_exc()
            return False