import pandas as pd
import numpy as np
import logging
import time
from sqlalchemy import text, create_engine
from datetime import datetime, timedelta
from typing import Tuple, List, Dict, Optional
from heuristicCalc import FareStatisticalAnalysis

logger = logging.getLogger(__name__)


class CalUpsell:
    """Optimized fare data extraction and management class."""
    
    CACHE_TTL_SECONDS = 300
    DEFAULT_CHUNK_SIZE = 10000
    LOOKBACK_DAYS = 395  # 365 + 30 days
    MIN_MONTHS_FOR_ROLLING = 13
    
    def __init__(self, logger: logging.Logger, params: Dict, overrides: Dict, context: Dict):
        """Initialize with required connections and dynamic date calculation."""
        self.logger = logger
        self.rm_rd_conn = params['wrconn']
        self.dw_conn = params['dwconn']
        self.esb_conn = params['rdEsbConn']
        
        # Calculate dynamic dates
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        self.data_start = (today - timedelta(days=self.LOOKBACK_DAYS)).replace(day=1)
        self.historical_start = today - timedelta(days=30)
        self.historical_end = today - timedelta(days=1)
        
        # Cache
        self._table_empty_cache = None
        self._cache_timestamp = None
        
        # call heuristic calc
        self.calc = FareStatisticalAnalysis(logger, params, {}, {})
        
    @property
    def _is_cache_valid(self) -> bool:
        """Check if cache is still valid."""
        return (
            self._cache_timestamp is not None and 
            (datetime.now() - self._cache_timestamp).total_seconds() < self.CACHE_TTL_SECONDS
        )

    def get_table_date_range(self) -> Tuple[Optional[datetime], Optional[datetime], int]:
        """Get the current date range and month count in the table."""
        query = """
        SELECT 
            MIN(DepartureDate) as min_date,
            MAX(DepartureDate) as max_date,
            COUNT(DISTINCT DATE_FORMAT(DepartureDate, '%%Y-%%m')) as month_count,
            COUNT(*) as total_records
        FROM QP_DW_RMALLOC.calendar_AF
        """
        try:
            result = pd.read_sql(query, self.rm_rd_conn)
            
            if result.empty or result['total_records'].iloc[0] == 0:
                return None, None, 0
                
            min_date = pd.to_datetime(result['min_date'].iloc[0]) if result['min_date'].iloc[0] else None
            max_date = pd.to_datetime(result['max_date'].iloc[0]) if result['max_date'].iloc[0] else None
            month_count = result['month_count'].iloc[0] or 0
            
            self.logger.info(f"Table contains {month_count} months from {min_date} to {max_date}")
            return min_date, max_date, month_count
            
        except Exception as e:
            self.logger.error(f"Error getting table date range: {e}")
            return None, None, 0

    @staticmethod
    def get_last_completed_month() -> Tuple[datetime, datetime]:
        """Get the last completed month (previous month)."""
        today = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        
        if today.month == 1:
            first_day = datetime(today.year - 1, 12, 1)
        else:
            first_day = datetime(today.year, today.month - 1, 1)
        
        if first_day.month == 12:
            last_day = datetime(first_day.year + 1, 1, 1) - timedelta(days=1)
        else:
            last_day = datetime(first_day.year, first_day.month + 1, 1) - timedelta(days=1)
        
        return first_day, last_day

    def get_oldest_month_range(self) -> Tuple[Optional[datetime], Optional[datetime]]:
        """Get the date range of the oldest month in the table."""
        query = """
        SELECT 
            DATE_FORMAT(MIN(DepartureDate), '%%Y-%%m-01') as oldest_month_start,
            LAST_DAY(MIN(DepartureDate)) as oldest_month_end
        FROM QP_DW_RMALLOC.calendar_AF
        """
        try:
            result = pd.read_sql(query, self.rm_rd_conn)
            
            if result.empty or pd.isna(result['oldest_month_start'].iloc[0]):
                return None, None
            
            oldest_start = pd.to_datetime(result['oldest_month_start'].iloc[0])
            oldest_end = pd.to_datetime(result['oldest_month_end'].iloc[0])
            
            return oldest_start, oldest_end
            
        except Exception as e:
            self.logger.error(f"Error getting oldest month range: {e}")
            return None, None

    def remove_month_data(self, start_date: datetime, end_date: datetime) -> int:
        """Remove data for a specific month range."""
        delete_query = """
        DELETE FROM QP_DW_RMALLOC.calendar_AF 
        WHERE DepartureDate >= :start_date AND DepartureDate <= :end_date
        """
        
        try:
            with self.rm_rd_conn.connect() as connection:
                result = connection.execute(
                    text(delete_query),
                    {'start_date': start_date, 'end_date': end_date}
                )
                rows_deleted = result.rowcount
            
            self.logger.info(f"Removed {rows_deleted} records from {start_date.strftime('%Y-%m')} month")
            return rows_deleted
            
        except Exception as e:
            self.logger.error(f"Error removing month data: {e}")
            raise

    def remove_target_month_if_exists(self, start_date: datetime, end_date: datetime) -> int:
        """Remove existing data for the target month to prevent duplicates."""
        check_query = """
        SELECT COUNT(*) as count FROM QP_DW_RMALLOC.calendar_AF
        WHERE DepartureDate >= :start_date AND DepartureDate <= :end_date
        """
        
        try:
            # Check if target month data exists
            with self.rm_rd_conn.connect() as connection:
                result = connection.execute(
                    text(check_query),
                    {'start_date': start_date, 'end_date': end_date}
                )
                existing_count = result.fetchone()[0]
            
            if existing_count > 0:
                self.logger.info(f"Found {existing_count} existing records for target month {start_date.strftime('%Y-%m')}, removing them")
                return self.remove_month_data(start_date, end_date)
            else:
                self.logger.info(f"No existing data found for target month {start_date.strftime('%Y-%m')}")
                return 0
                
        except Exception as e:
            self.logger.error(f"Error checking/removing target month data: {e}")
            return 0

    def extract_fare_data_with_sectors(self, start_date: datetime, end_date: datetime) -> Tuple[pd.DataFrame, List[str]]:
        """Extract fare data and return both the data and unique sectors found."""
        query = """
        SELECT 
            CONCAT(PX.Origin, PX.Destination) AS Sector,
            PX.DepartureDate,
            SUM(PX.DiscountedBaseFare_Pro + PX.YQ_Pro + PX.RCS + PX.CUTE) AS TotalFare,
            COUNT(CASE WHEN SUBSTRING(PX.FareBasis, 1, 1) != 'S' 
                THEN PX.PassengerID END) AS PassengerCount
        FROM QP_DL_NAVDATA.QP_DL_FLOWNDATAPAX PX
        WHERE PX.DepartureDate >= %s
        AND PX.DepartureDate < %s
        AND PX.BookingID IS NOT NULL
        AND SUBSTRING(PX.FareBasis, 5, 1) != 'T'
        AND PX.FlightNumber < 6000
        GROUP BY PX.Origin, PX.Destination, PX.DepartureDate
        HAVING PassengerCount > 0
        """
        
        try:
            df = pd.read_sql(query, self.esb_conn, 
                           params=[start_date.strftime('%Y-%m-%d'), 
                                  (end_date + timedelta(days=1)).strftime('%Y-%m-%d')])
            
            sectors = []
            if not df.empty:
                df['AverageFare'] = df['TotalFare'] / df['PassengerCount']
                sectors = df['Sector'].unique().tolist()
                df = df[['Sector', 'DepartureDate', 'AverageFare']].copy()
            
            return df, sectors
        except Exception as e:
            self.logger.error(f"Error extracting fare data: {e}")
            raise

    def get_existing_sectors(self) -> List[str]:
        """Get existing sectors from current data."""
        query = """
        SELECT DISTINCT Sector FROM QP_DW_RMALLOC.calendar_AF
        WHERE DepartureDate >= %s
        ORDER BY Sector
        """
        
        try:
            recent_date = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
            result = pd.read_sql(query, self.rm_rd_conn, params=[recent_date])
            return result['Sector'].tolist() if not result.empty else []
        except Exception as e:
            self.logger.warning(f"Could not fetch existing sectors: {e}")
            return []

    def create_complete_month_data(self, start_date: datetime, end_date: datetime, 
                                  actual_data: pd.DataFrame, sectors: List[str]) -> pd.DataFrame:
        """Create complete month dataset with all dates filled."""
        try:
            if not sectors:
                self.logger.warning("No sectors provided for complete month data")
                return pd.DataFrame(columns=['Sector', 'DepartureDate', 'AverageFare'])
            
            self.logger.info(f"Creating complete month data for {len(sectors)} sectors from {start_date.date()} to {end_date.date()}")
            
            # Create date range as a list of date objects (not datetime)
            date_range = pd.date_range(start=start_date.date(), end=end_date.date(), freq='D')
            date_list = [d.date() for d in date_range]
            
            self.logger.info(f"Date range contains {len(date_list)} days")
            
            # Create records using list comprehension for better control
            records = []
            for sector in sectors:
                for date_obj in date_list:
                    records.append({
                        'Sector': str(sector),  # Ensure string type
                        'DepartureDate': date_obj,  # Use date object, not datetime
                        'AverageFare': 0.0
                    })
            
            # Create DataFrame from records
            complete_df = pd.DataFrame(records)
            
            if complete_df.empty:
                self.logger.error("Failed to create DataFrame from records")
                return pd.DataFrame(columns=['Sector', 'DepartureDate', 'AverageFare'])
            
            self.logger.info(f"Created DataFrame with {len(complete_df)} rows")
            
            # Merge with actual data if available
            if not actual_data.empty:
                try:
                    actual_processed = actual_data.copy()
                    
                    # Normalize actual data date format
                    if pd.api.types.is_datetime64_dtype(actual_processed['DepartureDate']):
                        actual_processed['DepartureDate'] = actual_processed['DepartureDate'].dt.date
                    elif isinstance(actual_processed['DepartureDate'].iloc[0], str):
                        actual_processed['DepartureDate'] = pd.to_datetime(actual_processed['DepartureDate']).dt.date
                    
                    # Simple merge approach
                    complete_df = complete_df.set_index(['Sector', 'DepartureDate'])
                    actual_processed = actual_processed.set_index(['Sector', 'DepartureDate'])
                    
                    # Update with actual values where they exist
                    complete_df.update(actual_processed)
                    complete_df = complete_df.reset_index()
                    
                    updated_count = (complete_df['AverageFare'] > 0).sum()
                    self.logger.info(f"Updated {updated_count} rows with actual fare data")
                    
                except Exception as merge_error:
                    self.logger.error(f"Error merging actual data: {merge_error}")
                    self.logger.info("Proceeding with zero-filled data only")
            
            # Final validation before returning
            if complete_df.empty:
                self.logger.error("DataFrame became empty after processing")
                return pd.DataFrame(columns=['Sector', 'DepartureDate', 'AverageFare'])
            
            # Ensure proper column order
            complete_df = complete_df[['Sector', 'DepartureDate', 'AverageFare']].copy()
            
            self.logger.info(f"Returning complete dataset with {len(complete_df)} rows")
            return complete_df
            
        except Exception as e:
            self.logger.error(f"Critical error in create_complete_month_data: {str(e)[:200]}")
            # Return empty DataFrame instead of raising to prevent cascade failures
            return pd.DataFrame(columns=['Sector', 'DepartureDate', 'AverageFare'])

    def insert_fare_data(self, df: pd.DataFrame) -> int:
        """Insert fare data into calendar_AF table with robust error handling."""
        if df.empty:
            self.logger.warning("DataFrame is empty, nothing to insert")
            return 0
        
        try:
            # Validate DataFrame structure first
            if not isinstance(df, pd.DataFrame):
                raise ValueError(f"Expected DataFrame, got {type(df)}")
            
            expected_columns = ['Sector', 'DepartureDate', 'AverageFare']
            if not all(col in df.columns for col in expected_columns):
                raise ValueError(f"Missing columns. Expected: {expected_columns}, Got: {list(df.columns)}")
            
            df_insert = df[expected_columns].copy()
            
            # Check for data corruption
            if len(df_insert) == 0:
                self.logger.warning("DataFrame has no rows after column selection")
                return 0
            
            # Log DataFrame state before processing
            self.logger.info(f"Pre-processing: {len(df_insert)} rows, columns: {list(df_insert.columns)}")
            
            # Check if DataFrame got corrupted (common sign: wrong data types)
            try:
                sample_sector = df_insert['Sector'].iloc[0]
                sample_date = df_insert['DepartureDate'].iloc[0]
                sample_fare = df_insert['AverageFare'].iloc[0]
                
                self.logger.info(f"Sample row - Sector: {type(sample_sector).__name__}:{sample_sector}, "
                               f"Date: {type(sample_date).__name__}:{sample_date}, "
                               f"Fare: {type(sample_fare).__name__}:{sample_fare}")
                
            except Exception as sample_error:
                self.logger.error(f"DataFrame appears corrupted - cannot access sample data: {sample_error}")
                raise ValueError("DataFrame structure is corrupted")
            
            # Ensure proper data types
            df_insert['AverageFare'] = pd.to_numeric(df_insert['AverageFare'], errors='coerce')
            
            # Handle date conversion carefully
            if not pd.api.types.is_datetime64_dtype(df_insert['DepartureDate']):
                try:
                    df_insert['DepartureDate'] = pd.to_datetime(df_insert['DepartureDate'])
                except Exception as date_error:
                    self.logger.error(f"Date conversion failed: {date_error}")
                    raise ValueError("Cannot convert DepartureDate to datetime")
            
            # Final validation
            if df_insert.isna().any().any():
                na_summary = df_insert.isna().sum()
                self.logger.warning(f"Found NaN values: {na_summary.to_dict()}")
                df_insert = df_insert.dropna()
                self.logger.info(f"After dropping NaN: {len(df_insert)} rows remain")
            
            if len(df_insert) == 0:
                self.logger.warning("No valid data remaining after cleaning")
                return 0
            
            # Log final state before insertion
            self.logger.info(f"Final validation - Shape: {df_insert.shape}, "
                           f"Date range: {df_insert['DepartureDate'].min()} to {df_insert['DepartureDate'].max()}, "
                           f"Sectors: {df_insert['Sector'].nunique()}")
            
            # Insert with error handling
            chunk_size = min(self.DEFAULT_CHUNK_SIZE, len(df_insert))
            
            df_insert.to_sql(
                'calendar_AF', 
                self.rm_rd_conn, 
                schema='QP_DW_RMALLOC',
                if_exists='append', 
                index=False, 
                chunksize=chunk_size,
                method='multi'
            )
            
            self.logger.info(f"Successfully inserted {len(df_insert)} records")
            return len(df_insert)
            
        except ValueError as ve:
            self.logger.error(f"Data validation error: {ve}")
            return 0
        except Exception as e:
            self.logger.error(f"Insertion failed: {str(e)[:200]}...")  # Truncate long error messages
            self.logger.error(f"DataFrame type: {type(df)}, Shape: {getattr(df, 'shape', 'unknown')}")
            return 0

    def perform_rolling_update(self) -> Dict:
        """Perform the rolling monthly update - core logic."""
        # Get current table state
        min_date, max_date, month_count = self.get_table_date_range()
        
        # Get target month (last completed month)
        target_start, target_end = self.get_last_completed_month()
        
        self.logger.info(f"Rolling update: Current table has {month_count} months")
        self.logger.info(f"Target month: {target_start.strftime('%Y-%m')} ({target_start.date()} to {target_end.date()})")
        
        # Extract data for target month
        actual_data, new_sectors = self.extract_fare_data_with_sectors(target_start, target_end)
        existing_sectors = self.get_existing_sectors()
        all_sectors = list(set(new_sectors + existing_sectors))
        
        # Create complete month dataset
        complete_month_data = self.create_complete_month_data(target_start, target_end, actual_data, all_sectors)
        
        if complete_month_data.empty:
            self.logger.warning("No data created for target month")
            return {
                "action": "no_data",
                "records_added": 0,
                "records_removed": 0,
                "target_month": target_start.strftime('%Y-%m')
            }
        
        rows_removed_total = 0
        
        # Step 1: Remove existing target month data to prevent duplicates
        target_rows_removed = self.remove_target_month_if_exists(target_start, target_end)
        rows_removed_total += target_rows_removed
        
        # Step 2: Remove oldest month if we have enough months (after accounting for target month removal)
        adjusted_month_count = month_count - (1 if target_rows_removed > 0 else 0)
        if adjusted_month_count >= self.MIN_MONTHS_FOR_ROLLING:
            oldest_start, oldest_end = self.get_oldest_month_range()
            if oldest_start and oldest_end:
                # Only remove if it's different from target month
                if oldest_start.strftime('%Y-%m') != target_start.strftime('%Y-%m'):
                    self.logger.info(f"Removing oldest month: {oldest_start.strftime('%Y-%m')}")
                    oldest_rows_removed = self.remove_month_data(oldest_start, oldest_end)
                    rows_removed_total += oldest_rows_removed
                else:
                    self.logger.info("Oldest month is same as target month (already removed)")
        else:
            self.logger.info(f"Table has only {adjusted_month_count} effective months, skipping oldest month removal")
        
        # Step 3: Insert new month data
        records_added = self.insert_fare_data(complete_month_data)
        
        return {
            "action": "rolling_update_completed",
            "records_added": records_added,
            "records_removed": rows_removed_total,
            "target_month": target_start.strftime('%Y-%m'),
            "months_in_table": month_count,
            "target_month_existed": target_rows_removed > 0
        }

    def perform_initial_load(self) -> Dict:
        """Perform initial data load for empty table."""
        self.logger.info("Performing initial data load")
        
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Extract data for the entire lookback period
        all_sectors = set()
        monthly_data = []
        
        current_date = self.data_start
        while current_date < today:
            # Calculate month boundaries
            first_day = current_date.replace(day=1)
            if current_date.month == 12:
                next_month = datetime(current_date.year + 1, 1, 1)
            else:
                next_month = datetime(current_date.year, current_date.month + 1, 1)
            
            month_end = min(next_month - timedelta(days=1), today - timedelta(days=1))
            
            # Extract data for this month
            actual_df, month_sectors = self.extract_fare_data_with_sectors(first_day, month_end)
            
            all_sectors.update(month_sectors)
            if not actual_df.empty:
                monthly_data.append(actual_df)
            
            current_date = next_month
        
        if not monthly_data:
            self.logger.warning("No data found for initial load")
            return {
                "action": "initial_load_no_data",
                "records_added": 0
            }
        
        # Combine all monthly data
        combined_df = pd.concat(monthly_data, ignore_index=True)
        complete_df = self.create_complete_month_data(
            self.data_start, 
            today - timedelta(days=1), 
            combined_df, 
            list(all_sectors)
        )
        
        records_added = self.insert_fare_data(complete_df)
        
        return {
            "action": "initial_load_completed",
            "records_added": records_added
        }

    def run(self) -> Dict:
        """Main method to run fare data extraction with simplified logic."""
        start_time = time.time()
        results = {
            "status": "success",
            "total_records": 0,
            "extraction_type": "unknown",
            "execution_time_seconds": 0,
            "rows_removed": 0,
            "action_details": {},
            "error_summary": None
        }
        
        try:
            # Check current table state
            min_date, max_date, month_count = self.get_table_date_range()
            
            if month_count == 0:
                # Empty table - perform initial load
                self.logger.info("Table is empty - performing initial load")
                action_result = self.perform_initial_load()
                results["extraction_type"] = "initial_load"
            else:
                # Table has data - perform rolling update
                self.logger.info("Table has data - performing rolling update")
                action_result = self.perform_rolling_update()
                results["extraction_type"] = "rolling_update"
            
            # Update results with action details
            results["action_details"] = action_result
            results["total_records"] = action_result.get("records_added", 0)
            results["rows_removed"] = action_result.get("records_removed", 0)
            
            # Check if operation was successful
            if action_result.get("action") in ["no_data", "no_update_needed"]:
                results["status"] = "completed_no_changes"
            elif action_result.get("records_added", 0) > 0:
                results["status"] = "success"
            else:
                results["status"] = "completed_with_issues"
                results["error_summary"] = "No records were inserted"
            
            self.logger.info(f"Operation completed: {action_result}")
                
        except Exception as e:
            results["status"] = "error"
            error_msg = str(e)
            # Truncate very long error messages but keep the essential part
            if len(error_msg) > 500:
                error_msg = error_msg[:250] + " ... " + error_msg[-250:]
            
            results["error_summary"] = error_msg
            self.logger.error(f"EXTRACTION FAILED: {error_msg}")
            
            # Don't re-raise - let the statistical analysis still run
        
        finally:
            # Always run statistical analysis
            try:
                self.logger.info("Running statistical analysis")
                self.calc.run()
                self.logger.info("Statistical analysis completed successfully")
            except Exception as calc_error:
                calc_error_msg = str(calc_error)[:200] + "..." if len(str(calc_error)) > 200 else str(calc_error)
                self.logger.error(f"Statistical analysis failed: {calc_error_msg}")
                if results["status"] == "success":
                    results["status"] = "success_with_calc_error"
                
            results["execution_time_seconds"] = round(time.time() - start_time, 2)
        
        return results