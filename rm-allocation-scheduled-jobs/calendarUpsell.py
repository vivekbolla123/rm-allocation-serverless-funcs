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

    def check_table_empty(self) -> bool:
        """Check if calendar_AF table is empty with caching."""
        if self._is_cache_valid and self._table_empty_cache is not None:
            return self._table_empty_cache
            
        query = "SELECT COUNT(*) as count FROM QP_DW_RMALLOC.calendar_AF LIMIT 1"
        try:
            result = pd.read_sql(query, self.rm_rd_conn)
            is_empty = result['count'].iloc[0] == 0
            
            self._table_empty_cache = is_empty
            self._cache_timestamp = datetime.now()
            return is_empty
        except Exception as e:
            self.logger.error(f"Error checking if table is empty: {e}")
            return True

    def check_historical_data_exists(self) -> bool:
        """Check if required historical data exists for analysis."""
        query = """
        SELECT COUNT(*) as count FROM QP_DW_RMALLOC.calendar_AF
        WHERE DepartureDate >= %s AND DepartureDate <= %s
        """
        try:
            result = pd.read_sql(query, self.rm_rd_conn, 
                               params=[self.historical_start.strftime('%Y-%m-%d'), 
                                      self.historical_end.strftime('%Y-%m-%d')])
            return result['count'].iloc[0] > 0
        except Exception as e:
            self.logger.error(f"Error checking historical data: {e}")
            return False

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

    def remove_oldest_month(self) -> int:
        """Remove the oldest month from calendar_AF table."""
        try:
            oldest_month_query = """
            SELECT DATE_FORMAT(MIN(DepartureDate), '%Y-%m-01') as oldest_month_start,
                   LAST_DAY(MIN(DepartureDate)) as oldest_month_end
            FROM QP_DW_RMALLOC.calendar_AF
            """
            
            result = pd.read_sql(oldest_month_query, self.rm_rd_conn)
            
            if result.empty or pd.isna(result['oldest_month_start'].iloc[0]):
                return 0
            
            oldest_month_start = result['oldest_month_start'].iloc[0]
            oldest_month_end = result['oldest_month_end'].iloc[0]
            
            delete_query = """
            DELETE FROM QP_DW_RMALLOC.calendar_AF 
            WHERE DepartureDate >= %s AND DepartureDate <= %s
            """
            
            with self.rm_rd_conn.connect() as connection:
                result = connection.execute(
                    text(delete_query),
                    {'start_date': oldest_month_start, 'end_date': oldest_month_end}
                )
                rows_deleted = result.rowcount
                connection.commit()
            
            self.logger.info(f"Removed {rows_deleted} records from oldest month: {oldest_month_start}")
            return rows_deleted
            
        except Exception as e:
            self.logger.error(f"Error removing oldest month: {e}")
            raise

    def check_month_exists(self, month_start: datetime, month_end: datetime) -> bool:
        """Check if data for a specific month already exists."""
        query = """
        SELECT 1 FROM QP_DW_RMALLOC.calendar_AF
        WHERE DepartureDate >= %s AND DepartureDate <= %s
        LIMIT 1
        """
        try:
            result = pd.read_sql(query, self.rm_rd_conn, 
                               params=[month_start.strftime('%Y-%m-%d'), month_end.strftime('%Y-%m-%d')])
            return not result.empty
        except Exception as e:
            self.logger.error(f"Error checking month existence: {e}")
            return False

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

    def fill_missing_dates(self, df: pd.DataFrame, start_date: datetime, end_date: datetime, 
                          sectors: Optional[List[str]] = None) -> pd.DataFrame:
        """Fill in missing dates with zero values for each sector."""
        if sectors is None or len(sectors) == 0:
            if df.empty:
                return pd.DataFrame(columns=['Sector', 'DepartureDate', 'AverageFare'])
            sectors = df['Sector'].unique().tolist()
        
        # Create complete date range
        date_range = pd.date_range(start=start_date.date(), end=end_date.date(), freq='D').date
        
        # Create complete grid
        sector_grid = np.repeat(sectors, len(date_range))
        date_grid = np.tile(date_range, len(sectors))
        
        complete_df = pd.DataFrame({
            'Sector': sector_grid,
            'DepartureDate': date_grid,
            'AverageFare': 0.0
        })
        
        # Merge with actual data if available
        if not df.empty:
            df_processed = df.copy()
            if pd.api.types.is_datetime64_dtype(df_processed['DepartureDate']):
                df_processed['DepartureDate'] = df_processed['DepartureDate'].dt.date
            elif isinstance(df_processed['DepartureDate'].iloc[0], str):
                df_processed['DepartureDate'] = pd.to_datetime(df_processed['DepartureDate']).dt.date
            
            # Merge and update with actual values
            merged_df = complete_df.merge(
                df_processed[['Sector', 'DepartureDate', 'AverageFare']],
                on=['Sector', 'DepartureDate'],
                how='left',
                suffixes=('', '_actual')
            )
            
            # Update with actual values where available
            mask = merged_df['AverageFare_actual'].notna()
            merged_df.loc[mask, 'AverageFare'] = merged_df.loc[mask, 'AverageFare_actual']
            complete_df = merged_df[['Sector', 'DepartureDate', 'AverageFare']].copy()
        
        return complete_df

    def insert_fare_data(self, df: pd.DataFrame) -> int:
        """Insert fare data into calendar_AF table."""
        if df.empty:
            return 0
        
        try:
            df_insert = df.copy()
            df_insert['AverageFare'] = df_insert['AverageFare'].astype('float64')
            
            if not pd.api.types.is_datetime64_dtype(df_insert['DepartureDate']):
                df_insert['DepartureDate'] = pd.to_datetime(df_insert['DepartureDate'])
            
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
            
            self.logger.info(f"Inserted {len(df_insert)} records into calendar_AF")
            return len(df_insert)
            
        except Exception as e:
            self.logger.error(f"Error inserting fare data: {e}")
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

    def extract_historical_data(self) -> pd.DataFrame:
        """Extract historical data needed for analysis calculations."""
        self.logger.info("Extracting historical data for analysis calculations")
        
        actual_df, sectors = self.extract_fare_data_with_sectors(self.historical_start, self.historical_end)
        existing_sectors = self.get_existing_sectors()
        all_sectors = list(set(sectors + existing_sectors))
        
        complete_df = self.fill_missing_dates(actual_df, self.historical_start, self.historical_end, all_sectors)
        return complete_df

    def extract_full_data(self) -> pd.DataFrame:
        """Extract complete data for initial load."""
        self.logger.info("Performing full data extraction")
        
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        self.logger.info(f"Extracting data from {self.data_start.strftime('%Y-%m-%d')} to {today.strftime('%Y-%m-%d')}")
        
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
        
        if monthly_data:
            combined_df = pd.concat(monthly_data, ignore_index=True)
            complete_df = self.fill_missing_dates(combined_df, self.data_start, today - timedelta(days=1), list(all_sectors))
            return complete_df
        else:
            return pd.DataFrame(columns=['Sector', 'DepartureDate', 'AverageFare'])

    def extract_latest_month_data(self) -> pd.DataFrame:
        """Extract data for the latest completed month."""
        month_start, month_end = self.get_last_completed_month()
        self.logger.info(f"Extracting data for latest completed month: {month_start.strftime('%Y-%m')}")
        
        if self.check_month_exists(month_start, month_end):
            self.logger.info(f"Data for {month_start.strftime('%Y-%m')} already exists, skipping")
            return pd.DataFrame()
        
        return self.extract_month_data(month_start, month_end)

    def extract_month_data(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Extract data for a specific month with all dates filled in."""
        actual_df, sectors = self.extract_fare_data_with_sectors(start_date, end_date)
        complete_df = self.fill_missing_dates(actual_df, start_date, end_date, sectors)
        return complete_df

    def run(self) -> Dict:
        """Main method to run fare data extraction."""
        start_time = time.time()
        results = {
            "status": "not_attempted",
            "total_records": 0,
            "extraction_type": "unknown",
            "execution_time_seconds": 0,
            "rows_removed": 0,
            "historical_backfill": False
        }
        
        final_df = pd.DataFrame()
        
        try:
            is_first_time = self.check_table_empty()
            if is_first_time:
                self.logger.info("First-time execution - performing full data extraction")
                results["extraction_type"] = "full_initial"
                final_df = self.extract_full_data()
                results["historical_backfill"] = True
            else:
                has_historical_data = self.check_historical_data_exists()
                
                if not has_historical_data:
                    self.logger.info("Missing historical data - performing backfill")
                    results["extraction_type"] = "historical_backfill"
                    historical_df = self.extract_historical_data()
                    
                    if not historical_df.empty:
                        records_inserted = self.insert_fare_data(historical_df)
                        results["historical_backfill"] = True
                        self.logger.info(f"Historical backfill completed: {records_inserted} records")
                    
                monthly_df = self.extract_latest_month_data()
                
                if not monthly_df.empty:
                    results["extraction_type"] = ("incremental_with_backfill" 
                                                if not has_historical_data else "incremental")
                    rows_removed = self.remove_oldest_month()
                    results["rows_removed"] = rows_removed
                    final_df = monthly_df
                else:
                    if not has_historical_data and results["historical_backfill"]:
                        results["status"] = "historical_backfill_only"
                        results["total_records"] = records_inserted
                    else:
                        results["status"] = "no_new_data"
                    results["execution_time_seconds"] = round(time.time() - start_time, 2)
                    return results  # MAKE SURE THIS RETURN IS HERE
            
            # Process final_df
            if not final_df.empty:
                records_inserted = self.insert_fare_data(final_df)
                results["total_records"] = records_inserted
                results["status"] = "success"
                self.logger.info(f"Extraction completed successfully. Total records: {records_inserted}")
            else:
                results["status"] = "no_data"
                
            # Run statistical analysis
            try:
                self.calc.run()
            except Exception as calc_error:
                self.logger.error(f"Statistical analysis failed: {calc_error}")
                
        except Exception as e:
            results["status"] = "error"
            results["error"] = str(e)
            self.logger.error(f"Error in fare extraction: {e}", exc_info=True)
        
        finally:
            results["execution_time_seconds"] = round(time.time() - start_time, 2)
        
        return results