import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from pymongo import UpdateOne
import traceback

logger = logging.getLogger(__name__)


class ForecastDateConfig:
    """Maintains the forecast_date_config collection for configurable historical date ranges"""
    
    FORECAST_HORIZON_DAYS = 90
    MIN_MONTHS_FOR_ROLLING = 13
    BATCH_SIZE = 1000
    
    def __init__(self, logger: logging.Logger, params: Dict, overrides: Dict, context: Dict):
        """Initialize with MongoDB connection and calculate date ranges."""
        self.logger = logger
        self.mongo_client = params['mongoconn']
        self.db = self.mongo_client['qp_dw_rmalloc']
        self.rm_rd_esb_conn = params.get('rdEsbConn')  # MySQL ESB connection
        
        # Calculate dynamic dates
        self.today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        self.forecast_end = self.today + timedelta(days=self.FORECAST_HORIZON_DAYS - 1)
        
        self.config_collection = 'forecast_date_config'
        
    def _get_default_date_ranges(self, departure_date: datetime) -> Tuple[datetime, datetime, datetime, datetime]:
        """
        Calculate default date ranges using forecast logic.
        
        LastM (Month-over-Month Comparison):
        ------------------------------------
        Purpose: Compare with recent historical data (last 3 weeks)
        Logic: Simple 21-day lookback from current date
        Example: If today is 2026-01-20:
                 - LastMStartDate = 2025-12-30 (today - 21 days)
                 - LastMEndDate = 2026-01-19 (yesterday)
        Rationale: Captures recent trends from the past 3 weeks
        
        LastY (Year-over-Year Comparison):
        -----------------------------------
        Purpose: Compare with same period last year to capture seasonal patterns
        Logic: Take the departure date from last year (52 weeks ago), then add ±2 weeks buffer
        Example: For departure on 2026-02-16:
                 - Anchor date = 2025-02-16 (52 weeks ago, same departure date)
                 - LastYStartDate = 2025-02-02 (anchor - 2 weeks)
                 - LastYEndDate = 2025-03-02 (anchor + 2 weeks)
        Rationale: 4-week window captures year-over-year trends while accounting for 
                   calendar shifts (holidays, events may not fall on exact same date)
        
        Returns: (lastm_start, lastm_end, lasty_start, lasty_end)
        """
        # LastM: Simple 3-week (21 days) lookback from today
        lastm_start = self.today - timedelta(days=21)
        lastm_end = self.today - timedelta(days=1)
        
        # LastY: ±2 weeks around same departure date last year
        anchor_date = departure_date - timedelta(weeks=52)
        lasty_start = anchor_date - timedelta(weeks=2)
        lasty_end = anchor_date + timedelta(weeks=2)
        
        return lastm_start, lastm_end, lasty_start, lasty_end
    
    def _fetch_sectors(self) -> pd.DataFrame:
        """
        Fetch unique sectors from MySQL ESB QP_DL_NAVDATA.QP_DL_BOOKEDDATAPAX table
        for next 90 days.
        
        Returns DataFrame with Sector, Region columns.
        """
        try:
            start_date_str = self.today.strftime('%Y-%m-%d')
            end_date_str = self.forecast_end.strftime('%Y-%m-%d')
            
            query = f"""
            SELECT DISTINCT
                CONCAT(Origin, Destination) AS Sector,
                CASE 
                    WHEN DomesticFlag = 1 THEN 'D'
                    ELSE 'I'
                END AS Region
            FROM QP_DL_NAVDATA.QP_DL_BOOKEDDATAPAX
            WHERE DepartureDate >= '{start_date_str}'
              AND DepartureDate <= '{end_date_str}'
            ORDER BY Sector
            """
            
            df = pd.read_sql(query, self.rm_rd_esb_conn)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error fetching sectors from ESB: {e}")
            return pd.DataFrame(columns=['Sector', 'Region'])
    
    def _prepare_batch_updates(self, sector_date_list: List[Tuple[datetime, str, str]]) -> Tuple[List, List]:
        """
        Prepare batch operations for MongoDB bulk write.
        
        Args:
            sector_date_list: List of (departure_date, sector, region) tuples
        
        Returns:
            Tuple of (bulk_operations, stats_dict)
        """
        collection = self.db[self.config_collection]
        bulk_operations = []
        stats = {'to_insert': 0, 'to_update': 0, 'to_roll': 0, 'to_reset': 0, 'skipped': 0}
        
        # Fetch existing records for this batch
        departure_dates = [dt.strftime('%Y-%m-%d') for dt, _, _ in sector_date_list]
        sectors = list(set([sec for _, sec, _ in sector_date_list]))
        
        existing_records = {}
        cursor = collection.find({
            'DepartureDate': {'$in': departure_dates},
            'Sector': {'$in': sectors}
        })
        
        for record in cursor:
            key = (record['DepartureDate'], record['Sector'])
            existing_records[key] = record
        
        # Process each sector-date combination
        for departure_date, sector, region in sector_date_list:
            departure_date_str = departure_date.strftime('%Y-%m-%d')
            key = (departure_date_str, sector)
            
            existing_record = existing_records.get(key)
            
            if existing_record:
                # Check if already updated today (idempotency check)
                updated_at = existing_record.get('UpdatedAt')
                if updated_at:
                    # Convert to date for comparison
                    if isinstance(updated_at, datetime):
                        updated_date = updated_at.date()
                    else:
                        try:
                            updated_date = datetime.fromisoformat(str(updated_at)).date()
                        except:
                            updated_date = None
                    
                    # Skip if already updated today
                    if updated_date == self.today.date():
                        stats['skipped'] += 1
                        continue
                
                # Record exists - apply rolling/reset logic
                manual_flag = existing_record.get('ManualFlag', 'N')
                lastm_rolling = existing_record.get('LastMRolling', 'N')
                lasty_rolling = existing_record.get('LastYRolling', 'N')
                reset_date_str = existing_record.get('ResetDate')
                
                # Check if reset is needed
                should_reset = False
                if reset_date_str:
                    try:
                        reset_date = datetime.strptime(reset_date_str, '%Y-%m-%d').date()
                        if reset_date <= self.today.date():
                            should_reset = True
                    except Exception:
                        should_reset = True  # Reset on parse error
                
                if should_reset:
                    # Reset to defaults
                    lastm_start, lastm_end, lasty_start, lasty_end = self._get_default_date_ranges(departure_date)
                    manual_flag = 'N'
                    lastm_rolling = 'Y'
                    lasty_rolling = 'N'
                    reset_date_str = None
                    stats['to_reset'] += 1
                    
                elif manual_flag == 'Y':
                    # Manual mode - roll dates if flags are on
                    lastm_start = datetime.strptime(existing_record['LastMStartDate'], '%Y-%m-%d')
                    lastm_end = datetime.strptime(existing_record['LastMEndDate'], '%Y-%m-%d')
                    lasty_start = datetime.strptime(existing_record['LastYStartDate'], '%Y-%m-%d')
                    lasty_end = datetime.strptime(existing_record['LastYEndDate'], '%Y-%m-%d')
                    
                    # Calculate days since last update for catch-up rolling
                    days_to_roll = 1  # Default: roll by 1 day
                    if updated_at:
                        try:
                            if isinstance(updated_at, datetime):
                                last_update_date = updated_at.date()
                            else:
                                last_update_date = datetime.fromisoformat(str(updated_at)).date()
                            
                            # Calculate days difference (excluding today, since we're updating today)
                            days_diff = (self.today.date() - last_update_date).days
                            if days_diff > 1:
                                # Script missed some days, roll by the number of days missed
                                days_to_roll = days_diff
                        except Exception:
                            days_to_roll = 1  # Fallback to 1 day on error
                    
                    if lastm_rolling == 'Y':
                        lastm_start = lastm_start + timedelta(days=days_to_roll)
                        lastm_end = lastm_end + timedelta(days=days_to_roll)
                        stats['to_roll'] += 1
                    
                    if lasty_rolling == 'Y':
                        lasty_start = lasty_start + timedelta(days=days_to_roll)
                        lasty_end = lasty_end + timedelta(days=days_to_roll)
                        if lastm_rolling == 'N':  # Only count once if both rolling
                            stats['to_roll'] += 1
                else:
                    # Not manual mode - use default logic
                    lastm_start, lastm_end, lasty_start, lasty_end = self._get_default_date_ranges(departure_date)
                
                # Add update operation - fields in specified order
                bulk_operations.append(
                    UpdateOne(
                        {
                            'DepartureDate': departure_date_str,
                            'Sector': sector
                        },
                        {
                            '$set': {
                                'Region': region,
                                'LastMStartDate': lastm_start.strftime('%Y-%m-%d'),
                                'LastMEndDate': lastm_end.strftime('%Y-%m-%d'),
                                'LastYStartDate': lasty_start.strftime('%Y-%m-%d'),
                                'LastYEndDate': lasty_end.strftime('%Y-%m-%d'),
                                'LastMRolling': lastm_rolling,
                                'LastYRolling': lasty_rolling,
                                'ResetDate': reset_date_str,
                                'ManualFlag': manual_flag,
                                'UpdatedAt': datetime.now()
                            }
                        }
                    )
                )
                stats['to_update'] += 1
            
            else:
                # New record - use default logic, fields in specified order
                lastm_start, lastm_end, lasty_start, lasty_end = self._get_default_date_ranges(departure_date)
                
                bulk_operations.append(
                    UpdateOne(
                        {
                            'DepartureDate': departure_date_str,
                            'Sector': sector
                        },
                        {
                            '$setOnInsert': {
                                'Sector': sector,
                                'DepartureDate': departure_date_str,
                                'Region': region,
                                'LastMStartDate': lastm_start.strftime('%Y-%m-%d'),
                                'LastMEndDate': lastm_end.strftime('%Y-%m-%d'),
                                'LastYStartDate': lasty_start.strftime('%Y-%m-%d'),
                                'LastYEndDate': lasty_end.strftime('%Y-%m-%d'),
                                'LastMRolling': 'Y',
                                'LastYRolling': 'N',
                                'ResetDate': None,
                                'ManualFlag': 'N',
                                'CreatedAt': datetime.now(),
                                'UpdatedAt': datetime.now()
                            }
                        },
                        upsert=True
                    )
                )
                stats['to_insert'] += 1
        
        return bulk_operations, stats
    
    def _execute_batch_updates(self, bulk_operations: List) -> Dict:
        """
        Execute bulk write operations to MongoDB.
        
        Returns:
            Dict with execution results
        """
        if not bulk_operations:
            return {'inserted': 0, 'updated': 0, 'errors': 0}
        
        try:
            collection = self.db[self.config_collection]
            result = collection.bulk_write(bulk_operations, ordered=False)
            
            return {
                'inserted': result.upserted_count,
                'updated': result.modified_count,
                'matched': result.matched_count,
                'errors': 0
            }
        except Exception as e:
            self.logger.error(f"Batch write error: {e}")
            return {'inserted': 0, 'updated': 0, 'errors': len(bulk_operations)}
    
    def _cleanup_old_records(self) -> int:
        """Delete records with DepartureDate < today"""
        try:
            collection = self.db[self.config_collection]
            today_str = self.today.strftime('%Y-%m-%d')
            
            result = collection.delete_many({
                'DepartureDate': {'$lt': today_str}
            })
            
            return result.deleted_count
            
        except Exception as e:
            self.logger.error(f"Error cleaning up old records: {e}")
            return 0
    
    def run(self) -> Dict:
        """Main maintenance process with batch updates and minimal logging"""
        start_time = datetime.now()
        self.logger.info(f"Forecast Date Config START - Window: {self.today.strftime('%Y-%m-%d')} to {self.forecast_end.strftime('%Y-%m-%d')}")
        
        results = {
            'status': 'not_attempted',
            'deleted_records': 0,
            'processed_combinations': 0,
            'inserted_records': 0,
            'updated_records': 0,
            'reset_records': 0,
            'rolled_records': 0,
            'skipped_records': 0,
            'error_records': 0,
            'sectors_count': 0,
            'batches_processed': 0,
            'execution_time_seconds': 0
        }
        
        try:
            # Step 1: Cleanup old records
            deleted_count = self._cleanup_old_records()
            results['deleted_records'] = deleted_count
            
            # Step 2: Fetch sectors
            sectors_df = self._fetch_sectors()
            
            if sectors_df.empty:
                self.logger.warning("No sectors found to process")
                results['status'] = 'no_sectors'
                return results
            
            results['sectors_count'] = len(sectors_df)
            self.logger.info(f"Processing {results['sectors_count']} sectors")
            
            # Step 3: Process in batches
            forecast_dates = pd.date_range(start=self.today.date(), end=self.forecast_end.date(), freq='D')
            
            # Build list of all sector-date combinations
            all_combinations = []
            for _, sector_row in sectors_df.iterrows():
                sector = sector_row['Sector']
                region = sector_row['Region']
                for departure_date in forecast_dates:
                    all_combinations.append((departure_date, sector, region))
            
            total_combinations = len(all_combinations)
            
            # Process in batches
            batch_num = 0
            total_inserted = total_updated = total_reset = total_rolled = total_errors = total_skipped = 0
            
            for i in range(0, total_combinations, self.BATCH_SIZE):
                batch_num += 1
                batch = all_combinations[i:i + self.BATCH_SIZE]
                
                # Prepare and execute batch
                bulk_ops, prep_stats = self._prepare_batch_updates(batch)
                exec_results = self._execute_batch_updates(bulk_ops)
                
                # Accumulate stats
                total_inserted += exec_results['inserted']
                total_updated += exec_results['updated']
                total_errors += exec_results['errors']
                total_reset += prep_stats['to_reset']
                total_rolled += prep_stats['to_roll']
                total_skipped += prep_stats['skipped']
            
            results.update({
                'status': 'success',
                'processed_combinations': total_combinations,
                'inserted_records': total_inserted,
                'updated_records': total_updated,
                'reset_records': total_reset,
                'rolled_records': total_rolled,
                'skipped_records': total_skipped,
                'error_records': total_errors,
                'batches_processed': batch_num
            })
            
            # Final summary
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            results['execution_time_seconds'] = round(duration, 2)
            
            self.logger.info(
                f"Forecast Date Config COMPLETE - "
                f"Time: {duration:.1f}s | Deleted: {deleted_count} | "
                f"Inserted: {total_inserted} | Updated: {total_updated} | "
                f"Skipped: {total_skipped} | Reset: {total_reset} | Rolled: {total_rolled} | Errors: {total_errors}"
            )
            
        except Exception as e:
            results['status'] = 'error'
            results['error'] = str(e)
            self.logger.error(f"Forecast Date Config FAILED: {e}")
            self.logger.error(traceback.format_exc())
        
        finally:
            end_time = datetime.now()
            results['execution_time_seconds'] = round((end_time - start_time).total_seconds(), 2)
        
        return results
