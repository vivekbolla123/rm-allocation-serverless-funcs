import io
import boto3
import pandas as pd
import numpy as np
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple, List
from sqlalchemy import text
from pathlib import Path
import configsettings as config


class FareAnalysisConfig:
    """Configuration constants for the fare analysis."""
    ROLLING_WINDOW_SIZE = 29
    PEAK_THRESHOLD = 1.1
    SMOOTHING_WINDOW = 5
    ANALYSIS_PERIOD_DAYS = 365
    CUTOFF_BUFFER_DAYS = 15
    START_BUFFER_DAYS=14
    BATCH_SIZE = 1000
    VALIDATION_SAMPLE_SIZE = 100
    DECIMAL_PRECISION = 6
    
    DOW_ALIASES = {
        'Monday': 'Friday',
        'Tuesday': 'Thursday', 
        'Wednesday': 'Wednesday',
        'Thursday': 'Tuesday',
        'Friday': 'Monday',
        'Saturday': 'Sunday',
        'Sunday': 'Saturday'
    }


class FareStatisticalAnalysis:
    """Main class for fare statistical analysis with holiday integration and upsell calculation."""
    
    def __init__(self, logger, params, overrides, context):
        """Initialize with database connections and configuration."""
        self.logger = logger
        self.rm_rd_conn = params['wrconn']
        self.s3_client = params['s3']
        self.bucket_name = config.S3_RM_BUCKET_NAME
        self.s3_file_path = config.S3_FILE_PATH
        
        # Date calculations
        today = datetime.now().date()
        self.cutoff_date = today - timedelta(days=FareAnalysisConfig.CUTOFF_BUFFER_DAYS)
        self.start_date = self.cutoff_date - timedelta(days=FareAnalysisConfig.ANALYSIS_PERIOD_DAYS)

    # ============================================================================
    # DATA FETCHING METHODS
    # ============================================================================
    
    def _fetch_calendar_data(self) -> pd.DataFrame:
        """Fetch fare data from calendar_AF table."""
        query = """
        SELECT Sector, DepartureDate, AverageFare
        FROM QP_DW_RMALLOC.calendar_AF
        WHERE AverageFare IS NOT NULL AND AverageFare > 0
        ORDER BY Sector, DepartureDate
        """
        
        df = pd.read_sql(query, self.rm_rd_conn)
        if df.empty:
            return df
            
        df['DepartureDate'] = pd.to_datetime(df['DepartureDate'])
        df['AverageFare'] = pd.to_numeric(df['AverageFare'], errors='coerce')
        return df.dropna(subset=['AverageFare'])

    def _fetch_holidays(self) -> pd.DataFrame:
        """Fetch holidays from config_holiday_list table."""
        query = """
        SELECT Date as holiday_date, Holiday as holiday_name
        FROM QP_DW_RMALLOC.config_holiday_list
        WHERE Date >= CURDATE() - INTERVAL 365 DAY
        AND Date <= CURDATE() + INTERVAL 365 DAY
        ORDER BY Date
        """
        
        holidays_df = pd.read_sql(query, self.rm_rd_conn)
        if not holidays_df.empty:
            holidays_df['holiday_date'] = pd.to_datetime(holidays_df['holiday_date'])
        return holidays_df

    def _fetch_profile_fares(self) -> pd.DataFrame:
        """Fetch profile fares data for upsell calculation."""
        query = """
        SELECT sector, month, dow, (SUM(Q10)/6) as Q10
        FROM QP_DW_RMALLOC.config_profile_fares
        GROUP BY sector, month, dow
        """
        
        return pd.read_sql(query, self.rm_rd_conn)

    # ============================================================================
    # STATISTICAL CALCULATION METHODS
    # ============================================================================
    
    def _calculate_centered_rolling_average(self, operational_df: pd.DataFrame) -> List[float]:
        """Calculate centered rolling average for operational days."""
        rolling_avg_list = []
        half_window = FareAnalysisConfig.ROLLING_WINDOW_SIZE // 2
        
        for idx in range(len(operational_df)):
            start_idx = max(0, idx - half_window)
            end_idx = min(len(operational_df), idx + half_window + 1)
            
            window_size = end_idx - start_idx
            if window_size >= FareAnalysisConfig.ROLLING_WINDOW_SIZE or (idx < half_window or idx >= len(operational_df) - half_window):
                window_data = operational_df.iloc[start_idx:end_idx]
                rolling_avg_list.append(window_data['AverageFare'].mean())
            else:
                rolling_avg_list.append(np.nan)
                
        return rolling_avg_list

    def _calculate_smoothed_peaks(self, operational_df: pd.DataFrame) -> List[int]:
        """Calculate smoothed peaks using two-pass algorithm with max logic."""
        # First pass
        smoothed_peaks_pass1 = self._smooth_peaks_single_pass(operational_df['peak'])
        operational_df['smoothed_peak_pass1'] = smoothed_peaks_pass1
        
        max_peaks = []
        for i in range(len(operational_df)):
            original_peak = operational_df.iloc[i]['peak']
            smoothed_pass1 = smoothed_peaks_pass1[i]
            max_peaks.append(max(original_peak, smoothed_pass1))
        
        # Add max_peaks as intermediate column (optional, for debugging)
        operational_df['max_peak_pass1'] = max_peaks
        
        # Second pass on the max values
        return self._smooth_peaks_single_pass(pd.Series(max_peaks))

    def _smooth_peaks_single_pass(self, peak_series: pd.Series) -> List[int]:
        """Single pass smoothing algorithm."""
        smoothed_list = []
        half_window = FareAnalysisConfig.SMOOTHING_WINDOW // 2
        
        for idx in range(len(peak_series)):
            start_idx = max(0, idx - half_window)
            end_idx = min(len(peak_series), idx + half_window + 1)
            
            window_size = end_idx - start_idx
            if window_size >= 3:
                window_peaks = peak_series.iloc[start_idx:end_idx]
                smoothed_list.append(1 if window_peaks.mean() > 0.5 else 0)
            else:
                smoothed_list.append(0)
                
        return smoothed_list

    def _assign_band_ids(self, df: pd.DataFrame, sector: str) -> pd.DataFrame:
        """Assign band IDs to consecutive groups of smoothed peaks."""
        if df.empty or 'smoothed_peak' not in df.columns:
            df['band_id'] = None
            return df
        
        df_sorted = df.sort_values('DepartureDate').reset_index(drop=True)
        df_sorted['band_id'] = None
        
        smoothed_peaks = df_sorted['smoothed_peak'].values
        band_counter = 0
        current_band_id = None
        
        for i, peak in enumerate(smoothed_peaks):
            if peak == 1:
                if i == 0 or smoothed_peaks[i-1] == 0:
                    band_counter += 1
                    current_band_id = f"{sector}{band_counter:03d}"
                df_sorted.loc[i, 'band_id'] = current_band_id
            else:
                current_band_id = None
        
        return df_sorted

    def _process_sector_statistics(self, sector_df: pd.DataFrame, sector: str) -> Optional[pd.DataFrame]:
        """Process statistics for a single sector."""
        sector_df = sector_df.sort_values('DepartureDate').reset_index(drop=True)

        # Separate operational and non-operational days
        operational_df = sector_df[sector_df['AverageFare'] > 0].copy()
        non_operational_df = sector_df[sector_df['AverageFare'] == 0].copy()
        
        # Check minimum operational days BEFORE any row removal
        if len(operational_df) < FareAnalysisConfig.ROLLING_WINDOW_SIZE:
            return None
        
        # Calculate rolling averages for ALL operational data first
        operational_df['rolling_avg'] = self._calculate_centered_rolling_average(operational_df)
        operational_df['coefficient'] = operational_df['AverageFare'] / operational_df['rolling_avg']
        operational_df['peak'] = (operational_df['coefficient'] >= FareAnalysisConfig.PEAK_THRESHOLD).astype(int)
        operational_df['smoothed_peak'] = self._calculate_smoothed_peaks(operational_df)
        operational_df['day_of_week'] = operational_df['DepartureDate'].dt.day_name()
        
        # ROW-BASED FILTERING: Remove first 15 and last 14 operational rows
        start_buffer = FareAnalysisConfig.START_BUFFER_DAYS
        end_buffer = FareAnalysisConfig.START_BUFFER_DAYS
        
        total_rows = len(operational_df)
        
        # Check if enough rows remain after removing buffers
        if total_rows <= (start_buffer + end_buffer):
            return None
        
        # Remove first 15 and last 14 operational rows
        operational_output = operational_df.iloc[start_buffer:-end_buffer].copy()
        
        # Filter out rows with invalid rolling averages
        operational_output = operational_output[
            operational_output['rolling_avg'].notna()
        ].copy()
        
        # Process non-operational days (keep same date-based logic for non-operational)
        non_operational_output = self._process_non_operational_days(non_operational_df)
        
        # Combine and assign band IDs
        complete_df = self._combine_operational_non_operational(operational_output, non_operational_output)
        if complete_df is not None:
            complete_df = self._assign_band_ids(complete_df, sector)
        
        return complete_df

    def _process_non_operational_days(self, non_operational_df: pd.DataFrame) -> pd.DataFrame:
        """Process non-operational days for the output window."""
        if non_operational_df.empty:
            return pd.DataFrame()
            
        filtered_df = non_operational_df[
            (non_operational_df['DepartureDate'].dt.date >= self.start_date) &
            (non_operational_df['DepartureDate'].dt.date <= self.cutoff_date)
        ].copy()
        
        if not filtered_df.empty:
            filtered_df['rolling_avg'] = np.nan
            filtered_df['coefficient'] = np.nan
            filtered_df['peak'] = 0
            filtered_df['smoothed_peak'] = 0
            filtered_df['day_of_week'] = filtered_df['DepartureDate'].dt.day_name()
        
        return filtered_df

    def _combine_operational_non_operational(self, operational_df: pd.DataFrame, 
                                           non_operational_df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Combine operational and non-operational dataframes."""
        if operational_df.empty and non_operational_df.empty:
            return None
            
        if non_operational_df.empty:
            return operational_df
        elif operational_df.empty:
            return non_operational_df
        else:
            combined_df = pd.concat([operational_df, non_operational_df], ignore_index=True)
            return combined_df.sort_values('DepartureDate').reset_index(drop=True)

    def calculate_statistics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate statistics for all sectors."""
        if df.empty:
            return pd.DataFrame()
        
        results = []
        sectors = df['Sector'].unique()
        
        for sector in sectors:
            sector_df = df[df['Sector'] == sector].copy()
            processed_df = self._process_sector_statistics(sector_df, sector)
            
            if processed_df is not None:
                results.append(processed_df)
        
        return pd.concat(results, ignore_index=True) if results else pd.DataFrame()

    # ============================================================================
    # HOLIDAY ANALYSIS METHODS
    # ============================================================================
    
    def _find_closest_holiday_to_band(self, band_start: datetime, band_end: datetime, 
                                     holidays_df: pd.DataFrame) -> Optional[Dict]:
        """Find the closest holiday to a band."""
        if holidays_df.empty:
            return None
        
        # Convert to datetime if needed
        if isinstance(band_start, str):
            band_start = pd.to_datetime(band_start)
        if isinstance(band_end, str):
            band_end = pd.to_datetime(band_end)
        
        # Check for holidays within the band
        holidays_in_band = holidays_df[
            (holidays_df['holiday_date'] >= band_start) & 
            (holidays_df['holiday_date'] <= band_end)
        ]
        
        if not holidays_in_band.empty:
            closest_holiday = holidays_in_band.sort_values('holiday_date').iloc[0]
            return {
                'holiday_date': closest_holiday['holiday_date'],
                'holiday_name': closest_holiday['holiday_name'],
                'days_to_holiday': 0,
                'position': 'within_band'
            }
        
        # Find closest to start or end
        holidays_copy = holidays_df.copy()
        holidays_copy['days_to_start'] = abs((holidays_copy['holiday_date'] - band_start).dt.days)
        holidays_copy['days_to_end'] = abs((holidays_copy['holiday_date'] - band_end).dt.days)
        holidays_copy['min_days_to_band'] = holidays_copy[['days_to_start', 'days_to_end']].min(axis=1)
        
        closest_holiday = holidays_copy.loc[holidays_copy['min_days_to_band'].idxmin()]
        
        position = 'before_band' if closest_holiday['days_to_start'] <= closest_holiday['days_to_end'] else 'after_band'
        days_difference = closest_holiday['days_to_start'] if position == 'before_band' else closest_holiday['days_to_end']
        
        return {
            'holiday_date': closest_holiday['holiday_date'],
            'holiday_name': closest_holiday['holiday_name'],
            'days_to_holiday': days_difference,
            'position': position
        }

    def _find_next_year_holiday(self, holiday_name: str, current_holiday_date: datetime, 
                               holidays_df: pd.DataFrame) -> Optional[datetime]:
        """Find the next year's occurrence of the same holiday."""
        next_year = current_holiday_date.year + 1
        
        next_year_holidays = holidays_df[
            (holidays_df['holiday_name'] == holiday_name) & 
            (holidays_df['holiday_date'].dt.year == next_year)
        ]
        
        if not next_year_holidays.empty:
            return next_year_holidays.iloc[0]['holiday_date']
        
        # Estimate based on date pattern
        try:
            return current_holiday_date.replace(year=next_year)
        except ValueError:
            # Handle leap year edge case
            if current_holiday_date.month == 2 and current_holiday_date.day == 29:
                return current_holiday_date.replace(year=next_year, day=28)
            else:
                return current_holiday_date + timedelta(days=365)

    def _calculate_af_last_holiday_dow(self, statistical_df: pd.DataFrame) -> pd.DataFrame:
        """Calculate AF_LastHolidayDOW based on DOW matching logic."""
        if statistical_df.empty or statistical_df['band_id'].isna().all():
            statistical_df['AF_LastHolidayDOW'] = None
            return statistical_df
        
        statistical_df['AF_LastHolidayDOW'] = None
        unique_bands = statistical_df[statistical_df['band_id'].notna()]['band_id'].unique()
        
        for band_id in unique_bands:
            band_mask = statistical_df['band_id'] == band_id
            band_data = statistical_df[band_mask].copy()
            
            for idx in band_data.index:
                target_dow = statistical_df.loc[idx, 'future_dow']
                if pd.isna(target_dow):
                    continue
                
                af_value = self._get_dow_matched_fare(band_data, target_dow)
                statistical_df.loc[idx, 'AF_LastHolidayDOW'] = af_value
        
        return statistical_df

    def _get_dow_matched_fare(self, band_data: pd.DataFrame, target_dow: str) -> float:
        """Get fare based on DOW matching priority."""
        # Try exact DOW match
        same_dow_data = band_data[band_data['day_of_week'] == target_dow]
        if not same_dow_data.empty:
            return same_dow_data['AverageFare'].max()
        
        # Try alias DOW
        alias_dow = FareAnalysisConfig.DOW_ALIASES.get(target_dow)
        if alias_dow:
            alias_dow_data = band_data[band_data['day_of_week'] == alias_dow]
            if not alias_dow_data.empty:
                return alias_dow_data['AverageFare'].max()
        
        # Try Wednesday
        wednesday_data = band_data[band_data['day_of_week'] == 'Wednesday']
        if not wednesday_data.empty:
            return wednesday_data['AverageFare'].max()
        
        # Return maximum fare in band
        return band_data['AverageFare'].max()

    def add_holiday_analysis(self, statistical_df: pd.DataFrame) -> pd.DataFrame:
        """Add holiday information and map to next year's holiday dates."""
        if statistical_df.empty:
            self._add_empty_holiday_columns(statistical_df)
            return statistical_df
        
        holidays_df = self._fetch_holidays()
        if holidays_df.empty:
            self._add_empty_holiday_columns(statistical_df)
            return statistical_df
        
        self._add_empty_holiday_columns(statistical_df)
        
        if not statistical_df['band_id'].isna().all():
            self._process_holiday_mapping(statistical_df, holidays_df)
        
        # Calculate AF_LastHolidayDOW and profile fares
        statistical_df = self._calculate_af_last_holiday_dow(statistical_df)
        statistical_df = self._map_profile_fares_and_calculate_upsell(statistical_df)
        
        return statistical_df

    def _add_empty_holiday_columns(self, df: pd.DataFrame) -> None:
        """Add empty holiday columns to dataframe."""
        holiday_columns = ['holiday_name', 'days_from_closest_holiday', 'next_year_date', 
                          'next_year_month', 'future_dow']
        for col in holiday_columns:
            df[col] = None

    def _process_holiday_mapping(self, statistical_df: pd.DataFrame, holidays_df: pd.DataFrame) -> None:
        """Process holiday mapping for all bands."""
        bands_df = statistical_df[statistical_df['band_id'].notna()].copy()
        unique_bands = bands_df.groupby(['Sector', 'band_id']).agg({
            'DepartureDate': ['min', 'max']
        }).reset_index()
        
        unique_bands.columns = ['Sector', 'band_id', 'band_start_date', 'band_end_date']
        
        for _, band in unique_bands.iterrows():
            self._process_single_band_holiday(statistical_df, band, holidays_df)

    def _process_single_band_holiday(self, statistical_df: pd.DataFrame, band: pd.Series, 
                                   holidays_df: pd.DataFrame) -> None:
        """Process holiday mapping for a single band."""
        band_id = band['band_id']
        band_start = pd.to_datetime(band['band_start_date'])
        band_end = pd.to_datetime(band['band_end_date'])
        
        closest_holiday = self._find_closest_holiday_to_band(band_start, band_end, holidays_df)
        band_mask = statistical_df['band_id'] == band_id
        
        if closest_holiday:
            self._apply_holiday_mapping(statistical_df, band_mask, closest_holiday, holidays_df)
        else:
            self._apply_no_holiday_mapping(statistical_df, band_mask)

    def _apply_holiday_mapping(self, statistical_df: pd.DataFrame, band_mask: pd.Series, 
                             closest_holiday: Dict, holidays_df: pd.DataFrame) -> None:
        """Apply holiday mapping to band rows."""
        holiday_name = closest_holiday['holiday_name']
        current_holiday_date = closest_holiday['holiday_date']
        
        next_year_holiday_date = self._find_next_year_holiday(holiday_name, current_holiday_date, holidays_df)
        
        statistical_df.loc[band_mask, 'holiday_name'] = holiday_name
        
        # Calculate future dates and details
        band_rows = statistical_df[band_mask].copy()
        for idx in band_rows.index:
            current_date = pd.to_datetime(statistical_df.loc[idx, 'DepartureDate'])
            days_diff = (current_date - current_holiday_date).days
            statistical_df.loc[idx, 'days_from_closest_holiday'] = days_diff
            
            if next_year_holiday_date:
                mapped_future_date = next_year_holiday_date + timedelta(days=days_diff)
                statistical_df.loc[idx, 'next_year_date'] = mapped_future_date.strftime('%Y-%m-%d')
                statistical_df.loc[idx, 'next_year_month'] = mapped_future_date.strftime('%B')
                statistical_df.loc[idx, 'future_dow'] = mapped_future_date.strftime('%A')

    def _apply_no_holiday_mapping(self, statistical_df: pd.DataFrame, band_mask: pd.Series) -> None:
        """Apply no holiday mapping to band rows."""
        statistical_df.loc[band_mask, 'holiday_name'] = 'No Holiday Found'

    # ============================================================================
    # UPSELL CALCULATION METHODS
    # ============================================================================
    
    def _map_profile_fares_and_calculate_upsell(self, statistical_df: pd.DataFrame) -> pd.DataFrame:
        """Map profile fares to bands and calculate upsell."""
        if statistical_df.empty or statistical_df['band_id'].isna().all():
            statistical_df['profileFaresQ10'] = None
            statistical_df['upsell'] = None
            return statistical_df
        
        profile_fares = self._fetch_profile_fares()
        if profile_fares.empty:
            statistical_df['profileFaresQ10'] = None
            statistical_df['upsell'] = None
            return statistical_df
        
        # Initialize columns
        statistical_df['profileFaresQ10'] = None
        statistical_df['upsell'] = None
        
        # Normalize for matching
        profile_fares['month_lower'] = profile_fares['month'].str.lower()
        profile_fares['dow_lower'] = profile_fares['dow'].str.lower()
        
        # Map profile fares
        self._map_profile_fares_to_bands(statistical_df, profile_fares)
        
        # Calculate upsell
        self._calculate_upsell_values(statistical_df)
        
        return statistical_df

    def _map_profile_fares_to_bands(self, statistical_df: pd.DataFrame, profile_fares: pd.DataFrame) -> None:
        """Map profile fares to band rows."""
        band_rows = statistical_df[statistical_df['band_id'].notna()].copy()
        
        for idx in band_rows.index:
            sector = statistical_df.loc[idx, 'Sector']
            next_year_month = str(statistical_df.loc[idx, 'next_year_month']).lower() if pd.notna(statistical_df.loc[idx, 'next_year_month']) else None
            future_dow = str(statistical_df.loc[idx, 'future_dow']).lower() if pd.notna(statistical_df.loc[idx, 'future_dow']) else None
            
            if not next_year_month or not future_dow:
                continue
            
            profile_q10 = self._find_matching_profile_fare(profile_fares, sector, next_year_month, future_dow)
            if profile_q10 is not None:
                statistical_df.loc[idx, 'profileFaresQ10'] = profile_q10

    def _find_matching_profile_fare(self, profile_fares: pd.DataFrame, sector: str, 
                                  month: str, dow: str) -> Optional[float]:
        """Find matching profile fare with fallback logic."""
        # Exact match
        exact_match = profile_fares[
            (profile_fares['sector'] == sector) &
            (profile_fares['month_lower'] == month) &
            (profile_fares['dow_lower'] == dow)
        ]
        
        if not exact_match.empty:
            return exact_match.iloc[0]['Q10']
        
        # Month match fallback
        month_match = profile_fares[
            (profile_fares['sector'] == sector) &
            (profile_fares['month_lower'] == month)
        ]
        
        if not month_match.empty:
            return month_match['Q10'].mean()
        
        # Sector match fallback
        sector_match = profile_fares[profile_fares['sector'] == sector]
        if not sector_match.empty:
            return sector_match['Q10'].mean()
        
        return None

    def _calculate_upsell_values(self, statistical_df: pd.DataFrame) -> None:
        """Calculate upsell values for each band."""
        unique_bands = statistical_df[statistical_df['band_id'].notna()]['band_id'].unique()
        
        for band_id in unique_bands:
            band_mask = statistical_df['band_id'] == band_id
            band_data = statistical_df[band_mask].copy()
            
            max_af_last_holiday_dow = band_data['AF_LastHolidayDOW'].max()
            if pd.isna(max_af_last_holiday_dow):
                continue
            
            for idx in band_data.index:
                profile_q10 = statistical_df.loc[idx, 'profileFaresQ10']
                
                if pd.notna(profile_q10) and profile_q10 > 0:
                    upsell_value = (max_af_last_holiday_dow - profile_q10) / profile_q10
                    statistical_df.loc[idx, 'upsell'] = upsell_value
    # ============================================================================
    # DATABASE UPDATE METHODS
    # ============================================================================
    
    def update_upsell_table(self, statistical_df: pd.DataFrame) -> Dict:
        """Update config_profile_fare_station_upsell table with calculated upsell values."""
        start_time = time.time()
        update_results = {
            "status": "not_attempted",
            "records_to_update": 0,
            "records_updated": 0,
            "records_inserted": 0,
            "records_skipped": 0,  # NEW: Track skipped records
            "records_failed": 0,
            "execution_time_seconds": 0
        }
        
        try:
            upsell_data = self._prepare_upsell_data(statistical_df)
            if upsell_data.empty:
                update_results["status"] = "no_data"
                return update_results
            
            update_results["records_to_update"] = len(upsell_data)
            
            # Process in batches
            total_updated, total_inserted, total_failed = self._process_upsell_batches(upsell_data)
            
            # Calculate skipped records
            total_processed = total_updated + total_inserted + total_failed
            total_skipped = update_results["records_to_update"] - total_processed
            
            update_results.update({
                "records_updated": total_updated,
                "records_inserted": total_inserted,
                "records_skipped": total_skipped,
                "records_failed": total_failed,
                "status": "success"
            })
            
        except Exception as e:
            update_results["status"] = "error"
            update_results["error"] = str(e)
            self.logger.error(f"Error updating upsell table: {str(e)}")
        
        finally:
            update_results["execution_time_seconds"] = round(time.time() - start_time, 2)
        
        return update_results

    def _prepare_upsell_data(self, statistical_df: pd.DataFrame) -> pd.DataFrame:
        """Prepare upsell data for database update."""
        upsell_data = statistical_df[
            (statistical_df['band_id'].notna()) & 
            (statistical_df['upsell'].notna()) & 
            (statistical_df['next_year_date'].notna()) &
            (statistical_df['upsell'] > 0)  # Only positive upsell values (keeps existing filter)
        ].copy()
        
        if upsell_data.empty:
            return pd.DataFrame()

        # Get unique combinations with max upsell
        upsell_summary = upsell_data.groupby(['Sector', 'next_year_date']).agg({
            'upsell': 'max'
        }).reset_index()

        # Convert date format
        upsell_summary['date_formatted'] = pd.to_datetime(upsell_summary['next_year_date']).dt.strftime('%m/%d/%Y')

        # Convert upsell to percentage format: multiply by 100, round, and add % symbol
        upsell_summary['upsell_percentage'] = upsell_summary['upsell'].apply(
            lambda x: f"{round(x * 100)}%" if x > 0 else None
        )

        # Filter out any rows where percentage conversion resulted in 0% or None
        upsell_summary = upsell_summary[
            (upsell_summary['upsell_percentage'].notna()) & 
            (upsell_summary['upsell_percentage'] != '0%')
        ]

        return upsell_summary

    def _process_upsell_batches(self, upsell_data: pd.DataFrame) -> Tuple[int, int, int]:
        """Process upsell data in batches."""
        records_to_process = [
            {
                'sector': row['Sector'],
                'date': row['date_formatted'],
                'upsell_value': row['upsell_percentage']  # Now contains formatted percentage string
            }
            for _, row in upsell_data.iterrows()
        ]
        
        total_updated = total_inserted = total_failed = 0
        
        for i in range(0, len(records_to_process), FareAnalysisConfig.BATCH_SIZE):
            batch = records_to_process[i:i + FareAnalysisConfig.BATCH_SIZE]
            batch_updated, batch_inserted, batch_failed = self._process_single_upsell_batch(batch)
            
            total_updated += batch_updated
            total_inserted += batch_inserted
            total_failed += batch_failed
        
        return total_updated, total_inserted, total_failed

    def _process_single_upsell_batch(self, batch: List[Dict]) -> Tuple[int, int, int]:
        """
        Process a single batch of upsell records.
        
        Edge Cases Handled:
        1. Record exists: Updates only if new value > existing value AND both values are positive
        2. Record doesn't exist: Creates new record only if value is positive
        3. Negative values: Never updated (skipped with logging)
        4. Database constraints: Handles varchar(15) for date, varchar(6) for sector, varchar(45) for value
        """
        updated_count = inserted_count = failed_count = 0
        
        try:
            # Use connection directly for transaction management
            with self.rm_rd_conn.begin() as trans:
                for record in batch:
                    try:
                        sector = record['sector']
                        date_str = record['date']
                        upsell_percentage = record['upsell_value']  # Already formatted as "XX%"
                        
                        # Extract numeric value to check if positive
                        new_numeric = self._extract_percentage_value(upsell_percentage)
                        
                        # SKIP ANY NEGATIVE VALUES
                        if new_numeric <= 0:
                            self.logger.info(f"Skipping negative/zero upsell for {sector} {date_str}: {upsell_percentage}")
                            continue
                        
                        # Validate field lengths to prevent database errors
                        if len(date_str) > 15:
                            self.logger.error(f"Date field too long for {sector} {date_str}: {len(date_str)} chars")
                            failed_count += 1
                            continue
                            
                        if len(sector) > 6:
                            self.logger.error(f"Sector field too long: {sector} ({len(sector)} chars)")
                            failed_count += 1
                            continue
                            
                        if len(upsell_percentage) > 45:
                            self.logger.error(f"Value field too long for {sector} {date_str}: {upsell_percentage} ({len(upsell_percentage)} chars)")
                            failed_count += 1
                            continue
                        
                        # Check if record exists and get current value
                        check_query = text("""
                            SELECT value 
                            FROM QP_DW_RMALLOC.config_profile_fare_station_upsell 
                            WHERE sector = :sector AND date = :date_str
                        """)
                        
                        result = trans.execute(check_query, {
                            'sector': sector,
                            'date_str': date_str
                        })
                        
                        existing_record = result.fetchone()
                        
                        if existing_record:
                            # Record exists - check if new value is greater
                            existing_value_str = existing_record[0].strip() if existing_record[0] else "0%"
                            existing_numeric = self._extract_percentage_value(existing_value_str)
                            
                            # ADDITIONAL CHECK: Skip if existing value is negative
                            if existing_numeric <= 0:
                                self.logger.info(f"Skipping update for {sector} {date_str}: existing value is negative/zero {existing_value_str}")
                                continue
                            
                            if new_numeric > existing_numeric:
                                # Update only if new value is greater AND both are positive
                                update_query = text("""
                                    UPDATE QP_DW_RMALLOC.config_profile_fare_station_upsell 
                                    SET value = :upsell_value
                                    WHERE sector = :sector AND date = :date_str
                                """)
                                
                                trans.execute(update_query, {
                                    'upsell_value': upsell_percentage,
                                    'sector': sector,
                                    'date_str': date_str
                                })
                                updated_count += 1
                                
                            else:
                                # Skip update - existing value is greater or equal
                                self.logger.info(f"Skipping update for {sector} {date_str}: existing {existing_value_str} >= new {upsell_percentage}")
                                
                        else:
                            # Insert new record only if value is positive (already checked above)
                            insert_query = text("""
                                INSERT INTO QP_DW_RMALLOC.config_profile_fare_station_upsell 
                                (date, sector, value) 
                                VALUES (:date_str, :sector, :upsell_value)
                            """)
                            
                            trans.execute(insert_query, {
                                'date_str': date_str,
                                'sector': sector,
                                'upsell_value': upsell_percentage
                            })
                            inserted_count += 1
                            
                    except Exception as record_error:
                        self.logger.error(f"Error processing record {record}: {str(record_error)}")
                        failed_count += 1
                        continue
                
                # Transaction will auto-commit when exiting the 'with' block successfully
                
        except Exception as batch_error:
            self.logger.error(f"Error processing batch: {str(batch_error)}")
            # Mark all records in batch as failed if transaction fails
            failed_count = len(batch)
            updated_count = inserted_count = 0
        
        return updated_count, inserted_count, failed_count

    def _extract_percentage_value(self, percentage_str: str) -> float:
        """
        Extract numeric value from percentage string.
        
        Args:
            percentage_str: String like "25%" or "0%" or empty string
            
        Returns:
            float: Numeric value (e.g., 25.0 for "25%")
        """
        try:
            if not percentage_str or percentage_str.strip() == "":
                return 0.0
                
            # Remove % symbol and any whitespace, then convert to float
            numeric_str = percentage_str.strip().replace('%', '')
            return float(numeric_str) if numeric_str else 0.0
            
        except (ValueError, AttributeError) as e:
            self.logger.warning(f"Could not parse percentage value '{percentage_str}': {e}")
            return 0.0


    def validate_upsell_updates(self, statistical_df: pd.DataFrame) -> Dict:
        """Validate upsell updates by checking a sample of records."""
        validation_results = {
            "status": "not_attempted",
            "sample_size": 0,
            "matches_found": 0,
            "mismatches_found": 0,
            "records_not_found": 0
        }
        
        try:
            upsell_data = self._prepare_upsell_data(statistical_df)
            if upsell_data.empty:
                validation_results["status"] = "no_data"
                return validation_results
            
            # Sample for validation
            sample_data = upsell_data.head(FareAnalysisConfig.VALIDATION_SAMPLE_SIZE)
            validation_results["sample_size"] = len(sample_data)
            
            matches = mismatches = not_found = 0
            
            for _, row in sample_data.iterrows():
                sector = row['Sector']
                date_formatted = row['date_formatted']
                expected_percentage = row['upsell_percentage']  # Already formatted as "XX%"
                
                query = text("""
                    SELECT value 
                    FROM QP_DW_RMALLOC.config_profile_fare_station_upsell 
                    WHERE sector = :sector AND date = :date_str
                """)
                
                result = pd.read_sql(query, self.rm_rd_conn, params={
                    'sector': sector,
                    'date_str': date_formatted
                })
                
                if result.empty:
                    not_found += 1
                else:
                    db_value = result.iloc[0]['value'].strip()  # Remove any whitespace
                    if db_value == expected_percentage:
                        matches += 1
                    else:
                        mismatches += 1
                        self.logger.error(f"Mismatch for {sector} {date_formatted}: Expected {expected_percentage}, Found {db_value}")
            
            validation_results.update({
                "matches_found": matches,
                "mismatches_found": mismatches,
                "records_not_found": not_found,
                "status": "success"
            })
            
        except Exception as e:
            validation_results["status"] = "error"
            validation_results["error"] = str(e)
            self.logger.error(f"Error validating upsell updates: {str(e)}")
        
        return validation_results

    # ============================================================================
    # S3 METHODS
    # ============================================================================
    
    def upload_to_s3(self, df: pd.DataFrame) -> None:
        """Upload results to S3."""
        try:
            current_year = datetime.now().strftime('%Y')
            current_month = datetime.now().strftime('%m')
            s3_key = (f"{self.s3_file_path}forecast/{current_year}/{current_month}/"
                     f"Holiday_Upsell_Forecast_{current_year}_{current_month}.csv")
            
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_content = csv_buffer.getvalue()
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=csv_content
            )
            
        except Exception as e:
            self.logger.error(f"Error uploading to S3: {e}")
            raise

    # ============================================================================
    # MAIN EXECUTION METHOD
    # ============================================================================
    
    def run(self) -> Dict:
        """Main method to run the complete fare analysis pipeline."""
        start_time = time.time()
        results = {
            "status": "not_attempted",
            "execution_time_seconds": 0,
            "output_file": None,
            "total_records": 0,
            "sectors_processed": 0,
            "total_bands": 0,
            "bands_with_holidays": 0,
            "upsell_table_update": None,
            "validation_results": None
        }
        
        try:
            # Step 1: Fetch and validate data
            df = self._fetch_calendar_data()
            if df.empty:
                results["status"] = "no_data"
                return results
            
            # Step 2: Calculate statistics
            statistical_df = self.calculate_statistics(df)
            if statistical_df.empty:
                results["status"] = "calculation_failed"
                return results
            
            # Step 3: Add holiday analysis and upsell calculation
            statistical_df_with_holidays = self.add_holiday_analysis(statistical_df)
            
            # Step 4: Save results
            self.upload_to_s3(statistical_df_with_holidays)
            
            # Step 5: Update upsell table
            upsell_update_results = self.update_upsell_table(statistical_df_with_holidays)
            results["upsell_table_update"] = upsell_update_results
            
            # Step 6: Validate updates
            if upsell_update_results["status"] == "success":
                validation_results = self.validate_upsell_updates(statistical_df_with_holidays)
                results["validation_results"] = validation_results
            
            # Step 7: Calculate summary statistics
            results.update(self._calculate_summary_statistics(statistical_df_with_holidays))
            results["status"] = "success"
            
            self._print_execution_summary(results, time.time() - start_time)
            
        except Exception as e:
            results["status"] = "error"
            results["error"] = str(e)
            self.logger.error(f"Error in statistical analysis: {str(e)}")
        
        finally:
            results["execution_time_seconds"] = round(time.time() - start_time, 2)
        
        return results

    def _calculate_summary_statistics(self, df: pd.DataFrame) -> Dict:
        """Calculate summary statistics for the results."""
        total_bands = df['band_id'].nunique() - (1 if df['band_id'].isna().any() else 0)
        bands_with_holidays = len(df[
            (df['band_id'].notna()) & 
            (df['holiday_name'] != 'No Holiday Found') &
            (df['holiday_name'].notna())
        ]['band_id'].unique()) if total_bands > 0 else 0
        
        return {
            "total_records": len(df),
            "sectors_processed": df['Sector'].nunique(),
            "total_bands": total_bands,
            "bands_with_holidays": bands_with_holidays
        }

    def _print_execution_summary(self, results: Dict, execution_time: float) -> None:
        """Print comprehensive execution summary."""
        print(f"\n{'='*60}")
        print("EXECUTION SUMMARY")
        print(f"{'='*60}")
        print(f"Total execution time: {round(execution_time, 2)} seconds")
        print(f"Records processed: {results['total_records']}")
        print(f"Sectors processed: {results['sectors_processed']}")
        print(f"Peak bands identified: {results['total_bands']}")
        print(f"Bands with holidays: {results['bands_with_holidays']}")
        
        upsell_results = results.get("upsell_table_update")
        if upsell_results and upsell_results["status"] == "success":
            print(f"\nUpsell Table Updates:")
            print(f"  - Records updated: {upsell_results['records_updated']}")
            print(f"  - Records inserted: {upsell_results['records_inserted']}")
            print(f"  - Records failed: {upsell_results['records_failed']}")
            print(f"  - Update time: {upsell_results['execution_time_seconds']} seconds")
            
            validation_results = results.get("validation_results")
            if validation_results and validation_results["status"] == "success":
                print(f"\nValidation Results:")
                print(f"  - Sample validated: {validation_results['sample_size']}")
                print(f"  - Matches: {validation_results['matches_found']}")
                print(f"  - Mismatches: {validation_results['mismatches_found']}")
        elif upsell_results:
            print(f"\nUpsell Table Update Failed: {upsell_results.get('error', 'Unknown error')}")
        
        print(f"{'='*60}")