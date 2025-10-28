import io
import boto3
import pandas as pd
import numpy as np
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple, List
from pathlib import Path
import configsettings as config
from pymongo import MongoClient, UpdateOne
from bson import ObjectId


class FareAnalysisConfig:
    """Configuration constants for the fare analysis."""
    ROLLING_WINDOW_SIZE = 29
    PEAK_THRESHOLD = 1.1
    SMOOTHING_WINDOW = 5
    ANALYSIS_PERIOD_DAYS = 365
    CUTOFF_BUFFER_DAYS = 15
    START_BUFFER_DAYS = 14
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
        """Initialize with MongoDB connection and configuration."""
        self.logger = logger
        self.mongo_conn = params['mongoconn']  # Single MongoDB connection
        self.db = self.mongo_conn["qp_dw_rmalloc"]  # MongoDB database
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
        """Fetch fare data from calendar_AF collection, excluding international sectors."""
        try:
            collection = self.db['calendar_AF']
            
            # Query MongoDB with aggregation to format date
            pipeline = [
                {
                    '$match': {
                        'AverageFare': {'$exists': True, '$ne': None, '$gt': 0}
                    }
                },
                {
                    '$project': {
                        '_id': 0,
                        'Sector': 1,
                        'DepartureDate': {
                            '$dateToString': {
                                'format': '%Y-%m-%d',
                                'date': '$DepartureDate'
                            }
                        },
                        'AverageFare': 1
                    }
                },
                {
                    '$sort': {'Sector': 1, 'DepartureDate': 1}
                }
            ]
            
            cursor = collection.aggregate(pipeline)
            df = pd.DataFrame(list(cursor))
            
            if df.empty:
                return df
            
            # Convert data types
            df['DepartureDate'] = pd.to_datetime(df['DepartureDate'])
            df['AverageFare'] = pd.to_numeric(df['AverageFare'], errors='coerce')
            df = df.dropna(subset=['AverageFare'])
            
            # Filter out international sectors
            df = self._filter_domestic_sectors(df)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error fetching calendar data from MongoDB: {e}")
            return pd.DataFrame()
    
    def _filter_domestic_sectors(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter out international sectors based on config_station_master."""
        try:
            if df.empty:
                return df
            
            # Get international stations from config_station_master
            station_master_collection = self.db['config_station_master']
            international_stations = station_master_collection.find(
                {'route': 'I'},
                {'station': 1, '_id': 0}
            )
            
            # Create set of international station codes
            international_set = {doc['station'] for doc in international_stations}
            
            if not international_set:
                self.logger.info("No international stations found in config_station_master")
                return df
            
            self.logger.info(f"Found {len(international_set)} international stations")
            
            # Extract origin and destination from Sector (format: "XXX-YYY")
            df[['Origin', 'Destination']] = df['Sector'].str.split('-', expand=True)
            
            # Filter out sectors where either origin or destination is international
            initial_count = len(df)
            df = df[
                ~(df['Origin'].isin(international_set) | df['Destination'].isin(international_set))
            ]
            
            # Drop temporary columns
            df = df.drop(columns=['Origin', 'Destination'])
            
            filtered_count = initial_count - len(df)
            self.logger.info(f"Filtered out {filtered_count} international sector records")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error filtering international sectors: {e}")
            # Return original dataframe if filtering fails
            return df

    def _fetch_holidays(self) -> pd.DataFrame:
        """Fetch holidays from config_holiday_list collection."""
        try:
            collection = self.db['config_holiday_list']
            
            # Calculate date range
            start_date = datetime.now() - timedelta(days=365)
            end_date = datetime.now() + timedelta(days=365)
            
            query = {
                'Date': {
                    '$gte': start_date,
                    '$lte': end_date
                }
            }
            
            projection = {
                '_id': 0,
                'Date': 1,
                'Holiday': 1
            }
            
            cursor = collection.find(query, projection).sort('Date', 1)
            holidays_df = pd.DataFrame(list(cursor))
            
            if not holidays_df.empty:
                holidays_df.rename(columns={'Date': 'holiday_date', 'Holiday': 'holiday_name'}, inplace=True)
                holidays_df['holiday_date'] = pd.to_datetime(holidays_df['holiday_date'])
            
            return holidays_df
            
        except Exception as e:
            self.logger.error(f"Error fetching holidays from MongoDB: {e}")
            return pd.DataFrame()

    def _fetch_profile_fares(self) -> pd.DataFrame:
        """Fetch profile fares data for upsell calculation."""
        try:
            collection = self.db['config_profile_fares']
            
            # MongoDB aggregation pipeline
            pipeline = [
                {
                    '$group': {
                        '_id': {
                            'sector': '$sector',
                            'month': '$month',
                            'dow': '$dow'
                        },
                        'Q10': {'$sum': {'$divide': ['$Q10', 6]}}
                    }
                },
                {
                    '$project': {
                        '_id': 0,
                        'sector': '$_id.sector',
                        'month': '$_id.month',
                        'dow': '$_id.dow',
                        'Q10': 1
                    }
                }
            ]
            
            cursor = collection.aggregate(pipeline)
            return pd.DataFrame(list(cursor))
            
        except Exception as e:
            self.logger.error(f"Error fetching profile fares from MongoDB: {e}")
            return pd.DataFrame()

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
        
        # Process non-operational days
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
                
            for idx in band_data.index:
                profile_q10 = statistical_df.loc[idx, 'profileFaresQ10']
                
                if pd.notna(profile_q10) and profile_q10 > 0:
                    upsell_value = (band_data.loc[idx, 'AF_LastHolidayDOW'] - profile_q10) / profile_q10
                    statistical_df.loc[idx, 'upsell'] = upsell_value
    
    def update_upsell_table(self, statistical_df: pd.DataFrame) -> Dict:
        """Update config_profile_fare_station_upsell collection with calculated upsell values."""
        start_time = time.time()
        update_results = {
            "status": "not_attempted",
            "records_to_update": 0,
            "records_updated": 0,
            "records_inserted": 0,
            "records_skipped": 0,
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
            self.logger.error(f"Error updating upsell collection: {str(e)}")
        
        finally:
            update_results["execution_time_seconds"] = round(time.time() - start_time, 2)
        
        return update_results

    def _prepare_upsell_data(self, statistical_df: pd.DataFrame) -> pd.DataFrame:
        """Prepare upsell data for database update."""
        upsell_data = statistical_df[
            (statistical_df['band_id'].notna()) & 
            (statistical_df['upsell'].notna()) & 
            (statistical_df['next_year_date'].notna())
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
            lambda x: f"{round(x * 100)}%"
        )

        return upsell_summary

    def _process_upsell_batches(self, upsell_data: pd.DataFrame) -> Tuple[int, int, int]:
        """Process upsell data in batches."""
        records_to_process = [
            {
                'sector': row['Sector'],
                'date': row['date_formatted'],
                'upsell_value': row['upsell_percentage']  # Formatted percentage string
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
        Process a single batch of upsell records using MongoDB.
        
        Update Logic:
        - If current upsell value has decimals: Skip update
        - Otherwise: Always update regardless of sign or magnitude
        """
        updated_count = inserted_count = failed_count = 0
        
        try:
            collection = self.db['config_profile_fare_station_upsell']
            
            for record in batch:
                try:
                    sector = record['sector']
                    date_str = record['date']
                    upsell_percentage = record['upsell_value']  # Already formatted as "XX%"
                    
                    # Validate field lengths
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
                    
                    # Check if record exists
                    existing_record = collection.find_one({
                        'sector': sector,
                        'date': date_str
                    })
                    
                    if existing_record and 'value' in existing_record:
                        # Check if existing value has decimals
                        existing_value_str = existing_record.get('value', '0%').strip()
                        existing_numeric = self._extract_percentage_value(existing_value_str)
                        
                        # Skip update if existing value has decimals
                        if existing_numeric != int(existing_numeric):
                            self.logger.info(
                                f"Skipping update for {sector} {date_str}: "
                                f"Current value {existing_value_str} has decimals"
                            )
                            continue
                    
                    # Update or insert the record
                    result = collection.update_one(
                        {
                            'sector': sector,
                            'date': date_str
                        },
                        {
                            '$set': {
                                'value': upsell_percentage
                            }
                        },
                        upsert=True
                    )
                    
                    if result.matched_count > 0:
                        if result.modified_count > 0:
                            updated_count += 1
                            self.logger.info(f"Updated: {sector} {date_str} = {upsell_percentage}")
                    else:
                        # New record inserted
                        inserted_count += 1
                        self.logger.info(f"Inserted: {sector} {date_str} = {upsell_percentage}")
                        
                except Exception as e:
                    failed_count += 1
                    self.logger.error(f"Failed to process record {sector} {date_str}: {str(e)}")
                    continue
                    
        except Exception as e:
            self.logger.error(f"Batch processing error: {str(e)}")
            
        return updated_count, inserted_count, failed_count

    def _extract_percentage_value(self, percentage_str: str) -> float:
        """Extract numeric value from percentage string."""
        try:
            # Remove '%' and any whitespace, then convert to float
            return float(percentage_str.replace('%', '').strip())
        except (ValueError, AttributeError):
            return 0.0

    def validate_upsell_updates(self, upsell_data: pd.DataFrame) -> Dict:
        """Validate that upsell updates were correctly written to the database."""
        validation_results = {
            "status": "not_attempted",
            "sample_size": 0,
            "matches_found": 0,
            "mismatches_found": 0,
            "records_not_found": 0
        }
        
        try:
            sample_data = upsell_data.head(FareAnalysisConfig.VALIDATION_SAMPLE_SIZE)
            validation_results["sample_size"] = len(sample_data)
            
            collection = self.db['config_profile_fare_station_upsell']
            matches = mismatches = not_found = 0
            
            for _, row in sample_data.iterrows():
                sector = row['Sector']
                date_formatted = row['date_formatted']
                expected_percentage = row['upsell_percentage']  # Already formatted as "XX%"
                
                result = collection.find_one({
                    'sector': sector,
                    'date': date_formatted
                })
                
                if not result:
                    not_found += 1
                else:
                    db_value = result.get('value', '').strip()  # Remove any whitespace
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
            
        except Exception as e:
            results["status"] = "error"
            results["error"] = str(e)
            self.logger.error(f"Error in statistical analysis: {str(e)}")
        
        finally:
            results["execution_time_seconds"] = round(time.time() - start_time, 2)
        
        return results