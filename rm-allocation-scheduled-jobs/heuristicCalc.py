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
from pymongo import MongoClient


class FareAnalysisConfig:
    """Configuration constants for the fare analysis."""
    BATCH_SIZE = 1000
    VALIDATION_SAMPLE_SIZE = 100
    DECIMAL_PRECISION = 6
    ANALYSIS_LOOKBACK_YEARS = 2
    
    DOW_ALIASES = {
        'Monday': 'Friday',
        'Tuesday': 'Thursday', 
        'Wednesday': 'Wednesday',
        'Thursday': 'Tuesday',
        'Friday': 'Monday',
        'Saturday': 'Sunday',
        'Sunday': 'Saturday'
    }


class BandConfig:
    """Band definitions relative to holiday (±4 weeks + 1 day, excluding holiday itself)."""
    BANDS = {
                'P4': (-27, -21),
                'P3': (-20, -14),
                'P2': (-13, -7),
                'P1': (-6, -1),
                'S1': (0, 6),
                'S2': (7, 13),
                'S3': (14, 20),
                'S4': (21, 27)
            }
    
    DOW_MATCHING_BANDS = ['P2', 'P3', 'P4', 'S2', 'S3', 'S4']
    DIRECT_BANDS = ['P1', 'S1']


class FareStatisticalAnalysis:
    """Holiday-based fare projection using band mapping with weighted averages."""
    
    def __init__(self, logger, params, overrides, context):
        self.logger = logger
        self.mongo_conn = params['mongoconn']
        self.db = self.mongo_conn["qp_dw_rmalloc"]
        self.s3_client = params['s3']
        self.bucket_name = config.S3_RM_BUCKET_NAME
        self.s3_file_path = config.S3_FILE_PATH
        
        self.start_date = (datetime.now() + timedelta(days=1)).date()
        self.end_date = self.start_date + timedelta(days=365)
        
        self.holiday_mappings_cache = None
        self.profile_fares_cache = None
        self.domestic_sectors_cache = None

    # ============================================================================
    # DATA FETCHING
    # ============================================================================
    
    def _fetch_domestic_sectors(self) -> List[str]:
        """Fetch only domestic sectors from station master."""
        collection = self.db['config_station_master']
        
        # MongoDB query
        pipeline = [
            {'$match': {'route': 'D'}},
            {'$group': {'_id': '$station'}},
            {'$project': {'station': '$_id', '_id': 0}}
        ]
        
        results = list(collection.aggregate(pipeline))
        return [doc['station'] for doc in results] if results else []
    
    def _filter_domestic_sectors(self, sectors: List[str]) -> List[str]:
        """Filter to keep only domestic sectors."""
        if not self.domestic_sectors_cache:
            return sectors
        
        domestic_stations = set(self.domestic_sectors_cache)
        
        # Filter sectors where both origin and destination are domestic
        domestic_sectors = []
        for sector in sectors:
            if len(sector) >= 6:
                origin = sector[:3]
                destination = sector[3:6]
                if origin in domestic_stations and destination in domestic_stations:
                    domestic_sectors.append(sector)
        
        return domestic_sectors
    
    def _fetch_calendar_data(self) -> pd.DataFrame:
        """Fetch historical fare data for domestic sectors only."""
        lookback_date = datetime.now() - timedelta(days=365 * FareAnalysisConfig.ANALYSIS_LOOKBACK_YEARS)
        lookback_date_str = lookback_date.strftime('%Y-%m-%d')
        
        collection = self.db['calendar_af']
        
        # MongoDB query - DepartureDate stored as string 'YYYY-MM-DD'
        query = {
            'AverageFare': {'$exists': True, '$ne': None, '$gt': 0},
            'DepartureDate': {'$gte': lookback_date_str}
        }
        
        projection = {
            '_id': 0,
            'Sector': 1,
            'DepartureDate': 1,
            'AverageFare': 1
        }
        
        results = list(collection.find(query, projection).sort('Sector', 1).sort('DepartureDate', 1))
        df = pd.DataFrame(results)
        
        if not df.empty:
            df['DepartureDate'] = pd.to_datetime(df['DepartureDate'])
            df['AverageFare'] = pd.to_numeric(df['AverageFare'], errors='coerce')
            df = df.dropna(subset=['AverageFare'])
            
            # Filter domestic sectors
            domestic_sectors = self._filter_domestic_sectors(df['Sector'].unique().tolist())
            df = df[df['Sector'].isin(domestic_sectors)]
        
        return df

    def _fetch_holidays(self) -> pd.DataFrame:
        """Fetch holidays for historical and target years."""
        historical_start = datetime.now() - timedelta(days=730)
        historical_start_str = historical_start.strftime('%Y-%m-%d')
        end_date_str = self.end_date.strftime('%Y-%m-%d')
        
        collection = self.db['config_holiday_list']
        
        # MongoDB query - Date stored as string 'YYYY-MM-DD'
        query = {
            'Date': {
                '$gte': historical_start_str,
                '$lte': end_date_str
            }
        }
        
        projection = {
            '_id': 0,
            'Date': 1,
            'Holiday': 1
        }
        
        results = list(collection.find(query, projection).sort('Date', 1))
        holidays_df = pd.DataFrame(results)
        
        if not holidays_df.empty:
            holidays_df.rename(columns={'Date': 'holiday_date', 'Holiday': 'holiday_name'}, inplace=True)
            holidays_df['holiday_date'] = pd.to_datetime(holidays_df['holiday_date'])
            holidays_df['year'] = holidays_df['holiday_date'].dt.year
        
        return holidays_df
    
    def _fetch_mapped_months(self) -> pd.DataFrame:
        """Fetch mapped months for date range."""
        start_date = datetime.now()
        end_date = datetime.now() + timedelta(days=365)
        start_date_str = start_date.strftime('%m/%d/%Y')  # Changed to MM/DD/YYYY
        end_date_str = end_date.strftime('%m/%d/%Y')
        
        collection = self.db['config_date_event']
        
        # Fetch all documents in range
        query = {}  # Fetch all for now, filter in pandas
        
        projection = {
            '_id': 0,
            'date': 1,
            'mappedMonth': 1
        }
        
        results = list(collection.find(query, projection))
        mapped_month_df = pd.DataFrame(results)
        
        if not mapped_month_df.empty:
            # Convert string date to datetime
            mapped_month_df['date_dt'] = pd.to_datetime(mapped_month_df['date'], format='%m/%d/%Y')
            
            # Filter by date range
            mapped_month_df = mapped_month_df[
                (mapped_month_df['date_dt'] >= start_date) & 
                (mapped_month_df['date_dt'] <= end_date)
            ]
            
            # Keep the datetime version for merging
            mapped_month_df = mapped_month_df[['date_dt', 'mappedMonth']]
            mapped_month_df.rename(columns={'date_dt': 'date'}, inplace=True)
        
        return mapped_month_df

    def _fetch_profile_fares(self) -> pd.DataFrame:
        """Fetch profile fares for upsell calculation - keep H1/H2 separate."""
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
                    'Q10': {'$sum': '$Q10'}
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'sector': '$_id.sector',
                    'month': '$_id.month',
                    'dow': '$_id.dow',
                    'Q10': {'$divide': ['$Q10', 6]}
                }
            }
        ]
        
        results = list(collection.aggregate(pipeline))
        df = pd.DataFrame(results)
        
        if not df.empty:
            # Keep original month with H1/H2
            df['month_lower'] = df['month'].str.lower()
            df['dow_lower'] = df['dow'].str.lower()
        
        return df

    def _get_month_half(self, date: datetime) -> str:
        """
        Determine if date is in first or second half of month.
        First half: days 1-15 → H1
        Second half: days 16+ → H2
        """
        return 'H1' if date.day <= 15 else 'H2'

    # ============================================================================
    # BAND EXTRACTION AND MAPPING
    # ============================================================================
    
    def preprocess_holiday_bands(self, calendar_data: pd.DataFrame, holidays_df: pd.DataFrame) -> Dict:
        """Extract historical bands and create mappings to future dates."""
        start_time = time.time()
        
        holiday_mappings = {}
        skipped_holidays = []
        
        # Use all holidays before start_date as historical
        historical_holidays = holidays_df[holidays_df['holiday_date'].dt.date < self.start_date]
        
        target_holidays = holidays_df[
            (holidays_df['holiday_date'].dt.date >= self.start_date) &
            (holidays_df['holiday_date'].dt.date <= self.end_date)
        ]
        
        
        for _, target_holiday in target_holidays.iterrows():
            holiday_name = target_holiday['holiday_name']
            target_date = target_holiday['holiday_date']
            
            hist_holiday = historical_holidays[
                historical_holidays['holiday_name'] == holiday_name
            ].sort_values('holiday_date', ascending=False).head(1)
            
            if hist_holiday.empty:
                skipped_holidays.append(f"{holiday_name} ({target_date.date()})")
                continue
                
            hist_date = hist_holiday.iloc[0]['holiday_date']
            
            holiday_mappings[holiday_name] = {
                'target_date': target_date,
                'hist_date': hist_date,
                'sectors': {}
            }
            
            for sector in calendar_data['Sector'].unique():
                sector_data = calendar_data[calendar_data['Sector'] == sector]
                sector_bands = self._extract_and_map_bands(
                    sector_data, hist_date, target_date
                )
                
                if sector_bands:
                    holiday_mappings[holiday_name]['sectors'][sector] = sector_bands
        
        return holiday_mappings

    def _extract_and_map_bands(self, sector_data: pd.DataFrame, 
                               hist_date: datetime, target_date: datetime) -> Dict:
        """Extract historical bands and map to target dates."""
        bands_mapping = {}
        
        for band_name, (start_offset, end_offset) in BandConfig.BANDS.items():
            hist_start = hist_date + timedelta(days=start_offset)
            hist_end = hist_date + timedelta(days=end_offset)
            
            hist_band = sector_data[
                (sector_data['DepartureDate'] >= hist_start) &
                (sector_data['DepartureDate'] <= hist_end)
            ].copy()
            
            if hist_band.empty:
                continue
            
            target_start = target_date + timedelta(days=start_offset)
            target_end = target_date + timedelta(days=end_offset)
            target_dates = pd.date_range(target_start, target_end, freq='D')
            
            mapped_dates = []
            
            if band_name in BandConfig.DIRECT_BANDS:
                for target_dt in target_dates:
                    days_offset = (target_dt - target_date).days
                    hist_dt = hist_date + timedelta(days=days_offset)
                    
                    fare_row = hist_band[hist_band['DepartureDate'].dt.date == hist_dt.date()]
                    if not fare_row.empty:
                        mapped_dates.append({
                            'target_date': target_dt.date(),
                            'fare': fare_row.iloc[0]['AverageFare'],
                            'match_type': 'direct',
                            'band': band_name,
                            'mapped_dow': target_dt.strftime('%A'),
                            'days_from_holiday': days_offset
                        })
            else:
                hist_band['day_of_week'] = hist_band['DepartureDate'].dt.day_name()
                
                for target_dt in target_dates:
                    target_dow = target_dt.strftime('%A')
                    days_offset = (target_dt - target_date).days
                    fare = self._get_dow_matched_fare(hist_band, target_dow)
                    
                    if fare is not None:
                        mapped_dates.append({
                            'target_date': target_dt.date(),
                            'fare': fare,
                            'match_type': 'dow_matched',
                            'band': band_name,
                            'mapped_dow': target_dow,
                            'days_from_holiday': days_offset
                        })
            
            if mapped_dates:
                bands_mapping[band_name] = mapped_dates
        
        return bands_mapping

    def _get_dow_matched_fare(self, band_data: pd.DataFrame, target_dow: str) -> Optional[float]:
        """Get fare using DOW matching priority - returns first matching occurrence."""
        if band_data.empty:
            return None
        
        same_dow = band_data[band_data['day_of_week'] == target_dow]
        if not same_dow.empty:
            return same_dow.iloc[0]['AverageFare']
        
        alias_dow = FareAnalysisConfig.DOW_ALIASES.get(target_dow)
        if alias_dow:
            alias_data = band_data[band_data['day_of_week'] == alias_dow]
            if not alias_data.empty:
                return alias_data.iloc[0]['AverageFare']
        
        wednesday_data = band_data[band_data['day_of_week'] == 'Wednesday']
        if not wednesday_data.empty:
            return wednesday_data.iloc[0]['AverageFare']
        
        return band_data['AverageFare'].max()

    # ============================================================================
    # VOID PERIOD HANDLING
    # ============================================================================
    
    def _get_dow_position_in_month(self, date: datetime) -> int:
        """Get the nth occurrence of this DOW in the month."""
        day = date.day
        return (day - 1) // 7 + 1

    def _handle_void_period(self, sector: str, target_date: datetime, 
                           calendar_data: pd.DataFrame) -> Optional[float]:
        """Handle dates outside holiday bands using DOW position matching."""
        target_dow = target_date.strftime('%A')
        target_month = target_date.month
        target_position = self._get_dow_position_in_month(target_date)
        
        prev_year = target_date.year - 1
        
        historical_data = calendar_data[
            (calendar_data['Sector'] == sector) &
            (calendar_data['DepartureDate'].dt.year == prev_year) &
            (calendar_data['DepartureDate'].dt.month == target_month)
        ].copy()
        
        if historical_data.empty:
            return None
        
        historical_data['day_of_week'] = historical_data['DepartureDate'].dt.day_name()
        historical_data['dow_position'] = historical_data['DepartureDate'].apply(
            self._get_dow_position_in_month
        )
        
        exact_match = historical_data[
            (historical_data['day_of_week'] == target_dow) &
            (historical_data['dow_position'] == target_position)
        ]
        
        if not exact_match.empty:
            return exact_match['AverageFare'].iloc[0]
        
        fallback = historical_data[
            (historical_data['day_of_week'] == target_dow) &
            (historical_data['dow_position'] == target_position - 1)
        ]
        
        if not fallback.empty:
            return fallback['AverageFare'].iloc[0]
        
        return None

    # ============================================================================
    # WEIGHTED FARE CALCULATION (CORRECTED WEIGHTS)
    # ============================================================================
    
    def _calculate_weight(self, days_from_holiday: int) -> float:
        """
        Calculate weight using exponential decay formula:
        w(x) = 1 if -7 <= x <= 7 (P1/S1 bands)
        w(x) = e^(-(x-7)) if x > 7 (S2, S3, S4)
        w(x) = e^(x+7) if x < -7 (P2, P3, P4)
        """
        if -7 <= days_from_holiday <= 7:
            return 1.0
        elif days_from_holiday > 7:
            return np.exp(-(days_from_holiday - 7))
        else:
            return np.exp(days_from_holiday + 7)

    def _calculate_projected_fare_from_bands(self, sector: str, target_date: datetime,
                                            holiday_mappings: Dict) -> Tuple[Optional[float], Dict]:
        """Calculate fare using weighted average across all relevant holiday bands."""
        numerator = 0.0
        denominator = 0.0
        
        target_date_obj = target_date.date() if isinstance(target_date, datetime) else target_date
        
        debug_info = {
            'holidays': [],
            'bands': [],
            'weights': [],
            'fares': [],
            'match_types': [],
            'mapped_dows': []
        }
        
        for holiday_name, holiday_data in holiday_mappings.items():
            sectors_dict = holiday_data.get('sectors', {})
            sector_bands = sectors_dict.get(sector)
            
            if not sector_bands:
                continue
            
            for band_name, mapped_dates in sector_bands.items():
                for mapping in mapped_dates:
                    mapping_date = mapping['target_date']
                    if isinstance(mapping_date, datetime):
                        mapping_date = mapping_date.date()
                    
                    if mapping_date == target_date_obj:
                        fare = mapping['fare']
                        days_from_holiday = mapping.get('days_from_holiday', 0)
                        weight = self._calculate_weight(days_from_holiday)
                        match_type = mapping.get('match_type', 'unknown')
                        mapped_dow = mapping.get('mapped_dow', 'N/A')
                        
                        debug_info['holidays'].append(holiday_name)
                        debug_info['bands'].append(band_name)
                        debug_info['weights'].append(f"{weight:.4f}")
                        debug_info['fares'].append(f"{fare:.2f}")
                        debug_info['match_types'].append(match_type)
                        debug_info['mapped_dows'].append(mapped_dow)
                        
                        numerator += weight * fare
                        denominator += weight
                        break
        
        result = numerator / denominator if denominator > 0 else None
        return result, debug_info

    # ============================================================================
    # SECTOR PROCESSING
    # ============================================================================
    
    def process_sector_year_projection(self, sector: str, 
                                       holiday_mappings: Dict,
                                       calendar_data: pd.DataFrame) -> pd.DataFrame:
        """Process one sector for next 365 days."""
        date_range = pd.date_range(start=self.start_date, end=self.end_date, freq='D')
        
        results = []
        
        for target_date in date_range:
            projected_af, debug_info = self._calculate_projected_fare_from_bands(
                sector, target_date, holiday_mappings
            )
            
            if projected_af is not None:
                holiday_context = '|'.join(debug_info['holidays']) if debug_info['holidays'] else 'Holiday Period'
                bands_used = '|'.join(debug_info['bands'])
                weights_used = '|'.join(debug_info['weights'])
                fares_used = '|'.join(debug_info['fares'])
                match_types = '|'.join(debug_info['match_types'])
                mapped_dows = '|'.join(debug_info['mapped_dows'])
            else:
                projected_af = self._handle_void_period(sector, target_date, calendar_data)
                if projected_af is not None:
                    holiday_context = 'Void Period'
                    bands_used = 'N/A'
                    weights_used = 'N/A'
                    fares_used = f"{projected_af:.2f}"
                    match_types = 'void_dow_position'
                    mapped_dows = target_date.strftime('%A')
                else:
                    continue
            
            results.append({
                'Sector': sector,
                'DepartureDate': target_date,
                'ProjectedAF': round(projected_af, FareAnalysisConfig.DECIMAL_PRECISION),
                'Holidays': holiday_context,
                'Bands': bands_used,
                'Weights': weights_used,
                'HistoricalFares': fares_used,
                'MatchTypes': match_types,
                'MappedDOW': mapped_dows,
                'Month': target_date.strftime('%B'),
                'DayOfWeek': target_date.strftime('%A')
            })
        
        return pd.DataFrame(results)

    def run_sequential_processing(self, sectors: List[str]) -> pd.DataFrame:
        """Process sectors sequentially."""
        calendar_data = self._fetch_calendar_data()
        holidays_df = self._fetch_holidays()
        
        self.holiday_mappings_cache = self.preprocess_holiday_bands(calendar_data, holidays_df)
        self.profile_fares_cache = self._fetch_profile_fares()
        self.mapped_months_cache = self._fetch_mapped_months()
        
        results = []
        total_sectors = len(sectors)
        
        for idx, sector in enumerate(sectors, 1):
            try:
                sector_df = self.process_sector_year_projection(
                    sector,
                    self.holiday_mappings_cache,
                    calendar_data
                )
                
                if not sector_df.empty:
                    results.append(sector_df)
                    
            except Exception as e:
                self.logger.error(f"Error processing {sector}: {e}")
                continue
        
        if not results:
            return pd.DataFrame()
        
        final_df = pd.concat(results, ignore_index=True)
        final_df = self._calculate_upsells_vectorized(final_df)
        
        return final_df

    # ============================================================================
    # UPSELL CALCULATION
    # ============================================================================
    
    def _calculate_upsells_vectorized(self, projection_df: pd.DataFrame) -> pd.DataFrame:
        """Calculate upsells using vectorized operations with mappedMonth logic."""
        if projection_df.empty or self.profile_fares_cache is None or self.profile_fares_cache.empty:
            projection_df['ProfileFareQ10'] = None
            projection_df['Upsell'] = None
            return projection_df
        
        profile_fares = self.profile_fares_cache
        
        # Convert DepartureDate to datetime if not already
        projection_df['DepartureDate_dt'] = pd.to_datetime(projection_df['DepartureDate'])
        
        # Merge with mapped months to get the mappedMonth for each departure date
        if self.mapped_months_cache is not None and not self.mapped_months_cache.empty:
            projection_df = projection_df.merge(
                self.mapped_months_cache,
                left_on='DepartureDate_dt',
                right_on='date',
                how='left'
            )
            
            # Drop the 'date' column from the merge (duplicate of DepartureDate_dt)
            projection_df.drop(columns=['date'], inplace=True, errors='ignore')
        else:
            # If no mapped months cache, create empty mappedMonth column
            projection_df['mappedMonth'] = None
        
        # Calculate fallback month_half
        projection_df['month_half_calc'] = projection_df['DepartureDate_dt'].apply(
            lambda x: 'H1' if x.day <= 15 else 'H2'
        )
        
        # Use mappedMonth if available, otherwise construct from Month + calculated half
        projection_df['month_with_half'] = np.where(
            projection_df['mappedMonth'].notna(),
            projection_df['mappedMonth'].str.lower(),
            projection_df['Month'].str.lower() + '_' + projection_df['month_half_calc'].str.lower()
        )
        
        projection_df['dow_lower'] = projection_df['DayOfWeek'].str.lower()
        
        # Merge with profile fares using the mapped month
        projection_df = projection_df.merge(
            profile_fares[['sector', 'month_lower', 'dow_lower', 'Q10']],
            left_on=['Sector', 'month_with_half', 'dow_lower'],
            right_on=['sector', 'month_lower', 'dow_lower'],
            how='left'
        )
        
        projection_df.rename(columns={'Q10': 'ProfileFareQ10'}, inplace=True)
        projection_df.drop(columns=['sector', 'month_lower', 'dow_lower', 'month_half_calc', 
                                    'month_with_half', 'DepartureDate_dt', 'mappedMonth'], 
                        inplace=True, errors='ignore')
        
        # Calculate upsell as whole number percentage
        projection_df['Upsell'] = np.where(
            (projection_df['ProfileFareQ10'].notna()) & (projection_df['ProfileFareQ10'] > 0),
            np.round((projection_df['ProjectedAF'] - projection_df['ProfileFareQ10']) / projection_df['ProfileFareQ10'] * 100),
            None
        )
        
        return projection_df

    # ============================================================================
    # DATABASE UPDATES
    # ============================================================================
    
    def update_upsell_table(self, projection_df: pd.DataFrame) -> Dict:
        """Update config_profile_fare_station_upsell collection with calculated upsell values."""
        start_time = time.time()
        update_results = {
            "status": "not_attempted",
            "records_to_update": 0,
            "records_updated": 0,
            "records_inserted": 0,
            "records_skipped_manual": 0,
            "records_failed": 0,
            "execution_time_seconds": 0
        }
        
        try:
            upsell_data = self._prepare_upsell_data(projection_df)
            if upsell_data.empty:
                update_results["status"] = "no_data"
                return update_results
            
            update_results["records_to_update"] = len(upsell_data)
            
            total_updated, total_inserted, total_failed = self._process_upsell_batches(upsell_data)
            
            total_processed = total_updated + total_inserted + total_failed
            total_skipped = update_results["records_to_update"] - total_processed
            
            update_results.update({
                "records_updated": total_updated,
                "records_inserted": total_inserted,
                "records_skipped_manual": total_skipped,
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

    def _prepare_upsell_data(self, projection_df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare upsell data for database update.
        
        Logic:
        1. Filter records with valid Upsell values
        2. Group by Sector and DepartureDate, taking MAX upsell per combination
        3. Format upsell as whole number percentage (without % symbol)
        """
        # Filter valid upsell records
        upsell_data = projection_df[
            (projection_df['Upsell'].notna()) & 
            (projection_df['Upsell'] > 0)
        ].copy()
        
        if upsell_data.empty:
            return pd.DataFrame()

        # Group by Sector and Date, taking maximum upsell value
        upsell_summary = upsell_data.groupby(['Sector', 'DepartureDate']).agg({
            'Upsell': 'max'
        }).reset_index()

        # Format date as MM/DD/YYYY
        upsell_summary['date_formatted'] = pd.to_datetime(
            upsell_summary['DepartureDate']
        ).dt.strftime('%m/%d/%Y')
        
        # Convert upsell to string (already a whole number from earlier calculation)
        upsell_summary['upsell_percentage'] = upsell_summary['Upsell'].apply(
            lambda x: str(int(round(x))) if pd.notna(x) else None
        )
        
        # Remove any records with null or zero upsell
        upsell_summary = upsell_summary[
            (upsell_summary['upsell_percentage'].notna()) & 
            (upsell_summary['upsell_percentage'] != '0')
        ]

        return upsell_summary

    def _process_upsell_batches(self, upsell_data: pd.DataFrame) -> Tuple[int, int, int]:
        """Process upsell data in batches."""
        records_to_process = [
            {
                'sector': row['Sector'],
                'date': row['date_formatted'],
                'upsell_value': row['upsell_percentage']
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
        - Only update records where mode = 'S' (System) or mode doesn't exist
        - Skip records where mode = 'M' (Manual) AND value is not empty
        - Allow update if mode = 'M' but value is empty ("")
        - Set mode = 'S' for all system updates
        """
        updated_count = inserted_count = failed_count = 0
        
        try:
            collection = self.db['config_profile_fare_station_upsell']
            
            for record in batch:
                try:
                    sector = record['sector']
                    date_str = record['date']
                    clean_value = record['upsell_value']
                    
                    if len(date_str) > 15 or len(sector) > 6 or len(clean_value) > 45:
                        self.logger.error(f"Field length validation failed for {sector} {date_str}")
                        failed_count += 1
                        continue
                    
                    existing_record = collection.find_one({
                        'sector': sector,
                        'date': date_str
                    })
                    
                    if existing_record and existing_record.get('mode') == 'M':
                        existing_value = existing_record.get('value', '').strip()
                        if existing_value != '':
                            continue
                    
                    result = collection.update_one(
                        {
                            'sector': sector,
                            'date': date_str,
                            '$or': [
                                {'mode': 'S'},
                                {'mode': {'$exists': False}},
                                {'mode': 'M', 'value': ''}
                            ]
                        },
                        {
                            '$set': {
                                'value': clean_value,
                                'mode': 'S'
                            }
                        },
                        upsert=True
                    )
                    
                    if result.matched_count > 0:
                        if result.modified_count > 0:
                            updated_count += 1
                    else:
                        inserted_count += 1
                        
                except Exception as e:
                    failed_count += 1
                    self.logger.error(f"Failed to process record {sector} {date_str}: {str(e)}")
                    
        except Exception as e:
            self.logger.error(f"Batch processing error: {str(e)}")
            
        return updated_count, inserted_count, failed_count

    def validate_upsell_updates(self, projection_df: pd.DataFrame) -> Dict:
        """
        Validate that upsell updates were correctly written to the database.
        
        Checks:
        - Sample of records matches expected values
        - Manual records were not overwritten
        - System records were updated correctly
        """
        validation_results = {
            "status": "not_attempted",
            "sample_size": 0,
            "matches_found": 0,
            "mismatches_found": 0,
            "records_not_found": 0,
            "manual_records_skipped": 0
        }
        
        try:
            # Prepare upsell data (same as update method)
            upsell_data = self._prepare_upsell_data(projection_df)
            
            if upsell_data.empty:
                validation_results["status"] = "no_data"
                return validation_results
            
            # Sample first N records
            sample_data = upsell_data.head(FareAnalysisConfig.VALIDATION_SAMPLE_SIZE)
            validation_results["sample_size"] = len(sample_data)
            
            collection = self.db['config_profile_fare_station_upsell']
            matches = mismatches = not_found = manual_skipped = 0
            
            for _, row in sample_data.iterrows():
                sector = row['Sector']
                date_formatted = row['date_formatted']
                expected_value = row['upsell_percentage']
                
                result = collection.find_one({
                    'sector': sector,
                    'date': date_formatted
                })
                
                if not result:
                    not_found += 1
                    self.logger.warning(f"Record not found: {sector} {date_formatted}")
                else:
                    # Check if it's a manual record
                    if result.get('mode') == 'M':
                        manual_skipped += 1
                    else:
                        # Validate system record
                        db_value = result.get('value', '').strip()
                        if db_value == expected_value:
                            matches += 1
                        else:
                            mismatches += 1
                            self.logger.error(
                                f"Mismatch for {sector} {date_formatted}: "
                                f"Expected {expected_value}, Found {db_value}"
                            )
            
            validation_results.update({
                "matches_found": matches,
                "mismatches_found": mismatches,
                "records_not_found": not_found,
                "manual_records_skipped": manual_skipped,
                "status": "success"
            })
            
        except Exception as e:
            validation_results["status"] = "error"
            validation_results["error"] = str(e)
            self.logger.error(f"Error validating upsell updates: {str(e)}")
        
        return validation_results

    # ============================================================================
    # S3 UPLOAD
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
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=csv_buffer.getvalue()
            )
            
        except Exception as e:
            self.logger.error(f"S3 upload error: {e}")
            raise

    # ============================================================================
    # MAIN EXECUTION
    # ============================================================================
    
    def run(self) -> Dict:
        """Main execution method."""
        start_time = time.time()
        results = {
            "status": "not_attempted",
            "execution_time_seconds": 0,
            "total_records": 0,
            "sectors_processed": 0,
            "projection_start_date": str(self.start_date),
            "projection_end_date": str(self.end_date),
            "upsell_table_update": None,
            "validation_results": None
        }
        
        try:
            # Fetch domestic sectors first
            self.domestic_sectors_cache = self._fetch_domestic_sectors()
            
            calendar_data = self._fetch_calendar_data()
            if calendar_data.empty:
                results["status"] = "no_data"
                return results
            
            sectors = calendar_data['Sector'].unique().tolist()
            
            projection_df = self.run_sequential_processing(sectors)
            
            if projection_df.empty:
                results["status"] = "calculation_failed"
                return results
            
            self.upload_to_s3(projection_df)
            
            upsell_update_results = self.update_upsell_table(projection_df)
            results["upsell_table_update"] = upsell_update_results
            
            if upsell_update_results["status"] == "success":
                validation_results = self.validate_upsell_updates(projection_df)
                results["validation_results"] = validation_results
            
            results.update({
                "total_records": len(projection_df),
                "sectors_processed": projection_df['Sector'].nunique(),
                "status": "success"
            })
            
        except Exception as e:
            results["status"] = "error"
            results["error"] = str(e)
            self.logger.error(f"Error in fare analysis: {str(e)}")
        
        finally:
            results["execution_time_seconds"] = round(time.time() - start_time, 2)
        
        return results