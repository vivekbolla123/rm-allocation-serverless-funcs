import io
import boto3
import pandas as pd
import logging
import os
from sqlalchemy import create_engine, text
import configsettings
from constants_utils import ConstantsUtils
from datetime import datetime, timedelta, date
import traceback
import uuid
import pytz
import numpy as np

from helpers.nav_db_helpers.ladder_nav_helper import LadderNavHelper
from helpers.pa_db_helpers.ladder_pa_helper import LadderPAHelper
from helpers.esb_db_helpers.ladder_esb_helper import LadderEsbHelper
from helpers.dw_db_helpers.ladder_dw_helper import LadderDWHelper
from helpers.pa_db_helpers.market_list_handler import MarketListHandler, COLLECTION_MAP


class LadderGen:

    def __init__(self, logger, params, overrides, context):
        """Initialize with required connections and constants."""
        self.logger = logger
        self.db = params['mongo']
        self.db_fares = params['mongofares']
        self.nav_conn = params['navconn']
        self.rm_rd_esb_conn = params['rdEsbConn']
        self.dw_conn = params['dwconn']
        self.nav_db_name = configsettings.NAV_DB_NAME
        self.nav_db_dw_name = configsettings.NAV_DB_DW_NAME
        self.nav_db_wb_name = configsettings.NAV_DB_WB_NAME
        self.BUCKET_NAME = configsettings.PA_BUCKET_NAME
        self.FILE_PATH = configsettings.PA_FILE_PATH
        self.constants = ConstantsUtils(self.db)
        self.nav_helper = LadderNavHelper(self.nav_conn, self.nav_db_name, self.nav_db_dw_name, self.nav_db_wb_name, logger)
        self.esb_helper = LadderEsbHelper(self.rm_rd_esb_conn, self.db_fares, logger)
        self.dw_helper = LadderDWHelper(self.dw_conn, logger)
        self.route = overrides['route']
        self.analystName = overrides['analyst']
        self.batch_run_id = overrides.get('batch_run_id', None)
        self.batch_run_id = overrides.get('run_id', overrides.get('batch_run_id', None))
        self.batch_total = overrides.get('batch_total', None)

        # Load per-sector config from the appropriate MongoDB collection
        ml_handler = MarketListHandler(self.db, logger, self.constants)
        all_configs = ml_handler.load_sector_configs(self.route)
        collection_name = COLLECTION_MAP.get(self.route, '?')
        if not all_configs:
            raise ValueError(f"No valid sector configs found in collection '{collection_name}' for route '{self.route}'")

        # If ladder_codes provided in overrides (from SQS batch), filter to only those.
        # Otherwise process all (local/direct invocation).
        requested_codes = overrides.get('ladder_codes', None)
        if requested_codes:
            self.sector_configs = {lc: all_configs[lc] for lc in requested_codes if lc in all_configs}
            skipped = [lc for lc in requested_codes if lc not in all_configs]
            if skipped:
                self.logger.warning(f"Ladder codes not found in {collection_name}: {skipped}")
        else:
            self.sector_configs = all_configs

        if not self.sector_configs:
            raise ValueError(f"No matching ladder codes found in '{collection_name}'")

        # sector_configs is keyed by LadderCode
        self.ladder_codes = list(self.sector_configs.keys())
        self.sectors = [self.sector_configs[lc].sector for lc in self.ladder_codes]
        self.logger.info(f"Processing {len(self.ladder_codes)} entries from '{collection_name}': {self.ladder_codes}")

        # Derive route code used internally ('D' or 'I')
        # CNX is international connections
        self.type = 'connection' if self.route == 'CNX' else 'direct'
        if self.route == 'CNX':
            self.route = 'I'  # connections are international

        self.sector = None  # Will be set per sector during processing
        self.sector_currency = None  # Will be set per sector
        self.conversion_rate = None  # Will be set if currency conversion needed

        # Static configuration parameters for ladder generation
        self.min_fare_threshold_domestic = lambda fare: max(0.02 * fare, 50)
        self.min_fare_threshold_international = lambda fare: 0.02 * fare
        self.ladder_bins_b2c = 58
        self.ladder_bins_b2b = 16
        self.total_ladder_points = 64
        self.B2B_RBD_LIST = self.constants.B2B_RBD_VALUES.split(',')
        self.B2C_RBD_LIST = self.constants.B2C_RBD_VALUES.split(',')
        self.TOTAL_B2C_FARES = len(self.B2C_RBD_LIST)
        self.TOTAL_B2B_FARES = len(self.B2B_RBD_LIST)
        self.TOTAL_FARES = self.TOTAL_B2C_FARES + self.TOTAL_B2B_FARES
        self.BUFFER_FARES_B2C = 6
        self.BUFFER_FARES_B2B = 4
        self.CORE_FARES_B2C = self.TOTAL_B2C_FARES - self.BUFFER_FARES_B2C
        self.CORE_FARES_B2B = self.TOTAL_B2B_FARES - self.BUFFER_FARES_B2B

        # Per-sector parameters — will be set in _apply_sector_config() before each sector run
        self.fare_source_flag = None
        self.date_range_start = None
        self.date_range_end = None
        self.carrier_inclusion = None
        self.carrier_exclusion = None
        self.min_distinct_fares_required = None
        self.COVERAGE_PARAMETER = None
        self.UP_B2C = None
        self.UP_B2B = None
        self.MARKET_FLAG = None
        self.B2B_THRESHOLD = None
        self.MARKET_FARE_MIN_YIELD = None
        self.MAX_MARKETFARES_THRESHOLD_B2B = None
        self.MAX_MARKETFARES_THRESHOLD_B2C = None
        self.MIN_MARKETFARES_THRESHOLD_B2B = None
        self.MIN_MARKETFARES_THRESHOLD_B2C = None
        self.MAX_ESBFARES_THRESHOLD_B2B = None
        self.MAX_ESBFARES_THRESHOLD_B2C = None

        self.B2B_CHANNEL = "B2B"
        self.B2C_CHANNEL = "B2C"
        self.pa_helper = LadderPAHelper(self, self.db, self.db_fares, logger, self.calculate_all_in_fare)

        # Data containers
        self.tax_master_dom_data = None
        self.tax_master_intl_data = None
        self.market_fares_data = None
        self.city_pair_data = None
        self.currentVersion = None
        self.fare_cap_value = None  # Distance-based fare cap for current sector (DOM only)
        # Failure tracking
        self.failure_reason = None
        self.outlier_count = None

        # Farecap distance config from rm_parameter_values
        self.FARECAP_DISTANCE_CONFIG = self.constants.FARECAP_DISTANCE_CONFIG

    def _apply_sector_config(self, ladder_code):
        """Apply per-entry configuration from the loaded MarketListConfig."""
        cfg = self.sector_configs[ladder_code]

        self.fare_source_flag = cfg.fare_source_flag
        self.date_range_start = cfg.date_range_start
        self.date_range_end = cfg.date_range_end
        self.carrier_inclusion = cfg.carrier_inclusion if isinstance(cfg.carrier_inclusion, list) else []
        self.carrier_exclusion = cfg.carrier_exclusion if isinstance(cfg.carrier_exclusion, list) else []
        self.min_distinct_fares_required = cfg.b2c_distinct_fare_count
        self.COVERAGE_PARAMETER = cfg.coverage_parameter
        self.UP_B2C = cfg.up_b2c
        self.UP_B2B = cfg.up_b2b
        self.MARKET_FLAG = cfg.market_flag
        self.B2B_THRESHOLD = cfg.b2b_threshold
        self.MARKET_FARE_MIN_YIELD = cfg.yield_threshold

        self.MIN_MARKETFARES_THRESHOLD_B2C = cfg.min_threshold_b2c
        self.MAX_MARKETFARES_THRESHOLD_B2C = cfg.max_threshold_b2c
        self.MIN_MARKETFARES_THRESHOLD_B2B = cfg.min_threshold_b2b
        self.MAX_MARKETFARES_THRESHOLD_B2B = cfg.max_threshold_b2b
        self.MAX_ESBFARES_THRESHOLD_B2C = cfg.max_threshold_b2c
        self.MAX_ESBFARES_THRESHOLD_B2B = cfg.max_threshold_b2b

        self.logger.info(f"Applied config for {ladder_code} (sector={cfg.sector}): "
                         f"source={self.fare_source_flag}, "
                         f"dates={self.date_range_start} to {self.date_range_end}, "
                         f"thresholds B2C={self.MIN_MARKETFARES_THRESHOLD_B2C}-{self.MAX_MARKETFARES_THRESHOLD_B2C}, "
                         f"B2B={self.MIN_MARKETFARES_THRESHOLD_B2B}-{self.MAX_MARKETFARES_THRESHOLD_B2B}")
        
        
    def _get_reverse_sector(self, sector):
        """Get the reverse sector (e.g., BOMDEL -> DELBOM)."""
        if len(sector) != 6:
            return None
        return sector[3:] + sector[:3]
    
    def _get_fare_cap_for_distance(self, distance_km):
        """Get the fare cap value based on sector distance using FARECAP_DISTANCE_CONFIG.
        
        Config format: [(0, 500, 7500), (500, 1000, 12000), (1000, 1500, 15000), (1500, inf, 20000)]
        Returns the cap value for the matching distance range, or None if no match.
        """
        try:
            for min_km, max_km, cap_value in self.FARECAP_DISTANCE_CONFIG:
                if min_km <= distance_km < max_km:
                    # self.logger.info(f"Distance {distance_km} km falls in range {min_km}-{max_km} km, fare cap = {cap_value}")
                    return cap_value
            
            # If distance exceeds all ranges, use the last (highest) range
            if self.FARECAP_DISTANCE_CONFIG:
                last_cap = self.FARECAP_DISTANCE_CONFIG[-1][2]
                self.logger.warning(f"Distance {distance_km} km exceeds all configured ranges, using max cap = {last_cap}")
                return last_cap
            
            self.logger.warning(f"No fare cap config available for distance {distance_km} km")
            return None
        except Exception as e:
            self.logger.error(f"Error looking up fare cap for distance {distance_km}: {str(e)}")
            return None
    
    def _setup_currency_conversion(self):
        """Setup currency conversion for the current sector and reverse sector."""
        try:
            # Get currency for the forward sector
            self.sector_currency = self.pa_helper.get_sector_currency(self.sector, self.route)
            
            # Check if conversion is needed for forward sector
            if self.sector_currency != 'INR':
                self.logger.info(f"Sector {self.sector} requires currency conversion: INR -> {self.sector_currency}")
                self.conversion_rate = self.dw_helper.get_conversion_rate('INR', self.sector_currency)
                if self.conversion_rate is None:
                    self.logger.warning(f"Could not get conversion rate for INR -> {self.sector_currency}, will not convert fares")
                    self.conversion_rate = 1.0
                else:
                    self.logger.info(f"Forward conversion rate (INR -> {self.sector_currency}): {self.conversion_rate}")
            else:
                self.logger.info(f"Sector {self.sector} uses INR, no currency conversion needed")
                self.conversion_rate = 1.0

            # Get currency for the reverse direction (e.g., HKTBOM for BOMHKT)
            # In qp_fares, Origin/Dest stay the same — only Fare_Direction='T' for reverse
            reverse_sector = self._get_reverse_sector(self.sector)
            if reverse_sector:
                self.reverse_sector_currency = self.pa_helper.get_sector_native_currency(self.sector, self.route, fare_direction='T')
                
                if self.reverse_sector_currency != 'INR':
                    self.logger.info(f"Reverse sector {reverse_sector} currency: {self.reverse_sector_currency}")
                    self.reverse_conversion_rate = self.dw_helper.get_conversion_rate('INR', self.reverse_sector_currency)
                    if self.reverse_conversion_rate is None:
                        self.logger.warning(f"Could not get conversion rate for INR -> {self.reverse_sector_currency}")
                        self.reverse_conversion_rate = 1.0
                    else:
                        self.logger.info(f"Reverse conversion rate (INR -> {self.reverse_sector_currency}): {self.reverse_conversion_rate}")
                else:
                    self.logger.info(f"Reverse sector {reverse_sector} uses INR, no conversion needed")
                    self.reverse_conversion_rate = 1.0
            else:
                self.reverse_conversion_rate = 1.0
                
        except Exception as e:
            self.logger.error(f"Error setting up currency conversion: {str(e)}")
            self.conversion_rate = 1.0
            self.reverse_conversion_rate = 1.0
    
    def _convert_fares_to_local_currency(self, fare_points):
        """Convert fare points from INR to local currency if needed."""
        try:
            if self.conversion_rate == 1.0:
                self.logger.info("No currency conversion needed (rate = 1.0)")
                return fare_points
            
            converted_fares = [fare * self.conversion_rate for fare in fare_points]
            
            self.logger.info(f"Currency Conversion Applied: INR -> {self.sector_currency} at rate {self.conversion_rate}")
            
            return converted_fares
            
        except Exception as e:
            self.logger.error(f"Error converting fares to local currency: {str(e)}")
            return fare_points  # Return original fares on error
    
    def _convert_fare_to_inr(self, fare):
        """Convert a single fare from local currency to INR for yield calculation."""
        try:
            if self.conversion_rate == 1.0:
                return fare  # Already in INR
            
            # Divide by conversion rate to get back to INR
            inr_fare = fare / self.conversion_rate
            return inr_fare
            
        except Exception as e:
            self.logger.error(f"Error converting fare to INR: {str(e)}")
            return fare  # Return original fare on error
    
    def collect_all_data(self):
        """Collect all required data efficiently."""
        try:
            success_count = 0
            try:
                self.tax_master_dom_data, self.tax_master_intl_data, failure_reason = self.pa_helper.collect_tax_master_data()
                if failure_reason:
                    self.failure_reason = failure_reason
                else:
                    success_count += 1
            except Exception as e:
                self.logger.error(f"Tax master data collection failed: {str(e)}")
                if not self.failure_reason:
                    self.failure_reason = f"Tax master data collection failed: {str(e)}"
            
            try:
                # Collect market fares for both directions
                reverse_sector = self._get_reverse_sector(self.sector)
                self.market_fares_data, failure_reason = self.pa_helper.collect_market_fares_bidirectional(
                    self.route, self.sector, reverse_sector, self.MAX_MARKETFARES_THRESHOLD_B2B, self.MAX_MARKETFARES_THRESHOLD_B2C, layer_type=self.type, carrier_inclusion=self.carrier_inclusion, carrier_exclusion=self.carrier_exclusion
                )
                if failure_reason:
                    self.failure_reason = failure_reason
                else:
                    # Normalize reverse-sector fares to INR
                    if self.market_fares_data is not None and not self.market_fares_data.empty and self.reverse_conversion_rate != 1.0:
                        reverse_origin = reverse_sector[:3]
                        reverse_dest = reverse_sector[3:]
                        reverse_mask = (
                            (self.market_fares_data['source_airport'] == reverse_origin) &
                            (self.market_fares_data['destination_airport'] == reverse_dest)
                        )
                        reverse_count = reverse_mask.sum()
                        if reverse_count > 0:
                            self.market_fares_data.loc[reverse_mask, 'gross_fare'] = (
                                self.market_fares_data.loc[reverse_mask, 'gross_fare'] / self.reverse_conversion_rate
                            )
                            self.logger.info(f"Converted {reverse_count} reverse-sector market fares to INR (÷ {self.reverse_conversion_rate})")
                    
                    if self.market_fares_data is not None and not self.market_fares_data.empty:
                        initial_count = len(self.market_fares_data)
                        b2c_mask = (self.market_fares_data['channel'] == 'B2C') & (self.market_fares_data['gross_fare'] >= self.MIN_MARKETFARES_THRESHOLD_B2C) & (self.market_fares_data['gross_fare'] <= self.MAX_MARKETFARES_THRESHOLD_B2C)
                        b2b_mask = (self.market_fares_data['channel'] == 'B2B') & (self.market_fares_data['gross_fare'] >= self.MIN_MARKETFARES_THRESHOLD_B2B) & (self.market_fares_data['gross_fare'] <= self.MAX_MARKETFARES_THRESHOLD_B2B)
                        other_mask = ~self.market_fares_data['channel'].isin(['B2C', 'B2B'])
                        
                        self.market_fares_data = self.market_fares_data[b2c_mask | b2b_mask | other_mask].copy()
                        
                        filtered_count = initial_count - len(self.market_fares_data)
                        if filtered_count > 0:
                            self.logger.info(f"Filtered {filtered_count} market fares exceeding thresholds after conversion (B2C: {self.MIN_MARKETFARES_THRESHOLD_B2C}-{self.MAX_MARKETFARES_THRESHOLD_B2C}, B2B: {self.MIN_MARKETFARES_THRESHOLD_B2B}-{self.MAX_MARKETFARES_THRESHOLD_B2B})")
                    
                    success_count += 1
            except Exception as e:
                self.logger.error(f"Market fares collection failed: {str(e)}")
                if not self.failure_reason:
                    self.failure_reason = f"Market fares collection failed: {str(e)}"
            
            # Non-critical data collections
            try:
                self.city_pair_data = self.nav_helper.collect_city_pair_data(self.sector)
                success_count += 1
            except Exception as e:
                self.logger.error(f"City pair data collection failed (non-critical): {str(e)}")
            
            # Collect distance-based fare cap (DOM only)
            try:
                if self.route == 'D':
                    origin = self.sector[:3]
                    destination = self.sector[3:]
                    # Get distance from Navitaire CityPair data collected above
                    sector_distance = None
                    if self.city_pair_data is not None and not self.city_pair_data.empty:
                        # Find matching forward sector
                        match = self.city_pair_data[
                            (self.city_pair_data['DepartureStation'] == origin) & 
                            (self.city_pair_data['ArrivalStation'] == destination)
                        ]
                        if not match.empty:
                            sector_distance = float(match.iloc[0]['ActualDistance'])
                        else:
                            # Try reverse sector as fallback
                            reverse_match = self.city_pair_data[
                                (self.city_pair_data['DepartureStation'] == destination) & 
                                (self.city_pair_data['ArrivalStation'] == origin)
                            ]
                            if not reverse_match.empty:
                                sector_distance = float(reverse_match.iloc[0]['ActualDistance'])
                    
                    if sector_distance is not None:
                        gross_fare_cap = self._get_fare_cap_for_distance(sector_distance)
                        # Fare cap includes base fare + RCS + CUTE
                        # For bidirectional consistency, average both origins' RCS/CUTE
                        rcs_fwd = cute_fwd = rcs_rev = cute_rev = 0
                        if self.tax_master_dom_data is not None and not self.tax_master_dom_data.empty:
                            # Forward origin taxes
                            fwd_tax = self.tax_master_dom_data[self.tax_master_dom_data['Code'] == origin]
                            if not fwd_tax.empty:
                                rcs_fwd = float(fwd_tax.iloc[0].get('RCS', 0) or 0)
                                cute_fwd = float(fwd_tax.iloc[0].get('CUTE', 0) or 0)
                            # Reverse origin taxes
                            rev_tax = self.tax_master_dom_data[self.tax_master_dom_data['Code'] == destination]
                            if not rev_tax.empty:
                                rcs_rev = float(rev_tax.iloc[0].get('RCS', 0) or 0)
                                cute_rev = float(rev_tax.iloc[0].get('CUTE', 0) or 0)
                            self.logger.info(f"Tax deductions - {origin}: RCS={rcs_fwd}, CUTE={cute_fwd}; {destination}: RCS={rcs_rev}, CUTE={cute_rev}")
                        
                        # Average both origins for bidirectional consistency
                        avg_rcs = (rcs_fwd + rcs_rev) / 2
                        avg_cute = (cute_fwd + cute_rev) / 2
                        self.fare_cap_value = gross_fare_cap - avg_rcs - avg_cute
                        self.logger.info(f"Distance-based fare cap for {self.sector}: "
                                       f"distance={sector_distance} km, gross_cap={gross_fare_cap}, "
                                       f"avg_RCS={avg_rcs}, avg_CUTE={avg_cute}, "
                                       f"effective_base_fare_cap={self.fare_cap_value}")
                    else:
                        self.fare_cap_value = None
                        self.logger.warning(f"No distance found for sector {self.sector}, fare cap not applied")
                else:
                    # No fare cap for international routes
                    self.fare_cap_value = None
                    self.logger.info(f"Fare cap not applied for international route {self.sector}")
            except Exception as e:
                self.logger.error(f"Fare cap distance lookup failed (non-critical): {str(e)}")
                self.fare_cap_value = None
            
            # Check if critical data is missing
            critical_failures = []
            if self.market_fares_data is None or self.market_fares_data.empty:
                critical_failures.append("Market fares data")
            if self.tax_master_dom_data is None or self.tax_master_intl_data is None:
                if (self.tax_master_dom_data is None or self.tax_master_dom_data.empty) and \
                (self.tax_master_intl_data is None or self.tax_master_intl_data.empty):
                    critical_failures.append("Tax master data")
            
            if critical_failures and not self.failure_reason:
                self.failure_reason = f"Critical data collection failed: {', '.join(critical_failures)}"
                return False
            
            return success_count >= 2
            
        except Exception as e:
            self.logger.error(f"Error in data collection: {str(e)}")
            if not self.failure_reason:
                self.failure_reason = f"Data collection process failed: {str(e)}"
            raise

    def remove_outliers(self, fares, period_label=""):
        """Remove lower outliers using Modified Z-Score method."""
        data = pd.DataFrame(fares)
        
        if len(data) <= 1:
            return data
        
        fare_l1 = data["Fare"].values
        
        # Step 1: Calculate Median
        MED = np.median(fare_l1)
        
        # Step 2: Calculate MAD
        MAD = np.median(np.abs(fare_l1 - MED))
        if MAD == 0:
            MAD = 0.15 * MED
        
        # Step 3: Calculate MZS for all fares
        coverage_value = self.COVERAGE_PARAMETER
        MZS = np.abs(fare_l1 - MED) / MAD
        
        # Add MZS column to original data for analysis
        data_with_mzs = data.copy()
        data_with_mzs['MZS'] = MZS
        data_with_mzs['IsOutlier'] = (fare_l1 < MED) & (MZS >= coverage_value)
        data_with_mzs['Median'] = MED
        data_with_mzs['MAD'] = MAD
        data_with_mzs['CoverageThreshold'] = coverage_value
        
        try:
            current_date = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{self.sector}_{current_date}_{period_label}.csv"
            s3_key = f"{self.FILE_PATH}outliers/{filename}"
            csv_buffer = io.StringIO()
            data_with_mzs.sort_values('Fare').to_csv(csv_buffer, index=False)
            
            s3_client = boto3.client('s3')
            s3_client.put_object(
                Bucket=self.BUCKET_NAME,
                Key=s3_key,
                Body=csv_buffer.getvalue(),
                ContentType='text/csv'
            )
            
        except Exception as e:
            self.logger.warning(f"Failed to export to S3: {str(e)}")
        
        mask = (fare_l1 >= MED) | (MZS < coverage_value)
        filtered_data = data[mask].copy()
        
        removed_count = len(data) - len(filtered_data)
        
        self.outlier_count = removed_count if removed_count > 0 else 0
        
        return filtered_data

    def get_historical_booking_data(self, channel):
        """Get booking data for ladder generation.
        
        For Bookings (ESB) source:
          - If date_range_start/end are provided, use them directly.
          - If empty, fall back to NDO priority cascade (B2C: 30→60→365, B2B: 60→365).
        
        For Market source:
          - Always uses default 0-365 forward dates (NDO-based).
          - Date range columns are ignored.
        """
        try:
            reverse_sector = self._get_reverse_sector(self.sector)

            if self.fare_source_flag.lower() == 'esb' and self.date_range_start and self.date_range_end:
                # Bookings with explicit date range from input file
                period_label = f"{self.date_range_start}_to_{self.date_range_end}"
                combined = self._fetch_booking_data_esb(channel, reverse_sector,
                                                        str(self.date_range_start),
                                                        str(self.date_range_end))
                combined = self.remove_outliers(combined, period_label=period_label)
                distinct_fares = len(combined['Fare'].unique()) if not combined.empty else 0

                if channel == self.B2C_CHANNEL and distinct_fares < self.min_distinct_fares_required:
                    self.failure_reason = (
                        f"Insufficient distinct fares for {channel}. "
                        f"Found: {distinct_fares}, Required: {self.min_distinct_fares_required}, "
                        f"Date range: {self.date_range_start} to {self.date_range_end}"
                    )
                    self.logger.warning(self.failure_reason)
                    return pd.DataFrame(), "Insufficient distinct fares"

                return combined, period_label
            else:
                # Market fares OR Bookings without date range — use NDO cascade
                return self._get_historical_data_by_ndo(channel, reverse_sector)

        except Exception as e:
            self.logger.error(f"Error in data collection: {str(e)}")
            self.failure_reason = f"Data collection failed: {str(e)}"
            raise

    def _get_historical_data_by_ndo(self, channel, reverse_sector):
        """Fallback: fetch data using NDO priority cascade.
        
        For Market source: uses 0-365 forward departure dates (default).
        For Bookings (ESB) without date range: tries NDO 30→60→365 (B2C) or 60→365 (B2B).
        """
        if channel == self.B2C_CHANNEL:
            # Priority 1: NDO 30
            combined_30 = self._fetch_data_by_ndo(30, channel, reverse_sector)
            combined_30 = self.remove_outliers(combined_30, period_label="ndo30")
            distinct_30 = len(combined_30['Fare'].unique()) if not combined_30.empty else 0
            if distinct_30 >= self.min_distinct_fares_required:
                return combined_30, "NDO 30"

            # Priority 2: NDO 60
            combined_60 = self._fetch_data_by_ndo(60, channel, reverse_sector)
            combined_60 = self.remove_outliers(combined_60, period_label="ndo60")
            distinct_60 = len(combined_60['Fare'].unique()) if not combined_60.empty else 0
            if distinct_60 >= self.min_distinct_fares_required:
                return combined_60, "NDO 60"

            # Priority 3: NDO 365
            combined_365 = self._fetch_data_by_ndo(365, channel, reverse_sector)
            combined_365 = self.remove_outliers(combined_365, period_label="ndo365")
            distinct_365 = len(combined_365['Fare'].unique()) if not combined_365.empty else 0
            if distinct_365 >= self.min_distinct_fares_required:
                return combined_365, "NDO 365"

            self.failure_reason = (
                f"Insufficient distinct fares. Found: NDO-30: {distinct_30}, "
                f"NDO-60: {distinct_60}, NDO-365: {distinct_365}. "
                f"Required: {self.min_distinct_fares_required} for Channel:{channel}"
            )
            self.logger.warning(self.failure_reason)
            return pd.DataFrame(), "Insufficient distinct fares"
        else:
            # B2B: Priority 1: NDO 60
            combined_60 = self._fetch_data_by_ndo(60, channel, reverse_sector)
            combined_60 = self.remove_outliers(combined_60, period_label="ndo60")
            if not combined_60.empty:
                return combined_60, "NDO 60"

            # B2B: Priority 2: NDO 365
            combined_365 = self._fetch_data_by_ndo(365, channel, reverse_sector)
            combined_365 = self.remove_outliers(combined_365, period_label="ndo365")
            return combined_365, "NDO 365"

    def _fetch_booking_data_esb(self, channel, reverse_sector, start_str, end_str):
        """Fetch ESB booking data for an explicit date range."""
        max_fare = self.MAX_ESBFARES_THRESHOLD_B2C if channel == self.B2C_CHANNEL else self.MAX_ESBFARES_THRESHOLD_B2B

        combined_df = self.esb_helper.collect_combined_booking_data_bidirectional(
            start_str, end_str, self.sector, reverse_sector, channel
        )

        if not combined_df.empty:
            if self.reverse_conversion_rate != 1.0:
                reverse_mask = combined_df['Origin'] + combined_df['Destination'] == reverse_sector
                if reverse_mask.any():
                    combined_df.loc[reverse_mask, 'Fare'] = combined_df.loc[reverse_mask, 'Fare'] / self.reverse_conversion_rate
                    self.logger.info(f"Converted {reverse_mask.sum()} reverse fares to INR (÷ {self.reverse_conversion_rate})")

            before_count = len(combined_df)
            combined_df = combined_df[combined_df['Fare'] <= max_fare]
            self.logger.info(f"ESB fare threshold {max_fare} INR: {before_count} -> {len(combined_df)} records")

            combined_df = combined_df.dropna(subset=['Fare'])
            combined_df = combined_df.sort_values('Fare', ascending=True).reset_index(drop=True)

        return combined_df

    def _fetch_data_by_ndo(self, ndo, channel, reverse_sector):
        """Fetch data using NDO. ESB looks backward, Market looks forward (0-365 default)."""
        if self.fare_source_flag.lower() == 'esb':
            end_date = datetime.now()
            start_date = end_date - timedelta(days=ndo)
            return self._fetch_booking_data_esb(channel, reverse_sector,
                                                 start_date.strftime('%Y-%m-%d'),
                                                 end_date.strftime('%Y-%m-%d'))
        else:
            # Market fares — always forward looking
            max_fare = self.MAX_MARKETFARES_THRESHOLD_B2C if channel == self.B2C_CHANNEL else self.MAX_MARKETFARES_THRESHOLD_B2B

            combined_df = self.esb_helper.collect_combined_market_fare_data_bidirectional(
                ndo, self.sector, reverse_sector, channel,
                max_fare_threshold=max_fare,
                conversion_rate=self.reverse_conversion_rate,
                layer_type=self.type,
                carrier_inclusion=self.carrier_inclusion
            )

            if not combined_df.empty:
                combined_df['Fare'] = combined_df.apply(
                    lambda row: self._calculate_base_xqp_fare(
                        row['Fare'], row['source_airport'], row['destination_airport']
                    ), axis=1
                )
                combined_df = combined_df.dropna(subset=['Fare'])
                combined_df = combined_df.sort_values('Fare', ascending=True).reset_index(drop=True)
                self.logger.info(f"Converted {len(combined_df)} market fares from gross to base fare")

            return combined_df

    def _apply_fare_cap_to_bookings(self, booking_data, channel):
        """Filter out booking fares that exceed the fare cap.
        
        Uses the distance-based fare cap value directly as the base fare cap.
        Only applies for domestic routes (fare_cap_value is None for international).
        """
        if booking_data.empty or self.fare_cap_value is None:
            return booking_data
        
        base_fare_cap = self.fare_cap_value
        
        original_count = len(booking_data)
        filtered_data = booking_data[booking_data['Fare'] <= base_fare_cap].copy()
        removed_count = original_count - len(filtered_data)
        
        if removed_count > 0:
            self.logger.info(f"Fare cap filter ({channel}): removed {removed_count} of {original_count} "
                           f"bookings with base fare > {base_fare_cap:.2f}")
        
        return filtered_data

    def create_fare_bins(self, booking_data, channel):
        """Create fare bins and apply currency conversion if needed."""
        try:
            if(channel == self.B2C_CHANNEL):
                ladderBins = self.CORE_FARES_B2C
            else:
                ladderBins = self.CORE_FARES_B2B
            if booking_data.empty:
                self.failure_reason = "Empty booking data provided for fare bin creation"
                return []
            
            sorted_fares = booking_data['Fare'].sort_values().values
            if len(sorted_fares) < ladderBins:
                self.failure_reason = f"Insufficient data points for binning. Found: {len(sorted_fares)}, Required: {ladderBins}"
                return []
            
            bin_size = len(sorted_fares) // ladderBins
            remainder = len(sorted_fares) % ladderBins
            
            fare_points = []
            start_idx = 0
            
            for i in range(ladderBins):
                current_bin_size = bin_size + (1 if i < remainder else 0)
                end_idx = start_idx + current_bin_size
                
                bin_fares = sorted_fares[start_idx:end_idx]
                if len(bin_fares) > 0:
                    fare_points.append(np.mean(bin_fares))
                
                start_idx = end_idx
            
            if len(fare_points) != ladderBins:
                self.failure_reason = f"Fare bin creation failed. Expected {ladderBins} bins, got {len(fare_points)}"
                return []
            
            # Apply currency conversion after fare bins are created
            self.logger.info(f"Created {len(fare_points)} fare bins for {channel} channel")
            # converted_fare_points = self._convert_fares_to_local_currency(fare_points)
            
            return fare_points
            
        except Exception as e:
            self.logger.error(f"Error creating fare bins: {str(e)}")
            self.failure_reason = f"Fare bin creation failed: {str(e)}"
            raise

    def apply_minimum_threshold(self, fare_points, channel):
        """Apply minimum threshold logic to core fares, stopping before UP range to prevent inflation."""
        try:
            if not fare_points:
                self.failure_reason = "No fare points provided for threshold application"
                return fare_points
            
            threshold_func = (self.min_fare_threshold_domestic if self.route == 'D' 
                            else self.min_fare_threshold_international)
            
            adjusted_fares = fare_points.copy()
            
            # Determine how many fares to adjust based on channel
            # Stop before UP range to prevent artificial inflation of start fare
            if channel == self.B2C_CHANNEL:
                up_value = self.UP_B2C
            else:  # B2B
                up_value = self.UP_B2B
            
            # Calculate stopping point: total_fares - UP - 1
            # This ensures the start fare for UP calculation is not artificially inflated
            num_to_adjust = len(adjusted_fares) - up_value - 1
            
            # Ensure we don't go out of bounds
            num_to_adjust = max(0, min(num_to_adjust, len(adjusted_fares) - 1))
            
            self.logger.info(f"Applying minimum threshold to {channel} core fares:")
            self.logger.info(f"  Total core fares: {len(adjusted_fares)}")
            self.logger.info(f"  UP value: {up_value}")
            self.logger.info(f"  Will adjust first {num_to_adjust} fare pairs (stop before UP range)")
            
            adjustments_made = 0
            
            for i in range(num_to_adjust):
                current_fare = adjusted_fares[i]
                next_fare = adjusted_fares[i + 1]
                min_threshold = threshold_func(current_fare)
                
                if next_fare - current_fare < min_threshold:
                    adjusted_fares[i + 1] = current_fare + min_threshold
                    adjustments_made += 1
            
            self.logger.info(f"  Core threshold adjustments made: {adjustments_made}")
            
            return adjusted_fares
            
        except Exception as e:
            self.logger.error(f"Error applying minimum threshold: {str(e)}")
            self.failure_reason = f"Minimum threshold application failed: {str(e)}"
            raise

    # Helper Methods
    def _get_distance(self, origin, destination):
        """Get distance for sector."""
        try:
            if self.city_pair_data is None or self.city_pair_data.empty:
                return 0
            distance_data = self.city_pair_data[
                (self.city_pair_data['DepartureStation'] == origin) &
                (self.city_pair_data['ArrivalStation'] == destination)
            ]
            return float(distance_data.iloc[0]['ActualDistance']) if not distance_data.empty else 0
        except Exception:
            return 0

    def _get_yq_for_distance(self, distance):
        """Get YQ based on distance (km) for domestic routes from config."""
        if distance <= 0:
            return 0
            
        try:
            for min_dist, max_dist, yq_val in self.constants.YQ_DISTANCE_CONFIG:
                if min_dist < distance <= max_dist:
                    return float(yq_val)
        except AttributeError:
            self.logger.warning("YQ_DISTANCE_CONFIG not found in constants.")
            
        return 0

    def _calculate_taxes_domestic(self, base_fare, origin, destination):
        """Calculate all taxes for domestic fare to get all-in fare."""
        try:
            if self.tax_master_dom_data is None or self.tax_master_dom_data.empty:
                return 0
                
            tax_data = self.tax_master_dom_data[self.tax_master_dom_data['Code'] == origin]
            if tax_data.empty:
                return 0
            
            tax_row = tax_data.iloc[0]
            gst_rate = float(tax_row.get('GST', 0)) / 100
            rcs = float(tax_row.get('RCS', 0)) if pd.notna(tax_row.get('RCS')) else 0
            cute = float(tax_row.get('CUTE', 0)) if pd.notna(tax_row.get('CUTE')) else 0
            asf = float(tax_row.get('ASF', 0)) if pd.notna(tax_row.get('ASF')) else 0
            psf = float(tax_row.get('PSF', 0)) if pd.notna(tax_row.get('PSF')) else 0
            udf_df = float(tax_row.get('UDF_DF', 0)) if pd.notna(tax_row.get('UDF_DF')) else 0
            
            # Apply special handling for RCS sectors
            if destination in self.constants.PRICING_RCS_SECTORS:
                rcs = 0
            
            # Add DUF for specific sectors
            duf = 0
            if destination in self.constants.PRICING_DUF_SECTORS:
                duf = float(self.constants.PRICING_DUF_SECTORS[destination])
            
            # Calculate GST
            if origin in self.constants.PRICING_GST_SECTORS or destination in self.constants.PRICING_GST_SECTORS:
                gst = 0
            else:
                gst = gst_rate * (base_fare + rcs + cute)
            
            # Calculate YQ based on distance
            distance = self._get_distance(origin, destination)
            yq = self._get_yq_for_distance(distance)
            
            total_taxes = asf + psf + udf_df + rcs + cute + gst + duf + yq + (yq*gst_rate)
            return total_taxes
            
        except Exception:
            return 0

    def _calculate_taxes_international(self, base_fare, origin, destination):
        """Calculate all taxes for international fare to get all-in fare."""
        try:
            if self.tax_master_intl_data is None or self.tax_master_intl_data.empty:
                return 0
                
            sector_key = f"{origin}{destination}"
            tax_data = self.tax_master_intl_data[self.tax_master_intl_data['Sectors'] == sector_key]
            if tax_data.empty:
                return 0
            
            tax_row = tax_data.iloc[0]
            
            # Extract all tax components
            gst_rate = float(tax_row.get('GST', 0)) / 100
            cute = float(tax_row.get('CUTE', 0)) if pd.notna(tax_row.get('CUTE')) else 0
            asf = float(tax_row.get('ASF', 0)) if pd.notna(tax_row.get('ASF')) else 0
            psf_adf = float(tax_row.get('PSF_ADF', 0)) if pd.notna(tax_row.get('PSF_ADF')) else 0
            udf = float(tax_row.get('UDF', 0)) if pd.notna(tax_row.get('UDF')) else 0
            psc_pz = float(tax_row.get('PSC_PZ', 0)) if pd.notna(tax_row.get('PSC_PZ')) else 0
            rcs_cute = float(tax_row.get('RCS_CUTE', 0)) if pd.notna(tax_row.get('RCS_CUTE')) else 0
            rcs = float(tax_row.get('RCS', 0)) if pd.notna(tax_row.get('RCS')) else 0
            
            # Additional taxes
            tax1 = float(tax_row.get('Tax1', 0)) if pd.notna(tax_row.get('Tax1')) else 0
            tax2 = float(tax_row.get('Tax2', 0)) if pd.notna(tax_row.get('Tax2')) else 0
            tax3 = float(tax_row.get('Tax3', 0)) if pd.notna(tax_row.get('Tax3')) else 0
            tax4 = float(tax_row.get('Tax4', 0)) if pd.notna(tax_row.get('Tax4')) else 0
            tax5 = float(tax_row.get('Tax5', 0)) if pd.notna(tax_row.get('Tax5')) else 0
            
            # YQ and YQ_GST from tax master international
            yq = float(tax_row.get('YQ', 0)) if pd.notna(tax_row.get('YQ')) else 0
            yq_gst_rate = float(tax_row.get('YQ_GST', 0)) / 100 if pd.notna(tax_row.get('YQ_GST')) else 0
            
            # Calculate GST
            gst = gst_rate * (base_fare + rcs + cute + rcs_cute)
            
            total_taxes = asf + psf_adf + udf + psc_pz + rcs_cute + rcs + cute + gst + tax1 + tax2 + tax3 + tax4 + tax5 + yq + (yq * yq_gst_rate)
            return total_taxes
            
        except Exception:
            return 0

    def calculate_all_in_fare(self, base_fare, origin, destination, ftype):
        """Calculate all-in fare based on route type."""
        try:
            if self.route == 'D':
                taxes = self._calculate_taxes_domestic(base_fare, origin, destination)
            else:
                taxes = self._calculate_taxes_international(base_fare, origin, destination)
            
            if ftype == "current":
                return base_fare + taxes
            else:
                return base_fare - taxes
            
        except Exception as e:
            self.logger.warning(f"Error calculating all-in fare: {str(e)}")
            return base_fare
        
    def _calculate_base_xqp_fare(self, gross_fare, origin, destination):
        """
        Calculate base XQP fare using reverse tax calculation based on route.
        Note: For yield calculation, gross_fare should be in INR.
        """
        try:
            if self.route == 'D':
                return self._calculate_base_domestic_fare(gross_fare, origin, destination)
            elif self.route == 'I':
                return self._calculate_base_international_fare(gross_fare, origin, destination)
            else:
                raise ValueError(f"Invalid route: {self.route}")
                
        except Exception as e:
            self.logger.error(f"Error calculating base XQP fare: {str(e)}")
            return gross_fare

    def _calculate_base_domestic_fare(self, gross_fare, origin, destination):
        """Calculate base domestic fare using the correct reverse formula: Base Fare = [Gross Fare - GST×(RCS+CUTE) - RCS - Other Taxes] / (1 + GST)"""
        try:
            # Get tax data
            tax_data = self.tax_master_dom_data[self.tax_master_dom_data['Code'] == origin]
            
            if tax_data.empty:
                self.logger.warning(f"No domestic tax data found for base calculation: {origin}")
                return gross_fare
            
            tax_row = tax_data.iloc[0]
            
            gst_rate = float(tax_row.get('GST', 0)) / 100  # GST rate (e.g., 18% = 0.18)
            rcs = float(tax_row.get('RCS', 0)) if pd.notna(tax_row.get('RCS')) else 0
            cute = float(tax_row.get('CUTE', 0)) if pd.notna(tax_row.get('CUTE')) else 0
            asf = float(tax_row.get('ASF', 0)) if pd.notna(tax_row.get('ASF')) else 0
            psf = float(tax_row.get('PSF', 0)) if pd.notna(tax_row.get('PSF')) else 0
            udf_df = float(tax_row.get('UDF_DF', 0)) if pd.notna(tax_row.get('UDF_DF')) else 0
            
            # Apply same special handling for RCS sectors - set RCS to 0
            if destination in self.constants.PRICING_RCS_SECTORS:
                rcs = 0
            
            # Start with gross fare and subtract DUF if applicable
            adjusted_gross_fare = gross_fare
            
            if destination in self.constants.PRICING_DUF_SECTORS:
                duf_value = float(self.constants.PRICING_DUF_SECTORS[destination])
                adjusted_gross_fare -= duf_value
                
            # Calculate YQ based on distance
            distance = self._get_distance(origin, destination)
            yq = self._get_yq_for_distance(distance)
                
            if origin in self.constants.PRICING_GST_SECTORS or destination in self.constants.PRICING_GST_SECTORS:
                other_taxes = asf + psf + udf_df + rcs + cute + yq + (yq*gst_rate)
                base_fare = adjusted_gross_fare - other_taxes
            else:
                gst_on_taxes = gst_rate * (rcs + cute)
                other_taxes = asf + psf + udf_df + cute + yq + (yq*gst_rate)
                base_fare = (adjusted_gross_fare - gst_on_taxes - rcs - other_taxes) / (1 + gst_rate)
            
            return base_fare
            
        except Exception as e:
            self.logger.error(f"Error calculating base domestic fare: {str(e)}")
            
    def _calculate_base_international_fare(self, gross_fare, origin, destination):
        """Calculate base international fare using the correct reverse formula: Base Fare = [Gross Fare - GST×(RCS+CUTE) - RCS - Other Taxes] / (1 + GST)"""
        try:
            if self.tax_master_intl_data is None or self.tax_master_intl_data.empty:
                return 0
                
            sector_key = f"{origin}{destination}"
            tax_data = self.tax_master_intl_data[self.tax_master_intl_data['Sectors'] == sector_key]
            if tax_data.empty:
                return 0
            
            tax_row = tax_data.iloc[0]
            
            # Extract all tax components
            gst_rate = float(tax_row.get('GST', 0)) / 100
            cute = float(tax_row.get('CUTE', 0)) if pd.notna(tax_row.get('CUTE')) else 0
            asf = float(tax_row.get('ASF', 0)) if pd.notna(tax_row.get('ASF')) else 0
            psf_adf = float(tax_row.get('PSF_ADF', 0)) if pd.notna(tax_row.get('PSF_ADF')) else 0
            udf = float(tax_row.get('UDF', 0)) if pd.notna(tax_row.get('UDF')) else 0
            psc_pz = float(tax_row.get('PSC_PZ', 0)) if pd.notna(tax_row.get('PSC_PZ')) else 0
            rcs_cute = float(tax_row.get('RCS_CUTE', 0)) if pd.notna(tax_row.get('RCS_CUTE')) else 0
            rcs = float(tax_row.get('RCS', 0)) if pd.notna(tax_row.get('RCS')) else 0
            
            # Additional taxes
            tax1 = float(tax_row.get('Tax1', 0)) if pd.notna(tax_row.get('Tax1')) else 0
            tax2 = float(tax_row.get('Tax2', 0)) if pd.notna(tax_row.get('Tax2')) else 0
            tax3 = float(tax_row.get('Tax3', 0)) if pd.notna(tax_row.get('Tax3')) else 0
            tax4 = float(tax_row.get('Tax4', 0)) if pd.notna(tax_row.get('Tax4')) else 0
            tax5 = float(tax_row.get('Tax5', 0)) if pd.notna(tax_row.get('Tax5')) else 0
            
            # YQ and YQ_GST from tax master international
            yq = float(tax_row.get('YQ', 0)) if pd.notna(tax_row.get('YQ')) else 0
            yq_gst_rate = float(tax_row.get('YQ_GST', 0)) / 100 if pd.notna(tax_row.get('YQ_GST')) else 0
            
            # Apply same special handling for RCS sectors - set RCS to 0
            if destination in self.constants.PRICING_RCS_SECTORS:
                rcs = 0
            
            # Start with gross fare and subtract DUF if applicable
            adjusted_gross_fare = gross_fare
            
            gst_on_taxes = gst_rate * (rcs + cute + rcs_cute)
            other_taxes  = asf + psf_adf + udf + psc_pz + tax1 + tax2 + tax3 + tax4 + tax5 + yq + (yq * yq_gst_rate)
            base_fare    = (
                adjusted_gross_fare - rcs - cute - rcs_cute - gst_on_taxes - other_taxes
            ) / (1 + gst_rate)

            return base_fare
            
        except Exception as e:
            self.logger.error(f"Error calculating base international fare: {str(e)}")

    def _find_valid_market_fare(self, market_data, fare_type, origin, destination):
        """Find valid market fare that satisfies yield constraint.
        
        Market fares are stored as gross_fare (all-in). This method:
        1. Filters fares by fare cap (gross_fare <= fare_cap)
        2. Performs yield check using reverse-calculated base fare
        3. Returns the BASE FARE (reverse-calculated) instead of gross_fare
        """
        try:
            if market_data.empty:
                return None
            
            # Apply fare cap filter to market fares (convert gross to base fare per-row)
            filtered_data = market_data.copy()
            if self.fare_cap_value is not None:
                before_count = len(filtered_data)
                # Convert gross fares to base fares using each row's own source/destination
                filtered_data['base_fare_calc'] = filtered_data.apply(
                    lambda row: self._calculate_base_xqp_fare(
                        row['gross_fare'], row['source_airport'], row['destination_airport']
                    ), axis=1
                )
                filtered_data = filtered_data[filtered_data['base_fare_calc'] <= self.fare_cap_value]
                filtered_data = filtered_data.drop(columns=['base_fare_calc'])
                after_count = len(filtered_data)
                if before_count != after_count:
                    self.logger.info(f"Fare cap filter on market fares (base fare comparison): {before_count - after_count} fares removed "
                                   f"(base_fare_cap={self.fare_cap_value:.2f})")
            
            if filtered_data.empty:
                self.logger.warning(f"No market fares remain after fare cap filter")
                return None
            
            # Data is already filtered for B2C and instances >= 2 from MongoDB
            if fare_type == 'min':
                sorted_data = filtered_data.sort_values('gross_fare', ascending=True)
            else:
                sorted_data = filtered_data.sort_values('gross_fare', ascending=False)
            
            # Get distance for yield calculation
            distance = self._get_distance(origin, destination)
            if distance == 0:
                self.logger.warning(f"Distance is 0 for {origin}-{destination}, cannot calculate yield")
                # Return base fare without yield check if distance not available
                if not sorted_data.empty:
                    first_row = sorted_data.iloc[0]
                    gross_fare = first_row['gross_fare']
                    row_origin = first_row['source_airport']
                    row_dest = first_row['destination_airport']
                    # For international routes with currency conversion, convert to INR first
                    if self.route == 'I' and self.conversion_rate != 1.0:
                        gross_fare_inr = self._convert_fare_to_inr(gross_fare)
                    else:
                        gross_fare_inr = gross_fare
                    base_fare = self._calculate_base_xqp_fare(gross_fare_inr, row_origin, row_dest)
                    return base_fare
                return None
            
            # Perform yield check for both domestic and international
            for _, row in sorted_data.iterrows():
                gross_fare = row['gross_fare']
                row_origin = row['source_airport']
                row_dest = row['destination_airport']
                
                try:
                    # For international routes with currency conversion, convert fare to INR for yield calculation
                    if self.route == 'I' and self.conversion_rate != 1.0:
                        # Market fare is in local currency, convert to INR for yield check
                        gross_fare_inr = self._convert_fare_to_inr(gross_fare)
                    else:
                        # Domestic or international without conversion - already in INR
                        gross_fare_inr = gross_fare
                    
                    # Calculate base fare and yield using each row's own origin/destination
                    base_fare = self._calculate_base_xqp_fare(gross_fare_inr, row_origin, row_dest)
                    yield_value = base_fare / distance
                    
                    # Check yield constraint
                    if yield_value >= self.MARKET_FARE_MIN_YIELD:
                        # Return BASE FARE in INR
                        return base_fare
                        
                except Exception as e:
                    self.logger.warning(f"Error checking yield for fare {gross_fare}: {str(e)}")
                    continue
            
            route_type = "International" if self.route == 'I' else "Domestic"
            self.logger.warning(f"No valid {fare_type} market fare found satisfying yield >= {self.MARKET_FARE_MIN_YIELD} for {route_type} route")
            return None
            
        except Exception as e:
            self.logger.error(f"Error finding valid market fare: {str(e)}")
            return None

    def calculate_buffer_fares_b2c(self, core_fares, booking_data):
        """Calculate buffer fares f1 to f3 and fn-2 to fn."""
        try:
            if not core_fares or booking_data.empty:
                self.failure_reason = "Missing core fares or booking data"
                return []
            
            if self.market_fares_data is None or self.market_fares_data.empty:
                self.failure_reason = "No market fares data available"
                return []
            
            # Extract origin and destination from sector
            origin = self.sector[:3]
            destination = self.sector[3:]
            
            # Filter for B2C data
            b2c_data = self.market_fares_data[self.market_fares_data['channel'] == 'B2C'].copy()
            
            # Validate core fares count
            if len(core_fares) != self.CORE_FARES_B2C:
                self.logger.warning(f"B2C core fares count mismatch: got {len(core_fares)}, expected {self.CORE_FARES_B2C}")
                if len(core_fares) > self.CORE_FARES_B2C:
                    core_fares = core_fares[:self.CORE_FARES_B2C]
                else:
                    while len(core_fares) < self.CORE_FARES_B2C:
                        core_fares.append(core_fares[-1] if core_fares else 0)
            
            f4 = core_fares[0]
            f61 = core_fares[-1]
            
            # Get min/max booking fares (already in base INR)
            min_booking_fare = float(booking_data['Fare'].min())
            max_booking_fare = float(booking_data['Fare'].max())
            
            # Find valid market fares
            valid_min_market_fare = self._find_valid_market_fare(b2c_data, 'min', origin, destination)
            valid_max_market_fare = self._find_valid_market_fare(b2c_data, 'max', origin, destination)
            
            self.logger.info(f"B2C Buffer Fare Calculation Started")
            self.logger.info(f"  Core fares: {len(core_fares)} (f4-f61)")
            self.logger.info(f"  Booking range: {min_booking_fare:.2f} - {max_booking_fare:.2f} {self.sector_currency}")
            self.logger.info(f"  Market fares: min={valid_min_market_fare}, max={valid_max_market_fare}")
            
            # ===== LOWER BUFFER FARES (f1, f2, f3) =====
            threshold_f4 = 0.96 * f4
            
            if valid_min_market_fare is not None and valid_min_market_fare < threshold_f4:
                f2 = valid_min_market_fare
                f1 = f2 - (0.02 * f2)
                f3 = (f2 + f4) / 2
                self.logger.info(f"Lower buffer: Using market fare {f2:.2f} (< {threshold_f4:.2f})")
            else:
                f3 = 0.98 * f4
                f2 = 0.98 * f3
                f1 = 0.98 * f2
                reason = "no valid fare" if valid_min_market_fare is None else f"fare {valid_min_market_fare:.2f} >= {threshold_f4:.2f}"
                self.logger.info(f"Lower buffer: Using multiplicative approach ({reason})")
            
            self.logger.info(f"  f1 = {f1:.2f}, f2 = {f2:.2f}, f3 = {f3:.2f}")
            
            # ===== HIGHER BUFFER FARES - Calculate fmax =====
            if self.MARKET_FLAG == "Y" and valid_max_market_fare is not None:
                fmax_new = max(max_booking_fare, valid_max_market_fare)
                self.logger.info(f"fmax calculation: max(booking={max_booking_fare:.2f}, market={valid_max_market_fare:.2f}) = {fmax_new:.2f}")
            else:
                fmax_new = max_booking_fare
                self.logger.info(f"fmax calculation: using max booking fare = {fmax_new:.2f} (MARKET_FLAG={self.MARKET_FLAG})")
            
            # Build initial complete ladder (before UP recalculation)
            initial_ladder = [f1, f2, f3] + core_fares
            
            # Identify starting fare for upsell calculation
            start_position = self.TOTAL_B2C_FARES - self.UP_B2C - 1
            
            if start_position < 0 or start_position >= len(initial_ladder):
                self.logger.error(f"Invalid UP_B2C configuration: start_position={start_position}, ladder_length={len(initial_ladder)}")
                self.failure_reason = f"Invalid UP_B2C configuration"
                return []
            
            start_fare = initial_ladder[start_position]
            start_fare_number = start_position + 1
            
            self.logger.info(f"Higher buffer UP calculation:")
            self.logger.info(f"  UP_B2C: {self.UP_B2C}")
            self.logger.info(f"  Start fare: f{start_fare_number} = {start_fare:.2f}")
            self.logger.info(f"  Target fmax: {fmax_new:.2f}")
            
            # Validate start_fare < fmax_new to avoid negative upsell
            if start_fare >= fmax_new:
                self.logger.error(f"Start fare f{start_fare_number} ({start_fare:.2f}) >= fmax ({fmax_new:.2f})")
                self.logger.error(f"This indicates core fares were pushed too high")
                self.failure_reason = f"Invalid fare structure: f{start_fare_number} >= fmax"
                return []
            
            # Calculate upsell factor
            upsell_factor = (fmax_new / start_fare) ** (1 / self.UP_B2C) - 1
            
            self.logger.info(f"  Upsell factor: {upsell_factor:.6f} ({upsell_factor*100:.4f}%)")
            
            if upsell_factor < 0.001:
                self.logger.warning(f"  Very small upsell factor - f{start_fare_number} is very close to fmax")
            
            # Generate recalculated fares using upsell factor
            recalculated_fares = []
            current_fare = start_fare
            
            self.logger.info(f"  Generating {self.UP_B2C} UP fares")
            for i in range(self.UP_B2C):
                current_fare = current_fare * (1 + upsell_factor)
                recalculated_fares.append(current_fare)
            
            # Build complete ladder (before final threshold adjustment)
            num_fares_to_keep = self.TOTAL_B2C_FARES - self.UP_B2C
            complete_ladder = initial_ladder[:num_fares_to_keep] + recalculated_fares
            
            self.logger.info(f"Complete ladder built: {len(complete_ladder)} fares")
            self.logger.info(f"  f1-f3: Lower buffer (3 fares)")
            self.logger.info(f"  f4-f{num_fares_to_keep}: Core ({num_fares_to_keep - 3} fares)")
            self.logger.info(f"  f{num_fares_to_keep + 1}-fmax: UP recalculated ({self.UP_B2C} fares)")
            
            # ===== APPLY MINIMUM THRESHOLD TO ALL FARES =====
            self.logger.info(f"Applying minimum threshold to all {self.TOTAL_B2C_FARES} fares...")
            
            threshold_func = (self.min_fare_threshold_domestic if self.route == 'D' 
                            else self.min_fare_threshold_international)
            
            adjustments_made = 0
            adjustments_detail = []
            
            # Forward pass: apply minimum threshold adjustments
            for i in range(len(complete_ladder) - 1):
                current_fare = complete_ladder[i]
                next_fare = complete_ladder[i + 1]
                min_threshold = threshold_func(current_fare)
                
                if next_fare - current_fare < min_threshold:
                    old_value = next_fare
                    complete_ladder[i + 1] = current_fare + min_threshold
                    adjustments_made += 1
                    
                    adjustment_info = f"f{i+1}→f{i+2}: {old_value:.2f} → {complete_ladder[i + 1]:.2f} (threshold: {min_threshold:.2f})"
                    adjustments_detail.append(adjustment_info)
            
            self.logger.info(f"Forward pass: {adjustments_made} threshold adjustments made")
            
            # Enforce fare cap: distribute fares exceeding cap evenly below it
            if self.fare_cap_value is not None:
                # Find the first fare that exceeds the cap
                first_capped_idx = None
                for i in range(len(complete_ladder)):
                    if complete_ladder[i] > self.fare_cap_value:
                        first_capped_idx = i
                        break
                
                if first_capped_idx is not None:
                    # Anchor = the fare just before the first capped fare
                    if first_capped_idx > 0:
                        anchor_fare = complete_ladder[first_capped_idx - 1]
                    else:
                        anchor_fare = 0
                    
                    num_fares_to_distribute = len(complete_ladder) - first_capped_idx
                    # Distribute evenly between anchor and fare_cap_value
                    # num_fares_to_distribute fares in (anchor, fare_cap_value] range
                    step = (self.fare_cap_value - anchor_fare) / (num_fares_to_distribute + 1)
                    
                    self.logger.info(f"Fare cap distribution: {num_fares_to_distribute} fares from f{first_capped_idx+1} to f{len(complete_ladder)}")
                    self.logger.info(f"  Anchor (f{first_capped_idx}): {anchor_fare:.2f}, Cap: {self.fare_cap_value:.2f}, Step: {step:.2f}")
                    
                    for i in range(num_fares_to_distribute):
                        idx = first_capped_idx + i
                        complete_ladder[idx] = anchor_fare + step * (i + 1)
            
            # Log final ladder summary
            self.logger.info(f"Final B2C ladder summary: Total={len(complete_ladder)}, Min={complete_ladder[0]:.2f}, Max={complete_ladder[-1]:.2f}")
            
            # Validate exactly self.TOTAL_B2C_FARES fares
            if len(complete_ladder) != self.TOTAL_B2C_FARES:
                self.logger.error(f"B2C ladder count error: got {len(complete_ladder)}, expected {self.TOTAL_B2C_FARES}")
                self.failure_reason = f"B2C ladder generation failed: got {len(complete_ladder)} fares, expected {self.TOTAL_B2C_FARES}"
                return []
            
            # Validate monotonically increasing
            for i in range(len(complete_ladder) - 1):
                if complete_ladder[i] >= complete_ladder[i + 1]:
                    self.logger.error(f"Non-increasing fares: f{i+1} ({complete_ladder[i]:.2f}) >= f{i+2} ({complete_ladder[i+1]:.2f})")
                    self.failure_reason = "Ladder has non-increasing fares"
                    return []
            
            # Validate minimum threshold compliance (skip for fares in cap distribution zone)
            threshold_violations = 0
            for i in range(len(complete_ladder) - 1):
                current_fare = complete_ladder[i]
                next_fare = complete_ladder[i + 1]
                # Skip threshold check for fares that were redistributed near the cap
                if self.fare_cap_value is not None and i >= (first_capped_idx - 1 if first_capped_idx is not None else len(complete_ladder)):
                    continue
                min_threshold = threshold_func(current_fare)
                actual_diff = next_fare - current_fare
                
                if actual_diff < min_threshold - 0.01:
                    self.logger.error(f"Threshold violation: f{i+1}→f{i+2}: diff={actual_diff:.2f} < threshold={min_threshold:.2f}")
                    threshold_violations += 1
            
            if threshold_violations > 0:
                self.logger.error(f"Found {threshold_violations} threshold violations")
                self.failure_reason = "Threshold violations detected in final ladder"
                return []
            
            return complete_ladder
            
        except Exception as e:
            self.logger.error(f"Error calculating B2C buffer fares: {str(e)}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            self.failure_reason = f"B2C buffer fare calculation failed: {str(e)}"
            raise
        
    def calculate_buffer_fares_b2b(self, core_fares, booking_data, b2c_min):
        """Calculate buffer fares f1 to f2 and fn-1 to fn for B2B ladder."""
        try:
            if not core_fares or booking_data.empty:
                self.failure_reason = "Missing core fares or booking data"
                return []
            
            if self.market_fares_data is None or self.market_fares_data.empty:
                self.failure_reason = "No market fares data available"
                return []
            
            # Extract origin and destination from sector
            origin = self.sector[:3]
            destination = self.sector[3:]
            
            # Filter for B2B data
            b2b_data = self.market_fares_data[self.market_fares_data['channel'] == 'B2B'].copy()
            
            # Validate core fares count 
            if len(core_fares) != self.CORE_FARES_B2B:
                self.logger.warning(f"B2B core fares count mismatch: got {len(core_fares)}, expected {self.CORE_FARES_B2B}")
                if len(core_fares) > self.CORE_FARES_B2B:
                    core_fares = core_fares[:self.CORE_FARES_B2B]
                else:
                    while len(core_fares) < self.CORE_FARES_B2B:
                        core_fares.append(core_fares[-1] if core_fares else 0)
            
            f3 = core_fares[0]
            # f18 = core_fares[-1]
            
            # Get min/max booking fares (already in base INR)
            min_booking_fare = float(booking_data['Fare'].min())
            max_booking_fare = float(booking_data['Fare'].max())
            
            # Find valid B2B market fares
            valid_min_market_fare = self._find_valid_market_fare(b2b_data, 'min', origin, destination)
            valid_max_market_fare = self._find_valid_market_fare(b2b_data, 'max', origin, destination)
            
            self.logger.info(f"B2B Buffer Fare Calculation Started")
            self.logger.info(f"  Core fares: {len(core_fares)}")
            self.logger.info(f"  Booking range: {min_booking_fare:.2f} - {max_booking_fare:.2f} {self.sector_currency}")
            self.logger.info(f"  Market fares: min={valid_min_market_fare}, max={valid_max_market_fare}")
            self.logger.info(f"  B2C_min (for constraint): {b2c_min:.2f}")
            
            # ===== LOWER BUFFER FARES (f1, f2) =====
            b2c_threshold_limit = b2c_min - self.B2B_THRESHOLD
            
            self.logger.info(f"B2B constraint analysis:")
            self.logger.info(f"  B2C_min: {b2c_min:.2f}")
            self.logger.info(f"  B2B_THRESHOLD: {self.B2B_THRESHOLD}")
            self.logger.info(f"  Threshold limit (B2C - threshold): {b2c_threshold_limit:.2f}")
            self.logger.info(f"  f3 (lowest bin): {f3:.2f}")
            
            # Case 1: XQP fare exists AND XQP < f3 AND XQP < B2C_min - threshold
            case_1_satisfied = False
            if valid_min_market_fare is not None:
                case_1_constraint_1 = valid_min_market_fare < f3
                case_1_constraint_2 = valid_min_market_fare < b2c_threshold_limit
                
                self.logger.info(f"Case 1 analysis - XQP fare: {valid_min_market_fare:.2f}")
                self.logger.info(f"  XQP < f3: {case_1_constraint_1}")
                self.logger.info(f"  XQP < B2C-threshold: {case_1_constraint_2}")
                
                if case_1_constraint_1 and case_1_constraint_2:
                    f2 = valid_min_market_fare
                    f1 = f2 - (0.02 * f2)
                    case_1_satisfied = True
                    self.logger.info(f"Case 1 SELECTED: Using B2B market fare")
                    self.logger.info(f"  f1 = {f1:.2f}, f2 = {f2:.2f}")
            else:
                self.logger.info(f"Case 1 analysis - No valid XQP fare found")
            
            if not case_1_satisfied:
                # Case 2: f3 < B2C_min - threshold
                case_2_constraint = f3 < b2c_threshold_limit
                self.logger.info(f"Case 2 analysis: f3 < B2C-threshold: {case_2_constraint}")
                
                if case_2_constraint:
                    f2 = 0.98 * f3
                    f1 = 0.96 * f2
                    self.logger.info(f"Case 2 SELECTED: Multiplicative approach")
                    self.logger.info(f"  f1 = {f1:.2f} (0.96*f2), f2 = {f2:.2f} (0.98*f3)")
                else:
                    # Case 3: No valid XQP AND f3 >= B2C_min - threshold
                    f1 = b2c_threshold_limit
                    f2 = (f1 + f3) / 2
                    self.logger.info(f"Case 3 SELECTED: Constraint-based approach")
                    self.logger.info(f"  f1 = {f1:.2f} (B2C-threshold), f2 = {f2:.2f} ((f1+f3)/2)")
            
            # ===== HIGHER BUFFER FARES - Calculate fmax_b2b =====
            if self.MARKET_FLAG == "Y" and valid_max_market_fare is not None:
                fmax_b2b_new = max(max_booking_fare, valid_max_market_fare)
                self.logger.info(f"fmax_b2b calculation: max(booking={max_booking_fare:.2f}, market={valid_max_market_fare:.2f}) = {fmax_b2b_new:.2f}")
            else:
                fmax_b2b_new = max_booking_fare
                self.logger.info(f"fmax_b2b calculation: using max booking fare = {fmax_b2b_new:.2f} (MARKET_FLAG={self.MARKET_FLAG})")
            
            # Build initial complete ladder (before UP recalculation)
            initial_ladder = [f1, f2] + core_fares
            
            # Identify starting fare for upsell calculation
            # For UP=5: start from f15 (position 14, 0-indexed)
            start_position = self.TOTAL_B2B_FARES - self.UP_B2B - 1
            
            if start_position < 0 or start_position >= len(initial_ladder):
                self.logger.error(f"Invalid UP_B2B configuration: start_position={start_position}, ladder_length={len(initial_ladder)}")
                self.failure_reason = f"Invalid UP_B2B configuration"
                return []
            
            start_fare = initial_ladder[start_position]
            start_fare_number = start_position + 1
            
            self.logger.info(f"Higher buffer UP calculation:")
            self.logger.info(f"  UP_B2B: {self.UP_B2B}")
            self.logger.info(f"  Start fare: f{start_fare_number} = {start_fare:.2f}")
            self.logger.info(f"  Target fmax_b2b: {fmax_b2b_new:.2f}")
            
            # Validate start_fare < fmax_b2b_new
            if start_fare >= fmax_b2b_new:
                self.logger.error(f"Start fare f{start_fare_number} ({start_fare:.2f}) >= fmax_b2b ({fmax_b2b_new:.2f})")
                self.failure_reason = f"Invalid fare structure: f{start_fare_number} >= fmax_b2b"
                return []
            
            # Calculate upsell factor
            upsell_factor = (fmax_b2b_new / start_fare) ** (1 / self.UP_B2B) - 1
            
            self.logger.info(f"  Upsell factor: {upsell_factor:.6f} ({upsell_factor*100:.4f}%)")
            
            if upsell_factor < 0.001:
                self.logger.warning(f"  Very small upsell factor - f{start_fare_number} is very close to fmax_b2b")
            
            # Generate recalculated fares using upsell factor
            recalculated_fares = []
            current_fare = start_fare
            
            self.logger.info(f"  Generating {self.UP_B2B} UP fares")
            for i in range(self.UP_B2B):
                current_fare = current_fare * (1 + upsell_factor)
                recalculated_fares.append(current_fare)
            
            # Build complete ladder (before final threshold adjustment)
            num_fares_to_keep = self.TOTAL_B2B_FARES - self.UP_B2B
            complete_ladder = initial_ladder[:num_fares_to_keep] + recalculated_fares
            
            self.logger.info(f"Complete ladder built: {len(complete_ladder)} fares")
            self.logger.info(f"  f1-f2: Lower buffer (2 fares)")
            self.logger.info(f"  f3-f{num_fares_to_keep}: Core ({num_fares_to_keep - 2} fares)")
            self.logger.info(f"  f{num_fares_to_keep + 1}-fmax_b2b: UP recalculated ({self.UP_B2B} fares)")
            
            # ===== APPLY MINIMUM THRESHOLD TO ALL B2B FARES =====
            self.logger.info(f"Applying minimum threshold to all {self.TOTAL_B2B_FARES} fares...")
            
            threshold_func = (self.min_fare_threshold_domestic if self.route == 'D' 
                            else self.min_fare_threshold_international)
            
            adjustments_made = 0
            adjustments_detail = []
            
            # Forward pass: apply minimum threshold adjustments
            for i in range(len(complete_ladder) - 1):
                current_fare = complete_ladder[i]
                next_fare = complete_ladder[i + 1]
                min_threshold = threshold_func(current_fare)
                
                if next_fare - current_fare < min_threshold:
                    old_value = next_fare
                    complete_ladder[i + 1] = current_fare + min_threshold
                    adjustments_made += 1
                    
                    adjustment_info = f"f{i+1}→f{i+2}: {old_value:.2f} → {complete_ladder[i + 1]:.2f} (threshold: {min_threshold:.2f})"
                    adjustments_detail.append(adjustment_info)
            
            self.logger.info(f"Forward pass: {adjustments_made} threshold adjustments made")
            
            # Enforce fare cap: distribute fares exceeding cap evenly below it
            if self.fare_cap_value is not None:
                # Find the first fare that exceeds the cap
                first_capped_idx = None
                for i in range(len(complete_ladder)):
                    if complete_ladder[i] > self.fare_cap_value:
                        first_capped_idx = i
                        break
                
                if first_capped_idx is not None:
                    # Anchor = the fare just before the first capped fare
                    if first_capped_idx > 0:
                        anchor_fare = complete_ladder[first_capped_idx - 1]
                    else:
                        anchor_fare = 0
                    
                    num_fares_to_distribute = len(complete_ladder) - first_capped_idx
                    # Distribute evenly between anchor and fare_cap_value
                    step = (self.fare_cap_value - anchor_fare) / (num_fares_to_distribute + 1)
                    
                    self.logger.info(f"Fare cap distribution: {num_fares_to_distribute} fares from f{first_capped_idx+1} to f{len(complete_ladder)}")
                    self.logger.info(f"  Anchor (f{first_capped_idx}): {anchor_fare:.2f}, Cap: {self.fare_cap_value:.2f}, Step: {step:.2f}")
                    
                    for i in range(num_fares_to_distribute):
                        idx = first_capped_idx + i
                        complete_ladder[idx] = anchor_fare + step * (i + 1)
            
            # Log final ladder summary
            self.logger.info(f"Final B2B ladder summary: Total={len(complete_ladder)}, Min={complete_ladder[0]:.2f}, Max={complete_ladder[-1]:.2f}")
            
            # Validate exactly self.TOTAL_B2B_FARES fares
            if len(complete_ladder) != self.TOTAL_B2B_FARES:
                self.logger.error(f"B2B ladder count error: got {len(complete_ladder)}, expected {self.TOTAL_B2B_FARES}")
                self.failure_reason = f"B2B ladder generation failed: got {len(complete_ladder)} fares, expected {self.TOTAL_B2B_FARES}"
                return []
            
            # Validate monotonically increasing
            for i in range(len(complete_ladder) - 1):
                if complete_ladder[i] >= complete_ladder[i + 1]:
                    self.logger.error(f"Non-increasing fares: f{i+1} ({complete_ladder[i]:.2f}) >= f{i+2} ({complete_ladder[i+1]:.2f})")
                    self.failure_reason = "B2B ladder has non-increasing fares"
                    return []
            
            # Validate minimum threshold compliance (skip for fares in cap distribution zone)
            threshold_violations = 0
            for i in range(len(complete_ladder) - 1):
                current_fare = complete_ladder[i]
                next_fare = complete_ladder[i + 1]
                # Skip threshold check for fares that were redistributed near the cap
                if self.fare_cap_value is not None and i >= (first_capped_idx - 1 if first_capped_idx is not None else len(complete_ladder)):
                    continue
                min_threshold = threshold_func(current_fare)
                actual_diff = next_fare - current_fare
                
                if actual_diff < min_threshold - 0.01:
                    self.logger.error(f"Threshold violation: f{i+1}→f{i+2}: diff={actual_diff:.2f} < threshold={min_threshold:.2f}")
                    threshold_violations += 1
            
            if threshold_violations > 0:
                self.logger.error(f"Found {threshold_violations} threshold violations")
                self.failure_reason = "Threshold violations detected in final B2B ladder"
                return []
            
            # Final B2B constraint validation
            b2b_min = complete_ladder[0]  # f1
            
            self.logger.info(f"B2B Constraint Validation:")
            self.logger.info(f"  B2B_min (f1): {b2b_min:.2f}")
            self.logger.info(f"  B2C_min: {b2c_min:.2f}")
            self.logger.info(f"  B2B_THRESHOLD: {self.B2B_THRESHOLD}")
            self.logger.info(f"  Required: B2B_min <= {b2c_threshold_limit:.2f}")
            self.logger.info(f"  Constraint satisfied: {b2b_min <= b2c_threshold_limit}")
            
            if b2b_min > b2c_threshold_limit:
                margin_violation = b2b_min - b2c_threshold_limit
                self.logger.error(f"B2B constraint violated by {margin_violation:.2f}")
                self.failure_reason = f"B2B constraint violation: B2B_min={b2b_min:.2f} > threshold={b2c_threshold_limit:.2f}"
                return []
            else:
                margin = b2c_threshold_limit - b2b_min
                self.logger.info(f"B2B constraint satisfied with margin of {margin:.2f}")
            
            return complete_ladder
            
        except Exception as e:
            self.logger.error(f"Error calculating B2B buffer fares: {str(e)}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            self.failure_reason = f"B2B buffer fare calculation failed: {str(e)}"
            raise
    
    
    def generate_saver_ladder(self):
        """Main ladder generation method."""
        try:
            booking_data_b2c, data_period = self.get_historical_booking_data(self.B2C_CHANNEL)
            booking_data_b2b, data_period = self.get_historical_booking_data(self.B2B_CHANNEL)
            
            # Apply fare cap to booking data before binning
            booking_data_b2c = self._apply_fare_cap_to_bookings(booking_data_b2c, self.B2C_CHANNEL)
            booking_data_b2b = self._apply_fare_cap_to_bookings(booking_data_b2b, self.B2B_CHANNEL)
            
            if booking_data_b2c.empty or booking_data_b2b.empty:
                if not self.failure_reason:
                    self.failure_reason = "No booking data available for ladder generation"
                return None
            
            fare_points_b2c = self.create_fare_bins(booking_data_b2c, self.B2C_CHANNEL)
            fare_points_b2b = self.create_fare_bins(booking_data_b2b, self.B2B_CHANNEL)
            if not fare_points_b2c or not fare_points_b2b:
                if not self.failure_reason:
                    self.failure_reason = "Failed to create fare bins"
                return None
            
            # Apply minimum threshold only for market fares, not ESB fares
            if self.fare_source_flag.lower() != 'esb':
                adjusted_fare_points_b2c = self.apply_minimum_threshold(fare_points_b2c, self.B2C_CHANNEL)
                adjusted_fare_points_b2b = self.apply_minimum_threshold(fare_points_b2b, self.B2B_CHANNEL)
                if not adjusted_fare_points_b2c or not adjusted_fare_points_b2b:
                    if not self.failure_reason:
                        self.failure_reason = "Failed to apply minimum threshold"
                    return None
            else:
                self.logger.info("Skipping minimum threshold for ESB fares")
                adjusted_fare_points_b2c = fare_points_b2c
                adjusted_fare_points_b2b = fare_points_b2b
                
            complete_ladder_b2c = self.calculate_buffer_fares_b2c(adjusted_fare_points_b2c, booking_data_b2c)
            if not complete_ladder_b2c:
                if not self.failure_reason:
                    self.failure_reason = "Failed to calculate B2C buffer fares"
                return None
            
            complete_ladder_b2b = self.calculate_buffer_fares_b2b(adjusted_fare_points_b2b, booking_data_b2b, min(complete_ladder_b2c))
            if not complete_ladder_b2b:
                if not self.failure_reason:
                    self.failure_reason = "Failed to calculate B2B buffer fares"
                return None
            
            # Final B2B constraint validation
            b2b_min = min(complete_ladder_b2b)
            b2c_min = min(complete_ladder_b2c)
            b2c_threshold_limit = b2c_min - self.B2B_THRESHOLD
            
            self.logger.info(f"B2B Constraint Validation:")
            self.logger.info(f"  B2B_min: {b2b_min:.2f}")
            self.logger.info(f"  B2C_min: {b2c_min:.2f}")
            self.logger.info(f"  B2B_THRESHOLD: {self.B2B_THRESHOLD}")
            self.logger.info(f"  Required: B2B_min < {b2c_threshold_limit:.2f}")
            self.logger.info(f"  Constraint satisfied: {b2b_min <= b2c_threshold_limit}")
            
            if b2b_min > b2c_threshold_limit:
                margin_violation = b2b_min - b2c_threshold_limit
                self.logger.info(f"B2B constraint validation: B2B constraint violated by {margin_violation:.2f}")
                self.failure_reason = f"B2B constraint violation: B2B_min={b2b_min:.2f} >= B2C_threshold_limit={b2c_threshold_limit:.2f}"
                return None
            else:
                margin = b2c_threshold_limit - b2b_min
                self.logger.info(f"B2B constraint validation: B2B constraint satisfied with margin of {margin:.2f}")
            
            # Fares are already in base form for both sources:
            # - ESB returns DiscountedBaseFare_Pro (base fares)
            # - Market fares are converted from gross to base upfront in _fetch_data_by_ndo
            base_ladder_b2c = list(complete_ladder_b2c)
            base_ladder_b2b = list(complete_ladder_b2b)
            self.logger.info(f"Ladder fares already in base form (source: {self.fare_source_flag})")
            
            # Enforce fare cap on final base ladder (DOM only)
            if self.fare_cap_value is not None:
                capped_b2c = 0
                for i in range(len(base_ladder_b2c)):
                    if base_ladder_b2c[i] > self.fare_cap_value:
                        base_ladder_b2c[i] = self.fare_cap_value
                        capped_b2c += 1
                
                capped_b2b = 0
                for i in range(len(base_ladder_b2b)):
                    if base_ladder_b2b[i] > self.fare_cap_value:
                        base_ladder_b2b[i] = self.fare_cap_value
                        capped_b2b += 1
                
                if capped_b2c > 0 or capped_b2b > 0:
                    self.logger.info(f"Fare cap enforcement: capped {capped_b2c} B2C and {capped_b2b} B2B base fares to {self.fare_cap_value:.2f}")
            
            # Convert final base INR ladder to sector's local currency for international routes
            if self.conversion_rate != 1.0:
                base_ladder_b2c = [fare * self.conversion_rate for fare in base_ladder_b2c]
                base_ladder_b2b = [fare * self.conversion_rate for fare in base_ladder_b2b]
                self.logger.info(f"Converted final ladder from base INR to {self.sector_currency} (rate: {self.conversion_rate})")
                self.logger.info(f"  B2C range: {min(base_ladder_b2c):.2f} - {max(base_ladder_b2c):.2f} {self.sector_currency}")
                self.logger.info(f"  B2B range: {min(base_ladder_b2b):.2f} - {max(base_ladder_b2b):.2f} {self.sector_currency}")
            
            # Determine source label
            fare_source = 'bookings' if self.fare_source_flag.lower() == 'esb' else 'market'
            
            return {
                'ladder_code' : self.ladder_code,
                'sector': self.sector,
                'route': self.route,
                'fare_source': fare_source,
                'data_period': data_period,
                'total_booking_records_b2c': len(booking_data_b2c),
                'total_booking_records_b2b': len(booking_data_b2b),
                'distinct_fares_count_b2c': len(booking_data_b2c['Fare'].unique()),
                'distinct_fares_count_b2b': len(booking_data_b2b['Fare'].unique()),
                'ladder_fares_b2c': base_ladder_b2c,
                'ladder_fares_b2b': base_ladder_b2b,
                'generation_timestamp': datetime.now()
            }
            
        except Exception as e:
            self.logger.error(f"Error generating ladder: {str(e)}")
            self.failure_reason = f"Ladder generation failed: {str(e)}"
            raise

    def run(self):
        """Main execution method - processes all sectors under a single run_id."""
        # Use batch_run_id if dispatched from orchestrator, otherwise generate new
        run_id = self.batch_run_id or str(uuid.uuid4())
        self.start_time = datetime.now(pytz.timezone('Asia/Kolkata'))
        all_results = []
        all_ladder_results = []
        total_outliers = 0
        
        # Get version info once for the batch
        try:
            self.currentVersion = self.pa_helper.get_current_version()
        except Exception as e:
            self.logger.warning(f"Version info collection failed: {str(e)}")
            self.currentVersion = None
        
        # Create initial audit record for the batch
        try:
            sectors_str = ', '.join(self.sectors)
            first_cfg = self.sector_configs[self.ladder_codes[0]]
            fare_source = 'bookings' if first_cfg.fare_source_flag == 'esb' else 'market'
            self.pa_helper.save_batch_run_audit(
                sectors_str, self.route, self.analystName, None, 
                self.start_time, self.currentVersion, run_id, None, 
                None, None, 'IN_PROGRESS', is_initial=True, source=fare_source,
                batch_total=self.batch_total
            )
        except Exception as e:
            self.logger.error(f"Failed to create initial batch audit record: {str(e)}")
        
        # Process each ladder code entry
        for ladder_code in self.ladder_codes:
            cfg = self.sector_configs[ladder_code]
            self.sector = cfg.sector
            self.ladder_code = ladder_code
            self.logger.info(f"=" * 80)
            self.logger.info(f"Processing {ladder_code} (sector: {self.sector}, run_id: {run_id})")
            self.logger.info(f"=" * 80)
            
            # Reset failure tracking
            self.failure_reason = None
            self.outlier_count = None
            self.fare_cap_value = None

            # Apply per-entry configuration from the input collection
            self._apply_sector_config(ladder_code)
            
            result = self._run_single_sector(run_id)
            all_results.append(result)
            
            # Accumulate outliers
            if self.outlier_count:
                total_outliers += self.outlier_count
            
            # Collect successful ladder results
            if result['status'] == 'SUCCESS' and result.get('ladder_result'):
                all_ladder_results.append(result['ladder_result'])
            
            self.logger.info(f"{ladder_code} ({self.sector}) completed with status: {result['status']}")
            if result['status'] == 'FAILED':
                self.logger.error(f"{ladder_code} ({self.sector}) failure reason: {result.get('reason', 'Unknown')}")
        
        # Summary
        success_count = sum(1 for r in all_results if r['status'] == 'SUCCESS')
        failed_count = len(all_results) - success_count
        
        self.logger.info(f"=" * 80)
        self.logger.info(f"BATCH SUMMARY: {success_count} succeeded, {failed_count} failed out of {len(all_results)} sectors")
        self.logger.info(f"=" * 80)
        
        # Update batch audit with final status
        try:
            batch_status = 'SUCCESS' if success_count > 0 else 'FAILED'
            failure_reasons = [r.get('reason') for r in all_results if r['status'] == 'FAILED']
            batch_failure_reason = '; '.join(failure_reasons) if failure_reasons else None
            
            self.pa_helper.save_batch_run_audit(
                sectors_str, self.route, self.analystName, total_outliers,
                self.start_time, self.currentVersion, run_id, batch_failure_reason,
                all_ladder_results, 'ladder_gen_output', batch_status, is_initial=False,
                success_count=success_count, failed_count=failed_count, source=fare_source
            )
        except Exception as e:
            self.logger.error(f"Failed to update batch audit record: {str(e)}")
        
        return {
            'statusCode': 200 if success_count > 0 else 500,
            'run_id': run_id,
            'total_sectors': len(all_results),
            'successful': success_count,
            'failed': failed_count,
            'results': all_results
        }
    
    def _run_single_sector(self, run_id):
        """Execute ladder generation for a single sector using provided run_id."""
        
        try:
            # Setup currency conversion for this sector
            self._setup_currency_conversion()
            
            # Collect all data
            data_collection_success = self.collect_all_data()
            
            if not data_collection_success:
                return {
                    'ladder_result': None,
                    'file_path': None,
                    'run_id': run_id,
                    'ladder_code': self.ladder_code,
                    'sector': self.sector,
                    'status': 'FAILED',
                    'reason': self.failure_reason or 'Data collection failed'
                }
            
            # Generate ladder
            ladder_result = self.generate_saver_ladder()
            
            if not ladder_result:
                return {
                    'ladder_result': None,
                    'file_path': None,
                    'run_id': run_id,
                    'ladder_code': self.ladder_code,
                    'sector': self.sector,
                    'status': 'FAILED',
                    'reason': self.failure_reason or 'Ladder generation failed'
                }
            
            # Save results
            try:
                file_path = self.pa_helper.save_ladder_to_csv(ladder_result, run_id, self.sector, self.analystName, self.route)
                
            except Exception as e:
                self.logger.error(f"Error saving results: {str(e)}")
                self.failure_reason = f"Failed to save results: {str(e)}"
                
                return {
                    'ladder_result': ladder_result,
                    'file_path': None,
                    'run_id': run_id,
                    'ladder_code': self.ladder_code,
                    'sector': self.sector,
                    'status': 'FAILED',
                    'reason': self.failure_reason
                }
            
            route_desc = "Domestic" if self.route == 'D' else "International"
            self.logger.info(f"Ladder generation completed: {self.ladder_code} / {self.sector} ({route_desc})")
            
            return {
                'ladder_result': ladder_result,
                'file_path': file_path,
                'run_id': run_id,
                'route': self.route,
                'route_description': route_desc,
                'ladder_code': self.ladder_code,
                'sector': self.sector,
                'status': 'SUCCESS',
                'summary': {
                    'b2c_total_ladder_points': len(ladder_result['ladder_fares_b2c']),
                    'b2c_min_fare': round(min(ladder_result['ladder_fares_b2c']), 2),
                    'b2c_max_fare': round(max(ladder_result['ladder_fares_b2c']), 2),
                    'b2b_total_ladder_points': len(ladder_result['ladder_fares_b2b']),
                    'b2b_min_fare': round(min(ladder_result['ladder_fares_b2b']), 2),
                    'b2b_max_fare': round(max(ladder_result['ladder_fares_b2b']), 2),
                    'data_period': ladder_result['data_period'],
                    'booking_records_used_b2c': ladder_result['total_booking_records_b2c'],
                    'booking_records_used_b2b': ladder_result['total_booking_records_b2b'],
                    'distinct_fares_analyzed_b2c': ladder_result['distinct_fares_count_b2c'],
                    'distinct_fares_analyzed_b2b': ladder_result['distinct_fares_count_b2b'],
                    'generation_time': ladder_result['generation_timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
                    'b2c_core_fare_points': self.CORE_FARES_B2C,
                    'b2c_buffer_fare_points': self.BUFFER_FARES_B2C,
                    'b2b_core_fare_points': self.CORE_FARES_B2B,
                    'b2b_buffer_fare_points': self.BUFFER_FARES_B2B
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error in sector execution: {str(e)}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            
            if not self.failure_reason:
                self.failure_reason = f"Unexpected error in sector execution: {str(e)}"
            
            return {
                'ladder_result': None,
                'file_path': None,
                'run_id': run_id,
                'ladder_code': self.ladder_code,
                'sector': self.sector,
                'status': 'FAILED',
                'reason': self.failure_reason,
                'error_details': str(e)
            }