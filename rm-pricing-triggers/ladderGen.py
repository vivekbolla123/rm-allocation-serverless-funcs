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


class LadderGen:
    def __init__(self, logger, params, overrides, context):
        """Initialize with required connections and constants."""
        self.logger = logger
        self.db = params['mongo']
        self.db_fares = params['mongofares']
        self.nav_conn = params['navconn']
        self.rm_rd_esb_conn = params['rdEsbConn']
        self.nav_db_name = configsettings.NAV_DB_NAME
        self.nav_db_dw_name = configsettings.NAV_DB_DW_NAME
        self.nav_db_wb_name = configsettings.NAV_DB_WB_NAME
        self.constants = ConstantsUtils(self.db)
        self.pa_helper = LadderPAHelper(self.db, self.db_fares, logger, self.calculate_all_in_fare)
        self.nav_helper = LadderNavHelper(self.nav_conn, self.nav_db_name, self.nav_db_dw_name, self.nav_db_wb_name, logger)
        self.esb_helper = LadderEsbHelper(self.rm_rd_esb_conn, logger)
        self.route = overrides['route']
        self.analystName = overrides['analyst']
        self.sector = overrides['sector']
        
        # Configuration parameters for ladder generation
        self.min_fare_threshold_domestic = lambda fare: max(0.02 * fare, 50)
        self.min_fare_threshold_international = lambda fare: 0.02 * fare
        self.min_distinct_fares_required = 150
        self.ladder_bins = 58
        self.total_ladder_points = 64
        self.COVERAGE_PARAMETER = 3.0
        
        # Data containers
        self.tax_master_dom_data = None
        self.tax_master_intl_data = None
        self.market_fares_data = None
        self.city_pair_data = None
        self.currentVersion = None
        # Failure tracking
        self.failure_reason = None
        
        
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
                self.market_fares_data, failure_reason = self.pa_helper.collect_market_fares(self.route, self.sector)
                if failure_reason:
                    self.failure_reason = failure_reason
                else:
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

    def remove_outliers(self, fares):
        """Remove lower outliers using Modified Z-Score method."""
        data = pd.DataFrame(fares)
        
        if len(data) <= 1:
            return data
        
        fare_l1 = data["Fare"].values
        
        MED = np.median(fare_l1)
        
        MAD = np.median(np.abs(fare_l1 - MED))
        if MAD == 0:
            MAD = 0.15 * MED
        
        coverage_value = self.COVERAGE_PARAMETER
        
        # Calculate MZS for all fares
        MZS = np.abs(fare_l1 - MED) / MAD
        
        mask = (fare_l1 >= MED) | (MZS < coverage_value)
        
        filtered_data = data[mask].copy()
        
        removed_count = len(data) - len(filtered_data)
        if removed_count > 0:
            self.logger.info(f"Removed {removed_count} lower outliers from {len(data)} fares")
        
        return filtered_data

    def get_historical_booking_data(self):
        """Get booking data with priority logic."""
        try:
            current_date = datetime.now().date()
            
            # Priority 1: Last 30 days
            start_date_30 = current_date - timedelta(days=30)
            combined_30 = self.esb_helper.collect_combined_booking_data(start_date_30, current_date, self.sector)
            combined_30 = self.remove_outliers(combined_30)
            distinct_fares_30 = len(combined_30['Fare'].unique()) if not combined_30.empty else 0
            
            if distinct_fares_30 >= self.min_distinct_fares_required:
                return combined_30, "Last 30 days"
            
            # Priority 2: Last 60 days
            start_date_60 = current_date - timedelta(days=60)
            combined_60 = self.esb_helper.collect_combined_booking_data(start_date_60, current_date, self.sector)
            combined_60 = self.remove_outliers(combined_60)
            distinct_fares_60 = len(combined_60['Fare'].unique()) if not combined_60.empty else 0
            
            if distinct_fares_60 >= self.min_distinct_fares_required:
                return combined_60, "Last 60 days"
            
            # Priority 3: Previous year same period
            prev_year_current = current_date.replace(year=current_date.year - 1)
            prev_year_start = prev_year_current + timedelta(days=30)
            combined_prev = self.esb_helper.collect_combined_booking_data(prev_year_start, prev_year_current, self.sector)
            combined_prev = self.remove_outliers(combined_prev)
            distinct_fares_prev = len(combined_prev['Fare'].unique()) if not combined_prev.empty else 0
            
            if distinct_fares_prev >= self.min_distinct_fares_required:
                return combined_prev, "Previous year same period"
            
            self.failure_reason = f"Insufficient distinct fares across all periods. Found: 30-day: {distinct_fares_30}, 60-day: {distinct_fares_60}, Previous year: {distinct_fares_prev}. Required: {self.min_distinct_fares_required}"
            self.logger.warning(self.failure_reason)
            return pd.DataFrame(), "Insufficient distinct fares"
            
        except Exception as e:
            self.logger.error(f"Error in booking data collection: {str(e)}")
            self.failure_reason = f"Booking data collection failed: {str(e)}"
            raise

    def create_fare_bins(self, booking_data):
        """Create 58 equal bins and calculate mean fares."""
        try:
            if booking_data.empty:
                self.failure_reason = "Empty booking data provided for fare bin creation"
                return []
            
            sorted_fares = booking_data['Fare'].sort_values().values
            if len(sorted_fares) < self.ladder_bins:
                self.failure_reason = f"Insufficient data points for binning. Found: {len(sorted_fares)}, Required: {self.ladder_bins}"
                return []
            
            bin_size = len(sorted_fares) // self.ladder_bins
            remainder = len(sorted_fares) % self.ladder_bins
            
            fare_points = []
            start_idx = 0
            
            for i in range(self.ladder_bins):
                current_bin_size = bin_size + (1 if i < remainder else 0)
                end_idx = start_idx + current_bin_size
                
                bin_fares = sorted_fares[start_idx:end_idx]
                if len(bin_fares) > 0:
                    fare_points.append(np.mean(bin_fares))
                
                start_idx = end_idx
            
            if len(fare_points) != self.ladder_bins:
                self.failure_reason = f"Fare bin creation failed. Expected {self.ladder_bins} bins, got {len(fare_points)}"
                return []
            
            return fare_points
            
        except Exception as e:
            self.logger.error(f"Error creating fare bins: {str(e)}")
            self.failure_reason = f"Fare bin creation failed: {str(e)}"
            raise

    def apply_minimum_threshold(self, fare_points):
        """Apply minimum threshold logic to ensure fare differences."""
        try:
            if not fare_points:
                self.failure_reason = "No fare points provided for threshold application"
                return fare_points
            
            threshold_func = (self.min_fare_threshold_domestic if self.route == 'D' 
                            else self.min_fare_threshold_international)
            
            adjusted_fares = fare_points.copy()
            
            for i in range(len(adjusted_fares) - 1):
                current_fare = adjusted_fares[i]
                next_fare = adjusted_fares[i + 1]
                min_threshold = threshold_func(current_fare)
                
                if next_fare - current_fare < min_threshold:
                    adjusted_fares[i + 1] = current_fare + min_threshold
                    self.logger.debug(f"Adjusted fare {i+1}: {next_fare} -> {adjusted_fares[i + 1]}")
            
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
            
            total_taxes = asf + psf + udf_df + rcs + cute + gst + duf
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
            
            # Calculate GST
            gst = gst_rate * (base_fare + rcs + cute + rcs_cute)
            
            total_taxes = asf + psf_adf + udf + psc_pz + rcs_cute + rcs + cute + gst + tax1 + tax2 + tax3 + tax4 + tax5
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
        """Calculate base XQP fare using reverse tax calculation based on route."""
        try:
            if self.route == 'D':
                return self._calculate_base_domestic_fare(gross_fare, origin, destination)
            elif self.route == 'I':
                # For international routes, base fare calculation is not required
                # Return the gross fare as base fare for international
                self.logger.debug(f"International route: returning gross fare as base fare for {origin}-{destination}")
                return gross_fare
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
            
            # Special handling for DUF sectors - subtract DUF value first
            if destination in self.constants.PRICING_DUF_SECTORS:
                duf_value = float(self.constants.PRICING_DUF_SECTORS[destination])
                adjusted_gross_fare -= duf_value
            
            # For base fare calculation, use original GST rate (don't apply GST exemption here)
            # This is because we need to reverse the original gross fare calculation which might have used 0 GST
            # But for base fare calculation, we use the original GST rate from tax master
            
            # Check if this was a GST exempt sector in gross fare calculation
            if origin in self.constants.PRICING_GST_SECTORS or destination in self.constants.PRICING_GST_SECTORS:
                # For GST exempt sectors, the gross fare was calculated with GST=0
                # So reverse calculation: Base Fare = (Gross Fare - DUF - Other Taxes) / 1
                # Since GST components were 0 in gross fare calculation
                other_taxes = asf + psf + udf_df + rcs + cute
                base_fare = adjusted_gross_fare - other_taxes
            else:
                # Standard reverse calculation: Base Fare = [Gross Fare - GST×(RCS+CUTE) - RCS - Other Taxes] / (1 + GST)
                gst_on_taxes = gst_rate * (rcs + cute)
                other_taxes = asf + psf + udf_df + cute
                base_fare = (adjusted_gross_fare - gst_on_taxes - rcs - other_taxes) / (1 + gst_rate)
            
            return base_fare
            
        except Exception as e:
            self.logger.error(f"Error calculating base domestic fare: {str(e)}")

    def _find_valid_market_fare(self, market_data, fare_type, origin, destination):
        """Find valid market fare that satisfies yield constraint (domestic only)."""
        try:
            if market_data.empty:
                return None
            
            # Data is already filtered for B2C and instances >= 2 from MongoDB
            if fare_type == 'min':
                sorted_data = market_data.sort_values('gross_fare', ascending=True)
            else:
                sorted_data = market_data.sort_values('gross_fare', ascending=False)
            
            # For international routes, yield check not needed
            if self.route == 'I':
                # Return first fare (already satisfies instances >= 2)
                return sorted_data.iloc[0]['gross_fare'] if not sorted_data.empty else None
            
            # For domestic routes, check yield constraint
            distance = self._get_distance(origin, destination)
            if distance == 0:
                self.logger.warning(f"Distance is 0 for {origin}-{destination}, cannot calculate yield")
                return None
            
            for _, row in sorted_data.iterrows():
                gross_fare = row['gross_fare']
                
                try:
                    # Calculate base fare and yield
                    base_fare = self._calculate_base_xqp_fare(gross_fare, origin, destination)
                    yield_value = base_fare / distance
                    
                    # Check yield constraint (domestic only)
                    if yield_value >= 1.5:
                        self.logger.info(f"Found valid {fare_type} market fare: {gross_fare:.2f} (yield={yield_value:.2f})")
                        return gross_fare
                    else:
                        self.logger.debug(f"Market fare {gross_fare:.2f} failed yield check: {yield_value:.2f} < 1.5")
                        
                except Exception as e:
                    self.logger.warning(f"Error checking yield for fare {gross_fare}: {str(e)}")
                    continue
            
            self.logger.warning(f"No valid {fare_type} market fare found satisfying yield >= 1.5")
            return None
            
        except Exception as e:
            self.logger.error(f"Error finding valid market fare: {str(e)}")
            return None

    def calculate_buffer_fares(self, core_fares, booking_data):
        """Calculate buffer fares f1-f3 and f62-f64."""
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
            
            # Data is already filtered for B2C and instances >= 2
            b2c_data = self.market_fares_data.copy()
            
            f4 = core_fares[0]
            f61 = core_fares[-1]
            
            # Find valid market fares (with yield check for domestic)
            valid_min_market_fare = self._find_valid_market_fare(b2c_data, 'min', origin, destination)
            valid_max_market_fare = self._find_valid_market_fare(b2c_data, 'max', origin, destination)
            
            # Calculate lower buffer fares (f1, f2, f3)
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
            
            threshold_f61 = 1.04 * f61
            
            if valid_max_market_fare is not None and valid_max_market_fare > threshold_f61:
                f63 = valid_max_market_fare
                f64 = f63 + (0.02 * f63)
                f62 = (f61 + f63) / 2
                self.logger.info(f"Upper buffer: Using market fare {f63:.2f} (> {threshold_f61:.2f})")
            else:
                f62 = 1.02 * f61
                f63 = 1.02 * f62
                f64 = 1.02 * f63
                reason = "no valid fare" if valid_max_market_fare is None else f"fare {valid_max_market_fare:.2f} <= {threshold_f61:.2f}"
                self.logger.info(f"Upper buffer: Using multiplicative approach ({reason})")
            
            complete_ladder = [f1, f2, f3] + core_fares + [f62, f63, f64]
            return complete_ladder
            
        except Exception as e:
            self.logger.error(f"Error calculating buffer fares: {str(e)}")
            self.failure_reason = f"Buffer fare calculation failed: {str(e)}"
            raise
        
    def generate_saver_ladder(self):
        """Main ladder generation method."""
        try:
            booking_data, data_period = self.get_historical_booking_data()
            if booking_data.empty:
                if not self.failure_reason:
                    self.failure_reason = "No booking data available for ladder generation"
                return None
            
            fare_points = self.create_fare_bins(booking_data)
            if not fare_points:
                if not self.failure_reason:
                    self.failure_reason = "Failed to create fare bins"
                return None
            
            adjusted_fare_points = self.apply_minimum_threshold(fare_points)
            if not adjusted_fare_points:
                if not self.failure_reason:
                    self.failure_reason = "Failed to apply minimum threshold"
                return None
                
            complete_ladder = self.calculate_buffer_fares(adjusted_fare_points, booking_data)
            
            if not complete_ladder:
                if not self.failure_reason:
                    self.failure_reason = "Failed to calculate buffer fares"
                return None
            
            return {
                'sector': self.sector,
                'route': self.route,
                'data_period': data_period,
                'total_booking_records': len(booking_data),
                'distinct_fares_count': len(booking_data['Fare'].unique()),
                'ladder_fares': complete_ladder,
                'generation_timestamp': datetime.now()
            }
            
        except Exception as e:
            self.logger.error(f"Error generating ladder: {str(e)}")
            self.failure_reason = f"Ladder generation failed: {str(e)}"
            raise

    def run(self):
        """Main execution method."""
        run_id = str(uuid.uuid4())
        
        try:
            self.start_time = datetime.now(pytz.timezone('Asia/Kolkata'))
            
            # Get version info
            try:
                self.currentVersion = self.pa_helper.get_current_version()
            except Exception as e:
                self.logger.warning(f"Version info collection failed: {str(e)}")
            
            # Insert initial audit record with IN_PROGRESS status BEFORE data collection
            try:
                self.pa_helper.save_run_audit(self.sector, self.route, self.analystName, self.start_time, self.currentVersion, run_id, self.failure_reason, None, None, 'IN_PROGRESS', is_initial=True)
            except Exception as e:
                self.logger.error(f"Failed to create initial audit record: {str(e)}")
            
            # Collect all data
            data_collection_success = self.collect_all_data()
            
            if not data_collection_success:
                # Update audit with failure
                self.pa_helper.save_run_audit(self.sector, self.route, self.analystName, self.start_time, self.currentVersion, run_id, self.failure_reason, None, None, 'FAILED', is_initial=False)
                return {
                    'ladder_result': None,
                    'file_path': None,
                    'run_id': run_id,
                    'status': 'FAILED',
                    'reason': self.failure_reason or 'Data collection failed'
                }
            
            # Generate ladder
            ladder_result = self.generate_saver_ladder()
            
            if not ladder_result:
                # Update audit with failure
                self.pa_helper.save_run_audit(self.sector, self.route, self.analystName, self.start_time, self.currentVersion, run_id, self.failure_reason, None, None, 'FAILED', is_initial=False)
                return {
                    'ladder_result': None,
                    'file_path': None,
                    'run_id': run_id,
                    'status': 'FAILED',
                    'reason': self.failure_reason or 'Ladder generation failed'
                }
            
            # Save results
            try:
                file_path = self.pa_helper.save_ladder_to_csv(ladder_result, run_id, self.sector, self.analystName, self.route)
                # Update audit with success - use original run_id
                self.pa_helper.save_run_audit(self.sector, self.route, self.analystName, self.start_time, self.currentVersion, run_id, None, ladder_result, file_path, 'SUCCESS', is_initial=False)
                
            except Exception as e:
                self.logger.error(f"Error saving results: {str(e)}")
                self.failure_reason = f"Failed to save results: {str(e)}"
                
                # Update audit with failure
                self.pa_helper.save_run_audit(self.sector, self.route, self.analystName, self.start_time, self.currentVersion, run_id, self.failure_reason, ladder_result, None, 'FAILED', is_initial=False)
                return {
                    'ladder_result': ladder_result,
                    'file_path': None,
                    'run_id': run_id,
                    'status': 'FAILED',
                    'reason': self.failure_reason
                }
            
            route_desc = "Domestic" if self.route == 'D' else "International"
            print(f"Ladder generation completed: {self.sector} ({route_desc})")
            
            return {
                'ladder_result': ladder_result,
                'file_path': file_path,
                'run_id': run_id,  # Return the save_run_id for output table reference
                'route': self.route,
                'route_description': route_desc,
                'sector': self.sector,
                'status': 'SUCCESS',
                'summary': {
                    'total_ladder_points': len(ladder_result['ladder_fares']),
                    'min_fare': round(min(ladder_result['ladder_fares']), 2),
                    'max_fare': round(max(ladder_result['ladder_fares']), 2),
                    'data_period': ladder_result['data_period'],
                    'booking_records_used': ladder_result['total_booking_records'],
                    'distinct_fares_analyzed': ladder_result['distinct_fares_count'],
                    'generation_time': ladder_result['generation_timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
                    'core_fare_points': 58,
                    'buffer_fare_points': 6
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error in main execution: {str(e)}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            
            if not self.failure_reason:
                self.failure_reason = f"Unexpected error in main execution: {str(e)}"
            
            # Update audit with failure
            try:
                self.pa_helper.save_run_audit(self.sector, self.route, self.analystName, self.start_time, self.currentVersion, run_id, self.failure_reason, None, None, 'FAILED', is_initial=False)
            except Exception as audit_error:
                self.logger.error(f"Failed to update failure audit: {str(audit_error)}")
            
            return {
                'ladder_result': None,
                'file_path': None,
                'run_id': run_id,
                'status': 'FAILED',
                'reason': self.failure_reason,
                'error_details': str(e)
            }