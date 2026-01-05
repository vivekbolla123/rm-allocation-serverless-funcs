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
        self.BUCKET_NAME = configsettings.PA_BUCKET_NAME
        self.FILE_PATH = configsettings.PA_FILE_PATH
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
        self.ladder_bins_b2c = 58
        self.ladder_bins_b2b = 16
        self.total_ladder_points = 64
        self.COVERAGE_PARAMETER = self.constants.COVERAGE_PARAMETER
        self.UP_B2C = self.constants.UP_B2C
        self.UP_B2B = self.constants.UP_B2B
        self.MARKET_FLAG = self.constants.MARKET_FLAG
        self.B2B_THRESHOLD = self.constants.B2B_THRESHOLD
        self.B2B_CHANNEL = "B2B"
        self.B2C_CHANNEL = "B2C"
        
        # Data containers
        self.tax_master_dom_data = None
        self.tax_master_intl_data = None
        self.market_fares_data = None
        self.city_pair_data = None
        self.currentVersion = None
        # Failure tracking
        self.failure_reason = None
        self.outlier_count = None
        
        
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
        """Get booking data with priority logic."""
        try:
            current_date = datetime.now().date()
            if channel == self.B2C_CHANNEL:
                # Priority 1: Last 30 days
                start_date_30 = current_date - timedelta(days=30)
                combined_30 = self.esb_helper.collect_combined_booking_data(start_date_30, current_date, self.sector, channel)
                combined_30 = self.remove_outliers(combined_30, period_label="30day")
                distinct_fares_30 = len(combined_30['Fare'].unique()) if not combined_30.empty else 0
                
                if distinct_fares_30 >= self.min_distinct_fares_required:
                    return combined_30, "Last 30 days"
                
                # Priority 2: Last 60 days
                start_date_60 = current_date - timedelta(days=60)
                combined_60 = self.esb_helper.collect_combined_booking_data(start_date_60, current_date, self.sector, channel)
                combined_60 = self.remove_outliers(combined_60, period_label="60day")
                distinct_fares_60 = len(combined_60['Fare'].unique()) if not combined_60.empty else 0
                
                if distinct_fares_60 >= self.min_distinct_fares_required:
                    return combined_60, "Last 60 days"
                
                # Priority 3: Previous year same period
                prev_year_current = current_date.replace(year=current_date.year - 1)
                prev_year_start = prev_year_current - timedelta(days=30)
                combined_prev = self.esb_helper.collect_combined_booking_data(prev_year_start, prev_year_current, self.sector, channel)
                combined_prev = self.remove_outliers(combined_prev, period_label="prev_year")
                distinct_fares_prev = len(combined_prev['Fare'].unique()) if not combined_prev.empty else 0
                
                if distinct_fares_prev >= self.min_distinct_fares_required:
                    return combined_prev, "Previous year same period"
                
                self.failure_reason = f"Insufficient distinct fares across all periods. Found: 30-day: {distinct_fares_30}, 60-day: {distinct_fares_60}, Previous year: {distinct_fares_prev}. Required: {self.min_distinct_fares_required} for Channel:{channel}"
                self.logger.warning(self.failure_reason)
                return pd.DataFrame(), "Insufficient distinct fares"
            
            else:
                # Priority 2: Last 60 days
                start_date_60 = current_date - timedelta(days=60)
                combined_60 = self.esb_helper.collect_combined_booking_data(start_date_60, current_date, self.sector, channel)
                combined_60 = self.remove_outliers(combined_60, period_label="60day")
                return combined_60, "Last 60 days"
            
        except Exception as e:
            self.logger.error(f"Error in booking data collection: {str(e)}")
            self.failure_reason = f"Booking data collection failed: {str(e)}"
            raise

    def create_fare_bins(self, booking_data, channel):
        """Create 58 equal bins and calculate mean fares."""
        try:
            if(channel == self.B2C_CHANNEL):
                ladderBins = self.ladder_bins_b2c
            else:
                ladderBins = self.ladder_bins_b2b
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
            
            if destination in self.constants.PRICING_DUF_SECTORS:
                duf_value = float(self.constants.PRICING_DUF_SECTORS[destination])
                adjusted_gross_fare -= duf_value
                
            if origin in self.constants.PRICING_GST_SECTORS or destination in self.constants.PRICING_GST_SECTORS:
                other_taxes = asf + psf + udf_df + rcs + cute
                base_fare = adjusted_gross_fare - other_taxes
            else:
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

    def calculate_buffer_fares_b2c(self, core_fares, booking_data):
        """Calculate buffer fares f1-f3 and f60-f64 to create exactly 64 fares."""
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
            
            # Core fares should be exactly 58 fares (f4-f61)
            if len(core_fares) != 58:
                self.logger.warning(f"B2C core fares count mismatch: got {len(core_fares)}, expected 58")
                # Adjust core fares to exactly 58
                if len(core_fares) > 58:
                    core_fares = core_fares[:58]
                else:
                    # Pad with last fare value
                    while len(core_fares) < 58:
                        core_fares.append(core_fares[-1] if core_fares else 0)
            
            f4 = core_fares[0]
            f61 = core_fares[-1]  # This should be f61 (index 57 in 58-element array)
            
            min_booking_fare = booking_data['Fare'].min()
            max_booking_fare = booking_data['Fare'].max()
            
            min_booking_fare = float(min_booking_fare) if min_booking_fare else None
            max_booking_fare = float(max_booking_fare) if max_booking_fare else None
            
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
            
            # Calculate f64 (highest fare)
            if self.MARKET_FLAG == "True" and valid_max_market_fare is not None:
                f64 = max(max_booking_fare, valid_max_market_fare)
                self.logger.info(f"f64 calculation: max(booking={max_booking_fare:.2f}, market={valid_max_market_fare:.2f}) = {f64:.2f}")
            else:
                f64 = max_booking_fare
                self.logger.info(f"f64 calculation: using max booking fare = {f64:.2f} (market flag={self.MARKET_FLAG})")

            # Calculate higher buffer fares using UP_B2C
            # For UP_B2C=5: Generate f60, f61, f62, f63, f64
            # Start from f(64-UP_B2C) = f59, then generate UP_B2C fares: f60, f61, f62, f63, f64
            
            # f59 should be at index 55 in core_fares (f4=0, f5=1, ..., f59=55, f60=56, f61=57)
            f59_index = 55  # f59 is at index 55 in core_fares (f4 is at index 0)
            
            if f59_index >= len(core_fares):
                self.logger.error(f"Cannot find f59: index {f59_index} >= core_fares length {len(core_fares)}")
                self.failure_reason = f"Invalid core fares structure for UP_B2C calculation"
                return []
            
            start_fare = core_fares[f59_index]  # f59
            upsell_factor = (f64 / start_fare) ** (1 / self.UP_B2C) - 1
            
            self.logger.info(f"Higher buffer calculation: start_fare=f59={start_fare:.2f}, "
                f"f64={f64:.2f}, UP_B2C={self.UP_B2C}, upsell_factor={upsell_factor:.6f}")
            
            # Generate f60, f61, f62, f63, f64 (UP_B2C fares total)
            higher_ladder_fares = []
            current_fare = start_fare
            
            for i in range(self.UP_B2C):
                current_fare = current_fare * (1 + upsell_factor)
                fare_number = 60 + i  # f60, f61, f62, f63, f64
                higher_ladder_fares.append(current_fare)
                self.logger.info(f"f{fare_number} = {current_fare:.2f}")
            
            # Remove overlapping fares from core_fares
            # core_fares has f4-f61 (58 fares), we want f4-f59 (56 fares), so remove last 2 fares (f60, f61)
            overlap_count = 2  # Remove f60, f61 from core
            adjusted_core_fares = core_fares[:-overlap_count]
            
            # Complete ladder: [f1, f2, f3] + adjusted_core_fares[f4-f59] + higher_ladder_fares[f60-f64]
            complete_ladder = [f1, f2, f3] + adjusted_core_fares + higher_ladder_fares
            
            self.logger.info(f"Complete B2C ladder: {len(complete_ladder)} fares (f1-f64)")
            self.logger.info(f"  Lower buffer: f1, f2, f3 (3 fares)")
            self.logger.info(f"  Core: f4-f59 ({len(adjusted_core_fares)} fares)")
            self.logger.info(f"  Higher buffer: f60-f64 ({len(higher_ladder_fares)} fares)")
            
            # Validate we have exactly 64 fares
            if len(complete_ladder) != 64:
                self.logger.error(f"B2C ladder count error: got {len(complete_ladder)}, expected 64")
                self.failure_reason = f"B2C ladder generation failed: got {len(complete_ladder)} fares, expected 64"
                return []
            
            return complete_ladder
            
        except Exception as e:
            self.logger.error(f"Error calculating buffer fares: {str(e)}")
            self.failure_reason = f"Buffer fare calculation failed: {str(e)}"
            raise
        
    def calculate_buffer_fares_b2b(self, core_fares, booking_data, b2c_min):
        """Calculate buffer fares f1-f2 and f16-f20 for B2B ladder to create exactly 20 fares."""
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
            
            # Core fares should be exactly 16 fares (f3-f18)
            if len(core_fares) != 16:
                self.logger.warning(f"B2B core fares count mismatch: got {len(core_fares)}, expected 16")
                # Adjust core fares to exactly 16
                if len(core_fares) > 16:
                    core_fares = core_fares[:16]
                else:
                    # Pad with last fare value
                    while len(core_fares) < 16:
                        core_fares.append(core_fares[-1] if core_fares else 0)
            
            f3 = core_fares[0]  # Lowest bin fare
            f18 = core_fares[-1]  # This should be f18 (index 15 in 16-element array)
            
            min_booking_fare = booking_data['Fare'].min()
            max_booking_fare = booking_data['Fare'].max()
            
            min_booking_fare = float(min_booking_fare) if min_booking_fare else None
            max_booking_fare = float(max_booking_fare) if max_booking_fare else None
            
            # Find valid B2B market fares
            valid_min_market_fare = self._find_valid_market_fare(b2b_data, 'min', origin, destination)
            valid_max_market_fare = self._find_valid_market_fare(b2b_data, 'max', origin, destination)
            
            # Calculate lower buffer fares (f1, f2)
            # Constraint: B2B minimum fare should be LOWER than B2C minimum fare by at least B2B_THRESHOLD (100)
            # Target: B2B_min < B2C_min - B2B_THRESHOLD
            
            b2c_threshold_limit = b2c_min - self.B2B_THRESHOLD
            
            self.logger.info(f"B2B constraint analysis:")
            self.logger.info(f"  B2C_min (V9): {b2c_min:.2f}")
            self.logger.info(f"  B2B_THRESHOLD: {self.B2B_THRESHOLD}")
            self.logger.info(f"  Threshold limit (B2C - 100): {b2c_threshold_limit:.2f}")
            self.logger.info(f"  f3 (lowest bin): {f3:.2f}")
            
            # Case 1: If XQP fare exists that satisfies constraints (XQP < B2C - 100 and XQP < f3 and minimum instances)
            case_1_satisfied = False
            if valid_min_market_fare is not None:
                case_1_constraint_1 = valid_min_market_fare < f3
                case_1_constraint_2 = valid_min_market_fare < b2c_threshold_limit
                self.logger.info(f"Case 1 analysis - XQP fare: {valid_min_market_fare:.2f}")
                self.logger.info(f"  XQP < f3: {case_1_constraint_1} ({valid_min_market_fare:.2f} < {f3:.2f})")
                self.logger.info(f"  XQP < B2C-100: {case_1_constraint_2} ({valid_min_market_fare:.2f} < {b2c_threshold_limit:.2f})")
                
                if case_1_constraint_1 and case_1_constraint_2:
                    f2 = valid_min_market_fare
                    f1 = f2 - (0.02 * f2)
                    case_1_satisfied = True
                    self.logger.info(f"Case 1 SELECTED: Using B2B market fare f2={f2:.2f}, f1={f1:.2f}")
            else:
                self.logger.info(f"Case 1 analysis - No valid XQP fare found")
            
            if not case_1_satisfied:
                # Case 2: If f3 < B2C(V9) - 100, use multiplicative approach
                case_2_constraint = f3 < b2c_threshold_limit
                self.logger.info(f"Case 2 analysis:")
                self.logger.info(f"  f3 < B2C-100: {case_2_constraint} ({f3:.2f} < {b2c_threshold_limit:.2f})")
                
                if case_2_constraint:
                    f2 = 0.98 * f3
                    f1 = 0.96 * f2
                    self.logger.info(f"Case 2 SELECTED: f3 < B2C-threshold, using multiplicative f2={f2:.2f}, f1={f1:.2f}")
                else:
                    # Case 3: No XQP fare satisfying constraints - ensure B2B constraint is met
                    f1 = b2c_threshold_limit
                    f2 = (f1 + f3) / 2
                    self.logger.info(f"Case 3 SELECTED: No valid conditions met, f1={f1:.2f}, f2={f2:.2f}")
                    self.logger.info(f"Case 3 reasoning:")
                    self.logger.info(f"  - No XQP fare satisfying all constraints (< f3 AND < B2C-100)")
                    self.logger.info(f"  - f3 ({f3:.2f}) >= B2C-100 ({b2c_threshold_limit:.2f})")
            
            # Calculate f20 (highest buffer fare)
            if self.MARKET_FLAG == "True" and valid_max_market_fare is not None:
                f20 = max(max_booking_fare, valid_max_market_fare)
                self.logger.info(f"f20 calculation: max(booking={max_booking_fare:.2f}, market={valid_max_market_fare:.2f}) = {f20:.2f}")
            else:
                f20 = max_booking_fare
                self.logger.info(f"f20 calculation: using max booking fare = {f20:.2f} (market flag={self.MARKET_FLAG})")
            
            # Calculate higher buffer fares using UP_B2B
            # For UP_B2B=5: Generate f16, f17, f18, f19, f20
            # Start from f(20-UP_B2B) = f15, then generate UP_B2B fares: f16, f17, f18, f19, f20
            
            # f15 should be at index 12 in core_fares (f3=0, f4=1, ..., f15=12, f16=13, f17=14, f18=15)
            f15_index = 12  # f15 is at index 12 in core_fares (f3 is at index 0)
            
            if f15_index >= len(core_fares):
                self.logger.error(f"Cannot find f15: index {f15_index} >= core_fares length {len(core_fares)}")
                self.failure_reason = f"Invalid core fares structure for UP_B2B calculation"
                return []
            
            start_fare = core_fares[f15_index]  # f15
            upsell_factor = (f20 / start_fare) ** (1 / self.UP_B2B) - 1
            
            self.logger.info(f"Higher buffer calculation: start_fare=f15={start_fare:.2f}, "
                f"f20={f20:.2f}, UP_B2B={self.UP_B2B}, upsell_factor={upsell_factor:.6f}")
            
            # Generate f16, f17, f18, f19, f20 (UP_B2B fares total)
            higher_ladder_fares = []
            current_fare = start_fare
            
            for i in range(self.UP_B2B):
                current_fare = current_fare * (1 + upsell_factor)
                fare_number = 16 + i  # f16, f17, f18, f19, f20
                higher_ladder_fares.append(current_fare)
                self.logger.info(f"f{fare_number} = {current_fare:.2f}")
            
            # Remove overlapping fares from core_fares
            # core_fares has f3-f18 (16 fares), we want f3-f15 (13 fares), so remove last 3 fares (f16, f17, f18)
            overlap_count = 3  # Remove f16, f17, f18 from core
            adjusted_core_fares = core_fares[:-overlap_count]
            
            # Complete B2B ladder: [f1, f2] + adjusted_core_fares[f3-f15] + higher_ladder_fares[f16-f20]
            complete_ladder = [f1, f2] + adjusted_core_fares + higher_ladder_fares
            
            self.logger.info(f"Complete B2B ladder: {len(complete_ladder)} fares (f1-f20)")
            self.logger.info(f"  Lower buffer: f1, f2 (2 fares)")
            self.logger.info(f"  Core: f3-f15 ({len(adjusted_core_fares)} fares)")
            self.logger.info(f"  Higher buffer: f16-f20 ({len(higher_ladder_fares)} fares)")
            self.logger.info(f"B2B-B2C constraint: f1={f1:.2f} < B2C_min={b2c_min:.2f} - {self.B2B_THRESHOLD} = {b2c_threshold_limit:.2f}")
            
            # Validate we have exactly 20 fares
            if len(complete_ladder) != 20:
                self.logger.error(f"B2B ladder count error: got {len(complete_ladder)}, expected 20")
                self.failure_reason = f"B2B ladder generation failed: got {len(complete_ladder)} fares, expected 20"
                return []
            
            return complete_ladder
            
        except Exception as e:
            self.logger.error(f"Error calculating B2B buffer fares: {str(e)}")
            self.failure_reason = f"B2B buffer fare calculation failed: {str(e)}"
            raise
    
    def generate_saver_ladder(self):
        """Main ladder generation method."""
        try:
            booking_data_b2c, data_period = self.get_historical_booking_data(self.B2C_CHANNEL)
            booking_data_b2b, data_period = self.get_historical_booking_data(self.B2B_CHANNEL)
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
            
            adjusted_fare_points_b2c = self.apply_minimum_threshold(fare_points_b2c)
            adjusted_fare_points_b2b = self.apply_minimum_threshold(fare_points_b2b)
            if not adjusted_fare_points_b2c or not adjusted_fare_points_b2b:
                if not self.failure_reason:
                    self.failure_reason = "Failed to apply minimum threshold"
                return None
                
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
            
            return {
                'sector': self.sector,
                'route': self.route,
                'data_period': data_period,
                'total_booking_records_b2c': len(booking_data_b2c),
                'total_booking_records_b2b': len(booking_data_b2b),
                'distinct_fares_count_b2c': len(booking_data_b2c['Fare'].unique()),
                'distinct_fares_count_b2b': len(booking_data_b2b['Fare'].unique()),
                'ladder_fares_b2c': complete_ladder_b2c,
                'ladder_fares_b2b': complete_ladder_b2b,
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
                self.pa_helper.save_run_audit(self.sector, self.route, self.analystName, self.outlier_count, self.start_time, self.currentVersion, run_id, self.failure_reason, None, None, 'IN_PROGRESS', is_initial=True)
            except Exception as e:
                self.logger.error(f"Failed to create initial audit record: {str(e)}")
            
            # Collect all data
            data_collection_success = self.collect_all_data()
            
            if not data_collection_success:
                # Update audit with failure
                self.pa_helper.save_run_audit(self.sector, self.route, self.analystName, self.outlier_count, self.start_time, self.currentVersion, run_id, self.failure_reason, None, None, 'FAILED', is_initial=False)
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
                self.pa_helper.save_run_audit(self.sector, self.route, self.analystName, self.outlier_count, self.start_time, self.currentVersion, run_id, self.failure_reason, None, None, 'FAILED', is_initial=False)
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
                self.pa_helper.save_run_audit(self.sector, self.route, self.analystName, self.outlier_count, self.start_time, self.currentVersion, run_id, None, ladder_result, file_path, 'SUCCESS', is_initial=False)
                
            except Exception as e:
                self.logger.error(f"Error saving results: {str(e)}")
                self.failure_reason = f"Failed to save results: {str(e)}"
                
                # Update audit with failure
                self.pa_helper.save_run_audit(self.sector, self.route, self.analystName, self.outlier_count, self.start_time, self.currentVersion, run_id, self.failure_reason, ladder_result, None, 'FAILED', is_initial=False)
                return {
                    'ladder_result': ladder_result,
                    'file_path': None,
                    'run_id': run_id,
                    'status': 'FAILED',
                    'reason': self.failure_reason
                }
            
            route_desc = "Domestic" if self.route == 'D' else "International"
            self.logger.info(f"Ladder generation completed: {self.sector} ({route_desc})")
            
            return {
                'ladder_result': ladder_result,
                'file_path': file_path,
                'run_id': run_id,
                'route': self.route,
                'route_description': route_desc,
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
                    'b2c_core_fare_points': 58,
                    'b2c_buffer_fare_points': 6,
                    'b2b_core_fare_points': 16,
                    'b2b_buffer_fare_points': 4
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error in main execution: {str(e)}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            
            if not self.failure_reason:
                self.failure_reason = f"Unexpected error in main execution: {str(e)}"
            
            # Update audit with failure
            try:
                self.pa_helper.save_run_audit(self.sector, self.route, self.analystName, self.outlier_count, self.start_time, self.currentVersion, run_id, self.failure_reason, None, None, 'FAILED', is_initial=False)
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