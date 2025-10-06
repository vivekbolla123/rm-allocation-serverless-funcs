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


class LadderGen:
    def __init__(self, logger, params, overrides, context):
        """Initialize with required connections and constants."""
        self.logger = logger
        self.rm_rd_conn = params['wrconn']
        self.nav_conn = params['navconn']
        self.rm_rd_esb_conn = params['rdEsbConn']
        self.nav_db_name = configsettings.NAV_DB_NAME
        self.nav_db_dw_name = configsettings.NAV_DB_DW_NAME
        self.nav_db_wb_name = configsettings.NAV_DB_WB_NAME
        self.constants = ConstantsUtils(self.rm_rd_conn)
        self.route = overrides['route']
        self.analystName = overrides['analyst']
        self.sector = overrides['sector']
        
        # Configuration parameters for ladder generation
        self.min_fare_threshold_domestic = lambda fare: max(0.02 * fare, 50)
        self.min_fare_threshold_international = lambda fare: 0.02 * fare
        self.min_distinct_fares_required = 150
        self.ladder_bins = 58
        self.total_ladder_points = 64
        
        # Data containers
        self.tax_master_dom_data = None
        self.tax_master_intl_data = None
        self.market_fares_data = None
        self.city_pair_data = None
        self.booked_data = None
        
        # Failure tracking
        self.failure_reason = None

    # Data Collection Methods

    def collect_tax_master_data(self):
        """Collect both domestic and international tax master data."""
        try:
            # Domestic tax data
            dom_query = "SELECT Station, Code, ASF, CUTE, PSF, RCS, UDF_DF, GST FROM QP_DW_PRIAUT.tax_mast_dom"
            self.tax_master_dom_data = pd.read_sql(dom_query, self.rm_rd_conn)
            
            # International tax data
            intl_query = """
            SELECT Currency, Sectors, GST, CUTE, ASF, PSF_ADF, UDF, PSC_PZ, RCS_CUTE, RCS, 
                   Tax1, Tax2, Tax3, Tax4, Tax5 
            FROM QP_DW_PRIAUT.tax_mast_intl
            """
            self.tax_master_intl_data = pd.read_sql(intl_query, self.rm_rd_conn)
            
            if self.tax_master_dom_data.empty and self.tax_master_intl_data.empty:
                self.failure_reason = "No tax master data available"
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error collecting tax master data: {str(e)}")
            self.failure_reason = f"Tax master data collection failed: {str(e)}"
            raise

    def collect_city_pair_data(self):
        """Collect city pair data for distances - filtered for current sector."""
        try:
            origin = self.sector[:3]
            destination = self.sector[3:]
            
            query = f"""
            SELECT CityPairID, CityPair, CityPairGroup, DepartureStation, ArrivalStation, ActualDistance
            FROM {self.nav_db_dw_name}dw.CityPair
            WHERE (DepartureStation = '{origin}' AND ArrivalStation = '{destination}')
            """
            
            self.city_pair_data = pd.read_sql(query, self.nav_conn)
            
            if self.city_pair_data.empty:
                self.failure_reason = f"No city pair data found for sector {self.sector}"
                self.logger.warning(self.failure_reason)
                # Don't raise exception, this is not critical for ladder generation
                
            return self.city_pair_data
            
        except Exception as e:
            self.logger.error(f"Error collecting city pair data: {str(e)}")
            self.failure_reason = f"City pair data collection failed: {str(e)}"
            raise

    def collect_market_fares(self):
        """Collect individual market fares for constraint checking."""
        try:
            route_filter = "AND outbound_stop = 0" if self.route == 'D' else "AND outbound_stop >= 0"
            wherecondition = f"""outbound_airline_code NOT IN ('9I', 'IC', 'S9', 'S5','QP','G8') 
                    AND CONCAT(source_airport, destination_airport) = '{self.sector}'
                    AND CAST(departure_date AS DATE) BETWEEN CURDATE() AND DATE_ADD(CURDATE(), INTERVAL 365 DAY)
                    AND created_at > DATE_SUB(NOW(), INTERVAL 24 HOUR)
                    {route_filter}"""
            
            query = f"""
            WITH combined_data AS (
                SELECT gross_fare, outbound_airline_code, destination_airport, source_airport, channel, created_at
                FROM market_fares_ag
                WHERE {wherecondition}
                
                UNION ALL
                
                SELECT gross_fare, outbound_airline_code, destination_airport, source_airport, channel, created_at
                FROM market_fares_rg
                WHERE {wherecondition}
            ),
            fare_instances AS (
                SELECT gross_fare, outbound_airline_code, source_airport, destination_airport, channel, created_at,
                    COUNT(*) OVER (PARTITION BY source_airport, destination_airport, channel, gross_fare) as instances
                FROM combined_data
            ),
            latest_records AS (
                SELECT gross_fare, outbound_airline_code, source_airport, destination_airport, channel, instances,
                    ROW_NUMBER() OVER (PARTITION BY channel, destination_airport, source_airport, outbound_airline_code ORDER BY created_at DESC) as row_num
                FROM fare_instances
            )
            SELECT source_airport, destination_airport, channel, gross_fare, outbound_airline_code, instances
            FROM latest_records
            WHERE row_num = 1
            ORDER BY channel, gross_fare ASC
            """
            
            self.market_fares_data = pd.read_sql(query, self.rm_rd_conn)
            
            if self.market_fares_data.empty:
                self.failure_reason = f"No market fares data available for sector {self.sector}"
                self.logger.warning(self.failure_reason)
                
            return self.market_fares_data
            
        except Exception as e:
            self.logger.error(f"Error collecting market fares: {str(e)}")
            self.failure_reason = f"Market fares data collection failed: {str(e)}"
            raise
        
    def collect_booked_data(self, date_range_days=30):
        """Collect passenger booking data from Navitaire stored procedure for PLF calculation."""
        try:
            start_date = date.today()
            end_date = start_date + timedelta(days=date_range_days)
            
            # Format dates for SQL Server stored procedure
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')
            
            # Execute stored procedure
            query = f"""
            EXEC {self.nav_db_wb_name}[dbo].[SP_Bookeddata]
            @DepartureDate_from = N'{start_date_str}',
            @DepartureDate_to = N'{end_date_str}'
            """
            
            self.booked_data = pd.read_sql(query, self.nav_conn)
            
            return self.booked_data
            
        except Exception as e:
            self.logger.error(f"Error collecting booked data: {str(e)}")
            self.failure_reason = f"Booked data collection failed: {str(e)}"
            raise

    def collect_combined_booking_data(self, start_date, end_date):
        """Optimized single query for both flown and booked data."""
        try:
            wherecondition = f"""WHERE PX.BookingDate >= '{start_date}' AND PX.BookingDate < '{end_date}'
                    AND SUBSTRING(PX.FareBasis, 5, 1) = 'R'
                    AND LEFT(PX.FareBasis, 1) NOT IN ('S', 'W')
                    AND CONCAT(PX.Origin, PX.Destination) = '{self.sector}'
                    AND PX.BookingID IS NOT NULL
                    AND IL.FlightNumber < 7000"""
                    
            query = f"""
            SELECT Fare, FareBasis, BookingDate, Origin, Destination, data_source
            FROM (
                SELECT
                    (PX.DiscountedBaseFare_Pro + PX.YQ_Pro + PX.RCS + PX.CUTE + PX.SGST + PX.UGST + PX.IGST + PX.CGST + PX.AirportCharges + PX.DUDF) AS Fare,
                    PX.FareBasis, PX.BookingDate, PX.Origin, PX.Destination, 'FLOWN' as data_source
                FROM QP_DL_NAVDATA.QP_DL_FLOWNDATAPAX PX
                JOIN QP_DL_NAVDATA.QP_DL_InventoryLeg IL ON PX.InventoryLegID = IL.InventoryLegID
                {wherecondition}
                    
                UNION ALL
                
                SELECT
                    (PX.DiscountedBaseFare_Pro + PX.YQ_Pro + PX.RCS + PX.CUTE + PX.SGST + PX.UGST + PX.IGST + PX.CGST + PX.AirportCharges + PX.DUDF) AS Fare,
                    PX.FareBasis, PX.BookingDate, PX.Origin, PX.Destination, 'BOOKED' as data_source
                FROM QP_DL_NAVDATA.QP_DL_BOOKEDDATAPAX PX
                JOIN QP_DL_NAVDATA.QP_DL_InventoryLeg IL ON PX.InventoryLegID = IL.InventoryLegID
                {wherecondition}
            ) combined_bookings
            ORDER BY Fare ASC
            """
            
            combined_data = pd.read_sql(query, self.rm_rd_esb_conn)
            
            if not combined_data.empty:
                flown_count = len(combined_data[combined_data['data_source'] == 'FLOWN'])
                booked_count = len(combined_data[combined_data['data_source'] == 'BOOKED'])
            
            return combined_data
            
        except Exception as e:
            self.logger.error(f"Error collecting booking data: {str(e)}")
            self.failure_reason = f"Booking data collection failed: {str(e)}"
            raise

    # Core Ladder Generation Methods
    def get_historical_booking_data(self):
        """Get booking data with priority logic."""
        try:
            current_date = datetime.now().date()
            
            # Priority 1: Last 30 days
            start_date_30 = current_date - timedelta(days=30)
            combined_30 = self.collect_combined_booking_data(start_date_30, current_date)
            distinct_fares_30 = len(combined_30['Fare'].unique()) if not combined_30.empty else 0
            
            if distinct_fares_30 >= self.min_distinct_fares_required:
                return combined_30, "Last 30 days"
            
            # Priority 2: Last 60 days
            start_date_60 = current_date - timedelta(days=60)
            combined_60 = self.collect_combined_booking_data(start_date_60, current_date)
            distinct_fares_60 = len(combined_60['Fare'].unique()) if not combined_60.empty else 0
            
            if distinct_fares_60 >= self.min_distinct_fares_required:
                return combined_60, "Last 60 days"
            
            # Priority 3: Previous year same period
            prev_year_current = current_date.replace(year=current_date.year - 1)
            prev_year_start = prev_year_current + timedelta(days=30)
            combined_prev = self.collect_combined_booking_data(prev_year_start, prev_year_current)
            distinct_fares_prev = len(combined_prev['Fare'].unique()) if not combined_prev.empty else 0
            
            if distinct_fares_prev >= self.min_distinct_fares_required:
                return combined_prev, "Previous year same period"
            
            # Set failure reason with specific details
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
            
            # Check each fare against the NEXT fare and adjust the NEXT fare if needed
            for i in range(len(adjusted_fares) - 1):
                current_fare = adjusted_fares[i]
                next_fare = adjusted_fares[i + 1]
                min_threshold = threshold_func(current_fare)
                
                # If difference is less than threshold, adjust the NEXT fare upward
                if next_fare - current_fare < min_threshold:
                    adjusted_fares[i + 1] = current_fare + min_threshold
                    self.logger.debug(f"Adjusted fare {i+1}: {next_fare} -> {adjusted_fares[i + 1]}")
            
            return adjusted_fares
            
        except Exception as e:
            self.logger.error(f"Error applying minimum threshold: {str(e)}")
            self.failure_reason = f"Minimum threshold application failed: {str(e)}"
            raise

    # Helper Methods for Constraint Checking
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

    def _calculate_base_domestic_fare(self, gross_fare, origin, destination):
        """Calculate base domestic fare using reverse tax calculation."""
        try:
            if self.tax_master_dom_data is None or self.tax_master_dom_data.empty:
                return gross_fare
                
            tax_data = self.tax_master_dom_data[self.tax_master_dom_data['Code'] == origin]
            if tax_data.empty:
                return gross_fare
            
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
            
            adjusted_gross_fare = gross_fare
            
            # Special handling for DUF sectors
            if destination in self.constants.PRICING_DUF_SECTORS:
                adjusted_gross_fare -= float(self.constants.PRICING_DUF_SECTORS[destination])
            
            # GST exempt sectors
            if origin in self.constants.PRICING_GST_SECTORS or destination in self.constants.PRICING_GST_SECTORS:
                return adjusted_gross_fare - (asf + psf + udf_df + rcs + cute)
            else:
                gst_on_taxes = gst_rate * (rcs + cute)
                other_taxes = asf + psf + udf_df + cute
                return (adjusted_gross_fare - gst_on_taxes - rcs - other_taxes) / (1 + gst_rate)
            
        except Exception:
            return gross_fare

    def _calculate_base_xqp_fare(self, gross_fare, origin, destination):
        """Calculate base fare based on route."""
        if self.route == 'D':
            return self._calculate_base_domestic_fare(gross_fare, origin, destination)
        return gross_fare  # International routes use gross fare


    def _check_all_constraints(self, instances, yield_value, carrier):
        """Check constraints - only instances and yield (for domestic)."""
        try:
            min_instances_ok = instances >= 2
            carrier_ok = True  # Already filtered at SQL level
            
            if self.route == 'D':
                min_yield_ok = yield_value >= 1.5
                return min_instances_ok and min_yield_ok and carrier_ok
            else:
                return min_instances_ok and carrier_ok
            
        except Exception:
            return False

    def _find_valid_market_fare(self, market_data, fare_type):
        """Find valid market fare that satisfies constraints by iterating through individual fares."""
        try:
            if market_data.empty:
                return None
            
            # Filter for B2C channel and sort appropriately
            b2c_data = market_data[market_data['channel'] == 'B2C'].copy()
            
            if b2c_data.empty:
                self.logger.warning("No B2C market data available for fare validation")
                return None
            
            if fare_type == 'min':
                # For minimum: start with lowest fare and go higher until constraint satisfied
                sorted_data = b2c_data.sort_values('gross_fare', ascending=True)
            else:
                # For maximum: start with highest fare and go lower until constraint satisfied  
                sorted_data = b2c_data.sort_values('gross_fare', ascending=False)
            
            # Iterate through fares in order
            for _, row in sorted_data.iterrows():
                fare = row['gross_fare']
                carrier = row['outbound_airline_code']
                instances = row['instances']
                origin = row['source_airport']
                destination = row['destination_airport']
                
                if pd.isna(fare) or pd.isna(carrier):
                    continue
                
                try:
                    # Check only instances constraint
                    if instances >= 2:
                        return fare
                    else:
                        self.logger.debug(f"Market fare {fare:.2f} from {carrier} failed instances check: {instances} < 2")
                        
                except Exception as e:
                    self.logger.warning(f"Error checking constraints for market fare {fare}: {str(e)}")
                    continue
            
            # No valid fare found after checking all fares
            self.logger.warning(f"No valid {fare_type} market fare found that satisfies constraints")
            return None
            
        except Exception as e:
            self.logger.error(f"Error finding valid market fare: {str(e)}")
            return None

    def calculate_buffer_fares(self, core_fares, booking_data):
        """Calculate buffer fares f1-f3 and f62-f64."""
        try:
            if not core_fares or booking_data.empty:
                self.failure_reason = "Missing core fares or booking data for buffer calculation"
                return []
            
            if self.market_fares_data is None or self.market_fares_data.empty:
                self.failure_reason = "No market fares data available for buffer calculation"
                return []
            
            b2c_market_data = self.market_fares_data[self.market_fares_data['channel'] == 'B2C'].copy()
            
            if b2c_market_data.empty:
                self.failure_reason = "No B2C market fares available for buffer calculation"
                return []
            
            min_booking_fare = booking_data['Fare'].min()
            max_booking_fare = booking_data['Fare'].max()
            f4, f61 = core_fares[0], core_fares[-1]
            
            # Find valid market fares
            valid_min_market_fare = self._find_valid_market_fare(b2c_market_data, 'min')
            valid_max_market_fare = self._find_valid_market_fare(b2c_market_data, 'max')
            
            # STOP if no valid market fares found
            if valid_min_market_fare is None and valid_max_market_fare is None:
                self.failure_reason = "No valid market fares found that satisfy constraints for buffer calculation"
                return None
            
            # Calculate lower buffer (f1-f3)
            f2 = valid_min_market_fare if (valid_min_market_fare and valid_min_market_fare < min_booking_fare) else min_booking_fare
            threshold = max(0.02 * f2, 50) if self.route == 'D' else 0.02 * f2
            f1 = f2 - threshold
            f3 = (f2 + f4) / 2
            
            # Calculate upper buffer (f62-f64)
            f63 = valid_max_market_fare if (valid_max_market_fare and valid_max_market_fare > max_booking_fare) else max_booking_fare
            threshold = max(0.02 * f63, 50) if self.route == 'D' else 0.02 * f63
            f64 = f63 + threshold
            f62 = (f61 + f63) / 2
            
            complete_ladder = [f1, f2, f3] + core_fares + [f62, f63, f64]
            
            return complete_ladder
            
        except Exception as e:
            self.logger.error(f"Error calculating buffer fares: {str(e)}")
            self.failure_reason = f"Buffer fare calculation failed: {str(e)}"
            raise

    def generate_saver_ladder(self):
        """Main ladder generation method."""
        try:
            # Get historical booking data
            booking_data, data_period = self.get_historical_booking_data()
            if booking_data.empty:
                if not self.failure_reason:
                    self.failure_reason = "No booking data available for ladder generation"
                return None
            
            # Create and adjust fare bins
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

    # Utility and Output Methods
    def get_current_version(self):
        """Get current version information."""
        try:
            query = text("""
                SELECT * FROM QP_DW_RMALLOC.currentVersion
                WHERE tableName IN ('QP_DW_PRIAUT.qp_fares','QP_DW_PRIAUT.tax_mast_dom','QP_DW_PRIAUT.tax_mast_intl')
            """)
            self.currVersion = pd.read_sql(query, self.rm_rd_conn)
        except Exception as e:
            self.logger.error(f"Error getting version info: {str(e)}")
            self.failure_reason = f"Version info collection failed: {str(e)}"
            raise

    def _get_fare_point_types(self, total_points):
        """Generate fare point types."""
        return ['Lower_Buffer' if i <= 3 else 'Upper_Buffer' if i >= 62 else 'Core_Ladder' 
                for i in range(1, total_points + 1)]

    def save_ladder_to_csv(self, ladder_result):
        """Save ladder to database with current QP fares and RBD mapping."""
        try:
            if not ladder_result:
                return None, None
            
            run_id = str(uuid.uuid4())
            
            # Get current QP fares for the sector (64 RBDs ordered ascending)
            # Handle fare direction properly - need to check both F and T directions
            current_fares_query = f"""
            SELECT Fare_Amount as fare_amount, LEFT(Fare_Basis_Code, 2) as RBD
            FROM QP_DW_PRIAUT.qp_fares 
            WHERE Carrier_Code = 'QP'
            AND (
                (Fare_Direction = 'F' AND CONCAT(Origin, Destination) = '{self.sector}') OR
                (Fare_Direction = 'T' AND CONCAT(Destination, Origin) = '{self.sector}')
            )
            AND Rule_Nbr = {'1001' if self.route == 'D' else 'R101'}
            AND (OW_RT = 'O')
            AND (Footnote IS NULL OR Footnote = '')
            ORDER BY Fare_Amount ASC 
            LIMIT 64
            """
            
            try:
                current_qp_fares_df = pd.read_sql(current_fares_query, self.rm_rd_conn)
                if not current_qp_fares_df.empty:
                    current_qp_fares = current_qp_fares_df['fare_amount'].tolist()
                    current_rbds = current_qp_fares_df['RBD'].tolist()
                else:
                    current_qp_fares = [0] * 64
                    current_rbds = [f"RBD_{i+1}" for i in range(64)]
            except Exception as e:
                self.logger.warning(f"Could not fetch current QP fares: {str(e)}")
                current_qp_fares = [0] * 64
                current_rbds = [f"RBD_{i+1}" for i in range(64)]
            
            # Ensure we have exactly 64 current fares and RBDs
            while len(current_qp_fares) < 64:
                current_qp_fares.append(0)
                current_rbds.append(f"RBD_{len(current_rbds)+1}")
            current_qp_fares = current_qp_fares[:64]
            current_rbds = current_rbds[:64]
            
            # Get RBD names from rm_parameter_values (fallback if QP fares don't have 64 RBDs)
            rbd_query = """
            SELECT parameterValue 
            FROM QP_DW_PRIAUT.rm_parameter_values 
            WHERE parameterKey = 'RBD_SEQUENCE'
            """
            
            try:
                rbd_result = pd.read_sql(rbd_query, self.rm_rd_conn)
                if not rbd_result.empty:
                    # Use parameter RBDs if available, otherwise use RBDs from QP fares
                    parameter_rbds = rbd_result.iloc[0]['parameterValue'].split(',')
                    # Use current RBDs from QP fares if we have them, otherwise use parameter RBDs
                    if len([rbd for rbd in current_rbds if not rbd.startswith('RBD_')]) >= 10:
                        rbd_names = current_rbds
                    else:
                        rbd_names = parameter_rbds[:64] if len(parameter_rbds) >= 64 else current_rbds
                else:
                    rbd_names = current_rbds
            except Exception as e:
                self.logger.warning(f"Could not fetch RBD names: {str(e)}")
                rbd_names = current_rbds
            
            # Ensure we have exactly 64 RBDs
            while len(rbd_names) < 64:
                rbd_names.append(f"RBD_{len(rbd_names)+1}")
            rbd_names = rbd_names[:64]
            
            # Create the ladder DataFrame for database insert
            ladder_df = pd.DataFrame({
                'sector': [ladder_result['sector']] * 64,
                'route': [ladder_result['route']] * 64,
                'fare_point_number': range(1, 65),
                'rbd_name': rbd_names,
                'current_qp_fare': current_qp_fares,
                'new_ladder_fare': ladder_result['ladder_fares'],
                'data_period': [ladder_result['data_period']] * 64,
                'distinct_fares_count': [ladder_result['distinct_fares_count']] * 64,
                'run_id': [run_id] * 64,
                'created_at': [datetime.now(pytz.timezone('Asia/Kolkata'))] * 64,
                'analyst': [self.analystName] * 64
            })
            
            # Save to database
            table_name = 'ladder_gen_output'
            ladder_df.to_sql(table_name, self.rm_rd_conn, schema='QP_DW_PRIAUT', 
                        if_exists='append', index=False, method='multi')
            
            return f"QP_DW_PRIAUT.{table_name}", run_id
            
        except Exception as e:
            self.logger.error(f"Error saving ladder to database: {str(e)}")
            self.failure_reason = f"Failed to save ladder to database: {str(e)}"
            raise

    def save_run_audit(self, run_id, ladder_result, file_path, status='SUCCESS'):
        """Save audit information with enhanced failure tracking."""
        try:
            if status == 'SUCCESS' and not ladder_result:
                status = 'FAILED'
                
            total_points = len(ladder_result['ladder_fares']) if ladder_result else 0
            tax_table = 'QP_DW_PRIAUT.tax_mast_dom' if self.route == 'D' else 'QP_DW_PRIAUT.tax_mast_intl'
            
            # Get version info safely
            qp_fares_version = None
            tax_master_version = None
            
            try:
                if hasattr(self, 'currVersion') and self.currVersion is not None and not self.currVersion.empty:
                    qp_version_rows = self.currVersion[self.currVersion['tableName'] == 'QP_DW_PRIAUT.qp_fares']
                    tax_version_rows = self.currVersion[self.currVersion['tableName'] == tax_table]
                    
                    qp_fares_version = qp_version_rows['curr_version'].values[0] if not qp_version_rows.empty else None
                    tax_master_version = tax_version_rows['curr_version'].values[0] if not tax_version_rows.empty else None
            except Exception:
                pass
            
            audit_data = pd.DataFrame({
                'run_id': [run_id],
                'sector': [self.sector],
                'start_time': [getattr(self, 'start_time', datetime.now())],
                'end_time': [datetime.now(pytz.timezone('Asia/Kolkata'))],
                'total_ladder_points': [total_points],
                'core_ladder_points': [58 if total_points > 0 else 0],
                'buffer_ladder_points': [6 if total_points > 0 else 0],
                'min_ladder_fare': [round(min(ladder_result['ladder_fares']), 2) if ladder_result and ladder_result.get('ladder_fares') else None],
                'max_ladder_fare': [round(max(ladder_result['ladder_fares']), 2) if ladder_result and ladder_result.get('ladder_fares') else None],
                'data_period_used': [ladder_result['data_period'] if ladder_result else None],
                'total_booking_records': [ladder_result['total_booking_records'] if ladder_result else 0],
                'distinct_fares_count': [ladder_result['distinct_fares_count'] if ladder_result else 0],
                'route': [self.route],
                'qp_fares': [qp_fares_version],
                'tax_master': [tax_master_version],
                'analyst': [self.analystName],
                'status': [status],
                'failure_reason': [self.failure_reason if status == 'FAILED' else None]
            })
            
            audit_data.to_sql('ladder_generation_audits', self.rm_rd_conn, schema='QP_DW_PRIAUT', 
                            if_exists='append', index=False, method='multi')
            
            return 'QP_DW_PRIAUT.ladder_generation_audits'
            
        except Exception as e:
            self.logger.error(f"Error saving audit: {str(e)}")
            try:
                # Fallback minimal audit record
                minimal_audit = pd.DataFrame({
                    'run_id': [run_id],
                    'sector': [self.sector],
                    'start_time': [getattr(self, 'start_time', datetime.now())],
                    'end_time': [datetime.now(pytz.timezone('Asia/Kolkata'))],
                    'total_ladder_points': [0],
                    'core_ladder_points': [0],
                    'buffer_ladder_points': [0],
                    'min_ladder_fare': [None],
                    'max_ladder_fare': [None],
                    'data_period_used': [None],
                    'total_booking_records': [0],
                    'distinct_fares_count': [0],
                    'route': [self.route],
                    'qp_fares': [None],
                    'tax_master': [None],
                    'analyst': [self.analystName],
                    'status': ['FAILED'],
                    'failure_reason': [f"Audit save failed: {str(e)}. Original failure: {self.failure_reason}"]
                })
                
                minimal_audit.to_sql('ladder_generation_audits', self.rm_rd_conn, schema='QP_DW_PRIAUT', 
                                if_exists='append', index=False, method='multi')
                return 'QP_DW_PRIAUT.ladder_generation_audits'
            except:
                self.logger.error("Failed to save even minimal audit record")
                return None

    def collect_all_data(self):
        """Collect all required data efficiently."""
        try:
            success_count = 0
            total_operations = 4
            
            # Track individual collection operations
            try:
                self.collect_market_fares()
                success_count += 1
            except Exception as e:
                self.logger.error(f"Market fares collection failed: {str(e)}")
                if not self.failure_reason:
                    self.failure_reason = f"Market fares collection failed: {str(e)}"
            
            try:
                self.collect_tax_master_data()
                success_count += 1
            except Exception as e:
                self.logger.error(f"Tax master data collection failed: {str(e)}")
                if not self.failure_reason:
                    self.failure_reason = f"Tax master data collection failed: {str(e)}"
            
            try:
                self.collect_city_pair_data()
                success_count += 1
            except Exception as e:
                self.logger.error(f"City pair data collection failed: {str(e)}")
                # City pair data is not critical, so don't set failure_reason
                
            try:
                self.collect_booked_data()
                success_count += 1
            except Exception as e:
                self.logger.error(f"Booked data collection failed: {str(e)}")
                # This is handled separately in get_historical_booking_data()
            
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
            
            return success_count >= 2  # At least market fares and tax master should succeed
            
        except Exception as e:
            self.logger.error(f"Error in data collection: {str(e)}")
            if not self.failure_reason:
                self.failure_reason = f"Data collection process failed: {str(e)}"
            raise

    def run(self):
        """Main execution method."""
        run_id = str(uuid.uuid4())
        
        try:
            self.start_time = datetime.now(pytz.timezone('Asia/Kolkata'))
            
            # Get version info and collect data
            try:
                self.get_current_version()
            except Exception as e:
                self.logger.warning(f"Version info collection failed: {str(e)}")
                # Continue without version info
                
            data_collection_success = self.collect_all_data()
            if not data_collection_success:
                # Save failure audit
                self.save_run_audit(run_id, None, None, 'FAILED')
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
                # Save failure audit
                self.save_run_audit(run_id, None, None, 'FAILED')
                return {
                    'ladder_result': None,
                    'file_path': None,
                    'run_id': run_id,
                    'status': 'FAILED',
                    'reason': self.failure_reason or 'Ladder generation failed'
                }
            
            # Save results
            try:
                file_path, save_run_id = self.save_ladder_to_csv(ladder_result)
                # Use the run_id from save operation if available
                if save_run_id:
                    run_id = save_run_id
                self.save_run_audit(run_id, ladder_result, file_path, 'SUCCESS')
            except Exception as e:
                self.logger.error(f"Error saving results: {str(e)}")
                self.failure_reason = f"Failed to save results: {str(e)}"
                self.save_run_audit(run_id, ladder_result, None, 'FAILED')
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
                'run_id': run_id,
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
            
            # Ensure failure reason is set
            if not self.failure_reason:
                self.failure_reason = f"Unexpected error in main execution: {str(e)}"
            
            # Save failure audit
            try:
                self.save_run_audit(run_id, None, None, 'FAILED')
            except Exception as audit_error:
                self.logger.error(f"Failed to save failure audit: {str(audit_error)}")
            
            return {
                'ladder_result': None,
                'file_path': None,
                'run_id': run_id,
                'status': 'FAILED',
                'reason': self.failure_reason,
                'error_details': str(e)
            }