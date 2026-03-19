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
from helpers.nav_db_helpers.action_nav_helper import ActionNavHelper
from helpers.pa_db_helpers.action_pa_helper import ActionPAHelper

class ActionSectors:
    def __init__(self, logger, params, overrides, context):
        """Initialize with required connections and constants."""
        self.logger = logger
        self.db = params['mongo']
        self.db_fares = params['mongofares']
        self.nav_conn = params['navconn']
        self.nav_db_name = configsettings.NAV_DB_NAME
        self.nav_db_dw_name = configsettings.NAV_DB_DW_NAME
        self.nav_db_wb_name = configsettings.NAV_DB_WB_NAME
        self.constants = ConstantsUtils(self.db)
        self.MARKET_FARE_MIN_YIELD = self.constants.MARKET_FARE_MIN_YIELD
        self.pa_helper = ActionPAHelper(self.db, self.db_fares, logger)
        self.nav_helper = ActionNavHelper(self.nav_conn, self.nav_db_name, self.nav_db_dw_name, self.nav_db_wb_name, logger)
        self.route = overrides['route']
        self.analystName = overrides['analyst']
                
        # Initialize data containers
        self.qp_fares_data = None
        self.tax_master_dom_data = None
        self.tax_master_intl_data = None
        self.market_fares_data = None
        self.booked_data = None
        self.city_pair_data = None
        self.active_sectors = None
        self.currentVersion = None

    def collect_all_data(self, booking_days=30):
        """Collect all required data for actionable sectors analysis based on route."""
        try:
            
            # Collect QP fares based on route
            self.active_sectors = self.nav_helper.collect_active_sectors()
            self.qp_fares_data = self.pa_helper.collect_qp_fares(self.route, self.active_sectors)
            self.booked_data = self.nav_helper.collect_booked_data(booking_days)
            self.city_pair_data = self.nav_helper.collect_city_pair_data()
            
            # Collect tax master data based on route
            if self.route == 'D':
                self.tax_master_dom_data = self.pa_helper.collect_tax_master_domestic()
            elif self.route == 'I':
                self.tax_master_intl_data = self.pa_helper.collect_tax_master_international()
            
            # Collect market fares based on route
            self.market_fares_data = self.pa_helper.collect_market_fares(self.route)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error in data collection: {str(e)}")
            self.logger.error(traceback.format_exc())
            raise

    def generate_actionable_sectors(self):
        """Generate final actionable sectors output with optimized XQP data."""
        try:
            # No need for RBD mappings anymore since we get min/max directly from fare amounts
            # Get unique sectors from QP fares
            unique_sectors = self.qp_fares_data[['SECTOR', 'Org', 'Dst']].drop_duplicates()
            
            final_results = []
            
            for _, sector_row in unique_sectors.iterrows():
                sector = sector_row['SECTOR']
                origin = sector_row['Org']
                destination = sector_row['Dst']
                
                sector_qp_fares = self.qp_fares_data[self.qp_fares_data['SECTOR'] == sector]
                
                # Process B2B and B2C separately using pre-aggregated XQP data
                for channel in ['B2B', 'B2C']:
                    actionable_records = self._evaluate_channel_actions(
                        sector, origin, destination, channel, 
                        sector_qp_fares, self.market_fares_data, {}  # Empty dict since we don't need RBD mappings
                    )
                    
                    if actionable_records:
                        final_results.extend(actionable_records)
            
            # Convert to DataFrame
            final_df = pd.DataFrame(final_results)
            
            return final_df
            
        except Exception as e:
            self.logger.error(f"Error generating actionable sectors: {str(e)}")
            raise

    def _evaluate_channel_actions(self, sector, origin, destination, channel, sector_qp_fares, sector_xqp_fares, rbd_mappings):
        """Evaluate actions for a specific channel (B2B/B2C) using pre-aggregated XQP data."""
        try:
            # Filter XQP fares by channel (data is already pre-aggregated)
            channel_xqp_data = sector_xqp_fares[
                (sector_xqp_fares['source_airport'] == origin) &
                (sector_xqp_fares['destination_airport'] == destination) &
                (sector_xqp_fares['channel'] == channel)
            ]
            
            if channel_xqp_data.empty:
                self.logger.debug(f"No market fare data found for {sector} {channel}")
                return []
            
            # Get pre-calculated min/max fares, instances, and carriers
            xqp_record = channel_xqp_data.iloc[0]
            xqp_min_fare = float(xqp_record['min_fare'])
            xqp_max_fare = float(xqp_record['max_fare'])
            min_instances = int(xqp_record['min_instances'])
            max_instances = int(xqp_record['max_instances'])
            min_carrier = xqp_record['min_carrier']
            max_carrier = xqp_record['max_carrier']
            
            self.logger.debug(f"Market data for {sector} {channel}: min={xqp_min_fare} (instances={min_instances}), max={xqp_max_fare} (instances={max_instances})")
            
            # Get QP min and max fares based on fare amounts
            qp_min_fare, qp_max_fare, min_rbd, max_rbd = self._get_qp_min_max_fares(sector_qp_fares, origin, destination, channel)
            
            self.logger.debug(f"QP fares for {sector} {channel}: min_RBD={min_rbd}({qp_min_fare}), max_RBD={max_rbd}({qp_max_fare})")
            
            if qp_min_fare is None and qp_max_fare is None:
                self.logger.debug(f"No QP fares found for {sector} {channel}")
                return []
            
            # Evaluate actions based on comparison logic
            max_action_valid = qp_max_fare is not None and xqp_max_fare > qp_max_fare
            min_action_valid = qp_min_fare is not None and xqp_min_fare < qp_min_fare
            
            self.logger.debug(f"Action evaluation for {sector} {channel}: max_valid={max_action_valid}, min_valid={min_action_valid}")
            
            results = []
            
            # Create Max Action record if valid
            if max_action_valid:
                max_action_record = self._create_action_record(
                    sector, origin, destination, channel, "Max_Action",
                    qp_max_fare, xqp_max_fare, max_carrier, max_rbd,
                    qp_min_fare, qp_max_fare, min_rbd, max_rbd, max_instances
                )
                if max_action_record:
                    results.append(max_action_record)
            
            # Create Min Action record if valid
            if min_action_valid:
                min_action_record = self._create_action_record(
                    sector, origin, destination, channel, "Min_Action",
                    qp_min_fare, xqp_min_fare, min_carrier, min_rbd,
                    qp_min_fare, qp_max_fare, min_rbd, max_rbd, min_instances
                )
                if min_action_record:
                    results.append(min_action_record)
            
            if not results:
                self.logger.debug(f"No actionable condition met for {sector} {channel}")
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error evaluating channel actions: {str(e)}")
            return []

    def _create_action_record(self, sector, origin, destination, channel, action_type, 
                            qp_fare, xqp_fare, xqp_carrier, action_rbd,
                            qp_min_fare, qp_max_fare, min_rbd, max_rbd, instances):
        """Create a single action record for the given parameters."""
        try:
            # Calculate fare difference based on action type
            if action_type == "Max_Action":
                fare_diff = xqp_fare - qp_fare
            else:  # Min_Action
                fare_diff = qp_fare - xqp_fare
            
            # Calculate other metrics
            plf = self._calculate_forward_plf(origin, destination)
            distance = self._get_distance(origin, destination)
            
            # Calculate yield and base fare only for domestic routes
            if self.route == 'D':
                base_xqp_fare = self._calculate_base_xqp_fare(xqp_fare, origin, destination)
                yield_value = base_xqp_fare / distance if distance > 0 else 0
            elif self.route == 'I':
                # For international routes, base fare calculation and yield are not required
                base_xqp_fare = xqp_fare  # Use gross fare as base fare
                yield_value = 0  # Not applicable for international
            else:
                raise ValueError(f"Invalid route: {self.route}")
            
            # Check constraints
            constraints_satisfied = self._check_all_constraints(
                instances, fare_diff, yield_value, plf, xqp_carrier
            )
            
            self.logger.debug(f"Constraints for {sector} {channel} {action_type}: instances={instances}≥2, fare_diff={fare_diff}≥1, plf={plf}<0.5, satisfied={constraints_satisfied}")
            
            # Build result record with route-specific fields
            result = {
                'Sector': sector,
                'Channel': channel,
                'Carrier': xqp_carrier,
                'Min_QP': qp_min_fare if qp_min_fare else None,
                'Max_QP': qp_max_fare if qp_max_fare else None,
                'Min_QP_RBD': min_rbd if min_rbd else 'N/A',
                'Max_QP_RBD': max_rbd if max_rbd else 'N/A',
                'Action_RBD': action_rbd,
                'XQP_Fare': round(xqp_fare, 2),
                'Fare_difference': round(fare_diff, 2),
                'PLF': f"{plf:.0%}",
                'Instances': instances,
                'Base_XQP_Fare': round(base_xqp_fare, 2),
                'Distance': int(distance),
                'New_Fare_Point': round(yield_value, 2) if self.route == 'D' else None,
                'Yield': round(yield_value, 2) if self.route == 'D' else None,
                'Action_required': action_type,
                'Req_Min_Instances': 2,
                'Req_Min_FD': 1,
                'Req_Min_Yield': self.MARKET_FARE_MIN_YIELD if self.route == 'D' else None,
                'Req_Discared_Carrier': '[9I, IC, S9, S5]',
                'Req_Forward_PLF': 0.5,
                'Constraint': 'Satisfied' if constraints_satisfied else 'Will not be satisfied'
            }
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error creating action record: {str(e)}")
            return None

    def _get_qp_min_max_fares(self, sector_qp_fares, origin, destination, channel):
        """Get QP min and max gross fares for specific channel based on fare amounts."""
        try:
            # Filter by channel
            channel_fares = sector_qp_fares[sector_qp_fares['channel'] == channel]
            
            if channel_fares.empty:
                self.logger.debug(f"No QP fares found for {channel} channel in sector {origin}-{destination}")
                return None, None, None, None
            
            # Get min and max fare amounts
            min_fare_amount = channel_fares['TOTAL'].min()
            max_fare_amount = channel_fares['TOTAL'].max()
            
            # Get the records for min and max fares
            min_fare_record = channel_fares[channel_fares['TOTAL'] == min_fare_amount].iloc[0]
            max_fare_record = channel_fares[channel_fares['TOTAL'] == max_fare_amount].iloc[0]
            
            # Calculate gross fares for min and max
            min_base_fare = float(min_fare_record['TOTAL'])
            max_base_fare = float(max_fare_record['TOTAL'])
            min_currency = min_fare_record['currency']
            max_currency = max_fare_record['currency']
            min_rbd = min_fare_record['RBD']
            max_rbd = max_fare_record['RBD']
            
            self.logger.debug(f"Min fare for {channel}: RBD={min_rbd}, Amount={min_base_fare}, Currency={min_currency}")
            self.logger.debug(f"Max fare for {channel}: RBD={max_rbd}, Amount={max_base_fare}, Currency={max_currency}")
            
            # Calculate gross fares
            if self.route == 'D':
                min_gross_fare = self._calculate_domestic_gross_fare(min_base_fare, origin, destination)
                max_gross_fare = self._calculate_domestic_gross_fare(max_base_fare, origin, destination)
            elif self.route == 'I':
                min_gross_fare = self._calculate_international_gross_fare(min_base_fare, origin, destination, min_currency)
                max_gross_fare = self._calculate_international_gross_fare(max_base_fare, origin, destination, max_currency)
            else:
                raise ValueError(f"Invalid route: {self.route}")
            
            return min_gross_fare, max_gross_fare, min_rbd, max_rbd
            
        except Exception as e:
            self.logger.error(f"Error getting QP min/max fares for {origin} {channel}: {str(e)}")
            return None, None, None, None

    def _calculate_domestic_gross_fare(self, base_fare, origin, destination):
        """Calculate domestic gross fare using the standard formula: Gross Fare = Base Fare + (Base Fare x GST) + (GSTx(RCS+CUTE)) + Other Taxes"""
        try:
            # Get tax data and calculate gross fare
            tax_data = self.tax_master_dom_data[self.tax_master_dom_data['Code'] == origin]
            
            if tax_data.empty:
                self.logger.warning(f"No tax data found for origin: {origin}")
            
            tax_row = tax_data.iloc[0]
            
            # Extract tax values - GST is the Goods and Services Tax rate (e.g., 18% stored as 18 in database)
            try:
                gst_rate = float(tax_row['GST']) / 100 if pd.notna(tax_row['GST']) else 0.0  # Convert percentage to decimal
                rcs = float(tax_row['RCS']) if pd.notna(tax_row['RCS']) else 0.0
                cute = float(tax_row['CUTE']) if pd.notna(tax_row['CUTE']) else 0.0
                asf = float(tax_row['ASF']) if pd.notna(tax_row['ASF']) else 0.0
                psf = float(tax_row['PSF']) if pd.notna(tax_row['PSF']) else 0.0
                udf_df = float(tax_row['UDF_DF']) if pd.notna(tax_row['UDF_DF']) else 0.0
                
            except (KeyError, ValueError, TypeError) as e:
                self.logger.error(f"Error extracting tax values for {origin}: {str(e)}")
            
            # Apply special handling for RCS sectors - set RCS to 0
            if destination in self.constants.PRICING_RCS_SECTORS:
                self.logger.debug(f"RCS sector detected: {destination}, setting RCS to 0")
                rcs = 0.0
            
            # Apply GST exemption for specific sectors - if origin OR destination is in GST exempt list, set GST to 0
            original_gst_rate = gst_rate  # Keep original for base fare calculation
            if origin in self.constants.PRICING_GST_SECTORS or destination in self.constants.PRICING_GST_SECTORS:
                self.logger.debug(f"GST exempt sector detected: {origin}-{destination}, setting GST to 0 for gross fare calculation")
                gst_rate = 0.0
            
            # Calculate using the formula: Gross Fare = Base Fare + (Base Fare × GST) + (GST×(RCS+CUTE)) + Other Taxes
            component1 = base_fare                           # Base Fare
            component2 = base_fare * gst_rate               # Base Fare × GST (0 if GST exempt)
            component3 = gst_rate * (rcs + cute)           # GST × (RCS + CUTE) (0 if GST exempt)
            component4 = asf + psf + udf_df + rcs + cute   # Other Taxes (includes RCS and CUTE directly)
            
            gross_fare = component1 + component2 + component3 + component4
            
            # Special handling for DUF sectors - add DUF value on top of everything
            if destination in self.constants.PRICING_DUF_SECTORS:
                duf_value = float(self.constants.PRICING_DUF_SECTORS[destination])
                self.logger.debug(f"DUF sector detected: {destination}, adding DUF: {duf_value}")
                gross_fare += duf_value
            
            self.logger.debug(f"Calculated domestic gross fare for {origin}-{destination}: {gross_fare} (GST rate used: {gst_rate*100}%, Original GST: {original_gst_rate*100}%)")
            return gross_fare
            
        except Exception as e:
            self.logger.error(f"Error calculating domestic gross fare: {str(e)}")
            return base_fare

    def _calculate_international_gross_fare(self, base_fare, origin, destination, currency):
        """Calculate international gross fare using the standard formula: Base Fare + (Base Fare × GST) + (GST×(RCS+CUTE)) + Other Taxes"""
        try:
            # Get international tax data based on sectors and currency
            sector_key = f"{origin}{destination}"
            tax_data = self.tax_master_intl_data[
                (self.tax_master_intl_data['Sectors'] == sector_key) &
                (self.tax_master_intl_data['Currency'] == currency)
            ]
            
            if tax_data.empty:
                # Try without currency filter if no exact match found
                tax_data = self.tax_master_intl_data[
                    self.tax_master_intl_data['Sectors'] == sector_key
                ]
                if tax_data.empty:
                    self.logger.warning(f"No international tax data found for sector: {sector_key}, currency: {currency}")
                else:
                    self.logger.warning(f"Using tax data without currency match for sector: {sector_key}")
            
            tax_row = tax_data.iloc[0]
            
            # Extract international tax values - GST is already in decimal format (e.g., 0.05 = 5%)
            try:
                gst_rate = float(tax_row['GST']) if pd.notna(tax_row['GST']) else 0.0  # Already in decimal format
                rcs = float(tax_row['RCS']) if pd.notna(tax_row['RCS']) else 0.0
                cute = float(tax_row['CUTE']) if pd.notna(tax_row['CUTE']) else 0.0
                
                # Other taxes for international
                asf = float(tax_row['ASF']) if pd.notna(tax_row['ASF']) else 0.0
                psf_adf = float(tax_row['PSF_ADF']) if pd.notna(tax_row['PSF_ADF']) else 0.0
                udf = float(tax_row['UDF']) if pd.notna(tax_row['UDF']) else 0.0
                psc_pz = float(tax_row['PSC_PZ']) if pd.notna(tax_row['PSC_PZ']) else 0.0
                rcs_cute = float(tax_row['RCS_CUTE']) if pd.notna(tax_row['RCS_CUTE']) else 0.0
                
                # Additional tax fields for international
                tax1 = float(tax_row['Tax1']) if pd.notna(tax_row['Tax1']) else 0.0
                tax2 = float(tax_row['Tax2']) if pd.notna(tax_row['Tax2']) else 0.0
                tax3 = float(tax_row['Tax3']) if pd.notna(tax_row['Tax3']) else 0.0
                tax4 = float(tax_row['Tax4']) if pd.notna(tax_row['Tax4']) else 0.0
                tax5 = float(tax_row['Tax5']) if pd.notna(tax_row['Tax5']) else 0.0
                
            except (KeyError, ValueError, TypeError) as e:
                self.logger.error(f"Error extracting international tax values for {sector_key}: {str(e)}")
            
            # Apply GST exemption for specific sectors - if origin OR destination is in GST exempt list, set GST to 0
            original_gst_rate = gst_rate  # Keep original for base fare calculation
            if origin in self.constants.PRICING_GST_SECTORS or destination in self.constants.PRICING_GST_SECTORS:
                self.logger.debug(f"International GST exempt sector detected: {origin}-{destination}, setting GST to 0 for gross fare calculation")
                gst_rate = 0.0
            
            # Calculate using the formula: Gross Fare = Base Fare + (Base Fare × GST) + (GST×(RCS+CUTE)) + Other Taxes
            component1 = base_fare                                                                      # Base Fare
            component2 = base_fare * gst_rate                                                          # Base Fare × GST (0 if GST exempt)
            component3 = gst_rate * (rcs + cute)                                                       # GST × (RCS + CUTE) (0 if GST exempt)
            component4 = asf + psf_adf + udf + psc_pz + rcs_cute + rcs + cute + tax1 + tax2 + tax3 + tax4 + tax5  # Other Taxes
            
            gross_fare = component1 + component2 + component3 + component4
            
            self.logger.debug(f"Calculated international gross fare for {origin}-{destination} ({currency}): {gross_fare} (GST rate used: {gst_rate*100}%, Original GST: {original_gst_rate*100}%)")
            return gross_fare
            
        except Exception as e:
            self.logger.error(f"Error calculating international gross fare: {str(e)}")

    def _calculate_forward_plf(self, origin, destination):
        """Calculate average forward PLF for next 30 days across all flights in sector."""
        try:
            sector_bookings = self.booked_data[
                (self.booked_data['Origin'] == origin) &
                (self.booked_data['Destination'] == destination)
            ]
            
            if sector_bookings.empty:
                return 0.0
            
            # Calculate PLF for each flight
            sector_bookings = sector_bookings.copy()
            sector_bookings['flight_plf'] = sector_bookings.apply(
                lambda row: row['bookedPax'] / row['capacity'] if row['capacity'] > 0 else 0.0, 
                axis=1
            )
            
            # Calculate average PLF across all flights in the sector
            average_plf = sector_bookings['flight_plf'].mean()
            
            self.logger.debug(f"Sector {origin}-{destination}: {len(sector_bookings)} flights, avg PLF: {average_plf:.2%}")
            
            return average_plf
            
        except Exception as e:
            self.logger.error(f"Error calculating forward PLF: {str(e)}")

    def _count_xqp_instances(self, xqp_data, target_fare):
        """Count instances of exact XQP fare - now using pre-aggregated instances count."""
        try:
            # With pre-aggregated data, we already have the instances count
            # This method is no longer needed but kept for compatibility
            return int(xqp_data.iloc[0]['instances']) if not xqp_data.empty else 0
            
        except Exception as e:
            self.logger.error(f"Error counting XQP instances: {str(e)}")
            return 0

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

    def _get_distance(self, origin, destination):
        """Get distance for sector from city pair data."""
        try:
            distance_data = self.city_pair_data[
                (self.city_pair_data['DepartureStation'] == origin) &
                (self.city_pair_data['ArrivalStation'] == destination)
            ]
            
            if distance_data.empty:
                return 0
            
            return float(distance_data.iloc[0]['ActualDistance'])
            
        except Exception as e:
            self.logger.error(f"Error getting distance: {str(e)}")


    def _check_all_constraints(self, instances, fare_diff, yield_value, plf, carrier):
        """Check all constraints and return True if all satisfied. Yield constraint not applicable for international routes."""
        try:
            # Common constraint checks
            min_instances_ok = instances >= 2
            min_fare_diff_ok = abs(fare_diff) >= 1
            forward_plf_ok = plf < 0.5
            # Note: carrier exclusion already applied at SQL level, so this check is always True
            carrier_ok = True  # All excluded carriers already filtered out
            
            if self.route == 'D':
                # Domestic: include yield constraint
                min_yield_ok = yield_value >= self.MARKET_FARE_MIN_YIELD
                all_satisfied = (min_instances_ok and min_fare_diff_ok and 
                               min_yield_ok and forward_plf_ok and carrier_ok)
                
                                
            elif self.route == 'I':
                # International: exclude yield constraint
                all_satisfied = (min_instances_ok and min_fare_diff_ok and 
                               forward_plf_ok and carrier_ok)
                
            else:
                raise ValueError(f"Invalid route: {self.route}")
            
            return all_satisfied
            
        except Exception as e:
            self.logger.error(f"Error checking constraints: {str(e)}")
            return False

    def run(self):
        """Main method to run complete actionable sectors analysis."""
        try:
            self.start_time = datetime.now(pytz.timezone('Asia/Kolkata'))
            
            # Get current version information
            self.currentVersion = self.pa_helper.get_current_version()
            
            # Create initial audit record with IN_PROGRESS status
            self.run_id = str(uuid.uuid4())
            self.pa_helper.save_run_audit(self.currentVersion, self.route, self.analystName, self.start_time, self.run_id, None, is_initial=True)
            
            # Collect all data
            self.collect_all_data()
            
            # Generate actionable sectors
            actionable_sectors = self.generate_actionable_sectors()
            
            # Save to database
            table_name = self.pa_helper.save_actionable_sectors_to_database(self.route, self.analystName, actionable_sectors, self.run_id)
            
            # Update audit record with final results
            self.pa_helper.save_run_audit(self.currentVersion, self.route, self.analystName, self.start_time, self.run_id, actionable_sectors, status='SUCCESS')
            
            # Log summary
            route_desc = "Domestic" if self.route == 'D' else "International"
            
            return {
                'actionable_sectors': actionable_sectors,
                'table_name': table_name,
                'run_id': self.run_id,
                'route': self.route,
                'route_description': route_desc,
                'summary': {
                    'total_actionable_records': len(actionable_sectors),
                    'max_actions': len(actionable_sectors[actionable_sectors['Action_required'] == 'Max_Action']),
                    'min_actions': len(actionable_sectors[actionable_sectors['Action_required'] == 'Min_Action']),
                    'constraints_satisfied': len(actionable_sectors[actionable_sectors['Constraint'] == 'Satisfied'])
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error in run method: {str(e)}")
            # Update audit with FAILED status
            self.pa_helper.save_run_audit(self.currentVersion, self.route, self.analystName, self.start_time, self.run_id, None, status='FAILED', is_initial=False)
            raise