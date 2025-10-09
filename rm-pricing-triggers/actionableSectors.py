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


class ActionSectors:
    def __init__(self, logger, params, overrides, context):
        """Initialize with required connections and constants."""
        self.logger = logger
        self.rm_rd_conn = params['wrconn']
        self.nav_conn = params['navconn']
        self.nav_db_name = configsettings.NAV_DB_NAME
        self.nav_db_dw_name = configsettings.NAV_DB_DW_NAME
        self.nav_db_wb_name = configsettings.NAV_DB_WB_NAME
        self.constants = ConstantsUtils(self.rm_rd_conn)
        self.route = overrides['route']
        self.analystName = overrides['analyst']
                
        # Initialize data containers
        self.qp_fares_data = None
        self.tax_master_dom_data = None
        self.tax_master_intl_data = None
        self.market_fares_data = None
        self.booked_data = None
        self.city_pair_data = None
        self.rm_parameters = None
        self.active_sectors = None

    def collect_qp_fares(self):
        """Collect QP fares data from rm_rd_conn based on route (domestic or international) handling both fare directions.
        Filters data to only include active sectors for domestic routes only."""
        try:
            if(self.route == 'D'):
                query = f"""
                SELECT 
                    CASE 
                        WHEN qf.Fare_Direction = 'F' THEN CONCAT(qf.Origin, qf.Destination)
                        WHEN qf.Fare_Direction = 'T' THEN CONCAT(qf.Destination, qf.Origin)
                        ELSE CONCAT(qf.Origin, qf.Destination)
                    END as SECTOR,
                    CASE 
                        WHEN qf.Fare_Direction = 'F' THEN qf.Origin
                        WHEN qf.Fare_Direction = 'T' THEN qf.Destination
                        ELSE qf.Origin
                    END as Org,
                    CASE 
                        WHEN qf.Fare_Direction = 'F' THEN qf.Destination
                        WHEN qf.Fare_Direction = 'T' THEN qf.Origin
                        ELSE qf.Destination
                    END as Dst,
                    qf.Routing as route,
                    LEFT(qf.Fare_Basis_Code, 2) as RBD,
                    qf.Fare_Basis_Code as Fare_Basis_Code,
                    qf.Fare_Amount as TOTAL,
                    qf.Currency as currency,
                    qf.Fare_Direction as fare_direction,
                    qf.Rule_Nbr as rule_number,
                    CASE 
                        WHEN qf.Rule_Nbr = '1001' THEN 'B2C'
                        WHEN qf.Rule_Nbr = '2001' THEN 'B2B'
                        ELSE 'Unknown'
                    END as channel
                FROM QP_DW_PRIAUT.qp_fares qf
                WHERE qf.Carrier_Code = 'QP'
                AND qf.Rule_Nbr in ('1001','2001')
                AND qf.Fare_Direction IN ('F', 'T')
                AND (qf.OW_RT = 'O' OR qf.OW_RT = 'C')
                AND (qf.Footnote is null OR qf.Footnote = "")
                AND (
                    EXISTS (
                        SELECT 1 FROM QP_DW_RMALLOC.config_station_master csm_org 
                        WHERE qf.Origin = csm_org.station AND csm_org.route = 'D'
                    )
                    OR 
                    EXISTS (
                        SELECT 1 FROM QP_DW_RMALLOC.config_station_master csm_dest 
                        WHERE qf.Destination = csm_dest.station AND csm_dest.route = 'D'
                    )
                )
                """
            elif(self.route == 'I'):
                query = f"""
                SELECT 
                    CASE 
                        WHEN qf.Fare_Direction = 'F' THEN CONCAT(qf.Origin, qf.Destination)
                        WHEN qf.Fare_Direction = 'T' THEN CONCAT(qf.Destination, qf.Origin)
                        ELSE CONCAT(qf.Origin, qf.Destination)
                    END as SECTOR,
                    CASE 
                        WHEN qf.Fare_Direction = 'F' THEN qf.Origin
                        WHEN qf.Fare_Direction = 'T' THEN qf.Destination
                        ELSE qf.Origin
                    END as Org,
                    CASE 
                        WHEN qf.Fare_Direction = 'F' THEN qf.Destination
                        WHEN qf.Fare_Direction = 'T' THEN qf.Origin
                        ELSE qf.Destination
                    END as Dst,
                    qf.Routing as route,
                    LEFT(qf.Fare_Basis_Code, 2) as RBD,
                    qf.Fare_Basis_Code as Fare_Basis_Code,
                    qf.Fare_Amount as TOTAL,
                    qf.Currency as currency,
                    qf.Fare_Direction as fare_direction,
                    qf.Rule_Nbr as rule_number,
                    CASE 
                        WHEN qf.Rule_Nbr IN ('M101', 'M201') THEN 'B2B'
                        WHEN qf.Rule_Nbr IN ('R101', 'R201') THEN 'B2C'
                        ELSE 'Unknown'
                    END as channel
                FROM QP_DW_PRIAUT.qp_fares qf
                WHERE qf.Carrier_Code = 'QP'
                AND qf.Rule_Nbr in ('M101','R101','M201','R201')
                AND qf.Fare_Direction IN ('F', 'T')
                AND (qf.OW_RT = 'O' OR qf.OW_RT = 'C')
                AND (qf.Footnote is null OR qf.Footnote = "")
                AND (
                    EXISTS (
                        SELECT 1 FROM QP_DW_RMALLOC.config_station_master csm_org 
                        WHERE qf.Origin = csm_org.station AND csm_org.route = 'I'
                    )
                    OR 
                    EXISTS (
                        SELECT 1 FROM QP_DW_RMALLOC.config_station_master csm_dest 
                        WHERE qf.Destination = csm_dest.station AND csm_dest.route = 'I'
                    )
                )
                """
            else:
                raise ValueError(f"Invalid Route Type: {self.route}")
            
            # Fetch all QP fares data first
            all_qp_fares = pd.read_sql(query, self.rm_rd_conn)
            
            # Filter by active sectors ONLY for domestic routes
            if self.route == 'D' and hasattr(self, 'active_sectors') and self.active_sectors is not None and not self.active_sectors.empty:
                # Create active sectors set for faster lookup (both directions)
                active_sectors_set = set()
                for _, row in self.active_sectors.iterrows():
                    # Add both directions of the sector
                    sector_forward = f"{row['DepartureStation']}{row['ArrivalStation']}"
                    active_sectors_set.add(sector_forward)
                
                # Filter QP fares to only include active sectors
                self.qp_fares_data = all_qp_fares[all_qp_fares['SECTOR'].isin(active_sectors_set)].copy()
                self.logger.info(f"Filtered domestic QP fares to {len(self.qp_fares_data)} records based on active sectors")
                
            else:
                # For international routes or when active sectors data is not available, use all QP fares data
                if self.route == 'I':
                    self.logger.info("International route detected. Using all QP fares data without active sector filtering.")
                else:
                    self.logger.info("Active sectors data not available for domestic route. Using all QP fares data.")
                self.qp_fares_data = all_qp_fares
            
            return self.qp_fares_data
            
        except Exception as e:
            self.logger.error(f"Error collecting QP fares data: {str(e)}")
            raise

    def collect_tax_master_domestic(self):
        """Collect domestic tax master data from rm_rd_conn."""
        try:
            query = """
            SELECT 
                Station,
                Code,
                ASF,
                CUTE,
                PSF,
                RCS,
                UDF_DF,
                GST
            FROM QP_DW_PRIAUT.tax_mast_dom
            """
            
            self.tax_master_dom_data = pd.read_sql(query, self.rm_rd_conn)
            
            return self.tax_master_dom_data
            
        except Exception as e:
            self.logger.error(f"Error collecting domestic tax master data: {str(e)}")
            raise

    def collect_tax_master_international(self):
        """Collect international tax master data from rm_rd_conn."""
        try:
            query = """
            SELECT 
                Currency,
                Sectors,
                GST,
                CUTE,
                ASF,
                PSF_ADF,
                UDF,
                PSC_PZ,
                RCS_CUTE,
                RCS,
                Tax1,
                Tax2,
                Tax3,
                Tax4,
                Tax5
            FROM QP_DW_PRIAUT.tax_mast_intl
            """
            
            self.tax_master_intl_data = pd.read_sql(query, self.rm_rd_conn)
            return self.tax_master_intl_data
            
        except Exception as e:
            self.logger.error(f"Error collecting international tax master data: {str(e)}")
            raise

    def collect_market_fares(self):
        """Collect market fares min/max data based on route (domestic/international) with carrier info."""
        try:
            # Base query components
            base_conditions = """
            WHERE outbound_airline_code NOT IN ('9I', 'IC', 'S9', 'S5','QP','G8') 
                AND CAST(departure_date AS DATE) >= CURDATE()
                AND CAST(departure_date AS DATE) <= CURDATE() + INTERVAL 365 DAY
                AND created_at > NOW() - INTERVAL 24 HOUR
            """
            
            if self.route == 'D':
                # Add domestic route filter
                route_filter = """
                AND outbound_stop = 0
                """
            elif self.route == 'I':
                # Add international route filter
                route_filter = """
                AND outbound_stop >= 0
                """
            else:
                raise ValueError(f"Invalid route: {self.route}")
            
            query = f"""
            WITH combined_data AS (
                SELECT 
                    gross_fare, 
                    outbound_airline_code, 
                    destination_airport,
                    source_airport,
                    channel
                FROM market_fares_ag
                {base_conditions}
                {route_filter}
                
                UNION ALL
                
                SELECT 
                    gross_fare,  
                    outbound_airline_code,
                    destination_airport,
                    source_airport,
                    channel
                FROM market_fares_rg
                {base_conditions}
                {route_filter}
            ),
            min_max_fares AS (
                SELECT 
                    source_airport,
                    destination_airport,
                    channel,
                    MIN(gross_fare) as min_fare,
                    MAX(gross_fare) as max_fare,
                    COUNT(DISTINCT gross_fare) as instances
                FROM combined_data
                GROUP BY source_airport, destination_airport, channel
            )
            SELECT 
                mmf.source_airport,
                mmf.destination_airport,
                mmf.channel,
                mmf.min_fare,
                mmf.max_fare,
                mmf.instances,
                (SELECT outbound_airline_code 
                FROM combined_data cd
                WHERE cd.source_airport = mmf.source_airport
                AND cd.destination_airport = mmf.destination_airport
                AND cd.channel = mmf.channel
                AND cd.gross_fare = mmf.min_fare
                LIMIT 1) as min_carrier,
                (SELECT outbound_airline_code 
                FROM combined_data cd
                WHERE cd.source_airport = mmf.source_airport
                AND cd.destination_airport = mmf.destination_airport
                AND cd.channel = mmf.channel
                AND cd.gross_fare = mmf.max_fare
                LIMIT 1) as max_carrier
            FROM min_max_fares mmf
            ORDER BY mmf.source_airport, mmf.destination_airport, mmf.channel
            """
            
            self.market_fares_data = pd.read_sql(query, self.rm_rd_conn)
            
            return self.market_fares_data
            
        except Exception as e:
            self.logger.error(f"Error collecting market fares data: {str(e)}")
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
            raise

    def collect_city_pair_data(self):
        """Collect city pair data for sector distances."""
        try:
            query = f"""
            SELECT 
                CityPairID,
                CityPair,
                CityPairGroup,
                DepartureStation,
                ArrivalStation,
                ActualDistance
            FROM {self.nav_db_dw_name}dw.CityPair
            """
            
            self.city_pair_data = pd.read_sql(query, self.nav_conn)
            
            return self.city_pair_data
            
        except Exception as e:
            self.logger.error(f"Error collecting city pair data: {str(e)}")
            raise
        
    def collect_active_sectors(self):
        """Collect active sectors."""
        try:
            query = f"""
            SELECT Distinct DepartureStation,
            ArrivalStation
            FROM {self.nav_db_name}rez.InventoryLeg
            WHERE DepartureDate>='{datetime.now().date()}'
            AND CarrierCode='QP'
            AND FlightNumber<7000
            """
            
            self.active_sectors = pd.read_sql(query, self.nav_conn)
            
            return self.active_sectors
            
        except Exception as e:
            self.logger.error(f"Error collecting active sectors data: {str(e)}")
            raise

    def collect_all_data(self, booking_days=30):
        """Collect all required data for actionable sectors analysis based on route."""
        try:
            
            # Collect QP fares based on route
            self.collect_active_sectors()
            self.collect_qp_fares()
            self.collect_booked_data(booking_days)
            self.collect_city_pair_data()
            
            # Collect tax master data based on route
            if self.route == 'D':
                self.collect_tax_master_domestic()
            elif self.route == 'I':
                self.collect_tax_master_international()
            
            # Collect market fares based on route
            self.collect_market_fares()
            
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
            instances = int(xqp_record['instances'])
            min_carrier = xqp_record['min_carrier']
            max_carrier = xqp_record['max_carrier']
            
            self.logger.debug(f"Market data for {sector} {channel}: min={xqp_min_fare}, max={xqp_max_fare}, instances={instances}")
            
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
                    qp_min_fare, qp_max_fare, min_rbd, max_rbd, instances
                )
                if max_action_record:
                    results.append(max_action_record)
            
            # Create Min Action record if valid
            if min_action_valid:
                min_action_record = self._create_action_record(
                    sector, origin, destination, channel, "Min_Action",
                    qp_min_fare, xqp_min_fare, min_carrier, min_rbd,
                    qp_min_fare, qp_max_fare, min_rbd, max_rbd, instances
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
                'Min_QP': qp_min_fare if qp_min_fare else 'N/A',
                'Max_QP': qp_max_fare if qp_max_fare else 'N/A',
                'Min_QP_RBD': min_rbd if min_rbd else 'N/A',
                'Max_QP_RBD': max_rbd if max_rbd else 'N/A',
                'Action_RBD': action_rbd,
                'XQP_Fare': round(xqp_fare, 2),
                'Fare_difference': round(fare_diff, 2),
                'PLF': f"{plf:.0%}",
                'Instances': instances,
                'Base_XQP_Fare': round(base_xqp_fare, 2),
                'Distance': int(distance),
                'New_Fare_Point': round(yield_value, 2) if self.route == 'D' else 'N/A',
                'Yield': round(yield_value, 2) if self.route == 'D' else 'N/A',
                'Action_required': action_type,
                'Req_Min_Instances': 2,
                'Req_Min_FD': 1,
                'Req_Min_Yield': 1.5 if self.route == 'D' else 'N/A',
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
                min_yield_ok = yield_value >= 1.5
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

    def save_actionable_sectors_to_database(self, actionable_df):
        """Save actionable sectors to single database table for both domestic and international."""
        try:
            if actionable_df.empty:
                return None, None
            
            # Single table for both domestic and international
            table_name = 'QP_DW_PRIAUT.actionable_sectors_output'
            run_id = str(uuid.uuid4())
            
            # Add metadata columns
            actionable_df = actionable_df.copy()
            actionable_df['run_id'] = run_id
            actionable_df['created_at'] = datetime.now(pytz.timezone('Asia/Kolkata'))
            actionable_df['route'] = self.route
            actionable_df['analyst'] = self.analystName
            
            # Always append data (never replace)
            actionable_df.to_sql(
                name='actionable_sectors_output',  # Table name without schema
                con=self.rm_rd_conn,
                schema='QP_DW_PRIAUT',
                if_exists='append',  # Always append to preserve historical data
                index=False,
                method='multi'  # For better performance with large datasets
            )
            
            
            return table_name, run_id
            
        except Exception as e:
            self.logger.error(f"Error saving actionable sectors to database: {str(e)}")
            raise
    
    def get_current_version(self):
         
        query = text(f"""
            SELECT * FROM QP_DW_RMALLOC.currentVersion
            WHERE tableName in ('QP_DW_PRIAUT.qp_fares','QP_DW_PRIAUT.tax_mast_dom','QP_DW_PRIAUT.tax_mast_intl');
        """)

        try:
            self.currVersion = pd.read_sql(query, self.rm_rd_conn)
            
        except Exception as e:
            self.logger.error(f"Unable to get current version: {str(e)}")
            raise

        
    def save_run_audit(self, run_id, actionable_df, table_name):
        """Save run audit information to pricing_run_audits table."""
        try:
            total_records = len(actionable_df)
            max_actions = len(actionable_df[actionable_df['Action_required'] == 'Max_Action'])
            min_actions = len(actionable_df[actionable_df['Action_required'] == 'Min_Action'])
            constraints_satisfied = len(actionable_df[actionable_df['Constraint'] == 'Satisfied'])

            # Determine tax table name based on route
            tax_table_name = 'QP_DW_PRIAUT.tax_mast_dom' if self.route == 'D' else 'QP_DW_PRIAUT.tax_mast_intl'

            # Extract version values from self.currVersion DataFrame
            qp_fares_version = self.currVersion.loc[
                self.currVersion['tableName'] == 'QP_DW_PRIAUT.qp_fares', 'curr_version'
            ].values[0]

            tax_file_version = self.currVersion.loc[
                self.currVersion['tableName'] == tax_table_name, 'curr_version'
            ].values[0]

            # Create audit record
            audit_data = {
                'run_id': [run_id],
                'start_time': [self.start_time],
                'end_time': [datetime.now(pytz.timezone('Asia/Kolkata'))],
                'table_name': [table_name.split('.')[1] if '.' in table_name else table_name],
                'total_actionable_records': [total_records],
                'max_actions': [max_actions],
                'min_actions': [min_actions],
                'constraints_satisfied': [constraints_satisfied],
                'route': [self.route],
                'qp_fares': [qp_fares_version],
                'tax_master': [tax_file_version],
                'analyst' : [self.analystName]
            }

            # Convert to DataFrame
            audit_df = pd.DataFrame(audit_data)

            # Save to database
            audit_df.to_sql(
                name='pricing_run_audits',
                con=self.rm_rd_conn,
                schema='QP_DW_PRIAUT',
                if_exists='append',
                index=False,
                method='multi'
            )

            audit_table_name = 'QP_DW_PRIAUT.pricing_run_audits'

            return audit_table_name

        except Exception as e:
            self.logger.error(f"Error saving run audit: {str(e)}")
            return None


    def run(self):
        """Main method to run complete actionable sectors analysis."""
        try:
            self.start_time = datetime.now(pytz.timezone('Asia/Kolkata'))
            
            # Get current version information for audit
            self.get_current_version()
            
            # Collect all data
            self.collect_all_data()
            
            # Generate actionable sectors
            actionable_sectors = self.generate_actionable_sectors()
            
            if actionable_sectors.empty:
                return {'actionable_sectors': actionable_sectors, 'table_name': None, 'run_id': None}
            
            # Save to database table
            table_name, run_id = self.save_actionable_sectors_to_database(actionable_sectors)
            
            # Save audit information
            self.save_run_audit(run_id, actionable_sectors, table_name)
            
            # Log summary
            route_desc = "Domestic" if self.route == 'D' else "International"
            
            return {
                'actionable_sectors': actionable_sectors,
                'table_name': table_name,
                'run_id': run_id,
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
            raise