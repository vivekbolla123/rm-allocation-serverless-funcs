import pandas as pd
import os
import pytz
import configsettings
from constants_utils import ConstantsUtils
from datetime import datetime, timedelta, date
from sqlalchemy import create_engine, text
import traceback
import uuid

class SalesFare:
    def __init__(self, logger, params, overrides, context):
        """Initialize with required connections and constants."""
        self.logger = logger
        self.rm_rd_conn = params['wrconn']
        self.nav_conn = params['navconn']
        self.nav_db_name = configsettings.NAV_DB_NAME
        self.constants = ConstantsUtils(self.rm_rd_conn)
        self.analystName = overrides['analyst']
        self.CONST_VALUE = 'VAL'
               
        # Initialize data containers
        self.sales_fares_data = None
        self.blackout_data = None
        self.currVersion = None
        self.failure_reason = None
        self.start_time = None
        self.active_sectors = None
        self.station_master = None
    
    def collect_sale_fares(self):
        """Collect sales fares data from database."""
        try:
            query = """
            SELECT
                sda.sector,
                sda.type,
                sda.currency,
                sda.sale_start,
                sda.sale_end,
                sdi.departure_start,
                sdi.departure_end,
                sdi.discount
            FROM
                QP_DW_PRIAUT.config_sale_dates sda
            JOIN
                QP_DW_PRIAUT.config_sale_discounts sdi ON sda.sale_id = sdi.sale_id
            """
           
            self.sales_fares_data = pd.read_sql(query, self.rm_rd_conn)
           
            # Convert date columns to datetime
            date_columns = ['sale_start', 'sale_end', 'departure_start', 'departure_end']
            for col in date_columns:
                if col in self.sales_fares_data.columns:
                    self.sales_fares_data[col] = pd.to_datetime(self.sales_fares_data[col])
           
            self.logger.info(f"Collected {len(self.sales_fares_data)} sales fare records")
            return self.sales_fares_data
           
        except Exception as e:
            self.logger.error(f"Error collecting sales fare data: {str(e)}")
            self.failure_reason = f"Sales fare collection failed: {str(e)}"
            raise
       
    def collect_blackout_dates(self):
        """Collect blackout dates data from database."""
        try:
            query = """
            SELECT
                sector, start_date, end_date
            FROM
                QP_DW_PRIAUT.config_blackout_dates
            ORDER BY sector, start_date
            """
           
            self.blackout_data = pd.read_sql(query, self.rm_rd_conn)
           
            # Convert date columns to datetime
            self.blackout_data['start_date'] = pd.to_datetime(self.blackout_data['start_date'])
            self.blackout_data['end_date'] = pd.to_datetime(self.blackout_data['end_date'])
           
            self.logger.info(f"Collected {len(self.blackout_data)} blackout date records")
            return self.blackout_data
           
        except Exception as e:
            self.logger.error(f"Error collecting blackout dates data: {str(e)}")
            self.failure_reason = f"Blackout dates collection failed: {str(e)}"
            raise
    
    def collect_active_sectors(self):
        """Collect active sectors from Navitaire."""
        try:
            query = f"""
            SELECT DISTINCT DepartureStation,
            ArrivalStation
            FROM {self.nav_db_name}rez.InventoryLeg
            WHERE DepartureDate>='{datetime.now().date()}'
            AND CarrierCode='QP'
            AND FlightNumber<7000
            """
            
            self.active_sectors = pd.read_sql(query, self.nav_conn)
            self.logger.info(f"Collected {len(self.active_sectors)} active sectors")
            
            return self.active_sectors
            
        except Exception as e:
            self.logger.error(f"Error collecting active sectors data: {str(e)}")
            self.failure_reason = f"Active sectors collection failed: {str(e)}"
            raise
    
    def collect_station_master(self):
        """Collect station master data to identify domestic/international stations."""
        try:
            query = """
            SELECT
                station, route
            FROM
                QP_DW_PRIAUT.config_station_master
            """
            
            self.station_master = pd.read_sql(query, self.rm_rd_conn)
            self.logger.info(f"Collected {len(self.station_master)} station records")
            
            return self.station_master
            
        except Exception as e:
            self.logger.error(f"Error collecting station master data: {str(e)}")
            self.failure_reason = f"Station master collection failed: {str(e)}"
            raise
    
    def expand_domestic_international_sectors(self):
        """
        Expand DOMESTIC/INTERNATIONAL sector types into actual sectors.
        For DOMESTIC type: Get all sectors where both departure and arrival are domestic (route='D')
        For INTERNATIONAL type: Get all sectors where at least one station is international (route='I')
        """
        try:
            if self.sales_fares_data is None:
                return
            
            # Get rows with DOMESTIC or INTERNATIONAL in sector field
            special_sectors = self.sales_fares_data[
                self.sales_fares_data['sector'].isin(['DOMESTIC', 'INTERNATIONAL'])
            ].copy()
            
            # Get rows with actual sectors
            actual_sectors = self.sales_fares_data[
                ~self.sales_fares_data['sector'].isin(['DOMESTIC', 'INTERNATIONAL'])
            ].copy()
            
            if special_sectors.empty:
                self.logger.info("No DOMESTIC/INTERNATIONAL sectors to expand")
                return
            
            # Create station route lookup
            station_route = dict(zip(self.station_master['station'], self.station_master['route']))
            
            expanded_rows = []
            
            for idx, row in special_sectors.iterrows():
                sector_type = row['sector']
                
                # Iterate through all active sectors
                for _, active_sector in self.active_sectors.iterrows():
                    dep_station = active_sector['DepartureStation']
                    arr_station = active_sector['ArrivalStation']
                    
                    # Get routes for both stations
                    dep_route = station_route.get(dep_station, None)
                    arr_route = station_route.get(arr_station, None)
                    
                    # Skip if station not found in master
                    if dep_route is None or arr_route is None:
                        continue
                    
                    # Check if this sector matches the type
                    is_match = False
                    if sector_type == 'DOMESTIC':
                        # Both stations must be domestic
                        is_match = (dep_route == 'D' and arr_route == 'D')
                    elif sector_type == 'INTERNATIONAL':
                        # At least one station must be international
                        is_match = (dep_route == 'I' or arr_route == 'I')
                    
                    if is_match:
                        # Create a new row for this sector
                        new_row = row.copy()
                        new_row['sector'] = f"{dep_station}{arr_station}"
                        expanded_rows.append(new_row)
            
            if expanded_rows:
                # Combine expanded rows with actual sectors
                expanded_df = pd.DataFrame(expanded_rows)
                self.sales_fares_data = pd.concat([actual_sectors, expanded_df], ignore_index=True)
                self.logger.info(f"Expanded {len(special_sectors)} special sectors into {len(expanded_rows)} actual sectors")
            else:
                self.sales_fares_data = actual_sectors
                self.logger.warning("No sectors matched DOMESTIC/INTERNATIONAL criteria")
            
        except Exception as e:
            self.logger.error(f"Error expanding domestic/international sectors: {str(e)}")
            self.failure_reason = f"Sector expansion failed: {str(e)}"
            raise
    
    def get_current_version(self):
        """Get current version information."""
        try:
            query = text("""
                SELECT * FROM QP_DW_RMALLOC.currentVersion
                WHERE tableName IN ('QP_DW_PRIAUT.config_sale_dates','QP_DW_PRIAUT.config_sale_discounts','QP_DW_PRIAUT.config_blackout_dates')
            """)
            self.currVersion = pd.read_sql(query, self.rm_rd_conn)
            self.logger.info("Version information collected successfully")
        except Exception as e:
            self.logger.error(f"Error getting version info: {str(e)}")
            # Don't raise - continue processing without version info
       
    def collect_data(self):
        """Collect all necessary data."""
        self.logger.info("Starting data collection...")
        self.collect_sale_fares()
        self.collect_blackout_dates()
        self.collect_active_sectors()
        self.collect_station_master()
        self.get_current_version()
        
        # Expand DOMESTIC/INTERNATIONAL sectors into actual sectors
        self.expand_domestic_international_sectors()
        
        self.logger.info("Data collection completed")
    
    def split_date_range_by_blackouts(self, sector, start_date, end_date):
        """
        Split a date range into multiple ranges excluding blackout dates.
        
        Args:
            sector: The sector code (e.g., 'AMDBLR')
            start_date: Start date of the range
            end_date: End date of the range
            
        Returns:
            List of tuples with (start_date, end_date) for valid periods
        """
        # Get blackout dates for this sector
        sector_blackouts = self.blackout_data[self.blackout_data['sector'] == sector]
        
        if sector_blackouts.empty:
            # No blackouts, return the original range
            return [(start_date, end_date)]
        
        # Sort blackouts by start date
        sector_blackouts = sector_blackouts.sort_values('start_date')
        
        valid_periods = []
        current_start = start_date
        
        for _, blackout in sector_blackouts.iterrows():
            blackout_start = blackout['start_date']
            blackout_end = blackout['end_date']
            
            # Skip if blackout is completely outside our range
            if blackout_end < start_date or blackout_start > end_date:
                continue
            
            # If there's a gap before the blackout, add it as a valid period
            if current_start < blackout_start:
                # End the period one day before blackout starts
                period_end = blackout_start - timedelta(days=1)
                if period_end >= start_date:
                    valid_periods.append((current_start, period_end))
            
            # Move current_start to one day after blackout ends
            current_start = blackout_end + timedelta(days=1)
        
        # Add the remaining period after the last blackout
        if current_start <= end_date:
            valid_periods.append((current_start, end_date))
        
        return valid_periods if valid_periods else []
    
    def determine_location_type(self, location_code):
        """
        Determine location type based on location code.
        Returns 'P' for city or 'N' for India.
        
        This is a placeholder - you should customize based on your business logic.
        """
        # Example logic: If it's a 3-letter code, it's likely a city (P)
        # If it's IN or IND, it's India (N)
        if location_code in ['IN', 'IND', 'INDIA']:
            return 'N'
        return 'P'
    
    def generate_output_data(self, run_id):
        """
        Generate the output data with sales fare data excluding blackout dates.
        Only includes rows where discount is present (not empty).
        If discount is 0, includes row with price as 0.
        Saves output to database table with run_id.
        """
        try:
            if self.sales_fares_data is None or self.blackout_data is None:
                self.logger.error("Data not collected. Run collect_data() first.")
                self.failure_reason = "Data not collected before generating output"
                return None
            
            output_rows = []
            
            # Filter out rows where discount is null/empty
            # Keep rows where discount is 0 or any other value
            valid_fares = self.sales_fares_data[self.sales_fares_data['discount'].notna()].copy()
            
            self.logger.info(f"Processing {len(valid_fares)} fare records with non-empty discounts")
            
            for idx, row in valid_fares.iterrows():
                sector = row['sector']
                departure_start = row['departure_start']
                departure_end = row['departure_end']
                
                # Extract origin and destination from sector
                # Assuming format like 'AMDBLR' where first 3 chars are origin
                if len(sector) >= 6:
                    origin = sector[:3]
                    destination = sector[3:6]
                else:
                    self.logger.warning(f"Invalid sector format: {sector}")
                    continue
                
                # Split the date range by blackouts
                valid_periods = self.split_date_range_by_blackouts(
                    sector, departure_start, departure_end
                )
                
                # Create output rows for each valid period
                for period_start, period_end in valid_periods:
                    output_row = {
                        'run_id': run_id,  # Add run_id as first column
                        'ACTION': 'I',
                        'ID': '',  # Leave blank - generated by Navitaire
                        'LOC1': origin,
                        'LOC1TYPE': self.determine_location_type(origin),
                        'DIRECTION': '>',
                        'LOC2': destination,
                        'LOC2TYPE': self.determine_location_type(destination),
                        'SALESSTART': row['sale_start'].strftime('%m/%d/%Y'),
                        'SALESEND': row['sale_end'].strftime('%m/%d/%Y'),
                        'TRAVELSTART': period_start.strftime('%m/%d/%Y'),
                        'TRAVELEND': period_end.strftime('%m/%d/%Y'),
                        'TSI': '',
                        'INCLUDED': '',
                        'INCLUDED#': '',
                        'DFLTCURR': row['currency'] if row['type'] == self.CONST_VALUE else '',
                        'CURR': row['currency'] if row['type'] == self.CONST_VALUE else '',
                        'PRICE': 0 if row['discount'] == 0 else row['discount'],
                        'POINTS': 0 if row['type'] == self.CONST_VALUE else '',
                        'PER#KG': '',
                        'PER#LBS': '',
                        'DEDUCTEDMODE': ''
                    }
                    output_rows.append(output_row)
            
            # Create DataFrame
            output_df = pd.DataFrame(output_rows)
            
            if output_df.empty:
                self.logger.warning("No output rows generated")
                return output_df
            
            # Save to database table
            table_name = 'sales_fare_output'
            output_df.to_sql(table_name, self.rm_rd_conn, schema='QP_DW_PRIAUT', 
                           if_exists='append', index=False, method='multi')
            
            self.logger.info(f"Output data saved to database: QP_DW_PRIAUT.{table_name}")
            self.logger.info(f"Total output rows: {len(output_df)}")
            self.logger.info(f"Run ID: {run_id}")
            
            return output_df
            
        except Exception as e:
            self.logger.error(f"Error generating output data: {str(e)}")
            self.logger.error(traceback.format_exc())
            self.failure_reason = f"Output generation failed: {str(e)}"
            raise
    
    def save_run_audit(self, run_id, output_df, status='SUCCESS'):
        """Save audit information with enhanced failure tracking."""
        try:
            if status == 'SUCCESS' and (output_df is None or output_df.empty):
                status = 'FAILED'
                if not self.failure_reason:
                    self.failure_reason = 'No output generated'
            
            # Calculate statistics
            total_output_rows = len(output_df) if output_df is not None else 0
            total_input_fares = len(self.sales_fares_data) if self.sales_fares_data is not None else 0
            valid_input_fares = len(self.sales_fares_data[self.sales_fares_data['discount'].notna()]) if self.sales_fares_data is not None else 0
            total_blackout_dates = len(self.blackout_data) if self.blackout_data is not None else 0
            
            # Get unique sectors processed
            unique_sectors = self.sales_fares_data['sector'].nunique() if self.sales_fares_data is not None else 0
            sectors_with_splits = 0
            
            if output_df is not None and not output_df.empty:
                # Count sectors that were split due to blackouts
                # Group by LOC1 and LOC2 combination to identify unique sectors
                output_df_copy = output_df.copy()
                output_df_copy['sector_key'] = output_df_copy['LOC1'] + output_df_copy['LOC2']
                sector_counts = output_df_copy.groupby('sector_key').size()
                sectors_with_splits = len(sector_counts[sector_counts > 1])
            
            # Get version info safely
            sale_dates_version = None
            sale_discounts_version = None
            blackout_dates_version = None
            
            try:
                if hasattr(self, 'currVersion') and self.currVersion is not None and not self.currVersion.empty:
                    sale_dates_rows = self.currVersion[self.currVersion['tableName'] == 'QP_DW_PRIAUT.config_sale_dates']
                    sale_discounts_rows = self.currVersion[self.currVersion['tableName'] == 'QP_DW_PRIAUT.config_sale_discounts']
                    blackout_dates_rows = self.currVersion[self.currVersion['tableName'] == 'QP_DW_PRIAUT.config_blackout_dates']
                    
                    sale_dates_version = sale_dates_rows['curr_version'].values[0] if not sale_dates_rows.empty else None
                    sale_discounts_version = sale_discounts_rows['curr_version'].values[0] if not sale_discounts_rows.empty else None
                    blackout_dates_version = blackout_dates_rows['curr_version'].values[0] if not blackout_dates_rows.empty else None
            except Exception:
                pass
            
            # Get min/max prices
            min_price = None
            max_price = None
            if output_df is not None and not output_df.empty and 'PRICE' in output_df.columns:
                price_values = output_df['PRICE'].dropna()
                if not price_values.empty:
                    min_price = round(float(price_values.min()), 2)
                    max_price = round(float(price_values.max()), 2)
            
            # Get date ranges
            min_travel_start = None
            max_travel_end = None
            if output_df is not None and not output_df.empty:
                if 'TRAVELSTART' in output_df.columns:
                    min_travel_start = pd.to_datetime(output_df['TRAVELSTART']).min()
                if 'TRAVELEND' in output_df.columns:
                    max_travel_end = pd.to_datetime(output_df['TRAVELEND']).max()
            
            audit_data = pd.DataFrame({
                'run_id': [run_id],
                'start_time': [self.start_time],
                'end_time': [datetime.now(pytz.timezone('Asia/Kolkata'))],
                'total_input_fares': [total_input_fares],
                'valid_input_fares': [valid_input_fares],
                'total_output_rows': [total_output_rows],
                'unique_sectors': [unique_sectors],
                'sectors_with_splits': [sectors_with_splits],
                'total_blackout_dates': [total_blackout_dates],
                'min_price': [min_price],
                'max_price': [max_price],
                'min_travel_start': [min_travel_start],
                'max_travel_end': [max_travel_end],
                'sale_dates_version': [sale_dates_version],
                'sale_discounts_version': [sale_discounts_version],
                'blackout_dates_version': [blackout_dates_version],
                'analyst': [self.analystName],
                'status': [status],
                'failure_reason': [self.failure_reason if status == 'FAILED' else None]
            })
            
            audit_data.to_sql('sales_fare_generation_audits', self.rm_rd_conn, schema='QP_DW_PRIAUT', 
                            if_exists='append', index=False, method='multi')
            
            self.logger.info(f"Audit record saved - Run ID: {run_id}, Status: {status}, Output Rows: {total_output_rows}")
            
            return 'QP_DW_PRIAUT.sales_fare_generation_audits'
            
        except Exception as e:
            self.logger.error(f"Error saving audit: {str(e)}")
            try:
                # Fallback minimal audit record
                minimal_audit = pd.DataFrame({
                    'run_id': [run_id],
                    'start_time': [self.start_time if self.start_time else datetime.now()],
                    'end_time': [datetime.now(pytz.timezone('Asia/Kolkata'))],
                    'total_input_fares': [0],
                    'total_output_rows': [0],
                    'sale_dates_version': [None],
                    'sale_discounts_version': [None],
                    'blackout_dates_version': [None],
                    'station_master_version': [None],
                    'analyst': [self.analystName],
                    'status': ['FAILED'],
                    'failure_reason': [f"Audit save failed: {str(e)}. Original failure: {self.failure_reason}"]
                })
                
                minimal_audit.to_sql('sales_fare_generation_audits', self.rm_rd_conn, schema='QP_DW_PRIAUT', 
                                if_exists='append', index=False, method='multi')
                
                self.logger.info("Minimal audit record saved")
                return 'QP_DW_PRIAUT.sales_fare_generation_audits'
            except Exception as final_error:
                self.logger.error(f"Failed to save even minimal audit record: {str(final_error)}")
                return None
    
    def run(self):
        """
        Main processing method to collect data and generate output.
        """
        run_id = str(uuid.uuid4())
        self.start_time = datetime.now(pytz.timezone('Asia/Kolkata'))
        status = 'SUCCESS'
        output_df = None
        
        try:
            self.logger.info(f"Starting sales fare processing... Run ID: {run_id}")
            
            # Collect data
            self.collect_data()
            
            # Generate output and save to database with run_id
            output_df = self.generate_output_data(run_id)
            
            self.logger.info("Sales fare processing completed successfully")
            
        except Exception as e:
            status = 'FAILED'
            if not self.failure_reason:
                self.failure_reason = f"Processing failed: {str(e)}"
            self.logger.error(f"Error in sales fare processing: {str(e)}")
            self.logger.error(traceback.format_exc())
        
        finally:
            # Save audit regardless of success or failure
            try:
                audit_table = self.save_run_audit(run_id, output_df, status)
                if audit_table:
                    self.logger.info(f"Audit saved to {audit_table}")
            except Exception as audit_error:
                self.logger.error(f"Failed to save audit: {str(audit_error)}")
            
            if status == 'FAILED':
                raise Exception(self.failure_reason)
        
        return output_df