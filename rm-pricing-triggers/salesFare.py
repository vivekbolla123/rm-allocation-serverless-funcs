import pandas as pd
import os
import pytz
import configsettings
from constants_utils import ConstantsUtils
from datetime import datetime, timedelta, date
from sqlalchemy import create_engine, text
import traceback
import uuid

from helpers.nav_db_helpers.sales_nav_helper import SalesNavHelper
from helpers.pa_db_helpers.sales_pa_helper import SalesPAHelper

class SalesFare:
    def __init__(self, logger, params, overrides, context):
        """Initialize with required connections and constants."""
        self.logger = logger
        self.db = params['mongo']
        self.master_db = params.get('mongo_master', params['mongo'])
        self.nav_conn = params['navconn']
        self.nav_db_name = configsettings.NAV_DB_NAME
        self.nav_db_dw_name = configsettings.NAV_DB_DW_NAME
        self.nav_db_wb_name = configsettings.NAV_DB_WB_NAME
        self.constants = ConstantsUtils(self.db)
        self.pa_helper = SalesPAHelper(self.db, logger, self.master_db)
        self.nav_helper = SalesNavHelper(self.nav_conn, self.nav_db_name, self.nav_db_dw_name, self.nav_db_wb_name, logger)
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
    
    def collect_data(self):
        """Collect all necessary data."""
        self.logger.info("Starting data collection...")
        self.sales_fares_data = self.pa_helper.collect_sale_fares()
        self.blackout_data = self.pa_helper.collect_blackout_dates()
        self.active_sectors = self.nav_helper.collect_active_sectors()
        self.station_master = self.pa_helper.collect_station_master()
        self.currVersion = self.pa_helper.get_current_version()
        
        # Expand DOMESTIC/INTERNATIONAL sectors into actual sectors
        self.expand_domestic_international_sectors()
        
        # Validate sectors before processing
        self.validate_sectors()
        
        self.logger.info("Data collection completed")
    
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
    
    def parse_sector(self, sector):
        """
        Parse sector string to extract origin and destination with their types.
        
        Handles special patterns:
        - 'BOM***' or 'AMD***' etc: Origin=station, Destination='**', both type P
        - '###RUH' or '###DXB' etc: Origin='IN', type N, Destination=station, type P
        - 'RUH###' or 'DXB###' etc: Origin=station, type P, Destination='IN', type N
        - 'AMDBLR' (6 chars): Origin=first 3, Destination=last 3, both type P
        
        Returns:
            tuple: (origin, origin_type, destination, destination_type)
        """
        sector = sector.strip()
        
        # Pattern: BOM*** (origin specified, destination wildcard)
        if sector.endswith('***'):
            origin = sector[:3]
            return (origin, 'P', '**', 'P')
        
        # Pattern: ###RUH (India to international destination)
        if sector.startswith('###'):
            destination = sector[3:6]
            return ('IN', 'N', destination, 'P')
        
        # Pattern: RUH### (international origin to India)
        if sector.endswith('###'):
            origin = sector[:3]
            return (origin, 'P', 'IN', 'N')
        
        # Standard 6-character sector (e.g., AMDBLR)
        if len(sector) >= 6:
            origin = sector[:3]
            destination = sector[3:6]
            return (origin, 'P', destination, 'P')
        
        # Invalid format
        raise ValueError(f"Invalid sector format: {sector}")
    
    def get_sector_type(self, sector, station_route):
        """
        Determine if a sector is DOMESTIC or INTERNATIONAL.
        
        Args:
            sector: The sector code
            station_route: Dictionary mapping stations to routes ('D' or 'I')
        
        Returns:
            'DOMESTIC' if both stations are domestic, 'INTERNATIONAL' otherwise
        """
        try:
            origin, origin_type, destination, destination_type = self.parse_sector(sector)
            
            # Handle special wildcards
            if origin == 'IN' or destination == 'IN':
                return 'INTERNATIONAL'
            
            if origin == '**' or destination == '**':
                # For wildcards, check the non-wildcard station
                station = origin if destination == '**' else destination
                route = station_route.get(station, None)
                
                # If station not found in master, raise error
                if route is None:
                    raise ValueError(f"Station '{station}' not found in station_master")
                
                return 'DOMESTIC' if route == 'D' else 'INTERNATIONAL'
            
            # Check both stations
            origin_route = station_route.get(origin, None)
            dest_route = station_route.get(destination, None)
            
            # Collect missing stations
            missing_stations = []
            if origin_route is None:
                missing_stations.append(origin)
            if dest_route is None:
                missing_stations.append(destination)
            
            # If any station is missing, raise error
            if missing_stations:
                raise ValueError(f"Station(s) not found in station_master: {', '.join(missing_stations)}")
            
            # If both domestic, return DOMESTIC, otherwise INTERNATIONAL
            if origin_route == 'D' and dest_route == 'D':
                return 'DOMESTIC'
            else:
                return 'INTERNATIONAL'
                
        except ValueError:
            # Re-raise ValueError as-is
            raise
        except Exception as e:
            # Wrap other exceptions
            raise ValueError(f"Error determining sector type for '{sector}': {str(e)}")
        
    def validate_sectors(self):
        """
        Validate that all sectors in sales_fares_data have valid stations in station_master.
        
        Raises:
            ValueError: If any sector contains stations not in station_master
        """
        self.logger.info("Validating sectors against station master...")
        
        if self.sales_fares_data is None or self.sales_fares_data.empty:
            self.logger.warning("No sales fare data to validate")
            return
        
        # Create station route lookup
        station_route = dict(zip(self.station_master['station'], self.station_master['route']))
        
        # Track all validation errors
        validation_errors = []
        unique_sectors = self.sales_fares_data['sector'].unique()
        
        for sector in unique_sectors:
            try:
                sector_type = self.get_sector_type(sector, station_route)
                self.logger.info(f"  ✓ Sector '{sector}' validated as {sector_type}")
            except ValueError as e:
                error_msg = f"Sector '{sector}': {str(e)}"
                validation_errors.append(error_msg)
                self.logger.error(f"  ✗ {error_msg}")
        
        # If there are any validation errors, raise exception
        if validation_errors:
            error_summary = f"Sector validation failed for {len(validation_errors)} sector(s):\n" + "\n".join(f"  - {err}" for err in validation_errors)
            raise ValueError(error_summary)
        
        self.logger.info(f"All {len(unique_sectors)} sectors validated successfully")
    
    def split_date_range_by_blackouts(self, sector, start_date, end_date, is_open_ended=False):
        """
        Split a date range into multiple ranges excluding blackout dates.
        Applies blackouts from exact sector match AND from DOMESTIC/INTERNATIONAL category.
        
        Args:
            sector: The sector code (e.g., 'AMDBLR')
            start_date: Start date of the range
            end_date: End date of the range (can be None for open-ended)
            is_open_ended: Whether this is an open-ended period
            
        Returns:
            List of tuples with (start_date, end_date) for valid periods
            For open-ended, the last tuple will have None as end_date
        """
        # Create station route lookup
        station_route = dict(zip(self.station_master['station'], self.station_master['route']))
        
        # Determine sector type (DOMESTIC or INTERNATIONAL)
        sector_type = self.get_sector_type(sector, station_route)
        
        # Get blackout dates for:
        # 1. Exact sector match
        # 2. Sector type (DOMESTIC/INTERNATIONAL)
        sector_blackouts = self.blackout_data[
            (self.blackout_data['sector'] == sector) | 
            (self.blackout_data['sector'] == sector_type)
        ].copy()
        
        if sector_blackouts.empty:
            # No blackouts, return the original range
            return [(start_date, end_date)]
        
        # Remove duplicate blackout periods (in case same dates appear in both exact and category)
        sector_blackouts = sector_blackouts.drop_duplicates(subset=['start_date', 'end_date'])
        
        # Sort blackouts by start date
        sector_blackouts = sector_blackouts.sort_values('start_date')
        
        # If open-ended, we need to consider all blackouts starting from start_date
        if is_open_ended:
            # Filter blackouts that start on or after the start_date
            sector_blackouts = sector_blackouts[sector_blackouts['start_date'] >= start_date]
        else:
            # For normal periods, filter blackouts within the range
            sector_blackouts = sector_blackouts[
                (sector_blackouts['end_date'] >= start_date) & 
                (sector_blackouts['start_date'] <= end_date)
            ]
        
        if sector_blackouts.empty:
            # No relevant blackouts
            return [(start_date, end_date)]
        
        valid_periods = []
        current_start = start_date
        
        for _, blackout in sector_blackouts.iterrows():
            blackout_start = blackout['start_date']
            blackout_end = blackout['end_date']
            
            # For normal periods, skip if blackout is completely outside our range
            if not is_open_ended:
                if blackout_end < start_date or blackout_start > end_date:
                    continue
            
            # If there's a gap before the blackout, add it as a valid period
            if current_start < blackout_start:
                # End the period one day before blackout starts
                period_end = blackout_start - timedelta(days=1)
                if not is_open_ended and period_end > end_date:
                    period_end = end_date
                if period_end >= start_date:
                    valid_periods.append((current_start, period_end))
            
            # Move current_start to one day after blackout ends
            current_start = blackout_end + timedelta(days=1)
        
        # Add the remaining period after the last blackout
        if is_open_ended:
            # For open-ended, add the period with None as end date
            valid_periods.append((current_start, None))
        else:
            # For normal periods, add if there's remaining time
            if current_start <= end_date:
                valid_periods.append((current_start, end_date))
        
        return valid_periods if valid_periods else []
    
    def run(self):
        """
        Main processing method to collect data and generate output.
        Groups input by 'key' column and produces a separate output per key.
        """
        self.start_time = datetime.now(pytz.timezone('Asia/Kolkata'))
        all_outputs = []
        
        try:
            self.logger.info("Starting sales fare processing...")
            
            # Collect data (includes validation)
            self.collect_data()
            
            if self.sales_fares_data is None or self.sales_fares_data.empty:
                raise ValueError("No sales fare data available to process")
            
            # Get unique keys - each key produces a separate output
            if 'key' not in self.sales_fares_data.columns:
                # Fallback: no key column, process all as single output
                unique_keys = ['default']
                self.sales_fares_data['key'] = 'default'
            else:
                unique_keys = self.sales_fares_data['key'].fillna('default').unique().tolist()
            
            self.logger.info(f"Found {len(unique_keys)} unique key(s): {unique_keys}")
            
            # Process each key group separately
            for key_val in unique_keys:
                run_id = str(uuid.uuid4())
                status = 'SUCCESS'
                output_df = None
                failure_reason = None
                
                try:
                    self.logger.info(f"Processing key '{key_val}' with Run ID: {run_id}")
                    
                    # Filter data for this key
                    key_data = self.sales_fares_data[
                        self.sales_fares_data['key'].fillna('default') == key_val
                    ].copy()
                    
                    self.logger.info(f"Key '{key_val}' has {len(key_data)} input rows")
                    
                    # Pass filtered data in context
                    context = {
                        'parse_sector': self.parse_sector,
                        'split_date_range_by_blackouts': self.split_date_range_by_blackouts,
                        'sales_fares_data': key_data
                    }
                    
                    # Generate output for this key group
                    output_df = self.pa_helper.generate_output_data(run_id, context)
                    
                    if output_df is not None and not output_df.empty:
                        all_outputs.append(output_df)
                    
                    self.logger.info(f"Key '{key_val}' processing completed successfully")
                    
                except ValueError as ve:
                    status = 'FAILED'
                    failure_reason = f"Validation error for key '{key_val}': {str(ve)}"
                    self.logger.error(failure_reason)
                    
                except Exception as e:
                    status = 'FAILED'
                    failure_reason = f"Processing failed for key '{key_val}': {str(e)}"
                    self.logger.error(failure_reason)
                    self.logger.error(traceback.format_exc())
                
                finally:
                    # Save audit for this key's run
                    try:
                        audit_table = self.pa_helper.save_run_audit(
                            self.analystName, self.currVersion, run_id, 
                            output_df, status, failure_reason, key_val=key_val
                        )
                        if audit_table:
                            self.logger.info(f"Audit saved for key '{key_val}' to {audit_table}")
                    except Exception as audit_error:
                        self.logger.error(f"Failed to save audit for key '{key_val}': {str(audit_error)}")
            
            self.logger.info(f"Sales fare processing completed. Generated {len(all_outputs)} output(s) from {len(unique_keys)} key(s)")
            
        except Exception as e:
            self.logger.error(f"Error in sales fare processing: {str(e)}")
            self.logger.error(traceback.format_exc())
            raise
        
        # Return combined output (or None if no outputs)
        if all_outputs:
            return pd.concat(all_outputs, ignore_index=True)
        return None