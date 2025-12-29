"""
Simple Mock Helper for Local Testing

This replaces database calls with CSV file reads when running locally.
Just change TEST_MODULE to switch between modules.
"""

import pandas as pd
import os
import logging
from typing import Dict, Any, Optional
from datetime import datetime

# Configuration - ONLY THING YOU NEED TO CHANGE
TEST_MODULE = 'action_sect'  # Change this to test different modules

# Fixed reference time for consistent test results
FIXED_REFERENCE_TIME = datetime(2024, 12, 29, 12, 0, 0)  # 2024-12-29 12:00:00

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUTS_DIR = os.path.join(BASE_DIR, 'inputs')
OUTPUTS_DIR = os.path.join(BASE_DIR, 'outputs', 'generated_outputs')

# Ensure output directory exists
os.makedirs(OUTPUTS_DIR, exist_ok=True)

# Module to CSV file mappings
CSV_MAPPINGS = {
    'sales_fare': {
        'collect_sale_fares': 'config_sale_dates.csv',
        'collect_blackout_dates': 'config_blackout_dates.csv', 
        'collect_station_master': 'config_station_master.csv',
        'collect_active_sectors': 'active_sectors.csv'
    },
    'ladder_gen': {
        'collect_tax_master_data': 'tax_master.csv',  # Special handling in __getattr__
        'collect_market_fares': 'market_fares_ag.csv',  # Aggregated format with gross_fare
        'collect_city_pair_data': 'city_pairs.csv',
        'collect_combined_booking_data': 'booking_data.csv',  # ESB helper - historical fares
        'collect_rm_parameter_values': 'rm_parameter_values.csv'
    },
    'action_sect': {
        'collect_qp_fares': 'qp_fares.csv',
        'collect_market_fares': 'market_fares_action.csv',  # Aggregated format with min/max fares
        'collect_tax_master_domestic': 'tax_master_domestic.csv',
        'collect_tax_master_international': 'tax_master_international.csv',
        'collect_active_sectors': 'active_sectors.csv',
        'collect_booked_data': 'booked_data.csv',  # NAV helper - flight capacity data
        'collect_combined_booking_data': 'booked_data.csv',  # ESB helper - flight capacity data
        'collect_city_pair_data': 'city_pairs.csv',
        'collect_rm_parameter_values': 'rm_parameter_values.csv'
    }
}

# Date columns that need parsing
DATE_COLUMNS = {
    'config_sale_dates.csv': ['sale_start', 'sale_end', 'departure_start', 'departure_end'],
    'config_blackout_dates.csv': ['start_date', 'end_date'],
    'booking_data.csv': ['BookingDate'],
    'qp_fares.csv': ['Effective_Date', 'Discontinue_Date'],
    'market_fares.csv': ['created_at', 'departure_date'],
    'market_fares_ag.csv': ['created_at', 'departure_date'],
    'market_fares_rg.csv': ['created_at', 'departure_date'],
    'market_fares_action.csv': ['created_at', 'departure_date'],
    'booked_data.csv': ['DepartureDate', 'DepartureTimeUTC']
}


class SimpleMockHelper:
    """
    Simple mock helper that replaces database calls with CSV file reads.
    Works with any module by using naming conventions.
    """
    
    def __init__(self, db, logger, module_key=None):
        self.db = db  # Not used in test mode
        self.logger = logger or logging.getLogger(__name__)
        self.module_key = module_key or TEST_MODULE
        self.CONST_VALUE = 'VAL'  # Common constant
        
        # Get CSV mappings for this module
        self.csv_mappings = CSV_MAPPINGS.get(self.module_key, {})
        
        if not self.csv_mappings:
            self.logger.warning(f"No CSV mappings found for module '{self.module_key}'")
    
    def __getattr__(self, method_name):
        """
        Automatically map collect_* methods to CSV files.
        Example: collect_sale_fares() -> reads config_sale_dates.csv
        """
        if method_name.startswith('collect_'):
            # Check if we have a specific mapping for this method
            if method_name in self.csv_mappings:
                csv_filename = self.csv_mappings[method_name]
                
                # Special handling for methods that return tuples
                if method_name == 'collect_market_fares':
                    if self.module_key == 'ladder_gen':
                        return lambda *args, **kwargs: (self._read_csv(csv_filename), None)
                    else:
                        # For action_sect, return DataFrame directly
                        return lambda *args, **kwargs: self._read_csv(csv_filename)
                elif method_name == 'collect_tax_master_data':
                    # Returns domestic and international tax data
                    dom_df = self._read_csv('tax_master_domestic.csv')
                    intl_df = self._read_csv('tax_master_international.csv')
                    return lambda *args, **kwargs: (dom_df, intl_df, None)
                elif method_name == 'collect_combined_booking_data':
                    # Different data for different modules
                    if self.module_key == 'ladder_gen':
                        # Ladder generation needs historical fare data
                        return lambda *args, **kwargs: self._read_csv('booking_data.csv')
                    else:
                        # Action sectors needs flight capacity data
                        return lambda *args, **kwargs: self._read_csv('booked_data.csv')
                else:
                    return lambda *args, **kwargs: self._read_csv(csv_filename)
            else:
                # Try to infer CSV filename from method name
                # collect_sale_fares -> sale_fares.csv
                inferred_name = method_name.replace('collect_', '') + '.csv'
                return lambda *args, **kwargs: self._read_csv(inferred_name)
        
        # For non-collect methods, raise AttributeError
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{method_name}'")
    
    def _read_csv(self, csv_filename):
        """Read CSV file and return as DataFrame"""
        csv_path = os.path.join(INPUTS_DIR, csv_filename)
        
        if not os.path.exists(csv_path):
            self.logger.error(f"CSV file not found: {csv_path}")
            return pd.DataFrame()  # Return empty DataFrame
        
        try:
            # Read CSV
            df = pd.read_csv(csv_path)
            
            # Parse date columns if specified
            if csv_filename in DATE_COLUMNS:
                for date_col in DATE_COLUMNS[csv_filename]:
                    if date_col in df.columns:
                        # Handle 'Open' values in date columns
                        df[date_col] = df[date_col].replace(['Open', 'OPEN', 'open'], pd.NaT)
                        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            
            self.logger.info(f"[TEST MODE] Loaded {len(df)} records from {csv_filename}")
            return df
            
        except Exception as e:
            self.logger.error(f"Error reading CSV {csv_filename}: {str(e)}")
            return pd.DataFrame()
    
    def get_current_version(self):
        """Return empty DataFrame for version info in test mode"""
        self.logger.info("[TEST MODE] Returning empty version info")
        return pd.DataFrame()
    
    def save_run_audit(self, *args, **kwargs):
        """Skip audit save in test mode"""
        self.logger.info("[TEST MODE] Audit save skipped")
        return 'mock_audit_table'
    
    def save_actionable_sectors_to_database(self, route, analyst_name, actionable_df, run_id):
        """Save actionable sectors to CSV in test mode"""
        try:
            if actionable_df is None or actionable_df.empty:
                self.logger.warning("[TEST MODE] No actionable sectors to save")
                return None
            
            # Save to CSV instead of MongoDB
            output_filename = f"actionable_sectors_output.csv"
            output_path = os.path.join(OUTPUTS_DIR, output_filename)
            
            # Add metadata columns with fixed values for deterministic testing
            actionable_df = actionable_df.copy()
            actionable_df['created_at'] = FIXED_REFERENCE_TIME  # Use fixed timestamp
            actionable_df['route'] = route
            actionable_df['analyst'] = analyst_name
            
            actionable_df.to_csv(output_path, index=False)
            
            self.logger.info(f"[TEST MODE] Saved {len(actionable_df)} actionable sectors to {output_path}")
            return 'actionable_sectors_output'
            
        except Exception as e:
            self.logger.error(f"Error saving actionable sectors: {str(e)}")
            return None
    
    def save_ladder_to_csv(self, ladder_result, run_id, sector, analyst_name, route):
        """Save ladder generation output to CSV in test mode"""
        try:
            if not ladder_result:
                self.logger.warning("[TEST MODE] No ladder result to save")
                return None
            
            # Save to CSV instead of MongoDB
            output_filename = f"ladder_gen_output.csv"
            output_path = os.path.join(OUTPUTS_DIR, output_filename)
            
            # Create ladder output records with fixed timestamp
            ladder_documents = []
            created_at = FIXED_REFERENCE_TIME  # Use fixed timestamp
            
            # Process B2B ladder (20 fares)
            ladder_fares_b2b = ladder_result.get('ladder_fares_b2b', [])
            if ladder_fares_b2b:
                for i, fare in enumerate(ladder_fares_b2b[:20]):
                    doc = {
                        'sector': sector,
                        'route': route,
                        'channel': 'B2B',
                        'fare_point_number': i + 1,
                        'rbd_name': f'Z{chr(74-i)}' if i < 10 else f'Z{9-i+10}',
                        'current_qp_base_fare': 0,  # Mock data
                        'current_qp_all_in_fare': 0,
                        'new_base_ladder_fare': round(fare, 2),
                        'new_all_in_ladder_fare': round(fare * 1.18, 2),  # Mock tax calculation
                        'data_period': ladder_result.get('data_period', 30),
                        'distinct_fares_count': ladder_result.get('distinct_fares_count_b2b', 0),
                        'total_booking_records': ladder_result.get('total_booking_records_b2b', 0),
                        'created_at': created_at,
                        'analyst': analyst_name
                    }
                    ladder_documents.append(doc)
            
            # Process B2C ladder (64 fares)
            ladder_fares_b2c = ladder_result.get('ladder_fares_b2c', [])
            if ladder_fares_b2c:
                rbd_names_b2c = [f"V{9-i}" for i in range(10)] + [f"U{9-i}" for i in range(10)] + \
                               [f"T{9-i}" for i in range(10)] + [f"R{9-i}" for i in range(10)] + \
                               [f"Q{9-i}" for i in range(10)] + ["QA", "O0", "N0", "M0", "K0", 
                                                                  "J0", "I0", "H0", "F0", "E0", 
                                                                  "D0", "C0", "B0", "Y0"]
                
                for i, fare in enumerate(ladder_fares_b2c[:64]):
                    doc = {
                        'sector': sector,
                        'route': route,
                        'channel': 'B2C',
                        'fare_point_number': i + 21,  # Start after B2B fares
                        'rbd_name': rbd_names_b2c[i] if i < len(rbd_names_b2c) else f'RBD_{i+1}',
                        'current_qp_base_fare': 0,  # Mock data
                        'current_qp_all_in_fare': 0,
                        'new_base_ladder_fare': round(fare, 2),
                        'new_all_in_ladder_fare': round(fare * 1.18, 2),  # Mock tax calculation
                        'data_period': ladder_result.get('data_period', 30),
                        'distinct_fares_count': ladder_result.get('distinct_fares_count_b2c', 0),
                        'total_booking_records': ladder_result.get('total_booking_records_b2c', 0),
                        'created_at': created_at,
                        'analyst': analyst_name
                    }
                    ladder_documents.append(doc)
            
            if ladder_documents:
                # Save to CSV
                ladder_df = pd.DataFrame(ladder_documents)
                ladder_df.to_csv(output_path, index=False)
                
                self.logger.info(f"[TEST MODE] Saved {len(ladder_documents)} ladder fares to {output_path}")
                return 'ladder_gen_output'
            else:
                self.logger.warning("[TEST MODE] No ladder documents to save")
                return None
            
        except Exception as e:
            self.logger.error(f"Error saving ladder output: {str(e)}")
            return None
    
    def generate_output_data(self, run_id, context):
        """
        Generate output data using the same logic as the real helper.
        This method processes the data and saves results to CSV.
        """
        try:
            self.logger.info(f"[TEST MODE] Generating output data for {self.module_key}")
            
            # For sales_fare module, we can implement the actual logic
            if self.module_key == 'sales_fare':
                return self._generate_sales_fare_output(run_id, context)
            else:
                # For other modules, return placeholder data
                self.logger.warning(f"[TEST MODE] Using placeholder output for {self.module_key}")
                output_df = pd.DataFrame({
                    'module': [self.module_key],
                    'timestamp': [FIXED_REFERENCE_TIME],  # Use fixed timestamp
                    'status': ['completed'],
                    'note': ['Placeholder output - implement specific logic']
                })
                
                # Save to CSV
                output_filename = f"{self.module_key}_output.csv"
                output_path = os.path.join(OUTPUTS_DIR, output_filename)
                output_df.to_csv(output_path, index=False)
                
                self.logger.info(f"[TEST MODE] Placeholder output saved to {output_path}")
                return output_df
            
        except Exception as e:
            self.logger.error(f"Error in generate_output_data: {str(e)}")
            # Return minimal DataFrame on error
            return pd.DataFrame({'error': [str(e)]})
    
    def _generate_sales_fare_output(self, run_id, context):
        """
        Generate sales fare output using the same logic as SalesPAHelper.
        This is a simplified version that works with CSV data.
        """
        try:
            # Get required functions from context
            parse_sector = context.get('parse_sector')
            split_date_range_by_blackouts = context.get('split_date_range_by_blackouts')
            sales_fares_data = context.get('sales_fares_data')
            
            if sales_fares_data is None or sales_fares_data.empty:
                self.logger.error("No sales fares data available")
                return pd.DataFrame()
            
            output_rows = []
            
            # Filter out rows where discount is null/empty
            valid_fares = sales_fares_data[sales_fares_data['discount'].notna()].copy()
            
            self.logger.info(f"[TEST MODE] Processing {len(valid_fares)} fare records")
            
            for idx, row in valid_fares.iterrows():
                sector = row['sector']
                departure_start = row['departure_start']
                departure_end = row['departure_end']
                
                # Check if departure_end is 'Open' or blank
                is_open_ended = pd.isna(departure_end) or departure_end == "" or departure_end == "open"
                
                try:
                    # Parse sector to get origin, destination and their types
                    if parse_sector:
                        origin, origin_type, destination, destination_type = parse_sector(sector)
                    else:
                        # Simple fallback parsing
                        if len(sector) >= 6:
                            origin, destination = sector[:3], sector[3:6]
                            origin_type = destination_type = 'P'
                        else:
                            continue
                except ValueError as e:
                    self.logger.warning(f"Skipping invalid sector: {str(e)}")
                    continue
                
                # Split the date range by blackouts if function available
                if split_date_range_by_blackouts:
                    valid_periods = split_date_range_by_blackouts(
                        sector, departure_start, departure_end, is_open_ended
                    )
                else:
                    # Simple fallback - use original dates
                    valid_periods = [(departure_start, departure_end)]
                
                # Create output rows for each valid period
                for period_start, period_end in valid_periods:
                    # Handle period_end which can be None for open-ended periods
                    travel_end = ''
                    if period_end is not None and pd.notna(period_end):
                        travel_end = period_end.strftime('%m/%d/%Y')
                    
                    output_row = {
                        'ACTION': 'I',
                        'ID': '',
                        'LOC1': origin,
                        'LOC1TYPE': origin_type,
                        'DIRECTION': '>',
                        'LOC2': destination,
                        'LOC2TYPE': destination_type,
                        'SALESSTART': row['sale_start'].strftime('%m/%d/%Y'),
                        'SALESEND': row['sale_end'].strftime('%m/%d/%Y'),
                        'TRAVELSTART': period_start.strftime('%m/%d/%Y'),
                        'TRAVELEND': travel_end,
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
                self.logger.warning("[TEST MODE] No output rows generated")
                return output_df
            
            # Save to CSV instead of MongoDB
            output_filename = f"{self.module_key}_output.csv"
            output_path = os.path.join(OUTPUTS_DIR, output_filename)
            output_df.to_csv(output_path, index=False)
            
            self.logger.info(f"[TEST MODE] Output saved to {output_path}")
            self.logger.info(f"[TEST MODE] Total output rows: {len(output_df)}")
            
            return output_df
            
        except Exception as e:
            self.logger.error(f"Error generating sales fare output: {str(e)}")
            raise


def create_mock_helper(db, logger, module_key=None):
    """Factory function to create mock helper"""
    return SimpleMockHelper(db, logger, module_key or TEST_MODULE)