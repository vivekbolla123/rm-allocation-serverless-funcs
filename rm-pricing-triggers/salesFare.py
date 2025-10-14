import pandas as pd
import os
import configsettings
from constants_utils import ConstantsUtils
from datetime import datetime, timedelta, date
import traceback
import uuid

class SalesFare:
    def __init__(self, logger, params, overrides, context):
        """Initialize with required connections and constants."""
        self.logger = logger
        self.rm_rd_conn = params['wrconn']
        self.constants = ConstantsUtils(self.rm_rd_conn)
        self.analystName = overrides['analyst']
               
        # Initialize data containers
        self.sales_fares_data = None
        self.blackout_data = None
    
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
            raise
       
    def collect_data(self):
        """Collect all necessary data."""
        self.logger.info("Starting data collection...")
        self.collect_sale_fares()
        self.collect_blackout_dates()
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
    
    def generate_output_file(self, output_path=None):
        """
        Generate the output file with sales fare data excluding blackout dates.
        Only includes rows where discount is present (not empty).
        If discount is 0, includes row with price as 0.
        """
        try:
            if self.sales_fares_data is None or self.blackout_data is None:
                self.logger.error("Data not collected. Run collect_data() first.")
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
                        'ACTION': 'I',
                        'ID': '',  # Leave blank - generated by Navitaire
                        'LOC1': origin,
                        'LOC1TYPE': self.determine_location_type(origin),
                        'DIRECTION': '>',
                        'LOC2': destination,
                        'LOC2TYPE': self.determine_location_type(destination),
                        'SALESSTART': row['sale_start'].strftime('%Y-%m-%d'),
                        'SALESEND': row['sale_end'].strftime('%Y-%m-%d'),
                        'TRAVELSTART': period_start.strftime('%Y-%m-%d'),
                        'TRAVELEND': period_end.strftime('%Y-%m-%d'),
                        'TSI': '',
                        'INCLUDED': '',
                        'INCLUDED#': '',
                        'DFLTCURR': row['currency'] if row['type'] == 'value' else '',
                        'CURR': row['currency'] if row['type'] == 'value' else '',
                        'PRICE': 0 if row['discount'] == 0 else row['discount'],
                        'POINTS': 0 if row['type'] == 'value' else '',
                        'PER#KG': '',
                        'PER#LBS': '',
                        'DEDUCTEDMODE': ''
                    }
                    output_rows.append(output_row)
            
            # Create DataFrame
            output_df = pd.DataFrame(output_rows)
            
            # Generate output file path if not provided
            if output_path is None:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                output_path = f'sales_fare_output_{timestamp}.csv'
            
            # Save to CSV
            output_df.to_csv(output_path, index=False)
            
            self.logger.info(f"Output file generated successfully: {output_path}")
            self.logger.info(f"Total output rows: {len(output_df)}")
            
            return output_df
            
        except Exception as e:
            self.logger.error(f"Error generating output file: {str(e)}")
            self.logger.error(traceback.format_exc())
            raise
    
    def run(self):
        """
        Main processing method to collect data and generate output.
        """
        try:
            self.logger.info("Starting sales fare processing...")
            
            # Collect data
            self.collect_data()
            
            # Generate output
            output_df = self.generate_output_file(None)
            
            self.logger.info("Sales fare processing completed successfully")
            
            return output_df
            
        except Exception as e:
            self.logger.error(f"Error in sales fare processing: {str(e)}")
            self.logger.error(traceback.format_exc())
            raise
