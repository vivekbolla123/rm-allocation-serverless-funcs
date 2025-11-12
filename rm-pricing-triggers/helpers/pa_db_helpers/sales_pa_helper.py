import pandas as pd
import pytz
from datetime import datetime, timedelta
import traceback

class SalesPAHelper:
    def __init__(self, db, logger):
        """Initialize with database connection and logger."""
        self.db = db
        self.logger = logger
        self.CONST_VALUE = 'VAL'
    
    def collect_sale_fares(self):
        """Collect sales fares data from MongoDB."""
        try:
            # Query to get sale fares
            pipeline = [
                {
                    '$lookup': {
                        'from': 'config_sale_discounts',
                        'localField': 'sale_id',
                        'foreignField': 'sale_id',
                        'as': 'discounts'
                    }
                },
                {
                    '$unwind': '$discounts'
                },
                {
                    '$project': {
                        'sector': 1,
                        'type': 1,
                        'currency': 1,
                        'sale_start': 1,
                        'sale_end': 1,
                        'departure_start': '$discounts.departure_start',
                        'departure_end': '$discounts.departure_end',
                        'discount': '$discounts.discount'
                    }
                }
            ]
            
            sales_fares_data = list(self.db.config_sale_dates.aggregate(pipeline))
            df = pd.DataFrame(sales_fares_data)
            
            if df.empty:
                self.logger.warning("No sales fare data found")
                return df
            
            # Convert date columns to datetime, handling 'Open' values
            date_columns = ['sale_start', 'sale_end', 'departure_start', 'departure_end']
            for col in date_columns:
                if col in df.columns:
                    # Replace 'Open' with NaN before conversion
                    df[col] = df[col].replace(['Open', 'OPEN', 'open'], pd.NaT)
                    # Convert to datetime
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            
            self.logger.info(f"Collected {len(df)} sales fare records")
            return df
            
        except Exception as e:
            self.logger.error(f"Error collecting sales fare data: {str(e)}")
            raise
    
    def collect_blackout_dates(self):
        """Collect blackout dates data from MongoDB."""
        try:
            blackout_data = list(self.db.config_blackout_dates.find({}, {'_id': 0}))
            df = pd.DataFrame(blackout_data)
            
            if df.empty:
                self.logger.info("No blackout dates found")
                return df
            
            # Convert date columns to datetime
            df['start_date'] = pd.to_datetime(df['start_date'])
            df['end_date'] = pd.to_datetime(df['end_date'])
            
            # Sort by sector and start_date
            df = df.sort_values(['sector', 'start_date'])
            
            self.logger.info(f"Collected {len(df)} blackout date records")
            return df
            
        except Exception as e:
            self.logger.error(f"Error collecting blackout dates data: {str(e)}")
            raise
    
    def collect_station_master(self):
        """Collect station master data from MongoDB."""
        try:
            station_data = list(self.db.config_station_master.find({}, {'_id': 0}))
            df = pd.DataFrame(station_data)
            
            self.logger.info(f"Collected {len(df)} station records")
            return df
            
        except Exception as e:
            self.logger.error(f"Error collecting station master data: {str(e)}")
            raise
    
    def get_current_version(self):
        """Get current version information from MongoDB."""
        try:
            version_data = list(self.db.currentVersion.find({
                'tableName': {
                    '$in': [
                        'config_sale_dates',
                        'config_blackout_dates'
                    ]
                }
            }, {'_id': 0}))
            
            df = pd.DataFrame(version_data)
            self.logger.info("Version information collected successfully")
            return df
            
        except Exception as e:
            self.logger.error(f"Error getting version info: {str(e)}")
            return pd.DataFrame()  # Return empty DataFrame on error
    
    def generate_output_data(self, run_id, context):
        """
        Generate the output data with sales fare data excluding blackout dates.
        Handles special sector formats and open-ended dates.
        
        Args:
            run_id: Unique run identifier
            context: Dictionary containing parse_sector function and other data
        """
        try:
            parse_sector = context['parse_sector']
            split_date_range_by_blackouts = context['split_date_range_by_blackouts']
            sales_fares_data = context['sales_fares_data']
            
            if sales_fares_data is None or sales_fares_data.empty:
                self.logger.error("No sales fares data available")
                return None
            
            output_rows = []
            
            # Filter out rows where discount is null/empty
            # Keep rows where discount is 0 or any other value
            valid_fares = sales_fares_data[sales_fares_data['discount'].notna()].copy()
            
            self.logger.info(f"Processing {len(valid_fares)} fare records with non-empty discounts")
            
            for idx, row in valid_fares.iterrows():
                sector = row['sector']
                departure_start = row['departure_start']
                departure_end = row['departure_end']
                
                # Check if departure_end is 'Open' or blank
                is_open_ended = pd.isna(departure_end) or departure_end == "" or departure_end == "open"
                
                try:
                    # Parse sector to get origin, destination and their types
                    origin, origin_type, destination, destination_type = parse_sector(sector)
                except ValueError as e:
                    self.logger.warning(f"Skipping invalid sector: {str(e)}")
                    continue
                
                # Split the date range by blackouts
                valid_periods = split_date_range_by_blackouts(
                    sector, departure_start, departure_end, is_open_ended
                )
                
                # Create output rows for each valid period
                for period_start, period_end in valid_periods:
                    # Handle period_end which can be None or NaT for open-ended periods
                    travel_end = ''
                    if period_end is not None and pd.notna(period_end):
                        travel_end = period_end.strftime('%m/%d/%Y')
                    
                    output_row = {
                        'run_id': run_id,
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
                self.logger.warning("No output rows generated")
                return output_df
            
            # Save to MongoDB collection
            collection_name = 'sales_fare_output'
            output_records = output_df.to_dict('records')
            self.db[collection_name].insert_many(output_records)
            
            self.logger.info(f"Output data saved to MongoDB collection: {collection_name}")
            self.logger.info(f"Total output rows: {len(output_df)}")
            self.logger.info(f"Run ID: {run_id}")
            
            return output_df
            
        except Exception as e:
            self.logger.error(f"Error generating output data: {str(e)}")
            self.logger.error(traceback.format_exc())
            raise
    
    def save_run_audit(self, analystName, currVersion, run_id, output_df, status, failure_reason=None):
        """Save audit information with enhanced failure tracking."""
        try:
            if status == 'SUCCESS' and (output_df is None or output_df.empty):
                status = 'FAILED'
                if not failure_reason:
                    failure_reason = 'No output generated'
            
            # Calculate statistics
            total_output_rows = len(output_df) if output_df is not None else 0
            
            sales_dates_version = None
            blackout_dates_version = None
            try:
                if currVersion is not None and not currVersion.empty:
                    sales_dates_rows = currVersion[currVersion['table_name'] == 'config_sale_dates']
                    blackout_dates_rows = currVersion[currVersion['table_name'] == 'config_blackout_dates']
                    
                    sales_dates_version = sales_dates_rows['curr_version'].values[0] if not sales_dates_rows.empty else None
                    blackout_dates_version = blackout_dates_rows['curr_version'].values[0] if not blackout_dates_rows.empty else None
            except Exception:
                pass
            
            audit_data = {
                'run_id': run_id,
                'start_time': datetime.now(pytz.timezone('Asia/Kolkata')),
                'end_time': datetime.now(pytz.timezone('Asia/Kolkata')),
                'status': status,
                'total_output_rows': total_output_rows,
                'sales_dates_version': sales_dates_version,
                'blackout_dates_version': blackout_dates_version,
                'analystName': analystName,
                'failure_reason': failure_reason if status == 'FAILED' else None
            }
            
            # Save to MongoDB
            self.db.sales_fare_generation_audits.insert_one(audit_data)
            
            self.logger.info(f"Audit record saved - Run ID: {run_id}, Status: {status}, Output Rows: {total_output_rows}")
            
            return 'sales_fare_generation_audits'
            
        except Exception as e:
            self.logger.error(f"Error saving audit: {str(e)}")
            try:
                # Fallback minimal audit record
                minimal_audit = {
                    'run_id': run_id,
                    'start_time': datetime.now(pytz.timezone('Asia/Kolkata')),
                    'end_time': datetime.now(pytz.timezone('Asia/Kolkata')),
                    'total_output_rows': 0,
                    'status': 'FAILED',
                    'failure_reason': f"Audit save failed: {str(e)}. Original failure: {failure_reason}"
                }
                
                self.db.sales_fare_generation_audits.insert_one(minimal_audit)
                
                self.logger.info("Minimal audit record saved")
                return 'sales_fare_generation_audits'
            except Exception as final_error:
                self.logger.error(f"Failed to save even minimal audit record: {str(final_error)}")
                return None