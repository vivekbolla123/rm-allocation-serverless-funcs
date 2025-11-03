import datetime
import logging
import pandas as pd


class QCCheck:

    def __init__(self, logger, params, overrides, context):
        self.logger = logging.getLogger()
        self.mongoconn = params['mongoconn']  # Single MongoDB connection
        
        # MongoDB database
        self.db = self.mongoconn["qp_dw_rmalloc"]  # or self.mongoconn['your_db_name']

    def run(self):
        self.check_profile_fare_ranges()

    def check_profile_fare_ranges(self):
        try:
            # Fetch min_max_profile from MongoDB
            min_max_collection = self.db['min_max_profile']
            min_max_cursor = min_max_collection.find({}, {'_id': 0})
            min_max_df = pd.DataFrame(list(min_max_cursor))
            
            if min_max_df.empty:
                self.logger.warning("min_max_profile collection is empty")
                return []

            # Fetch config_profile_fares from MongoDB
            profile_fares_collection = self.db['config_profile_fares']
            profile_fares_cursor = profile_fares_collection.find({}, {'_id': 0})
            profile_fares_df = pd.DataFrame(list(profile_fares_cursor))
            
            if profile_fares_df.empty:
                self.logger.warning("config_profile_fares collection is empty")
                return []

            # Build nested dictionary from min_max_df
            nested_dict = {}
            for _, row in min_max_df.iterrows():
                md_min_max = row['md_min_max']
                month_dow = md_min_max[:-4]
                range_type = md_min_max[-3:]

                for sector in row.index.drop('md_min_max'):
                    if pd.isna(row[sector]):
                        continue
                    if sector not in nested_dict:
                        nested_dict[sector] = {}
                    if month_dow not in nested_dict[sector]:
                        nested_dict[sector][month_dow] = {}
                    nested_dict[sector][month_dow][range_type] = row[sector]

            out_of_range_fares = []
            q_columns = [col for col in profile_fares_df.columns if col.startswith('Q') or col.startswith('q')]
            self.logger.info(f"Processing {len(profile_fares_df)} profile fares...")

            for _, row in profile_fares_df.iterrows():
                sector = row['sector']
                month_abbr = row['month']
                dow = row['dow']
                month_dow = f"{month_abbr}_{dow}"

                if sector not in nested_dict or month_dow not in nested_dict[sector]:
                    continue

                min_fare = nested_dict[sector][month_dow].get('min')
                max_fare = nested_dict[sector][month_dow].get('max')

                if min_fare is None or max_fare is None:
                    continue

                for q_col in q_columns:
                    fare = row.get(q_col)
                    
                    if fare is None or pd.isna(fare):
                        continue

                    if fare < min_fare or fare > max_fare:
                        out_of_range_fares.append({
                            'timestamp': datetime.datetime.utcnow(),
                            'fare_type': 'Profile',
                            'sector': sector,
                            'month': row['month'],
                            'dow': dow,
                            'column_name': q_col,
                            'fare': float(fare),
                            'min_fare': float(min_fare),
                            'max_fare': float(max_fare)
                        })

            if out_of_range_fares:
                try:
                    # Insert out-of-range fares into MongoDB
                    out_of_range_collection = self.db['out_of_range_fares']
                    result = out_of_range_collection.insert_many(out_of_range_fares)
                    self.logger.info(f"Inserted {len(result.inserted_ids)} out-of-range fare records")
                except Exception as e:
                    self.logger.error(f"Error inserting out-of-range fares: {str(e)}")
            else:
                self.logger.info("No out-of-range fares found.")

            return out_of_range_fares

        except Exception as e:
            self.logger.error(f"Error in check_profile_fare_ranges: {str(e)}")
            return []