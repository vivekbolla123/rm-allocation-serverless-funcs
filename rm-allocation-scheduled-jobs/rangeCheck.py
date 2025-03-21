import datetime
import logging
import pandas as pd
from sqlalchemy import text


class QCCheck:

    def __init__(self, logger, params, overrides, context):
        self.logger = logging.getLogger()
        self.rm_rd_conn = params['wrconn']

    def run(self):
        self.check_profile_fare_ranges()

    def check_profile_fare_ranges(self):
        try:
            min_max_query = """
            SELECT * FROM QP_DW_RMALLOC.min_max_profile
            """
            min_max_df = pd.read_sql(min_max_query, self.rm_rd_conn)

            profile_fares_query = """
            SELECT * FROM QP_DW_RMALLOC.config_profile_fares
            """
            profile_fares_df = pd.read_sql(profile_fares_query, self.rm_rd_conn)
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
            q_columns = [col for col in profile_fares_df.columns if col.startswith('Q')]
            print(f"Processing {len(profile_fares_df)} profile fares...")

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
                    fare = row[q_col]

                    if pd.isna(fare):
                        continue

                    if fare < min_fare or fare > max_fare:
                        out_of_range_fares.append({
                            'timestamp': datetime.datetime.now(),
                            'fare_type': 'Profile',
                            'sector': sector,
                            'month': row['month'],
                            'dow': dow,
                            'column_name': q_col,
                            'fare': fare,
                            'min_fare': min_fare,
                            'max_fare': max_fare
                        })

            if out_of_range_fares:
                out_of_range_df = pd.DataFrame(out_of_range_fares)
                try:
                    out_of_range_df.to_sql(
                        'out_of_range_fares',
                        self.rm_rd_conn,
                        schema='QP_RM_AUDITS',
                        if_exists='append',
                        index=False
                    )
                except Exception as e:
                    self.logger.error(f"Error inserting out-of-range fares: {str(e)}")
            else:
                self.logger.info("No out-of-range fares found.")

            return out_of_range_fares

        except Exception as e:
            self.logger.error(f"Error in check_profile_fare_ranges: {str(e)}")
            return []
