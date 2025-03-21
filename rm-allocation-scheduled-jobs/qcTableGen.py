import logging
from pandas import read_sql
from sqlalchemy import text
import pandas as pd

class QCProcess:

    def __init__(self, logger, params, overrides, context):
        self.logger = logging.getLogger()
        self.rm_rd_conn = params['wrconn']
        self.profile_fares = self.fetch_profile_fares()
    
    def fetch_profile_fares(self):
        profile_fares_query = "SELECT * FROM QP_DW_RMALLOC.config_profile_fares"
        df = read_sql(profile_fares_query, self.rm_rd_conn)
        df.columns = [col.lower() for col in df.columns]
        return df
    
    def run(self):
        self.create_profile_fare_table()
        self.create_completeness_table()
        
    def create_profile_fare_table(self):
        df = self.profile_fares
        
        q_columns = [f'q{i}' for i in range(21)]
        
        def process_data(agg_func):
            grouped = df.groupby(['month', 'dow', 'sector'])
            
            result = grouped[q_columns].agg(agg_func)
            
            result_df = result.reset_index()
            result_df['md_min_max'] = result_df['month'] + '_' + result_df['dow'] + '_' + \
                                    ('min' if agg_func == 'min' else 'max')
            
            result_df['fare'] = (result_df[q_columns].apply(agg_func, axis=1) * (0.9 if agg_func == 'min' else 1.1)).round()
            
            return result_df
        
        min_df = process_data('min')
        max_df = process_data('max')
        
        final_df = pd.concat([min_df, max_df])
        
        pivot_df = final_df.pivot_table(
            index=['md_min_max'], 
            columns='sector', 
            values='fare', 
            aggfunc='first'
        ).reset_index()

        with self.rm_rd_conn.connect() as connection:
            connection.execute(text("DROP TABLE IF EXISTS min_max_profile"))
        
        try:
            pivot_df.to_sql(
                'min_max_profile', 
                self.rm_rd_conn, 
                schema='QP_DW_RMALLOC', 
                if_exists='replace',
                index=False
            )
            self.logger.info("Successfully created 'min_max_profile' table.")
        except Exception as e:
            self.logger.error(f"Error creating 'min_max_profile' table: {str(e)}")
        
        return pivot_df
    
    
    def create_completeness_table(self):
        profile_fares = self.profile_fares

        no_show_query = "SELECT * FROM QP_DW_RMALLOC.no_show_probabilities"
        no_show_probabilities = read_sql(no_show_query, self.rm_rd_conn)
        
        no_show_probabilities.columns = [col.lower() for col in no_show_probabilities.columns]

        profile_completeness = (
            profile_fares
            .groupby(['sector', 'month'])
            .size()
            .unstack(fill_value=0)
            .reset_index()
        )
        profile_completeness['type'] = 'profile'

        no_show_completeness = (
            no_show_probabilities
            .groupby(['sector', 'month'])
            .size()
            .unstack(fill_value=0)
            .reset_index()
        )
        no_show_completeness['type'] = 'no_show_probabilities'

        completeness = pd.concat([profile_completeness, no_show_completeness], ignore_index=True)

        with self.rm_rd_conn.connect() as connection:
            connection.execute(text("DROP TABLE IF EXISTS completeness"))

        completeness.to_sql('completeness', self.rm_rd_conn, index=False, if_exists='replace')
        print("Created 'completeness' table.") 