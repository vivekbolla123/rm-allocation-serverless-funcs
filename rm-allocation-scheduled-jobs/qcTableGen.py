import logging
import pandas as pd

class QCProcess:

    def __init__(self, logger, params, overrides, context):
        self.logger = logging.getLogger()
        self.mongoconn = params['mongoconn']  # Single MongoDB connection
        
        # MongoDB database
        self.db = self.mongoconn["qp_dw_rmalloc"]  # or self.mongoconn['your_db_name']
        
        self.profile_fares = self.fetch_profile_fares()
    
    def fetch_profile_fares(self):
        """
        Fetch profile fares from MongoDB collection
        """
        collection = self.db['config_profile_fares']
        
        # Fetch all documents
        cursor = collection.find({}, {'_id': 0})
        df = pd.DataFrame(list(cursor))
        
        if df.empty:
            self.logger.warning("No profile fares found in MongoDB")
            return pd.DataFrame()
        
        # Ensure column names are lowercase
        df.columns = [col.lower() for col in df.columns]
        return df
    
    def run(self):
        self.create_profile_fare_table()
        self.create_completeness_table()
        
    def create_profile_fare_table(self):
        """
        Create min_max_profile collection in MongoDB
        """
        df = self.profile_fares
        
        if df.empty:
            self.logger.warning("Profile fares DataFrame is empty, skipping min_max_profile creation")
            return None
        
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
        
        try:
            # Convert DataFrame to list of dictionaries
            records = pivot_df.to_dict('records')
            
            # Drop existing collection and recreate
            collection = self.db['min_max_profile']
            collection.drop()
            
            if records:
                # Insert new documents
                collection.insert_many(records)
                self.logger.info(f"Successfully created 'min_max_profile' collection with {len(records)} documents.")
            else:
                self.logger.warning("No records to insert into 'min_max_profile' collection.")
            
        except Exception as e:
            self.logger.error(f"Error creating 'min_max_profile' collection: {str(e)}")
        
        return pivot_df
    
    
    def create_completeness_table(self):
        """
        Create completeness collection in MongoDB
        """
        profile_fares = self.profile_fares
        
        if profile_fares.empty:
            self.logger.warning("Profile fares DataFrame is empty, skipping completeness creation")
            return
        
        # Fetch no_show_probabilities from MongoDB
        collection = self.db['no_show_probabilities']
        cursor = collection.find({}, {'_id': 0})
        no_show_probabilities = pd.DataFrame(list(cursor))
        
        if no_show_probabilities.empty:
            self.logger.warning("No show probabilities collection is empty")
            no_show_probabilities = pd.DataFrame(columns=['sector', 'month'])
        else:
            no_show_probabilities.columns = [col.lower() for col in no_show_probabilities.columns]

        # Create profile completeness
        profile_completeness = (
            profile_fares
            .groupby(['sector', 'month'])
            .size()
            .unstack(fill_value=0)
            .reset_index()
        )
        profile_completeness['type'] = 'profile'

        # Create no_show completeness
        if not no_show_probabilities.empty and 'sector' in no_show_probabilities.columns and 'month' in no_show_probabilities.columns:
            no_show_completeness = (
                no_show_probabilities
                .groupby(['sector', 'month'])
                .size()
                .unstack(fill_value=0)
                .reset_index()
            )
            no_show_completeness['type'] = 'no_show_probabilities'
            
            completeness = pd.concat([profile_completeness, no_show_completeness], ignore_index=True)
        else:
            self.logger.warning("Skipping no_show completeness due to missing data")
            completeness = profile_completeness

        try:
            # Convert DataFrame to list of dictionaries
            records = completeness.to_dict('records')
            
            # Drop existing collection and recreate
            collection = self.db['completeness']
            collection.drop()
            
            if records:
                # Insert new documents
                collection.insert_many(records)
                self.logger.info(f"Successfully created 'completeness' collection with {len(records)} documents.")
            else:
                self.logger.warning("No records to insert into 'completeness' collection.")
            
        except Exception as e:
            self.logger.error(f"Error creating 'completeness' collection: {str(e)}")