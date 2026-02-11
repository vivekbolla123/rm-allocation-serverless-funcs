
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import joblib
from io import BytesIO
from xgboost import XGBRegressor
from sklearn.preprocessing import LabelEncoder

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class MLForecasting:
    def __init__(self, logger, params, overrides, context):
        self.logger = logger
        self.params = params
        self.overrides = overrides
        self.context = context
        self.dwconn = params.get("dwconn")
        self.esbconn = params.get("esbconn")
        self.s3 = params.get("s3")
        self.bucket = "uat-rm-allocation-ml-models"
        self.model_key = "ml_forecast_model.pkl"
        self.encoders_key = "ml_encoders.pkl"
        
        # BRD Constants
        self.TRAIN_START_DATE = '2024-01-01'
        self.NDO_RANGE = range(0, 31) 

    def run(self):
        try:
            action = self.overrides.get("action", "score")
            self.logger.info(f"Starting ML Forecasting with action: {action}")
            
            if action == "train":
                self.train_pipeline()
            elif action == "score":
                self.score_pipeline()
            else:
                self.logger.error(f"Invalid apply_action: {action}")
                
        except Exception as e:
            self.logger.error(f"Error in MLForecasting run: {e}")
            raise

    def fetch_training_data(self, start_date, end_date):
        self.logger.info(f"Fetching training data from {start_date} to {end_date}")
        
        # Using QP_DL_FLOWNDATAPAX for historical training data
        query = f"""
            SELECT 
                IL.InventoryLegID,
                IL.DepartureDate,
                IL.DepartureStation,
                IL.ArrivalStation,
                IL.FlightNumber,
                IL.STD,
                IL.AdjustedCapacity,
                FP.BookingDate,
                FP.FareBasis,
                FP.PaxType,
                FP.RevenuePax,
                FP.DomesticFlag
            FROM QP_DL_InventoryLeg IL
            JOIN QP_DL_FLOWNDATAPAX FP ON IL.InventoryLegID = FP.InventoryLegID
            WHERE IL.DepartureDate >= '{start_date}' AND IL.DepartureDate < '{end_date}'
            AND FP.DomesticFlag = 1
        """
        
        try:
            df = pd.read_sql(query, self.esbconn)
            self.logger.info(f"Fetched {len(df)} rows of training data.")
            return df
        except Exception as e:
            self.logger.error(f"Error fetching training data: {e}")
            return pd.DataFrame()

    def fetch_scoring_data(self, start_date, end_date):
        # Using QP_DL_BOOKEDDATAPAX for future/scoring data
        self.logger.info(f"Fetching scoring data from {start_date} to {end_date}")
        query = f"""
            SELECT 
                IL.InventoryLegID,
                IL.DepartureDate,
                IL.DepartureStation,
                IL.ArrivalStation,
                IL.FlightNumber,
                IL.STD,
                IL.AdjustedCapacity,
                BP.BookingDate,
                BP.FareBasis,
                BP.PaxType,
                BP.RevenuePax,
                BP.DomesticFlag
            FROM QP_DL_InventoryLeg IL
            LEFT JOIN QP_DL_BOOKEDDATAPAX BP ON IL.InventoryLegID = BP.InventoryLegID
            WHERE IL.DepartureDate BETWEEN '{start_date}' AND '{end_date}'
            AND (BP.DomesticFlag = 1 OR BP.DomesticFlag IS NULL) -- Handle joins where no bookings yet (future)
        """
        # Note: If no bookings exist (Left Join is NULL), DomesticFlag is NULL. 
        # We might need to filter Domestic on InventoryLeg level if possible, but schema for IL wasn't fully detailed with DomesticFlag in user prompt.
        # Assuming we filter what we can. If BP is null, we strictly keep the leg for scoring (0 bookings).
        
        df = pd.read_sql(query, self.esbconn)
        self.logger.info(f"Fetched {len(df)} rows of scoring data.")
        return df

    def preprocess_data(self, df, is_training=True):
        self.logger.info("Preprocessing data...")
        self.logger.info(f"Input DataFrame shape: {df.shape}")
        
        if df.empty:
            return df
            
        # 0. Extract Unique Legs explicitly to preserve them even if they have no bookings
        leg_cols = ['InventoryLegID', 'DepartureDate', 'DepartureStation', 'ArrivalStation', 'FlightNumber', 'STD', 'AdjustedCapacity']
        unique_legs = df[leg_cols].drop_duplicates()
        unique_legs['DepartureDate'] = pd.to_datetime(unique_legs['DepartureDate']) # Ensure correct type for merge
            
        # 1. Standardize Dates and Calculate NDO
        df['DepartureDate'] = pd.to_datetime(df['DepartureDate'])
        df['BookingDate'] = pd.to_datetime(df['BookingDate'])
        
        # Handle cases where BookingDate is NaT 
        # For pivoting pax counts, we only look at rows with valid bookings
        bookings_df = df.dropna(subset=['BookingDate']).copy()
        
        # Ensure BookingDate is not later than DepartureDate (data errors)
        bookings_df = bookings_df[bookings_df['BookingDate'] <= bookings_df['DepartureDate']]
        
        bookings_df['NDO'] = (bookings_df['DepartureDate'] - bookings_df['BookingDate']).dt.days
        # Filter NDOs relevant for the model
        bookings_df = bookings_df[bookings_df['NDO'] >= 0] 
        self.logger.info(f"Bookings shape after filtering: {bookings_df.shape}") 

        # 2. Categorize Pax Types
        def categorize_pax(row):
            fb = str(row['FareBasis']) if pd.notna(row['FareBasis']) else ""
            if len(fb) >= 5:
                char_code = fb[4] 
                if char_code == 'C': return 'Corporate'
                if char_code == 'T': return 'TBF'
                if char_code == 'G': return 'Group'
            if not fb.startswith('S'): 
                return 'FIT'
            return 'Other'

        bookings_df['PaxCategory'] = bookings_df.apply(categorize_pax, axis=1)
        bookings_df = bookings_df[bookings_df['PaxCategory'] != 'Other'] 

        # 3. Aggregating Counts per ID, NDO, Category
        # Note: We pivot on bookings_df, so IDs with 0 bookings won't be here yet
        pivot_df = bookings_df.pivot_table(
            index=['InventoryLegID', 'NDO'],
            columns='PaxCategory',
            values='RevenuePax',
            aggfunc='sum'
        ).fillna(0).reset_index()
        self.logger.info(f"Pivot table (bookings only) shape: {pivot_df.shape}")

        # Ensure all columns exist
        for col in ['Corporate', 'Group', 'FIT', 'TBF']:
            if col not in pivot_df.columns:
                pivot_df[col] = 0

        pivot_df['TotalPax'] = pivot_df['Corporate'] + pivot_df['Group'] + pivot_df['FIT'] + pivot_df['TBF']

        # 4. Reindex to Full Grid (All Legs x All NDOs)
        # Use legitimate unique_legs extracted from raw input (including those with 0 bookings)
        leg_ids = unique_legs['InventoryLegID'].unique()
        ndo_grid = pd.MultiIndex.from_product([leg_ids, self.NDO_RANGE], names=['InventoryLegID', 'NDO'])
        
        pivot_df = pivot_df.set_index(['InventoryLegID', 'NDO'])
        pivot_df = pivot_df.reindex(ndo_grid, fill_value=0).reset_index()
        
        # Merge back static leg info
        pivot_df = pivot_df.merge(unique_legs, on='InventoryLegID', how='left')

        # 5. Cumulative Calculations
        # Sort by ID and NDO (descending 30->0). 
        pivot_df = pivot_df.sort_values(by=['InventoryLegID', 'NDO'], ascending=[True, False])
        
        # Calculate cumulative sums
        groupby_cols = ['InventoryLegID']
        pivot_df['Cum_Total'] = pivot_df.groupby(groupby_cols)['TotalPax'].cumsum()
        pivot_df['Cum_Corp'] = pivot_df.groupby(groupby_cols)['Corporate'].cumsum()
        pivot_df['Cum_Group'] = pivot_df.groupby(groupby_cols)['Group'].cumsum()
        pivot_df['Cum_FIT'] = pivot_df.groupby(groupby_cols)['FIT'].cumsum()
        pivot_df['Cum_TBF'] = pivot_df.groupby(groupby_cols)['TBF'].cumsum()

        # 6. Normalize by Capacity (PLF)
        pivot_df['Cum_PLF'] = pivot_df['Cum_Total'] / pivot_df['AdjustedCapacity']
        
        # 7. Lag Features (Lag_1)
        pivot_df['Lag_1_PLF'] = pivot_df.groupby(groupby_cols)['Cum_PLF'].shift(1) # Correct if sorted (30->0) row N is 29, N-1 is 30.
        pivot_df['Lag_1_Corp'] = pivot_df.groupby(groupby_cols)['Cum_Corp'].shift(1)
        pivot_df['Lag_1_Group'] = pivot_df.groupby(groupby_cols)['Cum_Group'].shift(1)
        pivot_df['Lag_1_FIT'] = pivot_df.groupby(groupby_cols)['Cum_FIT'].shift(1)
        pivot_df['Lag_1_TBF'] = pivot_df.groupby(groupby_cols)['Cum_TBF'].shift(1)

        # Impute Lags (ffill/bfill as per BRD)
        pivot_df = pivot_df.fillna(0) # Simple strategy for now, refine if BRD specifies "Backfill" for start.

        # 8. Feature Engineering
        # Date Features
        pivot_df['Sector'] = pivot_df['DepartureStation'] + pivot_df['ArrivalStation']
        pivot_df['Day_of_Week'] = pivot_df['DepartureDate'].dt.dayofweek
        pivot_df['Month'] = pivot_df['DepartureDate'].dt.month
        pivot_df['Week'] = pivot_df['DepartureDate'].dt.isocalendar().week.astype(int)
        
        # Fourier Features
        pivot_df['month_sin'] = np.sin(2 * np.pi * pivot_df['Month'] / 12)
        pivot_df['week_cos'] = np.cos(2 * np.pi * pivot_df['Week'] / 52)
        pivot_df['Weekend_Flag'] = pivot_df['Day_of_Week'].apply(lambda x: 1 if x >= 5 else 0)

        # Time Bucket
        def get_time_bucket(std):
            # std is datetime or time. Extract hour.
            if isinstance(std, str): 
                # Parse if string (hh:mm:ss)
                try: h = int(std.split(':')[0])
                except: h = 12
            elif hasattr(std, 'components'): # pandas Timestamp or Time
                 h = std.hour
            else:
                 h = 12 # Default
            
            # Simple approximation of clusters based on BRD ranges
            if 2 < h <= 7: return 0
            if 7 < h <= 11: return 1
            if 11 < h <= 14: return 2
            if 14 < h <= 18: return 3
            if 18 < h <= 21: return 4
            return 5 # 21:00 - 02:30

        pivot_df['Time_Bucket'] = pivot_df['STD'].apply(get_time_bucket)

        # Target (Final PLF) for Training
        if is_training:
            # Target is the Bookload at NDO 0 (or final). 
            target_map = pivot_df[pivot_df['NDO'] == 0][['InventoryLegID', 'Cum_PLF']].set_index('InventoryLegID')['Cum_PLF'].to_dict()
            pivot_df['Target_PLF'] = pivot_df['InventoryLegID'].map(target_map)
            # Drop rows where target is NaN (insufficient data)
            pivot_df = pivot_df.dropna(subset=['Target_PLF'])

        return pivot_df

    def train_pipeline(self):
        self.logger.info("Starting Training Pipeline")
        
        # Iterative Month-wise Processing
        start_date = datetime.strptime(self.TRAIN_START_DATE, '%Y-%m-%d')
        current_date = datetime.now()
        
        processed_frames = []
        
        iter_date = start_date
        while iter_date < current_date:
            # Define chunk window (1 month)
            # Find next month start
            if iter_date.month == 12:
                next_month = iter_date.replace(year=iter_date.year + 1, month=1, day=1)
            else:
                next_month = iter_date.replace(month=iter_date.month + 1, day=1)
            
            # Bound by current date
            chunk_end = min(next_month, current_date)
            
            str_start = iter_date.strftime('%Y-%m-%d')
            str_end = chunk_end.strftime('%Y-%m-%d')
            
            self.logger.info(f"Processing chunk: {str_start} to {str_end}")
            
            df_chunk_raw = self.fetch_training_data(str_start, str_end)
            if not df_chunk_raw.empty:
                self.logger.info(f"Chunk {str_start} to {str_end}: {len(df_chunk_raw)} raw rows")
                df_chunk_proc = self.preprocess_data(df_chunk_raw, is_training=True)
                if not df_chunk_proc.empty:
                    self.logger.info(f"Chunk {str_start} to {str_end}: {len(df_chunk_proc)} processed rows")
                    processed_frames.append(df_chunk_proc)
            
            iter_date = next_month

        if not processed_frames:
            self.logger.warning("No training data found after processing all chunks.")
            return
            
        # Concatenate all processed chunks
        df_proc = pd.concat(processed_frames, ignore_index=True)
        self.logger.info(f"Total processed records for training: {len(df_proc)}")
        
        # Encoders
        le_sector = LabelEncoder()
        df_proc['Sector_Encoded'] = le_sector.fit_transform(df_proc['Sector'])
        
        features = ['NDO', 'Sector_Encoded', 'Time_Bucket', 'Weekend_Flag', 
                    'month_sin', 'week_cos', 'Lag_1_PLF', 'AdjustedCapacity',
                    'Lag_1_Corp', 'Lag_1_Group', 'Lag_1_FIT', 'Lag_1_TBF']
        
        X = df_proc[features]
        y = df_proc['Target_PLF']
        
        model = XGBRegressor(
            colsample_bytree=0.67394,
            n_estimators=141,
            max_depth=4,
            learning_rate=0.132,
            gamma=0.467,
            subsample=0.865,
            min_child_weight=2,
            reg_alpha=0.926,
            reg_lambda=1.59,
            random_state=42,
            n_jobs=-1
        )
        model.fit(X, y)
        
        self.save_artifacts(model, le_sector)
        self.logger.info("Training finished.")

    def save_artifacts(self, model, le_sector):
        # Save Model
        with BytesIO() as f:
            joblib.dump(model, f)
            f.seek(0)
            self.s3.put_object(Bucket=self.bucket, Key=self.model_key, Body=f.read())
            
        # Save Encoders
        with BytesIO() as f:
            joblib.dump({'sector': le_sector}, f)
            f.seek(0)
            self.s3.put_object(Bucket=self.bucket, Key=self.encoders_key, Body=f.read())

    def load_artifacts(self):
        # Load Model
        response = self.s3.get_object(Bucket=self.bucket, Key=self.model_key)
        model = joblib.load(BytesIO(response['Body'].read()))
        
        # Load Encoders
        response = self.s3.get_object(Bucket=self.bucket, Key=self.encoders_key)
        encoders = joblib.load(BytesIO(response['Body'].read()))
        
        return model, encoders

    def score_pipeline(self):
        self.logger.info("Starting Scoring Pipeline")
        
        # 1. Fetch Future Flights (Next 30 days)
        start_date = datetime.now().strftime('%Y-%m-%d')
        end_date = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
        
        df_raw = self.fetch_scoring_data(start_date, end_date)
        if df_raw.empty:
            self.logger.warning("No future flights found for scoring.")
            return

        # 2. Preprocess
        # Reuse preprocess_data but handle is_training=False
        df_proc = self.preprocess_data(df_raw, is_training=False)
        self.logger.info(f"Scoring data processed shape (full grid): {df_proc.shape}")
        
        # Filter for Current NDO only (DepartureDate - Today)
        # We calculate NDO based on Date difference (ignoring time) to match the business logic of "Today's NDO"
        today_date = pd.to_datetime(datetime.now().date())
        
        # Calculate the Target NDO for each flight based on today's date
        df_proc['Target_NDO'] = (df_proc['DepartureDate'].dt.floor('D') - today_date).dt.days
        
        # Filter: Keep only the row where the NDO matches the Target NDO
        df_proc = df_proc[df_proc['NDO'] == df_proc['Target_NDO']]
        self.logger.info(f"Scoring data filtered for Current NDO shape: {df_proc.shape}")
        
        # 3. Load Model & Encoders
        model, encoders = self.load_artifacts()
        le_sector = encoders['sector']
        
        # Handle new sectors (Label Encoding)
        # Identify known classes
        known_sectors = set(le_sector.classes_)
        # Replace unknown sectors with a placeholder or handle gracefully
        # Simple approach: Map unknown to -1 or a specific class if possible. 
        # Here we mask them.
        df_proc['Sector_Encoded'] = df_proc['Sector'].apply(lambda x: le_sector.transform([x])[0] if x in known_sectors else -1)
        
        # 4. Predict
        features = ['NDO', 'Sector_Encoded', 'Time_Bucket', 'Weekend_Flag', 
                    'month_sin', 'week_cos', 'Lag_1_PLF', 'AdjustedCapacity',
                    'Lag_1_Corp', 'Lag_1_Group', 'Lag_1_FIT', 'Lag_1_TBF']
        
        # Ensure Validation of features existence
        for f in features:
            if f not in df_proc.columns:
                df_proc[f] = 0
                
        X_score = df_proc[features]
        # Handle -1 in Sector_Encoded if XGBoost can't handle it (it can handle splits, but -1 is distinct)
        
        preds = model.predict(X_score)
        df_proc['Predicted_PLF'] = preds
        self.logger.info(f"Generated {len(preds)} predictions.")
        df_proc['Predicted_Bookload'] = df_proc['Predicted_PLF'] * df_proc['AdjustedCapacity']
        
        # 5. Write to DB
        self.save_forecast_results(df_proc)
        self.logger.info("Scoring finished and results saved.")


    def save_forecast_results(self, df):
        # Save to Forecasting_Results_ML table
        # Schema: InventoryLegID, DepartureDate, FlightNumber, Sector, Predicted_PLF, etc.
        
        output_df = df[['InventoryLegID', 'DepartureDate', 'FlightNumber', 'Sector', 'NDO', 'Predicted_PLF', 'Predicted_Bookload']].copy()
        output_df['CreatedDatetime'] = datetime.now()
        
        # Using to_sql if permitted, or custom insert
        # Check if table exists, create if not (manual or auto)
        
        # For this implementation, we assume table exists or we append
        try:
            output_df.to_sql('Forecasting_Results_ML', self.dwconn, if_exists='append', index=False, chunksize=1000)
        except Exception as e:
            self.logger.error(f"Failed to write results to DB: {e}")
            # Fallback or strict fail

