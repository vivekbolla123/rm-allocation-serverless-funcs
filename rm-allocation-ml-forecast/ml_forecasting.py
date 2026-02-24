
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
        # Fix #1: BRD says NDO 0-38
        self.NDO_RANGE = range(0, 39)

        # Fix #6: Feature list matching BRD spec exactly
        self.FEATURES = [
            'NDO', 'Sector_Encoded', 'Day_Encoded', 'Time_Bucket', 'Weekend_Flag',
            'month_sin', 'week_cos', 'Lag_1_PLF', 'Sector_Capacity',
            'Lag_1_Corp', 'Lag_1_Group', 'Lag_1_FIT', 'Lag_1_TBF'
        ]

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
    FROM (
        SELECT InventoryLegID, DepartureDate, DepartureStation, 
               ArrivalStation, FlightNumber, STD, AdjustedCapacity
        FROM QP_DL_InventoryLeg
        WHERE DepartureDate >= '{start_date}'
            AND DepartureDate < '{end_date}'
            AND FlightNumber < 7000
    ) IL
    INNER JOIN (
        SELECT InventoryLegID, BookingDate, FareBasis, 
               PaxType, RevenuePax, DomesticFlag
        FROM QP_DL_FLOWNDATAPAX
        WHERE DomesticFlag = 1
            AND FareBasis NOT LIKE 'S%%'
            AND InventoryLegID IS NOT NULL
    ) FP ON IL.InventoryLegID = FP.InventoryLegID
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
            RIGHT JOIN QP_DL_BOOKEDDATAPAX BP ON IL.InventoryLegID = BP.InventoryLegID
            AND BP.DomesticFlag = 1          -- ← Moved here
            AND BP.Farebasis NOT LIKE 'S%%'
            WHERE IL.DepartureDate BETWEEN '{start_date}' AND '{end_date}'
           AND IL.FLightNumber<7000 
        """
        
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
        unique_legs['DepartureDate'] = pd.to_datetime(unique_legs['DepartureDate'])

        # Fix #10: Compute Sector early so it's available for Sector_Capacity
        unique_legs['Sector'] = unique_legs['DepartureStation'] + unique_legs['ArrivalStation']

        # Fix #2: Compute Sector_Capacity = mean(AdjustedCapacity) per Sector + DepartureDate
        unique_legs['Sector_Capacity'] = unique_legs.groupby(
            ['Sector', 'DepartureDate']
        )['AdjustedCapacity'].transform('mean')
            
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
        bookings_df['NDO'] = bookings_df['NDO'].clip(upper=38)
        self.logger.info(f"Bookings shape after filtering: {bookings_df.shape}") 

        # 2. Categorize Pax Types
        # BRD: SUBSTRING(farebasis, 5, 1) = 'C' -> Corporate, 'T' -> TBF, 'G' -> Group
        #      NOT IN ('T','C','G') AND FareBasis Not Like 'S%' -> FIT
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

        # 4. Reindex to Full Grid (All Legs x All NDOs 0-38)
        leg_ids = unique_legs['InventoryLegID'].unique()
        ndo_grid = pd.MultiIndex.from_product([leg_ids, self.NDO_RANGE], names=['InventoryLegID', 'NDO'])
        
        pivot_df = pivot_df.set_index(['InventoryLegID', 'NDO'])
        pivot_df = pivot_df.reindex(ndo_grid, fill_value=0).reset_index()
        
        # Merge back static leg info (now includes Sector and Sector_Capacity)
        pivot_df = pivot_df.merge(unique_legs, on='InventoryLegID', how='left')

        # 5. Cumulative Calculations — sorted descending (NDO 38 -> 0)
        pivot_df = pivot_df.sort_values(by=['InventoryLegID', 'NDO'], ascending=[True, False])
        
        # Calculate cumulative sums (descending: NDO 38 first, cumulating toward NDO 0)
        groupby_cols = ['InventoryLegID']
        pivot_df['Cum_Total'] = pivot_df.groupby(groupby_cols)['TotalPax'].cumsum()
        pivot_df['Cum_Corp'] = pivot_df.groupby(groupby_cols)['Corporate'].cumsum()
        pivot_df['Cum_Group'] = pivot_df.groupby(groupby_cols)['Group'].cumsum()
        pivot_df['Cum_FIT'] = pivot_df.groupby(groupby_cols)['FIT'].cumsum()
        pivot_df['Cum_TBF'] = pivot_df.groupby(groupby_cols)['TBF'].cumsum()

        # Fix #5: Convert ALL cumulative counts to percentages by dividing by AdjustedCapacity
        pivot_df['Cum_PLF'] = pivot_df['Cum_Total'] / pivot_df['AdjustedCapacity']
        pivot_df['Cum_Corp'] = pivot_df['Cum_Corp'] / pivot_df['AdjustedCapacity']
        pivot_df['Cum_Group'] = pivot_df['Cum_Group'] / pivot_df['AdjustedCapacity']
        pivot_df['Cum_FIT'] = pivot_df['Cum_FIT'] / pivot_df['AdjustedCapacity']
        pivot_df['Cum_TBF'] = pivot_df['Cum_TBF'] / pivot_df['AdjustedCapacity']

        # 6. Lag Features (shift(1) on descending-sorted data => previous day's cumulative)
        pivot_df['Lag_1_PLF'] = pivot_df.groupby(groupby_cols)['Cum_PLF'].shift(1)
        pivot_df['Lag_1_Corp'] = pivot_df.groupby(groupby_cols)['Cum_Corp'].shift(1)
        pivot_df['Lag_1_Group'] = pivot_df.groupby(groupby_cols)['Cum_Group'].shift(1)
        pivot_df['Lag_1_FIT'] = pivot_df.groupby(groupby_cols)['Cum_FIT'].shift(1)
        pivot_df['Lag_1_TBF'] = pivot_df.groupby(groupby_cols)['Cum_TBF'].shift(1)

        # Fix #4: BRD imputation — backfill then frontfill per InventoryLegID, then safety fillna(0)
        lag_cols = ['Lag_1_PLF', 'Lag_1_Corp', 'Lag_1_Group', 'Lag_1_FIT', 'Lag_1_TBF']
        pivot_df[lag_cols] = pivot_df.groupby('InventoryLegID')[lag_cols].transform(
            lambda g: g.bfill().ffill()
        )
        pivot_df[lag_cols] = pivot_df[lag_cols].fillna(0)

        # 7. Feature Engineering
        # Sector was already computed on unique_legs and merged in step 4
        pivot_df['Day_of_Week'] = pivot_df['DepartureDate'].dt.dayofweek
        pivot_df['Month'] = pivot_df['DepartureDate'].dt.month
        pivot_df['Week'] = pivot_df['DepartureDate'].dt.isocalendar().week.astype(int)
        
        # Fourier Features
        pivot_df['month_sin'] = np.sin(2 * np.pi * pivot_df['Month'] / 12)
        pivot_df['week_cos'] = np.cos(2 * np.pi * pivot_df['Week'] / 52)
        pivot_df['Weekend_Flag'] = pivot_df['Day_of_Week'].apply(lambda x: 1 if x >= 5 else 0)

        # Time Bucket (BRD clusters)
        def get_time_bucket(std):
            if isinstance(std, str): 
                try: h = int(std.split(':')[0])
                except: h = 12
            elif hasattr(std, 'hour'):
                h = std.hour
            else:
                h = 12

            # BRD: Cluster 0: 02:30–07:30, 1: 07:30–11:00, 2: 11:00–14:30,
            #      3: 14:30–18:00, 4: 18:00–21:00, 5: 21:00–02:30
            if 2 < h <= 7: return 0
            if 7 < h <= 11: return 1
            if 11 < h <= 14: return 2
            if 14 < h <= 18: return 3
            if 18 < h <= 21: return 4
            return 5  # 21:00 - 02:30

        pivot_df['Time_Bucket'] = pivot_df['STD'].apply(get_time_bucket)

        # Target (Final PLF at NDO 0) for Training
        if is_training:
            target_map = pivot_df[pivot_df['NDO'] == 0][['InventoryLegID', 'Cum_PLF']].set_index('InventoryLegID')['Cum_PLF'].to_dict()
            pivot_df['Target_PLF'] = pivot_df['InventoryLegID'].map(target_map)
            # Drop rows where target is NaN (insufficient data)
            pivot_df = pivot_df.dropna(subset=['Target_PLF'])

        return pivot_df

    def train_pipeline(self):
        self.logger.info("Starting Training Pipeline")
        
        # Iterative Month-wise Processing
        start_date = datetime.strptime(self.TRAIN_START_DATE, '%Y-%m-%d')
        current_date = datetime.strptime('2026-02-11', '%Y-%m-%d')
        
        processed_frames = []
        
        iter_date = start_date
        while iter_date < current_date:
            # Define chunk window (1 month)
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
        
        # Fix #3: Day_Encoded using LabelEncoder
        le_day = LabelEncoder()
        df_proc['Day_Encoded'] = le_day.fit_transform(df_proc['Day_of_Week'])
        
        X = df_proc[self.FEATURES]
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
        
        # Save model and both encoders
        self.save_artifacts(model, le_sector, le_day)
        self.logger.info("Training finished.")

    def save_artifacts(self, model, le_sector, le_day):
        # Save Model
        with BytesIO() as f:
            joblib.dump(model, f)
            f.seek(0)
            self.s3.put_object(Bucket=self.bucket, Key=self.model_key, Body=f.read())
            
        # Save Encoders (sector + day)
        with BytesIO() as f:
            joblib.dump({'sector': le_sector, 'day': le_day}, f)
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

        # 2. Preprocess (builds the full NDO grid with cumulative sums, lags, features)
        df_proc = self.preprocess_data(df_raw, is_training=False)
        self.logger.info(f"Scoring data processed shape (full grid): {df_proc.shape}")
        
        # Filter for Current NDO only (DepartureDate - Today)
        today_date = pd.to_datetime(datetime.now().date())
        
        # Calculate the Target NDO for each flight based on today's date
        df_proc['Target_NDO'] = (df_proc['DepartureDate'].dt.floor('D') - today_date).dt.days
        
        # Keep only the row where the NDO matches the Target NDO
        df_proc = df_proc[df_proc['NDO'] == df_proc['Target_NDO']].copy()
        self.logger.info(f"Scoring data filtered for Current NDO shape: {df_proc.shape}")
        
        # Fix #9: Removed manual lag override — the bfill/ffill imputation in
        # preprocess_data already provides correct lag values from the full NDO grid.
        
        # 3. Load Model & Encoders
        model, encoders = self.load_artifacts()
        le_sector = encoders['sector']
        le_day = encoders['day']
        
        # Handle new sectors (Label Encoding)
        known_sectors = set(le_sector.classes_)
        df_proc['Sector_Encoded'] = df_proc['Sector'].apply(
            lambda x: le_sector.transform([x])[0] if x in known_sectors else -1
        )
        
        # Day Encoding (consistent with training)
        known_days = set(le_day.classes_)
        df_proc['Day_Encoded'] = df_proc['Day_of_Week'].apply(
            lambda x: le_day.transform([x])[0] if x in known_days else -1
        )
        
        # 4. Predict
        # Ensure all features exist
        for f in self.FEATURES:
            if f not in df_proc.columns:
                df_proc[f] = 0
                
        X_score = df_proc[self.FEATURES]
        
        # Log feature stats for debugging
        self.logger.info(f"Feature stats before scoring:\n{X_score.describe().to_string()}")
        
        preds = model.predict(X_score)
        # Clip predictions to valid PLF range [0, 1]
        df_proc['Predicted_PLF'] = np.clip(preds, 0.0, 1.0)
        
        # Floor: predicted final PLF can never be less than current cumulative PLF
        # Bookings are cumulative and can only increase toward departure
        df_proc['Predicted_PLF'] = np.maximum(df_proc['Predicted_PLF'], df_proc['Cum_PLF'])
        
        self.logger.info(f"Generated {len(preds)} predictions. PLF range: [{df_proc['Predicted_PLF'].min():.4f}, {df_proc['Predicted_PLF'].max():.4f}]")
        df_proc['Predicted_Bookload'] = df_proc['Predicted_PLF'] * df_proc['AdjustedCapacity']
        
        # 5. Write to DB
        self.save_forecast_results(df_proc)
        self.logger.info("Scoring finished and results saved.")


    def save_forecast_results(self, df):
        # Save to Forecasting_Results_ML table
        # Fix #8: Include STD (DepartureTime) in output as per BRD
        output_df = df[['InventoryLegID', 'DepartureDate', 'FlightNumber', 'Sector',
                        'STD', 'NDO', 'Cum_PLF', 'Predicted_PLF', 'Predicted_Bookload']].copy()
        output_df['CreatedDatetime'] = datetime.now()
        
        try:
            output_df.to_sql('Forecasting_Results_ML', self.dwconn, if_exists='append', index=False, chunksize=1000)
        except Exception as e:
            self.logger.error(f"Failed to write results to DB: {e}")
