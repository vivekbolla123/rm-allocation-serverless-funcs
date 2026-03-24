import logging
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pickle
import configsettings
 
 
class MLForecastScore:
    def __init__(self, logger, params, overrides, context):
        self.logger = logger
        self.params = params
        self.overrides = overrides
        self.context = context
        self.dwconn = params.get("dwconn")
        self.esbconn = params.get("rdEsbConn")
        self.s3 = params.get("s3")
        self.s3_old = params.get("s3_old")
 
        
        self.s3_bucket = configsettings.ML_FORECAST_SCORE_S3_BUCKET_NAME 
        self.s3_base_path = configsettings.ML_FORECAST_SCORE_S3_FILE_PATH
 
        # Model artifacts stored in mlforecast/models/ folder
        self.model_key = f"{self.s3_base_path}/mlforecast/models/xgb_plf_latest.pkl"
        self.sector_mapping_key = f"{self.s3_base_path}/mlforecast/models/sector_mapping_latest.pkl"
        self.day_mapping_key = f"{self.s3_base_path}/mlforecast/models/day_mapping_latest.pkl"
 
        # BRD Constants — use 0-299 grid to match reference (wider grid for proper lag propagation)
        self.NDO_RANGE = range(0, 300)
 
        self.FEATURES = [
            'Sector_encoded', 'day_encoded', 'Time_Bucket', 'NDO',
            'Lag_1', 'weekend_flag', 'month_sin', 'week_cos',
            'Sector_Capacity', 'Lag_1_corporate', 'Lag_1_tbf',
            'Lag_1_groups', 'Lag_1_FIT'
        ]
 
    def run(self):
        try:
            self.logger.info("Starting ML Forecast Scoring")
            self.score_pipeline()
        except Exception as e:
            self.logger.error(f"Error in MLForecastScore run: {e}")
            raise
 
    def fetch_scoring_data(self, start_date, end_date):
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
            AND BP.DomesticFlag = 1
            AND BP.Farebasis NOT LIKE 'S%%'
            WHERE IL.DepartureDate BETWEEN '{start_date}' AND '{end_date}'
           AND IL.FLightNumber<7000 
        """
        df = pd.read_sql(query, self.esbconn)
        self.logger.info(f"Fetched {len(df)} rows of scoring data.")
        return df
 
    def preprocess_data(self, df):
        """Preprocess scoring data into feature-engineered DataFrame."""
        self.logger.info("Preprocessing data...")
        self.logger.info(f"Input DataFrame shape: {df.shape}")
 
        if df.empty:
            return df
 
        # 0. Extract Unique Legs
        leg_cols = [
            'InventoryLegID', 'DepartureDate', 'DepartureStation',
            'ArrivalStation', 'FlightNumber', 'STD', 'AdjustedCapacity'
        ]
        unique_legs = df[leg_cols].drop_duplicates()
        unique_legs['DepartureDate'] = pd.to_datetime(unique_legs['DepartureDate'])
        unique_legs['Sector'] = unique_legs['DepartureStation'] + unique_legs['ArrivalStation']
 
        # Sector_Capacity = mean(AdjustedCapacity) per Sector + DepartureDate
        unique_legs['Sector_Capacity'] = unique_legs.groupby(
            ['Sector', 'DepartureDate']
        )['AdjustedCapacity'].transform('mean')
 
        # 1. Standardize Dates and Calculate NDO
        df['DepartureDate'] = pd.to_datetime(df['DepartureDate'])
        df['BookingDate'] = pd.to_datetime(df['BookingDate'])
 
        bookings_df = df.dropna(subset=['BookingDate']).copy()
        bookings_df = bookings_df[bookings_df['BookingDate'] <= bookings_df['DepartureDate']]
        bookings_df['NDO'] = (bookings_df['DepartureDate'] - bookings_df['BookingDate']).dt.days
        bookings_df = bookings_df[(bookings_df['NDO'] >= 0) & (bookings_df['NDO'] <= 299)]
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
        pivot_df = bookings_df.pivot_table(
            index=['InventoryLegID', 'NDO'],
            columns='PaxCategory',
            values='RevenuePax',
            aggfunc='sum'
        ).fillna(0).reset_index()
        self.logger.info(f"Pivot table shape: {pivot_df.shape}")
 
        for col in ['Corporate', 'Group', 'FIT', 'TBF']:
            if col not in pivot_df.columns:
                pivot_df[col] = 0
 
        pivot_df['TotalPax'] = pivot_df['Corporate'] + pivot_df['Group'] + pivot_df['FIT'] + pivot_df['TBF']
 
        # 4. Reindex to Full Grid (All Legs x All NDOs)
        leg_ids = unique_legs['InventoryLegID'].unique()
        ndo_grid = pd.MultiIndex.from_product([leg_ids, self.NDO_RANGE], names=['InventoryLegID', 'NDO'])
 
        pivot_df = pivot_df.set_index(['InventoryLegID', 'NDO'])
        pivot_df = pivot_df.reindex(ndo_grid).reset_index()
 
        # Fill pax columns with 0 for cumsum (NaN would break cumsum)
        pax_cols = ['Corporate', 'Group', 'FIT', 'TBF', 'TotalPax']
        pivot_df[pax_cols] = pivot_df[pax_cols].fillna(0)
 
        # Merge back static leg info
        pivot_df = pivot_df.merge(unique_legs, on='InventoryLegID', how='left')
 
        # 5. Cumulative Calculations — sorted descending (NDO high -> 0)
        pivot_df = pivot_df.sort_values(by=['InventoryLegID', 'NDO'], ascending=[True, False])
 
        groupby_cols = ['InventoryLegID']
        pivot_df['Cum_Total'] = pivot_df.groupby(groupby_cols)['TotalPax'].cumsum()
        pivot_df['Cum_Corp'] = pivot_df.groupby(groupby_cols)['Corporate'].cumsum()
        pivot_df['Cum_Group'] = pivot_df.groupby(groupby_cols)['Group'].cumsum()
        pivot_df['Cum_FIT'] = pivot_df.groupby(groupby_cols)['FIT'].cumsum()
        pivot_df['Cum_TBF'] = pivot_df.groupby(groupby_cols)['TBF'].cumsum()
 
        # Cum_PLF as percentage (0-100)
        pivot_df['Cum_PLF'] = (pivot_df['Cum_Total'] / pivot_df['AdjustedCapacity']) * 100
        pivot_df['Cum_Corp'] = (pivot_df['Cum_Corp'] / pivot_df['AdjustedCapacity']) * 100
        pivot_df['Cum_Group'] = (pivot_df['Cum_Group'] / pivot_df['AdjustedCapacity']) * 100
        pivot_df['Cum_FIT'] = (pivot_df['Cum_FIT'] / pivot_df['AdjustedCapacity']) * 100
        pivot_df['Cum_TBF'] = (pivot_df['Cum_TBF'] / pivot_df['AdjustedCapacity']) * 100
 
        # Set each cumulative to NaN where that specific category has zero cumulative pax
        pivot_df['Cum_PLF'] = np.where(pivot_df['Cum_Total'] == 0, np.nan, pivot_df['Cum_PLF'])
        pivot_df['Cum_Corp'] = np.where(pivot_df.groupby(groupby_cols)['Corporate'].cumsum() == 0, np.nan, pivot_df['Cum_Corp'])
        pivot_df['Cum_Group'] = np.where(pivot_df.groupby(groupby_cols)['Group'].cumsum() == 0, np.nan, pivot_df['Cum_Group'])
        pivot_df['Cum_FIT'] = np.where(pivot_df.groupby(groupby_cols)['FIT'].cumsum() == 0, np.nan, pivot_df['Cum_FIT'])
        pivot_df['Cum_TBF'] = np.where(pivot_df.groupby(groupby_cols)['TBF'].cumsum() == 0, np.nan, pivot_df['Cum_TBF'])
 
        # 6. Lag Features — shift only, lags will be NaN where no bookings exist
        pivot_df['Lag_1'] = pivot_df.groupby(groupby_cols)['Cum_PLF'].shift(1)
        pivot_df['Lag_1_corporate'] = pivot_df.groupby(groupby_cols)['Cum_Corp'].shift(1)
        pivot_df['Lag_1_groups'] = pivot_df.groupby(groupby_cols)['Cum_Group'].shift(1)
        pivot_df['Lag_1_FIT'] = pivot_df.groupby(groupby_cols)['Cum_FIT'].shift(1)
        pivot_df['Lag_1_tbf'] = pivot_df.groupby(groupby_cols)['Cum_TBF'].shift(1)
 
        # 7. Feature Engineering
        pivot_df['day_name'] = pivot_df['DepartureDate'].dt.day_name()
        pivot_df['Month'] = pivot_df['DepartureDate'].dt.month
        pivot_df['Week'] = pivot_df['DepartureDate'].dt.isocalendar().week.astype(int)
 
        pivot_df['month_sin'] = np.sin(2 * np.pi * pivot_df['Month'] / 12)
        pivot_df['week_cos'] = np.cos(2 * np.pi * pivot_df['Week'] / 52)
        pivot_df['weekend_flag'] = np.where(pivot_df['day_name'].isin(['Saturday', 'Sunday']), 1, 0)
 
        # Time Bucket
        from datetime import time as dt_time
 
        def get_time_bucket(std):
            if isinstance(std, str):
                try:
                    t = pd.to_datetime(std).time()
                except:
                    t = dt_time(12, 0)
            elif hasattr(std, 'time') and callable(std.time):
                t = std.time()
            elif hasattr(std, 'hour'):
                t = std
            else:
                t = dt_time(12, 0)
 
            if dt_time(2, 30) <= t < dt_time(7, 30): return 0
            if dt_time(7, 30) <= t < dt_time(11, 0): return 1
            if dt_time(11, 0) <= t < dt_time(14, 30): return 2
            if dt_time(14, 30) <= t < dt_time(18, 0): return 3
            if dt_time(18, 0) <= t < dt_time(21, 0): return 4
            return 5
 
        pivot_df['Time_Bucket'] = pivot_df['STD'].apply(get_time_bucket)
 
        return pivot_df
 
    def load_artifacts(self):
        # Load Model
        response = self.s3_old.get_object(Bucket=self.s3_bucket, Key=self.model_key)
        model = pickle.loads(response['Body'].read())
 
        # Load Sector Mapping dict
        response = self.s3_old.get_object(Bucket=self.s3_bucket, Key=self.sector_mapping_key)
        sector_mapping = pickle.loads(response['Body'].read())
 
        # Load Day Mapping dict
        response = self.s3_old.get_object(Bucket=self.s3_bucket, Key=self.day_mapping_key)
        day_mapping = pickle.loads(response['Body'].read())
 
        return model, sector_mapping, day_mapping
 
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
        df_proc = self.preprocess_data(df_raw)
        self.logger.info(f"Scoring data processed shape (full grid): {df_proc.shape}")
 
        # Filter NDO <= 100 first (matches reference), then filter for current NDO
        df_proc = df_proc[(df_proc['NDO'] >= 0) & (df_proc['NDO'] <= 100)].reset_index(drop=True)
 
        today_date = pd.to_datetime(datetime.now().date())
        df_proc['Target_NDO'] = (df_proc['DepartureDate'].dt.floor('D') - today_date).dt.days
        df_proc = df_proc[df_proc['NDO'] == df_proc['Target_NDO']].copy()
 
        # Filter NDO <= 30 for final scoring (matches reference)
        df_proc = df_proc[df_proc['NDO'] <= 30].reset_index(drop=True)
        self.logger.info(f"Scoring data filtered for Current NDO shape: {df_proc.shape}")
 
        # 3. Load Model & Mappings
        model, sector_mapping, day_mapping = self.load_artifacts()
 
        # Sector Encoding — dict-based mapping matching reference
        df_proc['Sector_encoded'] = df_proc['Sector'].map(sector_mapping).fillna(-1).astype(int)
 
        # Day Encoding — map day_name strings matching reference
        df_proc['day_encoded'] = df_proc['day_name'].map(day_mapping)
 
        # 4. Predict
        for f in self.FEATURES:
            if f not in df_proc.columns:
                df_proc[f] = 0
 
        X_score = df_proc[self.FEATURES]
 
        self.logger.info(f"Feature stats before scoring:\n{X_score.describe().to_string()}")
 
        preds = model.predict(X_score)
        # Clip predictions to valid PLF percentage range [0, 100]
        df_proc['ForecastPLF'] = preds
 
        self.logger.info(f"Generated {len(preds)} predictions. PLF range: [{df_proc['ForecastPLF'].min():.4f}, {df_proc['ForecastPLF'].max():.4f}]")
        df_proc['Predicted_Bookload'] = (df_proc['ForecastPLF'] / 100) * df_proc['AdjustedCapacity']
 
        # 5. Write to DB
        self.save_forecast_results(df_proc)
 
        # 6. Upload CSV to S3
        self._upload_to_s3(df_proc)
 
        self.logger.info("Scoring finished and results saved.")
 
    def save_forecast_results(self, df):
        """Save to forecasting_results_ml table with Origin, Destination, and ForecastPLF."""
        output_df = df[[
            'InventoryLegID', 'DepartureDate', 'FlightNumber',
            'DepartureStation', 'ArrivalStation', 'Sector',
            'STD', 'NDO', 'Cum_PLF', 'ForecastPLF', 'Predicted_Bookload'
        ]].copy()
 
        # Rename DepartureStation/ArrivalStation to Origin/Destination
        output_df.rename(columns={
            'DepartureStation': 'Origin',
            'ArrivalStation': 'Destination'
        }, inplace=True)
 
        # Convert DepartureDate to date (remove time component)
        output_df['DepartureDate'] = pd.to_datetime(output_df['DepartureDate']).dt.date
 
        output_df['CreatedDatetime'] = datetime.now()
 
        try:
            output_df.to_sql('forecast_results_ml', self.dwconn, if_exists='replace', index=False, chunksize=1000)
            self.logger.info(f"Saved {len(output_df)} forecast results to DB.")
        except Exception as e:
            self.logger.error(f"Failed to write results to DB: {e}")
 
    def _upload_to_s3(self, df):
        """Upload scoring results CSV to S3 with mlforecast naming convention."""
        try:
            output_df = df[[
                'InventoryLegID', 'DepartureDate', 'FlightNumber',
                'DepartureStation', 'ArrivalStation', 'Sector',
                'STD', 'NDO', 'Cum_PLF', 'ForecastPLF', 'Predicted_Bookload'
            ]].copy()
 
            output_df.rename(columns={
                'DepartureStation': 'Origin',
                'ArrivalStation': 'Destination'
            }, inplace=True)
 
            output_df['CreatedDatetime'] = datetime.now()
 
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            current_year = datetime.now().strftime('%Y')
            current_month = datetime.now().strftime('%m')
            current_day = datetime.now().strftime('%d')
 
            s3_key = (f"{self.s3_base_path}/mlforecast/{current_year}/{current_month}/{current_day}/"
                      f"mlforecast_{timestamp}.csv")
 
            temp_file = f"/tmp/mlforecast_{timestamp}.csv"
            output_df.to_csv(temp_file, index=False)
 
            self.s3_old.upload_file(Filename=temp_file, Bucket=self.s3_bucket, Key=s3_key)
            os.remove(temp_file)
 
            self.logger.info(f"Successfully uploaded ML forecast to s3://{self.s3_bucket}/{s3_key}")
        except Exception as e:
            self.logger.error(f"Error uploading ML forecast CSV to S3: {e}")