import logging
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time as dt_time
import pickle
import joblib
from io import BytesIO
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
        self.MAX_PREDICTED_CANCELLATIONS = 14
 
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

    # ==================== CANCELLATION ADJUSTMENT METHODS ====================

    def _get_time_cluster_for_std(self, std_time):
        """Convert STD to time cluster string for band matching."""
        if pd.isna(std_time):
            return 'Unknown'
        try:
            if isinstance(std_time, str):
                std_time_obj = datetime.strptime(std_time, '%H:%M:%S').time()
            elif isinstance(std_time, timedelta):
                std_time_obj = (datetime.min + std_time).time()
            elif isinstance(std_time, dt_time):
                std_time_obj = std_time
            elif hasattr(std_time, 'time') and callable(std_time.time):
                std_time_obj = std_time.time()
            else:
                return 'Unknown'
        except ValueError:
            return 'Unknown'

        t_0230 = dt_time(2, 30)
        t_0730 = dt_time(7, 30)
        t_1100 = dt_time(11, 0)
        t_1430 = dt_time(14, 30)
        t_1800 = dt_time(18, 0)
        t_2100 = dt_time(21, 0)

        if std_time_obj <= t_0230 or std_time_obj > t_2100:
            return '21:00-02:30'
        elif std_time_obj <= t_0730:
            return '02:30-07:30'
        elif std_time_obj <= t_1100:
            return '07:30-11:00'
        elif std_time_obj <= t_1430:
            return '11:00-14:30'
        elif std_time_obj <= t_1800:
            return '14:30-18:00'
        elif std_time_obj <= t_2100:
            return '18:00-21:00'
        else:
            return 'Unknown'

    def fetch_cancellation_rates(self):
        """Fetch cancellation rates from the DW database."""
        self.logger.info("Fetching cancellation rates for PLF adjustment...")
        try:
            query = """
            SELECT 
                Sector, DayOfWeek, TimeCluster, CustomerSegment, NDOBand, FareRange,
                FlownPassengers, CancelledPassengers, CancellationRate, NDOBandKey
            FROM cancellation_rates
            """
            df = pd.read_sql(query, self.dwconn)
            if df.empty:
                self.logger.warning("No cancellation rates found in database.")
            else:
                self.logger.info(f"Fetched {len(df)} cancellation rate records.")
            return df
        except Exception as e:
            self.logger.error(f"Error fetching cancellation rates: {e}")
            return pd.DataFrame()

    def get_bookload_by_bands(self, departure_date, origin, destination, flight_number, std_time):
        """Get current bookload for a specific flight aggregated by band dimensions."""
        try:
            time_cluster = self._get_time_cluster_for_std(std_time)
            dep_date_str = departure_date.strftime('%Y-%m-%d') if hasattr(departure_date, 'strftime') else str(departure_date)

            query = f"""
            SELECT
                CONCAT('{origin}', '-', '{destination}') AS Sector,
                DAYNAME('{dep_date_str}') AS DayOfWeek,
                '{time_cluster}' AS TimeCluster,
                CASE
                    WHEN SUBSTRING(BD.FareBasis, 5, 1) = 'C' THEN 'C'
                    WHEN SUBSTRING(BD.FareBasis, 5, 1) = 'F' THEN 'F'
                    WHEN LEFT(BD.FareBasis, 1) = 'G' THEN 'GROUP'
                    WHEN SUBSTRING(BD.FareBasis, 5, 1) = 'K' THEN 'K'
                    WHEN SUBSTRING(BD.FareBasis, 5, 1) = 'M' THEN 'M'
                    WHEN SUBSTRING(BD.FareBasis, 5, 1) = 'R' THEN 'R'
                    WHEN LEFT(BD.FareBasis, 1) = 'S' THEN 'STAFF'
                    WHEN SUBSTRING(BD.FareBasis, 5, 1) = 'V' THEN 'V'
                    WHEN SUBSTRING(BD.FareBasis, 5, 1) IN ('A', 'B', 'D', 'J', 'N', 'O', 'S') THEN 'Others'
                    ELSE 'Unknown'
                END AS CustomerSegment,
                CASE
                    WHEN DATEDIFF(BD.DepartureDate, DATE(BD.BookingDate)) = 0 THEN '1. 0'
                    WHEN DATEDIFF(BD.DepartureDate, DATE(BD.BookingDate)) BETWEEN 1 AND 2 THEN '2. 1-2'
                    WHEN DATEDIFF(BD.DepartureDate, DATE(BD.BookingDate)) BETWEEN 3 AND 7 THEN '3. 3-7'
                    WHEN DATEDIFF(BD.DepartureDate, DATE(BD.BookingDate)) BETWEEN 8 AND 14 THEN '4. 8-14'
                    WHEN DATEDIFF(BD.DepartureDate, DATE(BD.BookingDate)) BETWEEN 15 AND 30 THEN '5. 15-30'
                    WHEN DATEDIFF(BD.DepartureDate, DATE(BD.BookingDate)) BETWEEN 31 AND 60 THEN '6. 31-60'
                    WHEN DATEDIFF(BD.DepartureDate, DATE(BD.BookingDate)) > 60 THEN '7. 60+'
                    ELSE 'Unknown'
                END AS NDOBand,
                CASE
                    WHEN (BD.DiscountedBaseFare_Pro + BD.YQ_Pro + BD.RCS + BD.CUTE) = 0 THEN '1. 0-1000'
                    WHEN (BD.DiscountedBaseFare_Pro + BD.YQ_Pro + BD.RCS + BD.CUTE) BETWEEN 0.0001 AND 1000 THEN '1. 0-1000'
                    WHEN (BD.DiscountedBaseFare_Pro + BD.YQ_Pro + BD.RCS + BD.CUTE) BETWEEN 1000.0001 AND 3000 THEN '2. 1000-3000'
                    WHEN (BD.DiscountedBaseFare_Pro + BD.YQ_Pro + BD.RCS + BD.CUTE) BETWEEN 3000.0001 AND 4000 THEN '3. 3000-4000'
                    WHEN (BD.DiscountedBaseFare_Pro + BD.YQ_Pro + BD.RCS + BD.CUTE) BETWEEN 4000.0001 AND 5000 THEN '4. 4000-5000'
                    WHEN (BD.DiscountedBaseFare_Pro + BD.YQ_Pro + BD.RCS + BD.CUTE) BETWEEN 5000.0001 AND 8000 THEN '5. 5000-8000'
                    WHEN (BD.DiscountedBaseFare_Pro + BD.YQ_Pro + BD.RCS + BD.CUTE) BETWEEN 8000.0001 AND 10000 THEN '6. 8000-10000'
                    WHEN (BD.DiscountedBaseFare_Pro + BD.YQ_Pro + BD.RCS + BD.CUTE) BETWEEN 10000.0001 AND 15000 THEN '7. 10000-15000'
                    WHEN (BD.DiscountedBaseFare_Pro + BD.YQ_Pro + BD.RCS + BD.CUTE) > 15000 THEN '8. 15000+'
                    ELSE 'Unknown'
                END AS FareRange,
                COUNT(BD.PassengerID) AS PassengerCount
            FROM
                QP_DL_BOOKEDDATAPAX BD
            WHERE
                BD.DepartureDate = '{dep_date_str}'
                AND BD.Origin = '{origin}'
                AND BD.Destination = '{destination}'
                AND TRIM(BD.FlightNumber) = '{str(flight_number).strip()}'
            GROUP BY
                CustomerSegment, NDOBand, FareRange
            """
            df = pd.read_sql(query, self.esbconn)
            return df
        except Exception as e:
            self.logger.debug(f"Error getting bookload by bands for {origin}-{destination} {flight_number}: {e}")
            return pd.DataFrame()

    def predict_cancellations_for_flight(self, row, cancellation_rates):
        """Predict cancellations for a flight based on current bookload and historical rates."""
        try:
            if cancellation_rates.empty:
                return 0

            ndo_curr = row.get('Target_NDO')
            if pd.isna(ndo_curr):
                return 0

            # Determine which NDO band to use based on current NDO
            if ndo_curr == 2:
                ndo_band_key = 'ndo_2'
            elif ndo_curr == 1:
                ndo_band_key = 'ndo_1'
            else:
                return 0  # NDO 0 or >2 — no cancellation prediction

            # Get bookload by bands for this flight
            bookload_df = self.get_bookload_by_bands(
                row['DepartureDate'],
                row['DepartureStation'],
                row['ArrivalStation'],
                row['FlightNumber'],
                row.get('STD')
            )

            if bookload_df.empty:
                return 0

            # Filter cancellation rates for the appropriate NDO band
            rates_for_ndo = cancellation_rates[cancellation_rates['NDOBandKey'] == ndo_band_key]
            if rates_for_ndo.empty:
                return 0

            # Merge bookload with cancellation rates
            merged = bookload_df.merge(
                rates_for_ndo,
                on=['Sector', 'DayOfWeek', 'TimeCluster', 'CustomerSegment', 'NDOBand', 'FareRange'],
                how='left'
            )
            merged['CancellationRate'] = merged['CancellationRate'].fillna(0)

            # Calculate predicted cancellations
            merged['PredictedCancellations'] = merged['PassengerCount'] * merged['CancellationRate']
            total_predicted = merged['PredictedCancellations'].sum()

            # Apply cap
            return int(round(min(total_predicted, self.MAX_PREDICTED_CANCELLATIONS)))

        except Exception as e:
            self.logger.debug(f"Error predicting cancellations for {row.get('DepartureStation', 'N/A')}-{row.get('ArrivalStation', 'N/A')}: {e}")
            return 0

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

        # 5. Cancellation Adjustment
        self.logger.info("Calculating predicted cancellations for PLF adjustment...")
        cancellation_rates = self.fetch_cancellation_rates()

        if not cancellation_rates.empty:
            df_proc['PredictedCancellations'] = df_proc.apply(
                lambda row: self.predict_cancellations_for_flight(row, cancellation_rates), axis=1
            )

            # Calculate Adjusted Forecast PLF
            df_proc['Forecast_Seats'] = (df_proc['ForecastPLF'] / 100) * df_proc['AdjustedCapacity']
            df_proc['Adjusted_Forecast_Seats'] = (df_proc['Forecast_Seats'] - df_proc['PredictedCancellations']).clip(lower=0)
            df_proc['AdjustedForecastPLF'] = np.where(
                df_proc['AdjustedCapacity'] > 0,
                (df_proc['Adjusted_Forecast_Seats'] / df_proc['AdjustedCapacity']) * 100,
                np.nan
            )
            df_proc['AdjustedForecastPLF'] = df_proc['AdjustedForecastPLF'].round(2)
            df_proc['Adjusted_Predicted_Bookload'] = df_proc['Adjusted_Forecast_Seats']

            # Drop intermediate columns
            df_proc = df_proc.drop(columns=['Forecast_Seats', 'Adjusted_Forecast_Seats'], errors='ignore')

            # Log cancellation statistics
            cxl_count = len(df_proc[df_proc['PredictedCancellations'] > 0])
            cxl_stats = df_proc['PredictedCancellations'].describe()
            self.logger.info(
                f"Predicted Cancellations: Mean={cxl_stats['mean']:.2f}, "
                f"Max={cxl_stats['max']:.0f}, "
                f"Total flights with cancellations={cxl_count}"
            )
        else:
            self.logger.warning("Cancellation rates not available. Setting AdjustedForecastPLF to ForecastPLF.")
            df_proc['PredictedCancellations'] = 0
            df_proc['AdjustedForecastPLF'] = df_proc['ForecastPLF']
            df_proc['Adjusted_Predicted_Bookload'] = df_proc['Predicted_Bookload']

        # 6. Write to DB
        self.save_forecast_results(df_proc)

        # 7. Upload CSV to S3
        self._upload_to_s3(df_proc)

        self.logger.info("Scoring finished and results saved.")
 
    def save_forecast_results(self, df):
        """Save to forecasting_results_ml table with Origin, Destination, ForecastPLF, and cancellation adjustment."""
        output_cols = [
            'InventoryLegID', 'DepartureDate', 'FlightNumber',
            'DepartureStation', 'ArrivalStation', 'Sector',
            'STD', 'NDO', 'Cum_PLF', 'ForecastPLF', 'Predicted_Bookload',
            'PredictedCancellations', 'AdjustedForecastPLF', 'Adjusted_Predicted_Bookload'
        ]
        output_df = df[[c for c in output_cols if c in df.columns]].copy()

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
            output_cols = [
                'InventoryLegID', 'DepartureDate', 'FlightNumber',
                'DepartureStation', 'ArrivalStation', 'Sector',
                'STD', 'NDO', 'Cum_PLF', 'ForecastPLF', 'Predicted_Bookload',
                'PredictedCancellations', 'AdjustedForecastPLF', 'Adjusted_Predicted_Bookload'
            ]
            output_df = df[[c for c in output_cols if c in df.columns]].copy()

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