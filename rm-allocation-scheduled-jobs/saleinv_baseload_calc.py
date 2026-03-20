import pandas as pd
from constants_utils import ConstantsUtils
import traceback
import warnings
import time
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import calendar
warnings.filterwarnings('ignore')

COLLECTION_NAME = 'saleinv_flown_data'

VALUE_COLUMNS = ['BaseLoad', 'CorporateContribution', 'AvgFare_FIT_0_7', 'AvgFare_FIT_30Plus', 'FarePremium', 'FlownPLF']

GROUP_COLS = ['Sector', 'TimeCluster', 'DayOfWeek']
KEY_COLS = ['Month'] + GROUP_COLS


class SaleInvBaseloadCalc:

    def __init__(self, logger, params, overrides, context):
        self.context = context
        self.logger = logger
        self.dwconn = params['dwconn']
        self.ebsconn = params['rdEsbConn']
        self.mongoconn = params['mongoconn']
        self.db = self.mongoconn['qp_dw_rmalloc']
        self.constants = ConstantsUtils(self.db)

        start_date_str = overrides.get('start_date', '2025-01-01')
        self.start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()

        self.time_clusters = self._fetch_time_clusters()

    # ── Retry helper ──────────────────────────────────────────────────

    def _retry_db_operation(self, operation, description, max_retries=3, delay=5):
        for attempt in range(1, max_retries + 1):
            try:
                return operation()
            except Exception as e:
                if attempt == max_retries:
                    self.logger.error(f"{description} failed after {max_retries} attempts: {e}")
                    raise
                self.logger.warning(f"{description} failed (attempt {attempt}/{max_retries}), retrying in {delay}s...")
                time.sleep(delay)

    # ── Time clusters ─────────────────────────────────────────────────

    def _fetch_time_clusters(self):
        def _do_fetch():
            clusters = list(self.db.config_clusters.find({}, {'_id': 0, 'cluster': 1, 'startTime': 1, 'endTime': 1}))
            return [(c['cluster'], c['startTime'], c['endTime']) for c in clusters]
        return self._retry_db_operation(_do_fetch, "Fetch time clusters")

    def _get_time_cluster_case_statement(self):
        cases = []
        for cluster_id, start_time, end_time in self.time_clusters:
            if start_time > end_time:
                cases.append(f"WHEN CAST(IL.STD AS TIME) BETWEEN '{start_time}' AND '23:59:59' THEN {cluster_id}")
                cases.append(f"WHEN CAST(IL.STD AS TIME) BETWEEN '00:00:00' AND '{end_time}' THEN {cluster_id}")
            else:
                cases.append(f"WHEN CAST(IL.STD AS TIME) BETWEEN '{start_time}' AND '{end_time}' THEN {cluster_id}")
        return "CASE " + " ".join(cases) + " ELSE NULL END"

    # ── Data fetch ────────────────────────────────────────────────────

    def _fetch_flowndatapax_data_for_month(self, month_start, month_end):
        try:
            time_cluster_case = self._get_time_cluster_case_statement()

            query = f"""
            SELECT
                CONCAT(PX.Origin, PX.Destination) AS Sector,
                PX.DepartureDate,
                (WEEKDAY(PX.DepartureDate) + 1) AS DayOfWeek,
                {time_cluster_case} AS TimeCluster,
                DATEDIFF(PX.DepartureDate, DATE(PX.BookingUTC)) AS NDO,
                PX.PassengerID,
                PX.FlightNumber,
                PX.FareBasis,
                PX.AdjustedCapacity,
                (CASE WHEN SUBSTRING(PX.FareBasis, 5, 1) = 'T' THEN PX.FB_Net ELSE PX.DiscountedBaseFare_Pro END
                    + PX.YQ_Pro + PX.RCS + PX.CUTE) AS TotalFare,
                PX.OrganizationCode
            FROM QP_DL_NAVDATA.QP_DL_FLOWNDATAPAX PX
            INNER JOIN QP_DL_NAVDATA.QP_DL_InventoryLeg IL
                ON IL.InventoryLegID = PX.InventoryLegID
            WHERE PX.DepartureDate >= '{month_start}'
                AND PX.DepartureDate <= '{month_end}'
                AND PX.BookingID IS NOT NULL
                AND IL.CarrierCode = 'QP'
                AND SUBSTRING(PX.FareBasis, 1, 1) != 'S'
                AND SUBSTRING(PX.FareBasis, 5, 1) != 'G'
                AND PX.OrganizationCode NOT LIKE '%%3T'
            """

            df = self._retry_db_operation(
                lambda: pd.read_sql(query, self.ebsconn),
                f"Fetch FLOWNDATAPAX {month_start} to {month_end}"
            )
            self.logger.info(f"Fetched {len(df)} records from FLOWNDATAPAX for {month_start} to {month_end}")

            if df.empty:
                return df

            df['IsCorporate'] = df['FareBasis'].str[4] == 'C'
            return df

        except Exception as e:
            self.logger.error(f"Error fetching FLOWNDATAPAX data: {traceback.format_exc()}")
            raise

    # ── Metric calculations ───────────────────────────────────────────

    def _calc_baseload(self, df, month_key):
        df_ndo30 = df[df['NDO'] >= 30]
        if df_ndo30.empty:
            return pd.DataFrame(columns=KEY_COLS + ['BaseLoad'])

        flight_level = df_ndo30.groupby(
            GROUP_COLS + ['DepartureDate', 'FlightNumber']
        ).agg(PaxCount=('PassengerID', 'count'), AdjustedCapacity=('AdjustedCapacity', 'first')).reset_index()

        result = flight_level.groupby(GROUP_COLS).agg(
            TotalPax=('PaxCount', 'sum'), TotalCapacity=('AdjustedCapacity', 'sum')
        ).reset_index()

        result['BaseLoad'] = (result['TotalPax'] / result['TotalCapacity'] * 100).round(0)
        result.insert(0, 'Month', month_key)
        return result[KEY_COLS + ['BaseLoad']]

    def _calc_corporate_contribution(self, df, month_key):
        df_ndo07 = df[(df['NDO'] >= 0) & (df['NDO'] <= 7)]
        if df_ndo07.empty:
            return pd.DataFrame(columns=KEY_COLS + ['CorporateContribution'])

        result = df_ndo07.groupby(GROUP_COLS).agg(
            TotalPax=('PassengerID', 'count'), CorporatePax=('IsCorporate', 'sum')
        ).reset_index()

        result['CorporateContribution'] = (result['CorporatePax'] / result['TotalPax'] * 100).round(0)
        result.insert(0, 'Month', month_key)
        return result[KEY_COLS + ['CorporateContribution']]

    def _calc_avg_fare(self, df, month_key, ndo_min, ndo_max, col_name):
        if ndo_max is None:
            filtered = df[df['NDO'] >= ndo_min]
        else:
            filtered = df[(df['NDO'] >= ndo_min) & (df['NDO'] <= ndo_max)]
        if filtered.empty:
            return pd.DataFrame(columns=KEY_COLS + [col_name])

        result = filtered.groupby(GROUP_COLS).agg(
            **{col_name: ('TotalFare', 'mean')}
        ).reset_index()
        result[col_name] = result[col_name].round(0)
        result.insert(0, 'Month', month_key)
        return result[KEY_COLS + [col_name]]

    def _calc_fare_premium(self, fares_07, fares_30):
        if fares_07.empty or fares_30.empty:
            return pd.DataFrame(columns=KEY_COLS + ['FarePremium'])

        merged = fares_07.merge(fares_30, on=KEY_COLS, how='inner')
        if merged.empty:
            return pd.DataFrame(columns=KEY_COLS + ['FarePremium'])

        merged['FarePremium'] = ((merged['AvgFare_FIT_0_7'] / merged['AvgFare_FIT_30Plus']) - 1) * 100
        merged['FarePremium'] = merged['FarePremium'].round(0)
        return merged[KEY_COLS + ['FarePremium']]

    def _calc_flown_plf(self, df, month_key):
        if df.empty:
            return pd.DataFrame(columns=KEY_COLS + ['FlownPLF'])

        flight_level = df.groupby(
            GROUP_COLS + ['DepartureDate', 'FlightNumber']
        ).agg(PaxCount=('PassengerID', 'count'), AdjustedCapacity=('AdjustedCapacity', 'first')).reset_index()

        result = flight_level.groupby(GROUP_COLS).agg(
            TotalPax=('PaxCount', 'sum'), TotalCapacity=('AdjustedCapacity', 'sum')
        ).reset_index()

        result['FlownPLF'] = (result['TotalPax'] / result['TotalCapacity'] * 100).round(0)
        result.insert(0, 'Month', month_key)
        return result[KEY_COLS + ['FlownPLF']]

    # ── Grid expansion & missing value fill ───────────────────────────

    DOW_ORDER = [1, 2, 3, 4, 5, 6, 7]

    def _expand_full_grid(self, sectors, month_key):
        """Create full Sector x TimeCluster x DayOfWeek grid."""
        clusters = [c[0] for c in self.time_clusters]
        grid = pd.MultiIndex.from_product(
            [sectors, clusters, self.DOW_ORDER],
            names=GROUP_COLS
        ).to_frame(index=False)
        grid.insert(0, 'Month', month_key)
        return grid

    def _fill_missing_values(self, df, value_col, imputed_col):
        """
        3-step fill for a single value column. Sets imputed_col = 1 only for rows
        where that specific field was imputed, 0 otherwise.
        """
        df[imputed_col] = (df[value_col].isna()).astype(int)

        # Step 1: Horizontal (same Sector + TimeCluster, mean across DOWs, actuals only)
        actuals = df[df[imputed_col] == 0]
        h_mean = actuals.groupby(['Month', 'Sector', 'TimeCluster'])[value_col].mean().reset_index()
        h_mean.rename(columns={value_col: '_h'}, inplace=True)
        df = df.merge(h_mean, on=['Month', 'Sector', 'TimeCluster'], how='left')
        m1 = df[value_col].isna() & df['_h'].notna()
        df.loc[m1, value_col] = df.loc[m1, '_h'].round(0)
        df.drop(columns=['_h'], inplace=True)

        # Step 2: Vertical (same Sector + DOW, mean across clusters, actuals only)
        actuals2 = df[df[imputed_col] == 0]
        v_mean = actuals2.groupby(['Month', 'Sector', 'DayOfWeek'])[value_col].mean().reset_index()
        v_mean.rename(columns={value_col: '_v'}, inplace=True)
        df = df.merge(v_mean, on=['Month', 'Sector', 'DayOfWeek'], how='left')
        m2 = df[value_col].isna() & df['_v'].notna()
        df.loc[m2, value_col] = df.loc[m2, '_v'].round(0)
        df.drop(columns=['_v'], inplace=True)

        # Step 3: Vertical relaxed (using all values incl filled)
        v_all = df[df[value_col].notna()].groupby(['Month', 'Sector', 'DayOfWeek'])[value_col].mean().reset_index()
        v_all.rename(columns={value_col: '_va'}, inplace=True)
        df = df.merge(v_all, on=['Month', 'Sector', 'DayOfWeek'], how='left')
        m3 = df[value_col].isna() & df['_va'].notna()
        df.loc[m3, value_col] = df.loc[m3, '_va'].round(0)
        df.drop(columns=['_va'], inplace=True)

        return df

    # ── MongoDB operations ────────────────────────────────────────────

    def _get_latest_month(self):
        """Get the latest Month in the combined collection."""
        def _do_fetch():
            pipeline = [{'$group': {'_id': None, 'max_month': {'$max': '$Month'}}}]
            result = list(self.db[COLLECTION_NAME].aggregate(pipeline))
            return result[0]['max_month'] if result and result[0]['max_month'] else None
        return self._retry_db_operation(_do_fetch, f"Fetch latest month from {COLLECTION_NAME}")

    def _insert_to_mongo(self, df, month_key):
        if df.empty:
            self.logger.warning(f"Skipping insert for {COLLECTION_NAME} — empty DataFrame")
            return 0

        def _do_insert():
            collection = self.db[COLLECTION_NAME]
            records = df.to_dict('records')
            collection.delete_many({'Month': month_key})
            collection.insert_many(records)
            return len(records)

        return self._retry_db_operation(_do_insert, f"Insert into {COLLECTION_NAME} for {month_key}")

    # ── Month helpers ─────────────────────────────────────────────────

    def _build_month_info(self, month_key):
        d = datetime.strptime(month_key, '%Y-%m').date()
        last_day = calendar.monthrange(d.year, d.month)[1]
        return {'month_start': d, 'month_end': d.replace(day=last_day), 'month_key': month_key}

    def _get_month_range(self, start_key, end_key):
        months = []
        current = datetime.strptime(start_key, '%Y-%m').date()
        end = datetime.strptime(end_key, '%Y-%m').date()
        while current <= end:
            months.append(self._build_month_info(current.strftime('%Y-%m')))
            current += relativedelta(months=1)
        return months

    # ── Process a single month ────────────────────────────────────────

    def _process_month(self, df, month_key):
        """Calculate all metrics, merge, expand grid, fill missing, insert."""
        # Calculate individual metrics
        baseload = self._calc_baseload(df, month_key)
        corp_contrib = self._calc_corporate_contribution(df, month_key)
        fares_07 = self._calc_avg_fare(df, month_key, 0, 7, 'AvgFare_FIT_0_7')
        fares_30 = self._calc_avg_fare(df, month_key, 30, None, 'AvgFare_FIT_30Plus')
        fare_premium = self._calc_fare_premium(fares_07, fares_30)
        flown_plf = self._calc_flown_plf(df, month_key)

        # Collect all unique sectors across all metrics
        all_sectors = set()
        for mdf in [baseload, corp_contrib, fares_07, fares_30, fare_premium, flown_plf]:
            if not mdf.empty:
                all_sectors.update(mdf['Sector'].unique())

        if not all_sectors:
            self.logger.warning(f"No sectors found for {month_key}")
            return

        # Build full grid and merge all metrics onto it
        combined = self._expand_full_grid(sorted(all_sectors), month_key)

        for mdf in [baseload, corp_contrib, fares_07, fares_30, fare_premium, flown_plf]:
            if not mdf.empty:
                combined = combined.merge(mdf, on=KEY_COLS, how='left')

        # Ensure all value columns exist (in case a metric df was empty)
        for vc in VALUE_COLUMNS:
            if vc not in combined.columns:
                combined[vc] = None

        # Fill missing values per field independently with per-field imputed flags
        for vc in VALUE_COLUMNS:
            combined = self._fill_missing_values(combined, vc, f'Imputed_{vc}')

        # Select final columns
        final_cols = KEY_COLS + VALUE_COLUMNS + [f'Imputed_{vc}' for vc in VALUE_COLUMNS]
        combined = combined[final_cols]

        count = self._insert_to_mongo(combined, month_key)
        self.logger.info(f"Inserted {count} records into {COLLECTION_NAME} for {month_key}")

    # ── Main run ──────────────────────────────────────────────────────

    def run(self):
        try:
            current_date = datetime.now().date()
            last_completed_key = (current_date.replace(day=1) - timedelta(days=1)).strftime('%Y-%m')
            default_start_key = self.start_date.strftime('%Y-%m')

            latest_month = self._get_latest_month()
            if latest_month:
                start_key = (datetime.strptime(latest_month, '%Y-%m').date() + relativedelta(months=1)).strftime('%Y-%m')
            else:
                start_key = default_start_key

            if start_key > last_completed_key:
                self.logger.info("Already up to date. Nothing to process.")
                return

            all_months = self._get_month_range(start_key, last_completed_key)

            for idx, month_info in enumerate(all_months, 1):
                month_key = month_info['month_key']
                try:
                    df = self._fetch_flowndatapax_data_for_month(month_info['month_start'], month_info['month_end'])
                    if df.empty:
                        self.logger.warning(f"No data for {month_key}, skipping...")
                        continue
                    self._process_month(df, month_key)
                except Exception as e:
                    self.logger.error(f"Error processing month {month_key}: {e}")
                    self.logger.error(traceback.format_exc())
                    continue

            self.logger.info("Sales Inventory Baseload Calculation completed successfully.")

        except Exception as e:
            self.logger.error(f"Error in run method: {traceback.format_exc()}")
            raise
