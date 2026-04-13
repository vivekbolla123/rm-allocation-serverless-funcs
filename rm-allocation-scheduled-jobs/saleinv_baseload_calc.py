import pandas as pd
import numpy as np
from constants_utils import ConstantsUtils
import traceback
import warnings
import time
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import calendar
warnings.filterwarnings('ignore')

COLLECTION_NAME = 'saleinv_flown_data'
LEANPEAK_COLLECTION_NAME = 'saleinv_flown_leanpeak'

VALUE_COLUMNS = ['BaseLoad', 'CorporateContribution_Overall', 'CorporateContribution_Near', 'AvgFare_FIT_Near', 'AvgFare_FIT_Far', 'FarePremium', 'FlownPLF']

GROUP_COLS = ['Sector', 'TimeCluster', 'DayOfWeek']
KEY_COLS = ['Month'] + GROUP_COLS

LEANPEAK_DATE_GROUP_COLS = ['Sector', 'DepartureDate', 'DayOfWeek']
LEANPEAK_DOW_GROUP_COLS = ['Sector', 'DayOfWeek']


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
        self._load_tunable_config()

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

    # ── Tunable config ────────────────────────────────────────────────

    def _load_tunable_config(self):
        """Load tunable constants from saleinv_parameter_values collection."""
        import json

        def _do_load():
            param_docs = {
                doc['parameterKey']: doc['parameterValue']
                for doc in self.db.saleinv_parameter_values.find(
                    {'parameterKey': {'$in': ['BASELOAD_PARAMS', 'LEANPEAK_PARAMS']}},
                    {'_id': 0, 'parameterKey': 1, 'parameterValue': 1}
                )
            }
            return param_docs

        raw = self._retry_db_operation(_do_load, "Load tunable config")

        bl = json.loads(raw.get('BASELOAD_PARAMS', '{}'))
        lp = json.loads(raw.get('LEANPEAK_PARAMS', '{}'))

        self.baseload_ndo_min = int(bl.get('BASELOAD_NDO_MIN', '31'))
        self.corp_ndo_min = int(bl.get('CORP_CONTRI_NDO_MIN', '0'))
        self.corp_ndo_max = int(bl.get('CORP_CONTRI_NDO_MAX', '7'))
        self.near_ndo_min = int(bl.get('AVG_FARE_NEAR_NDO_MIN', '0'))
        self.near_ndo_max = int(bl.get('AVG_FARE_NEAR_NDO_MAX', '7'))
        self.far_ndo_min = int(bl.get('AVG_FARE_FAR_NDO_MIN', '31'))
        self.lp_threshold_multiplier = float(lp.get('LEANPEAK_THRESHOLD_MULTIPLIER', '0.75'))
        self.lp_smooth_window = int(lp.get('LEANPEAK_SMOOTH_WINDOW', '4'))
        self.lp_min_band_length = int(lp.get('LEANPEAK_MIN_BAND_LENGTH', '3'))

        self.logger.info("Tunable config loaded from saleinv_parameter_values")

    # ── Time clusters ─────────────────────────────────────────────────

    def _fetch_time_clusters(self):
        def _do_fetch():
            clusters = list(self.db.config_clusters.find({}, {'_id': 0, 'cluster': 1, 'startTime': 1, 'endTime': 1}))
            # Zero-pad times for consistent string comparison (e.g., "2:30:00" → "02:30:00")
            def pad_time(t):
                parts = t.split(':')
                parts[0] = parts[0].zfill(2)
                return ':'.join(parts)
            return [(c['cluster'], pad_time(c['startTime']), pad_time(c['endTime'])) for c in clusters]
        return self._retry_db_operation(_do_fetch, "Fetch time clusters")

    def _get_time_cluster_case_statement(self):
        cases = []
        for cluster_id, start_time, end_time in self.time_clusters:
            if start_time > end_time:
                cases.append(f"WHEN CAST(IL.STD AS TIME) >= '{start_time}' AND CAST(IL.STD AS TIME) <= '23:59:59' THEN {cluster_id}")
                cases.append(f"WHEN CAST(IL.STD AS TIME) >= '00:00:00' AND CAST(IL.STD AS TIME) < '{end_time}' THEN {cluster_id}")
            else:
                cases.append(f"WHEN CAST(IL.STD AS TIME) >= '{start_time}' AND CAST(IL.STD AS TIME) < '{end_time}' THEN {cluster_id}")
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
                DATEDIFF(PX.DepartureDate, DATE(PX.BookingDate)) AS NDO,
                PX.PassengerID,
                PX.FlightNumber,
                PX.FareBasis,
                PX.AdjustedCapacity,
                (CASE WHEN SUBSTRING(PX.FareBasis, 5, 1) = 'T' THEN PX.FB_Net ELSE PX.DiscountedBaseFare_Pro END
                    + PX.YQ_Pro + PX.RCS + PX.CUTE) AS TotalFare,
                (CASE WHEN SUBSTRING(PX.FareBasis, 5, 1) = 'T' THEN PX.FB_Net ELSE PX.DiscountedBaseFare_Pro END
                    + PX.YQ_Pro + PX.RCS + PX.CUTE) AS PRASKFare,
                PX.OrganizationCode
            FROM QP_DL_NAVDATA.QP_DL_FLOWNDATAPAX PX
            INNER JOIN QP_DL_NAVDATA.QP_DL_InventoryLeg IL
                ON IL.InventoryLegID = PX.InventoryLegID
            WHERE PX.DepartureDate >= '{month_start}'
                AND PX.DepartureDate <= '{month_end}'
                AND PX.BookingID IS NOT NULL
                AND IL.CarrierCode = 'QP'
                AND SUBSTRING(PX.FareBasis, 1, 1) != 'S'
                AND PX.OrganizationCode NOT LIKE '%%3T'
            """

            df = self._retry_db_operation(
                lambda: pd.read_sql(query, self.ebsconn),
                f"Fetch FLOWNDATAPAX {month_start} to {month_end}"
            )
            # df = pd.read_csv('flown_02_2025.csv')
            self.logger.info(f"Fetched {len(df)} records from FLOWNDATAPAX for {month_start} to {month_end}")

            if df.empty:
                return df

            df['IsCorporate'] = df['FareBasis'].str[4] == 'C'
            df['IsGroup'] = df['FareBasis'].str[4] == 'G'
            return df

        except Exception as e:
            self.logger.error(f"Error fetching FLOWNDATAPAX data: {traceback.format_exc()}")
            raise

    # ── Metric calculations ───────────────────────────────────────────

    def _calc_baseload(self, df, month_key):
        df_far_ndo = df[df['NDO'] >= self.baseload_ndo_min]
        if df_far_ndo.empty:
            return pd.DataFrame(columns=KEY_COLS + ['BaseLoad'])

        flight_level = df_far_ndo.groupby(
            GROUP_COLS + ['DepartureDate', 'FlightNumber']
        ).agg(PaxCount=('PassengerID', 'count'), AdjustedCapacity=('AdjustedCapacity', 'first')).reset_index()

        result = flight_level.groupby(GROUP_COLS).agg(
            TotalPax=('PaxCount', 'sum'), TotalCapacity=('AdjustedCapacity', 'sum')
        ).reset_index()

        result['BaseLoad'] = (result['TotalPax'] / result['TotalCapacity'] * 100).round(0)
        result.insert(0, 'Month', month_key)
        return result[KEY_COLS + ['BaseLoad']]

    def _calc_corporate_contribution(self, df, month_key):
        # Overall corporate contribution (all NDOs)
        if df.empty:
            return pd.DataFrame(columns=KEY_COLS + ['CorporateContribution_Overall']), \
                   pd.DataFrame(columns=KEY_COLS + ['CorporateContribution_Near'])

        overall = df.groupby(GROUP_COLS).agg(
            TotalPax=('PassengerID', 'count'), CorporatePax=('IsCorporate', 'sum')
        ).reset_index()
        overall['CorporateContribution_Overall'] = (overall['CorporatePax'] / overall['TotalPax'] * 100).round(0)
        overall.insert(0, 'Month', month_key)
        overall = overall[KEY_COLS + ['CorporateContribution_Overall']]

        # Near-NDO corporate contribution (0-7)
        df_near_ndo = df[(df['NDO'] >= self.corp_ndo_min) & (df['NDO'] <= self.corp_ndo_max)]
        if df_near_ndo.empty:
            near = pd.DataFrame(columns=KEY_COLS + ['CorporateContribution_Near'])
        else:
            near = df_near_ndo.groupby(GROUP_COLS).agg(
                TotalPax=('PassengerID', 'count'), CorporatePax=('IsCorporate', 'sum')
            ).reset_index()
            near['CorporateContribution_Near'] = (near['CorporatePax'] / near['TotalPax'] * 100).round(0)
            near.insert(0, 'Month', month_key)
            near = near[KEY_COLS + ['CorporateContribution_Near']]

        return overall, near

    def _calc_avg_fare(self, df, month_key, ndo_min, ndo_max, col_name):
        df = df[~df['IsGroup']]
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

    def _calc_fare_premium(self, fares_near, fares_far):
        if fares_near.empty or fares_far.empty:
            return pd.DataFrame(columns=KEY_COLS + ['FarePremium'])

        merged = fares_near.merge(fares_far, on=KEY_COLS, how='inner')
        if merged.empty:
            return pd.DataFrame(columns=KEY_COLS + ['FarePremium'])

        merged['FarePremium'] = ((merged['AvgFare_FIT_Near'] / merged['AvgFare_FIT_Far']) - 1) * 100
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

    # ── City pair distance fetch ─────────────────────────────────────

    def _fetch_city_pair_distances(self):
        """Fetch ActualDistance for all active city pairs from DW."""
        try:
            query = """
            SELECT
                CONCAT(DepartureStation, ArrivalStation) AS Sector,
                ActualDistance AS Distance
            FROM QP_DWH_MASTER.CityPair
            """
            df = self._retry_db_operation(
                lambda: pd.read_sql(query, self.dwconn),
                "Fetch CityPair distances"
            )
            self.logger.info(f"Fetched {len(df)} city pair distances")
            return df
        except Exception as e:
            self.logger.error(f"Error fetching city pair distances: {traceback.format_exc()}")
            raise

    # ── Lean Peak calculations ────────────────────────────────────────

    def _calc_prask_by_sector_dow_date(self, df, month_key):
        """PRASK grouped by Sector, DayOfWeek, DepartureDate.
        Numerator (fare sum) is per-passenger, denominator (capacity×distance) is per-flight."""
        if df.empty:
            return pd.DataFrame(columns=['Month'] + LEANPEAK_DATE_GROUP_COLS + ['PRASK'])

        # Numerator: sum of PRASKFare across all pax
        fare_agg = df.groupby(LEANPEAK_DATE_GROUP_COLS).agg(
            SumFare=('PRASKFare', 'sum')
        ).reset_index()

        # Denominator: sum of AdjustedCapacity × Distance, once per flight
        flight_level = df.groupby(
            LEANPEAK_DATE_GROUP_COLS + ['FlightNumber']
        ).agg(AdjustedCapacity=('AdjustedCapacity', 'first'), Distance=('Distance', 'first')).reset_index()
        flight_level['CapDist'] = flight_level['AdjustedCapacity'] * flight_level['Distance']
        cap_agg = flight_level.groupby(LEANPEAK_DATE_GROUP_COLS).agg(
            SumCapDist=('CapDist', 'sum')
        ).reset_index()

        agg = fare_agg.merge(cap_agg, on=LEANPEAK_DATE_GROUP_COLS, how='inner')
        agg['PRASK'] = (agg['SumFare'] / agg['SumCapDist']).round(2)
        agg.insert(0, 'Month', month_key)
        return agg[['Month'] + LEANPEAK_DATE_GROUP_COLS + ['PRASK']]


    def _calc_prask_by_dow(self, df, month_key):
        """PRASK grouped by DayOfWeek only.
        Numerator (fare sum) is per-passenger, denominator (capacity*distance) is per-flight."""
        if df.empty:
            return pd.DataFrame(columns=['Month', 'DayOfWeek', 'DOW_PRASK'])

        # Numerator: sum of PRASKFare across all pax
        fare_agg = df.groupby(LEANPEAK_DOW_GROUP_COLS).agg(
            SumFare=('PRASKFare', 'sum')
        ).reset_index()

        # Denominator: sum of AdjustedCapacity * Distance, once per flight
        flight_level = df.groupby(
            LEANPEAK_DATE_GROUP_COLS + ['FlightNumber']
        ).agg(AdjustedCapacity=('AdjustedCapacity', 'first'), Distance=('Distance', 'first')).reset_index()
        flight_level['CapDist'] = flight_level['AdjustedCapacity'] * flight_level['Distance']
        cap_agg = flight_level.groupby(LEANPEAK_DOW_GROUP_COLS).agg(
            SumCapDist=('CapDist', 'sum')
        ).reset_index()

        agg = fare_agg.merge(cap_agg, on=LEANPEAK_DOW_GROUP_COLS, how='inner')
        agg['DOW_PRASK'] = (agg['SumFare'] / agg['SumCapDist']).round(2)
        agg.insert(0, 'Month', month_key)
        return agg[['Month'] + LEANPEAK_DOW_GROUP_COLS + ['DOW_PRASK']]

    def _calc_deviation(self, prask_df, dow_prask_df):
        """Deviation% = (PRASK / DOW_PRASK - 1) × 100."""
        merged = prask_df.merge(dow_prask_df, on=['Month'] + LEANPEAK_DOW_GROUP_COLS, how='left')
        merged['Deviation_Pct'] = ((merged['PRASK'] / merged['DOW_PRASK']) - 1) * 100
        merged['Deviation_Pct'] = merged['Deviation_Pct'].round(2)
        return merged

    def _calc_sector_threshold(self, deviation_df):
        """Threshold = 0.75 × Std(Deviation%) grouped by Sector."""
        sector_std = deviation_df.groupby('Sector')['Deviation_Pct'].std().reset_index()
        sector_std.rename(columns={'Deviation_Pct': 'Sector_Std'}, inplace=True)
        sector_std['Threshold'] = (self.lp_threshold_multiplier * sector_std['Sector_Std']).round(2)
        sector_std['Threshold'] = sector_std['Threshold'].fillna(np.inf)
        return sector_std[['Sector', 'Threshold']]

    def _calc_lean_peak_tag(self, deviation_df, threshold_df):
        """Tag: 1 if Deviation% >= Threshold, -1 if Deviation% <= -Threshold, else 0."""
        merged = deviation_df.merge(threshold_df, on='Sector', how='left')
        merged['Tag'] = 0
        merged.loc[merged['Deviation_Pct'] >= merged['Threshold'], 'Tag'] = 1
        merged.loc[merged['Deviation_Pct'] <= -merged['Threshold'], 'Tag'] = -1
        return merged

    def _fill_missing_dates(self, tagged_df, month_key):
        """Expand each sector to cover all dates in the month, filling missing dates with Tag=0."""
        d = datetime.strptime(month_key, '%Y-%m').date()
        last_day = calendar.monthrange(d.year, d.month)[1]
        all_dates = pd.date_range(d, d.replace(day=last_day), freq='D')

        sectors = tagged_df['Sector'].unique()
        full_grid = pd.MultiIndex.from_product(
            [sectors, all_dates], names=['Sector', 'DepartureDate']
        ).to_frame(index=False)
        full_grid['DayOfWeek'] = full_grid['DepartureDate'].dt.weekday + 1

        tagged_df['DepartureDate'] = pd.to_datetime(tagged_df['DepartureDate'])
        merged = full_grid.merge(tagged_df, on=['Sector', 'DepartureDate', 'DayOfWeek'], how='left')
        merged['Tag'] = merged['Tag'].fillna(0).astype(int)

        # Carry forward columns that came from the tag step (Month, thresholds, etc.)
        for col in ['Month']:
            if col in merged.columns:
                merged[col] = merged[col].fillna(month_key)

        return merged.sort_values(['Sector', 'DepartureDate']).reset_index(drop=True)

    def _smooth_lone_zeros(self, tags, window=4):
        """Sliding window: if exactly 1 zero among `window` consecutive values
        and the rest are all the same sign, fill that zero with that sign.
        Uses numpy rolling for vectorized computation."""
        arr = np.array(tags)
        n = len(arr)
        if n < window:
            return tags

        for i in range(n - window + 1):
            w = arr[i:i + window]
            zero_mask = w == 0
            if zero_mask.sum() != 1:
                continue
            zero_pos = zero_mask.argmax()
            if zero_pos == 0 or zero_pos == window - 1:
                continue
            non_zeros = w[~zero_mask]
            if non_zeros[0] == non_zeros[1] == non_zeros[2]:
                arr[i + zero_pos] = non_zeros[0]

        return arr.tolist()

    def _remove_short_bands(self, tags, min_length=3):
        """Remove non-zero bands shorter than min_length using numpy diff-based run detection."""
        arr = np.array(tags)
        n = len(arr)
        if n == 0:
            return tags

        # Find run boundaries using diff
        diff = np.diff(arr, prepend=arr[0] - 1)
        starts = np.where(diff != 0)[0]

        for k in range(len(starts)):
            s = starts[k]
            e = starts[k + 1] if k + 1 < len(starts) else n
            if arr[s] != 0 and (e - s) < min_length:
                arr[s:e] = 0

        return arr.tolist()

    def _smooth_tags(self, tagged_df):
        """Apply smoothing per sector: fill lone zeros, then remove short bands.
        Uses sort_values + groupby transform to avoid per-group concat."""
        tagged_df = tagged_df.sort_values(['Sector', 'DepartureDate']).reset_index(drop=True)

        def _apply_smoothing(group_tags):
            tags = group_tags.tolist()
            tags = self._smooth_lone_zeros(tags, window=self.lp_smooth_window)
            tags = self._remove_short_bands(tags, min_length=self.lp_min_band_length)
            return pd.Series(tags, index=group_tags.index)

        tagged_df['Tag'] = tagged_df.groupby('Sector')['Tag'].transform(_apply_smoothing)
        return tagged_df

    def _insert_leanpeak_to_mongo(self, df, month_key):
        """Insert PRASK data (first df) into saleinv_leanpeak collection."""
        if df.empty:
            self.logger.warning(f"Skipping insert for {LEANPEAK_COLLECTION_NAME} — empty DataFrame")
            return 0

        def _do_insert():
            collection = self.db[LEANPEAK_COLLECTION_NAME]
            records = df.to_dict('records')
            collection.delete_many({'Month': month_key})
            collection.insert_many(records)
            return len(records)

        return self._retry_db_operation(_do_insert, f"Insert into {LEANPEAK_COLLECTION_NAME} for {month_key}")

    def _process_lean_peak(self, df, city_pair_df, month_key):
        """Calculate PRASK, deviation, threshold, and lean/peak tags for a month."""
        # Merge with city pair to get Distance
        merged = df.merge(city_pair_df, on='Sector', how='inner')
        if merged.empty:
            self.logger.warning(f"No city pair matches for lean peak calc in {month_key}")
            return

        # 1. PRASK by Sector, DOW, DepartureDate
        prask_df = self._calc_prask_by_sector_dow_date(merged, month_key)

        # 2. PRASK by DayOfWeek only
        dow_prask_df = self._calc_prask_by_dow(merged, month_key)

        # 3. Deviation%
        deviation_df = self._calc_deviation(prask_df, dow_prask_df)

        # 4. Sector-level threshold
        threshold_df = self._calc_sector_threshold(deviation_df)

        # 5. Lean/Peak tag
        tagged_df = self._calc_lean_peak_tag(deviation_df, threshold_df)

        # 6. Fill missing dates with Tag=0 before smoothing
        tagged_df = self._fill_missing_dates(tagged_df, month_key)

        # 7. Smooth tags per sector
        tagged_df = self._smooth_tags(tagged_df)

        # Keep only required columns
        tagged_df = tagged_df[['Sector', 'DepartureDate', 'DayOfWeek', 'Tag']]

        # Insert smoothed tagged df into collection
        insert_df = tagged_df.copy()
        insert_df.insert(0, 'Month', month_key)
        insert_df['DepartureDate'] = insert_df['DepartureDate'].astype(str)
        count = self._insert_leanpeak_to_mongo(insert_df, month_key)
        self.logger.info(f"Inserted {count} records into {LEANPEAK_COLLECTION_NAME} for {month_key}")

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

    def _process_month(self, df, month_key, city_pair_df):
        """Calculate all metrics, merge, expand grid, fill missing, insert."""
        # Calculate individual metrics
        baseload = self._calc_baseload(df, month_key)
        corp_overall, corp_near = self._calc_corporate_contribution(df, month_key)
        fares_near = self._calc_avg_fare(df, month_key, self.near_ndo_min, self.near_ndo_max, 'AvgFare_FIT_Near')
        fares_far = self._calc_avg_fare(df, month_key, self.far_ndo_min, None, 'AvgFare_FIT_Far')
        fare_premium = self._calc_fare_premium(fares_near, fares_far)
        flown_plf = self._calc_flown_plf(df, month_key)

        # Collect all unique sectors across all metrics
        all_sectors = set()
        for mdf in [baseload, corp_overall, corp_near, fares_near, fares_far, fare_premium, flown_plf]:
            if not mdf.empty:
                all_sectors.update(mdf['Sector'].unique())

        if not all_sectors:
            self.logger.warning(f"No sectors found for {month_key}")
            return

        # Build full grid and merge all metrics onto it
        combined = self._expand_full_grid(sorted(all_sectors), month_key)

        for mdf in [baseload, corp_overall, corp_near, fares_near, fares_far, fare_premium, flown_plf]:
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

        # Lean Peak calculation
        self._process_lean_peak(df, city_pair_df, month_key)

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

            # Fetch city pair distances once for all months
            city_pair_df = self._fetch_city_pair_distances()

            for idx, month_info in enumerate(all_months, 1):
                month_key = month_info['month_key']
                try:
                    df = self._fetch_flowndatapax_data_for_month(month_info['month_start'], month_info['month_end'])
                    if df.empty:
                        self.logger.warning(f"No data for {month_key}, skipping...")
                        continue
                    self._process_month(df, month_key, city_pair_df)
                except Exception as e:
                    self.logger.error(f"Error processing month {month_key}: {e}")
                    self.logger.error(traceback.format_exc())
                    continue

            self.logger.info("Sales Inventory Baseload Calculation completed successfully.")

        except Exception as e:
            self.logger.error(f"Error in run method: {traceback.format_exc()}")
            raise
