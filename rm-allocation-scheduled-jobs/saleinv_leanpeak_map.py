import pandas as pd
import numpy as np
import traceback
import time
import warnings
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import calendar
warnings.filterwarnings('ignore')

SOURCE_COLLECTION = 'saleinv_flown_leanpeak'
OUTPUT_COLLECTION = 'saleinv_mapped_leanpeak'
HOLIDAY_COLLECTION = 'saleinv_config_holiday_map'
MONTH_MAP_COLLECTION = 'saleinv_config_month_map'


class SaleInvLeanPeakMap:
    """
    Lean Peak Map Calculator.
    Maps historical lean/peak tags to target month using:
    1. DOW shift — match target dates to closest same-DOW source dates
    2. Holiday band shift — translate bands around holidays using date offset
    """

    def __init__(self, logger, params, overrides, context):
        self.context = context
        self.logger = logger
        self.mongoconn = params['mongoconn']
        self.db = self.mongoconn['qp_dw_rmalloc']

        self.month_key = overrides.get('month')
        if not self.month_key:
            raise ValueError("overrides must contain 'month' (e.g. '2026-03')")

        self.run_id = overrides.get('run_id')
        self.source_month = self._resolve_source_month()
        self._load_tunable_config()

    # ── Helpers ───────────────────────────────────────────────────────

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

    def _load_tunable_config(self):
        """Load tunable constants from saleinv_parameter_values collection."""
        def _do_load():
            keys = ['LEANPEAK_HOLIDAY_WINDOW_DAYS', 'LEANPEAK_MIN_BAND_LENGTH']
            param_docs = {
                doc['parameterKey']: doc['parameterValue']
                for doc in self.db.saleinv_parameter_values.find(
                    {'parameterKey': {'$in': keys}},
                    {'_id': 0, 'parameterKey': 1, 'parameterValue': 1}
                )
            }
            return param_docs

        params = self._retry_db_operation(_do_load, "Load tunable config")

        self.holiday_window_days = int(params.get('LEANPEAK_HOLIDAY_WINDOW_DAYS', '7'))
        self.min_band_length = int(params.get('LEANPEAK_MIN_BAND_LENGTH', '3'))

        self.logger.info("Tunable config loaded from saleinv_parameter_values")

    def _resolve_source_month(self):
        """Determine source month: check month_map override, else same month last year."""
        def _do_fetch():
            doc = self.db[MONTH_MAP_COLLECTION].find_one(
                {'target_month': self.month_key}, {'_id': 0, 'mapped_month': 1}
            )
            return doc['mapped_month'] if doc else None
        mapped = self._retry_db_operation(_do_fetch, "Fetch month mapping")

        if mapped:
            self.logger.info(f"Month mapping override: {self.month_key} -> {mapped}")
            return mapped

        target_date = datetime.strptime(self.month_key, '%Y-%m')
        source = target_date.replace(year=target_date.year - 1).strftime('%Y-%m')
        self.logger.info(f"Using default source month: {self.month_key} -> {source}")
        return source

    # ── Data fetch ────────────────────────────────────────────────────

    def _fetch_source_leanpeak(self):
        """Fetch historical lean/peak tags for the source month."""
        def _do_fetch():
            docs = list(self.db[SOURCE_COLLECTION].find(
                {'Month': self.source_month},
                {'_id': 0, 'Sector': 1, 'DepartureDate': 1, 'DayOfWeek': 1, 'Tag': 1}
            ))
            if not docs:
                return pd.DataFrame()
            df = pd.DataFrame(docs)
            df['DepartureDate'] = pd.to_datetime(df['DepartureDate'])
            return df
        return self._retry_db_operation(_do_fetch, f"Fetch {SOURCE_COLLECTION} for {self.source_month}")

    def _fetch_holidays(self):
        """Fetch holiday mappings where target_date falls in the target month."""
        def _do_fetch():
            target_date = datetime.strptime(self.month_key, '%Y-%m')
            last_day = calendar.monthrange(target_date.year, target_date.month)[1]
            month_start_str = target_date.strftime('%Y-%m-01')
            month_end_str = target_date.replace(day=last_day).strftime('%Y-%m-%d')

            docs = list(self.db[HOLIDAY_COLLECTION].find(
                {'target_date': {'$gte': month_start_str, '$lte': month_end_str}},
                {'_id': 0, 'mapped_date': 1, 'target_date': 1, 'event': 1}
            ))
            return docs
        return self._retry_db_operation(_do_fetch, f"Fetch holidays for {self.month_key}")

    # ── Step 1: DOW Shift ─────────────────────────────────────────────

    def _build_target_dates(self):
        """Build all dates in the target month with their DOW."""
        target_date = datetime.strptime(self.month_key, '%Y-%m')
        last_day = calendar.monthrange(target_date.year, target_date.month)[1]
        dates = pd.date_range(target_date, periods=last_day, freq='D')
        df = pd.DataFrame({'TargetDate': dates})
        df['DayOfWeek'] = df['TargetDate'].dt.weekday + 1  # 1=Mon, 7=Sun
        return df

    def _dow_shift(self, source_df):
        """Map each target date to the closest same-DOW source date's tag."""
        target_dates = self._build_target_dates()
        sectors = source_df['Sector'].unique()

        # Build source DOW lookup: {dow: sorted list of (date, day_of_month)}
        source_by_dow = {}
        for dow in range(1, 8):
            dow_dates = source_df[source_df['DayOfWeek'] == dow]['DepartureDate'].unique()
            source_by_dow[dow] = sorted(dow_dates)

        # Source month boundaries for validation
        source_date = datetime.strptime(self.source_month, '%Y-%m')
        source_month_start = source_date.replace(day=1)
        source_last_day = calendar.monthrange(source_date.year, source_date.month)[1]
        source_month_end = source_date.replace(day=source_last_day)

        # For each target date, find closest same-DOW source date
        target_to_source = {}
        for _, row in target_dates.iterrows():
            t_date = row['TargetDate']
            dow = row['DayOfWeek']
            candidates = source_by_dow.get(dow, [])
            if not candidates:
                target_to_source[t_date] = None
                continue

            # Find closest by day-of-month proximity
            t_dom = t_date.day
            best = min(candidates, key=lambda d: abs(pd.Timestamp(d).day - t_dom))
            best_ts = pd.Timestamp(best)

            # Validate: must be within source month
            if best_ts < pd.Timestamp(source_month_start) or best_ts > pd.Timestamp(source_month_end):
                target_to_source[t_date] = None
            else:
                target_to_source[t_date] = best_ts

        # Build mapped df
        rows = []
        for sector in sectors:
            sector_source = source_df[source_df['Sector'] == sector].set_index('DepartureDate')
            for _, row in target_dates.iterrows():
                t_date = row['TargetDate']
                dow = row['DayOfWeek']
                source_date_mapped = target_to_source.get(t_date)

                if source_date_mapped is not None and source_date_mapped in sector_source.index:
                    tag = int(sector_source.loc[source_date_mapped, 'Tag'])
                else:
                    tag = 0

                rows.append({
                    'Sector': sector,
                    'DepartureDate': t_date,
                    'DayOfWeek': dow,
                    'Tag': tag,
                    'Tag_Manual': 0
                })

        return pd.DataFrame(rows)

    # ── Step 2: Holiday Band Shift ──────────────────────────────────

    def _find_bands_in_range(self, sector_source, range_start, range_end):
        """Find all bands (consecutive same non-zero tags, length >= 3) that
        start or end within the given date range. Returns full bands including overflow."""
        sector_source = sector_source.sort_values('DepartureDate').reset_index(drop=True)
        dates = sector_source['DepartureDate'].values
        tags = sector_source['Tag'].values

        # Identify all bands
        bands = []
        i = 0
        while i < len(tags):
            if tags[i] == 0:
                i += 1
                continue
            val = tags[i]
            j = i
            while j < len(tags) and tags[j] == val:
                j += 1
            if (j - i) >= self.min_band_length:
                bands.append((i, j, val))  # start_idx, end_idx (exclusive), tag value
            i = j

        # Find bands that start or end within the range
        range_start_ts = pd.Timestamp(range_start)
        range_end_ts = pd.Timestamp(range_end)

        touching_bands = []
        for start_idx, end_idx, val in bands:
            band_start = pd.Timestamp(dates[start_idx])
            band_end = pd.Timestamp(dates[end_idx - 1])
            # Band starts or ends within the range
            if (range_start_ts <= band_start <= range_end_ts) or \
               (range_start_ts <= band_end <= range_end_ts):
                touching_bands.append((start_idx, end_idx))

        return touching_bands

    def _holiday_shift(self, mapped_df, source_df, holidays):
        """Apply holiday band translation for each holiday in the target month."""
        if not holidays:
            return mapped_df

        for holiday in holidays:
            mapped_date = pd.Timestamp(holiday['mapped_date'])
            target_date = pd.Timestamp(holiday['target_date'])
            event = holiday.get('event', 'Unknown')
            offset = target_date - mapped_date

            # ±N day window around source holiday
            source_range_start = mapped_date - timedelta(days=self.holiday_window_days)
            source_range_end = mapped_date + timedelta(days=self.holiday_window_days)

            sectors = source_df['Sector'].unique()

            for sector in sectors:
                sector_source = source_df[source_df['Sector'] == sector].copy()
                if sector_source.empty:
                    continue

                # Find bands touching the ±7 window
                touching_bands = self._find_bands_in_range(sector_source, source_range_start, source_range_end)

                sector_source_sorted = sector_source.sort_values('DepartureDate').reset_index(drop=True)
                source_dates = sector_source_sorted['DepartureDate'].values
                source_tags = sector_source_sorted['Tag'].values

                if touching_bands:
                    # Determine the full range to translate: union of 15-day window + overflowing bands
                    min_idx = min(b[0] for b in touching_bands)
                    max_idx = max(b[1] for b in touching_bands)

                    # Also include the 15-day window dates
                    window_mask = (sector_source_sorted['DepartureDate'] >= source_range_start) & \
                                  (sector_source_sorted['DepartureDate'] <= source_range_end)
                    window_indices = sector_source_sorted[window_mask].index

                    if len(window_indices) > 0:
                        translate_start = min(min_idx, window_indices[0])
                        translate_end = max(max_idx, window_indices[-1] + 1)
                    else:
                        translate_start = min_idx
                        translate_end = max_idx
                else:
                    # No touching bands — just translate the 15-day window as-is
                    window_mask = (sector_source_sorted['DepartureDate'] >= source_range_start) & \
                                  (sector_source_sorted['DepartureDate'] <= source_range_end)
                    window_indices = sector_source_sorted[window_mask].index
                    if len(window_indices) == 0:
                        continue
                    translate_start = window_indices[0]
                    translate_end = window_indices[-1] + 1

                # Translate source dates to target dates and overwrite
                for idx in range(translate_start, translate_end):
                    source_date = pd.Timestamp(source_dates[idx])
                    target_shifted = source_date + offset
                    tag_val = int(source_tags[idx])

                    # Overwrite in mapped_df
                    mask = (mapped_df['Sector'] == sector) & (mapped_df['DepartureDate'] == target_shifted)
                    if mask.any():
                        mapped_df.loc[mask, 'Tag'] = tag_val
            
            self.logger.info(f"Holiday shift applied for {event}: {mapped_date.strftime('%Y-%m-%d')} -> {target_date.strftime('%Y-%m-%d')}")

        return mapped_df

    # ── Insert & Run ──────────────────────────────────────────────────

    def _insert_to_mongo(self, df):
        """Insert mapped lean peak data into output collection."""
        if df.empty:
            self.logger.warning(f"Skipping insert for {OUTPUT_COLLECTION} — empty DataFrame")
            return 0

        def _do_insert():
            collection = self.db[OUTPUT_COLLECTION]
            records = df.to_dict('records')
            collection.delete_many({'Month': self.month_key})
            collection.insert_many(records)
            return len(records)

        return self._retry_db_operation(_do_insert, f"Insert into {OUTPUT_COLLECTION} for {self.month_key}")

    def run(self):
        try:
            self.logger.info(f"Starting Lean Peak Map for {self.month_key} (source: {self.source_month})")

            # Fetch source lean peak data
            source_df = self._fetch_source_leanpeak()
            if source_df.empty:
                self.logger.warning(f"No source lean peak data for {self.source_month}")
                self._update_run_summary(0, 'FAILED')
                return

            # Step 1: DOW shift
            mapped_df = self._dow_shift(source_df)
            self.logger.info(f"DOW shift complete: {len(mapped_df)} records")

            # Step 2: Holiday band shift (only if holidays exist)
            holidays = self._fetch_holidays()
            if holidays:
                mapped_df = self._holiday_shift(mapped_df, source_df, holidays)

            # Prepare for insert
            mapped_df.insert(0, 'Month', self.month_key)
            mapped_df['DepartureDate'] = mapped_df['DepartureDate'].astype(str)

            count = self._insert_to_mongo(mapped_df)
            self.logger.info(f"Inserted {count} records into {OUTPUT_COLLECTION} for {self.month_key}")

            self._update_run_summary(count, 'SUCCESS')

        except Exception as e:
            self._update_run_summary(0, 'FAILED')
            self.logger.error(f"Error in Lean Peak Map: {traceback.format_exc()}")
            raise

    def _update_run_summary(self, total_records, status):
        """Update the pre-inserted run summary doc with results."""
        if not self.run_id:
            self.logger.warning("No run_id provided, skipping run summary update")
            return

        def _do_update():
            self.db['saleinv_run_summary'].update_one(
                {'run_id': self.run_id},
                {'$set': {
                    'mapped_month': self.source_month,
                    'total_records': total_records,
                    'status': status,
                    'end_time': datetime.utcnow()
                }}
            )

        self._retry_db_operation(_do_update, f"Update run summary for {self.run_id}")
