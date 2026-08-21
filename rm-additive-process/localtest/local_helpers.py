"""
localtest/local_helpers.py
=============================================================================
The local substitutions. Production code is not modified — every difference
lives in a subclass here.

LocalAdditiveCache
    Replaces the two things AdditiveCache does with AWS:
      * the parquet cache moves to a local folder;
      * `write_partitioned_s3`, which is the single hook the PROD save path uses
        for every output dataset, writes to local MongoDB and to CSV instead.
    Running the model in PROD mode against this cache means the local flow
    exercises the same save path as production, not the TEST-only CSV branch.

LocalCsvEsbHelper
    Used only when no local MySQL is reachable. Rebuilds the three fetch results
    (`hist_raw_df`, `future_flight_master`, `forward_raw_df`) from the input CSVs
    with pandas, mirroring the SQL in AdditiveEsbHelper. This is a reimplementation
    and can drift from the SQL — prefer the MySQL backend, which runs the real
    queries. Use this to iterate on model logic without standing up a server.
"""

import math
import os
from datetime import date as dt_date, datetime, time as dt_time, timedelta

import numpy as np
import pandas as pd

from Constants import PREDICTION_HORIZON_DAYS
from helpers.db_helpers.additive_esb_helper import AdditiveEsbHelper
from helpers.utils.common_utils import AdditiveCache

from localtest.schema import (DATASET_ALIASES, OUTPUT_COLLECTION_PREFIX,
                              OUTPUT_COLLECTIONS, SUPERSEDED_DATASETS, TBL_BOOKED,
                              TBL_FLOWN, TBL_INVENTORY_LEG)


# =============================================================================
# Record sanitising (BSON cannot take numpy scalars, NaN, or datetime.time)
# =============================================================================

def _ndo_columns(df):
    """The 0..180 pickup-curve columns, which arrive as ints or digit strings."""
    return [c for c in df.columns
            if isinstance(c, (int, np.integer))
            or (isinstance(c, str) and c.isdigit())]


def _is_missing(value):
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _coerce(value):
    if _is_missing(value):
        return None
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        f = float(value)
        return None if math.isnan(f) or math.isinf(f) else f
    if isinstance(value, float):
        return None if math.isnan(value) or math.isinf(value) else value
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if isinstance(value, pd.Timedelta):
        return str(value)
    if isinstance(value, dt_time):
        return value.strftime('%H:%M:%S')
    if isinstance(value, datetime):
        return value
    if isinstance(value, dt_date):
        return datetime.combine(value, dt_time())
    if isinstance(value, np.ndarray):
        return [_coerce(v) for v in value.tolist()]
    return value


def to_mongo_records(df, include_ndo=False, logger=None):
    out = df
    if not include_ndo:
        drop = _ndo_columns(df)
        if drop:
            out = df.drop(columns=drop)
    out = out.copy()
    out.columns = [str(c) for c in out.columns]

    # The inventory-optimization frame legitimately arrives with two `FareBucket`
    # columns: compute_flight_inventory_optimization copies FareBasis into
    # FareBucket, and _save_results then renames FareBasis -> FareBucket as well.
    # to_dict('records') would drop one silently, so collapse them explicitly.
    duplicated = out.columns[out.columns.duplicated()].unique().tolist()
    if duplicated:
        if logger:
            logger.warning(f"[local-out] duplicate column(s) {duplicated} — keeping the "
                           f"first occurrence of each")
        out = out.loc[:, ~out.columns.duplicated()]

    return [{k: _coerce(v) for k, v in rec.items()} for rec in out.to_dict('records')]


# =============================================================================
# LocalAdditiveCache
# =============================================================================

class LocalAdditiveCache(AdditiveCache):
    """Mongo/CSV output sink in place of S3, with the parquet cache disabled.

    Two changes to AdditiveCache:

      * every parquet-cache read/write is a no-op, so a run leaves no cache files
        behind and always refetches. That is the same behaviour production gets
        with FORCE_BACKFILL set, which is what the local flow always uses.
      * `write_partitioned_s3` — the single hook the PROD save path uses for
        every output dataset — writes to local MongoDB and CSV.
    """

    def __init__(self, cfg, mongo_db, logger, include_ndo_in_mongo=False,
                 write_csv=True, write_full_csv=False):
        super().__init__('TEST', None, logger)
        self.cfg = cfg
        self.mongo_db = mongo_db
        self.output_dir = cfg.outputs_dir
        self.include_ndo_in_mongo = include_ndo_in_mongo
        self.write_csv = write_csv
        self.write_full_csv = write_full_csv
        self.written = {}
        # Files this run has already opened; the first write truncates, the rest
        # append, so all sectors land in one file.
        self._csv_started = set()
        os.makedirs(self.output_dir, exist_ok=True)

    # -- parquet cache disabled ---------------------------------------------
    # Nothing downstream needs it: an empty read makes the ESB helper take its
    # full-backfill branch and makes tagging take its cold-start branch, which is
    # exactly what a local run wants every time.
    def store_write(self, df, rel_path):
        return

    def store_read(self, rel_path):
        return None

    def read_sector_cache(self, kind, sectors):
        return pd.DataFrame()

    def write_sector_cache(self, kind, df, months=None):
        return

    def list_cache_months(self, kind, sector):
        return []

    def delete_cache_partition(self, kind, sector, month):
        return

    # -- the one hook the PROD save path uses for every output dataset -------
    def write_partitioned_s3(self, df, dataset, partition_cols=('SnapshotDate', 'Sector')):
        if dataset in SUPERSEDED_DATASETS:
            # The optimized frame carries every column of this one plus the AU
            # ladder, so it is written under this dataset's name instead.
            self.logger.info(f"[local-out] {dataset}: superseded by the optimized "
                             f"frame, skipped")
            return

        emitted_as = dataset
        dataset = DATASET_ALIASES.get(dataset, dataset)
        if dataset != emitted_as:
            self.logger.info(f"[local-out] {emitted_as}: persisted as {dataset}")

        collection_name = OUTPUT_COLLECTIONS.get(dataset)
        if collection_name is None:
            # The model computes the normal branch too; only weighted is persisted.
            self.logger.info(f"[local-out] {emitted_as}: skipped (not persisted locally)")
            return

        if df is None or df.empty:
            self.logger.warning(f"[local-out] {dataset}: nothing to write.")
            self.written[dataset] = 0
            return

        collection = self.mongo_db[collection_name]

        # Mirror the S3 partition overwrite: replace this snapshot+sector slice only.
        keys = [c for c in partition_cols if c in df.columns]
        if keys:
            for combo, _part in df.groupby(keys, sort=False):
                combo = combo if isinstance(combo, tuple) else (combo,)
                query = {k: _coerce(v) for k, v in zip(keys, combo)}
                collection.delete_many(query)

        records = to_mongo_records(df, include_ndo=self.include_ndo_in_mongo,
                                   logger=self.logger)
        if records:
            collection.insert_many(records)
        self.written[dataset] = len(records)
        self.logger.info(f"[local-out] mongo {self.mongo_db.name}.{collection_name}: "
                         f"{len(records)} docs")

        if self.write_csv:
            self._write_csv(df, dataset, keys)

    def _write_csv(self, df, dataset, keys):
        """One CSV per dataset, all sectors in it: `<dataset>.csv`.

        Sectors are processed one at a time, so the first write of a run truncates
        the file and later sectors append to it. No date in the name — a re-run
        overwrites. The snapshot date is still a column (`SnapshotDate`).

        The 0..180 pickup-curve columns are dropped so the file opens in a
        spreadsheet; `write_full_csv` adds a `*_full.csv` that keeps them.
        """
        curve_cols = _ndo_columns(df)
        trimmed = df.drop(columns=curve_cols) if curve_cols else df

        self._to_csv(trimmed, f'{dataset}.csv')
        if self.write_full_csv and curve_cols:
            self._to_csv(df, f'{dataset}_full.csv')

    def _to_csv(self, df, filename):
        path = os.path.join(self.output_dir, filename)
        out = df.copy()
        out.columns = [str(c) for c in out.columns]

        first = filename not in self._csv_started
        out.to_csv(path, index=False, mode='w' if first else 'a', header=first)
        self._csv_started.add(filename)

        verb = 'wrote' if first else 'appended'
        self.logger.info(f"[local-out] csv   {path} "
                         f"({verb} {len(out)} rows x {len(out.columns)} cols)")

    def ensure_output_indexes(self):
        for collection_name in set(OUTPUT_COLLECTIONS.values()):
            self.mongo_db[collection_name].create_index(
                [('SnapshotDate', 1), ('Sector', 1), ('DepartureDate', 1)])

    def clear_outputs(self):
        """Drop every add_model_* collection, including any left by an earlier
        configuration, so --reset-outputs really means a clean slate."""
        names = [n for n in self.mongo_db.list_collection_names()
                 if n.startswith(OUTPUT_COLLECTION_PREFIX)]
        for name in names:
            self.mongo_db[name].drop()
        self.logger.info(f"[local-out] dropped {len(names)} output collection(s) in "
                         f"{self.mongo_db.name}: {sorted(names) or '(none)'}")


# =============================================================================
# LocalCsvEsbHelper — pandas mirror of the ESB SQL, for runs without MySQL
# =============================================================================

class LocalCsvEsbHelper(AdditiveEsbHelper):
    """Serves the three ESB fetches from the input CSVs instead of MySQL.

    Reimplements the SQL semantics, so treat the MySQL backend as the reference.
    """

    def __init__(self, inputs_dir, logger):
        super().__init__(None, logger)
        self.inputs_dir = inputs_dir
        self._raw = {}

    # ---- CSV loading -----------------------------------------------------
    def _load(self, filename, date_cols=('DepartureDate', 'BookingDate'),
              datetime_cols=()):
        if filename in self._raw:
            return self._raw[filename]
        path = os.path.join(self.inputs_dir, filename)
        if not os.path.exists(path):
            raise FileNotFoundError(f"missing input CSV: {path}")
        df = pd.read_csv(path)
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        for col in datetime_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        self._raw[filename] = df
        return df

    def _legs(self):
        df = self._load('inventory_leg.csv', date_cols=('DepartureDate',),
                        datetime_cols=('STD',)).copy()
        df['Sector'] = df['DepartureStation'].astype(str) + df['ArrivalStation'].astype(str)
        return df

    # ---- shared fare / pax derivation (mirrors the SELECT in both queries) --
    @staticmethod
    def _fare_and_pax(df):
        fb = df['FareBasis'].astype(str)
        # SUBSTRING(FareBasis, 5, 1) = 'T'  ->  ticketed-net branch
        uses_net = fb.str.slice(4, 5).eq('T')
        base = np.where(uses_net, df['FB_Net'], df['DiscountedBaseFare_Pro'])
        fare = base + df['YQ_Pro'] + df['RCS'] + df['CUTE']
        pax = (~fb.str.contains('SOL', na=False)).astype(int)
        return fare, pax

    # ---- orchestrator ----------------------------------------------------
    def fetch_all_data(self, active_sectors, today, current_date, force_backfill):
        active_sectors = list(active_sectors)
        yesterday = today - timedelta(days=1)
        month_start = current_date.replace(day=1)
        cutoff = min(month_start - pd.DateOffset(years=1),
                     month_start - pd.Timedelta(days=365))
        hist_start = cutoff.date()
        forward_end = today + timedelta(days=PREDICTION_HORIZON_DAYS)
        booking_cutoff = today - timedelta(days=545)

        self.logger.info(f"[csv-esb] hist: {hist_start}..{yesterday}, "
                         f"forward: {today}..{forward_end}")

        hist_df = self._fetch_historical(active_sectors, hist_start, yesterday, cutoff)
        self.cache.data['hist_raw_df'] = hist_df
        self._merge_std(hist_df)
        self._fetch_forward(active_sectors, today, forward_end, booking_cutoff)

    # ---- Query 1 + 2 -----------------------------------------------------
    def _fetch_historical(self, active_sectors, hist_start, yesterday, cutoff):
        df = self._load('hist_flown_data.csv').copy()
        df['Sector'] = df['Origin'].astype(str) + df['Destination'].astype(str)

        df = df[df['Sector'].isin(active_sectors)]
        df = df[(df['AdjustedCapacity'] > 0) & (df['FlightNumber'] < 6000)]
        df = df[(df['DepartureDate'] >= pd.Timestamp(hist_start)) &
                (df['DepartureDate'] <= pd.Timestamp(yesterday))]
        df = df[(df['BookingDate'] >= df['DepartureDate'] - pd.Timedelta(days=365)) &
                (df['BookingDate'] <= df['DepartureDate'])]

        if df.empty:
            self.logger.warning("[csv-esb] no historical rows after filtering.")
            return pd.DataFrame(columns=['Sector', 'DepartureDate', 'NDO', 'FlightNumber',
                                         'InventoryLegID', 'PNR', 'RBD', 'FareBasisRaw',
                                         'Fare', 'Pax Count', 'Capacity', 'Distance', 'DOW'])

        fare, pax = self._fare_and_pax(df)
        df = df.assign(_fare=fare, _pax=pax)

        group_cols = ['Sector', 'DepartureDate', 'BookingDate', 'FlightNumber',
                      'InventoryLegID', 'PNR', 'RBD', 'FareBasis',
                      'AdjustedCapacity', 'Distance']
        agg = (df.groupby(group_cols, sort=False, dropna=False)
                 .agg(Fare=('_fare', 'sum'), **{'Pax Count': ('_pax', 'sum')})
                 .reset_index())

        agg['NDO'] = (agg['DepartureDate'] - agg['BookingDate']).dt.days
        agg = agg.rename(columns={'FareBasis': 'FareBasisRaw',
                                  'AdjustedCapacity': 'Capacity'})
        agg['DOW'] = agg['DepartureDate'].dt.day_name()
        agg = agg[agg['DepartureDate'] >= cutoff]

        keep = ['Sector', 'DepartureDate', 'NDO', 'FlightNumber', 'InventoryLegID',
                'PNR', 'RBD', 'FareBasisRaw', 'Fare', 'Pax Count', 'Capacity',
                'Distance', 'DOW']
        out = agg[keep].copy()
        self.logger.info(f"[csv-esb] Query 1: {len(out)} rows, "
                         f"{out['Sector'].nunique()} sector(s) from {cutoff.date()}")
        return out

    def _merge_std(self, hist_df):
        """Attach STD from the flight master and drop InventoryLegID, as Query 2 does."""
        legs = self._legs()[['InventoryLegID', 'STD']].drop_duplicates('InventoryLegID')
        merged = hist_df.merge(legs, on='InventoryLegID', how='left')
        merged['STD'] = pd.to_datetime(merged['STD']).dt.time
        merged.drop(columns=['InventoryLegID'], inplace=True, errors='ignore')
        unresolved = int(merged['STD'].isna().sum())
        if unresolved:
            self.logger.warning(f"[csv-esb] {unresolved} historical rows have no STD.")
        self.cache.data['hist_raw_df'] = merged

    # ---- Query 3 ---------------------------------------------------------
    def _fetch_forward(self, active_sectors, today, forward_end, booking_cutoff):
        legs = self._legs()
        legs = legs[(legs['Sector'].isin(active_sectors)) &
                    (legs['DepartureDate'] >= pd.Timestamp(today)) &
                    (legs['DepartureDate'] <= pd.Timestamp(forward_end)) &
                    (legs['FlightNumber'] < 6000) &
                    (legs['CarrierCode'] == 'QP') &
                    (legs['Status'] != 2) &
                    (legs['AdjustedCapacity'] > 0)].copy()

        booked = self._load('booked_data.csv').copy()
        booked = booked[(booked['BookingDate'] >= pd.Timestamp(booking_cutoff)) &
                        (booked['BookingDate'] < pd.Timestamp(today))]

        if not booked.empty:
            fare, pax = self._fare_and_pax(booked)
            booked = booked.assign(_fare=fare, _pax=pax)
            booked = (booked.groupby(['InventoryLegID', 'DepartureDate', 'PNR', 'RBD',
                                      'FareBasis', 'BookingDate'], sort=False, dropna=False)
                            .agg(Fare=('_fare', 'sum'), **{'Pax Count': ('_pax', 'sum')})
                            .reset_index())
        else:
            booked = pd.DataFrame(columns=['InventoryLegID', 'DepartureDate', 'PNR', 'RBD',
                                           'FareBasis', 'BookingDate', 'Fare', 'Pax Count'])

        left = legs[['InventoryLegID', 'Sector', 'STD', 'FlightNumber',
                     'AdjustedCapacity', 'DepartureDate']].rename(
            columns={'AdjustedCapacity': 'Capacity', 'DepartureDate': 'DepartDate_raw'})
        forward_df = left.merge(booked.rename(columns={'FareBasis': 'FareBasisRaw'}),
                                on='InventoryLegID', how='left')

        forward_df['NDO'] = (pd.to_datetime(forward_df['DepartureDate']) -
                             pd.to_datetime(forward_df['BookingDate'])).dt.days
        forward_df['STD'] = pd.to_datetime(forward_df['STD']).dt.time
        forward_df['DepartureDate'] = forward_df['DepartureDate'].fillna(
            forward_df['DepartDate_raw'])
        forward_df.drop(columns=['DepartDate_raw'], inplace=True, errors='ignore')
        forward_df['DepartureDate'] = pd.to_datetime(forward_df['DepartureDate'])
        forward_df['DOW'] = forward_df['DepartureDate'].dt.day_name()
        forward_df['FlightNumber'] = pd.to_numeric(forward_df['FlightNumber'],
                                                   errors='coerce')
        forward_df = forward_df.dropna(subset=['FlightNumber'])
        forward_df['FlightNumber'] = forward_df['FlightNumber'].astype(int)

        fm_df = forward_df[['Sector', 'DepartureDate', 'STD', 'FlightNumber',
                            'Capacity']].drop_duplicates(
            subset=['Sector', 'DepartureDate', 'STD', 'FlightNumber'], keep='first')
        self.cache.data['future_flight_master'] = fm_df
        self.logger.info(f"[csv-esb] Query 3: flight universe {len(fm_df)} flights "
                         f"across {fm_df['Sector'].nunique()} sector(s).")

        valid = forward_df['NDO'].isna() | ((forward_df['NDO'] >= 0) &
                                            (forward_df['NDO'] <= 180))
        forward_df = forward_df[valid]

        missing = set(active_sectors) - set(fm_df['Sector'].unique())
        if missing:
            self.logger.warning(f"[csv-esb] no future flights for {sorted(missing)}")
        self.cache.data['forward_raw_df'] = forward_df
        self.logger.info(f"[csv-esb] Query 3: {len(forward_df)} forward booking rows.")


# =============================================================================
# Diagnostics
# =============================================================================

def mysql_row_counts(cfg):
    """Row count per seeded table; used by the runner's readiness check."""
    from sqlalchemy import text
    engine = cfg.mysql_engine()
    counts = {}
    try:
        with engine.connect() as conn:
            for table in (TBL_FLOWN, TBL_BOOKED, TBL_INVENTORY_LEG):
                counts[table] = conn.execute(
                    text(f"SELECT COUNT(*) FROM `{table}`")).scalar()
    finally:
        engine.dispose()
    return counts
