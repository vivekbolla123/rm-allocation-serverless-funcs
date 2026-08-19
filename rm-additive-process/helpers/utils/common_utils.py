"""
helpers/utils/common_utils.py
=============================================================================
Shared, stateless helpers for the Additive Pickup Model + the AdditiveCache
class that both the processor and the ESB helper use for the parquet cache and
the run-scoped in-memory cache.

`from helpers.utils.common_utils import *` brings in the pure tagging/date
helpers, DatabaseConfig, and AdditiveCache.

Test mode + cache: AdditiveCache is constructed once in the processor with the
run mode and the (injected) S3 client, then handed to every helper. TEST reads
/writes local parquet under a working dir; PROD reads/writes S3. The in-memory
run-scoped dict lives on `cache.data`, so a fact written by the mongo helper
(e.g. tax master) is visible to the processor and the ESB helper — one cache,
shared by reference.
"""

import io
import math
import os
import re
from datetime import datetime, timedelta, time
import numpy as np

import pandas as pd
from scipy.stats import norm
from sqlalchemy import text

import configsettings
from Constants import (
    RUN_SCOPED_CACHE_KEYS, REFRESH_SPEC,
    EMSR_SIGMA_FACTOR, PROMO_FARE_RATIO, PROMO_RBD, OPEN_BUCKET_MIDPOINT_FACTOR
)

__all__ = [
    'get_time_cluster', 'get_quarter_abbr', 'numerical_tagging', 'tag_from_zscore',
    'middle_tagging', 'resolve_refresh_months', 'parse_fare_bounds',
    'run_sql', 'DatabaseConfig', 'AdditiveCache',
    'compute_flight_inventory_optimization', 'compute_inventory_optimization_df',
]


# =============================================================================
# TABLE NAMES (engines are injected; only the qualified names live here)
# =============================================================================

class DatabaseConfig:
    ESB_READ_SCHEMA       = configsettings.ESB_NAV_SCHEMA_NAME
    FLOWN_DATA_TABLE    = f'{ESB_READ_SCHEMA}.QP_DL_FLOWNDATAPAX'
    BOOKED_DATA_TABLE   = f'{ESB_READ_SCHEMA}.QP_DL_BOOKEDDATAPAX'
    FLIGHT_MASTER_TABLE = f'{ESB_READ_SCHEMA}.QP_DL_InventoryLeg'


# =============================================================================
# PURE HELPERS  (tagging / dates / parsing)  — moved verbatim from the model
# =============================================================================

def get_time_cluster(std_time):
    """Assign a time cluster string based on STD time object."""
    TIME_BANDS = [
        (time(2, 30), time(7, 30), '2:30-7:30'),
        (time(7, 30), time(11, 0), '7:30-11:00'),
        (time(11, 0), time(14, 30), '11:00-14:30'),
        (time(14, 30), time(18, 0), '14:30-18:00'),
        (time(18, 0), time(21, 0), '18:00-21:00'),
    ]
    if pd.isna(std_time):
        return np.nan
    if isinstance(std_time, str):
        std_time = datetime.strptime(std_time, '%H:%M:%S').time()
    elif isinstance(std_time, timedelta):
        std_time = (datetime.min + std_time).time()

    for start, end, label in TIME_BANDS:
        if start <= std_time < end:
            return label
    return '21:00-2:30'

def get_quarter_abbr(date):
    """Map a date to its quarter abbreviation (JFM, AMJ, JAS, OND)."""
    month = pd.to_datetime(date).month
    if month <= 3:
        return 'JFM'
    elif month <= 6:
        return 'AMJ'
    elif month <= 9:
        return 'JAS'
    return 'OND'


def numerical_tagging(tag):
    """Convert tagging string to numerical sort order."""
    mapping = {'Lean': 0, 'Shoulder': 1, 'Peak': 2, 'Super Peak': 3}
    return mapping.get(tag, 0)


def tag_from_zscore(z):
    """Assign lean/peak tag based on Z-Score value."""
    if z < -0.5:
        return 'Lean'
    elif z < 1:
        return 'Shoulder'
    elif z < 2:
        return 'Peak'
    return 'Super Peak'


def middle_tagging(tags):
    """Median-position tag of a (Tagging_int-sorted) group Series."""
    if len(tags) == 0:
        return None
    return tags.iloc[(len(tags) - 1) // 2]


def resolve_refresh_months(spec=None):
    """'' -> [] ; '3' -> last 3 month keys ; '2025-03,2025-11' -> those keys."""
    spec = REFRESH_SPEC if spec is None else spec
    spec = str(spec or '').strip()
    if not spec:
        return []
    if ',' in spec or '-' in spec:
        out = []
        for tok in spec.split(','):
            tok = tok.strip()
            if re.match(r'^\d{4}-\d{2}$', tok):
                out.append(tok)
        if out:
            return sorted(set(out))
    if spec.isdigit():
        n = int(spec)
        now = pd.Timestamp.now().normalize().replace(day=1)
        return sorted((now - pd.DateOffset(months=i)).strftime('%Y-%m') for i in range(n))
    return []


def parse_fare_bounds(fare_basis):
    """('7771.0-8654.0') -> (7771.0, 8654.0); ('28790.0+') -> (28790.0, None)."""
    s = str(fare_basis).strip()
    if s.endswith('+'):
        return float(s[:-1].replace('+', '')), None
    parts = s.split('-')
    return float(parts[0]), (float(parts[1]) if len(parts) > 1 and parts[1] else None)


def run_sql(engine, sql, chunksize=50000):
    with engine.connect() as conn:
        chunks = pd.read_sql(text(sql), conn, chunksize=chunksize)
        return pd.concat([c for c in chunks], ignore_index=True)


# =============================================================================
# AdditiveCache — mode-aware parquet cache + run-scoped in-memory dict
# =============================================================================

class AdditiveCache:
    """Single cache object shared by the processor and every helper.

    * `data`  : run-scoped in-memory dict (replaces the old self._data_cache).
    * parquet : TEST -> local dir; PROD -> S3 (bucket/prefix from Constants).
    """

    def __init__(self, mode, s3_client, logger):
        self.mode = mode
        self.s3 = s3_client
        self.logger = logger
        CACHE_DIR = os.path.join(os.getcwd(), 'additive_model_cache')
        self.s3_bucket = configsettings.ADDITIVE_MODEL_BUCKET
        self.s3_cache_prefix = f'{configsettings.ADDITIVE_MODEL_FILE_PATH}_cache'
        self.s3_output_prefix = f'{configsettings.ADDITIVE_MODEL_FILE_PATH}datasets'
        self.local_dir = CACHE_DIR
        self.data = {}                     # run-scoped in-memory cache

    # ---- in-memory run-scoped cache -------------------------------------
    def reset_batch(self):
        """Clear per-batch state but KEEP run-scoped master data."""
        self.data = {k: self.data[k] for k in RUN_SCOPED_CACHE_KEYS if k in self.data}

    # ---- parquet serialisation ------------------------------------------
    @staticmethod
    def to_parquet_bytes(df):
        out = df
        if any(not isinstance(c, str) for c in df.columns):
            out = df.copy()
            out.columns = [str(c) for c in out.columns]
        buf = io.BytesIO()
        out.to_parquet(buf, index=False, compression='snappy')
        buf.seek(0)
        return buf.getvalue()

    @staticmethod
    def restore_ndo_columns(df):
        df.columns = [int(c) if isinstance(c, str) and c.isdigit() else c
                      for c in df.columns]
        return df

    @staticmethod
    def month_key(ts):
        return pd.to_datetime(ts).strftime('%Y-%m')

    @staticmethod
    def sm_path(kind, sector, month):
        return f"{kind}/sector={sector}/month={month}.parquet"

    @staticmethod
    def sector_path(kind, sector):
        return f"{kind}/sector={sector}.parquet"

    # ---- low-level read / write -----------------------------------------
    def store_write(self, df, rel_path):
        if self.mode == 'TEST':
            full = os.path.join(self.local_dir, rel_path)
            os.makedirs(os.path.dirname(full), exist_ok=True)
            df.to_parquet(full, index=False, compression='snappy')
            return
        self.s3.put_object(Bucket=self.s3_bucket,
                           Key=f"{self.s3_cache_prefix}/{rel_path}",
                           Body=self.to_parquet_bytes(df))

    def store_read(self, rel_path):
        try:
            if self.mode == 'TEST':
                full = os.path.join(self.local_dir, rel_path)
                if not os.path.exists(full):
                    return None
                return self.restore_ndo_columns(pd.read_parquet(full))
            obj = self.s3.get_object(Bucket=self.s3_bucket,
                                     Key=f"{self.s3_cache_prefix}/{rel_path}")
            return self.restore_ndo_columns(pd.read_parquet(io.BytesIO(obj['Body'].read())))
        except Exception as e:
            self.logger.warning(f"  cache miss/unreadable {rel_path} ({e.__class__.__name__}).")
            return None

    # ---- sector/month partitioned cache ---------------------------------
    def read_sector_cache(self, kind, sectors):
        frames = []
        for s in sectors:
            for m in self.list_cache_months(kind, s):
                df = self.store_read(self.sm_path(kind, s, m))
                if df is not None and not df.empty:
                    frames.append(df)
        if not frames:
            return pd.DataFrame()
        out = pd.concat(frames, ignore_index=True)
        if 'DepartureDate' in out.columns:
            out['DepartureDate'] = pd.to_datetime(out['DepartureDate'])
        return out

    def write_sector_cache(self, kind, df, months=None):
        if df is None or df.empty or 'Sector' not in df.columns:
            return
        tmp = df.copy()
        tmp['_month'] = tmp['DepartureDate'].map(self.month_key)
        if months is not None:
            tmp = tmp[tmp['_month'].isin(set(months))]
        written = 0
        for (sector, month), part in tmp.groupby(['Sector', '_month'], sort=False):
            self.store_write(part.drop(columns='_month'), self.sm_path(kind, sector, month))
            written += 1
        self.logger.debug(f"  [{kind}] wrote {written} month partitions.")

    def list_cache_months(self, kind, sector):
        months = []
        if self.mode == 'TEST':
            d = os.path.join(self.local_dir, kind, f"sector={sector}")
            if os.path.isdir(d):
                for f in os.listdir(d):
                    m = re.match(r'month=(\d{4}-\d{2})\.parquet$', f)
                    if m:
                        months.append(m.group(1))
        else:
            prefix = f"{self.s3_cache_prefix}/{kind}/sector={sector}/"
            token = None
            while True:
                kw = dict(Bucket=self.s3_bucket, Prefix=prefix)
                if token:
                    kw['ContinuationToken'] = token
                resp = self.s3.list_objects_v2(**kw)
                for obj in resp.get('Contents', []):
                    m = re.search(r'month=(\d{4}-\d{2})\.parquet$', obj['Key'])
                    if m:
                        months.append(m.group(1))
                if not resp.get('IsTruncated'):
                    break
                token = resp.get('NextContinuationToken')
        return sorted(set(months))

    def delete_cache_partition(self, kind, sector, month):
        rel = self.sm_path(kind, sector, month)
        try:
            if self.mode == 'TEST':
                p = os.path.join(self.local_dir, rel)
                if os.path.exists(p):
                    os.remove(p)
            else:
                self.s3.delete_object(Bucket=self.s3_bucket,
                                      Key=f"{self.s3_cache_prefix}/{rel}")
        except Exception as e:
            self.logger.warning(f"  could not drop {rel} ({e.__class__.__name__}).")

    # ---- final dataset output (PROD) ------------------------------------
    @staticmethod
    def partition_value(v):
        if isinstance(v, (pd.Timestamp, datetime)):
            v = v.strftime('%Y-%m-%d')
        return re.sub(r'[^A-Za-z0-9_\-\.]', '_', str(v))

    def write_partitioned_s3(self, df, dataset, partition_cols=('SnapshotDate', 'Sector')):
        if df is None or df.empty:
            self.logger.warning(f"[S3] {dataset}: nothing to write.")
            return
        missing = [c for c in partition_cols if c not in df.columns]
        if missing:
            self.logger.error(f"[S3] {dataset}: missing partition cols {missing} — skipped.")
            return
        n = 0
        for keys, part in df.groupby(list(partition_cols), sort=False):
            keys = keys if isinstance(keys, tuple) else (keys,)
            path = "/".join(f"{c.lower()}={self.partition_value(v)}"
                            for c, v in zip(partition_cols, keys))
            body = part.drop(columns=list(partition_cols))
            self.s3.put_object(Bucket=self.s3_bucket,
                               Key=f"{self.s3_output_prefix}/{dataset}/{path}/data.parquet",
                               Body=self.to_parquet_bytes(body))
            n += 1
        self.logger.info(f"[S3] {dataset}: {n} partitions, {len(df)} rows.")


# =============================================================================
# INVENTORY OPTIMIZATION (EMSR-b)
# =============================================================================

def compute_flight_inventory_optimization(
    flight_df,
    pax_col='predicted_pax_count',
    std_col='predicted_pax_std',
    sigma_factor=EMSR_SIGMA_FACTOR,
    promo_ratio=PROMO_FARE_RATIO,
    promo_rbd=PROMO_RBD,
    open_bucket_markup=OPEN_BUCKET_MIDPOINT_FACTOR
):
    """
    Computes EMSR-b Protection Limits and AU (Authorization Levels) for a single flight.
    
    Steps:
      1. Sorts fare classes descending (highest fare -> lowest filed fare).
      2. Aggregates mean demand: Mu_Bar = sum(Mu_1..Mu_j)
      3. Aggregates std dev: Sigma_Bar = min(sqrt(sum(Sigma_i^2)), Factor * Mu_Bar)
      4. Aggregated weighted fare: F_j = sum(Mu_i * f_i) / Mu_Bar
      5. Next lower fare f_{j+1} (for lowest filed class, f_{n+1} is 80% promo fare).
      6. Probability Prob_j = 1 - (f_{j+1} / F_j)
      7. Z-score z = Phi^{-1}(Prob_j)
      8. Protection Limit y_j = max(0, ceil(Mu_Bar + z * Sigma_Bar)) bounded by Capacity.
      9. AU_j = Capacity - Protection_{j-1} (highest class AU = Capacity).
      10. Promo fare (W RBD) allocated at promo_ratio of lowest filed fare.
      11. Determines Opening Fare without considering Promo inventory.
    """
    if flight_df is None or flight_df.empty:
        return pd.DataFrame()

    df = flight_df.copy()

    # Ensure both FareBucket and FareBasis exist and match
    if 'FareBucket' not in df.columns and 'FareBasis' in df.columns:
        df['FareBucket'] = df['FareBasis']
    elif 'FareBasis' not in df.columns and 'FareBucket' in df.columns:
        df['FareBasis'] = df['FareBucket']

    # Parse lower and upper bucket bounds if missing or not numeric
    if 'FareBucket_Lower' not in df.columns or 'FareBucket_Upper' not in df.columns:
        source_col = 'FareBasis' if 'FareBasis' in df.columns else 'FareBucket'
        bounds = df[source_col].apply(parse_fare_bounds)
        df['FareBucket_Lower'] = [b[0] for b in bounds]
        df['FareBucket_Upper'] = [b[1] for b in bounds]

    df['FareBucket_Lower'] = pd.to_numeric(df['FareBucket_Lower'], errors='coerce')
    df['FareBucket_Upper'] = pd.to_numeric(df['FareBucket_Upper'], errors='coerce')

    # Compute FareMidpoint: (lower + upper)/2 for closed, lower * markup for open-ended
    if 'FareMidpoint' not in df.columns:
        df['FareMidpoint'] = np.where(
            df['FareBucket_Upper'].notna() & (df['FareBucket_Upper'] > 0),
            (df['FareBucket_Lower'] + df['FareBucket_Upper']) / 2.0,
            df['FareBucket_Lower'] * open_bucket_markup
        )
    df['FareMidpoint'] = df['FareMidpoint'].round(2)

    # Sort descending by FareMidpoint (highest fare class 1 -> lowest filed class n)
    df = df.sort_values('FareMidpoint', ascending=False).reset_index(drop=True)

    # Extract flight-level capacity and current bookings
    cap_val = df['Capacity'].iloc[0] if 'Capacity' in df.columns and pd.notna(df['Capacity'].iloc[0]) else 189
    capacity = int(cap_val) if cap_val > 0 else 0

    cb_col = 'Current_Bookings' if 'Current_Bookings' in df.columns else ('Total Bookings' if 'Total Bookings' in df.columns else None)
    current_bookings = float(df[cb_col].iloc[0]) if cb_col and pd.notna(df[cb_col].iloc[0]) else 0.0

    # Ensure pax and std columns exist
    if pax_col not in df.columns:
        pax_col = 'Final_pax_count' if 'Final_pax_count' in df.columns else ('predicted_pax_count' if 'predicted_pax_count' in df.columns else df.columns[0])
    if std_col not in df.columns:
        df[std_col] = 0.0

    mu = pd.to_numeric(df[pax_col], errors='coerce').fillna(0.0).clip(lower=0.0).tolist()
    sigma = pd.to_numeric(df[std_col], errors='coerce').fillna(0.0).clip(lower=0.0).tolist()
    fares = df['FareMidpoint'].tolist()
    n = len(df)

    # Lowest filed fare midpoint for Promo calculation
    f_lowest = fares[-1] if n > 0 else 0.0
    f_promo = round(promo_ratio * f_lowest, 2)
    lowest_lower = df['FareBucket_Lower'].iloc[-1] if n > 0 else f_lowest
    lowest_upper = df['FareBucket_Upper'].iloc[-1] if n > 0 else f_lowest
    promo_lower = round(promo_ratio * lowest_lower, 2) if pd.notna(lowest_lower) else f_promo
    promo_upper = round(promo_ratio * lowest_upper, 2) if pd.notna(lowest_upper) else f_promo

    # Calculate protection limits y_1 ... y_n for filed classes
    y = []
    for j in range(n):
        mu_bar = sum(mu[:j+1])
        sig_bar_raw = math.sqrt(sum(s**2 for s in sigma[:j+1]))
        sig_bar = min(sig_bar_raw, sigma_factor * mu_bar)
        F_j = (sum(mu[i] * fares[i] for i in range(j+1)) / mu_bar) if mu_bar > 0 else fares[j]
        f_next = fares[j+1] if j < n - 1 else f_promo

        if F_j > 0:
            prob = 1.0 - (f_next / F_j)
        else:
            prob = 0.5
        prob_clipped = float(np.clip(prob, 1e-6, 1.0 - 1e-6))
        z = float(norm.ppf(prob_clipped))

        y_j = max(0, math.ceil(mu_bar + z * sig_bar))
        if y:
            y_j = max(y_j, y[-1])  # monotonic nesting
        y_j = min(y_j, capacity)   # capacity ceiling
        y.append(y_j)

    # Calculate AU (Authorization Level) for all classes
    # Class 1: AU = capacity
    # Class j (2..n): AU = capacity - y[j-2]
    # Promo (n+1): AU = capacity - y[n-1]
    au = [capacity]
    for j in range(1, n):
        au.append(max(0, capacity - y[j-1]))
    au_promo = max(0, capacity - y[n-1]) if n > 0 else capacity

    df['ProtectionLimit'] = y
    df['AU'] = au
    if 'RBD' not in df.columns:
        df['RBD'] = None

    # Construct Promo row
    promo_row = df.iloc[-1].to_dict()
    if 'FareBucket' in promo_row:
        promo_row['FareBucket'] = 'Promo'
    if 'FareBasis' in promo_row:
        promo_row['FareBasis'] = 'Promo'
    promo_row['FareBucket_Lower'] = promo_lower
    promo_row['FareBucket_Upper'] = promo_upper
    promo_row['FareMidpoint'] = f_promo
    promo_row[pax_col] = 0.0
    promo_row[std_col] = 0.0
    promo_row['ProtectionLimit'] = 0
    promo_row['AU'] = au_promo
    promo_row['RBD'] = promo_rbd

    # Determine Opening Fare WITHOUT consideration of Promo inventory
    # Evaluate filed classes (indices 0 to n-1) with AU > current_bookings
    open_filed = df[df['AU'] > current_bookings]
    if not open_filed.empty:
        # Lowest filed fare is the last row in descending sorted list that is open
        opening_row = open_filed.iloc[-1]
    else:
        # If all filed classes are closed, default to highest fare class
        opening_row = df.iloc[0]

    opening_bucket_name = opening_row.get('FareBucket', opening_row.get('FareBasis', ''))
    opening_midpoint = opening_row['FareMidpoint']

    df['OpeningFareBucket'] = opening_bucket_name
    df['OpeningFareMidpoint'] = opening_midpoint
    df['IsOpeningFare'] = (df.index == opening_row.name)

    promo_row['OpeningFareBucket'] = opening_bucket_name
    promo_row['OpeningFareMidpoint'] = opening_midpoint
    promo_row['IsOpeningFare'] = False

    promo_df = pd.DataFrame([promo_row])
    result_df = pd.concat([df, promo_df], ignore_index=True)
    return result_df


def compute_inventory_optimization_df(
    bucket_df,
    pax_col='predicted_pax_count',
    std_col='predicted_pax_std',
    sigma_factor=EMSR_SIGMA_FACTOR,
    promo_ratio=PROMO_FARE_RATIO,
    promo_rbd=PROMO_RBD,
    open_bucket_markup=OPEN_BUCKET_MIDPOINT_FACTOR
):
    """
    Applies inventory optimization (EMSR-b protection limits and AU) across all flights
    in the fare-bucket DataFrame.
    """
    if bucket_df is None or bucket_df.empty:
        return bucket_df

    date_col = 'DepartureDate_future' if 'DepartureDate_future' in bucket_df.columns else ('DepartureDate' if 'DepartureDate' in bucket_df.columns else None)
    flt_col = 'FlightNumber_future' if 'FlightNumber_future' in bucket_df.columns else ('FlightNumber' if 'FlightNumber' in bucket_df.columns else None)
    sec_col = 'Sector' if 'Sector' in bucket_df.columns else None

    if not (date_col and flt_col and sec_col):
        return compute_flight_inventory_optimization(
            bucket_df, pax_col=pax_col, std_col=std_col,
            sigma_factor=sigma_factor, promo_ratio=promo_ratio,
            promo_rbd=promo_rbd, open_bucket_markup=open_bucket_markup
        )

    flight_group_cols = [sec_col, date_col, flt_col]
    flight_dfs = []
    for _, group in bucket_df.groupby(flight_group_cols, sort=False):
        opt_group = compute_flight_inventory_optimization(
            group, pax_col=pax_col, std_col=std_col,
            sigma_factor=sigma_factor, promo_ratio=promo_ratio,
            promo_rbd=promo_rbd, open_bucket_markup=open_bucket_markup
        )
        flight_dfs.append(opt_group)

    if not flight_dfs:
        return pd.DataFrame()
    return pd.concat(flight_dfs, ignore_index=True)