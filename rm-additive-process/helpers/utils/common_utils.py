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
try:
    from scipy.stats import norm
except (ImportError, ModuleNotFoundError):
    from statistics import NormalDist

    class _NormAdapter:
        @staticmethod
        def ppf(p):
            if isinstance(p, (np.ndarray, list)):
                p = np.clip(p, 1e-12, 1.0 - 1e-12)
                return np.vectorize(lambda val: NormalDist().inv_cdf(float(val)))(p)
            p_clipped = float(np.clip(p, 1e-12, 1.0 - 1e-12))
            return NormalDist().inv_cdf(p_clipped)

        @staticmethod
        def pdf(x):
            if isinstance(x, (np.ndarray, list)):
                x = np.array(x, dtype=float)
                return (1.0 / math.sqrt(2.0 * math.pi)) * np.exp(-0.5 * x * x)
            x = float(x)
            return (1.0 / math.sqrt(2.0 * math.pi)) * math.exp(-0.5 * x * x)

        @staticmethod
        def cdf(x):
            if isinstance(x, (np.ndarray, list)):
                x = np.array(x, dtype=float)
                return 0.5 * (1.0 + np.vectorize(math.erf)(x / math.sqrt(2.0)))
            x = float(x)
            return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

    norm = _NormAdapter()
from sqlalchemy import text

import configsettings
from Constants import (
    RUN_SCOPED_CACHE_KEYS, REFRESH_SPEC,
    EMSR_SIGMA_FACTOR, PROMO_FARE_RATIO, PROMO_RBD, OPEN_BUCKET_MIDPOINT_FACTOR,
    BUY_UP_FACTOR
)

__all__ = [
    'get_time_cluster', 'get_quarter_abbr', 'numerical_tagging', 'tag_from_zscore',
    'middle_tagging', 'resolve_refresh_months', 'parse_fare_bounds',
    'run_sql', 'DatabaseConfig', 'AdditiveCache',
    'compute_flight_inventory_optimization', 'compute_inventory_optimization_df',
    'compute_flight_inventory_ng', 'compute_inventory_ng_df', 'INVENTORY_NG_COLUMNS',
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
    open_bucket_markup=OPEN_BUCKET_MIDPOINT_FACTOR,
    buy_up_factor=BUY_UP_FACTOR
):
    """
    Computes EMSR-b Protection Limits and AU (Authorization Levels) for a single flight.
    
    Steps:
      1. Sorts fare classes descending (highest fare -> lowest filed fare).
      2. Aggregates mean demand: Mu_Bar = sum(Mu_1..Mu_j)
      3. Aggregates std dev: Sigma_Bar = min(sqrt(sum(Sigma_i^2)), Factor * Mu_Bar)
      4. Aggregated weighted fare: F_j = sum(Mu_i * f_i) / Mu_Bar
      5. Next lower fare f_{j+1} (for lowest filed class, f_{n+1} is 80% promo fare).
      6. Probability Prob_j = (1 - (f_{j+1} / F_j)) * 1 / (1 - alpha), where alpha is
         the buy-up factor (`buy_up_factor`, 0 by default = no buy-up).
      7. Z-score z = Phi^{-1}(Prob_j)
      8. Protection Limit y_j = max(0, ceil(Mu_Bar + z * Sigma_Bar)), monotonically
         nested. Not capped at Capacity -- it is the demand the class wants held
         back, and the AU below is floored at 0 regardless.
      9. AU_j = Capacity - Protection_{j-1} (highest class AU = Capacity).
      10. Promo fare (W RBD) allocated at promo_ratio of lowest filed fare.
      11. Determines Opening Fare without considering Promo inventory.

    `pax_col` must hold the mean demand per fare bucket and `std_col` its standard
    deviation. Callers are responsible for passing the demand they intend to
    optimise on -- see AdditivePickupModelProcessor._compute_inventory_optimization,
    which passes the pipeline's final per-bucket demand.
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

    # Set FareMidpoint from FareBucket_Lower
    if 'FareMidpoint' not in df.columns:
        df['FareMidpoint'] = df['FareBucket_Lower']
    df['FareMidpoint'] = pd.to_numeric(df['FareMidpoint'], errors='coerce').fillna(0.0).round(2)

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

    # Buy-up factor: Prob_j is scaled by 1 / (1 - alpha). alpha must stay below 1,
    # and a negative alpha would suppress the probability rather than lift it.
    alpha = float(buy_up_factor or 0.0)
    alpha = float(np.clip(alpha, 0.0, 0.99))
    buy_up_multiplier = 1.0 / (1.0 - alpha)

    # Calculate protection limits y_1 ... y_n for filed classes
    y = []
    for j in range(n):
        mu_bar = sum(mu[:j+1])
        sig_bar_raw = math.sqrt(sum(s**2 for s in sigma[:j+1]))
        sig_bar = min(sig_bar_raw, sigma_factor * mu_bar)
        F_j = (sum(mu[i] * fares[i] for i in range(j+1)) / mu_bar) if mu_bar > 0 else fares[j]
        f_next = fares[j+1] if j < n - 1 else f_promo

        if F_j > 0:
            prob = (1.0 - (f_next / F_j)) * buy_up_multiplier
        else:
            prob = 0.5
        prob_clipped = float(np.clip(prob, 1e-6, 1.0 - 1e-6))
        z = float(norm.ppf(prob_clipped))

        # Protection limit is NOT capped at capacity: the reported value is the
        # demand the class would like protected, and AU below is floored at 0
        # anyway, so capping here would only understate protection on busy flights.
        y_j = max(0.0, float(mu_bar + z * sig_bar))
        if y:
            y_j = max(y_j, y[-1])  # monotonic nesting
        y.append(y_j)

    # Calculate AU (Authorization Level) for all classes
    # Class 1: AU = capacity
    # Class j (2..n): AU = capacity - y[j-2]
    # Promo (n+1): AU = capacity - y[n-1]
    au = [float(capacity)]
    for j in range(1, n):
        au.append(max(0.0, float(capacity - y[j-1])))
    au_promo = max(0.0, float(capacity - y[n-1])) if n > 0 else float(capacity)

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
    open_bucket_markup=OPEN_BUCKET_MIDPOINT_FACTOR,
    buy_up_factor=BUY_UP_FACTOR
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
            promo_rbd=promo_rbd, open_bucket_markup=open_bucket_markup,
            buy_up_factor=buy_up_factor
        )

    flight_group_cols = [sec_col, date_col, flt_col]
    flight_dfs = []
    for _, group in bucket_df.groupby(flight_group_cols, sort=False):
        opt_group = compute_flight_inventory_optimization(
            group, pax_col=pax_col, std_col=std_col,
            sigma_factor=sigma_factor, promo_ratio=promo_ratio,
            promo_rbd=promo_rbd, open_bucket_markup=open_bucket_markup,
            buy_up_factor=buy_up_factor
        )
        flight_dfs.append(opt_group)

    if not flight_dfs:
        return pd.DataFrame()
    return pd.concat(flight_dfs, ignore_index=True)


# =============================================================================
# NextGen Inventory Optimization (inventory_ng)
# =============================================================================

INVENTORY_NG_COLUMNS = [
    'sector', 'date', 'DOW', 'STD', 'flight', 'NDO', 'capacity', 'Total_Bookings',
    'fare', 'oal_b2c_min', 'anchor_fare', 'strategy_fare', 'Promo_Flag',
    'mu_bar_orig', 'mu', 'AU', 'P_eq', 'mu_bar_2', 'mu_2', 'AU_2', 'AU_Revised',
    'predicted_pickup', 'Predicted_Flown', 'Predicted_pickup_Revised', 'Predicted_Flown_Revised',
    'LF_Forecast_Revised', 'AF_Forecast', 'RASK_Forecast', 'Distance', 'RPKM', 'ASKM',
    'Capped_Predicted_Flown_Revised', 'A_cap', 'Current_Revenue', 'Expected_Revenue',
    'Total_Expected_Revenue', 'Predicted_Revenue', 'SnapshotDate', 'Tagging',
    'FareBucket', 'sigma', 'STD_future', 'qp_b2c_min', 'sigma_bar', 'F_j',
    'protection_limit_yj', 'ProtectionLimit', 'sigma_3', 'ProtectionLimit_3', 'sigma_bar_3'
]


def compute_flight_inventory_ng(
    flight_df,
    pax_col='Final_pax_count',
    std_col='weighted_pax_std',
    capacity=None,
    current_bookings=None,
    current_revenue=None,
    distance=None,
    market_min_fare=None,
    buy_up_factor=0.0,
    sigma_factor=0.25,
    first_rbd=20.0,
    promo_ratio=0.80,
    backstop=None,
    hardanchor=None,
    open_bucket_markup=OPEN_BUCKET_MIDPOINT_FACTOR
):
    """
    Computes full NextGen AU optimization, competition response, demand redistribution,
    and closed-form revenue forecasting for a single flight.
    Produces a DataFrame conforming to INVENTORY_NG_COLUMNS (50 columns).
    """
    if flight_df is None or flight_df.empty:
        return pd.DataFrame(columns=INVENTORY_NG_COLUMNS)

    df = flight_df.copy()

    # Extract flight metadata
    sec = str(df['Sector'].iloc[0]) if 'Sector' in df.columns else str(df.get('sector', pd.Series([''])).iloc[0])
    raw_date = df['DepartureDate'].iloc[0] if 'DepartureDate' in df.columns else (df.get('DepartureDate_future', df.get('date', pd.Series(['']))).iloc[0])
    dep_date_str = str(pd.to_datetime(raw_date).date()) if pd.notna(raw_date) else ''
    dow = str(df['DOW'].iloc[0]) if 'DOW' in df.columns else (pd.to_datetime(raw_date).day_name() if pd.notna(raw_date) else '')
    std_str = str(df['STD'].iloc[0]) if 'STD' in df.columns else str(df.get('STD_future', pd.Series([''])).iloc[0])
    flt_col_val = df['FlightNumber'].iloc[0] if 'FlightNumber' in df.columns else (df.get('FlightNumber_future', df.get('flight', pd.Series([0]))).iloc[0])
    flt_num = int(flt_col_val) if pd.notna(flt_col_val) else 0

    cap_val = capacity if capacity is not None else (df['Capacity'].iloc[0] if 'Capacity' in df.columns and pd.notna(df['Capacity'].iloc[0]) else df.get('capacity', pd.Series([189.0])).iloc[0])
    cap = float(cap_val) if pd.notna(cap_val) and float(cap_val) > 0 else 189.0

    cb_col = 'Current_Bookings' if 'Current_Bookings' in df.columns else ('Total_Bookings' if 'Total_Bookings' in df.columns else ('Total Bookings' if 'Total Bookings' in df.columns else None))
    cb_val = current_bookings if current_bookings is not None else (df[cb_col].iloc[0] if cb_col and pd.notna(df[cb_col].iloc[0]) else 0.0)
    cb = float(cb_val) if pd.notna(cb_val) else 0.0

    curr_rev_val = current_revenue if current_revenue is not None else (df['Current_Revenue'].iloc[0] if 'Current_Revenue' in df.columns and pd.notna(df['Current_Revenue'].iloc[0]) else 0.0)
    curr_rev = float(curr_rev_val) if pd.notna(curr_rev_val) else 0.0

    dist_val = distance if distance is not None else (df['Distance'].iloc[0] if 'Distance' in df.columns and pd.notna(df['Distance'].iloc[0]) else 1000.0)
    dist = float(dist_val) if pd.notna(dist_val) and float(dist_val) > 0 else 1000.0

    raw_snap = df['SnapshotDate'].iloc[0] if 'SnapshotDate' in df.columns else None
    snap_date_str = str(pd.to_datetime(raw_snap).date()) if raw_snap is not None and pd.notna(raw_snap) else dep_date_str

    tagging = str(df['Tagging'].iloc[0]) if 'Tagging' in df.columns else 'Shoulder'
    ndo_val = df['future_flight_NDO'].iloc[0] if 'future_flight_NDO' in df.columns else df.get('NDO', pd.Series([0])).iloc[0]
    ndo = int(ndo_val) if pd.notna(ndo_val) else 0

    # Ensure FareBucket and bounds
    if 'FareBucket' not in df.columns and 'FareBasis' in df.columns:
        df['FareBucket'] = df['FareBasis']
    if 'FareBucket_Lower' not in df.columns or 'FareBucket_Upper' not in df.columns:
        source_col = 'FareBucket' if 'FareBucket' in df.columns else 'FareBasis'
        bounds = df[source_col].apply(parse_fare_bounds)
        df['FareBucket_Lower'] = [b[0] for b in bounds]
        df['FareBucket_Upper'] = [b[1] for b in bounds]

    df['FareBucket_Lower'] = pd.to_numeric(df['FareBucket_Lower'], errors='coerce')
    df['FareBucket_Upper'] = pd.to_numeric(df['FareBucket_Upper'], errors='coerce')

    # Compute fare from FareBucket_Lower
    df['fare'] = pd.to_numeric(df['FareBucket_Lower'], errors='coerce').fillna(0.0).round(2)

    # Sort descending by fare (highest class 0 to lowest class n-1)
    df = df.sort_values('fare', ascending=False).reset_index(drop=True)
    fares = df['fare'].tolist()
    n = len(df)

    # Extract mu and sigma
    actual_pax_col = pax_col if pax_col in df.columns else next((c for c in ['OptimizationDemand', 'WTP_pax_count', 'New Final_pax_count', 'Final_pax_count', 'weighted_pax_count', 'predicted_pax_count', 'mu'] if c in df.columns), df.columns[0])
    actual_std_col = std_col if std_col in df.columns else next((c for c in ['OptimizationDemandStd', 'weighted_pax_std', 'predicted_pax_std', 'sigma'] if c in df.columns), None)

    mu = pd.to_numeric(df[actual_pax_col], errors='coerce').fillna(0.0).clip(lower=0.0).tolist()
    sigma = pd.to_numeric(df[actual_std_col], errors='coerce').fillna(0.0).clip(lower=0.0).tolist() if actual_std_col else [0.0] * n

    # Phase 1: Initial EMSR-b
    f_lowest = fares[-1] if n > 0 else 0.0
    f_promo = round(promo_ratio * f_lowest, 2)
    alpha = float(np.clip(buy_up_factor or 0.0, 0.0, 0.99))
    buy_up_mult = 1.0 / (1.0 - alpha)
    sig_fact = float(sigma_factor if sigma_factor is not None else 0.25)
    first_rbd_seats = float(first_rbd if first_rbd is not None else 20.0)

    mu_bar_list = []
    sig_bar_list = []
    F_j_list = []
    y_list = []

    for j in range(n):
        mu_bar = float(sum(mu[:j+1]))
        sig_raw = math.sqrt(sum(s**2 for s in sigma[:j+1]))
        sig_bar = min(sig_raw, sig_fact * mu_bar)
        F_j = (sum(mu[i] * fares[i] for i in range(j+1)) / mu_bar) if mu_bar > 0 else fares[j]
        f_next = fares[j+1] if j < n - 1 else f_promo

        if F_j > 0:
            prob = (1.0 - (f_next / F_j)) * buy_up_mult
        else:
            prob = 0.5
        prob_clipped = float(np.clip(prob, 1e-6, 1.0 - 1e-6))
        z = float(norm.ppf(prob_clipped))

        y_j = max(0.0, float(mu_bar + z * sig_bar))
        if y_list:
            y_j = max(y_j, y_list[-1])  # monotonic nesting

        mu_bar_list.append(mu_bar)
        sig_bar_list.append(sig_bar)
        F_j_list.append(F_j)
        y_list.append(y_j)

    au_list = [float(cap)]
    for j in range(1, n):
        au_list.append(max(0.0, float(cap - y_list[j-1])))

    # Anchor Fare A: lowest filed fare where AU > current_bookings
    open_classes = [i for i in range(n) if au_list[i] > cb]
    if open_classes:
        anchor_fare = float(fares[open_classes[-1]])
    else:
        anchor_fare = float(fares[0]) if n > 0 else 0.0

    # Strategy Fare C
    if market_min_fare is not None and pd.notna(market_min_fare):
        c_min_val = float(market_min_fare)
        valid_fares = [f for f in fares if f <= c_min_val]
        strategy_fare = float(valid_fares[0]) if valid_fares else float(fares[-1])
    else:
        strategy_fare = anchor_fare

    # Phase 3: Demand Redistribution & Revised AU
    A = anchor_fare
    C = strategy_fare

    strat_candidates = [f for f in fares if f <= C]
    C_ladder = strat_candidates[0] if strat_candidates else fares[-1]
    strat_idx = fares.index(C_ladder)

    anchor_candidates = [f for f in fares if f <= A]
    a_ladder = anchor_candidates[0] if anchor_candidates else fares[-1]
    a_idx = fares.index(a_ladder)

    if C_ladder <= a_ladder or strategy_fare is None or pd.isna(strategy_fare):
        P_eq_list = [np.nan] * n
        mu_bar_2_list = mu_bar_list
        mu_2_list = mu
        sigma_2_list = sigma if sigma else [sig_fact * m for m in mu_2_list]
        sigma_3_list = sigma_2_list
        sigma_bar_3_list = sig_bar_list
        y_2_list = y_list
        au_2_list = au_list
        au_revised = [float(x) for x in au_2_list]
        au_revised[strat_idx] = min(au_revised[strat_idx], cb + first_rbd_seats)
    else:
        P_max = fares[0]

        P_eq_list = []
        mu_bar_2_list = []

        for i, p in enumerate(fares):
            if p < C_ladder:
                peq = 0.0
                m_bar_2 = 0.0
            elif p == C_ladder:
                peq = A
                anchor_candidates = [f for f in fares if f <= A]
                a_ladder = anchor_candidates[0] if anchor_candidates else fares[-1]
                a_idx = fares.index(a_ladder)
                m_bar_2 = mu_bar_list[a_idx]
            elif p >= P_max:
                peq = P_max
                m_bar_2 = mu_bar_list[0]
            else:
                denom = (P_max - C_ladder)
                peq = A + ((p - C_ladder) / denom) * (P_max - A) if denom > 0 else A

                if peq >= fares[0]:
                    m_bar_2 = mu_bar_list[0]
                elif peq <= fares[-1]:
                    m_bar_2 = mu_bar_list[-1]
                else:
                    m_bar_2 = 0.0
                    for k in range(len(fares) - 1):
                        if fares[k] >= peq >= fares[k+1]:
                            frac = (fares[k] - peq) / (fares[k] - fares[k+1]) if (fares[k] - fares[k+1]) > 0 else 0.0
                            m_bar_2 = mu_bar_list[k] + frac * (mu_bar_list[k+1] - mu_bar_list[k])
                            break

            P_eq_list.append(peq)
            mu_bar_2_list.append(m_bar_2)

        mu_2_list = []
        for i in range(n):
            if fares[i] < C_ladder:
                mu_2_list.append(0.0)
            elif i == 0:
                mu_2_list.append(mu_bar_2_list[0])
            else:
                mu_2_list.append(max(0.0, mu_bar_2_list[i] - mu_bar_2_list[i-1]))

        # For redistributed demand, standard deviation is parameterized by sig_fact * mu_2
        sigma_2_list = [sig_fact * m for m in mu_2_list]
        sigma_3_list = sigma_2_list
        sigma_bar_3_list = [sig_fact * sum(mu_2_list[:j+1]) for j in range(n)]

        y_2_list = []
        for j in range(n):
            if fares[j] < C_ladder:
                y_2_list.append(cap)
                continue
            m_bar = sum(mu_2_list[:j+1])
            s_bar = sig_fact * m_bar
            F_j_2 = (sum(mu_2_list[i] * fares[i] for i in range(j+1)) / m_bar) if m_bar > 0 else fares[j]
            f_next = fares[j+1] if j < n - 1 else f_promo

            if F_j_2 > 0:
                prob = (1.0 - (f_next / F_j_2)) * buy_up_mult
            else:
                prob = 0.5
            prob_clipped = float(np.clip(prob, 1e-6, 1.0 - 1e-6))
            z = float(norm.ppf(prob_clipped))
            y_j = max(0.0, float(m_bar + z * s_bar))
            if y_2_list and y_2_list[-1] < cap:
                y_j = max(y_j, y_2_list[-1])
            y_2_list.append(y_j)

        au_2_list = [float(cap)]
        for j in range(1, n):
            if fares[j] < C_ladder:
                au_2_list.append(0.0)
            else:
                au_2_list.append(max(0.0, float(cap - y_2_list[j-1])))

        if au_2_list[strat_idx] < cb:
            elevate_au = cb
            for k in range(strat_idx, -1, -1):
                if au_2_list[k] >= cb:
                    elevate_au = au_2_list[k]
                    break
            for k in range(strat_idx, -1, -1):
                if au_2_list[k] < elevate_au:
                    au_2_list[k] = elevate_au

        au_revised = [float(x) for x in au_2_list]
        au_revised[strat_idx] = min(au_revised[strat_idx], float(cb + first_rbd_seats))

    # Backstop floor
    if backstop is not None and pd.notna(backstop):
        try:
            b_val = float(backstop)
            for i, f in enumerate(fares):
                if f < b_val:
                    au_revised[i] = 0.0
        except ValueError:
            pass

    # Hardanchor cap
    if hardanchor is not None and pd.notna(hardanchor):
        try:
            h_val = float(hardanchor)
            for i, f in enumerate(fares):
                if f >= h_val:
                    au_revised[i] = cap
        except ValueError:
            pass

    # Phase 4: Closed-Form Revenue Forecasting
    A_cap = [0.0] * n
    ER = [0.0] * n
    Ai_list = [0.0] * n

    for i in reversed(range(n)):
        if fares[i] < (anchor_fare - 1e-5):
            Ai_list[i] = 0.0
            A_cap[i] = 0.0
            ER[i] = 0.0
            continue

        sum_lower = sum(A_cap[i+1:])
        Ai = max(0.0, float(au_2_list[i]) - cb - sum_lower)
        Ai_list[i] = Ai
        m_i = float(mu_2_list[i])
        s_i = float(sigma_2_list[i])

        if s_i > 0:
            zi = (Ai - m_i) / s_i
            phi_z = float(norm.pdf(zi))
            Phi_z = float(norm.cdf(zi))
            A_hat_i = m_i * Phi_z - s_i * phi_z + Ai * (1.0 - Phi_z)
        else:
            A_hat_i = min(m_i, Ai)

        A_cap[i] = max(0.0, A_hat_i)
        ER[i] = fares[i] * A_cap[i]

    total_expected_revenue = float(sum(ER))
    predicted_revenue = curr_rev + total_expected_revenue
    pred_pickup_init = float(sum(mu))
    pred_flown_init = cb + pred_pickup_init
    # For LF_Forecast_Revised, sum mu from anchor fare and above
    pred_pickup_rev = float(sum(m for f, m in zip(fares, mu) if f >= anchor_fare - 1e-5))
    pred_flown_rev = cb + pred_pickup_rev

    denom = min(cap, pred_flown_rev) if pred_flown_rev > 0 else cap
    af_forecast = (predicted_revenue / denom) if denom > 0 else 0.0
    lf_forecast_rev = ((pred_flown_rev / cap) * 100.0) if cap > 0 else 0.0
    rask_forecast = (predicted_revenue / (cap * dist)) if (cap * dist) > 0 else 0.0
    rpkm = pred_flown_rev * dist
    askm = cap * dist
    capped_pred_flown_rev = min(cap, pred_flown_rev)
    qp_b2c_min_val = float(fares[-1]) if n > 0 else 0.0

    # Build output DataFrame
    out_df = pd.DataFrame({
        'sector': [sec] * n,
        'date': [dep_date_str] * n,
        'DOW': [dow] * n,
        'STD': [std_str] * n,
        'flight': [flt_num] * n,
        'NDO': [ndo] * n,
        'capacity': [cap] * n,
        'Total_Bookings': [cb] * n,
        'fare': fares,
        'oal_b2c_min': [market_min_fare if market_min_fare is not None else np.nan] * n,
        'anchor_fare': [anchor_fare] * n,
        'strategy_fare': [strategy_fare] * n,
        'Promo_Flag': [0] * n,
        'mu_bar_orig': mu_bar_list,
        'mu': mu,
        'AU': au_list,
        'P_eq': P_eq_list,
        'mu_bar_2': mu_bar_2_list,
        'mu_2': mu_2_list,
        'AU_2': au_2_list,
        'AU_Revised': au_revised,
        'predicted_pickup': [pred_pickup_init] * n,
        'Predicted_Flown': [pred_flown_init] * n,
        'Predicted_pickup_Revised': [pred_pickup_rev] * n,
        'Predicted_Flown_Revised': [pred_flown_rev] * n,
        'LF_Forecast_Revised': [lf_forecast_rev] * n,
        'AF_Forecast': [af_forecast] * n,
        'RASK_Forecast': [rask_forecast] * n,
        'Distance': [dist] * n,
        'RPKM': [rpkm] * n,
        'ASKM': [askm] * n,
        'Capped_Predicted_Flown_Revised': [capped_pred_flown_rev] * n,
        'A_cap': A_cap,
        'Current_Revenue': [curr_rev] * n,
        'Expected_Revenue': ER,
        'Total_Expected_Revenue': [total_expected_revenue] * n,
        'Predicted_Revenue': [predicted_revenue] * n,
        'SnapshotDate': [snap_date_str] * n,
        'Tagging': [tagging] * n,
        'FareBucket': df['FareBucket'].tolist(),
        'sigma': sigma,
        'STD_future': [std_str] * n,
        'qp_b2c_min': [qp_b2c_min_val] * n,
        'sigma_bar': sig_bar_list,
        'F_j': F_j_list,
        'protection_limit_yj': y_list,
        'ProtectionLimit': y_list,
        'sigma_3': sigma_3_list,
        'ProtectionLimit_3': y_2_list,
        'sigma_bar_3': sigma_bar_3_list
    })

    return out_df[INVENTORY_NG_COLUMNS]


def compute_inventory_ng_df(
    bucket_df,
    pax_col='Final_pax_count',
    std_col='weighted_pax_std',
    buy_up_factor=0.0,
    sigma_factor=0.25,
    first_rbd=20.0,
    promo_ratio=0.80,
    open_bucket_markup=OPEN_BUCKET_MIDPOINT_FACTOR
):
    """
    Applies NextGen inventory optimization across all flights in the fare-bucket DataFrame.
    Returns a consolidated DataFrame conforming to INVENTORY_NG_COLUMNS.
    """
    if bucket_df is None or bucket_df.empty:
        return pd.DataFrame(columns=INVENTORY_NG_COLUMNS)

    date_col = 'DepartureDate_future' if 'DepartureDate_future' in bucket_df.columns else ('DepartureDate' if 'DepartureDate' in bucket_df.columns else None)
    flt_col = 'FlightNumber_future' if 'FlightNumber_future' in bucket_df.columns else ('FlightNumber' if 'FlightNumber' in bucket_df.columns else ('flight' if 'flight' in bucket_df.columns else None))
    sec_col = 'Sector' if 'Sector' in bucket_df.columns else ('sector' if 'sector' in bucket_df.columns else None)

    if not (date_col and flt_col and sec_col):
        return compute_flight_inventory_ng(
            bucket_df,
            pax_col=pax_col,
            std_col=std_col,
            buy_up_factor=buy_up_factor,
            sigma_factor=sigma_factor,
            first_rbd=first_rbd,
            promo_ratio=promo_ratio,
            open_bucket_markup=open_bucket_markup
        )

    snap_col = 'SnapshotDate' if 'SnapshotDate' in bucket_df.columns else ('snapshot_date' if 'snapshot_date' in bucket_df.columns else None)
    ndo_col = 'future_flight_NDO' if 'future_flight_NDO' in bucket_df.columns else ('NDO' if 'NDO' in bucket_df.columns else None)

    flight_group_cols = [c for c in [snap_col, sec_col, date_col, flt_col, ndo_col] if c is not None]
    flight_dfs = []
    for _, group in bucket_df.groupby(flight_group_cols, sort=False):
        opt_group = compute_flight_inventory_ng(
            group,
            pax_col=pax_col,
            std_col=std_col,
            buy_up_factor=buy_up_factor,
            sigma_factor=sigma_factor,
            first_rbd=first_rbd,
            promo_ratio=promo_ratio,
            open_bucket_markup=open_bucket_markup
        )
        flight_dfs.append(opt_group)

    if not flight_dfs:
        return pd.DataFrame(columns=INVENTORY_NG_COLUMNS)
    return pd.concat(flight_dfs, ignore_index=True)