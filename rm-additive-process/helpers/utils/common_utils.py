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
import os
import re
from datetime import datetime
from datetime import datetime, timedelta, time

import pandas as pd
from sqlalchemy import text

import configsettings
from Constants import (
    RUN_SCOPED_CACHE_KEYS, REFRESH_SPEC
)

import numpy as np
import scipy.stats as stats

__all__ = [
    'get_time_cluster', 'get_quarter_abbr', 'numerical_tagging', 'tag_from_zscore',
    'middle_tagging', 'resolve_refresh_months', 'parse_fare_bounds',
    'run_sql', 'DatabaseConfig', 'AdditiveCache',
    'norm_cdf', 'norm_pdf', 'norm_ppf', 'filter_outliers_mad',
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

TIME_BANDS = [
    (time(2, 30), time(7, 30), '2:30-7:30'),
    (time(7, 30), time(11, 0), '7:30-11:00'),
    (time(11, 0), time(14, 30), '11:00-14:30'),
    (time(14, 30), time(18, 0), '14:30-18:00'),
    (time(18, 0), time(21, 0), '18:00-21:00'),
]


def get_time_cluster(std_time):
    """Map a departure time to its time-band label (from Constants.TIME_BANDS)."""
    if std_time is None or (isinstance(std_time, float) and std_time != std_time):
        return None
    for start, end, label in TIME_BANDS:
        if start <= std_time < end:
            return label
    return TIME_BANDS[-1][2] if TIME_BANDS else None


def get_quarter_abbr(date):
    q = (pd.to_datetime(date).month - 1) // 3 + 1
    return f"Q{q}"


def numerical_tagging(tag):
    return {'Lean': -1, 'Neutral': 0, 'Peak': 1}.get(tag, 0)


def tag_from_zscore(z):
    if z is None or (isinstance(z, float) and z != z):
        return 'Neutral'
    if z <= -0.5:
        return 'Lean'
    if z >= 0.5:
        return 'Peak'
    return 'Neutral'


def middle_tagging(tags):
    """Majority/middle tag of a small ordered collection."""
    vals = [numerical_tagging(t) for t in tags if t is not None]
    if not vals:
        return 'Neutral'
    avg = sum(vals) / len(vals)
    if avg <= -0.5:
        return 'Lean'
    if avg >= 0.5:
        return 'Peak'
    return 'Neutral'


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


def norm_cdf(z):
    """Standard normal cumulative distribution function Φ(z)."""
    return float(stats.norm.cdf(z))


def norm_pdf(z):
    """Standard normal probability density function ϕ(z)."""
    return float(stats.norm.pdf(z))


def norm_ppf(p, clamp_eps=1e-6):
    """Standard normal quantile function Φ^-1(p) with numerical clamping."""
    p_clamped = min(max(float(p), clamp_eps), 1.0 - clamp_eps)
    return float(stats.norm.ppf(p_clamped))


def filter_outliers_mad(values, coverage_param=3.0):
    """Filter numerical values using Modified Z-Score based on Median Absolute Deviation (MAD).

    Coverage parameter defaults to 3.0 (per ConstantsUtils / BRD).
    If MAD == 0, fallback to 0.15 * MED.
    Returns the filtered subset list of values.
    """
    if not values:
        return []
    clean_vals = [float(v) for v in values if pd.notna(v)]
    if len(clean_vals) <= 1:
        return clean_vals

    arr = np.array(clean_vals)
    med = np.median(arr)
    mad = np.median(np.abs(arr - med))
    if mad == 0:
        mad = 0.15 * med
    if mad == 0:
        return clean_vals

    mzs = np.abs(arr - med) / mad
    filtered = arr[mzs < coverage_param]
    return filtered.tolist()



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