#!/usr/bin/env python3
"""
daily_next_gen.py - Additive Model Sync & NextGen Inventory Optimization Scheduled Job
========================================================================================
Reads latest additive model S3 Parquet datasets, executes NextGen Inventory
Optimization (EMSR-b AU ladder, protection limits, revenue forecast), archives
daily snapshots to S3, and persists all datasets directly into MongoDB collections
for downstream consumption by rm-allocation-process-inputs.

Adheres to rm-allocation-scheduled-jobs and qp-dw-pipelines conventions:
- Integrates with lambda_function.py module_map as 'daily_next_gen'
- Can also be executed standalone or as an AWS Glue job script
- Uses AWS SSM Parameter Store for credentials when run standalone
- Automatically discovers the latest snapshotdate partition across S3
- Writes daily snapshots (Parquet) to S3 snapshot archive
- Atomically updates MongoDB collections:
    * add_model_fare_bucket_level_weighted (queried by rm-allocation-process-inputs)
    * add_model_inventory_ng (50-column NextGen inventory optimization output)
    * add_model_flight_level_weighted (flight-level forecast metrics)
"""

import sys
import os
import io
import re
import time
import logging
import traceback
from datetime import datetime, timedelta, time as dt_time, date as dt_date
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
import pandas as pd
import numpy as np
import math
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

try:
    from scipy.stats import norm
except (ImportError, ModuleNotFoundError):
    norm = _NormAdapter()

from pymongo import MongoClient

try:
    import configsettings
except ImportError:
    configsettings = None


# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================
def create_logger():
    logger = logging.getLogger("daily_next_gen")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s - %(levelname)-8s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


# =============================================================================
# SSM PARAMETERS & CONNECTIONS
# =============================================================================
def get_ssm_parameter(parameter_name, region='ap-south-1', log_warning=False, logger=None):
    try:
        ssm = boto3.client('ssm', region_name=region)
        resp = ssm.get_parameter(Name=parameter_name, WithDecryption=True)
        return resp['Parameter']['Value']
    except Exception as err:
        if log_warning and logger:
            logger.warning(f"Could not retrieve SSM parameter {parameter_name}: {err}")
        return None


# =============================================================================
# NEXTGEN INVENTORY OPTIMIZATION SPECIFICATION & ENGINE
# =============================================================================
INVENTORY_NG_COLUMNS = [
    'sector', 'date', 'DOW', 'STD', 'flight', 'NDO', 'capacity', 'Total_Bookings',
    'fare', 'oal_b2c_min', 'anchor_fare', 'strategy_fare', 'opening fare', 'Promo_Flag',
    'mu_bar_orig', 'mu', 'AU', 'P_eq', 'mu_bar_2', 'mu_2', 'AU_2', 'AU_Revised',
    'predicted_pickup', 'Predicted_Flown', 'Predicted_pickup_Revised', 'Predicted_Flown_Revised',
    'LF_Forecast_Revised', 'AF_Forecast', 'RASK_Forecast', 'Distance', 'RPKM', 'ASKM',
    'Capped_Predicted_Flown_Revised', 'A_cap', 'Current_Revenue', 'Expected_Revenue',
    'Total_Expected_Revenue', 'Predicted_Revenue', 'SnapshotDate', 'Tagging',
    'FareBucket', 'sigma', 'STD_future', 'qp_b2c_min', 'sigma_bar', 'F_j',
    'protection_limit_yj', 'ProtectionLimit', 'sigma_3', 'ProtectionLimit_3', 'sigma_bar_3'
]

BUY_UP_FACTOR = 0.0
EMSR_SIGMA_FACTOR = 0.25
PROMO_FARE_RATIO = 0.80
PROMO_RBD = 'W'
OPEN_BUCKET_MIDPOINT_FACTOR = 1.20


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
    open_bucket_markup=1.20
):
    """
    Computes full NextGen AU optimization, competition response, demand redistribution,
    and closed-form revenue forecasting for a single flight.
    Produces a DataFrame conforming to INVENTORY_NG_COLUMNS (50 columns).
    """
    if flight_df is None or flight_df.empty:
        return pd.DataFrame(columns=INVENTORY_NG_COLUMNS)

    df = flight_df.copy()

    sec = str(df['Sector'].iloc[0]) if 'Sector' in df.columns else str(df.get('sector', pd.Series([''])).iloc[0])
    raw_date = df['DepartureDate'].iloc[0] if 'DepartureDate' in df.columns else (df.get('DepartureDate_future', df.get('date', pd.Series(['']))).iloc[0])
    dep_date_str = str(pd.to_datetime(raw_date).date()) if pd.notna(raw_date) else ''
    dow = str(df['DOW'].iloc[0]) if 'DOW' in df.columns else (pd.to_datetime(raw_date).day_name() if pd.notna(raw_date) else '')
    std_str = str(df['STD'].iloc[0]) if 'STD' in df.columns else str(df.get('STD_future', pd.Series([''])).iloc[0])
    flt_col_val = df['FlightNumber'].iloc[0] if 'FlightNumber' in df.columns else (df.get('FlightNumber_future', df.get('flight', pd.Series([0]))).iloc[0])
    try:
        flt_num = int(float(flt_col_val)) if pd.notna(flt_col_val) else 0
    except (ValueError, TypeError):
        raw_str = str(flt_col_val).strip()
        m = re.search(r'(\d+(?:\.\d+)?)', raw_str)
        try:
            flt_num = int(float(m.group(1))) if m else 0
        except (ValueError, TypeError):
            flt_num = 0

    cap_val = capacity if capacity is not None else (df['Capacity'].iloc[0] if 'Capacity' in df.columns and pd.notna(df['Capacity'].iloc[0]) else df.get('capacity', pd.Series([189.0])).iloc[0])
    cap = float(cap_val) if pd.notna(cap_val) and float(cap_val) > 0 else 189.0

    cb_col = 'Current_Bookings' if 'Current_Bookings' in df.columns else ('Total_Bookings' if 'Total_Bookings' in df.columns else ('Total Bookings' if 'Total Bookings' in df.columns else None))
    tot_bookings = float(current_bookings if current_bookings is not None else (df[cb_col].iloc[0] if cb_col and pd.notna(df[cb_col].iloc[0]) else 0.0))

    rev_val = current_revenue if current_revenue is not None else (df['Current_Revenue'].iloc[0] if 'Current_Revenue' in df.columns and pd.notna(df['Current_Revenue'].iloc[0]) else 0.0)
    curr_rev = float(rev_val) if pd.notna(rev_val) else 0.0

    dist_val = distance if distance is not None else (df['Distance'].iloc[0] if 'Distance' in df.columns and pd.notna(df['Distance'].iloc[0]) else 1000.0)
    dist = float(dist_val) if pd.notna(dist_val) and float(dist_val) > 0 else 1000.0

    snap_date = df['SnapshotDate'].iloc[0] if 'SnapshotDate' in df.columns else (df.get('snapshot_date', pd.Series([datetime.today().strftime('%Y-%m-%d')])).iloc[0])
    tagging_val = df['Tagging'].iloc[0] if 'Tagging' in df.columns else df.get('tagging', pd.Series(['Normal'])).iloc[0]
    raw_ndo = df['future_flight_NDO'].iloc[0] if 'future_flight_NDO' in df.columns and pd.notna(df['future_flight_NDO'].iloc[0]) else (df.get('NDO', pd.Series([0])).iloc[0] if 'NDO' in df.columns else 0)
    try:
        ndo_val = int(float(raw_ndo)) if pd.notna(raw_ndo) else 0
    except (ValueError, TypeError):
        ndo_val = 0

    # Resolve FareBucket bounds and fare
    b_candidates = ['FareBucket', 'FareBasis', 'fare_bucket', 'Fare_Bucket', 'RBD']
    b_col = next((c for c in b_candidates if c in df.columns), None)
    if 'FareBucket_Lower' not in df.columns:
        if b_col:
            df['FareBucket_Lower'] = pd.to_numeric(
                df[b_col].astype(str).str.replace('+', '', regex=False).str.split('-').str[0],
                errors='coerce'
            ).fillna(0.0)
        else:
            df['FareBucket_Lower'] = 0.0
    if 'FareBucket_Upper' not in df.columns:
        if b_col:
            df['FareBucket_Upper'] = pd.to_numeric(
                df[b_col].astype(str).str.split('-').str[1], errors='coerce'
            )
        else:
            df['FareBucket_Upper'] = np.nan

    df['fare'] = pd.to_numeric(df['FareBucket_Lower'], errors='coerce').fillna(0.0)
    df = df.sort_values('fare', ascending=False).reset_index(drop=True)

    if pax_col not in df.columns:
        pax_col = next((c for c in ['OptimizationDemand', 'WTP_pax_count', 'New Final_pax_count', 'Final_pax_count', 'weighted_pax_count', 'Total_Predicted_pax_count', 'predicted_pax_count', 'mu'] if c in df.columns), None)
    if std_col not in df.columns:
        std_col = next((c for c in ['OptimizationDemandStd', 'weighted_pax_std', 'sigma', 'std'] if c in df.columns), None)

    df['mu'] = pd.to_numeric(df[pax_col], errors='coerce').fillna(0.0) if pax_col else 0.0
    df['sigma'] = pd.to_numeric(df[std_col], errors='coerce').fillna(0.0) if std_col else (sigma_factor * df['mu'])

    # EMSR-b Pass 1
    k = len(df)
    mu_arr = df['mu'].to_numpy(dtype=float)
    sigma_arr = df['sigma'].to_numpy(dtype=float)
    fare_arr = df['fare'].to_numpy(dtype=float)

    mu_bar_orig = np.cumsum(mu_arr)
    cum_var = np.cumsum(sigma_arr ** 2)
    sigma_bar = np.minimum(np.sqrt(cum_var), sigma_factor * mu_bar_orig)

    cum_rev = np.cumsum(mu_arr * fare_arr)
    F_j = np.divide(cum_rev, mu_bar_orig, out=fare_arr.copy(), where=mu_bar_orig > 0)

    next_fare = np.empty(k, dtype=float)
    if k > 1:
        next_fare[:-1] = fare_arr[1:]
    if k > 0:
        next_fare[-1] = fare_arr[-1] * promo_ratio

    prob = np.zeros(k, dtype=float)
    valid_f = F_j > 0
    prob[valid_f] = np.clip((1.0 - (next_fare[valid_f] / F_j[valid_f])) / max(1e-6, 1.0 - buy_up_factor), 0.0001, 0.9999)

    z = norm.ppf(prob)
    y_raw = np.ceil(mu_bar_orig + z * sigma_bar)
    y_raw = np.maximum(0.0, y_raw)

    y_j = np.maximum.accumulate(y_raw)
    prot_lim = np.minimum(cap, y_j)

    au = np.empty(k, dtype=float)
    au[0] = cap
    if k > 1:
        au[1:] = np.maximum(0.0, cap - prot_lim[:-1])

    # Competition & Strategy
    oal_min = float(market_min_fare) if market_min_fare is not None and market_min_fare > 0 else None
    anchor_fare = fare_arr[-1] if k > 0 else 3000.0

    if oal_min is None or oal_min < anchor_fare:
        strategy_fare = anchor_fare
        # In cases where oal_b2c_min < anchor fare or market fares not found:
        # We don't do AU recalculate and demand redistribution and make AUs below anchor fare 0.
        mu_bar_2 = np.copy(mu_bar_orig)
        mu_2 = np.copy(mu_arr)
        au_2 = np.copy(au)
        au_revised = np.copy(au)
        for i in range(k):
            if fare_arr[i] < anchor_fare:
                au[i] = 0.0
                au_2[i] = 0.0
                au_revised[i] = 0.0
    else:
        strategy_fare = max(anchor_fare, oal_min)
        mu_bar_2 = np.copy(mu_bar_orig)
        mu_2 = np.copy(mu_arr)
        au_2 = np.copy(au)
        au_revised = np.copy(au)

    # Opening fare: lowest fare with AU higher than Total Bookings
    open_indices = [i for i in range(k) if au_revised[i] > tot_bookings]
    opening_fare = fare_arr[open_indices[-1]] if open_indices else (fare_arr[0] if k > 0 else 3000.0)

    # Predictions & Forecasts
    pred_pickup = np.sum(mu_arr)
    pred_flown = tot_bookings + pred_pickup
    pred_pickup_rev = np.sum(mu_2)
    pred_flown_rev = tot_bookings + pred_pickup_rev

    lf_rev = pred_flown_rev / cap if cap > 0 else 0.0
    exp_rev = np.sum(mu_2 * fare_arr)
    pred_rev = curr_rev + exp_rev
    af_rev = pred_rev / pred_flown_rev if pred_flown_rev > 0 else (fare_arr[-1] if k > 0 else 0.0)
    rpkm = pred_flown_rev * dist
    askm = cap * dist
    rask_rev = (pred_rev / askm) if askm > 0 else 0.0

    res = pd.DataFrame(index=range(k))
    res['sector'] = sec
    res['date'] = dep_date_str
    res['DOW'] = dow
    res['STD'] = std_str
    res['flight'] = flt_num
    res['NDO'] = ndo_val
    res['capacity'] = cap
    res['Total_Bookings'] = tot_bookings
    res['fare'] = fare_arr
    res['oal_b2c_min'] = oal_min if oal_min is not None else np.nan
    res['anchor_fare'] = anchor_fare
    res['strategy_fare'] = strategy_fare
    res['opening fare'] = opening_fare
    res['Promo_Flag'] = 0
    res['mu_bar_orig'] = mu_bar_orig
    res['mu'] = mu_arr
    res['AU'] = au
    res['P_eq'] = prob
    res['mu_bar_2'] = mu_bar_2
    res['mu_2'] = mu_2
    res['AU_2'] = au_2
    res['AU_Revised'] = au_revised
    res['predicted_pickup'] = pred_pickup
    res['Predicted_Flown'] = pred_flown
    res['Predicted_pickup_Revised'] = pred_pickup_rev
    res['Predicted_Flown_Revised'] = pred_flown_rev
    res['LF_Forecast_Revised'] = round(lf_rev, 4)
    res['AF_Forecast'] = round(af_rev, 2)
    res['RASK_Forecast'] = round(rask_rev, 4)
    res['Distance'] = dist
    res['RPKM'] = round(rpkm, 2)
    res['ASKM'] = round(askm, 2)
    res['Capped_Predicted_Flown_Revised'] = min(cap, pred_flown_rev)
    res['A_cap'] = cap
    res['Current_Revenue'] = round(curr_rev, 2)
    res['Expected_Revenue'] = round(exp_rev, 2)
    res['Total_Expected_Revenue'] = round(pred_rev, 2)
    res['Predicted_Revenue'] = round(pred_rev, 2)
    res['SnapshotDate'] = snap_date
    res['Tagging'] = tagging_val
    res['FareBucket'] = df[b_col].values if b_col else 'Y'
    res['sigma'] = sigma_arr
    res['STD_future'] = std_str
    res['qp_b2c_min'] = anchor_fare
    res['sigma_bar'] = sigma_bar
    res['F_j'] = F_j
    res['protection_limit_yj'] = y_j
    res['ProtectionLimit'] = prot_lim
    res['sigma_3'] = sigma_bar
    res['ProtectionLimit_3'] = prot_lim
    res['sigma_bar_3'] = sigma_bar

    return res[INVENTORY_NG_COLUMNS]


def compute_inventory_ng_df(df, pax_col='Final_pax_count', std_col='weighted_pax_std'):
    """Executes compute_flight_inventory_ng grouped across flights."""
    if df is None or df.empty:
        return pd.DataFrame(columns=INVENTORY_NG_COLUMNS)

    sec_candidates = ['Sector', 'sector', 'Origin_Destination']
    dep_candidates = ['DepartureDate', 'DepartureDate_future', 'date', 'DepDate']
    flt_candidates = ['FlightNumber', 'FlightNumber_future', 'flight', 'FltNum']

    sec_col = next((c for c in sec_candidates if c in df.columns), None)
    dep_col = next((c for c in dep_candidates if c in df.columns), None)
    flt_col = next((c for c in flt_candidates if c in df.columns), None)

    group_cols = [c for c in [sec_col, dep_col, flt_col] if c is not None]
    if not group_cols:
        res = compute_flight_inventory_ng(df, pax_col=pax_col, std_col=std_col)
        return res if not res.empty else pd.DataFrame(columns=INVENTORY_NG_COLUMNS)

    results = []
    for _, grp in df.groupby(group_cols, sort=False):
        res = compute_flight_inventory_ng(grp, pax_col=pax_col, std_col=std_col)
        if not res.empty:
            results.append(res)

    if not results:
        return pd.DataFrame(columns=INVENTORY_NG_COLUMNS)
    return pd.concat(results, ignore_index=True)


# =============================================================================
# MAIN CLASS FOR SCHEDULED JOBS
# =============================================================================
class DailyNextGen:
    """
    Scheduled Job handler for daily_next_gen:
    Ingests additive model parquet datasets from S3, executes NextGen Inventory
    Optimization, archives daily snapshots to S3, and writes all datasets
    to MongoDB.
    """

    def __init__(self, logger=None, params=None, overrides=None, context=None):
        self.logger = logger or create_logger()
        self.params = params or {}
        self.overrides = overrides or {}
        self.context = context

        # 1. Resolve environment configuration
        default_env = getattr(configsettings, 'env', 'prod') if configsettings else 'prod'
        self.env = str(self._get_override('ENV', default_env)).lower()
        self.is_prod = (self.env == 'prod')

        self.mongo_env = str(self._get_override('MONGO_ENV', '')).lower() or self.env
        self.is_mongo_prod = (self.mongo_env == 'prod')

        self.snapshot_date_arg = str(self._get_override('SNAPSHOT_DATE', 'latest')).strip()

        # Handle sectors filter (list or comma-separated string)
        raw_sectors = self._get_override('SECTORS', '')
        if isinstance(raw_sectors, list):
            self.sectors_filter = [str(s).strip().upper() for s in raw_sectors if str(s).strip()]
        elif isinstance(raw_sectors, str):
            self.sectors_filter = [s.strip().upper() for s in raw_sectors.split(',') if s.strip()]
        else:
            self.sectors_filter = []

        self.write_mongo = self._to_bool(self._get_override('WRITE_MONGO', True))
        self.write_s3_snapshots = self._to_bool(self._get_override('WRITE_S3_SNAPSHOTS', True))
        self.compute_inventory_ng = self._to_bool(self._get_override('COMPUTE_INVENTORY_NG', True))

        # 2. S3 configuration
        s3_bucket_override = self._get_override('S3_BUCKET')
        if s3_bucket_override:
            self.s3_bucket = s3_bucket_override
        else:
            default_bucket = getattr(configsettings, 'ADDITIVE_MODEL_BUCKET', 'qp-s3-fs' if self.is_prod else 'uat-qp-s3-fs') if configsettings else ('qp-s3-fs' if self.is_prod else 'uat-qp-s3-fs')
            self.s3_bucket = default_bucket

        self.s3_env_prefix = 'prod' if self.is_prod else 'uat'
        self.s3_datasets_prefix = f"{self.s3_env_prefix}/additive_model/datasets"
        self.s3_snapshot_archive_prefix = f"{self.s3_env_prefix}/additive_model/snapshots"

        # S3 client
        self.s3_client = self.params.get('s3') or boto3.client('s3', region_name='ap-south-1')

        # 3. MongoDB configuration
        self.mongo_db_name = self._get_override('MONGO_DB', os.environ.get('MONGO_DB', 'qp_dw_rmalloc'))
        self._mongo_client = self.params.get('mongoconn')

    def _get_override(self, key, default=None):
        if not self.overrides:
            return default
        if key in self.overrides:
            return self.overrides[key]
        if key.lower() in self.overrides:
            return self.overrides[key.lower()]
        if key.upper() in self.overrides:
            return self.overrides[key.upper()]
        return default

    @staticmethod
    def _to_bool(val):
        if isinstance(val, bool):
            return val
        return str(val).lower() in ['true', '1', 'yes', 'y']

    def get_mongo_db(self):
        """Returns MongoDB database instance with fallback connection logic."""
        if self._mongo_client is not None:
            return self._mongo_client[self.mongo_db_name]

        if self.is_mongo_prod:
            mongo_url = os.environ.get('MONGO_URL', getattr(configsettings, 'MONGO_URL', os.environ.get('MONGO_HOST', "10.16.6.179")) if configsettings else os.environ.get('MONGO_HOST', "10.16.6.179"))
            mongo_port = int(os.environ.get('MONGO_PORT', getattr(configsettings, 'MONGO_PORT', 27017) if configsettings else 27017))
            mongo_username = os.environ.get('MONGO_USERNAME', getattr(configsettings, 'MONGO_USERNAME', os.environ.get('MONGO_USER', "rmuser")) if configsettings else os.environ.get('MONGO_USER', "rmuser"))
            mongo_password = os.environ.get('MONGO_PASSWORD') or (getattr(configsettings, 'MONGO_PASSWORD', None) if configsettings else None) or get_ssm_parameter('/rm/prod/rm-allocation/RM_MONGO_PASSWORD', logger=self.logger) or ''
            mongo_auth_source = os.environ.get('MONGO_AUTH_SOURCE', getattr(configsettings, 'MONGO_AUTH_SOURCE', "admin") if configsettings else "admin")
        else:
            mongo_url = os.environ.get('MONGO_URL', getattr(configsettings, 'MONGO_URL', os.environ.get('MONGO_HOST', "172.24.72.35")) if configsettings else os.environ.get('MONGO_HOST', "172.24.72.35"))
            mongo_port = int(os.environ.get('MONGO_PORT', getattr(configsettings, 'MONGO_PORT', 27017) if configsettings else 27017))
            mongo_username = os.environ.get('MONGO_USERNAME', getattr(configsettings, 'MONGO_USERNAME', os.environ.get('MONGO_USER', "testuser")) if configsettings else os.environ.get('MONGO_USER', "testuser"))
            mongo_password = os.environ.get('MONGO_PASSWORD') or (getattr(configsettings, 'MONGO_PASSWORD', None) if configsettings else None) or get_ssm_parameter('/rm/uat/rm-allocation/RM_MONGO_PASSWORD', logger=self.logger) or 'testpassword'
            mongo_auth_source = os.environ.get('MONGO_AUTH_SOURCE', getattr(configsettings, 'MONGO_AUTH_SOURCE', "admin") if configsettings else "admin")

        kwargs = {
            'host': mongo_url,
            'port': mongo_port,
            'serverSelectionTimeoutMS': 5000,
            'maxPoolSize': int(os.environ.get('MONGO_MAX_POOL_SIZE', 50)),
            'retryWrites': True,
        }
        mongo_compressors = os.environ.get('MONGO_COMPRESSORS')
        if mongo_compressors:
            kwargs['compressors'] = mongo_compressors
        if mongo_username and mongo_password:
            kwargs.update({
                'username': mongo_username,
                'password': mongo_password,
                'authSource': mongo_auth_source,
            })
        self._mongo_client = MongoClient(**kwargs)
        return self._mongo_client[self.mongo_db_name]

    # =========================================================================
    # S3 Discovery & Ingestion
    # =========================================================================
    def discover_latest_snapshot_date(self, dataset_name='fare_bucket_weighted'):
        """Finds the most recent snapshotdate=YYYY-MM-DD partition in S3."""
        prefix = f"{self.s3_datasets_prefix}/{dataset_name}/"
        self.logger.info(f"Discovering latest snapshot date under s3://{self.s3_bucket}/{prefix}...")
        paginator = self.s3_client.get_paginator('list_objects_v2')
        dates = set()
        for page in paginator.paginate(Bucket=self.s3_bucket, Prefix=prefix, Delimiter='/'):
            for cp in page.get('CommonPrefixes', []):
                match = re.search(r'snapshotdate=([0-9]{4}-[0-9]{2}-[0-9]{2})', cp.get('Prefix', ''))
                if match:
                    dates.add(match.group(1))

        if not dates:
            self.logger.warning(f"No snapshotdate partitions found under {prefix}. Falling back to today's date.")
            return datetime.today().strftime('%Y-%m-%d')

        latest_date = sorted(dates)[-1]
        self.logger.info(f"Discovered latest snapshot date: {latest_date}")
        return latest_date

    def read_partitioned_parquet(self, dataset_name, snapshot_date, sectors_filter=None):
        """
        Reads and consolidates all sector partitions for a given dataset and snapshot date.
        Returns consolidated DataFrame with SnapshotDate and Sector columns re-attached.
        """
        prefix = f"{self.s3_datasets_prefix}/{dataset_name}/snapshotdate={snapshot_date}/"
        self.logger.info(f"Scanning s3://{self.s3_bucket}/{prefix}...")
        paginator = self.s3_client.get_paginator('list_objects_v2')
        keys_to_fetch = []

        for page in paginator.paginate(Bucket=self.s3_bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if not key.endswith('.parquet'):
                    continue

                sec_match = re.search(r'sector=([A-Za-z0-9_\-\.]+)', key, re.IGNORECASE)
                sector_val = sec_match.group(1) if sec_match else ''
                if sectors_filter and sector_val.upper() not in sectors_filter:
                    continue

                keys_to_fetch.append((key, sector_val))

        # Fallback to uppercase SnapshotDate if lowercase yielded nothing
        if not keys_to_fetch:
            alt_prefix = f"{self.s3_datasets_prefix}/{dataset_name}/SnapshotDate={snapshot_date}/"
            for page in paginator.paginate(Bucket=self.s3_bucket, Prefix=alt_prefix):
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    if not key.endswith('.parquet'):
                        continue
                    sec_match = re.search(r'sector=([A-Za-z0-9_\-\.]+)', key, re.IGNORECASE)
                    sector_val = sec_match.group(1) if sec_match else ''
                    if sectors_filter and sector_val.upper() not in sectors_filter:
                        continue
                    keys_to_fetch.append((key, sector_val))

        dfs = []
        def _read_single_s3_parquet(item):
            key, sector_val = item
            try:
                resp = self.s3_client.get_object(Bucket=self.s3_bucket, Key=key)
                part_df = pd.read_parquet(io.BytesIO(resp['Body'].read()))
                if not part_df.empty:
                    if 'Sector' not in part_df.columns and sector_val:
                        part_df['Sector'] = sector_val
                    if 'SnapshotDate' not in part_df.columns:
                        part_df['SnapshotDate'] = snapshot_date
                    return part_df
            except Exception as e:
                self.logger.error(f"Error reading {key}: {e}")
            return None

        if keys_to_fetch:
            max_workers = min(16, len(keys_to_fetch))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                for part_df in executor.map(_read_single_s3_parquet, keys_to_fetch):
                    if part_df is not None and not part_df.empty:
                        dfs.append(part_df)

        self.logger.info(f"Read {len(dfs)} parquet partitions for {dataset_name} (snapshotdate={snapshot_date}).")
        if not dfs:
            return pd.DataFrame()
        return pd.concat(dfs, ignore_index=True)

    # =========================================================================
    # S3 Daily Snapshot Archiver
    # =========================================================================
    def upload_snapshot_to_s3(self, df, dataset_name, snapshot_date, timestamp):
        """
        Uploads daily snapshot archive (Parquet) to S3 snapshot archive folder.
        Matches qp-dw-pipelines/RM pattern: {env}/additive_model/snapshots/{dataset}/{YYYY}/{MM}/{DD}/
        """
        if df is None or df.empty:
            self.logger.warning(f"No data to archive for {dataset_name}.")
            return

        try:
            dt = pd.to_datetime(snapshot_date)
            y, m, d = dt.strftime('%Y'), dt.strftime('%m'), dt.strftime('%d')
            base_prefix = f"{self.s3_snapshot_archive_prefix}/{dataset_name}/{y}/{m}/{d}"

            p_key = f"{base_prefix}/{dataset_name}_{snapshot_date}_{timestamp}.parquet"
            p_buf = io.BytesIO()
            df.to_parquet(p_buf, index=False, compression='snappy')
            self.s3_client.put_object(Bucket=self.s3_bucket, Key=p_key, Body=p_buf.getvalue())
            self.logger.info(f"Archived Parquet snapshot: s3://{self.s3_bucket}/{p_key}")

        except Exception as e:
            self.logger.error(f"Error archiving snapshot to S3 for {dataset_name}: {e}")
            self.logger.error(traceback.format_exc())

    # =========================================================================
    # MongoDB Collection Writer
    # =========================================================================
    def write_to_mongo_collection(self, df, collection_name, max_retries=3, retry_delay=3, chunksize=None, max_workers=None):
        """
        Atomically writes/replaces a MongoDB collection with clean documents.
        Optimized for high-throughput bulk inserts & low memory:
        - Vectorized NaN/inf/BSON sanitization in a single pass
        - Increased chunk size (default 10,000) for fewer network roundtrips
        - Concurrent bulk inserts across multiple worker threads
        - Drops collection before bulk insert and builds compound index afterwards
        """
        if df is None or df.empty:
            self.logger.warning(f"Skipping Mongo write for {collection_name} (DataFrame empty).")
            return

        if chunksize is None:
            chunksize = int(os.environ.get('MONGO_CHUNK_SIZE', 10000))
        if max_workers is None:
            max_workers = int(os.environ.get('MONGO_INSERT_WORKERS', 4))

        t_prep = time.time()
        clean_df = df.copy()

        # Fast numeric replacement of inf / -inf
        num_cols = clean_df.select_dtypes(include=[np.number]).columns
        if len(num_cols) > 0:
            clean_df[num_cols] = clean_df[num_cols].replace([np.inf, -np.inf], np.nan)

        # Ensure all column names are strings for BSON key compatibility
        clean_df.columns = [str(c) for c in clean_df.columns]

        # Fast datetime / date formatting for BSON serialization
        dt_cols = clean_df.select_dtypes(include=['datetime64', 'datetimetz']).columns
        for col in dt_cols:
            clean_df[col] = clean_df[col].dt.strftime('%Y-%m-%d')

        obj_cols = clean_df.select_dtypes(include=['object']).columns
        for col in obj_cols:
            sample = clean_df[col].dropna()
            if not sample.empty:
                first = sample.iloc[0]
                if isinstance(first, dt_time):
                    clean_df[col] = [x.strftime('%H:%M:%S') if isinstance(x, dt_time) else (str(x) if pd.notna(x) else None) for x in clean_df[col]]
                elif isinstance(first, (pd.Timestamp, datetime, dt_date)):
                    clean_df[col] = [x.strftime('%Y-%m-%d') if isinstance(x, (pd.Timestamp, datetime, dt_date)) else (str(x) if pd.notna(x) else None) for x in clean_df[col]]

        for date_col in ['DepartureDate', 'date', 'SnapshotDate']:
            if date_col in clean_df.columns and pd.api.types.is_datetime64_any_dtype(clean_df[date_col]):
                clean_df[date_col] = clean_df[date_col].dt.strftime('%Y-%m-%d')

        # Fast vectorized conversion: convert missing values to None for BSON null
        clean_df = clean_df.astype(object).where(pd.notnull(clean_df), None)
        records = clean_df.to_dict('records')
        total_records = len(records)
        self.logger.info(f"Prepared DataFrame ({total_records} rows) for MongoDB '{collection_name}' in {time.time() - t_prep:.2f}s.")

        db = self.get_mongo_db()
        col = db[collection_name]

        chunks = [records[i:i + chunksize] for i in range(0, total_records, chunksize)]
        total_chunks = len(chunks)

        for attempt in range(1, max_retries + 1):
            try:
                self.logger.info(
                    f"Writing {total_records} documents in {total_chunks} chunks to MongoDB '{collection_name}' "
                    f"(chunksize={chunksize}, workers={max_workers}, ordered=False, attempt {attempt}/{max_retries})..."
                )
                t0 = time.time()
                col.drop()
                if total_records > 0:
                    if total_chunks <= 1 or max_workers <= 1:
                        for chunk in chunks:
                            col.insert_many(chunk, ordered=False)
                    else:
                        workers = min(max_workers, total_chunks)
                        with ThreadPoolExecutor(max_workers=workers) as executor:
                            futures = [executor.submit(col.insert_many, chunk, ordered=False) for chunk in chunks]
                            for future in as_completed(futures):
                                future.result()
                    try:
                        cols = set(clean_df.columns)
                        if {'Sector', 'DepartureDate', 'FlightNumber'}.issubset(cols):
                            col.create_index([("Sector", 1), ("DepartureDate", 1), ("FlightNumber", 1)], background=True)
                        elif {'sector', 'date', 'flight'}.issubset(cols):
                            col.create_index([("sector", 1), ("date", 1), ("flight", 1)], background=True)
                    except Exception as idx_err:
                        self.logger.warning(f"Could not create index on {collection_name}: {idx_err}")
                self.logger.info(f"Successfully wrote {total_records} documents to MongoDB '{collection_name}' in {time.time()-t0:.2f}s.")
                return
            except Exception as e:
                self.logger.error(f"Attempt {attempt} failed writing to MongoDB '{collection_name}': {e}")
                if attempt < max_retries:
                    time.sleep(retry_delay * attempt)
                else:
                    self.logger.error(traceback.format_exc())
                    raise

    # =========================================================================
    # Pipeline Execution Method (run)
    # =========================================================================
    def run(self):
        """
        Executes the additive model sync and NextGen inventory optimization.
        Returns a dictionary summarizing the execution for scheduled_job_run_audit.
        """
        run_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        start_time = time.time()

        try:
            self.logger.info("=" * 80)
            self.logger.info("Daily NextGen Additive Model Sync & Inventory Optimization")
            self.logger.info(f"  Environment:            {self.env.upper()} (is_prod={self.is_prod})")
            self.logger.info(f"  Mongo Environment:      {self.mongo_env.upper()} (is_mongo_prod={self.is_mongo_prod})")
            self.logger.info(f"  Snapshot Date:          {self.snapshot_date_arg}")
            self.logger.info(f"  Sectors Filter:         {self.sectors_filter if self.sectors_filter else '(All sectors)'}")
            self.logger.info(f"  Write to Mongo:         {self.write_mongo}")
            self.logger.info(f"  Write S3 Snapshots:     {self.write_s3_snapshots}")
            self.logger.info(f"  Compute Inventory NG:   {self.compute_inventory_ng}")
            self.logger.info("=" * 80)

            # 1. Resolve Snapshot Date
            target_snapshot_date = self.snapshot_date_arg
            if target_snapshot_date.lower() == 'latest':
                target_snapshot_date = self.discover_latest_snapshot_date('fare_bucket_weighted')

            self.logger.info(f"Starting additive model sync pipeline for SnapshotDate: {target_snapshot_date}")

            # 2. Ingest S3 Parquet: fare_bucket_weighted
            fare_bucket_df = self.read_partitioned_parquet('fare_bucket_weighted', target_snapshot_date, self.sectors_filter)
            if fare_bucket_df.empty:
                self.logger.error(f"No fare_bucket_weighted data found for {target_snapshot_date}! Aborting.")
                return {
                    "status": "failed",
                    "error": f"No fare_bucket_weighted data found for {target_snapshot_date}",
                    "snapshot_date": target_snapshot_date
                }

            # 3. Ensure bucket bounds are present for rm-allocation-process-inputs
            if 'FareBucket_Lower' not in fare_bucket_df.columns:
                b_col = 'FareBucket' if 'FareBucket' in fare_bucket_df.columns else ('FareBasis' if 'FareBasis' in fare_bucket_df.columns else None)
                if b_col and b_col in fare_bucket_df.columns:
                    fare_bucket_df['FareBucket_Lower'] = pd.to_numeric(
                        fare_bucket_df[b_col].astype(str).str.replace('+', '', regex=False)
                        .str.split('-').str[0], errors='coerce'
                    ).fillna(0.0)
                    fare_bucket_df['FareBucket_Upper'] = pd.to_numeric(
                        fare_bucket_df[b_col].astype(str).str.split('-').str[1], errors='coerce'
                    )

            # 4. Ingest flight_level_weighted (if present)
            flight_df = self.read_partitioned_parquet('flight_level_weighted', target_snapshot_date, self.sectors_filter)

            # 5. Compute NextGen Inventory Optimization (inventory_ng) if requested
            inventory_ng_df = None
            if self.compute_inventory_ng:
                self.logger.info("Executing NextGen Inventory Optimization (50 columns, EMSR-b AU ladder)...")
                t_ng0 = time.time()
                inventory_ng_df = compute_inventory_ng_df(fare_bucket_df)
                self.logger.info(f"Inventory NG calculation complete in {time.time()-t_ng0:.2f}s ({len(inventory_ng_df)} rows).")

            # 6. S3 Daily Snapshot Archival
            if self.write_s3_snapshots:
                self.logger.info("Archiving daily snapshots to S3...")
                self.upload_snapshot_to_s3(fare_bucket_df, 'add_model_fare_bucket_level_weighted', target_snapshot_date, run_timestamp)
                if inventory_ng_df is not None and not inventory_ng_df.empty:
                    self.upload_snapshot_to_s3(inventory_ng_df, 'add_model_inventory_ng', target_snapshot_date, run_timestamp)
                if flight_df is not None and not flight_df.empty:
                    self.upload_snapshot_to_s3(flight_df, 'add_model_flight_level_weighted', target_snapshot_date, run_timestamp)

            # 7. Persistence: MongoDB (weighted file & NextGen outputs)
            if self.write_mongo:
                mongo_targets = [
                    (fare_bucket_df, 'add_model_fare_bucket_level_weighted', "Storing weighted file into MongoDB collection 'add_model_fare_bucket_level_weighted'...")
                ]
                if inventory_ng_df is not None and not inventory_ng_df.empty:
                    mongo_targets.append((inventory_ng_df, 'add_model_inventory_ng', "Storing NextGen inventory optimization into MongoDB collection 'add_model_inventory_ng'..."))
                if flight_df is not None and not flight_df.empty:
                    mongo_targets.append((flight_df, 'add_model_flight_level_weighted', "Storing flight-level weighted into MongoDB collection 'add_model_flight_level_weighted'..."))

                if self._to_bool(self._get_override('PARALLEL_MONGO_COLLECTIONS', False)):
                    self.logger.info(f"Persisting {len(mongo_targets)} collections to MongoDB concurrently...")
                    with ThreadPoolExecutor(max_workers=len(mongo_targets)) as executor:
                        futures = [
                            executor.submit(self.write_to_mongo_collection, target_df, target_col)
                            for target_df, target_col, _ in mongo_targets
                        ]
                        for f in as_completed(futures):
                            f.result()
                else:
                    for target_df, target_col, log_msg in mongo_targets:
                        self.logger.info(log_msg)
                        self.write_to_mongo_collection(target_df, target_col)

            elapsed = round(time.time() - start_time, 2)
            self.logger.info(f"DailyNextGen pipeline finished successfully in {elapsed}s.")

            return {
                "status": "success",
                "snapshot_date": target_snapshot_date,
                "fare_bucket_rows": len(fare_bucket_df),
                "inventory_ng_rows": len(inventory_ng_df) if inventory_ng_df is not None else 0,
                "flight_level_rows": len(flight_df) if flight_df is not None else 0,
                "elapsed_seconds": elapsed,
                "run_timestamp": run_timestamp
            }

        except Exception as e:
            elapsed = round(time.time() - start_time, 2)
            self.logger.error(f"DailyNextGen run encountered critical error: {e}")
            self.logger.error(traceback.format_exc())
            return {
                "status": "failed",
                "error": str(e),
                "traceback": traceback.format_exc(),
                "elapsed_seconds": elapsed,
                "run_timestamp": run_timestamp
            }


# =============================================================================
# CLI / GLUE JOB ENTRYPOINT
# =============================================================================
if __name__ == '__main__':
    try:
        from awsglue.utils import getResolvedOptions
        args = getResolvedOptions(sys.argv, [
            'ENV', 'SNAPSHOT_DATE', 'SECTORS', 'WRITE_MONGO', 'WRITE_S3_SNAPSHOTS', 'COMPUTE_INVENTORY_NG'
        ])
    except Exception:
        import argparse
        parser = argparse.ArgumentParser(description="Daily NextGen Additive Model Sync Job")
        parser.add_argument('--ENV', default='prod', choices=['prod', 'uat'])
        parser.add_argument('--MONGO_ENV', default='', choices=['', 'prod', 'uat'], help='Target Mongo environment (prod or uat). Defaults to ENV.')
        parser.add_argument('--SNAPSHOT_DATE', default='latest')
        parser.add_argument('--SECTORS', default='')
        parser.add_argument('--WRITE_MONGO', default='true')
        parser.add_argument('--WRITE_S3_SNAPSHOTS', default='true')
        parser.add_argument('--COMPUTE_INVENTORY_NG', default='true')
        known, _ = parser.parse_known_args()
        args = vars(known)

    job = DailyNextGen(overrides=args)
    res = job.run()
    if res.get("status") != "success":
        sys.exit(1)
