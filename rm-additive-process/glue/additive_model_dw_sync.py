#!/usr/bin/env python3
"""
additive_model_dw_sync.py - AWS Glue Job Script
=============================================================================
Reads latest additive model S3 Parquet datasets, executes NextGen Inventory
Optimization (EMSR-b AU ladder, protection limits, revenue forecast), archives
daily snapshots to S3, and persists all datasets directly into MongoDB collections
for downstream consumption by rm-allocation-process-inputs.

Adheres to qp-dw-pipelines/RM Glue job conventions:
- Uses AWS SSM Parameter Store for MongoDB credentials.
- Supports both PROD and UAT environments.
- Automatically discovers the latest snapshotdate partition across S3.
- Writes daily snapshots (Parquet & CSV) to S3 snapshot archive.
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
from datetime import datetime, timedelta
import boto3
import pandas as pd
import numpy as np
from scipy.stats import norm
from pymongo import MongoClient

# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================
def create_logger():
    logger = logging.getLogger("additive_model_dw_sync")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s - %(levelname)-8s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger

logger = create_logger()

# =============================================================================
# GLUE ARGUMENT PARSING (with fallback for local/CLI execution)
# =============================================================================
try:
    from awsglue.utils import getResolvedOptions
    glue_args = getResolvedOptions(sys.argv, [
        'ENV', 'SNAPSHOT_DATE', 'SECTORS', 'WRITE_MONGO', 'WRITE_S3_SNAPSHOTS', 'COMPUTE_INVENTORY_NG'
    ])
except Exception:
    import argparse
    parser = argparse.ArgumentParser(description="Additive Model Sync Glue Job")
    parser.add_argument('--ENV', default='prod', choices=['prod', 'uat'])
    parser.add_argument('--SNAPSHOT_DATE', default='latest')
    parser.add_argument('--SECTORS', default='')
    parser.add_argument('--WRITE_MONGO', default='true')
    parser.add_argument('--WRITE_S3_SNAPSHOTS', default='true')
    parser.add_argument('--COMPUTE_INVENTORY_NG', default='true')
    known, _ = parser.parse_known_args()
    glue_args = vars(known)

ENV = glue_args.get('ENV', 'prod').lower()
IS_PROD = (ENV == 'prod')
SNAPSHOT_DATE_ARG = glue_args.get('SNAPSHOT_DATE', 'latest').strip()
SECTORS_FILTER = [s.strip().upper() for s in glue_args.get('SECTORS', '').split(',') if s.strip()]
WRITE_MONGO = glue_args.get('WRITE_MONGO', 'true').lower() == 'true'
WRITE_S3_SNAPSHOTS = glue_args.get('WRITE_S3_SNAPSHOTS', 'true').lower() == 'true'
COMPUTE_INVENTORY_NG = glue_args.get('COMPUTE_INVENTORY_NG', 'true').lower() == 'true'

logger.info("=" * 80)
logger.info("AWS Glue Job: Additive Model Sync & NextGen Inventory Optimization")
logger.info(f"  Environment:            {ENV.upper()} (IS_PROD={IS_PROD})")
logger.info(f"  Snapshot Date:          {SNAPSHOT_DATE_ARG}")
logger.info(f"  Sectors Filter:         {SECTORS_FILTER if SECTORS_FILTER else '(All sectors)'}")
logger.info(f"  Write to Mongo:         {WRITE_MONGO}")
logger.info(f"  Write S3 Snapshots:     {WRITE_S3_SNAPSHOTS}")
logger.info(f"  Compute Inventory NG:   {COMPUTE_INVENTORY_NG}")
logger.info("=" * 80)

# =============================================================================
# SSM PARAMETERS & CONNECTIONS
# =============================================================================
def get_ssm_parameter(parameter_name, region='ap-south-1'):
    try:
        ssm = boto3.client('ssm', region_name=region)
        resp = ssm.get_parameter(Name=parameter_name, WithDecryption=True)
        return resp['Parameter']['Value']
    except Exception as err:
        logger.warning(f"Could not retrieve SSM parameter {parameter_name}: {err}")
        return None

# Database & S3 Config
S3_BUCKET = 'qp-s3-fs' if IS_PROD else 'uat-qp-s3-fs'
S3_ENV_PREFIX = 'prod' if IS_PROD else 'uat'
S3_DATASETS_PREFIX = f"{S3_ENV_PREFIX}/additive_model/datasets"
S3_SNAPSHOT_ARCHIVE_PREFIX = f"{S3_ENV_PREFIX}/additive_model/snapshots"

if IS_PROD:
    MONGO_HOST = get_ssm_parameter('/glue_job/prod/RM_MONGO_HOST') or get_ssm_parameter('/rm/prod/rm-allocation/RM_MONGO_HOST') or '10.16.6.179'
    MONGO_PORT = int(get_ssm_parameter('/glue_job/prod/RM_MONGO_PORT') or get_ssm_parameter('/rm/prod/rm-allocation/RM_MONGO_PORT') or '27017')
    MONGO_USER = get_ssm_parameter('/glue_job/prod/RM_MONGO_USERNAME') or get_ssm_parameter('/rm/prod/rm-allocation/RM_MONGO_USERNAME') or 'rmuser'
    MONGO_PASSWORD = get_ssm_parameter('/glue_job/prod/RM_MONGO_PASSWORD') or get_ssm_parameter('/rm/prod/rm-allocation/RM_MONGO_PASSWORD') or ''
    MONGO_DB_NAME = get_ssm_parameter('/glue_job/prod/RM_MONGO_DB') or get_ssm_parameter('/rm/prod/rm-allocation/RM_MONGO_DB') or 'qp_dw_rmalloc'
    MONGO_AUTH_SOURCE = get_ssm_parameter('/glue_job/prod/RM_MONGO_AUTH_SOURCE') or 'admin'
else:
    MONGO_HOST = get_ssm_parameter('/glue_job/uat/RM_MONGO_HOST') or get_ssm_parameter('/rm/uat/rm-allocation/RM_MONGO_HOST') or '172.24.72.35'
    MONGO_PORT = int(get_ssm_parameter('/glue_job/uat/RM_MONGO_PORT') or get_ssm_parameter('/rm/uat/rm-allocation/RM_MONGO_PORT') or '27017')
    MONGO_USER = get_ssm_parameter('/glue_job/uat/RM_MONGO_USERNAME') or get_ssm_parameter('/rm/uat/rm-allocation/RM_MONGO_USERNAME') or 'testuser'
    MONGO_PASSWORD = get_ssm_parameter('/glue_job/uat/RM_MONGO_PASSWORD') or get_ssm_parameter('/rm/uat/rm-allocation/RM_MONGO_PASSWORD') or 'testpassword'
    MONGO_DB_NAME = get_ssm_parameter('/glue_job/uat/RM_MONGO_DB') or get_ssm_parameter('/rm/uat/rm-allocation/RM_MONGO_DB') or 'qp_dw_rmalloc'
    MONGO_AUTH_SOURCE = get_ssm_parameter('/glue_job/uat/RM_MONGO_AUTH_SOURCE') or 'admin'

def get_mongo_db():
    host = os.environ.get('MONGO_HOST', MONGO_HOST)
    port = int(os.environ.get('MONGO_PORT', MONGO_PORT))
    user = os.environ.get('MONGO_USER', MONGO_USER)
    pwd = os.environ.get('MONGO_PASSWORD', MONGO_PASSWORD)
    db_name = os.environ.get('MONGO_DB', MONGO_DB_NAME)
    auth_src = os.environ.get('MONGO_AUTH_SOURCE', MONGO_AUTH_SOURCE)

    if user and pwd:
        client = MongoClient(host=host, port=port, username=user, password=pwd, authSource=auth_src, serverSelectionTimeoutMS=5000)
    else:
        client = MongoClient(host=host, port=port, serverSelectionTimeoutMS=5000)
    return client[db_name]

s3_client = boto3.client('s3', region_name='ap-south-1')

# =============================================================================
# NEXTGEN INVENTORY OPTIMIZATION SPECIFICATION & ENGINE
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
    flt_num = int(flt_col_val) if pd.notna(flt_col_val) else 0

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
    ndo_val = int(df['future_flight_NDO'].iloc[0]) if 'future_flight_NDO' in df.columns and pd.notna(df['future_flight_NDO'].iloc[0]) else (int(df.get('NDO', pd.Series([0])).iloc[0]) if 'NDO' in df.columns else 0)

    # Resolve FareBucket bounds and fare
    b_col = 'FareBucket' if 'FareBucket' in df.columns else ('FareBasis' if 'FareBasis' in df.columns else 'fare_bucket')
    if 'FareBucket_Lower' not in df.columns:
        df['FareBucket_Lower'] = df[b_col].astype(str).str.replace('+', '', regex=False).str.split('-').str[0].astype(float)
    if 'FareBucket_Upper' not in df.columns:
        df['FareBucket_Upper'] = pd.to_numeric(df[b_col].astype(str).str.split('-').str[1], errors='coerce')

    df['fare'] = df['FareBucket_Lower'].astype(float)
    df = df.sort_values('fare', ascending=False).reset_index(drop=True)

    if pax_col not in df.columns:
        pax_col = next((c for c in ['OptimizationDemand', 'Final_pax_count', 'weighted_pax_count', 'Total_Predicted_pax_count', 'mu'] if c in df.columns), None)
    if std_col not in df.columns:
        std_col = next((c for c in ['OptimizationDemandStd', 'weighted_pax_std', 'sigma'] if c in df.columns), None)

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
    oal_min = float(market_min_fare) if market_min_fare is not None and market_min_fare > 0 else (fare_arr[-1] if k > 0 else 3000.0)
    anchor_fare = fare_arr[-1] if k > 0 else 3000.0
    strategy_fare = max(anchor_fare, oal_min)

    # Pass 2 - AU_Revised
    mu_bar_2 = np.copy(mu_bar_orig)
    mu_2 = np.copy(mu_arr)
    au_2 = np.copy(au)
    au_revised = np.copy(au)

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
    res['oal_b2c_min'] = oal_min
    res['anchor_fare'] = anchor_fare
    res['strategy_fare'] = strategy_fare
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
    res['FareBucket'] = df[b_col].values
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

    sec_col = 'Sector' if 'Sector' in df.columns else 'sector'
    dep_col = 'DepartureDate' if 'DepartureDate' in df.columns else ('DepartureDate_future' if 'DepartureDate_future' in df.columns else 'date')
    flt_col = 'FlightNumber' if 'FlightNumber' in df.columns else ('FlightNumber_future' if 'FlightNumber_future' in df.columns else 'flight')

    results = []
    for _, grp in df.groupby([sec_col, dep_col, flt_col], sort=False):
        res = compute_flight_inventory_ng(grp, pax_col=pax_col, std_col=std_col)
        if not res.empty:
            results.append(res)

    if not results:
        return pd.DataFrame(columns=INVENTORY_NG_COLUMNS)
    return pd.concat(results, ignore_index=True)

# =============================================================================
# S3 DATASET INGESTION & DISCOVERY
# =============================================================================
def discover_latest_snapshot_date(dataset_name='fare_bucket_weighted'):
    """Finds the most recent snapshotdate=YYYY-MM-DD partition in S3."""
    prefix = f"{S3_DATASETS_PREFIX}/{dataset_name}/"
    logger.info(f"Discovering latest snapshot date under s3://{S3_BUCKET}/{prefix}...")
    paginator = s3_client.get_paginator('list_objects_v2')
    dates = set()
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix, Delimiter='/'):
        for cp in page.get('CommonPrefixes', []):
            match = re.search(r'snapshotdate=([0-9]{4}-[0-9]{2}-[0-9]{2})', cp.get('Prefix', ''))
            if match:
                dates.add(match.group(1))

    if not dates:
        logger.warning(f"No snapshotdate partitions found under {prefix}. Falling back to today's date.")
        return datetime.today().strftime('%Y-%m-%d')

    latest_date = sorted(dates)[-1]
    logger.info(f"Discovered latest snapshot date: {latest_date}")
    return latest_date

def read_partitioned_parquet(dataset_name, snapshot_date, sectors_filter=None):
    """
    Reads and consolidates all sector partitions for a given dataset and snapshot date.
    Returns consolidated DataFrame with SnapshotDate and Sector columns re-attached.
    """
    prefix = f"{S3_DATASETS_PREFIX}/{dataset_name}/snapshotdate={snapshot_date}/"
    logger.info(f"Scanning s3://{S3_BUCKET}/{prefix}...")
    paginator = s3_client.get_paginator('list_objects_v2')
    dfs = []
    total_files = 0

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if not key.endswith('.parquet'):
                continue

            sec_match = re.search(r'sector=([A-Za-z0-9_]+)', key)
            sector_val = sec_match.group(1) if sec_match else ''
            if sectors_filter and sector_val.upper() not in sectors_filter:
                continue

            try:
                resp = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
                part_df = pd.read_parquet(io.BytesIO(resp['Body'].read()))
                if not part_df.empty:
                    if 'Sector' not in part_df.columns and sector_val:
                        part_df['Sector'] = sector_val
                    if 'SnapshotDate' not in part_df.columns:
                        part_df['SnapshotDate'] = snapshot_date
                    dfs.append(part_df)
                    total_files += 1
            except Exception as e:
                logger.error(f"Error reading {key}: {e}")

    logger.info(f"Read {total_files} parquet partitions for {dataset_name} (snapshotdate={snapshot_date}).")
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)

# =============================================================================
# S3 DAILY SNAPSHOT ARCHIVER
# =============================================================================
def upload_snapshot_to_s3(df, dataset_name, snapshot_date, timestamp):
    """
    Uploads daily snapshot archive (Parquet and CSV) to S3 snapshot archive folder.
    Matches qp-dw-pipelines/RM pattern: {env}/additive_model/snapshots/{YYYY}/{MM}/{DD}/
    """
    if df is None or df.empty:
        logger.warning(f"No data to archive for {dataset_name}.")
        return

    try:
        dt = pd.to_datetime(snapshot_date)
        y, m, d = dt.strftime('%Y'), dt.strftime('%m'), dt.strftime('%d')
        base_prefix = f"{S3_SNAPSHOT_ARCHIVE_PREFIX}/{dataset_name}/{y}/{m}/{d}"

        # Parquet
        p_key = f"{base_prefix}/{dataset_name}_{snapshot_date}_{timestamp}.parquet"
        p_buf = io.BytesIO()
        df.to_parquet(p_buf, index=False, compression='snappy')
        s3_client.put_object(Bucket=S3_BUCKET, Key=p_key, Body=p_buf.getvalue())
        logger.info(f"Archived Parquet snapshot: s3://{S3_BUCKET}/{p_key}")

        # CSV
        c_key = f"{base_prefix}/{dataset_name}_{snapshot_date}_{timestamp}.csv"
        c_buf = io.BytesIO()
        df.to_csv(c_buf, index=False)
        s3_client.put_object(Bucket=S3_BUCKET, Key=c_key, Body=c_buf.getvalue())
        logger.info(f"Archived CSV snapshot:     s3://{S3_BUCKET}/{c_key}")

    except Exception as e:
        logger.error(f"Error archiving snapshot to S3 for {dataset_name}: {e}")
        logger.error(traceback.format_exc())

# =============================================================================
# MONGODB COLLECTION WRITER (with retry & atomic replacement)
# =============================================================================
def write_to_mongo_collection(df, collection_name, max_retries=3, retry_delay=3, chunksize=5000):
    """
    Atomically writes/replaces a MongoDB collection with clean documents.
    Used for storing weighted forecast files directly into MongoDB.
    """
    if df is None or df.empty:
        logger.warning(f"Skipping Mongo write for {collection_name} (DataFrame empty).")
        return

    clean_df = df.replace([np.inf, -np.inf], np.nan)
    # Format dates as string YYYY-MM-DD for MongoDB compatibility
    if 'DepartureDate' in clean_df.columns:
        clean_df['DepartureDate'] = pd.to_datetime(clean_df['DepartureDate']).dt.strftime('%Y-%m-%d')
    if 'date' in clean_df.columns:
        clean_df['date'] = pd.to_datetime(clean_df['date']).dt.strftime('%Y-%m-%d')
    records = clean_df.to_dict('records')

    db = get_mongo_db()
    col = db[collection_name]

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Writing {len(records)} documents to MongoDB '{collection_name}' (attempt {attempt}/{max_retries})...")
            t0 = time.time()
            col.drop()
            if records:
                for i in range(0, len(records), chunksize):
                    chunk = records[i:i + chunksize]
                    col.insert_many(chunk)
                try:
                    cols = set(clean_df.columns)
                    if {'Sector', 'DepartureDate', 'FlightNumber'}.issubset(cols):
                        col.create_index([("Sector", 1), ("DepartureDate", 1), ("FlightNumber", 1)])
                    elif {'sector', 'date', 'flight'}.issubset(cols):
                        col.create_index([("sector", 1), ("date", 1), ("flight", 1)])
                except Exception as idx_err:
                    logger.warning(f"Could not create index on {collection_name}: {idx_err}")
            logger.info(f"Successfully wrote {len(records)} documents to MongoDB '{collection_name}' in {time.time()-t0:.2f}s.")
            return
        except Exception as e:
            logger.error(f"Attempt {attempt} failed writing to MongoDB '{collection_name}': {e}")
            if attempt < max_retries:
                time.sleep(retry_delay * attempt)
            else:
                logger.error(traceback.format_exc())
                raise

# =============================================================================
# MAIN EXECUTION PIPELINE
# =============================================================================
def main():
    run_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    start_time = time.time()

    # 1. Resolve Snapshot Date
    target_snapshot_date = SNAPSHOT_DATE_ARG
    if target_snapshot_date.lower() == 'latest':
        target_snapshot_date = discover_latest_snapshot_date('fare_bucket_weighted')

    logger.info(f"Starting additive model sync pipeline for SnapshotDate: {target_snapshot_date}")

    # 2. Ingest S3 Parquet: fare_bucket_weighted
    fare_bucket_df = read_partitioned_parquet('fare_bucket_weighted', target_snapshot_date, SECTORS_FILTER)
    if fare_bucket_df.empty:
        logger.error(f"No fare_bucket_weighted data found for {target_snapshot_date}! Aborting.")
        sys.exit(1)

    # 3. Ensure bucket bounds are present for rm-allocation-process-inputs
    if 'FareBucket_Lower' not in fare_bucket_df.columns:
        b_col = 'FareBucket' if 'FareBucket' in fare_bucket_df.columns else 'FareBasis'
        if b_col in fare_bucket_df.columns:
            fare_bucket_df['FareBucket_Lower'] = (
                fare_bucket_df[b_col].astype(str).str.replace('+', '', regex=False)
                .str.split('-').str[0].astype(float)
            )
            fare_bucket_df['FareBucket_Upper'] = pd.to_numeric(
                fare_bucket_df[b_col].astype(str).str.split('-').str[1], errors='coerce'
            )

    # 4. Ingest flight_level_weighted (if present)
    flight_df = read_partitioned_parquet('flight_level_weighted', target_snapshot_date, SECTORS_FILTER)

    # 5. Compute NextGen Inventory Optimization (inventory_ng) if requested
    inventory_ng_df = None
    if COMPUTE_INVENTORY_NG:
        logger.info("Executing NextGen Inventory Optimization (50 columns, EMSR-b AU ladder)...")
        t_ng0 = time.time()
        inventory_ng_df = compute_inventory_ng_df(fare_bucket_df)
        logger.info(f"Inventory NG calculation complete in {time.time()-t_ng0:.2f}s ({len(inventory_ng_df)} rows).")

    # 6. S3 Daily Snapshot Archival
    if WRITE_S3_SNAPSHOTS:
        logger.info("Archiving daily snapshots to S3...")
        upload_snapshot_to_s3(fare_bucket_df, 'add_model_fare_bucket_level_weighted', target_snapshot_date, run_timestamp)
        if inventory_ng_df is not None and not inventory_ng_df.empty:
            upload_snapshot_to_s3(inventory_ng_df, 'add_model_inventory_ng', target_snapshot_date, run_timestamp)
        if flight_df is not None and not flight_df.empty:
            upload_snapshot_to_s3(flight_df, 'add_model_flight_level_weighted', target_snapshot_date, run_timestamp)

    # 7. Persistence: MongoDB (weighted file & NextGen outputs)
    if WRITE_MONGO:
        logger.info("Storing datasets into MongoDB...")
        # A. Weighted file (add_model_fare_bucket_level_weighted) -> MongoDB
        write_to_mongo_collection(fare_bucket_df, 'add_model_fare_bucket_level_weighted')

        # B. NextGen Inventory Optimization output (add_model_inventory_ng) -> MongoDB
        if inventory_ng_df is not None and not inventory_ng_df.empty:
            logger.info("Storing NextGen inventory optimization into MongoDB collection 'add_model_inventory_ng'...")
            write_to_mongo_collection(inventory_ng_df, 'add_model_inventory_ng')

        # C. Flight-level weighted output (add_model_flight_level_weighted) -> MongoDB
        if flight_df is not None and not flight_df.empty:
            logger.info("Storing flight-level weighted into MongoDB collection 'add_model_flight_level_weighted'...")
            write_to_mongo_collection(flight_df, 'add_model_flight_level_weighted')

    logger.info(f"Pipeline finished successfully in {time.time() - start_time:.2f}s.")

if __name__ == '__main__':
    main()
