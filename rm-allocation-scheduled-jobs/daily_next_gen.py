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

import json


def _safe_float(val, default=0.0):
    if val is None or pd.isna(val):
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _get_collection(db_obj, coll_name):
    if db_obj is None:
        return None
    coll = getattr(db_obj, coll_name, None)
    if coll is not None:
        return coll
    if hasattr(db_obj, '__getitem__'):
        try:
            return db_obj[coll_name]
        except Exception:
            return None
    return None


class FareTaxHelper:
    """
    Helper for bidirectional conversion between All-In (Gross) Fares and Airline Component Fares.
    Adheres strictly to the additive-process tax conversion logic:

        Airline Component = (All-In Fare - Other Taxes) / (1 + GST Rate)
        All-In Fare       = (Airline Component * (1 + GST Rate)) + Other Taxes

    Where:
        - Domestic Other Taxes = ASF + PSF + UDF_DF + DUF
        - International Other Taxes = ASF + PSF_ADF + UDF + PSC_PZ + Tax1 + Tax2 + Tax3 + Tax4 + Tax5
        - GST Rate is set to 0.0 for GST-exempt sectors (e.g. IXB, GAU, IXA, AJL).
        - Airline fees (RCS, CUTE, YQ) remain part of the Airline Component and are not stripped.
    """

    def __init__(self, mongo_master_db=None, constants_utils=None, rmdatabasehelper=None, logger=None, db=None):
        self.mongo_master_db = mongo_master_db
        self.constants_utils = constants_utils
        self.rmdatabasehelper = rmdatabasehelper
        self.logger = logger or logging.getLogger("FareTaxHelper")
        self.db = db

        self._dom_map = {}
        self._intl_map = {}
        self._duf_sectors = {}
        self._gst_sectors = set()
        self._loaded = False

    def _ensure_loaded(self):
        """Lazy-loads tax masters and pricing constants from Mongo or local fallback."""
        if self._loaded:
            return

        # 1. Load Domestic Tax Master (tax_mast_dom)
        dom_docs = []
        candidate_dbs = [
            self.mongo_master_db,
            getattr(self.rmdatabasehelper, 'master_db', None),
            self.db,
            getattr(self.rmdatabasehelper, 'mongoclient', None)
        ]
        for cdb in candidate_dbs:
            if cdb is not None:
                coll = _get_collection(cdb, 'tax_mast_dom')
                if coll is not None:
                    try:
                        docs = list(coll.find({}, {"_id": 0}))
                        if docs:
                            dom_docs = docs
                            break
                    except Exception as e:
                        self.logger.warning(f"Error querying tax_mast_dom collection: {e}")

        for doc in dom_docs:
            code = str(doc.get('Code', '')).strip()
            if code:
                gst_val = _safe_float(doc.get('GST', 0))
                gst_rate = gst_val / 100.0 if gst_val > 1.0 else gst_val
                self._dom_map[code] = {
                    'GST': gst_rate,
                    'RCS': _safe_float(doc.get('RCS', 0)),
                    'CUTE': _safe_float(doc.get('CUTE', 0)),
                    'ASF': _safe_float(doc.get('ASF', 0)),
                    'PSF': _safe_float(doc.get('PSF', 0)),
                    'UDF_DF': _safe_float(doc.get('UDF_DF', 0)),
                }

        # 2. Load International Tax Master (tax_mast_intl)
        intl_docs = []
        for cdb in candidate_dbs:
            if cdb is not None:
                coll = _get_collection(cdb, 'tax_mast_intl')
                if coll is not None:
                    try:
                        docs = list(coll.find({}, {"_id": 0}))
                        if docs:
                            intl_docs = docs
                            break
                    except Exception as e:
                        self.logger.warning(f"Error querying tax_mast_intl collection: {e}")

        for doc in intl_docs:
            sector = str(doc.get('Sectors', '')).strip()
            if sector:
                gst_val = _safe_float(doc.get('GST', 0))
                self._intl_map[sector] = {
                    'GST': gst_val,
                    'CUTE': _safe_float(doc.get('CUTE', 0)),
                    'ASF': _safe_float(doc.get('ASF', 0)),
                    'PSF_ADF': _safe_float(doc.get('PSF_ADF', 0)),
                    'UDF': _safe_float(doc.get('UDF', 0)),
                    'PSC_PZ': _safe_float(doc.get('PSC_PZ', 0)),
                    'RCS_CUTE': _safe_float(doc.get('RCS_CUTE', 0)),
                    'RCS': _safe_float(doc.get('RCS', 0)),
                    'Tax1': _safe_float(doc.get('Tax1', 0)),
                    'Tax2': _safe_float(doc.get('Tax2', 0)),
                    'Tax3': _safe_float(doc.get('Tax3', 0)),
                    'Tax4': _safe_float(doc.get('Tax4', 0)),
                    'Tax5': _safe_float(doc.get('Tax5', 0)),
                }

        # 3. Load Pricing Constants (PRICING_DUF_SECTORS, PRICING_GST_SECTORS)
        param_coll = None
        for cdb in candidate_dbs:
            if cdb is not None:
                param_coll = _get_collection(cdb, 'rm_parameter_values')
                if param_coll is not None:
                    break

        if param_coll is not None:
            try:
                cursor = param_coll.find(
                    {"parameterKey": {"$in": ['PRICING_DUF_SECTORS', 'PRICING_GST_SECTORS']}},
                    {"parameterKey": 1, "parameterValue": 1, "_id": 0}
                )
                for doc in cursor:
                    key = doc.get('parameterKey')
                    val = doc.get('parameterValue')
                    try:
                        parsed = json.loads(val) if isinstance(val, str) else val
                        if key == 'PRICING_DUF_SECTORS' and isinstance(parsed, dict):
                            self._duf_sectors.update(parsed)
                        elif key == 'PRICING_GST_SECTORS' and isinstance(parsed, list):
                            self._gst_sectors.update(parsed)
                    except Exception:
                        pass
            except Exception as e:
                self.logger.warning(f"Error querying rm_parameter_values: {e}")

        if not self._gst_sectors:
            gst_const = getattr(self.constants_utils, 'PRICING_GST_SECTORS', ['IXB', 'GAU', 'IXA', 'AJL'])
            self._gst_sectors = set(gst_const) if gst_const else {'IXB', 'GAU', 'IXA', 'AJL'}

        if not self._duf_sectors:
            duf_const = getattr(self.constants_utils, 'PRICING_DUF_SECTORS', {})
            self._duf_sectors = dict(duf_const) if duf_const else {}

        self._loaded = True

    def _sector_other_taxes(self, sector):
        self._ensure_loaded()
        sector = str(sector).strip().replace('/', '')
        if len(sector) < 6:
            return 0.0, 0.0, False

        origin, destination = sector[:3], sector[3:6]
        is_domestic = sector not in self._intl_map

        if is_domestic:
            tax_row = self._dom_map.get(origin)
            if not tax_row:
                return 0.0, 0.0, False

            gst_raw = tax_row['GST']
            gst_rate = gst_raw / 100.0 if gst_raw > 1.0 else gst_raw
            if origin in self._gst_sectors or destination in self._gst_sectors:
                gst_rate = 0.0

            duf = float(self._duf_sectors.get(destination, 0.0))
            other_taxes = (
                float(tax_row.get('ASF', 0.0) or 0.0) +
                float(tax_row.get('PSF', 0.0) or 0.0) +
                float(tax_row.get('UDF_DF', 0.0) or 0.0) +
                duf
            )
            return other_taxes, gst_rate, True

        tax_row = self._intl_map.get(sector)
        if not tax_row:
            return 0.0, 0.0, False

        gst_raw = tax_row['GST']
        gst_rate = gst_raw / 100.0 if gst_raw > 1.0 else gst_raw
        if origin in self._gst_sectors or destination in self._gst_sectors:
            gst_rate = 0.0

        other_taxes = (
            float(tax_row.get('ASF', 0.0) or 0.0) +
            float(tax_row.get('PSF_ADF', 0.0) or 0.0) +
            float(tax_row.get('UDF', 0.0) or 0.0) +
            float(tax_row.get('PSC_PZ', 0.0) or 0.0) +
            float(tax_row.get('Tax1', 0.0) or 0.0) +
            float(tax_row.get('Tax2', 0.0) or 0.0) +
            float(tax_row.get('Tax3', 0.0) or 0.0) +
            float(tax_row.get('Tax4', 0.0) or 0.0) +
            float(tax_row.get('Tax5', 0.0) or 0.0)
        )
        return other_taxes, gst_rate, True

    def allin_to_airline_component(self, gross_fare, origin, destination, route=None):
        if gross_fare is None or pd.isna(gross_fare):
            return None
        try:
            gross = float(gross_fare)
        except (ValueError, TypeError):
            return gross_fare

        if gross <= 0:
            return 0.0

        sector = f"{origin}{destination}".replace('/', '')
        other_taxes, gst_rate, ok = self._sector_other_taxes(sector)

        if not ok:
            if origin in self._gst_sectors or destination in self._gst_sectors:
                gst_rate = 0.0
            else:
                gst_rate = 0.05
            other_taxes = 0.0

        airline_comp = (gross - other_taxes) / (1.0 + gst_rate)
        return max(0.0, float(airline_comp))

    def airline_component_to_allin(self, airline_comp, origin, destination, route=None):
        if airline_comp is None or pd.isna(airline_comp):
            return None
        try:
            comp = float(airline_comp)
        except (ValueError, TypeError):
            return airline_comp

        if comp <= 0:
            return 0.0

        sector = f"{origin}{destination}".replace('/', '')
        other_taxes, gst_rate, ok = self._sector_other_taxes(sector)

        if not ok:
            if origin in self._gst_sectors or destination in self._gst_sectors:
                gst_rate = 0.0
            else:
                gst_rate = 0.05
            other_taxes = 0.0

        allin = (comp * (1.0 + gst_rate)) + other_taxes
        return max(0.0, float(allin))


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


def parse_time_to_minutes(time_val):
    """Parses various time representations into minutes from midnight (0..1439)."""
    if time_val is None or pd.isna(time_val):
        return None
    if isinstance(time_val, (dt_time, datetime)):
        return time_val.hour * 60 + time_val.minute
    s = str(time_val).strip()
    if not s or s.lower() == 'nan':
        return None
    upper_s = s.upper()
    if 'AM' in upper_s or 'PM' in upper_s:
        match = re.search(r'(\d{1,2}):(\d{2})(?::(\d{2}))?\s*(AM|PM)', upper_s)
        if match:
            h = int(match.group(1))
            m = int(match.group(2))
            meridiem = match.group(4)
            if meridiem == 'PM' and h < 12:
                h += 12
            elif meridiem == 'AM' and h == 12:
                h = 0
            return h * 60 + m
    match = re.search(r'(\d{1,2}):(\d{2})', s)
    if match:
        return int(match.group(1)) * 60 + int(match.group(2))
    return None


def is_flag_enabled(val):
    """Safely checks if a flag is truthy / enabled ('1', 1, 'Y', 'True', True)."""
    if val is None or pd.isna(val):
        return False
    if isinstance(val, (bool, np.bool_)):
        return bool(val)
    s = str(val).strip().upper()
    return s in ('1', '1.0', 'Y', 'YES', 'TRUE', 'T')


def filter_market_fares_by_time(
    fares_df,
    flight_std,
    time_range='TR',
    carrier_exclusion=None,
    flight_exclusion=None,
    connection_fare_flag=0,
    additional_connection_duration=4.0,
    direct_duration_mins=None
):
    """
    Filters market fares according to daily_next_gen rules:
    - Carrier exclusion: removes excluded competitors (in addition to QP and G8).
    - Flight exclusion: removes specific excluded competitor flight numbers.
    - Connection fare filtering: if enabled, includes connecting flights with duration
      <= direct_duration + additional_connection_duration * 60; otherwise direct flights only.
    - FR: full 24 hrs except 11pm to 4am (23:00 to 04:00), but if flight itself
          is from 11pm to 4am then use whole 24 hrs (00:00 to 23:59).
    - TR: check for OAL fare in +/- 2 hours of flight departure time, if not extend
          time range by 2 hrs on both sides and check repeatedly until a fare is found.
    - ExTR: same as TR but start with +/- 4 hours instead of 2.
    Extension of time range is strictly limited to same departure date (00:00 to 23:59).
    """
    if fares_df is None or fares_df.empty:
        return pd.DataFrame()

    df = fares_df.copy()

    # 1. Carrier exclusion
    if carrier_exclusion and str(carrier_exclusion).strip() not in ('**', 'NAN', 'nan', '', 'None'):
        excl_list = [c.strip().upper() for c in re.split(r'[,/|\s]+', str(carrier_exclusion)) if c.strip() and c.strip() != '**']
        carr_col = next((c for c in ['outbound_airline_code', 'AirlineCode', 'airline_code', 'Carrier', 'carrier'] if c in df.columns), None)
        if excl_list and carr_col:
            df = df[~df[carr_col].astype(str).str.upper().isin(excl_list)]
        if df.empty:
            return df

    # 1b. Flight number exclusion
    if flight_exclusion and str(flight_exclusion).strip() not in ('**', 'NAN', 'nan', '', 'None'):
        excl_flts = [f.strip().upper() for f in re.split(r'[,/|\s]+', str(flight_exclusion)) if f.strip() and f.strip() != '**']
        flt_col = next((c for c in ['outbound_flight_no', 'FlightNumber', 'flight_number', 'FlightNo', 'flight_no'] if c in df.columns), None)
        if excl_flts and flt_col:
            df = df[~df[flt_col].astype(str).str.upper().isin(excl_flts)]
        if df.empty:
            return df

    # 2. Connection vs Direct flight filtering
    stops_col = next((c for c in ['outbound_stops', 'stops', 'Stops', 'outbound_stop'] if c in df.columns), None)
    if stops_col and stops_col in df.columns:
        stops_s = pd.to_numeric(df[stops_col], errors='coerce').fillna(0)
        if not is_flag_enabled(connection_fare_flag):
            df = df[stops_s == 0]
        else:
            dur_col = next((c for c in ['outbound_duration', 'duration', 'Duration', 'flight_duration'] if c in df.columns), None)
            if dur_col and dur_col in df.columns:
                direct_mins = float(direct_duration_mins or 120.0)
                add_mins = float(additional_connection_duration or 4.0) * 60.0
                max_allowed = direct_mins + add_mins

                def _parse_dur_mins(v):
                    if pd.isna(v): return direct_mins
                    s = str(v).strip()
                    if ':' in s:
                        p = s.split(':')
                        try: return float(p[0]) * 60.0 + float(p[1])
                        except Exception: return direct_mins
                    try:
                        n = float(s)
                        return n if n > 24.0 else n * 60.0
                    except Exception:
                        return direct_mins

                dur_s = df[dur_col].apply(_parse_dur_mins)
                df = df[(stops_s == 0) | ((stops_s > 0) & (dur_s <= max_allowed))]
        if df.empty:
            return df

    std_mins = parse_time_to_minutes(flight_std)
    if std_mins is None:
        std_mins = 720  # default 12:00 PM noon if missing

    if 'dep_mins' not in df.columns:
        time_col = next((c for c in ['outbound_departuretime', 'DepTime', 'departure_time', 'dep_time', 'STD'] if c in df.columns), None)
        if not time_col:
            return df
        df['dep_mins'] = df[time_col].apply(parse_time_to_minutes)

    df = df[df['dep_mins'].notna()]
    if df.empty:
        return df

    tr = str(time_range).strip().upper()

    # 3. FR rule: full 24 hrs except 11pm to 4am (23:00 to 04:00)
    # but if flight itself from 11pm to 4am then use whole 24 hrs
    if tr == 'FR':
        is_night_flight = (std_mins >= 1380 or std_mins < 240)
        if is_night_flight:
            return df[(df['dep_mins'] >= 0) & (df['dep_mins'] <= 1439)]
        else:
            return df[(df['dep_mins'] >= 240) & (df['dep_mins'] < 1380)]

    # 4. TR / ExTR rule:
    # TR: start +/- 2 hrs, extend by 2 hrs on both sides until fare found.
    # ExTR: start +/- 4 hrs, extend by 2 hrs on both sides until fare found.
    # Extension limited to same departure date [0, 1439].
    initial_delta = 4 if tr == 'EXTR' else 2
    delta_hrs = initial_delta
    max_delta_hrs = 24

    while delta_hrs <= max_delta_hrs:
        win_start = max(0, std_mins - delta_hrs * 60)
        win_end = min(1439, std_mins + delta_hrs * 60)
        filtered = df[(df['dep_mins'] >= win_start) & (df['dep_mins'] <= win_end)]
        if not filtered.empty:
            return filtered
        if win_start == 0 and win_end == 1439:
            break
        delta_hrs += 2

    return df[(df['dep_mins'] >= 0) & (df['dep_mins'] <= 1439)]


def get_ndo_band(ndo, mongo_db=None):
    """Determines NDO band: (start, end] convention (0, 1, 2, 3, 4, 5, 6, 7)."""
    n = int(float(ndo)) if ndo is not None and pd.notna(ndo) else 0
    if n <= 0:
        return 0
    elif n == 1:
        return 1
    if mongo_db is not None:
        try:
            coll = getattr(mongo_db, 'ndo_bands', None)
            if coll is not None:
                doc = coll.find_one({"start": {"$lt": n}, "end": {"$gte": n}})
                if doc:
                    return int(doc.get("ndo_band", doc.get("id", 0)))
        except Exception:
            pass
    if n <= 3:
        return 2
    elif n <= 7:
        return 3
    elif n <= 14:
        return 4
    elif n <= 30:
        return 5
    elif n <= 60:
        return 6
    else:
        return 7


def get_plf_band(plf_pct, mongo_db=None):
    """Determines PLF band (0..5) from booked PLF percentage."""
    val = float(plf_pct) if plf_pct is not None and pd.notna(plf_pct) else 0.0
    if 0.0 < val <= 1.5:
        val = val * 100.0
    if mongo_db is not None:
        try:
            coll = getattr(mongo_db, 'comp_response_plf_bands', None)
            if coll is not None:
                doc = coll.find_one({"start": {"$lte": val}, "end": {"$gt": val}})
                if doc:
                    return int(doc.get("plf_band", doc.get("id", 0)))
        except Exception:
            pass
    if val < 80.0:
        return 0
    elif val < 85.0:
        return 1
    elif val < 90.0:
        return 2
    elif val < 95.0:
        return 3
    elif val < 100.0:
        return 4
    else:
        return 5


def evaluate_grid_response(grid_rule_str, anchor_fare, comp_fare, comp_med_fare=None, threshold_factor=None, tolerance=None):
    """Evaluates grid response rule against anchor fare (A) and competitor fare (C/med).
    Applies tolerance / threshold_factor floor if provided:
      rule_resp = max(thresh_fare, rule_resp)
    """
    A = float(anchor_fare or 0.0)
    C = float(comp_fare or 0.0)
    med_C = float(comp_med_fare or C) if comp_med_fare is not None else C

    rule = str(grid_rule_str).strip()
    if not rule or rule.upper() == 'NA':
        return C

    C_val = med_C if re.search(r'\bmed\b', rule, re.IGNORECASE) else C

    expr = rule
    if '/' in expr:
        expr = expr.split('/')[-1].strip()

    expr = re.sub(r'(?i)\bmin\b', 'min', expr)
    expr = re.sub(r'(?i)\bmax\b', 'max', expr)
    expr = re.sub(r'(\d+(?:\.\d+)?)\s*[aA]\b', r'\1*A', expr)
    expr = re.sub(r'(\d+(?:\.\d+)?)\s*[cC]\b', r'\1*C', expr)
    expr = re.sub(r'(?i)\bA\b', 'A', expr)
    expr = re.sub(r'(?i)\bC\b', 'C', expr)

    safe_env = {
        "A": A,
        "C": C_val,
        "min": min,
        "max": max,
        "round": round
    }

    try:
        rule_resp = float(eval(expr, {"__builtins__": {}}, safe_env))
    except Exception:
        rule_resp = C_val

    tol_param = tolerance if tolerance is not None else threshold_factor
    if tol_param is not None and pd.notna(tol_param) and str(tol_param).strip().upper() not in ['', 'NA', 'NAN']:
        try:
            tf = float(tol_param)
            if tf > 100.0:
                rule_resp = max(tf, rule_resp)
            else:
                if tf > 1.5:
                    tf = tf / 100.0
                if tf > 0:
                    c_for_thresh = med_C if re.search(r'\bmed\b', rule, re.IGNORECASE) else C
                    thresh_fare = tf * float(c_for_thresh)
                    rule_resp = max(thresh_fare, rule_resp)
        except Exception:
            pass

    return rule_resp


def load_comp_response_grid(mongo_client=None, mongo_db=None, grid_name='DEFAULT', logger=None):
    """Loads all 48 combination rules for grid_name from MongoDB."""
    grid_cache = {}
    if mongo_client is None and mongo_db is None:
        return grid_cache

    target_colls = []
    if mongo_db is not None and hasattr(mongo_db, 'comp_response_grid'):
        target_colls.append(mongo_db.comp_response_grid)
    if mongo_client is not None:
        for db_name in ['rm_database', 'qp_rm_master', 'local_qp_rm_master', 'local_qp_dw_rmalloc']:
            try:
                if db_name in mongo_client.list_database_names():
                    target_colls.append(mongo_client[db_name]['comp_response_grid'])
            except Exception:
                pass

    for coll in target_colls:
        if coll is None:
            continue
        try:
            docs = list(coll.find({"grid_name": grid_name}))
            if docs:
                for d in docs:
                    key = (int(d.get("ndo_band", 0)), int(d.get("plf_band", 0)))
                    grid_cache[key] = d
                if logger:
                    logger.info(f"Loaded {len(grid_cache)} comp_response_grid rules for grid '{grid_name}'.")
                break
        except Exception as e:
            if logger:
                logger.warning(f"Error querying comp_response_grid: {e}")

    return grid_cache


def fetch_market_fares_b2c(mongo_client=None, mongo_db=None, origin=None, destin=None, dep_date=None, connection_fare_flag=0, logger=None):
    """Fetches direct or connecting B2C competitor market fares for origin-destination-departure date."""
    if (mongo_client is None and mongo_db is None) or not origin or not destin or not dep_date:
        return pd.DataFrame()

    dep_date_str = str(dep_date)[:10]
    query = {
        'channel': 'B2C',
        'source_airport': origin.upper(),
        'destination_airport': destin.upper(),
        'outbound_airline_code': {'$nin': ['QP', 'G8']}
    }
    if not is_flag_enabled(connection_fare_flag):
        query['outbound_stop'] = {'$in': [0, '0', 0.0, None]}
    try:
        dt = pd.to_datetime(dep_date_str)
        query['$or'] = [
            {'departure_date': {'$gte': dt, '$lt': dt + pd.Timedelta(days=1)}},
            {'departure_date': dep_date_str},
            {'departure_date': {'$regex': f'^{dep_date_str}'}}
        ]
    except Exception:
        query['departure_date'] = dep_date_str

    target_colls = []
    if mongo_client is not None:
        for db_name in ['qp_rm_master', 'rm_database', 'local_qp_rm_master', 'local_qp_dw_rmalloc']:
            try:
                if db_name in mongo_client.list_database_names():
                    db_obj = mongo_client[db_name]
                    for coll_name in ['market_fares_rg', 'market_fares_ag', 'market_fares']:
                        if coll_name in db_obj.list_collection_names():
                            target_colls.append(db_obj[coll_name])
            except Exception:
                pass
    if mongo_db is not None:
        for coll_name in ['market_fares_rg', 'market_fares_ag', 'market_fares']:
            if hasattr(mongo_db, coll_name):
                target_colls.append(getattr(mongo_db, coll_name))

    projection = {
        'outbound_departuretime': 1,
        'gross_fare': 1,
        'outbound_airline_code': 1,
        'departure_date': 1,
        'outbound_stop': 1,
        'outbound_duration': 1
    }

    all_records = []
    for coll in target_colls:
        try:
            records = list(coll.find(query, projection))
            if records:
                all_records.extend(records)
                break
        except Exception:
            continue

    if not all_records:
        return pd.DataFrame()

    df = pd.DataFrame(all_records)
    if 'gross_fare' in df.columns:
        df['gross_fare'] = pd.to_numeric(df['gross_fare'], errors='coerce')
        df = df[df['gross_fare'] > 0]
    return df


def load_market_list(mongo_client=None, mongo_db=None, logger=None):
    """Loads market_list collection documents from MongoDB."""
    if mongo_client is None and mongo_db is None:
        return pd.DataFrame()

    target_colls = []
    if mongo_db is not None:
        for coll_name in ['market_list', 'marketlist']:
            if hasattr(mongo_db, coll_name):
                target_colls.append(getattr(mongo_db, coll_name))
            elif isinstance(mongo_db, dict) and coll_name in mongo_db:
                target_colls.append(mongo_db[coll_name])
            else:
                try:
                    if coll_name in mongo_db.list_collection_names():
                        target_colls.append(mongo_db[coll_name])
                except Exception:
                    pass

    if mongo_client is not None:
        for db_name in ['rm_database', 'qp_rm_master', 'local_qp_rm_master', 'local_qp_dw_rmalloc']:
            try:
                if db_name in mongo_client.list_database_names():
                    db_obj = mongo_client[db_name]
                    for coll_name in ['market_list', 'marketlist']:
                        if coll_name in db_obj.list_collection_names():
                            target_colls.append(db_obj[coll_name])
            except Exception:
                pass

    for coll in target_colls:
        if coll is None:
            continue
        try:
            docs = list(coll.find({}))
            if docs:
                if logger:
                    logger.info(f"Loaded {len(docs)} market_list records from MongoDB.")
                return pd.DataFrame(docs)
        except Exception as e:
            if logger:
                logger.warning(f"Error querying market_list: {e}")

    return pd.DataFrame()


def match_market_list_config(market_list_df, origin, destin, flight_num, dep_date, dow=None):
    """
    Matches flight details against market_list records to extract NG and operational parameters.
    Precedence:
    1. Origin, Destin, FlightNumber match covering dep_date (with DOW mask check if applicable).
    2. Origin, Destin, FlightNumber match (any).
    3. Route-level match on Origin, Destin.
    Returns matched row dict or empty dict if not found.
    """
    if market_list_df is None or market_list_df.empty or not origin or not destin:
        return {}

    org_upper = str(origin).strip().upper()
    dest_upper = str(destin).strip().upper()

    sec_matches = market_list_df[
        (market_list_df['Origin'].astype(str).str.strip().str.upper() == org_upper) &
        (market_list_df['Destin'].astype(str).str.strip().str.upper() == dest_upper)
    ]
    if sec_matches.empty:
        return {}

    flt_str = str(flight_num).strip()
    m = re.search(r'(\d+)', flt_str)
    flt_int = int(m.group(1)) if m else None

    if flt_int is not None and 'FlightNumber' in sec_matches.columns:
        flt_col = pd.to_numeric(sec_matches['FlightNumber'].astype(str).str.extract(r'(\d+)', expand=False), errors='coerce')
        flt_matches = sec_matches[flt_col == flt_int]
        candidates = flt_matches if not flt_matches.empty else sec_matches
    else:
        candidates = sec_matches

    if dep_date is not None and not candidates.empty:
        try:
            target_dt = pd.to_datetime(dep_date).date()
        except Exception:
            target_dt = None

        if target_dt is not None:
            def _parse_d(v):
                if pd.isna(v): return None
                if isinstance(v, (datetime, dt_date)):
                    return v.date() if isinstance(v, datetime) else v
                for fmt in ('%m/%d/%Y', '%Y-%m-%d', '%d/%m/%Y', '%Y/%m/%d'):
                    try: return datetime.strptime(str(v).strip(), fmt).date()
                    except Exception: pass
                try: return pd.to_datetime(v).date()
                except Exception: return None

            date_matches = []
            for _, r in candidates.iterrows():
                p_start = _parse_d(r.get('PerStart'))
                p_end = _parse_d(r.get('PerEnd')) or p_start
                if p_start and p_end:
                    if p_start <= target_dt <= p_end:
                        dow_mask = str(r.get('DOW', '')).strip()
                        if len(dow_mask) == 7 and dow_mask not in ('9999999', '1111111'):
                            if dow_mask[target_dt.weekday()] == '1':
                                date_matches.append(r)
                        else:
                            date_matches.append(r)
            if date_matches:
                return date_matches[0].to_dict()

    return candidates.iloc[0].to_dict()


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
    open_bucket_markup=1.20,
    grid_doc=None,
    market_fares_df=None,
    time_range=None,
    criteria=None,
    response_rule=None,
    carrier_exclusion=None,
    flight_exclusion=None,
    connection_fare_flag=0,
    additional_connection_duration=4.0,
    direct_duration_mins=None,
    competition_response_enabled=True,
    promo_flag=0,
    backstop=None,
    hardanchor=None,
    plf_threshold=95.0,
    tolerance=None,
    tax_helper=None,
    origin=None,
    destin=None,
    route=None
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
    sec_clean = sec.replace('-', '').replace('_', '').strip().upper()
    if not origin and len(sec_clean) >= 6:
        origin = sec_clean[:3]
    if not destin and len(sec_clean) >= 6:
        destin = sec_clean[3:6]
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
    try:
        alpha = float(buy_up_factor or 0.0)
    except (ValueError, TypeError):
        alpha = 0.0
    if alpha > 1.0:
        alpha = alpha / 100.0
    alpha = float(np.clip(alpha, 0.0, 0.99))
    valid_f = F_j > 0
    prob[valid_f] = np.clip((1.0 - (next_fare[valid_f] / F_j[valid_f])) / max(1e-6, 1.0 - alpha), 0.0001, 0.9999)

    z = norm.ppf(prob)
    y_raw = np.ceil(mu_bar_orig + z * sigma_bar)
    y_raw = np.maximum(0.0, y_raw)

    y_j = np.maximum.accumulate(y_raw)
    prot_lim = np.minimum(cap, y_j)

    au = np.empty(k, dtype=float)
    au[0] = cap
    if k > 1:
        au[1:] = np.maximum(0.0, cap - prot_lim[:-1])
    au_promo_1 = max(0.0, float(cap - prot_lim[-1])) if k > 0 else float(cap)

    # Competition & Strategy
    if grid_doc:
        time_range = time_range or grid_doc.get('time_range', 'TR')
        criteria = criteria or grid_doc.get('criteria', 'Min')
        response_rule = response_rule or grid_doc.get('response_rule', 'C')
    time_range = time_range or 'TR'
    criteria = criteria or 'Min'
    response_rule = response_rule or 'C'

    comp_enabled = is_flag_enabled(competition_response_enabled) if competition_response_enabled is not None else True

    oal_min = None
    comp_fare = None
    c_med = None

    if market_fares_df is not None and not market_fares_df.empty:
        if direct_duration_mins is None:
            dur_col = next((c for c in ['flight_duration', 'outbound_duration', 'Duration', 'duration'] if c in df.columns), None)
            if dur_col and pd.notna(df[dur_col].iloc[0]):
                direct_duration_mins = parse_time_to_minutes(df[dur_col].iloc[0])
            if not direct_duration_mins:
                direct_duration_mins = (dist / 550.0) * 60.0 + 30.0

        mkt_fares_filtered_input = market_fares_df
        if tax_helper and origin and destin and 'gross_fare' in market_fares_df.columns:
            mkt_fares_filtered_input = market_fares_df.copy()
            mkt_fares_filtered_input['gross_fare'] = mkt_fares_filtered_input['gross_fare'].apply(
                lambda f: tax_helper.allin_to_airline_component(float(f), origin, destin, route=route) if pd.notna(f) else f
            )

        filtered_mf = filter_market_fares_by_time(
            mkt_fares_filtered_input,
            std_str,
            time_range=time_range,
            carrier_exclusion=carrier_exclusion,
            flight_exclusion=flight_exclusion,
            connection_fare_flag=connection_fare_flag,
            additional_connection_duration=additional_connection_duration,
            direct_duration_mins=direct_duration_mins
        )
        if not filtered_mf.empty and 'gross_fare' in filtered_mf.columns:
            valid_fares = pd.to_numeric(filtered_mf['gross_fare'], errors='coerce').dropna()
            valid_fares = valid_fares[valid_fares > 0]
            if not valid_fares.empty:
                c_min = float(valid_fares.min())
                c_med = float(valid_fares.median())
                c_max = float(valid_fares.max())
                oal_min = c_min

                crit_upper = str(criteria).upper()
                if 'MAX' in crit_upper:
                    comp_fare = c_max
                elif 'MED' in crit_upper:
                    comp_fare = c_med
                else:
                    comp_fare = c_min

    if oal_min is None and market_min_fare is not None and market_min_fare > 0:
        if tax_helper and origin and destin:
            try:
                oal_min = tax_helper.allin_to_airline_component(float(market_min_fare), origin, destin, route=route)
            except Exception:
                oal_min = float(market_min_fare)
        else:
            oal_min = float(market_min_fare)
        comp_fare = oal_min

    # Resolve backstop in airline component
    b_val = None
    if backstop is not None and pd.notna(backstop):
        try:
            b_float = float(backstop)
            if b_float > 0:
                if tax_helper and origin and destin:
                    try:
                        b_val = tax_helper.allin_to_airline_component(b_float, origin, destin, route=route)
                    except Exception:
                        b_val = b_float
                else:
                    b_val = b_float
        except (ValueError, TypeError):
            b_val = None

    # Resolve hardanchor in airline component
    h_val = None
    curr_plf = (tot_bookings / cap * 100.0) if cap > 0 else 0.0
    plf_thresh = float(plf_threshold if plf_threshold is not None and pd.notna(plf_threshold) else 95.0)
    if hardanchor is not None and pd.notna(hardanchor):
        try:
            h_float = float(hardanchor)
            if h_float > 0:
                if tax_helper and origin and destin:
                    try:
                        h_val = tax_helper.allin_to_airline_component(h_float, origin, destin, route=route)
                    except Exception:
                        h_val = h_float
                else:
                    h_val = h_float
        except (ValueError, TypeError):
            raw_ha_str = str(hardanchor).strip().upper()
            if raw_ha_str in ['MIN', 'MAX', 'MED', 'MINO', 'MAXO', 'MEDC']:
                if oal_min is not None and oal_min > 0:
                    h_val = oal_min

    # Anchor Fare A: lowest filed fare where AU > Total_Bookings (EMSR-b)
    open_indices = [i for i in range(k) if au[i] > tot_bookings]
    if open_indices:
        anchor_fare = float(fare_arr[open_indices[-1]])
    else:
        anchor_fare = float(fare_arr[0]) if k > 0 else 3000.0

    anchor_candidates = [f for f in fare_arr if f <= anchor_fare]
    a_ladder = anchor_candidates[0] if anchor_candidates else (fare_arr[-1] if k > 0 else 3000.0)
    a_idx = list(fare_arr).index(a_ladder) if a_ladder in fare_arr else (k - 1)

    if comp_enabled and comp_fare is not None and oal_min is not None and oal_min >= anchor_fare:
        # 1. Fare obtained from grid calculation
        grid_calc_fare = evaluate_grid_response(
            response_rule,
            anchor_fare,
            comp_fare,
            comp_med_fare=c_med,
            threshold_factor=None,
            tolerance=None
        )

        # 2. Tolerance in the grid
        c_for_thresh = c_med if (c_med is not None and re.search(r'\bmed\b', str(response_rule), re.IGNORECASE)) else comp_fare
        grid_tol = None
        if grid_doc:
            grid_tol = grid_doc.get('tolerance', grid_doc.get('tolernace', grid_doc.get('threshold_factor')))

        grid_tol_fare = 0.0
        if grid_tol is not None and pd.notna(grid_tol) and str(grid_tol).strip().upper() not in ['', 'NA', 'NAN']:
            try:
                gt = float(grid_tol)
                if gt > 100.0:
                    grid_tol_fare = gt
                elif gt > 1.5:
                    if c_for_thresh is not None:
                        grid_tol_fare = (gt / 100.0) * float(c_for_thresh)
                elif gt > 0.0:
                    if c_for_thresh is not None:
                        grid_tol_fare = gt * float(c_for_thresh)
            except (ValueError, TypeError):
                pass

        # 3. Tolerance in ML input
        ml_tol_fare = 0.0
        ml_tol = tolerance
        if ml_tol is not None and pd.notna(ml_tol) and str(ml_tol).strip().upper() not in ['', 'NA', 'NAN']:
            try:
                mt = float(ml_tol)
                if mt > 100.0:
                    if tax_helper and origin and destin:
                        try:
                            ml_tol_fare = float(tax_helper.allin_to_airline_component(mt, origin, destin, route=route))
                        except Exception:
                            ml_tol_fare = mt
                    else:
                        ml_tol_fare = mt
                elif mt > 1.5:
                    if c_for_thresh is not None:
                        ml_tol_fare = (mt / 100.0) * float(c_for_thresh)
                elif mt > 0.0:
                    if c_for_thresh is not None:
                        ml_tol_fare = mt * float(c_for_thresh)
            except (ValueError, TypeError):
                pass

        # Higher fare considered as tolerance
        tolerance_fare = max(grid_tol_fare, ml_tol_fare)

        # Strategy fare: max of tolerance fare and grid calculation fare
        target_resp = max(grid_calc_fare, tolerance_fare)
        strat_candidates = [f for f in fare_arr if f <= target_resp]
        strategy_fare = strat_candidates[0] if strat_candidates else fare_arr[-1]
    else:
        strategy_fare = anchor_fare

    if not comp_enabled or oal_min is None or oal_min < anchor_fare or strategy_fare <= a_ladder:
        strategy_fare = anchor_fare
        mu_bar_2 = np.copy(mu_bar_orig)
        mu_2 = np.copy(mu_arr)
        au_2 = np.copy(au)
        for j in range(a_idx + 1, k):
            au_2[j] = 0.0

        if au_2[a_idx] < tot_bookings:
            elevate_au = tot_bookings
            for m in range(a_idx, -1, -1):
                if au_2[m] >= tot_bookings:
                    elevate_au = au_2[m]
                    break
            for m in range(a_idx, -1, -1):
                if au_2[m] < elevate_au:
                    au_2[m] = elevate_au

        # 1. FirstRBD logic applied
        au_revised = np.copy(au_2)
        if first_rbd is not None and first_rbd > 0 and not np.isinf(first_rbd):
            au_revised[a_idx] = min(au_revised[a_idx], tot_bookings + first_rbd)
        for j in range(a_idx + 1, k):
            au_revised[j] = 0.0

        # 2. Hardanchor with PLF threshold: applied after FirstRBD logic
        if h_val is not None and h_val > 0:
            lower_cands = [f for f in fare_arr if f <= h_val]
            h_ladder = lower_cands[0] if lower_cands else fare_arr[-1]
            ha_idx = list(fare_arr).index(h_ladder)

            plf_pct = (plf_thresh / 100.0) if plf_thresh > 1.5 else plf_thresh
            plf_target = float(round(plf_pct * cap, 2))
            plf_target = min(float(cap), max(0.0, plf_target))

            if au_revised[ha_idx] < plf_target:
                au_revised[ha_idx] = plf_target

            for i in range(ha_idx):
                if au_revised[i] >= au_revised[ha_idx]:
                    pass
                else:
                    au_revised[i] = plf_target

        # 3. Backstop floor: applied after Hardanchor logic
        if b_val is not None and b_val > 0:
            for i, f in enumerate(fare_arr):
                if f < b_val:
                    au_revised[i] = 0.0

        P_eq_vals = prob
        y_2_list = prot_lim
        au_promo_2 = au_promo_1
        au_revised_promo = min(au_promo_2, tot_bookings + 10.0) if is_flag_enabled(promo_flag) else 0.0
        if b_val is not None and b_val > 0 and (round(float(promo_ratio) * float(strategy_fare), 2) < b_val):
            au_revised_promo = 0.0
    else:
        strat_idx = list(fare_arr).index(strategy_fare)
        P_max = fare_arr[0]

        cum_mu = 0.0
        p_min_old = strategy_fare
        threshold = 0.1 * float(cap)
        for idx in range(a_idx, strat_idx - 1, -1):
            cum_mu += float(mu_arr[idx])
            if cum_mu >= threshold:
                p_min_old = float(fare_arr[idx])
                break

        P_eq_list = []
        mu_bar_2_list = []
        for i, p in enumerate(fare_arr):
            if p < strategy_fare:
                peq = 0.0
                m_bar_2 = 0.0
            elif p == strategy_fare:
                peq = anchor_fare
                m_bar_2 = mu_bar_orig[a_idx]
            elif p >= P_max:
                peq = P_max
                m_bar_2 = mu_bar_orig[0]
            else:
                denom = (P_max - strategy_fare)
                peq = p_min_old + ((p - strategy_fare) / denom) * (P_max - p_min_old) if denom > 0 else p_min_old
                if peq >= fare_arr[0]:
                    m_bar_2 = mu_bar_orig[0]
                elif peq <= fare_arr[-1]:
                    m_bar_2 = mu_bar_orig[-1]
                else:
                    m_bar_2 = 0.0
                    for m in range(k - 1):
                        if fare_arr[m] >= peq >= fare_arr[m+1]:
                            frac = (fare_arr[m] - peq) / (fare_arr[m] - fare_arr[m+1]) if (fare_arr[m] - fare_arr[m+1]) > 0 else 0.0
                            m_bar_2 = mu_bar_orig[m] + frac * (mu_bar_orig[m+1] - mu_bar_orig[m])
                            break
            P_eq_list.append(peq)
            mu_bar_2_list.append(m_bar_2)

        mu_2_list = []
        for i in range(k):
            if fare_arr[i] < strategy_fare:
                mu_2_list.append(0.0)
            elif i == 0:
                mu_2_list.append(mu_bar_2_list[0])
            else:
                mu_2_list.append(max(0.0, mu_bar_2_list[i] - mu_bar_2_list[i-1]))

        mu_bar_2 = np.array(mu_bar_2_list, dtype=float)
        mu_2 = np.array(mu_2_list, dtype=float)

        f_promo = promo_ratio * fare_arr[-1] if k > 0 else 0.0
        y_2_list = []
        for j in range(k):
            if fare_arr[j] < strategy_fare:
                y_2_list.append(float(cap))
                continue
            m_bar = np.sum(mu_2[:j+1])
            s_bar = sigma_factor * m_bar
            F_j_2 = (np.sum(mu_2[:j+1] * fare_arr[:j+1]) / m_bar) if m_bar > 0 else fare_arr[j]
            f_next = fare_arr[j+1] if j < k - 1 else f_promo
            if F_j_2 > 0:
                p_val = (1.0 - (f_next / F_j_2)) / max(1e-6, 1.0 - alpha)
            else:
                p_val = 0.5
            p_clipped = float(np.clip(p_val, 0.0001, 0.9999))
            z_val = float(norm.ppf(p_clipped))
            yj = max(0.0, float(m_bar + z_val * s_bar))
            if y_2_list and y_2_list[-1] < cap:
                yj = max(yj, y_2_list[-1])
            y_2_list.append(yj)

        au_2 = np.empty(k, dtype=float)
        au_2[0] = cap
        for j in range(1, k):
            if fare_arr[j] < strategy_fare:
                au_2[j] = 0.0
            else:
                au_2[j] = max(0.0, cap - y_2_list[j-1])

        if au_2[strat_idx] < tot_bookings:
            elevate_au = tot_bookings
            for m in range(strat_idx, -1, -1):
                if au_2[m] >= tot_bookings:
                    elevate_au = au_2[m]
                    break
            for m in range(strat_idx, -1, -1):
                if au_2[m] < elevate_au:
                    au_2[m] = elevate_au

        # 1. FirstRBD logic applied
        au_revised = np.copy(au_2)
        if first_rbd is not None and first_rbd > 0 and not np.isinf(first_rbd):
            au_revised[strat_idx] = min(au_revised[strat_idx], tot_bookings + first_rbd)
        for j in range(strat_idx + 1, k):
            au_revised[j] = 0.0

        # 2. Hardanchor with PLF threshold: applied after FirstRBD logic
        if h_val is not None and h_val > 0:
            lower_cands = [f for f in fare_arr if f <= h_val]
            h_ladder = lower_cands[0] if lower_cands else fare_arr[-1]
            ha_idx = list(fare_arr).index(h_ladder)

            plf_pct = (plf_thresh / 100.0) if plf_thresh > 1.5 else plf_thresh
            plf_target = float(round(plf_pct * cap, 2))
            plf_target = min(float(cap), max(0.0, plf_target))

            if au_revised[ha_idx] < plf_target:
                au_revised[ha_idx] = plf_target

            for i in range(ha_idx):
                if au_revised[i] >= au_revised[ha_idx]:
                    pass
                else:
                    au_revised[i] = plf_target

        # 3. Backstop floor: applied after Hardanchor logic
        if b_val is not None and b_val > 0:
            for i, f in enumerate(fare_arr):
                if f < b_val:
                    au_revised[i] = 0.0

        P_eq_vals = np.array(P_eq_list, dtype=float)
        y_promo_comp = y_2_list[-1] if len(y_2_list) > 0 else (prot_lim[-1] if k > 0 else 0.0)
        au_promo_2 = max(0.0, float(cap - y_promo_comp))
        au_revised_promo = min(au_promo_2, tot_bookings + 10.0) if is_flag_enabled(promo_flag) else 0.0
        if b_val is not None and b_val > 0 and (round(float(promo_ratio) * float(strategy_fare), 2) < b_val):
            au_revised_promo = 0.0

    # Opening fare: lowest fare with AU higher than Total Bookings (respecting backstop floor)
    open_indices = [i for i in range(k) if au_revised[i] > tot_bookings]
    if open_indices:
        opening_fare = fare_arr[open_indices[-1]]
    else:
        if b_val is not None and b_val > 0:
            valid_b = [f for f in fare_arr if f >= b_val]
            opening_fare = valid_b[-1] if valid_b else (fare_arr[0] if k > 0 else 3000.0)
        else:
            opening_fare = fare_arr[0] if k > 0 else 3000.0
    if b_val is not None and b_val > 0 and opening_fare < b_val:
        opening_fare = b_val

    # Closed-Form Revenue Forecasting (Phase 4)
    A_cap = [0.0] * k
    ER = [0.0] * k
    Ai_list = [0.0] * k

    for i in reversed(range(k)):
        if fare_arr[i] < (anchor_fare - 1e-5):
            Ai_list[i] = 0.0
            A_cap[i] = 0.0
            ER[i] = 0.0
            continue

        sum_lower = sum(A_cap[i+1:])
        Ai = max(0.0, float(au_2[i]) - tot_bookings - sum_lower)
        Ai_list[i] = Ai
        m_i = float(mu_2[i])
        s_i = float(sigma_factor * mu_2[i])

        if s_i > 0:
            zi = (Ai - m_i) / s_i
            phi_z = float(norm.pdf(zi))
            Phi_z = float(norm.cdf(zi))
            A_hat_i = m_i * Phi_z - s_i * phi_z + Ai * (1.0 - Phi_z)
        else:
            A_hat_i = min(m_i, Ai)

        A_cap[i] = max(0.0, A_hat_i)
        ER[i] = fare_arr[i] * A_cap[i]

    total_expected_revenue = float(sum(ER))
    predicted_revenue = curr_rev + total_expected_revenue

    pred_pickup = float(np.sum(mu_arr))
    pred_flown = tot_bookings + pred_pickup
    pred_pickup_rev = float(sum(m for f, m in zip(fare_arr, mu_arr) if f >= anchor_fare - 1e-5))
    pred_flown_rev = tot_bookings + pred_pickup_rev

    denom = min(cap, pred_flown_rev) if pred_flown_rev > 0 else cap
    af_rev = (predicted_revenue / denom) if denom > 0 else 0.0
    lf_rev = (pred_flown_rev / cap) if cap > 0 else 0.0
    rpkm = pred_flown_rev * dist
    askm = cap * dist
    rask_rev = (predicted_revenue / askm) if askm > 0 else 0.0

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
    res['P_eq'] = P_eq_vals
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
    res['A_cap'] = A_cap
    res['Current_Revenue'] = round(curr_rev, 2)
    res['Expected_Revenue'] = [round(x, 2) for x in ER]
    res['Total_Expected_Revenue'] = round(total_expected_revenue, 2)
    res['Predicted_Revenue'] = round(predicted_revenue, 2)
    res['SnapshotDate'] = snap_date
    res['Tagging'] = tagging_val
    res['FareBucket'] = df[b_col].values if b_col else 'Y'
    res['sigma'] = sigma_arr
    res['STD_future'] = std_str
    res['qp_b2c_min'] = float(fare_arr[-1]) if k > 0 else anchor_fare
    res['sigma_bar'] = sigma_bar
    res['F_j'] = F_j
    res['protection_limit_yj'] = y_j
    res['ProtectionLimit'] = prot_lim
    res['sigma_3'] = sigma_factor * mu_2
    res['ProtectionLimit_3'] = y_2_list
    res['sigma_bar_3'] = np.array([sigma_factor * np.sum(mu_2[:j+1]) for j in range(k)])

    promo_fare = round(float(promo_ratio) * float(strategy_fare), 2)
    promo_row = {
        'sector': sec,
        'date': dep_date_str,
        'DOW': dow,
        'STD': std_str,
        'flight': flt_num,
        'NDO': ndo_val,
        'capacity': cap,
        'Total_Bookings': tot_bookings,
        'fare': promo_fare,
        'oal_b2c_min': oal_min if oal_min is not None else np.nan,
        'anchor_fare': anchor_fare,
        'strategy_fare': strategy_fare,
        'opening fare': opening_fare,
        'Promo_Flag': 1,
        'mu_bar_orig': 0.0,
        'mu': 0.0,
        'AU': au_promo_1,
        'P_eq': np.nan,
        'mu_bar_2': 0.0,
        'mu_2': 0.0,
        'AU_2': au_promo_2,
        'AU_Revised': au_revised_promo,
        'predicted_pickup': pred_pickup,
        'Predicted_Flown': pred_flown,
        'Predicted_pickup_Revised': pred_pickup_rev,
        'Predicted_Flown_Revised': pred_flown_rev,
        'LF_Forecast_Revised': round(lf_rev, 4),
        'AF_Forecast': round(af_rev, 2),
        'RASK_Forecast': round(rask_rev, 4),
        'Distance': dist,
        'RPKM': round(rpkm, 2),
        'ASKM': round(askm, 2),
        'Capped_Predicted_Flown_Revised': min(cap, pred_flown_rev),
        'A_cap': 0.0,
        'Current_Revenue': round(curr_rev, 2),
        'Expected_Revenue': 0.0,
        'Total_Expected_Revenue': round(total_expected_revenue, 2),
        'Predicted_Revenue': round(predicted_revenue, 2),
        'SnapshotDate': snap_date,
        'Tagging': tagging_val,
        'FareBucket': 'Promo',
        'sigma': 0.0,
        'STD_future': std_str,
        'qp_b2c_min': float(fare_arr[-1]) if k > 0 else anchor_fare,
        'sigma_bar': 0.0,
        'F_j': promo_fare,
        'protection_limit_yj': y_j[-1] if k > 0 else 0.0,
        'ProtectionLimit': prot_lim[-1] if k > 0 else 0.0,
        'sigma_3': 0.0,
        'ProtectionLimit_3': y_2_list[-1] if len(y_2_list) > 0 else (prot_lim[-1] if k > 0 else 0.0),
        'sigma_bar_3': 0.0
    }
    res = pd.concat([res, pd.DataFrame([promo_row])], ignore_index=True)

    return res[INVENTORY_NG_COLUMNS]


def compute_inventory_ng_df(
    df,
    pax_col='Final_pax_count',
    std_col='weighted_pax_std',
    mongo_client=None,
    mongo_db=None,
    grid_name='DEFAULT',
    market_list_df=None,
    logger=None
):
    """
    Executes compute_flight_inventory_ng grouped across flights with competition response grid,
    market fares from MongoDB, and market_list configurations (NG columns, carrier exclusion,
    connection flag/duration, promo flag, first RBD allocation).
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=INVENTORY_NG_COLUMNS)

    # 1. Load market_list if not provided
    if market_list_df is None:
        market_list_df = load_market_list(mongo_client=mongo_client, mongo_db=mongo_db, logger=logger)

    tax_helper = None
    if FareTaxHelper is not None:
        try:
            tax_helper = FareTaxHelper(mongo_master_db=mongo_db, db=mongo_db, logger=logger)
        except Exception as e:
            if logger:
                logger.warning(f"Could not initialize FareTaxHelper: {e}")

    # 2. Caches for comp grids and market fares
    grid_caches = {}
    mkt_fares_cache = {}

    def _get_grid_doc(g_name, ndo_band, plf_band):
        g_key = str(g_name).strip() if g_name else 'DEFAULT'
        if g_key not in grid_caches:
            grid_caches[g_key] = load_comp_response_grid(
                mongo_client=mongo_client,
                mongo_db=mongo_db,
                grid_name=g_key,
                logger=logger
            )
        return grid_caches[g_key].get((ndo_band, plf_band))

    def _get_market_fares(origin, destin, dep_date_str, conn_flag):
        if not (origin and destin and dep_date_str and (mongo_client is not None or mongo_db is not None)):
            return None
        cache_key = (origin, destin, dep_date_str, conn_flag)
        if cache_key not in mkt_fares_cache:
            mkt_fares_cache[cache_key] = fetch_market_fares_b2c(
                mongo_client=mongo_client,
                mongo_db=mongo_db,
                origin=origin,
                destin=destin,
                dep_date=dep_date_str,
                connection_fare_flag=conn_flag,
                logger=logger
            )
        return mkt_fares_cache[cache_key]

    sec_candidates = ['Sector', 'sector', 'Origin_Destination']
    dep_candidates = ['DepartureDate', 'DepartureDate_future', 'date', 'DepDate']
    flt_candidates = ['FlightNumber', 'FlightNumber_future', 'flight', 'FltNum']

    sec_col = next((c for c in sec_candidates if c in df.columns), None)
    dep_col = next((c for c in dep_candidates if c in df.columns), None)
    flt_col = next((c for c in flt_candidates if c in df.columns), None)

    group_cols = [c for c in [sec_col, dep_col, flt_col] if c is not None]

    def _process_flight_group(grp):
        sec_val = str(grp[sec_col].iloc[0]).strip().upper() if sec_col else ''
        origin, destin = None, None
        if '-' in sec_val:
            parts = sec_val.split('-')
            if len(parts) >= 2:
                origin, destin = parts[0][:3], parts[1][:3]
        elif '_' in sec_val:
            parts = sec_val.split('_')
            if len(parts) >= 2:
                origin, destin = parts[0][:3], parts[1][:3]
        elif len(sec_val) >= 6:
            origin, destin = sec_val[:3], sec_val[3:6]

        dep_val = grp[dep_col].iloc[0] if dep_col else None
        dep_date_str = str(pd.to_datetime(dep_val).date()) if pd.notna(dep_val) else None
        dow_val = grp['DOW'].iloc[0] if 'DOW' in grp.columns else (pd.to_datetime(dep_val).day_name() if pd.notna(dep_val) else None)

        flt_val = grp[flt_col].iloc[0] if flt_col else 0
        try:
            flt_num = int(float(flt_val)) if pd.notna(flt_val) else 0
        except (ValueError, TypeError):
            m = re.search(r'(\d+)', str(flt_val).strip())
            flt_num = int(m.group(1)) if m else 0

        ndo_col = next((c for c in ['future_flight_NDO', 'NDO', 'ndo', 'D_flight_NDO'] if c in grp.columns), None)
        ndo_val = grp[ndo_col].iloc[0] if ndo_col else 0

        cap_col = next((c for c in ['Capacity', 'capacity', 'Cap'] if c in grp.columns), None)
        cap_val = float(grp[cap_col].iloc[0]) if cap_col and pd.notna(grp[cap_col].iloc[0]) else 189.0

        bk_col = next((c for c in ['Current_Bookings', 'Total_Bookings', 'current_bookings', 'total_bookings'] if c in grp.columns), None)
        bk_val = float(grp[bk_col].iloc[0]) if bk_col and pd.notna(grp[bk_col].iloc[0]) else 0.0

        rev_col = next((c for c in ['Current_Revenue', 'current_revenue'] if c in grp.columns), None)
        curr_rev_val = float(grp[rev_col].iloc[0]) if rev_col and pd.notna(grp[rev_col].iloc[0]) else 0.0

        dist_col = next((c for c in ['Distance', 'distance'] if c in grp.columns), None)
        dist_val = float(grp[dist_col].iloc[0]) if dist_col and pd.notna(grp[dist_col].iloc[0]) else None

        # Match flight against market_list records
        matched_cfg = match_market_list_config(
            market_list_df,
            origin=origin,
            destin=destin,
            flight_num=flt_num,
            dep_date=dep_date_str,
            dow=dow_val
        )

        # 1. Buy up factor
        buy_up_val = matched_cfg.get('buy_up_factor_NG')
        try:
            buy_up_factor = float(buy_up_val) if buy_up_val is not None and pd.notna(buy_up_val) else 0.0
        except (ValueError, TypeError):
            buy_up_factor = 0.0

        # 2. Connection fare flag
        conn_flag_raw = matched_cfg.get('connection_fare_flag_NG', 0)
        conn_flag = 1 if is_flag_enabled(conn_flag_raw) else 0

        # 3. Additional connection duration (hours)
        add_conn_raw = matched_cfg.get('additional_connection_duration_NG', 4.0)
        try:
            add_conn_dur = float(add_conn_raw) if add_conn_raw is not None and pd.notna(add_conn_raw) else 4.0
        except (ValueError, TypeError):
            add_conn_dur = 4.0

        # 4. CompetitionResponseGrid_NG
        cfg_grid = matched_cfg.get('CompetitionResponseGrid_NG')
        grid_to_use = str(cfg_grid).strip() if cfg_grid and pd.notna(cfg_grid) and str(cfg_grid).strip() not in ('', 'nan', 'None') else grid_name

        # 5. competition_response_flag_NG
        comp_flag_raw = matched_cfg.get('competition_response_flag_NG')
        if comp_flag_raw is not None and pd.notna(comp_flag_raw) and not is_flag_enabled(comp_flag_raw):
            comp_enabled = False
        else:
            comp_enabled = True

        # 6. PromoFlag_NG
        promo_flag_raw = matched_cfg.get('PromoFlag_NG', 0)
        promo_flag = 1 if is_flag_enabled(promo_flag_raw) else 0

        # 7. Carrier and flight exclusions
        carr_excl = matched_cfg.get('CarrExlusionB2C')
        flight_excl = matched_cfg.get('flightExclusionB2C')

        # 8. FirstRBDAlloc
        first_rbd_raw = matched_cfg.get('FirstRBDAlloc')
        try:
            if first_rbd_raw is not None and pd.notna(first_rbd_raw):
                f_rbd = float(first_rbd_raw)
                first_rbd = f_rbd if f_rbd > 0 else float('inf')
            else:
                first_rbd = 20.0
        except (ValueError, TypeError):
            first_rbd = 20.0

        # 9. B2CBackstop
        bs_raw = matched_cfg.get('B2CBackstop', matched_cfg.get('b2c_backstop'))

        # 10. hardanchor, plfthreshold & tolerance
        ha_raw = matched_cfg.get('hardanchor', matched_cfg.get('hardAnchor', matched_cfg.get('hard_anchor_fare')))
        plf_thresh_raw = matched_cfg.get('plfthreshold', matched_cfg.get('plfThreshold', 95.0))
        try:
            plf_thresh = float(plf_thresh_raw) if plf_thresh_raw is not None and pd.notna(plf_thresh_raw) else 95.0
        except (ValueError, TypeError):
            plf_thresh = 95.0
        tol_raw = matched_cfg.get('B2CTolerance', matched_cfg.get('tolerance', matched_cfg.get('b2c_tolerance', matched_cfg.get('b2cTolerance'))))

        # Direct duration (minutes)
        dur_col = next((c for c in ['flight_duration', 'outbound_duration', 'Duration', 'duration'] if c in grp.columns), None)
        direct_dur_mins = None
        if dur_col and pd.notna(grp[dur_col].iloc[0]):
            direct_dur_mins = parse_time_to_minutes(grp[dur_col].iloc[0])
        if not direct_dur_mins and dist_val:
            direct_dur_mins = (dist_val / 550.0) * 60.0 + 30.0

        # Competition grid lookup
        booked_plf = (bk_val / cap_val * 100.0) if cap_val > 0 else 0.0
        ndo_band = get_ndo_band(ndo_val, mongo_db=mongo_db)
        plf_band = get_plf_band(booked_plf, mongo_db=mongo_db)
        grid_doc = _get_grid_doc(grid_to_use, ndo_band, plf_band)

        # Market fares lookup
        mkt_df = _get_market_fares(origin, destin, dep_date_str, conn_flag)

        return compute_flight_inventory_ng(
            grp,
            pax_col=pax_col,
            std_col=std_col,
            capacity=cap_val,
            current_bookings=bk_val,
            current_revenue=curr_rev_val,
            distance=dist_val,
            buy_up_factor=buy_up_factor,
            first_rbd=first_rbd,
            grid_doc=grid_doc,
            market_fares_df=mkt_df,
            carrier_exclusion=carr_excl,
            flight_exclusion=flight_excl,
            connection_fare_flag=conn_flag,
            additional_connection_duration=add_conn_dur,
            direct_duration_mins=direct_dur_mins,
            competition_response_enabled=comp_enabled,
            promo_flag=promo_flag,
            backstop=bs_raw,
            hardanchor=ha_raw,
            plf_threshold=plf_thresh,
            tolerance=tol_raw,
            tax_helper=tax_helper,
            origin=origin,
            destin=destin
        )

    if not group_cols:
        res = _process_flight_group(df)
        return res if not res.empty else pd.DataFrame(columns=INVENTORY_NG_COLUMNS)

    results = []
    for _, grp in df.groupby(group_cols, sort=False):
        res = _process_flight_group(grp)
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
                self.logger.info("Executing NextGen Inventory Optimization (50 columns, EMSR-b AU ladder, DEFAULT competition response)...")
                t_ng0 = time.time()
                mongo_db_for_ng = None
                mongo_client_for_ng = None
                try:
                    mongo_db_for_ng = self.get_mongo_db()
                    mongo_client_for_ng = getattr(self, '_mongo_client', None)
                except Exception as e:
                    self.logger.warning(f"Could not connect to MongoDB for market fares / comp response grid: {e}")

                inventory_ng_df = compute_inventory_ng_df(
                    fare_bucket_df,
                    mongo_client=mongo_client_for_ng,
                    mongo_db=mongo_db_for_ng,
                    grid_name='DEFAULT',
                    logger=self.logger
                )
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
