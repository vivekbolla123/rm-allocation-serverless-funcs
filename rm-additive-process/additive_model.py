"""
Additive Pickup Model - Lambda module (rm-additive-process)
===========================================================
Runs the additive pickup forecast for the SECTOR BIN handed to this Lambda by
rm-additive-prefetch. The model logic (fare buckets -> lean/peak tagging ->
NDO pivot -> flight matching -> mean/weighted prediction -> remaining-RBD
distribution -> S3 output) is unchanged from the original Glue job.

What changed vs the Glue job:
  * No module-level SSM / engine creation. All connections are INJECTED via
    `params` by lambda_function.py (rdEsbConn, dwconn, mongo, mongofares,
    mongo_master, s3).
  * Config lives in configsettings.py (infra) + additive_constants.py (model).
  * Entry point matches the shared module contract:
        Module(logger, params, overrides, context) ; Module.run() -> dict
  * Sectors come from overrides['sectors'] (the bin). TEST mode falls back to
    additive_constants.TEST_SECTORS.
  * Per-sector timing is written to the `additive_model_run_audit` collection
    (qp_dw_priaut) so the next prefetch can bin-pack from real durations.

Modules:
    1. Fare Bucket Calculation
    2. Historical Lean/Peak Tagging (Z-Score based)
    3. Future Lean/Peak Tagging (50th percentile)
    4. Flight Matching (Sector, DOW, Tagging, Time Band)
    5. Additive Pickup Prediction (Mean)
    6. Remaining RBD Distribution
    7. Market Fare Integration (RateGain OAL/QP)
    8. Inventory Optimization (Protection Limits & AU)
"""

import io
import json
import re
import time as _time
import traceback
from uuid import uuid4
from datetime import datetime, timedelta, time

import boto3
import pandas as pd
import numpy as np
from sqlalchemy import text
import logging
import os
import concurrent.futures
from scipy.stats import norm

import configsettings
from Constants import *   # noqa: F401,F403  (model + infra constants)

# Module logger — inherits the handler/root logger config in Lambda.
logger = logging.getLogger()

# =============================================================================
# DATABASE CONFIGURATION  (table names only — engines are injected)
# =============================================================================

class DatabaseConfig:
    # Read tables live in the ESB schema; the injected ESB engine may already
    # default to this schema, but we qualify explicitly as the Glue job did.
    FLOWN_DATA_TABLE   = f'{configsettings.ESB_READ_SCHEMA}.QP_DL_FLOWNDATAPAX'
    BOOKED_DATA_TABLE  = f'{configsettings.ESB_READ_SCHEMA}.QP_DL_BOOKEDDATAPAX'
    FLIGHT_MASTER_TABLE = f'{configsettings.ESB_READ_SCHEMA}.QP_DL_InventoryLeg'


# =============================================================================
# UTILITY FUNCTIONS
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
    month = date.month
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
    if len(tags) == 0:
        return None
    return tags.iloc[(len(tags) - 1) // 2]


# =============================================================================
# MAIN PROCESSOR CLASS
# =============================================================================

class AdditivePickupModelProcessor:
    """
    Processes the Additive Pickup Model pipeline:
      1. Fetch fare bucket configuration
      2. Compute historical lean/peak tagging
      3. Compute future lean/peak tagging
      4. Fetch flown + booked data, pivot by NDO
      5. Match future flights to historical flights
      6. Calculate mean predictions at fare bucket and flight level
      7. Distribute remaining RBD pax
      8. Integrate market fare data (OAL/QP)
      9. Compute inventory optimization (protection limits & AU)
      10. Save results to DW and S3
    """

    def __init__(self, logger, params, overrides, context):
        """Lambda module contract.

        Args:
            logger:    injected logger.
            params:    dict with injected connections/clients. Expected keys:
                       rdEsbConn (ESB SQLAlchemy engine), dwconn (DW engine),
                       mongo (qp_dw_rmalloc DB), mongofares (qp_dw_rmalloc DB),
                       mongo_master (qp_rm_master DB), s3 (boto3 S3 client).
            overrides: dict. Recognised keys:
                       mode ('PROD'|'TEST'), sectors (list — the bin),
                       run_id, batch_index, batch_total, sectors_expected,
                       force_backfill, refresh_spec, analyst.
            context:   Lambda context (unused; kept for contract parity).
        """
        self.logger = logger
        self.context = context
        overrides = overrides or {}

        # --- time anchors ---
        self.today = datetime.now()
        self.current_date = pd.to_datetime(self.today.date())
        self.prediction_limit = self.current_date + pd.Timedelta(days=PREDICTION_HORIZON_DAYS)
        self.one_year_back = self.current_date - pd.Timedelta(days=365)

        # =============================================================================
        # RUN MODE
        # =============================================================================
        # 'PROD' -> S3 output + audit writes. 'TEST' -> local CSV/parquet, no audit.
        # Overridable per-invocation via overrides['mode'].
        RUN_MODE = 'PROD'

        # Sector list used only in TEST mode (keeps a local run tiny).
        TEST_SECTORS = ['AUHBLR']
        TEST_OUTPUT_DIR = os.path.join(os.getcwd(), 'additive_model_output')


        # --- run mode ---
        self.mode = (overrides.get('mode') or RUN_MODE).upper()
        self.output_dir = TEST_OUTPUT_DIR
        if self.mode == 'TEST':
            os.makedirs(self.output_dir, exist_ok=True)
            logger.info(f"Running in TEST mode — output -> {self.output_dir}")

        # --- injected connections (NO SSM / engine creation here) ---
        self.esb_engine = params.get('rdEsbConn')
        self.s3_client = params.get('s3')
        # self.db_audit = params.get('mongo')          # qp_dw_priaut
        # self.db_fares = params.get('mongofares')     # qp_dw_rmalloc
        # self.db_master = params.get('mongo_master')  # qp_rm_master
        self.db = params['mongo']
        self.master_db = params.get('mongo_master', params['mongo'])
        self.s3_bucket = configsettings.ADDITIVE_MODEL_BUCKET
        self.s3_cache_prefix = f'{configsettings.ADDITIVE_MODEL_FILE_PATH}_cache'
        self.s3_output_prefix = f'{configsettings.ADDITIVE_MODEL_FILE_PATH}datasets'
        CACHE_DIR = os.path.join(os.getcwd(), 'additive_model_cache')
        self.local_cach_dir = CACHE_DIR if self.mode == 'TEST' else None

        # --- per-invocation overrides for backfill/refresh ---
        # (module-level constants stay the default; overrides win when present)
        global FORCE_BACKFILL, REFRESH_SPEC
        if 'force_backfill' in overrides:
            FORCE_BACKFILL = 'true' if overrides['force_backfill'] else ''
        if 'refresh_spec' in overrides:
            REFRESH_SPEC = overrides['refresh_spec'] or ''

        # --- sector bin + run metadata (from prefetch) ---
        self._overrides = overrides
        self.analyst = overrides.get('analyst', 'system')
        self.run_id = overrides.get('run_id') or str(uuid4())
        self.batch_index = overrides.get('batch_index')
        self.batch_total = overrides.get('batch_total')
        self.sectors_expected = overrides.get('sectors_expected')

        bin_sectors = overrides.get('sectors')
        if self.mode == 'TEST':
            self.active_sectors = list(TEST_SECTORS)
        elif bin_sectors:
            self.active_sectors = list(bin_sectors)
        else:
            self.active_sectors = []   # standalone full run -> fetch all active

        self._data_cache = {}

# =============================================================================
# Resolve the spec into a concrete list of 'YYYY-MM' strings
# =============================================================================

    def _resolve_refresh_months(self):
        """
        Turn REFRESH_MONTHS into a sorted list of 'YYYY-MM', clamped to the
        retained window (older months are dropped by retention anyway, so
        refetching them would be immediately pruned).
        """
        if not REFRESH_SPEC:
            return []

        month_start = self.current_date.replace(day=1)
        floor = min(month_start - pd.DateOffset(years=1),
                    month_start - pd.Timedelta(days=365))
        floor_m = floor.strftime('%Y-%m')
        this_m = month_start.strftime('%Y-%m')

        months = set()
        if REFRESH_SPEC.isdigit():
            n = int(REFRESH_SPEC)
            m = month_start
            for _ in range(n):
                m = m - pd.DateOffset(months=1)
                months.add(m.strftime('%Y-%m'))
        else:
            for tok in REFRESH_SPEC.split(','):
                tok = tok.strip()
                if tok:
                    months.add(tok)

        # keep only months inside the retained window and not the current month
        # (the current month is already handled by the daily incremental)
        valid = sorted(m for m in months if floor_m <= m < this_m)
        dropped = sorted(months - set(valid))
        if dropped:
            logger.warning(f"[Refresh] Ignoring out-of-window months: {dropped}")
        return valid


# =============================================================================
# The refresh itself — refetch whole months, rewrite only those partitions
# =============================================================================

    def _refresh_months(self, engine, months):
        """
        Refetch `months` (list of 'YYYY-MM') from FLOWNDATAPAX for the batch's
        sectors, and rewrite ONLY those month partitions of hist_flown.

        Whole-month refetch (not per-date) because a cancellation can touch any
        date in the month and refetching the month is one clean query per span.
        Tagging for those months is rebuilt on the next month-start re-tag, or
        immediately if you pass rebuild_tags=True (rare — only if a refreshed
        month must re-tag before the 1st).
        """
        if not months:
            return

        logger.info(f"[Refresh] Refetching {len(months)} month(s) "
                    f"{months[0]}..{months[-1]} for {len(self.active_sectors)} sectors.")

        # Contiguous or not, fetch each month as its own date span.
        conds = " OR ".join(
            f"(qdb.Origin = '{s[:3]}' AND qdb.Destination = '{s[3:]}')"
            for s in sorted(self.active_sectors))

        refetched = []
        for m in months:
            first = pd.to_datetime(m + '-01')
            last = (first + pd.DateOffset(months=1)) - pd.Timedelta(days=1)
            q = self._build_flown_query(
                f"qdb.DepartureDate BETWEEN '{first.date()}' AND '{last.date()}'", conds)
            df = self._run_sql(engine, q)
            if not df.empty:
                df['DepartureDate'] = pd.to_datetime(df['DepartureDate'])
                df['DOW'] = df['DepartureDate'].dt.day_name()
                refetched.append(df)
            logger.info(f"  [Refresh] {m}: {len(df)} rows.")

        if not refetched:
            logger.info("[Refresh] Nothing returned — partitions left as-is.")
            return

        fresh = pd.concat(refetched, ignore_index=True)

        # Rewrite exactly the refreshed month partitions. Because the fetch is
        # authoritative for the whole month, we overwrite rather than merge —
        # this is what actually removes the cancelled PNRs from the cache.
        self._write_sector_cache('hist_flown', fresh, months=set(months))
        logger.info(f"[Refresh] Rewrote {len(months)} hist_flown partitions "
                    f"for {fresh['Sector'].nunique()} sectors.")

    # =========================================================================
    # STEP 0: FETCH ACTIVE SECTORS
    # =========================================================================

    def _fetch_active_sectors(self):
        """Resolve the sectors this invocation will process.

        Normal path: sectors already set from overrides['sectors'] (the bin) or
        TEST_SECTORS — nothing to fetch. Only a standalone full run (no bin,
        PROD) falls through to reading qp_rm_master.active_sectors.
        """
        if self.mode == 'TEST':
            logger.info(f"TEST mode — sectors: {self.active_sectors}")
            return self.active_sectors

        if self.active_sectors:
            logger.info(f"Processing assigned bin of {len(self.active_sectors)} sector(s).")
            return self.active_sectors

        logger.info("No bin supplied — fetching ALL active sectors (standalone run).")
        try:
            docs = list(self.master_db.active_sectors.find({}, {"_id": 0, "sector": 1}))
            if not docs:
                raise RuntimeError("No active sectors found in qp_rm_master.active_sectors")
            self.active_sectors = [d['sector'] for d in docs if d.get('sector')]
            logger.info(f"Loaded {len(self.active_sectors)} active sectors.")
            return self.active_sectors
        except Exception as e:
            logger.error(f"Error fetching active sectors: {e}")
            logger.error(traceback.format_exc())
            raise

    def _fetch_tax_master_data(self):
        """Fetch domestic and international tax master data + pricing constants from MongoDB."""
        logger.info("Fetching tax master data (domestic + international + pricing constants)...")
        try:

            # --- Domestic tax master: keyed by origin airport code ---
            dom_docs = list(self.master_db.tax_mast_dom.find({}, {"_id": 0}))
            dom_map = {}
            for doc in dom_docs:
                code = str(doc.get('Code', '')).strip()
                if code:
                    dom_map[code] = {
                        'GST':    float(doc.get('GST', 0) or 0) / 100,
                        'RCS':    float(doc.get('RCS', 0) or 0),
                        'CUTE':   float(doc.get('CUTE', 0) or 0),
                        'ASF':    float(doc.get('ASF', 0) or 0),
                        'PSF':    float(doc.get('PSF', 0) or 0),
                        'UDF_DF': float(doc.get('UDF_DF', 0) or 0),
                    }
            self._data_cache['tax_mast_dom'] = dom_map
            logger.info(f"Fetched {len(dom_map)} domestic tax master records.")

            # --- International tax master: keyed by sector string e.g. "BOMKWI" ---
            intl_docs = list(self.master_db.tax_mast_intl.find({}, {"_id": 0}))
            intl_map = {}
            for doc in intl_docs:
                sector = str(doc.get('Sectors', '')).strip()
                if sector:
                    intl_map[sector] = {
                        'GST':     float(doc.get('GST', 0) or 0),
                        'CUTE':    float(doc.get('CUTE', 0) or 0),
                        'ASF':     float(doc.get('ASF', 0) or 0),
                        'PSF_ADF': float(doc.get('PSF_ADF', 0) or 0),
                        'UDF':     float(doc.get('UDF', 0) or 0),
                        'PSC_PZ':  float(doc.get('PSC_PZ', 0) or 0),
                        'RCS_CUTE':float(doc.get('RCS_CUTE', 0) or 0),
                        'RCS':     float(doc.get('RCS', 0) or 0),
                        'Tax1':    float(doc.get('Tax1', 0) or 0),
                        'Tax2':    float(doc.get('Tax2', 0) or 0),
                        'Tax3':    float(doc.get('Tax3', 0) or 0),
                        'Tax4':    float(doc.get('Tax4', 0) or 0),
                        'Tax5':    float(doc.get('Tax5', 0) or 0),
                    }
            self._data_cache['tax_mast_intl'] = intl_map
            logger.info(f"Fetched {len(intl_map)} international tax master records.")

            # --- Pricing constants (sector-level exclusion rules) ---
            # Fetch all three parameters in one query
            param_keys = ['PRICING_DUF_SECTORS', 'PRICING_GST_SECTORS', 'PRICING_RCS_SECTORS']
            params_cursor = self.db.rm_parameter_values.find(
                {"parameterKey": {"$in": param_keys}},
                {"parameterKey": 1, "parameterValue": 1, "_id": 0}
            )
            
            pricing_constants = {
                'PRICING_DUF_SECTORS': {},   # dict: destination -> DUF amount
                'PRICING_GST_SECTORS': [],   # list: airport codes exempt from GST
                'PRICING_RCS_SECTORS': [],   # list: destination codes where RCS = 0
            }
            
            for doc in params_cursor:
                key = doc['parameterKey']
                try:
                    pricing_constants[key] = json.loads(doc['parameterValue'])
                except (json.JSONDecodeError, TypeError) as e:
                    logger.warning(f"Could not parse pricing constant {key}: {e}")
            
            self._data_cache['pricing_duf_sectors'] = pricing_constants['PRICING_DUF_SECTORS']
            self._data_cache['pricing_gst_sectors'] = set(pricing_constants['PRICING_GST_SECTORS'])
            self._data_cache['pricing_rcs_sectors'] = set(pricing_constants['PRICING_RCS_SECTORS'])
            
            logger.info(
                f"Loaded pricing constants — "
                f"DUF sectors: {len(self._data_cache['pricing_duf_sectors'])}, "
                f"GST-exempt: {len(self._data_cache['pricing_gst_sectors'])}, "
                f"RCS-zero: {len(self._data_cache['pricing_rcs_sectors'])}"
            )

        except Exception as e:
            logger.error(f"Error fetching tax master data: {e}")
            logger.error(traceback.format_exc())
            self._data_cache['tax_mast_dom'] = {}
            self._data_cache['tax_mast_intl'] = {}
            self._data_cache['pricing_duf_sectors'] = {}
            self._data_cache['pricing_gst_sectors'] = set()
            self._data_cache['pricing_rcs_sectors'] = set()
       

    # =========================================================================
    # CONSOLIDATED DATA FETCHING (incremental with local parquet cache)
    # =========================================================================
    def _get_s3(self):
        return self.s3_client

    def _get_esb_engine(self):
        return self.esb_engine

    def _fetch_all_data(self):
        """
        Incremental data fetching strategy:

        HISTORICAL DATA:
          - First run: full backfill from FLOWNDATAPAX, saved to parquet cache
          - Subsequent runs: load cache, fetch yesterday only, append, prune

        STD LOOKUP:
          - Fetched for any new InventoryLegIDs not already in cache
          - Keyed by leg ID over PAST departures — disjoint from the forward scan

        FORWARD DATA (single InventoryLeg scan, two outputs):
          - `future_flight_master` : every SCHEDULED flight, booked or not
          - `forward_raw_df`       : booking-level rows for the NDO model
          Both come from one query. InventoryLeg is the driving table of the
          LEFT JOIN, so every scheduled flight is already in the result set;
          querying it a second time for the same date range would be pure
          duplication.
        """
        # TEST writes the parquet cache locally; PROD caches to S3, and the
        # Lambda working dir is read-only, so only create the local dir in TEST.
        if self.mode == 'TEST':
            os.makedirs(self.local_cach_dir, exist_ok=True)
        engine = self._get_esb_engine()

        # --- Date boundaries (real DATE/DATETIME columns -> quoted literals) ---
        today = self.today.date()
        yesterday = today - timedelta(days=1)
        month_start = self.current_date.replace(day=1)
        tag_floor = month_start - pd.DateOffset(years=1)
        future_tag_floor = month_start - pd.Timedelta(days=365)
        hist_start = min(tag_floor, future_tag_floor).date()

        forward_end = today + timedelta(days=PREDICTION_HORIZON_DAYS)
        booking_cutoff = today - timedelta(days=545)

        logger.info(f"Date windows — hist: {hist_start}..{yesterday}, "
                    f"forward: {today}..{forward_end}")

        fm_station_conditions = " OR ".join([
            f"(fm.DepartureStation = '{s[:3]}' AND fm.ArrivalStation = '{s[3:]}')"
            for s in self.active_sectors
        ])

        # =====================================================================
        # QUERY 1: HISTORICAL FLOWN DATA (incremental, sector-aware cache)
        # =====================================================================
        hist_df = self._fetch_historical_incremental(engine, hist_start, yesterday)
        self._data_cache['hist_raw_df'] = hist_df

        # =====================================================================
        # QUERY 2: STD LOOKUP (incremental — only new leg IDs, PAST departures)
        # =====================================================================
        self._fetch_std_incremental(engine, hist_df)

        # =====================================================================
        # QUERY 3: FORWARD DATA (always fresh — future bookings change daily)
        # =====================================================================
        logger.info(f"[Query 3] Fetching forward data: {today} to {forward_end}...")

        forward_query = f"""
        SELECT
            fm.InventoryLegID,
            CONCAT(fm.DepartureStation, fm.ArrivalStation) AS Sector,
            fm.STD AS STD_dt,
            fm.FlightNumber,
            fm.AdjustedCapacity as Capacity,
            fm.DepartureDate AS DepartDate_raw,
            qdb.DepartureDate,
            qdb.PNR,
            qdb.RBD,
            qdb.FareBasis AS FareBasisRaw,
            qdb.BookingDate,
            DATEDIFF(qdb.DepartureDate, qdb.BookingDate) AS NDO,
            CASE
                WHEN SUBSTRING(qdb.FareBasis, 5, 1) = 'T'
                THEN SUM(qdb.FB_Net + qdb.YQ_Pro + qdb.RCS + qdb.CUTE)
                ELSE SUM(qdb.DiscountedBaseFare_Pro + qdb.YQ_Pro + qdb.RCS + qdb.CUTE)
            END AS Fare,
            SUM(CASE
                WHEN qdb.FareBasis NOT LIKE '%SOL%' THEN 1
                ELSE 0
            END) AS `Pax Count`
        FROM {DatabaseConfig.FLIGHT_MASTER_TABLE} fm
        LEFT JOIN {DatabaseConfig.BOOKED_DATA_TABLE} qdb
            ON qdb.InventoryLegID = fm.InventoryLegID
            AND qdb.BookingDate >= '{booking_cutoff}' AND qdb.BookingDate < '{today}'
        WHERE fm.DepartureDate BETWEEN '{today}' AND '{forward_end}'
            AND ({fm_station_conditions})
            AND fm.FlightNumber < 6000
            AND fm.CarrierCode = 'QP'
            AND fm.Status <> 2
            AND fm.AdjustedCapacity > 0
        GROUP BY fm.InventoryLegID, fm.DepartureStation, fm.ArrivalStation, fm.STD,
                 fm.FlightNumber, fm.Capacity, fm.DepartureDate,
                 qdb.DepartureDate, qdb.PNR, qdb.RBD,
                 qdb.FareBasis, qdb.BookingDate
        """

        try:
            with engine.connect() as conn:
                chunks = pd.read_sql(text(forward_query), conn, chunksize=50000)
                forward_df = pd.concat([chunk for chunk in chunks], ignore_index=True)
            logger.info(f"[Query 3] Done — {len(forward_df)} records.")
        except Exception as e:
            logger.error(f"Error fetching forward data: {e}")
            logger.error(traceback.format_exc())
            raise

        # ---------------------------------------------------------------------
        # Post-processing
        # ---------------------------------------------------------------------
        # STD is a DATETIME — derive datetime.time directly.
        forward_df['STD'] = pd.to_datetime(forward_df['STD_dt']).dt.time
        forward_df.drop(columns=['STD_dt'], inplace=True, errors='ignore')

        # Flights with no bookings have qdb.DepartureDate NULL — fall back to fm's.
        if 'DepartDate_raw' in forward_df.columns:
            forward_df['DepartureDate'] = forward_df['DepartureDate'].fillna(
                forward_df['DepartDate_raw']
            )
            forward_df.drop(columns=['DepartDate_raw'], inplace=True, errors='ignore')
        forward_df['DepartureDate'] = pd.to_datetime(forward_df['DepartureDate'])
        forward_df['DOW'] = forward_df['DepartureDate'].dt.day_name()

        forward_df['FlightNumber'] = pd.to_numeric(forward_df['FlightNumber'], errors='coerce')
        forward_df = forward_df.dropna(subset=['FlightNumber'])
        forward_df['FlightNumber'] = forward_df['FlightNumber'].astype(int)
        fm_df = forward_df[['Sector', 'DepartureDate', 'STD',
                            'FlightNumber', 'Capacity']].copy()
        fm_df = fm_df.drop_duplicates(
            subset=['Sector', 'DepartureDate', 'STD', 'FlightNumber'], keep='first'
        )
        self._data_cache['future_flight_master'] = fm_df
        logger.info(f"[Query 3] Flight universe: {len(fm_df)} scheduled flights across "
                    f"{fm_df['Sector'].nunique()} sectors.")

        # --- NDO restriction applies to the BOOKING rows only ---
        valid_ndo = forward_df['NDO'].isna() | (
            (forward_df['NDO'] >= 0) & (forward_df['NDO'] <= 180)
        )
        forward_df = forward_df[valid_ndo]

        # Diagnostic: sectors with no future flights at all
        fwd_sectors = set(fm_df['Sector'].unique()) if not fm_df.empty else set()
        missing_sectors = set(self.active_sectors) - fwd_sectors
        if missing_sectors:
            logger.warning(f"[Query 3] {len(missing_sectors)} sectors have NO future flights "
                           f"in flight_master: {sorted(missing_sectors)}")
        logger.info(f"[Query 3] Sectors with forward data: {sorted(fwd_sectors)}")

        self._data_cache['forward_raw_df'] = forward_df

    def _build_flown_query(self, date_predicate, sector_conditions):
        """Single source of truth for the flown-data SELECT (backfill + incremental)."""
        return f"""
        SELECT
            CONCAT(qdb.Origin, qdb.Destination) AS Sector,
            qdb.DepartureDate,
            DATEDIFF(qdb.DepartureDate, qdb.BookingDate) AS NDO,
            qdb.FlightNumber,
            qdb.InventoryLegID,
            qdb.PNR,
            qdb.RBD,
            qdb.FareBasis AS FareBasisRaw,
            CASE
                WHEN SUBSTRING(qdb.FareBasis, 5, 1) = 'T'
                THEN SUM(qdb.FB_Net + qdb.YQ_Pro + qdb.RCS + qdb.CUTE)
                ELSE SUM(qdb.DiscountedBaseFare_Pro + qdb.YQ_Pro + qdb.RCS + qdb.CUTE)
            END AS Fare,
            SUM(CASE WHEN qdb.FareBasis NOT LIKE '%SOL%' THEN 1 ELSE 0 END) AS `Pax Count`,
            qdb.AdjustedCapacity AS Capacity,
            qdb.Distance
        FROM {DatabaseConfig.FLOWN_DATA_TABLE} qdb
        WHERE {date_predicate}
            AND ({sector_conditions})
            AND qdb.AdjustedCapacity > 0
            AND FlightNumber < 6000
            AND qdb.BookingDate >= DATE_SUB(qdb.DepartureDate, INTERVAL 365 DAY)
            AND qdb.BookingDate <= qdb.DepartureDate
        GROUP BY qdb.Origin, qdb.Destination, qdb.DepartureDate,
                 qdb.BookingDate, qdb.FlightNumber, qdb.InventoryLegID,
                 qdb.PNR, qdb.RBD, qdb.FareBasis,
                 qdb.AdjustedCapacity, qdb.Distance
        """

    def _fetch_historical_incremental(self, engine, hist_start, yesterday):
        cached_df = pd.DataFrame() if FORCE_BACKFILL else \
            self._read_sector_cache('hist_flown', self.active_sectors)

        cached_sectors = set(cached_df['Sector'].unique()) if not cached_df.empty else set()
        new_sectors = set(self.active_sectors) - cached_sectors
        existing_sectors = set(self.active_sectors) & cached_sectors
        logger.info(f"  Batch: {len(self.active_sectors)} sectors "
                    f"(cached {len(existing_sectors)}, new {len(new_sectors)})")

        touched_months = set()          # (implicit) — track which months to rewrite
        new_dfs = []

        # --- backfill new sectors: full window ---
        if new_sectors:
            conds = " OR ".join(
                f"(qdb.Origin = '{s[:3]}' AND qdb.Destination = '{s[3:]}')"
                for s in sorted(new_sectors))
            logger.info(f"  Backfilling {len(new_sectors)} new sectors...")
            q = self._build_flown_query(
                f"qdb.DepartureDate BETWEEN '{hist_start}' AND '{yesterday}'", conds)
            df = self._run_sql(engine, q)
            logger.info(f"[Query END] {len(df)} rows")
            df['DepartureDate'] = pd.to_datetime(df['DepartureDate'])
            #TODO check for duplication
            new_dfs.append(df)
            touched_months = None       # None => write ALL months for these sectors

        # --- incremental: yesterday only, for cached sectors ---
        if existing_sectors:
            conds = " OR ".join(
                f"(qdb.Origin = '{s[:3]}' AND qdb.Destination = '{s[3:]}')"
                for s in sorted(existing_sectors))
            logger.info(f"  Incremental (yesterday) for {len(existing_sectors)} sectors...")
            q = self._build_flown_query(
                f"qdb.DepartureDate BETWEEN '{yesterday}' AND '{yesterday}'", conds)
            df = self._run_sql(engine, q)
            df['DepartureDate'] = pd.to_datetime(df['DepartureDate'])
            new_dfs.append(df)
            if touched_months is not None:
                touched_months.add(self._month_key(yesterday))
            # drop yesterday from the cached copy so the rewrite is clean
            y_month = self._month_key(yesterday)
            cached_df = cached_df[
                ~((cached_df['DepartureDate'] == pd.to_datetime(yesterday)))]
#TODO: Chck dupication for all_new_df
        if new_dfs:
            all_new = pd.concat(new_dfs, ignore_index=True)
            all_new['DepartureDate'] = pd.to_datetime(all_new['DepartureDate'])
            all_new['DOW'] = all_new['DepartureDate'].dt.day_name()
            hist_df = pd.concat([cached_df, all_new], ignore_index=True)
        else:
            hist_df = cached_df

        # --- retention: drop whole partitions strictly below the floor month ---
        month_start = self.current_date.replace(day=1)
        cutoff = min(month_start - pd.DateOffset(years=1),
                     month_start - pd.Timedelta(days=365))
        cutoff_month = cutoff.strftime('%Y-%m')

        before = len(hist_df)
        hist_df = hist_df[hist_df['DepartureDate'] >= cutoff]
        if len(hist_df) != before:
            # physically remove now-empty old partitions
            for s in self.active_sectors:
                for m in self._list_cache_months('hist_flown', s):
                    if m < cutoff_month:
                        self._delete_cache_partition('hist_flown', s, m)
            # the floor month itself may have lost some rows -> rewrite it
            if touched_months is not None:
                touched_months.add(cutoff_month)

        # --- write only the changed partitions ---
        # backfill (touched_months is None) rewrites every month present;
        # incremental rewrites just the month(s) in touched_months.
        self._write_sector_cache('hist_flown', hist_df, months=touched_months)

        logger.info(f"[Query 1] {len(hist_df)} rows, {hist_df['Sector'].nunique()} sectors "
                    f"(from {cutoff.date()}).")
        logger.info(f"[hist_flown] {hist_df['DepartureDate'].min().date()}..{hist_df['DepartureDate'].max().date()}")
        return hist_df


    def _fetch_std_incremental(self, engine, hist_df):
        """
        Fetch STD per InventoryLegID for legs not already cached, then merge STD
        into hist_df and store it on _data_cache['hist_raw_df'].

        STD cache is SECTOR-PARTITIONED to avoid write races when multiple
        parallel workers (Glue or Lambda) run concurrently:
            _cache/std_lookup/sector=BOMDEL.parquet
            _cache/std_lookup/sector=DELBOM.parquet
        Each worker only reads/writes its own sectors' files.
        """
        logger.info("[Query 2] Fetching STD lookup (incremental, sector-partitioned)...")

        # --- Read existing STD cache for this batch's sectors only ---
        std_frames = []
        for sector in self.active_sectors:
            cached = self._store_read(f'std_lookup/sector={sector}.parquet')
            if cached is not None and not cached.empty:
                cached['Sector'] = sector
                std_frames.append(cached)

        std_cache = pd.concat(std_frames, ignore_index=True) if std_frames else \
            pd.DataFrame(columns=['InventoryLegID', 'STD', 'Sector'])

        all_ids = set(hist_df['InventoryLegID'].unique())
        cached_ids = (set(std_cache['InventoryLegID'].unique())
                      if not std_cache.empty else set())
        missing = list(all_ids - cached_ids)

        if missing:
            logger.info(f"  Need STD for {len(missing)} new legs "
                        f"(cache has {len(cached_ids)}).")
            new_dfs = []
            for i in range(0, len(missing), 5000):
                ids_str = ", ".join(str(int(x)) for x in missing[i:i + 5000])
                q = f"""SELECT InventoryLegID, STD,
                        CONCAT(DepartureStation, ArrivalStation) AS Sector
                        FROM {DatabaseConfig.FLIGHT_MASTER_TABLE}
                        WHERE InventoryLegID IN ({ids_str})"""
                with engine.connect() as conn:
                    new_dfs.append(pd.read_sql(text(q), conn))

            if new_dfs:
                new_std = pd.concat(new_dfs, ignore_index=True)
                std_cache = pd.concat([std_cache, new_std], ignore_index=True)
                std_cache.drop_duplicates(subset=['InventoryLegID'],
                                          keep='last', inplace=True)

                # Write back per-sector (only this batch's sectors)
                for sector in self.active_sectors:
                    sector_std = std_cache[std_cache['Sector'] == sector]
                    if not sector_std.empty:
                        self._store_write(
                            sector_std[['InventoryLegID', 'STD']],
                            f'std_lookup/sector={sector}.parquet')
                logger.info(f"  STD cache updated: {len(std_cache)} entries "
                            f"across {std_cache['Sector'].nunique()} sectors.")
        else:
            logger.info(f"  All {len(all_ids)} leg IDs found in STD cache.")

        # Merge STD into hist_df (drop Sector from std_cache to avoid collision)
        merged = hist_df.merge(
            std_cache[['InventoryLegID', 'STD']].drop_duplicates(subset=['InventoryLegID']),
            on='InventoryLegID', how='left')
        merged['STD'] = pd.to_datetime(merged['STD']).dt.time
        merged.drop(columns=['InventoryLegID'], inplace=True)

        unresolved = int(merged['STD'].isna().sum())
        if unresolved:
            logger.warning(f"  {unresolved} rows have no STD after merge.")

        # --- Flight count summary per sector (unique InventoryLegIDs = unique flights) ---
        sector_flight_counts = std_cache.groupby('Sector')['InventoryLegID'].nunique()
        for sector in self.active_sectors:
            count = int(sector_flight_counts.get(sector, 0))
            logger.info(f"[STD] {sector}: {count} unique flights in full historical range")

        self._data_cache['hist_raw_df'] = merged

    def _sector_other_taxes(self, sector):
        """
        Pass-through (non-airline) taxes and the applicable GST rate for a sector.
        These are exactly the components to strip when going all-in -> airline comp.
        """
        sector = str(sector).strip()
        if len(sector) < 6:
            return 0.0, 0.0, False

        origin, destination = sector[:3], sector[3:6]

        duf_sectors = self._data_cache.get('pricing_duf_sectors', {})
        gst_sectors = self._data_cache.get('pricing_gst_sectors', set())
        intl_map = self._data_cache.get('tax_mast_intl', {})
        dom_map = self._data_cache.get('tax_mast_dom', {})

        # A sector present in the international master is international.
        is_domestic = sector not in intl_map

        if is_domestic:
            tax_row = dom_map.get(origin)
            if not tax_row:
                return 0.0, 0.0, False

            gst_rate = tax_row['GST']  # already /100 at fetch time
            if origin in gst_sectors or destination in gst_sectors:
                gst_rate = 0.0

            duf = float(duf_sectors[destination]) if destination in duf_sectors else 0.0
            other_taxes = tax_row['ASF'] + tax_row['PSF'] + tax_row['UDF_DF'] + duf
            return other_taxes, gst_rate, True

        tax_row = intl_map.get(sector)
        if not tax_row:
            return 0.0, 0.0, False

        gst_rate = tax_row['GST'] / 100  # stored raw for intl
        if origin in gst_sectors or destination in gst_sectors:
            gst_rate = 0.0

        other_taxes = (
            tax_row['ASF'] + tax_row['PSF_ADF'] + tax_row['UDF'] + tax_row['PSC_PZ'] +
            tax_row['Tax1'] + tax_row['Tax2'] + tax_row['Tax3'] +
            tax_row['Tax4'] + tax_row['Tax5']
        )
        return other_taxes, gst_rate, True
    
    def _fares_to_airline_component(self, fares_df, fare_col='TOTAL'):
        """
        Convert all-in fares to airline component (base + RCS + CUTE + YQ):

            airline_comp = (allin - other_taxes) / (1 + gst_rate)

        Sectors with no tax master row are dropped — a bucket ladder built on an
        unconverted all-in fare would be silently wrong, which is worse than a gap.
        """
        df = fares_df.copy()

        lookup = {}
        unresolved = []
        for sector in df['SECTOR'].unique():
            other_taxes, gst_rate, ok = self._sector_other_taxes(sector)
            if ok:
                lookup[sector] = (other_taxes, gst_rate)
            else:
                unresolved.append(sector)

        if unresolved:
            logger.warning(f"No tax master entry for {len(unresolved)} sectors — "
                        f"excluded from fare buckets: {sorted(unresolved)[:10]}"
                        f"{' ...' if len(unresolved) > 10 else ''}")
            df = df[~df['SECTOR'].isin(unresolved)]

        if df.empty:
            return df

        df['_other_taxes'] = df['SECTOR'].map(lambda s: lookup[s][0])
        df['_gst_rate'] = df['SECTOR'].map(lambda s: lookup[s][1])

        df['AirlineComponent'] = (df[fare_col] - df['_other_taxes']) / (1 + df['_gst_rate'])

        # A non-positive airline component means the all-in fare was below its own
        # pass-through taxes — bad data, not a real fare.
        bad = df['AirlineComponent'] <= 0
        if bad.any():
            logger.warning(f"Dropping {int(bad.sum())} fare rows with non-positive "
                        f"airline component (all-in below pass-through taxes).")
            df = df[~bad]

        df.drop(columns=['_other_taxes', '_gst_rate'], inplace=True)

        logger.info(f"Converted {len(df)} fares to airline component "
                    f"across {df['SECTOR'].nunique()} sectors.")
        return df

    def _derive_flown_data(self):
        """
        Derive the flown DataFrame (for NDO pickup model) from cached historical data.
        Equivalent output to the old _fetch_flown_data().
        """
        logger.info("Deriving flown data from cached historical fetch...")
        df = self._data_cache['hist_raw_df'].copy()
        df = df[(df['NDO'] >= 0) & (df['NDO'] <= 180)]
        # Keep columns needed for NDO model
        flown_df = df[['Sector', 'DepartureDate', 'DOW', 'NDO', 'FlightNumber',
                       'STD', 'PNR', 'RBD', 'Fare', 'Pax Count']].copy()
        logger.info(f"Flown data derived: {len(flown_df)} records.")
        return flown_df

    def _derive_lean_peak_source(self):
        """
        Flight-level revenue/capacity/distance for RASK tagging.

        Returns the FULL fetched span (no date clip). Historical tagging clips to
        its rolling 365-day window; future tagging clips to its prev-month window.
        Clipping here would narrow the frame for whichever consumer needs the
        wider window.
        """
        df = self._data_cache['hist_raw_df'].copy()

        # SOL exclusion
        df = df[~df['FareBasisRaw'].str.contains('SOL', na=False)]
        df['DepartureDate'] = pd.to_datetime(df['DepartureDate'])

        lean_peak_df = df.groupby(
            ['Sector', 'DepartureDate', 'FlightNumber', 'STD']
        ).agg({'Fare': 'sum', 'Capacity': 'max', 'Distance': 'first'}).reset_index()

        lean_peak_df.rename(columns={'STD': 'STD_time', 'Fare': 'Revenue'}, inplace=True)

        logger.info(f"Lean/peak source (unclipped): {len(lean_peak_df)} rows, "
                    f"{lean_peak_df['DepartureDate'].min().date()}.."
                    f"{lean_peak_df['DepartureDate'].max().date()}")
        return lean_peak_df

    def _derive_booked_data(self):
        """
        Derive forward booked pax data (for NDO pickup model) from cached forward data.
        Only includes rows where BookingDate is not null (i.e. actual bookings exist).
        Equivalent output to old _fetch_booked_data().
        """
        logger.info("Deriving booked data from cached forward fetch...")
        df = self._data_cache['forward_raw_df'].copy()

        # Filter: only rows with actual bookings (PNR not null)
        df = df[df['PNR'].notna() & (df['Pax Count'] > 0)]

        booked_df = df[['Sector', 'DepartureDate', 'DOW', 'NDO', 'FlightNumber',
                        'STD', 'PNR', 'RBD', 'Fare', 'Pax Count']].copy()
        logger.info(f"Booked data derived: {len(booked_df)} records.")
        return booked_df

    def _derive_future_flights(self):
        """
        Future flights = the SCHEDULE, left-merged with whatever bookings exist.
        Flights with no bookings yet get Total Bookings = 0 rather than dropping
        out of the forecast.
        """
        logger.info("Deriving future flights (schedule left-merged with bookings)...")

        fm_df = self._data_cache['future_flight_master'].copy()
        df = self._data_cache['forward_raw_df']

        merge_keys = ['Sector', 'DepartureDate', 'STD', 'FlightNumber']

        if df.empty:
            bookings = pd.DataFrame(columns=merge_keys + ['Pax Count'])
        else:
            bookings = df.groupby(merge_keys, dropna=False).agg(
                {'Pax Count': 'sum'}
            ).reset_index()

        future_df = fm_df.merge(bookings, on=merge_keys, how='left')
        future_df.rename(columns={'Pax Count': 'Total Bookings'}, inplace=True)
        future_df['Total Bookings'] = future_df['Total Bookings'].fillna(0).astype(int)

        zero_booking = int((future_df['Total Bookings'] == 0).sum())
        logger.info(f"Future flights derived: {len(future_df)} flights "
                    f"({zero_booking} with no bookings yet — retained, not dropped).")
        return future_df

    # =========================================================================
    # STEP 1: COMPUTE FARE BUCKETS (from qp_dw_rmalloc.fares via GP logic)
    # =========================================================================

    def _fetch_fare_buckets(self):
        """
        Fetch QP fare data from MongoDB (qp_dw_rmalloc.fares), convert the all-in
        TOTAL to airline component, then compute per-sector GP fare buckets.

        Returns DataFrame: Sector, FareBasis (range string e.g. '3000-4000' / '25088+')
        """
        logger.info("Computing fare buckets from qp_dw_rmalloc.fares (GP logic)...")
        try:
            docs = list(self.db.fares.find(
                {
                    "SECTOR": {"$in": self.active_sectors},
                    "TOTAL": {"$ne": 1},
                    "RBD": {"$in": REQUIRED_RBDS}
                },
                {"_id": 0, "SECTOR": 1, "RBD": 1, "TOTAL": 1}
            ))

            if not docs:
                logger.error("No fare data found in qp_dw_rmalloc.fares.")
                raise RuntimeError("Fare data missing from MongoDB")

            fares_df = pd.DataFrame(docs)
            fares_df['TOTAL'] = pd.to_numeric(fares_df['TOTAL'], errors='coerce')
            fares_df = fares_df.dropna(subset=['TOTAL'])

            logger.info(f"Fetched {len(fares_df)} all-in fare records across "
                        f"{fares_df['SECTOR'].nunique()} sectors.")

            # --- Convert all-in -> airline component BEFORE bucketing ---
            fares_df = self._fares_to_airline_component(fares_df, fare_col='TOTAL')
            if fares_df.empty:
                raise RuntimeError("No fares left after airline-component conversion")

            # GP logic operates on the airline component from here on
            fares_df['TOTAL'] = fares_df['AirlineComponent'].round(0).astype(int)

            all_buckets = []
            for sector, group in fares_df.groupby('SECTOR'):
                all_buckets.extend(self._compute_gp_fare_buckets(sector, group))

            if not all_buckets:
                raise RuntimeError("No fare buckets computed from GP logic")

            farebasis_df = pd.DataFrame(all_buckets, columns=['Sector', 'FareBasis'])
            logger.info(f"Computed {len(farebasis_df)} fare bucket entries "
                        f"across {farebasis_df['Sector'].nunique()} sectors "
                        f"(airline-component space).")
            self._data_cache['farebasis_df'] = farebasis_df
            return farebasis_df

        except Exception as e:
            logger.error(f"Error computing fare buckets: {e}")
            logger.error(traceback.format_exc())
            raise

    def _compute_gp_fare_buckets(self, sector, fares_group):
        """
        Compute fare buckets for a single sector using geometric progression.
        
        BRD Logic:
          1. Sort unique fare components ascending
          2. G1 = min, G19 = max → compute 19 GP values (common ratio R)
          3. Match each GP value to the closest actual fare component
          4. Consecutive matched values form bucket boundaries:
             bucket_1 = [matched_1, matched_2)
             bucket_2 = [matched_2, matched_3)
             ...
             bucket_18 = [matched_18, matched_19)
             bucket_19 = [matched_19, +∞)
          5. Result: 19 fare buckets per sector
        
        Returns list of (sector, fare_bucket_string) tuples.
        """
        # Get unique sorted fare components
        fare_values = sorted(fares_group['TOTAL'].unique())

        if len(fare_values) < 2:
            return [(sector, f"{fare_values[0]}+")]

        g1 = fare_values[0]   # min
        g19 = fare_values[-1]  # max
        n_points = 19

        # Compute common ratio: G19 = G1 * R^(n-1) → R = (G19/G1)^(1/(n-1))
        if g1 <= 0:
            g1 = 1  # safety for log
        common_ratio = (g19 / g1) ** (1.0 / (n_points - 1))

        # Generate 19 GP values
        gp_values = [g1 * (common_ratio ** i) for i in range(n_points)]

        # Match each GP value to closest actual fare component
        fare_arr = np.array(fare_values)
        matched_fares = []
        for gp_val in gp_values:
            diffs = np.abs(fare_arr - gp_val)
            closest_idx = diffs.argmin()
            matched_fare = int(fare_arr[closest_idx])
            # Avoid consecutive duplicates (if two GP values map to same fare)
            if not matched_fares or matched_fares[-1] != matched_fare:
                matched_fares.append(matched_fare)

        # Need at least 2 matched fares to form buckets
        if len(matched_fares) < 2:
            return [(sector, f"{matched_fares[0]}+")]

        # Form buckets from CONSECUTIVE matched fares
        # bucket_i = [matched_i, matched_{i+1})  →  "lower-upper"
        # Last bucket is open-ended: "matched_last+"
        buckets = []
        for i in range(len(matched_fares) - 1):
            lower = matched_fares[i]
            upper = matched_fares[i + 1]
            buckets.append((sector, f"{lower}-{upper}"))

        # Final open-ended bucket from the last matched value
        buckets.append((sector, f"{matched_fares[-1]}+"))

        return buckets

    # =========================================================================
    # STEP 4: COMPUTE LEAN/PEAK TAGGING
    # =========================================================================

    def _tagging_retention_floor(self):
        """Rolling floor = current_date - 365. Matches _lean_peak_window start."""
        return self.current_date - pd.Timedelta(days=365)
    
    def _lean_peak_window(self):
        """Rolling 365: [current_date - 365, current_date).  Latest included = yesterday."""
        start = self.current_date - pd.Timedelta(days=365)
        return start, self.current_date

    def _compute_lean_peak_tagging(self, lean_peak_df):
        logger.info("Computing historical lean/peak tagging via Z-Score...")

        # --- clip the wide source to the rolling 365-day historical window ---
        start, end = self._lean_peak_window()          # [current_date - 365, current_date)
        src = lean_peak_df[(lean_peak_df['DepartureDate'] >= start) &
                           (lean_peak_df['DepartureDate'] < end)]
        logger.info(f"[Tagging] rolling window {start.date()}..{(end - pd.Timedelta(days=1)).date()}, "
                    f"{len(src)} source rows.")

        fresh_tags = self._zscore_tag(src)
        fresh_tags['DepartureDate'] = pd.to_datetime(fresh_tags['DepartureDate'])
        logger.info(f"Freshly tagged {len(fresh_tags)} historical flights (full rolling year).")

        retention_floor = start                        # == current_date - 365

        hist_tags = pd.DataFrame() if FORCE_BACKFILL else \
            self._read_sector_cache('tagging', self.active_sectors)

        touched_months = set()

        if hist_tags is None or hist_tags.empty:
            # S1 — COLD START: seed the whole rolling year.
            hist_tags = fresh_tags.copy()
            touched_months = None                      # write every month present
            logger.info(f"[Tagging] No cache — SEEDED {len(hist_tags)} rows "
                        f"({start.date()}..{(end - pd.Timedelta(days=1)).date()}).")

        elif self.today.day == 1:
            # S3 — MONTH START: replace all freshly-tagged dates, full re-tag.
            refreshed = fresh_tags['DepartureDate'].unique()
            hist_tags = hist_tags[~hist_tags['DepartureDate'].isin(refreshed)]
            hist_tags = pd.concat([hist_tags, fresh_tags], ignore_index=True)
            touched_months = None                      # rewrite all month partitions
            logger.info(f"[Tagging] Month start — re-tagged & replaced {len(refreshed)} dates.")

        else:
            # S2 — NORMAL DAY: append yesterday only.
            yday = pd.to_datetime(self.today.date() - timedelta(days=1))
            y_tags = fresh_tags[fresh_tags['DepartureDate'] == yday]
            hist_tags = pd.concat([hist_tags, y_tags], ignore_index=True)
            touched_months.add(self._month_key(yday))
            # rolling floor advances daily: the floor month loses its oldest day,
            # so that partition must be rewritten too.
            touched_months.add(self._month_key(retention_floor))
            logger.info(f"[Tagging] Appended {len(y_tags)} rows for {yday.date()}.")

        hist_tags = hist_tags.drop_duplicates(
            subset=['Sector', 'DepartureDate', 'FlightNumber', 'Time Cluster'], keep='last')

        # --- retention: drop whole partitions below the rolling floor,
        #     rewrite the floor month if it lost rows ---
        before = len(hist_tags)
        hist_tags = hist_tags[hist_tags['DepartureDate'] >= retention_floor]
        floor_month = self._month_key(retention_floor)
        if len(hist_tags) != before:
            for s in self.active_sectors:
                for m in self._list_cache_months('tagging', s):
                    if m < floor_month:
                        self._delete_cache_partition('tagging', s, m)
            if touched_months is not None:
                touched_months.add(floor_month)

        self._write_sector_cache('tagging', hist_tags, months=touched_months)

        logger.info(f"Historical tagging ready: {len(hist_tags)} rows cached, "
                    f"retained from {retention_floor.date()}.")
        return hist_tags

    # =========================================================================
    # STEP 5: COMPUTE FUTURE LEAN/PEAK TAGGING (50th percentile)
    # =========================================================================

    def _compute_future_tagging(self, lean_peak_df):
        """
        Future lean/peak tagging via 50th percentile, computed from tags RE-DERIVED
        on the trailing 365 days ending on the last day of the previous month.

        Returns:
            mid_time_cluster_tagging: Sector/DOW/Quarter/TimeCluster
            mid_tagging:              Sector/DOW/Quarter (fallback)
        """
        logger.info("Computing future lean/peak tagging (re-derived on prev-month window)...")

        last_day_prev_month = self.current_date.replace(day=1) - pd.Timedelta(days=1)
        start_date = last_day_prev_month - pd.Timedelta(days=365)

        src = lean_peak_df.copy()
        src['DepartureDate'] = pd.to_datetime(src['DepartureDate'])
        src = src[(src['DepartureDate'] > start_date) & (src['DepartureDate'] <= last_day_prev_month)]
        logger.info(f"[Future Tagging] Basis window "
                    f"{start_date.date()} .. {last_day_prev_month.date()} ({len(src)} source rows).")

        # Re-derive Z-score tags on the window (independent of the frozen hist cache)
        tags = self._zscore_tag(src)
        tags['DepartureDate'] = pd.to_datetime(tags['DepartureDate'])
        tags['DOW'] = tags['DepartureDate'].dt.day_name()
        tags['Quarter'] = tags['DepartureDate'].apply(get_quarter_abbr)
        tags['Tagging_int'] = tags['Tagging'].apply(numerical_tagging)

        # One descending sort by Tagging_int serves BOTH percentile groupbys:
        # within any (Sector, DOW, Quarter) block the Tagging_int order is preserved
        # even after sub-selecting a single Time Cluster, so middle_tagging (the
        # positional middle) is the true 50th percentile in both.
        tags = tags.sort_values(
            by=['Sector', 'DOW', 'Quarter', 'Time Cluster', 'Tagging_int'],
            ascending=[True, True, True, True, False]
        )

        mid_time_cluster_tagging = (
            tags.groupby(['Sector', 'DOW', 'Quarter', 'Time Cluster'], sort=False)['Tagging']
            .apply(middle_tagging)
            .reset_index(name='Tagging')
        )

        tags = tags.sort_values(
            by=['Sector', 'DOW', 'Quarter', 'Tagging_int'],
            ascending=[True, True, True, False]
        )
        
        mid_tagging = (
            tags.groupby(['Sector', 'DOW', 'Quarter'], sort=False)['Tagging']
            .apply(middle_tagging)
            .reset_index(name='Tagging')
        )

        # tags = tags.sort_values(
        #     by=['Sector', 'Quarter', 'Tagging_int'],
        #     ascending=[True, True, False]
        # )

        # mid_tagging_quarter = (
        #     tags.groupby(['Sector', 'Quarter'], sort=False)['Tagging']
        #     .apply(middle_tagging)
        #     .reset_index(name='Tagging')
        # )

        # tags = tags.sort_values(
        #     by=['Sector', 'Tagging_int'],
        #     ascending=[True, False]
        # )

        # mid_tagging_sector = (
        #     tags.groupby(['Sector'], sort=False)['Tagging']
        #     .apply(middle_tagging)
        #     .reset_index(name='Tagging')
        # )

        # tags = tags.sort_values(
        #     by=['DOW', 'Quarter', 'Tagging_int'],
        #     ascending=[True, True, False]
        # )

        # mid_tagging_quarter = (
        #     tags.groupby(['DOW', 'Quarter'], sort=False)['Tagging']
        #     .apply(middle_tagging)
        #     .reset_index(name='Tagging')
        # )

        logger.info(f"Future tagging computed: {len(mid_time_cluster_tagging)} TC-level, "
                    f"{len(mid_tagging)} fallback entries.")
        return mid_time_cluster_tagging, mid_tagging

    # =========================================================================
    # STEP 6: PREPARE COMBINED FLOWN+BOOKED DATA & ASSIGN FARE BUCKETS
    # =========================================================================

    def _prepare_combined_data(self, flown_df, booked_df, farebasis_df):
        """
        Combine flown + booked data, remove SOL, assign fare buckets.
        Returns the combined DataFrame with FareBasis column.
        """
        logger.info("Preparing combined flown + booked data with fare bucket assignment...")

        df = pd.concat([flown_df, booked_df], ignore_index=True)
        df = df[df['Pax Count'] > 0]

        # Build sector -> fare range lookup
        sector_ranges = {}
        for _, row in farebasis_df.iterrows():
            sector = row['Sector']
            label = row['FareBasis']
            if not label or label == '':
                continue
            if label.endswith('+'):
                lower = float(label[:-1])
                upper = float('inf')
            else:
                parts = label.split('-')
                if len(parts) == 2:
                    lower = float(parts[0])
                    upper = float(parts[1])
                else:
                    continue
            if sector not in sector_ranges:
                sector_ranges[sector] = []
            sector_ranges[sector].append((lower, upper, label))

        # Lower the lowest fare bucket bound by 20% to capture near-boundary fares
        for sector in sector_ranges:
            ranges = sector_ranges[sector]
            if ranges:
                # Find the bucket with the lowest lower bound
                ranges.sort(key=lambda x: x[0])
                lowest_lower, lowest_upper, lowest_label = ranges[0]
                # Replace with 20% reduced lower bound (display label stays the same)
                ranges[0] = (lowest_lower * 0.8, lowest_upper, lowest_label)
                sector_ranges[sector] = ranges

        # Vectorized fare bucket assignment
        df['AvgFare'] = df['Fare'] / df['Pax Count']

        def assign_fare_bucket(row):
            sector = row['Sector']
            value = row['AvgFare']
            if sector not in sector_ranges:
                return ''
            for lower, upper, label in sector_ranges[sector]:
                if lower <= value < upper:
                    return label
            return ''

        df['FareBasis'] = df.apply(assign_fare_bucket, axis=1)

        # Merge to get official FareBasis label
        df = df.merge(farebasis_df, on=['Sector', 'FareBasis'], how='left', suffixes=('', '_cfg'))

        # Ensure STD is time type
        if df['STD'].dtype == object:
            df['STD'] = pd.to_datetime(df['STD'], format='%H:%M:%S', errors='coerce').dt.time

        logger.info(f"Combined data prepared: {len(df)} rows.")
        return df

    # =========================================================================
    # STEP 7: PIVOT NDO DATA
    # =========================================================================

    def _pivot_ndo_data(self, combined_df, farebasis_df, historical_tagging_df,
                        mid_time_cluster_tagging, mid_tagging):
        """
        Pivot the required RBD data by NDO, impute missing fare buckets,
        fill zeros for past booking dates, and merge lean/peak tagging.
        """
        logger.info("Pivoting NDO data for required RBDs...")
        # Split into required and remaining
        combined_df = combined_df[(combined_df['NDO'] >= 0) & (combined_df['NDO'] <= 180)]
        combined_df = combined_df[(combined_df['DepartureDate'] >= self.one_year_back)]
        main_df = combined_df[
            (combined_df['RBD'].isin(REQUIRED_RBDS)) & (combined_df['FareBasis'] != '')
        ].copy()
        remaining_df = combined_df[
            ~combined_df['RBD'].isin(REQUIRED_RBDS) | (combined_df['FareBasis'] == '')
        ].copy()

        self._data_cache['remaining_df'] = remaining_df

        # Ensure NDO is integer (may come as float from DB/parquet cache)
        main_df['NDO'] = main_df['NDO'].astype(int)

        # Pivot: index=flight+fare bucket, columns=NDO, values=Pax Count
        pivot_ndo = main_df.pivot_table(
            index=['Sector', 'DepartureDate', 'DOW', 'FlightNumber', 'STD', 'FareBasis'],
            columns='NDO',
            values='Pax Count',
            aggfunc='sum'
        ).reset_index()

        # Ensure all NDO columns 0-180 exist
        for i in NDO_RANGE:
            if i not in pivot_ndo.columns:
                pivot_ndo[i] = np.nan

        ndo_cols = sorted([c for c in pivot_ndo.columns if isinstance(c, (int, np.integer))], key=int)
        self._data_cache['ndo_cols'] = ndo_cols

        # Impute missing fare buckets per flight
        logger.info("Imputing missing fare buckets per flight...")
        combo_main = main_df[['Sector', 'DepartureDate', 'FlightNumber', 'DOW', 'STD']].drop_duplicates()
        combo_fare = farebasis_df[['Sector', 'FareBasis']].drop_duplicates()
        combos = combo_main.merge(combo_fare, on='Sector', how='left')
        combos['DepartureDate'] = pd.to_datetime(combos['DepartureDate'])
        pivot_ndo['DepartureDate'] = pd.to_datetime(pivot_ndo['DepartureDate'])

        # Left join to ensure all fare bucket rows exist
        pivot_ndo = combos.merge(
            pivot_ndo,
            on=['Sector', 'DepartureDate', 'DOW', 'FlightNumber', 'STD', 'FareBasis'],
            how='left'
        )

        # Fill zeros for past booking dates
        logger.info("Filling zeros for past booking dates...")
        old_date_mask = pivot_ndo['DepartureDate'] < self.current_date
        pivot_ndo.loc[old_date_mask, ndo_cols] = pivot_ndo.loc[old_date_mask, ndo_cols].fillna(0)

        # For future dates, fill zeros only where booking date is already past
        for ndo in ndo_cols:
            days = int(ndo)
            mask = (
                (pivot_ndo['DepartureDate'] >= self.current_date) &
                (pivot_ndo['DepartureDate'] - pd.to_timedelta(days, unit='D') < self.current_date)
            )
            pivot_ndo.loc[mask, ndo] = pivot_ndo.loc[mask, ndo].fillna(0)

        # Add Time Cluster
        if pivot_ndo['STD'].dtype == object:
            pivot_ndo['STD'] = pd.to_datetime(pivot_ndo['STD'], format='%H:%M:%S', errors='coerce').dt.time
        pivot_ndo['Time Cluster'] = pivot_ndo['STD'].apply(get_time_cluster)
        pivot_ndo['FlightNumber'] = pivot_ndo['FlightNumber'].astype(int)

        # Merge historical tagging
        historical_tagging_df['DepartureDate'] = pd.to_datetime(historical_tagging_df['DepartureDate'])
        historical_tagging_df['FlightNumber'] = historical_tagging_df['FlightNumber'].astype(int)
        pivot_ndo = pivot_ndo.merge(
            historical_tagging_df,
            on=['Sector', 'FlightNumber', 'DepartureDate', 'Time Cluster'],
            how='left'
        )

        # Fill NaN tagging with future tagging lookup
        pivot_ndo['Quarter'] = pivot_ndo['DepartureDate'].apply(get_quarter_abbr)
        self._fill_nan_tagging(pivot_ndo, mid_time_cluster_tagging, mid_tagging)

        logger.info(f"Pivot NDO data ready: {len(pivot_ndo)} rows, "
                    f"remaining NaN tagging: {pivot_ndo['Tagging'].isna().sum()}")
        return pivot_ndo
    
    def _zscore_tag(self, lean_peak_df):
        """Compute Sector-TimeCluster Z-score lean/peak tags for one source slice."""
        df = lean_peak_df.copy()
        df['DepartureDate'] = pd.to_datetime(df['DepartureDate'])

        if df['STD_time'].dtype == object:
            sample = df['STD_time'].dropna().iloc[0] if not df['STD_time'].dropna().empty else None
            if sample and isinstance(sample, str):
                df['STD_time'] = pd.to_datetime(df['STD_time'], format='%H:%M:%S', errors='coerce').dt.time
        df['Time Cluster'] = df['STD_time'].apply(get_time_cluster)

        agg_df = df.groupby(
            ['Sector', 'DepartureDate', 'FlightNumber', 'Time Cluster']
        ).agg({'Revenue': 'sum', 'Capacity': 'sum', 'Distance': 'first'}).reset_index()

        tc_agg = agg_df.groupby(['Sector', 'Time Cluster']).agg(
            {'Revenue': 'sum', 'Capacity': 'sum'}
        ).reset_index().rename(columns={'Revenue': 'TC_Revenue', 'Capacity': 'TC_Capacity'})
        agg_df = agg_df.merge(tc_agg, on=['Sector', 'Time Cluster'], how='left')

        agg_df['TC_ASKM'] = agg_df['TC_Capacity'] * agg_df['Distance']
        agg_df['TC_RASK'] = agg_df['TC_Revenue'] / agg_df['TC_ASKM']
        agg_df['ASKM'] = agg_df['Capacity'] * agg_df['Distance']
        agg_df['RASK'] = agg_df['Revenue'] / agg_df['ASKM']

        count_df = agg_df.groupby(['Sector', 'Time Cluster'])['DepartureDate'].count().reset_index(name='Count')
        agg_df = agg_df.merge(count_df, on=['Sector', 'Time Cluster'], how='left')
        agg_df['Mean_Val'] = (agg_df['TC_RASK'] - agg_df['RASK']) ** 2

        sum_mean = agg_df.groupby(['Sector', 'Time Cluster'])['Mean_Val'].sum().reset_index(name='Sum_Mean_Val')
        agg_df = agg_df.merge(sum_mean, on=['Sector', 'Time Cluster'], how='left')
        agg_df['STD_val'] = np.sqrt(agg_df['Sum_Mean_Val'] / (agg_df['Count'] - 1))

        agg_df['Z_Score'] = (agg_df['RASK'] - agg_df['TC_RASK']) / agg_df['STD_val']
        agg_df = agg_df[agg_df['Z_Score'].notna()]
        agg_df = agg_df[(agg_df['Z_Score'] > -6) & (agg_df['Z_Score'] < 6)]
        # if z score tagging goes out of range then remove outliers and re-run it again
        agg_df['Tagging'] = agg_df['Z_Score'].apply(tag_from_zscore)

        return agg_df[['Sector', 'DepartureDate', 'FlightNumber', 'Time Cluster', 'Tagging']].copy()


    def _fill_nan_tagging(self, df, mid_time_cluster_tagging, mid_tagging):
        """Fill NaN tagging values using future tagging lookups (in-place)."""
        # First pass: Sector/DOW/Quarter/Time Cluster level
        na_mask = df['Tagging'].isna()
        if na_mask.sum() > 0:
            lookup = mid_time_cluster_tagging.rename(columns={'Tagging': 'Tagging_lookup'})
            na_rows = df.loc[na_mask, ['Sector', 'DOW', 'Quarter', 'Time Cluster']].copy()
            merged = na_rows.merge(lookup, on=['Sector', 'DOW', 'Quarter', 'Time Cluster'], how='left')
            df.loc[na_mask, 'Tagging'] = merged['Tagging_lookup'].values

        # Second pass: Sector/DOW/Quarter level (fallback)
        na_mask = df['Tagging'].isna()
        if na_mask.sum() > 0:
            lookup = mid_tagging.rename(columns={'Tagging': 'Tagging_mid'})
            na_rows = df.loc[na_mask, ['Sector', 'DOW', 'Quarter']].copy()
            merged = na_rows.merge(lookup, on=['Sector', 'DOW', 'Quarter'], how='left')
            df.loc[na_mask, 'Tagging'] = merged['Tagging_mid'].values

    # =========================================================================
    # STEP 8: PREPARE FUTURE FLIGHTS WITH TAGGING
    # =========================================================================

    def _prepare_future_flights(self, future_df, mid_time_cluster_tagging, mid_tagging, farebasis_df):
        """
        Prepare future flights DataFrame with tagging and fare bucket expansion.
        """
        logger.info("Preparing future flights with tagging...")

        future_df['DepartureDate'] = pd.to_datetime(future_df['DepartureDate'])
        future_df = future_df[future_df['DepartureDate'] <= self.prediction_limit]

        if future_df['STD'].dtype == object:
            future_df['STD'] = pd.to_datetime(future_df['STD'], format='%H:%M:%S', errors='coerce').dt.time

        future_df['Quarter'] = future_df['DepartureDate'].apply(get_quarter_abbr)
        future_df['DOW'] = future_df['DepartureDate'].dt.day_name()
        future_df['Time Cluster'] = future_df['STD'].apply(get_time_cluster)

        # Merge tagging
        future_df = future_df.merge(
            mid_time_cluster_tagging,
            on=['Sector', 'DOW', 'Quarter', 'Time Cluster'],
            how='left'
        )
        self._fill_nan_tagging(future_df, mid_time_cluster_tagging, mid_tagging)

        # Expand with fare buckets
        future_df_expanded = future_df.merge(farebasis_df, on='Sector', how='left')

        # Create keys
        future_df['Key'] = (
            future_df['Sector'] + '_' +
            future_df['DepartureDate'].dt.strftime('%Y-%m-%d') + '_' +
            future_df['FlightNumber'].astype(str)
        )
        future_df_expanded['Key'] = (
            future_df_expanded['Sector'] + '_' +
            future_df_expanded['DepartureDate'].dt.strftime('%Y-%m-%d') + '_' +
            future_df_expanded['FlightNumber'].astype(str)
        )

        logger.info(f"Future flights prepared: {len(future_df)} flights, "
                    f"{len(future_df_expanded)} expanded rows.")
        return future_df, future_df_expanded

    # =========================================================================
    # STEP 9: FLIGHT MATCHING LOGIC
    # =========================================================================

    def _find_matching_flights(self, future_df, hist_flight_df):
        """
        For each future flight, find >= MIN_FLIGHT_MATCHES historical flights.
        Returns dict: {future_key: [historical keys]}. Flights that can't reach the
        minimum candidate count are skipped entirely (absent from the dict).
        """
        logger.info("Building flight match dictionary...")
        min_matches = MIN_FLIGHT_MATCHES

        hist = hist_flight_df.copy()
        hist['Key'] = (
            hist['Sector'] + '_' +
            hist['DepartureDate'].dt.strftime('%Y-%m-%d') + '_' +
            hist['FlightNumber'].astype(str)
        )

        def to_minutes(t):
            return t.hour * 60 + t.minute if pd.notna(t) else np.nan

        hist['STD_min'] = hist['STD'].apply(to_minutes)

        time_windows = [2, 4, 6, 8, 12]  # hours
        matches_dict = {}
        skipped = 0

        for _, future_row in future_df.iterrows():
            future_key = future_row['Key']
            sector = future_row['Sector']
            dow = future_row['DOW']
            tagging = future_row['Tagging']
            future_date = future_row['DepartureDate']
            future_min = to_minutes(future_row['STD'])

            # 1) Sector + DOW + Tagging, past flights only
            candidates = hist[
                (hist['Sector'] == sector) &
                (hist['DOW'] == dow) &
                (hist['Tagging'] == tagging) &
                (hist['DepartureDate'] < future_date)
            ]

            # 2) Fallback: drop DOW -> Sector + Tagging
            if len(candidates) < min_matches:
                candidates = hist[
                    (hist['Sector'] == sector) &
                    (hist['Tagging'] == tagging) &
                    (hist['DepartureDate'] < future_date)
                ]

            # 3) Still not enough candidates -> skip this flight
            if len(candidates) < min_matches or pd.isna(future_min):
                skipped += 1
                continue

            # 4) Expand STD window until we reach min_matches
            matched = candidates.iloc[0:0]
            for window_hours in time_windows:
                diffs = (candidates['STD_min'] - future_min).abs()
                diffs = diffs.where(diffs <= 720, 1440 - diffs)  # wrap-around
                matched = candidates[diffs <= window_hours * 60]
                if len(matched) >= min_matches:
                    matches_dict[future_key] = matched['Key'].tolist()
                    break
            else:
                # Never hit min_matches across all windows — keep whatever we found
                matches_dict[future_key] = matched['Key'].tolist() if not matched.empty else []

        total_matches = sum(len(v) for v in matches_dict.values())
        logger.info(f"Flight matching complete: {len(matches_dict)} future flights matched, "
                    f"{skipped} skipped (insufficient candidates), {total_matches} total matches.")
        return matches_dict

    # =========================================================================
    # STEP 10: BUILD RESULT DATASET (FARE BUCKET LEVEL MATCHING)
    # =========================================================================

    def _build_matched_result(self, future_df_expanded, pivot_ndo, flight_matches):
        """
        For each future flight key, join matched historical NDO data at fare bucket level.
        Returns the master result DataFrame.
        """
        logger.info("Building fare bucket level matched result dataset...")
        ndo_cols = self._data_cache['ndo_cols']

        # Index pivot_ndo by Key for fast lookup
        pivot_ndo_indexed = pivot_ndo.set_index('Key') if 'Key' not in pivot_ndo.index.names else pivot_ndo

        dfs = []
        batch_size = 500
        keys = list(flight_matches.keys())

        for batch_start in range(0, len(keys), batch_size):
            batch_keys = keys[batch_start:batch_start + batch_size]
            for key in batch_keys:
                matched_hist_keys = flight_matches[key]
                if not matched_hist_keys:
                    continue

                future_sub = future_df_expanded[future_df_expanded['Key'] == key]
                hist_sub = pivot_ndo[pivot_ndo['Key'].isin(matched_hist_keys)]

                if future_sub.empty or hist_sub.empty:
                    continue

                merged = future_sub.merge(
                    hist_sub,
                    on=['Sector', 'DOW', 'Tagging', 'FareBasis'],
                    how='left',
                    suffixes=('_future', '_hist')
                )
                dfs.append(merged)

        if not dfs:
            logger.warning("No matched results produced.")
            return pd.DataFrame()

        result_df = pd.concat(dfs, ignore_index=True, copy=False)
        logger.info(f"Matched result dataset: {len(result_df)} rows.")
        return result_df

    # =========================================================================
    # STEP 11: CALCULATE PREDICTIONS (NORMAL MEAN)
    # =========================================================================

    def _calculate_predictions(self, result_df):
        """
        Predictions at fare bucket and flight level, NORMAL and WEIGHTED.

        Returns:
            result_merged_df      fare bucket level, normal   (predicted_pax_count)
            result_final_df       flight level, normal
            result_merged_df_std  fare bucket level std (normal)
            weighted_merged_df    fare bucket level, weighted (weighted_pax_count)
            weighted_final_df     flight level, weighted
        """
        logger.info("Calculating additive pickup predictions (normal + weighted)...")
        ndo_cols = self._data_cache['ndo_cols']

        group_cols = ['Sector', 'DepartureDate_future', 'DOW', 'STD_future',
                    'FlightNumber_future', 'Capacity', 'Total Bookings', 'Tagging', 'FareBasis']
        flight_group_cols = ['Sector', 'DepartureDate_future', 'DOW', 'STD_future',
                            'FlightNumber_future', 'Capacity', 'Total Bookings', 'Tagging']

        def _future_ndo(frame):
            return (pd.to_datetime(frame['DepartureDate_future']) - self.current_date).dt.days

        def sum_predicted_pax(row, cols):
            ndo_limit = row['future_flight_NDO']
            total = 0.0
            for ndo in cols:
                if int(ndo) <= ndo_limit:
                    val = row[ndo]
                    if pd.notna(val):
                        total += val
            return total

        # ========================== NORMAL BRANCH ================================
        result_merged_df = result_df.groupby(group_cols)[ndo_cols].mean().reset_index()
        result_merged_df_std = result_df.groupby(group_cols)[ndo_cols].std().reset_index()

        for df_ref in [result_merged_df, result_merged_df_std]:
            df_ref['future_flight_NDO'] = _future_ndo(df_ref)

        result_merged_df['predicted_pax_count'] = result_merged_df.apply(
            lambda r: sum_predicted_pax(r, ndo_cols), axis=1
        )

        result_merged_df_std[ndo_cols] = result_merged_df_std[ndo_cols] ** 2
        result_merged_df_std['predicted_pax_std'] = result_merged_df_std.apply(
            lambda r: sum_predicted_pax(r, ndo_cols), axis=1
        )
        result_merged_df_std['predicted_pax_std'] = np.sqrt(result_merged_df_std['predicted_pax_std'])

        # Flight level — NDO columns summed across fare buckets so the flight-level
        # frame carries the same 0-180 curve as the fare-bucket-level frame.
        flight_agg = {ndo: 'sum' for ndo in ndo_cols}
        flight_agg['predicted_pax_count'] = 'sum'
        flight_agg['future_flight_NDO'] = 'first'
        result_final_df = result_merged_df.groupby(flight_group_cols).agg(flight_agg).reset_index()

        result_final_df_std = result_merged_df_std.groupby(flight_group_cols).agg(
            predicted_pax_std_square=('predicted_pax_std', lambda x: np.sqrt((x ** 2).sum()))
        ).reset_index()
        result_final_df = result_final_df.merge(result_final_df_std, on=flight_group_cols, how='left')

        result_merged_df = result_merged_df.merge(
            result_merged_df_std[['Sector', 'DepartureDate_future', 'FlightNumber_future',
                                'FareBasis', 'predicted_pax_std']],
            on=['Sector', 'DepartureDate_future', 'FlightNumber_future', 'FareBasis'],
            how='left'
        )

        # ========================= WEIGHTED BRANCH ===============================
        wmean, wstd = self._weighted_ndo_stats(result_df, group_cols, ndo_cols)

        weighted_merged_df = wmean.reset_index()
        weighted_std_df = wstd.reset_index()

        for df_ref in [weighted_merged_df, weighted_std_df]:
            df_ref['future_flight_NDO'] = _future_ndo(df_ref)

        weighted_merged_df['weighted_pax_count'] = weighted_merged_df.apply(
            lambda r: sum_predicted_pax(r, ndo_cols), axis=1
        )

        weighted_std_df[ndo_cols] = weighted_std_df[ndo_cols] ** 2
        weighted_std_df['weighted_pax_std'] = weighted_std_df.apply(
            lambda r: sum_predicted_pax(r, ndo_cols), axis=1
        )
        weighted_std_df['weighted_pax_std'] = np.sqrt(weighted_std_df['weighted_pax_std'])

        weighted_merged_df = weighted_merged_df.merge(
            weighted_std_df[['Sector', 'DepartureDate_future', 'FlightNumber_future',
                            'FareBasis', 'weighted_pax_std']],
            on=['Sector', 'DepartureDate_future', 'FlightNumber_future', 'FareBasis'],
            how='left'
        )

        # BRD: Flight level Prediction = SUM(Fare Bucket Level Predictions)
        w_flight_agg = {ndo: 'sum' for ndo in ndo_cols}
        w_flight_agg['weighted_pax_count'] = 'sum'
        w_flight_agg['future_flight_NDO'] = 'first'
        weighted_final_df = weighted_merged_df.groupby(flight_group_cols).agg(w_flight_agg).reset_index()

        weighted_final_std = weighted_std_df.groupby(flight_group_cols).agg(
            weighted_pax_std_square=('weighted_pax_std', lambda x: np.sqrt((x ** 2).sum()))
        ).reset_index()
        weighted_final_df = weighted_final_df.merge(weighted_final_std, on=flight_group_cols, how='left')

        logger.info(f"Predictions complete (alpha={WEIGHT_ALPHA}) — "
                    f"normal fare bucket: {len(result_merged_df)}, flight: {len(result_final_df)}; "
                    f"weighted fare bucket: {len(weighted_merged_df)}, flight: {len(weighted_final_df)}.")

        return (result_merged_df, result_final_df, result_merged_df_std,
                weighted_merged_df, weighted_final_df)

    # =========================================================================
    # STEP 12: REMAINING RBD DISTRIBUTION
    # =========================================================================

    def _distribute_remaining_rbd(self, future_df, pivot_ndo, flight_matches,
                                result_merged_df, result_final_df,
                                count_col='predicted_pax_count'):
        """
        Distribute remaining (non-required RBD) pax across fare buckets proportionally
        to each bucket's share of `count_col`.
        """
        logger.info(f"Distributing remaining RBD bookings (basis: {count_col})...")
        ndo_cols = self._data_cache['ndo_cols']
        remaining_df = self._data_cache['remaining_df']

        remaining_df = remaining_df[~remaining_df['RBD'].isin(REQUIRED_RBDS)]
        remaining_df = remaining_df.groupby(
            ['Sector', 'DepartureDate', 'DOW', 'FlightNumber', 'NDO', 'STD']
        )['Pax Count'].sum().reset_index()
        remaining_df['NDO'] = remaining_df['NDO'].astype(int)

        remaining_pivot = remaining_df.pivot_table(
            index=['Sector', 'DepartureDate', 'DOW', 'FlightNumber', 'STD'],
            columns='NDO', values='Pax Count', aggfunc='sum'
        ).reset_index()

        remaining_pivot['DepartureDate'] = pd.to_datetime(remaining_pivot['DepartureDate'])
        remaining_pivot['Key'] = (
            remaining_pivot['Sector'] + '_' +
            remaining_pivot['DepartureDate'].dt.strftime('%Y-%m-%d') + '_' +
            remaining_pivot['FlightNumber'].astype(str)
        )

        dfs = []
        for key, matched_keys in flight_matches.items():
            if not matched_keys:
                continue
            future_sub = future_df[future_df['Key'] == key]
            hist_sub = remaining_pivot[remaining_pivot['Key'].isin(matched_keys)]
            if future_sub.empty or hist_sub.empty:
                continue
            merged = future_sub.merge(hist_sub, on=['Sector', 'DOW'], how='left',
                                    suffixes=('_future', '_hist'))
            dfs.append(merged)

        if not dfs:
            logger.warning("No remaining RBD matches found.")
            return result_merged_df, result_final_df

        remaining_pax = pd.concat(dfs, ignore_index=True)
        remaining_pax['future_flight_NDO'] = (
            remaining_pax['DepartureDate_future'] - self.current_date
        ).dt.days
        remaining_pax.fillna(0, inplace=True)

        def sum_ndo(row):
            total = 0.0
            ndo_limit = row['future_flight_NDO']
            for ndo in ndo_cols:
                if int(ndo) <= ndo_limit and ndo in row.index:
                    val = row.get(ndo, 0)
                    if pd.notna(val):
                        total += val
            return total

        remaining_pax['remaining_raw'] = remaining_pax.apply(sum_ndo, axis=1)

        remaining_merged = remaining_pax.groupby(
            ['Sector', 'DepartureDate_future', 'STD_future', 'FlightNumber_future']
        )['remaining_raw'].mean().reset_index()

        remaining_renamed = remaining_merged.rename(columns={'remaining_raw': 'remaining_pax_count'})
        merge_keys = ['Sector', 'DepartureDate_future', 'FlightNumber_future']

        # ---- Fare bucket level ----
        result_merged_df.drop(columns=['remaining_pax_count'], errors='ignore', inplace=True)
        result_merged_df = result_merged_df.merge(
            remaining_renamed[merge_keys + ['remaining_pax_count']], on=merge_keys, how='left'
        )

        # ---- Flight level ----
        result_final_df.drop(columns=['remaining_pax_count'], errors='ignore', inplace=True)
        result_final_df = result_final_df.merge(
            remaining_renamed[merge_keys + ['remaining_pax_count']], on=merge_keys, how='left'
        )
        result_final_df['remaining_pax_count'] = result_final_df['remaining_pax_count'].fillna(0)
        result_final_df['Final_pax_count'] = (
            result_final_df[count_col] + result_final_df['remaining_pax_count']
        )
        result_final_df['Final_Total_Bookings'] = (
            result_final_df['Total Bookings'] + result_final_df['Final_pax_count']
        )

        # ---- Proportional redistribution, driven by count_col ----
        flight_totals = (result_merged_df.groupby(merge_keys)[count_col]
                        .sum().reset_index(name='Total_Predicted_pax_count'))
        result_merged_df.drop(columns=['Total_Predicted_pax_count'], errors='ignore', inplace=True)
        merged_totals = result_merged_df[merge_keys].merge(flight_totals, on=merge_keys, how='left')
        result_merged_df['Total_Predicted_pax_count'] = merged_totals['Total_Predicted_pax_count'].values

        result_merged_df['FareBasis_Prediction_ratio'] = np.where(
            result_merged_df['Total_Predicted_pax_count'] > 0,
            result_merged_df[count_col] / result_merged_df['Total_Predicted_pax_count'],
            0
        )
        result_merged_df['Remaining_pax_distribution'] = (
            result_merged_df['FareBasis_Prediction_ratio'] *
            result_merged_df['remaining_pax_count'].fillna(0)
        )
        result_merged_df['Final_pax_count'] = (
            result_merged_df[count_col] + result_merged_df['Remaining_pax_distribution']
        )

        logger.info(f"Remaining RBD distribution complete (basis: {count_col}).")
        return result_merged_df, result_final_df
    
    def _sum_open_ndo(self, df, ndo_cols):
        """
        Sum the NDO columns that are still OPEN for each row, i.e. every NDO
        <= future_flight_NDO. NaN cells contribute 0.

        Equivalent to the row-wise sum_predicted_pax, vectorised.
        """
        ndo_arr = np.array([int(c) for c in ndo_cols])              # (n_ndo,)
        limits = df['future_flight_NDO'].to_numpy(dtype=float)      # (n_rows,)

        vals = df[ndo_cols].to_numpy(dtype=float)                   # (n_rows, n_ndo)
        open_mask = ndo_arr[None, :] <= limits[:, None]

        return np.where(open_mask & ~np.isnan(vals), np.nan_to_num(vals), 0.0).sum(axis=1)
    
    def _weighted_ndo_stats(self, result_df, group_cols, ndo_cols, alpha=WEIGHT_ALPHA):
        """
        Weighted mean and weighted std of every NDO column, grouped by group_cols.

        Weights decay with the age of the MATCHED HISTORICAL flight relative to the
        snapshot date (BRD: w(x) = alpha^x, x = SD - DD, or 1 when SD < DD).

        Weights are applied PER CELL: an NDO cell that is NaN contributes to neither
        the weighted sum, the weight sum, nor M for that NDO.

        Computed column-by-column so peak memory stays O(n_rows), not
        O(n_rows x n_ndo_cols).

        Returns (wmean_df, wstd_df) — indexed by group_cols, columns = ndo_cols.
        """
        n_rows = len(result_df)
        logger.info(f"  [Weighted] {n_rows} rows x {len(ndo_cols)} NDO columns "
                    f"(streaming, alpha={alpha})...")

        # ---- x and w(x) per BRD --------------------------------------------------
        hist_dd = pd.to_datetime(result_df['DepartureDate_hist'])
        delta_days = (self.current_date - hist_dd).dt.days          # SD - DD

        # SD >= DD -> x = (SD - DD) ; SD < DD -> x = 1
        x = delta_days.where(delta_days >= 0, WEIGHT_FUTURE_X)
        w = (alpha ** x).fillna(0.0).to_numpy(dtype=float)          # (n_rows,)

        # ---- Group codes, computed ONCE -----------------------------------------

        grouper = result_df.groupby(group_cols, sort=False, dropna=False)
        codes = grouper.ngroup().to_numpy()
        group_index = grouper.size().index
        n_groups = len(group_index)

        logger.info(f"  [Weighted] {n_groups} groups.")

        mean_out = np.empty((n_groups, len(ndo_cols)), dtype=float)
        std_out = np.empty((n_groups, len(ndo_cols)), dtype=float)

        # ---- One NDO column at a time -------------------------------------------
        for j, col in enumerate(ndo_cols):
            vc = result_df[col].to_numpy(dtype=float)               # (n_rows,)

            # A cell contributes only if it has a value AND a non-zero weight
            present = (~np.isnan(vc)) & (w > 0)

            v_filled = np.where(present, vc, 0.0)
            w_filled = np.where(present, w, 0.0)

            num = np.bincount(codes, weights=v_filled * w_filled, minlength=n_groups)
            den = np.bincount(codes, weights=w_filled, minlength=n_groups)
            M = np.bincount(codes, weights=present.astype(float), minlength=n_groups)

            # Weighted mean — groups with zero total weight stay NaN, matching
            # .mean() on an all-NaN slice.
            safe_den = np.where(den > 0, den, np.nan)
            mean = num / safe_den

            # Weighted std (BRD):  sqrt( SUM(w*(x-mu)^2) / (((M-1)/M) * SUM(w)) )
            # mean[codes] broadcasts each group's mean back onto its own rows.
            dev = np.where(present, (vc - mean[codes]) ** 2 * w_filled, 0.0)
            wdev = np.bincount(codes, weights=dev, minlength=n_groups)

            # M <= 1 -> dispersion is not estimable -> NaN
            safe_M = np.where(M > 1, M, np.nan)
            denom = ((safe_M - 1) / safe_M) * safe_den
            denom = np.where(denom > 0, denom, np.nan)

            mean_out[:, j] = mean
            std_out[:, j] = np.sqrt(wdev / denom)

        wmean = pd.DataFrame(mean_out, index=group_index, columns=ndo_cols)
        wstd = pd.DataFrame(std_out, index=group_index, columns=ndo_cols)

        logger.info(f"  [Weighted] Done — {n_groups} groups x {len(ndo_cols)} NDO columns.")
        return wmean, wstd
    # =========================================================================
    # STEP 15: SAVE RESULTS
    # =========================================================================

    def _save_to_csv(self, df, filename):
        """Save DataFrame to a local CSV file in the output directory."""
        filepath = os.path.join(self.output_dir, filename)
        df.to_csv(filepath, index=False)
        logger.info(f"Saved {len(df)} rows to CSV: {filepath}")

    def _save_to_parquet(self, df, filename):
        """
        Save a DataFrame to parquet in the output directory.

        Column names are cast to str because parquet requires string names — pandas
        would do this silently otherwise. Note the NDO columns therefore round-trip
        as '0'..'180', not 0..180.
        """
        filepath = os.path.join(self.output_dir, filename)

        out = df
        if any(not isinstance(c, str) for c in df.columns):
            out = df.copy()
            out.columns = [str(c) for c in out.columns]

        try:
            out.to_parquet(filepath, index=False, compression='snappy')
            size_mb = os.path.getsize(filepath) / (1024 * 1024)
            logger.info(f"Saved {len(df)} rows to parquet ({size_mb:.1f} MB): {filepath}")
        except Exception as e:
            # Never let a debug artifact kill the run — fall back to CSV.
            logger.warning(f"Parquet write failed for {filename} ({e}); falling back to CSV.")
            self._save_to_csv(df, filename.replace('.parquet', '.csv'))


    def _save_intermediate(self, df, name, fmt=None):
        """
        Save an intermediate DataFrame in TEST mode for component testing.
        No-op in PROD mode.

        fmt:
        'parquet' — force parquet
        'csv'     — force CSV
        None      — auto: parquet for wide/large frames, CSV otherwise
        """
        if self.mode != 'TEST':
            return

        if fmt is None:
            fmt = 'parquet' if len(df) >= PARQUET_ROW_THRESHOLD else 'csv'

        if fmt == 'parquet':
            self._save_to_parquet(df, f"intermediate_{name}.parquet")
        else:
            self._save_to_csv(df, f"intermediate_{name}.csv")

    @staticmethod
    def _to_parquet_bytes(df):
        out = df
        if any(not isinstance(c, str) for c in df.columns):
            out = df.copy()
            out.columns = [str(c) for c in out.columns]
        buf = io.BytesIO()
        out.to_parquet(buf, index=False, compression='snappy')
        buf.seek(0)
        return buf.getvalue()

    @staticmethod
    def _restore_ndo_columns(df):
        df.columns = [int(c) if isinstance(c, str) and c.isdigit() else c
                      for c in df.columns]
        return df

    def _store_write(self, df, rel_path):
        if self.mode == 'TEST':
            full = os.path.join(self.local_cach_dir, rel_path)
            os.makedirs(os.path.dirname(full), exist_ok=True)
            df.to_parquet(full, index=False, compression='snappy')
            return
        key = f"{self.s3_cache_prefix}/{rel_path}"
        self._get_s3().put_object(Bucket=self.s3_bucket, Key=key,
                                  Body=self._to_parquet_bytes(df))

    def _store_read(self, rel_path):
        try:
            if self.mode == 'TEST':
                full = os.path.join(self.local_cach_dir, rel_path)
                if not os.path.exists(full):
                    return None
                return self._restore_ndo_columns(pd.read_parquet(full))
            key = f"{self.s3_cache_prefix}/{rel_path}"
            obj = self._get_s3().get_object(Bucket=self.s3_bucket, Key=key)
            return self._restore_ndo_columns(pd.read_parquet(io.BytesIO(obj['Body'].read())))
        except Exception as e:
            logger.warning(f"  cache miss/unreadable {rel_path} ({e.__class__.__name__}).")
            return None
    
    @staticmethod
    def _sector_cache_path(kind, sector):
        return f"{kind}/sector={sector}.parquet"

    def _read_sector_cache(self, kind, sectors):
        """Concatenate all month partitions for these sectors. Empty frame if none."""
        frames = []
        for s in sectors:
            for m in self._list_cache_months(kind, s):
                df = self._store_read(self._sm_path(kind, s, m))
                if df is not None and not df.empty:
                    frames.append(df)
        if not frames:
            return pd.DataFrame()
        out = pd.concat(frames, ignore_index=True)
        if 'DepartureDate' in out.columns:
            out['DepartureDate'] = pd.to_datetime(out['DepartureDate'])
        return out

    def _write_sector_cache(self, kind, df, months=None):
        if df is None or df.empty or 'Sector' not in df.columns:
            return
        tmp = df.copy()
        tmp['_month'] = tmp['DepartureDate'].map(self._month_key)

        if months is not None:
            months = set(months)
            tmp = tmp[tmp['_month'].isin(months)]

        written = 0
        for (sector, month), part in tmp.groupby(['Sector', '_month'], sort=False):
            self._store_write(part.drop(columns='_month'),
                              self._sm_path(kind, sector, month))
            written += 1
        logger.debug(f"  [{kind}] wrote {written} month partitions.")

    @staticmethod
    def _run_sql(engine, sql, chunksize=50000):
        with engine.connect() as conn:
            chunks = pd.read_sql(text(sql), conn, chunksize=chunksize)
            return pd.concat([c for c in chunks], ignore_index=True)
        
    @staticmethod
    def _partition_value(v):
        if isinstance(v, (pd.Timestamp, datetime)):
            v = v.strftime('%Y-%m-%d')
        return re.sub(r'[^A-Za-z0-9_\-\.]', '_', str(v))

    def _write_partitioned_s3(self, df, dataset, partition_cols=('SnapshotDate', 'Sector')):
        if df is None or df.empty:
            logger.warning(f"[S3] {dataset}: nothing to write.")
            return
        missing = [c for c in partition_cols if c not in df.columns]
        if missing:
            logger.error(f"[S3] {dataset}: missing partition cols {missing} — skipped.")
            return
        s3 = self._get_s3()
        n = 0
        for keys, part in df.groupby(list(partition_cols), sort=False):
            keys = keys if isinstance(keys, tuple) else (keys,)
            path = "/".join(f"{c.lower()}={self._partition_value(v)}"
                            for c, v in zip(partition_cols, keys))
            body = part.drop(columns=list(partition_cols))
            key = f"{self.s3_output_prefix}/{dataset}/{path}/data.parquet"
            s3.put_object(Bucket=self.s3_bucket, Key=key, Body=self._to_parquet_bytes(body))
            n += 1
        logger.info(f"[S3] {dataset}: {n} partitions, {len(df)} rows.")

    def _reset_batch_cache(self):
        """Clear per-batch state but KEEP run-scoped master data. No return value."""
        preserved = {k: self._data_cache[k]
                     for k in RUN_SCOPED_CACHE_KEYS if k in self._data_cache}
        self._data_cache = preserved

    # =============================================================================
    # 1. PARTITION HELPERS
    # =============================================================================

    @staticmethod
    def _month_key(ts):
        """DepartureDate -> 'YYYY-MM' partition value."""
        return pd.to_datetime(ts).strftime('%Y-%m')

    @staticmethod
    def _sm_path(kind, sector, month):
        return f"{kind}/sector={sector}/month={month}.parquet"

    def _list_cache_months(self, kind, sector):
        """
        Which month partitions exist for a sector. Needed so a read can glob and
        so retention can drop whole partitions.
          TEST: list the local directory.
          PROD: list the S3 prefix.
        """
        months = []
        if self.mode == 'TEST':
            d = os.path.join(self.local_cach_dir, kind, f"sector={sector}")
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
                resp = self._get_s3().list_objects_v2(**kw)
                for obj in resp.get('Contents', []):
                    m = re.search(r'month=(\d{4}-\d{2})\.parquet$', obj['Key'])
                    if m:
                        months.append(m.group(1))
                if not resp.get('IsTruncated'):
                    break
                token = resp.get('NextContinuationToken')
        return sorted(set(months))

    def _delete_cache_partition(self, kind, sector, month):
        """Drop one month partition (retention). Best-effort."""
        rel = self._sm_path(kind, sector, month)
        try:
            if self.mode == 'TEST':
                p = os.path.join(self.local_cach_dir, rel)
                if os.path.exists(p):
                    os.remove(p)
            else:
                self._get_s3().delete_object(Bucket=self.s3_bucket,
                                             Key=f"{self.s3_cache_prefix}/{rel}")
        except Exception as e:
            logger.warning(f"  could not drop {rel} ({e.__class__.__name__}).")
    # =========================================================================
    # MAIN ORCHESTRATOR
    # =========================================================================

    def run(self):
        """Entry point. Processes the assigned sector bin one sector at a time
        (for accurate per-sector timing), writes per-sector audit stats, and
        returns a status dict. Model results are identical to batch processing
        because every step groups by Sector."""
        start = datetime.now()
        logger.info("=" * 80)
        logger.info(f"ADDITIVE PICKUP MODEL (process) — run_id={self.run_id} "
                    f"batch={self.batch_index}/{self.batch_total}")
        logger.info(f"Mode: {self.mode} | Backfill: {FORCE_BACKFILL or 'off'}")
        logger.info(f"Snapshot: {self.current_date.date()}")

        succeeded, failed = [], []
        sector_stats = {}
        try:
            # resolve sectors (bin / TEST / standalone-all)
            self._fetch_active_sectors()
            sectors = sorted(self.active_sectors)
            logger.info(f"{len(sectors)} sector(s) to process: {sectors}")

            # run-scoped master data (fetched once, survives per-sector resets)
            self._fetch_tax_master_data()

            for i, sector in enumerate(sectors, 1):
                logger.info("-" * 80)
                logger.info(f"[{i}/{len(sectors)}] Sector {sector}")
                self.active_sectors = [sector]
                t0 = _time.perf_counter()
                try:
                    self._run_batch([sector])
                    dt = round(_time.perf_counter() - t0, 3)
                    succeeded.append(sector)
                    sector_stats[sector] = {'seconds': dt, 'status': 'SUCCESS'}
                    logger.info(f"  {sector} done in {dt}s")
                except Exception as e:
                    dt = round(_time.perf_counter() - t0, 3)
                    failed.append(sector)
                    sector_stats[sector] = {'seconds': dt, 'status': 'FAILED',
                                            'error': str(e)[:300]}
                    logger.error(f"  {sector} FAILED after {dt}s: {e}")
                    logger.error(traceback.format_exc())
                finally:
                    self._audit_upsert_sector(sector, sector_stats[sector])
                    self._reset_batch_cache()

            logger.info("=" * 80)
            logger.info(f"COMPLETE — {len(succeeded)} OK, {len(failed)} failed, "
                        f"{datetime.now() - start}")

            # last bin to finish stamps the run terminal state
            self._audit_finalize()

            status = ('SUCCESS' if not failed
                      else ('PARTIAL' if succeeded else 'FAILED'))
            return {
                'status': status,
                'run_id': self.run_id,
                'batch_index': self.batch_index,
                'sectors_succeeded': succeeded,
                'sectors_failed': failed,
                'sector_stats': sector_stats,
                'failure_reason': None if not failed else f"{len(failed)} sector(s) failed",
            }
        except Exception as e:
            logger.error(f"Fatal error in additive process run_id={self.run_id}: {e}")
            logger.error(traceback.format_exc())
            self._audit_finalize()
            return {
                'status': 'FAILED',
                'run_id': self.run_id,
                'batch_index': self.batch_index,
                'sectors_succeeded': succeeded,
                'sectors_failed': failed or sorted(self.active_sectors),
                'sector_stats': sector_stats,
                'failure_reason': f"Unexpected error: {e}",
            }
        finally:
            self._close_connections()

    def _run_batch(self, sectors):
        """Run steps 1-15 for one sector batch. Saves every step's output in TEST mode."""
        self._reset_batch_cache()

        t_fetch_start = _time.perf_counter()

        # --- Step 1: Fare buckets ---
        farebasis_df = self._fetch_fare_buckets()
        self._save_intermediate(farebasis_df, '01_fare_buckets')

        # --- Step 2: Fetch (cached) ---
        self._fetch_all_data()

        t_fetch_end = _time.perf_counter()
        fetch_secs = round(t_fetch_end - t_fetch_start, 2)
        logger.info(f"[TIMING] {sectors} — FETCH (Steps 1-2): {fetch_secs}s")

        t_process_start = _time.perf_counter()

        # --- Step 3: Derive datasets ---
        flown_df = self._derive_flown_data()
        lean_peak_source = self._derive_lean_peak_source()
        if lean_peak_source.empty:
            logger.warning(f"No historical flown data for {sectors} — skipping batch.")
            return
        booked_df = self._derive_booked_data()
        future_flight_df = self._derive_future_flights()
        self._save_intermediate(flown_df, '03a_flown_data')
        self._save_intermediate(lean_peak_source, '03b_lean_peak_source')
        self._save_intermediate(booked_df, '03c_booked_data')
        self._save_intermediate(future_flight_df, '03d_future_flights')

        # --- Step 4: Historical lean/peak tagging ---
        historical_tagging = self._compute_lean_peak_tagging(lean_peak_source)
        self._save_intermediate(historical_tagging, '04_historical_tagging')

        # --- Step 5: Future lean/peak tagging ---
        mid_tc_tagging, mid_tagging = self._compute_future_tagging(lean_peak_source)
        self._save_intermediate(mid_tc_tagging, '05a_future_tagging_tc')
        self._save_intermediate(mid_tagging, '05b_future_tagging_fallback')

        # --- Step 6: Combine + assign fare buckets ---
        combined_df = self._prepare_combined_data(flown_df, booked_df, farebasis_df)
        self._save_intermediate(combined_df, '06_combined_data')

        # --- Step 7: Pivot NDO ---
        pivot_ndo = self._pivot_ndo_data(combined_df, farebasis_df, historical_tagging,
                                         mid_tc_tagging, mid_tagging)
        self._save_intermediate(pivot_ndo, '07_pivot_ndo', fmt='parquet')

        # --- Step 8: Prepare future flights ---
        future_df, future_df_expanded = self._prepare_future_flights(
            future_flight_df, mid_tc_tagging, mid_tagging, farebasis_df)
        self._save_intermediate(future_df, '08a_future_flights_tagged')
        self._save_intermediate(future_df_expanded, '08b_future_flights_expanded')

        # --- Step 9: Flight matching ---
        hist_flight_df = pivot_ndo[
            ['Sector', 'DepartureDate', 'DOW', 'FlightNumber', 'STD', 'Tagging', 'Time Cluster']
        ].drop_duplicates()
        pivot_ndo['Key'] = (
            pivot_ndo['Sector'] + '_' +
            pivot_ndo['DepartureDate'].dt.strftime('%Y-%m-%d') + '_' +
            pivot_ndo['FlightNumber'].astype(str))

        flight_matches = self._find_matching_flights(future_df, hist_flight_df)
        self._save_intermediate(
            pd.DataFrame([{'FutureKey': k, 'MatchCount': len(v)}
                          for k, v in flight_matches.items()]),
            '09_flight_matches_summary')

        # --- Step 10: Build matched result ---
        result_df = self._build_matched_result(future_df_expanded, pivot_ndo, flight_matches)
        if result_df.empty:
            logger.warning(f"No matched results for {sectors} — batch produced no output.")
            return
        self._save_intermediate(result_df, '10_matched_result', fmt='parquet')

        # --- Step 11: Predictions (normal + weighted) ---
        (result_merged_df, result_final_df, _,
         weighted_merged_df, weighted_final_df) = self._calculate_predictions(result_df)
        self._save_intermediate(result_merged_df, '11a_pred_fare_bucket_normal')
        self._save_intermediate(weighted_merged_df, '11b_pred_fare_bucket_weighted')
        self._save_intermediate(result_final_df, '11c_pred_flight_normal')
        self._save_intermediate(weighted_final_df, '11d_pred_flight_weighted')
        del result_df

        # --- Step 12: Remaining RBD distribution ---
        (result_merged_df, result_final_df) = self._distribute_remaining_rbd(
            future_df, pivot_ndo, flight_matches,
            result_merged_df, result_final_df, count_col='predicted_pax_count')
        (weighted_merged_df, weighted_final_df) = self._distribute_remaining_rbd(
            future_df, pivot_ndo, flight_matches,
            weighted_merged_df, weighted_final_df, count_col='weighted_pax_count')
        self._save_intermediate(result_merged_df, '12a_fare_bucket_after_remaining_normal')
        self._save_intermediate(weighted_merged_df, '12b_fare_bucket_after_remaining_weighted')
        self._save_intermediate(result_final_df, '12c_flight_after_remaining_normal')
        self._save_intermediate(weighted_final_df, '12d_flight_after_remaining_weighted')

        t_process_end = _time.perf_counter()
        process_secs = round(t_process_end - t_process_start, 2)
        logger.info(f"[TIMING] {sectors} — PROCESS (Steps 3-12): {process_secs}s")

        t_save_start = _time.perf_counter()

        # --- Step 15: Save final results ---
        self._save_results(result_merged_df, result_final_df, None,
                           weighted_merged_df, weighted_final_df, sectors=sectors)

        t_save_end = _time.perf_counter()
        save_secs = round(t_save_end - t_save_start, 2)
        logger.info(f"[TIMING] {sectors} — SAVE (Step 15): {save_secs}s")
        logger.info(f"[TIMING] {sectors} — TOTAL: fetch={fetch_secs}s, "
                    f"process={process_secs}s, save={save_secs}s, "
                    f"sum={round(fetch_secs + process_secs + save_secs, 2)}s")
    def _audit_upsert_sector(self, sector, stat):
        """Write this sector's timing into the single run document.

        One doc per run in qp_dw_rmalloc.additive_model_run_audit. Each process
        Lambda upserts its own sectors on a disjoint `sector_stats.<sector>`
        path and atomically bumps `sectors_completed`, so parallel bins never
        collide. The next prefetch reads `sector_stats` to bin-pack.
        """
        if self.mode != 'PROD' or self.db is None:
            logger.info(f"[audit] Skipped — mode={self.mode}, db={'ok' if self.db else 'None'}")
            return
        try:
            logger.info(f"[audit] Writing sector={sector} to run_id={self.run_id}")
            result = self.db.additive_model_run_audit.update_one(
                {'run_id': self.run_id},
                {
                    '$set': {
                        f'sector_stats.{sector}': stat,
                        'module': 'additive_pickup_model'
                    },
                    '$setOnInsert': {
                        'start_time': datetime.now(),
                        'status': 'IN_PROGRESS',
                        'snapshot_date': self.current_date.strftime('%Y-%m-%d'),
                        'sectors_expected': self.sectors_expected,
                        'triggered_by': self.analyst,
                    },
                    '$inc': {'sectors_completed': 1},
                    '$addToSet': {'batches_seen': self.batch_index},
                },
                upsert=True,
            )
            logger.info(f"[audit] {sector}: matched={result.matched_count}, "
                        f"modified={result.modified_count}, upserted={result.upserted_id}")
        except Exception as e:
            logger.error(f"audit upsert failed for {sector}: {e}")

    def _audit_finalize(self):
        """Last bin to finish closes the run (idempotent, guarded on IN_PROGRESS).

        Only stamps terminal state once all expected sectors have reported. If
        `sectors_expected` is unknown (standalone run) we can't tell when the
        fan-out is done, so we skip finalisation and leave the doc open.
        """
        if self.mode != 'PROD' or self.db is None:
            return
        try:
            doc = self.db.additive_model_run_audit.find_one(
                {'run_id': self.run_id},
                {'sectors_completed': 1, 'sectors_expected': 1, 'sector_stats': 1},
            )
            if not doc:
                return
            expected = doc.get('sectors_expected')
            completed = len(doc.get('sector_stats', {}))   # distinct sectors, idempotent
            if expected is None or completed < expected:
                return

            stats = doc.get('sector_stats', {}) or {}
            n_fail = sum(1 for v in stats.values() if v.get('status') == 'FAILED')
            result = self.db.additive_model_run_audit.update_one(
                {'run_id': self.run_id, 'status': 'IN_PROGRESS'},   # guard: first wins
                {'$set': {
                    'end_time': datetime.now(),
                    'status': 'SUCCESS' if n_fail == 0 else 'PARTIAL',
                    'sectors_failed_count': n_fail,
                }},
            )
            # Only the invocation that actually flipped the run triggers the DW load.
            if result.modified_count == 1:
                logger.info(f"[run {self.run_id}] finalized "
                            f"({'SUCCESS' if n_fail == 0 else 'PARTIAL'}) — triggering DW load.")
                self._trigger_dw_load()
        except Exception as e:
            logger.error(f"audit finalise failed: {e}")

    def _trigger_dw_load(self):
        """Invoke the S3->DW loader ONCE, after every sector's S3 output for this
        run is written. Fire-and-forget async invoke; failures don't fail the run."""
        fn = getattr(configsettings, 'ADDITIVE_DW_LOADER_LAMBDA', None)
        if not fn:
            logger.warning("ADDITIVE_DW_LOADER_LAMBDA not configured — DW load not triggered.")
            return
        try:
            payload = {'run_id': self.run_id,
                       'snapshot_date': self.current_date.strftime('%Y-%m-%d')}
            boto3.client('lambda').invoke(
                FunctionName=fn, InvocationType='Event',      # async
                Payload=json.dumps(payload).encode('utf-8'))
            logger.info(f"[run {self.run_id}] DW loader invoked ({fn}).")
        except Exception as e:
            logger.error(f"Failed to trigger DW loader: {e}")

    def _close_connections(self):
        """No-op for injected connections.

        The ESB/DW engines and Mongo client are owned by lambda_function.py and
        reused across warm invocations — disposing them here would break the
        next request. We only drop our references to per-run data.
        """
        self._data_cache = {}

    def _save_results(self, result_merged_df, result_final_df, optimized_df,
                      weighted_merged_df=None, weighted_final_df=None, sectors=None):
        """
        TEST -> timestamped CSV locally.
        PROD -> idempotent DW tables + Hive-partitioned S3 (snapshot_date/sector).
        `sectors` scopes the DW delete-before-append so one batch cannot wipe another.

        Derived columns (grain matters — see _distribute_remaining_rbd):
          Predicted_Total_Bookings  = Current_Bookings + Total_Predicted_pax_count
                                      -> FARE-BUCKET frames (they carry Total_Predicted_pax_count)
          Load_Factor               = Final_Total_Bookings / Capacity
                                      -> FLIGHT-LEVEL frames (they carry Final_Total_Bookings)
        Both are guarded so a run with no remaining-RBD matches (which early-returns
        the pre-distribution frame) skips them instead of KeyError-ing.
        """
        timestamp = self.today.strftime('%Y%m%d_%H%M%S')

        # ---------------- column definitions ----------------
        fare_bucket_cols = [
            'SnapshotDate', 'run_id', 'Sector', 'DepartureDate', 'DOW', 'STD',
            'FlightNumber', 'Capacity', 'Current_Bookings', 'Tagging', 'FareBucket',
            'future_flight_NDO', 'predicted_pax_count', 'predicted_pax_std',
            'Remaining_pax_count', 'Remaining_pax_distribution', 'Final_pax_count',
            'Total_Predicted_pax_count', 'Predicted_Total_Bookings',
            'FareBasis_Prediction_ratio', 'FareBucket_Lower', 'FareBucket_Upper']

        flight_level_cols = [
            'SnapshotDate', 'run_id', 'Sector', 'DepartureDate', 'DOW', 'STD',
            'FlightNumber', 'Capacity', 'Current_Bookings', 'Tagging',
            'future_flight_NDO', 'predicted_pax_count', 'predicted_pax_std_square',
            'Remaining_pax_count', 'Final_pax_count', 'Final_Total_Bookings',
            'Load_Factor']

        weighted_fare_bucket_cols = [
            'SnapshotDate', 'run_id', 'Sector', 'DepartureDate', 'DOW', 'STD',
            'FlightNumber', 'Capacity', 'Current_Bookings', 'Tagging', 'FareBucket',
            'future_flight_NDO', 'weighted_pax_count', 'weighted_pax_std',
            'Remaining_pax_count', 'Remaining_pax_distribution', 'Final_pax_count',
            'Total_Predicted_pax_count', 'Predicted_Total_Bookings',
            'FareBasis_Prediction_ratio', 'FareBucket_Lower', 'FareBucket_Upper']

        weighted_flight_level_cols = [
            'SnapshotDate', 'run_id', 'Sector', 'DepartureDate', 'DOW', 'STD',
            'FlightNumber', 'Capacity', 'Current_Bookings', 'Tagging',
            'future_flight_NDO', 'weighted_pax_count', 'weighted_pax_std_square',
            'Remaining_pax_count', 'Final_pax_count', 'Final_Total_Bookings',
            'Load_Factor']

        optimization_cols = [
            'SnapshotDate', 'run_id', 'Sector', 'DepartureDate', 'DOW', 'STD',
            'FlightNumber', 'Capacity', 'Current_Bookings', 'Tagging', 'FareBucket',
            'FareBucket_Lower', 'FareBucket_Upper', 'FareMidpoint',
            'predicted_pax_count', 'predicted_pax_std', 'ProtectionLimit', 'AU']

        # ---------------- helpers (parse bounds BEFORE the FareBasis rename) ----------------
        FLIGHT_RENAME = {
            'DepartureDate_future': 'DepartureDate',
            'STD_future': 'STD',
            'FlightNumber_future': 'FlightNumber',
            'Total Bookings': 'Current_Bookings',
            'remaining_pax_count': 'Remaining_pax_count'}
        BUCKET_RENAME = dict(FLIGHT_RENAME, **{'FareBasis': 'FareBucket'})

        def _parse_bucket_bounds(df):
            if df is None or 'FareBasis' not in df.columns:
                return df
            if 'FareBucket_Lower' not in df.columns:
                df = df.copy()
                df['FareBucket_Lower'] = (
                    df['FareBasis'].str.replace('+', '', regex=False)
                    .str.split('-').str[0].astype(float))
                df['FareBucket_Upper'] = pd.to_numeric(
                    df['FareBasis'].str.split('-').str[1], errors='coerce')
            return df

        def _finalise(df, rename_map):
            if df is None:
                return None
            df = df.rename(columns=rename_map)
            df['SnapshotDate'] = self.current_date
            df['run_id'] = self.run_id          # constant for the whole run
            return df

        def _subset(df, cols):
            return df[[c for c in cols if c in df.columns]]

        # ---------------- prepare every frame identically ----------------
        result_merged_df = _parse_bucket_bounds(result_merged_df)
        weighted_merged_df = _parse_bucket_bounds(weighted_merged_df)
        optimized_df = _parse_bucket_bounds(optimized_df)

        result_final_df = _finalise(result_final_df, FLIGHT_RENAME)
        result_merged_df = _finalise(result_merged_df, BUCKET_RENAME)
        weighted_final_df = _finalise(weighted_final_df, FLIGHT_RENAME)
        weighted_merged_df = _finalise(weighted_merged_df, BUCKET_RENAME)
        optimized_df = _finalise(optimized_df, BUCKET_RENAME)

        # ---------------- derived columns (correct grain, guarded) ----------------
        # Predicted_Total_Bookings — FARE-BUCKET frames
        for _df in (result_merged_df, weighted_merged_df):
            if _df is not None and {'Current_Bookings',
                                    'Total_Predicted_pax_count'}.issubset(_df.columns):
                _df['Predicted_Total_Bookings'] = (
                    _df['Current_Bookings'] + _df['Total_Predicted_pax_count'])

        # Load_Factor — FLIGHT-LEVEL frames (ratio, not sum)
        for _df in (result_final_df, weighted_final_df):
            if _df is not None and {'Final_Total_Bookings',
                                    'Capacity'}.issubset(_df.columns):
                _df['Load_Factor'] = np.where(
                    _df['Capacity'] > 0,
                    _df['Final_Total_Bookings'] / _df['Capacity'],
                    np.nan)

        # (frame, trimmed cols, s3 dataset, dw table)
        DATASETS = [
            (result_final_df,    flight_level_cols,          'flight_level_normal',
             'add_model_flight_level'),
            (result_merged_df,   fare_bucket_cols,           'fare_bucket_normal',
             'add_model_fare_bucket_level'),
            (weighted_final_df,  weighted_flight_level_cols, 'flight_level_weighted',
             'add_model_flight_level_weighted'),
            (weighted_merged_df, weighted_fare_bucket_cols,  'fare_bucket_weighted',
             'add_model_fare_bucket_level_weighted')]

        # =====================================================================
        # TEST — timestamped CSV in the local output directory
        # =====================================================================
        if self.mode == 'TEST':
            logger.info(f"Saving results to CSV in: {self.output_dir}")

            for frame, cols, name, _dw in DATASETS:
                if frame is None or frame.empty:
                    continue
                self._save_to_csv(_subset(frame, cols), f'{name}_{timestamp}.csv')

            if optimized_df is not None and not optimized_df.empty:
                self._save_to_csv(_subset(optimized_df, optimization_cols),
                                  f'inventory_optimization_{timestamp}.csv')

            # Full frames (all columns incl. NDO 0-180) -> parquet, not CSV.
            for frame, _cols, name, _dw in DATASETS:
                if frame is None or frame.empty:
                    continue
                self._save_to_parquet(frame, f'{name}_full_{timestamp}.parquet')

            logger.info("All TEST results saved successfully.")
            return

        # =====================================================================
                # PROD — S3 ONLY, partitioned by snapshot_date / sector
                # (DW writes removed; each sector's partition is written independently
                #  so parallel workers never conflict)
                # =====================================================================
        logger.info(f"Saving to partitioned S3 "
                    f"({len(sectors) if sectors else '?'} sectors)...")

        for frame, cols, s3_dataset, _dw in DATASETS:
            if frame is None or frame.empty:
                logger.warning(f"{s3_dataset}: empty, skipped.")
                continue
            # full frame (incl. NDO curve) to S3; the trimmed `cols` are for
            # a lighter query-facing copy if you want one later.
            self._write_partitioned_s3(frame, s3_dataset)

        if optimized_df is not None and not optimized_df.empty:
            self._write_partitioned_s3(optimized_df, 'inventory_optimization')

        logger.info("All PROD results saved to S3 successfully.")