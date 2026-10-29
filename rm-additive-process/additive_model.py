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
from Constants import *
from helpers.utils.common_utils import *
from helpers.db_helpers.additive_esb_helper import AdditiveEsbHelper
from helpers.db_helpers.additive_mongo_helper import AdditiveMongoHelper
from helpers.db_helpers.additive_dw_helper import AdditiveDwHelper

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

        # Injected ConstantsUtils — the single source of truth for tunable
        # constants (DB overrides applied, correct casts). Model logic must read
        # constants from here, NOT the module-level Constants.py values.
        self.constants_utils = params.get('constantutils')

        # --- time anchors ---
        self.today = datetime.now()
        self.current_date = pd.to_datetime(self.today.date())
        self.prediction_limit = self.current_date + pd.Timedelta(
            days=self.constants_utils.PREDICTION_HORIZON_DAYS)
        self.one_year_back = self.current_date - pd.Timedelta(days=365)

        # =============================================================================
        # RUN MODE
        # =============================================================================
        # 'PROD' -> S3 output + audit writes. 'TEST' -> local CSV/parquet, no audit.
        # Overridable per-invocation via overrides['mode'].
        RUN_MODE = 'PROD'

        # Sector list used only in TEST mode (keeps a local run tiny).
        TEST_SECTORS = ['BOMDEL', 'DELBOM']
        self.test_sectors = TEST_SECTORS
        TEST_OUTPUT_DIR = os.path.join(os.getcwd(), 'additive_model_output')


        # --- run mode ---
        self.mode = (overrides.get('mode') or RUN_MODE).upper()
        self.output_dir = TEST_OUTPUT_DIR
        if self.mode == 'TEST':
            os.makedirs(self.output_dir, exist_ok=True)
            logger.info(f"Running in TEST mode — output -> {self.output_dir}")

        # --- injected connections (NO SSM / engine creation here) ---
        self.esb_engine = params.get('rdEsbConn')
        self.dw_engine = params.get('dwconn')   # RM_DW_DATA — currency_conversion
        self.s3_client = params.get('s3')
        # self.db_audit = params.get('mongo')          # qp_dw_priaut
        # self.db_fares = params.get('mongofares')     # qp_dw_rmalloc
        # self.db_master = params.get('mongo_master')  # qp_rm_master
        self.db = params['mongo']
        self.master_db = params.get('mongo_master', params['mongo'])

        self.esb_helper = AdditiveEsbHelper(self.esb_engine, logger)
        self.dw_helper = AdditiveDwHelper(self.dw_engine, logger)
        self.mongo_helper = AdditiveMongoHelper(self.db, self.master_db, logger,
                                                dw_helper=self.dw_helper)

        self.s3_bucket = configsettings.ADDITIVE_MODEL_BUCKET
        self.s3_cache_prefix = f'{configsettings.ADDITIVE_MODEL_FILE_PATH}_cache'
        self.s3_output_prefix = f'{configsettings.ADDITIVE_MODEL_FILE_PATH}datasets'
        CACHE_DIR = os.path.join(os.getcwd(), 'additive_model_cache')
        self.local_cach_dir = CACHE_DIR if self.mode == 'TEST' else None

        # --- shared cache (mode-aware) + bind helpers ---
        self.cache = AdditiveCache(self.mode, self.s3_client, logger)
        self.cache.s3_bucket = self.s3_bucket
        self.cache.s3_cache_prefix = self.s3_cache_prefix
        self.cache.s3_output_prefix = self.s3_output_prefix
        self.cache.local_dir = self.local_cach_dir or os.path.join(os.getcwd(), 'additive_model_cache')
        self.esb_helper.bind_context(self.cache)
        self.dw_helper.bind_context(self.cache)
        self.mongo_helper.bind_context(self.cache, self.mode)

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

        # audit run-context for the mongo helper
        self.mongo_helper.set_run(self.run_id, self.sectors_expected, self.analyst,
                                  self.batch_index, self.current_date.strftime('%Y-%m-%d'))

        # Explicit sectors (from overrides) win in ANY mode — incl. TEST.
        bin_sectors = overrides.get('sectors')
        if bin_sectors:
            self.active_sectors = list(bin_sectors)
        elif self.mode == 'TEST':
            self.active_sectors = list(self.test_sectors)
        else:
            self.active_sectors = []   # standalone full run -> fetch all active

        # --- mapped-sector fallback (N:1 donor) ---------------------------------
        # New contract (per 1-sector message):
        #   * a DONOR message carries `dependent_sectors` = [dep, dep, ...]. The
        #     donor processes normally, then re-enqueues each dependent (see run()).
        #   * a DEPENDENT message carries `mapped_sector` = donor. It borrows the
        #     (now warm) donor history via self.mapped_sectors[dep] = donor.
        # In TEST / standalone (no SQS metadata), fall back to the config map.
        self.dependent_sectors = list(overrides.get('dependent_sectors') or [])
        self.mapped_sector = overrides.get('mapped_sector')   # donor for THIS run

        self.mapped_sectors = {}     # {dependent_sector: donor}
        if self.mapped_sector:
            for s in self.active_sectors:
                self.mapped_sectors[s] = self.mapped_sector
        if not self.mapped_sectors and not self.dependent_sectors:
            # No explicit metadata (TEST or standalone) — pull the full config map
            # and keep only mappings whose dependent we are actually processing.
            try:
                full_map = self.mongo_helper.load_mapped_sectors()   # {dep: donor}
                self.mapped_sectors = {s: m for s, m in full_map.items()
                                       if s in set(self.active_sectors)}
            except Exception as e:
                logger.warning(f"[MappedSectors] unavailable: {e.__class__.__name__}: {e}")
                self.mapped_sectors = {}
        if self.mapped_sectors:
            logger.info(f"[MappedSectors] active fallbacks (dependent->donor): {self.mapped_sectors}")
        if self.dependent_sectors:
            logger.info(f"[MappedSectors] this donor {self.active_sectors} carries "
                        f"dependents to re-enqueue: {self.dependent_sectors}")

        # Guards the one-shot parallel dependent fan-out (post-cache). See
        # _enqueue_dependents_after_cache.
        self._deps_enqueued = False

        self.cache.data = {}

# =============================================================================
# Resolve the spec into a concrete list of 'YYYY-MM' strings
# =============================================================================



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
            q = self.esb_helper.build_flown_query(
                f"qdb.DepartureDate BETWEEN '{first.date()}' AND '{last.date()}'", conds)
            df = run_sql(engine, q)
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
        self.cache.write_sector_cache('hist_flown', fresh, months=set(months))
        logger.info(f"[Refresh] Rewrote {len(months)} hist_flown partitions "
                    f"for {fresh['Sector'].nunique()} sectors.")

    # =========================================================================
    # STEP 0: FETCH ACTIVE SECTORS
    # =========================================================================


       

    # =========================================================================
    # CONSOLIDATED DATA FETCHING (incremental with local parquet cache)
    # =========================================================================







    def _sector_other_taxes(self, sector):
        """
        Pass-through (non-airline) taxes and the applicable GST rate for a sector.
        These are exactly the components to strip when going all-in -> airline comp.
        """
        sector = str(sector).strip()
        if len(sector) < 6:
            return 0.0, 0.0, False

        origin, destination = sector[:3], sector[3:6]

        duf_sectors = self.cache.data.get('pricing_duf_sectors', {})
        gst_sectors = self.cache.data.get('pricing_gst_sectors', set())
        intl_map = self.cache.data.get('tax_mast_intl', {})
        dom_map = self.cache.data.get('tax_mast_dom', {})

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
        df = self.cache.data['hist_raw_df'].copy()
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
        df = self.cache.data['hist_raw_df'].copy()

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
        df = self.cache.data['forward_raw_df'].copy()

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

        fm_df = self.cache.data['future_flight_master'].copy()
        df = self.cache.data['forward_raw_df']

        merge_keys = ['Sector', 'DepartureDate', 'STD', 'FlightNumber']

        # No future schedule for this sector -> nothing to forecast. Fail with a
        # clear, actionable reason instead of letting an empty-frame merge raise a
        # cryptic "datetime64 vs object" dtype error downstream.
        if fm_df.empty:
            raise RuntimeError(
                f"No future flights in the schedule for sector(s) "
                f"{self.active_sectors} within the prediction window "
                f"[{self.current_date.date()}..{self.prediction_limit.date()}] — "
                f"nothing to forecast.")

        if df.empty:
            bookings = pd.DataFrame(columns=merge_keys + ['Pax Count', 'Fare'])
        else:
            rev_df = df[df['Pax Count'] > 0]
            bookings = rev_df.groupby(merge_keys, dropna=False).agg(
                {'Pax Count': 'sum', 'Fare': 'sum'}
            ).reset_index()

        # Align dtypes on merge keys before merging. In pandas 1.x/2.x, groupby
        # with dropna=False and empty frames can silently change column dtypes
        # (datetime64 -> object, or time-object columns inferred as datetime64
        # when all-None), which makes the merge raise "datetime64 vs object". Coerce
        # BOTH sides — including an empty `bookings` frame — to merge-safe types.
        for target in (fm_df, bookings):
            target['DepartureDate'] = pd.to_datetime(target['DepartureDate'], errors='coerce')
            # STD holds datetime.time objects (or None); convert to string to avoid
            # pandas type-inference issues on merge. Downstream _prepare_future_flights
            # already converts str -> time via pd.to_datetime(..., format='%H:%M:%S').
            target['STD'] = target['STD'].apply(
                lambda x: str(x) if pd.notna(x) else None)

        future_df = fm_df.merge(bookings, on=merge_keys, how='left')
        future_df.rename(columns={'Pax Count': 'Total Bookings', 'Fare': 'Current_Revenue'}, inplace=True)
        future_df['Total Bookings'] = future_df['Total Bookings'].fillna(0).astype(int)
        future_df['Current_Revenue'] = future_df['Current_Revenue'].fillna(0).round(2)

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
            fares_df = self.mongo_helper.fetch_fares(
                self.active_sectors, self.constants_utils.FARE_BUCKET_RBDS)

            # --- Step A: all-in -> airline component, IN LOCAL CURRENCY ---
            # For an international sector both the QP fare (TOTAL) and the intl tax
            # master are in the origin's local currency, so the tax stripping must
            # happen while everything is still local. Domestic sectors are already
            # INR, so this is a no-op for them currency-wise.
            fares_df = self._fares_to_airline_component(fares_df, fare_col='TOTAL')
            if fares_df.empty:
                raise RuntimeError("No fares left after airline-component conversion")

            # --- Step B: convert the (local-currency) airline component -> INR ---
            # Only international-origin sectors are converted; domestic pass through.
            fares_df = self.mongo_helper.apply_currency_conversion(
                fares_df, fare_col='AirlineComponent')

            # GP logic operates on the (now INR) airline component from here on
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
            self.cache.data['farebasis_df'] = farebasis_df
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
        n_points = self.constants_utils.GP_FARE_BUCKET_POINTS

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
            self.cache.read_sector_cache('tagging', self.active_sectors)

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
            touched_months.add(self.cache.month_key(yday))
            # rolling floor advances daily: the floor month loses its oldest day,
            # so that partition must be rewritten too.
            touched_months.add(self.cache.month_key(retention_floor))
            logger.info(f"[Tagging] Appended {len(y_tags)} rows for {yday.date()}.")

        hist_tags = hist_tags.drop_duplicates(
            subset=['Sector', 'DepartureDate', 'FlightNumber', 'Time Cluster'], keep='last')

        # --- retention: drop whole partitions below the rolling floor,
        #     rewrite the floor month if it lost rows ---
        before = len(hist_tags)
        hist_tags = hist_tags[hist_tags['DepartureDate'] >= retention_floor]
        floor_month = self.cache.month_key(retention_floor)
        if len(hist_tags) != before:
            for s in self.active_sectors:
                for m in self.cache.list_cache_months('tagging', s):
                    if m < floor_month:
                        self.cache.delete_cache_partition('tagging', s, m)
            if touched_months is not None:
                touched_months.add(floor_month)

        self.cache.write_sector_cache('tagging', hist_tags, months=touched_months)

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
            dict of 7 fallback frames keyed by level name (q_dow_tc, q_dow,
            q_tc, q, dow_tc, dow, sector). MP-sector levels are resolved at
            fill-time by looking up the donor sector in these same frames.
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

        # 7 grouping shapes per the BRD fallback ladder (coarsest last).
        # 'TC' == Time Cluster. Each returns a frame [<keys>, 'Tagging'].
        levels = self._future_tag_levels(tags)

        logger.info("Future tagging computed (7 levels): " + ", ".join(
            f"{name}={len(df)}" for name, df in levels.items()))
        return levels

    # BRD future-tagging fallback ladder (per sector scope). Order matters:
    # each is a coarser 50th-percentile grouping than the previous.
    FUTURE_TAG_LEVELS = [
        ('q_dow_tc', ['Sector', 'Quarter', 'DOW', 'Time Cluster']),
        ('q_dow',    ['Sector', 'Quarter', 'DOW']),
        ('q_tc',     ['Sector', 'Quarter', 'Time Cluster']),
        ('q',        ['Sector', 'Quarter']),
        ('dow_tc',   ['Sector', 'DOW', 'Time Cluster']),
        ('dow',      ['Sector', 'DOW']),
        ('sector',   ['Sector']),
    ]
    def _future_tag_levels(self, tags):
        """Compute the 7-level 50th-percentile tagging frames from a tags frame.
        A group with < FUTURE_TAG_MIN_ENTRIES rows is dropped from that level so
        the fill step falls through to a coarser (better-supported) level."""
        min_entries = self.constants_utils.FUTURE_TAG_MIN_ENTRIES
        out = {}
        for name, keys in self.FUTURE_TAG_LEVELS:
            t = tags.sort_values(by=keys + ['Tagging_int'],
                                 ascending=[True] * len(keys) + [False])
            grp = t.groupby(keys, sort=False)
            mid = grp['Tagging'].apply(middle_tagging).reset_index(name='Tagging')
            counts = grp.size().reset_index(name='_n')
            mid = mid.merge(counts, on=keys, how='left')
            mid = mid[mid['_n'] >= min_entries].drop(columns='_n')
            out[name] = mid
        return out

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
                # Reduce the lowest lower bound (display label stays the same)
                ranges[0] = (lowest_lower * self.constants_utils.FARE_BUCKET_LOWER_FACTOR,
                             lowest_upper, lowest_label)
                sector_ranges[sector] = ranges

        # Donor (mapped) history rows keep Sector=donor and have NO buckets of
        # their own (buckets are computed for the dependent only). Bucket a donor
        # row using the DEPENDENT's ranges so its fares land in the same bucket
        # structure the dependent will match against. `mapped_sectors` is
        # {dependent: donor}; invert it to resolve a donor row's dependent.
        donor_to_dependent = {donor: dep for dep, donor in self.mapped_sectors.items()}

        # Vectorized fare bucket assignment
        df['AvgFare'] = df['Fare'] / df['Pax Count']

        def assign_fare_bucket(row):
            sector = row['Sector']
            value = row['AvgFare']
            # A donor row borrows the dependent's fare-bucket ranges.
            ranges_sector = sector if sector in sector_ranges else donor_to_dependent.get(sector)
            ranges = sector_ranges.get(ranges_sector)
            if not ranges:
                return ''
            for lower, upper, label in ranges:
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
                        tag_levels):
        """
        Pivot the required RBD data by NDO, impute missing fare buckets,
        fill zeros for past booking dates, and merge lean/peak tagging.
        """
        logger.info("Pivoting NDO data for required RBDs...")
        required_rbds = self.constants_utils.REQUIRED_RBDS
        # Split into required and remaining
        combined_df = combined_df[(combined_df['NDO'] >= 0) & (combined_df['NDO'] <= 180)]
        combined_df = combined_df[(combined_df['DepartureDate'] >= self.one_year_back)]
        main_df = combined_df[
            (combined_df['RBD'].isin(required_rbds)) & (combined_df['FareBasis'] != '')
        ].copy()
        remaining_df = combined_df[
            ~combined_df['RBD'].isin(required_rbds) | (combined_df['FareBasis'] == '')
        ].copy()

        self.cache.data['remaining_df'] = remaining_df

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
        for i in self.constants_utils.NDO_RANGE:
            if i not in pivot_ndo.columns:
                pivot_ndo[i] = np.nan

        ndo_cols = sorted([c for c in pivot_ndo.columns if isinstance(c, (int, np.integer))], key=int)
        self.cache.data['ndo_cols'] = ndo_cols

        # Impute missing fare buckets per flight
        logger.info("Imputing missing fare buckets per flight...")
        combo_main = main_df[['Sector', 'DepartureDate', 'FlightNumber', 'DOW', 'STD']].drop_duplicates()
        combo_fare = farebasis_df[['Sector', 'FareBasis']].drop_duplicates()
        # Donor (mapped) sectors have no buckets of their own; give each donor the
        # DEPENDENT's bucket list so donor history is imputed across the same
        # bucket structure the dependent matches against. {dependent: donor}.
        if self.mapped_sectors:
            donor_bucket_rows = []
            for dep, donor in self.mapped_sectors.items():
                dep_buckets = combo_fare[combo_fare['Sector'] == dep]['FareBasis'].tolist()
                for fb in dep_buckets:
                    donor_bucket_rows.append({'Sector': donor, 'FareBasis': fb})
            if donor_bucket_rows:
                combo_fare = pd.concat(
                    [combo_fare, pd.DataFrame(donor_bucket_rows)], ignore_index=True
                ).drop_duplicates()
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

        # Fill NaN tagging with future tagging lookup (14-level ladder)
        pivot_ndo['Quarter'] = pivot_ndo['DepartureDate'].apply(get_quarter_abbr)
        self._fill_nan_tagging(pivot_ndo, tag_levels)

        logger.info(f"Pivot NDO data ready: {len(pivot_ndo)} rows, "
                    f"remaining NaN tagging: {pivot_ndo['Tagging'].isna().sum()}")
        return pivot_ndo, remaining_df
    
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


    def _fill_nan_tagging(self, df, tag_levels):
        """Fill NaN tagging (in-place) via the 14-level BRD ladder:
        7 own-sector levels (Sector-Quarter-DOW-TC ... Sector), then the same 7
        on the MAPPED (donor) sector for any sector with a 1:1 mapping.
        `tag_levels` is the dict returned by _compute_future_tagging."""

        def _apply(level_name, join_keys, sector_col):
            """Fill remaining NaNs from tag_levels[level_name], joining df[sector_col]
            (real sector or mapped donor) onto the lookup's 'Sector'."""
            na_mask = df['Tagging'].isna()
            if na_mask.sum() == 0:
                return
            lookup = tag_levels.get(level_name)
            if lookup is None or lookup.empty:
                return
            lookup = lookup.rename(columns={'Tagging': '_tag_lk'})
            # left-side keys: the sector column may be the donor ('_MP_Sector')
            left_keys = [sector_col] + [k for k in join_keys if k != 'Sector']
            na_rows = df.loc[na_mask, left_keys].copy()
            na_rows = na_rows.rename(columns={sector_col: 'Sector'})
            merged = na_rows.merge(lookup, on=join_keys, how='left')
            df.loc[na_mask, 'Tagging'] = merged['_tag_lk'].values

        # --- own-sector ladder (levels 1-7) ---
        for name, keys in self.FUTURE_TAG_LEVELS:
            _apply(name, keys, 'Sector')

        # --- mapped-sector ladder (levels 8-14) ---
        if self.mapped_sectors and df['Tagging'].isna().any():
            df['_MP_Sector'] = df['Sector'].map(self.mapped_sectors)
            if df['_MP_Sector'].notna().any():
                for name, keys in self.FUTURE_TAG_LEVELS:
                    _apply(name, keys, '_MP_Sector')
            df.drop(columns=['_MP_Sector'], inplace=True, errors='ignore')

    # =========================================================================
    # STEP 8: PREPARE FUTURE FLIGHTS WITH TAGGING
    # =========================================================================

    def _prepare_future_flights(self, future_df, tag_levels, farebasis_df):
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

        # Assign tagging via the full 14-level ladder (level 1 = Sector-Quarter-DOW-TC).
        # Object dtype (not float NaN) so the ladder can assign string tags without
        # the pandas "incompatible dtype" FutureWarning.
        future_df['Tagging'] = pd.Series(pd.NA, index=future_df.index, dtype='object')
        self._fill_nan_tagging(future_df, tag_levels)

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

    # All tag ints, used to detect when a ±N window already covers every tag.
    _ALL_TAG_INTS = {0, 1, 2, 3}

    # Reason codes for future flights that matched at NO ladder level (Step 9).
    # Stored per run in qp_dw_rmalloc.additive_model_unmatched_flights as
    # sectors.<SECTOR>.<REASON> = ['YYYY-MM-DD_FLTNO', ...]; counts per reason
    # also go to additive_model_run_audit.sector_stats.<SECTOR>.unmatched_by_reason.
    #
    # Unmapped sector (no donor):
    #   NO_HIST       - sector has NO flown history at all.
    #   OWN_LT_MIN    - sector has flown history, but fewer than MIN_FLIGHT_MATCHES
    #                   even at the widest level (whole sector, all taggings).
    # Mapped (dependent) sector — own ladder failed, then the donor was tried:
    #   DONOR_NO_HIST - donor has NO flown history (e.g. donor cache missing), so
    #                   the fallback had nothing to offer. Check the donor's data.
    #   DONOR_LT_MIN  - donor has flown history, but its ladder (DOW-Tagging ±Nh,
    #                   Tagging) also fell short of MIN_FLIGHT_MATCHES.
    UNMATCHED_NO_HIST = 'NO_HIST'
    UNMATCHED_OWN_LT_MIN = 'OWN_LT_MIN'
    UNMATCHED_DONOR_NO_HIST = 'DONOR_NO_HIST'
    UNMATCHED_DONOR_LT_MIN = 'DONOR_LT_MIN'

    @staticmethod
    def _tag_window(tag, n):
        """Set of tag strings within +-n of `tag` on the numerical scale."""
        base = numerical_tagging(tag)
        names = {'Lean': 0, 'Shoulder': 1, 'Peak': 2, 'Super Peak': 3}
        return {name for name, i in names.items() if abs(i - base) <= n}

    def _find_matching_flights(self, future_df, hist_flight_df):
        """
        For each future flight, walk the BRD flown-flight fallback ladder to gather
        >= MIN_FLIGHT_MATCHES historical (FLOWN) matches, then apply the SAME winning
        group to the future (UNFLOWN) flights with NO minimum. `self.mapped_sectors`
        supplies a 1:1 donor sector for the final MP fallback levels.

        Returns {future_key: [historical/future match keys]}.
        """
        logger.info("Building flight match dictionary (flown ladder + MP fallback)...")
        min_matches = self.constants_utils.MIN_FLIGHT_MATCHES

        hist = hist_flight_df.copy()
        hist['Key'] = (
            hist['Sector'] + '_' +
            hist['DepartureDate'].dt.strftime('%Y-%m-%d') + '_' +
            hist['FlightNumber'].astype(str)
        )

        def to_minutes(t):
            return t.hour * 60 + t.minute if pd.notna(t) else np.nan

        hist['STD_min'] = hist['STD'].apply(to_minutes)

        # Split flown (complete NDO curve) vs unflown (future) once.
        flown = hist[hist['DepartureDate'] < self.current_date].copy()
        unflown = hist[hist['DepartureDate'] >= self.current_date].copy()

        time_band_hours = self.constants_utils.TIME_BAND_HOURS   # +-Nh band on DOW-Tagging levels

        def _select(pool, sector, tag_set, dow, future_min, time_band):
            """Apply one group definition to a candidate pool. tag_set=None means
            all taggings; dow=None means any DOW; time_band=None means any STD."""
            m = pool['Sector'] == sector
            if tag_set is not None:
                m &= pool['Tagging'].isin(tag_set)
            if dow is not None:
                m &= pool['DOW'] == dow
            cand = pool[m]
            if time_band is not None and pd.notna(future_min) and not cand.empty:
                diffs = (cand['STD_min'] - future_min).abs()
                diffs = diffs.where(diffs <= 720, 1440 - diffs)   # midnight wrap
                cand = cand[diffs <= time_band * 60]
            return cand

        # Cap on matches kept per future flight. Coarse ladder levels
        # (Sector-all-taggings) can return the ENTIRE sector history (hundreds of
        # flights); Step 10 then explodes to (future x buckets x matches x 181 NDO
        # cols). Keeping the closest-STD, then most-recent N bounds that product
        # with negligible effect on the averaged prediction. DB-overridable via
        # MAX_FLIGHT_MATCHES_KEY; getattr fallback keeps it working before the
        # deployed constants file is updated. 0 / None => no cap.
        max_matches = getattr(self.constants_utils, 'MAX_FLIGHT_MATCHES', 60)

        def _cap(cand, future_min):
            """Keep the closest-STD, then most-recent, `max_matches` rows.
            Ordering is deterministic so the kept set is stable across runs."""
            if not max_matches or len(cand) <= max_matches:
                return cand
            c = cand.copy()
            if pd.notna(future_min):
                d = (c['STD_min'] - future_min).abs()
                c['_std_prox'] = d.where(d <= 720, 1440 - d)   # midnight wrap
            else:
                c['_std_prox'] = 0
            # closest STD first, then most-recent departure, then Key for stability
            c = c.sort_values(['_std_prox', 'DepartureDate', 'Key'],
                              ascending=[True, False, True]).head(max_matches)
            return c.drop(columns='_std_prox')

        def _ladder(sector, tagging, dow, future_min, is_mp):
            """Yield (tag_set, dow, time_band) group definitions in BRD order.
            Own-sector: DOW-Tagging(+-2h) -> Tagging -> +-1 -> +-2 -> ... -> all.
            MP-sector: only DOW-Tagging(+-2h) -> Tagging."""
            # 1) Sector - DOW - Tagging  (+-Nh band)
            yield ({tagging}, dow, time_band_hours)
            # 2) Sector - Tagging  (any DOW, any time)
            yield ({tagging}, None, None)
            if is_mp:
                return
            # 3..: Sector - (+-N Tagging) until the window covers all tags
            n = 1
            prev = {tagging}
            while True:
                win = self._tag_window(tagging, n)
                if win != prev:
                    yield (win, None, None)
                    prev = win
                covered = {numerical_tagging(t) for t in win}
                if covered >= self._ALL_TAG_INTS:
                    break
                n += 1
            # final) Sector - (All taggings)
            yield (None, None, None)

        matches_dict = {}
        donor_won = {}   # {future_key: (donor_sector, dependent_sector)} — fallback wins
        # Future flights that failed EVERY ladder level, grouped as
        # {sector: {reason: ['YYYY-MM-DD_FLTNO', ...]}}. Sector prefix is dropped
        # from the key (it is the outer dict key) to keep the audit compact.
        unmatched_keys = {}
        # Sectors with ANY flown history — computed once so the per-flight reason
        # lookup is O(1). The last own-ladder level is "whole sector, all
        # taggings", so a sector WITH history that still fails is short of
        # min_matches in total (OWN_LT_MIN), not missing history.
        flown_sectors = set(flown['Sector'].unique())
        skipped = 0

        for _, future_row in future_df.iterrows():
            future_key = future_row['Key']
            sector = future_row['Sector']
            dow = future_row['DOW']
            tagging = future_row['Tagging']
            future_min = to_minutes(future_row['STD'])
            donor = self.mapped_sectors.get(sector)

            winning = None   # (scope_sector, tag_set, dow, time_band)

            # --- own-sector flown ladder ---
            for tag_set, dow_used, band in _ladder(sector, tagging, dow, future_min, is_mp=False):
                cand = _select(flown, sector, tag_set, dow_used, future_min, band)
                if len(cand) >= min_matches:
                    winning = (sector, tag_set, dow_used, band)
                    break

            # --- MP-sector flown ladder (DOW-Tagging, Tagging only) ---
            if winning is None and donor:
                for tag_set, dow_used, band in _ladder(donor, tagging, dow, future_min, is_mp=True):
                    cand = _select(flown, donor, tag_set, dow_used, future_min, band)
                    if len(cand) >= min_matches:
                        winning = (donor, tag_set, dow_used, band)
                        break

            if winning is None:
                skipped += 1
                # Reason = the last pool that was tried and fell short (codes
                # documented at UNMATCHED_* above).
                if donor:
                    reason = (self.UNMATCHED_DONOR_LT_MIN if donor in flown_sectors
                              else self.UNMATCHED_DONOR_NO_HIST)
                elif sector in flown_sectors:
                    reason = self.UNMATCHED_OWN_LT_MIN
                else:
                    reason = self.UNMATCHED_NO_HIST
                (unmatched_keys.setdefault(sector, {})
                               .setdefault(reason, [])
                               .append(future_key[len(sector) + 1:]))
                continue

            # --- apply the winning group to BOTH flown and unflown pools ---
            scope_sector, tag_set, dow_used, band = winning
            flown_match = _cap(
                _select(flown, scope_sector, tag_set, dow_used, future_min, band), future_min)
            unflown_match = _cap(
                _select(unflown, scope_sector, tag_set, dow_used, future_min, band), future_min)
            keys = flown_match['Key'].tolist() + unflown_match['Key'].tolist()
            matches_dict[future_key] = keys

            # Record when a future flight was matched via its DONOR (fallback),
            # not its own history. The matched rows carry Sector=donor; the merge
            # steps (10 & 12) relabel exactly these rows to the dependent so the
            # Sector-keyed joins line up. Own-sector wins are NOT recorded, so a
            # dependent with enough own data behaves exactly like a standalone run.
            if donor and scope_sector == donor:
                donor_won[future_key] = (donor, sector)

        self._donor_won = donor_won
        self._unmatched_keys = unmatched_keys   # consumed by run() for the audit

        total_matches = sum(len(v) for v in matches_dict.values())
        logger.info(f"Flight matching complete: {len(matches_dict)} future flights matched, "
                    f"{skipped} skipped (no group reached {min_matches} flown matches), "
                    f"{total_matches} total matches. "
                    f"{len(donor_won)} matched via donor fallback.")
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
        ndo_cols = self.cache.data['ndo_cols']

        # Future keys that were matched via their donor (fallback). Their matched
        # historical rows carry Sector=donor and must be relabelled to the
        # dependent so the Sector-keyed merge below lines up. Own-sector wins are
        # absent here, so a data-rich dependent merges purely on its own history.
        donor_won = getattr(self, '_donor_won', {})

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
                hist_sub = pivot_ndo[pivot_ndo['Key'].isin(matched_hist_keys)].drop(columns=['DOW'], errors='ignore')

                if future_sub.empty or hist_sub.empty:
                    continue

                # Donor fallback: relabel the matched donor rows to the dependent
                # Sector for THIS future key only, so the join keys align.
                if key in donor_won:
                    donor_sector, dependent_sector = donor_won[key]
                    hist_sub = hist_sub.copy()
                    hist_sub.loc[hist_sub['Sector'] == donor_sector, 'Sector'] = dependent_sector

                merged = future_sub.merge(
                    hist_sub,
                    on=['Sector', 'Tagging', 'FareBasis'],
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
        ndo_cols = self.cache.data['ndo_cols']

        group_cols = ['Sector', 'DepartureDate_future', 'DOW', 'STD_future',
                    'FlightNumber_future', 'Capacity', 'Total Bookings', 'Current_Revenue', 'Tagging', 'FareBasis']
        flight_group_cols = ['Sector', 'DepartureDate_future', 'DOW', 'STD_future',
                            'FlightNumber_future', 'Capacity', 'Total Bookings', 'Current_Revenue', 'Tagging']

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
        result_merged_df_std = result_merged_df_std.copy()   # de-fragment before new-col add
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
        weighted_std_df = weighted_std_df.copy()   # de-fragment before new-col add
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

        # --- Matched current bookings (benchmark vs the flight's own Total Bookings) ---
        mcb = self._matched_current_bookings(result_df, ndo_cols)
        _mcb_key = ['Sector', 'DepartureDate_future', 'FlightNumber_future']
        result_merged_df = result_merged_df.merge(mcb, on=_mcb_key, how='left')
        result_final_df = result_final_df.merge(mcb, on=_mcb_key, how='left')
        weighted_merged_df = weighted_merged_df.merge(mcb, on=_mcb_key, how='left')
        weighted_final_df = weighted_final_df.merge(mcb, on=_mcb_key, how='left')

        logger.info(f"Predictions complete (alpha={self.constants_utils.WEIGHT_ALPHA}) — "
                    f"normal fare bucket: {len(result_merged_df)}, flight: {len(result_final_df)}; "
                    f"weighted fare bucket: {len(weighted_merged_df)}, flight: {len(weighted_final_df)}.")

        return (result_merged_df, result_final_df, result_merged_df_std,
                weighted_merged_df, weighted_final_df)

    def _build_and_predict_chunked(self, future_df_expanded, pivot_ndo, flight_matches):
        """Memory-safe Steps 10+11: build the matched result and reduce it to the
        five prediction frames WITHOUT ever holding the full exploded cross-product
        in memory.

        Every Step-11 reduction (mean/std/weighted-mean, matched-current-bookings)
        groups by the FULL future-flight identity, so a future key's rows never
        interact with another key's rows. Processing future keys in chunks and
        concatenating the *reduced* per-chunk frames is therefore numerically
        identical to processing all keys at once — no match is dropped, no value
        changes. Only the transient exploded frame is bounded (one chunk at a time).
        """
        keys = [k for k, v in flight_matches.items() if v]
        if not keys:
            logger.warning("No matched results produced.")
            empty = pd.DataFrame()
            return empty, empty, empty, empty, empty

        chunk_size = getattr(self.constants_utils, 'PREDICT_CHUNK_KEYS', 300)
        parts = {'rm': [], 'rf': [], 'rms': [], 'wm': [], 'wf': []}
        n_chunks = (len(keys) + chunk_size - 1) // chunk_size

        # TEST-only: collect each chunk's exploded matched frame so the full
        # intermediate_10_matched_result artifact is still produced for local
        # testing. Skipped in PROD to preserve the memory bound.
        collect_matched = (self.mode == 'TEST')
        matched_parts = [] if collect_matched else None

        for ci in range(0, len(keys), chunk_size):
            chunk_keys = keys[ci:ci + chunk_size]
            sub_matches = {k: flight_matches[k] for k in chunk_keys}
            # Build the exploded frame for THIS chunk only.
            result_df = self._build_matched_result(future_df_expanded, pivot_ndo, sub_matches)
            if result_df.empty:
                continue
            if collect_matched:
                matched_parts.append(result_df.copy())
            rm, rf, rms, wm, wf = self._calculate_predictions(result_df)
            parts['rm'].append(rm)
            parts['rf'].append(rf)
            parts['rms'].append(rms)
            parts['wm'].append(wm)
            parts['wf'].append(wf)
            del result_df, rm, rf, rms, wm, wf
            logger.info(f"  [Step10+11] chunk {ci // chunk_size + 1}/{n_chunks} done "
                        f"({len(chunk_keys)} future keys).")

        def _cat(lst):
            return (pd.concat(lst, ignore_index=True, copy=False)
                    if lst else pd.DataFrame())

        if collect_matched and matched_parts:
            self._save_intermediate(_cat(matched_parts), '10_matched_result', fmt='parquet')

        return (_cat(parts['rm']), _cat(parts['rf']), _cat(parts['rms']),
                _cat(parts['wm']), _cat(parts['wf']))

    # =========================================================================
    # STEP 12: REMAINING RBD DISTRIBUTION
    # =========================================================================

    def _zero_remaining_rows_for_missing_keys(self, matched_keys, present_keys,
                                              pivot_ndo_cols):
        """
        Build all-zero remaining-RBD rows for FLOWN matched flights that have no
        rows in remaining_pivot (i.e. they booked zero non-required-RBD pax).

        A matched Key looks like 'Sector_YYYY-MM-DD_FlightNumber'. A key is
        re-injected only when:
          * it is not already present in remaining_pivot (present_keys), AND
          * its historical departure date is < current_date (flown).
        Future matches are intentionally left out — they have incomplete curves.

        Returns a DataFrame mirroring remaining_pivot's schema (index cols + NDO
        value cols set to 0), or None when there is nothing to add.
        """
        missing = [k for k in matched_keys if k not in present_keys]
        if not missing:
            return None

        recs = []
        for k in missing:
            # Split from the right so sectors containing underscores are safe.
            parts = k.rsplit('_', 2)
            if len(parts) != 3:
                continue
            sector, dd_str, flight_no = parts
            dd = pd.to_datetime(dd_str, errors='coerce')
            if pd.isna(dd) or dd >= self.current_date:
                # Skip unparseable keys and future flights (not flown history).
                continue
            # Match remaining_pivot's int64 FlightNumber when the token is numeric.
            fn = int(flight_no) if flight_no.isdigit() else flight_no
            recs.append({'Sector': sector, 'DepartureDate': dd,
                         'DOW': dd.day_name(), 'FlightNumber': fn,
                         'STD': None, 'Key': k})
        if not recs:
            return None

        zero_df = pd.DataFrame(recs)
        # All NDO columns are zero (this flight picked up no remaining-RBD pax).
        # Build every NDO column in one shot and concat once — assigning them one
        # at a time fragments the frame (pandas PerformanceWarning) and is slow.
        if pivot_ndo_cols:
            ndo_zeros = pd.DataFrame(
                0, index=zero_df.index, columns=list(pivot_ndo_cols))
            zero_df = pd.concat([zero_df, ndo_zeros], axis=1)
        return zero_df

    def _distribute_remaining_rbd(self, future_df, pivot_ndo, flight_matches,
                                result_merged_df, result_final_df,
                                count_col='predicted_pax_count'):
        """
        Distribute remaining (non-required RBD) pax across fare buckets proportionally
        to each bucket's share of `count_col`.
        """
        logger.info(f"Distributing remaining RBD bookings (basis: {count_col})...")
        ndo_cols = self.cache.data['ndo_cols']
        remaining_df = self.cache.data['remaining_df']

        remaining_df = remaining_df[~remaining_df['RBD'].isin(self.constants_utils.REQUIRED_RBDS)]
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

        # Only use flown flights for remaining RBD estimation (future flights
        # have incomplete booking data and would understate the remaining pax).
        remaining_pivot = remaining_pivot[remaining_pivot['DepartureDate'] < self.current_date]

        # A FLOWN matched flight that booked ZERO remaining-RBD pax has no rows in
        # remaining_df, so it never appears in remaining_pivot and would silently
        # drop out of the average. That is real evidence of low remaining demand and
        # must be kept: we re-inject such flights below as all-zero rows (remaining_raw
        # = 0) carrying their true departure date so their recency weight is correct.
        # Set of flown keys we DO have remaining data for (used to detect the gaps):
        present_keys = set(remaining_pivot['Key'])
        # NDO value columns currently on the pivot (fill these with 0 for zero rows).
        pivot_ndo_cols = [c for c in remaining_pivot.columns
                          if c not in ('Sector', 'DepartureDate', 'DOW',
                                       'FlightNumber', 'STD', 'Key')]

        # Mirror _build_matched_result's merge-column handling: DOW is dropped from
        # the historical side there (it was applied as a preference during Step 9,
        # so re-merging on it would over-restrict). Drop the SAME columns here so
        # the remaining-RBD join behaves identically. For a future flight matched
        # via its DONOR (fallback), the donor's rows carry Sector=donor and are
        # relabelled to the dependent per-key below so the Sector merge lines up.
        DROP_FOR_MERGE = ['DOW']
        donor_won = getattr(self, '_donor_won', {})
        dfs = []
        for key, matched_keys in flight_matches.items():
            if not matched_keys:
                continue
            future_sub = future_df[future_df['Key'] == key]
            if future_sub.empty:
                continue
            hist_sub = remaining_pivot[remaining_pivot['Key'].isin(matched_keys)].drop(
                columns=DROP_FOR_MERGE, errors='ignore')

            # Re-inject FLOWN matched flights that had zero remaining-RBD pax (and so
            # are absent from remaining_pivot) as all-zero rows. Only flown keys
            # (hist departure < current_date) are added — future matches stay excluded.
            zero_rows = self._zero_remaining_rows_for_missing_keys(
                matched_keys, present_keys, pivot_ndo_cols)
            if zero_rows is not None and not zero_rows.empty:
                zero_rows = zero_rows.drop(columns=DROP_FOR_MERGE, errors='ignore')
                hist_sub = pd.concat([hist_sub, zero_rows], ignore_index=True)

            if hist_sub.empty:
                continue
            # Donor fallback: relabel matched donor rows to the dependent Sector
            # for THIS future key so the Sector-level join aligns.
            if key in donor_won:
                donor_sector, dependent_sector = donor_won[key]
                hist_sub = hist_sub.copy()
                hist_sub.loc[hist_sub['Sector'] == donor_sector, 'Sector'] = dependent_sector
            merged = future_sub.merge(hist_sub, on=['Sector'], how='left',
                                    suffixes=('_future', '_hist'))
            dfs.append(merged)

        if not dfs:
            logger.warning("No remaining RBD matches found.")
            return result_merged_df, result_final_df

        remaining_pax = pd.concat(dfs, ignore_index=True)
        remaining_pax['future_flight_NDO'] = (
            remaining_pax['DepartureDate_future'] - self.current_date
        ).dt.days

        # Compute recency weights (same rule as the weighted prediction branch):
        # flown (SD >= DD) -> WEIGHT_ALPHA^(SD - DD); future/unflown (SD < DD) ->
        # WEIGHT_ALPHA_FUTURE^(DD - SD) (0 by default). remaining_pivot is already
        # flown-only here, so the future branch is currently inert, but the rule is
        # kept identical across all branches.
        hist_dd = pd.to_datetime(remaining_pax['DepartureDate_hist'])
        delta_days = (self.current_date - hist_dd).dt.days
        _alpha = self.constants_utils.WEIGHT_ALPHA
        _alpha_future = self.constants_utils.WEIGHT_ALPHA_FUTURE
        x_flown = delta_days.where(delta_days >= 0, 0)
        x_future = (-delta_days).where(delta_days < 0, 0)
        remaining_pax['_weight'] = np.where(
            delta_days.to_numpy() >= 0,
            (_alpha ** x_flown).fillna(0.0).to_numpy(dtype=float),
            (_alpha_future ** x_future).fillna(0.0).to_numpy(dtype=float))

        # Fill NaNs with 0 ONLY on numeric columns (NDO cells with no booking,
        # unmatched merge cells). Filling the whole frame downcasts object columns
        # and triggers a pandas FutureWarning; the string/date columns don't need
        # filling anyway — only the numeric NDO/aggregation columns are summed.
        num_cols = remaining_pax.select_dtypes(include=[np.number]).columns
        remaining_pax[num_cols] = remaining_pax[num_cols].fillna(0)

        # Vectorized replacement for the old per-row apply(sum_ndo, axis=1):
        # for each row, sum the NDO columns whose column-integer <= that row's
        # future_flight_NDO. Broadcasting a (1, n_ndo) column-index vector against
        # the (rows, 1) NDO-limit column yields the same masked row-sum in C,
        # avoiding a Python-level loop over rows x 181 NDO columns.
        present_ndo = [n for n in ndo_cols if n in remaining_pax.columns]
        if present_ndo:
            ndo_arr = np.array([int(n) for n in present_ndo])                 # (n_ndo,)
            vals = remaining_pax[present_ndo].to_numpy(dtype=float)           # (rows, n_ndo)
            limits = remaining_pax['future_flight_NDO'].to_numpy()[:, None]   # (rows, 1)
            mask = ndo_arr[None, :] <= limits                                 # (rows, n_ndo)
            remaining_pax['remaining_raw'] = np.where(
                mask, np.nan_to_num(vals), 0.0).sum(axis=1)
        else:
            remaining_pax['remaining_raw'] = 0.0

        # Weighted mean of remaining pax (recency-weighted, same as main prediction)
        remaining_pax['_w_raw'] = remaining_pax['remaining_raw'] * remaining_pax['_weight']
        group_keys = ['Sector', 'DepartureDate_future', 'STD_future', 'FlightNumber_future']

        # TEST-only: per matched-row remaining-RBD breakdown — for each future
        # flight x matched historical flight, the remaining pax it contributed
        # (remaining_raw, closed to NDO <= future_flight_NDO) and its weight.
        if self.mode == 'TEST':
            dbg_cols = [c for c in [
                'Sector', 'DepartureDate_future', 'STD_future', 'FlightNumber_future',
                'DepartureDate_hist', 'FlightNumber_hist', 'future_flight_NDO',
                '_weight', 'remaining_raw'] if c in remaining_pax.columns]
            dbg = remaining_pax[dbg_cols].rename(columns={'_weight': 'Weight'})
            self._save_intermediate(dbg, f'12_remaining_matched_rows_{count_col}', fmt='csv')
        agg = remaining_pax.groupby(group_keys).agg(
            w_sum=('_w_raw', 'sum'),
            w_total=('_weight', 'sum')
        ).reset_index()
        agg['remaining_pax_count'] = np.where(
            agg['w_total'] > 0, agg['w_sum'] / agg['w_total'], 0.0)

        remaining_renamed = agg[group_keys + ['remaining_pax_count']]
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

    # =========================================================================
    # STEP 13: DEMAND MULTIPLIER  (Demand Adjusted Prediction)
    # =========================================================================
    # Analyst-supplied, FLIGHT-level signal keyed on (DepartureDate, Sector,
    # FlightNumber). After Step 12 the Final Prediction is scaled:
    #
    #     DAP = Final Prediction * (100 + Demand Multiplier) / 100
    #
    # Default multiplier is 0% -> DAP == Final Prediction (no change). Because the
    # multiplier is flight-level, each fare bucket is scaled by the same factor, so
    #   bucket DAP = (bucket Final / flight Final) * flight DAP = bucket Final * f
    # which is exactly the BRD's proportional redistribution.



    # =========================================================================
    # STEP 14: WTP FARE-BUCKET REDISTRIBUTION (price inflation / deflation)
    # =========================================================================
    # Sector x Month-Year price signal. New bucket bounds = old / (1 + WTP%),
    # then each bucket's demand-adjusted pax (DAP) is re-integrated over the
    # rescaled window using the 2-term adjacency formula from the BRD example:
    #   inflation (factor>1): pull from the previous (lower) bucket
    #     mid : (L_i - L_i/f)*PPU_{i-1} + (U_i/f - L_i)*PPU_i
    #     first:                          (U_0/f - L_0)*PPU_0
    #     top : (L_top - L_top/f)*PPU_{top-1} + pax_top           (retains own pax)
    #   deflation (factor<1): pull from the forward (higher) bucket
    #     mid : (U_i - L_i/f)*PPU_i + (U_i/f - U_i)*PPU_{i+1}
    #     first: mid_0 + (L_0/f - L_0)*PPU_0                      (absorb bottom leftover)
    #     top : total - sum(all others)                           (absorb residual)
    # Terms are signed (a term goes negative when a bucket is narrower than the
    # shift) — this is what conserves the flight total, so we do NOT clamp them.
    # PPU of the open top bucket uses the previous bucket's width as denominator.


    @staticmethod
    def _wtp_redistribute_flight(lowers, uppers, pax, pct):
        """WTP redistribution for ONE flight (per the WTP sample method):
        density = pax / bucket width (open top bucket uses the previous width);
        rescale the bounds by 1/D (rounded); integrate the density over each
        bucket's new window (overlaps clamped >= 0, so never negative); the lowest
        bucket absorbs the left-most remaining pax (deflation); a running cap keeps
        the flight total from exceeding the original. Non-negative; total <= original."""
        n = len(pax)
        if n == 0 or pct == 0:
            return list(pax)
        D = (100.0 + pct) / 100.0
        uppers = [None if (u is None or (isinstance(u, float) and u != u)) else u
                  for u in uppers]

        # widths + effective upper (open top bucket uses the previous bucket's width)
        diff, eff_up = [], []
        for i in range(n):
            if uppers[i] is None:
                w = (uppers[i - 1] - lowers[i - 1]) if i > 0 else 0.0
                diff.append(w)
                eff_up.append(lowers[i] + w)
            else:
                diff.append(uppers[i] - lowers[i])
                eff_up.append(uppers[i])
        dens = [pax[i] / diff[i] if diff[i] else 0.0 for i in range(n)]

        # new bounds, rounded (per the sample)
        nl = [round(lowers[i] / D, 0) for i in range(n)]
        nu = [round(eff_up[i] / D, 0) for i in range(n)]
        dmin, dmax = min(lowers), max(eff_up)

        def _integrate(a, b):
            if a is None or b is None:
                return 0.0
            a = max(a, dmin)
            b = min(b, dmax)
            if b <= a:
                return 0.0
            t = 0.0
            for lo, hi, de in zip(lowers, eff_up, dens):
                slo, shi = max(a, lo), min(b, hi)
                if shi > slo:
                    t += (shi - slo) * de
            return t

        raw = []
        for i in range(n):
            val = _integrate(nl[i], nu[i])
            # deflation edge: the lowest bucket takes the left-most remaining pax
            if i == 0 and nl[i] is not None and nl[i] > dmin:
                val += _integrate(dmin, nl[i])
            raw.append(val)

        # running cap: sum(new) never exceeds the original total; each >= 0
        total = sum(pax)
        running = 0.0
        out = []
        for v in raw:
            rem = total - running
            if rem <= 0:
                out.append(0.0)
            elif v > rem:
                out.append(rem)
                running += rem
            else:
                out.append(max(v, 0.0))
                running += max(v, 0.0)
        return out

    def _apply_wtp_redistribution(self, bucket_df, count_col='New Final_pax_count'):
        """Step 14 — redistribute the DEMAND-ADJUSTED pax ('New Final_pax_count')
        within each flight's fare buckets for price inflation/deflation. Adds
        WTP_Factor and WTP_pax_count. Falls back to Final_pax_count only if the
        demand-adjusted column is absent."""
        if bucket_df is None or bucket_df.empty:
            return bucket_df
        src = count_col if count_col in bucket_df.columns else 'Final_pax_count'
        if src not in bucket_df.columns:
            logger.warning("[WTP] no source pax column; skipping.")
            return bucket_df

        wtp = self.mongo_helper.load_wtp()
        df = bucket_df.copy()
        dd = pd.to_datetime(df['DepartureDate_future'])
        df['WTP_Factor'] = [wtp.get((s, d.year, d.month), 0.0)
                            for s, d in zip(df['Sector'], dd)]

        # bucket bounds: prefer the parsed columns, else parse the FareBasis range
        if 'FareBucket_Lower' in df.columns and 'FareBucket_Upper' in df.columns:
            df['_wlo'] = pd.to_numeric(df['FareBucket_Lower'], errors='coerce')
            df['_wup'] = pd.to_numeric(df['FareBucket_Upper'], errors='coerce')
        elif 'FareBasis' in df.columns:
            bounds = df['FareBasis'].apply(parse_fare_bounds)
            df['_wlo'] = [b[0] for b in bounds]
            df['_wup'] = [b[1] for b in bounds]
        else:
            logger.warning("[WTP] no bucket bounds (FareBucket_Lower/Upper or FareBasis); skipping.")
            return bucket_df

        df['WTP_pax_count'] = df[src]
        gcols = ['Sector', 'DepartureDate_future', 'FlightNumber_future']
        n_adj = 0
        for _, idx in df.groupby(gcols).groups.items():
            sub = df.loc[idx].sort_values('_wlo')
            pct = float(sub['WTP_Factor'].iloc[0])
            if pct == 0:
                continue
            uppers = [None if (u != u) else u for u in sub['_wup'].tolist()]   # NaN -> None
            new = self._wtp_redistribute_flight(
                sub['_wlo'].tolist(), uppers, sub[src].tolist(), pct)
            df.loc[sub.index, 'WTP_pax_count'] = new
            n_adj += 1

        df.drop(columns=['_wlo', '_wup'], inplace=True, errors='ignore')
        logger.info(f"[WTP] redistributed {n_adj} flight(s) with a non-zero factor "
                    f"(source={src}).")
        return df

    def _apply_demand_multiplier(self, flight_df, bucket_df):
        """Step 13 — adds: Demand Multiplier, Demand Multiplier Number,
        New predicted_pax_count_final, Fare Bucket Ratio, New Final_pax_count,
        New Final Total Bookings. (Before = Final_pax_count / predicted_pax_count_final.)"""
        dm = self.mongo_helper.load_demand_multiplier()          # or self.mongo_helper.load_demand_multiplier()

        def _attach(df):
            if df is None or df.empty:
                return df
            df = df.copy()
            date_col = 'DepartureDate_future' if 'DepartureDate_future' in df.columns else 'DepartureDate'
            flt_col  = 'FlightNumber_future'  if 'FlightNumber_future'  in df.columns else 'FlightNumber'
            cb_col   = 'Current_Bookings'     if 'Current_Bookings'     in df.columns else 'Total Bookings'
            if 'predicted_pax_count_final' not in df.columns:
                df['predicted_pax_count_final'] = (
                    df.groupby(['Sector', date_col, flt_col])['Final_pax_count'].transform('sum'))
            dd  = pd.to_datetime(df[date_col]).dt.normalize()
            flt = pd.to_numeric(df[flt_col], errors='coerce').astype('Int64')
            pct = pd.Series([dm.get((s, d, int(f)) if pd.notna(f) else None, 0.0)
                             for s, d, f in zip(df['Sector'], dd, flt)], index=df.index)
            df['Demand Multiplier']             = pct.map(lambda v: f"{v:g}%")
            df['Demand Multiplier Number']      = (100.0 + pct) / 100.0
            df['New predicted_pax_count_final'] = df['predicted_pax_count_final'] * df['Demand Multiplier Number']
            df['Fare Bucket Ratio']             = np.where(
                df['predicted_pax_count_final'] != 0,
                df['Final_pax_count'] / df['predicted_pax_count_final'], 0.0)
            df['New Final_pax_count']           = df['Fare Bucket Ratio'] * df['New predicted_pax_count_final']
            df['New Final Total Bookings']      = df[cb_col] + df['New predicted_pax_count_final']
            return df

        return _attach(flight_df), _attach(bucket_df)

    def _matched_current_bookings(self, result_df, ndo_cols):
        """Per future flight: the average and weighted-average bookings the matched
        historical flights had ALREADY received by the future flight's current NDO.

        Matches the raw notebook logic: each matched flight's current bookings =
        SUM of its raw 'Pax Count' where NDO_future < NDO (closed window, n exclusive),
        merged on FLIGHT IDENTITY (all fare buckets) — not the 0..180 fare-bucket
        pivot. Average = mean across matched flights; weighted average uses the same
        weight w = alpha^x, x = SD - DD_hist as the weighted prediction.

        Booking source is the UNION of flown history (hist_raw_df) and forward
        bookings (forward_raw_df). The notebook kept future bookings inside
        hist_raw_df; the flight-matching split later separated them, so both are
        re-unioned here. This lets an UNFLOWN matched flight contribute its real,
        NDO-aligned bookings (its count where NDO_future < NDO) instead of a fake
        zero — the closed-window rule is identical for flown and unflown matches.
        """
        empty = pd.DataFrame(columns=['Sector', 'DepartureDate_future', 'FlightNumber_future',
                                      'Matched_Current_Bookings',
                                      'Weighted_Matched_Current_Bookings'])
        if result_df is None or result_df.empty:
            return empty

        pair_keys = ['Sector', 'DepartureDate_future', 'FlightNumber_future',
                     'DepartureDate_hist', 'FlightNumber_hist']
        pairs = result_df.drop_duplicates(pair_keys)[pair_keys].copy()
        pairs['NDO_future'] = (pd.to_datetime(pairs['DepartureDate_future'])
                               - self.current_date).dt.days
        _delta = (self.current_date - pd.to_datetime(pairs['DepartureDate_hist'])).dt.days
        # flown (SD >= DD): w = WEIGHT_ALPHA^(SD - DD).
        # future/unflown (SD < DD): w = WEIGHT_ALPHA_FUTURE^(DD - SD) (0 by default).
        _alpha = self.constants_utils.WEIGHT_ALPHA
        _alpha_future = self.constants_utils.WEIGHT_ALPHA_FUTURE
        _x_flown = _delta.where(_delta >= 0, 0)
        _x_future = (-_delta).where(_delta < 0, 0)
        pairs['Weight'] = np.where(
            _delta.to_numpy() >= 0,
            (_alpha ** _x_flown).fillna(0.0).to_numpy(dtype=float),
            (_alpha_future ** _x_future).fillna(0.0).to_numpy(dtype=float))

        # Booking source = FLOWN history (hist_raw_df) UNION FORWARD bookings
        # (forward_raw_df). The flight-matching split moved future flights out of
        # hist_raw_df into forward_raw_df; without the forward side, unflown
        # matched flights would resolve to booked=0 (fake zeros) and drag the
        # weighted average down. Both carry per-NDO 'Pax Count', so the SAME
        # closed-window rule (NDO_future < NDO) applies uniformly to flown and
        # unflown matches — the unflown flight then contributes its real, NDO-
        # aligned bookings (what it had by the equivalent days-out point), not its
        # raw as-of-snapshot total.
        _src_cols = ['Sector', 'DepartureDate', 'FlightNumber', 'NDO', 'Pax Count']
        _hist_raw = self.cache.data.get('hist_raw_df')
        _fwd_raw = self.cache.data.get('forward_raw_df')
        _frames = []
        if _hist_raw is not None and not _hist_raw.empty:
            _frames.append(_hist_raw[_src_cols])
        if _fwd_raw is not None and not _fwd_raw.empty:
            # forward rows include schedule rows with no booking (PNR null); keep
            # only actual bookings so a null-PNR row doesn't inject phantom pax.
            _fwd = _fwd_raw
            if 'PNR' in _fwd.columns:
                _fwd = _fwd[_fwd['PNR'].notna()]
            _frames.append(_fwd[_src_cols])
        if not _frames:
            return empty
        hist = pd.concat(_frames, ignore_index=True).copy()
        hist = hist.rename(columns={'DepartureDate': 'DepartureDate_hist',
                                    'FlightNumber': 'FlightNumber_hist'})
        hist['FlightNumber_hist'] = pd.to_numeric(hist['FlightNumber_hist'], errors='coerce')
        pairs['FlightNumber_hist'] = pd.to_numeric(pairs['FlightNumber_hist'], errors='coerce')
        # align the merge-key dtypes (one side can be object/string dates -> datetime64)
        hist['DepartureDate_hist'] = pd.to_datetime(hist['DepartureDate_hist'], errors='coerce').dt.normalize()
        pairs['DepartureDate_hist'] = pd.to_datetime(pairs['DepartureDate_hist'], errors='coerce').dt.normalize()

        merged = pairs.merge(hist, on=['Sector', 'DepartureDate_hist', 'FlightNumber_hist'], how='left')
        # closed window: bookings made when the flight was more than NDO_future days out
        merged['closed'] = np.where(merged['NDO_future'] < merged['NDO'],
                                    merged['Pax Count'].fillna(0.0), 0.0)

        per_hist = merged.groupby(pair_keys, as_index=False).agg(
            booked=('closed', 'sum'), Weight=('Weight', 'first'))

        def _agg(g):
            ws = g['Weight'].sum()
            wavg = (g['booked'] * g['Weight']).sum() / ws if ws > 0 else g['booked'].mean()
            return pd.Series({'Matched_Current_Bookings': g['booked'].mean(),
                              'Weighted_Matched_Current_Bookings': wavg})

        # Select the value columns _agg needs AFTER groupby so the grouping
        # columns aren't passed into _agg. This avoids the pandas 2.2
        # "apply operated on the grouping columns" FutureWarning without using the
        # include_groups kwarg (which doesn't exist on the pinned pandas 1.4.3).
        grp = per_hist.groupby(['Sector', 'DepartureDate_future', 'FlightNumber_future'],
                               sort=False)
        return grp[['booked', 'Weight']].apply(_agg).reset_index()

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
    
    def _weighted_ndo_stats(self, result_df, group_cols, ndo_cols, alpha=None):
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
        if alpha is None:
            alpha = self.constants_utils.WEIGHT_ALPHA
        n_rows = len(result_df)
        logger.info(f"  [Weighted] {n_rows} rows x {len(ndo_cols)} NDO columns "
                    f"(streaming, alpha={alpha})...")

        # ---- x and w(x) per BRD --------------------------------------------------
        hist_dd = pd.to_datetime(result_df['DepartureDate_hist'])
        delta_days = (self.current_date - hist_dd).dt.days          # SD - DD

        # SD >= DD (flown)          -> w = WEIGHT_ALPHA        ^ (SD - DD)
        # SD <  DD (future/unflown) -> w = WEIGHT_ALPHA_FUTURE ^ (DD - SD)
        # WEIGHT_ALPHA_FUTURE defaults to 0 (unflown matches excluded); set it to
        # WEIGHT_ALPHA (0.99) to restore the original decayed future weight.
        alpha_future = self.constants_utils.WEIGHT_ALPHA_FUTURE
        dd_np = delta_days.to_numpy(dtype=float)
        x_flown = delta_days.where(delta_days >= 0, 0)
        x_future = (-delta_days).where(delta_days < 0, 0)
        w = np.where(
            dd_np >= 0,
            (alpha ** x_flown).fillna(0.0).to_numpy(dtype=float),
            (alpha_future ** x_future).fillna(0.0).to_numpy(dtype=float))   # (n_rows,)

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
            fmt = 'parquet' if len(df) >= self.constants_utils.PARQUET_ROW_THRESHOLD else 'csv'

        if fmt == 'parquet':
            self._save_to_parquet(df, f"intermediate_{name}.parquet")
        else:
            self._save_to_csv(df, f"intermediate_{name}.csv")

    # =========================================================================
    # MAIN ORCHESTRATOR
    # =========================================================================

    def _enqueue_dependents_after_cache(self):
        """Fan out this donor's dependent sectors to SQS as soon as the donor's
        history is CACHED (end of Step 2) — not after the donor finishes steps
        3-15. This lets each dependent run IN PARALLEL with the donor's own
        processing, since a dependent only needs the donor's warm cached history.

        Each dependent message carries `mapped_sector` = the donor so the worker
        borrows the (now warm) donor history when it runs.

        Fires at most ONCE per invocation (guarded by `self._deps_enqueued`), and
        only when this invocation carried `dependent_sectors` (i.e. it IS a donor).
        No-op in non-PROD mode (no real queue) and when there are no dependents.
        """
        if getattr(self, '_deps_enqueued', False):
            return
        if not self.dependent_sectors:
            return

        # The donor for this message is the sector we just fetched/cached.
        this_msg = self._donor_sectors_this_msg()
        donor = this_msg[0] if this_msg else None
        if not donor:
            return

        # Mark as attempted so a redelivery/loop does not double-send. Set before
        # the PROD/queue checks so TEST mode also records the (no-op) attempt.
        self._deps_enqueued = True

        if self.mode != 'PROD':
            logger.info(f"[MappedSectors] TEST mode — skipping SQS enqueue of "
                        f"dependents {self.dependent_sectors} (donor {donor} cached).")
            return

        queue_url = getattr(configsettings, 'ADDITIVE_SQS_URL', None)
        if not queue_url:
            logger.error("[MappedSectors] ADDITIVE_SQS_URL not configured — cannot "
                         f"enqueue dependents {self.dependent_sectors}.")
            return

        sqs = boto3.client('sqs')
        sent = 0
        for dep in self.dependent_sectors:
            msg = {
                'modules': {'additive_process': 1},
                'overrides': {
                    'sectors': [dep],
                    'mapped_sector': donor,
                    'run_id': self.run_id,
                    'batch_index': self.batch_index,
                    'batch_total': self.batch_total,
                    'sectors_expected': self.sectors_expected,
                    'mode': self.mode,
                    'analyst': self.analyst,
                },
            }
            try:
                sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(msg))
                sent += 1
            except Exception as e:
                logger.error(f"[MappedSectors] failed to enqueue dependent {dep}: {e}")
        logger.info(f"[MappedSectors] enqueued {sent}/{len(self.dependent_sectors)} "
                    f"dependent(s) of donor {donor} (parallel, post-cache): "
                    f"{self.dependent_sectors}")

    def _donor_sectors_this_msg(self):
        """The donor sector(s) handled by this invocation (the message's own
        sectors, captured before the per-sector loop mutates active_sectors).
        A donor message is 1 sector, but keep it list-safe."""
        return list(getattr(self, '_msg_sectors', self.active_sectors))

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
            # resolve sectors (message / TEST / standalone-all)
            self.active_sectors = self.mongo_helper.fetch_active_sectors(self.mode, self.active_sectors, self.test_sectors)
            sectors = sorted(self.active_sectors)
            # Capture this message's own sector(s) BEFORE the loop mutates
            # self.active_sectors — used to identify the donor for the dependent
            # re-enqueue after the loop.
            self._msg_sectors = list(sectors)
            logger.info(f"{len(sectors)} sector(s) to process: {sectors}")

            # run-scoped master data (fetched once, survives per-sector resets)
            self.mongo_helper.fetch_tax_master_data()

            for i, sector in enumerate(sectors, 1):
                logger.info("-" * 80)
                donor = self.mapped_sectors.get(sector)
                logger.info(f"[{i}/{len(sectors)}] Sector {sector}"
                            + (f" (mapped -> {donor})" if donor else ""))
                self.active_sectors = [sector]
                t0 = _time.perf_counter()
                try:
                    # Fail-fast: a DEPENDENT sector (one carried by a mapped_sector
                    # message) MUST have a resolvable donor. If the mapping is
                    # missing, fail the sector directly instead of running it as a
                    # standalone (which would silently produce a no-donor forecast).
                    if self.mapped_sector and not donor:
                        raise RuntimeError(
                            f"Dependent sector {sector} has no mapped (donor) sector — "
                            f"failing sector (mapping required).")
                    self._run_batch([sector], donor=donor)
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
                    # Future flights that matched at NO ladder level, for THIS
                    # sector only: {reason: ['YYYY-MM-DD_FLTNO', ...]}. The run
                    # audit keeps counts only; the flight lists go to the separate
                    # additive_model_unmatched_flights doc (one per run).
                    reasons = (getattr(self, '_unmatched_keys', None) or {}).get(sector) or {}
                    by_reason = {r: len(keys) for r, keys in reasons.items()}
                    n_unmatched = sum(by_reason.values())
                    stat = sector_stats.get(sector, {})
                    stat['unmatched_count'] = n_unmatched
                    if by_reason:
                        stat['unmatched_by_reason'] = by_reason
                        logger.info(f"  {sector}: {n_unmatched} future flight(s) "
                                    f"unmatched at all levels {by_reason}")
                    sector_stats[sector] = stat
                    self.mongo_helper.audit_upsert_sector(sector, sector_stats[sector])
                    self.mongo_helper.audit_unmatched_flights(sector, reasons)
                    # Clear after this sector's audit so a sector that fails before
                    # Step 9 (e.g. dependent with no donor) can never inherit the
                    # previous sector's unmatched flights.
                    self._unmatched_keys = {}
                    self.cache.reset_batch()

            # Dependents are enqueued earlier, right after the donor's history is
            # cached (see _enqueue_dependents_after_cache, called from _run_batch),
            # so they run in parallel with the donor's own processing. As a safety
            # net, attempt the fan-out again here in case a donor batch cached but
            # the early enqueue was skipped (idempotent via self._deps_enqueued).
            self._enqueue_dependents_after_cache()

            logger.info("=" * 80)
            logger.info(f"COMPLETE — {len(succeeded)} OK, {len(failed)} failed, "
                        f"{datetime.now() - start}")

            # last bin to finish stamps the run terminal state
            self.mongo_helper.audit_finalize()

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
            self.mongo_helper.audit_finalize()
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

    def _restrict_future_frames_to(self, keep_sectors):
        """Keep only `keep_sectors` rows in the FUTURE frames so a fetched donor
        contributes history only (no donor output). Historical frames are left
        intact for the fallback ladders."""
        keep = set(keep_sectors)
        for key in ('future_flight_master', 'forward_raw_df'):
            df = self.cache.data.get(key)
            if df is not None and not df.empty and 'Sector' in df.columns:
                self.cache.data[key] = df[df['Sector'].isin(keep)].copy()

    def _run_batch(self, sectors, donor=None):
        """Run steps 1-15 for one sector batch. Saves every step's output in TEST mode.

        `donor` (mapped sector): when set, its HISTORICAL data (flown, tagging,
        lean/peak) is fetched alongside the sector and kept under the donor's OWN
        Sector label (NOT relabelled). The dependent's own history is tried first
        across all 7 tagging/matching levels; the donor is consulted only as a
        fallback when the dependent's own data cannot clear the match threshold.
        Winning donor rows are relabelled to the dependent Sector at merge time
        only (see _build_matched_result / _distribute_remaining_rbd). The donor's
        FUTURE flights are stripped so only the real sector produces output.
        """
        self.cache.reset_batch()
        # Per-batch: future keys matched via donor fallback (set in Step 9). Reset
        # so a prior sector's donor wins never leak into this batch's merges.
        self._donor_won = {}
        # Per-batch: future flight keys that failed EVERY match ladder level
        # (set in Step 9, surfaced to the audit by run()). Reset so an early
        # return (e.g. no history) leaves an empty dict, not a stale one.
        self._unmatched_keys = {}

        t_fetch_start = _time.perf_counter()

        # --- Step 1: Fare buckets (DEPENDENT sector only — the bucket is its own) ---
        # Fail-fast: a dependent (mapped) sector with NO QP fares to build buckets
        # cannot produce output — fail it directly rather than borrowing blindly.
        farebasis_df = self._fetch_fare_buckets()
        dep_buckets = farebasis_df[farebasis_df['Sector'].isin(self.active_sectors)]
        if dep_buckets.empty:
            raise RuntimeError(
                f"No QP fare buckets for dependent sector(s) {self.active_sectors} — "
                f"failing sector (cannot build fare buckets).")
        self._save_intermediate(farebasis_df, '01_fare_buckets')

        # --- Step 2: Fetch (cached). Include the donor so its history is present. ---
        fetch_sectors = list(dict.fromkeys(self.active_sectors + ([donor] if donor else [])))
        self.esb_helper.fetch_all_data(fetch_sectors, self.today.date(), self.current_date, FORCE_BACKFILL)

        if donor:
            # Strip the donor's FUTURE flights/bookings so it never produces output.
            # Its flown HISTORY stays under Sector=donor (no relabel) so the
            # dependent's own-sector ladder is tried first and the donor is only
            # borrowed as a fallback.
            self._restrict_future_frames_to(self.active_sectors)

        # Parallel fan-out: the donor's history is now cached (warm). Dependents
        # can run in parallel with the donor's own processing — enqueue them now
        # rather than waiting for the donor to finish steps 3-15.
        self._enqueue_dependents_after_cache()

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

        # --- Step 5: Future lean/peak tagging (7-level ladder dict) ---
        tag_levels = self._compute_future_tagging(lean_peak_source)
        for name, df in tag_levels.items():
            self._save_intermediate(df, f'05_future_tagging_{name}')

        # --- Step 6: Combine + assign fare buckets ---
        combined_df = self._prepare_combined_data(flown_df, booked_df, farebasis_df)
        self._save_intermediate(combined_df, '06_combined_data')

        # --- Step 7: Pivot NDO ---
        pivot_ndo, remaining_df = self._pivot_ndo_data(
            combined_df, farebasis_df, historical_tagging, tag_levels)
        self._save_intermediate(pivot_ndo, '07_pivot_ndo', fmt='parquet')
        self._save_intermediate(remaining_df, '07_remaining_data', fmt='csv')

        # --- Step 8: Prepare future flights ---
        future_df, future_df_expanded = self._prepare_future_flights(
            future_flight_df, tag_levels, farebasis_df)
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

        # --- Steps 10+11: Build matched result + predictions (chunked, memory-safe) ---
        # Processed in chunks of future keys so the exploded (future x buckets x
        # matches x 181 NDO) frame is never fully materialized. Output is identical
        # to the all-at-once path because every reduction groups by future-flight
        # identity — no match is dropped.
        (result_merged_df, result_final_df, _,
         weighted_merged_df, weighted_final_df) = self._build_and_predict_chunked(
            future_df_expanded, pivot_ndo, flight_matches)
        if result_merged_df.empty:
            logger.warning(f"No matched results for {sectors} — batch produced no output.")
            return
        self._save_intermediate(result_merged_df, '11a_pred_fare_bucket_normal')
        self._save_intermediate(weighted_merged_df, '11b_pred_fare_bucket_weighted')
        self._save_intermediate(result_final_df, '11c_pred_flight_normal')
        self._save_intermediate(weighted_final_df, '11d_pred_flight_weighted')

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

        # --- Step 13: Demand Multiplier (Demand Adjusted Prediction) ---
        result_final_df, result_merged_df     = self._apply_demand_multiplier(result_final_df, result_merged_df)
        weighted_final_df, weighted_merged_df = self._apply_demand_multiplier(weighted_final_df, weighted_merged_df)
        self._save_intermediate(result_final_df, '13a_flight_demand_adjusted_normal')
        self._save_intermediate(result_merged_df, '13b_fare_bucket_demand_adjusted_normal')
        self._save_intermediate(weighted_final_df, '13c_flight_demand_adjusted_weighted')
        self._save_intermediate(weighted_merged_df, '13d_fare_bucket_demand_adjusted_weighted')

        # --- Step 14: WTP fare-bucket redistribution (price inflation/deflation) ---
        result_merged_df   = self._apply_wtp_redistribution(result_merged_df,   count_col='New Final_pax_count')
        weighted_merged_df = self._apply_wtp_redistribution(weighted_merged_df, count_col='New Final_pax_count')
        self._save_intermediate(result_merged_df, '14a_fare_bucket_wtp_normal')
        self._save_intermediate(weighted_merged_df, '14b_fare_bucket_wtp_weighted')

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


    def _close_connections(self):
        """No-op for injected connections.

        The ESB/DW engines and Mongo client are owned by lambda_function.py and
        reused across warm invocations — disposing them here would break the
        next request. We only drop our references to per-run data.
        """
        self.cache.data = {}

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
            'SnapshotDate', 'Sector', 'DepartureDate', 'DOW', 'STD',
            'FlightNumber', 'Capacity', 'Current_Bookings', 'Current_Revenue', 'Tagging', 'FareBucket',
            'future_flight_NDO', 'predicted_pax_count', 'predicted_pax_std',
            'Matched_Current_Bookings',
            'Remaining_pax_count', 'Remaining_pax_distribution', 'Final_pax_count',
            'Total_Predicted_pax_count', 'Predicted_Total_Bookings',
            'predicted_pax_count_final',
            'Demand Multiplier', 'Demand Multiplier Number',
            'New predicted_pax_count_final', 'Fare Bucket Ratio',
            'New Final_pax_count', 'New Final Total Bookings',
            'WTP_Factor', 'WTP_pax_count',
            'FareBasis_Prediction_ratio', 'FareBucket_Lower', 'FareBucket_Upper']

        flight_level_cols = [
            'SnapshotDate', 'Sector', 'DepartureDate', 'DOW', 'STD',
            'FlightNumber', 'Capacity', 'Current_Bookings', 'Current_Revenue', 'Tagging',
            'future_flight_NDO', 'predicted_pax_count', 'predicted_pax_std_square',
            'Matched_Current_Bookings',
            'Remaining_pax_count', 'Final_pax_count', 'Final_Total_Bookings',
            'predicted_pax_count_final',
            'Demand Multiplier', 'Demand Multiplier Number',
            'New predicted_pax_count_final', 'New Final Total Bookings',
            'Load_Factor']

        weighted_fare_bucket_cols = [
            'SnapshotDate', 'Sector', 'DepartureDate', 'DOW', 'STD',
            'FlightNumber', 'Capacity', 'Current_Bookings', 'Current_Revenue', 'Tagging', 'FareBucket',
            'future_flight_NDO', 'weighted_pax_count', 'weighted_pax_std',
            'Weighted_Matched_Current_Bookings',
            'Remaining_pax_count', 'Remaining_pax_distribution', 'Final_pax_count',
            'Total_Predicted_pax_count', 'Predicted_Total_Bookings',
            'predicted_pax_count_final',
            'Demand Multiplier', 'Demand Multiplier Number',
            'New predicted_pax_count_final', 'Fare Bucket Ratio',
            'New Final_pax_count', 'New Final Total Bookings',
            'WTP_Factor', 'WTP_pax_count',
            'FareBasis_Prediction_ratio', 'FareBucket_Lower', 'FareBucket_Upper']

        weighted_flight_level_cols = [
            'SnapshotDate', 'Sector', 'DepartureDate', 'DOW', 'STD',
            'FlightNumber', 'Capacity', 'Current_Bookings', 'Current_Revenue', 'Tagging',
            'future_flight_NDO', 'weighted_pax_count', 'weighted_pax_std_square',
            'Weighted_Matched_Current_Bookings',
            'Remaining_pax_count', 'Final_pax_count', 'Final_Total_Bookings',
            'predicted_pax_count_final',
            'Demand Multiplier', 'Demand Multiplier Number',
            'New predicted_pax_count_final', 'New Final Total Bookings',
            'Load_Factor']

        optimization_cols = [
            'SnapshotDate', 'Sector', 'DepartureDate', 'DOW', 'STD',
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
                remaining = _df['Remaining_pax_count'].fillna(0) if 'Remaining_pax_count' in _df.columns else 0
                _df['Predicted_Total_Bookings'] = (
                    _df['Current_Bookings'] + _df['Total_Predicted_pax_count'] + remaining)

        # Load_Factor — FLIGHT-LEVEL frames (ratio, not sum)
        for _df in (result_final_df, weighted_final_df):
            if _df is not None and {'Final_Total_Bookings',
                                    'Capacity'}.issubset(_df.columns):
                _df['Load_Factor'] = np.where(
                    _df['Capacity'] > 0,
                    _df['Final_Total_Bookings'] / _df['Capacity'],
                    np.nan)

        # (frame, trimmed cols, s3 dataset, dw table)
        # WEIGHTED-ONLY: the normal-path frames (result_final_df / result_merged_df)
        # are computed for internal steps but NOT persisted. Only the weighted
        # fare-bucket and flight-level outputs are stored (TEST + PROD).
        DATASETS = [
            (weighted_final_df,  weighted_flight_level_cols, 'flight_level_weighted',
             'add_model_flight_level_weighted'),
            (weighted_merged_df, weighted_fare_bucket_cols,  'fare_bucket_weighted',
             'add_model_fare_bucket_level_weighted')]

        # =====================================================================
        # TEST — timestamped CSV in the local output directory
        # =====================================================================
        # Only the weighted fare-bucket and weighted flight-level CSVs are written.
        # Normal-path CSVs and the full-parquet dumps are intentionally not saved.
        if self.mode == 'TEST':
            logger.info(f"Saving results to CSV in: {self.output_dir}")

            for frame, cols, name, _dw in DATASETS:
                if frame is None or frame.empty:
                    continue
                self._save_to_csv(_subset(frame, cols), f'{name}_{timestamp}.csv')

            # Full weighted frames (all columns incl. NDO 0-180) -> parquet.
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
        # Only the weighted outputs are stored, as the FULL frame (incl. the
        # pivoted NDO 0-180 curve). Normal-path S3 writes are intentionally removed.
        logger.info(f"Saving to partitioned S3 "
                    f"({len(sectors) if sectors else '?'} sectors)...")

        for frame, cols, s3_dataset, _dw in DATASETS:
            if frame is None or frame.empty:
                logger.warning(f"{s3_dataset}: empty, skipped.")
                continue
            # full frame (incl. NDO curve) to S3; the trimmed `cols` are for
            # a lighter query-facing copy if you want one later.
            self.cache.write_partitioned_s3(frame, s3_dataset)

        logger.info("All PROD results saved to S3 successfully.")