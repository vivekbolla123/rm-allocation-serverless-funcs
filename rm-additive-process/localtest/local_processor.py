"""
localtest/local_processor.py
=============================================================================
LocalAdditiveProcessor — the production AdditivePickupModelProcessor, wired to
local stores.

It runs with mode='PROD' on purpose. PROD is the path that actually ships, and
it is the only mode that reads the demand multiplier and WTP from MongoDB,
writes the run audit, and saves results through `cache.write_partitioned_s3`.
LocalAdditiveCache redirects that one call to local MongoDB + CSV, so the local
flow follows the production code path with local data underneath.

Deviations from PROD:

  * step 15 (EMSR-b protection limits and AUs) is driven by the WEIGHTED branch
    instead of the normal one — see _compute_inventory_optimization below;
  * `fare_buckets.csv` in inputs/ can stand in for step 1 (GP bucket build);
  * `tagging.csv` in inputs/ can stand in for step 4 (Z-score tagging).

The two CSV stand-ins each need the file to exist AND the corresponding flag, so
the real logic runs by default.

No intermediate artefacts and no parquet cache are written: the only files this
flow produces are the input CSVs and the output CSVs.
"""

import os

import pandas as pd

from additive_model import AdditivePickupModelProcessor

from localtest.local_helpers import LocalAdditiveCache, LocalCsvEsbHelper
from localtest.schema import OVERRIDE_FARE_BUCKETS_CSV, OVERRIDE_TAGGING_CSV


class LocalAdditiveProcessor(AdditivePickupModelProcessor):

    def __init__(self, logger, params, overrides, context, cfg,
                 esb_backend='mysql',
                 use_csv_fare_buckets=False, use_csv_tagging=False,
                 dump_derived=False, include_ndo_in_mongo=False,
                 write_full_csv=False):
        overrides = dict(overrides or {})
        overrides['mode'] = 'PROD'
        super().__init__(logger, params, overrides, context)

        self.cfg = cfg
        self.use_csv_fare_buckets = use_csv_fare_buckets
        self.use_csv_tagging = use_csv_tagging
        self.dump_derived = dump_derived
        self._dumped = set()
        self.output_dir = cfg.outputs_dir
        # Captured at step 14 so step 15 can build the AU ladder from it.
        self._weighted_bucket_df = None

        # --- swap S3 cache -> local cache, then re-bind both helpers ---
        local_cache = LocalAdditiveCache(cfg, self.db, logger,
                                         include_ndo_in_mongo=include_ndo_in_mongo,
                                         write_full_csv=write_full_csv)
        local_cache.data = self.cache.data
        self.cache = local_cache
        self.local_cach_dir = None
        self.mongo_helper.bind_context(self.cache, self.mode)

        # --- ESB backend ---
        self.esb_backend = esb_backend
        if esb_backend == 'csv':
            logger.warning('[local] ESB backend = csv (pandas mirror of the SQL). '
                           'Start a local MySQL and use --esb mysql to exercise the '
                           'real queries.')
            self.esb_helper = LocalCsvEsbHelper(cfg.inputs_dir, logger)
        self.esb_helper.bind_context(self.cache)

        self.cache.ensure_output_indexes()

    # =====================================================================
    # Step 1 — fare buckets (optional CSV stand-in)
    # =====================================================================
    def _fetch_fare_buckets(self):
        # Step 1 is the first thing _run_batch does, so this is where a batch
        # starts: drop any frame captured for the previous sector.
        self._weighted_bucket_df = None

        path = os.path.join(self.cfg.inputs_dir, OVERRIDE_FARE_BUCKETS_CSV)
        if self.use_csv_fare_buckets and os.path.exists(path):
            df = pd.read_csv(path)
            missing = {'Sector', 'FareBasis'} - set(df.columns)
            if missing:
                raise ValueError(f'{OVERRIDE_FARE_BUCKETS_CSV} is missing columns {missing}')
            df = df[df['Sector'].isin(self.active_sectors)][['Sector', 'FareBasis']]
            df = df.drop_duplicates()
            if df.empty:
                raise RuntimeError(f'{OVERRIDE_FARE_BUCKETS_CSV} has no rows for '
                                   f'{self.active_sectors}')
            self.logger.info(f'[local] step 1 from CSV: {len(df)} fare buckets across '
                             f"{df['Sector'].nunique()} sector(s)")
            self.cache.data['farebasis_df'] = df
            return df

        df = super()._fetch_fare_buckets()
        if self.dump_derived:
            self._dump(df, OVERRIDE_FARE_BUCKETS_CSV)
        return df

    # =====================================================================
    # Step 4 — historical lean/peak tagging (optional CSV stand-in)
    # =====================================================================
    def _compute_lean_peak_tagging(self, lean_peak_df):
        path = os.path.join(self.cfg.inputs_dir, OVERRIDE_TAGGING_CSV)
        if self.use_csv_tagging and os.path.exists(path):
            df = pd.read_csv(path)
            required = {'Sector', 'DepartureDate', 'FlightNumber', 'Time Cluster', 'Tagging'}
            missing = required - set(df.columns)
            if missing:
                raise ValueError(f'{OVERRIDE_TAGGING_CSV} is missing columns {missing}')
            df = df[df['Sector'].isin(self.active_sectors)].copy()
            df['DepartureDate'] = pd.to_datetime(df['DepartureDate'], errors='coerce')
            df = df.dropna(subset=['DepartureDate', 'FlightNumber'])
            df['FlightNumber'] = df['FlightNumber'].astype(int)
            self.logger.info(f'[local] step 4 from CSV: {len(df)} tagged flights')
            return df[['Sector', 'DepartureDate', 'FlightNumber', 'Time Cluster', 'Tagging']]

        df = super()._compute_lean_peak_tagging(lean_peak_df)
        if self.dump_derived:
            self._dump(df, OVERRIDE_TAGGING_CSV)
        return df

    # =====================================================================
    # Step 14 — capture the weighted frame on its way past
    # =====================================================================
    def _apply_wtp_redistribution(self, bucket_df, count_col='New Final_pax_count'):
        out = super()._apply_wtp_redistribution(bucket_df, count_col=count_col)
        # _run_batch calls this twice per sector with identical arguments, once
        # per branch. The weighted frame is the one carrying weighted_pax_count.
        if out is not None and 'weighted_pax_count' in getattr(out, 'columns', []):
            self._weighted_bucket_df = out
        return out

    # =====================================================================
    # Step 15 — AU ladder from the WEIGHTED branch
    # =====================================================================
    def _compute_inventory_optimization(self, bucket_df, base_col=None, base_std_col=None):
        """EMSR-b protection limits and AUs, built from the WEIGHTED branch.

        Production hands step 15 the NORMAL fare-bucket frame (additive_model.py:
        `self._compute_inventory_optimization(result_merged_df)`). This flow
        persists only the weighted branch, so it substitutes the weighted frame and
        the weighted base columns. Demand selection, sigma scaling and the buy-up
        factor are all inherited unchanged from production.

        **The AUs here therefore differ from production by design** — production
        would use `predicted_pax_count` / `predicted_pax_std` as the sigma base.
        """
        source = self._weighted_bucket_df
        if source is None or source.empty or 'weighted_pax_count' not in source.columns:
            self.logger.warning('[local] step 15: no weighted frame captured — '
                                'falling back to the production (normal-branch) ladder.')
            return super()._compute_inventory_optimization(bucket_df)

        self.logger.info('[local] step 15 from the WEIGHTED branch')
        return super()._compute_inventory_optimization(
            source, base_col='weighted_pax_count', base_std_col='weighted_pax_std')

    # =====================================================================
    def _dump(self, df, filename):
        """Write a computed step back into inputs/ so it can be edited and replayed.

        run() processes one sector at a time, so the first sector truncates the
        file and the rest append to it.
        """
        if df is None or df.empty:
            return
        path = os.path.join(self.cfg.inputs_dir, filename)
        first = filename not in self._dumped
        df.to_csv(path, index=False, mode='w' if first else 'a', header=first)
        self._dumped.add(filename)
        self.logger.info(f"[local] dumped derived {filename} "
                         f"({len(df)} rows, {'wrote' if first else 'appended'}) -> {path}")

    def output_summary(self):
        return dict(self.cache.written)
