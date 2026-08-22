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

The single deviation from PROD is step 15: the EMSR-b protection limits and AUs
are built from the WEIGHTED branch instead of the normal one, because the
weighted branch is the only one this flow persists. See
_compute_inventory_optimization below. Every other step runs exactly as it ships.

No intermediate artefacts and no parquet cache are written: the only files this
flow produces are the input CSVs and the output CSVs.
"""

from additive_model import AdditivePickupModelProcessor

from localtest.local_helpers import LocalAdditiveCache, LocalCsvEsbHelper


class LocalAdditiveProcessor(AdditivePickupModelProcessor):

    def __init__(self, logger, params, overrides, context, cfg,
                 esb_backend='mysql', include_ndo_in_mongo=False,
                 write_full_csv=False, anchor=None):
        overrides = dict(overrides or {})
        overrides['mode'] = 'PROD'
        super().__init__(logger, params, overrides, context)

        # --- anchor date override ---
        anchor_date = anchor or overrides.get('anchor')
        if anchor_date:
            from datetime import date, datetime
            import pandas as pd
            from Constants import PREDICTION_HORIZON_DAYS
            if isinstance(anchor_date, str):
                anchor_date = date.fromisoformat(anchor_date)
            self.today = datetime.combine(anchor_date, datetime.min.time())
            self.current_date = pd.to_datetime(anchor_date)
            self.prediction_limit = self.current_date + pd.Timedelta(days=PREDICTION_HORIZON_DAYS)
            self.one_year_back = self.current_date - pd.Timedelta(days=365)

        self.cfg = cfg
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

    def _run_batch(self, sectors):
        # One batch is one sector; drop the frame captured for the previous one.
        self._weighted_bucket_df = None
        return super()._run_batch(sectors)

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

    def output_summary(self):
        return dict(self.cache.written)
