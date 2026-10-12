"""
helpers/db_helpers/additive_mongo_helper.py
=============================================================================
All MongoDB reads/writes for the Additive Pickup Model.

  self.db        -> qp_dw_rmalloc  (fares, rm_parameter_values, additive_model_run_audit)
  self.master_db -> qp_rm_master   (active_sectors, tax_mast_dom/intl,
                                    config_sector_demand_multiplier, config_quarter_wtp,
                                    config_station_master)
  self.dw_helper -> AdditiveDwHelper (RM_DW_DATA, optional) (currency_conversion —
                                    international fare currency -> INR)

Cache + test mode: bind the shared AdditiveCache once (bind_context); tax/pricing
constants are written into `self.cache.data` so the processor and ESB helper see
them. Demand-multiplier and WTP maps are read from Mongo in PROD and from local
CSVs in TEST, and memoised on the helper for the invocation.
"""

import os
import json
import traceback

import pandas as pd

import configsettings


class AdditiveMongoHelper:
    """MongoDB database operations for the Additive Pickup Model."""

    def __init__(self, mongo_db, master_db, logger, dw_helper=None):
        """
        Args:
            mongo_db:  qp_dw_rmalloc DB handle (fares, rm_parameter_values, audit)
            master_db: qp_rm_master DB handle (active_sectors, tax, config_*)
            logger:    logger instance
            dw_helper: AdditiveDwHelper for RM_DW_DATA reads (currency_conversion
                       ->INR rates for international fares). Optional — if not
                       supplied, currency conversion is skipped (fares used as-is).
        """
        self.db = mongo_db
        self.master_db = master_db
        self.logger = logger
        self.dw_helper = dw_helper
        # bound later via bind_context / set_run
        self.cache = None
        self.mode = 'PROD'
        self._dm = None
        self._wtp = None
        self._intl_stations = None
        self._mapped_sectors = None
        # run metadata for audit
        self.run_id = None
        self.sectors_expected = None
        self.analyst = 'system'
        self.batch_index = None
        self.snapshot_date = None

    # =========================================================================
    # CONTEXT BINDING
    # =========================================================================
    def bind_context(self, cache, mode):
        """Share the processor's AdditiveCache + run mode."""
        self.cache = cache
        self.mode = mode
        return self

    def set_run(self, run_id, sectors_expected, analyst, batch_index, snapshot_date):
        """Run metadata used by the audit writes."""
        self.run_id = run_id
        self.sectors_expected = sectors_expected
        self.analyst = analyst
        self.batch_index = batch_index
        self.snapshot_date = snapshot_date
        return self

    def _config_db(self):
        """DB holding config_* collections. Defaults to master_db; override with
        configsettings.ADDITIVE_CONFIG_DB if they live elsewhere."""
        name = getattr(configsettings, 'ADDITIVE_CONFIG_DB', None)
        if name and self.master_db is not None:
            try:
                return self.master_db.client[name]
            except Exception:
                pass
        return self.master_db if self.master_db is not None else self.db

    # =========================================================================
    # STEP 0: ACTIVE SECTORS
    # =========================================================================
    def fetch_active_sectors(self, mode, sectors, test_sectors):
        """Resolve the sectors this invocation will process.

        Normal path: `sectors` already holds the assigned bin (or TEST_SECTORS).
        Only a standalone PROD run with no bin falls through to qp_rm_master.
        """
        # Explicit sectors win in any mode (incl. a TEST run with custom sectors).
        if sectors:
            self.logger.info(f"Processing {len(sectors)} requested sector(s).")
            return list(sectors)

        if mode == 'TEST':
            self.logger.info(f"TEST mode — default sectors: {test_sectors}")
            return list(test_sectors)

        self.logger.info("No bin supplied — fetching ALL active sectors (standalone run).")
        try:
            docs = list(self.master_db.active_sectors.find({}, {"_id": 0, "sector": 1}))
            if not docs:
                raise RuntimeError("No active sectors found in qp_rm_master.active_sectors")
            resolved = [d['sector'] for d in docs if d.get('sector')]
            self.logger.info(f"Loaded {len(resolved)} active sectors.")
            return resolved
        except Exception as e:
            self.logger.error(f"Error fetching active sectors: {e}")
            self.logger.error(traceback.format_exc())
            raise

    # =========================================================================
    # TAX MASTER + PRICING CONSTANTS  (run-scoped -> cache.data)
    # =========================================================================
    def fetch_tax_master_data(self):
        """Domestic + international tax master + pricing constants -> cache.data."""
        self.logger.info("Fetching tax master data (domestic + international + pricing constants)...")
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
            self.cache.data['tax_mast_dom'] = dom_map
            self.logger.info(f"Fetched {len(dom_map)} domestic tax master records.")

            # --- International tax master: keyed by sector string e.g. "BOMKWI" ---
            intl_docs = list(self.master_db.tax_mast_intl.find({}, {"_id": 0}))
            intl_map = {}
            for doc in intl_docs:
                sector = str(doc.get('Sectors', '')).strip()
                if sector:
                    intl_map[sector] = {
                        'GST':      float(doc.get('GST', 0) or 0),
                        'CUTE':     float(doc.get('CUTE', 0) or 0),
                        'ASF':      float(doc.get('ASF', 0) or 0),
                        'PSF_ADF':  float(doc.get('PSF_ADF', 0) or 0),
                        'UDF':      float(doc.get('UDF', 0) or 0),
                        'PSC_PZ':   float(doc.get('PSC_PZ', 0) or 0),
                        'RCS_CUTE': float(doc.get('RCS_CUTE', 0) or 0),
                        'RCS':      float(doc.get('RCS', 0) or 0),
                        'Tax1':     float(doc.get('Tax1', 0) or 0),
                        'Tax2':     float(doc.get('Tax2', 0) or 0),
                        'Tax3':     float(doc.get('Tax3', 0) or 0),
                        'Tax4':     float(doc.get('Tax4', 0) or 0),
                        'Tax5':     float(doc.get('Tax5', 0) or 0),
                    }
            self.cache.data['tax_mast_intl'] = intl_map
            self.logger.info(f"Fetched {len(intl_map)} international tax master records.")

            # --- Pricing constants (sector-level exclusion rules) ---
            param_keys = ['PRICING_DUF_SECTORS', 'PRICING_GST_SECTORS', 'PRICING_RCS_SECTORS']
            params_cursor = self.db.rm_parameter_values.find(
                {"parameterKey": {"$in": param_keys}},
                {"parameterKey": 1, "parameterValue": 1, "_id": 0}
            )
            pricing_constants = {
                'PRICING_DUF_SECTORS': {},
                'PRICING_GST_SECTORS': [],
                'PRICING_RCS_SECTORS': [],
            }
            for doc in params_cursor:
                key = doc['parameterKey']
                try:
                    pricing_constants[key] = json.loads(doc['parameterValue'])
                except (json.JSONDecodeError, TypeError) as e:
                    self.logger.warning(f"Could not parse pricing constant {key}: {e}")

            self.cache.data['pricing_duf_sectors'] = pricing_constants['PRICING_DUF_SECTORS']
            self.cache.data['pricing_gst_sectors'] = set(pricing_constants['PRICING_GST_SECTORS'])
            self.cache.data['pricing_rcs_sectors'] = set(pricing_constants['PRICING_RCS_SECTORS'])
            self.logger.info(
                f"Loaded pricing constants — "
                f"DUF sectors: {len(self.cache.data['pricing_duf_sectors'])}, "
                f"GST-exempt: {len(self.cache.data['pricing_gst_sectors'])}, "
                f"RCS-zero: {len(self.cache.data['pricing_rcs_sectors'])}"
            )
        except Exception as e:
            self.logger.error(f"Error fetching tax master data: {e}")
            self.logger.error(traceback.format_exc())
            self.cache.data['tax_mast_dom'] = {}
            self.cache.data['tax_mast_intl'] = {}
            self.cache.data['pricing_duf_sectors'] = {}
            self.cache.data['pricing_gst_sectors'] = set()
            self.cache.data['pricing_rcs_sectors'] = set()

    # =========================================================================
    # RAW FARES  (bucket computation stays in the processor)
    # =========================================================================
    def fetch_fares(self, sectors, required_rbds):
        """Raw QP all-in fares for these sectors from qp_dw_rmalloc.fares.

        Includes `currency` (native currency of the fare's TOTAL) so the caller
        can convert international fares to INR before any tax/bucket math —
        see apply_currency_conversion.
        """
        self.logger.info("Fetching fares from qp_dw_rmalloc.fares ...")
        docs = list(self.db.fares.find(
            {"SECTOR": {"$in": list(sectors)}, "TOTAL": {"$nin": [1, 99999]}, "route": {"$eq": "L"},
             "RBD": {"$in": list(required_rbds)}},
            {"_id": 0, "SECTOR": 1, "RBD": 1, "TOTAL": 1, "currency": 1}
        ))
        if not docs:
            self.logger.error("No fare data found in qp_dw_rmalloc.fares.")
            raise RuntimeError("Fare data missing from MongoDB")
        fares_df = pd.DataFrame(docs)
        fares_df['TOTAL'] = pd.to_numeric(fares_df['TOTAL'], errors='coerce')
        fares_df = fares_df.dropna(subset=['TOTAL'])
        if 'currency' in fares_df.columns:
            fares_df['currency'] = fares_df['currency'].astype(str).str.strip().replace(
                {'': 'INR', 'nan': 'INR', 'None': 'INR'})
        else:
            fares_df['currency'] = 'INR'
        self.logger.info(f"Fetched {len(fares_df)} all-in fare records across "
                         f"{fares_df['SECTOR'].nunique()} sectors.")
        return fares_df

    # =========================================================================
    # CURRENCY CONVERSION  (international fares -> INR before tax/bucket math)
    # =========================================================================
    def fetch_international_stations(self):
        """{station} set of INTERNATIONAL stations from qp_rm_master.config_station_master
        (route == 'I'). A sector's origin (SECTOR[:3]) in this set means its QP
        fare TOTAL is denominated in the fare's local `currency`, not INR."""
        if self._intl_stations is not None:
            return self._intl_stations
        stations = set()
        try:
            docs = self.master_db.config_station_master.find(
                {'route': 'I'}, {'_id': 0, 'station': 1})
            stations = {str(d['station']).strip() for d in docs if d.get('station')}
            self.logger.info(f"[Currency] {len(stations)} international station(s) loaded "
                             f"from config_station_master.")
        except Exception as e:
            self.logger.error(f"[Currency] failed to load config_station_master "
                              f"({e.__class__.__name__}: {e}); no sector will be treated "
                              f"as international — fares used as-is.")
            stations = set()
        self._intl_stations = stations
        return stations

    def apply_currency_conversion(self, fares_df, fare_col='TOTAL'):
        """
        Convert `fare_col` from an international sector's LOCAL currency to INR.

        IMPORTANT — call this AFTER the airline-component conversion, not before.
        For an international sector BOTH the QP fare AND the international tax
        master (ASF/PSF/UDF/taxes/GST) are denominated in the origin's local
        currency, so the airline component
            (fare_local - taxes_local) / (1 + gst)
        must be computed while everything is still in local currency. Only the
        RESULTING airline component is then converted to INR here — converting the
        fare first would mix an INR fare with local-currency taxes. So the caller
        passes fare_col='AirlineComponent' (or whichever column holds the
        already-tax-stripped, still-local value).

        A sector's origin is international when SECTOR[:3] is in
        config_station_master (route='I') — e.g. JEDBOM, DOHBOM, BKKBOM. A
        sector like BOMJED (origin BOM, domestic) is NOT converted: QP already
        prices that fare in INR. Domestic-origin rows, and any row whose
        currency is already 'INR', pass through with ConversionRate = 1.0.

        Fully vectorized (no per-row Python loop). Rows for an international
        sector whose currency has no ->INR rate available (DW lookup miss or
        DW unreachable) are LEFT UNCONVERTED and logged — surfaced instead of
        silently mis-priced, but never dropped, so bucket coverage isn't lost.
        """
        df = fares_df.copy()
        if 'currency' not in df.columns:
            df['currency'] = 'INR'
        df['currency'] = df['currency'].astype(str).str.strip().replace(
            {'': 'INR', 'nan': 'INR', 'None': 'INR'})

        origin = df['SECTOR'].astype(str).str[:3]
        intl_stations = self.fetch_international_stations()
        is_intl_origin = origin.isin(intl_stations)
        needs_conversion = is_intl_origin & df['currency'].ne('INR')

        orig_col = f'{fare_col}_LOCAL'
        if not needs_conversion.any():
            self.logger.info(f"[Currency] no international-origin fares needing "
                             f"conversion — {fare_col} used as-is.")
            df[orig_col] = df[fare_col]
            return df

        needed_currencies = df.loc[needs_conversion, 'currency'].unique().tolist()
        if self.dw_helper is not None:
            rates_df = self.dw_helper.fetch_currency_conversion_rates(needed_currencies)
        else:
            self.logger.warning("[Currency] no DW helper bound — cannot fetch ->INR "
                                f"rates for {sorted(set(needed_currencies))}; "
                                "these fares stay unconverted.")
            rates_df = pd.DataFrame(columns=['currency', 'ConversionRate'])

        df = df.merge(rates_df, on='currency', how='left')
        df.loc[~needs_conversion, 'ConversionRate'] = 1.0   # domestic-origin / already-INR

        no_rate = needs_conversion & df['ConversionRate'].isna()
        if no_rate.any():
            affected = sorted(df.loc[no_rate, ['SECTOR', 'currency']]
                              .drop_duplicates().itertuples(index=False, name=None))
            self.logger.warning(
                f"[Currency] no ->INR rate for {int(no_rate.sum())} fare row(s) "
                f"({len(affected)} distinct sector/currency pair(s), e.g. "
                f"{affected[:10]}); leaving {fare_col} unconverted for these rows."
            )
            df.loc[no_rate, 'ConversionRate'] = 1.0   # leave value as-is (fallback)

        converted_mask = needs_conversion & (~no_rate)
        self.logger.info(
            f"[Currency] converting {fare_col} for {int(converted_mask.sum())} fare "
            f"row(s) across {df.loc[converted_mask, 'SECTOR'].nunique()} international "
            f"sector(s) to INR (currencies: "
            f"{sorted(df.loc[converted_mask, 'currency'].unique())})."
        )

        df[orig_col] = df[fare_col]
        df[fare_col] = df[fare_col] * df['ConversionRate']
        return df.drop(columns=['ConversionRate'])

    # =========================================================================
    # DEMAND MULTIPLIER  (flight-level: DepartureDate, Sector, FlightNumber)
    # =========================================================================
    def load_demand_multiplier(self):
        """{(Sector, DepartureDate(normalized Timestamp), FlightNumber:int): pct}."""
        if self._dm is not None:
            return self._dm
        dm = {}
        try:
            if self.mode == 'TEST':
                path = r'C:\Users\kenneth.menezes\Work\RM\rm-allocation-serverless-funcs\rm-additive-process\testcases\inputs\Demand_multiplier.csv'
                raw = pd.read_csv(path) if os.path.exists(path) else pd.DataFrame()
            else:
                coll = getattr(configsettings, 'ADDITIVE_DEMAND_MULTIPLIER_COLL',
                               'config_sector_demand_multiplier')
                raw = pd.DataFrame(list(self._config_db()[coll].find({}, {'_id': 0})))
            dm = self._normalize_demand_multiplier(raw)
            self.logger.info(f"[DemandMultiplier] loaded {len(dm)} flight-level entries ({self.mode}).")
        except Exception as e:
            self.logger.warning(f"[DemandMultiplier] load failed ({e.__class__.__name__}: {e}); "
                                f"defaulting every flight to 0%.")
        self._dm = dm
        return dm

    @staticmethod
    def _normalize_demand_multiplier(raw):
        if raw is None or raw.empty:
            return {}
        df = raw.rename(columns={c: str(c).strip() for c in raw.columns})
        dm_col = next((c for c in ('Demand_Multiplier', 'DemandMultiplier',
                                   'demand_multiplier') if c in df.columns), None)
        if dm_col is None or not {'DepartureDate', 'Sector', 'FlightNumber'}.issubset(df.columns):
            return {}

        def parse_pct(v):
            if pd.isna(v):
                return 0.0
            try:
                return float(str(v).strip().replace('%', ''))
            except ValueError:
                return 0.0

        df = df[['DepartureDate', 'Sector', 'FlightNumber', dm_col]].copy()
        df['_pct'] = df[dm_col].apply(parse_pct)
        df['DepartureDate'] = pd.to_datetime(df['DepartureDate'], dayfirst=True,
                                             errors='coerce').dt.normalize()
        df['FlightNumber'] = pd.to_numeric(df['FlightNumber'], errors='coerce')
        df = df.dropna(subset=['DepartureDate', 'FlightNumber', 'Sector'])
        df['FlightNumber'] = df['FlightNumber'].astype(int)
        return {(s, d, int(f)): float(p)
                for s, d, f, p in zip(df['Sector'], df['DepartureDate'],
                                      df['FlightNumber'], df['_pct'])}

    # =========================================================================
    # WTP  (Sector x Month-Year)
    # =========================================================================
    def load_wtp(self):
        """{(Sector, year:int, month:int): pct_float}."""
        if self._wtp is not None:
            return self._wtp
        wtp = {}
        try:
            if self.mode == 'TEST':
                path = r'C:\Users\kenneth.menezes\Work\RM\rm-allocation-serverless-funcs\rm-additive-process\testcases\inputs\WTP.csv'
                raw = pd.read_csv(path) if os.path.exists(path) else pd.DataFrame()
            else:
                coll = getattr(configsettings, 'ADDITIVE_WTP_COLL', 'config_quarter_wtp')
                raw = pd.DataFrame(list(self._config_db()[coll].find({}, {'_id': 0})))
            wtp = self._normalize_wtp(raw)
            self.logger.info(f"[WTP] loaded {len(wtp)} Sector x month entries ({self.mode}).")
        except Exception as e:
            self.logger.warning(f"[WTP] load failed ({e.__class__.__name__}: {e}); "
                                f"defaulting every flight to 0%.")
        self._wtp = wtp
        return wtp

    @staticmethod
    def _normalize_wtp(raw):
        if raw is None or raw.empty:
            return {}
        df = raw.rename(columns={c: str(c).strip() for c in raw.columns})
        fac_col = next((c for c in ('WTP Factor', 'WTP_Factor', 'Factor', 'WTP')
                        if c in df.columns), None)
        if fac_col is None or 'Sector' not in df.columns or 'Year' not in df.columns:
            return {}

        def parse_pct(v):
            if pd.isna(v):
                return 0.0
            try:
                return float(str(v).strip().replace('%', ''))
            except ValueError:
                return 0.0

        _M = {m.lower(): i for i, m in enumerate(
            ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
             'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'], start=1)}

        def month_num(v):
            s = str(v).strip().lower()
            if s[:3] in _M:
                return _M[s[:3]]
            try:
                n = int(float(s))
                return n if 1 <= n <= 12 else None
            except ValueError:
                return None

        def quarter_months(v):
            s = str(v).strip().upper().replace('Q', '')
            try:
                q = int(float(s))
            except ValueError:
                return []
            return [3 * (q - 1) + 1, 3 * (q - 1) + 2, 3 * (q - 1) + 3] if 1 <= q <= 4 else []

        df['_pct'] = df[fac_col].apply(parse_pct)
        df['_year'] = pd.to_numeric(df['Year'], errors='coerce')
        df = df.dropna(subset=['_year'])
        df['_year'] = df['_year'].astype(int)

        out = {}
        if 'Month' in df.columns:
            df['_mn'] = df['Month'].apply(month_num)
            for s, y, m, pct in zip(df['Sector'], df['_year'], df['_mn'], df['_pct']):
                if s and m:
                    out[(s, int(y), int(m))] = float(pct)
        elif 'Quarter' in df.columns:
            for s, y, q, pct in zip(df['Sector'], df['_year'], df['Quarter'], df['_pct']):
                for m in quarter_months(q):
                    if s:
                        out[(s, int(y), int(m))] = float(pct)
        return out

    # =========================================================================
    # MAPPED SECTORS  (Sector -> Mapped Sector, 1:1 fallback donor)
    # =========================================================================
    def load_mapped_sectors(self):
        """{sector: mapped_sector} — the analyst-configured 1:1 fallback donor.

        A sector with thin/no history falls back to its mapped sector's flown
        data, tagging and NDO curves. TEST reads a local CSV; PROD reads
        qp_rm_master.config_additive_mapped_sectors.
        """
        if self._mapped_sectors is not None:
            return self._mapped_sectors
        mapping = {}
        try:
            if self.mode == 'TEST':
                path = os.path.join(
                    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                    'testcases', 'inputs', 'Mapped_sectors.csv')
                raw = pd.read_csv(path) if os.path.exists(path) else pd.DataFrame()
            else:
                coll = getattr(configsettings, 'ADDITIVE_MAPPED_SECTORS_COLL',
                               'config_additive_mapped_sectors')
                raw = pd.DataFrame(list(self._config_db()[coll].find({}, {'_id': 0})))
            mapping = self._normalize_mapped_sectors(raw)
            self.logger.info(f"[MappedSectors] loaded {len(mapping)} mapping(s) ({self.mode}).")
        except Exception as e:
            self.logger.warning(f"[MappedSectors] load failed ({e.__class__.__name__}: {e}); "
                                f"no sector fallbacks available.")
        self._mapped_sectors = mapping
        return mapping

    @staticmethod
    def _normalize_mapped_sectors(raw):
        """Return {sector: mapped_sector}; case-insensitive column match, 1:1."""
        if raw is None or raw.empty:
            return {}
        df = raw.rename(columns={c: str(c).strip().lower() for c in raw.columns})
        sec_col = next((c for c in ('sector',) if c in df.columns), None)
        map_col = next((c for c in ('mapped_sector', 'mappedsector',
                                    'mp_sector', 'mapped') if c in df.columns), None)
        if sec_col is None or map_col is None:
            return {}
        out = {}
        for s, m in zip(df[sec_col], df[map_col]):
            if pd.notna(s) and pd.notna(m):
                s, m = str(s).strip(), str(m).strip()
                if s and m and s != m:
                    out[s] = m
        return out

    # =========================================================================
    # AUDIT  (qp_dw_rmalloc.additive_model_run_audit — one doc per run)
    # =========================================================================
    def audit_upsert_sector(self, sector, stat):
        if self.mode != 'PROD' or self.db is None:
            self.logger.info(f"[audit] Skipped — mode={self.mode}")
            return
        try:
            result = self.db.additive_model_run_audit.update_one(
                {'run_id': self.run_id},
                {
                    '$set': {f'sector_stats.{sector}': stat, 'module': 'additive_pickup_model'},
                    '$setOnInsert': {
                        'start_time': __import__('datetime').datetime.now(),
                        'status': 'IN_PROGRESS',
                        'snapshot_date': self.snapshot_date,
                        'sectors_expected': self.sectors_expected,
                        'triggered_by': self.analyst,
                    },
                    '$inc': {'sectors_completed': 1},
                    '$addToSet': {'batches_seen': self.batch_index},
                },
                upsert=True,
            )
            self.logger.info(f"[audit] {sector}: matched={result.matched_count}, "
                             f"modified={result.modified_count}, upserted={result.upserted_id}")
        except Exception as e:
            self.logger.error(f"audit upsert failed for {sector}: {e}")

    def audit_finalize(self):
        """Last bin to finish closes the run (idempotent, guarded on IN_PROGRESS).
        Completion counted as distinct sectors in sector_stats."""
        if self.mode != 'PROD' or self.db is None:
            return
        try:
            doc = self.db.additive_model_run_audit.find_one(
                {'run_id': self.run_id},
                {'sectors_expected': 1, 'sector_stats': 1},
            )
            if not doc:
                return
            expected = doc.get('sectors_expected')
            completed = len(doc.get('sector_stats', {}))
            if expected is None or completed < expected:
                return
            stats = doc.get('sector_stats', {}) or {}
            n_fail = sum(1 for v in stats.values() if v.get('status') == 'FAILED')
            self.db.additive_model_run_audit.update_one(
                {'run_id': self.run_id, 'status': 'IN_PROGRESS'},
                {'$set': {
                    'end_time': __import__('datetime').datetime.now(),
                    'status': 'SUCCESS' if n_fail == 0 else 'PARTIAL',
                    'sectors_failed_count': n_fail,
                }},
            )
        except Exception as e:
            self.logger.error(f"audit finalise failed: {e}")