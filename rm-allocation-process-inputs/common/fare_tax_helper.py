import json
import logging
import os
import pandas as pd


def _safe_float(val, default=0.0):
    if val is None or pd.isna(val):
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


class FareTaxHelper:
    """
    Helper for bidirectional conversion between All-In (Gross) Fares and Airline Component Fares.
    Adheres strictly to the additive-process tax conversion logic from `additive_model.py`:

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
        """Loads tax masters and pricing constants via rm_database_helper."""
        if self._loaded:
            return

        dom_docs = []
        intl_docs = []
        pricing_params = {}

        if self.rmdatabasehelper is not None:
            if hasattr(self.rmdatabasehelper, 'get_tax_master_domestic'):
                dom_docs = self.rmdatabasehelper.get_tax_master_domestic()
            if hasattr(self.rmdatabasehelper, 'get_tax_master_international'):
                intl_docs = self.rmdatabasehelper.get_tax_master_international()
            if hasattr(self.rmdatabasehelper, 'get_pricing_tax_parameters'):
                pricing_params = self.rmdatabasehelper.get_pricing_tax_parameters()

        candidate_dbs = [
            self.mongo_master_db,
            getattr(self.rmdatabasehelper, 'master_db', None),
            self.db,
            getattr(self.rmdatabasehelper, 'mongoclient', None)
        ]
        from helper.rm_database_helper import RMDatabaseHelper
        if not dom_docs:
            dom_docs = RMDatabaseHelper.fetch_tax_master_domestic(candidate_dbs)
        if not intl_docs:
            intl_docs = RMDatabaseHelper.fetch_tax_master_international(candidate_dbs)
        if not pricing_params:
            pricing_params = RMDatabaseHelper.fetch_pricing_tax_parameters(candidate_dbs)

        # 1. Populate Domestic Tax Map
        for doc in dom_docs or []:
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

        # 2. Populate International Tax Map
        for doc in intl_docs or []:
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

        # 3. Populate Pricing Constants
        if pricing_params:
            self._duf_sectors.update(pricing_params.get('duf_sectors', {}))
            self._gst_sectors.update(pricing_params.get('gst_sectors', set()))

        # Fallback to constants_utils if not found in db
        if not self._gst_sectors:
            gst_const = getattr(self.constants_utils, 'PRICING_GST_SECTORS', ['IXB', 'GAU', 'IXA', 'AJL'])
            self._gst_sectors = set(gst_const) if gst_const else {'IXB', 'GAU', 'IXA', 'AJL'}

        if not self._duf_sectors:
            duf_const = getattr(self.constants_utils, 'PRICING_DUF_SECTORS', {})
            self._duf_sectors = dict(duf_const) if duf_const else {}

        self._loaded = True

    def _sector_other_taxes(self, sector):
        """
        Pass-through (non-airline) taxes and the applicable GST rate for a sector.
        These are exactly the components to strip when going all-in -> airline comp.
        Exact logic from additive_model.py:_sector_other_taxes.
        """
        self._ensure_loaded()
        sector = str(sector).strip().replace('/', '')
        if len(sector) < 6:
            return 0.0, 0.0, False

        origin, destination = sector[:3], sector[3:6]

        # A sector present in the international master is international.
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

    def is_domestic(self, origin, destination, route=None):
        """Checks if a sector is domestic or international."""
        if route == 'I':
            return False
        if route == 'D':
            return True
        self._ensure_loaded()
        sector_key = f"{origin}{destination}".replace('/', '')
        return sector_key not in self._intl_map

    def allin_to_airline_component(self, gross_fare, origin, destination, route=None):
        """
        Convert all-in fare to airline component:
            airline_comp = (allin - other_taxes) / (1 + gst_rate)
        """
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
            # Fallback if sector tax master row is missing
            if origin in self._gst_sectors or destination in self._gst_sectors:
                gst_rate = 0.0
            else:
                gst_rate = 0.05
            other_taxes = 0.0

        airline_comp = (gross - other_taxes) / (1.0 + gst_rate)
        return max(0.0, float(airline_comp))

    def airline_component_to_allin(self, airline_comp, origin, destination, route=None):
        """
        Convert airline component to all-in fare:
            allin = (airline_comp * (1 + gst_rate)) + other_taxes
        """
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

    def fares_to_airline_component(self, fares_df, fare_col='TOTAL'):
        """
        Convert all-in fares to airline component (base + RCS + CUTE + YQ):

            airline_comp = (allin - other_taxes) / (1 + gst_rate)

        Exact implementation from additive_model.py:_fares_to_airline_component.
        Sectors with no tax master row are dropped.
        """
        if fares_df is None or fares_df.empty:
            return fares_df
        self._ensure_loaded()
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
            self.logger.warning(f"No tax master entry for {len(unresolved)} sectors — "
                                f"excluded from fare buckets: {sorted(unresolved)[:10]}"
                                f"{' ...' if len(unresolved) > 10 else ''}")
            df = df[~df['SECTOR'].isin(unresolved)]

        if df.empty:
            return df

        df['_other_taxes'] = df['SECTOR'].map(lambda s: lookup[s][0])
        df['_gst_rate'] = df['SECTOR'].map(lambda s: lookup[s][1])

        df['AirlineComponent'] = (df[fare_col] - df['_other_taxes']) / (1 + df['_gst_rate'])

        bad = df['AirlineComponent'] <= 0
        if bad.any():
            self.logger.warning(f"Dropping {int(bad.sum())} fare rows with non-positive "
                                f"airline component (all-in below pass-through taxes).")
            df = df[~bad]

        df.drop(columns=['_other_taxes', '_gst_rate'], inplace=True)
        return df

    # Alias matching additive_model.py method name
    _fares_to_airline_component = fares_to_airline_component

    def convert_market_fares_dataframe(self, df, origin, destination, gross_col='gross_fare', dest_col='airline_component', route=None):
        """
        Helper to batch-convert competitor market fares dataframe from all-in to airline component.
        """
        if df is None or df.empty or gross_col not in df.columns:
            return df

        sector = f"{origin}{destination}".replace('/', '')
        other_taxes, gst_rate, ok = self._sector_other_taxes(sector)

        if not ok:
            if origin in self._gst_sectors or destination in self._gst_sectors:
                gst_rate = 0.0
            else:
                gst_rate = 0.05
            other_taxes = 0.0

        res = df.copy()
        numeric_gross = pd.to_numeric(res[gross_col], errors='coerce')
        res[dest_col] = (numeric_gross - other_taxes) / (1.0 + gst_rate)
        res[dest_col] = res[dest_col].clip(lower=0.0)
        return res
