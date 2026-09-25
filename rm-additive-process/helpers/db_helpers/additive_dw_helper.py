"""
helpers/db_helpers/additive_dw_helper.py
=============================================================================
All DW (RM_DW_DATA, MySQL) SQL reads for the Additive Pickup Model.

Currently: currency_conversion — ->INR conversion rates for international fares
(fares denominated in an origin station's local currency must be converted to
INR before any tax stripping / fare-bucket math, since every other money source
in the model is INR).

Injected engine + shared cache (same pattern as AdditiveEsbHelper). The engine
is optional — if it is not supplied (dwconn missing from params), rate lookups
return empty and the caller leaves those fares unconverted (and logs it).
"""

import pandas as pd
from sqlalchemy import text


class AdditiveDwHelper:
    """RM_DW_DATA (MySQL) data access for the Additive Pickup Model."""

    def __init__(self, dw_engine, logger):
        self.engine = dw_engine
        self.logger = logger
        self.cache = None

    def bind_context(self, cache):
        self.cache = cache
        return self

    # =========================================================================
    # CURRENCY CONVERSION RATES  (RM_DW_DATA.currency_conversion)
    # =========================================================================
    def fetch_currency_conversion_rates(self, currencies):
        """
        Targeted ->INR conversion rates from RM_DW_DATA.currency_conversion for
        the given source currencies. INR is excluded by the caller (rate 1.0).

        Returns a DataFrame ['currency', 'ConversionRate']. Empty (not raised) on
        any failure — the caller falls back to leaving those fares unconverted
        and logs which sectors/currencies were affected.
        """
        empty = pd.DataFrame(columns=['currency', 'ConversionRate'])
        currencies = sorted({str(c).strip() for c in currencies if c and str(c).strip() != 'INR'})
        if not currencies:
            return empty
        if self.engine is None:
            self.logger.warning(f"[Currency] no DW engine injected — cannot fetch "
                                f"->INR rates for {currencies}; these fares stay unconverted.")
            return empty
        try:
            params = {f"c{i}": ccy for i, ccy in enumerate(currencies)}
            placeholders = ", ".join(f":{name}" for name in params)
            query = text(
                "SELECT FromCurrencyCode AS currency, ConversionRate "
                "FROM currency_conversion "
                "WHERE ToCurrencyCode = 'INR' "
                f"AND FromCurrencyCode IN ({placeholders})"
            )
            with self.engine.connect() as conn:
                df = pd.read_sql(query, conn, params=params)
            df['currency'] = df['currency'].astype(str).str.strip()
            df['ConversionRate'] = pd.to_numeric(df['ConversionRate'], errors='coerce')
            df = df.dropna(subset=['currency', 'ConversionRate'])
            df = df.drop_duplicates(subset=['currency'])
            self.logger.info(f"[Currency] fetched {len(df)}/{len(currencies)} ->INR "
                             f"rate(s) from RM_DW_DATA.currency_conversion.")
            return df
        except Exception as e:
            self.logger.error(f"[Currency] RM_DW_DATA.currency_conversion lookup failed "
                              f"for {currencies} ({e.__class__.__name__}: {e}); "
                              f"these fares stay unconverted.")
            return empty
