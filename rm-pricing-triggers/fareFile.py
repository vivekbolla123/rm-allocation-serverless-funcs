import io
import boto3
import pandas as pd
import logging
import os
import configsettings
from constants_utils import ConstantsUtils
from datetime import datetime, timedelta, date
import traceback
import uuid
import pytz
import numpy as np
from helpers.pa_db_helpers.fare_pa_helper import FarePAHelper


class FareFiling:
    def __init__(self, logger, params, overrides, context):
        """Initialize with required connections and constants."""
        self.logger = logger
        self.db = params['mongo']
        self.master_db = params.get('mongo_master', params['mongo'])
        self.constants = ConstantsUtils(self.db)
        self.pa_helper = FarePAHelper(self.db, logger, self.master_db)
        self.type = overrides['type']
        self.route = overrides['route']
        self.analystName = overrides['analyst']

        # Nav DB connection for city pair distance lookup
        self.nav_conn = params.get('navconn', None)
        self.nav_db_dw_name = configsettings.NAV_DB_DW_NAME
        self.cwt_upsell_params = self.constants.CWT_UPSELL

        # Initialize data containers
        self.qp_fares_data = None
        self.product_upsell_data = None
        self.product_upsell_master_data = None
        self.product_rules_data = None
        self.tax_master_dom_data = None
        self.tax_master_intl_data = None
        self.saver_data = None
        self.footnote_master_data = None
        self.currentVersion = None
        self.isSaver = True if self.type == "Saver" else False
        self.city_pair_data = None
        self.B2B_RBD_LIST = self.constants.B2B_RBD_VALUES.split(',')
        self.B2C_RBD_LIST = self.constants.B2C_RBD_VALUES.split(',')
        self.TOTAL_B2C_FARES = len(self.B2C_RBD_LIST)
        self.TOTAL_B2B_FARES = len(self.B2B_RBD_LIST)
        self.B2B_SAVER = '2001'
        self.B2C_SAVER = '1001'

        # RBD list for iteration
        self.rbd_list = None

        self.existing_qp_sectors = set()

        # Currency mapping for international sectors
        self.sector_currency_map = {}

    # ─────────────────────────────────────────────────────────────────────────
    # CwT helpers
    # ─────────────────────────────────────────────────────────────────────────
    # Domestic only: primary rule -> CwT companion rule
    CWT_PRIMARY_TO_COMPANION = {
        '1001': '5001',
        '1002': '5002',
        '1006': '5006',
    }

    def _get_cwt_upsell_params(self, cwt_rule: str) -> dict:
        """Return upsell parameters for a CwT rule from constants (fallback)."""
        default = {'type': 'PERCENTAGE', 'value': 5, 'min_value': None, 'max_value': None}
        return self.cwt_upsell_params.get(cwt_rule, default)

    # ─────────────────────────────────────────────────────────────────────────

    def collect_all_data(self):
        """Collect all required data for fare filing based on type."""
        try:
            if self.isSaver:
                self.saver_data = self.pa_helper.collect_saver_data(self.isSaver)

                if self.saver_data.empty:
                    raise ValueError("Saver table is empty")

                saver_sectors = self.saver_data['sector'].unique().tolist()
                self.logger.info(f"Found {len(saver_sectors)} unique sectors in saver table: {saver_sectors}")

                self.existing_qp_sectors = self.pa_helper._identify_existing_qp_sectors(saver_sectors)
                self.product_upsell_data = self.pa_helper.collect_upsell_data(self.isSaver)

                upsell_sectors = self.product_upsell_data['sector'].unique().tolist() if not self.product_upsell_data.empty else []
                missing_sectors = [s for s in saver_sectors if s not in upsell_sectors]

                if missing_sectors:
                    self.logger.info(f"Found {len(missing_sectors)} sectors in saver not in product_upsell, checking master")
                    self.product_upsell_master_data = self.pa_helper.collect_upsell_master_data(self.isSaver)

                    if self.product_upsell_master_data.empty:
                        raise ValueError(f"Sectors {missing_sectors} not found in product_upsell and product_upsell_master is empty")

                    master_sectors = self.product_upsell_master_data['sector'].unique().tolist()
                    still_missing = [s for s in missing_sectors if s not in master_sectors]
                    if still_missing:
                        raise ValueError(f"Sectors {still_missing} not found in product_upsell or product_upsell_master")

                    master_rows = self.product_upsell_master_data[
                        self.product_upsell_master_data['sector'].isin(missing_sectors)
                    ]
                    self.product_upsell_data = pd.concat([self.product_upsell_data, master_rows], ignore_index=True)
                    self.logger.info(f"Added {len(master_rows)} rows from master for missing sectors")

                if not self.product_upsell_data.empty:
                    original_count = len(self.product_upsell_data)
                    self.product_upsell_data = self.product_upsell_data[
                        self.product_upsell_data['sector'].isin(saver_sectors)
                    ]
                    if original_count > len(self.product_upsell_data):
                        self.logger.info(f"Filtered out {original_count - len(self.product_upsell_data)} upsell rows for sectors not in saver table")

                    if self.product_upsell_data.empty:
                        raise ValueError(f"No product upsell data found for saver sectors: {saver_sectors}")

            else:
                self.product_upsell_data = self.pa_helper.collect_upsell_data(self.isSaver)

                if self.product_upsell_data.empty:
                    raise ValueError("Product upsell table is empty")

                unique_sectors = self.product_upsell_data['sector'].unique().tolist()
                self.existing_qp_sectors = self.pa_helper._identify_existing_qp_sectors(unique_sectors)

                base_footnotes = []
                base_ndos = []

                if 'base_footnote' in self.product_upsell_data.columns:
                    base_footnotes = self.product_upsell_data['base_footnote'].dropna().unique().tolist()
                    base_footnotes.extend(['', None])

                if 'base_ndo' in self.product_upsell_data.columns:
                    base_ndos = self.product_upsell_data['base_ndo'].dropna().unique().tolist()
                    base_ndos.extend(['X', '', None])

                self.qp_fares_data = self.pa_helper.collect_qp_fares(
                    unique_sectors, self.route,
                    base_footnotes=base_footnotes if base_footnotes else None,
                    base_ndos=base_ndos if base_ndos else None
                )

                if self.qp_fares_data.empty:
                    raise ValueError(f"No QP fares found for any of the sectors: {unique_sectors}")

                fetched_sectors = self.qp_fares_data['SECTOR'].unique().tolist()
                missing_sectors = [s for s in unique_sectors if s not in fetched_sectors]
                if missing_sectors:
                    raise ValueError(
                        f"VALIDATION FAILED: {len(missing_sectors)} sector(s) from product_upsell "
                        f"not found in qp_fares (rules 1001/2001).\n"
                        f"Missing sectors: {missing_sectors}\nFound sectors: {fetched_sectors}"
                    )

            self.product_rules_data = self.pa_helper.collect_product_rules_data()
            self.product_rules_dict = self.product_rules_data.set_index('rule_nbr')['letter'].to_dict()

            self.footnote_master_data = self.pa_helper.collect_footnote_master_data()

            self.tax_master_dom_data, self.tax_master_intl_data, error = self.pa_helper.collect_tax_master_data()
            if error:
                self.logger.warning(f"Tax master data issue: {error}")

            self.rbd_list = self.constants.RBD_VALUES.split(',')

            if self.route == 'I':
                self._build_currency_map()

            try:
                if self.nav_conn is not None:
                    query = f"""
                    SELECT
                        CityPairID, CityPair, CityPairGroup, DepartureStation, ArrivalStation, ActualDistance
                    FROM {self.nav_db_dw_name}dw.CityPair
                    """
                    self.city_pair_data = pd.read_sql(query, self.nav_conn)
                    self.logger.info(f"Collected {len(self.city_pair_data)} city pair records for YQ distance lookup")
                else:
                    self.logger.warning("No nav connection available for city pair data")
            except Exception as e:
                self.logger.warning(f"City pair data collection failed (non-critical): {str(e)}")

            return True

        except Exception as e:
            self.logger.error(f"Error in data collection: {str(e)}")
            self.logger.error(traceback.format_exc())
            raise

    def _build_currency_map(self):
        """Build currency mapping for international sectors."""
        try:
            source = self.saver_data if (self.isSaver and self.saver_data is not None and not self.saver_data.empty) else None
            if source is not None:
                for _, row in source.iterrows():
                    sector = row['sector']
                    currency = row.get('currency', 'INR')
                    self.sector_currency_map[sector] = currency if pd.notna(currency) and currency != '' else 'INR'
            elif self.qp_fares_data is not None and not self.qp_fares_data.empty:
                for _, row in self.qp_fares_data.iterrows():
                    sector = row['SECTOR']
                    currency = row.get('currency', 'INR')
                    self.sector_currency_map[sector] = currency if pd.notna(currency) and currency != '' else 'INR'

            self.logger.info(f"Built currency map for {len(self.sector_currency_map)} sectors")
        except Exception as e:
            self.logger.warning(f"Error building currency map: {str(e)}")
            self.sector_currency_map = {}

    def get_pos_from_saver(self, sector, rbd):
        """Get POS value from saver table."""
        try:
            if self.isSaver:
                fare_row = self.saver_data[
                    (self.saver_data['sector'] == sector) &
                    (self.saver_data['rbd'] == rbd)
                ]
                if not fare_row.empty:
                    pos = fare_row.iloc[0].get('pos', 'B')
                    return pos if pd.notna(pos) and pos != '' else 'B'
            return 'B'
        except Exception as e:
            self.logger.warning(f"Error getting POS for {sector}-{rbd}: {str(e)}")
            return 'B'

    def get_saver_fares(self, sector, rbd, base_footnote=None, base_ndo=None, pu_rule_nbr=None):
        """Get base saver fare for a given sector and RBD with footnote/NDO matching."""
        try:
            if self.isSaver:
                cond = (self.saver_data['sector'] == sector) & (self.saver_data['rbd'] == rbd)

                if base_footnote is not None:
                    if base_footnote == '':
                        cond = cond & (self.saver_data['footnote'].isna() | (self.saver_data['footnote'] == ''))
                    else:
                        cond = cond & (self.saver_data['footnote'] == base_footnote)

                if base_ndo is not None:
                    if base_ndo in ('', 'X'):
                        cond = cond & (self.saver_data['ndo'].isna() | self.saver_data['ndo'].isin(['', 'X']))
                    else:
                        cond = cond & (self.saver_data['ndo'] == base_ndo)

                row = self.saver_data[cond]
                return row.iloc[0]['fare'] if not row.empty else None
            else:
                cond = (self.qp_fares_data['SECTOR'] == sector) & (self.qp_fares_data['RBD'] == rbd)

                if base_footnote is not None:
                    if base_footnote == '':
                        cond = cond & (self.qp_fares_data['footnote'].isna() | (self.qp_fares_data['footnote'] == ''))
                    else:
                        cond = cond & (self.qp_fares_data['footnote'] == base_footnote)

                if base_ndo is not None:
                    if base_ndo in ('', 'X'):
                        cond = cond & (self.qp_fares_data['NDO'].isna() | self.qp_fares_data['NDO'].isin(['', 'X']))
                    else:
                        cond = cond & (self.qp_fares_data['NDO'] == base_ndo)

                row = self.qp_fares_data[cond]
                return row.iloc[0]['TOTAL'] if not row.empty else None

        except Exception as e:
            self.logger.warning(f"Error getting saver fare for {sector}-{rbd}: {str(e)}")
            return None

    def calculate_all_in_fare(self, base_fare, origin, destination, ftype):
        """Calculate all-in fare or extract base fare depending on ftype."""
        try:
            if self.route == 'D':
                return self._calculate_taxes_domestic(base_fare, origin, destination, ftype)
            elif self.route == 'I':
                return self._calculate_taxes_international(base_fare, origin, destination, ftype)
            else:
                raise ValueError(f"Invalid route: {self.route}")
        except Exception as e:
            self.logger.warning(f"Error calculating all-in fare: {str(e)}")
            return base_fare

    def _calculate_taxes_domestic(self, base_fare, origin, destination, ftype):
        """Calculate all taxes for domestic fare."""
        try:
            if self.tax_master_dom_data is None or self.tax_master_dom_data.empty:
                return 0
            tax_data = self.tax_master_dom_data[self.tax_master_dom_data['Code'] == origin]
            if tax_data.empty:
                return 0

            distance = self._get_distance(origin, destination)
            yq = self._get_yq_for_distance(distance)
            tax_row = tax_data.iloc[0]
            gst_rate = float(tax_row.get('GST', 0)) / 100
            rcs = float(tax_row.get('RCS', 0)) if pd.notna(tax_row.get('RCS')) else 0
            cute = float(tax_row.get('CUTE', 0)) if pd.notna(tax_row.get('CUTE')) else 0
            asf = float(tax_row.get('ASF', 0)) if pd.notna(tax_row.get('ASF')) else 0
            psf = float(tax_row.get('PSF', 0)) if pd.notna(tax_row.get('PSF')) else 0
            udf_df = float(tax_row.get('UDF_DF', 0)) if pd.notna(tax_row.get('UDF_DF')) else 0

            if destination in self.constants.PRICING_RCS_SECTORS:
                rcs = 0
            duf = float(self.constants.PRICING_DUF_SECTORS[destination]) if destination in self.constants.PRICING_DUF_SECTORS else 0
            if origin in self.constants.PRICING_GST_SECTORS or destination in self.constants.PRICING_GST_SECTORS:
                gst_rate = 0

            if ftype == 'base':
                return base_fare + asf + psf + udf_df + rcs + cute + gst_rate * (base_fare + rcs + cute) + duf + yq + (yq * gst_rate)
            else:
                return (base_fare - ((gst_rate * (rcs + cute)) + rcs + cute + duf + asf + psf + udf_df + yq + (yq * gst_rate))) / (1 + gst_rate)
        except Exception:
            return 0

    def _calculate_taxes_international(self, gross_fare, origin, destination, ftype):
        """Calculate all taxes for international fare."""
        try:
            if self.tax_master_intl_data is None or self.tax_master_intl_data.empty:
                return 0
            tax_data = self.tax_master_intl_data[self.tax_master_intl_data['Sectors'] == f"{origin}{destination}"]
            if tax_data.empty:
                return 0

            tax_row = tax_data.iloc[0]
            gst_rate = float(tax_row.get('GST', 0)) / 100
            cute = float(tax_row.get('CUTE', 0)) if pd.notna(tax_row.get('CUTE')) else 0
            asf = float(tax_row.get('ASF', 0)) if pd.notna(tax_row.get('ASF')) else 0
            psf_adf = float(tax_row.get('PSF_ADF', 0)) if pd.notna(tax_row.get('PSF_ADF')) else 0
            udf = float(tax_row.get('UDF', 0)) if pd.notna(tax_row.get('UDF')) else 0
            psc_pz = float(tax_row.get('PSC_PZ', 0)) if pd.notna(tax_row.get('PSC_PZ')) else 0
            rcs = float(tax_row.get('RCS', 0)) if pd.notna(tax_row.get('RCS')) else 0
            tax1 = float(tax_row.get('Tax1', 0)) if pd.notna(tax_row.get('Tax1')) else 0
            tax2 = float(tax_row.get('Tax2', 0)) if pd.notna(tax_row.get('Tax2')) else 0
            tax3 = float(tax_row.get('Tax3', 0)) if pd.notna(tax_row.get('Tax3')) else 0
            tax4 = float(tax_row.get('Tax4', 0)) if pd.notna(tax_row.get('Tax4')) else 0
            tax5 = float(tax_row.get('Tax5', 0)) if pd.notna(tax_row.get('Tax5')) else 0
            yq = float(tax_row.get('YQ', 0)) if pd.notna(tax_row.get('YQ')) else 0
            yq_gst_rate = float(tax_row.get('YQ_GST', 0)) / 100 if pd.notna(tax_row.get('YQ_GST')) else 0

            if ftype == 'base':
                gst = gst_rate * (gross_fare + rcs + cute)
                return gross_fare + asf + psf_adf + udf + psc_pz + rcs + cute + gst + tax1 + tax2 + tax3 + tax4 + tax5 + yq + (yq * yq_gst_rate)
            else:
                gst_on_taxes = gst_rate * (rcs + cute)
                other_taxes = asf + psf_adf + udf + psc_pz + tax1 + tax2 + tax3 + tax4 + tax5 + yq + (yq * yq_gst_rate)
                return (gross_fare - (rcs + cute + gst_on_taxes + other_taxes)) / (1 + gst_rate)
        except Exception as e:
            self.logger.error(f"Error calculating international fare: {str(e)}")

    def _get_distance(self, origin, destination):
        """Get distance for sector from city pair data."""
        try:
            if self.city_pair_data is None or self.city_pair_data.empty:
                return 0
            d = self.city_pair_data[
                (self.city_pair_data['DepartureStation'] == origin) &
                (self.city_pair_data['ArrivalStation'] == destination)
            ]
            return float(d.iloc[0]['ActualDistance']) if not d.empty else 0
        except Exception:
            return 0

    def _get_yq_for_distance(self, distance):
        """Get YQ based on distance (km) for domestic routes."""
        if distance <= 0:
            return 0
        try:
            for min_dist, max_dist, yq_val in self.constants.YQ_DISTANCE_CONFIG:
                if min_dist < distance <= max_dist:
                    return float(yq_val)
        except AttributeError:
            self.logger.warning("YQ_DISTANCE_CONFIG not found in constants.")
        return 0

    def apply_upsell(self, base_value, upsell_type, upsell_value, min_value, max_value):
        """Apply upsell to base value with min/max constraints."""
        try:
            upsell_amount = base_value * (upsell_value / 100) if upsell_type == 'PERCENTAGE' else upsell_value
            if min_value is not None and pd.notna(min_value) and upsell_amount < min_value:
                upsell_amount = min_value
            if max_value is not None and pd.notna(max_value) and upsell_amount > max_value:
                upsell_amount = max_value
            return base_value + upsell_amount
        except Exception as e:
            self.logger.warning(f"Error applying upsell: {str(e)}")
            return base_value

    def get_rbd_range(self, rbd_start, rbd_end):
        """Get list of RBDs between start and end (inclusive)."""
        try:
            start_idx = self.rbd_list.index(rbd_start)
            end_idx = self.rbd_list.index(rbd_end)
            return self.rbd_list[start_idx:end_idx + 1]
        except ValueError as e:
            self.logger.warning(f"Invalid RBD range {rbd_start}-{rbd_end}: {str(e)}")
            return []

    def generate_farebasis(self, rbd, rule_nbr, ow_rt, ndo, sector,
                           pos=None, route_type='D', footnote='', currency='INR'):
        """Generate fare basis code following the 8-character format."""
        try:
            letter = self.product_rules_dict.get(rule_nbr, 'R')
            ow_rt_char = 'O' if ow_rt == 'O' else 'R' if ow_rt == 'R' else 'C' if ow_rt == 'C' else ow_rt
            route_char = ('I' if currency == 'INR' else 'Z') if route_type == 'I' else 'I'
            ndo_char = ndo if pd.notna(ndo) and ndo else 'X'
            sixth_char = pos if (pos is not None and pos != '') else ('B' if sector in self.existing_qp_sectors else 'W')
            return f"{rbd}{ow_rt_char}7{letter}{sixth_char}{route_char}{ndo_char}"
        except Exception as e:
            self.logger.warning(f"Error generating farebasis: {str(e)}")
            return f"{rbd}O7RWIX"

    # ─────────────────────────────────────────────────────────────────────────
    # Shared utilities
    # ─────────────────────────────────────────────────────────────────────────

    def _round_fare(self, fare, currency):
        """Round fare based on currency."""
        if currency == 'INR' or currency not in self.constants.CURRENCY_ROUND_OFF_LIST:
            return round(fare)
        return round(fare, int(self.constants.CURRENCY_ROUND_OFF_LIST[currency]))

    def _parse_date(self, date_str):
        """Parse a date string in MM/DD/YYYY format. Returns None if invalid/empty."""
        if not date_str or (isinstance(date_str, str) and date_str.strip() == ''):
            return None
        if isinstance(date_str, str) and date_str.strip().lower() == 'open':
            return None
        try:
            return datetime.strptime(date_str.strip(), '%m/%d/%Y').date()
        except (ValueError, AttributeError):
            return None

    def _resolve_footnote(self, upsell_row):
        """
        Resolve the footnote for an upsell row.
        
        Priority:
          1. If the upsell row has a non-empty 'footnote' value, use it directly.
          2. Otherwise, use the sales/travel date range columns from the upsell row
             to find a matching footnote in footnote_master.

        Date range matching logic:
          - If upsell has sales dates, the master must have sales dates that cover the upsell range.
          - If upsell has travel dates, the master must have travel dates that cover the upsell range.
          - If upsell has both, the master must cover both ranges.
          - The master may have additional date types beyond what the upsell specifies.
          - Empty start date -> treat as current date.
          - Empty/Open end date -> treat as start date + 365 days.
          - When a date type is EMPTY on the upsell row (e.g. no travel dates at all),
            match master rows where that type's end is also empty/open AND start <= today
            (i.e. currently active open-ended entries).

        Returns:
            The resolved footnote string, or '' if no match found.
        """
        # Priority 1: direct footnote on upsell row
        direct_footnote = upsell_row.get('footnote', '')
        if pd.notna(direct_footnote) and str(direct_footnote).strip() != '':
            return str(direct_footnote).strip()

        # If no footnote master data loaded, return empty
        if self.footnote_master_data is None or self.footnote_master_data.empty:
            return ''

        today = date.today()

        # Extract upsell row date fields
        u_sales_start = upsell_row.get('sales_start', '')
        u_sales_end = upsell_row.get('sales_end', '')
        u_travel_start = upsell_row.get('travel_start', '')
        u_travel_end = upsell_row.get('travel_end', '')

        # Normalize NaN to empty string
        if pd.isna(u_sales_start):
            u_sales_start = ''
        if pd.isna(u_sales_end):
            u_sales_end = ''
        if pd.isna(u_travel_start):
            u_travel_start = ''
        if pd.isna(u_travel_end):
            u_travel_end = ''

        u_sales_start = str(u_sales_start).strip()
        u_sales_end = str(u_sales_end).strip()
        u_travel_start = str(u_travel_start).strip()
        u_travel_end = str(u_travel_end).strip()

        has_sales = (u_sales_start != '' or u_sales_end != '')
        has_travel = (u_travel_start != '' or (u_travel_end != '' and u_travel_end.lower() != 'open'))

        # If no date info at all, no footnote to resolve
        if not has_sales and not has_travel:
            return ''

        # Build effective date range from start/end strings
        def _effective_range(start_str, end_str):
            """Convert start/end strings to effective date range."""
            start_dt = self._parse_date(start_str)
            end_dt = self._parse_date(end_str)

            if start_dt is None:
                start_dt = today
            if end_dt is None:
                end_dt = start_dt + timedelta(days=365)

            return start_dt, end_dt

        u_sales_range = _effective_range(u_sales_start, u_sales_end) if has_sales else None
        u_travel_range = _effective_range(u_travel_start, u_travel_end) if has_travel else None

        # Search footnote_master for the best (narrowest) match
        best_match = None
        best_span = None  # total days span of the matching master range (smaller = more specific)

        for _, fm_row in self.footnote_master_data.iterrows():
            fm_footnote = fm_row.get('footnote', '')
            if pd.isna(fm_footnote) or str(fm_footnote).strip() == '':
                continue

            fm_sales_start = str(fm_row.get('sales_start', '')).strip() if pd.notna(fm_row.get('sales_start', '')) else ''
            fm_sales_end = str(fm_row.get('sales_end', '')).strip() if pd.notna(fm_row.get('sales_end', '')) else ''
            fm_travel_start = str(fm_row.get('travel_start', '')).strip() if pd.notna(fm_row.get('travel_start', '')) else ''
            fm_travel_end = str(fm_row.get('travel_end', '')).strip() if pd.notna(fm_row.get('travel_end', '')) else ''

            fm_has_sales = (fm_sales_start != '' or fm_sales_end != '')
            fm_has_travel = (fm_travel_start != '' or (fm_travel_end != '' and fm_travel_end.lower() != 'open'))

            # If upsell needs sales match but master has no sales dates, skip
            if has_sales and not fm_has_sales:
                continue

            # If upsell needs travel match but master has no travel dates, skip
            if has_travel and not fm_has_travel:
                continue

            # Check sales range: upsell sales range must fall within master sales range
            fm_s_range = None
            if has_sales and u_sales_range:
                fm_s_range = _effective_range(fm_sales_start, fm_sales_end)
                if u_sales_range[0] < fm_s_range[0] or u_sales_range[1] > fm_s_range[1]:
                    continue

            # Check travel range: upsell travel range must fall within master travel range
            fm_t_range = None
            if has_travel and u_travel_range:
                fm_t_range = _effective_range(fm_travel_start, fm_travel_end)
                if u_travel_range[0] < fm_t_range[0] or u_travel_range[1] > fm_t_range[1]:
                    continue

            # Calculate span (narrower = better match)
            span = 0
            if fm_s_range:
                span += (fm_s_range[1] - fm_s_range[0]).days
            if fm_t_range:
                span += (fm_t_range[1] - fm_t_range[0]).days

            if best_match is None or span < best_span:
                best_match = str(fm_footnote).strip()
                best_span = span

        if best_match:
            return best_match

        # No match found
        self.logger.warning(
            f"No footnote master match for sales=({u_sales_start} to {u_sales_end}), "
            f"travel=({u_travel_start} to {u_travel_end})"
        )
        return ''

    def _make_output_row(self, sector, rbd, rule_nbr, fare, sort_price,
                         ow_rt, footnote, ndo, pos, currency, farebasis,
                         actual_origin, actual_destination, fare_direction,
                         flt_nbr=''):
        """Build a standardised output row dict."""
        return {
            'Carrier_Code': 'QP',
            'Action_Code': 'I',
            'Origin': actual_origin,
            'Destination': actual_destination,
            'Fare_Basis_Code': farebasis,
            'OW_RT': ow_rt,
            'Routing': '',
            'Footnote': footnote,
            'Currency': currency,
            'Fare_Amount': fare,
            'Point_Price': 0,
            'Effective_Date': '',
            'Tariff_No': '',
            'Rule_Nbr': str(rule_nbr),
            'Link': '',
            'Sequence': '',
            'SOS_Charge': 0,
            'Sort_Price': sort_price,
            'Fare_Direction': fare_direction,
            'Trip_Direction': '',
            'Inbound_Outbound': '',
            'Discontinue_Date': '',
            'In_Batch': '',
            'OpenJaw': 'TRUE',
            'RBD': rbd,
            'Sector': sector,
            'flt_nbr': flt_nbr,
        }

    # ─────────────────────────────────────────────────────────────────────────
    # Core per-row upsell processing
    # ─────────────────────────────────────────────────────────────────────────

    def _process_single_upsell_row(self, upsell_row, sector, rbd, rule_nbr,
                                    origin, destination, currency,
                                    existing_sort_prices):
        """
        Process one upsell input row for a given sector/rbd/rule_nbr.
        Returns a completed output row dict, or None if saver fare is missing.
        """
        pos_value = upsell_row.get('pos', 'B')
        if pd.isna(pos_value) or pos_value == '':
            pos_value = 'B'

        base_footnote = upsell_row.get('base_footnote', None)
        if pd.isna(base_footnote):
            base_footnote = None

        base_ndo = upsell_row.get('base_ndo', None)
        if pd.isna(base_ndo):
            base_ndo = None

        saver_allin_fare = self.get_saver_fares(sector, rbd, base_footnote, base_ndo, rule_nbr)
        if saver_allin_fare is None:
            self.logger.warning(
                f"No saver fare found for {sector}-{rbd} "
                f"(base_footnote: {base_footnote}, base_ndo: {base_ndo})"
            )
            return None

        saver_base_fare = self.calculate_all_in_fare(saver_allin_fare, origin, destination, ftype="allin")

        base_all_in = upsell_row.get('base_all_in', 'All-in')
        if pd.isna(base_all_in) or base_all_in == '':
            base_all_in = 'All-in'

        fare_for_upsell = saver_base_fare if base_all_in == 'Base' else saver_allin_fare
        if not self.isSaver:
            fare_for_upsell = self.calculate_all_in_fare(fare_for_upsell, origin, destination, ftype="base")

        upsell_type = upsell_row['type']
        upsell_value = float(upsell_row['value']) if pd.notna(upsell_row['value']) else 0
        min_value = float(upsell_row['min_value']) if (pd.notna(upsell_row['min_value']) and upsell_row['min_value'] != '') else None
        max_value = float(upsell_row['max_value']) if (pd.notna(upsell_row['max_value']) and upsell_row['max_value'] != '') else None

        upsold_fare = self.apply_upsell(fare_for_upsell, upsell_type, upsell_value, min_value, max_value)

        if base_all_in == 'Base':
            final_base_fare = self._round_fare(upsold_fare, currency)
        else:
            final_base_fare = self._round_fare(
                self.calculate_all_in_fare(upsold_fare, origin, destination, ftype="allin"),
                currency
            )

        # ow_rt: drives OW_RT output column, defaults to 'O'
        ow_rt = upsell_row.get('ow_rt', 'O')
        if pd.isna(ow_rt) or ow_rt == '':
            ow_rt = 'O'

        ndo = upsell_row.get('ndo', 'X')
        if pd.isna(ndo) or ndo == '':
            ndo = 'X'

        footnote = self._resolve_footnote(upsell_row)

        flt_nbr = upsell_row.get('flt_nbr', '')
        if pd.isna(flt_nbr):
            flt_nbr = ''
        flt_nbr = str(flt_nbr).strip()

        flt_tag = upsell_row.get('flt_tag', '')
        if pd.isna(flt_tag):
            flt_tag = ''
        flt_tag = str(flt_tag).strip()

        sort_price = upsell_row.get('sort_price')
        if pd.isna(sort_price) or sort_price == '':
            sort_price = final_base_fare
        else:
            sort_price = float(sort_price)

        # Farebasis 3rd char: flt_tag if non-empty, else ow_rt, else 'O'
        farebasis_owrt = flt_tag if flt_tag else (ow_rt if ow_rt else 'O')

        farebasis = self.generate_farebasis(
            rbd, rule_nbr,
            'C' if rule_nbr[1] == '2' else farebasis_owrt,
            ndo, sector, pos=pos_value,
            route_type=self.route, footnote=footnote, currency=currency
        )

        actual_origin, actual_destination, fare_direction = self.pa_helper._determine_fare_direction(sector)

        return self._make_output_row(
            sector, rbd, rule_nbr, final_base_fare, sort_price,
            ow_rt, footnote, ndo, pos_value, currency, farebasis,
            actual_origin, actual_destination, fare_direction,
            flt_nbr=flt_nbr
        )

    # ─────────────────────────────────────────────────────────────────────────
    # flt_nbr default-ladder generation
    # ─────────────────────────────────────────────────────────────────────────

    def _generate_flt_nbr_default_row(self, sector, rbd, rule_nbr,
                                       flt_rows_output, explicit_default_row,
                                       origin, destination, currency):
        """
        Produce the default (no-flt_nbr) ladder for a group that has
        flight-specific rows.

        Priority:
          1. explicit_default_row — already fully computed, return as-is.
          2. Auto-generate: fare = max(Fare_Amount) across flt_rows_output.
             Default params: footnote='', ndo='X', ow_rt='O', pos='W'.

        Returns one output row dict, or None on failure.
        """
        if explicit_default_row is not None:
            return explicit_default_row

        if not flt_rows_output:
            return None

        max_fare = self._round_fare(max(r['Fare_Amount'] for r in flt_rows_output), currency)

        farebasis = self.generate_farebasis(
            rbd, rule_nbr, 'O', 'X', sector,
            pos='W', route_type=self.route, footnote='', currency=currency
        )

        actual_origin, actual_destination, fare_direction = self.pa_helper._determine_fare_direction(sector)

        return self._make_output_row(
            sector, rbd, rule_nbr, max_fare, max_fare,
            'O', '', 'X', 'W', currency, farebasis,
            actual_origin, actual_destination, fare_direction,
            flt_nbr=''
        )

    # ─────────────────────────────────────────────────────────────────────────
    # CwT companion row generation
    # ─────────────────────────────────────────────────────────────────────────

    def _find_cwt_config_row(self, cwt_rule, sector, footnote, ndo, flt_nbr):
        """
        Look up a matching CwT config row in product_upsell_data for
        (cwt_rule, sector, footnote, ndo, flt_nbr).

        Returns the first matching Series, or None.
        """
        cwt_data = self.product_upsell_data[self.product_upsell_data['rule_nbr'] == cwt_rule]
        if cwt_data.empty:
            return None

        mask = cwt_data['sector'] == sector

        # Footnote match
        if footnote == '':
            mask = mask & (cwt_data['footnote'].isna() | (cwt_data['footnote'] == ''))
        else:
            mask = mask & (cwt_data['footnote'] == footnote)

        # NDO match
        if ndo in ('X', ''):
            mask = mask & (cwt_data['ndo'].isna() | cwt_data['ndo'].isin(['', 'X']))
        else:
            mask = mask & (cwt_data['ndo'] == ndo)

        # flt_nbr match
        flt_str = str(flt_nbr).strip() if flt_nbr else ''
        if flt_str == '':
            mask = mask & (cwt_data['flt_nbr'].isna() | (cwt_data['flt_nbr'].astype(str).str.strip() == ''))
        else:
            mask = mask & (cwt_data['flt_nbr'].astype(str).str.strip() == flt_str)

        matched = cwt_data[mask]
        return matched.iloc[0] if not matched.empty else None

    def _generate_cwt_row_for_primary(self, primary_row, cwt_rule,
                                   cwt_config_row, existing_sort_prices):
        """
        Generate one CwT companion output row from a single primary output row.
        Only rule_nbr, fare_amount and sort_price change from the primary row.
        All other columns (farebasis, ow_rt, footnote, ndo, pos, flt_nbr, etc.)
        are mirrored directly from the primary row.

        cwt_config_row: matching row from product_upsell_data for this cwt_rule,
                        or None -> use constant fallback (self.cwt_upsell_params).
        """
        sector = primary_row['Sector']
        rbd = primary_row['RBD']
        currency = primary_row.get('Currency', 'INR')
        primary_fare = primary_row['Fare_Amount']
        flt_nbr = primary_row.get('flt_nbr', '')

        # Mirror these directly from primary — do NOT recompute
        ow_rt = primary_row.get('OW_RT', 'O')
        footnote = primary_row.get('Footnote', '')
        source_basis = primary_row.get('Fare_Basis_Code', '')

        # Upsell params: prefer table row, fall back to constants
        if cwt_config_row is not None:
            upsell_type = cwt_config_row['type']
            upsell_value = float(cwt_config_row['value']) if pd.notna(cwt_config_row['value']) else 0
            min_value = float(cwt_config_row['min_value']) if (pd.notna(cwt_config_row['min_value']) and cwt_config_row['min_value'] != '') else None
            max_value = float(cwt_config_row['max_value']) if (pd.notna(cwt_config_row['max_value']) and cwt_config_row['max_value'] != '') else None
        else:
            fallback = self._get_cwt_upsell_params(cwt_rule)
            upsell_type = fallback['type']
            upsell_value = fallback['value']
            min_value = fallback['min_value']
            max_value = fallback['max_value']

        cwt_fare = self._round_fare(
            self.apply_upsell(primary_fare, upsell_type, upsell_value, min_value, max_value),
            currency
        )

        # Sort price: existing dump if sector exists, else cwt_fare
        sort_price_key = f"{sector}_{rbd}_{cwt_rule}"
        if sector in self.existing_qp_sectors:
            sort_price = existing_sort_prices.get(sort_price_key, cwt_fare)
        else:
            sort_price = cwt_fare

        # Farebasis: replace only the rule letter (char index 3) in the source farebasis.
        # All other characters (ow_rt char, pos, route, ndo) stay identical to primary.
        cwt_letter = self.product_rules_dict.get(cwt_rule, 'R')
        if len(source_basis) == 8:
            cwt_farebasis = source_basis[:4] + cwt_letter + source_basis[5:]
        else:
            # Fallback: regenerate — should not normally happen
            ndo = source_basis[7] if len(source_basis) == 8 else 'X'
            pos = source_basis[5] if len(source_basis) == 8 else 'W'
            cwt_farebasis = self.generate_farebasis(
                rbd, cwt_rule, ow_rt, ndo, sector,
                pos=pos, route_type='D', footnote=footnote, currency=currency
            )

        actual_origin, actual_destination, fare_direction = self.pa_helper._determine_fare_direction(sector)

        return self._make_output_row(
            sector, rbd, cwt_rule, cwt_fare, sort_price,
            ow_rt, footnote,
            source_basis[7] if len(source_basis) == 8 else 'X',   # ndo
            source_basis[5] if len(source_basis) == 8 else 'W',   # pos
            currency, cwt_farebasis,
            actual_origin, actual_destination, fare_direction,
            flt_nbr=flt_nbr
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Main calculation entry point
    # ─────────────────────────────────────────────────────────────────────────

    def calculate_product_upsells(self):
        """Calculate product upsells for all sectors and rule numbers."""
        try:
            all_output_rows = []
            existing_sort_prices = self.pa_helper._get_existing_sort_prices()

            # Track primary rules processed for domestic 2001 companion generation.
            # Key: (sector, rbd, ow_rt, footnote, flt_nbr)  Value: {rule_nbr: fare}
            primary_rules_processed = {}

            # CwT rules and 2001 are excluded from the main loop
            CWT_RULES = set(self.CWT_PRIMARY_TO_COMPANION.values())   # {5001, 5002, 5006}
            SKIP_RULES = CWT_RULES | {self.B2B_SAVER}

            # Group by (sector, rule_nbr)
            unique_sector_rules = self.product_upsell_data.groupby(
                ['sector', 'rule_nbr']
            ).first().reset_index()

            for _, combo_row in unique_sector_rules.iterrows():
                sector = combo_row['sector']
                rule_nbr = combo_row['rule_nbr']

                if self.route == 'D' and rule_nbr in SKIP_RULES:
                    continue

                origin = sector[:3]
                destination = sector[3:]
                currency = self.sector_currency_map.get(sector, 'INR')

                all_rows_for_rule = self.product_upsell_data[
                    (self.product_upsell_data['sector'] == sector) &
                    (self.product_upsell_data['rule_nbr'] == rule_nbr)
                ]

                # Split into flight-specific and default rows
                flt_mask = (
                    all_rows_for_rule['flt_nbr'].notna() &
                    (all_rows_for_rule['flt_nbr'].astype(str).str.strip() != '')
                )
                flt_specific_rows = all_rows_for_rule[flt_mask]
                default_rows = all_rows_for_rule[~flt_mask]

                primary_rows_for_rule = []

                if not flt_specific_rows.empty:
                    # ── Flight-specific path ──────────────────────────────────────
                    flt_all_output = []

                    for _, flt_row in flt_specific_rows.iterrows():
                        rbd_range = self.get_rbd_range(flt_row['rbd_start'], flt_row['rbd_end'])
                        if not rbd_range:
                            self.logger.warning(
                                f"Invalid RBD range for {sector}-{rule_nbr} "
                                f"flt={flt_row['flt_nbr']}: "
                                f"{flt_row['rbd_start']} to {flt_row['rbd_end']}"
                            )
                            continue

                        for rbd in rbd_range:
                            result = self._process_single_upsell_row(
                                flt_row, sector, rbd, rule_nbr,
                                origin, destination, currency, existing_sort_prices
                            )
                            if result is not None:
                                flt_all_output.append(result)

                    primary_rows_for_rule.extend(flt_all_output)

                    # ── Default ladders: one per unique ow_rt value ───────────────
                    # Group flt_all_output by ow_rt, take max fare per (ow_rt, rbd).
                    # Default farebasis uses ow_rt (O if empty); OW_RT column = ow_rt.
                    actual_origin, actual_destination, fare_direction = self.pa_helper._determine_fare_direction(sector)

                    # Collect unique ow_rt values present in flight rows
                    ow_rt_values = set(r['OW_RT'] for r in flt_all_output)

                    for ow_rt_val in ow_rt_values:
                        # Build rbd -> max fare map for this ow_rt group
                        rbd_max_fare = {}
                        for flt_row in flt_all_output:
                            if flt_row['OW_RT'] != ow_rt_val:
                                continue
                            rbd = flt_row['RBD']
                            fare = flt_row['Fare_Amount']
                            if rbd not in rbd_max_fare or fare > rbd_max_fare[rbd]:
                                rbd_max_fare[rbd] = fare

                        # Farebasis 3rd char for default: ow_rt_val (already 'O' if was empty)
                        farebasis_owrt = ow_rt_val if ow_rt_val else 'O'

                        for rbd, max_fare in rbd_max_fare.items():
                            farebasis = self.generate_farebasis(
                                rbd, rule_nbr,
                                'C' if rule_nbr[1] == '2' else farebasis_owrt,
                                'X', sector,
                                pos='W', route_type=self.route, footnote='', currency=currency
                            )
                            default_row = self._make_output_row(
                                sector, rbd, rule_nbr, max_fare, max_fare,
                                ow_rt_val, '', 'X', 'W', currency, farebasis,
                                actual_origin, actual_destination, fare_direction,
                                flt_nbr=''
                            )
                            primary_rows_for_rule.append(default_row)

                    # ── Process non-flight default rows with different RBD ranges ─
                    if not default_rows.empty:
                        # Determine which RBDs are already covered by flight rows
                        flt_rbd_set = set(r['RBD'] for r in flt_all_output)

                        for (rbd_start, rbd_end), range_group in default_rows.groupby(
                            ['rbd_start', 'rbd_end']
                        ):
                            rbd_range = self.get_rbd_range(rbd_start, rbd_end)
                            if not rbd_range:
                                continue

                            # Only process RBDs not already covered by flight rows
                            new_rbds = [r for r in rbd_range if r not in flt_rbd_set]
                            if not new_rbds:
                                continue

                            for rbd in new_rbds:
                                for _, upsell_row in range_group.iterrows():
                                    result = self._process_single_upsell_row(
                                        upsell_row, sector, rbd, rule_nbr,
                                        origin, destination, currency, existing_sort_prices
                                    )
                                    if result is not None:
                                        primary_rows_for_rule.append(result)

                else:
                    # ── No flight rows — original path ────────────────────────────
                    for (rbd_start, rbd_end), range_group in default_rows.groupby(
                        ['rbd_start', 'rbd_end']
                    ):
                        rbd_range = self.get_rbd_range(rbd_start, rbd_end)
                        if not rbd_range:
                            self.logger.warning(
                                f"Invalid RBD range for {sector}-{rule_nbr}: {rbd_start} to {rbd_end}"
                            )
                            continue

                        for rbd in rbd_range:
                            for _, upsell_row in range_group.iterrows():
                                result = self._process_single_upsell_row(
                                    upsell_row, sector, rbd, rule_nbr,
                                    origin, destination, currency, existing_sort_prices
                                )
                                if result is not None:
                                    primary_rows_for_rule.append(result)

                all_output_rows.extend(primary_rows_for_rule)

                # ── Track for 2001 companion generation ───────────────────────────
                if self.route == 'D' and rule_nbr in ['1002', '1006', '1007']:
                    for row in primary_rows_for_rule:
                        key = (sector, row['RBD'], row['OW_RT'], row['Footnote'], row.get('flt_nbr', ''))
                        primary_rules_processed.setdefault(key, {})[rule_nbr] = row['Fare_Amount']

                # ── Generate CwT companion rows for each primary row ──────────────
                if self.route == 'D' and rule_nbr in self.CWT_PRIMARY_TO_COMPANION:
                    cwt_rule = self.CWT_PRIMARY_TO_COMPANION[rule_nbr]
                    for primary_row in primary_rows_for_rule:
                        basis = primary_row.get('Fare_Basis_Code', '')
                        decoded_ndo = basis[7] if len(basis) == 8 else 'X'

                        cwt_config = self._find_cwt_config_row(
                            cwt_rule, sector,
                            primary_row.get('Footnote', ''),
                            decoded_ndo,
                            primary_row.get('flt_nbr', '')
                        )
                        cwt_row = self._generate_cwt_row_for_primary(
                            primary_row, cwt_rule, cwt_config, existing_sort_prices
                        )
                        if cwt_row:
                            all_output_rows.append(cwt_row)

            # ── Domestic 2001 companion (from 1007) ───────────────────────────────
            if self.route == 'D':
                companion_rows = self._generate_companion_rules_domestic(
                    primary_rules_processed, existing_sort_prices
                )
                all_output_rows.extend(companion_rows)

            # ── Saver fares ───────────────────────────────────────────────────────
            if self.isSaver:
                saver_output_rows = self.generate_saver_output()
                all_output_rows.extend(saver_output_rows)

            return pd.DataFrame(all_output_rows)

        except Exception as e:
            self.logger.error(f"Error calculating product upsells: {str(e)}")
            self.logger.error(traceback.format_exc())
            raise

    # ─────────────────────────────────────────────────────────────────────────
    # Domestic 2001 companion (from 1007)
    # ─────────────────────────────────────────────────────────────────────────

    def _generate_companion_rules_domestic(self, primary_rules_processed, existing_sort_prices):
        """Generate 2001 companion rules from 1007 primary rules (domestic only)."""
        try:
            companion_rows = []
            missing_primary_rules = []

            companion_configs = self.product_upsell_data[
                self.product_upsell_data['rule_nbr'] == self.B2B_SAVER
            ].copy()

            if companion_configs.empty and self.product_upsell_master_data is not None:
                companion_configs = self.product_upsell_master_data[
                    self.product_upsell_master_data['rule_nbr'] == self.B2B_SAVER
                ].copy()

            if companion_configs.empty:
                self.logger.info("No companion rule configurations found (2001)!")
                return companion_rows

            for _, config_row in companion_configs.iterrows():
                sector = config_row['sector']

                companion_footnote = config_row.get('footnote', '')
                if pd.isna(companion_footnote):
                    companion_footnote = ''

                base_footnote_for_search = config_row.get('base_footnote', '')
                if pd.isna(base_footnote_for_search):
                    base_footnote_for_search = ''

                base_ndo_for_search = config_row.get('base_ndo', 'X')
                if pd.isna(base_ndo_for_search) or base_ndo_for_search == '':
                    base_ndo_for_search = 'X'

                rbd_start = config_row['rbd_start']
                rbd_end = config_row['rbd_end']
                rbd_range = self.get_rbd_range(rbd_start, rbd_end)

                b2b_rbds = self.get_rbd_range(self.B2B_RBD_LIST[0], self.B2B_RBD_LIST[-1])
                if len(b2b_rbds) == self.TOTAL_B2B_FARES + self.TOTAL_B2C_FARES:
                    rbd_range = b2b_rbds

                if not rbd_range:
                    continue

                companion_ow_rt = config_row.get('ow_rt', 'O') or 'O'
                if pd.isna(companion_ow_rt):
                    companion_ow_rt = 'O'

                companion_ndo = config_row.get('ndo', 'X') or 'X'
                if pd.isna(companion_ndo):
                    companion_ndo = 'X'

                companion_pos = config_row.get('pos', 'B') or 'B'
                if pd.isna(companion_pos):
                    companion_pos = 'B'

                for rbd in rbd_range:
                    primary_base_fare = None

                    # Search primary_rules_processed for 1007 entry
                    exact_key = (sector, rbd, companion_ow_rt, base_footnote_for_search, '')
                    if exact_key in primary_rules_processed and '1007' in primary_rules_processed[exact_key]:
                        primary_base_fare = primary_rules_processed[exact_key]['1007']
                    else:
                        for key, rule_fares in primary_rules_processed.items():
                            key_sector, key_rbd, key_ow_rt, key_footnote, key_flt = key
                            footnote_match = (
                                (base_footnote_for_search == '' and key_footnote == '') or
                                (base_footnote_for_search == key_footnote)
                            )
                            if key_sector == sector and key_rbd == rbd and footnote_match and '1007' in rule_fares:
                                primary_base_fare = rule_fares['1007']
                                break

                    if primary_base_fare is None:
                        missing_primary_rules.append({
                            'companion_rule': self.B2B_SAVER, 'primary_rule': '1007',
                            'sector': sector, 'rbd': rbd,
                            'companion_footnote': companion_footnote,
                            'base_footnote': base_footnote_for_search,
                            'ow_rt': companion_ow_rt, 'ndo': companion_ndo, 'pos': companion_pos
                        })
                        self.logger.error(f"MISSING PRIMARY RULE: 2001 requires 1007 for {sector}-{rbd}")
                        continue

                    origin = sector[:3]
                    destination = sector[3:]
                    currency = self.sector_currency_map.get(sector, 'INR')

                    primary_allin = self.calculate_all_in_fare(primary_base_fare, origin, destination, ftype="base")

                    base_all_in = config_row.get('base_all_in', 'All-in') or 'All-in'
                    if pd.isna(base_all_in):
                        base_all_in = 'All-in'

                    fare_for_upsell = primary_base_fare if base_all_in == 'Base' else primary_allin

                    upsell_type = config_row['type']
                    upsell_value = float(config_row['value']) if pd.notna(config_row['value']) else 0
                    min_value = float(config_row['min_value']) if (pd.notna(config_row['min_value']) and config_row['min_value'] != '') else None
                    max_value = float(config_row['max_value']) if (pd.notna(config_row['max_value']) and config_row['max_value'] != '') else None

                    upsold = self.apply_upsell(fare_for_upsell, upsell_type, upsell_value, min_value, max_value)

                    if base_all_in == 'Base':
                        final_fare = self._round_fare(upsold, currency)
                    else:
                        final_fare = self._round_fare(
                            self.calculate_all_in_fare(upsold, origin, destination, ftype="allin"),
                            currency
                        )

                    sort_price = config_row.get('sort_price')
                    if pd.notna(sort_price) and sort_price != '':
                        sort_price = float(sort_price)
                    else:
                        existing_price = existing_sort_prices.get(f"{sector}_{rbd}_2001")
                        sort_price = existing_price if (sector in self.existing_qp_sectors and existing_price is not None) else final_fare

                    farebasis = self.generate_farebasis(
                        rbd, self.B2B_SAVER, 'C', companion_ndo, sector,
                        pos=companion_pos, route_type='D',
                        footnote=companion_footnote, currency=currency
                    )

                    actual_origin, actual_destination, fare_direction = self.pa_helper._determine_fare_direction(sector)

                    companion_rows.append(self._make_output_row(
                        sector, rbd, self.B2B_SAVER, final_fare, sort_price,
                        companion_ow_rt, companion_footnote, companion_ndo,
                        companion_pos, currency, farebasis,
                        actual_origin, actual_destination, fare_direction
                    ))

            if missing_primary_rules:
                self._handle_missing_primary_rules_failure(missing_primary_rules)

            return companion_rows

        except Exception as e:
            self.logger.error(f"Error generating companion rules: {str(e)}")
            self.logger.error(traceback.format_exc())
            return []

    def _generate_companion_rules_international(self, primary_rules_processed, existing_sort_prices, intl_base_ladder_map):
        """Generate companion rules from primary rules for INTERNATIONAL routes."""
        try:
            companion_rows = []
            missing_primary_rules = []

            sector_connection_map = {}
            if self.saver_data is not None and not self.saver_data.empty:
                from collections import Counter
                for sector in self.saver_data['sector'].unique():
                    vals = self.saver_data[self.saver_data['sector'] == sector]['connections'].fillna('').tolist()
                    most_common = Counter(vals).most_common(1)[0][0]
                    sector_connection_map[sector] = most_common
                    if len(set(vals)) > 1:
                        self.logger.warning(f"Sector {sector} has inconsistent connection values: {set(vals)}. Using: '{most_common}'")

            companion_configs = self.product_upsell_data[
                self.product_upsell_data['rule_nbr'].isin(list(intl_base_ladder_map.keys()))
            ].copy()

            if companion_configs.empty and self.product_upsell_master_data is not None:
                companion_configs = self.product_upsell_master_data[
                    self.product_upsell_master_data['rule_nbr'].isin(list(intl_base_ladder_map.keys()))
                ].copy()

            if companion_configs.empty:
                self.logger.info("No international companion rule configurations found")
                return companion_rows

            for _, config_row in companion_configs.iterrows():
                companion_rule = config_row['rule_nbr']
                sector = config_row['sector']

                is_connection = sector_connection_map.get(sector, '') == 'Y'
                companion_is_connection = '201' in companion_rule
                if companion_is_connection != is_connection:
                    continue

                if is_connection:
                    primary_rule = 'R201' if companion_rule in ['V201', 'C201', 'Z201', 'M201'] else None
                else:
                    primary_rule = 'R101' if companion_rule in ['V101', 'C101', 'Z101', 'M101'] else None

                if not primary_rule:
                    continue

                companion_footnote = config_row.get('footnote', '') or ''
                if pd.isna(companion_footnote):
                    companion_footnote = ''

                base_footnote_for_search = config_row.get('base_footnote', '') or ''
                if pd.isna(base_footnote_for_search):
                    base_footnote_for_search = ''

                base_ndo_for_search = config_row.get('base_ndo', 'X') or 'X'
                if pd.isna(base_ndo_for_search):
                    base_ndo_for_search = 'X'

                rbd_start = config_row['rbd_start']
                rbd_end = config_row['rbd_end']
                rbd_range = self.get_rbd_range(rbd_start, rbd_end)

                if companion_rule in ['M101', 'M201']:
                    if self.saver_data is not None and not self.saver_data.empty:
                        z_rbds = self.saver_data[
                            (self.saver_data['sector'] == sector) & self.saver_data['rbd'].notna()
                        ]['rbd'].unique().tolist()
                        rbd_range = sorted(set(rbd_range + z_rbds))

                if not rbd_range:
                    continue

                companion_ow_rt = config_row.get('ow_rt', 'O') or 'O'
                companion_ndo = config_row.get('ndo', 'X') or 'X'
                companion_pos = config_row.get('pos', 'B') or 'B'
                currency = self.sector_currency_map.get(sector, 'INR')

                for rbd in rbd_range:
                    primary_base_fare = None

                    for key, rule_fares in primary_rules_processed.items():
                        key_sector, key_rbd, key_ow_rt, key_footnote, key_flt = key
                        footnote_match = (
                            (base_footnote_for_search == '' and key_footnote == '') or
                            base_footnote_for_search == key_footnote
                        )
                        if key_sector == sector and key_rbd == rbd and footnote_match and primary_rule in rule_fares:
                            primary_base_fare = rule_fares[primary_rule]
                            break

                    if primary_base_fare is None:
                        missing_primary_rules.append({
                            'companion_rule': companion_rule, 'primary_rule': primary_rule,
                            'sector': sector, 'rbd': rbd,
                            'companion_footnote': companion_footnote,
                            'base_footnote': base_footnote_for_search,
                            'ow_rt': companion_ow_rt, 'ndo': companion_ndo,
                            'pos': companion_pos, 'is_connection': is_connection
                        })
                        continue

                    origin = sector[:3]
                    destination = sector[3:]
                    primary_allin = self.calculate_all_in_fare(primary_base_fare, origin, destination, ftype="base")

                    base_all_in = config_row.get('base_all_in', 'All-in') or 'All-in'
                    fare_for_upsell = primary_base_fare if base_all_in == 'Base' else primary_allin

                    upsell_type = config_row['type']
                    upsell_value = float(config_row['value']) if pd.notna(config_row['value']) else 0
                    min_value = float(config_row['min_value']) if (pd.notna(config_row['min_value']) and config_row['min_value'] != '') else None
                    max_value = float(config_row['max_value']) if (pd.notna(config_row['max_value']) and config_row['max_value'] != '') else None

                    upsold = self.apply_upsell(fare_for_upsell, upsell_type, upsell_value, min_value, max_value)

                    if base_all_in == 'Base':
                        final_fare = self._round_fare(upsold, currency)
                    else:
                        final_fare = self._round_fare(
                            self.calculate_all_in_fare(upsold, origin, destination, ftype="allin"),
                            currency
                        )

                    sort_price = config_row.get('sort_price')
                    if pd.notna(sort_price) and sort_price != '':
                        sort_price = float(sort_price)
                    else:
                        existing_price = existing_sort_prices.get(f"{sector}_{rbd}_{companion_rule}")
                        sort_price = existing_price if (sector in self.existing_qp_sectors and existing_price is not None) else final_fare

                    farebasis = self.generate_farebasis(
                        rbd, companion_rule,
                        'C' if companion_rule[1] == '2' else companion_ow_rt,
                        companion_ndo, sector, pos=companion_pos,
                        route_type='I', footnote=companion_footnote, currency=currency
                    )

                    actual_origin, actual_destination, fare_direction = self.pa_helper._determine_fare_direction(sector)

                    companion_rows.append(self._make_output_row(
                        sector, rbd, companion_rule, final_fare, sort_price,
                        companion_ow_rt, companion_footnote, companion_ndo,
                        companion_pos, currency, farebasis,
                        actual_origin, actual_destination, fare_direction
                    ))

            if missing_primary_rules:
                self._handle_missing_primary_rules_failure(missing_primary_rules)

            return companion_rows

        except Exception as e:
            self.logger.error(f"Error generating international companion rules: {str(e)}")
            self.logger.error(traceback.format_exc())
            return []

    def _handle_missing_primary_rules_failure(self, missing_primary_rules):
        """Handle failure when companion rules cannot be generated due to missing primary rules."""
        try:
            missing_by_companion = {}
            for m in missing_primary_rules:
                missing_by_companion.setdefault(m['companion_rule'], []).append(m)

            error_lines = [
                "COMPANION RULE GENERATION FAILED", "=" * 50,
                "The following companion rules are configured but their corresponding primary rules are missing:", ""
            ]

            for companion_rule, missing_list in missing_by_companion.items():
                primary_rule = missing_list[0]['primary_rule']
                error_lines.append(f"Companion Rule {companion_rule} (requires Primary Rule {primary_rule}):")
                by_sector = {}
                for m in missing_list:
                    by_sector.setdefault(m['sector'], []).append(m['rbd'])
                for sector, rbds in by_sector.items():
                    sample = ', '.join(rbds[:3]) + (f" ... (+{len(rbds)-3} more)" if len(rbds) > 3 else '')
                    error_lines.append(f"  Sector {sector}: {len(rbds)} RBDs ({sample})")
                error_lines.append("")

            error_lines.append(f"Total missing primary rule instances: {len(missing_primary_rules)}")
            self.logger.error("\n".join(error_lines))

            raise ValueError(
                f"Companion rule generation failed: {len(missing_primary_rules)} missing primary rule instances. "
                f"See logs for detailed breakdown."
            )
        except Exception as e:
            raise ValueError(
                f"Companion rule generation failed due to missing primary rules. "
                f"Count: {len(missing_primary_rules)}. Additional error: {str(e)}"
            )

    # ─────────────────────────────────────────────────────────────────────────
    # Saver output
    # ─────────────────────────────────────────────────────────────────────────

    def generate_saver_output(self):
        """Generate output rows for saver fares (rule 1001/2001 or international equivalents)."""
        try:
            saver_rows = []
            b2c_rbds = self.get_rbd_range(self.B2C_RBD_LIST[0], self.B2C_RBD_LIST[-1])
            b2b_rbds = self.get_rbd_range(self.B2B_RBD_LIST[0], self.B2B_RBD_LIST[-1])

            # Fetch sort prices once for CwT mirroring
            existing_sort_prices = self.pa_helper._get_existing_sort_prices()

            for _, saver_row in self.saver_data.iterrows():
                sector = saver_row['sector']
                rbd = saver_row['rbd']
                allin_fare = saver_row['fare']

                pos_value = saver_row.get('pos', 'B')
                if pd.isna(pos_value) or pos_value == '':
                    pos_value = 'B'

                saver_footnote = saver_row.get('footnote', '')
                if pd.isna(saver_footnote):
                    saver_footnote = ''

                saver_ndo = saver_row.get('ndo', 'X')
                if pd.isna(saver_ndo) or saver_ndo == '':
                    saver_ndo = 'X'

                saver_is_connections = saver_row.get('connections', '')
                if pd.isna(saver_is_connections):
                    saver_is_connections = ''

                currency = saver_row.get('currency', 'INR')
                if pd.isna(currency) or currency == '':
                    currency = 'INR'

                origin = sector[:3]
                destination = sector[3:]
                base_fare = self._round_fare(
                    self.calculate_all_in_fare(allin_fare, origin, destination, ftype="allin"),
                    currency
                )

                rules_to_generate = []
                if rbd in b2c_rbds:
                    if self.route == 'D':
                        rules_to_generate = [self.B2C_SAVER, self.B2B_SAVER]
                    elif self.route == 'I':
                        rules_to_generate = ['R201', 'M201'] if saver_is_connections == 'Y' else ['R101', 'M101']
                elif rbd in b2b_rbds:
                    if self.route == 'D':
                        rules_to_generate = [self.B2B_SAVER]
                    elif self.route == 'I':
                        rules_to_generate = ['M201'] if saver_is_connections == 'Y' else ['M101']

                actual_origin, actual_destination, fare_direction = self.pa_helper._determine_fare_direction(sector)

                for rule_nbr in rules_to_generate:
                    farebasis = self.generate_farebasis(
                        rbd, rule_nbr,
                        'C' if saver_is_connections == 'Y' else 'O',
                        saver_ndo, sector, pos=pos_value,
                        route_type=self.route, footnote=saver_footnote, currency=currency
                    )

                    saver_output_row = self._make_output_row(
                        sector, rbd, rule_nbr, base_fare, base_fare,
                        'O', saver_footnote, saver_ndo, pos_value,
                        currency, farebasis,
                        actual_origin, actual_destination, fare_direction,
                        flt_nbr=''
                    )
                    saver_rows.append(saver_output_row)

                    # CwT mirror for saver 1001 rows (domestic only)
                    if self.route == 'D' and rule_nbr == self.B2C_SAVER:
                        cwt_rule = self.CWT_PRIMARY_TO_COMPANION[self.B2C_SAVER]
                        cwt_config = self._find_cwt_config_row(
                            cwt_rule, sector, saver_footnote, saver_ndo, ''
                        )
                        cwt_row = self._generate_cwt_row_for_primary(
                            saver_output_row, cwt_rule, cwt_config, existing_sort_prices
                        )
                        if cwt_row:
                            saver_rows.append(cwt_row)

            return saver_rows

        except Exception as e:
            self.logger.error(f"Error generating saver output: {str(e)}")
            return []

    def generate_fare_ladder(self):
        """Generate complete fare ladder output."""
        try:
            return self.calculate_product_upsells()
        except Exception as e:
            self.logger.error(f"Error generating fare ladder: {str(e)}")
            self.logger.error(traceback.format_exc())
            raise

    def run(self):
        """Main method to run complete fare filing process."""
        try:
            self.start_time = datetime.now(pytz.timezone('Asia/Kolkata'))
            self.currentVersion = self.pa_helper.get_current_version()

            self.run_id = str(uuid.uuid4())
            route = self.route
            run_type = "Saver" if self.isSaver else "Product Upsell"

            self.pa_helper.save_run_audit(
                self.currentVersion, route, self.analystName,
                self.start_time, self.run_id, None,
                run_type=run_type, sectors_list=None,
                status='IN_PROGRESS', is_initial=True
            )

            self.collect_all_data()
            fare_ladder = self.generate_fare_ladder()
            sectors_list = fare_ladder['Sector'].unique().tolist() if not fare_ladder.empty else []

            table_name = self.pa_helper.save_ladder_to_database(
                route, self.analystName, fare_ladder, self.run_id
            )

            self.pa_helper.save_run_audit(
                self.currentVersion, route, self.analystName,
                self.start_time, self.run_id, fare_ladder,
                run_type=run_type, sectors_list=sectors_list,
                status='SUCCESS'
            )

            type_desc = "Saver Revision" if self.isSaver else "Product Upsell"
            self.logger.info(f"\n{'='*60}")
            self.logger.info(f"Fare Filing Complete - {type_desc}")
            self.logger.info(f"{'='*60}")
            self.logger.info(f"Total fares generated: {len(fare_ladder)}")
            self.logger.info(f"Unique sectors: {len(sectors_list)} - {sectors_list}")
            self.logger.info(f"Unique rule numbers: {fare_ladder['Rule_Nbr'].nunique()}")
            self.logger.info(f"Output saved to: {table_name}")
            self.logger.info(f"Run ID: {self.run_id}")

            return {
                'fare_ladder': fare_ladder,
                'table_name': table_name,
                'run_id': self.run_id,
                'type': type_desc,
                'run_type': run_type,
                'sectors': sectors_list,
                'summary': {
                    'total_fares': len(fare_ladder),
                    'unique_sectors': len(sectors_list),
                    'sectors_list': sectors_list,
                    'unique_rules': fare_ladder['Rule_Nbr'].nunique(),
                    'rule_breakdown': fare_ladder['Rule_Nbr'].value_counts().to_dict()
                }
            }

        except Exception as e:
            error_message = str(e)
            self.logger.error(f"Error in run method: {error_message}")
            self.logger.error(traceback.format_exc())

            try:
                self.pa_helper.save_run_audit(
                    self.currentVersion, route, self.analystName,
                    self.start_time, self.run_id, None,
                    run_type=run_type if 'run_type' in locals() else "Unknown",
                    sectors_list=None, status='FAILED',
                    is_initial=False, failure_reason=error_message
                )
            except Exception as audit_error:
                self.logger.error(f"Failed to update audit record: {str(audit_error)}")

            raise