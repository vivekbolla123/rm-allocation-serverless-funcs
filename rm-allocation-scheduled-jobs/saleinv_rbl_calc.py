import json
import pandas as pd
import traceback
import time
import warnings
from saleinv_baseload_calc import KEY_COLS
warnings.filterwarnings('ignore')


class SaleInvRBLCalc:
    """
    Recommended Base Load (RBL) Calculator.
    RBL = Metro Classification Value + Corp Contribution Mapped Value
          + Fare Premium Mapped Value + Flown PLF Mapped Value

    Reads baseload script outputs from MongoDB and parameter maps from
    saleinv_parameter_values collection.
    """

    def __init__(self, logger, params, overrides, context):
        self.context = context
        self.logger = logger
        self.mongoconn = params['mongoconn']
        self.db = self.mongoconn['qp_dw_rmalloc']

        # Single month passed via overrides (target month for RBL output)
        self.month_key = overrides.get('month')
        if not self.month_key:
            raise ValueError("overrides must contain 'month' (e.g. '2026-03')")

        self.run_id = overrides.get('run_id')

        # Load all config once
        self._load_all_config()

        # Resolve source month: which month's baseload data to use
        self.source_month = self._resolve_source_month()

    def _retry_db_operation(self, operation, description, max_retries=3, delay=5):
        """Retry a DB operation up to max_retries times with a delay between attempts."""
        for attempt in range(1, max_retries + 1):
            try:
                return operation()
            except Exception as e:
                if attempt == max_retries:
                    self.logger.error(f"{description} failed after {max_retries} attempts: {e}")
                    raise
                self.logger.warning(f"{description} failed (attempt {attempt}/{max_retries}), retrying in {delay}s...")
                time.sleep(delay)

    def _load_all_config(self):
        """Load all parameter maps and config from MongoDB in one go."""
        def _do_load():
            param_docs = {
                doc['parameterKey']: doc['parameterValue']
                for doc in self.db.saleinv_parameter_values.find(
                    {'parameterKey': {'$in': [
                        'RBL_CORP_CONTRI_MAP', 'RBL_FARE_PREMIUM_MAP',
                        'RBL_FLOWN_PLF_MAP', 'RBL_METRO_MAP', 'METRO_STATIONS'
                    ]}},
                    {'_id': 0, 'parameterKey': 1, 'parameterValue': 1}
                )
            }

            self.corp_contri_map = json.loads(param_docs['RBL_CORP_CONTRI_MAP'])
            self.fare_premium_map = json.loads(param_docs['RBL_FARE_PREMIUM_MAP'])
            self.flown_plf_map = json.loads(param_docs['RBL_FLOWN_PLF_MAP'])

            metro_entries = json.loads(param_docs['RBL_METRO_MAP'])
            self.metro_map = {e['metro_class']: e['value'] for e in metro_entries}

            self.metro_stations = set(json.loads(param_docs['METRO_STATIONS']))

            sector_docs = list(self.db.saleinv_config_sector_map.find({}, {'_id': 0}))
            self.sector_overrides = {
                d['sector']: {
                    'is_metro': d.get('is_metro'),
                    'mapped_sector': d.get('mapped_sector')
                }
                for d in sector_docs
            }

            month_docs = list(self.db.saleinv_config_month_map.find({}, {'_id': 0}))
            self.month_map = {d['target_month']: d['mapped_month'] for d in month_docs}

            self.logger.info("All RBL config loaded successfully")

        self._retry_db_operation(_do_load, "Load RBL config")

    def _resolve_source_month(self):
        """
        Determine which month's baseload data to use.
        Default: same month, previous year (e.g. 2026-03 -> 2025-03).
        Override: saleinv_config_month_map can map target_month to a different source month.
        """
        if self.month_key in self.month_map:
            source = self.month_map[self.month_key]
            self.logger.info(f"Month mapping override: {self.month_key} -> {source}")
            return source

        # Default: same month last year
        from datetime import datetime
        target_date = datetime.strptime(self.month_key, '%Y-%m')
        source_date = target_date.replace(year=target_date.year - 1)
        source = source_date.strftime('%Y-%m')
        self.logger.info(f"Using default source month: {self.month_key} -> {source}")
        return source

    def _get_mapped_value(self, actual_value, band_map):
        """Look up the mapped value for an actual value using band ranges."""
        if pd.isna(actual_value):
            return 0
        for band in band_map:
            if band['min'] <= actual_value < band['max']:
                return band['value']
        return 0

    def _is_metro_sector(self, sector):
        """
        Determine if a sector is metro.
        Priority: saleinv_config_sector_map override > METRO_STATIONS list logic.
        """
        if sector in self.sector_overrides:
            override = self.sector_overrides[sector]
            if override['is_metro'] is not None:
                return override['is_metro'] == 1

        origin = sector[:3]
        destination = sector[3:]
        return origin in self.metro_stations and destination in self.metro_stations

    def _get_metro_value(self, sector):
        """Get the metro classification value for a sector."""
        metro_class = 'metro' if self._is_metro_sector(sector) else 'nonmetro'
        return self.metro_map.get(metro_class, 0)

    def _fetch_baseload_data(self):
        """Fetch all metrics from saleinv_flown_data for the resolved source month."""
        def _do_fetch():
            docs = list(self.db['saleinv_flown_data'].find(
                {'Month': self.source_month},
                {'_id': 0, 'Month': 1, 'Sector': 1, 'TimeCluster': 1, 'DayOfWeek': 1,
                 'BaseLoad': 1, 'Imputed_BaseLoad': 1, 'CorporateContribution': 1, 'FarePremium': 1, 'FlownPLF': 1}
            ))
            if not docs:
                return pd.DataFrame()
            df = pd.DataFrame(docs)
            df['Month'] = self.month_key
            return df

        return self._retry_db_operation(_do_fetch, f"Fetch saleinv_flown_data for {self.source_month}")

    def _apply_sector_mapping(self, df):
        """
        For sectors with a mapped_sector in saleinv_config_sector_map,
        add the sector to output using the mapped sector's data.
        """
        if df.empty:
            return df

        mapped_sectors = {
            s: info['mapped_sector']
            for s, info in self.sector_overrides.items()
            if info.get('mapped_sector')
        }

        if not mapped_sectors:
            return df

        new_rows = []
        for target_sector, source_sector in mapped_sectors.items():
            source_data = df[df['Sector'] == source_sector].copy()
            if not source_data.empty:
                source_data['Sector'] = target_sector
                new_rows.append(source_data)

        if new_rows:
            mapped_df = pd.concat(new_rows, ignore_index=True)
            df = df[~df['Sector'].isin(mapped_sectors.keys())]
            df = pd.concat([df, mapped_df], ignore_index=True)

        return df

    def calculate_rbl(self):
        """
        Calculate Recommended Base Load for the configured month.
        RBL = Metro Value + Mapped Corp Contribution + Mapped Fare Premium + Mapped Flown PLF
        RBL_Capped = max(RBL, 30) if FlownPLF < 90, else RBL
        """
        try:
            self.logger.info(f"Calculating RBL for {self.month_key}...")

            df = self._fetch_baseload_data()

            if df.empty:
                self.logger.warning(f"No baseload data found for source month {self.source_month}")
                return pd.DataFrame()

            # Apply sector mapping on the combined data
            df = self._apply_sector_mapping(df)

            # Map actual values to parameter band values
            df['CorpContriMapped'] = df['CorporateContribution'].apply(
                lambda x: self._get_mapped_value(x, self.corp_contri_map)
            )
            df['FarePremiumMapped'] = df['FarePremium'].apply(
                lambda x: self._get_mapped_value(x, self.fare_premium_map)
            )
            df['FlownPLFMapped'] = df['FlownPLF'].apply(
                lambda x: self._get_mapped_value(x, self.flown_plf_map)
            )

            df['MetroValue'] = df['Sector'].apply(self._get_metro_value)

            df['RBL'] = (
                df['MetroValue'] +
                df['CorpContriMapped'] +
                df['FarePremiumMapped'] +
                df['FlownPLFMapped']
            )

            # RBL_Capped: if FlownPLF < 90, RBL must be at least 30
            df['RBL_Capped'] = df.apply(
                lambda r: max(r['RBL'], 30) if pd.notna(r['FlownPLF']) and r['FlownPLF'] < 90 else r['RBL'],
                axis=1
            )

            df['Baseload_Manual'] = 0
            df['RBL_Manual'] = 0

            self.logger.info(f"RBL calculated for {len(df)} combinations")
            return df[KEY_COLS + ['BaseLoad', 'Imputed_BaseLoad', 'RBL', 'RBL_Capped', 'Baseload_Manual', 'RBL_Manual']]

        except Exception as e:
            self.logger.error(f"Error calculating RBL: {traceback.format_exc()}")
            raise

    def _insert_to_mongo(self, df, collection_name):
        """Delete existing month data and insert new data with retry."""
        if df.empty:
            self.logger.warning(f"Skipping insert for {collection_name} — empty DataFrame")
            return

        def _do_insert():
            collection = self.db[collection_name]
            records = df.to_dict('records')
            collection.delete_many({'Month': self.month_key})
            collection.insert_many(records)
            self.logger.info(f"Inserted {len(records)} records into {collection_name} for {self.month_key}")

        self._retry_db_operation(_do_insert, f"Insert into {collection_name} for {self.month_key}")


    def run(self):
        """Main execution: calculate and insert RBL for the configured month."""
        try:
            rbl_df = self.calculate_rbl()
            self._insert_to_mongo(rbl_df, 'saleinv_rbl')
            self._update_run_summary(len(rbl_df), 'SUCCESS')

        except Exception as e:
            self._update_run_summary(0, 'FAILED')
            self.logger.error(f"Error in RBL run: {traceback.format_exc()}")
            raise

    def _update_run_summary(self, total_records, status):
        """Update the pre-inserted run summary doc with results."""
        if not self.run_id:
            self.logger.warning("No run_id provided, skipping run summary update")
            return

        from datetime import datetime

        def _do_update():
            self.db['saleinv_run_summary'].update_one(
                {'run_id': self.run_id},
                {'$set': {
                    'mapped_month': self.source_month,
                    'total_records': total_records,
                    'status': status,
                    'end_time': datetime.utcnow()
                }}
            )
            # self.logger.info(f"Updated run summary for run_id={self.run_id}, status={status}, records={total_records}")

        self._retry_db_operation(_do_update, f"Update run summary for {self.run_id}")
