import logging
from sys import exit
import json
from urllib.parse import quote_plus

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from redis import Redis
from datetime import datetime, timezone, timedelta
from boto3 import client
from pymongo import MongoClient
# from common_utils import instrument_engine

class LastSoldFare:

    def __init__(self):
        self.NavitaireConnection = None
        self.rmrdconn = None  # This will now be MongoDB client
        self.rm_db = None  # MongoDB database
        self.DWConnection = None  # Data warehouse (MySQL) engine
        self.logger = None
        self.cache_client = None
        # Saver fare override config (markers + Mongo query chunk size), loaded from
        # rm_parameter_values so it stays configurable without a code change.
        # fare_basis_markers is a list so more fare bases can be added in future.
        # Hardcoded defaults are used when the config document is absent/malformed.
        self.saver_fare_markers = ['C']
        self.fares_query_chunk_size = 500
        self.generateConfigs()
        self.get_filter_configs(['allfares', 'b2bfares', 'b2cfares'])
        self.get_last_sold_fare()

    def generateConfigs(self):
        ISPROD = True
        if ISPROD:
            CACHE_ENDPOINT = "ip-10-16-6-51.ap-south-1.compute.internal"
            CACHE_PORT = 6379
            CACHE_USERNAME = "default"
            CACHE_PASSWORD = self.get_credentials('/rm/prod/rm-allocation/RM_CACHE_PASSWORD')
            self.logger = logging.getLogger()
            self.logger.setLevel(logging.INFO)
            self.NAV_DB_NAME = "REZ0KWB01"
            
            # MongoDB connection
            RM_RD_PASSWORD = self.get_credentials('/rm/prod/rm-allocation/RM_MONGO_PASSWORD')
            RM_RD_USER = self.get_credentials('/copilot/rm/prod/secrets/QP_RM_MONGO_USERNAME')
            encoded_password = quote_plus(RM_RD_PASSWORD)
            MONGO_CONN_STRING = f"mongodb://{RM_RD_USER}:{encoded_password}@10.16.6.179:27017/"

            # Data warehouse (MySQL) connection - RM_DW_DATA schema, holds currency_conversion
            DW_DB_HOST = "prod-qp-dw.cz6mmsw6mnag.ap-south-1.rds.amazonaws.com"
            DW_DB_USER = self.get_credentials('/glue_job/prod/DW_DB_APPUSER_USERNAME')
            DW_DB_PASSWORD = self.get_credentials('/glue_job/prod/DW_DB_APPUSER_PASSWORD')
            DW_CONN_STRING = f"mysql+pymysql://{DW_DB_USER}:{quote_plus(DW_DB_PASSWORD)}@{DW_DB_HOST}/RM_DW_DATA"
            # Or if using MongoDB Atlas or other format:
            # MONGO_CONN_STRING = f"mongodb+srv://rmappuser:{RM_RD_PASSWORD}@your-cluster.mongodb.net/"

            try:
                self.cache_client = Redis(host=CACHE_ENDPOINT, port=CACHE_PORT, username=CACHE_USERNAME, password=CACHE_PASSWORD)
                if self.cache_client.ping():
                    print("Connected to Redis")

                self.NavitaireConnection = create_engine(
                    f"mssql+pymssql://0KODSUSER2:"
                    f"{quote_plus(self.get_credentials('/rm/prod/rm-allocation/NAV_DB_PASSWORD'))}@"
                    f"10.154.12.13:52900"
                )
                # instrument_engine(self.NavitaireConnection)

                # MongoDB connection
                self.rmrdconn = MongoClient(MONGO_CONN_STRING)
                self.rm_db = self.rmrdconn['qp_dw_rmalloc']  # Database name
                print("Connected to MongoDB")

                # Data warehouse (MySQL) connection - lazy pooled engine
                self.DWConnection = create_engine(DW_CONN_STRING, pool_pre_ping=True)
                print("Connected to DW")
                
            except Exception as e:
                self.logger.error(e)
                exit()
                
        else:
            CACHE_ENDPOINT = "172.24.72.8"
            CACHE_PORT = 6379
            CACHE_USERNAME = "default"
            CACHE_PASSWORD = self.get_credentials('/rm/uat/rm-allocation/RM_CACHE_PASSWORD')
            self.logger = logging.getLogger()
            self.logger.setLevel(logging.INFO)
            self.NAV_DB_NAME = "TST0KWB01"
            
            # MongoDB connection
            RM_RD_PASSWORD = self.get_credentials('/rm/uat/rm-allocation/RM_MONGO_PASSWORD')
            RM_RD_USER = self.get_credentials('/copilot/rm/uat/secrets/QP_RM_MONGO_USERNAME')
            encoded_password = quote_plus(RM_RD_PASSWORD)
            MONGO_CONN_STRING = f"mongodb://{RM_RD_USER}:{encoded_password}@172.24.72.35:27017/"

            # Data warehouse (MySQL) connection - RM_DW_DATA schema, holds currency_conversion
            DW_DB_HOST = "172.24.87.10"
            DW_DB_USER = self.get_credentials('/glue_job/uat/DW_DB_APPUSER_USERNAME')
            DW_DB_PASSWORD = self.get_credentials('/glue_job/uat/DW_DB_APPUSER_PASSWORD')
            DW_CONN_STRING = f"mysql+pymysql://{DW_DB_USER}:{quote_plus(DW_DB_PASSWORD)}@{DW_DB_HOST}/RM_DW_DATA"
            
            try:
                self.cache_client = Redis(host=CACHE_ENDPOINT, port=CACHE_PORT, username=CACHE_USERNAME, password=CACHE_PASSWORD)
                if self.cache_client.ping():
                    print("Connected to Redis")

                self.NavitaireConnection = create_engine(
                    f"mssql+pymssql://0KODSUSER2:"
                    f"{quote_plus(self.get_credentials('/rm/uat/rm-allocation/NAV_DB_PASSWORD'))}@"
                    f"10.154.12.19:52900"
                )
                # instrument_engine(self.NavitaireConnection)
                
                # MongoDB connection
                self.rmrdconn = MongoClient(MONGO_CONN_STRING)
                self.rm_db = self.rmrdconn['qp_dw_rmalloc']  # Database name
                print("Connected to MongoDB")

                # Data warehouse (MySQL) connection - lazy pooled engine
                self.DWConnection = create_engine(DW_CONN_STRING, pool_pre_ping=True)
                print("Connected to DW")
                
            except Exception as e:
                self.logger.error(e)
                exit()
                
    def get_filter_configs(self, filter_names):
        # MongoDB query instead of SQL
        collection = self.rm_db['rm_parameter_values']

        # Build filter parameter keys, plus the saver fare override config key.
        # Both are fetched in a single rm_parameter_values query.
        filter_keys = [f'fare_filter_{name}' for name in filter_names]
        saver_config_key = 'LAST_SOLD_SAVER_FARE_CONFIG'

        # Query MongoDB
        cursor = collection.find({
            'parameterKey': {'$in': filter_keys + [saver_config_key]}
        })
        
        # Convert to list of documents
        documents = list(cursor)
        
        # Build configs dictionary
        configs = {name: {} for name in filter_names}
        saver_config = {}
        for doc in documents:
            key = doc['parameterKey']
            param_value = doc.get('parameterValue')
            parsed = json.loads(param_value) if param_value else {}
            if key == saver_config_key:
                saver_config = parsed
            else:
                filter_name = key.replace('fare_filter_', '')
                configs[filter_name] = parsed

        self.filter_config = configs
        self._apply_saver_fare_config(saver_config)

    def _apply_saver_fare_config(self, config):
        """
        Apply the saver fare override settings parsed from rm_parameter_values
        (parameterKey 'LAST_SOLD_SAVER_FARE_CONFIG'), keeping the hardcoded
        defaults from __init__ (['C'] / 500) for anything missing or malformed.

        Expected parameterValue JSON, e.g.
            {"fare_basis_markers": ["C"], "fares_query_chunk_size": 500}
        fare_basis_markers is a list so additional fare bases can be added later.
        """
        try:
            markers = config.get('fare_basis_markers')
            if markers:
                # Accept a list (preferred) or a single string for convenience.
                if isinstance(markers, str):
                    markers = [markers]
                cleaned = [str(m).strip() for m in markers if str(m).strip()]
                if cleaned:
                    self.saver_fare_markers = cleaned

            chunk = config.get('fares_query_chunk_size')
            if chunk is not None:
                chunk = int(chunk)
                if chunk > 0:
                    self.fares_query_chunk_size = chunk
        except Exception as e:
            self.logger.error(
                f"Could not apply LAST_SOLD_SAVER_FARE_CONFIG: {e}; "
                f"using defaults markers={self.saver_fare_markers}, "
                f"chunk_size={self.fares_query_chunk_size}."
            )

        self.logger.info(
            f"Saver fare override config: markers={self.saver_fare_markers}, "
            f"fares_query_chunk_size={self.fares_query_chunk_size}."
        )

    def apply_filter(self, fares, filter_name):
        config = self.filter_config.get(filter_name, {})
        excluded_prefixes = config.get('excluded_class_prefixes', [])
        excluded_classes = config.get('excluded_classes', [])
        included_fare_basis = config.get('included_fare_basis', [])
        
        filtered_fares = []
        for fare in fares:
            # Check class prefix exclusions - G
            if any(fare['ClassOfService'].startswith(prefix) for prefix in excluded_prefixes):
                continue
            
            # Check class exclusions - G7
            if fare['ClassOfService'] in excluded_classes:
                continue
            
            # Check fare basis inclusion
            if included_fare_basis and fare['FareBasis'] not in included_fare_basis:
                continue
            
            filtered_fares.append(fare)
        return filtered_fares

    def get_last_sold_fare(self):
        time = None
        lastExecTime = None
        if self.cache_client.exists("LastSoldFare:executionTime"):
            lastExecTime = self.cache_client.get("LastSoldFare:executionTime").decode('utf-8')

        curr_minus_20 = datetime.now(timezone.utc) - timedelta(minutes=20)
        time = curr_minus_20.replace(second=0, microsecond=0, tzinfo=None)

        try:
            if lastExecTime:
                lastExecTime = datetime.strptime(lastExecTime, '%Y-%m-%d %H:%M:%S').replace(second=0, tzinfo=None)

                if lastExecTime < time:
                    time = lastExecTime - timedelta(minutes=20)

        except Exception as e:
            self.logger.error(f"Error parsing last execution time: {e}")
            time = curr_minus_20.replace(second=0)

        time = time.strftime("%Y-%m-%d %H:%M:%S")
        
        whereCondition = f"'{str(time)}'"
        if lastExecTime is None:
            whereCondition = "DATEADD(HOUR, -120, GETDATE())"  # 40 days

        self.cache_client.set("LastSoldFare:executionTime", datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'))
        
        df = self.lastSoldQuery(whereCondition)
        if df.shape[0] != 0:
            df['FareBasis'] = df.groupby(['BookingID', 'PassengerID'])['FareBasis'].apply(
                lambda x: x.replace('', pd.NA).ffill().bfill().replace(pd.NA, ''))

            # Convert BookingUTC to datetime
            df['BookingUTC'] = pd.to_datetime(df['BookingUTC'])

            # Sort the DataFrame by BookingUTC
            df = df.sort_values(by='BookingUTC')
            df = df.loc[df.groupby(['BookingID', 'PassengerID'])['BookingUTC'].idxmax()]

            df = df[
                df['FareBasis'].notna() & (df['FareBasis'] != '')]

            # For saver fares (default markers ['C'], i.e. SUBSTRING(FareBasis, 5, 1)
            # in the marker list), override the Navitaire-computed Fare with the
            # currency-adjusted fare sourced from the 'fares' collection (route 'L').
            df = self.override_saver_fares(df)

            self.updateValuesToRedis(df)

        print("completed")

    def fetch_route_l_fares(self, pairs_df):
        """
        Fetch only the route 'L' fares needed for the given distinct (SECTOR, RBD)
        combinations, supplied as a DataFrame with columns ['SECTOR', 'RBD'].

        The combinations are queried in bounded chunks (self.fares_query_chunk_size
        per request, configurable via rm_parameter_values) so a large batch can
        never build a single filter document that breaches MongoDB's 16MB BSON
        limit. Result size stays bounded by the distinct combinations in this run,
        not by collection size.

        Returns a DataFrame with columns ['SECTOR', 'RBD', 'currency', 'TOTAL'].
        """
        empty = pd.DataFrame(columns=['SECTOR', 'RBD', 'currency', 'TOTAL'])
        if pairs_df is None or pairs_df.empty:
            return empty

        collection = self.rm_db['fares']
        projection = {'SECTOR': 1, 'RBD': 1, 'currency': 1, 'TOTAL': 1, '_id': 0}
        frames = []

        try:
            # Iterate over chunks of the pairs DataFrame; each chunk becomes one
            # bounded $or query.
            for start in range(0, len(pairs_df), self.fares_query_chunk_size):
                chunk = pairs_df.iloc[start:start + self.fares_query_chunk_size]
                or_clauses = chunk.to_dict('records')  # [{'SECTOR':..,'RBD':..}, ...]
                docs = list(collection.find(
                    {'route': 'L', '$or': or_clauses}, projection
                ))
                if docs:
                    frames.append(pd.DataFrame(docs))
        except Exception as e:
            self.logger.error(f"Error fetching route 'L' fares: {e}")

        if not frames:
            return empty

        fares_df = pd.concat(frames, ignore_index=True)
        # Vectorized normalization of the join keys / currency.
        fares_df['SECTOR'] = fares_df['SECTOR'].astype(str).str.strip()
        fares_df['RBD'] = fares_df['RBD'].astype(str).str.strip()
        if 'currency' in fares_df.columns:
            fares_df['currency'] = fares_df['currency'].astype(str).str.strip()
        else:
            fares_df['currency'] = np.nan
        if 'TOTAL' not in fares_df.columns:
            fares_df['TOTAL'] = np.nan
        fares_df['TOTAL'] = pd.to_numeric(fares_df['TOTAL'], errors='coerce')
        # Keep one row per (SECTOR, RBD).
        fares_df = fares_df.drop_duplicates(subset=['SECTOR', 'RBD'])
        return fares_df[['SECTOR', 'RBD', 'currency', 'TOTAL']]

    def fetch_conversion_rates(self, currencies):
        """
        Fetch INR conversion rates for only the given source currencies.

        Primary source : RM_DW_DATA.currency_conversion (DW / MySQL).
        Fallback source: Navitaire dw.CurrencyConversionVersion (navods / MSSQL),
                         used only for currencies the DW could not supply (or if
                         the DW query fails entirely).

        Only the handful of currencies present in the fetched fares are queried
        from each source - nothing is loaded in full. INR is excluded by the
        caller (rate is 1).

        Returns a DataFrame with columns ['currency', 'ConversionRate'].
        """
        empty = pd.DataFrame(columns=['currency', 'ConversionRate'])
        currencies = [c for c in currencies if c and c != 'INR']
        if not currencies:
            return empty

        # 1) Primary: data warehouse.
        dw_rates = self._fetch_rates_from_dw(currencies)

        # 2) Fallback: Navitaire for any currency the DW did not resolve.
        resolved = set(dw_rates['currency']) if not dw_rates.empty else set()
        missing = [c for c in currencies if c not in resolved]
        if missing:
            self.logger.warning(
                f"Conversion rates not found in DW for {missing}; "
                f"falling back to Navitaire CurrencyConversionVersion."
            )
            nav_rates = self._fetch_rates_from_navitaire(missing)
            rates = pd.concat([dw_rates, nav_rates], ignore_index=True)
        else:
            rates = dw_rates

        if rates.empty:
            return empty
        return rates.drop_duplicates(subset=['currency'])

    def _fetch_rates_from_dw(self, currencies):
        """Targeted INR-rate lookup from the DW MySQL currency_conversion table."""
        empty = pd.DataFrame(columns=['currency', 'ConversionRate'])
        try:
            # Named bind params for a safe, targeted IN clause.
            params = {f"c{i}": ccy for i, ccy in enumerate(currencies)}
            placeholders = ", ".join(f":{name}" for name in params)
            query = text(
                "SELECT FromCurrencyCode AS currency, ConversionRate "
                "FROM currency_conversion "
                "WHERE ToCurrencyCode = 'INR' "
                f"AND FromCurrencyCode IN ({placeholders})"
            )
            df = pd.read_sql(query, self.DWConnection, params=params)
            return self._normalize_rates_df(df)
        except Exception as e:
            self.logger.error(
                f"DW currency_conversion lookup failed for {currencies}: {e}; "
                f"will fall back to Navitaire for these currencies."
            )
            return empty

    def _fetch_rates_from_navitaire(self, currencies):
        """
        Targeted INR-rate lookup from Navitaire dw.CurrencyConversionVersion.

        Uses the version effective now (VersionStartUTC <= now < VersionEndUTC)
        for ClientCode '0K', matching the join logic used in lastSoldQuery.
        """
        empty = pd.DataFrame(columns=['currency', 'ConversionRate'])
        try:
            params = {f"c{i}": ccy for i, ccy in enumerate(currencies)}
            placeholders = ", ".join(f":{name}" for name in params)
            query = text(
                f"SELECT FromCurrencyCode AS currency, ConversionRate "
                f"FROM {self.NAV_DB_NAME}.dw.CurrencyConversionVersion "
                f"WHERE ToCurrencyCode = 'INR' AND ClientCode = '0K' "
                f"AND VersionStartUTC <= GETUTCDATE() AND VersionEndUTC > GETUTCDATE() "
                f"AND FromCurrencyCode IN ({placeholders})"
            )
            df = pd.read_sql(query, self.NavitaireConnection, params=params)
            return self._normalize_rates_df(df)
        except Exception as e:
            self.logger.error(
                f"Navitaire CurrencyConversionVersion fallback failed for {currencies}: {e}"
            )
            return empty

    def _normalize_rates_df(self, df):
        """Vectorized cleanup of a rates DataFrame -> ['currency', 'ConversionRate']."""
        empty = pd.DataFrame(columns=['currency', 'ConversionRate'])
        if df is None or df.empty:
            return empty
        df = df.copy()
        df['currency'] = df['currency'].astype(str).str.strip()
        df['ConversionRate'] = pd.to_numeric(df['ConversionRate'], errors='coerce')
        df = df.dropna(subset=['currency', 'ConversionRate'])
        return df[['currency', 'ConversionRate']]

    def override_saver_fares(self, df):
        """
        For rows where FareBasis is one of the configured markers
        (self.saver_fare_markers, default ['C'] i.e. SUBSTRING(FareBasis, 5, 1) in
        the marker list), replace the Navitaire-computed Fare with the
        currency-adjusted fare from the route 'L' 'fares' collection for the
        matching sector and RBD (ClassOfService).

        Final fare = fares.TOTAL * ConversionRate(fares.currency -> INR).
        If currency is already INR, ConversionRate is treated as 1.
        Rows without a matching fares entry or conversion rate keep their existing Fare.

        Fully vectorized: sector/RBD keys, the Mongo fares join, the currency-rate
        join and the fare computation are all DataFrame operations - no per-row
        Python loops.

        Scalability: only the distinct (sector, rbd) combinations present in this
        batch of saver rows are fetched from Mongo (in bounded chunks), and only
        the distinct currencies of those fares are fetched from the DW/Navitaire.
        """
        saver_mask = df['FareBasis'].isin(self.saver_fare_markers)
        if not saver_mask.any():
            return df

        # Build normalized join keys on the saver rows (vectorized).
        saver_idx = df.index[saver_mask]
        sector = (df.loc[saver_idx, 'DepartureStation'].astype(str).str.strip()
                  + df.loc[saver_idx, 'ArrivalStation'].astype(str).str.strip())
        rbd = df.loc[saver_idx, 'ClassOfService'].astype(str).str.strip()
        saver_keys = pd.DataFrame({
            'SECTOR': sector.values,
            'RBD': rbd.values,
            'existing_fare': df.loc[saver_idx, 'Fare'].values,
        }, index=saver_idx)

        # 1) Distinct (sector, rbd) pairs -> targeted, chunked Mongo fetch.
        distinct_pairs = saver_keys[['SECTOR', 'RBD']].drop_duplicates()
        fares_df = self.fetch_route_l_fares(distinct_pairs)
        if fares_df.empty:
            self.logger.warning(
                f"FALLBACK to Navitaire fare: no route 'L' fares found for any of "
                f"{len(distinct_pairs)} sector/RBD combination(s); "
                f"keeping existing fares for {len(saver_keys)} saver row(s)."
            )
            return df

        # 2) Distinct source currencies from the fetched fares -> targeted rate fetch.
        needed = (fares_df.loc[fares_df['currency'].ne('INR'), 'currency']
                  .dropna().unique().tolist())
        rates_df = self.fetch_conversion_rates(needed)

        # 3) Join fares onto the saver rows (vectorized left merge).
        merged = saver_keys.merge(fares_df, on=['SECTOR', 'RBD'], how='left')
        merged.index = saver_keys.index

        # 4) Join conversion rates onto the merged fares (vectorized left merge).
        #    INR (or missing currency) is treated as rate 1.0.
        merged = merged.merge(rates_df, on='currency', how='left')
        merged.index = saver_keys.index
        is_inr = merged['currency'].isna() | merged['currency'].eq('INR')
        merged.loc[is_inr, 'ConversionRate'] = 1.0

        # 5) Vectorized fallback logging by category.
        no_fare = merged['TOTAL'].isna()
        no_rate = (~no_fare) & (~is_inr) & merged['ConversionRate'].isna()
        if no_fare.any():
            self.logger.warning(
                f"FALLBACK to Navitaire fare: no route 'L' fares entry for "
                f"{int(no_fare.sum())} saver row(s), e.g. sectors "
                f"{merged.loc[no_fare, 'SECTOR'].unique()[:5].tolist()}; "
                f"keeping existing fares."
            )
        if no_rate.any():
            self.logger.warning(
                f"FALLBACK to Navitaire fare: no ->INR rate in DW or Navitaire for "
                f"{int(no_rate.sum())} saver row(s), currencies "
                f"{merged.loc[no_rate, 'currency'].unique().tolist()}; "
                f"keeping existing fares."
            )

        # 6) Vectorized fare computation. Rows that fall back keep existing_fare.
        keep_existing = no_fare | no_rate
        adjusted = (merged['TOTAL'] * merged['ConversionRate']).round(4)
        new_fare = np.where(keep_existing, merged['existing_fare'], adjusted)

        df.loc[saver_idx, 'Fare'] = new_fare
        return df

    def updateValuesToRedis(self, last_booking_df):
        last_booking_df['FlightNumber'] = last_booking_df['FlightNumber'].astype(str).str.strip()
        for (departure, arrival, flight, date), group in last_booking_df.groupby(['DepartureStation', 'ArrivalStation', 'FlightNumber', 'DepartureDate']):
            # Generate the composite key
            key = f"LastSoldFare:{departure}{arrival}{flight}_{date}"
            
            # Prepare all fares list
            new_all_fares = group.sort_values('BookingUTC', ascending=False).apply(lambda row: {
                'BookingID': row['BookingID'],
                'PassengerID': row['PassengerID'],
                'ClassOfService': row['ClassOfService'],
                'FareBasis': row['FareBasis'],
                'Fare': row['Fare'],
                'BookingUTC': row['BookingUTC'].strftime('%Y-%m-%dT%H:%M:%S.%f')
            }, axis=1).tolist()

            filtered_all_fares = self.apply_filter(new_all_fares, 'allfares')
            filtered_b2b_fares = self.apply_filter(new_all_fares, 'b2bfares')
            filtered_b2c_fares = self.apply_filter(new_all_fares, 'b2cfares')
            
            # Check if key exists in Redis
            existing_data = self.cache_client.get(key)
            
            if existing_data:
                # Parse existing data
                existing_dict = json.loads(existing_data.decode('utf-8'))
                
                # Merge and deduplicate all fares (Booking Level)
                merged_all_fares = self._merge_and_limit_entries(existing_dict.get('allfares', []), filtered_all_fares, 7, False)
                b2b_fares = self._merge_and_limit_entries(existing_dict.get('b2bfares', []), filtered_b2b_fares, 3, False)
                b2c_fares = self._merge_and_limit_entries(existing_dict.get('b2cfares', []), filtered_b2c_fares, 3, False)
                
                # Merge and deduplicate all fares (Passenger Level)
                merged_all_fares_pax = self._merge_and_limit_entries(existing_dict.get('allfares_pax', []), filtered_all_fares, 7, True)
                b2b_fares_pax = self._merge_and_limit_entries(existing_dict.get('b2bfares_pax', []), filtered_b2b_fares, 3, True)
                b2c_fares_pax = self._merge_and_limit_entries(existing_dict.get('b2cfares_pax', []), filtered_b2c_fares, 3, True)
                
                # Prepare final dictionary
                final_data = {
                    'allfares': merged_all_fares,
                    'b2bfares': b2b_fares,
                    'b2cfares': b2c_fares,
                    'allfares_pax': merged_all_fares_pax,
                    'b2bfares_pax': b2b_fares_pax,
                    'b2cfares_pax': b2c_fares_pax
                }
            else:
                # If no existing data, create new lists using the new merge function to respect limits and deduplication
                final_data = {
                    'allfares': self._merge_and_limit_entries([], filtered_all_fares, 7, False),
                    'b2bfares': self._merge_and_limit_entries([], filtered_b2b_fares, 3, False),
                    'b2cfares': self._merge_and_limit_entries([], filtered_b2c_fares, 3, False),
                    'allfares_pax': self._merge_and_limit_entries([], filtered_all_fares, 7, True),
                    'b2bfares_pax': self._merge_and_limit_entries([], filtered_b2b_fares, 3, True),
                    'b2cfares_pax': self._merge_and_limit_entries([], filtered_b2c_fares, 3, True)
                }

            # Store in Redis as JSON
            self.cache_client.set(key, json.dumps(final_data))
    
    def _merge_and_limit_entries(self, existing_list, new_list, limit, dedup_by_pax):
        # Combine lists and remove duplicates
        combined_list = []
        seen_bookings = set()
        
        # Combine new and existing entries
        for item in new_list + existing_list:
            unique_key = item['BookingID']
            if dedup_by_pax:
                pax_id = item.get('PassengerID', '')
                unique_key = f"{item['BookingID']}_{pax_id}"
                
            if unique_key not in seen_bookings:
                combined_list.append(item)
                seen_bookings.add(unique_key)
        
        # Sort by BookingUTC and keep top entries
        return sorted(combined_list, 
                     key=lambda x: datetime.strptime(x['BookingUTC'], '%Y-%m-%dT%H:%M:%S.%f'), 
                     reverse=True)[:limit]

    def lastSoldQuery(self, whereCondition):
        # This still uses MSSQL for Navitaire database
        query = f"""
        Select 
            BookingID, BookingUTC, PassengerID, DepartureStation, ArrivalStation, DepartureDate, FlightNumber, 
            CASE 
                WHEN LEFT(FareBasis, 1) = 'G' THEN 'G' 
                ELSE SUBSTRING(FareBasis, 5, 1) 
            END as FareBasis, ClassOfService,
            Fare
            from 
            (
              Select
                BookingID, BookingUTC, PassengerID, SegmentID, InventoryLegID, DepartureStation, ArrivalStation, DepartureDate, FlightNumber, FareBasis, RecordLocator,
                CAST(sum(BaseFare+AirportCharges+Taxes) over (Partition by PassengerID,TripNumber,JourneyNumber,ModSegmentNumber)*(SQRT(Distance))/SUM(SQRT(Distance)) OVER (Partition by PassengerID,TripNumber,JourneyNumber,ModSegmentNumber) as decimal(19,4)) as Fare, ClassOfService
                from 
                (
                  SELECT
                    B.RecordLocator, B.BookingID, B.BookingUTC, B.Status, BP.PassengerID, PJS.SegmentID, IL.InventoryLegID, 
                    IL.DepartureStation, IL.ArrivalStation, IL.DepartureDate, IL.FlightNumber,
                    PJS.TripNumber, PJS.JourneyNumber, PJS.SegmentNumber, PJS.FareComponentNumber, PJS.FareJourneyType, PJS.FareBasis,
                    Case when PJS.FareJourneyType in (1,4) then SUM(Case when PJC.ChargeType in (0,1,2,3) then (PJC.ChargeAmount*CTM.PositiveNegativeFlag*CCV.ConversionRate) else 0 end) 
                      when PJS.FareJourneyType=2 then SUM(Case when PJC.ChargeType in (0,1,2,3) and PJS.SegmentNumber=PJL.LegNumber then (PJC.ChargeAmount*CTM.PositiveNegativeFlag*CCV.ConversionRate) else 0 end) else 0 end as BaseFare,
                    Case when PJS.FareJourneyType in (1,4) then SUM(Case when PJC.ChargeType=4 then (PJC.ChargeAmount*CTM.PositiveNegativeFlag*CCV.ConversionRate) else 0 end) 
                      when PJS.FareJourneyType=2 then SUM(Case when PJC.ChargeType=4 and PJS.SegmentNumber=PJL.LegNumber then (PJC.ChargeAmount*CTM.PositiveNegativeFlag*CCV.ConversionRate) else 0 end) else 0 end as AirportCharges,
                    Case when PJS.FareJourneyType in (1,4) then SUM(Case when PJC.ChargeType=5 then (PJC.ChargeAmount*CTM.PositiveNegativeFlag*CCV.ConversionRate) else 0 end) 
                    + SUM(Case when PJC.ChargeType=3 and CTM.IncludedTaxFlag=1  then (PJC.ChargeAmount*CCV.ConversionRate) else 0 end)
                    when PJS.FareJourneyType=2 then SUM(Case when PJC.ChargeType=5 and PJS.SegmentNumber=PJL.LegNumber then (PJC.ChargeAmount*CTM.PositiveNegativeFlag*CCV.ConversionRate) else 0 end) 
                    + SUM(Case when PJC.ChargeType=3 and CTM.IncludedTaxFlag=1  then (PJC.ChargeAmount*CCV.ConversionRate) else 0 end)
                    else 0 end as Taxes,
                    case when PJS.FareBasis='' then 1 else SegmentNumber end as ModSegmentNumber,
                    avg(CP.ActualDistance) as Distance,
                    PJS.ClassOfService
                  FROM
                    {self.NAV_DB_NAME}.rez.Booking B
                    INNER JOIN {self.NAV_DB_NAME}.rez.BookingPassenger BP ON BP.BookingID = B.BookingID
                    INNER JOIN {self.NAV_DB_NAME}.rez.PassengerJourneySegment PJS ON PJS.PassengerID = BP.PassengerID AND PJS.ClassOfService NOT IN ('S') 
                    INNER JOIN {self.NAV_DB_NAME}.rez.PassengerJourneyLeg PJL ON PJL.PassengerID = PJS.PassengerID AND PJL.SegmentID = PJS.SegmentID
                    INNER JOIN {self.NAV_DB_NAME}.rez.InventoryLeg IL ON IL.InventoryLegId = PJL.InventoryLegID AND IL.Status NOT IN (2) AND IL.Lid > 0
                    LEFT JOIN {self.NAV_DB_NAME}.rez.PassengerJourneyCharge PJC ON PJC.PassengerID = PJL.PassengerID AND PJC.SegmentID = PJL.SegmentID
                    LEFT JOIN {self.NAV_DB_NAME}.dw.ChargeTypeMatrix CTM ON CTM.ChargeTypeID = PJC.ChargeType
                    Left join {self.NAV_DB_NAME}.dw.CurrencyConversionVersion CCV ON CCV.FromCurrencyCode=PJC.CurrencyCode and CCV.ToCurrencyCode='INR' and CCV.ClientCode='0K' and CCV.VersionStartUTC<=PJC.CreatedUTC and CCV.VersionEndUTC>PJC.CreatedUTC
                    inner join {self.NAV_DB_NAME}.dw.CityPair CP ON CP.DepartureStation = IL.DepartureStation and CP.ArrivalStation = IL.ArrivalStation 
                  WHERE
                    BookingUTC >= {whereCondition}
                    AND B.Status = 2
                    AND (SUBSTRING(FareBasis, 5, 1) IN ('M', 'R', 'C', 'V', 'Z')
		            OR LEFT(FareBasis, 1) = 'G')
                  GROUP BY
                    B.BookingID, B.Status, BP.PassengerID, PJS.SegmentID, IL.InventoryLegID, IL.DepartureStation, IL.ArrivalStation, IL.DepartureDate, IL.FlightNumber,
                    PJS.TripNumber, PJS.JourneyNumber, PJS.SegmentNumber, PJS.FareComponentNumber, PJS.FareJourneyType, PJS.FareBasis, B.BookingUTC, B.RecordLocator, PJS.ClassOfService
                ) X
              ) Y
              WHERE
                SUBSTRING(FareBasis, 3, 1) != 'C'
        """
        df = pd.read_sql(query, self.NavitaireConnection)
        return df

    def get_credentials(self, parameter_name):
        ssm_client = client('ssm', region_name='ap-south-1')
        try:
            response = ssm_client.get_parameter(Name=parameter_name, WithDecryption=True)
        except ssm_client.exceptions.ParameterNotFound:
            print("not found")
        parameter_value = response['Parameter']['Value']
        return parameter_value


if __name__ == "__main__":
    LastSoldFare()