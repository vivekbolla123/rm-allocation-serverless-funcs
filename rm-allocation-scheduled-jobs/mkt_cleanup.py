import logging
from datetime import datetime, timedelta
from constants_utils import ConstantsUtils
import pandas as pd
import configsettings
import json


class MarketListCleanup:
    def __init__(self, logger, params, overrides, context):
        """Initialize MarketListCleanup with required connections and constants."""
        self.logger = logger
        self.mongo_client = params['mongoconn']
        self.mongo_db = self.mongo_client['qp_dw_rmalloc']
        self.nav_db_name = configsettings.NAV_DB_NAME
        self.nav_conn = params['navconn']

    def fetch_nav_flight_keys(self):
        """Fetch active flight keys from Navitaire InventoryLeg."""
        try:
            query = f"""
            SELECT DISTINCT
                DepartureStation AS Origin,
                ArrivalStation AS Destin,
                TRIM(FlightNumber) AS FlightNumber,
                CONVERT(VARCHAR(10), DepartureDate, 120) AS DepartureDate
            FROM {self.nav_db_name}rez.InventoryLeg
            WHERE DepartureDate >= CAST(GETDATE() AS DATE)
            AND CarrierCode='QP' AND FlightNumber < 7000
            """
            df = pd.read_sql(query, self.nav_conn)
            df['FlightNumber'] = df['FlightNumber'].astype(str).str.strip()
            # Build a set of (Origin, Destin, FlightNumber, DepartureDate yyyy-mm-dd)
            nav_keys = set()
            for _, row in df.iterrows():
                nav_keys.add((
                    row['Origin'].strip(),
                    row['Destin'].strip(),
                    row['FlightNumber'],
                    str(row['DepartureDate']).strip()[:10]  # yyyy-mm-dd
                ))
            self.logger.info(f"Fetched {len(nav_keys)} distinct flight keys from Navitaire")
            return nav_keys
        except Exception as e:
            self.logger.error(f"Error fetching Navitaire flight keys: {e}")
            raise

    def _convert_perstart_to_nav_date(self, perstart_val):
        """Convert PerStart (mm/dd/yyyy) to yyyy-mm-dd for comparison with Navitaire."""
        try:
            if isinstance(perstart_val, str):
                dt = datetime.strptime(perstart_val.strip(), '%m/%d/%Y')
                return dt.strftime('%Y-%m-%d')
            elif isinstance(perstart_val, datetime):
                return perstart_val.strftime('%Y-%m-%d')
            else:
                return pd.to_datetime(perstart_val).strftime('%Y-%m-%d')
        except Exception:
            return None

    def cleanup_collection(self, collection_name, nav_keys):
        """Delete docs from a collection whose flight key is not in Navitaire.
        
        Key: (Origin, Destin, FlightNumber, PerStart as yyyy-mm-dd)
        Only considers docs with PerStart >= today.
        """
        try:
            collection = self.mongo_db[collection_name]
            today_str = datetime.now().strftime('%Y-%m-%d')

            # Fetch all future docs with key fields
            cursor = collection.find(
                {},
                {'_id': 1, 'Origin': 1, 'Destin': 1, 'FlightNumber': 1, 'PerStart': 1}
            )

            ids_to_delete = []
            total_checked = 0

            for doc in cursor:
                perstart_nav = self._convert_perstart_to_nav_date(doc.get('PerStart'))
                if perstart_nav is None or perstart_nav < today_str:
                    continue  # skip past dates or unparseable

                flight_key = (
                    str(doc.get('Origin', '')).strip(),
                    str(doc.get('Destin', '')).strip(),
                    str(doc.get('FlightNumber', '')).strip(),
                    perstart_nav
                )
                total_checked += 1

                if flight_key not in nav_keys:
                    ids_to_delete.append(doc['_id'])

            deleted_count = 0
            if ids_to_delete:
                result = collection.delete_many({'_id': {'$in': ids_to_delete}})
                deleted_count = result.deleted_count

            self.logger.info(
                f"{collection_name}: checked {total_checked} future docs, "
                f"deleted {deleted_count} with no matching Navitaire flight"
            )
            return {'checked': total_checked, 'deleted': deleted_count}

        except Exception as e:
            self.logger.error(f"Error cleaning up {collection_name}: {e}")
            raise

    def run(self):
        """Main method: remove market_list / market_list_international docs
        whose flight key does not exist in Navitaire InventoryLeg."""
        try:
            nav_keys = self.fetch_nav_flight_keys()

            ml_result = self.cleanup_collection('market_list', nav_keys)
            mli_result = self.cleanup_collection('market_list_international', nav_keys)

            self.logger.info(
                f"Market list cleanup complete — "
                f"market_list: {ml_result['deleted']}/{ml_result['checked']} removed, "
                f"market_list_international: {mli_result['deleted']}/{mli_result['checked']} removed"
            )

        except Exception as e:
            self.logger.error(f"Error in MarketListCleanup run: {e}")
