import logging
from datetime import datetime, timedelta
from constants_utils import ConstantsUtils
import pandas as pd
import configsettings
import json


class MarketListCleanup:
    ALL_COLLECTIONS = ['market_list', 'market_list_international', 'market_list_connections']

    def __init__(self, logger, params, overrides, context):
        """Initialize MarketListCleanup with required connections and constants."""
        self.logger = logger
        self.mongo_client = params['mongoconn']
        self.mongo_db = self.mongo_client['qp_dw_rmalloc']
        self.nav_db_name = configsettings.NAV_DB_NAME
        self.nav_conn_db_name = configsettings.NAV_CONN_DB_NAME
        self.nav_conn = params['navconn']
        self.overrides = overrides or {}

    def _get_target_collections(self):
        """Determine which collections to clean based on overrides.
        
        If overrides contains a 'cleanup' key, only clean the specified collections.
        Otherwise, clean all three collections.
        """
        cleanup_override = self.overrides.get('cleanup')
        if cleanup_override:
            if isinstance(cleanup_override, str):
                targets = [c.strip() for c in cleanup_override.split(',')]
            elif isinstance(cleanup_override, list):
                targets = [c.strip() for c in cleanup_override]
            else:
                targets = self.ALL_COLLECTIONS

            # Validate collection names
            valid = [c for c in targets if c in self.ALL_COLLECTIONS]
            invalid = [c for c in targets if c not in self.ALL_COLLECTIONS]
            if invalid:
                self.logger.warning(f"Ignoring unknown collection names in overrides: {invalid}")
            return valid
        return list(self.ALL_COLLECTIONS)

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
            AND Status <> 2
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

    def fetch_nav_connection_keys(self):
        """Fetch active connection keys from Navitaire ActiveJourneyView."""
        try:
            query = f"""
            WITH JourneyData AS (
                SELECT
                    a.ScheduleJourneyID,
                    a.JourneyDepartureStation,
                    a.JourneyArrivalStation,
                    a.JourneyBeginDate,

                    -- Segment 1
                    LTRIM(RTRIM(CAST(s1.SegmentFlightNumber AS VARCHAR)))                                                       AS FlightNumber1,
                    s1.DepartureStation + s1.ArrivalStation                                                               AS Sector1,
                    CAST(a.JourneyBeginDate AS DATE)                                                                            AS DepartureDate1,
                    RIGHT('0' + CAST(s1.PaxSTD / 60 AS VARCHAR), 2) + ':' + RIGHT('0' + CAST(s1.PaxSTD % 60 AS VARCHAR), 2)   AS STD1,
                    RIGHT('0' + CAST(s1.PaxSTA / 60 AS VARCHAR), 2) + ':' + RIGHT('0' + CAST(s1.PaxSTA % 60 AS VARCHAR), 2)   AS STA1,

                    -- Segment 2
                    LTRIM(RTRIM(CAST(s2.SegmentFlightNumber AS VARCHAR)))                                                       AS FlightNumber2,
                    s2.DepartureStation + s2.ArrivalStation                                                               AS Sector2,
                    DATEADD(DAY, s2.OnwardDays, CAST(a.JourneyBeginDate AS DATE))                                               AS DepartureDate2,
                    RIGHT('0' + CAST(s2.PaxSTD / 60 AS VARCHAR), 2) + ':' + RIGHT('0' + CAST(s2.PaxSTD % 60 AS VARCHAR), 2)   AS STD2,
                    RIGHT('0' + CAST(s2.PaxSTA / 60 AS VARCHAR), 2) + ':' + RIGHT('0' + CAST(s2.PaxSTA % 60 AS VARCHAR), 2)   AS STA2,

                    ROW_NUMBER() OVER (
                        PARTITION BY
                            LTRIM(RTRIM(CAST(s1.SegmentFlightNumber AS VARCHAR))),
                            LTRIM(RTRIM(CAST(s2.SegmentFlightNumber AS VARCHAR))),
                            s1.DepartureStation + '-' + s1.ArrivalStation,
                            s2.DepartureStation + '-' + s2.ArrivalStation,
                            CAST(a.JourneyBeginDate AS DATE),
                            DATEADD(DAY, s2.OnwardDays, CAST(a.JourneyBeginDate AS DATE))
                        ORDER BY a.ScheduleJourneyID
                    ) AS rn

                FROM (
                    SELECT DISTINCT
                        ScheduleJourneyID,
                        JourneyDepartureStation,
                        JourneyArrivalStation,
                        JourneyBeginDate
                    FROM {self.nav_conn_db_name}Schedule.ActiveJourneyView
                    WHERE JourneyBeginDate >= CAST(GETDATE() AS DATE)
                ) a

                INNER JOIN {self.nav_conn_db_name}Schedule.ActiveJourneyView s1
                    ON s1.ScheduleJourneyID = a.ScheduleJourneyID
                    AND s1.SegmentNumber = 1

                INNER JOIN {self.nav_conn_db_name}Schedule.ActiveJourneyView s2
                    ON s2.ScheduleJourneyID = a.ScheduleJourneyID
                    AND s2.SegmentNumber = 2

                INNER JOIN {self.nav_db_name}Rez.InventoryLeg il1
                    ON il1.CarrierCode       = s1.SegmentCarrierCode
                    AND il1.FlightNumber     = s1.SegmentFlightNumber
                    AND il1.DepartureStation = s1.DepartureStation
                    AND il1.ArrivalStation   = s1.ArrivalStation
                    AND CAST(il1.DepartureDate AS DATE) = CAST(a.JourneyBeginDate AS DATE)
                    AND il1.Status = 0

                INNER JOIN {self.nav_db_name}Rez.InventoryLeg il2
                    ON il2.CarrierCode       = s2.SegmentCarrierCode
                    AND il2.FlightNumber     = s2.SegmentFlightNumber
                    AND il2.DepartureStation = s2.DepartureStation
                    AND il2.ArrivalStation   = s2.ArrivalStation
                    AND CAST(il2.DepartureDate AS DATE) = DATEADD(DAY, s2.OnwardDays, CAST(a.JourneyBeginDate AS DATE))
                    AND il2.Status = 0
            )

            SELECT
                ScheduleJourneyID,
                JourneyDepartureStation,
                JourneyArrivalStation,
                JourneyBeginDate,
                FlightNumber1,
                Sector1,
                DepartureDate1,
                STD1,
                STA1,
                FlightNumber2,
                Sector2,
                DepartureDate2,
                STD2,
                STA2
            FROM JourneyData
            WHERE rn = 1
            ORDER BY DepartureDate1, JourneyDepartureStation, JourneyArrivalStation;
            """
            df = pd.read_sql(query, self.nav_conn)
            df['FlightNumber1'] = df['FlightNumber1'].astype(str).str.strip()
            df['FlightNumber2'] = df['FlightNumber2'].astype(str).str.strip()
            df['Sector1'] = df['Sector1'].astype(str).str.strip()
            df['Sector2'] = df['Sector2'].astype(str).str.strip()

            # Build a set of (Sector1, Sector2, FlightNumber1, FlightNumber2, DepartureDate1 yyyy-mm-dd)
            conn_keys = set()
            for _, row in df.iterrows():
                conn_keys.add((
                    row['Sector1'],
                    row['Sector2'],
                    row['FlightNumber1'],
                    row['FlightNumber2'],
                    str(row['DepartureDate1']).strip()[:10]  # yyyy-mm-dd
                ))
            self.logger.info(f"Fetched {len(conn_keys)} distinct connection keys from Navitaire")
            return conn_keys
        except Exception as e:
            self.logger.error(f"Error fetching Navitaire connection keys: {e}")
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
                total_checked += 1
                perstart_nav = self._convert_perstart_to_nav_date(doc.get('PerStart'))
                if perstart_nav is None or perstart_nav < today_str:
                    ids_to_delete.append(doc['_id'])
                    continue  # delete past dates or unparseable

                flight_key = (
                    str(doc.get('Origin', '')).strip(),
                    str(doc.get('Destin', '')).strip(),
                    str(doc.get('FlightNumber', '')).strip(),
                    perstart_nav
                )

                if flight_key not in nav_keys:
                    ids_to_delete.append(doc['_id'])

            deleted_count = 0
            if ids_to_delete:
                result = collection.delete_many({'_id': {'$in': ids_to_delete}})
                deleted_count = result.deleted_count

            self.logger.info(
                f"{collection_name}: checked {total_checked} docs, "
                f"deleted {deleted_count} (past dates or no matching Navitaire flight)"
            )
            return f"checked: {total_checked}, deleted: {deleted_count}"

        except Exception as e:
            self.logger.error(f"Error cleaning up {collection_name}: {e}")
            raise

    def cleanup_connections_collection(self, conn_keys):
        """Delete docs from market_list_connections whose connection key is not in Navitaire.
        
        Key: (Sector1, Sector2, Flight1, Flight2, PerStart as yyyy-mm-dd)
        Only considers docs with PerStart >= today.
        """
        collection_name = 'market_list_connections'
        try:
            collection = self.mongo_db[collection_name]
            today_str = datetime.now().strftime('%Y-%m-%d')

            # Fetch all docs with key fields
            cursor = collection.find(
                {},
                {'_id': 1, 'Sector1': 1, 'Sector2': 1, 'Flight1': 1, 'Flight2': 1, 'PerStart': 1}
            )

            ids_to_delete = []
            total_checked = 0

            for doc in cursor:
                total_checked += 1
                perstart_nav = self._convert_perstart_to_nav_date(doc.get('PerStart'))
                if perstart_nav is None or perstart_nav < today_str:
                    ids_to_delete.append(doc['_id'])
                    continue  # delete past dates or unparseable

                conn_key = (
                    str(doc.get('Sector1', '')).strip(),
                    str(doc.get('Sector2', '')).strip(),
                    str(doc.get('Flight1', '')).strip(),
                    str(doc.get('Flight2', '')).strip(),
                    perstart_nav
                )

                if conn_key not in conn_keys:
                    ids_to_delete.append(doc['_id'])

            deleted_count = 0
            if ids_to_delete:
                result = collection.delete_many({'_id': {'$in': ids_to_delete}})
                deleted_count = result.deleted_count

            self.logger.info(
                f"{collection_name}: checked {total_checked} docs, "
                f"deleted {deleted_count} (past dates or no matching Navitaire connection)"
            )
            return f"checked: {total_checked}, deleted: {deleted_count}"

        except Exception as e:
            self.logger.error(f"Error cleaning up {collection_name}: {e}")
            raise

    def run(self):
        """Main method: remove docs from target collections whose keys
        do not exist in Navitaire (InventoryLeg for direct flights,
        ActiveJourneyView for connections)."""
        try:
            targets = self._get_target_collections()
            self.logger.info(f"Cleanup targets: {targets}")

            results = {}
            direct_collections = [c for c in targets if c in ('market_list', 'market_list_international')]
            do_connections = 'market_list_connections' in targets

            # Clean direct flight collections
            if direct_collections:
                nav_keys = self.fetch_nav_flight_keys()
                for coll in direct_collections:
                    results[coll] = self.cleanup_collection(coll, nav_keys)

            # Clean connections collection
            if do_connections:
                conn_keys = self.fetch_nav_connection_keys()
                results['market_list_connections'] = self.cleanup_connections_collection(conn_keys)

            # Summary log
            summary_parts = []
            for coll, res in results.items():
                summary_parts.append(f"{coll}: {res}")
            self.logger.info(f"Market list cleanup complete — {', '.join(summary_parts)}")

            return results

        except Exception as e:
            self.logger.error(f"Error in MarketListCleanup run: {e}")
