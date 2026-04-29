import logging
from datetime import datetime
from constants_utils import ConstantsUtils
import pandas as pd
import configsettings

class FetchActiveFlights:
    def __init__(self, logger, params, overrides, context):
        """Initialize FetchActiveFlights with required connections."""
        self.logger = logger
        self.mongo_client = params['mongoconn']
        self.mongo_db = self.mongo_client['qp_dw_rmalloc']
        self.nav_db_name = configsettings.NAV_DB_NAME
        self.nav_conn_db_name = configsettings.NAV_CONN_DB_NAME
        self.nav_conn = params['navconn']
        self.collection_legs = 'active_flight_legs'
        self.collection_connections = 'active_flight_connections'

    def fetch_nav_flights(self):
        """Fetch active flight keys from Navitaire InventoryLeg."""
        try:
            # Query all active legs >= today
            query = f"""
            SELECT DISTINCT
                DepartureStation AS Origin,
                ArrivalStation AS Destin,
                TRIM(FlightNumber) AS FlightNumber,
                CONVERT(VARCHAR(10), DepartureDate, 120) AS DepartureDate
            FROM {self.nav_db_name}rez.InventoryLeg
            WHERE DepartureDate >= CAST(GETDATE() AS DATE)
            AND CarrierCode='QP'
            """
            df = pd.read_sql(query, self.nav_conn)
            df['FlightNumber'] = df['FlightNumber'].astype(str).str.strip()
            df['Origin'] = df['Origin'].astype(str).str.strip()
            df['Destin'] = df['Destin'].astype(str).str.strip()
            df['DepartureDate'] = df['DepartureDate'].astype(str).str.strip().str[:10]
            
            records = df.to_dict('records')
            self.logger.info(f"Fetched {len(records)} distinct active flight records from Navitaire")
            return records
        except Exception as e:
            self.logger.error(f"Error fetching Navitaire flights: {e}")
            raise

    def fetch_nav_connections(self):
        """Fetch active connection keys from Navitaire."""
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

            SELECT DISTINCT
                FlightNumber1 AS Flight1,
                Sector1,
                FlightNumber2 AS Flight2,
                Sector2,
                CONVERT(VARCHAR(10), DepartureDate1, 120) AS DepartureDate
            FROM JourneyData
            WHERE rn = 1
            """
            df = pd.read_sql(query, self.nav_conn)
            df['Flight1'] = df['Flight1'].astype(str).str.strip()
            df['Sector1'] = df['Sector1'].astype(str).str.strip()
            df['Flight2'] = df['Flight2'].astype(str).str.strip()
            df['Sector2'] = df['Sector2'].astype(str).str.strip()
            df['DepartureDate'] = df['DepartureDate'].astype(str).str.strip().str[:10]
            
            records = df.to_dict('records')
            self.logger.info(f"Fetched {len(records)} distinct active connection records from Navitaire")
            return records
        except Exception as e:
            self.logger.error(f"Error fetching Navitaire connections: {e}")
            raise

    def store_active_flights(self, collection_name, records):
        """Store the active flight records into the MongoDB collection."""
        try:
            collection = self.mongo_db[collection_name]
            
            # Drop the collection to maintain only fresh data
            collection.drop()
            self.logger.info(f"Dropped collection {collection_name}")
            
            if records:
                # Insert the fresh records
                result = collection.insert_many(records)
                self.logger.info(f"Inserted {len(result.inserted_ids)} records into {collection_name}")
            else:
                self.logger.info(f"No records to insert into {collection_name}")
        except Exception as e:
            self.logger.error(f"Error storing active flights in {collection_name}: {e}")
            raise

    def run(self):
        """Main method to execute the fetch and store logic."""
        self.logger.info(f"Starting FetchActiveFlights job")
        try:
            # Direct flights
            leg_records = self.fetch_nav_flights()
            self.store_active_flights(self.collection_legs, leg_records)
            
            # Connections
            conn_records = self.fetch_nav_connections()
            self.store_active_flights(self.collection_connections, conn_records)
            
            self.logger.info(f"FetchActiveFlights job completed successfully.")
        except Exception as e:
            self.logger.error(f"Error in FetchActiveFlights run: {e}")

