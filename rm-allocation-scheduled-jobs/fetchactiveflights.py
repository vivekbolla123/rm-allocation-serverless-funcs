import logging
from datetime import datetime, timedelta
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
        """Fetch active connection keys from Navitaire using SP_GetNavitaireJourneys."""
        try:
            start_date = datetime.now().strftime('%Y-%m-%d')
            end_date = (datetime.now() + timedelta(days=365)).strftime('%Y-%m-%d')
            query = f"""EXEC {configsettings.NAV_DB_WB_NAME}[dbo].[SP_GetNavitaireJourneys]
                        @StartDate = '{start_date}',
                        @EndDate = '{end_date}'
                    """
            df = pd.read_sql(query, self.nav_conn)
            # Rename columns to match expected output
            df = df.rename(columns={'FlightNumber1': 'Flight1', 'FlightNumber2': 'Flight2'})
            df['DepartureDate'] = pd.to_datetime(df['DepartureDate1']).dt.strftime('%Y-%m-%d')
            df = df[['Flight1', 'Sector1', 'Flight2', 'Sector2', 'DepartureDate']].drop_duplicates()
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

