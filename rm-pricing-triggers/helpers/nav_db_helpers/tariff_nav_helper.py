import pandas as pd
from datetime import datetime, date


class TariffNavHelper:
    """Handles Navitaire-related database operations for the Tariff Sheet Automation module."""

    def __init__(self, nav_conn, nav_db_name, nav_db_dw_name, nav_db_wb_name, logger):
        """
        Initialize with Navitaire MSSQL connection and database name prefixes.

        Args:
            nav_conn: SQLAlchemy engine for Navitaire MSSQL.
            nav_db_name: Database name prefix for rez schema (e.g., '[REZ0KQP01].').
            nav_db_dw_name: Database name prefix for dw schema (e.g., '[DW0KQP01].').
            nav_db_wb_name: Database name prefix for wb/schedule schema (e.g., '[REZ0KWB01].').
            logger: Logger instance.
        """
        self.nav_conn = nav_conn
        self.nav_db_name = nav_db_name
        self.nav_db_dw_name = nav_db_dw_name
        self.nav_db_wb_name = nav_db_wb_name
        self.logger = logger

    def get_direct_sectors(self):
        """Query InventoryLeg for distinct direct sectors.

        Retrieves distinct DepartureStation, ArrivalStation pairs where
        CarrierCode='QP', FlightNumber < 6000, and DepartureDate >= today.

        Returns:
            pd.DataFrame: DataFrame with columns DepartureStation, ArrivalStation.
        """
        try:
            today = date.today()
            query = f"""
            SELECT DISTINCT DepartureStation, ArrivalStation
            FROM {self.nav_db_name}rez.InventoryLeg
            WHERE CarrierCode = 'QP'
            AND FlightNumber < 6000
            AND DepartureDate >= '{today}'
            AND Status <> 2
            """
            df = pd.read_sql(query, self.nav_conn)
            self.logger.info(f"Retrieved {len(df)} direct sectors from Navitaire InventoryLeg")
            return df
        except Exception as e:
            self.logger.error(f"Error retrieving direct sectors: {e}")
            raise

    def get_through_segments(self):
        """Query for through segments (JourneyType=2) where both legs share the same flight number.

        Uses the SP_GetNavitaireJourneys stored procedure to retrieve journey data,
        filtering for through flights (JourneyType=2) with matching flight numbers
        on both legs. Returns sectors as origin-of-leg1 + destination-of-leg2 with stops=1.

        Returns:
            pd.DataFrame: DataFrame with columns DepartureStation, ArrivalStation
                          representing through segment origin-destination pairs.
        """
        try:
            today = date.today()
            end_date = datetime(today.year + 1, today.month, today.day).strftime('%Y-%m-%d')

            query = f"""
            SELECT DISTINCT
                JourneyDepartureStation AS DepartureStation,
                JourneyArrivalStation AS ArrivalStation
            FROM (
                SELECT
                    JourneyDepartureStation,
                    JourneyArrivalStation,
                    FlightNumber1,
                    FlightNumber2,
                    JourneyType
                FROM OPENQUERY(
                    [{self.nav_db_wb_name.replace('[', '').replace('].', '').strip()}],
                    'EXEC dbo.SP_GetNavitaireJourneys ''{today}'', ''{end_date}'''
                )
            ) j
            WHERE j.JourneyType = 'Through'
            AND j.FlightNumber1 = j.FlightNumber2
            """

            # Try the stored procedure approach first
            try:
                df = pd.read_sql(query, self.nav_conn)
                self.logger.info(f"Retrieved {len(df)} through segments via SP_GetNavitaireJourneys")
                return df
            except Exception:
                self.logger.warning("SP_GetNavitaireJourneys via OPENQUERY failed, falling back to direct schedule query")

            # Fallback: query the ActiveJourneyView directly
            query_fallback = f"""
            SELECT DISTINCT
                s1.DepartureStation AS DepartureStation,
                s2.ArrivalStation AS ArrivalStation
            FROM {self.nav_db_wb_name}Schedule.ActiveJourneyView s1
            INNER JOIN {self.nav_db_wb_name}Schedule.ActiveJourneyView s2
                ON s1.ScheduleJourneyID = s2.ScheduleJourneyID
            WHERE s1.JourneyType = 2
            AND s1.SegmentNumber = 1 AND s1.LegNumber = 1
            AND s2.SegmentNumber = 1 AND s2.LegNumber = 2
            AND s1.SegmentFlightNumber = s2.SegmentFlightNumber
            AND s1.JourneyEndDate >= '{today}'
            """
            df = pd.read_sql(query_fallback, self.nav_conn)
            self.logger.info(f"Retrieved {len(df)} through segments from ActiveJourneyView")
            return df
        except Exception as e:
            self.logger.error(f"Error retrieving through segments: {e}")
            raise

    def get_city_pair_distances(self):
        """Query CityPair table for sector distance data.

        Returns:
            pd.DataFrame: DataFrame with columns CityPairID, CityPair,
                          DepartureStation, ArrivalStation, ActualDistance.
        """
        try:
            query = f"""
            SELECT
                CityPairID, CityPair, DepartureStation, ArrivalStation, ActualDistance
            FROM {self.nav_db_dw_name}dw.CityPair
            """
            df = pd.read_sql(query, self.nav_conn)
            self.logger.info(f"Retrieved {len(df)} city pair distance records")
            return df
        except Exception as e:
            self.logger.error(f"Error retrieving city pair distances: {e}")
            raise

    def collect_city_pair_data(self, sector):
        """Collect city pair data for distances - filtered for current sector."""
        try:
            origin = sector[:3]
            destination = sector[3:]
            
            query = f"""
            SELECT CityPairID, CityPair, CityPairGroup, DepartureStation, ArrivalStation, ActualDistance
            FROM {self.nav_db_dw_name}dw.CityPair
            WHERE (DepartureStation = '{origin}' AND ArrivalStation = '{destination}')
            """
            
            city_pair_data = pd.read_sql(query, self.nav_conn)
            
            if city_pair_data.empty:
                self.failure_reason = f"No city pair data found for sector {sector}"
                self.logger.warning(self.failure_reason)
                
            return city_pair_data
            
        except Exception as e:
            self.logger.error(f"Error collecting city pair data: {str(e)}")
            # City pair is not critical - just log and continue
            self.logger.warning("City pair data unavailable but not critical for ladder generation")
            city_pair_data = pd.DataFrame()
            return city_pair_data
