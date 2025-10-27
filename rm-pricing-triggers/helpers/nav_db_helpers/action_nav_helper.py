import pandas as pd
from datetime import datetime, timedelta, date

class ActionNavHelper:
    """Handles Navitaire-related database operations."""

    def __init__(self, nav_conn, nav_db_name, nav_db_dw_name, nav_db_wb_name, logger):
        self.nav_conn = nav_conn
        self.nav_db_name = nav_db_name
        self.nav_db_dw_name = nav_db_dw_name
        self.nav_db_wb_name = nav_db_wb_name
        self.logger = logger

    def collect_booked_data(self, date_range_days=30):
        """Collect passenger booking data for PLF calculation."""
        try:
            start_date = date.today()
            end_date = start_date + timedelta(days=date_range_days)
            query = f"""
            EXEC {self.nav_db_wb_name}[dbo].[SP_Bookeddata]
            @DepartureDate_from = N'{start_date.strftime('%Y-%m-%d')}',
            @DepartureDate_to = N'{end_date.strftime('%Y-%m-%d')}'
            """
            return pd.read_sql(query, self.nav_conn)
        except Exception as e:
            self.logger.error(f"Error collecting booked data: {e}")
            raise

    def collect_city_pair_data(self):
        """Collect city pair data for sector distances."""
        try:
            query = f"""
            SELECT 
                CityPairID, CityPair, CityPairGroup, DepartureStation, ArrivalStation, ActualDistance
            FROM {self.nav_db_dw_name}dw.CityPair
            """
            return pd.read_sql(query, self.nav_conn)
        except Exception as e:
            self.logger.error(f"Error collecting city pair data: {e}")
            raise

    def collect_active_sectors(self):
        """Collect active sectors from Navitaire."""
        try:
            query = f"""
            SELECT DISTINCT DepartureStation, ArrivalStation
            FROM {self.nav_db_name}rez.InventoryLeg
            WHERE DepartureDate >= '{datetime.now().date()}'
            AND CarrierCode='QP' AND FlightNumber < 7000
            """
            return pd.read_sql(query, self.nav_conn)
        except Exception as e:
            self.logger.error(f"Error collecting active sectors data: {e}")
            raise
