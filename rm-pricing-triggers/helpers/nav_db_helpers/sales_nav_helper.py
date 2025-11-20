import pandas as pd
from datetime import datetime, timedelta, date

class SalesNavHelper:
    """Handles Navitaire-related database operations."""

    def __init__(self, nav_conn, nav_db_name, nav_db_dw_name, nav_db_wb_name, logger):
        self.nav_conn = nav_conn
        self.nav_db_name = nav_db_name
        self.nav_db_dw_name = nav_db_dw_name
        self.nav_db_wb_name = nav_db_wb_name
        self.logger = logger

    def collect_active_sectors(self):
        """Collect active sectors from Navitaire."""
        try:
            query = f"""
            SELECT DISTINCT DepartureStation,
            ArrivalStation
            FROM {self.nav_db_name}rez.InventoryLeg
            WHERE DepartureDate>='{datetime.now().date()}'
            AND CarrierCode='QP'
            AND FlightNumber<7000
            """
            
            self.active_sectors = pd.read_sql(query, self.nav_conn)
            self.logger.info(f"Collected {len(self.active_sectors)} active sectors")
            
            return self.active_sectors
            
        except Exception as e:
            self.logger.error(f"Error collecting active sectors data: {str(e)}")
            self.failure_reason = f"Active sectors collection failed: {str(e)}"
            raise
    
    