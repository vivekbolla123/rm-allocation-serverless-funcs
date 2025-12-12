import pandas as pd
from datetime import datetime, timedelta, date

class LadderNavHelper:
    """Handles Navitaire-related database operations."""

    def __init__(self, nav_conn, nav_db_name, nav_db_dw_name, nav_db_wb_name, logger):
        self.nav_conn = nav_conn
        self.nav_db_name = 'REZ0KOD01.'
        self.nav_db_dw_name = 'REZ0KDW01.'
        self.nav_db_wb_name = 'REZ0KWB01.'
        self.logger = logger
        
        self.failure_reason = None

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