import pandas as pd
from datetime import datetime, timedelta, date

class LadderNavHelper:
    """Handles Navitaire-related database operations."""

    def __init__(self, nav_conn, nav_db_name, nav_db_dw_name, nav_db_wb_name, logger):
        self.nav_conn = nav_conn
        self.nav_db_name = nav_db_name
        self.nav_db_dw_name = nav_db_dw_name
        self.nav_db_wb_name = nav_db_wb_name
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
        
    def collect_booked_data(self, date_range_days=30):
        """Collect passenger booking data from Navitaire stored procedure for PLF calculation."""
        try:
            start_date = date.today()
            end_date = start_date + timedelta(days=date_range_days)
            
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')
            
            query = f"""
            EXEC {self.nav_db_wb_name}[dbo].[SP_Bookeddata]
            @DepartureDate_from = N'{start_date_str}',
            @DepartureDate_to = N'{end_date_str}'
            """
            
            booked_data = pd.read_sql(query, self.nav_conn)
            
            return booked_data
            
        except Exception as e:
            self.logger.error(f"Error collecting booked data: {str(e)}")
            # Booked data is not critical for ladder generation - just log and continue
            self.logger.warning("Booked data unavailable but not critical for ladder generation")
            self.booked_data = pd.DataFrame()
            return self.booked_data