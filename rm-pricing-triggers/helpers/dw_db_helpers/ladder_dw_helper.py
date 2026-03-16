import pandas as pd
from datetime import datetime, timedelta, date

class LadderDWHelper:
    """Handles dw-related database operations."""

    def __init__(self, dw_conn, logger):
        self.dw_conn = dw_conn
        self.logger = logger

    def fetch_all_conversion_data(self):
        """Fetch all currency conversion rates."""
        query = f"SELECT FromCurrencyCode, ToCurrencyCode, ConversionRate FROM currency_conversion"
        df = pd.read_sql(query, self.dw_conn)
        return df
    
    def get_conversion_rate(self, from_currency, to_currency):
        """Get conversion rate from one currency to another."""
        try:
            query = f"""
                SELECT ConversionRate 
                FROM currency_conversion 
                WHERE FromCurrencyCode = '{from_currency}' 
                AND ToCurrencyCode = '{to_currency}'
            """
            result = pd.read_sql(query, self.dw_conn)
            
            if result.empty:
                self.logger.warning(f"No conversion rate found for {from_currency} to {to_currency}")
                return None
            
            conversion_rate = float(result.iloc[0]['ConversionRate'])
            self.logger.info(f"Conversion rate {from_currency} -> {to_currency}: {conversion_rate}")
            return conversion_rate
            
        except Exception as e:
            self.logger.error(f"Error fetching conversion rate: {str(e)}")
            return None

    def get_city_pair_distance(self, origin, destination):
        """Get distance (in km) between two stations from the DW CityPair table.
        
        Checks forward direction first, then reverse if not found.
        Returns distance as float, or None if not found.
        """
        try:
            query = f"""
                SELECT ActualDistance 
                FROM QP_DWH_MASTER.CityPair 
                WHERE DepartureStation = '{origin}' 
                AND ArrivalStation = '{destination}'
            """
            result = pd.read_sql(query, self.dw_conn)
            
            if not result.empty:
                distance = float(result.iloc[0]['ActualDistance'])
                self.logger.info(f"City pair distance {origin}-{destination}: {distance} km")
                return distance
            
            # Try reverse direction
            query_reverse = f"""
                SELECT ActualDistance 
                FROM QP_DWH_MASTER.citypair 
                WHERE DepartureStation = '{destination}' 
                AND ArrivalStation = '{origin}'
            """
            result_reverse = pd.read_sql(query_reverse, self.dw_conn)
            
            if not result_reverse.empty:
                distance = float(result_reverse.iloc[0]['ActualDistance'])
                self.logger.info(f"City pair distance {origin}-{destination} (reverse lookup): {distance} km")
                return distance
            
            self.logger.warning(f"No city pair distance found for {origin}-{destination}")
            return None
            
        except Exception as e:
            self.logger.error(f"Error fetching city pair distance: {str(e)}")
            return None