import pandas as pd
from datetime import datetime, timedelta, date

class LadderEsbHelper:
    """Handles Navitaire-related database operations."""

    def __init__(self, rm_rd_esb_conn, logger):
        self.rm_rd_esb_conn = rm_rd_esb_conn
        self.logger = logger

    def collect_combined_booking_data(self, start_date, end_date, sector):
        """Optimized single query for both flown and booked data."""
        try:
            wherecondition = f"""WHERE PX.BookingDate >= '{start_date}' AND PX.BookingDate < '{end_date}'
                    AND SUBSTRING(PX.FareBasis, 5, 1) = 'R'
                    AND LEFT(PX.FareBasis, 1) NOT IN ('S', 'W')
                    AND CONCAT(PX.Origin, PX.Destination) = '{sector}'
                    AND PX.BookingID IS NOT NULL
                    AND IL.FlightNumber < 7000"""
                    
            query = f"""
            SELECT Fare, FareBasis, BookingDate, Origin, Destination, data_source
            FROM (
                SELECT
                    (PX.DiscountedBaseFare_Pro + PX.YQ_Pro + PX.RCS + PX.CUTE + PX.SGST + PX.UGST + PX.IGST + PX.CGST + PX.AirportCharges + PX.DUDF) AS Fare,
                    PX.FareBasis, PX.BookingDate, PX.Origin, PX.Destination, 'FLOWN' as data_source
                FROM QP_DL_NAVDATA.QP_DL_FLOWNDATAPAX PX
                JOIN QP_DL_NAVDATA.QP_DL_InventoryLeg IL ON PX.InventoryLegID = IL.InventoryLegID
                {wherecondition}
                    
                UNION ALL
                
                SELECT
                    (PX.DiscountedBaseFare_Pro + PX.YQ_Pro + PX.RCS + PX.CUTE + PX.SGST + PX.UGST + PX.IGST + PX.CGST + PX.AirportCharges + PX.DUDF) AS Fare,
                    PX.FareBasis, PX.BookingDate, PX.Origin, PX.Destination, 'BOOKED' as data_source
                FROM QP_DL_NAVDATA.QP_DL_BOOKEDDATAPAX PX
                JOIN QP_DL_NAVDATA.QP_DL_InventoryLeg IL ON PX.InventoryLegID = IL.InventoryLegID
                {wherecondition}
            ) combined_bookings
            ORDER BY Fare ASC
            """
            
            combined_data = pd.read_sql(query, self.rm_rd_esb_conn)
            
            return combined_data
            
        except Exception as e:
            self.logger.error(f"Error collecting booking data: {str(e)}")
            self.failure_reason = f"Booking data collection failed: {str(e)}"
            raise