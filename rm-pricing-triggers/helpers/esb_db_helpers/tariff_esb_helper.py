import pandas as pd
from datetime import datetime, timedelta


class TariffEsbHelper:
    """Handles ESB (MySQL) booking data collection for the Tariff Fare Monitor.

    Booking/flown fares come from the ESB tables QP_DL_FLOWNDATAPAX and
    QP_DL_BOOKEDDATAPAX, joined with QP_DL_InventoryLeg.
    """

    def __init__(self, rm_rd_esb_conn, logger):
        self.rm_rd_esb_conn = rm_rd_esb_conn
        self.logger = logger
        self.failure_reason = None

    def collect_weekly_booking_fares(self, active_sectors, days=7):
        """Collect min/max booking fares per sector for the preceding N days.

        Pulls from both FLOWN and BOOKED pax tables, filters to QP domestic
        flights (FlightNumber < 7000) within the active sector list, and
        aggregates min/max DiscountedBaseFare_Pro per sector at the DB level
        for efficiency.

        Args:
            active_sectors: list/set of 6-char active sector codes (e.g., 'BOMDEL').
            days: number of days to look back (default 7).

        Returns:
            dict: {sector: {'min_fare': float, 'max_fare': float, 'count': int}}
        """
        try:
            active_list = list(set(active_sectors))
            if not active_list:
                self.logger.warning("No active sectors provided for weekly booking fare collection")
                return {}

            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

            # Build IN clause for sectors (CONCAT(Origin, Destination))
            sector_in = ", ".join([f"'{s}'" for s in active_list])

            # Common WHERE condition for both source tables
            where_condition = f"""
                WHERE PX.BookingDate >= '{start_date}' AND PX.BookingDate < '{end_date}'
                    AND CONCAT(PX.Origin, PX.Destination) IN ({sector_in})
                    AND PX.BookingID IS NOT NULL
                    AND IL.FlightNumber < 7000
            """

            # Aggregate min/max per sector at DB level for efficiency
            query = f"""
            SELECT
                CONCAT(Origin, Destination) AS sector,
                MIN(Fare) AS min_fare,
                MAX(Fare) AS max_fare,
                COUNT(*) AS fare_count
            FROM (
                SELECT
                    PX.DiscountedBaseFare_Pro AS Fare,
                    PX.Origin, PX.Destination
                FROM QP_DL_NAVDATA.QP_DL_FLOWNDATAPAX PX
                JOIN QP_DL_NAVDATA.QP_DL_InventoryLeg IL ON PX.InventoryLegID = IL.InventoryLegID
                {where_condition}

                UNION ALL

                SELECT
                    PX.DiscountedBaseFare_Pro AS Fare,
                    PX.Origin, PX.Destination
                FROM QP_DL_NAVDATA.QP_DL_BOOKEDDATAPAX PX
                JOIN QP_DL_NAVDATA.QP_DL_InventoryLeg IL ON PX.InventoryLegID = IL.InventoryLegID
                {where_condition}
            ) combined_bookings
            WHERE Fare IS NOT NULL AND Fare > 0
            GROUP BY CONCAT(Origin, Destination)
            """

            df = pd.read_sql(query, self.rm_rd_esb_conn)

            sector_fares = {}
            for _, row in df.iterrows():
                sector = str(row['sector']).strip()
                sector_fares[sector] = {
                    'min_fare': float(row['min_fare']),
                    'max_fare': float(row['max_fare']),
                    'count': int(row['fare_count'])
                }

            self.logger.info(
                f"Collected weekly booking fares for {len(sector_fares)} sectors "
                f"(last {days} days, {start_date} to {end_date})"
            )
            return sector_fares

        except Exception as e:
            self.logger.error(f"Error collecting weekly booking fares: {str(e)}")
            self.failure_reason = f"Weekly booking fare collection failed: {str(e)}"
            return {}
