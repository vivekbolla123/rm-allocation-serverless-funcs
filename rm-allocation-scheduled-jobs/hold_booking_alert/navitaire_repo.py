from .database_models import HoldBookingQueryResult
from typing import List


class NavitaireRepository:
    def __init__(self, engine: any):
        self.engine = engine

    def fetch_hold_booking_by_inventory_leg(self) -> List[HoldBookingQueryResult]:
        sqlQuery = f"""
                EXEC [TST0KWB01].[dbo].[SP_GetHoldBookingData]
                """

        with self.engine.connect() as conn:

            result = conn.execute(sqlQuery)
            rows = result.fetchall()

            return [
                HoldBookingQueryResult(
                    inventory_leg_key=row.InventoryLegKey,
                    class_of_service=row.ClassOfService,
                    booking_on_hold=row.BookingsOnHold,
                    booking_on_hold_percentage=row.BookingsOnHoldPercentage,
                    total_booking=row.TotalBookings,
                )
                for row in rows
            ]
