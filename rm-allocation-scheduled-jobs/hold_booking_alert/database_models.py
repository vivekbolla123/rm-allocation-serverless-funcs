from dataclasses import dataclass
from datetime import datetime

@dataclass
class HoldBookingQueryResult:
    """Class for query output (mapping database rows)"""
    inventory_leg_key: str
    class_of_service: str
    total_booking: int
    booking_on_hold: int
    booking_on_hold_percentage: float