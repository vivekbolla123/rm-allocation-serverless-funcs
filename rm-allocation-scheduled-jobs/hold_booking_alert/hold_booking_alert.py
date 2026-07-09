from .navitaire_repo import NavitaireRepository
from .database_models import HoldBookingQueryResult
from .hold_booking_email_builder import HoldBookingEmailBuilder
import logging
from typing import List
from typing import Dict
from common_utils import GraphMailClient


class HoldBookingAlert:
    def __init__(self, logger, params, overrides, context):
        self.logger = logger or logging.getLogger()
        self.navitaire_repo = NavitaireRepository(params["navconn"])

    def run(self):
        hold_bookings: List[HoldBookingQueryResult] = (
            self.navitaire_repo.fetch_hold_booking_by_inventory_leg()
        )

        if hold_bookings.count == 0:
            self.logger.info(
                "HoldBookingAlert: No hold booking found in the given range"
            )
            return

        email_builder = HoldBookingEmailBuilder()
        subject, email_html_body = email_builder.build(hold_bookings, 20, "")

        mail_client = GraphMailClient(self.logger)

        recipient: str = "jashubhai.chaudhary@akasaair.com"
        mail_client.send_email(subject, email_html_body, recipient, is_html=True)

        return
