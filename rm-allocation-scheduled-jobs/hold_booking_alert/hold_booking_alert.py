from .navitaire_repo import NavitaireRepository
from .database_models import HoldBookingQueryResult
from .hold_booking_email_builder import HoldBookingEmailBuilder
import logging
from typing import List
from typing import Dict
from common_utils import GraphMailClient
from constants_utils import ConstantsUtils
from datetime import datetime
from zoneinfo import ZoneInfo


class HoldBookingAlert:
    def __init__(self, logger, params, overrides, context):
        self.logger = logger or logging.getLogger()
        self.navitaire_repo = NavitaireRepository(params["navconn"])
        self.mongoconn = params["mongoconn"]
        self.mongo_de_db = params["mongo_de_db"]

        # Application MongoDB database
        self.db = self.mongoconn["qp_dw_rmalloc"]

        # ConstantsUtils for system parameters
        self.constants = ConstantsUtils(self.db)

    def run(self):

        datetime_ist: str = datetime.now(ZoneInfo("Asia/Kolkata")).strftime(
            "%Y-%m-%d %H:%M:%S IST"
        )

        hold_bookings: List[HoldBookingQueryResult] = (
            self.navitaire_repo.fetch_hold_booking_by_inventory_leg(
                self.constants.HOLD_BOOKING_ALERT_NDO,
                self.constants.HOLD_BOOKING_ALERT_THRESHOLD,
            )
        )

        if hold_bookings.count == 0:
            self.logger.info(
                "HoldBookingAlert: No hold booking found in the given range"
            )
            return

        email_builder = HoldBookingEmailBuilder()
        email_html_body = email_builder.build(
            hold_bookings, self.constants.HOLD_BOOKING_ALERT_THRESHOLD, datetime_ist
        )

        mail_client = GraphMailClient(self.logger)

        recipients = [
            r.strip() for r in self.constants.HOLD_BOOKING_EMAIL_RECIPIENTS.split(",")
        ]
        for recipient in recipients:
            mail_client.send_email(
                self.constants.HOLD_BOOKING_EMAIL_SUBJECT,
                email_html_body,
                recipient,
                is_html=True,
            )

        return
