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
from .service_models import HoldBookingConfiguration
import configsettings


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

        config = self.parse_hold_booking_configuration()

        datetime_ist: str = datetime.now(ZoneInfo("Asia/Kolkata")).strftime(
            "%Y-%m-%d %H:%M:%S IST"
        )

        hold_bookings: List[HoldBookingQueryResult] = (
            self.navitaire_repo.fetch_hold_booking_by_inventory_leg(
                configsettings.NAV_DB_WB_NAME,
                config.alert_ndo,
                config.alert_threshold,
            )
        )

        if not hold_bookings:
            self.logger.info(
                "HoldBookingAlert: No hold booking found in the given range"
            )
            return

        email_builder = HoldBookingEmailBuilder()
        email_html_body = email_builder.build(
            hold_bookings, config.alert_threshold, datetime_ist
        )

        mail_client = GraphMailClient(self.logger)

        # recipients = [r.strip() for r in config.email_recipients.split(",")]
        # for recipient in recipients:
        mail_client.send_email(
            config.email_subject,
            email_html_body,
            config.email_recipients,
            is_html=True,
            attachments=None,
            cc=config.email_cc_recipients
        )

        return

    def parse_hold_booking_configuration(self) -> HoldBookingConfiguration:
        config = self.constants.HOLD_BOOKING_ALERT_CONFIGURATION
        return HoldBookingConfiguration(
            email_subject=config["email_subject"],
            alert_ndo=config["alert_ndo"],
            alert_threshold=config["alert_threshold"],
            email_recipients=config["email_recipients"],
            email_cc_recipients=config["email_cc"],
        )


""""
{
    "email_subject": "Hold Booking Alert",
    "email_recipients": "jashubhai.chaudhary@akasaair.com",
    "alert_threshold": 20,
    "alert_ndo": 7
}
"""
