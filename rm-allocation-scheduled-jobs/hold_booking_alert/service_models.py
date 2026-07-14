from dataclasses import dataclass


@dataclass
class HoldBookingConfiguration:
    """Class for query output (mapping database rows)"""

    email_subject: str
    email_recipients: list[str]
    email_cc_recipients: list[str]
    alert_threshold: int
    alert_ndo: int
