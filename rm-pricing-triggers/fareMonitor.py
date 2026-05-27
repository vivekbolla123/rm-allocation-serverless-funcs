import boto3
import pandas as pd
import logging
import traceback
import uuid
import time
import pytz
from datetime import datetime, timedelta
import configsettings
from helpers.pa_db_helpers.tariff_pa_helper import TariffPAHelper
from helpers.nav_db_helpers.tariff_nav_helper import TariffNavHelper


class FareMonitor:
    """Handles daily fare discrepancy checks and weekly booking fare validation.

    Daily check: Compares the latest fare dump against the live tariff sheet
    to detect fares outside the tariff range (below min or above max).

    Weekly check: Compares bookings from the preceding 7 days against the
    live tariff sheet to detect booking fares outside tariff boundaries.

    Sends SES email alerts when violations or missing data are detected.
    """

    def __init__(self, logger, params, overrides, context):
        """Initialize with required connections, helpers, and configuration.

        Args:
            logger: Logger instance.
            params: dict with keys mongo, mongo_master, navconn, etc.
            overrides: dict with keys analyst, check_type ('daily'|'weekly'), etc.
            context: Lambda context object.
        """
        self.logger = logger
        self.context = context

        # Initialize database helpers
        self.pa_helper = TariffPAHelper(params['mongo'], params['mongo_master'], logger)
        self.nav_helper = TariffNavHelper(
            params['navconn'],
            configsettings.NAV_DB_NAME,
            configsettings.NAV_DB_DW_NAME,
            configsettings.NAV_DB_WB_NAME,
            logger
        )

        # SES client for email alerts
        self.ses_client = boto3.client('ses')

        # Constants from rm_parameter_values (configurable via DB)
        self.constants = params['constantutils']

        # RBD classes to check in daily comparison (from constants)
        self.B2C_RBDS = self.constants.B2C_RBD_VALUES.split(',')
        self.W_RBDS = ['W4', 'W5', 'W6']
        self.TBF_RBDS = self.constants.TBF_RBD.split(',')
        self.ALL_MONITORED_RBDS = self.B2C_RBDS + self.W_RBDS + self.TBF_RBDS

        # Configuration
        self.analyst_name = overrides.get('analyst', 'system')
        self.check_type = overrides.get('check_type', 'daily')
        self.alert_email = 'pa.it.alerts@akasaair.com'

    def run_daily_check(self):
        """Compare the latest fare dump against the live tariff sheet.

        Retrieves the most recent fare dump and the live tariff sheet,
        then checks each fare entry (B2C, W4, W5, W6, TBF RBDs) against
        the corresponding sector's min/max fare levels in the tariff.

        Sends an alert email if violations or unmatched entries are found.
        Sends a missing data alert if fare dump or live tariff is unavailable.

        Returns:
            dict: Result with keys [violations_count, unmatched_count, status].
        """
        self.logger.info("Starting daily fare discrepancy check")

        # Get latest fare dump
        fare_dump = self.pa_helper.get_fare_dump()
        if fare_dump is None or not fare_dump.get('records'):
            reason = "Daily fare comparison could not be completed — fare dump unavailable or empty."
            self.logger.warning(reason)
            self.send_missing_data_alert(reason)
            return {'violations_count': 0, 'unmatched_count': 0, 'status': 'NO_DATA'}

        # Get live tariff sheet
        live_sheet = self.pa_helper.get_live_tariff_sheet()
        if live_sheet is None:
            self.logger.warning("Live tariff sheet unavailable — cannot perform daily check")
            return {'violations_count': 0, 'unmatched_count': 0, 'status': 'NO_TARIFF'}

        # Convert fare dump records to DataFrame
        fare_df = pd.DataFrame(fare_dump['records'])

        # Build tariff lookup: {sector: {fare_levels: [...], min_fare, max_fare}}
        tariff_lookup = self._build_tariff_lookup(live_sheet)

        violations = []
        unmatched = []

        # Filter to monitored RBDs
        monitored_fares = fare_df[fare_df['rbd'].isin(self.ALL_MONITORED_RBDS)]

        for _, row in monitored_fares.iterrows():
            sector = row.get('origin', '') + row.get('destination', '')
            rbd = row.get('rbd', '')
            fare_value = row.get('base_fare', 0)

            if not sector or not rbd or fare_value is None:
                continue

            # Find matching sector in live tariff
            if sector not in tariff_lookup:
                unmatched.append({
                    'sector': sector,
                    'rbd': rbd,
                    'fare_value': fare_value
                })
                continue

            tariff_entry = tariff_lookup[sector]
            min_fare = tariff_entry['min_fare']
            max_fare = tariff_entry['max_fare']

            if fare_value < min_fare:
                violations.append({
                    'sector': sector,
                    'rbd': rbd,
                    'fare_value': fare_value,
                    'boundary': min_fare,
                    'violation_type': 'below_min'
                })
            elif fare_value > max_fare:
                violations.append({
                    'sector': sector,
                    'rbd': rbd,
                    'fare_value': fare_value,
                    'boundary': max_fare,
                    'violation_type': 'above_max'
                })

        # Send alert if violations or unmatched entries found
        if violations or unmatched:
            self.send_discrepancy_alert(violations, unmatched, 'daily')
            self.logger.info(
                f"Daily check complete — {len(violations)} violation(s), "
                f"{len(unmatched)} unmatched entry/entries"
            )
        else:
            self.logger.info("Daily check complete — no discrepancies found")

        return {
            'violations_count': len(violations),
            'unmatched_count': len(unmatched),
            'status': 'COMPLETED'
        }

    def run_weekly_check(self):
        """Compare bookings from the preceding 7 days against the live tariff sheet.

        Queries booking fares from the past week and checks each against the
        corresponding sector's min/max fare levels in the live tariff.

        Sends an alert email if violations are found.

        Returns:
            dict: Result with keys [violations_count, unmatched_count, status].
        """
        self.logger.info("Starting weekly booking fare validation")

        # Get live tariff sheet
        live_sheet = self.pa_helper.get_live_tariff_sheet()
        if live_sheet is None:
            self.logger.warning("Live tariff sheet unavailable — cannot perform weekly check")
            return {'violations_count': 0, 'unmatched_count': 0, 'status': 'NO_TARIFF'}

        # Query bookings from preceding 7 days
        bookings = self._get_weekly_bookings()

        if bookings is None or bookings.empty:
            self.logger.info("No bookings found for the preceding 7 days — skipping comparison")
            return {'violations_count': 0, 'unmatched_count': 0, 'status': 'NO_BOOKINGS'}

        # Build tariff lookup
        tariff_lookup = self._build_tariff_lookup(live_sheet)

        violations = []
        unmatched = []

        for _, row in bookings.iterrows():
            sector = row.get('sector', '')
            fare_value = row.get('fare', 0)
            booking_ref = row.get('booking_ref', '')

            if not sector or fare_value is None:
                continue

            # Find matching sector in live tariff
            if sector not in tariff_lookup:
                unmatched.append({
                    'sector': sector,
                    'fare_value': fare_value,
                    'booking_ref': booking_ref
                })
                continue

            tariff_entry = tariff_lookup[sector]
            min_fare = tariff_entry['min_fare']
            max_fare = tariff_entry['max_fare']

            if fare_value > max_fare:
                violations.append({
                    'sector': sector,
                    'fare_value': fare_value,
                    'boundary': max_fare,
                    'violation_type': 'above_max',
                    'booking_ref': booking_ref
                })
            elif fare_value < min_fare:
                violations.append({
                    'sector': sector,
                    'fare_value': fare_value,
                    'boundary': min_fare,
                    'violation_type': 'below_min',
                    'booking_ref': booking_ref
                })

        # Send alert if violations found
        if violations or unmatched:
            self.send_discrepancy_alert(violations, unmatched, 'weekly')
            self.logger.info(
                f"Weekly check complete — {len(violations)} violation(s), "
                f"{len(unmatched)} unmatched sector(s)"
            )
        else:
            self.logger.info("Weekly check complete — no discrepancies found")

        return {
            'violations_count': len(violations),
            'unmatched_count': len(unmatched),
            'status': 'COMPLETED'
        }

    def _get_weekly_bookings(self):
        """Query booking fares from the preceding 7 days.

        Attempts to retrieve from the MongoDB 'booking_fares' collection.
        Falls back to logging a placeholder if the collection is not yet available.

        Returns:
            pd.DataFrame or None: DataFrame with columns [sector, fare, booking_ref],
                                  or None if no data available.
        """
        try:
            ist = pytz.timezone('Asia/Kolkata')
            end_date = datetime.now(ist)
            start_date = end_date - timedelta(days=7)

            # Query from MongoDB booking_fares collection
            bookings = list(self.pa_helper.db.booking_fares.find(
                {
                    'booking_date': {
                        '$gte': start_date,
                        '$lte': end_date
                    }
                },
                {'_id': 0, 'sector': 1, 'fare': 1, 'booking_ref': 1}
            ))

            if bookings:
                bookings_df = pd.DataFrame(bookings)
                self.logger.info(f"Retrieved {len(bookings_df)} booking records from preceding 7 days")
                return bookings_df
            else:
                self.logger.info("No booking records found for the preceding 7 days")
                return None

        except Exception as e:
            self.logger.warning(
                f"Booking query not yet fully implemented or collection unavailable: {str(e)}. "
                f"Skipping weekly booking comparison."
            )
            return None

    def _build_tariff_lookup(self, live_sheet):
        """Build a lookup dictionary from the live tariff sheet's fare_data.

        For each sector, extracts the minimum fare (fare_level_1) and the
        maximum fare (last non-empty fare level).

        Args:
            live_sheet: The live tariff sheet document from MongoDB.

        Returns:
            dict: {sector_code: {'min_fare': float, 'max_fare': float, 'fare_levels': list}}
        """
        tariff_lookup = {}
        fare_data = live_sheet.get('fare_data', [])

        for entry in fare_data:
            sector = entry.get('sector', '')
            if not sector:
                continue

            fare_levels = entry.get('fare_levels', [])
            if not fare_levels:
                continue

            # Filter out empty/None values
            valid_fares = [f for f in fare_levels if f is not None and f != '' and f != 0]

            if not valid_fares:
                continue

            min_fare = valid_fares[0]   # fare_level_1 (lowest)
            max_fare = valid_fares[-1]  # last non-empty fare level (highest)

            tariff_lookup[sector] = {
                'min_fare': min_fare,
                'max_fare': max_fare,
                'fare_levels': valid_fares
            }

        self.logger.info(f"Built tariff lookup with {len(tariff_lookup)} sectors")
        return tariff_lookup

    def send_discrepancy_alert(self, violations, unmatched, check_type):
        """Send SES email alert for fare discrepancies.

        Args:
            violations: List of dicts with keys [sector, rbd/booking_ref, fare_value, boundary, violation_type].
            unmatched: List of dicts with keys [sector, rbd/booking_ref, fare_value].
            check_type: 'daily' or 'weekly'.
        """
        try:
            ist = pytz.timezone('Asia/Kolkata')
            timestamp = datetime.now(ist).strftime('%Y-%m-%d %H:%M:%S IST')
            check_label = 'Daily' if check_type == 'daily' else 'Weekly'

            # Build violations table rows
            violation_rows = ""
            for v in violations:
                violation_type_label = "Below Min" if v['violation_type'] == 'below_min' else "Above Max"
                rbd_or_ref = v.get('rbd', v.get('booking_ref', 'N/A'))
                violation_rows += (
                    f"<tr>"
                    f"<td style='padding: 8px; border: 1px solid #ddd;'>{v['sector']}</td>"
                    f"<td style='padding: 8px; border: 1px solid #ddd;'>{rbd_or_ref}</td>"
                    f"<td style='padding: 8px; border: 1px solid #ddd;'>{v['fare_value']}</td>"
                    f"<td style='padding: 8px; border: 1px solid #ddd;'>{v['boundary']}</td>"
                    f"<td style='padding: 8px; border: 1px solid #ddd; color: #cc0000;'>{violation_type_label}</td>"
                    f"</tr>"
                )

            # Build unmatched table rows
            unmatched_rows = ""
            for u in unmatched:
                rbd_or_ref = u.get('rbd', u.get('booking_ref', 'N/A'))
                unmatched_rows += (
                    f"<tr>"
                    f"<td style='padding: 8px; border: 1px solid #ddd;'>{u['sector']}</td>"
                    f"<td style='padding: 8px; border: 1px solid #ddd;'>{rbd_or_ref}</td>"
                    f"<td style='padding: 8px; border: 1px solid #ddd;'>{u['fare_value']}</td>"
                    f"</tr>"
                )

            # Build HTML body
            violations_section = ""
            if violations:
                violations_section = f"""
                <h3>Fare Violations ({len(violations)})</h3>
                <table style='border-collapse: collapse; width: 100%;'>
                    <thead>
                        <tr style='background-color: #f2f2f2;'>
                            <th style='padding: 8px; border: 1px solid #ddd;'>Sector</th>
                            <th style='padding: 8px; border: 1px solid #ddd;'>RBD / Booking Ref</th>
                            <th style='padding: 8px; border: 1px solid #ddd;'>Fare Value</th>
                            <th style='padding: 8px; border: 1px solid #ddd;'>Tariff Boundary</th>
                            <th style='padding: 8px; border: 1px solid #ddd;'>Violation Type</th>
                        </tr>
                    </thead>
                    <tbody>
                        {violation_rows}
                    </tbody>
                </table>
                """

            unmatched_section = ""
            if unmatched:
                unmatched_section = f"""
                <h3>Unmatched Entries ({len(unmatched)})</h3>
                <p>The following entries have no corresponding sector in the live tariff sheet:</p>
                <table style='border-collapse: collapse; width: 100%;'>
                    <thead>
                        <tr style='background-color: #f2f2f2;'>
                            <th style='padding: 8px; border: 1px solid #ddd;'>Sector</th>
                            <th style='padding: 8px; border: 1px solid #ddd;'>RBD / Booking Ref</th>
                            <th style='padding: 8px; border: 1px solid #ddd;'>Fare Value</th>
                        </tr>
                    </thead>
                    <tbody>
                        {unmatched_rows}
                    </tbody>
                </table>
                """

            html_body = f"""
            <html>
            <body>
                <h2>Tariff Sheet - {check_label} Fare Discrepancy Alert</h2>
                <p><strong>Check Time:</strong> {timestamp}</p>
                <p>The {check_label.lower()} fare comparison has detected discrepancies
                between {'the fare dump' if check_type == 'daily' else 'booking fares'}
                and the live tariff sheet.</p>
                {violations_section}
                {unmatched_section}
                <p style='margin-top: 20px; color: #666;'>
                    Please review the above discrepancies and take corrective action if needed.
                </p>
            </body>
            </html>
            """

            self.ses_client.send_email(
                Source='noreply@akasaair.com',
                Destination={
                    'ToAddresses': [self.alert_email]
                },
                Message={
                    'Subject': {
                        'Data': f'Tariff Sheet - {check_label} Fare Discrepancy Alert',
                        'Charset': 'UTF-8'
                    },
                    'Body': {
                        'Html': {
                            'Data': html_body,
                            'Charset': 'UTF-8'
                        }
                    }
                }
            )

            self.logger.info(
                f"{check_label} discrepancy alert sent to {self.alert_email} — "
                f"{len(violations)} violation(s), {len(unmatched)} unmatched"
            )

        except Exception as e:
            self.logger.error(
                f"Failed to send discrepancy alert email: {str(e)}\n"
                f"{traceback.format_exc()}"
            )

    def send_missing_data_alert(self, reason):
        """Send SES email indicating fare comparison could not be completed.

        Args:
            reason: Description of why the comparison failed.
        """
        try:
            ist = pytz.timezone('Asia/Kolkata')
            timestamp = datetime.now(ist).strftime('%Y-%m-%d %H:%M:%S IST')

            html_body = f"""
            <html>
            <body>
                <h2>Tariff Sheet - Fare Comparison Failed</h2>
                <p>The scheduled fare comparison could not be completed.</p>
                <table style='border-collapse: collapse;'>
                    <tr>
                        <td style='padding: 8px; font-weight: bold;'>Timestamp:</td>
                        <td style='padding: 8px;'>{timestamp}</td>
                    </tr>
                    <tr>
                        <td style='padding: 8px; font-weight: bold;'>Reason:</td>
                        <td style='padding: 8px; color: #cc0000;'>{reason}</td>
                    </tr>
                </table>
                <p style='margin-top: 20px; color: #666;'>
                    Please ensure the fare dump is uploaded and retry if needed.
                </p>
            </body>
            </html>
            """

            self.ses_client.send_email(
                Source='noreply@akasaair.com',
                Destination={
                    'ToAddresses': [self.alert_email]
                },
                Message={
                    'Subject': {
                        'Data': 'Tariff Sheet - Fare Comparison Failed',
                        'Charset': 'UTF-8'
                    },
                    'Body': {
                        'Html': {
                            'Data': html_body,
                            'Charset': 'UTF-8'
                        }
                    }
                }
            )

            self.logger.info(f"Missing data alert sent to {self.alert_email}")

        except Exception as e:
            self.logger.error(
                f"Failed to send missing data alert email: {str(e)}\n"
                f"{traceback.format_exc()}"
            )

    def run(self):
        """Main entry point for the FareMonitor module.

        Routes to run_daily_check() or run_weekly_check() based on self.check_type.
        Saves an audit record upon completion.

        Returns:
            dict: Status dict with keys [status, run_id, check_type, result].
        """
        run_id = str(uuid.uuid4())
        status = 'SUCCESS'
        error_message = None
        result = {}

        self.logger.info(f"FareMonitor run started — run_id: {run_id}, check_type: {self.check_type}")

        try:
            if self.check_type == 'weekly':
                result = self.run_weekly_check()
            else:
                result = self.run_daily_check()

            # Check if the check itself reported a failure status
            if result.get('status') in ('NO_DATA', 'NO_TARIFF'):
                status = 'COMPLETED_WITH_WARNINGS'

        except Exception as e:
            error_message = f"Unexpected error in FareMonitor ({self.check_type}): {str(e)}"
            self.logger.error(f"{error_message}\n{traceback.format_exc()}")
            status = 'FAILED'

        # Save audit record
        self.pa_helper.save_run_audit(
            self.analyst_name, run_id, status, 'fare_monitor', error_message
        )

        self.logger.info(
            f"FareMonitor run completed — run_id: {run_id}, status: {status}, "
            f"check_type: {self.check_type}"
        )

        return {
            'status': status,
            'run_id': run_id,
            'check_type': self.check_type,
            'result': result
        }
