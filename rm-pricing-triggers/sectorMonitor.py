import pandas as pd
import logging
import traceback
import uuid
import time
import pytz
from datetime import datetime
import configsettings
from common_utils import GraphMailClient
from helpers.pa_db_helpers.tariff_pa_helper import TariffPAHelper
from helpers.nav_db_helpers.tariff_nav_helper import TariffNavHelper


class SectorMonitor:
    """Handles scheduled refresh of the active sector list from Navitaire
    and detection of new sectors not yet in the live tariff sheet.

    Scheduled to run 3x daily at 08:00, 13:00, 18:00 IST.
    Sends SES email alerts for new sectors or failures.
    """

    def __init__(self, logger, params, overrides, context):
        """Initialize with required connections, helpers, and configuration.

        Args:
            logger: Logger instance.
            params: dict with keys mongo, mongo_master, navconn, etc.
            overrides: dict with keys analyst, etc.
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

        # Email client for alerts (Microsoft Graph API)
        self.mail_client = GraphMailClient(logger)

        # Configuration
        self.analyst_name = overrides.get('analyst', 'system')
        self.constants = params['constantutils']
        self.alert_email = self.constants.TARIFF_ALERT_MAIL_ID
        self.timeout_seconds = 300  # 5 minutes

    def compose_active_sectors(self):
        """Compose the active sector list from Navitaire inventory and manual sectors.

        Queries Navitaire for direct sectors (stops=0) and through segments (stops=1),
        merges with manually maintained sectors, and deduplicates.

        Returns:
            pd.DataFrame: Combined sector DataFrame with columns
                          [sector, origin, destination, stops, source],
                          or None if Navitaire is unavailable (signals failure).
        """
        try:
            # Step 1: Get direct sectors from InventoryLeg (stops=0)
            direct_df = self.nav_helper.get_direct_sectors()
            direct_df = direct_df.rename(columns={
                'DepartureStation': 'origin',
                'ArrivalStation': 'destination'
            })
            direct_df['stops'] = 0
            direct_df['source'] = 'navitaire'
            direct_df['sector'] = direct_df['origin'] + direct_df['destination']
            self.logger.info(f"Composed {len(direct_df)} direct sectors from Navitaire")

            # Step 2: Get through segments (stops=1)
            through_df = self.nav_helper.get_through_segments()
            through_df = through_df.rename(columns={
                'DepartureStation': 'origin',
                'ArrivalStation': 'destination'
            })
            through_df['stops'] = 1
            through_df['source'] = 'navitaire'
            through_df['sector'] = through_df['origin'] + through_df['destination']
            self.logger.info(f"Composed {len(through_df)} through segments from Navitaire")

            # Step 3: Combine direct and through into single DataFrame
            nav_sectors_df = pd.concat([direct_df, through_df], ignore_index=True)
            nav_sectors_df = nav_sectors_df[['sector', 'origin', 'destination', 'stops', 'source']]

            # Step 4: Get manual sectors (always preserved)
            manual_df = self.pa_helper.get_manual_sectors()

            # Step 5: Merge — manual sectors take precedence on duplicates
            if manual_df is not None and not manual_df.empty:
                # Ensure manual_df has required columns
                manual_df = manual_df[['sector', 'origin', 'destination', 'stops', 'source']]

                # Remove navitaire entries that conflict with manual sectors
                nav_sectors_df = nav_sectors_df[
                    ~nav_sectors_df['sector'].isin(manual_df['sector'])
                ]

                # Combine: manual sectors first, then navitaire sectors
                combined_df = pd.concat([manual_df, nav_sectors_df], ignore_index=True)
            else:
                combined_df = nav_sectors_df

            # Step 6: Deduplicate by sector code (keep first occurrence)
            combined_df = combined_df.drop_duplicates(subset='sector', keep='first')
            combined_df = combined_df.reset_index(drop=True)

            self.logger.info(
                f"Active sector list composed: {len(combined_df)} total sectors "
                f"({len(direct_df)} direct, {len(through_df)} through, "
                f"{len(manual_df) if manual_df is not None and not manual_df.empty else 0} manual)"
            )

            return combined_df

        except Exception as e:
            # Navitaire unavailable — retain previous list, log error, return None
            self.logger.error(
                f"Failed to compose active sectors from Navitaire: {str(e)}\n"
                f"{traceback.format_exc()}"
            )
            return None

    def detect_new_sectors(self, current_sectors_df):
        """Detect sectors in the active list that are not in the live tariff sheet.

        Args:
            current_sectors_df: DataFrame of current active sectors.

        Returns:
            list: List of dicts with keys [sector, origin, destination, stops]
                  for each new sector not found in the live tariff sheet.
        """
        try:
            # Get live tariff sheet
            live_sheet = self.pa_helper.get_live_tariff_sheet()

            if live_sheet is None:
                self.logger.warning("No live tariff sheet found — all sectors considered new")
                return current_sectors_df[
                    ['sector', 'origin', 'destination', 'stops']
                ].to_dict('records')

            # Extract sectors from live tariff sheet's fare_data
            tariff_sectors = set()
            fare_data = live_sheet.get('fare_data', [])
            for entry in fare_data:
                sector_code = entry.get('sector', '')
                if sector_code:
                    tariff_sectors.add(sector_code)

            self.logger.info(f"Live tariff sheet contains {len(tariff_sectors)} sectors")

            # Find sectors in current list that are NOT in tariff
            current_sector_codes = set(current_sectors_df['sector'].tolist())
            new_sector_codes = current_sector_codes - tariff_sectors

            if not new_sector_codes:
                self.logger.info("No new sectors detected")
                return []

            # Build list of new sector details
            new_sectors = current_sectors_df[
                current_sectors_df['sector'].isin(new_sector_codes)
            ][['sector', 'origin', 'destination', 'stops']].to_dict('records')

            self.logger.info(f"Detected {len(new_sectors)} new sectors not in live tariff sheet")
            return new_sectors

        except Exception as e:
            self.logger.error(f"Error detecting new sectors: {str(e)}\n{traceback.format_exc()}")
            return []

    def send_new_sector_alert(self, new_sectors):
        """Send SES email alert for newly detected sectors.

        Args:
            new_sectors: List of dicts with keys [sector, origin, destination, stops].
        """
        try:
            ist = pytz.timezone('Asia/Kolkata')
            timestamp = datetime.now(ist).strftime('%Y-%m-%d %H:%M:%S IST')

            # Build HTML body
            sector_rows = ""
            for s in new_sectors:
                stops_label = "Direct" if s['stops'] == 0 else "1 Stop"
                sector_rows += (
                    f"<tr>"
                    f"<td style='padding: 8px; border: 1px solid #ddd;'>{s['sector']}</td>"
                    f"<td style='padding: 8px; border: 1px solid #ddd;'>{s['origin']} - {s['destination']}</td>"
                    f"<td style='padding: 8px; border: 1px solid #ddd;'>{stops_label}</td>"
                    f"</tr>"
                )

            html_body = f"""
            <html>
            <body>
                <h2>Tariff Sheet - New Sectors Detected</h2>
                <p>The following <strong>{len(new_sectors)}</strong> new sector(s) were detected
                in the active sector list that are not present in the current live tariff sheet.</p>
                <p><strong>Detection Time:</strong> {timestamp}</p>
                <table style='border-collapse: collapse; width: 100%;'>
                    <thead>
                        <tr style='background-color: #f2f2f2;'>
                            <th style='padding: 8px; border: 1px solid #ddd;'>Sector Code</th>
                            <th style='padding: 8px; border: 1px solid #ddd;'>Origin - Destination</th>
                            <th style='padding: 8px; border: 1px solid #ddd;'>Stops</th>
                        </tr>
                    </thead>
                    <tbody>
                        {sector_rows}
                    </tbody>
                </table>
                <p style='margin-top: 20px; color: #666;'>
                    Please update the live tariff sheet to include these sectors.
                </p>
            </body>
            </html>
            """

            self.mail_client.send_email(
                'Tariff Sheet - New Sectors Detected',
                html_body,
                self.alert_email,
                is_html=True
            )

            self.logger.info(
                f"New sector alert sent to {self.alert_email} "
                f"for {len(new_sectors)} sector(s)"
            )

        except Exception as e:
            self.logger.error(
                f"Failed to send new sector alert email: {str(e)}\n"
                f"{traceback.format_exc()}"
            )

    def send_failure_alert(self, error_message):
        """Send SES email alert for sector update failure.

        Args:
            error_message: Description of the failure.
        """
        try:
            ist = pytz.timezone('Asia/Kolkata')
            timestamp = datetime.now(ist).strftime('%Y-%m-%d %H:%M:%S IST')

            html_body = f"""
            <html>
            <body>
                <h2>Tariff Sheet - Sector Update Failed</h2>
                <p>The scheduled active sector list update has failed.</p>
                <table style='border-collapse: collapse;'>
                    <tr>
                        <td style='padding: 8px; font-weight: bold;'>Timestamp:</td>
                        <td style='padding: 8px;'>{timestamp}</td>
                    </tr>
                    <tr>
                        <td style='padding: 8px; font-weight: bold;'>Error:</td>
                        <td style='padding: 8px; color: #cc0000;'>{error_message}</td>
                    </tr>
                </table>
                <p style='margin-top: 20px; color: #666;'>
                    Please investigate and retry the sector update manually if needed.
                </p>
            </body>
            </html>
            """

            self.mail_client.send_email(
                'Tariff Sheet - Sector Update Failed',
                html_body,
                self.alert_email,
                is_html=True
            )

            self.logger.info(f"Failure alert sent to {self.alert_email}")

        except Exception as e:
            self.logger.error(
                f"Failed to send failure alert email: {str(e)}\n"
                f"{traceback.format_exc()}"
            )

    def run(self):
        """Main entry point for the SectorMonitor module.

        Executes the scheduled sector update workflow:
        1. Compose active sectors from Navitaire + manual sources
        2. Save to MongoDB if composition succeeded
        3. Detect new sectors not in live tariff sheet
        4. Send alerts as appropriate (new sectors or failures)
        5. Save audit record

        Returns:
            dict: Status dict with keys [status, run_id, new_sectors_count, error].
        """
        run_id = str(uuid.uuid4())
        start_time = time.time()
        status = 'SUCCESS'
        error_message = None
        new_sectors_count = 0

        self.logger.info(f"SectorMonitor run started — run_id: {run_id}")

        try:
            # Step 1: Compose active sectors
            sectors_df = self.compose_active_sectors()

            # Check timeout
            elapsed = time.time() - start_time
            if elapsed > self.timeout_seconds:
                error_message = (
                    f"Sector update timed out after {elapsed:.1f} seconds "
                    f"(limit: {self.timeout_seconds}s)"
                )
                self.logger.error(error_message)
                self.send_failure_alert(error_message)
                status = 'FAILED'
                self.pa_helper.save_run_audit(
                    self.analyst_name, run_id, status, 'sector_monitor', error_message
                )
                return {
                    'status': status,
                    'run_id': run_id,
                    'new_sectors_count': 0,
                    'error': error_message
                }

            if sectors_df is not None:
                # Step 2: Save composed sectors to MongoDB
                self.pa_helper.save_active_sectors(sectors_df)
                self.logger.info(f"Saved {len(sectors_df)} active sectors to MongoDB")

                # Step 3: Detect new sectors not in live tariff sheet
                new_sectors = self.detect_new_sectors(sectors_df)
                new_sectors_count = len(new_sectors)

                # Step 4: Send alert if new sectors detected
                if new_sectors:
                    self.send_new_sector_alert(new_sectors)
                    self.logger.info(f"Alert sent for {new_sectors_count} new sector(s)")
                else:
                    self.logger.info("No new sectors detected — no alert needed")

            else:
                # Composition failed (Navitaire unavailable)
                error_message = "Active sector composition failed — Navitaire data source unavailable"
                self.logger.error(error_message)
                self.send_failure_alert(error_message)
                status = 'FAILED'

        except Exception as e:
            error_message = f"Unexpected error in SectorMonitor: {str(e)}"
            self.logger.error(f"{error_message}\n{traceback.format_exc()}")
            self.send_failure_alert(error_message)
            status = 'FAILED'

        # Step 5: Save audit record
        self.pa_helper.save_run_audit(
            self.analyst_name, run_id, status, 'sector_monitor', error_message
        )

        elapsed_total = time.time() - start_time
        self.logger.info(
            f"SectorMonitor run completed — run_id: {run_id}, status: {status}, "
            f"new_sectors: {new_sectors_count}, elapsed: {elapsed_total:.1f}s"
        )

        return {
            'status': status,
            'run_id': run_id,
            'new_sectors_count': new_sectors_count,
            'error': error_message
        }
