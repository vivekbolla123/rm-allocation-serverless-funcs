import pandas as pd
import logging
import traceback
import uuid
import time
import pytz
import io
from datetime import datetime, timedelta
import configsettings
from common_utils import GraphMailClient
from constants_utils import ConstantsUtils
from helpers.pa_db_helpers.tariff_pa_helper import TariffPAHelper
from helpers.nav_db_helpers.tariff_nav_helper import TariffNavHelper
from helpers.esb_db_helpers.tariff_esb_helper import TariffEsbHelper


class FareMonitor:
    """Handles daily fare discrepancy checks and weekly fare validation.

    Daily check: Compares the latest fares (qp_fares) against the live tariff
    sheet to detect fares outside the tariff range (below min or above max).

    Weekly check: Compares the output summary selling fares (run_summary /
    run_summary_connections) AND bookings (ESB) from the preceding 7 days
    against the live tariff sheet boundaries.

    Sends email alerts (via Microsoft Graph) when violations or missing data
    are detected.
    """

    def __init__(self, logger, params, overrides, context):
        """Initialize with required connections, helpers, and configuration.

        Args:
            logger: Logger instance.
            params: dict with keys mongo, mongo_master, navconn, rdEsbConn, etc.
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
        self.esb_helper = TariffEsbHelper(params['rdEsbConn'], logger)

        # Email client for alerts (Microsoft Graph API)
        self.mail_client = GraphMailClient(logger)

        # Constants from rm_parameter_values (configurable via DB)
        self.constants = ConstantsUtils(params['mongo'])

        # RBD classes to check in daily comparison (from constants)
        self.B2C_RBDS = self.constants.B2C_RBD_VALUES.split(',')
        self.W_RBDS = self.constants.W_RBD.split(',')
        self.TBF_RBDS = self.constants.TBF_RBD.split(',')
        self.ALL_MONITORED_RBDS = self.B2C_RBDS + self.W_RBDS + self.TBF_RBDS

        # Configuration
        self.analyst_name = overrides.get('analyst', 'system')
        self.check_type = overrides.get('check_type', 'daily')
        self.alert_email = self.constants.TARIFF_ALERT_MAIL_ID
        self.tax_master_dom_data = None
        self.tax_master_intl_data = None
        self.city_pair_data = None

    def collect_all_data(self):
        success_count = 0
        try:
            self.tax_master_dom_data, self.tax_master_intl_data, failure_reason = self.pa_helper.collect_tax_master_data()
            if failure_reason:
                self.failure_reason = failure_reason
            else:
                success_count += 1
        except Exception as e:
            self.logger.error(f"Tax master data collection failed: {str(e)}")
            if not self.failure_reason:
                self.failure_reason = f"Tax master data collection failed: {str(e)}"
        
        # Non-critical data collections
            try:
                self.city_pair_data = self.nav_helper.collect_city_pair_data(self.sector)
                success_count += 1
            except Exception as e:
                self.logger.error(f"City pair data collection failed (non-critical): {str(e)}")
            
            # Collect distance-based fare cap (DOM only)
            try:
                if self.route == 'D':
                    origin = self.sector[:3]
                    destination = self.sector[3:]
                    # Get distance from Navitaire CityPair data collected above
                    sector_distance = None
                    if self.city_pair_data is not None and not self.city_pair_data.empty:
                        # Find matching forward sector
                        match = self.city_pair_data[
                            (self.city_pair_data['DepartureStation'] == origin) & 
                            (self.city_pair_data['ArrivalStation'] == destination)
                        ]
                        if not match.empty:
                            sector_distance = float(match.iloc[0]['ActualDistance'])
                        else:
                            # Try reverse sector as fallback
                            reverse_match = self.city_pair_data[
                                (self.city_pair_data['DepartureStation'] == destination) & 
                                (self.city_pair_data['ArrivalStation'] == origin)
                            ]
                            if not reverse_match.empty:
                                sector_distance = float(reverse_match.iloc[0]['ActualDistance'])
                    
                    if sector_distance is not None:
                        gross_fare_cap = self._get_fare_cap_for_distance(sector_distance)
                        # Fare cap includes base fare + RCS + CUTE
                        # For bidirectional consistency, average both origins' RCS/CUTE
                        rcs_fwd = cute_fwd = rcs_rev = cute_rev = 0
                        if self.tax_master_dom_data is not None and not self.tax_master_dom_data.empty:
                            # Forward origin taxes
                            fwd_tax = self.tax_master_dom_data[self.tax_master_dom_data['Code'] == origin]
                            if not fwd_tax.empty:
                                rcs_fwd = float(fwd_tax.iloc[0].get('RCS', 0) or 0)
                                cute_fwd = float(fwd_tax.iloc[0].get('CUTE', 0) or 0)
                            # Reverse origin taxes
                            rev_tax = self.tax_master_dom_data[self.tax_master_dom_data['Code'] == destination]
                            if not rev_tax.empty:
                                rcs_rev = float(rev_tax.iloc[0].get('RCS', 0) or 0)
                                cute_rev = float(rev_tax.iloc[0].get('CUTE', 0) or 0)
                            self.logger.info(f"Tax deductions - {origin}: RCS={rcs_fwd}, CUTE={cute_fwd}; {destination}: RCS={rcs_rev}, CUTE={cute_rev}")
                        
                        # Average both origins for bidirectional consistency
                        avg_rcs = (rcs_fwd + rcs_rev) / 2
                        avg_cute = (cute_fwd + cute_rev) / 2
                        self.fare_cap_value = gross_fare_cap - avg_rcs - avg_cute
                        self.logger.info(f"Distance-based fare cap for {self.sector}: "
                                       f"distance={sector_distance} km, gross_cap={gross_fare_cap}, "
                                       f"avg_RCS={avg_rcs}, avg_CUTE={avg_cute}, "
                                       f"effective_base_fare_cap={self.fare_cap_value}")
                    else:
                        self.fare_cap_value = None
                        self.logger.warning(f"No distance found for sector {self.sector}, fare cap not applied")
                else:
                    # No fare cap for international routes
                    self.fare_cap_value = None
                    self.logger.info(f"Fare cap not applied for international route {self.sector}")
            except Exception as e:
                self.logger.error(f"Fare cap distance lookup failed (non-critical): {str(e)}")
                self.fare_cap_value = None

    def run_daily_check(self):
        """Compare the latest fares (qp_fares) against the live tariff sheet.

        Retrieves monitored fares from qp_fares (the "fare dump") for active
        sectors and checks each fare (B2C, W4, W5, W6, TBF RBDs) against the
        corresponding sector's min/max in the live tariff sheet.

        Sends an alert email if violations or unmatched entries are found.

        Returns:
            dict: Result with keys [violations_count, unmatched_count, status].
        """
        self.logger.info("Starting daily fare discrepancy check")

        # Get live tariff sheet
        live_sheet = self.pa_helper.get_live_tariff_sheet()
        if live_sheet is None:
            self.logger.warning("Live tariff sheet unavailable — cannot perform daily check")
            return {'violations_count': 0, 'unmatched_count': 0, 'status': 'NO_TARIFF'}

        # Get active sectors
        active_sectors_df = self.pa_helper.get_active_sectors()
        if active_sectors_df is None or active_sectors_df.empty:
            self.logger.warning("No active sectors — cannot perform daily check")
            return {'violations_count': 0, 'unmatched_count': 0, 'status': 'NO_SECTORS'}

        active_sector_codes = active_sectors_df['sector'].tolist()

        # Get monitored fares from qp_fares (the "fare dump")
        fare_df = self.pa_helper.get_fares_for_monitor(
            active_sector_codes, self.ALL_MONITORED_RBDS, self.B2C_RBDS
        )

        if fare_df is None or fare_df.empty:
            reason = "Daily fare comparison could not be completed — no fares found in qp_fares for active sectors."
            self.logger.warning(reason)
            self.send_missing_data_alert(reason)
            return {'violations_count': 0, 'unmatched_count': 0, 'status': 'NO_DATA'}

        # Build tariff lookup: {sector: {fare_levels: [...], min_fare, max_fare}}
        tariff_lookup = self._build_tariff_lookup(live_sheet)

        violations = []
        unmatched = []

        for _, row in fare_df.iterrows():
            sector = row.get('sector', '')
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
    
    def _get_distance(self, origin, destination):
        """Get distance for sector."""
        # Collect city pair data (needed for distance-based tax calculations)
        try:
            self.city_pair_data = self.nav_helper.collect_city_pair_data(self.sector)
        except Exception as e:
            self.logger.warning(f"City pair data collection failed: {str(e)}")
        try:
            if self.city_pair_data is None or self.city_pair_data.empty:
                return 0
            distance_data = self.city_pair_data[
                (self.city_pair_data['DepartureStation'] == origin) &
                (self.city_pair_data['ArrivalStation'] == destination)
            ]
            return float(distance_data.iloc[0]['ActualDistance']) if not distance_data.empty else 0
        except Exception:
            return 0

    def _get_yq_for_distance(self, distance):
        """Get YQ based on distance (km) for domestic routes from config."""
        if distance <= 0:
            return 0
            
        try:
            for min_dist, max_dist, yq_val in self.constants.YQ_DISTANCE_CONFIG:
                if min_dist < distance <= max_dist:
                    return float(yq_val)
        except AttributeError:
            self.logger.warning("YQ_DISTANCE_CONFIG not found in constants.")
            
        return 0

    def _calculate_taxes_domestic(self, base_fare, origin, destination):
        """Calculate all taxes for domestic fare to get all-in fare."""
        try:
            if self.tax_master_dom_data is None or self.tax_master_dom_data.empty:
                return 0
                
            tax_data = self.tax_master_dom_data[self.tax_master_dom_data['Code'] == origin]
            if tax_data.empty:
                return 0
            
            tax_row = tax_data.iloc[0]
            gst_rate = float(tax_row.get('GST', 0)) / 100
            rcs = float(tax_row.get('RCS', 0)) if pd.notna(tax_row.get('RCS')) else 0
            cute = float(tax_row.get('CUTE', 0)) if pd.notna(tax_row.get('CUTE')) else 0
            asf = float(tax_row.get('ASF', 0)) if pd.notna(tax_row.get('ASF')) else 0
            psf = float(tax_row.get('PSF', 0)) if pd.notna(tax_row.get('PSF')) else 0
            udf_df = float(tax_row.get('UDF_DF', 0)) if pd.notna(tax_row.get('UDF_DF')) else 0
            
            # Apply special handling for RCS sectors
            if destination in self.constants.PRICING_RCS_SECTORS:
                rcs = 0
            
            # Add DUF for specific sectors
            duf = 0
            if destination in self.constants.PRICING_DUF_SECTORS:
                duf = float(self.constants.PRICING_DUF_SECTORS[destination])
            
            # Calculate GST
            if origin in self.constants.PRICING_GST_SECTORS or destination in self.constants.PRICING_GST_SECTORS:
                gst = 0
            else:
                gst = gst_rate * (base_fare + rcs + cute)
            
            # Calculate YQ based on distance
            distance = self._get_distance(origin, destination)
            yq = self._get_yq_for_distance(distance)
            
            total_taxes = asf + psf + udf_df + rcs + cute + gst + duf + yq + (yq*gst_rate)
            return total_taxes
            
        except Exception:
            return 0

    def _calculate_taxes_international(self, base_fare, origin, destination):
        """Calculate all taxes for international fare to get all-in fare."""
        try:
            if self.tax_master_intl_data is None or self.tax_master_intl_data.empty:
                return 0
                
            sector_key = f"{origin}{destination}"
            tax_data = self.tax_master_intl_data[self.tax_master_intl_data['Sectors'] == sector_key]
            if tax_data.empty:
                return 0
            
            tax_row = tax_data.iloc[0]
            
            # Extract all tax components
            gst_rate = float(tax_row.get('GST', 0)) / 100
            cute = float(tax_row.get('CUTE', 0)) if pd.notna(tax_row.get('CUTE')) else 0
            asf = float(tax_row.get('ASF', 0)) if pd.notna(tax_row.get('ASF')) else 0
            psf_adf = float(tax_row.get('PSF_ADF', 0)) if pd.notna(tax_row.get('PSF_ADF')) else 0
            udf = float(tax_row.get('UDF', 0)) if pd.notna(tax_row.get('UDF')) else 0
            psc_pz = float(tax_row.get('PSC_PZ', 0)) if pd.notna(tax_row.get('PSC_PZ')) else 0
            rcs_cute = float(tax_row.get('RCS_CUTE', 0)) if pd.notna(tax_row.get('RCS_CUTE')) else 0
            rcs = float(tax_row.get('RCS', 0)) if pd.notna(tax_row.get('RCS')) else 0
            
            # Additional taxes
            tax1 = float(tax_row.get('Tax1', 0)) if pd.notna(tax_row.get('Tax1')) else 0
            tax2 = float(tax_row.get('Tax2', 0)) if pd.notna(tax_row.get('Tax2')) else 0
            tax3 = float(tax_row.get('Tax3', 0)) if pd.notna(tax_row.get('Tax3')) else 0
            tax4 = float(tax_row.get('Tax4', 0)) if pd.notna(tax_row.get('Tax4')) else 0
            tax5 = float(tax_row.get('Tax5', 0)) if pd.notna(tax_row.get('Tax5')) else 0
            
            # YQ and YQ_GST from tax master international
            yq = float(tax_row.get('YQ', 0)) if pd.notna(tax_row.get('YQ')) else 0
            yq_gst_rate = float(tax_row.get('YQ_GST', 0)) / 100 if pd.notna(tax_row.get('YQ_GST')) else 0
            
            # Calculate GST
            gst = gst_rate * (base_fare + rcs + cute + rcs_cute)
            
            total_taxes = asf + psf_adf + udf + psc_pz + rcs_cute + rcs + cute + gst + tax1 + tax2 + tax3 + tax4 + tax5 + yq + (yq * yq_gst_rate)
            return total_taxes
            
        except Exception:
            return 0

    def calculate_all_in_fare(self, base_fare, origin, destination, ftype):
        """Calculate all-in fare based on route type."""
        try:
            if self.route == 'D':
                taxes = self._calculate_taxes_domestic(base_fare, origin, destination)
            else:
                taxes = self._calculate_taxes_international(base_fare, origin, destination)
            
            if ftype == "current":
                return base_fare + taxes
            else:
                return base_fare - taxes
            
        except Exception as e:
            self.logger.warning(f"Error calculating all-in fare: {str(e)}")
            return base_fare

    def run_weekly_check(self):
        """Compare selling fares AND bookings from the preceding 7 days against the live tariff.

        Two data sources, both checked against the live tariff sheet min/max per sector:
          1. Output summary selling fares — run_summary (direct) and
             run_summary_connections (through) in qp_dw_rmalloc.
          2. Bookings — ESB QP_DL_FLOWNDATAPAX and QP_DL_BOOKEDDATAPAX.

        For each active sector, if any selling/booking fare is below the lowest
        tariff level or above the highest tariff level, it is flagged.

        Returns:
            dict: Result with keys [violations_count, unmatched_count, status].
        """
        self.logger.info("Starting weekly fare validation (selling fares + bookings)")

        # Get live tariff sheet
        live_sheet = self.pa_helper.get_live_tariff_sheet()
        if live_sheet is None:
            self.logger.warning("Live tariff sheet unavailable — cannot perform weekly check")
            return {'violations_count': 0, 'unmatched_count': 0, 'status': 'NO_TARIFF'}

        # Get active sectors for filtering
        active_sectors_df = self.pa_helper.get_active_sectors()
        if active_sectors_df is None or active_sectors_df.empty:
            self.logger.warning("No active sectors — skipping weekly check")
            return {'violations_count': 0, 'unmatched_count': 0, 'status': 'NO_SECTORS'}

        active_sector_codes = set(active_sectors_df['sector'].tolist())

        # Source 1: selling fares from output summary (run_summary + connections)
        selling_fares = self.pa_helper.get_weekly_selling_fares(active_sector_codes)

        # Source 2: bookings from ESB (flown + booked pax)
        booking_fares = self.esb_helper.collect_weekly_booking_fares(active_sector_codes, days=7)

        if not selling_fares and not booking_fares:
            self.logger.info("No selling fare or booking data found for the preceding 7 days — skipping")
            return {'violations_count': 0, 'unmatched_count': 0, 'status': 'NO_DATA'}

        # Build tariff lookup
        tariff_lookup = self._build_tariff_lookup(live_sheet)

        violations = []
        unmatched = []

        # Check both sources against tariff boundaries
        for source_label, sector_fares in (('Selling Fare', selling_fares), ('Booking', booking_fares)):
            for sector, fares in sector_fares.items():
                origin = sector[:3]
                destination = sector[3:]
                if sector not in tariff_lookup:
                    unmatched.append({
                        'sector': sector,
                        'rbd': source_label,
                        'fare_value': f"min={fares['min_fare']}, max={fares['max_fare']}"
                    })
                    continue

                tariff_entry = tariff_lookup[sector]
                # self.calculate_all_in_fare(tariff_entry['min_fare'], origin, destination, ftype="current")
                tariff_min = self.calculate_all_in_fare(tariff_entry['min_fare'], origin, destination, ftype="current")
                tariff_max = self.calculate_all_in_fare(tariff_entry['max_fare'], origin, destination, ftype="current")

                # Below the lowest tariff level
                if fares['min_fare'] < tariff_min:
                    violations.append({
                        'sector': sector,
                        'rbd': f"{source_label} (count={fares['count']})",
                        'fare_value': fares['min_fare'],
                        'boundary': tariff_min,
                        'violation_type': 'below_min'
                    })

                # Above the highest tariff level
                if fares['max_fare'] > tariff_max:
                    violations.append({
                        'sector': sector,
                        'rbd': f"{source_label} (count={fares['count']})",
                        'fare_value': fares['max_fare'],
                        'boundary': tariff_max,
                        'violation_type': 'above_max'
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
        """Send email alert for fare discrepancies with CSV attachment.

        Instead of embedding a large HTML table (can be 5k+ rows), generates
        a CSV file with the violations and sends it as an attachment.

        Args:
            violations: List of dicts with keys [sector, rbd, fare_value, boundary, violation_type].
            unmatched: List of dicts with keys [sector, rbd, fare_value].
            check_type: 'daily' or 'weekly'.
        """
        try:
            ist = pytz.timezone('Asia/Kolkata')
            timestamp = datetime.now(ist).strftime('%Y-%m-%d %H:%M:%S IST')
            date_str = datetime.now(ist).strftime('%Y%m%d')
            check_label = 'Daily' if check_type == 'daily' else 'Weekly'

            # Build CSV content for violations
            csv_buffer = io.StringIO()
            csv_buffer.write("Sector,RBD/Source,Fare Value,Tariff Boundary,Violation Type\n")
            for v in violations:
                rbd_or_ref = v.get('rbd', 'N/A')
                violation_label = "Below Min" if v['violation_type'] == 'below_min' else "Above Max"
                csv_buffer.write(f"{v['sector']},{rbd_or_ref},{v['fare_value']},{v['boundary']},{violation_label}\n")

            # Encode CSV as base64 for attachment
            csv_content = csv_buffer.getvalue()
            filename = f"tariff_discrepancies_{check_type}_{date_str}.csv"

            # Build a summary HTML body (lightweight, no huge tables)
            html_body = f"""
            <html>
            <body>
                <h2>Tariff Sheet - {check_label} Fare Discrepancy Alert</h2>
                <p><strong>Check Time:</strong> {timestamp}</p>
                <p>The {check_label.lower()} fare comparison has detected discrepancies.</p>
                <table style='border-collapse: collapse;'>
                    <tr>
                        <td style='padding: 8px; font-weight: bold;'>Violations:</td>
                        <td style='padding: 8px; color: #cc0000;'>{len(violations)}</td>
                    </tr>
                </table>
                <p style='margin-top: 20px;'>
                    Please find the detailed discrepancy report in the attached CSV file.
                </p>
            </body>
            </html>
            """

            # Send email with CSV attachment
            attachments = [{
                'filename': filename,
                'content': csv_content,
                'file_type': 'csv'
            }]

            self.mail_client.send_email(
                f'Tariff Sheet - {check_label} Fare Discrepancy Alert',
                html_body,
                self.alert_email,
                is_html=True,
                attachments=attachments
            )

            self.logger.info(
                f"{check_label} discrepancy alert sent to {self.alert_email} — "
                f"{len(violations)} violation(s), {len(unmatched)} unmatched, "
                f"attached as {filename}"
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

            self.mail_client.send_email(
                'Tariff Sheet - Fare Comparison Failed',
                html_body,
                self.alert_email,
                is_html=True
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
        self.pa_helper.save_scheduled_job_audit(
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
