import io
import boto3
import pandas as pd
import numpy as np
import logging
import traceback
import uuid
import pytz
from datetime import datetime

import configsettings
from helpers.pa_db_helpers.tariff_pa_helper import TariffPAHelper
from helpers.nav_db_helpers.tariff_nav_helper import TariffNavHelper


# --- Custom Exceptions ---

class TariffGenError(Exception):
    """Base exception for tariff generation errors."""
    pass


class DataSourceError(TariffGenError):
    """Raised when a required data source is unavailable."""
    pass


class ValidationError(TariffGenError):
    """Raised when input data fails validation."""
    pass


class GenerationHaltError(TariffGenError):
    """Raised when generation must stop due to missing critical data."""
    pass


class TariffGen:
    """Orchestrates the full tariff sheet generation pipeline (Phase-2: GP-based fares).

    Each sector produces two rows (Maximum and Minimum fare type) with 15 fare levels.
    - Maximum row: Level 1 = Yield_Threshold*Distance/0.8, Level 15 = highest filed fare,
      Levels 2-14 = geometric progression (GP) between Level 1 and Level 15.
    - Minimum row: Level 1 = Yield_Threshold*Distance, Levels 2-15 = 0.9 * previous Max level.

    Handles fare extraction from fare dumps (B2C, W4/W5/W6, TBF RBDs),
    GP-based fare level calculation, market column formatting,
    YQ calculation, stops column population, information text section,
    tax data section, CSV generation, and S3 upload.
    """

    def __init__(self, logger, params, overrides, context):
        """Initialize with required connections, helpers, and configuration.

        Args:
            logger: Logger instance.
            params: dict with keys mongo, mongo_master, navconn, s3, etc.
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

        # S3 client
        self.s3 = params['s3']

        # Configuration
        self.analyst_name = overrides.get('analyst', 'system')
        self.BUCKET_NAME = configsettings.PA_BUCKET_NAME
        self.FILE_PATH = configsettings.PA_FILE_PATH

        # Constants from rm_parameter_values (configurable via DB)
        self.constants = params['constantutils']

        # RBD definitions from constants (configurable via rm_parameter_values)
        self.B2C_RBDS = self.constants.B2C_RBD_VALUES.split(',')
        self.W_RBDS = self.constants.W_RBD.split(',')
        self.TBF_RBDS = self.constants.TBF_RBD.split(',')

        # Fare level constants (Phase-2: GP-based 15 levels with Max/Min rows)
        self.MAX_FARE_LEVELS = 15
        self.YIELD_THRESHOLD = self.constants.YIELD_FACTOR  # 1.4 from rm_parameter_values
        self.MIN_TO_MAX_DIVISOR = 0.8  # Min Level 1 / 0.8 = Max Level 1
        self.MIN_DISCOUNT_FACTOR = 0.9  # Min Level N = 0.9 * Max Level (N-1)

        # YQ distance bands from constants (configurable via rm_parameter_values)
        # Format: [(0, 500, 199), (500, 1000, 299), (1000, 1500, 449), (1500, inf, 549)]
        self.YQ_BANDS = self.constants.YQ_DISTANCE_CONFIG

        # Data containers (populated in collect_data)
        self.fare_dump_df = None
        self.active_sectors_df = None
        self.station_master_df = None
        self.city_pair_df = None
        self.tax_master_df = None
        self.live_tariff_sheet = None

    def run(self):
        """Main entry point. Executes the full tariff generation pipeline.

        Returns:
            dict: Status dict with keys [status, run_id, s3_key, error].
        """
        run_id = str(uuid.uuid4())
        s3_key = None
        failure_reason = None
        ist = pytz.timezone('Asia/Kolkata')
        start_time = datetime.now(ist)

        self.logger.info(f"TariffGen run started — run_id: {run_id}")

        try:
            # Save initial audit with IN_PROGRESS
            self.pa_helper.save_run_audit(
                self.analyst_name, run_id, 'IN_PROGRESS', 'tariff_gen', start_time=start_time
            )

            # Step 1: Collect all required data
            self.collect_data()

            # Step 2: Generate tariff sheet DataFrame
            tariff_df = self.generate_tariff_sheet()

            # Step 3: Generate CSV content
            csv_content = self.generate_csv(tariff_df)

            # Step 4: Save output to S3 and MongoDB
            s3_key = self.save_output(run_id, tariff_df, csv_content)

            # Success audit
            self.pa_helper.save_run_audit(
                self.analyst_name, run_id, 'SUCCESS', 'tariff_gen', end_time=datetime.now(ist)
            )

            self.logger.info(f"TariffGen run completed — run_id: {run_id}, s3_key: {s3_key}")

            return {
                'status': 'SUCCESS',
                'run_id': run_id,
                's3_key': s3_key,
                'failure_reason': None
            }

        except GenerationHaltError as e:
            failure_reason = str(e)
            self.logger.error(f"TariffGen halted — run_id: {run_id}, reason: {failure_reason}")
            self.pa_helper.save_run_audit(
                self.analyst_name, run_id, 'FAILED', 'tariff_gen', failure_reason=failure_reason, end_time=datetime.now(ist)
            )
            return {
                'status': 'FAILED',
                'run_id': run_id,
                's3_key': None,
                'failure_reason': failure_reason
            }

        except Exception as e:
            failure_reason = f"Unexpected error: {str(e)}"
            self.logger.error(
                f"TariffGen failed — run_id: {run_id}\n{traceback.format_exc()}"
            )
            self.pa_helper.save_run_audit(
                self.analyst_name, run_id, 'FAILED', 'tariff_gen', failure_reason=failure_reason, end_time=datetime.now(ist)
            )
            return {
                'status': 'FAILED',
                'run_id': run_id,
                's3_key': None,
                'failure_reason': failure_reason
            }

    def collect_data(self):
        """Collect all required data sources for tariff generation.

        Retrieves fare dump, active sectors, station master, city pair distances,
        domestic tax master, and live tariff sheet from MongoDB and Navitaire.

        Returns:
            bool: True if all critical data collected successfully.

        Raises:
            GenerationHaltError: If critical data is missing or invalid.
        """
        self.logger.info("Collecting data for tariff generation...")

        # 1. Get active sectors first (needed for fare query)
        self.active_sectors_df = self.pa_helper.get_active_sectors()
        if self.active_sectors_df is None or self.active_sectors_df.empty:
            raise GenerationHaltError(
                "Active sector list is empty. Run sector monitor first."
            )
        self.logger.info(f"Active sectors loaded: {len(self.active_sectors_df)} sectors")

        # 2. Get QP fares from qp_fares collection (same source as ActionSectors/FareFiling)
        active_sector_codes = self.active_sectors_df['sector'].tolist()
        self.fare_dump_df = self.pa_helper.collect_qp_fares_for_tariff(active_sector_codes)
        if self.fare_dump_df is None or self.fare_dump_df.empty:
            raise GenerationHaltError(
                "No QP fares found in qp_fares collection for active sectors. "
                "Ensure fare filing has been run."
            )
        self.logger.info(f"QP fares loaded: {len(self.fare_dump_df)} records")

        # 3. Get station master
        self.station_master_df = self.pa_helper.get_station_master()
        if self.station_master_df is None or self.station_master_df.empty:
            self.logger.warning("Station master is empty — market formatting will use raw codes")

        # 4. Get city pair distances
        try:
            self.city_pair_df = self.nav_helper.get_city_pair_distances()
            self.logger.info(f"City pair distances loaded: {len(self.city_pair_df)} records")
        except Exception as e:
            self.logger.warning(f"City pair distances unavailable: {str(e)}. YQ will default to 0.")
            self.city_pair_df = pd.DataFrame()

        # 5. Get domestic tax master
        self.tax_master_df = self.pa_helper.get_domestic_tax_master()
        if self.tax_master_df is None or self.tax_master_df.empty:
            raise GenerationHaltError(
                "Domestic tax master is unavailable or contains no records. "
                "Cannot generate tariff sheet without tax data."
            )
        self.logger.info(f"Domestic tax master loaded: {len(self.tax_master_df)} records")

        # 6. Get live tariff sheet (for info text section)
        self.live_tariff_sheet = self.pa_helper.get_live_tariff_sheet()
        if self.live_tariff_sheet is None:
            raise GenerationHaltError(
                "Live tariff sheet is not available. "
                "Cannot generate information text section without a live tariff sheet."
            )
        self.logger.info("Live tariff sheet loaded")

        # 7. Validate QP fares cover active sectors
        fare_sectors = set(self.fare_dump_df['sector'].unique())
        active_sector_codes_set = set(active_sector_codes)
        missing_sectors = active_sector_codes_set - fare_sectors
        if missing_sectors:
            self.logger.warning(
                f"{len(missing_sectors)} active sector(s) have no QP fares: "
                f"{sorted(missing_sectors)}"
            )

        self.logger.info("Data collection complete — all critical sources available")
        return True

    def extract_b2c_fares(self):
        """Extract B2C fares from the fare dump.

        Filters: rule_number == '1001' AND footnote is empty/null/NaN.
        Excludes W-class and TBF RBDs (those are handled separately).
        Groups by sector, collects all distinct base_fare values.

        Returns:
            dict: {sector_code: [list of distinct fare values]}
        """
        active_sector_codes = set(self.active_sectors_df['sector'].tolist())
        b2c_fares = {}

        # Filter by rule_number = '1001'
        df = self.fare_dump_df.copy()
        df = df[df['rule_number'].astype(str) == '1001']

        # Filter out records with non-empty footnotes
        if 'footnote' in df.columns:
            df = df[
                df['footnote'].isna() |
                (df['footnote'].astype(str).str.strip() == '') |
                (df['footnote'].astype(str).str.lower() == 'nan')
            ]

        # Exclude W-class and TBF RBDs (handled by extract_w_fares and extract_tbf_fares)
        excluded_rbds = set(self.W_RBDS + self.TBF_RBDS)
        df = df[~df['rbd'].isin(excluded_rbds)]

        # Filter to active sectors only (sector column already exists from qp_fares query)
        df = df[df['sector'].isin(active_sector_codes)]

        # Group by sector, collect distinct fare values
        for sector, group in df.groupby('sector'):
            fares = group['base_fare'].dropna().unique().tolist()
            fares = [float(f) for f in fares if float(f) > 0]
            if fares:
                b2c_fares[sector] = fares
            else:
                self.logger.info(f"B2C: No qualifying fares for sector {sector}")

        self.logger.info(f"B2C fares extracted for {len(b2c_fares)} sectors")
        return b2c_fares

    def extract_w_fares(self):
        """Extract W4/W5/W6 fares from qp_fares.

        W-class RBDs are identified by matching the first 2 characters of
        Fare_Basis_Code to the W_RBD list (W4, W5, W6).
        
        No rule_number or footnote filter — W fares may have footnotes and
        can exist under any rule number.
        
        Takes the LOWEST fare per sector per RBD (minimum of all available).

        Returns:
            dict: {sector_code: [min_fare_w4, min_fare_w5, min_fare_w6]} (only non-null)
        """
        active_sector_codes = set(self.active_sectors_df['sector'].tolist())
        w_fares = {}

        df = self.fare_dump_df.copy()

        # Filter to W RBDs by matching first 2 chars of fare_basis_code
        df = df[df['rbd'].isin(self.W_RBDS)]

        # No footnote filter — W fares typically have footnotes
        # No rule_number filter — W fares can be under any rule

        # Filter to active sectors
        df = df[df['sector'].isin(active_sector_codes)]

        # Group by sector+rbd, take MIN(base_fare) — lowest available fare
        for sector, sector_group in df.groupby('sector'):
            sector_w_fares = []
            for rbd in self.W_RBDS:
                rbd_group = sector_group[sector_group['rbd'] == rbd]
                if not rbd_group.empty:
                    min_fare = rbd_group['base_fare'].dropna().min()
                    if pd.notna(min_fare) and float(min_fare) > 0:
                        sector_w_fares.append(float(min_fare))
            if sector_w_fares:
                w_fares[sector] = sector_w_fares

        self.logger.info(f"W fares extracted for {len(w_fares)} sectors")
        return w_fares

    def extract_tbf_fares(self):
        """Extract TBF fares (TY, TB, TC, TD, TE, TF) from qp_fares.

        TBF RBDs are identified by matching the first 2 characters of
        Fare_Basis_Code to the TBF_RBD list. They do not belong to a
        specific rule number (not necessarily 1001/B2C) and may have
        any footnote value.
        
        No rule_number or footnote filter.
        Takes the HIGHEST fare per sector per RBD (maximum of all available).

        Returns:
            dict: {sector_code: [max_fare_ty, max_fare_tb, ...]} (only non-null)
        """
        active_sector_codes = set(self.active_sectors_df['sector'].tolist())
        tbf_fares = {}

        df = self.fare_dump_df.copy()

        # Filter to TBF RBDs by matching first 2 chars of fare_basis_code
        df = df[df['rbd'].isin(self.TBF_RBDS)]

        # No rule_number filter — TBF fares are not tied to rule 1001
        # No footnote filter — TBF fares can have any footnote

        # Filter to active sectors
        df = df[df['sector'].isin(active_sector_codes)]

        # Group by sector+rbd, take MAX(base_fare) — highest available fare
        for sector, sector_group in df.groupby('sector'):
            sector_tbf_fares = []
            for rbd in self.TBF_RBDS:
                rbd_group = sector_group[sector_group['rbd'] == rbd]
                if not rbd_group.empty:
                    max_fare = rbd_group['base_fare'].dropna().max()
                    if pd.notna(max_fare) and float(max_fare) > 0:
                        sector_tbf_fares.append(float(max_fare))
            if sector_tbf_fares:
                tbf_fares[sector] = sector_tbf_fares

        self.logger.info(f"TBF fares extracted for {len(tbf_fares)} sectors")
        return tbf_fares

    def compute_gp_fare_levels(self, sector_fares):
        """Compute GP-based Maximum and Minimum fare levels for each sector.

        Phase-2 logic:
        - Maximum row:
            Level 1 = Yield_Threshold * Distance / 0.8
            Level 15 = highest filed fare (B2C + TBF combined)
            Levels 2-14 = GP interpolation between Level 1 and Level 15
        - Minimum row:
            Level 1 = Yield_Threshold * Distance
            Level N (2-15) = 0.9 * Max Level (N-1)

        If distance is unavailable, falls back to:
            Max Level 1 = min(filed fares) / 0.8
            Min Level 1 = min(filed fares)

        Args:
            sector_fares: dict {sector: [all fare values from B2C + TBF combined]}

        Returns:
            dict: {sector: {'max': [15 fare values], 'min': [15 fare values]}}
        """
        gp_levels = {}

        for sector, fares in sector_fares.items():
            if not fares:
                gp_levels[sector] = {
                    'max': [None] * self.MAX_FARE_LEVELS,
                    'min': [None] * self.MAX_FARE_LEVELS
                }
                continue

            # Highest filed fare = Level 15 of Maximum row
            max_fare_level_15 = max(fares)

            # Minimum fare floor = Yield_Threshold * Distance
            sector_distance = self._get_sector_distance(sector)
            if sector_distance is not None and sector_distance > 0:
                min_fare_level_1 = round(self.YIELD_THRESHOLD * sector_distance)
            else:
                # Fallback: use lowest filed fare as min level 1
                min_fare_level_1 = round(min(fares))
                self.logger.warning(
                    f"Sector {sector}: distance unavailable, using lowest filed fare "
                    f"({min_fare_level_1}) as Min Level 1"
                )

            # Maximum Level 1 = Min Level 1 / 0.8
            max_fare_level_1 = round(min_fare_level_1 / self.MIN_TO_MAX_DIVISOR)

            # Ensure Max Level 1 < Max Level 15 (sanity check)
            if max_fare_level_1 >= max_fare_level_15:
                self.logger.warning(
                    f"Sector {sector}: Max Level 1 ({max_fare_level_1}) >= Max Level 15 "
                    f"({max_fare_level_15}). Adjusting Max Level 15 to Max Level 1 * 2."
                )
                max_fare_level_15 = max_fare_level_1 * 2

            # --- Build Maximum row using GP ---
            # GP: a, a*r, a*r^2, ..., a*r^(n-1)
            # Level 1 = a = max_fare_level_1
            # Level 15 = a * r^14 = max_fare_level_15
            # r = (Level_15 / Level_1) ^ (1/14)
            n = self.MAX_FARE_LEVELS  # 15
            common_ratio = (max_fare_level_15 / max_fare_level_1) ** (1.0 / (n - 1))

            max_levels = []
            for i in range(n):
                fare_value = round(max_fare_level_1 * (common_ratio ** i))
                max_levels.append(fare_value)

            # Force exact endpoints to avoid floating-point drift
            max_levels[0] = max_fare_level_1
            max_levels[-1] = round(max_fare_level_15)

            # --- Build Minimum row ---
            # Min Level 1 = Yield_Threshold * Distance
            # Min Level N (2-15) = 0.9 * Max Level (N-1)
            min_levels = [min_fare_level_1]
            for i in range(1, n):
                min_level_value = round(self.MIN_DISCOUNT_FACTOR * max_levels[i - 1])
                min_levels.append(min_level_value)

            gp_levels[sector] = {
                'max': max_levels,
                'min': min_levels
            }

        self.logger.info(f"GP fare levels computed for {len(gp_levels)} sectors")
        return gp_levels

    def _get_sector_distance(self, sector_code):
        """Look up sector distance from city_pair_df.

        Args:
            sector_code: 6-char sector code (e.g., 'BOMDEL').

        Returns:
            float or None: Distance in km, or None if not found.
        """
        if self.city_pair_df is None or self.city_pair_df.empty:
            return None

        origin = sector_code[:3]
        destination = sector_code[3:]

        # Try forward direction
        match = self.city_pair_df[
            (self.city_pair_df['DepartureStation'] == origin) &
            (self.city_pair_df['ArrivalStation'] == destination)
        ]
        if not match.empty:
            return float(match.iloc[0]['ActualDistance'])

        # Try reverse direction as fallback
        reverse_match = self.city_pair_df[
            (self.city_pair_df['DepartureStation'] == destination) &
            (self.city_pair_df['ArrivalStation'] == origin)
        ]
        if not reverse_match.empty:
            return float(reverse_match.iloc[0]['ActualDistance'])

        self.logger.warning(f"No distance found for sector {sector_code}")
        return None

    def calculate_yq(self, distance_km):
        """Calculate YQ (fuel surcharge) based on sector distance.

        Uses YQ_DISTANCE_CONFIG from rm_parameter_values collection.
        Default bands (exclusive lower, inclusive upper):
        - 0 < distance <= 500:    YQ = 199
        - 500 < distance <= 1000: YQ = 299
        - 1000 < distance <= 1500: YQ = 449
        - distance > 1500:         YQ = 549
        - distance unavailable:    YQ = 0

        Args:
            distance_km: Sector distance in kilometers.

        Returns:
            int: YQ value.
        """
        if distance_km is None or distance_km <= 0:
            return 0

        for lower, upper, yq_value in self.YQ_BANDS:
            if lower < distance_km <= upper:
                return yq_value

        # Fallback (should not reach here given float('inf') upper bound)
        return 549

    def format_market(self, sector_code):
        """Format sector code into 'Origin City - Destination City' display format.

        Splits 6-char sector into origin ([:3]) and destination ([3:]).
        Maps each to city name from station_master_df.
        Falls back to raw airport code if mapping is missing.

        Args:
            sector_code: 6-character sector code (e.g., 'BOMDEL').

        Returns:
            str: Formatted market string (e.g., 'Mumbai - New Delhi').
        """
        origin = sector_code[:3]
        destination = sector_code[3:]

        origin_city = self._get_city_name(origin)
        destination_city = self._get_city_name(destination)

        return f"{origin_city} - {destination_city}"

    def _get_city_name(self, station_code):
        """Look up city name for a station code from station_master_df.

        Args:
            station_code: 3-character airport/station code.

        Returns:
            str: City name if found, otherwise the raw station code.
        """
        if self.station_master_df is None or self.station_master_df.empty:
            return station_code

        # Try common column names for city
        city_columns = ['name', 'city_name', 'city', 'City', 'CityName', 'CITY', 'Name']
        station_columns = ['station', 'Station', 'code', 'Code', 'station_code']

        station_col = None
        for col in station_columns:
            if col in self.station_master_df.columns:
                station_col = col
                break

        city_col = None
        for col in city_columns:
            if col in self.station_master_df.columns:
                city_col = col
                break

        if station_col is None or city_col is None:
            self.logger.warning(
                f"Station master missing expected columns. "
                f"Available: {list(self.station_master_df.columns)}"
            )
            return station_code

        match = self.station_master_df[
            self.station_master_df[station_col].astype(str).str.strip() == station_code
        ]

        if not match.empty:
            city_name = str(match.iloc[0][city_col]).strip()
            if city_name and city_name.lower() != 'nan':
                return city_name

        self.logger.warning(f"No city mapping found for station code: {station_code}")
        return station_code

    def _is_station_mapped(self, station_code):
        """Check if a station code exists in the station master.

        Args:
            station_code: 3-character airport/station code.

        Returns:
            bool: True if station is mapped, False otherwise.
        """
        if self.station_master_df is None or self.station_master_df.empty:
            return False

        station_columns = ['station', 'Station', 'code', 'Code', 'station_code']
        for col in station_columns:
            if col in self.station_master_df.columns:
                return station_code in self.station_master_df[col].astype(str).str.strip().values

        return False

    def generate_tariff_sheet(self):
        """Execute the full generation pipeline and return tariff DataFrame.

        Phase-2: Each sector produces two rows (Maximum and Minimum fare type)
        with 15 GP-based fare levels.

        Steps:
        1. Extract all fares (B2C, TBF) — W fares excluded from GP input
        2. Combine per sector
        3. Compute GP fare levels (Max and Min rows)
        4. Build DataFrame with Market, Fare_Type, Stops, YQ, Fare_Level_1..15

        Returns:
            pd.DataFrame: Complete tariff sheet DataFrame.
        """
        self.logger.info("Generating tariff sheet (Phase-2: GP-based)...")

        # Step 1: Extract fares from B2C and TBF sources
        # (W fares are not used for GP boundary computation per Phase-2 spec)
        b2c_fares = self.extract_b2c_fares()
        tbf_fares = self.extract_tbf_fares()

        # Step 2: Combine all fare values per sector (B2C + TBF only)
        all_sectors = set(
            list(b2c_fares.keys()) +
            list(tbf_fares.keys())
        )

        sector_fares = {}
        for sector in all_sectors:
            combined = []
            if sector in b2c_fares:
                combined.extend(b2c_fares[sector])
            if sector in tbf_fares:
                combined.extend(tbf_fares[sector])
            if combined:
                sector_fares[sector] = combined

        # Step 3: Compute GP fare levels
        gp_levels = self.compute_gp_fare_levels(sector_fares)

        # Step 4: Build DataFrame rows (2 rows per sector: Maximum, Minimum)
        rows = []
        unmapped_sectors = []

        for _, sector_row in self.active_sectors_df.iterrows():
            sector = sector_row['sector']

            # Skip sectors not in gp_levels (no fares extracted)
            if sector not in gp_levels:
                self.logger.info(f"Skipping sector {sector} — no fares extracted")
                continue

            # Check for unmapped stations — collect all failures before halting
            origin = sector[:3]
            destination = sector[3:]
            if not self._is_station_mapped(origin) or not self._is_station_mapped(destination):
                unmapped_origin = origin if not self._is_station_mapped(origin) else None
                unmapped_dest = destination if not self._is_station_mapped(destination) else None
                unmapped_codes = [c for c in [unmapped_origin, unmapped_dest] if c]
                unmapped_sectors.append(f"{sector} (unmapped: {', '.join(unmapped_codes)})")
                continue

            # Market column — check city name mapping
            origin_name = self._get_city_name(origin)
            destination_name = self._get_city_name(destination)
            if origin_name == origin or destination_name == destination:
                missing_names = []
                if origin_name == origin:
                    missing_names.append(origin)
                if destination_name == destination:
                    missing_names.append(destination)
                unmapped_sectors.append(f"{sector} (no city name: {', '.join(missing_names)})")
                continue

            market = f"{origin_name} - {destination_name}"

            # Stops column
            stops = int(sector_row.get('stops', 0))

            # YQ column
            distance = self._get_sector_distance(sector)
            yq = self.calculate_yq(distance)

            # Get GP levels for this sector
            sector_gp = gp_levels[sector]

            # Build Maximum row
            max_row = {
                'Market': market,
                'Fare_Type': 'Maximum',
                'Stops': stops,
                'Fuel Charge (YQ) in INR': yq
            }
            for i, fare in enumerate(sector_gp['max'], start=1):
                max_row[f'Fare_Level_{i}'] = fare

            # Build Minimum row
            min_row = {
                'Market': market,
                'Fare_Type': 'Minimum',
                'Stops': stops,
                'Fuel Charge (YQ) in INR': yq
            }
            for i, fare in enumerate(sector_gp['min'], start=1):
                min_row[f'Fare_Level_{i}'] = fare

            rows.append(max_row)
            rows.append(min_row)

        # Fail the entire run if any sectors have unmapped stations or missing city names
        if unmapped_sectors:
            error_msg = (
                f"Tariff generation failed — {len(unmapped_sectors)} sector(s) have stations "
                f"not in station master or missing city name mapping: {', '.join(unmapped_sectors)}. "
                f"Please update config_station_master with the missing stations/names."
            )
            raise GenerationHaltError(error_msg)

        # Create DataFrame and sort by Market, then Fare_Type (Max before Min)
        tariff_df = pd.DataFrame(rows)
        if not tariff_df.empty:
            tariff_df = tariff_df.sort_values(
                ['Market', 'Fare_Type'], ascending=[True, False]
            ).reset_index(drop=True)

        self.logger.info(
            f"Tariff sheet generated: {len(tariff_df)} rows "
            f"({len(tariff_df) // 2} sectors × 2 fare types)"
        )
        return tariff_df

    def generate_csv(self, tariff_df):
        """Generate CSV string from tariff DataFrame.

        Formats fare values with comma thousands separator (e.g., 1402 → '1,402').
        Empty fare levels are represented as '  ' (two spaces).
        Includes Fare_Type column (Maximum/Minimum) for each row.

        Args:
            tariff_df: DataFrame with tariff sheet data.

        Returns:
            str: CSV content string.
        """
        if tariff_df.empty:
            self.logger.warning("Tariff DataFrame is empty — generating empty CSV")
            return ""

        # Create a copy for formatting
        output_df = tariff_df.copy()

        # Format fare level columns
        fare_cols = [col for col in output_df.columns if col.startswith('Fare_Level_')]
        for col in fare_cols:
            output_df[col] = output_df[col].apply(
                lambda x: f"{int(x):,}" if pd.notna(x) and x is not None else "  "
            )

        # Format Stops and YQ as integers
        output_df['Stops'] = output_df['Stops'].astype(int)
        output_df['Fuel Charge (YQ) in INR'] = output_df['Fuel Charge (YQ) in INR'].astype(int)

        # Reorder columns: Market, Fare_Type, Stops, YQ, Fare_Level_1..15
        col_order = ['Market', 'Fare_Type', 'Stops', 'Fuel Charge (YQ) in INR'] + fare_cols
        output_df = output_df[col_order]

        # Generate CSV string
        csv_buffer = io.StringIO()
        output_df.to_csv(csv_buffer, index=False)
        csv_content = csv_buffer.getvalue()

        self.logger.info(f"CSV generated: {len(csv_content)} bytes")
        return csv_content

    def save_output(self, run_id, tariff_df, csv_content):
        """Save generated tariff sheet to S3 and MongoDB.

        Uploads CSV to S3 and saves metadata to generated_tariff_sheet collection.
        Each sector now produces two entries (Maximum, Minimum) in fare_data.

        Args:
            run_id: Unique identifier for the tariff generation run.
            tariff_df: DataFrame with tariff sheet data.
            csv_content: CSV string content.

        Returns:
            str: S3 key where the CSV was uploaded.
        """
        ist = pytz.timezone('Asia/Kolkata')
        timestamp = datetime.now(ist).strftime('%Y%m%d_%H%M%S')

        # Upload CSV to S3
        s3_key = f"{self.FILE_PATH}tariff_sheets/tariff_{timestamp}.csv"

        try:
            self.s3.put_object(
                Bucket=self.BUCKET_NAME,
                Key=s3_key,
                Body=csv_content,
                ContentType='text/csv'
            )
            self.logger.info(f"Tariff CSV uploaded to S3: s3://{self.BUCKET_NAME}/{s3_key}")
        except Exception as e:
            self.logger.error(f"Failed to upload CSV to S3: {str(e)}")
            raise

        # Build fare_data for MongoDB document
        fare_data = []
        if not tariff_df.empty:
            for _, row in tariff_df.iterrows():
                fare_levels = []
                for i in range(1, self.MAX_FARE_LEVELS + 1):
                    col = f'Fare_Level_{i}'
                    val = row.get(col)
                    if pd.notna(val) and val is not None:
                        fare_levels.append(int(val))

                fare_data.append({
                    'market': row['Market'],
                    'fare_type': row['Fare_Type'],
                    'stops': int(row['Stops']),
                    'yq': int(row['Fuel Charge (YQ) in INR']),
                    'fare_levels': fare_levels
                })

        # Get info text from live tariff sheet
        info_text = self.live_tariff_sheet.get('info_text', '') if self.live_tariff_sheet else ''

        # Save to MongoDB
        tariff_doc = {
            'run_id': run_id,
            'triggered_by': self.analyst_name,
            'fare_data': fare_data,
            'info_text': info_text,
            'status': 'success',
            'error_message': None,
            'sector_count': len(fare_data) // 2  # Each sector has 2 rows (Max + Min)
        }

        self.pa_helper.save_generated_tariff(tariff_doc)
        self.logger.info(f"Generated tariff sheet metadata saved to MongoDB")

        return s3_key
