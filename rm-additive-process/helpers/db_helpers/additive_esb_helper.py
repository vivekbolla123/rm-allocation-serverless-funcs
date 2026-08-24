"""
helpers/db_helpers/additive_esb_helper.py
=============================================================================
All ESB (MySQL) SQL reads for the Additive Pickup Model: historical flown data
(incremental, sector/month-partitioned cache), STD lookup (sector-partitioned),
and the forward flight/booking scan. Results are written into the shared
AdditiveCache's in-memory dict (cache.data), exactly like the monolith did.

Injected engine + shared cache; per-run values (sectors, dates, backfill flag)
are passed to fetch_all_data so the helper stays stateless between sectors.
"""

import traceback
from datetime import timedelta

import pandas as pd

from Constants import PREDICTION_HORIZON_DAYS
from helpers.utils.common_utils import DatabaseConfig, run_sql


class AdditiveEsbHelper:
    """MySQL/ESB data access for the Additive Pickup Model."""

    def __init__(self, esb_engine, logger):
        self.engine = esb_engine
        self.logger = logger
        self.cache = None

    def bind_context(self, cache):
        self.cache = cache
        return self

    # =========================================================================
    # QUERY BUILDER (single source of truth for the flown SELECT)
    # =========================================================================
    def build_flown_query(self, date_predicate, sector_conditions):
        return f"""
        SELECT
            CONCAT(qdb.Origin, qdb.Destination) AS Sector,
            qdb.DepartureDate,
            DATEDIFF(qdb.DepartureDate, qdb.BookingDate) AS NDO,
            qdb.FlightNumber,
            qdb.InventoryLegID,
            qdb.PNR,
            qdb.RBD,
            qdb.FareBasis AS FareBasisRaw,
            CASE
                WHEN SUBSTRING(qdb.FareBasis, 5, 1) = 'T'
                THEN SUM(qdb.FB_Net + qdb.YQ_Pro + qdb.RCS + qdb.CUTE)
                ELSE SUM(qdb.DiscountedBaseFare_Pro + qdb.YQ_Pro + qdb.RCS + qdb.CUTE)
            END AS Fare,
            SUM(CASE WHEN qdb.FareBasis NOT LIKE '%SOL%' THEN 1 ELSE 0 END) AS `Pax Count`,
            qdb.AdjustedCapacity AS Capacity,
            qdb.Distance
        FROM {DatabaseConfig.FLOWN_DATA_TABLE} qdb
        WHERE {date_predicate}
            AND ({sector_conditions})
            AND qdb.AdjustedCapacity > 0
            AND FlightNumber < 6000
            AND qdb.BookingDate >= DATE_SUB(qdb.DepartureDate, INTERVAL 365 DAY)
            AND qdb.BookingDate <= qdb.DepartureDate
        GROUP BY qdb.Origin, qdb.Destination, qdb.DepartureDate,
                 qdb.BookingDate, qdb.FlightNumber, qdb.InventoryLegID,
                 qdb.PNR, qdb.RBD, qdb.FareBasis,
                 qdb.AdjustedCapacity, qdb.Distance
        """

    # =========================================================================
    # FORWARD QUERY BUILDER (parameterized on booking source + windows)
    # =========================================================================
    def build_forward_query(self, booking_table, dep_start, dep_end,
                            booking_lower, booking_upper, fm_station_conditions):
        """Forward flight scan: InventoryLeg (schedule) LEFT JOIN a booking source
        (QP_DL_BOOKEDDATAPAX for still-open flights, or QP_DL_FLOWNDATAPAX to
        recover flights that have since flown). `booking_upper` is exclusive, so
        passing the snapshot date reconstructs the 'as of snapshot' booking state."""
        return f"""
        SELECT
            fm.InventoryLegID,
            CONCAT(fm.DepartureStation, fm.ArrivalStation) AS Sector,
            fm.STD AS STD_dt,
            fm.FlightNumber,
            fm.AdjustedCapacity as Capacity,
            fm.DepartureDate AS DepartDate_raw,
            qdb.DepartureDate,
            qdb.PNR,
            qdb.RBD,
            qdb.FareBasis AS FareBasisRaw,
            qdb.BookingDate,
            DATEDIFF(qdb.DepartureDate, qdb.BookingDate) AS NDO,
            CASE
                WHEN SUBSTRING(qdb.FareBasis, 5, 1) = 'T'
                THEN SUM(qdb.FB_Net + qdb.YQ_Pro + qdb.RCS + qdb.CUTE)
                ELSE SUM(qdb.DiscountedBaseFare_Pro + qdb.YQ_Pro + qdb.RCS + qdb.CUTE)
            END AS Fare,
            SUM(CASE
                WHEN qdb.FareBasis NOT LIKE '%SOL%' THEN 1
                ELSE 0
            END) AS `Pax Count`
        FROM {DatabaseConfig.FLIGHT_MASTER_TABLE} fm
        LEFT JOIN {booking_table} qdb
            ON qdb.InventoryLegID = fm.InventoryLegID
            AND qdb.BookingDate >= '{booking_lower}' AND qdb.BookingDate < '{booking_upper}'
        WHERE fm.DepartureDate BETWEEN '{dep_start}' AND '{dep_end}'
            AND ({fm_station_conditions})
            AND fm.FlightNumber < 6000
            AND fm.CarrierCode = 'QP'
            AND fm.Status <> 2
            AND fm.AdjustedCapacity > 0
        GROUP BY fm.InventoryLegID, fm.DepartureStation, fm.ArrivalStation, fm.STD,
                 fm.FlightNumber, fm.Capacity, fm.DepartureDate,
                 qdb.DepartureDate, qdb.PNR, qdb.RBD,
                 qdb.FareBasis, qdb.BookingDate
        """

    def _run_forward(self, query):
        with self.engine.connect() as conn:
            chunks = pd.read_sql(__import__('sqlalchemy').text(query), conn, chunksize=50000)
            return pd.concat([c for c in chunks], ignore_index=True)

    # =========================================================================
    # ORCHESTRATOR — historical + STD + forward -> cache.data
    # =========================================================================
    def fetch_all_data(self, active_sectors, today, current_date, force_backfill,
                       simulate=False, actual_today=None):
        engine = self.engine

        yesterday = today - timedelta(days=1)
        month_start = current_date.replace(day=1)
        tag_floor = month_start - pd.DateOffset(years=1)
        future_tag_floor = month_start - pd.Timedelta(days=365)
        hist_start = min(tag_floor, future_tag_floor).date()

        forward_end = today + timedelta(days=PREDICTION_HORIZON_DAYS)
        booking_cutoff = today - timedelta(days=730)

        self.logger.info(f"Date windows — hist: {hist_start}..{yesterday}, "
                         f"forward: {today}..{forward_end}")

        fm_station_conditions = " OR ".join([
            f"(fm.DepartureStation = '{s[:3]}' AND fm.ArrivalStation = '{s[3:]}')"
            for s in active_sectors
        ])

        # --- Query 1: historical flown (incremental, sector-aware cache) ---
        hist_df = self.fetch_historical_incremental(
            active_sectors, hist_start, yesterday, current_date, force_backfill,
            simulate=simulate)
        self.cache.data['hist_raw_df'] = hist_df

        # --- Query 2: STD lookup (incremental, sector-partitioned) ---
        self.fetch_std_incremental(active_sectors, hist_df)

        # --- Query 3: forward data ---
        # NORMAL: one scan of BOOKEDDATAPAX over [today, today+180], bookings made
        #         before `today` (== now). BOOKEDDATAPAX holds only open (not-yet-
        #         flown) bookings, which is exactly the forward universe live.
        #
        # SIMULATE: `today` is a past snapshot. The forward window [snapshot,
        #         snapshot+180] straddles the wall clock: flights departing
        #         [snapshot, actual_today) have ALREADY FLOWN and have been purged
        #         from BOOKEDDATAPAX, so we recover their as-of-snapshot bookings
        #         from FLOWNDATAPAX instead; flights [actual_today, snapshot+180]
        #         are still open in BOOKEDDATAPAX. Both scans filter BookingDate <
        #         snapshot, so each reconstructs the booking state as of the
        #         snapshot. (Caveat: FLOWNDATAPAX is net of no-shows/cancellations,
        #         so pre-snapshot bookings later cancelled are missed — accepted for
        #         this backtest.)
        scans = []
        if simulate and actual_today is not None and actual_today > today:
            self.logger.info(f"[Query 3][SIMULATE] snapshot={today}, actual_today={actual_today}; "
                             f"forward window {today}..{forward_end} split FLOWN+BOOKED.")
            flown_dep_end = actual_today - timedelta(days=1)   # since-flown portion
            scans.append(('FLOWN(since-flown)', self.build_forward_query(
                DatabaseConfig.FLOWN_DATA_TABLE, today, flown_dep_end,
                booking_cutoff, today, fm_station_conditions)))
            scans.append(('BOOKED(still-open)', self.build_forward_query(
                DatabaseConfig.BOOKED_DATA_TABLE, actual_today, forward_end,
                booking_cutoff, today, fm_station_conditions)))
        else:
            self.logger.info(f"[Query 3] Fetching forward data: {today} to {forward_end}...")
            scans.append(('BOOKED', self.build_forward_query(
                DatabaseConfig.BOOKED_DATA_TABLE, today, forward_end,
                booking_cutoff, today, fm_station_conditions)))

        try:
            parts = []
            for label, q in scans:
                part = self._run_forward(q)
                self.logger.info(f"[Query 3] {label}: {len(part)} records.")
                parts.append(part)
            forward_df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
            self.logger.info(f"[Query 3] Done — {len(forward_df)} records total.")
        except Exception as e:
            self.logger.error(f"Error fetching forward data: {e}")
            self.logger.error(traceback.format_exc())
            raise

        forward_df['STD'] = pd.to_datetime(forward_df['STD_dt']).dt.time
        forward_df.drop(columns=['STD_dt'], inplace=True, errors='ignore')
        if 'DepartDate_raw' in forward_df.columns:
            forward_df['DepartureDate'] = forward_df['DepartureDate'].fillna(forward_df['DepartDate_raw'])
            forward_df.drop(columns=['DepartDate_raw'], inplace=True, errors='ignore')
        forward_df['DepartureDate'] = pd.to_datetime(forward_df['DepartureDate'])
        forward_df['DOW'] = forward_df['DepartureDate'].dt.day_name()
        forward_df['FlightNumber'] = pd.to_numeric(forward_df['FlightNumber'], errors='coerce')
        forward_df = forward_df.dropna(subset=['FlightNumber'])
        forward_df['FlightNumber'] = forward_df['FlightNumber'].astype(int)

        fm_df = forward_df[['Sector', 'DepartureDate', 'STD', 'FlightNumber', 'Capacity']].copy()
        fm_df = fm_df.drop_duplicates(
            subset=['Sector', 'DepartureDate', 'STD', 'FlightNumber'], keep='first')
        self.cache.data['future_flight_master'] = fm_df
        self.logger.info(f"[Query 3] Flight universe: {len(fm_df)} scheduled flights across "
                         f"{fm_df['Sector'].nunique()} sectors.")

        valid_ndo = forward_df['NDO'].isna() | ((forward_df['NDO'] >= 0) & (forward_df['NDO'] <= 365))
        forward_df = forward_df[valid_ndo]

        fwd_sectors = set(fm_df['Sector'].unique()) if not fm_df.empty else set()
        missing_sectors = set(active_sectors) - fwd_sectors
        if missing_sectors:
            self.logger.warning(f"[Query 3] {len(missing_sectors)} sectors have NO future flights: "
                                f"{sorted(missing_sectors)}")
        self.cache.data['forward_raw_df'] = forward_df

    # =========================================================================
    # HISTORICAL FLOWN (incremental)
    # =========================================================================
    def fetch_historical_incremental(self, active_sectors, hist_start, yesterday,
                                     current_date, force_backfill, simulate=False):
        cache = self.cache
        cached_df = pd.DataFrame() if force_backfill else \
            cache.read_sector_cache('hist_flown', active_sectors)

        # SIMULATE: the cache may hold a window that extends PAST this snapshot
        # (a pre-seeded union, or leftovers from a later date already run). Those
        # departures are in this snapshot's future and must never enter the
        # historical inputs. Clip to strictly-before the snapshot; this also
        # leaves the per-sector frontier at snapshot-1, so the gap-fill below has
        # nothing to fetch (no ESB hit) when the base already covers the day.
        if simulate and not cached_df.empty and 'DepartureDate' in cached_df.columns:
            cached_df = cached_df[pd.to_datetime(cached_df['DepartureDate']) < pd.to_datetime(current_date)]

        cached_sectors = set(cached_df['Sector'].unique()) if not cached_df.empty else set()
        new_sectors = set(active_sectors) - cached_sectors
        existing_sectors = set(active_sectors) & cached_sectors
        self.logger.info(f"  Batch: {len(active_sectors)} sectors "
                         f"(cached {len(existing_sectors)}, new {len(new_sectors)})")

        touched_months = set()
        new_dfs = []

        if new_sectors:
            conds = " OR ".join(
                f"(qdb.Origin = '{s[:3]}' AND qdb.Destination = '{s[3:]}')"
                for s in sorted(new_sectors))
            self.logger.info(f"  Backfilling {len(new_sectors)} new sectors...")
            q = self.build_flown_query(
                f"qdb.DepartureDate BETWEEN '{hist_start}' AND '{yesterday}'", conds)
            df = run_sql(self.engine, q)
            self.logger.info(f"[Query END] {len(df)} rows")
            df['DepartureDate'] = pd.to_datetime(df['DepartureDate'])
            new_dfs.append(df)
            touched_months = None       # None => write ALL months for these sectors

        if existing_sectors:
            conds = " OR ".join(
                f"(qdb.Origin = '{s[:3]}' AND qdb.Destination = '{s[3:]}')"
                for s in sorted(existing_sectors))
            # Gap-aware incremental: fetch from the day AFTER the last cached
            # departure date up to yesterday. A daily run fetches one day; a
            # sequenced run with a multi-day gap (e.g. 01-06 -> 15-06) fills every
            # missing day, instead of only yesterday.
            ex_cached = cached_df[cached_df['Sector'].isin(existing_sectors)]
            last_cached = ex_cached['DepartureDate'].max() if not ex_cached.empty else None
            if last_cached is None or pd.isna(last_cached):
                gap_start = pd.to_datetime(yesterday)
            else:
                gap_start = pd.to_datetime(last_cached) + pd.Timedelta(days=1)
            gap_end = pd.to_datetime(yesterday)

            if gap_start <= gap_end:
                self.logger.info(f"  Incremental gap-fill {gap_start.date()}..{gap_end.date()} "
                                 f"for {len(existing_sectors)} sectors...")
                q = self.build_flown_query(
                    f"qdb.DepartureDate BETWEEN '{gap_start.date()}' AND '{gap_end.date()}'", conds)
                df = run_sql(self.engine, q)
                df['DepartureDate'] = pd.to_datetime(df['DepartureDate'])
                new_dfs.append(df)
                # drop any cached rows inside the refetch range (avoid dups)
                cached_df = cached_df[~((cached_df['Sector'].isin(existing_sectors)) &
                                        (cached_df['DepartureDate'] >= gap_start) &
                                        (cached_df['DepartureDate'] <= gap_end))]
                if touched_months is not None:
                    m = gap_start.replace(day=1)
                    while m <= gap_end:
                        touched_months.add(cache.month_key(m))
                        m = m + pd.DateOffset(months=1)
            else:
                self.logger.info(f"  Cache already current through {last_cached.date()}; "
                                 f"nothing to gap-fill.")

        if new_dfs:
            all_new = pd.concat(new_dfs, ignore_index=True)
            all_new['DepartureDate'] = pd.to_datetime(all_new['DepartureDate'])
            all_new['DOW'] = all_new['DepartureDate'].dt.day_name()
            hist_df = pd.concat([cached_df, all_new], ignore_index=True)
        else:
            hist_df = cached_df

        month_start = current_date.replace(day=1)
        cutoff = min(month_start - pd.DateOffset(years=1),
                     month_start - pd.Timedelta(days=365))
        cutoff_month = cutoff.strftime('%Y-%m')

        before = len(hist_df)
        hist_df = hist_df[hist_df['DepartureDate'] >= cutoff]
        if len(hist_df) != before and not simulate:
            # (skipped when simulating: the base is pre-seeded and shared across the
            #  whole date sequence — deleting would remove months an earlier day needs)
            for s in active_sectors:
                for m in cache.list_cache_months('hist_flown', s):
                    if m < cutoff_month:
                        cache.delete_cache_partition('hist_flown', s, m)
            if touched_months is not None:
                touched_months.add(cutoff_month)

        cache.write_sector_cache('hist_flown', hist_df, months=touched_months)

        self.logger.info(f"[Query 1] {len(hist_df)} rows, {hist_df['Sector'].nunique()} sectors "
                         f"(from {cutoff.date()}).")
        if not hist_df.empty:
            self.logger.info(f"[hist_flown] {hist_df['DepartureDate'].min().date()}.."
                             f"{hist_df['DepartureDate'].max().date()}")
        return hist_df

    # =========================================================================
    # STD LOOKUP (incremental, sector-partitioned)
    # =========================================================================
    def fetch_std_incremental(self, active_sectors, hist_df):
        cache = self.cache
        self.logger.info("[Query 2] Fetching STD lookup (incremental, sector-partitioned)...")

        std_frames = []
        for sector in active_sectors:
            cached = cache.store_read(f'std_lookup/sector={sector}.parquet')
            if cached is not None and not cached.empty:
                cached['Sector'] = sector
                std_frames.append(cached)

        std_cache = pd.concat(std_frames, ignore_index=True) if std_frames else \
            pd.DataFrame(columns=['InventoryLegID', 'STD', 'Sector'])

        all_ids = set(hist_df['InventoryLegID'].unique())
        cached_ids = set(std_cache['InventoryLegID'].unique()) if not std_cache.empty else set()
        missing = list(all_ids - cached_ids)

        if missing:
            self.logger.info(f"  Need STD for {len(missing)} new legs (cache has {len(cached_ids)}).")
            new_dfs = []
            for i in range(0, len(missing), 5000):
                ids_str = ", ".join(str(int(x)) for x in missing[i:i + 5000])
                q = f"""SELECT InventoryLegID, STD,
                        CONCAT(DepartureStation, ArrivalStation) AS Sector
                        FROM {DatabaseConfig.FLIGHT_MASTER_TABLE}
                        WHERE InventoryLegID IN ({ids_str})"""
                with self.engine.connect() as conn:
                    new_dfs.append(pd.read_sql(__import__('sqlalchemy').text(q), conn))
            if new_dfs:
                new_std = pd.concat(new_dfs, ignore_index=True)
                std_cache = pd.concat([std_cache, new_std], ignore_index=True)
                std_cache.drop_duplicates(subset=['InventoryLegID'], keep='last', inplace=True)
                for sector in active_sectors:
                    sector_std = std_cache[std_cache['Sector'] == sector]
                    if not sector_std.empty:
                        cache.store_write(sector_std[['InventoryLegID', 'STD']],
                                          f'std_lookup/sector={sector}.parquet')
                self.logger.info(f"  STD cache updated: {len(std_cache)} entries "
                                 f"across {std_cache['Sector'].nunique()} sectors.")
        else:
            self.logger.info(f"  All {len(all_ids)} leg IDs found in STD cache.")

        merged = hist_df.merge(
            std_cache[['InventoryLegID', 'STD']].drop_duplicates(subset=['InventoryLegID']),
            on='InventoryLegID', how='left')
        merged['STD'] = pd.to_datetime(merged['STD']).dt.time
        merged.drop(columns=['InventoryLegID'], inplace=True)

        unresolved = int(merged['STD'].isna().sum())
        if unresolved:
            self.logger.warning(f"  {unresolved} rows have no STD after merge.")

        cache.data['hist_raw_df'] = merged