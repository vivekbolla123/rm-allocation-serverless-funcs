import logging
import time
import traceback
from datetime import datetime, timedelta, timezone

import pandas as pd
import configsettings
from pymongo import UpdateOne
from constants_utils import ConstantsUtils


# IST is a fixed offset from UTC (no DST).
IST = timezone(timedelta(hours=5, minutes=30))


class NavitaireCache:
    """Prefetch per-flight bookload + opening RBD (and QP fares) from Navitaire
    for the full departure-date horizon and persist to MongoDB so downstream
    report/prefetch runs read from there instead of hitting Navitaire.

    Opening RBD (in SQL, via ClassRank)
    -----------------------------------
    The combined query computes the opening RBD per flight: among open classes
    (``ClassAU > 0``) that are still sellable net of current load
    (``ClassAU - bookedPax > 0``), the opening RBD is the one with the LOWEST
    ``ILC.ClassRank``. ClassRank is the inventory class ordering maintained by
    Navitaire, so we use it directly rather than a hard-coded class list.

    Batched by 30 days
    ------------------
    The full 0-365 query is heavy (~20-40s). It is split into 30-day batches
    (0-30, 31-60, ...) over the configured window; each batch is a single
    combined query and its results are upserted before the next batch runs.

    Storage: MongoDB (not Redis)
    ----------------------------
    ~72k flights are written to ``qp_rm_master.nav_flight_data`` as one document
    per flight, upserted by ``flight_key`` (non-destructive: existing docs are
    updated, never dropped). ``flight_key`` matches the schedule30 convention:
        ``{DepartureStation}{ArrivalStation}_{FlightNumber}_{DepartureDate}``.

    QP fares
    --------
    QP fares are read once from ``qp_dw_rmalloc.fares`` (route ``L``) into a
    ``{sector: {rbd: total}}`` lookup; each flight stores only ``qp_fare`` --
    the fare for that flight's opening RBD (``None`` if no fare is configured).
    """

    TARGET_COLLECTION = "nav_flight_data"

    # Navitaire query retry policy.
    MAX_RETRIES = 3
    RETRY_BACKOFF_SECONDS = 5

    # Batch sizing.
    BATCH_DAYS = 30            # departure-date window per Navitaire query
    MONGO_BATCH_SIZE = 2000    # docs per bulk_write

    def __init__(self, logger, params, overrides, context):
        self.logger = logger or logging.getLogger()
        self.context = context
        self.overrides = overrides or {}

        self.mongoconn = params["mongoconn"]
        self.mongo_master_db = params["mongo_master_db"]  # qp_rm_master
        self.nav_conn = params["navconn"]

        self.db = self.mongoconn["qp_dw_rmalloc"]
        self.nav_db_name = configsettings.NAV_DB_NAME

        self.constants = ConstantsUtils(self.db)

        # Available-RBD classes come from the configurable constant (CSV string).
        self.available_rbd_classes = [
            c.strip() for c in self.constants.B2C_RBD_VALUES.split(",") if c.strip()
        ]

        self.collection = self.mongo_master_db[self.TARGET_COLLECTION]

        # ---- Resolve the full NDO / departure-date window from overrides ----
        self.start_interval = int(self.overrides.get("start_interval", 0))
        self.end_interval = int(self.overrides.get("end_interval", 365))

        today = datetime.now(IST).date()

        departure_start = self.overrides.get("departure_start_date")
        departure_end = self.overrides.get("departure_end_date")

        if departure_start and departure_end:
            self.departure_start_date = datetime.strptime(
                departure_start, "%Y-%m-%d"
            ).date()
            self.departure_end_date = datetime.strptime(
                departure_end, "%Y-%m-%d"
            ).date()
        else:
            self.departure_start_date = today + timedelta(days=self.start_interval)
            self.departure_end_date = today + timedelta(days=self.end_interval)

    # ------------------------------------------------------------------ #
    # Key helpers
    # ------------------------------------------------------------------ #
    @staticmethod
    def generate_flight_key(row):
        """Unique per-flight key, matching the schedule30 convention."""
        flight_number = str(row["FlightNumber"]).strip()
        departure_date = row["DepartureDate"]
        if not isinstance(departure_date, str):
            departure_date = departure_date.strftime("%Y-%m-%d")
        return f"{row['Origin']}{row['Destination']}_{flight_number}_{departure_date}"

    def _date_batches(self):
        """Yield (batch_start, batch_end) date tuples of at most BATCH_DAYS."""
        cur = self.departure_start_date
        while cur <= self.departure_end_date:
            batch_end = min(
                cur + timedelta(days=self.BATCH_DAYS - 1), self.departure_end_date
            )
            yield cur, batch_end
            cur = batch_end + timedelta(days=1)

    # ------------------------------------------------------------------ #
    # Navitaire fetch (combined query, with retry)
    # ------------------------------------------------------------------ #
    def _read_sql_with_retry(self, query, label):
        """Run a Navitaire query with up to ``MAX_RETRIES`` attempts."""
        last_exc = None
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                df = pd.read_sql(query, self.nav_conn)
                if attempt > 1:
                    self.logger.info(
                        "%s query succeeded on attempt %s/%s",
                        label, attempt, self.MAX_RETRIES,
                    )
                return df
            except Exception as e:
                last_exc = e
                self.logger.warning(
                    "%s query failed on attempt %s/%s: %s",
                    label, attempt, self.MAX_RETRIES, str(e),
                )
                if attempt < self.MAX_RETRIES:
                    time.sleep(self.RETRY_BACKOFF_SECONDS * attempt)

        self.logger.error(
            "%s query exhausted %s retries: %s", label, self.MAX_RETRIES, str(last_exc)
        )
        raise last_exc

    def get_bookload_with_opening_rbd(self, batch_start, batch_end):
        """Combined query: per-flight bookload + opening RBD for a date batch.

        Opening RBD = the sellable open class (ClassAU > 0 AND
        ClassAU - bookedPax > 0) with the LOWEST ILC.ClassRank.
        """
        class_list = ",".join(f"'{c}'" for c in self.available_rbd_classes)
        start_str = batch_start.strftime("%Y-%m-%d")
        end_str = batch_end.strftime("%Y-%m-%d")

        query = f"""
        WITH Bookload AS (
            SELECT
                A.Origin,
                A.Destination,
                TRIM(A.FlightNumber) AS FlightNumber,
                A.DepartureDate,
                A.InventoryLegID,
                FORMAT(A.STD, 'HH:mm') AS DepartureTime,
                COUNT(A.PassengerID) AS bookedPax,
                A.AdjustedCapacity AS adjustedCapacity
            FROM
                (
                    SELECT
                        BP.PassengerID,
                        IL.InventoryLegID,
                        IL.DepartureStation AS Origin,
                        IL.ArrivalStation AS Destination,
                        IL.DepartureDate,
                        IL.FlightNumber,
                        IL.AdjustedCapacity,
                        IL.Lid,
                        IL.STD
                    FROM
                        {self.nav_db_name}[rez].InventoryLeg IL
                        LEFT OUTER JOIN {self.nav_db_name}[rez].PassengerJourneyLeg PJL
                            ON PJL.InventoryLegID = IL.InventoryLegID
                        LEFT OUTER JOIN {self.nav_db_name}[rez].PassengerJourneySegment PJS
                            ON PJS.PassengerID = PJL.PassengerID
                            AND PJS.SegmentID = PJL.SegmentID
                            AND (PJS.FareBasis != 'SO7NBSOL' OR PJS.FareBasis IS NULL)
                        LEFT OUTER JOIN {self.nav_db_name}[rez].BookingPassenger BP
                            ON BP.PassengerID = PJS.PassengerID
                    WHERE
                        IL.Lid > 0
                        AND IL.Status NOT IN (2)
                        AND IL.DepartureDate >= '{start_str}'
                        AND IL.DepartureDate <= '{end_str}'
                ) A
            GROUP BY
                A.Origin, A.Destination, A.FlightNumber, A.DepartureDate,
                A.InventoryLegID, A.STD, A.AdjustedCapacity
        ),
        RankedRbd AS (
            SELECT
                B.InventoryLegID,
                ILC.ClassOfService,
                ILC.ClassAU,
                ILC.ClassRank,
                -- Lowest-rank sellable class (opening RBD on the happy path).
                ROW_NUMBER() OVER (
                    PARTITION BY B.InventoryLegID
                    ORDER BY
                        CASE WHEN (ILC.ClassAU - B.bookedPax) > 0 THEN 0 ELSE 1 END ASC,
                        ILC.ClassRank ASC
                ) AS sellable_rn,
                -- Highest-rank open class (fallback when overbooked => none sellable).
                ROW_NUMBER() OVER (
                    PARTITION BY B.InventoryLegID
                    ORDER BY ILC.ClassRank DESC
                ) AS highest_rn,
                CASE WHEN (ILC.ClassAU - B.bookedPax) > 0 THEN 1 ELSE 0 END AS is_sellable
            FROM Bookload B
                JOIN {self.nav_db_name}[rez].InventoryLegClass ILC
                    ON ILC.InventoryLegID = B.InventoryLegID
            WHERE ILC.ClassAU > 0
                AND ILC.ClassOfService IN ({class_list})
        ),
        -- One opening RBD per leg: lowest-rank sellable class if any is
        -- sellable, otherwise (overbooked) the highest-rank open class.
        OpeningRbd AS (
            SELECT
                r.InventoryLegID,
                r.ClassOfService,
                r.ClassAU,
                r.ClassRank
            FROM RankedRbd r
            WHERE
                (EXISTS (SELECT 1 FROM RankedRbd s
                         WHERE s.InventoryLegID = r.InventoryLegID AND s.is_sellable = 1)
                 AND r.is_sellable = 1 AND r.sellable_rn = 1)
                OR
                (NOT EXISTS (SELECT 1 FROM RankedRbd s
                             WHERE s.InventoryLegID = r.InventoryLegID AND s.is_sellable = 1)
                 AND r.highest_rn = 1)
        )
        SELECT
            B.Origin,
            B.Destination,
            B.FlightNumber,
            B.DepartureDate,
            B.DepartureTime,
            B.bookedPax,
            B.adjustedCapacity,
            O.ClassOfService AS OpeningRBD,
            O.ClassAU        AS OpeningRBD_ClassAU,
            O.ClassRank      AS OpeningRBD_ClassRank
        FROM Bookload B
            LEFT OUTER JOIN OpeningRbd O
                ON O.InventoryLegID = B.InventoryLegID
        ORDER BY
            B.Origin, B.Destination, B.DepartureDate;
        """

        df = self._read_sql_with_retry(query, label=f"Bookload[{start_str}:{end_str}]")
        if df.empty:
            return df
        df["FlightNumber"] = df["FlightNumber"].astype(str).str.strip()
        df["flight_key"] = df.apply(self.generate_flight_key, axis=1)
        return df

    # ------------------------------------------------------------------ #
    # QP fares (from qp_rm_master.fares, mirrors backend getQpFaresBatch)
    # ------------------------------------------------------------------ #
    def get_qp_fares(self):
        """Return ``{sector: {rbd: total}}`` for route 'L' QP fares.

        Read from ``qp_dw_rmalloc.fares`` (SECTOR / RBD / TOTAL, route 'L').
        """
        fare_map = {}
        try:
            cursor = self.db["fares"].find(
                {"route": "L"},
                {"_id": 0, "SECTOR": 1, "RBD": 1, "TOTAL": 1},
            )
            for doc in cursor:
                sector = doc.get("SECTOR")
                rbd = doc.get("RBD")
                total = doc.get("TOTAL")
                if sector is None or rbd is None or total is None:
                    continue
                try:
                    total = float(total)
                except (ValueError, TypeError):
                    continue
                fare_map.setdefault(sector, {})[rbd] = total
            self.logger.info("Loaded QP fares for %s sectors", len(fare_map))
        except Exception as e:
            self.logger.warning("QP fares fetch failed (%s); storing without fares.", str(e))
        return fare_map

    # ------------------------------------------------------------------ #
    # Payload shaping
    # ------------------------------------------------------------------ #
    @staticmethod
    def _safe_int(value, default=0):
        try:
            if value is None or (isinstance(value, float) and pd.isna(value)):
                return default
            return int(value)
        except (ValueError, TypeError):
            return default

    @staticmethod
    def _clean_str(value):
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        return str(value).strip()

    def _build_doc(self, flight, cached_at_ist, qp_fares):
        """Build the MongoDB document stored for a single flight."""
        departure_date = flight["DepartureDate"]
        if not isinstance(departure_date, str):
            departure_date = pd.to_datetime(departure_date).strftime("%Y-%m-%d")

        origin = flight["Origin"]
        destination = flight["Destination"]
        sector = f"{origin}{destination}"

        opening_rbd = self._clean_str(flight.get("OpeningRBD"))

        # Fare for the opening RBD only (not the whole ladder).
        qp_fare = None
        if opening_rbd is not None:
            qp_fare = qp_fares.get(sector, {}).get(opening_rbd)

        return {
            "flight_key": flight["flight_key"],
            "sector": sector,
            "flight_number": str(flight["FlightNumber"]).strip(),
            "departure_station": origin,
            "arrival_station": destination,
            "departure_date": departure_date,
            "departure_time_ist": self._clean_str(flight.get("DepartureTime")),
            "booked_pax": self._safe_int(flight.get("bookedPax")),
            "adjusted_capacity": self._safe_int(flight.get("adjustedCapacity")),
            "opening_rbd": opening_rbd,
            "opening_rbd_class_au": self._safe_int(flight.get("OpeningRBD_ClassAU"), None),
            "opening_rbd_class_rank": self._safe_int(flight.get("OpeningRBD_ClassRank"), None),
            "qp_fare": qp_fare,
            "cached_at_ist": cached_at_ist,
        }

    # ------------------------------------------------------------------ #
    # MongoDB write (non-destructive upsert)
    # ------------------------------------------------------------------ #
    def upsert_flights(self, df, cached_at_ist, qp_fares):
        """Bulk-upsert flight docs by flight_key. Returns count written."""
        ops = []
        written = 0
        for _, flight in df.iterrows():
            doc = self._build_doc(flight, cached_at_ist, qp_fares)
            ops.append(
                UpdateOne(
                    {"flight_key": doc["flight_key"]},
                    {"$set": doc},
                    upsert=True,
                )
            )
            if len(ops) >= self.MONGO_BATCH_SIZE:
                self.collection.bulk_write(ops, ordered=False)
                written += len(ops)
                ops = []
        if ops:
            self.collection.bulk_write(ops, ordered=False)
            written += len(ops)
        return written

    def _write_meta(self, cached_at_ist, flight_count, refresh_failed, note=""):
        """Upsert a single metadata doc so staleness is always recorded."""
        meta = {
            "ndo_start": self.start_interval,
            "ndo_end": self.end_interval,
            "departure_start_date": self.departure_start_date.strftime("%Y-%m-%d"),
            "departure_end_date": self.departure_end_date.strftime("%Y-%m-%d"),
            "flight_count": flight_count,
            "cached_at_ist": cached_at_ist,
            "last_refresh_failed": refresh_failed,
            "note": note,
        }
        try:
            self.mongo_master_db["nav_flight_data_meta"].update_one(
                {"_id": "navitaire_cache"}, {"$set": meta}, upsert=True
            )
        except Exception as e:
            self.logger.error(f"Error writing nav cache meta: {str(e)}")

    # ------------------------------------------------------------------ #
    # Entry point
    # ------------------------------------------------------------------ #
    def run(self):
        """Fetch in 30-day batches and upsert to qp_rm_master.nav_flight_data."""
        self.logger.info(
            "Starting Navitaire cache refresh (%s to %s) in %s-day batches",
            self.departure_start_date, self.departure_end_date, self.BATCH_DAYS,
        )

        cached_at_ist = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")

        # QP fares fetched once and reused across all batches.
        qp_fares = self.get_qp_fares()

        total_written = 0
        failed_batches = []

        for batch_start, batch_end in self._date_batches():
            try:
                df = self.get_bookload_with_opening_rbd(batch_start, batch_end)
            except Exception as e:
                # This batch's existing docs stay untouched (stale). Record and
                # continue with other batches so one failure isn't fatal.
                failed_batches.append(f"{batch_start}:{batch_end} ({e})")
                self.logger.error(
                    "Batch %s..%s failed after retries; leaving its docs stale.",
                    batch_start, batch_end,
                )
                continue

            if df.empty:
                self.logger.info("Batch %s..%s returned no rows.", batch_start, batch_end)
                continue

            try:
                written = self.upsert_flights(df, cached_at_ist, qp_fares)
                total_written += written
                self.logger.info(
                    "Batch %s..%s: upserted %s flights", batch_start, batch_end, written
                )
            except Exception:
                failed_batches.append(f"{batch_start}:{batch_end} (mongo write)")
                self.logger.error("Mongo write failed for batch %s..%s", batch_start, batch_end)
                traceback.print_exc()

        refresh_failed = bool(failed_batches)
        note = "" if not refresh_failed else "failed batches: " + "; ".join(failed_batches)
        self._write_meta(
            cached_at_ist=cached_at_ist,
            flight_count=total_written,
            refresh_failed=refresh_failed,
            note=note,
        )

        self.logger.info(
            "Navitaire cache refresh done: %s flights upserted, %s failed batches",
            total_written, len(failed_batches),
        )
        return not refresh_failed
