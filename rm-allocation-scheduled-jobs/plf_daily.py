"""
plf_daily.py

Lambda-format scheduled job.

Lightweight extract of PLF and Expected_PLF for every flight key from the
current day through the next 180 days.

Logic is lifted directly from fare_plf_forecast.py:
  * PLF           -> booked passengers (QP_DL_BOOKEDDATAPAX, excl. SO7NBSOL)
                     over AdjustedCapacity (QP_DL_InventoryLeg), * 100.
  * Expected_PLF  -> curve-based target from MongoDB market_list + curves,
                     branching on NDO (>=22 / 1-21 / same-day).

Only the fields required for these two metrics are fetched; fare / TBF /
tax / cancellation logic from the original job is intentionally omitted.

Output is written to the MongoDB `plf_daily` collection (qp_dw_rmalloc),
replacing the previous snapshot on each run.
"""

import logging
import traceback
from datetime import datetime, timedelta, time
from uuid import uuid4

import numpy as np
import pandas as pd
import pytz
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from constants_utils import ConstantsUtils
import configsettings



class PlfDaily:
    """Extract PLF and Expected_PLF per flight key for the next 180 days."""

    def __init__(self, logger, params, overrides, context):
        """Initialize connections and settings."""
        self.logger = logger or logging.getLogger()
        self.overrides = overrides or {}

        # ESB (MySQL) read engine - used for booked/flight data pulls.
        self.esb_engine = params['rdEsbConn']
        # Single MongoDB connection for application DB (market_list / curves).
        self.mongoconn = params['mongoconn']
        self.db = self.mongoconn['qp_dw_rmalloc']
        self.constants = ConstantsUtils(self.db)
        # Destination MongoDB collection for the extract.
        self.output_collection = 'plf_daily'

        # Schema / table references.
        self.esb_read_schema = configsettings.ESB_READ_SCHEMA
        self.passenger_detail_table = f'{self.esb_read_schema}.QP_DL_BOOKEDDATAPAX'
        self.inventory_leg_table = f'{self.esb_read_schema}.QP_DL_InventoryLeg'

        # Horizon window (allow override).
        self.horizon_days = int(self.overrides.get('horizon_days', self.constants.PREDICTION_HORIZON_DAYS))
        self.today = datetime.now()
        self.forecast_end = self.today + timedelta(days=self.horizon_days)

        self._data_cache = {}

    # ---- helpers -----------------------------------------------------------
    def get_current_ist_time(self):
        ist = pytz.timezone('Asia/Kolkata')
        return datetime.now(ist)

    # ---- booked data -> PLF ------------------------------------------------
    def _fetch_booked_data(self):
        """
        Reproduce the PLF portion of fare_plf_forecast._fetch_data_from_db.
        Returns one row per flight key with PLF, Capacity, NDO_Curr, STD.
        """
        engine = self.esb_engine
        params = {
            'start_date': self.today.strftime('%Y-%m-%d'),
            'end_date': self.forecast_end.strftime('%Y-%m-%d'),
        }

        self.logger.info(
            f"Fetching passenger details between {params['start_date']} and {params['end_date']}"
        )

        # Passenger-level pull (only columns needed for PLF).
        pax_details_query = f"""
        SELECT
            FD.PassengerID,
            FD.DepartureDate,
            FD.FareBasis,
            FD.Origin,
            FD.Destination,
            TRIM(FD.FlightNumber) AS FlightNumber,
            IL.AdjustedCapacity AS Capacity
        FROM {self.passenger_detail_table} FD
        INNER JOIN {self.inventory_leg_table} IL
            ON IL.InventoryLegID = FD.InventoryLegID
        WHERE
            FD.DepartureDate BETWEEN :start_date AND :end_date
        """
        try:
            with engine.connect() as connection:
                chunks = pd.read_sql(text(pax_details_query), connection, params=params, chunksize=20000)
                df_pax = pd.concat([c for c in chunks]) if chunks else pd.DataFrame()
        except OperationalError as e:
            raise RuntimeError(f"Could not fetch passenger details: {e}") from e

        if df_pax is None or df_pax.empty:
            self.logger.warning("No passenger details fetched. PLF will come from zero bookings.")
            df_pax = pd.DataFrame(columns=['PassengerID', 'DepartureDate', 'FareBasis',
                                           'Origin', 'Destination', 'FlightNumber', 'Capacity'])

        # Exclude SO7NBSOL exactly like the source job.
        if not df_pax.empty:
            df_non_s = df_pax[df_pax['FareBasis'] != 'SO7NBSOL'].copy()
        else:
            df_non_s = pd.DataFrame()

        if not df_non_s.empty:
            df_counts = df_non_s.groupby(
                ['DepartureDate', 'Origin', 'Destination', 'FlightNumber'],
                as_index=False
            ).agg(PassengerCount=('PassengerID', 'count'))
        else:
            df_counts = pd.DataFrame(
                columns=['DepartureDate', 'Origin', 'Destination', 'FlightNumber', 'PassengerCount']
            )

        # Flight attributes (all scheduled flights, carrier QP, not cancelled).
        flight_attrs_query = f"""
        SELECT DISTINCT
            IL.DepartureDate,
            TRIM(IL.FlightNumber) AS FlightNumber,
            IL.DepartureStation AS Origin,
            IL.ArrivalStation AS Destination,
            IL.AdjustedCapacity AS Capacity,
            CAST(IL.STD AS TIME) AS STD
        FROM {self.inventory_leg_table} IL
        WHERE IL.DepartureDate BETWEEN :start_date AND :end_date
            AND IL.CarrierCode = 'QP'
            AND IL.Status != 2
        """
        try:
            with engine.connect() as connection:
                df_attrs = pd.read_sql(text(flight_attrs_query), connection, params=params)
        except OperationalError as e:
            raise RuntimeError(f"Could not fetch flight attributes: {e}") from e

        if df_attrs.empty:
            self.logger.warning("No flight attributes fetched. Returning empty booked data.")
            return pd.DataFrame(columns=[
                'DepartureDate', 'Origin', 'Destination', 'FlightNumber',
                'Capacity', 'PLF', 'STD', 'NDO_Curr'
            ])

        df_attrs['DepartureDate'] = pd.to_datetime(df_attrs['DepartureDate'])
        if not df_counts.empty:
            df_counts['DepartureDate'] = pd.to_datetime(df_counts['DepartureDate'])

        df = pd.merge(
            df_attrs, df_counts,
            on=['DepartureDate', 'Origin', 'Destination', 'FlightNumber'],
            how='left'
        )

        df['PassengerCount'] = df['PassengerCount'].fillna(0).astype(int)
        df['Capacity'] = pd.to_numeric(df['Capacity'], errors='coerce').fillna(0).astype(int)
        df['PLF'] = np.where(
            df['Capacity'] > 0,
            df['PassengerCount'] * 100.0 / df['Capacity'],
            0
        )
        df['PLF'] = df['PLF'].round(1)
        df['FlightNumber'] = df['FlightNumber'].astype(str).str.strip()

        # NDO_Curr - days until departure (needed for Expected_PLF).
        df['NDO_Curr'] = (df['DepartureDate'].dt.date - self.today.date()).apply(lambda d: d.days)

        df = df.sort_values(
            by=['DepartureDate', 'Origin', 'Destination', 'FlightNumber']
        ).reset_index(drop=True)

        self.logger.info(f"Processed booked data: {len(df)} flight records.")
        return df

    # ---- market / curves (for Expected_PLF) --------------------------------
    def _fetch_market_curves_data(self):
        self.logger.info("Fetching market list and curves data...")
        try:
            db = self.db

            df_market = pd.DataFrame(list(
                db.market_list.find(
                    {"CurveID": {"$ne": None, "$ne": ""}},
                    {"Origin": 1, "Destin": 1, "FlightNumber": 1, "CurveID": 1,
                     "PerStart": 1, "PerEnd": 1, "DOW": 1, "_id": 0}
                )
            ))

            if 'Destin' in df_market.columns:
                df_market = df_market.rename(columns={'Destin': 'Destination'})

            if df_market.empty:
                self.logger.warning("No market_list data found.")
                self._data_cache['market_list'] = pd.DataFrame()
                self._data_cache['curves'] = pd.DataFrame()
                return

            df_market['FlightNumber'] = df_market['FlightNumber'].astype(str).str.strip()
            df_market['Origin'] = df_market['Origin'].astype(str).str.strip()
            df_market['Destination'] = df_market['Destination'].astype(str).str.strip()
            self._data_cache['market_list'] = df_market
            self.logger.info(f"Fetched {len(df_market)} market list records.")

            curve_ids = df_market['CurveID'].unique().tolist()
            if not curve_ids:
                self._data_cache['curves'] = pd.DataFrame()
                return

            curves_cursor = db['curves'].find(
                {"CurveID": {"$in": curve_ids},
                 "NDO": {"$ne": None}, "LF": {"$ne": None}},
                {"NDO": 1, "CurveID": 1, "LF": 1, "_id": 0}
            )
            df_curves = pd.DataFrame(list(curves_cursor))

            if df_curves.empty:
                self.logger.warning("No curves data found.")
                self._data_cache['curves'] = pd.DataFrame()
                return

            df_curves.rename(columns={"LF": "LoadFactor"}, inplace=True)
            df_curves['NDO'] = pd.to_numeric(df_curves['NDO'], errors='coerce')
            df_curves['LoadFactor'] = pd.to_numeric(df_curves['LoadFactor'], errors='coerce')
            df_curves = df_curves.dropna(subset=['NDO', 'LoadFactor'])
            self._data_cache['curves'] = df_curves
            self.logger.info(f"Fetched {len(df_curves)} curves records.")

        except Exception as e:
            self.logger.error(f"Error fetching market curves data: {e}")
            self.logger.error(traceback.format_exc())
            self._data_cache['market_list'] = pd.DataFrame()
            self._data_cache['curves'] = pd.DataFrame()

    def _get_area_for_hour(self, hour):
        """Cumulative share of booking up to the given hour (config_share_of_booking)."""
        try:
            db = self.db
            pipeline = [
                {"$match": {"hours": {"$lte": hour}}},
                {"$group": {"_id": None, "sum": {"$sum": "$shareOfBooking"}}}
            ]
            result = list(db.config_share_of_booking.aggregate(pipeline))
            if result and result[0].get('sum') is not None:
                return float(result[0]['sum']) / 100
            return 1.0
        except Exception as e:
            self.logger.debug(f"Error getting area for hour {hour}: {e}")
            return 1.0

    def _parse_departure_time(self, std_time, departure_date):
        try:
            if pd.isna(std_time):
                return None
            if isinstance(departure_date, str):
                date_part = datetime.strptime(departure_date, '%Y-%m-%d').date()
            else:
                date_part = departure_date
            if isinstance(std_time, str):
                time_part = datetime.strptime(std_time, '%H:%M:%S').time()
            elif isinstance(std_time, timedelta):
                time_part = (datetime.min + std_time).time()
            elif isinstance(std_time, time):
                time_part = std_time
            else:
                return None
            return datetime.combine(date_part, time_part)
        except Exception as e:
            self.logger.debug(f"Error parsing departure time: {e}")
            return None

    # ---- Expected_PLF (verbatim logic from source job) ---------------------
    def _get_expected_plf(self, row):
        try:
            if self._data_cache.get('market_list') is None or self._data_cache['market_list'].empty:
                return np.nan
            if self._data_cache.get('curves') is None or self._data_cache['curves'].empty:
                return np.nan

            origin = str(row['Origin']).strip()
            destination = str(row['Destination']).strip()
            flight_number = str(row['FlightNumber']).strip()
            departure_date = str(row['DepartureDate']).strip()
            formatted_date = datetime.strptime(departure_date, '%Y-%m-%d').strftime('%m/%d/%Y')
            ndo_curr = row.get('NDO_Curr')
            capacity = row.get('Capacity', 0)
            std_time = row.get('STD')

            if pd.isna(ndo_curr):
                return np.nan

            market_match = self._data_cache['market_list'][
                (self._data_cache['market_list']['Origin'] == origin) &
                (self._data_cache['market_list']['Destination'] == destination) &
                (self._data_cache['market_list']['FlightNumber'] == flight_number) &
                (self._data_cache['market_list']['PerStart'] == formatted_date)
            ]

            if market_match.empty:
                market_match = self._data_cache['market_list'][
                    (self._data_cache['market_list']['Origin'] == origin) &
                    (self._data_cache['market_list']['Destination'] == destination)
                ]

            if market_match.empty:
                return np.nan

            curve_id = market_match.iloc[0]['CurveID']
            if pd.isna(curve_id) or curve_id == '':
                return np.nan

            if ndo_curr >= 22:
                lookup_ndo = int(ndo_curr) + 1
                curve_match = self._data_cache['curves'][
                    (self._data_cache['curves']['CurveID'] == curve_id) &
                    (self._data_cache['curves']['NDO'] == lookup_ndo)
                ]
                if curve_match.empty:
                    curve_for_id = self._data_cache['curves'][
                        self._data_cache['curves']['CurveID'] == curve_id
                    ]
                    if not curve_for_id.empty:
                        closest_ndo = curve_for_id.iloc[
                            (curve_for_id['NDO'] - lookup_ndo).abs().argsort()
                        ].iloc[0]
                        return closest_ndo['LoadFactor'] * 100
                    return np.nan
                return curve_match.iloc[0]['LoadFactor'] * 100

            elif 1 <= ndo_curr <= 21:
                return self._calculate_area_based_expected_plf(row, curve_id, capacity, int(ndo_curr))
            else:
                return self._calculate_same_day_expected_plf(row, curve_id, capacity, std_time)

        except Exception as e:
            self.logger.debug(
                f"Error getting expected PLF for {row.get('Origin', 'N/A')}-"
                f"{row.get('Destination', 'N/A')} flight {row.get('FlightNumber', 'N/A')}: {e}"
            )
            return np.nan

    def _calculate_area_based_expected_plf(self, row, curve_id, capacity, ndo_curr):
        try:
            start_curve_match = self._data_cache['curves'][
                (self._data_cache['curves']['CurveID'] == curve_id) &
                (self._data_cache['curves']['NDO'] == ndo_curr + 1)
            ]
            if start_curve_match.empty:
                start_curve_match = self._data_cache['curves'][
                    (self._data_cache['curves']['CurveID'] == curve_id) &
                    (self._data_cache['curves']['NDO'] == ndo_curr)
                ]
            if start_curve_match.empty:
                return np.nan

            start_of_day_book_load = start_curve_match.iloc[0]['LoadFactor']
            start_of_day_tgt_booked_load = start_of_day_book_load * capacity

            end_curve_match = self._data_cache['curves'][
                (self._data_cache['curves']['CurveID'] == curve_id) &
                (self._data_cache['curves']['NDO'] == ndo_curr)
            ]
            if end_curve_match.empty:
                return np.nan

            end_of_day_book_load = end_curve_match.iloc[0]['LoadFactor']
            end_of_day_tgt_booked_load = end_of_day_book_load * capacity

            current_time = datetime.now()
            area_at_current_time = self._get_area_for_hour(current_time.hour)

            tgt_at_current_hour = (
                (end_of_day_tgt_booked_load - start_of_day_tgt_booked_load) * area_at_current_time
                + start_of_day_tgt_booked_load
            )
            expected_plf = (tgt_at_current_hour / capacity) * 100 if capacity else np.nan
            return min(expected_plf, 100)
        except Exception as e:
            self.logger.debug(f"Error calculating area-based expected PLF for NDO {ndo_curr}: {e}")
            return np.nan

    def _calculate_same_day_expected_plf(self, row, curve_id, capacity, std_time):
        try:
            END_OF_DAY_LF_FACTOR = 0.95

            start_curve_match = self._data_cache['curves'][
                (self._data_cache['curves']['CurveID'] == curve_id) &
                (self._data_cache['curves']['NDO'] == 1)
            ]
            if start_curve_match.empty:
                start_curve_match = self._data_cache['curves'][
                    (self._data_cache['curves']['CurveID'] == curve_id) &
                    (self._data_cache['curves']['NDO'] == 0)
                ]
            if start_curve_match.empty:
                return np.nan

            end_curve_match = self._data_cache['curves'][
                (self._data_cache['curves']['CurveID'] == curve_id) &
                (self._data_cache['curves']['NDO'] == 0)
            ]
            if end_curve_match.empty:
                return np.nan

            start_of_day_book_load = start_curve_match.iloc[0]['LoadFactor']
            start_of_day_tgt_booked_load = start_of_day_book_load * capacity

            end_of_day_book_load = end_curve_match.iloc[0]['LoadFactor']
            end_of_day_tgt_booked_load = end_of_day_book_load * capacity
            end_of_day_tgt_booked_load = max(capacity * END_OF_DAY_LF_FACTOR, end_of_day_tgt_booked_load)

            current_time = self.get_current_ist_time()
            departure_time = self._parse_departure_time(std_time, row['DepartureDate'])
            if departure_time is None:
                return np.nan

            area_current_time = self._get_area_for_hour(current_time.hour)
            hours_2hrs_before_departure = max(0, (departure_time - timedelta(hours=2)).hour)
            area_2hrs_departure = self._get_area_for_hour(hours_2hrs_before_departure)

            if area_2hrs_departure > 0:
                area_ratio = area_current_time / area_2hrs_departure
                if area_current_time > area_2hrs_departure:
                    area_ratio = 1
            else:
                area_ratio = 1

            area_at_current_time = area_ratio
            tgt_at_current_hour = (
                (end_of_day_tgt_booked_load - start_of_day_tgt_booked_load) * area_at_current_time
                + start_of_day_tgt_booked_load
            )
            expected_plf = (tgt_at_current_hour / capacity) * 100 if capacity else np.nan
            return min(expected_plf, 100)
        except Exception as e:
            self.logger.debug(f"Error calculating same day expected PLF: {e}")
            return np.nan

    # ---- output ------------------------------------------------------------
    def _build_result(self):
        """Fetch, compute and return the final PLF / Expected_PLF frame."""
        # Curve data first so Expected_PLF can resolve.
        self._fetch_market_curves_data()

        booked = self._fetch_booked_data()
        if booked is None or booked.empty:
            self.logger.error("No booked/flight data available. Nothing to output.")
            return pd.DataFrame()

        # DepartureDate as plain date string for Expected_PLF matching.
        booked['DepartureDate'] = pd.to_datetime(booked['DepartureDate']).dt.strftime('%Y-%m-%d')

        self.logger.info("Calculating Expected_PLF per flight key...")
        booked['Expected_PLF'] = booked.apply(self._get_expected_plf, axis=1)

        result = booked[[
            'DepartureDate', 'Origin', 'Destination', 'FlightNumber',
            'PLF', 'Expected_PLF'
        ]].copy()

        result['PLF'] = result['PLF'].round(1)
        result['Expected_PLF'] = result['Expected_PLF'].round(2)
        result = result.sort_values(
            by=['DepartureDate', 'Origin', 'Destination', 'FlightNumber']
        ).reset_index(drop=True)

        exp_cov = result['Expected_PLF'].notna().sum()
        self.logger.info(
            f"Expected_PLF coverage: {exp_cov}/{len(result)} "
            f"({(exp_cov / len(result) * 100 if len(result) else 0):.1f}%)"
        )
        return result

    def store_in_mongo(self, df, version_id, run_date):
        """
        Persist the PLF / Expected_PLF extract into the plf_daily MongoDB
        collection. The collection is replaced on every run (truncate + insert)
        so it always holds the latest snapshot.
        """
        try:
            collection = self.db[self.output_collection]

            # Drop the existing snapshot (equivalent to TRUNCATE).
            collection.drop()

            if df is None or df.empty:
                self.logger.info("No records to insert into plf_daily.")
                return True

            records = df.to_dict('records')

            # Stamp each document with run metadata for traceability.
            for record in records:
                record['version_id'] = version_id
                record['run_date'] = run_date

            collection.insert_many(records)
            self.logger.info(
                f"Successfully inserted {len(records)} records into "
                f"{self.db.name}.{self.output_collection}"
            )
            return True
        except Exception as e:
            self.logger.error(f"Error storing data in MongoDB: {str(e)}")
            traceback.print_exc()
            return False

    # ---- orchestration -----------------------------------------------------
    def run(self):
        """Main execution method."""
        try:
            start = datetime.now()
            self.logger.info(
                f"Starting PLF / Expected_PLF extract: {self.today.date()} "
                f"-> {self.forecast_end.date()} ({self.horizon_days} days)."
            )

            result = self._build_result()

            if result is None or result.empty:
                self.logger.error("No data produced. Nothing to store.")
                return False

            version_id = str(uuid4())
            run_date = start.strftime('%Y-%m-%d')
            stored = self.store_in_mongo(result, version_id, run_date)

            self.logger.info(f"PLF daily extract done in {datetime.now() - start}.")
            return stored

        except Exception as e:
            self.logger.error(f"Error in PLF daily process: {str(e)}")
            traceback.print_exc()
            return False
