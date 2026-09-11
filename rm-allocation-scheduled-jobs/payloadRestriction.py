import pandas as pd
import logging
import configsettings
from constants_utils import ConstantsUtils
from common_utils import GraphMailClient
from datetime import datetime, timedelta
import traceback
import json
import boto3
import requests
from uuid import uuid4
import io

from sqlalchemy import text

from Constants import AKASA_AIRLINE_CODE, DATE_FORMAT, DATE_FORMAT_2


class NavInventoryClient:
    """Minimal NAV (Navitaire) inventory API client.

    Mirrors the contract of rm-allocation-process-inputs/helper/nav_api_helper.py
    but is kept self-contained here so the scheduled-jobs package does not need to
    import from another deployable. Handles token acquisition, fetching leg/nest
    keys from the manifest, and patching the leg LID + adjusted capacity.
    """

    def __init__(self, logger, NAV_BASE_API_URL, NAV_OCP_SUBSCRIPTION_KEY,
                 NAV_USERNAME, NAV_PWD, NAV_DOMAIN):
        self.logger = logger
        self.NAV_BASE_API_URL = NAV_BASE_API_URL
        self.NAV_OCP_SUBSCRIPTION_KEY = NAV_OCP_SUBSCRIPTION_KEY
        self.NAV_USERNAME = NAV_USERNAME
        self.NAV_PWD = NAV_PWD
        self.NAV_DOMAIN = NAV_DOMAIN
        self.base_url = f"{self.NAV_BASE_API_URL}/api"
        self.token = None
        self.session = requests.Session()
        self.session.headers.update(
            {"Ocp-Apim-Subscription-Key": self.NAV_OCP_SUBSCRIPTION_KEY}
        )
        self.get_token()

    def get_token(self):
        url = f"{self.base_url}/nsk/v2/token"
        credentials = {
            "credentials": {
                "username": self.NAV_USERNAME,
                "password": self.NAV_PWD,
                "domain": self.NAV_DOMAIN
            }
        }
        response = self.session.post(url, json=credentials)
        if response.status_code == 201:
            self.token = response.json().get('data', {}).get('token')
            self.session.headers.update({"Authorization": f"Bearer {self.token}"})
        else:
            self.logger.info("Failed to get NAV token: " + response.text)

    def get_leg_key(self, dep_date, carrier_code, identifier, origin, destin):
        """Resolve the leg key for a flight departing on dep_date for origin->destin.

        Returns (leg_key, error_reason). On success error_reason is None; on
        failure leg_key is None and error_reason describes why.
        """
        dep_date = datetime.strptime(str(dep_date), DATE_FORMAT).date().strftime(DATE_FORMAT_2)
        url = (
            f"{self.base_url}/nsk/v2/manifest?BeginDate={dep_date}&endDate={dep_date}"
            f"&CarrierCode={carrier_code}&Identifier={identifier}&FlightType=3"
        )
        response = self.session.get(url)
        if response.status_code != 200:
            reason = f"Manifest fetch failed (HTTP {response.status_code}): {response.text}"
            self.logger.info(reason)
            return None, reason

        data = response.json().get('data', [])
        for item in data:
            for journey in item.get('journeys', []):
                designator = journey.get('designator', {})
                if designator.get('destination') == destin and designator.get('origin') == origin:
                    for segment in journey.get('segments', []):
                        for leg in segment.get('legs', []):
                            return leg['legKey'], None
        reason = f"No matching leg found for {origin}-{destin} flight {identifier} on {dep_date}"
        self.logger.info(reason)
        return None, reason

    def get_leg_information(self, leg_key):
        """Fetch current leg information (lid, adjustedCapacity, capacity) for a leg key.

        Returns (info_dict, error_reason). info_dict has keys 'lid',
        'adjustedCapacity' and 'capacity' (the physical equipment capacity, may be
        None if absent). On failure info_dict is None.
        """
        url = f"{self.base_url}/dcs/v2/inventory/legs/{leg_key}"
        response = self.session.get(url)
        if response.status_code != 200:
            reason = f"Leg info fetch failed (HTTP {response.status_code}): {response.text}"
            self.logger.info(reason)
            return None, reason

        leg_information = response.json().get('data', {}).get('legInformation', {}) or {}
        return {
            'lid': leg_information.get('lid'),
            'adjustedCapacity': leg_information.get('adjustedCapacity'),
            'capacity': leg_information.get('capacity')
        }, None

    def update_leg_lid_capacity(self, leg_key, lid, adjusted_capacity):
        """Patch the leg with the Sales LID (lid) and Check-in LID (adjustedCapacity).

        Uses the same PATCH endpoint used by the allocation flow. Only the leg-level
        fields are updated; APs / nest / class structure are retained.

        Returns (success_bool, error_reason). On success error_reason is None.
        """
        url = f"{self.base_url}/dcs/v1/inventory/legs/{leg_key}"
        data = {
            "lid": int(lid),
            "adjustedCapacity": int(adjusted_capacity)
        }
        self.logger.info('payload restriction lid api called ' + str(url))
        self.logger.info('payload sent ' + str(data))

        response = self.session.patch(url, json=data)
        if response.status_code == 200:
            self.logger.info(
                f'leg {leg_key} updated: lid={lid}, adjustedCapacity={adjusted_capacity}'
            )
            return True, None
        else:
            reason = f"Leg patch failed (HTTP {response.status_code}): {response.text}"
            self.logger.info("Failed to update leg lid/capacity: " + response.text)
            return False, reason


class PayloadRestriction:
    """RM support payload restriction automation.

    Reads restriction inputs from qp_dw_rmalloc.config_payload_restriction (fields:
    flightNumber, origin, destination, depStart, depEnd, lid, adjustedCapacity).

    For each restriction, every flight of that sector departing within
    [depStart, depEnd] has its Sales LID (lid) and Check-in LID (adjustedCapacity)
    updated in NAV ODS via the inventory API (APs retained).

    The affected flights are then matched against the combined market list
    (market_list + market_list_international), written to
    market_list_payload_restriction, versioned, uploaded to S3, and a downstream
    adhoc reallocation Lambda is triggered. An email notification is sent to FA.
    """

    RUN_TYPE = "Payload_Restriction"
    MARKET_LIST_TABLE_NAME = "market_list_payload_restriction"
    S3_SUBFOLDER = "market_list_payload_restriction"

    AUDIT_COLLECTION = "payload_restriction_audits"

    def __init__(self, logger, params, overrides, context):
        """Initialize connections and settings."""
        self.logger = logger or logging.getLogger()
        self.mongoconn = params['mongoconn']
        self.nav_conn = params['navconn']  # Navitaire SQL Server (NAV ODS)
        self.s3_client = params['s3']
        self.lambda_client = params['lambda_client']

        # Application MongoDB database
        self.db = self.mongoconn["qp_dw_rmalloc"]
        self.nav_db_name = configsettings.NAV_DB_NAME

        # ConstantsUtils for system parameters
        self.constants = ConstantsUtils(self.db)

        # Who triggered this run (comes in on the invocation payload / overrides)
        self.overrides = overrides or {}
        self.analyst_name = self.overrides.get('analystName', 'System')

        # NAV inventory API client (lazy-initialized on first use)
        self._nav_client = None

    # -------------------------------------------------------------------------
    # NAV client
    # -------------------------------------------------------------------------

    def _get_nav_client(self):
        """Lazily create the NAV inventory API client."""
        if self._nav_client is None:
            self._nav_client = NavInventoryClient(
                self.logger,
                configsettings.NAV_BASE_URL,
                configsettings.NAV_OCP_SUBSCRIPTION_KEY,
                configsettings.NAV_USERNAME,
                configsettings.NAV_PWD,
                configsettings.NAV_DOMAIN
            )
        return self._nav_client

    # -------------------------------------------------------------------------
    # Fetch restriction inputs
    # -------------------------------------------------------------------------

    def get_payload_restrictions(self):
        """
        Read restriction inputs from config_payload_restriction.
        Returns a DataFrame with columns:
        flightNumber, origin, destination, depStart, depEnd, lid, adjustedCapacity.
        """
        try:
            collection = self.db['config_payload_restriction']
            projection = {
                '_id': 0,
                'flightNumber': 1,
                'origin': 1,
                'destination': 1,
                'depStart': 1,
                'depEnd': 1,
                'lid': 1,
                'adjustedCapacity': 1
            }
            docs = list(collection.find({}, projection))

            if not docs:
                self.logger.info("No records found in config_payload_restriction")
                return pd.DataFrame()

            df = pd.DataFrame(docs)

            # Normalize key string fields
            for col in ['flightNumber', 'origin', 'destination']:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.strip()

            self.logger.info(f"Fetched {len(df)} payload restriction records")
            return df

        except Exception as e:
            self.logger.error(f"Error fetching payload restrictions: {str(e)}")
            traceback.print_exc()
            return pd.DataFrame()

    # -------------------------------------------------------------------------
    # NAV ODS: find flights of a sector within a departure window
    # -------------------------------------------------------------------------

    def get_flights_for_restriction(self, origin, destination, flight_number, dep_start, dep_end):
        """
        Query NAV ODS InventoryLeg for every departure of origin->destination on the
        given flight number within [dep_start, dep_end]. Returns a DataFrame with
        FlightNumber, DepartureStation, ArrivalStation, DepartureDate.
        """
        try:
            dep_start_str = pd.to_datetime(dep_start).strftime('%Y-%m-%d')
            dep_end_str = pd.to_datetime(dep_end).strftime('%Y-%m-%d')

            query = text(f"""
            SELECT DISTINCT
                IL.FlightNumber,
                IL.DepartureStation,
                IL.ArrivalStation,
                IL.DepartureDate
            FROM {self.nav_db_name}rez.InventoryLeg IL
            WHERE IL.DepartureStation = :origin
                AND IL.ArrivalStation = :destination
                AND IL.FlightNumber = :flight_number
                AND IL.Lid > 0
                AND IL.Status NOT IN (2)
                AND IL.DepartureDate BETWEEN CAST(:dep_start AS DATE)
                                         AND CAST(:dep_end AS DATE)
            ORDER BY IL.DepartureDate;
            """)

            params = {
                "origin": origin,
                "destination": destination,
                "flight_number": flight_number,
                "dep_start": dep_start_str,
                "dep_end": dep_end_str,
            }

            df = pd.read_sql(query, self.nav_conn, params=params)
            self.logger.info(
                f"Found {len(df)} flights for {origin}-{destination} flight {flight_number} "
                f"between {dep_start_str} and {dep_end_str}"
            )
            return df

        except Exception as e:
            self.logger.error(f"Error fetching flights for restriction: {str(e)}")
            traceback.print_exc()
            return pd.DataFrame()

    # -------------------------------------------------------------------------
    # Apply LID / adjusted-capacity updates in NAV, collect affected flights
    # -------------------------------------------------------------------------

    def apply_restrictions(self, restrictions_df):
        """
        For each restriction, resolve every affected flight from NAV ODS and update
        its Sales LID (lid) and Check-in LID (adjustedCapacity) in NAV via the API.

        Returns a tuple:
          affected_df   : DataFrame of successfully updated flights (used for
                          market-list matching + notification).
          successes     : list of dicts describing each successful update, with the
                          original lid/adjustedCapacity and the new values.
          failures      : list of dicts describing each failed update, with reason.
        """
        successes = []
        failures = []

        if restrictions_df.empty:
            return pd.DataFrame(), successes, failures

        nav_client = self._get_nav_client()
        affected_flights = []

        for _, restriction in restrictions_df.iterrows():
            origin = restriction['origin']
            destination = restriction['destination']
            flight_number = restriction['flightNumber']
            dep_start = restriction['depStart']
            dep_end = restriction['depEnd']

            try:
                lid = int(restriction['lid'])
                adjusted_capacity = int(restriction['adjustedCapacity'])
            except (ValueError, TypeError):
                reason = "Invalid lid/adjustedCapacity value in config"
                self.logger.error(
                    f"{reason} for {origin}-{destination} flight {flight_number}; skipping"
                )
                failures.append({
                    'flightNumber': flight_number,
                    'origin': origin,
                    'destination': destination,
                    'departureDate': None,
                    'depStart': str(dep_start),
                    'depEnd': str(dep_end),
                    'requestedLid': restriction.get('lid'),
                    'requestedAdjustedCapacity': restriction.get('adjustedCapacity'),
                    'reason': reason
                })
                continue

            flights_df = self.get_flights_for_restriction(
                origin, destination, flight_number, dep_start, dep_end
            )

            if flights_df.empty:
                self.logger.info(
                    f"No NAV flights to update for {origin}-{destination} "
                    f"flight {flight_number}"
                )
                failures.append({
                    'flightNumber': flight_number,
                    'origin': origin,
                    'destination': destination,
                    'departureDate': None,
                    'depStart': str(dep_start),
                    'depEnd': str(dep_end),
                    'requestedLid': lid,
                    'requestedAdjustedCapacity': adjusted_capacity,
                    'reason': "No matching flights found in NAV ODS for the departure window"
                })
                continue

            for _, flight in flights_df.iterrows():
                dep_date = flight['DepartureDate']
                dep_date_iso = pd.to_datetime(dep_date).strftime('%Y-%m-%d')
                if isinstance(dep_date, str):
                    dep_date_mdy = pd.to_datetime(dep_date).strftime(DATE_FORMAT)
                else:
                    dep_date_mdy = dep_date.strftime(DATE_FORMAT)

                base_record = {
                    'flightNumber': flight_number,
                    'origin': origin,
                    'destination': destination,
                    'departureDate': dep_date_iso,
                    'requestedLid': lid,
                    'requestedAdjustedCapacity': adjusted_capacity
                }

                try:
                    leg_key, leg_reason = nav_client.get_leg_key(
                        dep_date_mdy, AKASA_AIRLINE_CODE, flight_number, origin, destination
                    )
                    if not leg_key:
                        failures.append({**base_record, 'reason': leg_reason or "Leg key not found"})
                        continue

                    # Capture the current (original) values before patching
                    original_info, info_reason = nav_client.get_leg_information(leg_key)
                    if original_info is None:
                        original_lid = None
                        original_adjusted_capacity = None
                        original_capacity = None
                        self.logger.info(
                            f"Could not read original leg info for {flight_number} "
                            f"{origin}-{destination} on {dep_date_iso}: {info_reason}"
                        )
                    else:
                        original_lid = original_info.get('lid')
                        original_adjusted_capacity = original_info.get('adjustedCapacity')
                        original_capacity = original_info.get('capacity')

                    # Guard: the new adjusted capacity cannot exceed the physical
                    # equipment capacity of the flight.
                    if original_capacity is not None and adjusted_capacity > original_capacity:
                        reason = (
                            f"Requested adjustedCapacity ({adjusted_capacity}) exceeds the "
                            f"equipment capacity ({original_capacity})"
                        )
                        self.logger.info(
                            f"Skipping NAV update for {flight_number} {origin}-{destination} "
                            f"on {dep_date_iso}: {reason}"
                        )
                        failures.append({
                            **base_record,
                            'legKey': leg_key,
                            'originalLid': original_lid,
                            'originalAdjustedCapacity': original_adjusted_capacity,
                            'equipmentCapacity': original_capacity,
                            'reason': reason
                        })
                        continue

                    success, patch_reason = nav_client.update_leg_lid_capacity(
                        leg_key, lid, adjusted_capacity
                    )

                    if success:
                        successes.append({
                            **base_record,
                            'legKey': leg_key,
                            'originalLid': original_lid,
                            'originalAdjustedCapacity': original_adjusted_capacity,
                            'newLid': lid,
                            'newAdjustedCapacity': adjusted_capacity
                        })
                        affected_flights.append({
                            'FlightNumber': flight_number,
                            'DepartureStation': origin,
                            'ArrivalStation': destination,
                            'DepartureDate': dep_date_iso,
                            'lid': lid,
                            'adjustedCapacity': adjusted_capacity
                        })
                    else:
                        failures.append({
                            **base_record,
                            'legKey': leg_key,
                            'originalLid': original_lid,
                            'originalAdjustedCapacity': original_adjusted_capacity,
                            'reason': patch_reason or "Leg patch failed"
                        })

                except Exception as e:
                    reason = f"Unexpected error: {str(e)}"
                    self.logger.error(
                        f"Error updating NAV leg for {origin}-{destination} flight "
                        f"{flight_number} on {dep_date_mdy}: {str(e)}"
                    )
                    traceback.print_exc()
                    failures.append({**base_record, 'reason': reason})

        affected_df = pd.DataFrame(affected_flights) if affected_flights else pd.DataFrame()
        self.logger.info(
            f"Applied restrictions: {len(successes)} successful, {len(failures)} failed"
        )
        return affected_df, successes, failures

    # -------------------------------------------------------------------------
    # Market list retrieval (same pattern as ScheduleThirty / EQTAlloc)
    # -------------------------------------------------------------------------

    def get_combined_market_list(self):
        """
        Get combined market list from MongoDB collections (domestic + international).
        No date filtering; restrictions can be for any future departure window.
        """
        try:
            config_collection = self.db['config_column_names']
            config_doc = config_collection.find_one(
                {'tableName': 'market_list_s3'},
                {'columns': 1, '_id': 0}
            )

            if not config_doc:
                self.logger.warning("No column configuration found, using fallback columns")
                columns_list = [
                    'Origin', 'Destin', 'FlightNumber', 'PerType', 'PerStart', 'PerEnd',
                    'DOW', 'TimeWindowStart', 'TimeWindowEnd', 'DaySpan', 'CurveID',
                    'CarrExlusionB2C', 'CarrExlusionB2B', 'flightExclusionB2C', 'flightExclusionB2B',
                    'fareAnchor', 'hardAnchor', 'plfThreshold', 'fareOffset', 'FirstRBDAlloc',
                    'OtherRBDAlloc', 'B2BBackstop', 'B2CBackstop', 'B2BFactor', 'obSeats',
                    'obFare', 'SkippingFactor', 'UUID', 'OwnFareFlag', 'analystName', 'AutoTimeRangeFlag',
                    'OverBooking', 'openingFares', 'profileFares', 'rbdPushFlag', 'B2CTolerance',
                    'B2BTolerance', 'distressInventoryFlag', 'seriesBlock', 'autoGroup',
                    'autoBackstopFlag', 'tbfFlag', 'CugFlag', 'MktFareFlag', 'directFlag', 'TBFEnhFlag',
                    'defaultMlFlag', 'b2bStrategy', 'Outbound_Stop', 'Marketfare_Upsell'
                ]
            else:
                columns_str = config_doc['columns']
                columns_list = [col.strip() for col in columns_str.split(',')]
                self.logger.info("Retrieved column configuration from config_column_names collection")

            projection = {col: 1 for col in columns_list}
            projection['_id'] = 0

            cursor1 = self.db['market_list'].find({}, projection)
            df1 = pd.DataFrame(list(cursor1))

            cursor2 = self.db['market_list_international'].find({}, projection)
            df2 = pd.DataFrame(list(cursor2))

            combined_df = pd.concat([df1, df2], ignore_index=True)

            if combined_df.empty:
                self.logger.warning("Combined market list is empty")
                return pd.DataFrame()

            if 'PerStart' in combined_df.columns:
                combined_df['PerStart_DT'] = pd.to_datetime(
                    combined_df['PerStart'], format='%m/%d/%Y', errors='coerce'
                )
            else:
                self.logger.warning("PerStart column not found in market list")

            self.logger.info(f"Combined market list has {len(combined_df)} records")
            return combined_df

        except Exception as e:
            self.logger.error(f"Error getting combined market list: {str(e)}")
            traceback.print_exc()
            return pd.DataFrame()

    # -------------------------------------------------------------------------
    # Matching logic (same as ScheduleThirty / EQTAlloc)
    # -------------------------------------------------------------------------

    def match_flights_with_market_list(self, flights_df, market_list_df):
        """
        Match affected flights against the market list.

        Matching keys: DepartureStation->Origin, ArrivalStation->Destin,
                       FlightNumber, DepartureDate->PerStart date.
        Falls back to the closest PerStart for the same route+flight when no exact
        date match is found.
        """
        if flights_df.empty:
            self.logger.info("No affected flights to match")
            return pd.DataFrame()

        if market_list_df.empty:
            self.logger.info("Empty market list - cannot match flights")
            return pd.DataFrame()

        market_list_df['FlightNumber'] = market_list_df['FlightNumber'].astype(str).str.strip()
        market_list_df['FlightNumber_norm'] = market_list_df['FlightNumber'].str.replace(r'\s+|-', '', regex=True)

        flights_df = flights_df.copy()
        flights_df['FlightNumber'] = flights_df['FlightNumber'].astype(str).str.strip()
        flights_df['FlightNumber_norm'] = flights_df['FlightNumber'].str.replace(r'\s+|-', '', regex=True)
        flights_df['DepartureDate_DT'] = pd.to_datetime(flights_df['DepartureDate'], errors='coerce')

        # Deduplicate affected flights before matching (a flight can appear once
        # per restriction row; market list match is by route+flight+date).
        flights_df = flights_df.drop_duplicates(
            subset=['DepartureStation', 'ArrivalStation', 'FlightNumber_norm', 'DepartureDate']
        )

        matches = []
        unmatched = []

        for _, flight in flights_df.iterrows():
            flight_label = (
                f"{flight['DepartureStation']}-{flight['ArrivalStation']} "
                f"Flight {flight['FlightNumber']} on {flight['DepartureDate']}"
            )

            exact = market_list_df[
                (market_list_df['Origin'] == flight['DepartureStation']) &
                (market_list_df['Destin'] == flight['ArrivalStation']) &
                (market_list_df['FlightNumber_norm'] == flight['FlightNumber_norm']) &
                (market_list_df['PerStart_DT'].dt.date == flight['DepartureDate_DT'].date())
            ]

            if not exact.empty:
                self.logger.info(f"Exact market list match: {flight_label}")
                match_row = exact.iloc[0].to_dict()
                match_row.pop('PerStart_DT', None)
                match_row.pop('FlightNumber_norm', None)
                matches.append(match_row)
            else:
                route_matches = market_list_df[
                    (market_list_df['Origin'] == flight['DepartureStation']) &
                    (market_list_df['Destin'] == flight['ArrivalStation']) &
                    (market_list_df['FlightNumber_norm'] == flight['FlightNumber_norm'])
                ]

                if route_matches.empty:
                    self.logger.warning(f"No market list entry for: {flight_label}")
                    unmatched.append(flight.to_dict())
                else:
                    route_matches = route_matches.copy()
                    route_matches['_date_diff'] = (
                        route_matches['PerStart_DT'] - flight['DepartureDate_DT']
                    ).abs()
                    closest = route_matches.sort_values('_date_diff').iloc[0].to_dict()
                    closest.pop('PerStart_DT', None)
                    closest.pop('FlightNumber_norm', None)
                    closest.pop('_date_diff', None)
                    matches.append(closest)
                    self.logger.info(
                        f"Closest date match used: {flight_label} "
                        f"(matched PerStart: {closest.get('PerStart')})"
                    )

        if unmatched:
            self.logger.info(f"{len(unmatched)} affected flights had no market list match")

        return pd.DataFrame(matches) if matches else pd.DataFrame()

    # -------------------------------------------------------------------------
    # Persist to market_list_payload_restriction (replace collection contents)
    # -------------------------------------------------------------------------

    def update_market_list_payload_restriction(self, market_list_df):
        """Clear market_list_payload_restriction and insert matched records fresh."""
        try:
            collection = self.db[self.MARKET_LIST_TABLE_NAME]

            if market_list_df.empty:
                self.logger.info(f"No records to insert into {self.MARKET_LIST_TABLE_NAME}")
                return

            drop_cols = ['PerStart_DT', 'FlightNumber_norm', 'DepartureDate_DT', '_date_diff']
            market_list_df = market_list_df.drop(
                [col for col in drop_cols if col in market_list_df.columns],
                axis=1
            )
            market_list_df['FlightNumber'] = pd.to_numeric(
                market_list_df['FlightNumber'], errors='coerce'
            )

            delete_result = collection.delete_many({})
            self.logger.info(
                f"Cleared {delete_result.deleted_count} existing records from "
                f"{self.MARKET_LIST_TABLE_NAME}"
            )

            records = market_list_df.to_dict('records')
            for record in records:
                record['_payload_restriction_processed_at'] = datetime.utcnow()

            collection.insert_many(records)
            self.logger.info(f"Inserted {len(records)} records into {self.MARKET_LIST_TABLE_NAME}")

        except Exception as e:
            self.logger.error(f"Error updating {self.MARKET_LIST_TABLE_NAME}: {str(e)}")
            traceback.print_exc()
            raise

    # -------------------------------------------------------------------------
    # Version tracking & S3 upload
    # -------------------------------------------------------------------------

    def update_current_version(self, version_id):
        """Update currentVersion collection for market_list_payload_restriction."""
        try:
            collection = self.db['currentVersion']
            collection.update_one(
                {'tableName': self.MARKET_LIST_TABLE_NAME},
                {
                    '$set': {
                        'curr_version': version_id,
                        'updated_at': datetime.utcnow()
                    }
                },
                upsert=True
            )
            self.logger.info(f"Updated currentVersion for {self.MARKET_LIST_TABLE_NAME}")
        except Exception as e:
            self.logger.error(f"Error updating currentVersion: {str(e)}")
            traceback.print_exc()
            raise

    def upload_to_s3(self, df, version_id):
        """Upload market_list_payload_restriction data to S3."""
        try:
            s3_client = self.s3_client
            bucket_name = configsettings.S3_BUCKET_NAME
            s3_folder_path = f"{configsettings.S3_FILE_PATH}rm-admin/{self.S3_SUBFOLDER}"
            file_name = f"{version_id}_{self.MARKET_LIST_TABLE_NAME}"
            s3_file_path = f"{s3_folder_path}/{file_name}"

            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)

            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_file_path,
                Body=csv_buffer.getvalue()
            )

            self.logger.info(f"Uploaded to S3: s3://{bucket_name}/{s3_file_path}")
            return True

        except Exception as e:
            self.logger.error(f"Error uploading to S3: {str(e)}")
            traceback.print_exc()
            return False

    # -------------------------------------------------------------------------
    # Lambda invocation (adhoc reallocation for restricted flights)
    # -------------------------------------------------------------------------

    def invoke_reallocation_lambda(self, run_id):
        """Invoke the prefetch-inputs Lambda to run the restricted flights as adhoc."""
        try:
            lambda_client = self.lambda_client
            lambda_function_name = configsettings.PREFETCH_INPUTS_LAMBDA

            payload = {
                "body": json.dumps({
                    "run_id": run_id,
                    "start_interval": 0,
                    "end_interval": 365,
                    "run_type": self.RUN_TYPE,
                    "market_list_table_name": self.MARKET_LIST_TABLE_NAME,
                    "route_type": "direct",
                    "update_navitaire_method": "api"
                })
            }

            response = lambda_client.invoke(
                FunctionName=lambda_function_name,
                InvocationType='Event',
                Payload=json.dumps(payload)
            )

            if response['StatusCode'] == 202:
                self.logger.info("Payload restriction reallocation Lambda invoked (async)")
                return {"status": "success", "statusCode": 202}
            else:
                self.logger.error(
                    f"Reallocation Lambda invocation failed: status {response['StatusCode']}"
                )
                return {"status": "error", "statusCode": response['StatusCode']}

        except Exception as e:
            self.logger.error(f"Error invoking reallocation Lambda: {str(e)}")
            traceback.print_exc()
            raise

    # -------------------------------------------------------------------------
    # Email notification
    # -------------------------------------------------------------------------

    def send_notification(self, affected_df, restrictions_df, successes, failures):
        """
        Send a styled HTML email to FA summarizing the payload restriction updates
        applied. Shows who triggered the run, a summary of restrictions processed
        and NAV flights updated, followed by a per-flight table showing the
        Sales LID and Check-in LID change (from -> to).
        """
        try:
            if restrictions_df.empty:
                return

            mail_client = GraphMailClient(self.logger)
            recipients = [
                r.strip() for r in self.constants.PAYLOAD_RESTRICTION_RECIPIENTS.split(',')
                if r.strip()
            ]

            try:
                import pytz
                timestamp = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S IST')
            except Exception:
                timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')

            subject = "Payload Restriction - LID Update Alert"

            successes = successes or []
            failures = failures or []
            total_restrictions = len(restrictions_df)
            total_success = len(successes)
            total_failed = len(failures)

            def fmt_change(old_val, new_val, color):
                """Render an old -> new value with the old struck-through/greyed."""
                old_display = "—" if old_val is None else old_val
                return (
                    f'<span style="color:#999999; text-decoration:line-through;">{old_display}</span>'
                    f'<span style="color:#bbbbbb;"> &rarr; </span>'
                    f'<span style="color:{color}; font-weight:600;">{new_val}</span>'
                )

            # Build one row per successfully updated flight, showing from -> to
            row_html = []
            for change in successes:
                flight_number = change.get('flightNumber')
                origin = change.get('origin')
                destination = change.get('destination')
                dep_date = change.get('departureDate')
                try:
                    dep_date_disp = pd.to_datetime(dep_date).strftime('%d %b %Y')
                except Exception:
                    dep_date_disp = dep_date

                lid_change = fmt_change(
                    change.get('originalLid'), change.get('newLid'), "#5c0fd9"
                )
                cap_change = fmt_change(
                    change.get('originalAdjustedCapacity'),
                    change.get('newAdjustedCapacity'), "#c2650f"
                )

                bg = "#ffffff" if len(row_html) % 2 == 0 else "#fafafa"
                row_html.append(f"""
                    <tr style="background-color:{bg};">
                        <td style="padding:9px 12px; font-size:12.5px; color:#333333; border-bottom:1px solid #eeeeee;">
                            QP {flight_number}
                        </td>
                        <td style="padding:9px 12px; font-size:12.5px; color:#333333; border-bottom:1px solid #eeeeee;">
                            {origin}&ndash;{destination}
                        </td>
                        <td style="padding:9px 12px; font-size:12.5px; color:#666666; border-bottom:1px solid #eeeeee;">
                            {dep_date_disp}
                        </td>
                        <td style="padding:9px 12px; font-size:12.5px; text-align:center; border-bottom:1px solid #eeeeee;">
                            {lid_change}
                        </td>
                        <td style="padding:9px 12px; font-size:12.5px; text-align:center; border-bottom:1px solid #eeeeee;">
                            {cap_change}
                        </td>
                    </tr>
                """)

            # Append one row per failed flight into the SAME table. The two
            # LID columns are replaced by a single cell showing a bold, all-caps
            # ACTION REQUIRED banner and the reason for failure.
            for fail in failures:
                flight_number = fail.get('flightNumber')
                origin = fail.get('origin')
                destination = fail.get('destination')
                dep_date = fail.get('departureDate')
                try:
                    dep_date_disp = pd.to_datetime(dep_date).strftime('%d %b %Y') if dep_date else "&mdash;"
                except Exception:
                    dep_date_disp = dep_date or "&mdash;"
                reason = fail.get('reason', 'Unknown error')

                bg = "#fdf6f6" if len(row_html) % 2 == 0 else "#fbeceb"
                row_html.append(f"""
                    <tr style="background-color:{bg};">
                        <td style="padding:9px 12px; font-size:12.5px; color:#333333; border-bottom:1px solid #eeeeee;">
                            QP {flight_number}
                        </td>
                        <td style="padding:9px 12px; font-size:12.5px; color:#333333; border-bottom:1px solid #eeeeee;">
                            {origin}&ndash;{destination}
                        </td>
                        <td style="padding:9px 12px; font-size:12.5px; color:#666666; border-bottom:1px solid #eeeeee;">
                            {dep_date_disp}
                        </td>
                        <td colspan="2" style="padding:9px 12px; font-size:12px; color:#b3261e; border-bottom:1px solid #eeeeee;">
                            <strong style="color:#b3261e; letter-spacing:0.5px;">ACTION REQUIRED</strong><br>
                            <span style="color:#8a3029;">{reason}</span>
                        </td>
                    </tr>
                """)

            if not row_html:
                row_html.append("""
                    <tr>
                        <td colspan="5" style="padding:14px 12px; font-size:12.5px; color:#999999; text-align:center; border-bottom:1px solid #eeeeee;">
                            No flights were processed.
                        </td>
                    </tr>
                """)

            restriction_rows = "".join(row_html)

            html_body = f"""
            <html>
            <body style="margin:0; padding:0; background-color:none; font-family:Arial, Helvetica, sans-serif;">
                <td align="center">
                    <table role="presentation" width="640" cellpadding="0" cellspacing="0" style="background-color:#ffffff; border:1px solid #e5e7eb; border-radius:8px;">

                        <!-- Header -->
                        <tr>
                            <td style="padding:24px 32px 16px 32px; border-bottom:1px solid #eeeeee;">
                                <h2 style="margin:0; color:#111111; font-size:19px; font-weight:600;">
                                    Payload Restriction - LID Update Alert
                                </h2>
                                <p style="margin:6px 0 0 0; font-size:12px; color:#888888;">
                                    {timestamp}
                                </p>
                                <p style="margin:8px 0 0 0; font-size:12.5px; color:#555555;">
                                    Triggered by <strong style="color:#5c0fd9;">{self.analyst_name}</strong>
                                </p>
                            </td>
                        </tr>

                        <!-- Body -->
                        <tr>
                            <td style="padding:24px 32px;">

                                <!-- Summary stats -->
                                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:26px;">
                                    <tr>
                                        <td style="width:33%; padding-right:12px; border-left:3px solid #888888; padding-left:12px;">
                                            <div style="font-size:12px; color:#888888; text-transform:uppercase; letter-spacing:0.4px; margin-bottom:4px;">
                                                Restrictions Processed
                                            </div>
                                            <div style="font-size:26px; color:#333333; font-weight:700;">
                                                {total_restrictions}
                                            </div>
                                            <div style="font-size:12px; color:#999999; margin-top:2px;">
                                                from config_payload_restriction
                                            </div>
                                        </td>
                                        <td style="width:33%; padding-right:12px; border-left:3px solid #2e7d32; padding-left:12px;">
                                            <div style="font-size:12px; color:#888888; text-transform:uppercase; letter-spacing:0.4px; margin-bottom:4px;">
                                                Successful Updates
                                            </div>
                                            <div style="font-size:26px; color:#2e7d32; font-weight:700;">
                                                {total_success}
                                            </div>
                                            <div style="font-size:12px; color:#999999; margin-top:2px;">
                                                NAV API calls
                                            </div>
                                        </td>
                                        <td style="width:33%; border-left:3px solid #b3261e; padding-left:12px;">
                                            <div style="font-size:12px; color:#888888; text-transform:uppercase; letter-spacing:0.4px; margin-bottom:4px;">
                                                Failed Updates
                                            </div>
                                            <div style="font-size:26px; color:#b3261e; font-weight:700;">
                                                {total_failed}
                                            </div>
                                            <div style="font-size:12px; color:#999999; margin-top:2px;">
                                                NAV API calls
                                            </div>
                                        </td>
                                    </tr>
                                </table>

                                <!-- Intro -->
                                <p style="margin:0 0 14px 0; font-size:12.5px; color:#888888; line-height:1.6;">
                                    The following payload restrictions have been applied in NAV ODS.
                                    Advance Purchase settings (APs) were retained; only the Sales LID and
                                    Check-in LID were updated, and the affected flights have been queued for
                                    adhoc reallocation. Each value below shows the change applied
                                    (previous &rarr; new).
                                </p>

                                <!-- Updates table (successful changes + failed flights) -->
                                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border:1px solid #eeeeee; border-radius:6px; border-collapse:separate;">
                                    <tr style="background-color:#f5f3fb;">
                                        <th style="padding:10px 12px; font-size:11px; color:#666666; text-transform:uppercase; letter-spacing:0.4px; text-align:left; border-bottom:1px solid #eeeeee;">Flight</th>
                                        <th style="padding:10px 12px; font-size:11px; color:#666666; text-transform:uppercase; letter-spacing:0.4px; text-align:left; border-bottom:1px solid #eeeeee;">Sector</th>
                                        <th style="padding:10px 12px; font-size:11px; color:#666666; text-transform:uppercase; letter-spacing:0.4px; text-align:left; border-bottom:1px solid #eeeeee;">Departure</th>
                                        <th style="padding:10px 12px; font-size:11px; color:#666666; text-transform:uppercase; letter-spacing:0.4px; text-align:center; border-bottom:1px solid #eeeeee;">Sales LID</th>
                                        <th style="padding:10px 12px; font-size:11px; color:#666666; text-transform:uppercase; letter-spacing:0.4px; text-align:center; border-bottom:1px solid #eeeeee;">Check-in LID</th>
                                    </tr>
                                    {restriction_rows}
                                </table>

                            </td>
                        </tr>

                        <!-- Footer -->
                        <tr>
                            <td style="padding:14px 32px; border-top:1px solid #eeeeee;">
                                <p style="margin:0; font-size:11px; color:#aaaaaa;">
                                    Automated notification &mdash; RM Support Payload Restriction job
                                </p>
                            </td>
                        </tr>

                    </table>
                </td>
            </body>
            </html>
            """

            mail_client.send_email(subject, html_body, recipients, is_html=True)

            self.logger.info(f"Payload restriction notification sent to {len(recipients)} recipients")

        except Exception as e:
            self.logger.error(f"Error sending payload restriction notification: {str(e)}")
            traceback.print_exc()

    # -------------------------------------------------------------------------
    # Main execution
    # -------------------------------------------------------------------------

    def run(self):
        """Main execution method for payload restriction processing."""
        try:
            run_id = str(uuid4())
            triggered_at = datetime.utcnow()
            self.logger.info(
                f"Starting Payload Restriction process with run_id: {run_id} "
                f"(triggered by {self.analyst_name})"
            )

            # Step 1: Read restriction inputs
            restrictions_df = self.get_payload_restrictions()
            if restrictions_df.empty:
                self.logger.info("No payload restrictions to process. Exiting.")
                return True

            # Step 2: Apply LID / adjusted-capacity updates in NAV (APs retained)
            affected_df, successes, failures = self.apply_restrictions(restrictions_df)

            # Step 4: Notify FA about applied restrictions
            self.send_notification(affected_df, restrictions_df, successes, failures)

            if affected_df.empty:
                self.logger.info("No NAV flights were updated; skipping reallocation.")
                return True

            # Step 5: Get combined market list
            market_list_df = self.get_combined_market_list()
            if market_list_df.empty:
                self.logger.warning("Market list is empty - cannot run reallocation")
                return False

            # Step 6: Match affected flights to market list
            market_list_pr_df = self.match_flights_with_market_list(affected_df, market_list_df)

            # Step 7: Replace market_list_payload_restriction contents
            self.update_market_list_payload_restriction(market_list_pr_df)

            # Step 8: If matches found, version + upload + trigger adhoc reallocation
            if not market_list_pr_df.empty:
                version_id = str(uuid4())
                self.logger.info(f"Generated version ID: {version_id}")

                self.update_current_version(version_id)
                self.upload_to_s3(market_list_pr_df, version_id)
                self.invoke_reallocation_lambda(run_id)

            self.logger.info("Payload Restriction process completed successfully")
            return True

        except Exception as e:
            self.logger.error(f"Error in Payload Restriction process: {str(e)}")
            traceback.print_exc()
            return False
