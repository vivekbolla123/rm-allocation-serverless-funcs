import pandas as pd
import logging
import configsettings
from constants_utils import ConstantsUtils
from datetime import datetime, timedelta
import traceback
import json
import boto3
from uuid import uuid4
import io


class EQTAlloc:
    """Class to manage equipment change (EQT) event processing.
    
    Runs on a 15-minute cron. Fetches EQT events from mongo_de_db.FlightSchedule.flight_event,
    matches them against the combined market list, upserts into market_list_eqt,
    and triggers a downstream Lambda for reallocation.
    """

    LOOKBACK_MINUTES = 15
    EVENT_TYPE = "EQT"

    def __init__(self, logger, params, overrides, context):
        """Initialize connections and settings."""
        self.logger = logger or logging.getLogger()
        self.mongoconn = params['mongoconn']
        self.mongo_de_db = params['mongo_de_db']

        # Application MongoDB database
        self.db = self.mongoconn["qp_dw_rmalloc"]

        # ConstantsUtils for system parameters
        self.constants = ConstantsUtils(self.db)

    # -------------------------------------------------------------------------
    # Fetch EQT events
    # -------------------------------------------------------------------------

    def get_eqt_events(self):
        """
        Query FlightSchedule.flight_event for EQT events created in the last 15 minutes.
        Returns a DataFrame with departure info needed for market list matching.
        """
        try:
            cutoff_time = datetime.utcnow() - timedelta(minutes=self.LOOKBACK_MINUTES)

            collection = self.mongo_de_db['flight_events']

            query = {
                "eventType": self.EVENT_TYPE,
                "createdAt": {"$gte": cutoff_time},
                "navUpdated": "1"
            }

            projection = {
                "_id": 1,
                "flight": 1,
                "departureStation": 1,
                "arrivalStation": 1,
                "departureDate": 1,
                "oldConfig": 1,
                "newConfig": 1,
                "navUpdated": 1,
                "createdAt": 1
            }

            docs = list(collection.find(query, projection))

            if not docs:
                self.logger.info("No EQT events found in the last 15 minutes")
                return pd.DataFrame()

            df = pd.DataFrame(docs)

            # Normalize field names to align with market list matching
            df = df.rename(columns={
                "flight": "FlightNumber",
                "departureStation": "DepartureStation",
                "arrivalStation": "ArrivalStation",
                "departureDate": "DepartureDate"
            })

            # Strip whitespace from key fields
            df['FlightNumber'] = df['FlightNumber'].astype(str).str.strip()
            df['DepartureStation'] = df['DepartureStation'].astype(str).str.strip()
            df['ArrivalStation'] = df['ArrivalStation'].astype(str).str.strip()
            df['DepartureDate'] = df['DepartureDate'].astype(str).str.strip()

            # Deduplicate: if multiple EQT events exist for the same flight+date, keep latest
            df = df.sort_values('createdAt', ascending=False)
            df = df.drop_duplicates(
                subset=['FlightNumber', 'DepartureStation', 'ArrivalStation', 'DepartureDate'],
                keep='first'
            )

            self.logger.info(f"Found {len(df)} unique EQT events in the last {self.LOOKBACK_MINUTES} minutes")
            return df

        except Exception as e:
            self.logger.error(f"Error fetching EQT events: {str(e)}")
            traceback.print_exc()
            return pd.DataFrame()

    # -------------------------------------------------------------------------
    # Market list retrieval (same pattern as ScheduleThirty)
    # -------------------------------------------------------------------------

    def get_combined_market_list(self):
        """
        Get combined market list from MongoDB collections (domestic + international).
        No date filtering applied here since EQT events can be for any future departure.
        """
        try:
            # Fetch column configuration
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

            # Domestic
            cursor1 = self.db['market_list'].find({}, projection)
            df1 = pd.DataFrame(list(cursor1))

            # International
            cursor2 = self.db['market_list_international'].find({}, projection)
            df2 = pd.DataFrame(list(cursor2))

            combined_df = pd.concat([df1, df2], ignore_index=True)

            if combined_df.empty:
                self.logger.warning("Combined market list is empty")
                return pd.DataFrame()

            # Parse PerStart for date matching
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
    # Matching logic
    # -------------------------------------------------------------------------

    def match_eqt_events_with_market_list(self, eqt_df, market_list_df):
        """
        Match EQT event flights against the market list.
        
        Matching keys: DepartureStation→Origin, ArrivalStation→Destin,
                       FlightNumber, DepartureDate→PerStart date.
        
        Falls back to closest future PerStart for the same route+flight if no
        exact date match is found.
        """
        if eqt_df.empty:
            self.logger.info("No EQT events to match")
            return pd.DataFrame()

        if market_list_df.empty:
            self.logger.info("Empty market list — cannot match EQT events")
            return pd.DataFrame()

        # Normalize flight numbers for comparison
        market_list_df['FlightNumber'] = market_list_df['FlightNumber'].astype(str).str.strip()
        market_list_df['FlightNumber_norm'] = market_list_df['FlightNumber'].str.replace(r'\s+|-', '', regex=True)
        eqt_df['FlightNumber_norm'] = eqt_df['FlightNumber'].str.replace(r'\s+|-', '', regex=True)

        # Parse departure date from EQT events
        eqt_df['DepartureDate_DT'] = pd.to_datetime(eqt_df['DepartureDate'], errors='coerce')

        matches = []
        unmatched = []

        for _, event_row in eqt_df.iterrows():
            flight_label = (
                f"{event_row['DepartureStation']}-{event_row['ArrivalStation']} "
                f"Flight {event_row['FlightNumber']} on {event_row['DepartureDate']}"
            )

            # Exact match: route + flight + departure date == PerStart date
            exact = market_list_df[
                (market_list_df['Origin'] == event_row['DepartureStation']) &
                (market_list_df['Destin'] == event_row['ArrivalStation']) &
                (market_list_df['FlightNumber_norm'] == event_row['FlightNumber_norm']) &
                (market_list_df['PerStart_DT'].dt.date == event_row['DepartureDate_DT'].date())
            ]

            if not exact.empty:
                self.logger.info(f"Exact market list match for EQT event: {flight_label}")
                match_row = exact.iloc[0].to_dict()
                match_row.pop('PerStart_DT', None)
                match_row.pop('FlightNumber_norm', None)
                matches.append(match_row)
            else:
                # Fallback: find closest future PerStart for same route+flight
                route_matches = market_list_df[
                    (market_list_df['Origin'] == event_row['DepartureStation']) &
                    (market_list_df['Destin'] == event_row['ArrivalStation']) &
                    (market_list_df['FlightNumber_norm'] == event_row['FlightNumber_norm'])
                ]

                if route_matches.empty:
                    self.logger.warning(f"No market list entry for EQT event: {flight_label}")
                    unmatched.append(event_row.to_dict())
                else:
                    # Pick the entry whose PerStart is closest to the event departure date
                    route_matches = route_matches.copy()
                    route_matches['_date_diff'] = (
                        route_matches['PerStart_DT'] - event_row['DepartureDate_DT']
                    ).abs()
                    closest = route_matches.sort_values('_date_diff').iloc[0].to_dict()
                    closest.pop('PerStart_DT', None)
                    closest.pop('FlightNumber_norm', None)
                    closest.pop('_date_diff', None)
                    matches.append(closest)
                    self.logger.info(
                        f"Closest date match used for EQT event: {flight_label} "
                        f"(matched PerStart: {closest.get('PerStart')})"
                    )

        if unmatched:
            self.logger.info(f"{len(unmatched)} EQT events had no market list match at all")

        return pd.DataFrame(matches) if matches else pd.DataFrame()

    # -------------------------------------------------------------------------
    # Persist to market_list_eqt (upsert, not truncate)
    # -------------------------------------------------------------------------

    def update_market_list_eqt(self, market_list_eqt_df):
        """
        Upsert matched records into market_list_eqt collection.
        Uses Origin+Destin+FlightNumber+PerStart as the composite key so that
        repeated EQT events for the same flight update rather than duplicate.
        """
        try:
            collection = self.db['market_list_eqt']

            if market_list_eqt_df.empty:
                self.logger.info("No records to upsert into market_list_eqt")
                return

            # Clean utility columns before persisting
            drop_cols = ['PerStart_DT', 'FlightNumber_norm', 'DepartureDate_DT']
            market_list_eqt_df = market_list_eqt_df.drop(
                [col for col in drop_cols if col in market_list_eqt_df.columns],
                axis=1
            )

            upsert_count = 0
            for _, row in market_list_eqt_df.iterrows():
                record = row.to_dict()

                filter_key = {
                    "Origin": record.get("Origin"),
                    "Destin": record.get("Destin"),
                    "FlightNumber": record.get("FlightNumber"),
                    "PerStart": record.get("PerStart")
                }

                # Add processing metadata
                record['_eqt_processed_at'] = datetime.utcnow()

                collection.update_one(
                    filter_key,
                    {"$set": record},
                    upsert=True
                )
                upsert_count += 1

            self.logger.info(f"Upserted {upsert_count} records into market_list_eqt")

        except Exception as e:
            self.logger.error(f"Error updating market_list_eqt: {str(e)}")
            traceback.print_exc()
            raise

    # -------------------------------------------------------------------------
    # Version tracking & S3 upload
    # -------------------------------------------------------------------------

    def update_current_version(self, version_id):
        """Update currentVersion collection for market_list_eqt."""
        try:
            collection = self.db['currentVersion']
            collection.update_one(
                {'tableName': 'market_list_eqt'},
                {
                    '$set': {
                        'curr_version': version_id,
                        'updated_at': datetime.utcnow()
                    }
                },
                upsert=True
            )
            self.logger.info("Updated currentVersion for market_list_eqt")
        except Exception as e:
            self.logger.error(f"Error updating currentVersion: {str(e)}")
            traceback.print_exc()
            raise

    def upload_to_s3(self, df, version_id):
        """Upload market_list_eqt data to S3."""
        try:
            s3_client = boto3.client('s3',region_name='ap-south-1')
            bucket_name = configsettings.S3_BUCKET_NAME
            s3_folder_path = f"{configsettings.S3_FILE_PATH}rm-admin/market_list_eqt"
            file_name = f"{version_id}_market_list_eqt"
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
    # Lambda invocation
    # -------------------------------------------------------------------------

    def invoke_eqt_lambda(self, run_id):
        """Invoke the prefetch-inputs Lambda for EQT reallocation."""
        try:
            lambda_client = boto3.client('lambda')
            lambda_function_name = configsettings.PREFETCH_INPUTS_LAMBDA

            payload = {
                "body": json.dumps({
                    "run_id": run_id,
                    "start_interval": 0,
                    "end_interval": 365,
                    "run_type": "Equipment_Change",
                    "market_list_table_name": "market_list_eqt",
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
                self.logger.info("EQT Lambda invoked successfully (async)")
                return {"status": "success", "statusCode": 202}
            else:
                self.logger.error(f"EQT Lambda invocation failed: status {response['StatusCode']}")
                return {"status": "error", "statusCode": response['StatusCode']}

        except Exception as e:
            self.logger.error(f"Error invoking EQT Lambda: {str(e)}")
            traceback.print_exc()
            raise

    # -------------------------------------------------------------------------
    # Main execution
    # -------------------------------------------------------------------------

    def run(self):
        """Main execution method for EQT allocation processing."""
        try:
            run_id = str(uuid4())
            self.logger.info(f"Starting EQT Allocation process with run_id: {run_id}")

            # Step 1: Fetch recent EQT events from DE database
            eqt_df = self.get_eqt_events()
            if eqt_df.empty:
                self.logger.info("No EQT events to process. Exiting.")
                return True

            # Step 2: Get combined market list
            market_list_df = self.get_combined_market_list()
            if market_list_df.empty:
                self.logger.warning("Market list is empty — cannot process EQT events")
                return False

            # Step 3: Match EQT events to market list
            market_list_eqt_df = self.match_eqt_events_with_market_list(eqt_df, market_list_df)

            # Step 4: Upsert into market_list_eqt
            self.update_market_list_eqt(market_list_eqt_df)

            # Step 5: If matches found, version + upload + trigger lambda
            if not market_list_eqt_df.empty:
                version_id = str(uuid4())
                self.logger.info(f"Generated version ID: {version_id}")

                self.update_current_version(version_id)
                self.upload_to_s3(market_list_eqt_df, version_id)
                self.invoke_eqt_lambda(run_id)

            # Step 6: Done — downstream logging handled by fetch-inputs Lambda
            self.logger.info("EQT Allocation process completed successfully")
            return True

        except Exception as e:
            self.logger.error(f"Error in EQT Allocation process: {str(e)}")
            traceback.print_exc()
            return False
