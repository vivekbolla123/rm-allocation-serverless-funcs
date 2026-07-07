import json
import configsettings
import pandas as pd
from datetime import datetime, timezone, timedelta
from constants_utils import ConstantsUtils
import traceback
import io
from common_utils import GraphMailClient
import requests
import uuid
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import warnings
warnings.filterwarnings('ignore')

class defaultMarketlist:
    def __init__(self, logger, params, overrides, context):
        self.context = context
        self.logger = logger
        self.navconn = params['navconn']  # Keep SQL for Navitaire
        self.mongo_conn = params['mongoconn']  # Single MongoDB connection
        self.dwconn = params.get('dwconn')
        self.db = self.mongo_conn["qp_dw_rmalloc"]
        self.master_db = params['mongo_master_db']
        self.cache_client = params['cache_client']
        self.constants = ConstantsUtils(self.db)
        self.default_list = json.loads(self.constants.DEFAULT_MARKET_LIST)
        self.max_days = self.constants.DEFAULT_MARKET_LIST_DAYS
        self.session = requests.Session()
        self.session.headers.update(
            {"Ocp-Apim-Subscription-Key": configsettings.NAV_OCP_SUBSCRIPTION_KEY}
        )
        self.mail_client = GraphMailClient(logger)
        self.alert_email_config = self.constants.DEFAULT_ML_ALERT_MAIL_ID

    def run(self):
        try:
            self.logger.info("Starting the run method.")
            currDate = datetime.now(timezone.utc)
            endDate = currDate + timedelta(days=self.max_days)
            endDate = endDate.date()

            # Navitaire query remains SQL (different system)
            NavitaireQuery = f"""
                SELECT
                    IL.DepartureStation as Origin,
                    IL.ArrivalStation as Destination,
                    IL.DepartureDate,
                    TRIM(IL.FlightNumber) as FlightNumber,
                    FORMAT(IL.STD, 'HH:mm') as DepartureTime,
                    FORMAT(IL.STDUTC, 'HH:mm') as DepartureTimeUTC
                FROM
                    {configsettings.NAV_DB_NAME}[rez].InventoryLeg IL
                WHERE IL.Lid > 0
                    AND IL.Status NOT IN (2, 3)
                    AND IL.DepartureDate >= CAST(GETDATE() AS DATE)
                    AND IL.DepartureDate <= '{endDate}'
            """
            navFlights = pd.read_sql(NavitaireQuery, self.navconn)
            
            # Filter for FlightNumber between 1000 and 6000 (exclusive)
            navFlights['FlightNumberNum'] = pd.to_numeric(navFlights['FlightNumber'], errors='coerce')
            navFlights = navFlights[(navFlights['FlightNumberNum'] > 1000) & (navFlights['FlightNumberNum'] < 6000)]
            navFlights = navFlights.drop(columns=['FlightNumberNum'])

            if navFlights.empty or len(navFlights) == 0:
                raise ValueError("No flights found in NavODS")

            # Fetch market_list from MongoDB
            mlFlights = self._fetch_marketlist_flights(endDate)

            if mlFlights.empty or len(mlFlights) == 0:
                raise ValueError("No flights found in Marketlist")

            navFlights['DepartureDate'] = pd.to_datetime(navFlights['DepartureDate'])
            mlFlights['DepartureDate'] = pd.to_datetime(mlFlights['DepartureDate'])

            # Get unmatched records - Fix: Use indicator merge to properly identify unmatched records
            merge_indicator = navFlights.merge(
                mlFlights[['Origin', 'Destination', 'FlightNumber', 'DepartureDate']],
                on=['Origin', 'Destination', 'FlightNumber', 'DepartureDate'],
                how='left',
                indicator=True
            )
            
            # Filter for records that exist only in navFlights (left_only)
            unmatched_navFlights = merge_indicator[merge_indicator['_merge'] == 'left_only'].drop('_merge', axis=1)
            unmatched_navFlights['sector'] = unmatched_navFlights['Origin'] + unmatched_navFlights['Destination']
            
            # Fetch analyst sector map from MongoDB
            analyst_sector_map = self._fetch_analyst_sector_map()
            if analyst_sector_map.empty:
                raise ValueError("No columns found for market_list table")
            
            # Fetch city pair master from Data Warehouse
            city_pair_map = self._fetch_city_pair_map()
            if city_pair_map.empty:
                raise ValueError("No city pairs found from QP_DWH_MASTER.CityPair")
            
            unmatched_navFlights = unmatched_navFlights.merge(
                analyst_sector_map[['sector', 'user1']],
                on=['sector'],
                how='left'
            )

            # Get default values
            default_list = self.default_list
            if len(default_list) == 0:
                raise ValueError("No default values found for market_list")
            column_names = list(default_list.keys())

            # Prepare data for insertion
            insert_data = []
            for _, row in unmatched_navFlights.iterrows():
                record = default_list.copy()
                record['Origin'] = row['Origin']
                record['Destin'] = row['Destination']
                record['PerStart'] = row['DepartureDate'].strftime('%m/%d/%Y')
                record['PerEnd'] = row['DepartureDate'].strftime('%m/%d/%Y')
                record['FlightNumber'] = int(row['FlightNumber'])
                analyst = row['user1']
                record['analystName'] = f"SYS / {str(analyst).strip()}" if pd.notna(analyst) and str(analyst).strip() else "SYS"
                record['UUID'] = str(uuid.uuid4())
                record['defaultMlFlag'] = 1
                record['createdAt'] = datetime.now(timezone.utc)
                weekday_num = row['DepartureDate'].weekday()

                sunday_based_day = (weekday_num + 1) % 7
                dow_base = list('9999999')
                dow_base[sunday_based_day] = '1'
                record['DOW'] = int(''.join(dow_base))
                insert_data.append([record.get(col, '') for col in column_names])

            # Create DataFrame for insertion
            insert_df = pd.DataFrame(insert_data, columns=column_names)

            final_df = insert_df.merge(
                city_pair_map,
                left_on=['Origin', 'Destin'],
                right_on=['DepartureStation', 'ArrivalStation'],
                how='left'
            )

            # Based on whether InternationalFlag == 1 or DomesticFlag == 1 add it in respective dfs
            # international_df = final_df[final_df['InternationalFlag'] == 1]
            domestic_df = final_df[final_df['DomesticFlag'] == 1]

            # Log warnings for records that did not match either flag
            unmatched_flags_df = final_df[
                (final_df['InternationalFlag'] != 1) & (final_df['DomesticFlag'] != 1)
            ]
            if not unmatched_flags_df.empty:
                unique_sectors = (unmatched_flags_df['Origin'] + unmatched_flags_df['Destin']).unique().tolist()
                self.logger.warning(
                    f"Found {len(unmatched_flags_df)} records with no matching "
                    f"InternationalFlag or DomesticFlag in QP_DWH_MASTER.CityPair. Unique Sectors: "
                    f"{', '.join(unique_sectors)}"
                )

            # Drop temporary merge columns
            columns_to_drop = ['DepartureStation', 'ArrivalStation', 'InternationalFlag', 'DomesticFlag']
            # international_df = international_df.drop(columns=columns_to_drop, errors='ignore')
            domestic_df = domestic_df.drop(columns=columns_to_drop, errors='ignore')

            # Insert into MongoDB collections
            self._insert_to_mongodb('market_list', domestic_df)
            # self._insert_to_mongodb('market_list_international', international_df)

            self.logger.info(f"Inserted {len(domestic_df)} unmatched records into market_list.")
            # self.logger.info(f"Inserted {len(international_df)} unmatched records into market_list_international.")

            # Build and upload audit logs for new records (mirrors AuditHelper.buildAuditLogs in Java)
            self._build_and_upload_audit_logs(domestic_df, "SYS")

            self._send_marketlist_alert()
                
            self.logger.info("Run method completed.")
            
        except Exception as e:
            self.logger.error(f"An error occurred in the run method: {traceback.format_exc()}")

    def _send_marketlist_alert(self):
        try:
            import pytz
            ist_tz = pytz.timezone('Asia/Kolkata')
            ist_now = datetime.now(ist_tz)
            timestamp = ist_now.strftime('%Y-%m-%d %H:%M:%S IST')
            date_str = ist_now.strftime('%Y%m%d')

            query = {'defaultMlFlag': {'$in': [1, "1"]}}
            projection = {'_id': 0, 'Origin': 1, 'Destin': 1, 'PerStart': 1, 'FlightNumber': 1, 'analystName': 1, 'createdAt': 1}
            dom_records = list(self.db['market_list'].find(query, projection))
            # intl_records = list(self.db['market_list_international'].find(query, projection))
            
            all_records = dom_records # + intl_records
            if not all_records:
                self.logger.info("No records found with defaultMlFlag=1. Skipping alert.")
                return

            all_flights_df = pd.DataFrame(all_records)

            csv_buffer = io.StringIO()
            all_flights_df.to_csv(csv_buffer, index=False)
            csv_content = csv_buffer.getvalue()
            filename = f"default_marketlist_{date_str}.csv"

            html_body = f"""
            <html>
            <body>
                <h2>Default ML Flights Alert</h2>
                <p><strong>Time:</strong> {timestamp}</p>
                <p>Default ML Flights Count: {len(all_flights_df)}</p>
                <p style='margin-top: 20px;'>
                    Please find the detailed list in the attached CSV file.
                </p>
                <p style='margin-top: 20px; font-style: italic; color: #555;'>
                    <strong>Note:</strong> While making changes to these default Market List Flights, turn off the 'defaultMlFlag' and upload for the changes to be applied.
                </p>
            </body>
            </html>
            """

            attachments = [{
                'filename': filename,
                'content': csv_content,
                'file_type': 'csv'
            }]

            self.mail_client.send_email(
                'Default Marketlist - System Added Flights Alert',
                html_body,
                self.alert_email_config.get('to', []),
                is_html=True,
                attachments=attachments,
                cc=self.alert_email_config.get('cc', [])
            )
            self.logger.info(f"Alert email sent to {self.alert_email_config.get('to', [])} with {len(all_flights_df)} system-added flights.")

        except Exception as e:
            self.logger.error(f"Failed to send default marketlist alert email: {str(e)}\\n{traceback.format_exc()}")

    def _fetch_marketlist_flights(self, endDate):
        """
        Fetch flights from market_list and market_list_international collections.
        Equivalent to UNION query in MySQL.
        """
        try:
            current_date = datetime.now().date()
            
            # Helper function to fetch from a collection
            def fetch_from_collection(collection_name):
                collection = self.db[collection_name]
                
                # MongoDB query
                # Note: Handling both string and datetime formats for PerStart
                cursor = collection.find(
                    {},
                    {
                        '_id': 0,
                        'Origin': 1,
                        'Destin': 1,
                        'PerStart': 1,
                        'FlightNumber': 1
                    }
                )
                
                df = pd.DataFrame(list(cursor))
                if df.empty:
                    return pd.DataFrame(columns=['Origin', 'Destination', 'DepartureDate', 'FlightNumber'])
                
                # Rename Destin to Destination for consistency
                df = df.rename(columns={'Destin': 'Destination'})
                
                # Convert PerStart to datetime (handle both string and datetime)
                df['DepartureDate'] = df['PerStart'].apply(
                    lambda x: pd.to_datetime(x, format='%m/%d/%Y', errors='coerce') 
                    if isinstance(x, str) else pd.to_datetime(x, errors='coerce')
                )
                
                # Filter by date range
                df = df[
                    (df['DepartureDate'].dt.date >= current_date) &
                    (df['DepartureDate'].dt.date <= endDate)
                ]
                
                # Clean FlightNumber (trim whitespace)
                df['FlightNumber'] = df['FlightNumber'].astype(str).str.strip()
                
                return df[['Origin', 'Destination', 'DepartureDate', 'FlightNumber']]
            
            # Fetch from both collections
            domestic_flights = fetch_from_collection('market_list')
            # international_flights = fetch_from_collection('market_list_international')
            
            # Union (concatenate) the results
            # mlFlights = pd.concat([domestic_flights, international_flights], ignore_index=True)
            mlFlights = domestic_flights
            
            # Remove duplicates
            mlFlights = mlFlights.drop_duplicates()
            
            self.logger.info(f"Fetched {len(mlFlights)} flights from market_list collections")
            return mlFlights
            
        except Exception as e:
            self.logger.error(f"Error fetching marketlist flights from MongoDB: {e}")
            return pd.DataFrame()

    def _fetch_analyst_sector_map(self):
        """Fetch analyst sector mapping from MongoDB."""
        try:
            collection = self.db['config_user_sector_map']
            cursor = collection.find({}, {'_id': 0, 'sector': 1, 'user1': 1})
            df = pd.DataFrame(list(cursor))
            
            self.logger.info(f"Fetched {len(df)} records from config_user_sector_map")
            return df
            
        except Exception as e:
            self.logger.error(f"Error fetching analyst sector map from MongoDB: {e}")
            return pd.DataFrame()

    def _fetch_city_pair_map(self):
        """Fetch city pair mappings from Data Warehouse (QP_DWH_MASTER.CityPair)."""
        try:
            query = """
                SELECT DepartureStation, ArrivalStation, InternationalFlag, DomesticFlag
                FROM QP_DWH_MASTER.CityPair
                WHERE ConnectionFlag = 0 AND InActive = 0
            """
            df = pd.read_sql(query, self.dwconn)
            self.logger.info(f"Fetched {len(df)} records from QP_DWH_MASTER.CityPair")
            
            # Clean whitespaces from merge keys
            if not df.empty:
                df['DepartureStation'] = df['DepartureStation'].astype(str).str.strip()
                df['ArrivalStation'] = df['ArrivalStation'].astype(str).str.strip()
                df = df.drop_duplicates(subset=['DepartureStation', 'ArrivalStation'])
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error fetching city pair map from DW: {traceback.format_exc()}")
            return pd.DataFrame()

    def _insert_to_mongodb(self, collection_name, df):
        """
        Insert DataFrame records into MongoDB collection.
        Converts all values to appropriate types: int, float, or string.
        
        Args:
            collection_name: Name of the collection
            df: DataFrame to insert
        """
        try:
            if df.empty:
                self.logger.info(f"No records to insert into {collection_name}")
                return
            
            collection = self.db[collection_name]
            
            # Helper function to convert a value to int, float, or string
            def parse_value(val):
                """
                Parse value to int, float, or string in that order of preference.
                """
                # Handle None/NaN
                if pd.isna(val) or val is None:
                    return None
                
                # If already correct type, return as-is
                if isinstance(val, (int, float, bool)):
                    # Try to convert float to int if it's a whole number
                    if isinstance(val, float):
                        if val.is_integer():
                            return int(val)
                    return val
                
                # Convert to string first for uniform processing
                str_val = str(val).strip()
                
                # Empty string handling
                if str_val == '' or str_val.lower() in ['nan', 'none', 'null']:
                    return None
                
                # Try integer conversion first
                try:
                    # Check if string represents an integer
                    if '.' not in str_val and 'e' not in str_val.lower():
                        return int(str_val)
                    else:
                        # Check if it's a float that's actually an integer
                        float_val = float(str_val)
                        if float_val.is_integer():
                            return int(float_val)
                        return float_val
                except (ValueError, OverflowError):
                    pass
                
                # Try float conversion
                try:
                    return float(str_val)
                except (ValueError, OverflowError):
                    pass
                
                # Return as string if all conversions fail
                return str_val
            
            # Convert DataFrame to list of dictionaries with type conversion
            records = []
            for _, row in df.iterrows():
                record = {}
                for col in df.columns:
                    val = row[col]
                    
                    # Special handling for specific fields
                    if col in ['PerStart', 'PerEnd']:
                        # Keep date strings as-is for MongoDB
                        if isinstance(val, str):
                            record[col] = val
                        elif pd.notna(val):
                            record[col] = str(val)
                        else:
                            record[col] = None
                    
                    elif col == 'UUID':
                        # Ensure UUID is string
                        record[col] = str(val) if pd.notna(val) else None
                    
                    
                    elif col in ['analystName', 'Origin', 'Destin']:
                        # Keep certain fields as strings
                        record[col] = str(val).strip() if pd.notna(val) else None
                    
                    elif col == 'createdAt':
                        # Keep datetime object for MongoDB
                        record[col] = val if pd.notna(val) else None
                    
                    else:
                        # Apply general type conversion
                        record[col] = parse_value(val)
                
                records.append(record)
            
            # Bulk insert
            if records:
                result = collection.insert_many(records)
                self.logger.info(f"Inserted {len(result.inserted_ids)} records into {collection_name}")
            else:
                self.logger.info(f"No valid records to insert into {collection_name}")
            
        except Exception as e:
            self.logger.error(f"Error inserting into {collection_name}: {traceback.format_exc()}")
            raise

    # ─── Audit Column Mapping ───────────────────────────────────────────────
    #
    # The Java AuditHelper writes Parquet using JavaBean property names (the Java
    # field names in MarketListEntity/AuditEntity), NOT the MongoDB @Column names.
    #
    # DYNAMIC RULE: Most mismatches are just Java's Introspector.decapitalize —
    # i.e. first character lowered. The code applies this automatically for any
    # field starting with an uppercase letter followed by a lowercase letter.
    #
    # EXPLICIT MAP: Only fields where the rename is NOT just first-char-lowercase
    # (completely different names, abbreviation changes, typo corrections, etc.)
    # need to be listed here.
    #
    # ⚠️  IMPORTANT: If adding a new field to MarketListEntity.java where
    #     @Column(name) differs from the Java field name by MORE than just
    #     lowering the first character, add it to _AUDIT_RENAME_MAP below.
    #     Prefer keeping @Column(name) == Java field name for new fields.
    _AUDIT_RENAME_MAP = {
        # MongoDB @Column(name)  →  Java field / Parquet column
        # These have genuine name differences (not just first-char case):
        'obFare': 'OBFare',                  # ob → OB (uppercase abbreviation)
        'obSeats': 'OBSeats',                # ob → OB (uppercase abbreviation)
        'B2BBackstop': 'b2bBackstop',        # B2B → b2b (full prefix lowered)
        'B2BFactor': 'b2bFactor',            # B2B → b2b
        'B2BTolerance': 'b2bTolerance',      # B2B → b2b
        'B2CBackstop': 'b2cBackstop',        # B2C → b2c
        'B2CTolerance': 'b2cTolerance',      # B2C → b2c
        'Outbound_Stop': 'b2bOutbound_Stop', # completely different name
        'CarrExlusionB2B': 'carrExclusionB2B',  # typo fix: Exlusion → Exclusion
        'CarrExlusionB2C': 'carrExclusionB2C',  # typo fix: Exlusion → Exclusion
        'CurveID': 'curveId',                # ID → Id (abbreviation case change)
        'TimeWindowEnd': 'endTime',          # completely different name
        'TimeWindowStart': 'startTime',      # completely different name
        'FirstRBDAlloc': 'firstAllocation',  # completely different name
        'OtherRBDAlloc': 'otherAllocation',  # completely different name
        'FlightNumber': 'flightNo',          # completely different name
        'TBFEnhFlag': 'tbfEnhFlag',          # TBF → tbf (full prefix lowered)
    }

    # Fields excluded from audit (Java nulls them in prepareNewRecordAudit).
    _AUDIT_EXCLUDED_FIELDS = {'UUID', 'Origin', 'Destin', 'PerType', 'PerStart', 'PerEnd', 'DOW', 'createdAt'}

    @staticmethod
    def _decapitalize(name):
        """Apply Java's Introspector.decapitalize rule: lowercase first char
        only if the second char is lowercase (or name is length 1).
        E.g. 'DaySpan' → 'daySpan', but 'B2B...' stays unchanged."""
        if not name or len(name) < 2:
            return name.lower() if name else name
        if name[0].isupper() and name[1].islower():
            return name[0].lower() + name[1:]
        return name

    def _build_and_upload_audit_logs(self, df, user_name):
        """
        Build audit logs for newly inserted market_list records and upload
        to S3 as Parquet, matching the Java AuditHelper output for new records.

        Dynamically derives columns from the DEFAULT_MARKET_LIST keys:
        - Fields in _AUDIT_EXCLUDED_FIELDS → emitted as null-valued cleared columns
        - Fields in _AUDIT_RENAME_MAP → renamed to the Java bean property name
        - All other fields → passed through with the same name (already matching)
        """
        try:
            if df is None or df.empty:
                self.logger.info("No records to audit.")
                return

            import pytz
            ist_now = datetime.now(pytz.timezone('Asia/Kolkata')).replace(tzinfo=None)
            changed_at_str = ist_now.isoformat()
            changed_at_date_str = ist_now.strftime('%Y-%m-%d')

            # Build the list of data columns dynamically from DEFAULT_MARKET_LIST keys
            data_columns = [
                col for col in self.default_list.keys()
                if col not in self._AUDIT_EXCLUDED_FIELDS
            ]

            audit_records = []
            for _, row in df.iterrows():
                audit = {}

                # Cleared columns (Java nulls these in prepareNewRecordAudit)
                audit['uuid'] = None
                audit['origin'] = None
                audit['destination'] = None
                audit['perType'] = None
                audit['startDate'] = None
                audit['endDate'] = None
                audit['dayOfWeek'] = None

                # Data columns: check explicit map first, then apply decapitalize,
                # then pass through unchanged
                for mongo_col in data_columns:
                    if mongo_col in self._AUDIT_RENAME_MAP:
                        audit_col = self._AUDIT_RENAME_MAP[mongo_col]
                    else:
                        audit_col = self._decapitalize(mongo_col)
                    val = row.get(mongo_col)
                    if pd.isna(val) or val is None:
                        audit[audit_col] = None
                    else:
                        audit[audit_col] = str(val) if not isinstance(val, str) else val

                # Audit metadata columns
                origin = str(row.get('Origin', '')).strip() if pd.notna(row.get('Origin')) else ''
                destin = str(row.get('Destin', '')).strip() if pd.notna(row.get('Destin')) else ''
                per_start = str(row.get('PerStart', '')) if pd.notna(row.get('PerStart')) else ''

                audit['sector'] = origin + destin
                audit['date'] = per_start.replace('/', '-')
                audit['userName'] = user_name
                audit['changedAt'] = changed_at_str
                audit['newRecord'] = True
                # flightNo comes from FlightNumber via the rename map, but we
                # override it here to ensure it's the clean string (no .0 float)
                audit['flightNo'] = (
                    str(int(float(row['FlightNumber']))) if pd.notna(row.get('FlightNumber')) else ''
                )

                # Partition column (added by Spark's functions.lit in Java)
                audit['changedAtDate'] = changed_at_date_str

                audit_records.append(audit)

            if not audit_records:
                self.logger.info("No audit records generated.")
                return

            audit_df = pd.DataFrame(audit_records)

            parquet_buffer = io.BytesIO()
            audit_df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
            parquet_buffer.seek(0)

            s3_client = boto3.client('s3')
            bucket = configsettings.S3_BUCKET_NAME
            base_path = f"{configsettings.S3_FILE_PATH}rm-admin/ml-audits/direct"
            partition_key = f"changedAtDate={changed_at_date_str}"
            file_name = f"part-default_ml-{uuid.uuid4().hex[:12]}.parquet"
            s3_key = f"{base_path}/{partition_key}/{file_name}"

            s3_client.put_object(
                Bucket=bucket,
                Key=s3_key,
                Body=parquet_buffer.getvalue(),
                ContentType='application/octet-stream'
            )

            self.logger.info(
                f"Audit logs uploaded: {len(audit_records)} new-record audits "
                f"to s3://{bucket}/{s3_key}"
            )

        except Exception as e:
            self.logger.error(f"Failed to build/upload audit logs: {str(e)}\n{traceback.format_exc()}")
