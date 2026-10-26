import json
import configsettings
import pandas as pd
import numpy as np
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
        self.overrides = overrides
        self.navconn = params['navconn']  # Keep SQL for Navitaire
        self.mongo_conn = params['mongoconn']  # Single MongoDB connection
        self.dwconn = params.get('dwconn')
        self.db = self.mongo_conn["qp_dw_rmalloc"]
        # self.master_db = params['mongo_master_db']
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
            start_day = int(self.overrides.get('start_day', 0)) if self.overrides else 0
            end_day = int(self.overrides.get('end_day', self.max_days)) if self.overrides else self.max_days
            startDate = currDate + timedelta(days=start_day)
            endDate = currDate + timedelta(days=end_day)
            startDate = startDate.date()
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
                    AND IL.DepartureDate >= '{startDate}'
                    AND IL.DepartureDate <= '{endDate}'
                    AND IL.FlightNumber < 7000
            """
            navFlights = pd.read_sql(NavitaireQuery, self.navconn)
            
            # Filter for FlightNumber between 1000 and 6000 (exclusive)
            navFlights['FlightNumberNum'] = pd.to_numeric(navFlights['FlightNumber'], errors='coerce')
            navFlights = navFlights[(navFlights['FlightNumberNum'] > 1000) & (navFlights['FlightNumberNum'] < 6000)]
            navFlights = navFlights.drop(columns=['FlightNumberNum'])

            if navFlights.empty or len(navFlights) == 0:
                raise ValueError("No flights found in NavODS")

            # Fetch market_list from MongoDB
            mlFlights = self._fetch_marketlist_flights(startDate, endDate)

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

            unmatched_navFlights = unmatched_navFlights.merge(
                city_pair_map[['DepartureStation', 'ArrivalStation', 'ActualDistance']],
                left_on=['Origin', 'Destination'],
                right_on=['DepartureStation', 'ArrivalStation'],
                how='left'
            )

            # 1. Time Windows
            unmatched_navFlights = self._calculate_time_windows(unmatched_navFlights)

            # 2. Dynamic Backstops
            # date_list = [(startDate + timedelta(days=i)).strftime('%m/%d/%Y') for i in range((endDate - startDate).days + 1)]
            unmatched_navFlights = self._calculate_dynamic_backstops(unmatched_navFlights)

            # 3. Build Insert DataFrame
            insert_df = self._build_insert_dataframe(unmatched_navFlights)

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

            # Restrict to only the expected columns from DEFAULT_MARKET_LIST
            expected_columns = list(insert_df.columns)
            domestic_df_to_insert = domestic_df[[c for c in expected_columns if c in domestic_df.columns]]
            
            # Fetch all existing unmanaged flights for the alert before insertion, regardless of date range
            existing_unmanaged = self._fetch_all_unmanaged_default_ml()

            # Insert into MongoDB collections
            self._insert_to_mongodb('market_list', domestic_df_to_insert)
            # self._insert_to_mongodb('market_list_international', international_df_to_insert)

            # Build and upload audit logs for new records (mirrors AuditHelper.buildAuditLogs in Java)
            self._build_and_upload_audit_logs(domestic_df, "SYS")

            # Push the newly-added default-ML flights to a dedicated collection and
            # trigger an adhoc allocation run for them (pattern per equipmentChange.py /
            # schedule30.py). Non-fatal: failures here must not abort the main job.
            self._trigger_default_ml_adhoc(domestic_df_to_insert)

            start_date_str = startDate.strftime('%Y-%m-%d')
            end_date_str = endDate.strftime('%Y-%m-%d')
            
            self._send_marketlist_alert(domestic_df, existing_unmanaged, start_date_str, end_date_str)
                
            self.logger.info("Run method completed.")
            
        except Exception as e:
            self.logger.error(f"An error occurred in the run method: {traceback.format_exc()}")

    def _calculate_time_windows(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate TimeWindowStart and TimeWindowEnd dynamically based on DepartureTime."""
        if df.empty:
            df['TimeWindowStart'] = None
            df['TimeWindowEnd'] = None
            return df
            
        ml_config = getattr(self.constants, 'DEFAULT_ML_CALC_CONFIG', {}) or {}
        time_window_hours = int(ml_config.get('time_window_hours', 2))
            
        dep_dt = pd.to_datetime(df['DepartureTime'], format='%H:%M', errors='coerce')
        df['TimeWindowStart'] = (dep_dt - pd.Timedelta(hours=time_window_hours)).dt.strftime('%H:%M')
        df['TimeWindowEnd'] = (dep_dt + pd.Timedelta(hours=time_window_hours)).dt.strftime('%H:%M')
        return df

    def _calculate_dynamic_backstops(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate dynamic B2B and B2C backstops using config collections in batches."""
        def get_default(key, default_val):
            try:
                v = self.default_list.get(key)
                if not v or str(v).strip() == "":
                    return default_val
                return float(v)
            except (ValueError, TypeError):
                return default_val
                
        default_b2b = get_default('B2BBackstop', 8000)
        default_b2c = get_default('B2CBackstop', 8000)
        
        if df.empty:
            df['B2BBackstop'] = default_b2b
            df['B2CBackstop'] = default_b2c
            return df
            
        ml_config = getattr(self.constants, 'DEFAULT_ML_CALC_CONFIG', {}) or {}
        backstop_mult = float(ml_config.get('backstop_distance_multiplier', 6))
        q_fare_level = str(ml_config.get('q_fare_level', '10'))
        q_fare_col = f"Q{q_fare_level}"

        df['sector'] = df['Origin'] + df['Destination']
        df['date_str'] = df['DepartureDate'].dt.strftime('%m/%d/%Y')
        df['dow_name'] = df['DepartureDate'].dt.strftime('%A')
        
        # 1. Global Pre-fetches
        # Date events (no filters as requested)
        config_date_event_cur = self.db['config_date_event'].find({}, {
            '_id': 0,
            'date': 1,
            'mappedMonth': 1
        })
        date_events_df = pd.DataFrame(list(config_date_event_cur))
        if not date_events_df.empty:
            date_events_df = date_events_df[['date', 'mappedMonth']].drop_duplicates()
            
        # Clusters (no filters)
        config_clusters_cur = self.db['config_clusters'].find({}, {'_id': 0})
        clusters_df = pd.DataFrame(list(config_clusters_cur))
        
        # 2. Assign clusters globally
        if not clusters_df.empty:
            def map_cluster(dep_time_str):
                try:
                    dep_dt = datetime.strptime(dep_time_str, '%H:%M')
                    dep_time = dep_dt.time()
                    for _, cl in clusters_df.iterrows():
                        start_time_str = cl.get('startTime')
                        end_time_str = cl.get('endTime')
                        if not start_time_str or not end_time_str:
                            continue
                        try:
                            st = datetime.strptime(start_time_str, '%H:%M:%S').time()
                        except ValueError:
                            st = datetime.strptime(start_time_str, '%H:%M').time()
                        try:
                            et = datetime.strptime(end_time_str, '%H:%M:%S').time()
                        except ValueError:
                            et = datetime.strptime(end_time_str, '%H:%M').time()
                        
                        if st <= et:
                            if st <= dep_time < et:
                                return cl.get('cluster')
                        else:
                            if dep_time >= st or dep_time < et:
                                return cl.get('cluster')
                except Exception:
                    pass
                return None
                
            df['cluster'] = df['DepartureTime'].apply(map_cluster)
        else:
            df['cluster'] = None
            
        # 3. Batch processing for heavy collections (upsell & profile fares)
        unique_sectors = df['sector'].dropna().unique().tolist()
        chunk_size = 5
        processed_batches = []
        
        for i in range(0, len(unique_sectors), chunk_size):
            batch_sectors = unique_sectors[i:i + chunk_size]
            batch_df = df[df['sector'].isin(batch_sectors)].copy()
            
            # Merge mappedMonth
            if not date_events_df.empty:
                batch_df = batch_df.merge(date_events_df, left_on='date_str', right_on='date', how='left')
            else:
                batch_df['mappedMonth'] = None
                
            # Fetch Upsell for batch (sector filter only)
            upsell_cur = self.db['config_profile_fare_station_upsell'].find({'sector': {'$in': batch_sectors}}, {
                '_id': 0,
                'sector': 1,
                'date': 1,
                'value': 1
            })
            upsell_df = pd.DataFrame(list(upsell_cur))
            if not upsell_df.empty:
                upsell_df['value'] = pd.to_numeric(upsell_df['value'], errors='coerce').fillna(0)
                upsell_df = upsell_df[['sector', 'date', 'value']].drop_duplicates(subset=['sector', 'date'])
                upsell_df.rename(columns={'value': 'upsell'}, inplace=True)
                batch_df = batch_df.merge(upsell_df, left_on=['sector', 'date_str'], right_on=['sector', 'date'], how='left')
            else:
                batch_df['upsell'] = 0.0
            batch_df['upsell'] = batch_df['upsell'].fillna(0.0)
                
            # Fetch Profile Fares for batch (sector and mapped_month filter)
            mapped_months = batch_df['mappedMonth'].dropna().unique().tolist()
            if mapped_months:
                profile_fares_cur = self.db['config_profile_fares'].find({
                    'sector': {'$in': batch_sectors},
                    'month': {'$in': mapped_months}
                }, {
                    '_id': 0,
                    'sector': 1,
                    'month': 1,
                    'dow': 1,
                    'cluster': 1,
                    q_fare_col: 1
                })
                profile_fares_df = pd.DataFrame(list(profile_fares_cur))
                if not profile_fares_df.empty and q_fare_col in profile_fares_df.columns:
                    profile_fares_df = profile_fares_df[['sector', 'month', 'dow', 'cluster', q_fare_col]].drop_duplicates()
                    batch_df = batch_df.merge(
                        profile_fares_df, 
                        left_on=['sector', 'mappedMonth', 'dow_name', 'cluster'],
                        right_on=['sector', 'month', 'dow', 'cluster'],
                        how='left'
                    )
                else:
                    batch_df[q_fare_col] = None
            else:
                batch_df[q_fare_col] = None
                
            # Calculate Backstop for batch
            batch_df['ActualDistance'] = pd.to_numeric(batch_df['ActualDistance'], errors='coerce')
            dist_scaled = batch_df['ActualDistance'] * backstop_mult
            q_fare_val = pd.to_numeric(batch_df[q_fare_col], errors='coerce')
            q_upsell = q_fare_val * (1 + batch_df['upsell'] / 100)
            
            mask_missing_dist = batch_df['ActualDistance'].isna()
            mask_missing_q = batch_df[q_fare_col].isna()
            mask_both_missing = mask_missing_dist & mask_missing_q
            
            if mask_missing_dist.any():
                missing_info_dist = batch_df[mask_missing_dist].apply(lambda r: f"{r.get('FlightNumber')}({r.get('date_str')})", axis=1).tolist()
                self.logger.warning(f"Missing ActualDistance for flights: {', '.join(missing_info_dist)}")
                
            if mask_missing_q.any():
                missing_info_q = batch_df[mask_missing_q].apply(lambda r: f"{r.get('FlightNumber')}({r.get('date_str')})", axis=1).tolist()
                self.logger.warning(f"Missing {q_fare_col} for flights: {', '.join(missing_info_q)}")
                
            if mask_both_missing.any():
                missing_info_both = batch_df[mask_both_missing].apply(lambda r: f"{r.get('FlightNumber')}({r.get('date_str')})", axis=1).tolist()
                self.logger.warning(f"Default Backstops (B2B:{default_b2b}/B2C:{default_b2c}) applied due to missing BOTH Dist and {q_fare_col} for: {', '.join(missing_info_both)}")
            
            # Use np.fmax which ignores NaN values (unless both are NaN)
            max_val = np.fmax(dist_scaled, q_upsell)
            batch_df['B2BBackstop'] = max_val.fillna(default_b2b)
            batch_df['B2CBackstop'] = max_val.fillna(default_b2c)
            
            processed_batches.append(batch_df)
            
        final_df = pd.concat(processed_batches, ignore_index=True) if processed_batches else df
        return final_df

    def _build_insert_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Build the DataFrame for insertion using vector operations."""
        if df.empty:
            return pd.DataFrame(columns=list(self.default_list.keys()))

        # Start with default values broadcasted across all rows
        insert_df = pd.DataFrame([self.default_list] * len(df))
        
        insert_df['Origin'] = df['Origin'].values
        insert_df['Destin'] = df['Destination'].values
        insert_df['PerStart'] = df['date_str'].values
        insert_df['PerEnd'] = df['date_str'].values
        insert_df['FlightNumber'] = df['FlightNumber'].astype(int).values
        
        analyst = df['user1'].fillna('')
        insert_df['analystName'] = np.where(
            analyst.str.strip() != '', 
            'SYS / ' + analyst.str.strip(), 
            'SYS'
        )
        insert_df['UUID'] = [str(uuid.uuid4()) for _ in range(len(df))]
        insert_df['defaultMlFlag'] = 1
        insert_df['createdAt'] = datetime.now(timezone.utc)
        
        # DOW logic
        weekday_num = df['DepartureDate'].dt.weekday
        sunday_based_day = (weekday_num + 1) % 7
        
        def make_dow(s):
            b = list('9999999')
            b[s] = '1'
            return int(''.join(b))
            
        insert_df['DOW'] = sunday_based_day.apply(make_dow).values
        
        # Add dynamic calculated values
        if 'TimeWindowStart' in df.columns:
            insert_df['TimeWindowStart'] = df['TimeWindowStart'].values
        if 'TimeWindowEnd' in df.columns:
            insert_df['TimeWindowEnd'] = df['TimeWindowEnd'].values
        if 'B2BBackstop' in df.columns:
            insert_df['B2BBackstop'] = df['B2BBackstop'].values
        if 'B2CBackstop' in df.columns:
            insert_df['B2CBackstop'] = df['B2CBackstop'].values
            
        # Ensure column order matches default_list keys
        return insert_df[list(self.default_list.keys())]

    def _send_marketlist_alert(self, new_records_df: pd.DataFrame, existing_records_df: pd.DataFrame, start_date_str: str, end_date_str: str):
        try:
            import pytz
            ist_tz = pytz.timezone('Asia/Kolkata')
            ist_now = datetime.now(ist_tz)
            timestamp = ist_now.strftime('%Y-%m-%d %H:%M:%S IST')
            date_str = ist_now.strftime('%Y%m%d')

            # Use columns from default_list but exclude specific internal fields for the CSV
            exclude_cols = {'PerType', 'UUID', 'createdAt'}
            col_list = [c for c in self.default_list.keys() if c not in exclude_cols]

            # Fetch column configurations
            flag_cols = []
            ordered_cols = []
            try:
                config_docs = self.db['config_column_names'].find({
                    "tableName": {"$in": ["market_list_flags", "market_list"]}
                })
                for doc in config_docs:
                    if doc.get('tableName') == "market_list_flags" and 'columns' in doc:
                        flag_cols = [col.strip() for col in doc['columns'].split(',') if col.strip()]
                    elif doc.get('tableName') == "market_list" and 'columns' in doc:
                        ordered_cols = [col.strip() for col in doc['columns'].split(',') if col.strip()]
            except Exception as e:
                self.logger.error(f"Error fetching column configurations from config_column_names: {e}")

            def prepare_csv(df):
                if df.empty:
                    return ""
                existing_cols = [c for c in col_list if c in df.columns]
                
                # Rearrange based on the config collection if available
                if ordered_cols:
                    rearranged_cols = [c for c in ordered_cols if c in existing_cols]
                    remaining_cols = [c for c in existing_cols if c not in rearranged_cols]
                    existing_cols = rearranged_cols + remaining_cols
                
                df_csv = df[existing_cols].copy()
                
                if flag_cols:
                    for col in flag_cols:
                        if col in df_csv.columns:
                            df_csv[col] = df_csv[col].apply(lambda x: 'Y' if str(x) in ['1', '1.0'] else ('N' if str(x) in ['0', '0.0'] else x))

                csv_buffer = io.StringIO()
                df_csv.to_csv(csv_buffer, index=False)
                return csv_buffer.getvalue()

            new_csv_content = prepare_csv(new_records_df)
            existing_csv_content = prepare_csv(existing_records_df)

            attachments = []
            if new_csv_content:
                attachments.append({
                    'filename': f"newly_added_default_ml_{date_str}.csv",
                    'content': new_csv_content,
                    'file_type': 'csv'
                })
            
            if existing_csv_content:
                attachments.append({
                    'filename': f"existing_unmanaged_default_ml_{date_str}.csv",
                    'content': existing_csv_content,
                    'file_type': 'csv'
                })

            if not attachments:
                self.logger.info("No records found to alert for defaultMlFlag=1. Skipping alert.")
                return

            html_body = f"""
            <html>
            <body style="margin:0; padding:0; background-color:none; font-family:Arial, Helvetica, sans-serif;">
                <td align="center">
                    <table role="presentation" width="600" cellpadding="0" cellspacing="0" style="background-color:#ffffff; border:1px solid #e5e7eb; border-radius:8px;">

                        <!-- Header -->
                        <tr>
                            <td style="padding:24px 32px 16px 32px; border-bottom:1px solid #eeeeee;">
                                <h2 style="margin:0; color:#111111; font-size:19px; font-weight:600;">
                                    Default ML Flights Alert
                                </h2>
                                <p style="margin:6px 0 0 0; font-size:12px; color:#888888;">
                                    {timestamp}
                                </p>
                            </td>
                        </tr>

                        <!-- Body -->
                        <tr>
                            <td style="padding:24px 32px;">

                                <!-- Summary stats -->
                                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:26px;">
                                    <tr>
                                        <td style="width:50%; padding-right:16px; border-left:3px solid #5c0fd9; padding-left:12px;">
                                            <div style="font-size:12px; color:#888888; text-transform:uppercase; letter-spacing:0.4px; margin-bottom:4px;">
                                                Newly Added ML Records
                                            </div>
                                            <div style="font-size:26px; color:#5c0fd9; font-weight:700;">
                                                {len(new_records_df)}
                                            </div>
                                            <div style="font-size:12px; color:#999999; margin-top:2px;">
                                                {start_date_str} to {end_date_str}
                                            </div>
                                        </td>
                                        <td style="width:50%; border-left:3px solid #c2650f; padding-left:12px;">
                                            <div style="font-size:12px; color:#888888; text-transform:uppercase; letter-spacing:0.4px; margin-bottom:4px;">
                                                Existing Unmanaged Records
                                            </div>
                                            <div style="font-size:26px; color:#c2650f; font-weight:700;">
                                                {len(existing_records_df)}
                                            </div>
                                            <div style="font-size:12px; color:#999999; margin-top:2px;">
                                                Needs analyst review
                                            </div>
                                        </td>
                                    </tr>
                                </table>

                                <!-- Details -->
                                <p style="margin:0 0 4px 0; font-size:12.5px; color:#888888; line-height:1.5;">
                                    Please find the detailed lists in the attached CSV files:
                                </p>
                                <p style="margin:0 0 4px 0; font-size:12.5px; color:#888888; line-height:1.6;">
                                    <strong style="color:#666666;">newly_added_default_ml</strong> — records just inserted by this job execution.
                                </p>
                                <p style="margin:0 0 8px 0; font-size:12.5px; color:#888888; line-height:1.6;">
                                    <strong style="color:#666666;">existing_unmanaged_default_ml</strong> — records still in the DB with
                                    <code style="background-color:#f5f5f5; padding:1px 4px; border-radius:3px; font-size:11.5px;">defaultMlFlag=1</code>
                                    not yet managed by an analyst. Please review these at the earliest.
                                </p>

                                <!-- Note -->
                                <p style="margin:0; padding-top:16px; font-size:12.5px; color:#8a6d1f; line-height:1.6;">
                                    <strong>Note:</strong> while making changes to these default Market List Flights, turn off the
                                    <code style="background-color:#f5f5f5; padding:1px 4px; border-radius:3px;">defaultMlFlag</code>
                                    and upload for the changes to be applied.
                                </p>

                            </td>
                        </tr>

                        <!-- Footer -->
                        <tr>
                            <td style="padding:14px 32px; border-top:1px solid #eeeeee;">
                                <p style="margin:0; font-size:11px; color:#aaaaaa;">
                                    Automated notification — Default ML Flights job
                                </p>
                            </td>
                        </tr>

                    </table>
                </td>
            </body>
            </html>
            """

            self.mail_client.send_email(
                'Default Marketlist - System Added Flights Alert',
                html_body,
                self.alert_email_config.get('to', []),
                is_html=True,
                attachments=attachments,
                cc=self.alert_email_config.get('cc', [])
            )

        except Exception as e:
            self.logger.error(f"Failed to send default marketlist alert email: {str(e)}\\n{traceback.format_exc()}")

    def _fetch_marketlist_flights(self, startDate, endDate):
        """
        Fetch flights from market_list and market_list_international collections.
        Equivalent to UNION query in MySQL.
        """
        try:
            # Helper function to fetch from a collection
            def fetch_from_collection(collection_name):
                collection = self.db[collection_name]
                
                # Convert startDate and endDate to datetime objects for MongoDB comparison
                start_dt = datetime(startDate.year, startDate.month, startDate.day)
                end_dt = datetime(endDate.year, endDate.month, endDate.day, 23, 59, 59)
                
                # MongoDB aggregation pipeline to parse dates and filter at DB level
                pipeline = [
                    {
                        "$addFields": {
                            "parsedDate": {
                                "$cond": {
                                    "if": { "$eq": [{ "$type": "$PerStart" }, "string"] },
                                    "then": {
                                        "$dateFromString": {
                                            "dateString": "$PerStart",
                                            "format": "%m/%d/%Y",
                                            "onError": None
                                        }
                                    },
                                    "else": "$PerStart"
                                }
                            }
                        }
                    },
                    {
                        "$match": {
                            "parsedDate": {
                                "$gte": start_dt,
                                "$lte": end_dt
                            }
                        }
                    },
                    {
                        "$project": {
                            '_id': 0,
                            'Origin': 1,
                            'Destin': 1,
                            'PerStart': 1,
                            'FlightNumber': 1,
                            'defaultMlFlag': 1,
                            'analystName': 1,
                            'createdAt': 1,
                            'parsedDate': 1
                        }
                    }
                ]
                
                cursor = collection.aggregate(pipeline)
                df = pd.DataFrame(list(cursor))
                
                if df.empty:
                    return pd.DataFrame(columns=['Origin', 'Destination', 'DepartureDate', 'FlightNumber', 'defaultMlFlag', 'analystName', 'createdAt'])
                
                # Rename Destin to Destination and parsedDate to DepartureDate for consistency
                df = df.rename(columns={'Destin': 'Destination', 'parsedDate': 'DepartureDate'})
                
                # Clean FlightNumber (trim whitespace)
                df['FlightNumber'] = df['FlightNumber'].astype(str).str.strip()
                
                return df[['Origin', 'Destination', 'DepartureDate', 'FlightNumber', 'defaultMlFlag', 'analystName', 'createdAt']]
            
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
                SELECT DepartureStation, ArrivalStation, InternationalFlag, DomesticFlag, ActualDistance
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

    def _fetch_all_unmanaged_default_ml(self) -> pd.DataFrame:
        """Fetch all existing market list records across all dates that have defaultMlFlag = 1."""
        try:
            pipeline = [
                {
                    "$match": {
                        "defaultMlFlag": 1
                    }
                }
            ]
            
            cursor = self.db['market_list'].aggregate(pipeline)
            df = pd.DataFrame(list(cursor))
            
            if df.empty:
                return pd.DataFrame(columns=['Origin', 'Destin', 'PerStart', 'FlightNumber', 'defaultMlFlag', 'analystName', 'createdAt'])
            
            if 'FlightNumber' in df.columns:
                df['FlightNumber'] = df['FlightNumber'].astype(str).str.strip()
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error fetching all unmanaged default ML flights: {e}")
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
                if str_val == '':
                    return ""
                
                if str_val.lower() in ['nan', 'none', 'null']:
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
                        # Keep datetime object for MongoDB, converting pandas Timestamp to native datetime
                        if pd.notna(val):
                            record[col] = val.to_pydatetime() if isinstance(val, pd.Timestamp) else val
                        else:
                            record[col] = None
                    
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

    # ─── Default-ML Adhoc Run ────────────────────────────────────────────────
    #
    # After new default-ML flights are inserted into market_list, push the same
    # rows into a dedicated collection (market_list_default) and trigger an adhoc
    # allocation run via the prefetch-inputs Lambda. Mirrors the mechanism used by
    # EQTAlloc (equipmentChange.py) and ScheduleThirty (schedule30.py):
    #   collection replace -> currentVersion upsert -> S3 CSV upload -> Lambda invoke

    DEFAULT_ML_ADHOC_TABLE = "market_list_default"

    def _trigger_default_ml_adhoc(self, new_flights_df):
        """Push newly-added default-ML flights to market_list_default and kick off
        an adhoc allocation run. Non-fatal: any failure is logged and swallowed so
        the primary default-ML job still completes."""
        try:
            if new_flights_df is None or new_flights_df.empty:
                self.logger.info("No new default-ML flights to run as adhoc; skipping adhoc trigger.")
                return

            run_id = str(uuid.uuid4())
            self.logger.info(f"Starting Default-ML adhoc run with run_id: {run_id}")

            adhoc_df = self._update_market_list_default(new_flights_df)
            if adhoc_df is None or adhoc_df.empty:
                self.logger.info("No records persisted to market_list_default; skipping adhoc trigger.")
                return

            version_id = str(uuid.uuid4())
            self.logger.info(f"Generated Default-ML adhoc version ID: {version_id}")

            self._update_current_version(self.DEFAULT_ML_ADHOC_TABLE, version_id)
            self._upload_adhoc_to_s3(adhoc_df, self.DEFAULT_ML_ADHOC_TABLE, version_id)
            self._invoke_default_ml_lambda(run_id)

            self.logger.info("Default-ML adhoc run triggered successfully")

        except Exception as e:
            self.logger.error(f"Failed to trigger Default-ML adhoc run: {str(e)}\n{traceback.format_exc()}")

    def _update_market_list_default(self, new_flights_df):
        """Replace market_list_default with the newly-added default-ML flights.
        Returns the DataFrame actually persisted (for S3 upload)."""
        try:
            collection = self.db[self.DEFAULT_ML_ADHOC_TABLE]

            adhoc_df = new_flights_df.copy()
            # FlightNumber stored as numeric in the market list collections.
            if 'FlightNumber' in adhoc_df.columns:
                adhoc_df['FlightNumber'] = pd.to_numeric(adhoc_df['FlightNumber'], errors='coerce')

            # fetch-inputs applies a `defaultMlFlag: 0` filter to every non-"market_list"
            # table (it's meant to exclude default-ML rows from managed adhoc tables).
            # These rows carry defaultMlFlag=1 from _build_insert_dataframe, so without
            # this they'd be filtered out and fetch-inputs would see an empty market list.
            # This adhoc run treats them as managed flights, so clear the flag on the copy
            # written to market_list_default (the source market_list rows keep flag=1).
            if 'defaultMlFlag' in adhoc_df.columns:
                adhoc_df['defaultMlFlag'] = 0

            # Each run replaces the full contents (same semantics as EQT/Thirty).
            delete_result = collection.delete_many({})
            self.logger.info(
                f"Cleared {delete_result.deleted_count} existing records from {self.DEFAULT_ML_ADHOC_TABLE}"
            )

            records = adhoc_df.to_dict('records')
            if records:
                collection.insert_many(records)
                self.logger.info(f"Inserted {len(records)} records into {self.DEFAULT_ML_ADHOC_TABLE}")
            return adhoc_df

        except Exception as e:
            self.logger.error(f"Error updating {self.DEFAULT_ML_ADHOC_TABLE}: {str(e)}\n{traceback.format_exc()}")
            raise

    def _update_current_version(self, table_name, version_id):
        """Upsert the currentVersion marker for the given adhoc table."""
        try:
            collection = self.db['currentVersion']
            collection.update_one(
                {'tableName': table_name},
                {'$set': {'curr_version': version_id, 'updated_at': datetime.utcnow()}},
                upsert=True
            )
            self.logger.info(f"Updated currentVersion for {table_name}")
        except Exception as e:
            self.logger.error(f"Error updating currentVersion for {table_name}: {str(e)}")
            raise

    def _upload_adhoc_to_s3(self, df, table_name, version_id):
        """Upload the adhoc market list CSV to S3 (version-prefixed file name)."""
        try:
            s3_client = boto3.client('s3')
            bucket_name = configsettings.S3_BUCKET_NAME
            s3_folder_path = f"{configsettings.S3_FILE_PATH}rm-admin/{table_name}"
            file_name = f"{version_id}_{table_name}"
            s3_file_path = f"{s3_folder_path}/{file_name}"

            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)

            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_file_path,
                Body=csv_buffer.getvalue()
            )
            self.logger.info(f"Uploaded adhoc market list to S3: s3://{bucket_name}/{s3_file_path}")
            return True
        except Exception as e:
            self.logger.error(f"Error uploading adhoc market list to S3: {str(e)}\n{traceback.format_exc()}")
            return False

    def _invoke_default_ml_lambda(self, run_id):
        """Invoke the prefetch-inputs Lambda to run the default-ML flights as adhoc."""
        try:
            lambda_client = boto3.client('lambda')
            lambda_function_name = configsettings.PREFETCH_INPUTS_LAMBDA

            payload = {
                "body": json.dumps({
                    "run_id": run_id,
                    "start_interval": 0,
                    "end_interval": 365,
                    "run_type": "Default_ML",
                    "market_list_table_name": self.DEFAULT_ML_ADHOC_TABLE,
                    "route_type": "direct",
                    "update_navitaire_method": "api"
                })
            }

            response = lambda_client.invoke(
                FunctionName=lambda_function_name,
                InvocationType='Event',
                Payload=json.dumps(payload)
            )

            status_code = response.get('StatusCode')
            if status_code == 202:
                self.logger.info("Default-ML adhoc Lambda invoked successfully (async)")
                return {"status": "success", "statusCode": 202}
            else:
                self.logger.error(f"Default-ML adhoc Lambda invocation failed: status {status_code}")
                return {"status": "error", "statusCode": status_code}

        except Exception as e:
            self.logger.error(f"Error invoking Default-ML adhoc Lambda: {str(e)}\n{traceback.format_exc()}")
            raise

    # ─── Audit Column Mapping ───────────────────────────────────────────────
    #
    # The Java AuditHelper writes ML audit Parquet using JavaBean property names
    # (the Java field names in MarketListEntity/AuditEntity), NOT the MongoDB
    # @Column names. input_audits.py translates those bean names back to the real
    # market_list field names on read via the single source of truth:
    #     Constants.ML_AUDIT_COLNAME_TO_MARKET_LIST_FIELD  (bean -> market_list)
    #
    # To keep THIS writer's output consistent with that reverse-mapping, we build
    # the audit rename map by INVERTING the shared constant (market_list -> bean).
    # This guarantees the default-ML audit Parquet matches the Java AuditHelper
    # output and the input_audits.py expectations, with no second map to maintain.
    #
    # DYNAMIC RULE: Any market_list column NOT in the inverted map is passed
    # through Java's Introspector.decapitalize (first char lowered when the
    # second char is lowercase) — which reproduces the bean name for all the
    # regular fields.
    #
    # ⚠️  Do NOT hand-edit a rename map here. If a new field's bean name differs
    #     from its market_list name, add it once to
    #     Constants.ML_AUDIT_COLNAME_TO_MARKET_LIST_FIELD and both the reader
    #     (input_audits.py) and this writer stay in sync automatically.

    # Fields excluded from the data-column audit loop.
    #  - UUID/Origin/Destin/PerType/PerStart/PerEnd/DOW/createdAt: Java nulls these
    #    in prepareNewRecordAudit; emitted as the fixed "cleared columns" below.
    #  - FlightNumber: the flight anchor is emitted explicitly as `flightNo` in the
    #    metadata block (identity/anchor field, excluded from the shared rename
    #    map), so skip it here to avoid a stray decapitalized `flightNumber` column.
    _AUDIT_EXCLUDED_FIELDS = {'UUID', 'Origin', 'Destin', 'PerType', 'PerStart', 'PerEnd', 'DOW', 'createdAt', 'FlightNumber'}

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

            # Invert the shared source-of-truth map (bean -> market_list) to get
            # market_list -> bean for this writer. Single source of truth lives in
            # Constants.ML_AUDIT_COLNAME_TO_MARKET_LIST_FIELD (via ConstantsUtils).
            colname_to_bean = {
                market_list_col: bean_col
                for bean_col, market_list_col in
                self.constants.ML_AUDIT_COLNAME_TO_MARKET_LIST_FIELD.items()
            }

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

                # Data columns: check inverted shared map first, then apply
                # decapitalize, then pass through unchanged
                for mongo_col in data_columns:
                    if mongo_col in colname_to_bean:
                        audit_col = colname_to_bean[mongo_col]
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
                # flightNo is the audit anchor (FlightNumber is excluded from the
                # data-column loop). Set explicitly as a clean string (no .0 float).
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
