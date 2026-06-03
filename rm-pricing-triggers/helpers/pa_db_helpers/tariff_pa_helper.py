import pandas as pd
from datetime import datetime, timedelta
import pytz


class TariffPAHelper:
    """Handles MongoDB database operations for the Tariff Sheet Automation module."""

    def __init__(self, mongo_db, master_db, logger):
        """
        Initialize with MongoDB database instances.

        Args:
            mongo_db: MongoDB database instance (qp_dw_priaut)
            master_db: MongoDB database instance for master/reference data (qp_rm_master)
            logger: Logger instance
        """
        self.db = mongo_db
        self.master_db = master_db
        self.logger = logger

    def get_active_sectors(self):
        """Read all documents from the active_sectors_tariff collection.

        Returns:
            pd.DataFrame: DataFrame containing all active sectors with columns
                          sector, origin, destination, stops, source, updated_at, updated_by.
        """
        try:
            sectors = list(self.db.active_sectors_tariff.find({}, {'_id': 0}))
            sectors_df = pd.DataFrame(sectors)
            self.logger.info(f"Retrieved {len(sectors_df)} active tariff sectors from MongoDB")
            return sectors_df
        except Exception as e:
            self.logger.error(f"Error retrieving active tariff sectors: {str(e)}")
            raise

    def save_active_sectors(self, sectors_df):
        """Upsert sectors into the active_sectors_tariff collection.

        Uses the 'sector' field as the key for upsert operations.

        Args:
            sectors_df: DataFrame containing sector records to upsert.
        """
        try:
            if sectors_df is None or sectors_df.empty:
                self.logger.info("No sectors to save")
                return

            collection = self.db.active_sectors_tariff
            records = sectors_df.to_dict('records')
            upsert_count = 0

            for record in records:
                record['updated_at'] = datetime.now(pytz.timezone('Asia/Kolkata'))
                collection.update_one(
                    {'sector': record['sector']},
                    {'$set': record},
                    upsert=True
                )
                upsert_count += 1

            self.logger.info(f"Upserted {upsert_count} sectors into active_sectors_tariff")
        except Exception as e:
            self.logger.error(f"Error saving active tariff sectors: {str(e)}")
            raise

    def get_fares_for_monitor(self, active_sectors, monitored_rbds, b2c_rbds):
        """Collect QP fares for the daily fare monitor check.

        The 'fare dump' is the qp_fares collection (same source as tariff generation).
        Returns fares for the monitored RBDs on active sectors:
          - B2C RBDs: rule 1001 only, empty footnote
          - W / TBF RBDs: any rule, any footnote (matched by first 2 chars of Fare_Basis_Code)

        Args:
            active_sectors: list of 6-char active sector codes.
            monitored_rbds: full list of RBDs to monitor (B2C + W + TBF).
            b2c_rbds: list of B2C RBDs (these require rule 1001 + empty footnote).

        Returns:
            pd.DataFrame with columns: sector, rbd, base_fare, rule_number, footnote
        """
        try:
            fares_df = self.collect_qp_fares_for_tariff(active_sectors)
            if fares_df is None or fares_df.empty:
                return pd.DataFrame(columns=['sector', 'rbd', 'base_fare', 'rule_number', 'footnote'])

            # Keep only monitored RBDs
            df = fares_df[fares_df['rbd'].isin(monitored_rbds)].copy()

            # For B2C RBDs, restrict to rule 1001 with empty footnote
            b2c_set = set(b2c_rbds)
            is_b2c = df['rbd'].isin(b2c_set)
            footnote_empty = (
                df['footnote'].isna() |
                (df['footnote'].astype(str).str.strip() == '') |
                (df['footnote'].astype(str).str.lower() == 'nan')
            )
            keep_b2c = is_b2c & (df['rule_number'].astype(str) == '1001') & footnote_empty
            keep_non_b2c = ~is_b2c

            df = df[keep_b2c | keep_non_b2c].copy()
            self.logger.info(f"Collected {len(df)} monitored fares for daily check across {df['sector'].nunique()} sectors")
            return df

        except Exception as e:
            self.logger.error(f"Error collecting fares for monitor: {str(e)}")
            raise

    def get_weekly_selling_fares(self, active_sectors):
        """Get min/max SellingFare per active sector from run_summary and run_summary_connections.

        Sources (qp_dw_rmalloc database):
          - run_summary (direct): sector = Origin + Destin
          - run_summary_connections (through): sector = first 3 of Sector1 + last 3 of Sector2

        Uses MongoDB aggregation ($match by CreatedAt last 7 days + sector, then $group)
        for efficiency on large collections (2.1M+ run_summary, 526K+ connections).

        Args:
            active_sectors: set/list of 6-char active sector codes.

        Returns:
            dict: {sector: {'min_fare': float, 'max_fare': float, 'count': int}}
        """
        try:
            active_set = set(active_sectors)
            ist = pytz.timezone('Asia/Kolkata')
            start_date = (datetime.now(ist) - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')

            # run_summary lives in qp_dw_rmalloc — reach it via the shared client
            db_rmalloc = self.db.client['qp_dw_rmalloc']

            sector_fares = {}

            # --- Direct sectors from run_summary ---
            active_origins_destins = [{'Origin': s[:3], 'Destin': s[3:]} for s in active_set]
            if active_origins_destins:
                direct_pipeline = [
                    {'$match': {
                        'CreatedAt': {'$gte': start_date},
                        '$or': active_origins_destins
                    }},
                    {'$addFields': {
                        'sector': {'$concat': ['$Origin', '$Destin']},
                        'selling_fare_num': {
                            '$convert': {'input': '$SellingFare', 'to': 'double', 'onError': None, 'onNull': None}
                        }
                    }},
                    {'$match': {'selling_fare_num': {'$ne': None, '$gt': 0}}},
                    {'$group': {
                        '_id': '$sector',
                        'min_fare': {'$min': '$selling_fare_num'},
                        'max_fare': {'$max': '$selling_fare_num'},
                        'count': {'$sum': 1}
                    }}
                ]
                direct_results = list(db_rmalloc.run_summary.aggregate(direct_pipeline, allowDiskUse=True))
                for doc in direct_results:
                    sector = doc['_id']
                    if sector in active_set:
                        sector_fares[sector] = {
                            'min_fare': doc['min_fare'],
                            'max_fare': doc['max_fare'],
                            'count': doc['count']
                        }
                self.logger.info(f"Weekly direct: {len(direct_results)} sectors from run_summary")

            # --- Through sectors from run_summary_connections ---
            conn_pipeline = [
                {'$match': {'CreatedAt': {'$gte': start_date}}},
                {'$addFields': {
                    'through_sector': {
                        '$concat': [
                            {'$substr': ['$Sector1', 0, 3]},
                            {'$substr': ['$Sector2', 3, 3]}
                        ]
                    },
                    'selling_fare_num': {
                        '$convert': {'input': '$SellingFare', 'to': 'double', 'onError': None, 'onNull': None}
                    }
                }},
                {'$match': {
                    'through_sector': {'$in': list(active_set)},
                    'selling_fare_num': {'$ne': None, '$gt': 0}
                }},
                {'$group': {
                    '_id': '$through_sector',
                    'min_fare': {'$min': '$selling_fare_num'},
                    'max_fare': {'$max': '$selling_fare_num'},
                    'count': {'$sum': 1}
                }}
            ]
            conn_results = list(db_rmalloc.run_summary_connections.aggregate(conn_pipeline, allowDiskUse=True))
            for doc in conn_results:
                sector = doc['_id']
                if sector in active_set:
                    if sector in sector_fares:
                        sector_fares[sector]['min_fare'] = min(sector_fares[sector]['min_fare'], doc['min_fare'])
                        sector_fares[sector]['max_fare'] = max(sector_fares[sector]['max_fare'], doc['max_fare'])
                        sector_fares[sector]['count'] += doc['count']
                    else:
                        sector_fares[sector] = {
                            'min_fare': doc['min_fare'],
                            'max_fare': doc['max_fare'],
                            'count': doc['count']
                        }
            self.logger.info(f"Weekly connections: {len(conn_results)} through sectors from run_summary_connections")
            self.logger.info(f"Weekly selling fares: {len(sector_fares)} total sectors")

            return sector_fares

        except Exception as e:
            self.logger.error(f"Error fetching weekly selling fares: {str(e)}")
            return {}

    def collect_qp_fares_for_tariff(self, active_sectors):
        """Collect QP fares from the qp_fares collection for tariff sheet generation.
        
        Queries the same qp_fares collection used by ActionSectors and FareFiling,
        applying domestic route filters. Returns all fares for active sectors with
        proper sector derivation based on Fare_Direction.
        
        Args:
            active_sectors: list of 6-char sector codes (e.g., ['BOMDEL', 'DELBOM', ...])
        
        Returns:
            pd.DataFrame with columns: sector, origin, destination, rbd, base_fare, 
                                       rule_number, footnote, channel, fare_direction
        """
        try:
            # Get domestic stations for filtering
            domestic_stations = list(self.master_db.config_station_master.find(
                {'route': 'D'},
                {'station': 1, '_id': 0}
            ))
            station_list = [s['station'] for s in domestic_stations]
            
            # Base match: all domestic QP fares (both B2C rule 1001 and B2B rule 2001)
            # Also include fares without rule filter for W-class and TBF extraction
            base_match = {
                'Carrier_Code': 'QP',
                'Fare_Direction': {'$in': ['F', 'T']},
                'OW_RT': {'$in': ['O', 'C']},
                '$or': [
                    {'Origin': {'$in': station_list}},
                    {'Destination': {'$in': station_list}}
                ]
            }
            
            pipeline = [
                {'$match': base_match},
                {
                    '$project': {
                        '_id': 0,
                        'sector': {
                            '$cond': {
                                'if': {'$eq': ['$Fare_Direction', 'F']},
                                'then': {'$concat': ['$Origin', '$Destination']},
                                'else': {'$concat': ['$Destination', '$Origin']}
                            }
                        },
                        'origin': {
                            '$cond': {
                                'if': {'$eq': ['$Fare_Direction', 'F']},
                                'then': '$Origin',
                                'else': '$Destination'
                            }
                        },
                        'destination': {
                            '$cond': {
                                'if': {'$eq': ['$Fare_Direction', 'F']},
                                'then': '$Destination',
                                'else': '$Origin'
                            }
                        },
                        'rbd': {'$substr': ['$Fare_Basis_Code', 0, 2]},
                        'base_fare': '$Fare_Amount',
                        'rule_number': '$Rule_Nbr',
                        'footnote': '$Footnote',
                        'fare_direction': '$Fare_Direction',
                        'fare_basis_code': '$Fare_Basis_Code',
                        'channel': {
                            '$switch': {
                                'branches': [
                                    {'case': {'$eq': ['$Rule_Nbr', '1001']}, 'then': 'B2C'},
                                    {'case': {'$eq': ['$Rule_Nbr', '2001']}, 'then': 'B2B'}
                                ],
                                'default': 'Other'
                            }
                        }
                    }
                }
            ]
            
            all_fares = list(self.db.qp_fares.aggregate(pipeline, allowDiskUse=True))
            fares_df = pd.DataFrame(all_fares)
            
            if fares_df.empty:
                self.logger.warning("No QP fares found in qp_fares collection")
                return fares_df
            
            # Filter to active sectors only
            fares_df = fares_df[fares_df['sector'].isin(active_sectors)].copy()
            self.logger.info(f"Collected {len(fares_df)} QP fares for {fares_df['sector'].nunique()} active sectors")
            
            return fares_df
            
        except Exception as e:
            self.logger.error(f"Error collecting QP fares for tariff: {str(e)}")
            raise

    def get_live_tariff_sheet(self):
        """Retrieve the document with status='active' from live_tariff_sheet collection.

        Returns:
            dict or None: The active live tariff sheet document, or None if not found.
        """
        try:
            live_sheet = self.db.live_tariff_sheet.find_one({'status': 'active'})
            if live_sheet:
                self.logger.info("Retrieved active live tariff sheet")
            else:
                self.logger.warning("No active live tariff sheet found")
            return live_sheet
        except Exception as e:
            self.logger.error(f"Error retrieving live tariff sheet: {str(e)}")
            raise

    def save_generated_tariff(self, tariff_doc):
        """Insert a document into the tariff_gen_output collection.

        Args:
            tariff_doc: Dictionary containing the generated tariff sheet data.

        Returns:
            str: The inserted document's ID as a string.
        """
        try:
            tariff_doc['generated_at'] = datetime.now(pytz.timezone('Asia/Kolkata'))
            result = self.db.tariff_gen_output.insert_one(tariff_doc)
            self.logger.info(f"Saved generated tariff sheet with ID: {result.inserted_id}")
            return str(result.inserted_id)
        except Exception as e:
            self.logger.error(f"Error saving generated tariff sheet: {str(e)}")
            raise

    def get_station_master(self):
        """Read from station_master collection in master_db.

        Returns:
            pd.DataFrame: DataFrame containing station master data.
        """
        try:
            station_data = list(self.master_db.config_station_master.find({}, {'_id': 0}))
            station_df = pd.DataFrame(station_data)
            self.logger.info(f"Retrieved {len(station_df)} records from station master")
            return station_df
        except Exception as e:
            self.logger.error(f"Error retrieving station master: {str(e)}")
            raise

    def get_domestic_tax_master(self):
        """Read from domestic_tax_master collection in master_db.

        Returns:
            pd.DataFrame: DataFrame containing domestic tax master data.
        """
        try:
            tax_data = list(self.master_db.tax_mast_dom.find({}, {'_id': 0}))
            tax_df = pd.DataFrame(tax_data)
            self.logger.info(f"Retrieved {len(tax_df)} records from domestic tax master")
            return tax_df
        except Exception as e:
            self.logger.error(f"Error retrieving domestic tax master: {str(e)}")
            raise

    def get_manual_sectors(self):
        """Retrieve sectors with source='manual' from active_sectors_tariff.

        Returns:
            pd.DataFrame: DataFrame containing manually added sectors.
        """
        try:
            manual_sectors = list(self.db.active_sectors_tariff.find(
                {'source': 'manual'},
                {'_id': 0}
            ))
            manual_df = pd.DataFrame(manual_sectors)
            self.logger.info(f"Retrieved {len(manual_df)} manual sectors from active_sectors_tariff")
            return manual_df
        except Exception as e:
            self.logger.error(f"Error retrieving manual sectors: {str(e)}")
            raise

    def save_run_audit(self, analyst, run_id, status, module, failure_reason=None, start_time=None, end_time=None):
        """Save audit record to tariff_gen_audit collection.

        Follows the same pattern as ladderGen audits:
        - Initial call with status='IN_PROGRESS' and start_time
        - Final call with status='SUCCESS' or 'FAILED', end_time, and failure_reason

        Args:
            analyst: Name of the analyst or system user who triggered the run.
            run_id: Unique identifier for this run.
            status: Run status ('IN_PROGRESS', 'SUCCESS', 'FAILED').
            module: Module name (e.g., 'tariff_gen').
            failure_reason: Optional failure reason if the run failed.
            start_time: Run start timestamp (IST).
            end_time: Run end timestamp (IST). Set on completion.
        """
        try:
            if status == 'IN_PROGRESS':
                # Initial audit record
                audit_record = {
                    'run_id': run_id,
                    'analyst': analyst,
                    'module': module,
                    'status': status,
                    'start_time': start_time or datetime.now(pytz.timezone('Asia/Kolkata')),
                    'end_time': None,
                    'failure_reason': None
                }
                self.db.tariff_gen_audit.insert_one(audit_record)
            else:
                # Update existing record with final status
                update_doc = {
                    '$set': {
                        'status': status,
                        'end_time': end_time or datetime.now(pytz.timezone('Asia/Kolkata')),
                        'failure_reason': failure_reason
                    }
                }
                self.db.tariff_gen_audit.update_one({'run_id': run_id}, update_doc)

            self.logger.info(f"Audit [{status}] for run_id: {run_id}, module: {module}")
        except Exception as e:
            self.logger.error(f"Error saving run audit: {str(e)}")
            raise

    def save_scheduled_job_audit(self, analyst, run_id, status, module, failure_reason=None):
        """Save audit record to scheduled_job_run_audits collection.

        Used by scheduled modules (sector_monitor, fare_monitor) that run on AWS cron.

        Args:
            analyst: Name of the analyst or system user who triggered the run.
            run_id: Unique identifier for this run.
            status: Run status (e.g., 'IN_PROGRESS', 'SUCCESS', 'FAILED').
            module: Module name (e.g., 'sector_monitor', 'fare_monitor').
            failure_reason: Optional failure reason if the run failed.
        """
        try:
            audit_record = {
                'run_id': run_id,
                'analyst': analyst,
                'module': module,
                'status': status,
                'timestamp': datetime.now(pytz.timezone('Asia/Kolkata')),
                'failure_reason': failure_reason
            }

            self.db.scheduled_job_run_audits.insert_one(audit_record)
            self.logger.info(f"Saved scheduled job audit for run_id: {run_id}, module: {module}, status: {status}")
        except Exception as e:
            self.logger.error(f"Error saving scheduled job audit: {str(e)}")
            raise
