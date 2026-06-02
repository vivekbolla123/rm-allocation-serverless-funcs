import pandas as pd
from datetime import datetime
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

    def get_fare_dump(self):
        """Retrieve the latest document with status='active' from fare_dump_tariff collection.

        Returns:
            dict or None: The active fare dump document, or None if not found.
        """
        try:
            fare_dump = self.db.fare_dump_tariff.find_one(
                {'status': 'active'},
                sort=[('uploaded_at', -1)]
            )
            if fare_dump:
                self.logger.info(f"Retrieved active fare dump: {fare_dump.get('filename', 'unknown')}")
            else:
                self.logger.warning("No active fare dump found in fare_dump_tariff")
            return fare_dump
        except Exception as e:
            self.logger.error(f"Error retrieving fare dump: {str(e)}")
            raise

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

    def save_run_audit(self, analyst, run_id, status, module, error_message=None):
        """Save audit record to tariff_gen_audit collection.

        Args:
            analyst: Name of the analyst or system user who triggered the run.
            run_id: Unique identifier for this run.
            status: Run status (e.g., 'IN_PROGRESS', 'SUCCESS', 'FAILED').
            module: Module name (e.g., 'tariff_gen').
            error_message: Optional error message if the run failed.
        """
        try:
            audit_record = {
                'run_id': run_id,
                'analyst': analyst,
                'module': module,
                'status': status,
                'timestamp': datetime.now(pytz.timezone('Asia/Kolkata')),
                'error_message': error_message
            }

            self.db.tariff_gen_audit.insert_one(audit_record)
            self.logger.info(f"Saved audit record for run_id: {run_id}, module: {module}, status: {status}")
        except Exception as e:
            self.logger.error(f"Error saving run audit: {str(e)}")
            raise

    def save_scheduled_job_audit(self, analyst, run_id, status, module, error_message=None):
        """Save audit record to scheduled_job_run_audits collection.

        Used by scheduled modules (sector_monitor, fare_monitor) that run on AWS cron.

        Args:
            analyst: Name of the analyst or system user who triggered the run.
            run_id: Unique identifier for this run.
            status: Run status (e.g., 'IN_PROGRESS', 'SUCCESS', 'FAILED').
            module: Module name (e.g., 'sector_monitor', 'fare_monitor').
            error_message: Optional error message if the run failed.
        """
        try:
            audit_record = {
                'run_id': run_id,
                'analyst': analyst,
                'module': module,
                'status': status,
                'timestamp': datetime.now(pytz.timezone('Asia/Kolkata')),
                'error_message': error_message
            }

            self.db.scheduled_job_run_audits.insert_one(audit_record)
            self.logger.info(f"Saved scheduled job audit for run_id: {run_id}, module: {module}, status: {status}")
        except Exception as e:
            self.logger.error(f"Error saving scheduled job audit: {str(e)}")
            raise
