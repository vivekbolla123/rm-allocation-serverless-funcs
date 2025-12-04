import pandas as pd
from datetime import datetime, timedelta
import pytz
from bson import ObjectId

class FarePAHelper:
    """Handles Pricing/Analytics-related database operations using MongoDB."""

    def __init__(self, mongo_db, logger):
        """
        Initialize with MongoDB database instance.
        
        Args:
            mongo_db: MongoDB database instance (e.g., client['QP_DW_PRIAUT'])
            logger: Logger instance
        """
        self.db = mongo_db
        self.logger = logger
        
    def collect_qp_fares(self, sectors):
        """Collect QP fares data from MongoDB for rule 1001 (Saver).
        Filters data to only include active sectors."""
        try:
            if isinstance(sectors, pd.Series):
                sectors = sectors.tolist()
            
            # Build base match conditions for rule 1001 only
            base_match = {
                'Carrier_Code': 'QP',
                'Rule_Nbr': '1001',  # Only get Saver fares
                'Fare_Direction': {'$in': ['F', 'T']},
                'OW_RT': {'$in': ['O', 'C']},
                '$or': [
                    {'Footnote': {'$exists': False}},
                    {'Footnote': None},
                    {'Footnote': ''}
                ]
            }
            
            # MongoDB aggregation pipeline
            pipeline = [
                {'$match': base_match},
                {
                    '$project': {
                        '_id': 0,
                        'SECTOR': {
                            '$cond': {
                                'if': {'$eq': ['$Fare_Direction', 'F']},
                                'then': {'$concat': ['$Origin', '$Destination']},
                                'else': {
                                    '$cond': {
                                        'if': {'$eq': ['$Fare_Direction', 'T']},
                                        'then': {'$concat': ['$Destination', '$Origin']},
                                        'else': {'$concat': ['$Origin', '$Destination']}
                                    }
                                }
                            }
                        },
                        'Org': {
                            '$cond': {
                                'if': {'$eq': ['$Fare_Direction', 'F']},
                                'then': '$Origin',
                                'else': {
                                    '$cond': {
                                        'if': {'$eq': ['$Fare_Direction', 'T']},
                                        'then': '$Destination',
                                        'else': '$Origin'
                                    }
                                }
                            }
                        },
                        'Dst': {
                            '$cond': {
                                'if': {'$eq': ['$Fare_Direction', 'F']},
                                'then': '$Destination',
                                'else': {
                                    '$cond': {
                                        'if': {'$eq': ['$Fare_Direction', 'T']},
                                        'then': '$Origin',
                                        'else': '$Destination'
                                    }
                                }
                            }
                        },
                        'route': '$Routing',
                        'RBD': {'$substr': ['$Fare_Basis_Code', 0, 2]},
                        'Fare_Basis_Code': 1,
                        'TOTAL': '$Fare_Amount',
                        'currency': '$Currency',
                        'fare_direction': '$Fare_Direction',
                        'rule_number': '$Rule_Nbr',
                        'channel': 'B2C'
                    }
                }
            ]
            
            # Execute aggregation
            all_qp_fares = list(self.db.qp_fares.aggregate(pipeline))
            qp_fares_data = pd.DataFrame(all_qp_fares)
            
            # Filter for requested sectors
            if not qp_fares_data.empty and sectors:
                qp_fares_data = qp_fares_data[qp_fares_data['SECTOR'].isin(sectors)]
            
            return qp_fares_data
            
        except Exception as e:
            self.logger.error(f"Error collecting QP fares data: {str(e)}")
            raise

    def collect_upsell_data(self, isSaver):
        """Collect Product Upsell data from MongoDB."""
        try:
            product_upsell = list(self.db.product_upsell.find(
                {},
                {
                    '_id': 0,
                    'rule_nbr': 1,
                    'sector': 1,
                    'rbd_start': 1,
                    'rbd_end': 1,
                    'footnote': 1,
                    'type': 1,
                    'value': 1,
                    'ow_rt': 1,
                    'ndo': 1,
                    'sort_price': 1,
                    'base_all_in': 1,
                    'min_value': 1,
                    'max_value': 1
                }
            ))
            
            product_upsell_data = pd.DataFrame(product_upsell)
            return product_upsell_data
            
        except Exception as e:
            self.logger.error(f"Error collecting Product Upsell data: {str(e)}")
            raise
        
    def collect_upsell_master_data(self, isSaver):
        """Collect Product Upsell master data from MongoDB."""
        try:
            product_upsell_master = list(self.db.product_upsell_master.find(
                {},
                {
                    '_id': 0,
                    'rule_nbr': 1,
                    'sector': 1,
                    'rbd_start': 1,
                    'rbd_end': 1,
                    'footnote': 1,
                    'type': 1,
                    'value': 1,
                    'ow_rt': 1,
                    'ndo': 1,
                    'sort_price': 1,
                    'base_all_in': 1,
                    'min_value': 1,
                    'max_value': 1
                }
            ))
            
            product_upsell_master_data = pd.DataFrame(product_upsell_master)
            return product_upsell_master_data
            
        except Exception as e:
            self.logger.error(f"Error collecting Product Upsell master data: {str(e)}")
            raise
        
    def collect_saver_data(self, isSaver):
        """Collect Saver data from MongoDB."""
        try:
            saver = list(self.db.saver.find(
                {},
                {
                    '_id': 0,
                    'sector': 1,
                    'rbd': 1,
                    'fare': 1
                }
            ))
            
            saver_data = pd.DataFrame(saver)
            return saver_data
            
        except Exception as e:
            self.logger.error(f"Error collecting Saver data: {str(e)}")
            raise

    def collect_tax_master_data(self):
        """Collect both domestic and international tax master data from MongoDB."""
        try:
            # Domestic tax data
            tax_master_dom = list(self.db.tax_mast_dom.find(
                {},
                {
                    '_id': 0,
                    'Station': 1,
                    'Code': 1,
                    'ASF': 1,
                    'CUTE': 1,
                    'PSF': 1,
                    'RCS': 1,
                    'UDF_DF': 1,
                    'GST': 1
                }
            ))
            tax_master_dom_data = pd.DataFrame(tax_master_dom)
            
            # International tax data
            tax_master_intl = list(self.db.tax_mast_intl.find(
                {},
                {
                    '_id': 0,
                    'Currency': 1,
                    'Sectors': 1,
                    'GST': 1,
                    'CUTE': 1,
                    'ASF': 1,
                    'PSF_ADF': 1,
                    'UDF': 1,
                    'PSC_PZ': 1,
                    'RCS_CUTE': 1,
                    'RCS': 1,
                    'Tax1': 1,
                    'Tax2': 1,
                    'Tax3': 1,
                    'Tax4': 1,
                    'Tax5': 1
                }
            ))
            tax_master_intl_data = pd.DataFrame(tax_master_intl)
            
            if tax_master_dom_data.empty and tax_master_intl_data.empty:
                self.failure_reason = "No tax master data available"
                return None, None, self.failure_reason
            
            return tax_master_dom_data, tax_master_intl_data, None
            
        except Exception as e:
            self.logger.error(f"Error collecting tax master data: {str(e)}")
            self.failure_reason = f"Tax master data collection failed: {str(e)}"
            return None, None, self.failure_reason 
        
    def collect_product_rules_data(self):
        """Collect product rules data from MongoDB."""
        try:
            product_rules = list(self.db.product_rules.find(
                {},
                {
                    '_id': 0,
                    'rule_nbr': 1,
                    'csx': 1,
                    'letter': 1,
                }
            ))
            
            product_rules_data = pd.DataFrame(product_rules)
            return product_rules_data
            
        except Exception as e:
            self.logger.error(f"Error collecting product rules data: {str(e)}")
            raise
    
    def save_ladder_to_database(self, route, analystName, ladder_df, run_id):
        """Save fare ladder to MongoDB collection."""
        try:
            if ladder_df.empty:
                self.logger.warning("No ladder data to save")
                return None
            
            collection_name = 'ladder_revision_output'
            
            # Add metadata columns
            ladder_df = ladder_df.copy()
            ladder_df['run_id'] = run_id
            ladder_df['created_at'] = datetime.now(pytz.timezone('Asia/Kolkata'))
            ladder_df['route'] = route
            ladder_df['analyst'] = analystName
            
            # Convert to dict for MongoDB insertion
            records = ladder_df.to_dict('records')
            
            # Insert documents
            self.db[collection_name].insert_many(records)
            
            self.logger.info(f"Saved {len(records)} fare records to MongoDB collection: {collection_name}")
            
            return collection_name
            
        except Exception as e:
            self.logger.error(f"Error saving fare ladder to database: {str(e)}")
            raise
    
    def get_current_version(self):
        """Get current table versions from MongoDB."""
        try:
            versions = list(self.db.current_table_version.find(
                {'table_name': {'$in': ['product_upsell', 'product_upsell_master', 'saver', 'qp_fares', 'tax_mast_dom', 'tax_mast_intl']}},
                {'_id': 0}
            ))
            
            currVersion = pd.DataFrame(versions)
            return currVersion
            
        except Exception as e:
            self.logger.error(f"Unable to get current version: {str(e)}")
            raise
        
    def save_run_audit(self, currVersion, route, analystName, start_time, run_id, ladder_df, 
                    run_type=None, sectors_list=None, status='SUCCESS', is_initial=False):
        """Save or update audit information for fare filing runs.
        
        Args:
            currVersion: Current version DataFrame
            route: Route type ('D' or 'I')
            analystName: Name of analyst
            start_time: Run start time
            run_id: Unique identifier for this run
            ladder_df: Result DataFrame from fare ladder generation (None for initial insert)
            run_type: Type of run ('Saver' or 'Product Upsell')
            sectors_list: List of sectors processed
            status: 'IN_PROGRESS', 'SUCCESS', or 'FAILED'
            is_initial: If True, performs INSERT for initial audit record
        """
        try:
            collection = self.db.fare_filing_run_audits
            
            if is_initial:
                # Initial audit record - insert with IN_PROGRESS status
                tax_table = 'tax_mast_dom' if route == 'D' else 'tax_mast_intl'
                
                # Get version info safely
                product_upsell_version = None
                product_upsell_master_version = None
                saver_version = None
                qp_fares_version = None
                tax_master_version = None
                
                try:
                    if currVersion is not None and not currVersion.empty:
                        product_upsell_rows = currVersion[currVersion['table_name'] == 'product_upsell']
                        product_upsell_master_rows = currVersion[currVersion['table_name'] == 'product_upsell_master']
                        saver_rows = currVersion[currVersion['table_name'] == 'saver']
                        qp_version_rows = currVersion[currVersion['table_name'] == 'qp_fares']
                        tax_version_rows = currVersion[currVersion['table_name'] == tax_table]
                        
                        product_upsell_version = product_upsell_rows['curr_version'].values[0] if not product_upsell_rows.empty else None
                        product_upsell_master_version = product_upsell_master_rows['curr_version'].values[0] if not product_upsell_master_rows.empty else None
                        saver_version = saver_rows['curr_version'].values[0] if not saver_rows.empty else None
                        qp_fares_version = qp_version_rows['curr_version'].values[0] if not qp_version_rows.empty else None
                        tax_master_version = tax_version_rows['curr_version'].values[0] if not tax_version_rows.empty else None
                except Exception:
                    pass
                
                initial_audit = {
                    'run_id': run_id,
                    'start_time': start_time,
                    'end_time': None,
                    'status': 'IN_PROGRESS',
                    'run_type': run_type,
                    'sectors_list': sectors_list,
                    'total_fares_generated': None,
                    'unique_sectors': None,
                    'unique_rules': None,
                    'route': route,
                    'product_upsell_version': product_upsell_version,
                    'product_upsell_master_version': product_upsell_master_version,
                    'saver_version': saver_version,
                    'qp_fares_version': qp_fares_version,
                    'tax_master_version': tax_master_version,
                    'analyst': analystName,
                    'failure_reason': None
                }
                
                collection.insert_one(initial_audit)
                
                self.logger.info(f"Initial audit record created with status IN_PROGRESS for run_id: {run_id}")
                return 'fare_filing_run_audits'
                
            else:
                # Update existing audit record with final results
                if status == 'SUCCESS' and (ladder_df is None or ladder_df.empty):
                    status = 'FAILED'
                    
                total_fares = len(ladder_df) if ladder_df is not None and not ladder_df.empty else 0
                unique_sectors = len(sectors_list) if sectors_list else 0
                unique_rules = ladder_df['Rule_Nbr'].nunique() if ladder_df is not None and not ladder_df.empty else 0
                
                update_doc = {
                    '$set': {
                        'end_time': datetime.now(pytz.timezone('Asia/Kolkata')),
                        'run_type': run_type,
                        'sectors_list': sectors_list,
                        'total_fares_generated': total_fares,
                        'unique_sectors': unique_sectors,
                        'unique_rules': unique_rules,
                        'status': status
                    }
                }
                
                collection.update_one(
                    {'run_id': run_id},
                    update_doc
                )
                
                self.logger.info(f"Audit record updated with status {status} for run_id: {run_id}")
                self.logger.info(f"Run Type: {run_type}, Sectors: {sectors_list}")
                return 'fare_filing_run_audits'
            
        except Exception as e:
            self.logger.error(f"Error saving/updating audit: {str(e)}")
            if not is_initial:
                # For update failures, try minimal fallback
                try:
                    collection.update_one(
                        {'run_id': run_id},
                        {
                            '$set': {
                                'end_time': datetime.now(pytz.timezone('Asia/Kolkata')),
                                'status': 'FAILED',
                                'failure_reason': f"Audit update failed: {str(e)}"
                            }
                        }
                    )
                    
                    return 'fare_filing_run_audits'
                except:
                    self.logger.error("Failed to save even minimal audit update")
                    return None
            else:
                return None