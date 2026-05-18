import pandas as pd
from datetime import datetime, timedelta, date
import pytz
from bson import ObjectId

class FarePAHelper:
    """Handles Pricing/Analytics-related database operations using MongoDB."""

    def __init__(self, mongo_db, logger, master_db=None):
        """
        Initialize with MongoDB database instance.
        
        Args:
            mongo_db: MongoDB database instance (e.g., client['QP_DW_PRIAUT'])
            logger: Logger instance
            master_db: MongoDB database instance for common collections (qp_rm_master)
        """
        self.db = mongo_db
        self.master_db = master_db if master_db is not None else mongo_db
        self.logger = logger
        
    def collect_qp_fares(self, sectors, route, base_footnotes=None, base_ndos=None):
        """Collect QP fares data from MongoDB for rules 1001 and 2001 (Saver).
        Fetches both rules since they can have different footnotes and NDO combinations.
        Now supports footnote and NDO filtering for optimized queries.
        
        Args:
            sectors: List of sectors to fetch
            base_footnotes: List of footnotes to fetch (if None, fetch all including empty)
            base_ndos: List of NDOs to fetch (if None, fetch all including empty)
        """
        try:
            if isinstance(sectors, pd.Series):
                sectors = sectors.tolist()
            
            if not sectors:
                return pd.DataFrame()
            
            # ===== FIX: Build sector match conditions WITHOUT Fare_Direction filter =====
            sector_conditions = []
            for sector in sectors:
                origin = sector[:3]
                destination = sector[3:]
                # Match Origin-Destination (will handle both F and T)
                sector_conditions.append({
                    'Origin': origin,
                    'Destination': destination
                })
                # Match Destination-Origin (will handle both F and T)
                sector_conditions.append({
                    'Origin': destination,
                    'Destination': origin
                })
            # ===========================================================================
            
            # Build footnote conditions
            footnote_conditions = []
            if base_footnotes:
                # Include specific footnotes
                footnote_conditions.extend([
                    {'Footnote': footnote} for footnote in base_footnotes if footnote
                ])
                # Also include empty footnotes if not explicitly excluded
                if '' in base_footnotes or None in base_footnotes:
                    footnote_conditions.extend([
                        {'Footnote': {'$exists': False}},
                        {'Footnote': None},
                        {'Footnote': ''}
                    ])
            else:
                # Default: include all footnotes (empty and non-empty)
                footnote_conditions = [
                    {'Footnote': {'$exists': False}},
                    {'Footnote': None},
                    {'Footnote': ''},
                    {'Footnote': {'$exists': True, '$ne': None, '$ne': ''}}
                ]
            
            # Build NDO conditions from fare basis code
            ndo_conditions = []
            if base_ndos:
                # Extract NDO from 8th position of fare basis code
                for ndo in base_ndos:
                    if ndo:
                        ndo_conditions.append({
                            'Fare_Basis_Code': {'$regex': f'^.{{7}}{ndo}$'}
                        })
                # Also include 'X' as default NDO if not explicitly excluded
                if 'X' in base_ndos or '' in base_ndos or None in base_ndos:
                    ndo_conditions.append({
                        'Fare_Basis_Code': {'$regex': '^.{7}X$'}
                    })
            else:
                # Default: include all NDOs
                ndo_conditions = [
                    {'Fare_Basis_Code': {'$exists': True}}
                ]
            
            # Build optimized match conditions - fetch both 1001 and 2001
            # Both rules are needed since they can have different footnotes/NDOs
            if route == 'D':
                base_match = {
                    'Carrier_Code': 'QP',
                    'Rule_Nbr': {'$in': ['1001', '2001']},
                    'OW_RT': {'$in': ['O', 'C']},
                    'Fare_Direction': {'$in': ['F', 'T']},  # ← FIX: Include both F and T
                    '$and': [
                        {'$or': sector_conditions},
                        {'$or': footnote_conditions},
                        {'$or': ndo_conditions}
                    ]
                }
                
                rule_cond = {
                                'if': {'$eq': ['$Rule_Nbr', '1001']},
                                'then': 'B2C',
                                'else': 'B2B'
                            }
            elif route == 'I':
                base_match = {
                    'Carrier_Code': 'QP',
                    'Rule_Nbr': {'$in': ['R101', 'M101', 'R201', 'M201']},
                    'OW_RT': {'$in': ['O', 'C', 'R']},
                    'Fare_Direction': {'$in': ['F', 'T']},  # ← FIX: Include both F and T
                    '$and': [
                        {'$or': sector_conditions},
                        {'$or': footnote_conditions},
                        {'$or': ndo_conditions}
                    ]
                }
                
                rule_cond = {
                                'if': {'$eq': ['$Rule_Nbr', 'R101', 'R201']},
                                'then': 'B2C',
                                'else': 'B2B'
                            }
            else:
                raise ValueError("Invalid route value: ", route)
            
            # Simplified aggregation pipeline
            pipeline = [
                {'$match': base_match},
                {
                    '$project': {
                        '_id': 0,
                        'Origin': 1,
                        'Destination': 1,
                        'Fare_Direction': 1,
                        'Fare_Basis_Code': 1,
                        'Fare_Amount': 1,
                        'Currency': 1,
                        'Routing': 1,
                        'Rule_Nbr': 1,
                        'Footnote': 1,
                        # Calculate RBD and NDO
                        'RBD': {'$substr': ['$Fare_Basis_Code', 0, 2]},
                        'NDO': {'$substr': ['$Fare_Basis_Code', 7, 1]}
                    }
                },
                {
                    '$project': {
                        'Origin': 1,
                        'Destination': 1,
                        'Fare_Direction': 1,
                        'Fare_Basis_Code': 1,
                        'RBD': 1,
                        'NDO': 1,
                        'Routing': 1,
                        'Rule_Nbr': 1,
                        'Footnote': 1,
                        # Calculate SECTOR - reverse for T direction
                        'SECTOR': {
                            '$cond': {
                                'if': {'$eq': ['$Fare_Direction', 'F']},
                                'then': {'$concat': ['$Origin', '$Destination']},
                                'else': {'$concat': ['$Destination', '$Origin']}  # T: reverse it
                            }
                        },
                        'Org': {
                            '$cond': {
                                'if': {'$eq': ['$Fare_Direction', 'F']},
                                'then': '$Origin',
                                'else': '$Destination'  # T: use Destination as logical origin
                            }
                        },
                        'Dst': {
                            '$cond': {
                                'if': {'$eq': ['$Fare_Direction', 'F']},
                                'then': '$Destination',
                                'else': '$Origin'  # T: use Origin as logical destination
                            }
                        },
                        'route': '$Routing',
                        'TOTAL': '$Fare_Amount',
                        'currency': '$Currency',
                        'fare_direction': '$Fare_Direction',
                        'rule_number': '$Rule_Nbr',
                        'footnote': '$Footnote',
                        'channel': {
                            '$cond': rule_cond
                        }
                    }
                }
            ]
            
            # Execute aggregation
            all_qp_fares = list(self.db.qp_fares.aggregate(pipeline))
            qp_fares_data = pd.DataFrame(all_qp_fares)
            
            # Filter by requested sectors after calculation
            if not qp_fares_data.empty and sectors:
                original_count = len(qp_fares_data)
                qp_fares_data = qp_fares_data[qp_fares_data['SECTOR'].isin(sectors)]
                filtered_count = len(qp_fares_data)
                self.logger.info(f"Filtered QP fares: {original_count} → {filtered_count} records for requested sectors")
            
            self.logger.info(f"Collected {len(qp_fares_data)} QP fares records (rules 1001/2001) for {len(sectors)} sectors")
            if not qp_fares_data.empty:
                found_sectors = sorted(qp_fares_data['SECTOR'].unique().tolist())
                self.logger.info(f"Found sectors: {found_sectors}")
            
            return qp_fares_data
            
        except Exception as e:
            self.logger.error(f"Error collecting QP fares data: {str(e)}")
            raise

    def _identify_existing_qp_sectors(self, sectors_to_check):
        """
        Identify which sectors already exist in qp_fares with Fare_Direction F or T.
        Sectors not found will use 'W' in farebasis instead of 'B'.
        """
        try:
            # Query qp_fares to check which sectors exist
            qp_fares_check = list(self.db.qp_fares.aggregate([
                {
                    '$match': {
                        'Carrier_Code': 'QP',
                        'Fare_Direction': {'$in': ['F', 'T']}
                    }
                },
                {
                    '$project': {
                        '_id': 0,
                        'SECTOR': {
                            '$cond': {
                                'if': {'$eq': ['$Fare_Direction', 'F']},
                                'then': {'$concat': ['$Origin', '$Destination']},
                                'else': {'$concat': ['$Destination', '$Origin']}
                            }
                        }
                    }
                },
                {
                    '$group': {
                        '_id': '$SECTOR'
                    }
                }
            ]))
            
            # Extract unique sectors
            self.existing_qp_sectors = {item['_id'] for item in qp_fares_check}
            
            return self.existing_qp_sectors
            
        except Exception as e:
            self.logger.error(f"Error identifying existing QP sectors: {str(e)}")
            # On error, assume all sectors are new (use 'W')
            self.existing_qp_sectors = set()

    def collect_upsell_data(self, isSaver):
        """Collect Product Upsell data from MongoDB with base footnote and NDO support."""
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
                    'max_value': 1,
                    'pos': 1,
                    'base_footnote': 1,
                    'base_ndo': 1,
                    'flt_nbr': 1,
                    'flt_tag': 1,
                    'sales_start': 1,
                    'sales_end': 1,
                    'travel_start': 1,
                    'travel_end': 1
                }
            ))
            
            product_upsell_data = pd.DataFrame(product_upsell)
            return product_upsell_data
            
        except Exception as e:
            self.logger.error(f"Error collecting Product Upsell data: {str(e)}")
            raise
        
    def collect_upsell_master_data(self, isSaver):
        """Collect Product Upsell master data from MongoDB with base footnote and NDO support."""
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
                    'max_value': 1,
                    'pos': 1,
                    'base_footnote': 1,
                    'base_ndo': 1,
                    'flt_nbr': 1,
                    'flt_tag': 1,
                    'sales_start': 1,
                    'sales_end': 1,
                    'travel_start': 1,
                    'travel_end': 1
                }
            ))
            
            product_upsell_master_data = pd.DataFrame(product_upsell_master)
            return product_upsell_master_data
            
        except Exception as e:
            self.logger.error(f"Error collecting Product Upsell master data: {str(e)}")
            raise
        
    def collect_saver_data(self, isSaver):
        """Collect Saver data from MongoDB with footnote and NDO support."""
        try:
            saver = list(self.db.saver.find(
                {},
                {
                    '_id': 0,
                    'sector': 1,
                    'rbd': 1,
                    'fare': 1,
                    'currency': 1,
                    'pos': 1,
                    'footnote': 1,
                    'ndo': 1,
                    'connections': 1
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
            tax_master_dom = list(self.master_db.tax_mast_dom.find(
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
            tax_master_intl = list(self.master_db.tax_mast_intl.find(
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
        
    def collect_footnote_master_data(self):
        """Collect footnote master data from MongoDB (footnote_master.config_footnote_master).
        
        Returns:
            DataFrame with columns: footnote, sales_start, sales_end, travel_start, travel_end
        """
        try:
            footnote_master = list(self.db.config_footnote_master.find(
                {},
                {
                    '_id': 0,
                    'footnote': 1,
                    'sales_start': 1,
                    'sales_end': 1,
                    'travel_start': 1,
                    'travel_end': 1
                }
            ))
            
            footnote_master_data = pd.DataFrame(footnote_master)
            self.logger.info(f"Collected {len(footnote_master_data)} footnote master records")
            return footnote_master_data
            
        except Exception as e:
            self.logger.error(f"Error collecting footnote master data: {str(e)}")
            return pd.DataFrame()

    def _get_existing_sort_prices(self):
        """Get existing sort prices from QP fares for rules 5001, 5002, 5006."""
        try:
            sort_prices = {}
            
            # Query QP fares for rules 5001, 5002, 5006
            qp_sort_prices = list(self.db.qp_fares.aggregate([
                {
                    '$match': {
                        'Carrier_Code': 'QP',
                        'Rule_Nbr': {'$in': ['5001', '5002', '5006']},
                        'Fare_Direction': {'$in': ['F', 'T']}
                    }
                },
                {
                    '$project': {
                        '_id': 0,
                        'SECTOR': {
                            '$cond': {
                                'if': {'$eq': ['$Fare_Direction', 'F']},
                                'then': {'$concat': ['$Origin', '$Destination']},
                                'else': {'$concat': ['$Destination', '$Origin']}
                            }
                        },
                        'RBD': {'$substr': ['$Fare_Basis_Code', 0, 2]},
                        'Rule_Nbr': 1,
                        'Sort_Price': 1
                    }
                }
            ]))
            
            # Build lookup dictionary: sector_rbd_rule -> sort_price
            for item in qp_sort_prices:
                key = f"{item['SECTOR']}_{item['RBD']}_{item['Rule_Nbr']}"
                sort_prices[key] = item.get('Sort_Price', None)
            
            self.logger.info(f"Loaded {len(sort_prices)} existing sort prices for rules 5001, 5002, 5006")
            return sort_prices
            
        except Exception as e:
            self.logger.error(f"Error getting existing sort prices: {str(e)}")
            return {}

    def _determine_fare_direction(self, sector):
        """
        Determine the actual origin, destination, and fare direction based on QP fares convention.
        """
        try:
            origin = sector[:3]
            destination = sector[3:]
            exists_as_f = list(self.db.qp_fares.find({
                'Carrier_Code': 'QP',
                'Origin': origin,
                'Destination': destination,
                'Fare_Direction': 'F'
            }).limit(1))
            
            if exists_as_f:
                return origin, destination, 'F'
            
            exists_as_t = list(self.db.qp_fares.find({
                'Carrier_Code': 'QP',
                'Origin': destination,
                'Destination': origin,
                'Fare_Direction': 'F'
            }).limit(1))
            
            if exists_as_t:
                return destination, origin, 'T'
            if origin < destination:
                return origin, destination, 'F'
            else:
                return destination, origin, 'T'
            
        except Exception as e:
            self.logger.error(f"Error determining fare direction for {sector}: {str(e)}")
            return sector[:3], sector[3:], 'F'

    
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
        
    def save_run_audit(self, currVersion, route, analystName, start_time, run_id, ladder_df, run_type=None, sectors_list=None, status='SUCCESS', is_initial=False, failure_reason=None):
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
            failure_reason: Detailed failure reason (only used when status='FAILED')
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
                    'confirmation': 'PENDING',
                    'confirmed_at': None,
                    'confirmed_by': None,
                    'upsert_master_result': None,
                    'update_qp_fares_result': None,
                    'failure_reason': None
                }
                
                collection.insert_one(initial_audit)
                
                self.logger.info(f"Initial audit record created with status IN_PROGRESS and confirmation PENDING for run_id: {run_id}")
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
                        'status': status,
                        'failure_reason': failure_reason if status == 'FAILED' else None
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