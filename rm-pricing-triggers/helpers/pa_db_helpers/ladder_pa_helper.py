import pandas as pd
from datetime import datetime, timedelta
import pytz
from bson import ObjectId

class LadderPAHelper:
    """Handles Ladder Pricing/Analytics-related database operations using MongoDB."""
    
    def __init__(self, mongo_db, fares_db, logger, calculate_all_in_fare_func=None):
        """
        Initialize with MongoDB database instance.
        
        Args:
            mongo_db: MongoDB database instance (e.g., client['QP_DW_PRIAUT'])
            logger: Logger instance
            calculate_all_in_fare_func: Function to calculate all-in fares
        """
        self.db = mongo_db
        self.fares_db = fares_db
        self.logger = logger
        self.failure_reason = None
        self.calculate_all_in_fare_func = calculate_all_in_fare_func
        
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
    
    def collect_market_fares(self, route, sector):
        """Collect individual market fares for constraint checking. Optimized for large datasets."""
        try:
            # Date filters - Keep as datetime objects
            now = datetime.now()
            date_filter = {
                'departure_date': {
                    '$gte': now.replace(hour=0, minute=0, second=0, microsecond=0),
                    '$lte': (now + timedelta(days=365)).replace(hour=23, minute=59, second=59, microsecond=999999)
                },
                'created_at': {'$gt': now - timedelta(hours=24)}
            }
            
            # Route filter
            if route == 'D':
                route_filter = {'outbound_stop': "0"}
            elif route == 'I':
                route_filter = {'outbound_stop': {'$gte': "0"}}
            else:
                raise ValueError(f"Invalid route: {route}")
            
            # Common match conditions
            match_stage = {
                'outbound_airline_code': {'$nin': ['9I', 'IC', 'S9', 'S5', 'QP', 'G8']},
                'channel': 'B2C',
                '$expr': {
                    '$eq': [
                        {'$concat': ['$source_airport', '$destination_airport']},
                        sector
                    ]
                },
                **date_filter,
                **route_filter
            }
            
            pipeline = [
                {'$match': match_stage},
                {
                    '$addFields': {
                        'gross_fare_numeric': {'$toDouble': '$gross_fare'}
                    }
                },
                # First count instances of each fare
                {
                    '$group': {
                        '_id': {
                            'source': '$source_airport',
                            'dest': '$destination_airport',
                            'channel': '$channel',
                            'fare': '$gross_fare_numeric'
                        },
                        'airlines': {'$addToSet': '$outbound_airline_code'},
                        'instances': {'$sum': 1}
                    }
                },
                {'$unwind': '$airlines'},
                {'$sort': {'_id.fare': 1}},
                {
                    '$group': {
                        '_id': {
                            'channel': '$_id.channel',
                            'airline': '$airlines',
                            'source': '$_id.source',
                            'dest': '$_id.dest'
                        },
                        'gross_fare': {'$first': '$_id.fare'},
                        'instances': {'$first': '$instances'}
                    }
                },
                {
                    '$project': {
                        '_id': 0,
                        'source_airport': '$_id.source',
                        'destination_airport': '$_id.dest',
                        'channel': '$_id.channel',
                        'gross_fare': 1,
                        'outbound_airline_code': '$_id.airline',
                        'instances': 1
                    }
                },
                {'$sort': {'channel': 1, 'gross_fare': 1}}
            ]
            
            # Collect from both collections
            self.logger.info(f"Collecting market fares for sector {sector} from market_fares_ag...")
            ag_data = list(self.fares_db.market_fares_ag.aggregate(pipeline, allowDiskUse=True))
            
            self.logger.info(f"Collecting market fares for sector {sector} from market_fares_rg...")
            rg_data = list(self.fares_db.market_fares_rg.aggregate(pipeline, allowDiskUse=True))
            
            # Combine data
            combined_data = ag_data + rg_data
            
            if not combined_data:
                self.failure_reason = f"No market fares data available for sector {sector}"
                self.logger.warning(self.failure_reason)
                return None, self.failure_reason
            
            # Convert to DataFrame
            market_fares_data = pd.DataFrame(combined_data)
            
            # If same airline appears in both collections, keep one and sum instances
            market_fares_data = market_fares_data.groupby(
                ['source_airport', 'destination_airport', 'channel', 'outbound_airline_code', 'gross_fare'],
                as_index=False
            ).agg({'instances': 'sum'})
            
            # Final sort
            market_fares_data = market_fares_data.sort_values(['channel', 'gross_fare']).reset_index(drop=True)
            
            self.logger.info(f"Collected {len(market_fares_data)} unique market fare records for sector {sector}")
            
            return market_fares_data, self.failure_reason
            
        except Exception as e:
            self.logger.error(f"Error collecting market fares: {str(e)}")
            self.failure_reason = f"Market fares data collection failed: {str(e)}"
            return None, self.failure_reason
    
    def get_current_version(self):
        """Get current version information from MongoDB."""
        try:
            versions = list(self.db.current_table_version.find(
                {'table_name': {'$in': ['qp_fares', 'tax_mast_dom', 'tax_mast_intl']}},
                {'_id': 0}
            ))
            
            return pd.DataFrame(versions)
            
        except Exception as e:
            self.logger.error(f"Error getting version info: {str(e)}")
            self.failure_reason = f"Version info collection failed: {str(e)}"
            raise
    
    def save_ladder_to_csv(self, ladder_result, run_id, sector, analystName, route):
        """Save ladder to MongoDB with current QP fares, RBD mapping, and all-in fares."""
        try:
            if not ladder_result:
                return None
            
            origin = sector[:3]
            destination = sector[3:]
            
            # Get current QP fares for the sector
            fare_match = {
                'Carrier_Code': 'QP',
                'Rule_Nbr': '1001' if route == 'D' else 'R101',
                'OW_RT': 'O',
                '$or': [
                    {'Footnote': {'$exists': False}},
                    {'Footnote': None},
                    {'Footnote': ''}
                ],
                '$or': [
                    {
                        'Fare_Direction': 'F',
                        '$expr': {
                            '$eq': [
                                {'$concat': ['$Origin', '$Destination']},
                                sector
                            ]
                        }
                    },
                    {
                        'Fare_Direction': 'T',
                        '$expr': {
                            '$eq': [
                                {'$concat': ['$Destination', '$Origin']},
                                sector
                            ]
                        }
                    }
                ]
            }
            
            try:
                current_qp_pipeline = [
                    {'$match': fare_match},
                    {
                        '$project': {
                            '_id': 0,
                            'fare_amount': '$Fare_Amount',
                            'RBD': {'$substr': ['$Fare_Basis_Code', 0, 2]}
                        }
                    },
                    {'$sort': {'fare_amount': 1}},
                    {'$limit': 64}
                ]
                
                current_qp_fares_list = list(self.db.qp_fares.aggregate(current_qp_pipeline))
                
                if current_qp_fares_list:
                    current_qp_fares = [item['fare_amount'] for item in current_qp_fares_list]
                    current_rbds = [item['RBD'] for item in current_qp_fares_list]
                else:
                    current_qp_fares = [0] * 64
                    current_rbds = [f"RBD_{i+1}" for i in range(64)]
                    
            except Exception as e:
                self.logger.warning(f"Could not fetch current QP fares: {str(e)}")
                current_qp_fares = [0] * 64
                current_rbds = [f"RBD_{i+1}" for i in range(64)]
            
            # Ensure we have exactly 64 current fares and RBDs
            while len(current_qp_fares) < 64:
                current_qp_fares.append(0)
                current_rbds.append(f"RBD_{len(current_rbds)+1}")
            current_qp_fares = current_qp_fares[:64]
            current_rbds = current_rbds[:64]
            
            # Get RBD names from rm_parameter_values
            try:
                rbd_doc = self.db.rm_parameter_values.find_one(
                    {'parameterKey': 'RBD_SEQUENCE'},
                    {'parameterValue': 1, '_id': 0}
                )
                
                if rbd_doc and 'parameterValue' in rbd_doc:
                    parameter_rbds = rbd_doc['parameterValue'].split(',')
                    
                    # Check if we have enough real RBDs from current fares
                    real_rbds_count = len([rbd for rbd in current_rbds if not rbd.startswith('RBD_')])
                    
                    if real_rbds_count >= 10:
                        rbd_names = current_rbds
                    else:
                        rbd_names = parameter_rbds[:64] if len(parameter_rbds) >= 64 else current_rbds
                else:
                    rbd_names = current_rbds
                    
            except Exception as e:
                self.logger.warning(f"Could not fetch RBD names: {str(e)}")
                rbd_names = current_rbds
            
            # Ensure we have exactly 64 RBDs
            while len(rbd_names) < 64:
                rbd_names.append(f"RBD_{len(rbd_names)+1}")
            rbd_names = rbd_names[:64]
            
            # Calculate all-in fares for both current and new fares
            current_all_in_fares = []
            new_all_in_fares = []
            
            for i in range(64):
                current_base = current_qp_fares[i]
                new_base = ladder_result['ladder_fares'][i]
                
                # Calculate all-in fares with taxes
                current_all_in = self.calculate_all_in_fare_func(current_base, origin, destination)
                new_all_in = self.calculate_all_in_fare_func(new_base, origin, destination)
                
                current_all_in_fares.append(round(current_all_in, 2))
                new_all_in_fares.append(round(new_all_in, 2))
            
            # Create ladder documents for MongoDB
            ladder_documents = []
            created_at = datetime.now(pytz.timezone('Asia/Kolkata'))
            
            for i in range(64):
                doc = {
                    'sector': ladder_result['sector'],
                    'route': ladder_result['route'],
                    'fare_point_number': i + 1,
                    'rbd_name': rbd_names[i],
                    'current_qp_fare': current_qp_fares[i],
                    'current_all_in_fare': current_all_in_fares[i],
                    'new_ladder_fare': ladder_result['ladder_fares'][i],
                    'new_all_in_fare': new_all_in_fares[i],
                    'data_period': ladder_result['data_period'],
                    'distinct_fares_count': ladder_result['distinct_fares_count'],
                    'run_id': run_id,
                    'created_at': created_at,
                    'analyst': analystName
                }
                ladder_documents.append(doc)
            
            # Insert into MongoDB
            collection_name = 'ladder_gen_output'
            self.db[collection_name].insert_many(ladder_documents)
            
            self.logger.info(f"Saved {len(ladder_documents)} ladder records to MongoDB collection: {collection_name}")
            
            return collection_name
            
        except Exception as e:
            self.logger.error(f"Error saving ladder to database: {str(e)}")
            self.failure_reason = f"Failed to save ladder to database: {str(e)}"
            raise
    
    def save_run_audit(self, sector, route, analystName, start_time, currVersion, run_id, 
                      failure_reason, ladder_result, file_path, status='SUCCESS', is_initial=False):
        """Save or update audit information with enhanced failure tracking.
        
        Args:
            sector: Sector code
            route: Route type ('D' or 'I')
            analystName: Name of analyst
            start_time: Run start time
            currVersion: Current version DataFrame
            run_id: Unique identifier for this run
            failure_reason: Reason for failure (if applicable)
            ladder_result: Result dictionary from ladder generation (None for initial insert)
            file_path: Path where output was saved
            status: 'IN_PROGRESS', 'SUCCESS', or 'FAILED'
            is_initial: If True, performs INSERT for initial audit record
        """
        try:
            collection = self.db.ladder_generation_audits
            
            if is_initial:
                # Initial audit record - insert with IN_PROGRESS status
                tax_table = 'tax_mast_dom' if route == 'D' else 'tax_mast_intl'
                
                # Get version info safely
                qp_fares_version = None
                tax_master_version = None
                
                try:
                    if currVersion is not None and not currVersion.empty:
                        qp_version_rows = currVersion[currVersion['table_name'] == 'qp_fares']
                        tax_version_rows = currVersion[currVersion['table_name'] == tax_table]
                        
                        qp_fares_version = qp_version_rows['curr_version'].values[0] if not qp_version_rows.empty else None
                        tax_master_version = tax_version_rows['curr_version'].values[0] if not tax_version_rows.empty else None
                except Exception:
                    pass
                
                initial_audit = {
                    'run_id': run_id,
                    'sector': sector,
                    'start_time': start_time,
                    'end_time': None,
                    'status': 'IN_PROGRESS',
                    'total_ladder_points': None,
                    'core_ladder_points': None,
                    'buffer_ladder_points': None,
                    'min_ladder_fare': None,
                    'max_ladder_fare': None,
                    'data_period_used': None,
                    'total_booking_records': None,
                    'distinct_fares_count': None,
                    'route': route,
                    'qp_fares': qp_fares_version,
                    'tax_master': tax_master_version,
                    'analyst': analystName,
                    'failure_reason': None
                }
                
                collection.insert_one(initial_audit)
                
                self.logger.info(f"Initial audit record created with status IN_PROGRESS for run_id: {run_id}")
                return 'ladder_generation_audits'
                
            else:
                # Update existing audit record with final results
                if status == 'SUCCESS' and not ladder_result:
                    status = 'FAILED'
                
                total_points = len(ladder_result['ladder_fares']) if ladder_result else 0
                
                update_doc = {
                    '$set': {
                        'end_time': datetime.now(pytz.timezone('Asia/Kolkata')),
                        'status': status,
                        'total_ladder_points': total_points,
                        'core_ladder_points': 58 if total_points > 0 else 0,
                        'buffer_ladder_points': 6 if total_points > 0 else 0,
                        'min_ladder_fare': round(min(ladder_result['ladder_fares']), 2) if ladder_result and ladder_result.get('ladder_fares') else None,
                        'max_ladder_fare': round(max(ladder_result['ladder_fares']), 2) if ladder_result and ladder_result.get('ladder_fares') else None,
                        'data_period_used': ladder_result['data_period'] if ladder_result else None,
                        'total_booking_records': ladder_result['total_booking_records'] if ladder_result else 0,
                        'distinct_fares_count': ladder_result['distinct_fares_count'] if ladder_result else 0,
                        'failure_reason': failure_reason if status == 'FAILED' else None
                    }
                }
                
                collection.update_one(
                    {'run_id': run_id},
                    update_doc
                )
                
                self.logger.info(f"Audit record updated with status {status} for run_id: {run_id}")
                return 'ladder_generation_audits'
            
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
                                'failure_reason': f"Audit update failed: {str(e)}. Original failure: {failure_reason}"
                            }
                        }
                    )
                    
                    return 'ladder_generation_audits'
                except:
                    self.logger.error("Failed to save even minimal audit update")
                    return None
            else:
                return None