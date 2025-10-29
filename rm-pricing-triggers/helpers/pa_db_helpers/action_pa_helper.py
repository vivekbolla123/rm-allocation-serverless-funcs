import pandas as pd
from datetime import datetime, timedelta
import pytz
from bson import ObjectId

class ActionPAHelper:
    """Handles Pricing/Analytics-related database operations using MongoDB."""

    def __init__(self, mongo_db, fares_db, logger):
        """
        Initialize with MongoDB database instance.
        
        Args:
            mongo_db: MongoDB database instance (e.g., client['QP_DW_PRIAUT'])
            logger: Logger instance
        """
        self.db = mongo_db
        self.fares_db = fares_db
        self.logger = logger
        
    def collect_qp_fares(self, route, active_sectors):
        """Collect QP fares data from MongoDB based on route (domestic or international).
        Filters data to only include active sectors for domestic routes only."""
        try:
            # Build base match conditions
            base_match = {
                'Carrier_Code': 'QP',
                'Fare_Direction': {'$in': ['F', 'T']},
                'OW_RT': {'$in': ['O', 'C']},
                '$or': [
                    {'Footnote': {'$exists': False}},
                    {'Footnote': None},
                    {'Footnote': ''}
                ]
            }
            
            if route == 'D':
                base_match['Rule_Nbr'] = {'$in': ['1001', '2001']}
                
                # Get domestic stations
                domestic_stations = list(self.db.config_station_master.find(
                    {'route': 'D'},
                    {'station': 1, '_id': 0}
                ))
                station_list = [s['station'] for s in domestic_stations]
                
                base_match['$or'] = [
                    {'Origin': {'$in': station_list}},
                    {'Destination': {'$in': station_list}}
                ]
                
            elif route == 'I':
                base_match['Rule_Nbr'] = {'$in': ['M101', 'R101', 'M201', 'R201']}
                
                # Get international stations
                intl_stations = list(self.db.config_station_master.find(
                    {'route': 'I'},
                    {'station': 1, '_id': 0}
                ))
                station_list = [s['station'] for s in intl_stations]
                
                base_match['$or'] = [
                    {'Origin': {'$in': station_list}},
                    {'Destination': {'$in': station_list}}
                ]
            else:
                raise ValueError(f"Invalid Route Type: {route}")
            
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
                        'channel': {
                            '$switch': {
                                'branches': [
                                    {
                                        'case': {'$in': ['$Rule_Nbr', ['1001', 'R101', 'R201']]},
                                        'then': 'B2C'
                                    },
                                    {
                                        'case': {'$in': ['$Rule_Nbr', ['2001', 'M101', 'M201']]},
                                        'then': 'B2B'
                                    }
                                ],
                                'default': 'Unknown'
                            }
                        }
                    }
                }
            ]
            
            # Execute aggregation
            all_qp_fares = list(self.db.qp_fares.aggregate(pipeline))
            all_qp_fares_df = pd.DataFrame(all_qp_fares)
            
            # Filter by active sectors ONLY for domestic routes
            if route == 'D' and active_sectors is not None and not active_sectors.empty:
                active_sectors_set = set()
                for _, row in active_sectors.iterrows():
                    sector_forward = f"{row['DepartureStation']}{row['ArrivalStation']}"
                    active_sectors_set.add(sector_forward)
                
                qp_fares_data = all_qp_fares_df[all_qp_fares_df['SECTOR'].isin(active_sectors_set)].copy()
                self.logger.info(f"Filtered domestic QP fares to {len(qp_fares_data)} records based on active sectors")
            else:
                if route == 'I':
                    self.logger.info("International route detected. Using all QP fares data without active sector filtering.")
                else:
                    self.logger.info("Active sectors data not available for domestic route. Using all QP fares data.")
                qp_fares_data = all_qp_fares_df
            
            return qp_fares_data
            
        except Exception as e:
            self.logger.error(f"Error collecting QP fares data: {str(e)}")
            raise

    def collect_tax_master_domestic(self):
        """Collect domestic tax master data from MongoDB."""
        try:
            tax_master_data = list(self.db.tax_mast_dom.find(
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
            
            tax_master_dom_data = pd.DataFrame(tax_master_data)
            return tax_master_dom_data
            
        except Exception as e:
            self.logger.error(f"Error collecting domestic tax master data: {str(e)}")
            raise

    def collect_tax_master_international(self):
        """Collect international tax master data from MongoDB."""
        try:
            tax_master_data = list(self.db.tax_mast_intl.find(
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
            
            tax_master_intl_data = pd.DataFrame(tax_master_data)
            return tax_master_intl_data
            
        except Exception as e:
            self.logger.error(f"Error collecting international tax master data: {str(e)}")
            raise

    def collect_market_fares(self, route):
        """Collect market fares min/max data based on route (domestic/international) with carrier info.
        Optimized for large datasets using MongoDB aggregation."""
        try:
            # Build route filter
            if route == 'D':
                route_filter = {'outbound_stop': "0"}
            elif route == 'I':
                route_filter = {'outbound_stop': {'$gte': "0"}}
            else:
                raise ValueError(f"Invalid route: {route}")
            
            # Date filters - Keep as datetime objects
            now = datetime.now()
            date_filter = {
                'departure_date': {
                    '$gte': now.replace(hour=0, minute=0, second=0, microsecond=0),
                    '$lte': (now + timedelta(days=365)).replace(hour=23, minute=59, second=59, microsecond=999999)
                },
                'created_at': {'$gt': now - timedelta(hours=24)}
            }
            
            # Common filters
            common_filter = {
                'outbound_airline_code': {'$nin': ['9I', 'IC', 'S9', 'S5', 'QP', 'G8']},
                **date_filter,
                **route_filter
            }
            
            # Optimized aggregation pipeline that does all calculations in MongoDB
            pipeline = [
                {'$match': common_filter},
                {
                    '$addFields': {
                        'gross_fare': {'$toDouble': '$gross_fare'}
                    }
                },
                {
                    '$group': {
                        '_id': {
                            'source': '$source_airport',
                            'dest': '$destination_airport',
                            'channel': '$channel',
                            'fare': '$gross_fare',
                            'carrier': '$outbound_airline_code'
                        },
                        'count': {'$sum': 1}
                    }
                },
                {
                    '$group': {
                        '_id': {
                            'source': '$_id.source',
                            'dest': '$_id.dest',
                            'channel': '$_id.channel'
                        },
                        'fares': {
                            '$push': {
                                'fare': '$_id.fare',
                                'carrier': '$_id.carrier',
                                'count': '$count'
                            }
                        },
                        'max_fare': {'$max': '$_id.fare'},
                        'min_fare': {'$min': '$_id.fare'}
                    }
                },
                {
                    '$project': {
                        '_id': 0,
                        'source_airport': '$_id.source',
                        'destination_airport': '$_id.dest',
                        'channel': '$_id.channel',
                        'min_fare': 1,
                        'max_fare': 1,
                        'min_details': {
                            '$filter': {
                                'input': '$fares',
                                'as': 'fare',
                                'cond': {'$eq': ['$$fare.fare', '$min_fare']}
                            }
                        },
                        'max_details': {
                            '$filter': {
                                'input': '$fares',
                                'as': 'fare',
                                'cond': {'$eq': ['$$fare.fare', '$max_fare']}
                            }
                        }
                    }
                },
                {
                    '$project': {
                        'source_airport': 1,
                        'destination_airport': 1,
                        'channel': 1,
                        'min_fare': 1,
                        'max_fare': 1,
                        'min_instances': {'$sum': '$min_details.count'},
                        'max_instances': {'$sum': '$max_details.count'},
                        # FIX: Access the 'carrier' field of the first element
                        'min_carrier': {
                            '$arrayElemAt': ['$min_details.carrier', 0]
                        },
                        'max_carrier': {
                            '$arrayElemAt': ['$max_details.carrier', 0]
                        }
                    }
                },
                {'$sort': {'source_airport': 1, 'destination_airport': 1, 'channel': 1}}
            ]
            
            # Execute pipeline on both collections
            self.logger.info("Aggregating market_fares_ag data...")
            ag_results = list(self.fares_db.market_fares_ag.aggregate(pipeline, allowDiskUse=True))
            
            self.logger.info("Aggregating market_fares_rg data...")
            rg_results = list(self.fares_db.market_fares_rg.aggregate(pipeline, allowDiskUse=True))
            
            # Combine results from both collections
            if not ag_results and not rg_results:
                return pd.DataFrame()
            
            # Convert to DataFrames
            ag_df = pd.DataFrame(ag_results) if ag_results else pd.DataFrame()
            rg_df = pd.DataFrame(rg_results) if rg_results else pd.DataFrame()
            
            # Combine and merge duplicates if same sector appears in both collections
            if not ag_df.empty and not rg_df.empty:
                combined = pd.concat([ag_df, rg_df], ignore_index=True)
                
                # Group by sector/channel to merge any duplicates
                result = combined.groupby(['source_airport', 'destination_airport', 'channel']).agg({
                    'min_fare': 'min',
                    'max_fare': 'max',
                    'min_instances': 'sum',
                    'max_instances': 'sum',
                    'min_carrier': 'first',
                    'max_carrier': 'first'
                }).reset_index()
            elif not ag_df.empty:
                result = ag_df
            else:
                result = rg_df
            
            self.logger.info(f"Collected market fares for {len(result)} sector-channel combinations")
            
            return result
        
        except Exception as e:
            self.logger.error(f"Error collecting market fares data: {str(e)}")
            raise

    
    def save_actionable_sectors_to_database(self, route, analystName, actionable_df, run_id):
        """Save actionable sectors to MongoDB collection."""
        try:
            if actionable_df.empty:
                return None
            
            collection_name = 'actionable_sectors_output'
            
            # Add metadata columns
            actionable_df = actionable_df.copy()
            actionable_df['run_id'] = run_id
            actionable_df['created_at'] = datetime.now(pytz.timezone('Asia/Kolkata'))
            actionable_df['route'] = route
            actionable_df['analyst'] = analystName
            
            # Convert to dict for MongoDB insertion
            records = actionable_df.to_dict('records')
            
            # Insert documents
            self.db[collection_name].insert_many(records)
            
            self.logger.info(f"Saved {len(records)} actionable sectors to MongoDB collection: {collection_name}")
            
            return collection_name
            
        except Exception as e:
            self.logger.error(f"Error saving actionable sectors to database: {str(e)}")
            raise
    
    def get_current_version(self):
        """Get current table versions from MongoDB."""
        try:
            versions = list(self.db.current_table_version.find(
                {'table_name': {'$in': ['qp_fares', 'tax_mast_dom', 'tax_mast_intl']}},
                {'_id': 0}
            ))
            
            currVersion = pd.DataFrame(versions)
            return currVersion
            
        except Exception as e:
            self.logger.error(f"Unable to get current version: {str(e)}")
            raise
        
    def save_run_audit(self, currVersion, route, analystName, start_time, run_id, actionable_df, status='SUCCESS', is_initial=False):
        """Save or update audit information with enhanced failure tracking.
        
        Args:
            currVersion: Current version DataFrame
            route: Route type ('D' or 'I')
            analystName: Name of analyst
            start_time: Run start time
            run_id: Unique identifier for this run
            actionable_df: Result DataFrame from actionable sectors generation (None for initial insert)
            status: 'IN_PROGRESS', 'SUCCESS', or 'FAILED'
            is_initial: If True, performs INSERT for initial audit record
        """
        try:
            collection = self.db.pricing_run_audits
            
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
                    'start_time': start_time,
                    'end_time': None,
                    'status': 'IN_PROGRESS',
                    'total_actionable_records': None,
                    'max_actions': None,
                    'min_actions': None,
                    'constraints_satisfied': None,
                    'route': route,
                    'qp_fares': qp_fares_version,
                    'tax_master': tax_master_version,
                    'analyst': analystName,
                    'failure_reason': None
                }
                
                collection.insert_one(initial_audit)
                
                self.logger.info(f"Initial audit record created with status IN_PROGRESS for run_id: {run_id}")
                return 'pricing_run_audits'
                
            else:
                # Update existing audit record with final results
                if status == 'SUCCESS' and actionable_df is None:
                    status = 'FAILED'
                    
                total_records = len(actionable_df) if actionable_df is not None else 0
                max_actions = len(actionable_df[actionable_df['Action_required'] == 'Max_Action']) if actionable_df is not None else 0
                min_actions = len(actionable_df[actionable_df['Action_required'] == 'Min_Action']) if actionable_df is not None else 0
                constraints_satisfied = len(actionable_df[actionable_df['Constraint'] == 'Satisfied']) if actionable_df is not None else 0
                
                update_doc = {
                    '$set': {
                        'end_time': datetime.now(pytz.timezone('Asia/Kolkata')),
                        'total_actionable_records': total_records,
                        'max_actions': max_actions,
                        'min_actions': min_actions,
                        'constraints_satisfied': constraints_satisfied,
                        'status': status
                    }
                }
                
                collection.update_one(
                    {'run_id': run_id},
                    update_doc
                )
                
                self.logger.info(f"Audit record updated with status {status} for run_id: {run_id}")
                return 'pricing_run_audits'
            
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
                    
                    return 'pricing_run_audits'
                except:
                    self.logger.error("Failed to save even minimal audit update")
                    return None
            else:
                return None