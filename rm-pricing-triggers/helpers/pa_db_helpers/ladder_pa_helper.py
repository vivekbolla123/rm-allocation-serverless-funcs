import pandas as pd
from datetime import datetime, timedelta
import pytz
from bson import ObjectId

class LadderPAHelper:
    """Handles Ladder Pricing/Analytics-related database operations using MongoDB."""
    
    def __init__(self, ladder_gen, mongo_db, fares_db, logger, calculate_all_in_fare_func=None):
        """
        Initialize with MongoDB database instance.
        
        Args:
            ladder_gen: LadderGen instance (for accessing shared methods/constants if needed)
            mongo_db: MongoDB database instance (e.g., client['QP_DW_PRIAUT'])
            logger: Logger instance
            calculate_all_in_fare_func: Function to calculate all-in fares
        """
        self.db = mongo_db
        self.fares_db = fares_db
        self.logger = logger
        self.failure_reason = None
        self.calculate_all_in_fare_func = calculate_all_in_fare_func
        self.B2B_RBD_LIST = ladder_gen.B2B_RBD_LIST
        self.B2C_RBD_LIST = ladder_gen.B2C_RBD_LIST
        self.TOTAL_B2C_FARES = ladder_gen.TOTAL_B2C_FARES
        self.TOTAL_B2B_FARES = ladder_gen.TOTAL_B2B_FARES
        self.TOTAL_FARES = ladder_gen.TOTAL_FARES
        self.BUFFER_FARES_B2C = ladder_gen.BUFFER_FARES_B2C
        self.BUFFER_FARES_B2B = ladder_gen.BUFFER_FARES_B2B
        self.CORE_FARES_B2C = ladder_gen.CORE_FARES_B2C
        self.CORE_FARES_B2B = ladder_gen.CORE_FARES_B2B
        
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
    
    def collect_market_fares(self, route, sector, max_market_b2b, max_market_b2c):
        """Collect market fares for both B2B and B2C channels. Optimized for large datasets."""
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

            route_filter = {'outbound_stop': "0"}

            if route == 'D':
                match_stage = {
                    'outbound_airline_code': {'$nin': ['9I', 'IC', 'S9', 'S5', 'QP', 'G8']},
                    'channel': {'$in': ['B2C', 'B2B']},
                    '$expr': {
                        '$eq': [
                            {'$concat': ['$source_airport', '$destination_airport']},
                            sector
                        ]
                    },
                    **date_filter,
                    **route_filter
                }
            elif route == 'I':
                match_stage = {
                    'outbound_airline_code': {'$in': ['6E','AI','IX','XY','FZ','GF','J9','KU','FD','SG','TG','WY','VZ','SV','F3']},
                    'channel': {'$in': ['B2C', 'B2B']},
                    '$expr': {
                        '$eq': [
                            {'$concat': ['$source_airport', '$destination_airport']},
                            sector
                        ]
                    },
                    **date_filter,
                    **route_filter
                }
            else:
                raise ValueError(f"Invalid route: {route}")

            pipeline = [
                {'$match': match_stage},
                {
                    '$addFields': {
                        'gross_fare_numeric': {'$toDouble': '$gross_fare'}
                    }
                },
                {
                    '$match': {
                        '$or': [
                            {'channel': 'B2C', 'gross_fare_numeric': {'$lte': max_market_b2c}},
                            {'channel': 'B2B', 'gross_fare_numeric': {'$lte': max_market_b2b}}
                        ]
                    }
                },
                # Group by fare + airline + channel to count instances — airline is included
                # so all fare levels are preserved, not just the cheapest per airline
                {
                    '$group': {
                        '_id': {
                            'source': '$source_airport',
                            'dest': '$destination_airport',
                            'channel': '$channel',
                            'fare': '$gross_fare_numeric',
                            'airline': '$outbound_airline_code'
                        },
                        'instances': {'$sum': 1}
                    }
                },
                # Keep only fares seen at least twice (dedup noise)
                {
                    '$match': {
                        'instances': {'$gte': 2}
                    }
                },
                {
                    '$project': {
                        '_id': 0,
                        'source_airport': '$_id.source',
                        'destination_airport': '$_id.dest',
                        'channel': '$_id.channel',
                        'gross_fare': '$_id.fare',
                        'outbound_airline_code': '$_id.airline',
                        'instances': 1
                    }
                },
                {'$sort': {'channel': 1, 'gross_fare': 1}}
            ]

            ag_data = list(self.fares_db.market_fares_ag.aggregate(pipeline, allowDiskUse=True))
            rg_data = list(self.fares_db.market_fares_rg.aggregate(pipeline, allowDiskUse=True))

            combined_data = ag_data + rg_data

            if not combined_data:
                self.failure_reason = f"No market fares data available for sector {sector}"
                self.logger.warning(self.failure_reason)
                return None, self.failure_reason

            market_fares_data = pd.DataFrame(combined_data)

            # If same airline+fare appears in both collections, sum instances
            market_fares_data = market_fares_data.groupby(
                ['source_airport', 'destination_airport', 'channel', 'outbound_airline_code', 'gross_fare'],
                as_index=False
            ).agg({'instances': 'sum'})

            market_fares_data = market_fares_data.sort_values(['channel', 'gross_fare']).reset_index(drop=True)

            self.logger.info(f"Market fares collected - B2C: {len(market_fares_data[market_fares_data['channel']=='B2C'])}, "
                            f"B2B: {len(market_fares_data[market_fares_data['channel']=='B2B'])}")

            return market_fares_data, None

        except Exception as e:
            self.logger.error(f"Error collecting market fares: {str(e)}")
            self.failure_reason = f"Market fares data collection failed: {str(e)}"
            return None, self.failure_reason


    def collect_market_fares_bidirectional(self, route, sector, reverse_sector, max_market_b2b, max_market_b2c):
        """Collect market fares for both directions and both B2B and B2C channels."""
        try:
            now = datetime.now()
            date_filter = {
                'departure_date': {
                    '$gte': now.replace(hour=0, minute=0, second=0, microsecond=0),
                    '$lte': (now + timedelta(days=365)).replace(hour=23, minute=59, second=59, microsecond=999999)
                },
                'created_at': {'$gt': now - timedelta(hours=72)}
            }

            route_filter = {'outbound_stop': "0"}

            sector_expr = {
                '$or': [
                    {
                        '$expr': {
                            '$eq': [
                                {'$concat': ['$source_airport', '$destination_airport']},
                                sector
                            ]
                        }
                    },
                    {
                        '$expr': {
                            '$eq': [
                                {'$concat': ['$source_airport', '$destination_airport']},
                                reverse_sector
                            ]
                        }
                    }
                ]
            }

            if route == 'D':
                match_stage = {
                    'outbound_airline_code': {'$nin': ['9I', 'IC', 'S9', 'S5', 'QP', 'G8']},
                    'channel': {'$in': ['B2C', 'B2B']},
                    **sector_expr,
                    **date_filter,
                    **route_filter
                }
            elif route == 'I':
                match_stage = {
                    'outbound_airline_code': {'$in': ['6E','AI','IX','XY','FZ','GF','J9','KU','FD','SG','TG','WY','VZ','SV','F3']},
                    'channel': {'$in': ['B2C', 'B2B']},
                    **sector_expr,
                    **date_filter,
                    **route_filter
                }
            else:
                raise ValueError(f"Invalid route: {route}")

            pipeline = [
                {'$match': match_stage},
                {
                    '$addFields': {
                        'gross_fare_numeric': {'$toDouble': '$gross_fare'}
                    }
                },
                {
                    '$match': {
                        '$or': [
                            {'channel': 'B2C', 'gross_fare_numeric': {'$lte': max_market_b2c}},
                            {'channel': 'B2B', 'gross_fare_numeric': {'$lte': max_market_b2b}}
                        ]
                    }
                },
                # Group by fare + airline + channel to count instances — airline is included
                # so all fare levels are preserved, not just the cheapest per airline
                {
                    '$group': {
                        '_id': {
                            'source': '$source_airport',
                            'dest': '$destination_airport',
                            'channel': '$channel',
                            'fare': '$gross_fare_numeric',
                            'airline': '$outbound_airline_code'
                        },
                        'instances': {'$sum': 1}
                    }
                },
                # Keep only fares seen at least twice (dedup noise)
                {
                    '$match': {
                        'instances': {'$gte': 2}
                    }
                },
                {
                    '$project': {
                        '_id': 0,
                        'source_airport': '$_id.source',
                        'destination_airport': '$_id.dest',
                        'channel': '$_id.channel',
                        'gross_fare': '$_id.fare',
                        'outbound_airline_code': '$_id.airline',
                        'instances': 1
                    }
                },
                {'$sort': {'channel': 1, 'gross_fare': 1}}
            ]

            ag_data = list(self.fares_db.market_fares_ag.aggregate(pipeline, allowDiskUse=True))
            rg_data = list(self.fares_db.market_fares_rg.aggregate(pipeline, allowDiskUse=True))

            combined_data = ag_data + rg_data

            if not combined_data:
                self.failure_reason = f"No market fares data available for sectors {sector} and {reverse_sector}"
                self.logger.warning(self.failure_reason)
                return None, self.failure_reason

            market_fares_data = pd.DataFrame(combined_data)

            # If same airline+fare appears in both collections, sum instances
            market_fares_data = market_fares_data.groupby(
                ['source_airport', 'destination_airport', 'channel', 'outbound_airline_code', 'gross_fare'],
                as_index=False
            ).agg({'instances': 'sum'})

            market_fares_data = market_fares_data.sort_values(['channel', 'gross_fare']).reset_index(drop=True)

            self.logger.info(f"Bidirectional market fares collected for {sector} + {reverse_sector} - "
                            f"B2C: {len(market_fares_data[market_fares_data['channel']=='B2C'])}, "
                            f"B2B: {len(market_fares_data[market_fares_data['channel']=='B2B'])}")

            return market_fares_data, None

        except Exception as e:
            self.logger.error(f"Error collecting bidirectional market fares: {str(e)}")
            self.failure_reason = f"Bidirectional market fares data collection failed: {str(e)}"
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
    
    def get_sector_currency(self, sector, route):
        """Get the currency for a specific sector from qp_fares."""
        try:
            # Determine rule number based on route
            rule_nbr = '1001' if route == 'D' else 'R101'
            
            # Query to get currency for the sector
            query = {
                'Carrier_Code': 'QP',
                'Rule_Nbr': rule_nbr,
                'OW_RT': 'O',
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
            
            # Get one document to check currency
            result = self.db.qp_fares.find_one(query, {'Currency': 1, '_id': 0})
            
            if result and 'Currency' in result:
                currency = result['Currency']
                self.logger.info(f"Sector {sector} uses currency: {currency}")
                return currency
            else:
                self.logger.warning(f"No currency found for sector {sector}, defaulting to INR")
                return 'INR'
                
        except Exception as e:
            self.logger.error(f"Error getting sector currency: {str(e)}")
            return 'INR'  # Default to INR on error

    def get_sector_native_currency(self, sector, route, fare_direction='F'):
        """Get the native currency for a sector using a specific Fare_Direction.
        
        In qp_fares, Origin/Destination stay the same for both directions.
        Only Fare_Direction changes: 'F' for forward, 'T' for reverse.
        For BOMHKT with direction='T', this returns the reverse (HKTBOM) currency (e.g., THB).
        
        Args:
            sector: The forward sector string e.g. 'BOMHKT'.
            route: 'D' or 'I'.
            fare_direction: 'F' for forward currency, 'T' for reverse currency.
        """
        try:
            rule_nbr = '1001' if route == 'D' else 'R101'
            origin = sector[:3]
            destination = sector[3:]
            
            query = {
                'Carrier_Code': 'QP',
                'Rule_Nbr': rule_nbr,
                'OW_RT': 'O',
                'Fare_Direction': fare_direction,
                'Origin': origin,
                'Destination': destination
            }
            
            result = self.db.qp_fares.find_one(query, {'Currency': 1, '_id': 0})
            
            if result and 'Currency' in result:
                currency = result['Currency']
                self.logger.info(f"Sector {sector} (direction={fare_direction}) currency: {currency}")
                return currency
            else:
                self.logger.warning(f"No currency found for sector {sector} (direction={fare_direction}), defaulting to INR")
                return 'INR'
                
        except Exception as e:
            self.logger.error(f"Error getting sector native currency: {str(e)}")
            return 'INR'
    
    def save_ladder_to_csv(self, ladder_result, run_id, sector, analystName, route):
        """Save both B2B and B2C ladders to MongoDB with current QP fares, RBD mapping, and all-in fares."""
        try:
            if not ladder_result:
                return None
            
            origin = sector[:3]
            destination = sector[3:]
            created_at = datetime.now(pytz.timezone('Asia/Kolkata'))
            
            # Get RBD sequence from rm_parameter_values
            rbd_names_b2b = self.B2B_RBD_LIST  
            rbd_names_b2c = self.B2C_RBD_LIST      
            
            # Get current QP fares for B2C (using V9-Y0 RBDs)
            fare_match_b2c = {
                'Carrier_Code': 'QP',
                'Rule_Nbr': '1001' if route == 'D' else 'R101',
                'OW_RT': 'O',
                'Fare_Basis_Code': {
                    '$regex': '^(?!Z)[A-Y]',  # Exclude Z* (B2B), include A-Y
                    '$not': {'$regex': '^W'}  # Exclude W*
                },
                '$and': [
                    {
                        '$or': [
                            {'Footnote': {'$exists': False}},
                            {'Footnote': None},
                            {'Footnote': ''}
                        ]
                    },
                    {
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
                ]
            }
            
            # Get current QP fares for B2B (using Z* RBDs)
            fare_match_b2b = {
                'Carrier_Code': 'QP',
                'Rule_Nbr': '2001' if route == 'D' else 'M101',
                'OW_RT': 'O',
                'Fare_Basis_Code': {
                    '$regex': '^Z',  # Only Z* RBDs (B2B)
                    '$not': {'$regex': '^W'}  # Exclude W*
                },
                '$and': [
                    {
                        '$or': [
                            {'Footnote': {'$exists': False}},
                            {'Footnote': None},
                            {'Footnote': ''}
                        ]
                    },
                    {
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
                ]
            }
            
            # Get B2C fares
            try:
                current_qp_pipeline_b2c = [
                    {'$match': fare_match_b2c},
                    {
                        '$project': {
                            '_id': 0,
                            'fare_amount': '$Fare_Amount',
                            'RBD': {'$substr': ['$Fare_Basis_Code', 0, 2]}
                        }
                    },
                    {'$sort': {'fare_amount': 1}},
                    {'$limit': self.TOTAL_B2C_FARES}
                ]
                
                current_qp_fares_list_b2c = list(self.db.qp_fares.aggregate(current_qp_pipeline_b2c))
                
                if current_qp_fares_list_b2c:
                    current_qp_fares_b2c = [item['fare_amount'] for item in current_qp_fares_list_b2c]
                else:
                    current_qp_fares_b2c = [0] * self.TOTAL_B2C_FARES
                    
            except Exception as e:
                self.logger.warning(f"Could not fetch current QP B2C fares: {str(e)}")
                current_qp_fares_b2c = [0] * self.TOTAL_B2C_FARES
            
            # Ensure we have exactly self.TOTAL_B2C_FARES current B2C fares
            while len(current_qp_fares_b2c) < self.TOTAL_B2C_FARES:
                current_qp_fares_b2c.append(0)
            current_qp_fares_b2c = current_qp_fares_b2c[:self.TOTAL_B2C_FARES]
            
            # Get B2B fares
            try:
                current_qp_pipeline_b2b = [
                    {'$match': fare_match_b2b},
                    {
                        '$project': {
                            '_id': 0,
                            'fare_amount': '$Fare_Amount',
                            'RBD': {'$substr': ['$Fare_Basis_Code', 0, 2]}
                        }
                    },
                    {'$sort': {'fare_amount': 1}},
                    {'$limit': self.TOTAL_B2B_FARES}
                ]
                
                current_qp_fares_list_b2b = list(self.db.qp_fares.aggregate(current_qp_pipeline_b2b))
                
                if current_qp_fares_list_b2b:
                    current_qp_fares_b2b = [item['fare_amount'] for item in current_qp_fares_list_b2b]
                else:
                    current_qp_fares_b2b = [0] * self.TOTAL_B2B_FARES
                    
            except Exception as e:
                self.logger.warning(f"Could not fetch current QP B2B fares: {str(e)}")
                current_qp_fares_b2b = [0] * self.TOTAL_B2B_FARES
            
            # Ensure we have exactly self.TOTAL_B2B_FARES current B2B fares
            while len(current_qp_fares_b2b) < self.TOTAL_B2B_FARES:
                current_qp_fares_b2b.append(0)
            current_qp_fares_b2b = current_qp_fares_b2b[:self.TOTAL_B2B_FARES]
            
            collection_name = 'ladder_gen_output'
            ladder_documents = []
            
            # Save B2B ladder first
            ladder_fares_b2b = ladder_result.get('ladder_fares_b2b', [])
            self.logger.info(f"B2B ladder fares count: {len(ladder_fares_b2b)} (expected: {self.TOTAL_B2B_FARES})")
            
            if ladder_fares_b2b:
                # Handle cases where B2B ladder might not be exactly self.TOTAL_B2B_FARES fares
                expected_b2b_count = self.TOTAL_B2B_FARES
                actual_b2b_count = len(ladder_fares_b2b)
                
                if actual_b2b_count != expected_b2b_count:
                    self.logger.warning(f"B2B ladder count mismatch: got {actual_b2b_count}, expected {expected_b2b_count}")
                    # Pad or truncate to match expected count
                    if actual_b2b_count < expected_b2b_count:
                        # Pad with the last fare value
                        last_fare = ladder_fares_b2b[-1] if ladder_fares_b2b else 0
                        ladder_fares_b2b.extend([last_fare] * (expected_b2b_count - actual_b2b_count))
                        self.logger.info(f"Padded B2B ladder from {actual_b2b_count} to {len(ladder_fares_b2b)} fares")
                    else:
                        # Truncate to expected count
                        ladder_fares_b2b = ladder_fares_b2b[:expected_b2b_count]
                        self.logger.info(f"Truncated B2B ladder from {actual_b2b_count} to {len(ladder_fares_b2b)} fares")
                
                for i in range(min(len(ladder_fares_b2b), self.TOTAL_B2B_FARES)):
                    current_base = current_qp_fares_b2b[i]
                    new_base = ladder_fares_b2b[i]
                    
                    # Calculate all-in fares with taxes
                    current_all_in = self.calculate_all_in_fare_func(current_base, origin, destination, ftype="current")
                    new_all_in = self.calculate_all_in_fare_func(new_base, origin, destination, ftype="new")
                    
                    doc = {
                        'sector': ladder_result['sector'],
                        'route': ladder_result['route'],
                        'channel': 'B2B',
                        'fare_point_number': i + 1,
                        'rbd_name': rbd_names_b2b[i],
                        'current_qp_base_fare': round(current_base, 2) if current_base else 0,
                        'current_qp_all_in_fare': round(current_all_in, 2),
                        'new_base_ladder_fare': round(new_all_in , 2),
                        'new_all_in_ladder_fare': round( new_base, 2),
                        'data_period': ladder_result['data_period'],
                        'distinct_fares_count': ladder_result.get('distinct_fares_count_b2b', 0),
                        'total_booking_records': ladder_result.get('total_booking_records_b2b', 0),
                        'run_id': run_id,
                        'created_at': created_at,
                        'analyst': analystName
                    }
                    ladder_documents.append(doc)
            
            # Save B2C ladder
            ladder_fares_b2c = ladder_result.get('ladder_fares_b2c', [])
            self.logger.info(f"B2C ladder fares count: {len(ladder_fares_b2c)} (expected: {self.TOTAL_B2C_FARES})")
            
            if ladder_fares_b2c:
                # Handle cases where B2C ladder might not be exactly self.TOTAL_B2C_FARES fares
                expected_b2c_count = self.TOTAL_B2C_FARES
                actual_b2c_count = len(ladder_fares_b2c)
                
                if actual_b2c_count != expected_b2c_count:
                    self.logger.warning(f"B2C ladder count mismatch: got {actual_b2c_count}, expected {expected_b2c_count}")
                    # Pad or truncate to match expected count
                    if actual_b2c_count < expected_b2c_count:
                        # Pad with the last fare value
                        last_fare = ladder_fares_b2c[-1] if ladder_fares_b2c else 0
                        ladder_fares_b2c.extend([last_fare] * (expected_b2c_count - actual_b2c_count))
                        self.logger.info(f"Padded B2C ladder from {actual_b2c_count} to {len(ladder_fares_b2c)} fares")
                    else:
                        # Truncate to expected count
                        ladder_fares_b2c = ladder_fares_b2c[:expected_b2c_count]
                        self.logger.info(f"Truncated B2C ladder from {actual_b2c_count} to {len(ladder_fares_b2c)} fares")
                
                for i in range(min(len(ladder_fares_b2c), self.TOTAL_B2C_FARES)):
                    current_base = current_qp_fares_b2c[i]
                    new_base = ladder_fares_b2c[i]
                    
                    # Calculate all-in fares with taxes
                    current_all_in = self.calculate_all_in_fare_func(current_base, origin, destination, ftype="current")
                    new_all_in = self.calculate_all_in_fare_func(new_base, origin, destination, ftype="new")
                    
                    doc = {
                        'sector': ladder_result['sector'],
                        'route': ladder_result['route'],
                        'channel': 'B2C',
                        'fare_point_number': i + self.TOTAL_B2B_FARES + 1,  # (start after B2B fares)
                        'rbd_name': rbd_names_b2c[i],
                        'current_qp_base_fare': round(current_base, 2) if current_base else 0,
                        'current_qp_all_in_fare': round(current_all_in, 2),
                        'new_base_ladder_fare': round(new_all_in, 2),
                        'new_all_in_ladder_fare': round(new_base, 2),
                        'data_period': ladder_result['data_period'],
                        'distinct_fares_count': ladder_result.get('distinct_fares_count_b2c', 0),
                        'total_booking_records': ladder_result.get('total_booking_records_b2c', 0),
                        'run_id': run_id,
                        'created_at': created_at,
                        'analyst': analystName
                    }
                    ladder_documents.append(doc)
            
            # Validate we have exactly self.TOTAL_FARES documents
            b2b_docs = len([d for d in ladder_documents if d['channel']=='B2B'])
            b2c_docs = len([d for d in ladder_documents if d['channel']=='B2C'])
            total_docs = len(ladder_documents)
            
            self.logger.info(f"Document count summary: B2B={b2b_docs}, B2C={b2c_docs}, Total={total_docs}")
            
            if total_docs != self.TOTAL_FARES:
                self.logger.warning(f"Expected {self.TOTAL_FARES} ladder documents but got {total_docs}")
                self.logger.warning(f"  B2B documents: {b2b_docs} (expected: {self.TOTAL_B2B_FARES})")
                self.logger.warning(f"  B2C documents: {b2c_docs} (expected: {self.TOTAL_B2C_FARES})")
            
            # Insert into MongoDB
            if ladder_documents:
                self.db[collection_name].insert_many(ladder_documents)
                self.logger.info(f"Saved {total_docs} ladder fares (B2B: {b2b_docs}, B2C: {b2c_docs})")
            else:
                self.logger.error("No ladder documents to save!")
                return None
                self.logger.info(f"Saved {len(ladder_documents)} ladder fares "
                            f"(B2B: {len([d for d in ladder_documents if d['channel']=='B2B'])}, "
                            f"B2C: {len([d for d in ladder_documents if d['channel']=='B2C'])})")
            
            return collection_name
            
        except Exception as e:
            self.logger.error(f"Error saving ladder to database: {str(e)}")
            self.failure_reason = f"Failed to save ladder to database: {str(e)}"
            raise

    def save_batch_run_audit(self, sectors, route, analystName, outlier_count, start_time, currVersion, run_id, 
                  failure_reason, ladder_results, file_path, status='SUCCESS', is_initial=False,
                  success_count=0, failed_count=0):
        """Save or update batch audit information for multiple sectors under one run_id."""
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
                    'sectors': sectors,  # Comma-separated list of sectors
                    'is_batch': True,
                    'start_time': start_time,
                    'end_time': None,
                    'status': 'IN_PROGRESS',
                    'total_sectors': len(sectors.split(', ')),
                    'successful_sectors': None,
                    'failed_sectors': None,
                    'outliers_removed': None,
                    'route': route,
                    'qp_fares': qp_fares_version,
                    'tax_master': tax_master_version,
                    'analyst': analystName,
                    'failure_reason': None
                }
                
                collection.insert_one(initial_audit)
                self.logger.info(f"Created batch audit record for run_id: {run_id}, sectors: {sectors}")
                
                return 'ladder_generation_audits'
                
            else:
                # Update existing audit record with final results
                if status == 'SUCCESS' and not ladder_results:
                    status = 'FAILED'
                
                # Aggregate statistics from all ladder results
                total_b2c_records = 0
                total_b2b_records = 0
                total_b2c_fares = 0
                total_b2b_fares = 0
                min_b2c_fare = None
                max_b2c_fare = None
                min_b2b_fare = None
                max_b2b_fare = None
                
                if ladder_results:
                    for lr in ladder_results:
                        total_b2c_records += lr.get('total_booking_records_b2c', 0)
                        total_b2b_records += lr.get('total_booking_records_b2b', 0)
                        total_b2c_fares += lr.get('distinct_fares_count_b2c', 0)
                        total_b2b_fares += lr.get('distinct_fares_count_b2b', 0)
                        
                        # Track min/max across all sectors
                        b2c_fares = lr.get('ladder_fares_b2c', [])
                        b2b_fares = lr.get('ladder_fares_b2b', [])
                        
                        if b2c_fares:
                            sector_min_b2c = min(b2c_fares)
                            sector_max_b2c = max(b2c_fares)
                            min_b2c_fare = sector_min_b2c if min_b2c_fare is None else min(min_b2c_fare, sector_min_b2c)
                            max_b2c_fare = sector_max_b2c if max_b2c_fare is None else max(max_b2c_fare, sector_max_b2c)
                        
                        if b2b_fares:
                            sector_min_b2b = min(b2b_fares)
                            sector_max_b2b = max(b2b_fares)
                            min_b2b_fare = sector_min_b2b if min_b2b_fare is None else min(min_b2b_fare, sector_min_b2b)
                            max_b2b_fare = sector_max_b2b if max_b2b_fare is None else max(max_b2b_fare, sector_max_b2b)
                
                update_doc = {
                    '$set': {
                        'end_time': datetime.now(pytz.timezone('Asia/Kolkata')),
                        'status': status,
                        'successful_sectors': success_count,
                        'failed_sectors': failed_count,
                        'outliers_removed': outlier_count,
                        'total_booking_records_b2c': total_b2c_records,
                        'total_booking_records_b2b': total_b2b_records,
                        'total_distinct_fares_b2c': total_b2c_fares,
                        'total_distinct_fares_b2b': total_b2b_fares,
                        'min_b2c_fare': round(min_b2c_fare, 2) if min_b2c_fare else None,
                        'max_b2c_fare': round(max_b2c_fare, 2) if max_b2c_fare else None,
                        'min_b2b_fare': round(min_b2b_fare, 2) if min_b2b_fare else None,
                        'max_b2b_fare': round(max_b2b_fare, 2) if max_b2b_fare else None,
                        'failure_reason': failure_reason if status == 'FAILED' else None
                    }
                }
                
                collection.update_one(
                    {'run_id': run_id},
                    update_doc
                )
                
                self.logger.info(f"Updated batch audit record for run_id: {run_id}, status: {status}")
                
                return 'ladder_generation_audits'
            
        except Exception as e:
            self.logger.error(f"Error saving/updating batch audit: {str(e)}")
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
                    self.logger.error("Failed to save even minimal batch audit update")
                    return None
            else:
                return None

    def save_run_audit(self, sector, route, analystName, outlier_count, start_time, currVersion, run_id, 
                  failure_reason, ladder_result, file_path, status='SUCCESS', is_initial=False):
        """Save or update audit information with enhanced failure tracking for both B2B and B2C."""
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
                    'b2c_total_ladder_points': None,
                    'b2c_core_ladder_points': None,
                    'b2c_buffer_ladder_points': None,
                    'b2b_total_ladder_points': None,
                    'b2b_core_ladder_points': None,
                    'b2b_buffer_ladder_points': None,
                    'outliers_removed': None,
                    'b2c_min_ladder_fare': None,
                    'b2c_max_ladder_fare': None,
                    'b2b_min_ladder_fare': None,
                    'b2b_max_ladder_fare': None,
                    'data_period_used': None,
                    'total_booking_records_b2c': None,
                    'total_booking_records_b2b': None,
                    'distinct_fares_count_b2c': None,
                    'distinct_fares_count_b2b': None,
                    'route': route,
                    'qp_fares': qp_fares_version,
                    'tax_master': tax_master_version,
                    'analyst': analystName,
                    'failure_reason': None
                }
                
                collection.insert_one(initial_audit)
                
                return 'ladder_generation_audits'
                
            else:
                # Update existing audit record with final results
                if status == 'SUCCESS' and not ladder_result:
                    status = 'FAILED'
                
                ladder_fares_b2c = ladder_result.get('ladder_fares_b2c', []) if ladder_result else []
                ladder_fares_b2b = ladder_result.get('ladder_fares_b2b', []) if ladder_result else []
                
                update_doc = {
                    '$set': {
                        'end_time': datetime.now(pytz.timezone('Asia/Kolkata')),
                        'status': status,
                        'b2c_total_ladder_points': len(ladder_fares_b2c) if ladder_fares_b2c else 0,
                        'b2c_core_ladder_points': self.CORE_FARES_B2C if ladder_fares_b2c else 0,
                        'b2c_buffer_ladder_points': self.BUFFER_FARES_B2C if ladder_fares_b2c else 0,
                        'b2b_total_ladder_points': len(ladder_fares_b2b) if ladder_fares_b2b else 0,
                        'b2b_core_ladder_points': self.CORE_FARES_B2B if ladder_fares_b2b else 0,
                        'b2b_buffer_ladder_points': self.BUFFER_FARES_B2B if ladder_fares_b2b else 0,
                        'outliers_removed': outlier_count,
                        'b2c_min_ladder_fare': round(min(ladder_fares_b2c), 2) if ladder_fares_b2c else None,
                        'b2c_max_ladder_fare': round(max(ladder_fares_b2c), 2) if ladder_fares_b2c else None,
                        'b2b_min_ladder_fare': round(min(ladder_fares_b2b), 2) if ladder_fares_b2b else None,
                        'b2b_max_ladder_fare': round(max(ladder_fares_b2b), 2) if ladder_fares_b2b else None,
                        'data_period_used': ladder_result['data_period'] if ladder_result else None,
                        'total_booking_records_b2c': ladder_result.get('total_booking_records_b2c', 0) if ladder_result else 0,
                        'total_booking_records_b2b': ladder_result.get('total_booking_records_b2b', 0) if ladder_result else 0,
                        'distinct_fares_count_b2c': ladder_result.get('distinct_fares_count_b2c', 0) if ladder_result else 0,
                        'distinct_fares_count_b2b': ladder_result.get('distinct_fares_count_b2b', 0) if ladder_result else 0,
                        'failure_reason': failure_reason if status == 'FAILED' else None
                    }
                }
                
                collection.update_one(
                    {'run_id': run_id},
                    update_doc
                )
                
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