import json
import traceback
import pandas as pd
import requests
import configsettings
from datetime import datetime

from allocation_summary import AllocationSummaryReport

class SFTPErrorRate:

    def __init__(self, logger, params, overrides, context):
        self.logger = logger
        self.mongoconn = params['mongoconn']  # Single MongoDB connection
        self.navconn = params['navconn']
        self.context = context
        self.isconnections = False
        
        # MongoDB collections
        self.db = self.mongoconn["qp_dw_rmalloc"]
        self.db_reports = self.mongoconn["qp_rm_reports"]
        self.allocation_summary_report = self.db_reports['allocation_summary_report']
        self.allocation_run_audit = self.db['allocation_run_audit']
        self.run_flight_date_audit = self.db['run_flight_date_audit']
        self.error_audit_process = self.db['error_audit_process']

    def get_allocation_count(self, run_id):
        """Fetch Successful Allocation Count & Total Allocation Count from run_summary 
        by aggregating allocationStatus for given run_id"""
        params = {}
        params['mongoconn'] = self.mongoconn

        alloc_summary = AllocationSummaryReport(self.logger, params, {}, self.context)
        allocation_count = alloc_summary.fetch_how_count_data(run_id, self.isconnections)
        return allocation_count[0]

    def add_allocation_summary_report(self, error_rate, total_rbd_count, sftp_push_run_id):
        """Insert into allocation_summary_report"""
        
        sftp_pushed_list = list(sftp_push_run_id)

        run_id = sftp_pushed_list[1]
        delete_result = self.allocation_summary_report.delete_many({'run_id': run_id})
        allocation_count = self.get_allocation_count([run_id])
        successfulAllocationCount = allocation_count['successfulAllocationCount']
        totalAllocationCount = allocation_count['totalAllocationCount']

        # Prepare document for MongoDB
        document = {
            'report_time_reference': sftp_pushed_list[0],
            'run_id': sftp_pushed_list[1],
            'type': sftp_pushed_list[2],
            'dtd': sftp_pushed_list[3],
            'start_time': sftp_pushed_list[4],
            'end_time': sftp_pushed_list[5],
            'actual_end_time': sftp_pushed_list[6],
            'user': sftp_pushed_list[7],
            'runTime': sftp_pushed_list[8],
            'market_list_name': sftp_pushed_list[9],
            'marketListCount': sftp_pushed_list[10],
            'Id_marketList': sftp_pushed_list[11],
            'Id_Curves': sftp_pushed_list[12],
            'Id_Strategy': sftp_pushed_list[13],
            'Id_QpFares': sftp_pushed_list[14],
            'region': sftp_pushed_list[15],
            'failures': error_rate,
            'total': total_rbd_count,
            'successfulAllocationCount': successfulAllocationCount,
            'totalAllocationCount': totalAllocationCount,
            'created_at': datetime.utcnow()
        }
        
        result = self.allocation_summary_report.insert_one(document)
        
        if result.inserted_id:
            self.logger.info(f"Added allocation_summary_report for run_id: '{run_id}' ")
            self.update_audit_complete(run_id)
            return True
        else:
            self.logger.warning("No Records inserted!")
            return False

    def update_audit_complete(self, run_id):
        """Update is_audit_completed"""

        result = self.allocation_run_audit.update_one(
            {'run_id': run_id},
            {'$set': {'is_audit_completed': '1', 'updated_at': datetime.utcnow()}}
        )
        
        if result.modified_count > 0:
            self.logger.info(f"Updated is_audit_completed='1' for run_id: '{run_id}' in allocation_run_audit")
        else:
            self.logger.warning("No Records Updated in allocation_run_audit!")
    
    def compute_error(self, rm_rbd, nav_rbd, rm_dept_date, nav_dept_date):
        """Calculate Error rate and Total RBD's"""

        error = 0
        total = 0
        if rm_dept_date == nav_dept_date:
            if nav_rbd['ClassOfService'] in rm_rbd:
                ClassOfService = nav_rbd['ClassOfService']
                if rm_rbd[ClassOfService] >= 0:
                    # Calculating total RBD's from RM DB
                    total += 1
                    if nav_rbd['ClassAU'] != rm_rbd[ClassOfService]:
                        # Calculating error_rate if AU's of Navitare ODS doesn't match with RM DB
                        error += 1
        
        return {"error_rate": error, "total_classes": total}
    
    
    def get_all_navitaire_rbd(self, flight_data):
        """Fetch all RBD's from Navitaire ODS at once for all flights in date range"""
        # Find global min and max dates across all flights
        min_global_date = min([flight_info['min_date'] for flight_info in flight_data.values()])
        max_global_date = max([flight_info['max_date'] for flight_info in flight_data.values()])
        
        # Query for regular flights - fetch ALL flights in the date range
        regular_query = f'''
            SELECT 
                ClassOfService, 
                ClassAU, 
                CONVERT(VARCHAR(10), IL.DepartureDate, 120) as DepartureDate,
                IL.FlightNumber,
                IL.DepartureStation as Origin,
                IL.ArrivalStation as Destination
            FROM {configsettings.NAV_DB_NAME}[Rez].[InventoryLegClass] ILC
            JOIN {configsettings.NAV_DB_NAME}[Rez].[InventoryLeg] IL ON IL.[InventoryLegID] = ILC.[InventoryLegID]
            WHERE IL.DepartureDate >= '{min_global_date}' 
            AND IL.DepartureDate <= '{max_global_date}'
            ORDER BY IL.DepartureDate
        '''
        regular_flights_df = pd.read_sql(regular_query, self.navconn)
        
        # Query for connection flights - fetch ALL connections in the date range
        connection_query = f'''
            SELECT 
                ClassOfService,
                ClassAU,
                CONVERT(VARCHAR(10), DepartureDate, 120) as DepartureDate,
                InventoryJourneyKey as FlightInfo
            FROM {configsettings.NAV_DB_NAME}[Rez].[InventoryJourneyClassStatus] 
            WHERE DepartureDate >= '{min_global_date}' 
            AND DepartureDate <= '{max_global_date}'
            ORDER BY DepartureDate
        '''
        connection_flights_df = pd.read_sql(connection_query, self.navconn)
        
        return regular_flights_df, connection_flights_df

    def get_flight_date_range(self, flight_audit_result):
        """Get min date and max date for flight number"""
        flights = {}
        for res in flight_audit_result:
            result, flight_key = None, None
            try:
                result = json.loads(res['result'])
                sector = result['sector1']
                origin, destination = sector[:2], sector[2:]
                flight_num = res['flightNumber']
                flight_key = origin + destination + flight_num
                flight_date = res['flightDate']
            except Exception as err:
                self.logger.debug(f"RunId: {res['runId']} Flight No.: {res['flightNumber']} & Flight Date: {res['flightDate']} has no AUs")
                continue
            
            if not flight_key in flights:
                # Initialize key as flightNumber & value as min_date & min_date as dict key value pair.
                flights[flight_key] = {"min_date": flight_date, 'max_date': flight_date, 'dates': {}}
            else:
                # Initialize Min & Max Flight Date for each Flight Number
                if flight_date < flights[flight_key]['min_date']:
                    flights[flight_key]['min_date'] = flight_date
                
                if flight_date > flights[flight_key]['max_date']:
                    flights[flight_key]['max_date'] = flight_date 

            flights[flight_key]['dates'][flight_date] = result
        return flights

    def insert_into_error_audit_process(self, run_id, flight_num, error, total):
        document = {
            'run_id': run_id,
            'flight_number': flight_num,
            'error': error,
            'total': total,
            'created_at': datetime.utcnow()
        }
        self.error_audit_process.insert_one(document)

    def check_run_in_error_audit_process(self, run_id, flight_num):
        """Checks if total & error is computed for given run_id for all flights in run_flight_date_audit.
            If run_id and flight number found in 'error_audit_process', skip iteration because error_rate & total is already computed"""
        
        error_audit = self.error_audit_process.find_one({
            'run_id': run_id,
            'flight_number': flight_num
        })
        
        return error_audit is not None
    
    def aggregate_compute_data(self, run_id):
        """Aggregate sum of error & total."""
        pipeline = [
            {'$match': {'run_id': run_id}},
            {'$group': {
                '_id': None,
                'total_failures': {'$sum': '$error'},
                'total': {'$sum': '$total'}
            }}
        ]
        
        result = list(self.error_audit_process.aggregate(pipeline))
        
        if result:
            total_failures = int(result[0]['total_failures'])
            total = int(result[0]['total'])
            self.logger.info(f"run_id: {run_id} Total failure: {total_failures} & Total RBD count: {total} ")
            return total_failures, total
        
        return 0, 0

    def delete_error_audit_process(self):
        self.error_audit_process.delete_many({})
    
    def process_flight_data(self, run_id, flights, regular_flights_df, connection_flights_df):
        """Process flight data and compute error rates"""
        for flight_key in flights.keys():
            flight_num = flight_key[6:]
            origin = flight_key[:3]
            destination = flight_key[3:6]
            
            if self.check_run_in_error_audit_process(run_id, flight_num):
                continue
                
            flight_details = flights[flight_key]
            
            # Process the data for this flight
            error_rate, total_class = 0, 0
            
            if "/" in flight_num:  # Connection flight
                self.isconnections = True
                # Filter connection flights data
                flightnums = flight_num.split("/")
                flight_filter = lambda x: all(fn in x for fn in flightnums)
                filtered_data = connection_flights_df[connection_flights_df['FlightInfo'].apply(flight_filter)]
            else:  # Regular flight
                # Filter regular flights data
                filtered_data = regular_flights_df[
                    (regular_flights_df['FlightNumber'] == flight_num) & 
                    (regular_flights_df['Origin'] == origin) & 
                    (regular_flights_df['Destination'] == destination)
                ]
            
            # Process the filtered data
            for _, nav_rbd in filtered_data.iterrows():
                nav_dept_date = nav_rbd['DepartureDate']
                if nav_dept_date not in flight_details["dates"]:
                    continue
                
                if not flight_details["dates"][nav_dept_date]:
                    continue
                    
                flight_result = flight_details["dates"][nav_dept_date]
                rm_dept_date = flight_result['departureDate']
                rbd = flight_result['auClasses']
                compute_data = self.compute_error(rbd, nav_rbd, rm_dept_date, nav_dept_date)
                error_rate += compute_data['error_rate']
                total_class += compute_data['total_classes']
            
            self.insert_into_error_audit_process(run_id, flight_key, error_rate, total_class)
            
    def calculate_runtime(self,start_time, actual_end_time):
        """Calculate runtime as actual_end_time - start_time
        
        Args:
            start_time: string datetime
            actual_end_time: datetime object
        
        Returns:
            string representation of runtime duration
        """
        try:
            # Convert start_time string to datetime if it's not already
            if isinstance(start_time, str):
                start_dt = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
            else:
                start_dt = start_time
            
            # Calculate runtime difference
            if actual_end_time and start_dt:
                runtime_delta = actual_end_time - start_dt
                
                # Format as string (e.g., "2:30:15" for 2 hours, 30 minutes, 15 seconds)
                total_seconds = int(runtime_delta.total_seconds())
                hours = total_seconds // 3600
                minutes = (total_seconds % 3600) // 60
                seconds = total_seconds % 60
                
                return f"{hours}:{minutes:02d}:{seconds:02d}"
            else:
                return ''
        except Exception as e:
            return ''
    
    def run(self):
        """Compares the AU from Navitaire ODS & RM DB and computes the error_rate and total RBD's from RM DB.
            Updates the same in allocation_summary_report"""

        try:
            # Get run_ids of pending audits
            pipeline = [
                {
                    '$match': {
                        'is_sftp_pushed': '1',
                        'update_navitaire_method': 'sftp',
                        'is_audit_completed': {'$in': ['0', None]}
                    }
                },
                {
                    '$group': {
                        '_id': '$run_id',
                        'report_time_reference': {'$first': '$start_time'},
                        'run_id': {'$first': '$run_id'},
                        'type': {'$first': {'$ifNull': ['$type', 0]}},
                        'dtd': {'$first': {
                            '$ifNull': [
                                {'$concat': [
                                    {'$toString': '$dtd_start'},
                                    '-',
                                    {'$toString': '$dtd_end'}
                                ]},
                                '0'
                            ]
                        }},
                        'start_time': {'$first': '$start_time'},
                        'end_time': {'$first': {'$ifNull': ['$actual_end_time', '']}},
                        'actual_end_time': {'$max': '$actual_end_time'},
                        'user': {'$first': '$user'},
                        'region': {'$first': '$region'},
                        'market_list_name': {'$first': '$market_list_name'},
                        'marketListCount': {'$first': '$marketListCount'},
                        'Id_marketList': {'$first': {'$ifNull': ['$market_list', 0]}},
                        'Id_Curves': {'$first': {'$ifNull': ['$Curves', 0]}},
                        'Id_Strategy': {'$first': {'$ifNull': ['$d1_d2_strategies', 0]}},
                        'Id_QpFares': {'$first': {'$ifNull': ['$Fares', 0]}}
                    }
                },
                {
                    '$sort': {'start_time': 1}
                }
            ]
            
            sftp_pushed_run_ids = list(self.allocation_run_audit.aggregate(pipeline))
            
            if sftp_pushed_run_ids:
                index = 0
                total_sftp_push = len(sftp_pushed_run_ids)
                
                for data in sftp_pushed_run_ids:
                    run_id = data['run_id']
                    self.logger.info(f"Run id: {run_id} :: START")
                    
                    # Query flight audit data
                    flight_audit_result = list(self.run_flight_date_audit.find({
                        'runId': run_id,
                        '$or': [
                            {'b2bstatus': 'Done'},
                            {'b2cstatus': 'Done'}
                        ],
                        'result': {'$ne': None}
                    }, {
                        'flightNumber': 1,
                        'flightDate': 1,
                        'result': 1,
                        'runId': 1
                    }))
                    
                    if not flight_audit_result:
                        self.logger.warning(f"No flight date audit for runId: '{run_id}' ")
                        continue

                    flights = self.get_flight_date_range(flight_audit_result)
                    
                    # Get all flight data at once
                    regular_flights_df, connection_flights_df = self.get_all_navitaire_rbd(flights)
                    
                    # Process flight data
                    self.process_flight_data(run_id, flights, regular_flights_df, connection_flights_df)
                    
                    run_failure, run_total = self.aggregate_compute_data(run_id)

                    self.delete_error_audit_process()
                    
                    if index < total_sftp_push:
                        # Convert MongoDB document to tuple format for compatibility
                        sftp_push_run_id = (
                            data['report_time_reference'],
                            data['run_id'],
                            data['type'],
                            data['dtd'],
                            data['start_time'],
                            data['end_time'],
                            data['actual_end_time'],
                            data['user'],
                            self.calculate_runtime(data['start_time'], data['actual_end_time']),  # Calculate runtime if needed
                            data['market_list_name'],
                            data['marketListCount'],
                            data['Id_marketList'],
                            data['Id_Curves'],
                            data['Id_Strategy'],
                            data['Id_QpFares'],
                            data['region']
                        )
                        index += 1
                        self.add_allocation_summary_report(run_failure, run_total, sftp_push_run_id)

                    self.logger.info(f"Run id: {run_id} :: END")
        except Exception as err:
            self.logger.error(f"ERROR: {err}")
            traceback.print_exc()