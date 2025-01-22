import json
import traceback
import pandas as pd
import requests
import configsettings
import pandas as pd
import traceback

from allocation_summary import AllocationSummaryReport

class SFTPErrorRate:

    def __init__(self, logger, params, overrides, context):
        self.logger = logger
        self.rm_rd_conn = params['rdconn']
        self.rm_wr_conn = params['wrconn']
        self.navconn = params['navconn']
        self.context=context
        self.isconnections=False

    def get_allocation_count(self, run_id):
        """Fetch Successful Allocation Count & Total Allocation Count from run_summary 
        by agrregating allocationStatus for given run_id"""
        params = {}
        params['rdconn'] = self.rm_rd_conn
        params['wrconn'] = self.rm_wr_conn

        alloc_summary = AllocationSummaryReport(self.logger, params, {}, self.context)
        allocation_count = alloc_summary.fetch_how_count_data(run_id, self.isconnections)
        return allocation_count[0]

    def add_allocation_summary_report(self, error_rate, total_rbd_count, sftp_push_run_id):
        """Insert into allocation_summary_report"""
        
        sftp_pushed_list = list(sftp_push_run_id)

        run_id = sftp_pushed_list[1]
        allocation_count = self.get_allocation_count([run_id])
        successfulAllocationCount = allocation_count[0]
        totalAllocationCount = allocation_count[1]

        sftp_pushed_list.extend([error_rate, total_rbd_count, successfulAllocationCount, totalAllocationCount])

        insert_query = """INSERT INTO QP_RM_REPORTS.allocation_summary_report 
                        (report_time_reference, run_id, type, dtd, start_time, end_time, actual_end_time, 
                        user, runTime, market_list_name, marketListCount, Id_marketList, Id_Curves, Id_Strategy, Id_QpFares,
                        failures, total, successfulAllocationCount, totalAllocationCount)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        
        add_summary_report = self.rm_wr_conn.execute(insert_query, tuple(sftp_pushed_list))
        if add_summary_report.rowcount > 0:
            self.logger.info(f"Added allocation_summary_report for run_id: '{run_id}' ")
            self.update_audit_complete(run_id)
            return True
        else:
            self.logger.warning("No Records inserted!")
            return False

    def update_audit_complete(self, run_id):
        """Update is_audit_completed"""

        update_query = "UPDATE allocation_run_audit SET is_audit_completed='1' WHERE run_id =%s "
        update_data = self.rm_wr_conn.execute(update_query, run_id)
        if update_data.rowcount > 0:
            self.logger.info(f"Updated is_audit_completed='1' for run_id: '{run_id}' in allocation_run_audit")
        else:
            self.logger.warning("No Records Updated in allocation_run_audit!")
    
    def compute_error(self, rm_rbd, nav_rbd, rm_dept_date, nav_dept_date):
        """Calculte Error rate and Total RBD's"""

        error = 0
        total = 0
        if rm_dept_date == nav_dept_date:
            # for key, value in nav_rbd.items():
            if nav_rbd['ClassOfService'] in rm_rbd:
                ClassOfService = nav_rbd['ClassOfService']
                if rm_rbd[ClassOfService] >= 0:
                    # Calculating total RBD's from RM DB
                    total+= 1
                    if nav_rbd['ClassAU'] != rm_rbd[ClassOfService]:
                        # Calculating error_rate if AU's of Navitare ODS doesn't match with RM DB
                        error+=1
        
        return {"error_rate": error, "total_classes": total}
    
    def get_navitaire_rbd(self, flight_num, min_date, max_date, origin, destination):
        """Fetch RBD's from Navitaire ODS"""
        query = f'''
            SELECT ClassOfService, ClassAU, CONVERT(VARCHAR(10), DepartureDate, 120) as DepartureDate
            FROM {configsettings.NAV_DB_NAME}[Rez].[InventoryLegClass] ILC
            JOIN {configsettings.NAV_DB_NAME}[Rez].[InventoryLeg] IL ON IL.[InventoryLegID] = ILC.[InventoryLegID]
            where IL.DepartureDate >= '{min_date}' and IL.DepartureDate <= '{max_date}'
            and IL.FlightNumber = '{flight_num}' and IL.DepartureStation='{origin}' and IL.ArrivalStation='{destination}'
            order by DepartureDate
        '''
        if "/" in flight_num:
            self.isconnections=True
            flightnums=flight_num.split("/")
            query=f'''SELECT  ClassOfService,ClassAU ,CONVERT(VARCHAR(10), DepartureDate, 120) as DepartureDate FROM {configsettings.NAV_DB_NAME}[Rez].[InventoryJourneyClassStatus] where DepartureDate>='{min_date}' and DepartureDate <= '{max_date}' and InventoryJourneyKey LIKE '%{flightnums[0]}%%{flightnums[1]}%'  order by DepartureDate'''
        response=pd.read_sql(query,self.navconn)
        return response
    
    def get_flight_date_range(self, flight_audit_result):
        """Get min date and max date for flight number"""
        flights = {}
        for res in flight_audit_result:
            result, flight_key = None, None
            try:
                result = json.loads(res[2])
                sector = result['sector1']
                origin, destination = sector[:2] , sector[2:]
                flight_num = res[0]
                flight_key = origin + destination + flight_num
                flight_date = res[1]
            except Exception as err:
                self.logger.debug(f"RunId: {res[3]} Flight No.: {res[0]} & Flight Date: {res[1]} has no AUs")
                continue
            
            if not flight_key in flights:
                # Initalize key as flightNumber & value as min_date & min_date as dict key value pair.
                flights[flight_key] = {"min_date": flight_date, 'max_date': flight_date, 'dates': {}}
            else:
                # Initalize Min & Max Flight Date for each Flight Number
                if flight_date < flights[flight_key]['min_date']:
                    flights[flight_key]['min_date'] = flight_date
                
                if flight_date > flights[flight_key]['max_date']:
                    flights[flight_key]['max_date'] = flight_date 

            flights[flight_key]['dates'][flight_date] = result
        return flights

    def insert_into_error_audit_process(self, run_id, flight_num, error, total):
        insert_query = """INSERT INTO error_audit_process (run_id, flight_number, error, total) VALUES (%s, %s, %s, %s)"""
        self.rm_wr_conn.execute(insert_query, (run_id, flight_num, error, total))

    def check_run_in_error_audit_process(self, run_id, flight_num):
        """Checks if total & error is computed for given run_id for all flights in run_flight_date_audit.
            If run_id and flight number found in 'error_audit_process', skip iteration because error_rate & total is already computed"""
        
        error_audit = self.rm_rd_conn.execute(f"SELECT run_id FROM error_audit_process where run_id='{run_id}' AND flight_number='{flight_num}' ").fetchone()        
        
        if not error_audit:
            return False
        
        return True
    
    def aggregate_compute_data(self, run_id):
        """Aggregate sum of error & total."""
        agg_data = self.rm_rd_conn.execute(f"SELECT SUM(error) AS total_failures, SUM(total) AS total FROM error_audit_process WHERE run_id='{run_id}' ").fetchone()
        self.logger.info(f"run_id: {run_id} Total failure: {int(agg_data[0])} & Total RBD count: {int(agg_data[1])} ")
        
        return agg_data[0], agg_data[1]

    def delete_error_audit_process(self):
        delete_query = """TRUNCATE error_audit_process"""
        self.rm_wr_conn.execute(delete_query)
    
    
    def run(self):
        """Compares the AU from Navitaire ODS & RM DB and computes the error_rate and total RBD's from RM DB.
            Updates the same in allocation_summary_report"""

        try:
            # get run_ids of pending audits
            sftp_pushed_run_ids = self.rm_rd_conn.execute(f"""SELECT start_time AS report_time_reference,
                                                            run_id AS runId, 
                                                            IFNULL(type, 0) AS type,
                                                            IFNULL(CONCAT(dtd_start, '-', dtd_end), 0) AS dtd,
                                                            start_time, 
                                                            IFNULL(actual_end_time, '') AS end_time,
                                                            IFNULL(actual_end_time, '') AS actual_end_time,
                                                            user, 
                                                            IFNULL(SEC_TO_TIME(TIMESTAMPDIFF(SECOND, start_time, MAX(actual_end_time))), '') AS runTime,
                                                            market_list_name,
                                                            marketListCount,
                                                            IFNULL(market_list, 0) AS Id_marketList,
                                                            IFNULL(Curves, 0) AS Id_Curves,
                                                            IFNULL(d1_d2_strategies, 0) AS Id_Strategy,
                                                            IFNULL(Fares, 0) AS Id_QpFares
                                                            FROM allocation_run_audit 
                                                            WHERE is_sftp_pushed = '1' AND 
                                                            update_navitaire_method='sftp' AND 
                                                            is_audit_completed = '0' 
                                                            GROUP BY run_id
                                                            ORDER BY start_time""").fetchall()
            
            if sftp_pushed_run_ids:
                index = 0

                total_sftp_push = len(sftp_pushed_run_ids)
                for data in sftp_pushed_run_ids:
                    run_id = data[1]
                    self.logger.info(f"Run id: {run_id} :: START")
                    flight_audit_result= self.rm_rd_conn.execute(f"SELECT flightNumber, flightDate, result, runId FROM run_flight_date_audit WHERE runId='{run_id}' AND (b2bstatus = 'Done' OR b2cstatus ='Done') AND result is NOT NULL").fetchall()
                    
                    if not flight_audit_result:
                        self.logger.warning(f"No flight date audit for runId: '{run_id}' ")
                        continue

                    flights = self.get_flight_date_range(flight_audit_result)
                    for flight_key in flights.keys():
                        
                        # Gets time left for the lambda function to die, if it is less than the configured time than terminate. 
                        # m_sec = self.context.get_remaining_time_in_millis()
                    
                        # if m_sec < configsettings.EXP_TIME:
                        #     self.logger.warning("Lambda function about to timeout! Terminating..")
                        #     return False

                        flight_num = flight_key[6:]
                        origin = flight_key[:3]
                        destination = flight_key[3:6]
                        
                        if self.check_run_in_error_audit_process(run_id, flight_num):
                            continue
                        
                        flight_details = flights[flight_key]
                        min_date, max_date = flight_details['min_date'] , flight_details['max_date']
                        nav_rbds = self.get_navitaire_rbd(flight_num, min_date, max_date, origin, destination)
                        # No nav_rbds found !
                        if nav_rbds.empty:
                            continue

                        error_rate , total_class = 0, 0
                        for i,nav_rbd in nav_rbds.iterrows():
                            
                            nav_dept_date = nav_rbd['DepartureDate']
                            if nav_dept_date not in flight_details["dates"]:
                                continue
                            
                            if not flight_details["dates"][nav_dept_date]:
                                continue
                            flight_result = flight_details["dates"][nav_dept_date]

                            rm_dept_date = flight_result['departureDate']
                            rbd = flight_result['auClasses']
                            compute_data = self.compute_error( rbd, nav_rbd, rm_dept_date, nav_dept_date)
                            error_rate += compute_data['error_rate']
                            total_class += compute_data['total_classes']
                        
                        self.insert_into_error_audit_process(run_id, flight_key, error_rate, total_class)
                    
                    run_failure, run_total = self.aggregate_compute_data(run_id)

                    self.delete_error_audit_process()
                    
                    if index < total_sftp_push:
                        sftp_push_run_id = sftp_pushed_run_ids[index]
                        index +=1
                        self.add_allocation_summary_report(run_failure, run_total, sftp_push_run_id)

                    self.logger.info(f"Run id: {run_id} :: END")
        except Exception as err:
            self.logger.error(f"ERROR: {err}")
            traceback.print_exc()
    
