import json
import requests
import configsettings

from allocation_summary import AllocationSummaryReport

class SFTPErrorRate:

    def __init__(self, logger, params, overrides):
        self.logger = logger
        self.rm_rd_conn = params['rdconn']
        self.rm_wr_conn = params['wrconn']

    def get_allocation_count(self, run_id):
        """Fetch Successful Allocation Count & Total Allocation Count from run_summary 
        by agrregating allocationStatus for given run_id"""

        alloc_summary = AllocationSummaryReport(self.rm_rd_conn, self.rm_wr_conn, self.logger)
        allocation_count = alloc_summary.fetch_how_count_data(run_id)
        return allocation_count[0]

    def add_allocation_summary_report(self, error_rate, total_rbd_count, sftp_push_run_id):
        """Insert into allocation_summary_report"""
        
        sftp_pushed_list = list(sftp_push_run_id)

        run_id = sftp_pushed_list[1]
        allocation_count = self.get_allocation_count(run_id)
        successfulAllocationCount = allocation_count[0]
        totalAllocationCount = allocation_count[1]

        sftp_pushed_list.extend([error_rate, total_rbd_count, successfulAllocationCount, totalAllocationCount])

        insert_query = """INSERT INTO QP_RM_REPORTS.allocation_summary_report 
                        (report_time_reference, run_id, type, dtd, start_time, end_time, actual_end_time, 
                        user, runTime, market_list_name, Id_marketList, Id_Curves, Id_Strategy, Id_QpFares,
                        failures, total, successfulAllocationCount, totalAllocationCount)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        
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
    
    def compute_error(self, rm_rbd, nav_rbd):
        """Calculte Error rate and Total RBD's"""

        error = 0
        total = 0
        for class_service, au in nav_rbd.items():
            if class_service in rm_rbd:
                if rm_rbd[class_service] >= 0:
                    # Calculating total RBD's from RM DB
                    total+= 1
                    if nav_rbd[class_service] != rm_rbd[class_service]:
                        # Calculating error_rate if AU's of Navitare ODS doesn't match with RM DB
                        error+=1
        
        return {"error_rate": error, "total_classes": total}
    
    def get_navitaire_rbd(self, departuredate, flightnumber):
        """Fetch RBD's from Navitaire ODS"""

        query = f'''
            SELECT ClassOfService, ClassAU
            FROM {configsettings.NAV_DB_NAME}[Rez].[InventoryLegClass] ILC
            JOIN {configsettings.NAV_DB_NAME}[Rez].[InventoryLeg] IL ON IL.[InventoryLegID] = ILC.[InventoryLegID]
            where IL.DepartureDate =  '{departuredate}'
            and IL.FlightNumber = '{flightnumber}'
            order by ClassAU
        '''
        payload = {
            "query":query
        }
        headers = {
            'x-api-key': configsettings.RM_GW_API_KEY
        }
        response = requests.request("POST",configsettings.NAV_API_URL ,headers=headers, data=json.dumps(payload))
        if response.status_code == 200:
            data = json.loads(response.text)
            nav_rbd = {item['ClassOfService']: item['ClassAU'] for item in data}
            return nav_rbd
        return False

    def run(self):
        """Compares the AU from Navitaire ODS & RM DB and computes the error_rate and total RBD's from RM DB.
            Updates the same in allocation_summary_report"""

        try:
            # get run_ids of pending audits
            sftp_pushed_run_ids = self.rm_rd_conn.execute(f"""SELECT start_time AS report_time_reference,
                                                            run_id AS runId, IFNULL(type, 0) AS type,
                                                            IFNULL(CONCAT(dtd_start, '-', dtd_end), 0) AS dtd,
                                                            start_time, 
                                                            IFNULL(actual_end_time, '') AS end_time,
                                                            IFNULL(actual_end_time, '') AS actual_end_time,
                                                            user, 
                                                            IFNULL(SEC_TO_TIME(TIMESTAMPDIFF(SECOND, start_time, MAX(actual_end_time))), '') AS runTime,
                                                            market_list_name,
                                                            IFNULL(market_list, 0) AS Id_marketList,
                                                            IFNULL(Curves, 0) AS Id_Curves,
                                                            IFNULL(d1_d2_strategies, 0) AS Id_Strategy,
                                                            IFNULL(Fares, 0) AS Id_QpFares
                                                            FROM allocation_run_audit 
                                                            WHERE is_sftp_pushed = '1' AND 
                                                            update_navitaire_method='sftp' AND 
                                                            is_audit_completed = '0' 
                                                            GROUP BY run_id
                                                            ORDER BY start_time desc""").fetchall()
            
            if sftp_pushed_run_ids:
                error_rate, total_class, index= 0,0,0
                total_sftp_push = len(sftp_pushed_run_ids)
                for data in sftp_pushed_run_ids:
                    flight_audit_result= self.rm_rd_conn.execute(f"SELECT flightNumber, flightDate, result FROM run_flight_date_audit WHERE runId='{data[1]}' AND (b2bstatus = 'Done' OR b2cstatus ='Done') ").fetchall()
                    if not flight_audit_result:
                        self.logger.warning(f"No flight date audit for runId: '{data[1]}' ")
                        continue

                    for res in flight_audit_result:
                        if not res[2]:
                            self.logger.warning(f"No AU's for runId: '{data[1]}' ")
                            continue

                        result = json.loads(res[2])
                        flight_num, flight_date, auClasses = res[0], res[1], result
                        nav_rbd = self.get_navitaire_rbd(flight_date, flight_num)
                        if not nav_rbd:
                            self.logger.info("RBD not Found!")
                            continue
                        
                        compute_data = self.compute_error(auClasses, nav_rbd)
                        error_rate += compute_data['error_rate']
                        total_class += compute_data['total_classes']
                            
                    # Adding run_ids into allocation_summary_report table
                    if index < total_sftp_push:
                        sftp_push_run_id = sftp_pushed_run_ids[index]
                        index +=1
                        self.add_allocation_summary_report(error_rate, total_class, sftp_push_run_id)
        except Exception as err:
            print(err)
            self.logger.error(err)
