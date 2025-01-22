from sqlalchemy import text

class AllocationSummaryReport:

    def __init__(self, logger, params, overrides, context):
        self.logger = logger
        self.rm_rd_conn = params['rdconn']
        self.rm_wr_conn = params['wrconn']
        self.interval_time = '6 hour'

        if 'interval_time' in overrides:
            self.interval_time = overrides['interval_time']

    def get_all_allocation_run_summary(self, allocation_run_ids):
        """Returns list of dictionaries with data from allocation_run_audit, run_summary & navitaire_allocation_audit"""
        result = self.fetch_allocation_run_summary(allocation_run_ids)
        if result:
            output = []
            run_ids = []
            for row in result:
                row_dict = {"report_time_reference": row[0],
                            "run_id": row[1],
                            "type": row[2],
                            "region": row[3],
                            "dtd": row[4],
                            "start_time": row[5],
                            "end_time": row[6],
                            "actual_end_time": row[7] or None,
                            "failures": row[8],
                            "total": row[9],
                            "marketListCount": row[10],
                            "user": row[11],
                            "runTime": row[12],
                            "successfulAllocationCount": None,
                            "totalAllocationCount": None,
                            "market_list_name": row[13],
                            "Id_marketList": row[14],
                            "Id_Curves": row[15],
                            "Id_Strategy": row[16],
                            "Id_QpFares": row[17],
                            "adhoc_run_status": row[18]
                            }
                
                output.append(row_dict)
                run_ids.append(row[1])
            
            run_ids = tuple(run_ids)
            how_counts = self.fetch_how_count_data(run_ids,False)
            how_counts=sorted(how_counts, key=lambda x: x[2], reverse=True)
            output=sorted(output, key=lambda x: x["run_id"], reverse=True)
            if how_counts:
                idx = 0
                for data in output:
                    if idx < len(how_counts) and data['run_id'] == how_counts[idx][2]:
                        data['successfulAllocationCount'] = how_counts[idx][0]
                        data['totalAllocationCount'] = how_counts[idx][1]
                        idx += 1
                    
            return output
        else:
            self.logger.info("No records Found for allocation summary!")
            return False

    def fetch_allocation_run_summary(self, allocation_run_ids):
        """Get data by joining allocation_run_audit & navitaire_allocation_audit"""
        if len(allocation_run_ids) == 1:
            run_id = allocation_run_ids[0][0]
            condition = f"WHERE ara.run_id = '{run_id}'"
        else:
            extracted_values = [item[0] for item in allocation_run_ids]
            run_ids = tuple(extracted_values)
            condition = f"WHERE ara.run_id IN {run_ids}"

        select = f"""SELECT ara.start_time AS report_time_reference,
                        ara.run_id AS runId,
                        IFNULL(ara.type, 0) AS type,
                        IFNULL(ara.region, '') AS region,
                        IFNULL(CONCAT(ara.dtd_start, '-', ara.dtd_end), 0) AS dtd,
                        ara.start_time AS start_time,
                        MAX(naa.end_time) AS end_time,
                        IFNULL(ara.actual_end_time, '') AS actual_end_time,
                        IFNULL(SUM(CASE WHEN naa.failures > 0 THEN 1 ELSE 0 END), - 1) AS failures,
                        IFNULL(COUNT(naa.totalApiCalls), - 1) AS total,
                        IFNULL(ara.marketListCount, - 1) AS marketListCount,
                        ara.user,
                        IFNULL(SEC_TO_TIME(TIMESTAMPDIFF(SECOND, ara.start_time, max(naa.end_time))), '') AS runTime,
                        ara.market_list_name,
                        IFNULL(ara.market_list, 0) AS Id_marketList,
                        IFNULL(ara.Curves, 0) AS Id_Curves,
                        IFNULL(ara.d1_d2_strategies, 0) AS Id_Strategy,
                        IFNULL(ara.Fares, 0) AS Id_QpFares,
                        IFNULL(ara.adhoc_run_status, '') AS adhoc_run_status
                    FROM
                        QP_DW_RMALLOC.allocation_run_audit ara 
                    LEFT JOIN
                        QP_DW_RMALLOC.navitaire_allocation_audit naa ON ara.run_id = naa.run_id 
                    {condition} AND ara.type != 'Local Run' AND (ara.total_batch_size-ara.batch_completed=0 or ara.total_batch_size is null)
                    GROUP BY ara.run_id"""
            
        result = self.rm_rd_conn.execute(select).fetchall()
        return result

    def fetch_how_count_data(self, run_ids,isconnections):
        """Get aggregate data of HowCount from run_summary"""
        table="run_summary"
        if isconnections:
            table="run_summary_connections"
        if len(run_ids) == 1:
            run_ids = run_ids[0]
            condition = f" WHERE rs.RunId = '{run_ids}' "
        else:
            condition = f" WHERE rs.RunId IN {run_ids} "
            
        get_how_count = f"""SELECT
                                SUM(CASE
                                    WHEN rs.allocationStatus = 1 THEN 1
                                    ELSE 0
                                END) AS successfulAllocationCount,
                                count(1) AS totalAllocationCount,
                                rs.runId
                            FROM
                                QP_DW_RMALLOC.{table} rs
                                {condition}
                            GROUP BY rs.RunId"""
            
        how_counts = self.rm_rd_conn.execute(get_how_count).fetchall()
        return how_counts
    
    def add_allocation_summary_report(self, allocation_summary_data):
        """Inserts allocation summary data into table `allocation_summary_report` """
        columns = ", ".join([f"`{col}`" for col in allocation_summary_data[0].keys()])
        placeholder = ", ".join([f":{col}" for col in allocation_summary_data[0].keys()])

        insert_query = f"INSERT INTO QP_RM_REPORTS.allocation_summary_report ({columns}) VALUES ({placeholder})"
        stmt = text(insert_query)
        result = self.rm_wr_conn.execute(stmt, allocation_summary_data)
        if result.rowcount > 0:
            self.logger.info(f"{result.rowcount} Records inserted Successfully!")
            return True
        else:
            self.logger.warning("No Records inserted!")
            return False

    def update_api_audit_complete(self, allocation_run_ids):
        """After inserting records in allocation_summary_report set is_audit_completed='1' """
        if len(allocation_run_ids) == 1:
            run_id = allocation_run_ids[0][0]
            condition = f"run_id = '{run_id}'"
        else:
            extracted_values = [item[0] for item in allocation_run_ids]
            run_ids = tuple(extracted_values)
            condition = f"run_id IN {run_ids}"

        self.rm_wr_conn.execute(f"UPDATE QP_DW_RMALLOC.allocation_run_audit SET is_audit_completed='1' WHERE {condition} ")
        self.logger.info(f"Audit completed for 'API' navitaire update method")
    
    def delete_run_ids(self, allocation_run_ids):
        if len(allocation_run_ids) == 1:
            run_id = allocation_run_ids[0][0]
            condition = f"run_id = '{run_id}'"
        else:
            extracted_values = [item[0] for item in allocation_run_ids]
            run_ids = tuple(extracted_values)
            condition = f"run_id IN {run_ids}"

        result = self.rm_wr_conn.execute(f"DELETE FROM QP_RM_REPORTS.allocation_summary_report WHERE {condition} ")
        if result.rowcount > 0:
            self.logger.info(f"{result.rowcount} Records Deleted Successfully!")
            return True
        else:
            self.logger.warning("No Records Deleted!")
            return False

    # Insert run_ids and other details into allocation_summary_report table
    def run(self):
        """This method aggregates data from allocation_run_audit, run_summary & navitaire_allocation_audit 
        inserts it into allocation_summary_report table"""
        try:
            # Since not being used below code is Commented
            # lastInsertedTime = "2023-08-31 00:00:00"
            # get_lastInsertedTime = self.rm_rd_conn.execute("SELECT report_time_reference FROM QP_RM_REPORTS.allocation_summary_report order by report_time_reference desc limit 1").fetchone()
            # if get_lastInsertedTime:
            #     lastInsertedTime = get_lastInsertedTime[0].strftime('%Y-%m-%d %H:%M:%S')

            # self.logger.debug(f"Last Inserted Time in allocation_summary_report: {lastInsertedTime}")
            # get latest run_ids with navitaire update method as API
            
            allocation_run_ids = self.rm_rd_conn.execute(f"""SELECT run_id FROM QP_DW_RMALLOC.allocation_run_audit
                                                         WHERE update_navitaire_method='api' 
                                                         AND start_time > NOW() - INTERVAL {self.interval_time}""").fetchall()
                                                        #  ( start_time > '{lastInsertedTime}' OR 
                                                        #  (is_run_completed = '0' AND start_time > NOW() - INTERVAL 1 hour ) )""").fetchall()
                
            self.logger.debug(f"Count of new allocation run_ids: {len(allocation_run_ids)}")
            if allocation_run_ids:
                allocation_summary_data = self.get_all_allocation_run_summary(allocation_run_ids)
                self.delete_run_ids(allocation_run_ids)
                is_added = self.add_allocation_summary_report(allocation_summary_data)
                if is_added:
                    self.update_api_audit_complete(allocation_run_ids)
                    return True
                return False
            else:
                self.logger.info("No new run_ids Found!")
        except Exception as err:
            self.logger.error(err)
