from datetime import datetime, timedelta

class AllocationSummaryReport:

    def __init__(self, logger, params, overrides, context):
        self.logger = logger
        self.mongo_db = params['mongoconn']["qp_dw_rmalloc"]
        self.mongo_db_archives = params['mongoconn']["qp_rm_reports"]
        self.interval_time = 10  # hours

        if 'interval_time' in overrides:
            self.interval_time = overrides['interval_time']

    def get_all_allocation_run_summary(self, allocation_run_ids):
        """Returns list of dictionaries with data from allocation_run_audit, run_summary & navitaire_allocation_audit"""
        result = self.fetch_allocation_run_summary(allocation_run_ids)
        if result:
            output = []
            run_ids = []
            
            for row in result:
                row_dict = {
                    "report_time_reference": row.get("report_time_reference"),
                    "run_id": row.get("run_id"),
                    "type": row.get("type"),
                    "region": row.get("region"),
                    "dtd": row.get("dtd"),
                    "start_time": row.get("start_time"),
                    "end_time": row.get("end_time"),
                    "actual_end_time": row.get("actual_end_time"),
                    "failures": row.get("failures"),
                    "total": row.get("total"),
                    "marketListCount": row.get("marketListCount"),
                    "user": row.get("user"),
                    "runTime": row.get("runTime"),
                    "successfulAllocationCount": None,
                    "totalAllocationCount": None,
                    "market_list_name": row.get("market_list_name"),
                    "Id_marketList": row.get("Id_marketList"),
                    "Id_Curves": row.get("Id_Curves"),
                    "Id_Strategy": row.get("Id_Strategy"),
                    "Id_QpFares": row.get("Id_QpFares"),
                    "adhoc_run_status": row.get("adhoc_run_status")
                }
                
                output.append(row_dict)
                run_ids.append(row.get("run_id"))
            
            how_counts = self.fetch_how_count_data(run_ids, False)
            how_counts = sorted(how_counts, key=lambda x: x["run_id"], reverse=True)
            output = sorted(output, key=lambda x: x["run_id"], reverse=True)
            
            if how_counts:
                idx = 0
                for data in output:
                    if idx < len(how_counts) and data['run_id'] == how_counts[idx]["run_id"]:
                        data['successfulAllocationCount'] = how_counts[idx]["successfulAllocationCount"]
                        data['totalAllocationCount'] = how_counts[idx]["totalAllocationCount"]
                        idx += 1
                    
            return output
        else:
            self.logger.info("No records Found for allocation summary!")
            return False

    def fetch_allocation_run_summary(self, allocation_run_ids):
        """Get data by joining allocation_run_audit & navitaire_allocation_audit"""
        run_id_list = [item[0] if isinstance(item, tuple) else item for item in allocation_run_ids]

        # MongoDB aggregation pipeline
        pipeline = [
            {
                "$match": {
                    "run_id": {"$in": run_id_list},
                    "type": {"$ne": "Local Run"},
                    "$or": [
                        {"$expr": {"$eq": [{"$subtract": ["$total_batch_size", "$batch_completed"]}, 0]}},
                        {"total_batch_size": None}
                    ]
                }
            },
            {
                "$lookup": {
                    "from": "navitaire_allocation_audit",
                    "localField": "run_id",
                    "foreignField": "run_id",
                    "as": "navitaire_data",
                    # OPTIMIZATION: Only fetch necessary fields from lookup
                    "pipeline": [
                        {
                            "$match": {
                                "type": {"$ne": "nextgen_au"}
                            }
                        },
                        {
                            "$project": {
                                "run_id": 1,
                                "end_time": 1,
                                "failures": 1
                            }
                        }
                    ]
                }
            },
            {
                "$addFields": {
                    "max_end_time": {"$max": "$navitaire_data.end_time"},
                    "failures_count": {
                        "$size": {
                            "$filter": {
                                "input": "$navitaire_data",
                                "as": "nav",
                                "cond": {"$gt": ["$$nav.failures", 0]}
                            }
                        }
                    },
                    "total_count": {"$size": "$navitaire_data"},
                    "run_time_seconds": {
                        "$dateDiff": {
                            "startDate": {"$dateFromString": {"dateString": "$start_time"}},
                            "endDate": {"$max": "$navitaire_data.end_time"},
                            "unit": "second"
                        }
                    }
                }
            },
            {
                "$project": {
                    # Don't include navitaire_data in final output
                    "navitaire_data": 0  # CRITICAL: Drop the large array before output
                }
            },
            {
                "$project": {
                    "report_time_reference": "$start_time",
                    "run_id": 1,
                    "type": {"$ifNull": ["$type", 0]},
                    "region": {"$ifNull": ["$region", ""]},
                    "dtd": {
                        "$ifNull": [
                            {"$concat": [{"$toString": "$dtd_start"}, "-", {"$toString": "$dtd_end"}]},
                            "0"
                        ]
                    },
                    "start_time": 1,
                    "end_time": "$max_end_time",
                    "actual_end_time": {"$ifNull": ["$actual_end_time", None]},
                    "failures": {"$ifNull": ["$failures_count", -1]},
                    "total": {"$ifNull": ["$total_count", -1]},
                    "marketListCount": {"$ifNull": ["$marketListCount", -1]},
                    "user": 1,
                    "runTime": {
                        "$ifNull": [
                            {
                                "$concat": [
                                    {"$toString": {"$floor": {"$divide": ["$run_time_seconds", 3600]}}},
                                    ":",
                                    {"$toString": {"$floor": {"$divide": [{"$mod": ["$run_time_seconds", 3600]}, 60]}}},
                                    ":",
                                    {"$toString": {"$mod": ["$run_time_seconds", 60]}}
                                ]
                            },
                            ""
                        ]
                    },
                    "market_list_name": 1,
                    "Id_marketList": {"$ifNull": ["$market_list", 0]},
                    "Id_Curves": {"$ifNull": ["$Curves", 0]},
                    "Id_Strategy": {"$ifNull": ["$d1_d2_strategies", 0]},
                    "Id_QpFares": {"$ifNull": ["$Fares", 0]},
                    "adhoc_run_status": {"$ifNull": ["$adhoc_run_status", ""]}
                }
            }
        ]

        # CRITICAL: Add allowDiskUse=True
        result = list(self.mongo_db.allocation_run_audit.aggregate(
            pipeline, 
            allowDiskUse=True
        ))
        return result

    def fetch_how_count_data(self, run_ids, isconnections):
        """Get aggregate data of HowCount from run_summary"""
        collection_name = "run_summary_connections" if isconnections else "run_summary"
        
        pipeline = [
            {
                "$match": {
                    "RunId": {"$in": run_ids}
                }
            },
            {
                "$group": {
                    "_id": "$RunId",
                    "successfulAllocationCount": {
                        "$sum": {
                            "$cond": [{"$eq": ["$allocationStatus", "1"]}, 1, 0]
                        }
                    },
                    "totalAllocationCount": {"$sum": 1}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "successfulAllocationCount": 1,
                    "totalAllocationCount": 1,
                    "run_id": "$_id"
                }
            }
        ]
        
        how_counts = list(self.mongo_db[collection_name].aggregate(pipeline))
        return how_counts

    def add_allocation_summary_report(self, allocation_summary_data):
        """Inserts allocation summary data into collection `allocation_summary_report` """
        try:
            result = self.mongo_db_archives.allocation_summary_report.insert_many(allocation_summary_data)
            inserted_count = len(result.inserted_ids)
            
            if inserted_count > 0:
                self.logger.info(f"{inserted_count} Records inserted Successfully!")
                return True
            else:
                self.logger.warning("No Records inserted!")
                return False
        except Exception as e:
            self.logger.error(f"Error inserting records: {e}")
            return False

    def update_api_audit_complete(self, allocation_run_ids):
        """After inserting records in allocation_summary_report set is_audit_completed='1' """
        run_id_list = [item[0] if isinstance(item, tuple) else item for item in allocation_run_ids]
        
        result = self.mongo_db.allocation_run_audit.update_many(
            {"run_id": {"$in": run_id_list}},
            {"$set": {"is_audit_completed": "1"}}
        )
        
        self.logger.info(f"Audit completed for 'API' navitaire update method. Modified {result.modified_count} documents")
    
    def delete_run_ids(self, allocation_run_ids):
        """Delete existing records for run_ids before inserting new ones"""
        run_id_list = [item[0] if isinstance(item, tuple) else item for item in allocation_run_ids]
        
        result = self.mongo_db_archives.allocation_summary_report.delete_many(
            {"run_id": {"$in": run_id_list}}
        )
        
        if result.deleted_count > 0:
            self.logger.info(f"{result.deleted_count} Records Deleted Successfully!")
            return True
        else:
            self.logger.warning("No Records Deleted!")
            return False

    def run(self):
        """This method aggregates data from allocation_run_audit, run_summary & navitaire_allocation_audit 
        inserts it into allocation_summary_report collection"""
        try:
            # Calculate time threshold
            time_threshold = datetime.now() - timedelta(hours=self.interval_time)
            time_threshold_str = time_threshold.strftime("%Y-%m-%d %H:%M:%S")
            # Get latest run_ids with navitaire update method as API
            allocation_run_ids = list(
                self.mongo_db.allocation_run_audit.find(
                    {
                        "update_navitaire_method": "api",
                        "start_time": {"$gt": time_threshold_str}
                    },
                    {"run_id": 1, "_id": 0}
                )
            )
            
            # Convert to list of tuples for backward compatibility
            allocation_run_ids = [(doc["run_id"],) for doc in allocation_run_ids]
                                                        
            self.logger.debug(f"Count of new allocation run_ids: {len(allocation_run_ids)}")
            
            if allocation_run_ids:
                allocation_summary_data = self.get_all_allocation_run_summary(allocation_run_ids)
                
                if allocation_summary_data:
                    self.delete_run_ids(allocation_run_ids)
                    is_added = self.add_allocation_summary_report(allocation_summary_data)
                    
                    if is_added:
                        self.update_api_audit_complete(allocation_run_ids)
                        return True
                return False
            else:
                self.logger.info("No new run_ids Found!")
                return True
                
        except Exception as err:
            self.logger.error(f"Error in allocation summary report: {err}")
            return False