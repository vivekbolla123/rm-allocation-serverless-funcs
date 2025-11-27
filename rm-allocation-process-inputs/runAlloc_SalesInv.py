import traceback
import json
import warnings
import os
from datetime import datetime

import pandas as pd

from Constants import *
from ownlogger import OwnLogger
from allocations import RBDAllocations

from helper.redis_helper import RedisHelper
from helper.rm_database_helper import RMDatabaseHelper

from utilities.utils import Utils
from utilities.time_utils import TimeUtils
from constants_utils import ConstantsUtils

from models.market_list_sales_row import MarketListSalesRow
from models.input_source import InputSource
from models.output_summary_sales import OutputSummarySales
from models.au_grid_row import AuGridRow

warnings.filterwarnings('ignore')

class DynAllocSalesInv:
    """
    Handles the "Sales Inventory Only" allocation logic.
    This logic creates a minimal AU Grid with allocations for W-class RBDs only.
    It can run for a single market or loop through a list of markets.
    """
    def __init__(self, inputData, currParams):
        self._initialize_params(inputData, currParams)
        # Start the Allocation based on the run mode
        if self.currParams.run_single_market:
            self.runSalesInventoryAllocationSingleMarket(0)
        else:
            self.runDynamicAllocation()

    def _initialize_params(self, inputData, currParams):
        """Initializes all necessary parameters and helper objects."""
        self.currParams = MarketListSalesRow(currParams)
        self.inputSource = InputSource(inputData)
        self.debugMode = inputData["debugMode"]
        self.NavitaireUpdateMethod = self.currParams.update_navitaire_method 
        self.RBD_QUEUE_URL = inputData["RBD_QUEUE_URL"]
        
        # self.sqs = self.currParams.sqs
        self.dwconn = self.currParams.dwconn
        self.cache_client = self.currParams.cache_client
        self.mongoclient = self.currParams.mongodbclient

        self.allocResults = []
        self.AUFileData = []
        self.utils = Utils(self.inputSource) # Utils is needed for runId generation
        self.runId = self.currParams.run_id if self.currParams.run_id else self.utils.getUniqueId()
        
        self.log_time = self.currParams.time_logger
        if self.log_time:
            self.log_time.update_run_id(self.runId)
        
        self.logger = OwnLogger(self.runId, self.inputSource)
        self.timeutils = TimeUtils(self.inputSource)
        self.constantsUtils = ConstantsUtils(self.mongoclient)
        self.rmdatabasehelper = RMDatabaseHelper(self.mongoclient, self.dwconn, self.runId, self.inputSource, self.debugMode, self.cache_client, self.currParams)
        self.redishelper = RedisHelper(self.runId, self.cache_client, self.inputSource)
        self.allocations = RBDAllocations(self.rmdatabasehelper, self.logger, self.constantsUtils, None)

        self.outputSummaryRow = OutputSummarySales()
        self.outputSummaryRow.id = self.utils.getUniqueId()
        self.outputSummaryRow.run_id = self.currParams.run_id
        self.outputSummaryRow.sector = self.currParams.sector
        self.outputSummaryRow.flight_number = str(self.currParams.flight_number)
        self.outputSummaryRow.departure_date = str(self.currParams.departure_date)
        self.outputSummaryRow.mapped_month = self.currParams.mapped_month
        self.outputSummaryRow.w4 = self.currParams.seats_sold_W4
        self.outputSummaryRow.w5 = self.currParams.seats_sold_W5
        self.outputSummaryRow.w6 = self.currParams.seats_sold_W6
        self.outputSummaryRow.hist_base_load = self.currParams.hist_base_plf
        self.outputSummaryRow.hist_curr_load = self.currParams.hist_curr_plf
        self.outputSummaryRow.curr_load = self.currParams.booked_pax
        self.rbd_list = ['W6', 'W5', 'W4']
        self.AU_COLUMN_LENGTH = self.rbd_list.__len__()

    def runDynamicAllocation(self):
        """
        Loads the market list from the database and loops through each market,
        running the sales inventory allocation for each one.
        """
        self.logger.info("Starting Dynamic Allocation for Sales Inventory")
        self.mktList = self.rmdatabasehelper.loadRunParams()
        startTime = self.timeutils.getCurrentDateTime()
        username = os.getlogin()

        self.rmdatabasehelper.insertRunStart(startTime, username)

        numMarkets = self.mktList.shape[0]
        self.logger.info(f"Market List has {numMarkets} Rows")

        for mktCounter in range(numMarkets):
            self.currParams = MarketListSalesRow(self.mktList.iloc[mktCounter])
            self.runSalesInventoryAllocationSingleMarket(mktCounter)

        self.outputResults()

        endTime = self.timeutils.getCurrentDateTime()
        self.rmdatabasehelper.updateRunEnd(endTime)
        runTime = (endTime - startTime).total_seconds()
        self.logger.info(f"Sales Inventory Allocation Completed in {runTime} seconds")

    def runSalesInventoryAllocationSingleMarket(self, mtkIndex):
        """
        Main execution function for the sales inventory logic for a single market.
        Contains conditional logic based on the saleinvflag.
        """
        self.logger.info("**********************************************")
        self.logger.info(f"Starting Sales Inventory Only Allocation for row : {mtkIndex + 1}")
        
        try:
            tgt_base_load = self.currParams.target_base_load
            adjusted_capacity = self.currParams.adjusted_capacity
            curr_load = self.currParams.booked_pax

            hist_completion_per = self.currParams.hist_curr_plf / self.currParams.hist_base_plf
            revised_curr_load_per = (curr_load + self.currParams.series_value) / adjusted_capacity
            curr_completion = revised_curr_load_per / self.currParams.target_base_load

            distress_plf_temp = (tgt_base_load/100) * hist_completion_per - revised_curr_load_per
            distress_au = round(distress_plf_temp * adjusted_capacity, 0)

            total_w_bookedload = self.currParams.seats_sold_W4 + self.currParams.seats_sold_W5 + self.currParams.seats_sold_W6
            w_threshold_overall = self.constantsUtils.SALE_AU_THRESHOLD_OVERALL
            w_threshold_per_run = self.constantsUtils.SALE_AU_THRESHOLD_PER_RUN
            sales_compl_reduction_per = self.constantsUtils.SALE_COMPL_REDUCTION_PER/100

            distress_au = min(distress_au, w_threshold_overall - total_w_bookedload)
            distress_au = min(distress_au, w_threshold_per_run)
            distress_au = distress_au + curr_load
            # rbd_list = ['W6', 'W5', 'W4']

            if distress_au > curr_load:
                self.logger.info(f"Distress Seats to Allocate for W-classes: {distress_au}")

                rec = AuGridRow(self.AU_COLUMN_LENGTH)
                rec.type = 'L'
                rec.flight = f' QP{str(self.currParams.flight_number)} {self.currParams.sector}'
                rec.start_date = self.currParams.departure_date
                rec.end_date = self.currParams.departure_date
                rec.day_of_week = 'Daily'
                rec.capacity = adjusted_capacity
                # rec.lid = curr_load

                rbd_counter = 1
                tgt_completion_per = hist_completion_per
                tgt_au_list = {}

                for rbd in reversed(self.rbd_list):
                    tgt_plf_per = tgt_completion_per * (tgt_base_load/100)
                    tgt_au = round(tgt_plf_per * adjusted_capacity, 0)
                    tgt_au_list[rbd] = tgt_au
                    
                    tgt_completion_per = tgt_completion_per - (tgt_completion_per * sales_compl_reduction_per)
                
                # au_count = 0
                new_bookedLoad = curr_load
                final_au_list = {}
                for rbd in self.rbd_list:
                    final_au = min(tgt_au_list[rbd] - new_bookedLoad, distress_au - new_bookedLoad)
                    final_au = final_au + new_bookedLoad

                    rec.set_class_attribute(rbd_counter, 'class_of_service', rbd)
                    rec.set_class_attribute(rbd_counter, 'class_au', final_au)
                    self.outputSummaryRow[rbd.lower()] = final_au - curr_load

                    new_bookedLoad = final_au
                    rbd_counter += 1
            
            rec.runid = self.outputSummaryRow.run_id
            
            self.AUFileData.append(rec)
            self.logger.info("Sales inventory allocation applied for W-classes in minimal AU grid.")

            if self.NavitaireUpdateMethod == CONST_API: self.updateNavitaire(rec)
            elif self.NavitaireUpdateMethod == CONST_SFTP: self.updateNavSFTP(rec)
            elif self.NavitaireUpdateMethod == S3: self.updateS3Push(rec)

            # Populate Summary rows
            self.outputSummaryRow.curr_completion = curr_completion
            self.outputSummaryRow.tgt_completion = tgt_base_load
            self.outputSummaryRow.booked_load = curr_load
            self.outputSummaryRow.w_booked_load = total_w_bookedload
            self.outputSummaryRow.mapped_month = self.currParams.mapped_month
            self.outputSummaryRow.hist_base_load = self.currParams.hist_base_plf
            self.outputSummaryRow.hist_curr_load = self.currParams.hist_curr_plf
            self.outputSummaryRow.curr_completion = curr_completion
            self.outputSummaryRow.tgt_completion = tgt_completion_per
            self.outputSummaryRow.final_distress = distress_au - curr_load
            
            self.populate_summary_rows(is_skipped=False)

            self.redishelper.updateSalesRunAuditReady()

        except Exception as e:
            self.logger.error("Exception occurred during Sales Inventory Allocation.")
            self.logger.error(e)
            traceback.print_exc()

    def populate_summary_rows(self):
        # Ensure Id is set for both summary rows before populating other attributes
        if not self.outputSummaryRow.id:
            self.outputSummaryRow.id = self.utils.getUniqueId()

        summary_row = [self.outputSummaryRow]
        self.allocResults.extend(summary_row)
        self.rmdatabasehelper.insertSalesSummaryRow(summary_row)

    def updateNavitaire(self, rec):
        """Sends AU data to the SQS queue for Navitaire update."""
        auData = {"runId": self.runId, "type": rec.type, "flight": rec.flight, "startDate": rec.start_date, "endDate": rec.end_date, "dayOfWeek": rec.day_of_week, "lid": rec.lid, "capacity": rec.capacity, "status": rec.status, "b2cRunId": rec.b2crunid, "b2bRunId": rec.b2brunid, "auClasses": {}}
        for i in range(self.AU_COLUMN_LENGTH):
            class_of_service = getattr(rec, f"class_of_service_{i+1}", None)
            if class_of_service:
                auClass = {"classOfService": class_of_service, "classAU": int(getattr(rec, f"class_au_{i+1}", -1))}
                auData['auClasses'][auClass['classOfService']] = auClass
        auDataStr = json.dumps(auData)
        self.sqs.send_message(QueueUrl=self.RBD_QUEUE_URL, MessageBody=auDataStr)

    def updateNavSFTP(self, rec):
        """Formats data and inserts it into the audit table for SFTP processing."""
        flightNo = rec.flight.split(" ")[1][2:7]
        sector = rec.flight.split(" ")[2]
        flightDate = datetime.strptime(str(rec.start_date), DATE_FORMAT_3).date().strftime(DATE_FORMAT_2)
        auData = {"type": rec.type, "sector1": sector, "flightNo1": flightNo, "departureDate": flightDate, "auClasses": {}}
        for i in range(self.AU_COLUMN_LENGTH):
            class_of_service = getattr(rec, f"class_of_service_{i+1}", None)
            if class_of_service:
                auData['auClasses'][class_of_service] = int(getattr(rec, f"class_au_{i+1}", -1))
        auDataStr = json.dumps(auData)
        self.rmdatabasehelper.insert_run_fltDate_audit(self.runId, flightNo, flightDate, auDataStr, "Ready", "Ready")
        
    def updateS3Push(self, rec):
        """Formats data and inserts it into the audit table for S3 push."""
        flightNo = rec.flight.split(" ")[1][2:7]
        flightDate = datetime.strptime(str(rec.start_date), DATE_FORMAT_3).date().strftime(DATE_FORMAT_2)
        auData = {"type": rec.type, "flight": rec.flight, "startDate": rec.start_date, "endDate": rec.end_date, "dayOfWeek": rec.day_of_week, "lid": rec.lid, "capacity": rec.capacity, "status": rec.status, "auClasses": {}}
        for i in range(self.AU_COLUMN_LENGTH):
            class_of_service = getattr(rec, f"class_of_service_{i+1}", None)
            if class_of_service:
                auClass = {"classOfService": class_of_service, "classAU": int(getattr(rec, f"class_au_{i+1}", -1))}
                auData['auClasses'][auClass['classOfService']] = auClass
        auDataStr = json.dumps(auData)
        self.rmdatabasehelper.insert_run_fltDate_audit(self.runId, flightNo, flightDate, auDataStr, "Ready", "Ready")

    def outputResults(self):
        """Outputs the AU Grid and Summary files."""
        if self.inputSource.output[CONST_AU_GRID] == FILE and self.AUFileData:
            AUFrame = pd.DataFrame([vars(row) for row in self.AUFileData])
            AUFrame = AUFrame.drop(columns=["b2brunid", "b2crunid", "au_column_length"], errors='ignore')
            AUFrame.to_csv(CONST_AU_GRID_SALES_FILE_PATH, index=False)

        if self.inputSource.output[CONST_SUMMARY_FILE] == FILE and self.allocResults:
            df = pd.DataFrame([vars(row) for row in self.allocResults])
            df = df.drop(columns=["Id"])
            df.to_csv(CONST_SUMMARY_SALES_FILE_PATH, index=False)