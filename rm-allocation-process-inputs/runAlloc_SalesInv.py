import traceback
import json
import warnings
import os
from datetime import datetime

import pandas as pd

from Constants import *
from ownlogger import OwnLogger
from allocations import RBDAllocations
from common.compute_utils import safe_round_fare, SUMMARY_FARE_COLUMNS

from helper.redis_helper import RedisHelper
from helper.rm_database_helper import RMDatabaseHelper

from utilities.utils import Utils
from utilities.time_utils import TimeUtils
from constants_utils import ConstantsUtils

from models.market_list_row import MarketListRow
from models.input_source import InputSource
from models.output_summary import OutputSummary
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
        self.currParams = MarketListRow(currParams)
        self.inputSource = InputSource(inputData)
        self.debugMode = inputData["debugMode"]
        self.NavitaireUpdateMethod = self.currParams.navitaire_update_method
        self.RBD_QUEUE_URL = inputData["RBD_QUEUE_URL"]
        
        self.sqs = self.currParams.sqs
        self.dwconn = self.currParams.dwconn
        self.cache_client = self.currParams.cache_client
        self.mongoclient = self.currParams.mongodbclient
        self.mongo_master_db = self.currParams.mongo_master_db

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

        self.AU_COLUMN_LENGTH = 4

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
            self.currParams = MarketListRow(self.mktList.iloc[mktCounter])
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
            # If saleinvflag is '1', proceed with normal allocation logic.
            if str(self.currParams.saleflag) == '1':
                self.logger.info(f"saleinvflag is '1'. Proceeding with Sales Inventory allocation.")
                
                currFltDate = self.currParams.per_start
                currFltDate_dt = datetime.strptime(currFltDate, DATE_FORMAT)
                dateFormatted_yyyymmdd = currFltDate_dt.strftime(DATE_FORMAT_2)
                
                currBookedLoad = float(self.currParams.bookedPax) if pd.notna(self.currParams.bookedPax) else 0.0
                currAdjustedCapacity = float(self.currParams.adjustedCapacity) if pd.notna(self.currParams.adjustedCapacity) else 0.0
                
                startOfDayTgtList = self.rmdatabasehelper.getTargetBookedLoadFromBenchmarkCurve(0, self.currParams.curve_id)
                startOfDayBookLoad = float(startOfDayTgtList['LF'][0]) if not startOfDayTgtList.empty else 0.0
                startOfDayTgtBookedLoad_abs = startOfDayBookLoad * currAdjustedCapacity
                distress_pax_count = round(startOfDayTgtBookedLoad_abs - currBookedLoad)

                sales_inv_target_rbd_names = ['W7', 'W6', 'W5', 'W4']
                
                currRec = AuGridRow(self.AU_COLUMN_LENGTH)
                currRec.type = 'L'
                currRec.flight = f' QP{str(self.currParams.flight_number)} {self.currParams.origin}{self.currParams.destin}'
                currRec.start_date = currFltDate_dt.strftime(DATE_FORMAT_3)
                currRec.end_date = currFltDate_dt.strftime(DATE_FORMAT_3)
                currRec.day_of_week = 'Daily'
                currRec.capacity = currAdjustedCapacity
                currRec.lid = currBookedLoad

                self.outputSummaryRow_b2c = OutputSummary()
                self.outputSummaryRow_b2b = OutputSummary()
                self.outputSummaryRow_b2c.Id = self.utils.getUniqueId()
                self.outputSummaryRow_b2b.Id = self.utils.getUniqueId()

                if self.inputSource.isUpdateNavitaire and self.NavitaireUpdateMethod != S3:
                    currRec.b2crunid = self.outputSummaryRow_b2c.Id
                    currRec.b2brunid = self.outputSummaryRow_b2b.Id
                
                # Perform the allocation for W-classes
                # Passing is_sale_active=True as requested. Assumes allocations.py is updated to handle this.
                self.allocations.sales_inv_allocation(
                    distress_pax_count, 1, currRec, sales_inv_target_rbd_names, 0,
                    dateFormatted_yyyymmdd, self.currParams.origin, self.currParams.destin, str(self.currParams.saleflag)
                )

                self.AUFileData.append(currRec)
                self.logger.info("Sales inventory allocation applied for W-classes in minimal AU grid.")
                
                if self.inputSource.isUpdateNavitaire:
                    if self.NavitaireUpdateMethod == CONST_API: self.updateNavitaire(currRec)
                    elif self.NavitaireUpdateMethod == CONST_SFTP: self.updateNavSFTP(currRec)
                    elif self.NavitaireUpdateMethod == S3: self.updateS3Push(currRec)

                # Populate Summary rows for successful allocation
                self.populate_summary_rows(is_skipped=False)

            else:
                # If saleinvflag is not '1', skip allocation and create a "skipped" summary record.
                self.logger.info(f"saleinvflag is not '1'. Skipping allocation and creating summary record.")
                self.outputSummaryRow_b2c = OutputSummary()
                self.outputSummaryRow_b2b = OutputSummary()
                self.populate_summary_rows(is_skipped=True)

            self.redishelper.updateRunAuditReady()

        except Exception as e:
            self.logger.error("Exception occurred during Sales Inventory Allocation.")
            self.logger.error(e)
            traceback.print_exc()

    def populate_summary_rows(self, is_skipped):
        """Populates the B2C and B2B summary rows for the final report."""
        how_determined_text = ""
        allocation_status = 0
        
        if is_skipped:
            how_determined_text = "Sales Inventory Only - Skipping allocation as saleinvflag is not 1"
            allocation_status = 0
        else:
            how_determined_text = "Sales Inventory Only (Minimal AU Grid - W classes only)"
            allocation_status = 1
            
        common_attrs = {
            "RunId": self.runId, "Origin": self.currParams.origin, "Destin": self.currParams.destin,
            "FltNum": str(self.currParams.flight_number), "DepDate": str(self.currParams.per_start),
            "BookedLoad": str(self.currParams.bookedPax), "TgtLoad": CONST_NA,
            "HowDetermined": how_determined_text,
            "OpenRBD": CONST_NA, "SellingFare": CONST_NA, "anchorFare": CONST_NA,
            "analystName": str(self.currParams.analyst_name), "allocationStatus": allocation_status,
            "criteria": CONST_NA, "fareOffset": CONST_NA, "profileFare": CONST_NA,
            "Variance": CONST_NA, "bookedPlf": CONST_NA
        }
        
        # Ensure Id is set for both summary rows before populating other attributes
        if not self.outputSummaryRow_b2c.Id:
            self.outputSummaryRow_b2c.Id = self.utils.getUniqueId()
        if not self.outputSummaryRow_b2b.Id:
            self.outputSummaryRow_b2b.Id = self.utils.getUniqueId()

        for attr, value in common_attrs.items():
            setattr(self.outputSummaryRow_b2c, attr, value)
            setattr(self.outputSummaryRow_b2b, attr, value)
        
        self.outputSummaryRow_b2c.Channel = B2C_CHANNEL
        self.outputSummaryRow_b2b.Channel = B2B_CHANNEL
        
        summary_rows = [self.outputSummaryRow_b2c, self.outputSummaryRow_b2b]
        self.allocResults.extend(summary_rows)
        self.rmdatabasehelper.insertSummaryRow([self.outputSummaryRow_b2c])
        self.rmdatabasehelper.insertSummaryRow([self.outputSummaryRow_b2b])
        

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
            for col in SUMMARY_FARE_COLUMNS:
                if col in df.columns:
                    df[col] = df[col].apply(safe_round_fare)
            df.to_csv(CONST_SUMMARY_SALES_FILE_PATH, index=False)