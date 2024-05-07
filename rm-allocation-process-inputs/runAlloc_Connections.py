import json
import traceback
import Constants
import os
import warnings

import pandas as pd
import time as time_sleep

from Constants import *

from helper.redis_helper import RedisHelper
from helper.rm_database_helper import RMDatabaseHelper
from helper.nav_database_helper import NavDatabaseHelper

from utilities.utils import Utils
from utilities.math_utils import MathUtils
from utilities.time_utils import TimeUtils
from utilities.constants_utils import ConstantsUtils

from models.input_source import InputSource
from models.output_summary_connections import OutputSummaryConnections
from models.au_grid_row import AuGridRow

from ownlogger import OwnLogger
from allocations import RBDAllocations
from determine_rbd import DetermineRBD

from datetime import  datetime, timedelta
from models.market_list_connections_row import MarketListConnectiosnsRow
from models.input_source import InputSource
warnings.filterwarnings('ignore')

class DynAllocConnections:

	def __init__(self, inputData, currParams):
		self._initialize_params(inputData, currParams)
		#Start the Allocation
		if (currParams["runSingleMarket"]):
			self.runDynamicAllocationSingleMarket(1)
		else:
			self.runDynamicAllocation()

	def runDynamicAllocation(self):
		print("Starting Dynamic Allocation")
		self.mktList = self.rmdatabasehelper.loadRunParams()
		startTime = self.timeutils.getCurrentDateTime()
		username = os.getlogin()
		
		self.rmdatabasehelper.insertRunStart(startTime, username)

		#The list of markets to run the allocation
		numMarkets = self.mktList.shape[0]
		print("Market List has " + str(numMarkets) + " Rows")

		if(self.debugMode):
			print("Market List : ")
			print(self.mktList)

		for mktCounter in range(numMarkets):
			self.currParams = MarketListConnectiosnsRow(self.mktList.iloc[mktCounter])
			self.runDynamicAllocationSingleMarket(mktCounter)
   
		self.outputResults()

		endTime = self.timeutils.getCurrentDateTime()
		self.rmdatabasehelper.updateRunEnd(endTime)
		runTime = (endTime - startTime).total_seconds()
		print("Allocation Completed in " + str(runTime) + " seconds")

	def runDynamicAllocationSingleMarket(self, mtkIndex):
		print("**********************************************")
		print("Starting Allocation for row : " + str(mtkIndex))

		if(self.debugMode):
			print(self.currParams)
		
		#Convert NDO to run dates as necessary
		currDate = self.timeutils.getCurrentDate()
		startDate = currDate
		endDate = currDate
		startNDO = self.currParams.per_start
		endNDO = self.currParams.per_end
		sector1 = self.currParams.sector1
		sector2 = self.currParams.sector2
		flightNo2 = self.currParams.flight2
		flightNo1=self.currParams.flight1

		startDate, endDate, startNDO, endNDO = self.getStartEndDate(currDate, startNDO, endNDO)
  
		print("Allocation for " + sector1 +" "+ sector2 + " from " + str(startDate) + " to " + str(endDate))
		self.wait_for_inputs()
		if self.inputSource.input_source[CONST_BOOKLOAD] == TEST_DATA:
			bookedLoad_sector1 = self.rmdatabasehelper.getCurrentBookedLoads(sector1[:3], sector1[-3:], startDate, endDate)
			bookedLoad_sector2 = self.rmdatabasehelper.getCurrentBookedLoads(sector2[:3], sector2[-3:], startDate, endDate)
		if self.inputSource.input_source[CONST_BOOKLOAD] == DB:
			bookedLoad_sector1 = self.navdatabasehelper.getCurrentBookedLoads(sector1[:3], sector1[-3:], startDate, endDate)
			bookedLoad_sector2 = self.navdatabasehelper.getCurrentBookedLoads(sector2[:3], sector2[-3:], startDate, endDate)

		

		try:
			for ndocounter in range(startNDO, endNDO+1):
				self.outputSummaryRow = OutputSummaryConnections()
				currDate = self.timeutils.getCurrentDate()
				tgtDate = datetime.strftime(currDate + timedelta(days=ndocounter),DATE_FORMAT_2)
    
				bookedLoad_sector1['DepartureDate'] = pd.to_datetime(bookedLoad_sector1['DepartureDate']).dt.strftime('%Y-%m-%d')
    
				bookedLoad_sector2['DepartureDate'] = pd.to_datetime(bookedLoad_sector2['DepartureDate']).dt.strftime('%Y-%m-%d')
    
				c1 = bookedLoad_sector1['DepartureDate'].eq(str(tgtDate))
				c2 = bookedLoad_sector1['FlightNumber'].eq(str(flightNo1))
    
				bookedLoadSingleRowSector1 = bookedLoad_sector1[c1 & c2]
    
				c1 = bookedLoad_sector2['DepartureDate'].eq(str(tgtDate))
				c2 = bookedLoad_sector2['FlightNumber'].eq(str(flightNo2))
    
				bookedLoadSingleRowSector2 = bookedLoad_sector2[c1&c2]
    
				if(bookedLoadSingleRowSector1.shape[0] == 0 or bookedLoadSingleRowSector2.shape[0] == 0):
					dateFormatted = datetime.strptime(str(startDate),DATE_FORMAT_2).date().strftime(DATE_FORMAT)
					print("Unable to get the Booked Load - Aborting")
					self.outputSummaryRow.Id = self.utils.getUniqueId()
					self.outputSummaryRow.RunId = self.runId
					self.outputSummaryRow.Sector1=sector1
					self.outputSummaryRow.Sector2=sector2
					self.outputSummaryRow.FltNum1=str(flightNo1)
					self.outputSummaryRow.FltNum2=str(flightNo2)
					self.outputSummaryRow.DepDate=str(dateFormatted)
					self.outputSummaryRow.HowDetermined = "unable to get Booked loads"
					self.outputSummaryRow.analystName=self.currParams.analyst_name
					self.allocResults.append(self.outputSummaryRow)
					self.outputSummaryRow.allocationStatus = 0
					self.redishelper.updateRunAudit()
					self.rmdatabasehelper.insertSummaryRow_connections(self.allocResults)
					break
				else:
					self.allocateAUforflights(tgtDate, bookedLoadSingleRowSector1, bookedLoadSingleRowSector2, ndocounter)
				
		except Exception as e:
			print("Exception occurred")
			traceback.print_exc()
			print(e)

		if not self.inputSource.isUpdateNavitaire:
			self.outputResults()

		if (self.inputSource.isUpdateNavitaire) and (self.NavitaireUpdateMethod==Constants.CONST_SFTP):
			update_query = " UPDATE run_flight_date_audit SET status = %s WHERE runId = %s and status = 'Pending' "
			self.wrconn.execute(update_query, "Ready", self.runId)

	def allocateAUforflights(self, current_date, bookedLoadSingleRowSector1, bookedLoadSingleRowSector2, ndocounter):
		self.outputSummaryRow_b2b = OutputSummaryConnections()
		self.outputSummaryRow_b2c = OutputSummaryConnections()
		self.outputSummaryRow_b2c.Bookedload1=bookedLoadSingleRowSector1["bookedPax"].values[0]
		self.outputSummaryRow_b2c.Bookedload2=bookedLoadSingleRowSector2["bookedPax"].values[0]
		self.outputSummaryRow_b2b.Bookedload1=bookedLoadSingleRowSector1["bookedPax"].values[0]
		self.outputSummaryRow_b2b.Bookedload2=bookedLoadSingleRowSector2["bookedPax"].values[0]
		sector1 = self.currParams.sector1
		sector2 = self.currParams.sector2
		flight1 = self.currParams.flight1
		flight2 = self.currParams.flight2
		dow = self.currParams.dow
		B2BBackstop = self.currParams.b2b_backstop
		B2CBackstop = self.currParams.b2c_backstop
		priceStrategy = self.currParams.pricestartegy
		discountValue = self.currParams.discountvalue
		SkippingFactor = self.currParams.skipping_factor
		b2bRunId = self.utils.getUniqueId()
		b2cRunId = self.utils.getUniqueId()

		print("Determining pricing for NDO " + str(ndocounter) + " on  " + str(current_date))
		dow = int(datetime.strftime(datetime.strptime(current_date, '%Y-%m-%d') + timedelta(days=ndocounter),"%w"))
		
		if (str(self.currParams.dow)[dow:(dow+1)] == "1" and bookedLoadSingleRowSector1['FlightNumber'].values[0] == str(flight1) and bookedLoadSingleRowSector2['FlightNumber'].values[0] == str(flight2)):
			capacity = min(bookedLoadSingleRowSector1['capacity'].values[0], bookedLoadSingleRowSector2['capacity'].values[0])
			bookload = max(bookedLoadSingleRowSector1['bookedPax'].values[0],bookedLoadSingleRowSector2['bookedPax'].values[0])
			print("Combined Booked Load for " + bookedLoadSingleRowSector1['FlightNumber'].values[0]+" "+str(bookedLoadSingleRowSector2['FlightNumber'].values[0]) + " on " + str(current_date) + " is :" + str(bookload))
			if self.inputSource.input_source[CONST_BOOKLOAD] == TEST_DATA:
				b2b_rbd_sector1 = self.rmdatabasehelper.getcurrentRBD(current_date, flight1, "B2B", sector1,bookedLoadSingleRowSector1['bookedPax'].values[0])
				b2c_rbd_sector1 = self.rmdatabasehelper.getcurrentRBD(current_date, flight1, "B2C", sector1,bookedLoadSingleRowSector1['bookedPax'].values[0])
				b2b_rbd_sector2 = self.rmdatabasehelper.getcurrentRBD(current_date, flight2, "B2B", sector2,bookedLoadSingleRowSector2['bookedPax'].values[0])
				b2c_rbd_sector2 = self.rmdatabasehelper.getcurrentRBD(current_date, flight2, "B2C", sector2,bookedLoadSingleRowSector2['bookedPax'].values[0])
			if self.inputSource.input_source[CONST_BOOKLOAD] == DB:
				b2b_rbd_sector1 = self.navdatabasehelper.getcurrentRBD(current_date, flight1, "B2B", sector1,bookedLoadSingleRowSector1['bookedPax'].values[0])
				b2c_rbd_sector1 = self.navdatabasehelper.getcurrentRBD(current_date, flight1, "B2C", sector1,bookedLoadSingleRowSector1['bookedPax'].values[0])
				b2b_rbd_sector2 = self.navdatabasehelper.getcurrentRBD(current_date, flight2, "B2B", sector2,bookedLoadSingleRowSector2['bookedPax'].values[0])
				b2c_rbd_sector2 = self.navdatabasehelper.getcurrentRBD(current_date, flight2, "B2C", sector2,bookedLoadSingleRowSector2['bookedPax'].values[0])
		
			if (b2c_rbd_sector1.shape[0] == 0 or b2c_rbd_sector2.shape[0] == 0):
				# unable to get bookload aborting
				self.outputSummaryRow_b2c.Backstop=B2CBackstop
				self.outputSummaryRow_b2b.Backstop=B2BBackstop
				openDataB2C = {'RBD': ['NA'], 'Fare': ['NA'], 'FareResult': ['B2C Data NA - Skipping allocation'], 'anchorFare': ['NA'], 'statusInd': [1], 'allocationStatus': [0]}
				openDataB2B = {'RBD': ['NA'], 'Fare': ['NA'], 'FareResult': ['B2C Data NA - Skipping allocation'], 'anchorFare': ['NA'], 'statusInd': [1], 'allocationStatus': [0]}
				if b2c_rbd_sector1.shape[0] != 0:
					self.outputSummaryRow_b2c.OpenRBD1=b2c_rbd_sector1["ClassOfService"].values[0]
					self.outputSummaryRow_b2c.SellingFare1=self.rmdatabasehelper.getSellingPrice(b2c_rbd_sector1["ClassOfService"].values[0],sector1,"B2C","L")
					if self.outputSummaryRow_b2c.SellingFare1==0:
						openDataB2C['FareResult']="QP Fares Not Available"
				if b2c_rbd_sector2.shape[0] != 0:
					self.outputSummaryRow_b2c.OpenRBD2=b2c_rbd_sector2["ClassOfService"].values[0]
					self.outputSummaryRow_b2c.SellingFare2=self.rmdatabasehelper.getSellingPrice(b2c_rbd_sector2["ClassOfService"].values[0],sector2,"B2C","L")
					if self.outputSummaryRow_b2c.SellingFare2==0:
						openDataB2C['FareResult']="QP Fares Not Available"
			else:
				self.outputSummaryRow_b2c.OpenRBD1=b2c_rbd_sector1["ClassOfService"].values[0]
				self.outputSummaryRow_b2c.OpenRBD2=b2c_rbd_sector2["ClassOfService"].values[0]
				b2cSectorTotal,self.outputSummaryRow_b2c.SellingFare1,self.outputSummaryRow_b2c.SellingFare2 = self.rmdatabasehelper.getFinalPrice(b2c_rbd_sector1, b2c_rbd_sector2, sector1, sector2, "B2C")
				origin = sector1[:3]
				destin = sector2[-3:]
				b2cTotal,b2cdiscount = self.getTotalPriceForConnections(priceStrategy, discountValue, b2cSectorTotal)
				self.outputSummaryRow_b2c.Discount=b2cdiscount
				openDataB2C = self.determineRBDToOpen(b2cTotal, origin, destin, "B2C", B2CBackstop)
				if (b2b_rbd_sector1.shape[0] == 0 or b2b_rbd_sector2.shape[0] == 0):
					# unable to get bookload aborting
					openDataB2B = {'RBD': ['NA'], 'Fare': ['NA'], 'FareResult': ['B2B Data NA - Skipping allocation'], 'anchorFare': ['NA'], 'statusInd': [1], 'allocationStatus': [0]}
					if b2b_rbd_sector1.shape[0] != 0:
						self.outputSummaryRow_b2b.OpenRBD1=b2b_rbd_sector1["ClassOfService"].values[0]
						self.outputSummaryRow_b2b.SellingFare1=self.rmdatabasehelper.getSellingPrice(b2b_rbd_sector1["ClassOfService"].values[0],sector1,"B2B","L")
						if self.outputSummaryRow_b2b.SellingFare1==0:
							openDataB2B['FareResult']="QP Fares Not Available"
					if b2b_rbd_sector2.shape[0] != 0:
						self.outputSummaryRow_b2b.OpenRBD2=b2b_rbd_sector2["ClassOfService"].values[0]
						self.outputSummaryRow_b2b.SellingFare2=self.rmdatabasehelper.getFinalPrice(b2b_rbd_sector2["ClassOfService"].values[0],sector2,"B2B","L")
						if self.outputSummaryRow_b2b.SellingFare2==0:
							openDataB2B['FareResult']="QP Fares Not Available"
				else:
					self.outputSummaryRow_b2b.OpenRBD1=b2b_rbd_sector1["ClassOfService"].values[0]
					self.outputSummaryRow_b2b.OpenRBD2=b2b_rbd_sector2["ClassOfService"].values[0]
					if (b2cTotal == 0):
						openDataB2C = {'RBD': ['NA'], 'Fare': ['NA'], 'FareResult': ['B2C Fares NA - Skipping allocation'], 'anchorFare': ['NA'], 'statusInd': [1], 'allocationStatus': [0]}
						openDataB2B = {'RBD': ['NA'], 'Fare': ['NA'], 'FareResult': ['B2B Fares NA - Skipping allocation'], 'anchorFare': ['NA'], 'statusInd': [1], 'allocationStatus': [0]}
					else:
						b2bSectorTotal,self.outputSummaryRow_b2b.SellingFare1,self.outputSummaryRow_b2b.SellingFare2 = self.rmdatabasehelper.getFinalPrice(b2b_rbd_sector1, b2b_rbd_sector2, sector1, sector2, "B2B")
						b2bTotal,b2bdiscount = self.getTotalPriceForConnections(priceStrategy, discountValue, b2bSectorTotal)
						self.outputSummaryRow_b2b.Discount=b2bdiscount
						openDataB2B = self.determineRBDToOpen(b2bTotal, origin, destin, "B2B", B2BBackstop)

			if (openDataB2C['statusInd'][0] != 1):
				# Legit Allocation - Write to AU File
				currRec = AuGridRow(self.AU_COLUMN_LENGTH_CONNECTIONS)
				currRec.type="R"  # Type
				currRec.flight=' QP' + str(flight1) + ' ' + sector1+'/'+' QP' + str(flight2) + ' ' + sector2
				currRec.start_date=datetime.strftime(datetime.strptime(current_date, '%Y-%m-%d'), '%m-%d-%Y')
				currRec.end_date=datetime.strftime(datetime.strptime(current_date, '%Y-%m-%d'), '%m-%d-%Y')
				currRec.day_of_week='Daily'
				# Condition Added since we dont need to update the AU Grid in Local but we need the Run Id's when we update Navitare via Server Run
				if self.inputSource.isUpdateNavitaire:
					currRec.b2crunid = self.outputSummaryRow_b2c.Id
					currRec.b2brunid = self.outputSummaryRow_b2b.Id

				# First - Adding all B2C RBD's
				b2cAllocationsUnits,brd_counter = self.allocations.b2CAllocation_connections(openDataB2C, currRec, capacity, bookload, SkippingFactor,self.currParams)

				# Now add all the Z RBDs
				brd_counter,currRec=self.allocations.b2BAllocation_connections(openDataB2B, currRec, b2cAllocationsUnits,brd_counter,self.currParams)

				self.AUFileData.append(currRec)
				if self.inputSource.isUpdateNavitaire:
					self.redishelper.setAllRunStatus2Ready()
					self.updateNavSFTP(currRec)
			current_date = datetime.strptime(str(current_date),DATE_FORMAT_2).date().strftime(DATE_FORMAT)
			self.outputSummaryRow_b2c.Id=b2cRunId
			self.outputSummaryRow_b2c.RunId=self.runId
			self.outputSummaryRow_b2c.Sector1=sector1
			self.outputSummaryRow_b2c.Sector2=sector2
			self.outputSummaryRow_b2c.FltNum1=str(flight1)
			self.outputSummaryRow_b2c.FltNum2=str(flight2)
			self.outputSummaryRow_b2c.DepDate=str(current_date)
			self.outputSummaryRow_b2c.BookedLoad=str(bookload)
			self.outputSummaryRow_b2c.allocationStatus=str(openDataB2C["allocationStatus"][0])
			self.outputSummaryRow_b2c.anchorFare=str(openDataB2C["anchorFare"][0])
			self.outputSummaryRow_b2c.OpenRBD=str(openDataB2C['RBD'][0])
			self.outputSummaryRow_b2c.SellingFare=str(openDataB2C["Fare"][0])
			self.outputSummaryRow_b2c.Channel='B2C'
			self.outputSummaryRow_b2c.HowDetermined=str(openDataB2C['FareResult'][0])
			self.outputSummaryRow_b2c.Backstop=B2CBackstop
			self.outputSummaryRow_b2c.analystName=self.currParams.analyst_name
			self.allocResults.append(self.outputSummaryRow_b2c)
			self.rmdatabasehelper.insertSummaryRow_connections(self.allocResults)
			
			self.outputSummaryRow_b2b.Id=b2bRunId
			self.outputSummaryRow_b2b.RunId=self.runId
			self.outputSummaryRow_b2b.Sector1=sector1
			self.outputSummaryRow_b2b.Sector2=sector2
			self.outputSummaryRow_b2b.FltNum1=str(flight1)
			self.outputSummaryRow_b2b.FltNum2=str(flight2)
			self.outputSummaryRow_b2b.DepDate=str(current_date)
			self.outputSummaryRow_b2b.BookedLoad=str(bookload)
			self.outputSummaryRow_b2b.allocationStatus=str(openDataB2B["allocationStatus"][0])
			self.outputSummaryRow_b2b.anchorFare=str(openDataB2B["anchorFare"][0])
			self.outputSummaryRow_b2b.OpenRBD=str(openDataB2B['RBD'][0])
			self.outputSummaryRow_b2b.SellingFare=str(openDataB2B["Fare"][0])
			self.outputSummaryRow_b2b.Channel='B2B'
			self.outputSummaryRow_b2b.HowDetermined=str(openDataB2B['FareResult'][0])
			self.outputSummaryRow_b2b.Backstop=B2BBackstop
			self.outputSummaryRow_b2b.analystName=self.currParams.analyst_name
			self.allocResults.append(self.outputSummaryRow_b2b)
			self.rmdatabasehelper.insertSummaryRow_connections(self.allocResults)
		else:
			print("Run Parameters requested to skip - (FlightNumber, DOW, or Status)")

	def getTotalPriceForConnections(self, priceStrategy, discountValue, sectorTotal):
		if (priceStrategy == 1):
			discount=discountValue
			total = (sectorTotal) - discount
		if (priceStrategy == 2):
			discount=(sectorTotal) * (discountValue / 100)
			total = (sectorTotal) - discount
		return total,discount

	def updateNavSFTP(self, rec):
		# QP1111 BOMHYD/ QP2222 HYDBLR
		parts = rec.flight.replace('/', '').strip().split()
		flightNo1 = parts[0][2:]
		flightNo2 = parts[2][2:]
		sector1 = parts[1]
		sector2 = parts[3]
		flightDate = datetime.strptime(str(rec.start_date), "%m-%d-%Y").date().strftime(Constants.DATE_FORMAT_2)
		auData = {"type": rec.type, "sector1": sector1, "flightNo1" : flightNo1, "sector2": sector2, "flightNo2" : flightNo2, "departureDate": flightDate, "auClasses": {}}
		auClass={}
		
		for i in range(self.AU_COLUMN_LENGTH_CONNECTIONS):
			auClass[getattr(rec, f"class_of_service_{i+1}", None)] = int(getattr(rec, f"class_au_{i+1}", None))
  
		auData['auClasses'] = auClass
		auDataStr = json.dumps(auData)
		self.rmdatabasehelper.insert_run_fltDate_audit(self.runId, flightNo1+"/"+flightNo2, flightDate, auDataStr, "Ready", "Ready")

	def determineRBDToOpen(self, finalprice, origin, destin, channel, backstop):
		anchorFare = finalprice
		openResult = pd.DataFrame(columns=['RBD', 'Fare', 'FareResult', 'anchorFare', 'statusInd','allocationStatus'])
		offset = 1
		print("Anchor Fare is " + str(anchorFare))
		openResult, rbddata = self.rmdatabasehelper.getValueWithOffset(origin, destin, anchorFare, offset, channel, openResult, 'R')

		if(rbddata.shape[0] == 0):
			openResult = self.determinerbd.extremeFaresSkipping(origin, destin, anchorFare, offset, channel, backstop, openResult, 'R')
		else:
			howDetermined = 'Matched and allocated'
			openResult = self.determinerbd.getFaresWithBackstop(origin, destin, anchorFare, offset, channel, backstop,  openResult, rbddata, howDetermined, 'R')
		return openResult

   
	def outputResults(self):
		if self.inputSource.output[CONST_AU_GRID] == FILE and len(self.AUFileData)!=0:
			AUlist = ['Type', 'Flight', 'StartDate', 'EndDate', 'DayOfWeek', 'Aircraft Type', 'Aircraft Suffix', 'Lid', 'Capacity', 'Status']
			length = Constants.AU_COLUMN_LENGTH_CONNECTIONS
			for i in range(length):
				AUlist.append(f'Class Of Service AU Class {i+1}')
				AUlist.append(f'Class Type AU Class {i+1}')
				AUlist.append(f'Class Nest AU Class {i+1}')
				AUlist.append(f'Class Rank AU Class {i+1}')
				AUlist.append(f'Class AU AU Class {i+1}')
				AUlist.append(f'Class Allotted AU Class {i+1}')
				AUlist.append(f'AP Restriction AU Class {i+1}')
			
			AUFrame = pd.DataFrame([vars(row) for row in self.AUFileData])
			AUFrame = AUFrame.drop(columns=["b2brunid", "b2crunid", "au_column_length"])
			AUFrame.columns = AUlist
			AUFrame.to_csv(CONST_AU_GRID_CONNECTIONS_FILE_PATH, index=False)
		if self.inputSource.output[CONST_SUMMARY_FILE] == FILE:
			df = pd.DataFrame([vars(row) for row in self.allocResults])
			df = df.drop(columns=["Id"])
			df.to_csv(CONST_SUMMARY_CONNECTIONS_FILE_PATH, index=False)

	def getStartEndDate(self, currDate, startNDO, endNDO):
		startDate = datetime.strptime(startNDO, Constants.DATE_FORMAT).date()
		endDate = datetime.strptime(endNDO, Constants.DATE_FORMAT).date()

		# Reset the startNDO and endNDO to actually NDO
		startNDO = (startDate - currDate).days
		endNDO = (endDate - currDate).days

		return startDate, endDate, startNDO, endNDO
	

	def _initialize_params(self, inputData, currParams):
		self.currParams=MarketListConnectiosnsRow(currParams)
		self.inputSource=InputSource(inputData)
		self.debugMode = inputData["debugMode"]
		self.NavitaireUpdateMethod = self.currParams.navitaire_update_method
		self.cache_client = self.currParams.cache_client
		self.NAV_DB_NAME = inputData["NAV_DB_NAME"]
		self.NAV_DB_DW_NAME = inputData["NAV_DB_DW_NAME"]
		self.NAV_DB_WB_NAME = inputData["NAV_DB_WB_NAME"]
		self.conn = self.currParams.dbconn
		self.wrconn = self.currParams.wrconn
		self.NavitaireConnection = self.currParams.navitaire_connection
		self.starttime = datetime.now()
		self.allocResults = []
		self.AUFileData = []
		if self.currParams.run_id:
			self.runId = self.currParams.run_id
		else:
			self.runId = self.utils.getUniqueId()
		self.redishelper = RedisHelper(
			self.runId, self.cache_client, self.inputSource)
		self.logger = OwnLogger(self.runId, self.inputSource)
		self.rmdatabasehelper = RMDatabaseHelper(self.conn, self.wrconn, self.runId, self.inputSource, self.debugMode, self.cache_client,self.currParams)
		self.constantsUtils = ConstantsUtils(self.rmdatabasehelper)
		self.utils = Utils(self.inputSource)
		self.mathutils = MathUtils(self.inputSource)
		self.timeutils = TimeUtils(self.inputSource)
		self.navdatabasehelper = NavDatabaseHelper(self.NavitaireConnection, self.conn, self.inputSource, self.NAV_DB_NAME, self.runId, self.debugMode, self.NAV_DB_WB_NAME,self.currParams)
		self.determinerbd = DetermineRBD(self.rmdatabasehelper, self.logger, self.inputSource, self.constantsUtils, self.navdatabasehelper, self.mathutils)
		self.allocations = RBDAllocations(self.rmdatabasehelper, self.logger, self.constantsUtils, self.mathutils)
		self.AU_COLUMN_LENGTH_CONNECTIONS = self.constantsUtils.AU_COLUMN_LENGTH_CONNECTIONS
	
		if inputData["latestCode"]:
			code_version = self.rmdatabasehelper.getLatestCodeVersion()
			if code_version != Constants.LATEST_VERSION:
				self.logger.info("Please use latest code! This is an old version!")
				exit()
	def wait_for_inputs(self):
		"""Waits until all inputs are ready."""
		can_copy = False
		while can_copy == False:
			can_copy = self.rmdatabasehelper.areAllInputsReady()
			if can_copy == False:
				self.logger.info('Waiting as inputs are busy...')
				time_sleep.sleep(WAIT_TIME)

	