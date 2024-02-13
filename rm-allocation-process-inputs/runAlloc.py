import traceback
import pandas as pd
import math
import os
import time as time_sleep
import json
import warnings
from Constants import *
from datetime import timedelta
from datetime import datetime
from ownlogger import OwnLogger
from helper.nav_api_helper import NavAPIHelper
from helper.rm_database_helper import RMDatabaseHelper
from utilities.math_utils import MathUtils
from utilities.time_utils import TimeUtils
from utilities.utils import Utils
from helper.nav_database_helper import NavDatabaseHelper
from helper.redis_helper import RedisHelper
from get_dynamic_time_d2 import GetDynamicTimeD2
from market_fares_service import MarketFareService
from determine_rbd import DetermineRBD
from models.market_list_row import MarketListRow
from utilities.constants_utils import ConstantsUtils
from models.input_source import InputSource
from models.output_summary import OutputSummary
from models.au_grid_row import AuGridRow
warnings.filterwarnings('ignore')

class DynAlloc:

	def __init__(self, inputData, currParams):
		self._initialize_params(inputData, currParams)
		
		#Start the Allocation
		if (self.currParams.run_single_market):
			self.runDynamicAllocationSingleMarket(1)
		else:
			self.runDynamicAllocation()

	def runDynamicAllocation(self):
		self.logger.info("Starting Dynamic Allocation")
		#Load the Run Parameters
		self.mktList=self.rmdatabasehelper.loadRunParams()
		startTime = self.timeutils.getCurrentDateTime()
		username = os.getlogin()
		
		self.rmdatabasehelper.insertRunStart(startTime, username)

		#The list of markets to run the allocation
		numMarkets = self.mktList.shape[0]
		self.logger.info("Market List has " + str(numMarkets) + " Rows")

		if(self.debugMode):
			self.logger.info("Market List : ")
			self.logger.info(self.mktList)

		for mktCounter in range(numMarkets):
			self.currParams=MarketListRow(self.mktList.iloc[mktCounter])
			self.runDynamicAllocationSingleMarket( mktCounter)

		self.outputResults()

		endTime = self.timeutils.getCurrentDateTime()
		self.rmdatabasehelper.updateRunEnd(endTime)
		runTime = (endTime - startTime).total_seconds()
		self.logger.info("Allocation Completed in " + str(runTime) + " seconds")

	def runDynamicAllocationSingleMarket(self,mtkIndex):
		
		self.logger.info("**********************************************")
		self.logger.info("Starting Allocation for row : " + str(mtkIndex+1))

		if(self.debugMode):
			self.logger.info(self.currParams)
		
		#Convert NDO to run dates as necessary
		currDate = self.timeutils.getCurrentDate()

		origin = self.currParams.origin
		destination = self.currParams.destin
		startDate = currDate
		endDate = currDate
		startNDO = self.currParams.per_start
		endNDO =self.currParams.per_end
		curveId = self.currParams.curve_id
		analystName = self.currParams.analyst_name
		FlightNumber = str(self.currParams.flight_number)
		self.currParams.strategy =self.currParams.fare_anchor

		startDate, endDate, startNDO, endNDO = self.getStartEndDate(currDate, startNDO, endNDO)
		
		self.logger.info("Allocation for " + origin + destination + " from " + str(startDate) + " to " + str(endDate))
		
		self.wait_for_inputs()
    
		if self.inputSource.input_source[CONST_BOOKLOAD] == TEST_DATA:
			bookedLoad = self.rmdatabasehelper.getCurrentBookedLoads(origin, destination, startDate, endDate)
		if self.inputSource.input_source[CONST_BOOKLOAD] == DB:
			bookedLoad = self.navdatabasehelper.getCurrentBookedLoads(origin, destination, startDate, endDate)
			
		try: 
			# For each NDO
			for ndocounter in range(startNDO, endNDO+1):
				self.outputSummaryRow=OutputSummary()
				currDate = self.timeutils.getCurrentDate()
				tgtDate = datetime.strftime(currDate + timedelta(days=ndocounter),DATE_FORMAT_2)
				if(bookedLoad.shape[0] == 0):
					dateFormatted = datetime.strptime(str(startDate), DATE_FORMAT_2).date().strftime(DATE_FORMAT)
					self.logger.info("Unable to get the Booked Load - Aborting")
					self.outputSummaryRow.Id = self.utils.getUniqueId() 
					self.outputSummaryRow.RunId = self.runId
					self.outputSummaryRow.Origin = origin
					self.outputSummaryRow.Destin = destination
					self.outputSummaryRow.FltNum = str(FlightNumber)
					self.outputSummaryRow.DepDate = str(dateFormatted) 
					self.outputSummaryRow.HowDetermined = "unable to get Booked loads"
					self.outputSummaryRow.analystName = analystName
					self.outputSummaryRow.allocationStatus = 0
					self.allocResults.append(self.outputSummaryRow)
					self.redishelper.updateRunAudit()
					self.rmdatabasehelper.insertSummaryRow(self.allocResults)
					break
				bookedLoad['DepartureDate'] = pd.to_datetime(bookedLoad['DepartureDate'])
				if(FlightNumber == '*'):
					bookedLoadSingleRow = bookedLoad[(bookedLoad['DepartureDate'] == tgtDate)]
				else:
					bookedLoadSingleRow = bookedLoad[(bookedLoad['DepartureDate'] == tgtDate) & (bookedLoad['FlightNumber'] == FlightNumber)]
				#Fetch the current booked load from Navitaire
				# If for whatever reason - unable to get the booked Load - Abort
				tgtBookedLoad = None
				if(bookedLoadSingleRow.shape[0] == 0):
					dateFormatted = datetime.strptime(str(tgtDate), DATE_FORMAT_2).date().strftime(DATE_FORMAT)
					self.logger.info("Unable to get the Booked Load - Aborting")
					self.outputSummaryRow.Id = self.utils.getUniqueId()
					self.outputSummaryRow.RunId = self.runId
					self.outputSummaryRow.Origin = origin 
					self.outputSummaryRow.Destin = destination 
					self.outputSummaryRow.FltNum = str(FlightNumber)
					self.outputSummaryRow.DepDate = str(dateFormatted)
					self.outputSummaryRow.TgtLoad = str(tgtBookedLoad)
					self.outputSummaryRow.HowDetermined = "unable to get Booked loads"
					self.outputSummaryRow.analystName =  analystName
					self.outputSummaryRow.allocationStatus =  0 
					self.allocResults.append(self.outputSummaryRow)
					self.redishelper.updateRunAudit()
					self.rmdatabasehelper.insertSummaryRow(self.allocResults)
				else:
					tgtBookedLoadList = self.rmdatabasehelper.getTargetBookedLoadFromBenchmarkCurve(ndocounter, curveId)
					if (len(tgtBookedLoadList) > 0):
						tgtBookedLoadPercent = float(tgtBookedLoadList['LF'][0])
						self.runDynamicAllocationSingleMarketSingleDay(ndocounter, bookedLoadSingleRow, tgtBookedLoadPercent)
					else:
						#Cannot get a reference curve - Abort
						self.logger.info("Unable to get a Target - Check the booking curves")
						self.outputSummaryRow.Id = self.utils.getUniqueId()
						self.outputSummaryRow.RunId = self.runId
						self.outputSummaryRow.Origin = origin
						self.outputSummaryRow.Destin = destination
						self.outputSummaryRow.FltNum = str(FlightNumber)
						self.outputSummaryRow.DepDate = str(tgtDate)
						self.outputSummaryRow.BookedLoad = bookedLoadSingleRow["bookedPax"].values[0]
						self.outputSummaryRow.TgtLoad = str(tgtBookedLoad)
						self.outputSummaryRow.HowDetermined = "No Reference Curve Found"
						self.outputSummaryRow.analystName =  analystName
						self.outputSummaryRow.allocationStatus =  0
						self.allocResults.append(self.outputSummaryRow)
						self.redishelper.updateRunAudit()
						self.rmdatabasehelper.insertSummaryRow(self.allocResults)
		except Exception as e:
			self.logger.info("Exception occurred")
			traceback.print_exc()
			self.logger.info(e)

		if  (self.inputSource.isUpdateNavitaire == False):
			self.outputResults()

		if (self.inputSource.isUpdateNavitaire) and (self.NavitaireUpdateMethod==CONST_SFTP):
			self.redishelper.setAllRunStatus2Ready()

	def runDynamicAllocationSingleMarketSingleDay(self, ndocounter, bookedLoad, tgtBookedLoadPercent):
		currDate = self.timeutils.getCurrentDate()
		tgtDate = datetime.strftime(currDate + timedelta(days=ndocounter),DATE_FORMAT_2)
		dow = int(datetime.strftime(currDate + timedelta(days=ndocounter),WEEK_DAY))
		
		self.logger.info("Determining pricing for NDO " + str(ndocounter) + " on  " + str(tgtDate))
		self.logger.info("Target Pax Load Percent is :" + str(tgtBookedLoadPercent))
		ndoBookedLoad = bookedLoad[bookedLoad['DepartureDate'].astype(str).str.contains(str(tgtDate))].reset_index(drop=True)

		numFlts = ndoBookedLoad.shape[0]
		if(self.debugMode):
			self.logger.info("Found " + str(numFlts) + " Flights")
			self.logger.info(ndoBookedLoad)

		for fltCounter in range(numFlts):
			currFlt = ndoBookedLoad['FlightNumber'][fltCounter]
			currBookedLoad = ndoBookedLoad["bookedPax"][fltCounter]
			currLid = ndoBookedLoad['capacity'][fltCounter]
			currAdjustedCapacity=ndoBookedLoad['adjustedCapacity'][fltCounter]
			currFltDate = datetime.strftime(ndoBookedLoad['DepartureDate'][fltCounter],DATE_FORMAT)
			currFltDepartureTime=ndoBookedLoad["DepartureTime"][fltCounter]

			self.runDynamicAllocationSingleFlight(currFlt, currBookedLoad, currLid, currFltDate,currFltDepartureTime, tgtBookedLoadPercent, dow, ndocounter, currAdjustedCapacity)

	def runDynamicAllocationSingleFlight(self, currFlt, currBookedLoad, currLid, currFltDate, currFltDepartureTime,tgtBookedLoadPercent, dow, ndocounter, currAdjustedCapacity):
		self.outputSummaryRow_b2b=OutputSummary()
		self.outputSummaryRow_b2c=OutputSummary()
		tgtBookloadFactor = tgtBookedLoadPercent	
		tgtBookedLoad = tgtBookedLoadPercent * currLid

		if(self.debugMode):
			self.logger.info("Ready to start Allocation")
			self.logger.info(self.currParams)
			self.logger.info("DOW is : " + str(dow))
			self.logger.info("Extract is " + str(self.currParams.dow)[dow:(dow+1)])

		dateFormatted = datetime.strptime(str(currFltDate), DATE_FORMAT).date().strftime(DATE_FORMAT_2)
		if ((str(currFlt) == str(self.currParams.flight_number)) or (self.currParams.flight_number == '*')) and (str(self.currParams.dow)[dow:(dow+1)] == "1"):
			self.logger.info("Actual Booked Load for " + str(self.currParams.flight_number) + " on " + str(currFltDate) + " is :" + str(currBookedLoad))

			totalCapacity = currLid
			adjustedCapacity=currAdjustedCapacity
			bookedDeficit = round(tgtBookedLoad - currBookedLoad)

			remainingCapacity = totalCapacity - currBookedLoad
   
			dPLF = self.calculateDplf(currBookedLoad, tgtBookloadFactor, totalCapacity)
			plfValue = dPLF * 100
			plf= (currBookedLoad/adjustedCapacity) * 100
			
			variance=self.calculateVariance(currBookedLoad, tgtBookedLoad, totalCapacity)
   
		
  
			totalCapacity, b2boverbookingflag = self.overBooking(currFltDate, ndocounter, totalCapacity, adjustedCapacity, plf)
			overBookingCount =float(totalCapacity-adjustedCapacity)
			self.logger.info("plfValue is " + str(plfValue))
			ndoBand = self.rmdatabasehelper.getNdoBand(ndocounter)
			dplfBand = self.rmdatabasehelper.getdpflBand(plfValue)
			self.checkTreshold(currFlt, ndocounter, plf)
   
			
			# Get Dynamic D1 (Time) Value
			DaySpan, input_time_str = self.autoTimeRange(currFltDepartureTime)
    
			b2b_startTime, b2b_endTime, b2b_time_range = self.dynamictimed2.getDynamicTime(self.currParams, dplfBand, ndoBand,DaySpan,B2B_CHANNEL)
			b2c_startTime, b2c_endTime, b2c_time_range = self.dynamictimed2.getDynamicTime(self.currParams, dplfBand, ndoBand,DaySpan,B2C_CHANNEL)


			# Get Dynamic D2 (Offset & Criteria) Value
			b2b_criteria, b2b_offset, b2b_strategy = self.dynamictimed2.getDynamicD2Value(self.currParams,dplfBand, ndoBand,B2B_CHANNEL)
			b2c_criteria, b2c_offset, b2c_strategy = self.dynamictimed2.getDynamicD2Value(self.currParams,dplfBand, ndoBand,B2C_CHANNEL)

   
			b2bRunId = self.utils.getUniqueId()
			b2cRunId = self.utils.getUniqueId()
  
			#Actual Work Happens here
			#Get the B2C and B2B Fare ranges
			self.logger.info("Getting market Fares")

			lowFareB2C, hiFareB2C, lowAirlineCodeB2C, hiAirlineCodeB2C, lowFilghtNumB2C, hiFilghtNumB2C, isB2CMarketFareAvailable, b2c_criteria, b2c_summary, b2c_startTime, b2c_endTime = self.marketfaresservice.getMarketFareAnchorFare(self.currParams,currFltDate, B2C_CHANNEL, b2c_startTime, b2c_endTime, DaySpan, b2c_criteria, input_time_str)
			lowFareB2B, hiFareB2B, lowAirlineCodeB2B, hiAirlineCodeB2B, lowFilghtNumB2B, hiFilghtNumB2B, isB2BMarketFareAvailable, b2b_criteria, b2b_summary, b2b_startTime, b2b_endTime = self.marketfaresservice.getMarketFareAnchorFare(self.currParams,currFltDate, B2B_CHANNEL, b2b_startTime, b2b_endTime, DaySpan, b2b_criteria, input_time_str)
   
			b2bSkipFlag = False
   
			self.currentTime =self.rmdatabasehelper.getCurrentTime(self.inputSource.currentTime, self.currParams, currFltDate,self.starttime)
			if(isB2CMarketFareAvailable):

				if (b2c_criteria == MAX_FARE_ANCHOR or b2c_criteria == MAXO_FARE_ANCHOR):
					self.outputSummaryRow_b2c.anchorFare = hiFareB2C
				if (b2c_criteria == MIN_FARE_ANCHOR or b2c_criteria == MINO_FARE_ANCHOR):
					self.outputSummaryRow_b2c.anchorFare = lowFareB2C
     
				self.logger.info("Market Fare Range for " + str(currFltDate) + " in the B2C Channel is :" + str(lowFareB2C) + " to " + str(hiFareB2C))
				openDataB2C, openingFaresFlag, self.outputSummaryRow_b2c.lastSellingFare, self.outputSummaryRow_b2c.endOfDayTgtLoad, self.outputSummaryRow_b2c.endOfDaydlfBand, self.outputSummaryRow_b2c.upsell, self.outputSummaryRow_b2c.bookingToday, self.outputSummaryRow_b2c.expectedEndOfDayBooking, self.outputSummaryRow_b2c.strategyFare, self.outputSummaryRow_b2c.expectedEndOfDayLoadFactor, self.outputSummaryRow_b2c.historicOwnFare, self.outputSummaryRow_b2c.state, self.outputSummaryRow_b2c.timeSectorDrop, self.outputSummaryRow_b2c.gridDrop = self.determinerbd.determineRBDToOpen(self.outputSummaryRow_b2c.anchorFare, b2c_offset,B2C_CHANNEL, dateFormatted, dplfBand, ndocounter, currBookedLoad, totalCapacity, self.currParams, plf, currFltDepartureTime, ndoBand, dPLF, tgtBookloadFactor,self.currParams.b2c_backstop,self.currentTime)
	
				if(isB2BMarketFareAvailable):

					if (b2b_criteria == MAX_FARE_ANCHOR or b2b_criteria == MAXO_FARE_ANCHOR):
						self.outputSummaryRow_b2b.anchorFare = hiFareB2B
					if (b2b_criteria == MIN_FARE_ANCHOR or b2b_criteria == MINO_FARE_ANCHOR):
						self.outputSummaryRow_b2b.anchorFare = lowFareB2B
				
		
					self.logger.info("Market Fare Range for " + str(currFltDate) + " in the B2B Channel is :" + str(lowFareB2B) + " to " + str(hiFareB2B))
					openDataB2B, openingFaresFlag, self.outputSummaryRow_b2b.lastSellingFare, self.outputSummaryRow_b2b.endOfDayTgtLoad, self.outputSummaryRow_b2b.endOfDaydlfBand, self.outputSummaryRow_b2b.upsell, self.outputSummaryRow_b2b.bookingToday, self.outputSummaryRow_b2b.expectedEndOfDayBooking, self.outputSummaryRow_b2b.strategyFare, self.outputSummaryRow_b2b.expectedEndOfDayLoadFactor, self.outputSummaryRow_b2b.historicOwnFare, self.outputSummaryRow_b2b.state, self.outputSummaryRow_b2b.timeSectorDrop, self.outputSummaryRow_b2b.gridDrop = self.determinerbd.determineRBDToOpen(self.outputSummaryRow_b2b.anchorFare, b2b_offset,B2B_CHANNEL, dateFormatted, dplfBand, ndocounter, currBookedLoad, totalCapacity, self.currParams, plf, currFltDepartureTime, ndoBand, dPLF, tgtBookloadFactor,self.currParams.b2b_backstop,self.currentTime)
		
				else:
					openDataB2B = {'RBD':['NA'], 'Fare':['NA'], 'FareResult':['B2B Market Data NA - Skipping allocation'],'anchorFare': ['NA'],'statusInd':[3], 'allocationStatus': [0]}
					
     
			else:
				openDataB2C = {'RBD':['NA'], 'Fare':['NA'], 'FareResult':['B2C Market Data NA - Skipping allocation'],'anchorFare': ['NA'],'statusInd':[1], 'allocationStatus': [0]}
				openDataB2B = {'RBD':['NA'], 'Fare':['NA'], 'FareResult':['B2C Market Data NA - Skipping allocation'],'anchorFare': ['NA'],'statusInd':[3], 'allocationStatus': [0]}
				self.redishelper.updateRunAudit()
    	
			#statusInd
			# 0 - Allocation went as planned
			# 1 - Could not find market fares 
			# 2 - Ran out of RBDs before the offset could be achieved

			if (openDataB2C['statusInd'][0] == 0) or (openDataB2C['statusInd'][0] == 2):
				#Legit Allocation - Write to AU File
				currRec = AuGridRow(self.AU_COLUMN_LENGTH)
				currRec.type='L' #Type
				currRec.flight=' QP' + str(currFlt) + ' ' + self.currParams.origin + self.currParams.destin
				currRec.start_date=datetime.strftime(datetime.strptime(currFltDate,'%m/%d/%Y'), '%m-%d-%Y')
				currRec.end_date=datetime.strftime(datetime.strptime(currFltDate,'%m/%d/%Y'), '%m-%d-%Y')
				currRec.day_of_week='Daily'
				# Condition Added since we dont need to update the AU Grid in Local but we need the Run Id's when we update Navitare via Server Run
				if (self.inputSource.isUpdateNavitaire):
					currRec.b2crunid=b2cRunId #10
					currRec.b2brunid=b2bRunId #11
				# First - Adding all B2C RBD's
				b2cAllocationsUnits,brd_counter = self.b2CAllocation(currBookedLoad, openDataB2C, currRec, totalCapacity, bookedDeficit, dPLF, remainingCapacity, ndocounter)

				isUpsellFlag = False
				b2bSkipFlag = False

				if(float(self.currParams.b2b_factor) == -1):
					b2bSkipFlag = True
				if(b2boverbookingflag):
					openDataB2B = {'RBD':['NA'], 'Fare':['NA'], 'FareResult':['B2B allocation zero Overbooking'],'anchorFare': ['NA'],'statusInd':[1], 'allocationStatus': [1]}
					
				#Now add all the Z RBDs
				isUpsellFlag ,brd_counter= self.b2BAllocation(currBookedLoad, openDataB2B, currRec, totalCapacity, dPLF, b2cAllocationsUnits, b2bSkipFlag,b2boverbookingflag,brd_counter)

				if(isUpsellFlag and not openingFaresFlag):
					openDataB2B = {'RBD':['NA'], 'Fare':['NA'], 'FareResult':['B2B allocation skipped - Flight in upsell'],'anchorFare': ['NA'],'statusInd':[1], 'allocationStatus': [1]}
					
     
				if(b2bSkipFlag and not openingFaresFlag):
					openDataB2B = {'RBD':['NA'], 'Fare':['NA'], 'FareResult':['B2B allocation skipped - B2B Factor is -1'],'anchorFare': ['NA'],'statusInd':[1], 'allocationStatus': [1]}
					
				
				#Ends statsusInd == 0
				if self.thresholdflag:
					currRec.set_class_attribute(brd_counter, 'class_au',0)
					currRec.set_class_attribute(brd_counter, 'class_of_service','L')
					currRec.set_class_attribute(brd_counter+1, 'class_au',0)
					currRec.set_class_attribute(brd_counter+1, 'class_of_service','P')
				else:
					currRec.set_class_attribute(brd_counter, 'class_au',-1)
					currRec.set_class_attribute(brd_counter, 'class_of_service','L')
					currRec.set_class_attribute(brd_counter+1, 'class_au',-1)
					currRec.set_class_attribute(brd_counter+1, 'class_of_service','P')
     
				self.AUFileData.append(currRec)
				
				if (self.inputSource.isUpdateNavitaire) and (self.NavitaireUpdateMethod==CONST_API):
					self.updateNavitaire(currRec)
     
				if (self.inputSource.isUpdateNavitaire) and (self.NavitaireUpdateMethod==CONST_SFTP):
					self.updateNavSFTP(currRec)
			if b2boverbookingflag:
				openDataB2C['FareResult'][0]=openDataB2C['FareResult'][0]+' -Overbooking'
				openDataB2B['FareResult'][0]=openDataB2B['FareResult'][0]+' -Overbooking'
			self.outputSummaryRow_b2c.Id =b2cRunId
			self.outputSummaryRow_b2c.RunId =self.runId
			self.outputSummaryRow_b2c.Origin =self.currParams.origin
			self.outputSummaryRow_b2c.Destin =self.currParams.destin
			self.outputSummaryRow_b2c.FltNum =str(currFlt)
			self.outputSummaryRow_b2c.DepDate =str(currFltDate)
			self.outputSummaryRow_b2c.BookedLoad =str(currBookedLoad)
			self.outputSummaryRow_b2c.TgtLoad =str(tgtBookedLoad)
			self.outputSummaryRow_b2c.TimeRange = str(b2c_time_range)
			self.outputSummaryRow_b2c.dlfBand = str(dplfBand)
			self.outputSummaryRow_b2c.ndoBand = str(ndoBand)
			self.outputSummaryRow_b2c.anchorFare =str(openDataB2C["anchorFare"][0])
			self.outputSummaryRow_b2c.MktFare_Min =str(round(float(lowFareB2C)))
			self.outputSummaryRow_b2c.MktFare_Max = hiFareB2C
			self.outputSummaryRow_b2c.OpenRBD =str(openDataB2C['RBD'][0])
			self.outputSummaryRow_b2c.SellingFare =str(openDataB2C["Fare"][0])
			self.outputSummaryRow_b2c.Channel =B2C_CHANNEL
			self.outputSummaryRow_b2c.HowDetermined =str(openDataB2C['FareResult'][0])
			self.outputSummaryRow_b2c.criteria = b2c_criteria
			self.outputSummaryRow_b2c.fareOffset = b2c_offset
			self.outputSummaryRow_b2c.Backstop = self.currParams.b2c_backstop
			self.outputSummaryRow_b2c.AirlineCode_Min = str(lowAirlineCodeB2C)
			self.outputSummaryRow_b2c.AirlineCode_Max = str(hiAirlineCodeB2C)
			self.outputSummaryRow_b2c.FlightNumber_Min = str(lowFilghtNumB2C)
			self.outputSummaryRow_b2c.FlightNumber_Max = str(hiFilghtNumB2C)
			self.outputSummaryRow_b2c.Variance = str(variance)
			self.outputSummaryRow_b2c.StrategyReference = str(b2c_strategy)
			self.outputSummaryRow_b2c.analystName = self.currParams.analyst_name
			self.outputSummaryRow_b2c.bookedPlf = str(plf)
			self.outputSummaryRow_b2c.startTime = b2c_startTime
			self.outputSummaryRow_b2c.endTime = b2c_endTime
			self.outputSummaryRow_b2c.allocationStatus = (openDataB2C['allocationStatus'][0]) 
			self.outputSummaryRow_b2c.marketFaresSummary = b2c_summary
			self.outputSummaryRow_b2c.overBookingCount =overBookingCount
			self.allocResults.append(self.outputSummaryRow_b2c)
			self.rmdatabasehelper.insertSummaryRow(self.allocResults)
   
			self.outputSummaryRow_b2b.Id =b2bRunId
			self.outputSummaryRow_b2b.RunId =self.runId
			self.outputSummaryRow_b2b.Origin =self.currParams.origin
			self.outputSummaryRow_b2b.Destin =self.currParams.destin
			self.outputSummaryRow_b2b.FltNum =str(currFlt)
			self.outputSummaryRow_b2b.DepDate =str(currFltDate) 
			self.outputSummaryRow_b2b.BookedLoad =str(currBookedLoad) 
			self.outputSummaryRow_b2b.TgtLoad =str(tgtBookedLoad) 
			self.outputSummaryRow_b2b.TimeRange = str(b2b_time_range) 
			self.outputSummaryRow_b2b.dlfBand = str(dplfBand) 
			self.outputSummaryRow_b2b.ndoBand = str(ndoBand) 
			self.outputSummaryRow_b2b.anchorFare =str(openDataB2B["anchorFare"][0]) 
			self.outputSummaryRow_b2b.MktFare_Min =str(round(float(lowFareB2B))) 
			self.outputSummaryRow_b2b.MktFare_Max = hiFareB2B 
			self.outputSummaryRow_b2b.OpenRBD =str(openDataB2B['RBD'][0]) 
			self.outputSummaryRow_b2b.SellingFare =str(openDataB2B["Fare"][0]) 
			self.outputSummaryRow_b2b.Channel =B2B_CHANNEL 
			self.outputSummaryRow_b2b.HowDetermined =str(openDataB2B['FareResult'][0]) 
			self.outputSummaryRow_b2b.criteria = b2b_criteria 
			self.outputSummaryRow_b2b.fareOffset = b2b_offset 
			self.outputSummaryRow_b2b.Backstop = self.currParams.b2b_backstop 
			self.outputSummaryRow_b2b.AirlineCode_Min = str(lowAirlineCodeB2B) 
			self.outputSummaryRow_b2b.AirlineCode_Max = str(hiAirlineCodeB2B) 
			self.outputSummaryRow_b2b.FlightNumber_Min = str(lowFilghtNumB2B) 
			self.outputSummaryRow_b2b.FlightNumber_Max = str(hiFilghtNumB2B) 
			self.outputSummaryRow_b2b.Variance = str(variance) 
			self.outputSummaryRow_b2b.StrategyReference = str(b2b_strategy) 
			self.outputSummaryRow_b2b.analystName = self.currParams.analyst_name
			self.outputSummaryRow_b2b.bookedPlf = str(plf)
			self.outputSummaryRow_b2b.startTime = b2b_startTime 
			self.outputSummaryRow_b2b.endTime =  b2b_endTime 
			self.outputSummaryRow_b2b.allocationStatus = (openDataB2B['allocationStatus'][0]) 
			self.outputSummaryRow_b2b.marketFaresSummary = b2b_summary
			self.outputSummaryRow_b2b.overBookingCount =overBookingCount
			self.allocResults.append(self.outputSummaryRow_b2b)
			self.rmdatabasehelper.insertSummaryRow(self.allocResults)
				
		else:
			if (str(self.currParams.dow)[dow:(dow+1)] == "9"):
				self.redishelper.updateRunAudit()
			self.logger.info("Run Parameters requested to skip - (FlightNumber, DOW, or Status)")

	def calculateVariance(self, currBookedLoad, tgtBookedLoad, totalCapacity):
		variance = (currBookedLoad - tgtBookedLoad) / totalCapacity
		return variance

	def calculateDplf(self, currBookedLoad, tgtBookloadFactor, totalCapacity):
		dPLF = (currBookedLoad/totalCapacity) - tgtBookloadFactor
		return dPLF

	def autoTimeRange(self, currFltDepartureTime):
		DaySpan=self.currParams.day_span
		input_time_str = currFltDepartureTime
				# This should be set according to your data
		time_bands,start_end_range=self.rmdatabasehelper.getDepartureTimeBands()
		time_range_start, time_range_end,dspan = self.find_time_range(input_time_str, time_bands,start_end_range)
		if int(self.currParams.auto_time_range_flag)==1:
			self.currParams.time_window_start=time_range_start.strftime("%H:%M")
			self.currParams.time_window_end=time_range_end.strftime("%H:%M")
			DaySpan=dspan
		return DaySpan,input_time_str

	def checkTreshold(self, currFlt, ndocounter, plf):
		thresholdValue = self.rmdatabasehelper.getPlfThreshhold(ndocounter,str(currFlt))
		self.thresholdflag = False
	
		if len(thresholdValue)!=0:
			if plf>thresholdValue[0]:
				self.thresholdflag=True

	def overBooking(self, currFltDate, ndocounter, totalCapacity, adjustedCapacity, plf):
		b2boverbookingflag=False
		if self.inputSource.input_source[CONST_BOOKLOAD] == TEST_DATA:
			nextFlight=self.rmdatabasehelper.checkForNextFLight(currFltDate,self.currParams)
		if self.inputSource.input_source[CONST_BOOKLOAD] == DB:
			nextFlight=self.navdatabasehelper.checkForNextFLight(currFltDate,self.currParams)
		if(self.canOverbook(ndocounter, totalCapacity, adjustedCapacity, plf, nextFlight)):
			overBookingCount=self.rmdatabasehelper.checkNowShowProb(self.currParams.origin+self.currParams.destin,self.currParams.flight_number,self.constantsUtils)
			totalCapacity=totalCapacity+overBookingCount
			if self.inputSource.input_source[CONST_OVERBOOKING] == DB:
				leg_key,nest_key,lid,nest_lid = self.navapihelper.get_leg_nest_details(currFltDate, AKASA_AIRLINE_CODE,self.currParams.flight_number)
				if(not totalCapacity == lid):
					self.navapihelper.update_leg(leg_key, lid=totalCapacity)
				if(not totalCapacity == nest_lid):
					self.navapihelper.update_leg_nest(leg_key,nest_key,totalCapacity)
			b2boverbookingflag = True
		return totalCapacity,b2boverbookingflag

	def canOverbook(self, ndocounter, totalCapacity, adjustedCapacity, plf, nextFlight):
		return self.currParams.over_booking==YES_FLAG and nextFlight and plf>self.constantsUtils.OVERBOOKING_LF and ndocounter<=self.constantsUtils.OVERBOOKING_END_NDO and ndocounter>=self.constantsUtils.OVERBOOKING_START_NDO and int(adjustedCapacity)==int(totalCapacity)
	
	def b2BAllocation(self, currBookedLoad, openDataB2B, currRec, totalCapacity, dPLF, b2cAllocationsUnits, b2bSkipFlag,b2boverbookingflag,brd_counter):
		isUpsellFlag = False
		B2BRBDs = self.rmdatabasehelper.fareExtract(self.currParams.origin, self.currParams.destin, B2B_CHANNEL, '')

		openbuketFlag = False
		openingRBD = openDataB2B['RBD'][0]
		
		PLF = currBookedLoad / totalCapacity
		counter = 0
		#Loop Thru
		
		for b2bctr in range(B2BRBDs.shape[0]):
			RBDLabel = B2BRBDs['RBD'][b2bctr]
			b2bStatus = openDataB2B['statusInd'][0]
			currRec.set_class_attribute(brd_counter, 'class_of_service',RBDLabel)
			if b2boverbookingflag:	
				currRec.set_class_attribute(brd_counter, 'class_au',0)
			else:
				if(b2bSkipFlag):
					currRec.set_class_attribute(brd_counter, 'class_au',-1)
				else:
					if RBDLabel == openingRBD:
						openbuketFlag = True
					
					if (openbuketFlag):
						#Allocation
						if((b2bStatus == 0) or (b2bStatus == 2)):
							if(dPLF < 0 and PLF < self.PLF_CURVE_VALUE):
								currRec.set_class_attribute(brd_counter, 'class_au',int(self.mathutils.generateRoundNumber(b2cAllocationsUnits[counter])))
							else:
								currRec.set_class_attribute(brd_counter, 'class_au',0)
								isUpsellFlag = True
							counter = counter + 1
						else:
							#No B2B Allocaion - Leave it to B2C Fare
							currRec.set_class_attribute(brd_counter, 'class_au',0)
					else:
						if b2bStatus == 3:
							currRec.set_class_attribute(brd_counter, 'class_au',-1)
						else:
							currRec.set_class_attribute(brd_counter, 'class_au',0)
			brd_counter=brd_counter+1
		return isUpsellFlag,brd_counter

	def b2CAllocation(self, currBookedLoad, openDataB2C, currRec, totalCapacity, bookedDeficit, dPLF, remainingCapacity, ndocounter):
		
		b2cAllocationsUnits = []
		B2CRBDs = self.rmdatabasehelper.fareExtract(self.currParams.origin, self.currParams.destin, B2C_CHANNEL, '')
  
		maxRBDIndex = B2CRBDs['id'].iloc[-1]
		maxRBDPrice = B2CRBDs['Total'].iloc[-1]
		openingRBD = openDataB2C['RBD'][0]
		openbuketFlag = False
		openingRBDPirce = 0
		openRBDIndex = 0				
		incremental = -1
		incrementalb2b = -1
		size = B2CRBDs.shape[0] - 1
		jumpingRBDPrice = 0
		entryRDB = True
		d3Allocation = 0
		b2cctr = 0
		b2bctr = 0
		jumpPercent = self.currParams.skipping_factor
		#reset
		#Loop Thru
		while b2cctr < B2CRBDs.shape[0] or b2bctr < self.B2B_RDB_COUNT :
			if b2cctr < B2CRBDs.shape[0]:
				RBDLabel = B2CRBDs['RBD'][b2cctr] 
				remainingRBD = maxRBDIndex - openRBDIndex
				firstRDBAllocation = int(self.currParams.first_rbd_alloc)
				otherRDBAllocation = int(self.currParams.other_rbd_alloc)
				price = B2CRBDs['Total'][b2cctr]
				b2bFactor = float(self.currParams.b2b_factor)
				nextPrice = price
				if(size > b2cctr):
					nextPrice = B2CRBDs['Total'][b2cctr + 1]
				currRec.set_class_attribute(b2cctr + 1, 'class_of_service', RBDLabel)
				if RBDLabel == openingRBD:
					openbuketFlag = True
					openingRBDPirce = B2CRBDs['Total'][b2cctr]
			
			#Allocation
			if(openbuketFlag):
				#Valid B2C Allocation
				#Positive Allocation
				if(firstRDBAllocation >= 0):	
					openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b = self.jumpingAllocation(currBookedLoad, bookedDeficit, openRBDIndex, incremental, jumpPercent, price, nextPrice, firstRDBAllocation, otherRDBAllocation, jumpingRBDPrice, b2bFactor, incrementalb2b)

				#Negative -99 Allocation
				elif(firstRDBAllocation == DYNAMIC_D3_D4):
					openRBDIndex, entryRDB, d3Allocation, d4Allocation, remainingRBD = self.d3d4Calculations(dPLF, remainingCapacity, ndocounter, maxRBDPrice, openingRBDPirce, openRBDIndex, entryRDB, otherRDBAllocation, remainingRBD, d3Allocation)
					openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b = self.jumpingAllocation(currBookedLoad, bookedDeficit, openRBDIndex, incremental, jumpPercent, price, nextPrice, d3Allocation, d4Allocation, jumpingRBDPrice, b2bFactor, incrementalb2b)

				#Negative -1 Allocation	
				elif(firstRDBAllocation == DEFICIT_ALLOCATION):
					bookedDef = int(round(bookedDeficit,0))
					if(bookedDef <= 0):
						bookedDef = 0
					openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b = self.jumpingAllocation(currBookedLoad, bookedDeficit, openRBDIndex, incremental, jumpPercent, price, nextPrice, bookedDef, otherRDBAllocation, jumpingRBDPrice, b2bFactor, incrementalb2b)
					
				#Negative -2 Allocation
				elif(firstRDBAllocation == LINEAR_ALLOCATION):
					allocation = int(round(remainingCapacity/otherRDBAllocation,0))
					openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b = self.jumpingAllocation(currBookedLoad, bookedDeficit, openRBDIndex, incremental, jumpPercent, price, nextPrice, allocation, allocation, jumpingRBDPrice, b2bFactor, incrementalb2b)

				#Negative -3 Allocation
				elif(firstRDBAllocation == LINEAR_ALLOCATION_WITH_JUMP):
					allocation = int(round(remainingCapacity/otherRDBAllocation,0))
					openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b = self.jumpingAllocation(currBookedLoad, bookedDeficit, openRBDIndex, incremental, jumpPercent, price, nextPrice, allocation, allocation, jumpingRBDPrice, b2bFactor, incrementalb2b)
					
				if (incremental >= totalCapacity):
					incremental = totalCapacity
     
				if (incrementalb2b >= totalCapacity):
					incrementalb2b = totalCapacity
     
				if (RBDLabel == self.HIGHEST_B2C_RBD_VALUE):
					incremental = totalCapacity
				if b2cctr < B2CRBDs.shape[0]:
					currRec.set_class_attribute(b2cctr + 1, 'class_au',int(incremental))
				b2cAllocationsUnits.append(incrementalb2b)
				b2bctr = b2bctr + 1
				
			else:
				currRec.set_class_attribute(b2cctr + 1, 'class_au',0)
    
			if b2cctr < B2CRBDs.shape[0]:				
				b2cctr= b2cctr + 1
			
		return b2cAllocationsUnits,b2cctr+1

	def jumpingAllocation(self, currBookedLoad, bookedDeficit, openRBDIndex, incremental, jumpPercent, price, nextPrice, d3Allocation, d4Allocation, jumpingRBDPrice, b2bFactor, incrementalb2b):
		if(incremental < 0):
			if(d3Allocation >= 0):
				#allocate the FirstRBD 
				incremental = currBookedLoad + d3Allocation
				incrementalb2b = currBookedLoad + (d3Allocation * b2bFactor)
			else:
				#negtive number means allocate the full deficit - Only if the deficit is positive - Lagging
				if(bookedDeficit > 0):
					incremental = currBookedLoad + bookedDeficit
				else:
					incremental = currBookedLoad + d4Allocation
					incrementalb2b = currBookedLoad + (d4Allocation * b2bFactor)
			jumpingRBDPrice = price + (price * (jumpPercent / 100))
		else:
			nextPriceDiff = abs(nextPrice - jumpingRBDPrice)
			currPriceDiff = abs(price - jumpingRBDPrice)
			if(jumpingRBDPrice >= nextPrice):
				pass
			elif(currPriceDiff <= nextPriceDiff):
				incremental = incremental + d4Allocation
				incrementalb2b = incrementalb2b + (d4Allocation * b2bFactor)
				jumpingRBDPrice = price + (price * (jumpPercent / 100))
				openRBDIndex = openRBDIndex + 1
			elif(currPriceDiff > nextPriceDiff):
				pass
		return openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b

	def d3d4Calculations(self, dPLF, remainingCapacity, ndocounter, maxRBDPrice, openingRBDPirce, openRBDIndex, entryRDB, otherRDBAllocation, remainingRBD, d3Allocation):
		maxLimit = self.mathutils.generateMaxValue(remainingCapacity, ndocounter)
		if(entryRDB):
			if(otherRDBAllocation > 0 and openingRBDPirce - maxRBDPrice != 0):
				remainingRBD = math.log(maxRBDPrice/openingRBDPirce, (1+otherRDBAllocation/100))

			d3Allocation = self.mathutils.dynamicAllocationD3Formula(dPLF, remainingCapacity, remainingRBD, ndocounter)
			if(d3Allocation <= self.MIN_D3_D4_VALUE):
				d3Allocation = self.MIN_D3_D4_VALUE
			elif(d3Allocation >= maxLimit):
				d3Allocation = maxLimit
			entryRDB = False
   
		openRBDIndex = openRBDIndex + 1

		d4Allocation = self.mathutils.dynamicAllocationD4Formula(dPLF, openRBDIndex, d3Allocation)

		if(d4Allocation <= self.MIN_D3_D4_VALUE):
			d4Allocation = self.MIN_D3_D4_VALUE
		elif(d4Allocation >= maxLimit):
			d4Allocation = maxLimit

		return openRBDIndex, entryRDB, d3Allocation, d4Allocation, remainingRBD

	def getHistoricFare(self, dPLF, strategyFare, timeSectorDrop, gridDrop, historicOwnFare, anchorFare):
		state = 'NA'
		self.logger.info("Historic Own fare is: " + str(historicOwnFare))
		if(historicOwnFare != 0):
			if(dPLF>=0):
				if(float(strategyFare) >= float(historicOwnFare)):
					anchorFare = strategyFare
					state = 'Adh_StrategyFare'
				else:
					anchorFare = historicOwnFare
					state = 'Adh_OwnFare'
			else:
				if(float(strategyFare) < float(historicOwnFare)):
					anchorFare = strategyFare
					state = 'Bdh_StrategyFare'
				if(str(timeSectorDrop).upper() == 'YES' and str(gridDrop).upper() == 'YES'):
					state = 'Bdh_ModOwnFare'
					delta = float(strategyFare) - float(historicOwnFare)
					if(float(historicOwnFare) <= float(strategyFare)):
						anchorFare = historicOwnFare + (delta / self.OWN_FARE_DELTA) * (dPLF + self.OWN_FARE_DELTA)
					if(dPLF <= (float(self.OWN_FARE_DELTA) * -1)):
						anchorFare = historicOwnFare
      
		return anchorFare, state

	def outputResults(self):
		# self.logger.info("Final Allocation Results:")
		# self.logger.info(self.allocResults)
		if self.inputSource.output[CONST_AU_GRID] == FILE:
			AUlist=['Type','Flight','StartDate','EndDate','DayOfWeek','Aircraft Type','Aircraft Suffix','Lid','Capacity','Status']
			length = self.AU_COLUMN_LENGTH
			for i in range(int(length)):
				AUlist.append(f'Class Of Service AU Class {i+1}')
				AUlist.append(f'Class Type AU Class {i+1}')
				AUlist.append(f'Class Nest AU Class {i+1}')
				AUlist.append(f'Class Rank AU Class {i+1}')
				AUlist.append(f'Class AU AU Class {i+1}')
				AUlist.append(f'Class Allotted AU Class {i+1}')
				AUlist.append(f'AP Restriction AU Class {i+1}')	
			AUFrame = pd.DataFrame([vars(row) for row in self.AUFileData])
			AUFrame=AUFrame.drop(columns=["b2brunid","b2crunid","au_column_length"])
			AUFrame.columns=AUlist
			AUFrame.to_csv(CONST_AU_GRID_FILE_PATH, index=False)
   
		if self.inputSource.output[CONST_SUMMARY_FILE] == FILE:
			
			df = pd.DataFrame([vars(row) for row in self.allocResults])
			df.to_csv(CONST_SUMMARY_FILE_PATH, index=False)

	def updateNavitaire(self, rec):
		auData = {"runId": self.runId, "type": rec.type, "flight": rec.flight, "startDate": rec.start_date, "endDate": rec.end_date, "dayOfWeek": rec.day_of_week,
                  "aircraftType": rec.aircraft_type, "aircraftSuffix": rec.aircraft_suffix, "lid": rec.lid, "capacity": rec.capacity, "status": rec.status, "b2cRunId": rec.b2crunid, "b2bRunId": rec.b2brunid, "auClasses": {}}

		for i in range(self.AU_COLUMN_LENGTH):
			auClass = {"classOfService": getattr(rec, f"class_of_service_{i+1}", None), "classType": getattr(rec, f"class_type_{i+1}", None), "classNest": int(getattr(rec, f"class_nest_{i+1}", None)),
                       "classRank": int(getattr(rec, f"class_rank_{i+1}", None)), "classAU": int(getattr(rec, f"class_au_{i+1}", None)), "classAllotted": int(getattr(rec, f"class_allotted_{i+1}", None)), "apRestriction": int(getattr(rec, f"ap_restriction_{i+1}", None))}
			auData['auClasses'][auClass['classOfService']] = auClass

		auDataStr = json.dumps(auData)
		#self.logger.info(auDataStr)
        # Send to queue for the market
		self.sqs.send_message(
            QueueUrl=self.RBD_QUEUE_URL,
            MessageBody=auDataStr
        )

	def updateNavSFTP(self, rec):
		flightNo = rec.flight.split(" ")[1][2:7]
		sector = rec.flight.split(" ")[2]
		flightDate = datetime.strptime(str(rec.start_date), "%m-%d-%Y").date().strftime(DATE_FORMAT_2)
		auData = {"type": rec.type, "sector1": sector, "flightNo1" : flightNo, "departureDate": flightDate, "auClasses": {}}
		auClass={}
		for i in range(self.AU_COLUMN_LENGTH):
			auClass[getattr(rec, f"class_of_service_{i+1}", None)] = int(getattr(rec, f"class_au_{i+1}", None))
  
		auData['auClasses'] = auClass
		auDataStr = json.dumps(auData)
		self.rmdatabasehelper.insert_run_fltDate_audit(self.runId, flightNo, flightDate, auDataStr, "Ready", "Ready")

	def getStartEndDate(self, currDate, startNDO, endNDO):
		startDate = datetime.strptime(startNDO, DATE_FORMAT).date()
		endDate = datetime.strptime(endNDO, DATE_FORMAT).date()
		#Reset the startNDO and endNDO to actually NDO
		startNDO = (startDate - currDate).days
		endNDO = (endDate - currDate).days
		
		return startDate, endDate, startNDO, endNDO

	
	def find_time_range(self,input_time_str, time_bands,start_end_range):
		input_time = datetime.strptime(input_time_str, '%H:%M').time()
		date_today = self.timeutils.getCurrentDateTime()
		lower_time = (datetime.combine(date_today, input_time) - timedelta(hours=1, minutes=30)).time()
		upper_time = (datetime.combine(date_today, input_time) + timedelta(hours=1, minutes=30)).time()

		# Initialize the start and end times
		start_time = end_time = None
		dayspan=0
		for start, end, time_band in time_bands:
			# Apply the logic based on the TimeBand value and the input time
			if start <= input_time <= end:
				if time_band==1:
					if input_time<datetime.strptime(start_end_range[0][0], '%H:%M').time():
						start_time=datetime.strptime(start_end_range[0][1], '%H:%M').time()
						end_time=max(datetime.strptime(start_end_range[0][0], '%H:%M').time(),upper_time)
						dayspan=-1
					else:
						start_time=min(datetime.strptime(start_end_range[0][1], '%H:%M').time(),lower_time)
						end_time=datetime.strptime(start_end_range[0][0], '%H:%M').time()
						dayspan=1
					return start_time,end_time,dayspan
				else:
					start_time = start
					end_time = end
					return min(lower_time, start_time), max(upper_time, end_time),dayspan
	

	def _initialize_params(self,inputData, currParams):
		self.currParams=MarketListRow(currParams)
		self.inputSource = InputSource(inputData)
		self.debugMode = inputData["debugMode"]
		self.NavitaireUpdateMethod=self.currParams.navitaire_update_method
		self.TABLE_MARKET_FARES = TABLE_MARKET_FARES
		self.MARKET_LIST_QUEUE_URL = inputData["MARKET_LIST_QUEUE_URL"]
		self.RBD_QUEUE_URL = inputData["RBD_QUEUE_URL"]
		self.AWS_SQS_QUEUE_URL=inputData["AU_CHUNK_QUEUE_URL"]
		self.NAV_DB_NAME = inputData["NAV_DB_NAME"]
		self.NAV_DB_DW_NAME = inputData["NAV_DB_DW_NAME"]
		self.NAV_DB_WB_NAME = inputData["NAV_DB_WB_NAME"]
		self.NAV_BASE_API_URL=inputData["NAV_BASE_API_URL"]
		self.NAV_OCP_SUBSCRIPTION_KEY = inputData["NAV_OCP_SUBSCRIPTION_KEY"]
		self.NAV_USERNAME = inputData["NAV_USERNAME"]
		self.NAV_PWD = inputData["NAV_PWD"]
		self.NAV_DOMAIN = inputData["NAV_DOMAIN"]
		self.sqs = self.currParams.sqs
		self.conn = self.currParams.dbconn
		self.wrconn = self.currParams.wrconn
		self.cache_client = self.currParams.cache_client
		self.starttime=datetime.now()
		self.NavitaireConnection = self.currParams.navitaire_connection
		self.OWN_FARE_DELTA = OWN_FARE_DELTA
		self.allocResults = []
		self.AUFileData = []
		if (self.currParams.run_id):
			self.runId = self.currParams.run_id
		else:
			self.runId = self.utils.getUniqueId()
		self.logger= OwnLogger(self.runId,self.inputSource)
		self.rmdatabasehelper=RMDatabaseHelper(self.conn,self.wrconn,self.runId,self.inputSource,self.debugMode,self.cache_client)
		self.constantsUtils=ConstantsUtils(self.rmdatabasehelper)
		self.utils=Utils(self.inputSource)
		self.mathutils=MathUtils(self.inputSource)
		self.timeutils=TimeUtils(self.inputSource)
		self.navdatabasehelper=NavDatabaseHelper(self.NavitaireConnection,self.conn,self.inputSource,self.NAV_DB_NAME,self.runId,self.debugMode,self.NAV_DB_WB_NAME)
		self.redishelper=RedisHelper(self.runId,self.cache_client,self.inputSource)
		if self.inputSource.input_source[CONST_OVERBOOKING] == DB:
			self.navapihelper = NavAPIHelper(self.NAV_BASE_API_URL,self.NAV_OCP_SUBSCRIPTION_KEY,self.NAV_USERNAME,self.NAV_PWD,self.NAV_DOMAIN, self.runId, self.inputSource)
		self.dynamictimed2=GetDynamicTimeD2(self.rmdatabasehelper)
		self.marketfaresservice=MarketFareService(self.rmdatabasehelper,self.constantsUtils)
		self.determinerbd=DetermineRBD(self.rmdatabasehelper,self.logger,self.inputSource,self.constantsUtils,self.navdatabasehelper)
		
		self.MIN_D3_D4_VALUE = self.constantsUtils.MIN_D3_D4_VALUE
		self.LINEAR_JUMP_VALUE = self.constantsUtils.LINEAR_JUMP_VALUE
		self.PLF_CURVE_VALUE = self.constantsUtils.PLF_CURVE_VALUE
		self.B2B_FARE_PRICE_COMPARISON = self.constantsUtils.B2B_FARE_PRICE_COMPARISON
		self.COVERAGE_PARAMETER = self.constantsUtils.COVERAGE_PARAMETER
		self.AU_COLUMN_LENGTH = self.constantsUtils.AU_COLUMN_LENGTH
		self.B2B_RDB_COUNT = self.constantsUtils.B2B_RDB_COUNT
		self.LOWEST_B2B_RBD_VALUE = self.constantsUtils.LOWEST_B2B_RBD_VALUE
		self.LOWEST_B2C_RBD_VALUE = self.constantsUtils.LOWEST_B2C_RBD_VALUE
		self.HIGHEST_B2B_RBD_VALUE = self.constantsUtils.HIGHEST_B2B_RBD_VALUE
		self.HIGHEST_B2C_RBD_VALUE = self.constantsUtils.HIGHEST_B2C_RBD_VALUE
		if(inputData["latestCode"]):
			code_version = self.rmdatabasehelper.getLatestCodeVersion()
			if(code_version != LATEST_VERSION):
				self.logger.info("Please use latest code! This is an old version!")
				exit()
    
	def wait_for_inputs(self):
		"""Waits until all inputs are ready."""
		can_copy=False
		while  can_copy == False:
			can_copy = self.rmdatabasehelper.areAllInputsReady()
			if can_copy==False:
				self.logger.info('Waiting as inputs are busy...')
				time_sleep.sleep(WAIT_TIME)
