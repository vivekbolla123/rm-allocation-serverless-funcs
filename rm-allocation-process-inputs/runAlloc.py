import traceback
import pandas as pd
import uuid
import math
import os
import time as time_sleep
import json
import logging
import warnings
import numpy as np
import random
import pytz
from Constants import *
from datetime import date
from datetime import timedelta
from datetime import time
from datetime import datetime
from dateutil import parser
import requests

warnings.filterwarnings('ignore')

class DynAlloc:

	def __init__(self, inputData, currParams):
		
		LATEST_VERSION = '4.0.0'
		
		self.inputSource = inputData
		#Set this to true for more verbose scree display during the run
		self.debugMode = self.inputSource["debugMode"]
		self.NavitaireUpdateMethod=currParams["NavitaireUpdateMethod"]
		if (currParams["runId"]):
			self.runId = currParams["runId"]
		else:
			self.runId = self.getUniqueId()
   
		self.logger= OwnLogger(self.runId,self.inputSource[CONST_LOGS][CONST_RUN_AUDIT])
  
		self.TABLE_MARKET_FARES = "market_fares"
		self.BookloadTableName = "bookload"
		self.WAIT_TIME = 30 # in seconds
		self.MARKET_LIST_QUEUE_URL = self.inputSource["MARKET_LIST_QUEUE_URL"]
		self.RBD_QUEUE_URL = self.inputSource["RBD_QUEUE_URL"]
		self.AWS_SQS_QUEUE_URL=self.inputSource["AU_CHUNK_QUEUE_URL"]
		self.NAV_DB_NAME = self.inputSource["NAV_DB_NAME"]
		self.NAV_DB_DW_NAME = self.inputSource["NAV_DB_DW_NAME"]
		self.NAV_DB_WB_NAME = self.inputSource["NAV_DB_WB_NAME"]
		self.API_KEY = self.inputSource["API_KEY"]
		self.API_URL = self.inputSource["API_URL"]
		
		self.sqs = currParams["sqs"]
		self.conn = currParams["dbconn"]
		self.wrconn = currParams["wrconn"]
		self.cache_client = currParams["cache_client"]
		self.starttime=datetime.now()
		self.NavitaireConnection = currParams["NavitaireConnection"]

		if(inputData["latestCode"]):
			code_version = self.getLatestCodeVersion()
			if(code_version != LATEST_VERSION):
				self.logger.info("Please use latest code! This is an old version!")
				exit()

		self.generateConstantValues()

		self.chunk_size = 4 * 1024 * 1024
		#Place Holders for Output
		self.allocResults = pd.DataFrame(columns=['Id','Origin','Destin','FltNum','DepDate','BookedLoad','TgtLoad','bookedPlf','Variance','strategyFare','lastSellingFare','upsell','anchorFare','OpenRBD','SellingFare','Channel','HowDetermined','RunId','CreatedAt','MktFare_Min','AirlineCode_Min','FlightNumber_Min','MktFare_Max','AirlineCode_Max','FlightNumber_Max','criteria','fareOffset','TimeRange','startTime','endTime','Backstop','StrategyReference','dlfBand','endOfDaydlfBand','ndoBand','analystName','allocationStatus'])
		self.AUFileData = []
		
		#Start the Allocation
		if (currParams["runSingleMarket"]):
			self.runDynamicAllocationSingleMarket(currParams, 1)
		else:
			self.runDynamicAllocation()

	def runDynamicAllocation(self):
		self.logger.info("Starting Dynamic Allocation")
		#Load the Run Parameters
		self.loadRunParams()
		startTime = self.getCurrentDateTime()
		username = os.getlogin()
		
		self.insertRunStart(startTime, username)

		#The list of markets to run the allocation
		numMarkets = self.mktList.shape[0]
		self.logger.info("Market List has " + str(numMarkets) + " Rows")

		if(self.debugMode):
			self.logger.info("Market List : ")
			self.logger.info(self.mktList)

		for mktCounter in range(numMarkets):
			currParams = self.mktList.iloc[mktCounter]
			self.runDynamicAllocationSingleMarket(currParams, mktCounter)

		self.outputResults()

		endTime = self.getCurrentDateTime()
		self.updateRunEnd(endTime)
		runTime = (endTime - startTime).total_seconds()
		self.logger.info("Allocation Completed in " + str(runTime) + " seconds")

	def runDynamicAllocationSingleMarket(self, currParams, mtkIndex):
		self.logger.info("**********************************************")
		self.logger.info("Starting Allocation for row : " + str(mtkIndex+1))

		if(self.debugMode):
			self.logger.info(currParams)
		
		#Convert NDO to run dates as necessary
		currDate = self.getCurrentDate()

		origin = currParams['Origin']
		destination = currParams['Destin']
		startDate = currDate
		endDate = currDate
		startNDO = currParams['PerStart']
		endNDO = currParams['PerEnd']
		B2BBackstop = currParams['B2BBackstop']
		B2CBackstop = currParams['B2CBackstop']
		curveId = currParams['CurveID']
		PerType = currParams['PerType']
		SkippingFactor = currParams['SkippingFactor']
		analystName = currParams['analystName']
		FlightNumber = str(currParams['FlightNumber'])
		currParams['Strategy'] = currParams['fareAnchor']

		startDate, endDate, startNDO, endNDO = self.getStartEndDate(PerType, currDate, startNDO, endNDO)
		
		self.logger.info("Allocation for " + origin + destination + " from " + str(startDate) + " to " + str(endDate))
		
		# check if inputs are available to be copied else wait and retry
		canCopy = False
		while canCopy == False:
			canCopy = self.areAllInputsReady()
			if canCopy == False:
				self.logger.info('Waiting as inputs are busy...')
				time_sleep.sleep(self.WAIT_TIME)
  
		bookedLoad = self.getCurrentBookedLoads(origin, destination, startDate, endDate)
		try: 
			# For each NDO
			for ndocounter in range(startNDO, endNDO+1):
				currDate = self.getCurrentDate()
				tgtDate = datetime.strftime(currDate + timedelta(days=ndocounter),"%Y-%m-%d")
				if(bookedLoad.shape[0] == 0):
					dateFormatted = datetime.strptime(str(startDate), DATE_FORMAT_2).date().strftime(DATE_FORMAT)
					self.logger.info("Unable to get the Booked Load - Aborting")
					self.allocResults = self.allocResults.append(
					{
						"Id":self.getUniqueId(), 
						"RunId":self.runId, 
						"Origin":origin, 
						"Destin":destination, 
						"FltNum":str(FlightNumber),
						"DepDate":str(dateFormatted), 
						"BookedLoad":"NA", 
						"TgtLoad":'NA', 
						'TimeRange': 'NA', 
						'dlfBand': 'NA', 
						'ndoBand': 'NA', 
						'anchorFare': 'NA', 
						"MktFare_Min":"NA",
						"MktFare_Max":"NA", 
						"fareOffset": "NA", 
						"OpenRBD": "NA", 
						"SellingFare":"NA", 
						'Channel':"NA", 
						'criteria':"NA",
						'HowDetermined':"unable to get Booked loads", 
						'Backstop': 'NA', 
						'AirlineCode_Min': 'NA', 
						'AirlineCode_Max': 'NA', 
						'FlightNumber_Min': 'NA', 
						'FlightNumber_Max': 'NA', 
						'Variance': 'NA', 
						'StrategyReference': 'NA', 
						'analystName': analystName,
						'bookedPlf': 'NA', 
						'startTime':'NA', 
						'endTime': 'NA', 
						'allocationStatus': 0, 
						'endOfDaydlfBand': 'NA', 
						'endOfDayTgtLoad': 'NA', 
						'lastSellingFare': 'NA', 
						'upsell': 'NA', 
						'strategyFare': 'NA',
						'bookingToday': 'NA',
						'expectedEndOfDayBooking': 'NA',
						'expectedEndOfDayLoadFactor': 'NA'
					}, ignore_index = True)
					self.updateRunAudit("Done", "Done", FlightNumber, tgtDate)
					self.insertSummaryRow()
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
					self.allocResults = self.allocResults.append(
					{
						"Id":self.getUniqueId(), 
      					"RunId":self.runId, 
           				"Origin":origin, 
               			"Destin":destination, 
                  		"FltNum":str(FlightNumber),
                    	"DepDate":str(dateFormatted), 
                     	"BookedLoad":"NA", 
                      	"TgtLoad":str(tgtBookedLoad), 
                       	'TimeRange': 'NA', 
                        'dlfBand': 'NA', 
                        'ndoBand': 'NA', 
                        'anchorFare': 'NA', 
                        "MktFare_Min":"NA",
                        "MktFare_Max":"NA", 
                        "fareOffset": "NA", 
                        "OpenRBD": "NA", 
                        "SellingFare":"NA", 
                        'Channel':"NA", 
                        'criteria':"NA",
                        'HowDetermined':"unable to get Booked loads", 
                        'Backstop': 'NA', 
                        'AirlineCode_Min': 'NA', 
                        'AirlineCode_Max': 'NA', 
                        'FlightNumber_Min': 'NA', 
                        'FlightNumber_Max': 'NA', 
                        'Variance': 'NA', 
                        'StrategyReference': 'NA', 
                        'analystName': analystName,
                        'bookedPlf': 'NA', 
                        'startTime':'NA', 
                        'endTime': 'NA', 
                        'allocationStatus': 0, 
                        'endOfDaydlfBand': 'NA', 
                        'endOfDayTgtLoad': 'NA', 
						'lastSellingFare': 'NA', 
						'upsell': 'NA', 
						'strategyFare': 'NA',
						'bookingToday': 'NA',
						'expectedEndOfDayBooking': 'NA',
						'expectedEndOfDayLoadFactor': 'NA'
					}, ignore_index = True)
					self.updateRunAudit("Done", "Done", FlightNumber, tgtDate)
					self.insertSummaryRow()
				else:
					tgtBookedLoadList = self.getTargetBookedLoadFromBenchmarkCurve(ndocounter, curveId)
					if (len(tgtBookedLoadList) > 0):
						tgtBookedLoad = float(tgtBookedLoadList['LF'][0])
						B2CAvgFare=float(tgtBookedLoadList['B2C_Avg_Fare'][0])
						B2BAvgFare=float(tgtBookedLoadList['B2B_Avg_Fare'][0])
						self.runDynamicAllocationSingleMarketSingleDay(ndocounter, bookedLoadSingleRow, tgtBookedLoad, currParams, B2CBackstop, B2BBackstop,B2CAvgFare,B2BAvgFare, SkippingFactor)
					else:
						#Cannot get a reference curve - Abort
						self.logger.info("Unable to get a Target - Check the booking curves")
						self.allocResults = self.allocResults.append(
						{
							"Id":self.getUniqueId(), 
							"RunId":self.runId, 
							"Origin":origin,
							"Destin":destination,
							"FltNum":str(FlightNumber),
							"DepDate":str(tgtDate), 
							"BookedLoad":bookedLoadSingleRow["bookedPax"].values[0], 
							"TgtLoad":str(tgtBookedLoad), 
							'TimeRange': 'NA', 
							'dlfBand': 'NA', 
							'ndoBand': 'NA', 
							'anchorFare': 'NA', 
							"MktFare_Min":"NA",
							"MktFare_Max":"NA", 
							"fareOffset": "NA", 
							"OpenRBD":"NA", 
							"SellingFare":"NA", 
							'criteria':"NA",
							'Channel':"NA", 
							'HowDetermined':"No Reference Curve Found", 
							'Backstop': 'NA', 
							'AirlineCode_Min': 'NA', 
							'AirlineCode_Max': 'NA', 
							'FlightNumber_Min': 'NA', 
							'FlightNumber_Max': 'NA', 
							'Variance': 'NA', 
							'StrategyReference': 'NA', 
							'analystName': analystName,
							'bookedPlf': 'NA',
							'startTime':'NA', 
							'endTime': 'NA', 
							'allocationStatus': 0, 
							'endOfDaydlfBand': 'NA', 
							'endOfDayTgtLoad': 'NA', 
							'lastSellingFare': 'NA', 
							'upsell': 'NA', 
							'strategyFare': 'NA',
							'bookingToday': 'NA',
							'expectedEndOfDayBooking': 'NA',
							'expectedEndOfDayLoadFactor': 'NA'
						}, ignore_index = True)
						self.updateRunAudit("Done", "Done", FlightNumber, tgtDate)
						self.insertSummaryRow()
		except Exception as e:
			self.logger.info("Exception occurred")
			traceback.print_exc()
			self.logger.info(e)

		if  (self.inputSource['isUpdateNavitaire'] == False):
			self.outputResults()

		if (self.inputSource['isUpdateNavitaire']) and (self.NavitaireUpdateMethod==CONST_SFTP):
			self.setAllRunStatus2Ready()

	def runDynamicAllocationSingleMarketSingleDay(self, ndocounter, bookedLoad, tgtBookedLoad, currParams, B2CBackstop, B2BBackstop,B2CAvgFare,B2BAvgFare, SkippingFactor):
		currDate = self.getCurrentDate()
		tgtDate = datetime.strftime(currDate + timedelta(days=ndocounter),"%Y-%m-%d")
		dow = int(datetime.strftime(currDate + timedelta(days=ndocounter),"%w"))
		
		self.logger.info("Determining pricing for NDO " + str(ndocounter) + " on  " + str(tgtDate))
		self.logger.info("Target Pax Load is :" + str(tgtBookedLoad))

		ndoBookedLoad = bookedLoad[bookedLoad['DepartureDate'].astype(str).str.contains(str(tgtDate))]
		ndoBookedLoad = ndoBookedLoad.reset_index(drop=True)
		
		numFlts = ndoBookedLoad.shape[0]
		if(self.debugMode):
			self.logger.info("Found " + str(numFlts) + " Flights")
			self.logger.info(ndoBookedLoad)

		for fltCounter in range(numFlts):
			currFlt = ndoBookedLoad['FlightNumber'][fltCounter]
			currBookedLoad = ndoBookedLoad["bookedPax"][fltCounter]
			currCapacity = ndoBookedLoad['capacity'][fltCounter]
			currFltDate = datetime.strftime(ndoBookedLoad['DepartureDate'][fltCounter],DATE_FORMAT)
			currFltDepartureTime=ndoBookedLoad["DepartureTime"][fltCounter]

			self.runDynamicAllocationSingleFlight(currFlt, currBookedLoad, currCapacity, currFltDate,currFltDepartureTime, tgtBookedLoad, dow, currParams, ndocounter, B2CBackstop, B2BBackstop,B2CAvgFare,B2BAvgFare, SkippingFactor)

	def runDynamicAllocationSingleFlight(self, currFlt, currBookedLoad, currCapacity, currFltDate, currFltDepartureTime,tgtBookedLoad, dow, currParams, ndocounter, B2CBackstop, B2BBackstop,B2CAvgFare,B2BAvgFare, SkippingFactor):
		
		tgtBookloadFactor = tgtBookedLoad	
		tgtBookedLoad = tgtBookedLoad * currCapacity

		if(self.debugMode):
			self.logger.info("Ready to start Allocation")
			self.logger.info(currParams)
			self.logger.info("DOW is : " + str(dow))
			self.logger.info("Extract is " + str(currParams['DOW'])[dow:(dow+1)])

		dateFormatted = datetime.strptime(str(currFltDate), DATE_FORMAT).date().strftime(DATE_FORMAT_2)
		if ((str(currFlt) == str(currParams['FlightNumber'])) or (currParams['FlightNumber'] == '*')) and (str(currParams['DOW'])[dow:(dow+1)] == "1"):
			self.logger.info("Actual Booked Load for " + str(currParams['FlightNumber']) + " on " + str(currFltDate) + " is :" + str(currBookedLoad))

			totalCapacity = currCapacity
			bookedDeficit = round(tgtBookedLoad - currBookedLoad)

			remainingCapacity = totalCapacity - currBookedLoad
   
			dPLF = (currBookedLoad/totalCapacity) - tgtBookloadFactor
			plfValue = dPLF * 100
			plf= (currBookedLoad/totalCapacity) * 100
			curveReference = (currBookedLoad - tgtBookedLoad) / totalCapacity
			self.logger.info("plfValue is " + str(plfValue))
			ndoBand = self.getNdoBand(ndocounter)
			dplfBand = self.getdpflBand(plfValue)
			thresholdValue = self.getPlfThreshhold(ndocounter,str(currFlt))
			self.thresholdflag=False
			if len(thresholdValue)!=0:
				if plf>thresholdValue[0]:
					self.thresholdflag=True
   
			OwnFareFlag=currParams['OwnFareFlag']
			DaySpan=currParams['DaySpan']
			# Get Dynamic D1 (Time) Value
			input_time_str = currFltDepartureTime
			# This should be set according to your data
			time_bands,start_end_range=self.getDepartureTimeBands()
			time_range_start, time_range_end,dspan = self.find_time_range(input_time_str, time_bands,start_end_range)
			if int(currParams["AutoTimeRangeFlag"])==1:
				currParams['TimeWindowStart']=time_range_start.strftime("%H:%M")
				currParams['TimeWindowEnd']=time_range_end.strftime("%H:%M")
				DaySpan=dspan
    
			b2b_startTime, b2b_endTime, b2b_time_range = self.getDynamicTime(currParams, dplfBand, ndoBand,DaySpan,"B2B")
			b2c_startTime, b2c_endTime, b2c_time_range = self.getDynamicTime(currParams, dplfBand, ndoBand,DaySpan,"B2C")

			# Get Dynamic D2 (Offset & Criteria) Value
			b2b_criteria, b2b_offset, b2b_strategy = self.getDynamicD2Value(currParams, dplfBand, ndoBand,"B2B")
			b2c_criteria, b2c_offset, b2c_strategy = self.getDynamicD2Value(currParams, dplfBand, ndoBand,"B2C")

   
			b2bRunId = self.getUniqueId()
			b2cRunId = self.getUniqueId()
  
			#Actual Work Happens here
			#Get the B2C and B2B Fare ranges
			self.logger.info("Getting market Fares")

			lowFareB2C, hiFareB2C, lowAirlineCodeB2C, hiAirlineCodeB2C, lowFilghtNumB2C, hiFilghtNumB2C, isB2CMarketFareAvailable = self.getMarketFareAnchorFare(currFltDate, currParams, B2CAvgFare, 'B2C', b2c_startTime, b2c_endTime, DaySpan)
			lowFareB2B, hiFareB2B, lowAirlineCodeB2B, hiAirlineCodeB2B, lowFilghtNumB2B, hiFilghtNumB2B, isB2BMarketFareAvailable = self.getMarketFareAnchorFare(currFltDate, currParams, B2BAvgFare, 'B2B', b2b_startTime, b2b_endTime, DaySpan)
   
			b2bSkipFlag = False
			
			b2cAnchorFare = 'NA'
			b2bAnchorFare = 'NA'
			lastSoldFareb2c = 'NA'
			lastSoldFareb2b = 'NA'
			upsellb2c = 'NA'
			upsellb2b = 'NA'
			startegyFareb2c = 'NA'
			startegyFareb2b = 'NA'
			endOfDayTgtBookedLoadb2c = 'NA'
			expectedEndOfDayDplfBandb2c = 'NA'
			bookingTodayb2c = 'NA'
			expectedEndOfDayBookingb2c = 'NA'
			endOfDayBookingsb2c = 'NA'
			endOfDayTgtBookedLoadb2b = 'NA'
			expectedEndOfDayDplfBandb2b = 'NA'
			bookingTodayb2b = 'NA'
			expectedEndOfDayBookingb2b = 'NA'
			endOfDayBookingsb2b = 'NA'
   
			if(isB2CMarketFareAvailable):
				
				if ((OwnFareFlag==YES_FLAG and B2CAvgFare==FARE_NOT_AVL and b2c_criteria==OWNF_FARE_ANCHOR) or (OwnFareFlag==NO_FLAG and b2c_criteria==OWNF_FARE_ANCHOR)):
					b2c_criteria=MINO_FARE_ANCHOR

				if (b2c_criteria == MAX_FARE_ANCHOR or b2c_criteria == MAXO_FARE_ANCHOR):
					b2cAnchorFare = hiFareB2C
				if (b2c_criteria == MIN_FARE_ANCHOR or b2c_criteria == MINO_FARE_ANCHOR):
					b2cAnchorFare = lowFareB2C
				if (b2c_criteria==OWNF_FARE_ANCHOR):
					b2cAnchorFare = B2CAvgFare
     
				self.logger.info("Market Fare Range for " + str(currFltDate) + " in the B2C Channel is :" + str(lowFareB2C) + " to " + str(hiFareB2C))
				openDataB2C, openingFaresFlag, lastSoldFareb2c, endOfDayTgtBookedLoadb2c, expectedEndOfDayDplfBandb2c, upsellb2c, bookingTodayb2c, expectedEndOfDayBookingb2c, startegyFareb2c, endOfDayBookingsb2c = self.determineRBDToOpen(currParams['Origin'], currParams['Destin'], b2cAnchorFare, b2c_offset,'B2C', B2CBackstop, currParams['FlightNumber'], dateFormatted, currParams['openingFares'], dplfBand, ndocounter, currBookedLoad, totalCapacity, currParams, plf, currFltDepartureTime)
    
				if (OwnFareFlag==YES_FLAG and B2CAvgFare!=FARE_NOT_AVL and b2c_criteria==OWNF_FARE_ANCHOR and not openingFaresFlag):
					openDataB2C['FareResult'] = "Matched and allocated at Own Fare"
				if (OwnFareFlag==YES_FLAG and B2CAvgFare==FARE_NOT_AVL and b2c_criteria==OWNF_FARE_ANCHOR and not openingFaresFlag):
					openDataB2C['FareResult'] = "Inconsistent inputs recheck-Y Flag/OWNF/Fare NA Allocated with MINO"
				if (OwnFareFlag==NO_FLAG and b2c_criteria==OWNF_FARE_ANCHOR and not openingFaresFlag):
					openDataB2C['FareResult'] = "Inconsistent inputs recheck-N Flag/OWNF"
	
				if(isB2BMarketFareAvailable):
					if ((OwnFareFlag==YES_FLAG and B2BAvgFare==FARE_NOT_AVL and b2b_criteria==OWNF_FARE_ANCHOR) or (OwnFareFlag==NO_FLAG and b2b_criteria==OWNF_FARE_ANCHOR)):
						b2b_criteria=MINO_FARE_ANCHOR

					if (b2b_criteria == MAX_FARE_ANCHOR or b2b_criteria == MAXO_FARE_ANCHOR):
						b2bAnchorFare = hiFareB2B
					if (b2b_criteria == MIN_FARE_ANCHOR or b2b_criteria == MINO_FARE_ANCHOR):
						b2bAnchorFare = lowFareB2B
					if (b2b_criteria==OWNF_FARE_ANCHOR):
						b2bAnchorFare = B2BAvgFare
		
					self.logger.info("Market Fare Range for " + str(currFltDate) + " in the B2B Channel is :" + str(lowFareB2B) + " to " + str(hiFareB2B))
					openDataB2B, openingFaresFlag, lastSoldFareb2b, endOfDayTgtBookedLoadb2b, expectedEndOfDayDplfBandb2b, upsellb2b, bookingTodayb2b, expectedEndOfDayBookingb2b, startegyFareb2b, endOfDayBookingsb2b = self.determineRBDToOpen(currParams['Origin'], currParams['Destin'], b2bAnchorFare, b2b_offset,'B2B', B2BBackstop, currParams['FlightNumber'], dateFormatted, currParams['openingFares'], dplfBand, ndocounter, currBookedLoad, totalCapacity, currParams, plf, currFltDepartureTime)
		
					if (OwnFareFlag==YES_FLAG and B2BAvgFare!=FARE_NOT_AVL and b2b_criteria==OWNF_FARE_ANCHOR and not openingFaresFlag):
						openDataB2B['FareResult'] = "Matched and allocated at Own Fare"
					if (OwnFareFlag==YES_FLAG and B2BAvgFare==FARE_NOT_AVL and b2b_criteria==OWNF_FARE_ANCHOR and not openingFaresFlag):
						openDataB2B['FareResult'] = "Inconsistent inputs recheck-Y Flag/OWNF/Fare NA Allocated with MINO"
					if (OwnFareFlag==NO_FLAG and b2b_criteria==OWNF_FARE_ANCHOR and not openingFaresFlag):
						openDataB2B['FareResult'] = "Inconsistent inputs recheck-N Flag/OWNF"
				else:
					openDataB2B = {'RBD':['NA'], 'Fare':['NA'], 'FareResult':['B2B Market Data NA - Skipping allocation'],'anchorFare': ['NA'],'statusInd':[3], 'allocationStatus': [0]}
					self.updateRunAuditb2b("Done", currParams['FlightNumber'], dateFormatted)
     
			else:
				openDataB2C = {'RBD':['NA'], 'Fare':['NA'], 'FareResult':['B2C Market Data NA - Skipping allocation'],'anchorFare': ['NA'],'statusInd':[1], 'allocationStatus': [0]}
				openDataB2B = {'RBD':['NA'], 'Fare':['NA'], 'FareResult':['B2C Market Data NA - Skipping allocation'],'anchorFare': ['NA'],'statusInd':[3], 'allocationStatus': [0]}
				self.updateRunAudit("Done", "Done", currParams['FlightNumber'], dateFormatted)
    	
			#statusInd
			# 0 - Allocation went as planned
			# 1 - Could not find market fares 
			# 2 - Ran out of RBDs before the offset could be achieved

			if (openDataB2C['statusInd'][0] == 0) or (openDataB2C['statusInd'][0] == 2):
				#Legit Allocation - Write to AU File
				currRec = []
				currRec.append('L') #Type
				currRec.append(' QP' + str(currFlt) + ' ' + currParams['Origin'] + currParams['Destin']) 
				currRec.append(datetime.strftime(datetime.strptime(currFltDate,'%m/%d/%Y'), '%m-%d-%Y'))
				currRec.append(datetime.strftime(datetime.strptime(currFltDate,'%m/%d/%Y'), '%m-%d-%Y'))
				currRec.append('Daily')
				currRec.append('') #AcftType
				currRec.append('') #Suffix
				currRec.append('') #Lid
				currRec.append('') #Cap
				currRec.append('') #Status
				# Condition Added since we dont need to update the AU Grid in Local but we need the Run Id's when we update Navitare via Server Run
				if (self.inputSource['isUpdateNavitaire']):
					currRec.append(b2cRunId) #10
					currRec.append(b2bRunId) #11

				# First - Adding all B2C RBD's
				b2cAllocationsUnits = self.B2CAllocation(currBookedLoad, currParams, openDataB2C, currRec, totalCapacity, bookedDeficit, dPLF, remainingCapacity, ndocounter, SkippingFactor)

				isUpsellFlag = False
				b2bSkipFlag = False

				if(float(currParams['B2BFactor']) == -1):
					b2bSkipFlag = True
     
				#Now add all the Z RBDs
				isUpsellFlag = self.B2BAllocation(currBookedLoad, currParams, openDataB2B, currRec, totalCapacity, dPLF, b2cAllocationsUnits, b2bSkipFlag)

				if(isUpsellFlag and not openingFaresFlag):
					openDataB2B = {'RBD':['NA'], 'Fare':['NA'], 'FareResult':['B2B allocation skipped - Flight in upsell'],'anchorFare': ['NA'],'statusInd':[1], 'allocationStatus': [1]}
					self.updateRunAuditb2b("Done", currParams['FlightNumber'], dateFormatted)
     
				if(b2bSkipFlag and not openingFaresFlag):
					openDataB2B = {'RBD':['NA'], 'Fare':['NA'], 'FareResult':['B2B allocation skipped - B2B Factor is -1'],'anchorFare': ['NA'],'statusInd':[1], 'allocationStatus': [1]}
					self.updateRunAuditb2b("Done", currParams['FlightNumber'], dateFormatted)
				
				#Ends statsusInd == 0
				if self.thresholdflag:
					currRec.extend(['L', '', -1, -1, 0, -1, -1, 'P', '', -1, -1, 0, -1, -1])
				else:
					currRec.extend(['L', '', -1, -1, -1, -1, -1, 'P', '', -1, -1, -1, -1, -1])
     
				self.AUFileData.append(currRec)
				
				if (self.inputSource['isUpdateNavitaire']) and (self.NavitaireUpdateMethod==CONST_API):
					self.updateNavitaire(currRec)
     
				if (self.inputSource['isUpdateNavitaire']) and (self.NavitaireUpdateMethod==CONST_SFTP):
					self.updateNavSFTP(currRec)
		
			self.allocResults = self.allocResults.append(
       		{
          		"Id":b2cRunId, 
            	"RunId":self.runId,
             	"Origin":currParams['Origin'],
              	"Destin":currParams['Destin'],
               	"FltNum":str(currFlt),
                "DepDate":str(currFltDate),
                "BookedLoad":str(currBookedLoad),
                "TgtLoad":str(tgtBookedLoad),
                'TimeRange': str(b2c_time_range),
                'dlfBand': str(dplfBand),
                'ndoBand': str(ndoBand),
                'anchorFare':str(openDataB2C["anchorFare"][0]),
                "MktFare_Min":str(round(float(lowFareB2C))),
                'MktFare_Max': hiFareB2C,
                "OpenRBD":str(openDataB2C['RBD'][0]),
                "SellingFare":str(openDataB2C["Fare"][0]),
                'Channel':'B2C',
                'HowDetermined':str(openDataB2C['FareResult'][0]),
                'criteria': b2c_criteria,
                'fareOffset': b2c_offset,
                'Backstop': B2CBackstop,
                'AirlineCode_Min': str(lowAirlineCodeB2C),
                'AirlineCode_Max': str(hiAirlineCodeB2C),
                'FlightNumber_Min': str(lowFilghtNumB2C),
                'FlightNumber_Max': str(hiFilghtNumB2C),
                'Variance': str(curveReference),
                'StrategyReference': str(b2c_strategy),
                'analystName': currParams['analystName'],
                'bookedPlf': str(plf),
                'startTime': b2c_startTime,
                'endTime': b2c_endTime,
                'allocationStatus': (openDataB2C['allocationStatus'][0]), 
				'endOfDaydlfBand': expectedEndOfDayDplfBandb2c, 
				'endOfDayTgtLoad': endOfDayTgtBookedLoadb2c, 
				'strategyFare': startegyFareb2c,
				'lastSellingFare': lastSoldFareb2c, 
				'upsell': upsellb2c,
				'bookingToday': bookingTodayb2c,
				'expectedEndOfDayBooking': expectedEndOfDayBookingb2c,
				'expectedEndOfDayLoadFactor': endOfDayBookingsb2c
           	}, ignore_index = True)
			self.insertSummaryRow()
			
			self.allocResults = self.allocResults.append(
       		{
             	"Id":b2bRunId,
              	"RunId":self.runId,
               	"Origin":currParams['Origin'],
                "Destin":currParams['Destin'],
                "FltNum":str(currFlt),
                "DepDate":str(currFltDate), 
                "BookedLoad":str(currBookedLoad), 
                "TgtLoad":str(tgtBookedLoad), 
                'TimeRange': str(b2b_time_range), 
                'dlfBand': str(dplfBand), 
                'ndoBand': str(ndoBand), 
                'anchorFare':str(openDataB2B["anchorFare"][0]), 
                "MktFare_Min":str(round(float(lowFareB2B))), 
                'MktFare_Max': hiFareB2B, 
                "OpenRBD":str(openDataB2B['RBD'][0]), 
                "SellingFare":str(openDataB2B["Fare"][0]), 
                'Channel':'B2B', 
                'HowDetermined':str(openDataB2B['FareResult'][0]), 
                'criteria': b2b_criteria, 
                'fareOffset': b2b_offset, 
                'Backstop': B2BBackstop, 
                'AirlineCode_Min': str(lowAirlineCodeB2B), 
                'AirlineCode_Max': str(hiAirlineCodeB2B), 
                'FlightNumber_Min': str(lowFilghtNumB2B), 
                'FlightNumber_Max': str(hiFilghtNumB2B), 
                'Variance': str(curveReference), 
                'StrategyReference': str(b2b_strategy), 
                'analystName': currParams['analystName'],
                'bookedPlf': str(plf),
                'startTime': b2b_startTime, 
                'endTime':  b2b_endTime, 
                'allocationStatus': (openDataB2B['allocationStatus'][0]), 
				'endOfDaydlfBand': expectedEndOfDayDplfBandb2b, 
				'endOfDayTgtLoad': endOfDayTgtBookedLoadb2b,
				'lastSellingFare': lastSoldFareb2b, 
				'upsell': upsellb2b, 
				'strategyFare': startegyFareb2b,
				'bookingToday': bookingTodayb2b,
				'expectedEndOfDayBooking': expectedEndOfDayBookingb2b,
				'expectedEndOfDayLoadFactor': endOfDayBookingsb2b
        	}, ignore_index = True)
			self.insertSummaryRow()
				
		else:
			if (str(currParams['DOW'])[dow:(dow+1)] == "9"):
				self.updateRunAudit("Done", "Done", currParams['FlightNumber'], dateFormatted)
			self.logger.info("Run Parameters requested to skip - (FlightNumber, DOW, or Status)")

	def getFares(self, currBookedLoad, currFltDate, currParams, ndocounter, totalCapacity, dplfBand, channel, plf, currFltDepartureTime):
		lastSoldFare = 0
		farePrice = self.getLastSellingFare(currFltDate, currParams)
		bookings = self.getNumberOfBookings(currFltDate, currParams)
		if(farePrice.shape[0] == 0):
			lastSoldFare = 0
		else:
			if channel == 'B2C':
				lastSoldFare = farePrice["B2Cfare"][0]
			if channel =='B2B':
				lastSoldFare = farePrice["B2Bfare"][0]
		if(bookings.shape[0] == 0):
			lastBookloadTillMidnight = 0
		else:
			lastBookloadTillMidnight = bookings["BookingsCount"][0]

		self.logger.info("Last Selling Fare is "+ str(lastSoldFare))
		self.logger.info("Number of Bookings is "+ str(lastBookloadTillMidnight))
		departureTime = currFltDepartureTime

		self.getCurrentTime(self.inputSource["currentTime"], currParams, currFltDate)

		curTime = datetime.strptime(str(self.currentTime), TIME_FORMAT)
		deptTime = datetime.strptime(str(departureTime), TIME_FORMAT)

		area = self.getArea(curTime.hour)
	
		self.logger.info("Area is:"+ str(area["sum"][0]))
		self.logger.info("current Run Time is:"+ str(curTime))
		self.logger.info("Flight Departure Time is:"+ str(deptTime))
	
		bookingToday = currBookedLoad - lastBookloadTillMidnight
		expectedEndOfDayBooking = lastBookloadTillMidnight + ((bookingToday / area["sum"][0]) * 100)
		endOfDayTgtBookedLoadList = self.getTargetBookedLoadFromBenchmarkCurve(ndocounter - 1, currParams['CurveID'])
		endOfDayTgtBookedLoad = float(endOfDayTgtBookedLoadList['LF'][0])
		expectedEndOfDaydPLF = ((expectedEndOfDayBooking/totalCapacity) - endOfDayTgtBookedLoad) * 100
		expectedEndOfDayDplfBand = self.getdpflBand(expectedEndOfDaydPLF)
		
		self.logger.info("endOfDayTgtBookedLoad is: "+ str(endOfDayTgtBookedLoad))
		self.logger.info("expectedEndOfDaydPLF is: "+ str(expectedEndOfDaydPLF))
		self.logger.info("expectedEndOfDayDplfBand is: "+ str(expectedEndOfDayDplfBand))

		if(ndocounter==0):
			noon = time(12, 0)
			if(deptTime.time() > noon):
				hours = (deptTime - timedelta(hours=2)).hour
				areaTillDeparture = self.getArea(hours)
				expectedEndOfDayBooking = lastBookloadTillMidnight + ((bookingToday * areaTillDeparture["sum"][0]) / area["sum"][0])
				plf = (currBookedLoad/totalCapacity) * 100
		
		self.logger.info("expectedEndOfDayBooking is: "+ str(expectedEndOfDayBooking))

		curTime = curTime.time().strftime(TIME_FORMAT)
		deptTime = deptTime.time().strftime(TIME_FORMAT)
	
		fares = self.getOpeningFareValue(dplfBand, expectedEndOfDayDplfBand, curTime, ndocounter, plf, deptTime)
  
		upsell = 0
		timeValue = 0
		if(random.choice([True, False])):
			upsell = lastSoldFare * 0.05
		if(random.choice([True, False, False, False])):
			timeValue = lastSoldFare * 0.025
    
		return lastSoldFare, endOfDayTgtBookedLoad, expectedEndOfDayDplfBand, fares, upsell, timeValue, bookingToday, expectedEndOfDayBooking

	def getMarketFareAnchorFare(self, currFltDate, currParams, avgFare, channel, startTime, endTime, DaySpan):
		mktFareRange = self.getMarketFareRange(channel,currFltDate, currParams,DaySpan,startTime,endTime)
		mktfareRange_minomaxo = self.getMarketFareRangeminomaxo(mktFareRange)

		#Does competetive fare exist
		#We only proceed if a B2C fare exists - If B2C does not exist, we will skip the whole thing even with a B2B fare
		# This is because, when we prepare the AU Grid, if we donr push inventory on to B2C RBD, then the flight will not be visible 
		# on OTA - Flag the error for correction here
		lowFare = 0
		hiFare = 0
		lowAirlineCode = "Market Fare not found"
		hiAirlineCode = "Market Fare not found"
		lowFilghtNum = "Market Fare not found"
		hiFilghtNum = "Market Fare not found"

		if(len(mktFareRange) > 0):
			lowFare = mktFareRange['gross_fare'][0]
			hiFare = mktFareRange['gross_fare'].iloc[-1]
			lowAirlineCode = mktFareRange['outbound_airline_code'][0]
			hiAirlineCode = mktFareRange['outbound_airline_code'].iloc[-1]
			lowFilghtNum = mktFareRange['outbound_flight'][0]
			hiFilghtNum = mktFareRange['outbound_flight'].iloc[-1]
			if (currParams['fareAnchor']==MINO_FARE_ANCHOR or currParams['fareAnchor']==MAXO_FARE_ANCHOR):
				lowAirlineCode = mktfareRange_minomaxo['LowFare_airline_code'][0]
				hiAirlineCode = mktfareRange_minomaxo['HiFare_airline_code'][0]
				lowFilghtNum = mktfareRange_minomaxo['LowFare_outbound_flight'][0]
				hiFilghtNum = mktfareRange_minomaxo['HiFare_outbound_flight'][0]
				lowFare = mktfareRange_minomaxo['LowFare'][0]
				hiFare = mktfareRange_minomaxo['HiFare'][0]
		
		isMarketFareAvailable = True
		if(lowFare == hiFare == 0 and avgFare != FARE_NOT_AVL):
			lowFare = avgFare
			hiFare = avgFare
		elif (lowFare == hiFare == 0 and avgFare == FARE_NOT_AVL):
			isMarketFareAvailable = False
   
		return lowFare,hiFare,lowAirlineCode,hiAirlineCode,lowFilghtNum,hiFilghtNum,isMarketFareAvailable

	def B2BAllocation(self, currBookedLoad, currParams, openDataB2B, currRec, totalCapacity, dPLF, b2cAllocationsUnits, b2bSkipFlag):
		isUpsellFlag = False
		B2BRBDs = self.fareExtract(currParams['Origin'], currParams['Destin'], 'B2B', '')

		openbuketFlag = False
		openingRBD = openDataB2B['RBD'][0]
		
		PLF = currBookedLoad / totalCapacity
		counter = 0
		#Loop Thru
		
		for b2bctr in range(B2BRBDs.shape[0]):
			RBDLabel = B2BRBDs['RBD'][b2bctr]
			b2bStatus = openDataB2B['statusInd'][0]
			currRec.append(RBDLabel)
			currRec.append('')
			currRec.append(-1)
			currRec.append(-1)
			
			if(b2bSkipFlag):
				currRec.append(-1)
			else:
				if RBDLabel == openingRBD:
					openbuketFlag = True
				
				if (openbuketFlag):
					#Allocation
					if((b2bStatus == 0) or (b2bStatus == 2)):
						if(dPLF < 0 and PLF < self.PLF_CURVE_VALUE):
							currRec.append(int(self.generateRoundNumber(b2cAllocationsUnits[counter])))
						else:
							currRec.append(0)
							isUpsellFlag = True
						counter = counter + 1
					else:
						#No B2B Allocaion - Leave it to B2C Fare
						currRec.append(0)
				else:
					if b2bStatus == 3:
						currRec.append(-1)
					else:
						currRec.append(0)

			currRec.append(-1)
			currRec.append(-1)
		return isUpsellFlag

	def B2CAllocation(self, currBookedLoad, currParams, openDataB2C, currRec, totalCapacity, bookedDeficit, dPLF, remainingCapacity, ndocounter, SkippingFactor):
		
		b2cAllocationsUnits = []
		B2CRBDs = self.fareExtract(currParams['Origin'], currParams['Destin'], 'B2C', '')
  
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
		jumpPercent = SkippingFactor
		#reset
		#Loop Thru
		while b2cctr < B2CRBDs.shape[0] or b2bctr < self.B2B_RDB_COUNT :
			
			if b2cctr < B2CRBDs.shape[0]:
				RBDLabel = B2CRBDs['RBD'][b2cctr] 
				remainingRBD = maxRBDIndex - openRBDIndex
				firstRDBAllocation = int(currParams['FirstRBDAlloc'])
				otherRDBAllocation = int(currParams['OtherRBDAlloc'])
				price = B2CRBDs['Total'][b2cctr]
				b2bFactor = float(currParams['B2BFactor'])
				nextPrice = price
				if(size > b2cctr):
					nextPrice = B2CRBDs['Total'][b2cctr + 1]
				
				currRec.append(RBDLabel)
				currRec.append('')
				currRec.append(-1)
				currRec.append(-1)
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
					currRec.append(int(incremental))
				b2cAllocationsUnits.append(incrementalb2b)
				b2bctr = b2bctr + 1
				
			else:
				currRec.append(0)
    
			if b2cctr < B2CRBDs.shape[0]:						
				currRec.append(-1)
				currRec.append(-1)
				b2cctr= b2cctr + 1
			
		return b2cAllocationsUnits

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
		maxLimit = self.generateMaxValue(remainingCapacity, ndocounter)
		if(entryRDB):
			if(otherRDBAllocation > 0 and openingRBDPirce - maxRBDPrice != 0):
				remainingRBD = math.log(maxRBDPrice/openingRBDPirce, (1+otherRDBAllocation/100))

			d3Allocation = self.dynamicAllocationD3Formula(dPLF, remainingCapacity, remainingRBD, ndocounter)
			if(d3Allocation <= self.MIN_D3_D4_VALUE):
				d3Allocation = self.MIN_D3_D4_VALUE
			elif(d3Allocation >= maxLimit):
				d3Allocation = maxLimit
			entryRDB = False
   
		openRBDIndex = openRBDIndex + 1

		d4Allocation = self.dynamicAllocationD4Formula(dPLF, openRBDIndex, d3Allocation)

		if(d4Allocation <= self.MIN_D3_D4_VALUE):
			d4Allocation = self.MIN_D3_D4_VALUE
		elif(d4Allocation >= maxLimit):
			d4Allocation = maxLimit

		return openRBDIndex, entryRDB, d3Allocation, d4Allocation, remainingRBD

	def dynamicAllocationD3Formula(self, dPLF, remainingCapacity, remainingRBD, ndocounter):
		return int(round((remainingCapacity/remainingRBD) * (1/(1+dPLF+0.001)) / math.exp(3*dPLF/((ndocounter/75)+0.1)),0))

	def dynamicAllocationD4Formula(self, dPLF, rbdPosition, d3Allocation):
		return int(round(d3Allocation * math.exp((dPLF*rbdPosition)/3),0))
 
	def generateMaxValue(self, remainingCapacity, ndocounter):
		return int(round(max((remainingCapacity/(1+ndocounter)),15),0))

	def fareExtract(self, origin, destination, channel, openingRBD):

		RBDQuery = "select ROW_NUMBER() OVER (ORDER BY TOTAL) AS id, RBD, Total from Fares where Org = '" + origin + "' and Dst = '" + destination + "' "
		
		if channel == 'B2C':
			RBDQuery = RBDQuery + " and LEFT(RBD,1) != 'Z' "
		if channel == 'B2B':
			RBDQuery = RBDQuery + " and LEFT(RBD,1) = 'Z' "
		if openingRBD != '':
			RBDQuery = RBDQuery + " and RBD = '" + openingRBD + "'"

		RBDQuery = RBDQuery + " order by Total asc"
		try:
			queryResult = pd.read_sql(RBDQuery, self.conn)
		except Exception as e:
			self.logger.error(e)
 
		return queryResult

	def determineRBDToOpen(self, origin, destin, anchorFare, offset, channel, backstop, flightNumber, currFltDate, openingFaresVariable, dplfBand, ndocounter, currBookedLoad, totalCapacity, currParams, plf, currFltDepartureTime):
		lastSoldFare = 'NA'
		endOfDayTgtBookedLoad = 'NA'
		expectedEndOfDayDplfBand = 'NA'
		upsell = 'NA'
		bookingToday = 'NA'
		expectedEndOfDayBooking = 'NA'
		startegyFare = 'NA'
		endOfDayBookings = 'NA'
  
		openResult = pd.DataFrame(columns=['RBD', 'Fare', 'FareResult', 'anchorFare', 'statusInd', 'allocationStatus'])
		
		howDetermined = ""
		openingFaresFlag = False
		openResult, rbddata = self.getValueWithOffset(origin, destin, anchorFare, offset, channel, flightNumber, currFltDate, openResult)
		if(rbddata is None):
			return openResult, False, lastSoldFare, endOfDayTgtBookedLoad, expectedEndOfDayDplfBand, upsell, bookingToday, expectedEndOfDayBooking, startegyFare, endOfDayBookings
		
		if(rbddata.shape[0] == 0):
			openResult = self.extremeFaresSkipping(origin, destin, anchorFare, offset, channel, backstop, flightNumber, currFltDate, openResult)
		else:
			openResult = self.getFaresWithBackstop(origin, destin, anchorFare, offset, channel, backstop, flightNumber, currFltDate, openResult, rbddata, openingFaresFlag, howDetermined, expectedEndOfDayDplfBand, dplfBand, ndocounter, plf)
		startegyFare = openResult['Fare'][0]
  
		# Last selling fare
		if(int(openingFaresVariable) == 1):
			self.logger.info("Anchor fare is : " + str(anchorFare))
			if(startegyFare != 'NA'):
				anchorFare = startegyFare
				#Get Last Selling B2B and B2C Fare
				lastSoldFare, endOfDayTgtBookedLoad, expectedEndOfDayDplfBand, fares, upsell, timeValue, bookingToday, expectedEndOfDayBooking = self.getFares(currBookedLoad, currFltDate, currParams, ndocounter, totalCapacity, dplfBand, channel, plf, currFltDepartureTime)
				endOfDayTgtBookedLoad = endOfDayTgtBookedLoad * totalCapacity
				endOfDayBookings = (float(expectedEndOfDayBooking) / float(totalCapacity)) * 100
				if(len(fares) > 0):
					if channel == 'B2C':
						openingValue = fares["B2CValue"][0]
					if channel == 'B2B':
						openingValue = fares["B2BValue"][0]
					self.logger.info("Booking Velocity value is :" + openingValue)
					anchorFare, howDetermined, offset, upsell = self.getFinalAnchorFare(openingValue, float(anchorFare), upsell, timeValue, float(lastSoldFare), offset)
					if(anchorFare == 0):
						if(ndocounter == 0):
							howDetermined = str(self.currentTime) + '_Load Factor_' + str(plf) + '_' + howDetermined
						else:
							howDetermined = str(self.currentTime) + '_' + str(dplfBand) + '_' + str(expectedEndOfDayDplfBand) + '_' + howDetermined
						openResult = {'RBD':['NA'], 'Fare':['NA'], 'FareResult':[howDetermined],'anchorFare': ['NA'],'statusInd':[1], 'allocationStatus': [1]}
						return openResult, openingFaresFlag, lastSoldFare, endOfDayTgtBookedLoad, expectedEndOfDayDplfBand, upsell, bookingToday, expectedEndOfDayBooking, startegyFare, endOfDayBookings
					openingFaresFlag = True
		
					openResult, rbddata = self.getValueWithOffset(origin, destin, anchorFare, offset, channel, flightNumber, currFltDate, openResult)
					
					if(rbddata.shape[0] == 0):
						openResult = self.extremeFaresSkipping(origin, destin, anchorFare, offset, channel, backstop, flightNumber, currFltDate, openResult)
					else:
						openResult = self.getFaresWithBackstop(origin, destin, anchorFare, offset, channel, backstop, flightNumber, currFltDate, openResult, rbddata, openingFaresFlag, howDetermined, expectedEndOfDayDplfBand, dplfBand, ndocounter, plf)
   

		return openResult, openingFaresFlag, lastSoldFare, endOfDayTgtBookedLoad, expectedEndOfDayDplfBand, upsell, bookingToday, expectedEndOfDayBooking, startegyFare, endOfDayBookings

	def getFaresWithBackstop(self, origin, destin, anchorFare, offset, channel, backstop, flightNumber, currFltDate, openResult, rbddata, openingFaresFlag, howDetermined, endOfDayDplfBand, dplfBand, ndocounter, plf):
		backstopFlag = False
		extremeFareFlag = False
		# We got back data purely on the basis of anchor fare - comparison with offset remains which is done in the subsequent section

		# Calculating the offset that is required based on entries remaining in the RBD table
		offsetToFetch = min(abs(int(offset)), rbddata.shape[0] - 1)
		if(int(offset) < 0):
			offsetToFetch = min(abs(int(offset)+1), rbddata.shape[0] - 1) 

		# Comparison with the backstop table - to ensure we are picking a value higher than the backstop
		defFareData = self.fetchExtremePublishedFare(origin, destin, channel, 'LOW', backstop)
	
		if(defFareData.shape[0] != 0):
			if(rbddata['Total'][0] < defFareData['Total'][0] and backstop > 0):
				backstopFlag = True
			
		# Edge case if the backstop values are placed very high - in which case it will place the backstop at Z0 and Y levels 
		if(defFareData.shape[0] == 0):
			defFareData = self.fetchExtremePublishedFare(origin, destin, channel, 'HI', 0)
		
		if(defFareData.shape[0] == 0):
			if (channel == 'B2B'):
				rbdLevel = self.HIGHEST_B2B_RBD_VALUE
			if (channel == 'B2C'):
				rbdLevel = self.HIGHEST_B2C_RBD_VALUE
    
			openResult = {'RBD':['NA'], 'Fare':['NA'], 'FareResult':['Market Fare higher than '+ rbdLevel +' levels - skipping ' + channel + ' allocation'],'anchorFare': [anchorFare],'statusInd':[1], 'allocationStatus': [1]}
			self.updateRunAuditb2c("Done", flightNumber, currFltDate)
			return openResult
		else:
			if(rbddata['RBD'][0] == self.LOWEST_B2B_RBD_VALUE or rbddata['RBD'][0] == self.LOWEST_B2C_RBD_VALUE):
				defFareData = self.fetchExtremePublishedFare(origin, destin, channel, 'LOW', backstop)
				if(rbddata['RBD'][0] == defFareData['RBD'][0]):
					extremeFareFlag = True
					RBDtoOpen = rbddata['RBD'][0]
					FareToOpen = rbddata['Total'][0]
				else:
					backstopFlag = True
					RBDtoOpen = defFareData['RBD'][0]
					FareToOpen = defFareData['Total'][0]
			else:
				if(float(defFareData['Total'][0]) >= float(rbddata['Total'][offsetToFetch])):
					offsetToFetch=min(len(defFareData['Total'])-1,offsetToFetch)
					if(rbddata['Total'][0] < defFareData['Total'][offsetToFetch] and backstop > 0):
						backstopFlag = True
					RBDtoOpen = defFareData['RBD'][0]
					FareToOpen = defFareData['Total'][0]
				else:
					RBDtoOpen = rbddata['RBD'][offsetToFetch]
					FareToOpen = rbddata['Total'][offsetToFetch]		
					
		self.logger.info('Determining ' + channel + ' Fare ' + str(FareToOpen) + " " + str(RBDtoOpen))

		if (channel == 'B2B'):
			#Special handling for B2B - if the B2B Anchor fare is higher than Z0 - Skip B2B
			limitB2BFare_Data = self.fetchExtremePublishedFare(origin, destin, channel, 'HI', 0)
			limitB2BFare = float(limitB2BFare_Data['Total'][0])

			if (float(anchorFare) > limitB2BFare):
				#Outside of B2B Levels - stick with B2C
				openResult = {'RBD':['NA'], 'Fare':['NA'], 'FareResult':['Market Fare higher than '+ self.HIGHEST_B2B_RBD_VALUE +' levels - skipping B2B allocation'],'anchorFare': [anchorFare],'statusInd':[1], 'allocationStatus': [1]}
				self.updateRunAuditb2b("Done", flightNumber, currFltDate)
				return openResult
    
			else:
				if(offsetToFetch == rbddata.shape[0]):
					openResult = {'RBD':[RBDtoOpen], 'Fare':[str(FareToOpen)], 'FareResult':['Not Enough Levels available - taking the Max/Min Available'],'anchorFare': [anchorFare],'statusInd':[2], 'allocationStatus': [1]}			
					return openResult
				else:
					if(extremeFareFlag):
						openResult = {'RBD':[RBDtoOpen], 'Fare':[str(FareToOpen)], 'FareResult':['Unable to Match Mkt - B2B allocation at extreme value'],'anchorFare': [anchorFare],'statusInd':[0], 'allocationStatus': [1]}			
						return openResult
					else:
						if(backstopFlag):
							openResult = {'RBD':[RBDtoOpen], 'Fare':[str(FareToOpen)], 'FareResult':['Matched and allocated - Backstop Hit'],'anchorFare': [anchorFare],'statusInd':[0], 'allocationStatus': [1]}			
							return openResult
						else:
							if(openingFaresFlag):
								if(ndocounter == 0):
									howDetermined = str(self.currentTime) + '_Load Factor_' + str(plf) + '_' + howDetermined
								else:
									howDetermined = str(self.currentTime) + '_' + str(dplfBand) + '_' + str(endOfDayDplfBand) + '_' + howDetermined
								openResult = {'RBD':[RBDtoOpen], 'Fare':[str(FareToOpen)], 'FareResult':[howDetermined],'anchorFare': [anchorFare],'statusInd':[0], 'allocationStatus': [1]}
								return openResult
							else:
								openResult = {'RBD':[RBDtoOpen], 'Fare':[str(FareToOpen)], 'FareResult':['Matched and allocated'],'anchorFare': [anchorFare],'statusInd':[0], 'allocationStatus': [1]}			
								return openResult
    
		else:
			if(offsetToFetch == rbddata.shape[0]):
				openResult = {'RBD':[RBDtoOpen], 'Fare':[str(FareToOpen)], 'FareResult':['Not Enough Levels available - taking the Max/Min Available'],'anchorFare': [anchorFare],'statusInd':[2], 'allocationStatus': [1]}			
				return openResult
			else:
				if(extremeFareFlag):
					openResult = {'RBD':[RBDtoOpen], 'Fare':[str(FareToOpen)], 'FareResult':['Unable to Match Mkt - B2C allocation at extreme value'],'anchorFare': [anchorFare],'statusInd':[0], 'allocationStatus': [1]}			
					return openResult
				else:
					if(backstopFlag):
						openResult = {'RBD':[RBDtoOpen], 'Fare':[str(FareToOpen)], 'FareResult':['Matched and allocated - Backstop Hit'],'anchorFare': [anchorFare],'statusInd':[0], 'allocationStatus': [1]}			
						return openResult
					else:
						if(openingFaresFlag):
							if(ndocounter == 0):
								howDetermined = str(self.currentTime) + '_Load Factor_' + str(plf) + '_' + howDetermined
							else:
								howDetermined = str(self.currentTime) + '_' + str(dplfBand) + '_' + str(endOfDayDplfBand) + '_' + howDetermined
							openResult = {'RBD':[RBDtoOpen], 'Fare':[str(FareToOpen)], 'FareResult':[howDetermined],'anchorFare': [anchorFare],'statusInd':[0], 'allocationStatus': [1]}			
							return openResult
						else:
							openResult = {'RBD':[RBDtoOpen], 'Fare':[str(FareToOpen)], 'FareResult':['Matched and allocated'],'anchorFare': [anchorFare],'statusInd':[0], 'allocationStatus': [1]}			
							return openResult

	def extremeFaresSkipping(self, origin, destin, anchorFare, offset, channel, backstop, flightNumber, currFltDate, openResult):
		self.logger.info("Market Fare is out of price ladder range - allocating at extreme values")
		#Fetch the Lowest or highest available fares

		defFareData = self.fetchExtremePublishedFare(origin, destin, channel, 'LOW', backstop)
		if (int(offset) >= 0):
			defFareData = self.fetchExtremePublishedFare(origin, destin, channel, "HI", backstop)

		# Edge case where backstop is placed at a very large value - allocates at Z0 or Y levels
		if (defFareData.shape[0] == 0):
			defFareData = self.fetchExtremePublishedFare(origin, destin, channel, 'HI', 0) 	

		if (channel == 'B2C'):
			openResult = {'RBD':[defFareData['RBD'][0]], 'Fare':[defFareData['Total'][0]], 'FareResult':['Unable to Match Mkt - B2C allocation at extreme value'],'anchorFare': [anchorFare],'statusInd':[2], 'allocationStatus': [1]}
			return openResult

		if(defFareData.shape[0]!=0):
			if (channel == 'B2B'):
				if (defFareData['RBD'][0] == self.HIGHEST_B2B_RBD_VALUE):
					openResult = {'RBD':['NA'], 'Fare':['NA'], 'FareResult':['Market Fare higher than '+ self.HIGHEST_B2B_RBD_VALUE +' levels - skipping B2B allocation'],'anchorFare': [anchorFare],'statusInd':[1], 'allocationStatus': [1]}
					self.updateRunAuditb2b("Done", flightNumber, currFltDate)
					return openResult
				else:
					openResult = {'RBD':[defFareData['RBD'][0]], 'Fare':[defFareData['Total'][0]], 'FareResult':['Unable to Match Mkt - B2B allocation at extreme value'],'anchorFare': [anchorFare],'statusInd':[2], 'allocationStatus': [1]}
					return openResult
		else:
			openResult = {'RBD':['NA'], 'Fare':['NA'], 'FareResult':['Unable to fetch extreme published Fares'],'anchorFare': [anchorFare],'statusInd':[2], 'allocationStatus': [0]}
			self.updateRunAuditb2b("Done", flightNumber, currFltDate)
			return openResult

	def getValueWithOffset(self, origin, destin, anchorFare, offset, channel, flightNumber, currFltDate, openResult):
		self.logger.info("Anchor Fare is " + str(anchorFare) + " with an offset of " + str(offset))

		RBDQuery = "select RBD, Total from Fares where Org = '" + origin + "' and Dst = '" + destin + "' "
		try:
			rbddata = pd.read_sql(RBDQuery, self.conn)
		except Exception as e:
			self.logger.error(e)
		if(rbddata.shape[0] == 0):
			self.logger.info('QP Fare not available. Please validate Fares table')
			openResult = {'RBD':['NA'], 'Fare':['NA'], 'FareResult':['QP Fares not available'],'anchorFare': [anchorFare],'statusInd':[1], 'allocationStatus': [1]}
			self.updateRunAudit("Done", "Done", flightNumber, currFltDate)
			return openResult, None
	
		if channel == 'B2C':
			RBDQuery = RBDQuery + " and LEFT(RBD,1) != 'Z'"
		if channel == 'B2B':
			RBDQuery = RBDQuery + " and LEFT(RBD,1) = 'Z'"

		sortorder = ' desc '

		if (int(offset) < 0):
			RBDQuery = RBDQuery + " and Total < " + str(anchorFare)
			sortorder = ' desc '
		if (int(offset) >= 0):
			anchorFare = float(anchorFare)
			if (anchorFare > 0):
				RBDQuery = RBDQuery + " and Total >= " + str(anchorFare)
			else:
				# Below condition forces zero records to be returned
				RBDQuery = RBDQuery + " and Total >= 1000000"
			sortorder = ' asc '

		RBDQuery = RBDQuery + " order by Total " + sortorder
		if(self.debugMode):
			self.logger.info(RBDQuery)

		try:
			rbddata = pd.read_sql(RBDQuery, self.conn)
		except Exception as e:
			self.logger.error(e)
		return openResult, rbddata

	def fetchExtremePublishedFare(self, origin, destin, channel, type, backstop):

		RBDQuery = "select RBD, Total from Fares where Org = '" + origin + "' and Dst = '" + destin + "'" 
		if channel == 'B2C':
			if type == 'LOW':
				RBDQuery = RBDQuery + " and LEFT(RBD,1) != 'Z' and Total >= '" + str(backstop) + "' order by Total asc" 
			else:
				RBDQuery = RBDQuery + " and LEFT(RBD,1) != 'Z' and Total >= '" + str(backstop) + "' order by Total desc"
		else:
			if type == 'LOW':
				RBDQuery = RBDQuery + " and LEFT(RBD,1) = 'Z' and Total >= '" + str(backstop) + "' order by Total asc"
			else:
				RBDQuery = RBDQuery + " and LEFT(RBD,1) = 'Z' and Total >= '" + str(backstop) + "' order by Total desc"
		try:
			defFareData = pd.read_sql(RBDQuery, self.conn)
		except Exception as e:
			self.logger.error(e)

		return defFareData

	def getMarketFareRange(self, channel, currFltDateStr, currParams,DaySpan,startTime,endtime):
		
		currFltDate = datetime.strptime(currFltDateStr, DATE_FORMAT).date()
		startDateTime=f"{currFltDate} {startTime}"
		enddateTime=f"{currFltDate} {endtime}"
		updateDate=currFltDate+timedelta(days=int(DaySpan))
		if currFltDate>updateDate:
			startDateTime=f"{updateDate} {startTime}"
			enddateTime=f"{currFltDate} {endtime}"
		elif currFltDate<updateDate:
			startDateTime=f"{currFltDate} {startTime}"
			enddateTime=f"{updateDate} {endtime}"
		mktLowestQuery = "Select gross_fare, outbound_airline_code, outbound_flight ,outbound_departuretime from " + self.TABLE_MARKET_FARES + " where source_airport = '" 
		mktLowestQuery = mktLowestQuery + currParams['Origin'] + "' and destination_airport = '" + currParams['Destin'] + "'"

		mktLowestQuery = mktLowestQuery + f"AND CAST(CONCAT(departure_date, ' ', outbound_departuretime) AS DATETIME) <= CAST('{enddateTime}' AS DATETIME) AND CAST(CONCAT(departure_date, ' ', outbound_departuretime) AS DATETIME) >= CAST('{startDateTime}' AS DATETIME)"
		mktLowestQuery = mktLowestQuery + " and channel = '" + channel + "' "
		mktLowestQuery = mktLowestQuery + " and outbound_airline_code <> '" + currParams['CarrExlusion'] + "' AND outbound_airline_code != 'QP' AND outbound_airline_code!='G8' AND outbound_stop = 0"
		if(self.inputSource[CONST_INPUT_SOURCE][CONST_MARKET_FARES] != TEST_DATA):
			mktLowestQuery = mktLowestQuery + " AND created_at > NOW() - INTERVAL 2 DAY "
		mktLowestQuery = mktLowestQuery + " ORDER BY gross_fare ASC"
		try:
			mktFareRange = pd.read_sql(mktLowestQuery, self.conn)
		except Exception as e:
			self.logger.error(e)

		return mktFareRange

	def getMarketFareRangeminomaxo(self, mktFareRange):
		data = pd.DataFrame(mktFareRange)
		fare_l1 = data["gross_fare"].tolist()
		fare_list = []
		if len(fare_l1)>1:
			coveragevalue = int(self.COVERAGE_PARAMETER)
			MED = np.median(fare_l1)
			MAD = np.median(np.abs(fare_l1- np.median(fare_l1)))
			if MAD==0:
				MAD=0.15*MED
			for fare in fare_l1:
				MZS = np.abs(fare - MED)/MAD
				if MZS < coveragevalue:
					fare_list.append(fare)
			min_fare = min(fare_list)
			max_fare = max(fare_list)
			min_fare_row = data[data["gross_fare"] == min_fare].iloc[0]
			max_fare_row = data[data["gross_fare"] == max_fare].iloc[0]
			min_airline_code = min_fare_row["outbound_airline_code"]
			min_outbound_flight = min_fare_row["outbound_flight"]
			max_airline_code = max_fare_row["outbound_airline_code"]
			max_outbound_flight = max_fare_row["outbound_flight"]
		elif len(fare_list)==0:
			min_fare=0
			max_fare=0
			min_airline_code = 'Market Fare not found'
			min_outbound_flight = 'Market Fare not found'
			max_airline_code = 'Market Fare not found'
			max_outbound_flight = 'Market Fare not found'
		else:
			min_fare=fare_list[0]
			max_fare=fare_list[0]
			min_fare_row = data[data["gross_fare"] == min_fare].iloc[0]
			max_fare_row = data[data["gross_fare"] == max_fare].iloc[0]
			min_airline_code = min_fare_row["outbound_airline_code"]
			min_outbound_flight = min_fare_row["outbound_flight"]
			max_airline_code = max_fare_row["outbound_airline_code"]
			max_outbound_flight = max_fare_row["outbound_flight"]
		mktFareRange = pd.DataFrame(
			{'LowFare': [min_fare], 'HiFare': [max_fare],'LowFare_airline_code': [min_airline_code], 'LowFare_outbound_flight': [min_outbound_flight], 'HiFare_airline_code': [max_airline_code], 'HiFare_outbound_flight': [max_outbound_flight]})
		return mktFareRange

	def getLastSellingFare(self, currFltDateStr, currParams):
	
		currFltDate = datetime.strptime(currFltDateStr, DATE_FORMAT_2).date()	
		# self.logger.info(self.getCurrentDate())
		
		if self.inputSource[CONST_INPUT_SOURCE][CONST_BOOKLOAD] == TEST_DATA:
			query=f'''
   				select B2Bfare,B2Cfare from booking_fare_data where FlightDate = '{currFltDate}' AND FlightNumber = '{currParams["FlightNumber"]}' AND Origin = '{currParams["Origin"]}' AND Destin = '{currParams["Destin"]}'
   			'''
			conn=self.conn
			farePrice = pd.read_sql(query, conn)


		if self.inputSource[CONST_INPUT_SOURCE][CONST_BOOKLOAD] == DB:
			query = f'''
					EXEC {self.NAV_DB_WB_NAME}[dbo].[LastFareForFlight]
						@DepartureDate = N'{currFltDate}',
						@FlightNumber = N'{currParams["FlightNumber"]}',
						@DepartureStation = N'{currParams["Origin"]}',
						@ArrivalStation = N'{currParams["Destin"]}'
			'''
			farePrice = pd.read_sql(query, self.NavitaireConnection)

			# payload = {
            # "query":query
			# }
			# headers = {
			# 	'x-api-key': self.API_KEY
			# }
			# response = requests.request("POST",self.API_URL,headers=headers, data=json.dumps(payload))
			# if response.status_code == 200:
			# 	data = json.loads(response.text)
			# 	farePrice = pd.DataFrame(data)
			# else:
			# 	farePrice = pd.DataFrame(columns=['B2Bfare', 'B2Cfare'])
    
		return farePrice

	def getNumberOfBookings(self, currFltDateStr, currParams):
	
		currFltDate = datetime.strptime(currFltDateStr, DATE_FORMAT_2).date()	
		# self.logger.info(self.getCurrentDate())
		
		if self.inputSource[CONST_INPUT_SOURCE][CONST_BOOKLOAD] == TEST_DATA:
			query=f'''
   				select BookingsCount,STD from booking_fare_data where FlightDate = '{currFltDate}' AND FlightNumber = '{currParams["FlightNumber"]}' AND Origin = '{currParams["Origin"]}' AND Destin = '{currParams["Destin"]}'
   			'''
			conn=self.conn
			farePrice = pd.read_sql(query, conn)


		if self.inputSource[CONST_INPUT_SOURCE][CONST_BOOKLOAD] == DB:
			currDate = self.getCurrentDate()
			query = f'''
					EXEC {self.NAV_DB_WB_NAME}[dbo].[GetBookingsCount]
						@DepartureDate = N'{currFltDate}',
						@FlightNumber = N'{currParams["FlightNumber"]}',
						@DepartureStation = N'{currParams["Origin"]}',
						@ArrivalStation = N'{currParams["Destin"]}',
						@ComparisonDate = N'{currDate}'
			'''
			farePrice = pd.read_sql(query, self.NavitaireConnection)

			# payload = {
            # "query":query
			# }
			# headers = {
			# 	'x-api-key': self.API_KEY
			# }
			# response = requests.request("POST",self.API_URL,headers=headers, data=json.dumps(payload))
			# if response.status_code == 200:
			# 	data = json.loads(response.text)
			# 	farePrice = pd.DataFrame(data)
			# else:
			# 	farePrice = pd.DataFrame(columns=['BookingsCount', 'STD'])
    
		return farePrice

	def getOpeningFareValue(self, dplfBand, endOfDayDplfBand, currentTime, ndocounter, loadFactor, departureTime):
	
		query = f'''
			SELECT B2CValue, B2BValue 
			FROM config_opening_fares 
			WHERE 1 = 1 
   			AND IFNULL(TIME(JSON_UNQUOTE(JSON_EXTRACT(conditions, '$.start_time'))), '00:00') <= '{currentTime}' 
			AND IFNULL(TIME(JSON_UNQUOTE(JSON_EXTRACT(conditions, '$.end_time'))), '23:59') >= '{currentTime}' 
			AND FIND_IN_SET('{ndocounter}',JSON_UNQUOTE(JSON_EXTRACT(conditions, '$.ndo_band'))) 
			AND FIND_IN_SET('{dplfBand}',IFNULL(JSON_UNQUOTE(JSON_EXTRACT(conditions, '$.start_of_day_dlf_band')), '{dplfBand}')) 
			AND FIND_IN_SET('{endOfDayDplfBand}',IFNULL(JSON_UNQUOTE(JSON_EXTRACT(conditions, '$.end_of_day_dlf_band')), '{endOfDayDplfBand}')) 
			AND IFNULL(JSON_UNQUOTE(JSON_EXTRACT(conditions, '$.min_load_factor')), 0) <= {loadFactor} 
			AND IFNULL(JSON_UNQUOTE(JSON_EXTRACT(conditions, '$.max_load_factor')), 100) >= {loadFactor} 
			AND IFNULL(JSON_UNQUOTE(JSON_EXTRACT(conditions, '$.departure_start_time')), '00:00') <= '{departureTime}' 
			AND IFNULL(JSON_UNQUOTE(JSON_EXTRACT(conditions, '$.departure_end_time')), '23:59') >= '{departureTime}' 
  		'''
		# self.logger.info(query)
		return pd.read_sql(query, self.conn)

	def getArea(self, currentHour):
		query = f''' SELECT sum(shareOfBooking) as sum FROM config_share_of_booking where hours <= {currentHour} '''
		return pd.read_sql(query, self.conn)

	def getFinalAnchorFare(self, anchorValue, anchorFare, upsell, timeValue, lastSoldFare, offset):
		anchorFares = 0
		howDetermined = ''
		if(anchorValue == 'A'):
			anchorFares =  max(anchorFare, lastSoldFare)
			if(anchorFare > lastSoldFare):
				howDetermined = 'UpsellN_AllocationY_Fare based from Strategy '
				upsell = 'NA'
			else:
				anchorFares = anchorFares + upsell
				howDetermined = 'UpsellY_AllocationY_Fare based from last sold Fare '
				offset = 0
		if(anchorValue == 'B'):
			anchorFares =  min(anchorFare, lastSoldFare)
			if(lastSoldFare == 0):
				anchorFares = anchorFare
			howDetermined = 'UpsellN_AllocationY_'
			if(anchorFare < lastSoldFare):
				howDetermined = howDetermined + 'Fare based from Strategy '
			else:
				howDetermined = howDetermined + 'Fare based from last sold Fare '
				offset = 0
			upsell = 'NA'
		if(anchorValue == 'C'):
			anchorFares =  anchorFare
			howDetermined = 'UpsellN_AllocationY_Fare based from Strategy '
			upsell = 'NA'
		if(anchorValue == 'D'):
			anchorFares =  0
			howDetermined = 'UpsellN_AllocationN_Allocations put as 0 '
			upsell = 'NA'
		if(anchorValue == 'E'):
			anchorFares =  lastSoldFare + upsell
			howDetermined = 'UpsellY_AllocationY_Fare based from last sold Fare '
			offset = 0
		if(anchorValue == 'F'):
			anchorFares =  min(anchorFare, lastSoldFare)
			if(lastSoldFare == 0):
				anchorFares = anchorFare
			howDetermined = ''
			if(anchorFare < lastSoldFare):
				howDetermined = 'UpsellN_AllocationY_Fare based from Strategy '
				upsell = 'NA'
			else:
				anchorFares = anchorFares + timeValue
				howDetermined = 'UpsellY_AllocationY_Fare based from last sold Fare '
				offset = 0
				upsell = timeValue
		return anchorFares, howDetermined, offset, upsell

	def getTargetBookedLoadFromBenchmarkCurve(self, ndocounter, curveID):
		tgtBookedLoad = []
  		#Fetch the Target Booked Load from the curve
		if ndocounter<0:
			ndocounter=0
		bookingCurveQuery = "Select LF,B2C_Avg_Fare,B2B_Avg_Fare from Curves where NDO = " + str(ndocounter) + " and CurveID = '" + curveID + "'"
		try:
			benchmark = pd.read_sql(bookingCurveQuery, self.conn)
		except Exception as e:
			self.logger.error(e)
		if(benchmark.shape[0] > 0):
			tgtBookedLoad = benchmark

		return tgtBookedLoad

	def insertSummaryRow(self):
		if self.inputSource[CONST_LOGS][CONST_RUN_SUMMARY] == DB:
			latest_row = self.allocResults.iloc[-1]

			insertQuery = f""" 
				INSERT INTO run_summary 
    			(Id, Origin, Destin, FltNum, DepDate, BookedLoad, TgtLoad, MktFare_Min, OpenRBD, SellingFare, Channel, HowDetermined, RunId, CreatedAt, MktFare_Max, fareAnchor, fareOffset, TimeRange, dlfBand, ndoBand, criteria, AirlineCode_Max, AirlineCode_Min, FlightNumber_Max, FlightNumber_Min, backstop, Variance, StrategyReference, analystName, bookedPlf, startTime, endTime, allocationStatus, endOfDaydlfBand, endOfDayTgtLoad, strategyFare, lastSellingFare, upsell, bookingToday, expectedEndOfDayBooking, expectedEndOfDayLoadFactor)  
       			VALUES (
					'{latest_row['Id']}', 
					'{latest_row['Origin']}', 
					'{latest_row['Destin']}', 
					'{latest_row['FltNum']}', 
					'{latest_row['DepDate']}', 
					'{latest_row['BookedLoad']}', 
					'{latest_row['TgtLoad']}', 
					'{str(latest_row['MktFare_Min'])}', 
					'{latest_row['OpenRBD']}', 
					'{str(latest_row['SellingFare'])}', 
					'{latest_row['Channel']}', 
					'{latest_row['HowDetermined']}', 
					'{latest_row['RunId']}', 
					'{self.getCurrentDateTime().strftime(DATE_TIME_FORMAT)}',
					'{str(latest_row['MktFare_Max'])}',
					'{str(latest_row['anchorFare'])}',
					'{str(latest_row['fareOffset'])}',
					'{str(latest_row['TimeRange'])}',
					'{str(latest_row['dlfBand'])}',
					'{str(latest_row['ndoBand'])}',
					'{str(latest_row['criteria'])}',
					'{str(latest_row['AirlineCode_Max'])}',
					'{str(latest_row['AirlineCode_Min'])}',
					'{str(latest_row['FlightNumber_Max'])}',
					'{str(latest_row['FlightNumber_Min'])}',
					'{str(latest_row['Backstop'])}',
					'{str(latest_row['Variance'])}',
					'{str(latest_row['StrategyReference'])}',
					'{str(latest_row['analystName'])}',
					'{str(latest_row['bookedPlf'])}',
					'{str(latest_row['startTime'])}',
					'{str(latest_row['endTime'])}',
					'{str(latest_row['allocationStatus'])}',
					'{str(latest_row['endOfDaydlfBand'])}',
					'{str(latest_row['endOfDayTgtLoad'])}',
					'{str(latest_row['strategyFare'])}',
					'{str(latest_row['lastSellingFare'])}',
					'{str(latest_row['upsell'])}',
					'{str(latest_row['bookingToday'])}',
					'{str(latest_row['expectedEndOfDayBooking'])}',
					'{str(latest_row['expectedEndOfDayLoadFactor'])}'
				)
			"""
			try:
				self.wrconn.execute(insertQuery)
			except Exception as e:
				self.logger.error(e)

	def getCurrentBookedLoads(self, origin, destin, startDate, endDate):
		
		if self.inputSource[CONST_INPUT_SOURCE][CONST_BOOKLOAD] == TEST_DATA:
			NavitaireQuery = "SELECT Origin, Destination, FlightNumber, DepartureDate, bookedPax, capacity,DepartureTime from " + self.BookloadTableName
			NavitaireQuery = NavitaireQuery + " where DepartureDate >= '" + str(startDate) + "' and DepartureDate <= '" +str(endDate) + "'"
			NavitaireQuery = NavitaireQuery + " and Origin = '" + origin + "' and Destination = '" + destin + "'"
			bookedData = pd.read_sql(NavitaireQuery, self.conn)
   
		if self.inputSource[CONST_INPUT_SOURCE][CONST_BOOKLOAD] == DB:
			NavitaireQuery = "select Origin, Destination, FlightNumber,DepartureDate,FORMAT(STDUTC, 'HH:MM') as DepartureTime, count(PassengerID) as bookedPax, max(Lid) as capacity from ("
			NavitaireQuery = NavitaireQuery + "select BP.PassengerID, IL.DepartureStation as Origin, IL.ArrivalStation as Destination, IL.DepartureDate,IL.FlightNumber, IL.AdjustedCapacity, IL.Lid, IL.STDUTC "
			NavitaireQuery = NavitaireQuery + " from "+self.NAV_DB_NAME+"[rez].InventoryLeg IL "
			NavitaireQuery = NavitaireQuery + " left outer join "+self.NAV_DB_NAME+"[rez].PassengerJourneyLeg PJL on  PJL.InventoryLegID = IL.InventoryLegID "
			NavitaireQuery = NavitaireQuery + " left outer join "+self.NAV_DB_NAME+"[rez].PassengerJourneySegment PJS on PJS.PassengerID = PJL.PassengerID and PJS.SegmentID = PJL.SegmentID and (PJS.FareBasis != 'SO7NBSOL' or PJS.FareBasis IS NULL) "
			NavitaireQuery = NavitaireQuery + " left outer join "+self.NAV_DB_NAME+"[rez].BookingPassenger BP on  BP.PassengerID = PJS.PassengerID "
			NavitaireQuery = NavitaireQuery + " where IL.Lid > 0 and IL.Status NOT IN (2) and IL.DepartureDate >= '" + str(startDate) + "' and IL.DepartureDate <= '" +str(endDate) + "' "
			NavitaireQuery = NavitaireQuery + " and IL.DepartureStation = '" + origin + "' and IL.ArrivalStation = '" + destin + "'"
			NavitaireQuery = NavitaireQuery + ") A group by Origin, Destination, FlightNumber, DepartureDate,STDUTC order by Origin, Destination, DepartureDate "

			bookedData = pd.read_sql(NavitaireQuery, self.NavitaireConnection)
			# payload = {
            # "query":NavitaireQuery
			# }
			# headers = {
			# 	'x-api-key': self.API_KEY
			# }
			# response = requests.request("POST",self.API_URL,headers=headers, data=json.dumps(payload))
			# if response.status_code == 200:
			# 	data = json.loads(response.text)
			# 	bookedData = pd.DataFrame(data)
			# else:
			# 	bookedData = pd.DataFrame(columns=['Origin', 'Destination', 'FlightNumber', 'DepartureDate', 'DepartureTime', 'bookedPax', 'capacity'])
   
		if(self.debugMode):
			self.logger.info(NavitaireQuery)
   
		return bookedData

	def getNdoBand(self, startNDO):
		ndoBandQuery = "SELECT ndo_band FROM ndo_bands where start <= " + str(startNDO) + " and end > " + str(startNDO) 
		try:
			ndoBandList = pd.read_sql(ndoBandQuery, self.conn)
		except Exception as e:
			self.logger.error(e)
		ndoBand = ndoBandList['ndo_band'][0]
		return ndoBand

	def getdpflBand(self, dPLF):
		dplfBandQuery = "SELECT dplf_band FROM dplf_bands where start <= " + str(dPLF) + " and end > " + str(dPLF) 
		try:
			dplfBandList = pd.read_sql(dplfBandQuery, self.conn)
		except Exception as e:
			self.logger.error(e)
		dplfBand = dplfBandList['dplf_band'][0]
		return dplfBand

	def getPlfThreshhold(self, ndocounter, flightNumber):
		pflThresholdQuery = "SELECT plf_threshold FROM config_pfl_threshold WHERE flightNumber = '"+ str(flightNumber) +"' AND NDO = '"+ str(ndocounter) +"' UNION " 
		pflThresholdQuery = pflThresholdQuery+ " SELECT plf_threshold FROM config_pfl_threshold WHERE flightNumber = '*' " 
		pflThresholdQuery = pflThresholdQuery+ " AND NOT EXISTS (SELECT 1 FROM config_pfl_threshold WHERE flightNumber = '"+ str(flightNumber) +"') AND NDO = '"+ str(ndocounter) +"' LIMIT 1" 
		try:
			pflThresholdList = pd.read_sql(pflThresholdQuery, self.conn)
		except Exception as e:
			self.logger.error(e)
		threshold = pflThresholdList['plf_threshold']
		return threshold
     
	def getDynamicTime(self, currParams, dplfBand, ndoBand,DaySpan,channel):
		startTime = currParams['TimeWindowStart']
		endTime = currParams['TimeWindowEnd']
		time_range = '0'
		if (str(currParams['strategyFlag']) == '1'):
			dynamicTimeList = self.getd1d2StrategyValue(currParams['Strategy'], dplfBand, ndoBand,channel)
			time_range = dynamicTimeList['time_range'][0]
			
			rangeList = self.getTimeRange(time_range)

			if(rangeList['type'][0] == CONST_ABSOLUTE):
				if DaySpan<0:
					endTime = rangeList['end'][0]
				elif DaySpan>0:
					startTime=rangeList['start'][0]
				else:
					endTime = rangeList['end'][0]
					startTime=rangeList['start'][0]
	
			if(rangeList['type'][0] == CONST_RELATIVE):
				startTime, endTime = self.getRelativeTImeValue(currParams, rangeList,DaySpan)
		
			if(rangeList['type'][0] == CONST_REFERENCE):
				rangeList = self.getTimeReferenceValue(currParams)
				startTime = rangeList['ar_start'][0]
				endTime = rangeList['ar_end'][0]
   
		return str(startTime), str(endTime), str(time_range)

	def getTimeRange(self, time_range):
		timeRangeQuery = "SELECT type, start, end FROM time_ranges where time_range = '"+ time_range +"'"
		try:
			rangeList = pd.read_sql(timeRangeQuery, self.conn)
		except Exception as e:
			self.logger.error(e)
		return rangeList

	def getTimeReferenceValue(self, currParams):
		timeRangeQuery = "SELECT ar_start, ar_end FROM allocation_acceptable_range_d1 where airports = '"+ currParams['Origin'] +"'"
		try:
			rangeList = pd.read_sql(timeRangeQuery, self.conn)
		except Exception as e:
			self.logger.error(e)
		return rangeList

	def getRelativeTImeValue(self, currParams, rangeList,DaySpan):
		start_datetime_object = datetime.strptime(currParams['PerStart'], DATE_FORMAT)
		end_datetime_object = datetime.strptime(currParams['PerEnd'], DATE_FORMAT)
	
		startDateTime = currParams['PerStart'] + ' ' + currParams['TimeWindowStart']
		endDateTime = currParams['PerEnd'] + ' ' + currParams['TimeWindowEnd']
	
		startTime = datetime.strptime(startDateTime, DATE_TIME_FORMAT2) + timedelta(minutes= int(rangeList['start'][0]))
		endTime = datetime.strptime(endDateTime, DATE_TIME_FORMAT2) + timedelta(minutes= int(rangeList['end'][0]))
		if DaySpan<0:
			startTime = datetime.strptime(startDateTime, DATE_TIME_FORMAT2)
		elif DaySpan>0:
			endTime = datetime.strptime(endDateTime, DATE_TIME_FORMAT2)
		startTime = max(startTime, start_datetime_object.replace(hour=int(CONST_START_TIME[:2]), minute=int(CONST_START_TIME[3:])))
		endTime = min(endTime, end_datetime_object.replace(hour=int(CONST_END_TIME[:2]), minute=int(CONST_END_TIME[3:])))
	
		startTime = f"{startTime.hour:02d}:{startTime.minute:02d}"
		endTime = f"{endTime.hour:02d}:{endTime.minute:02d}"
  
		return startTime, endTime

	def getDynamicD2Value(self, currParams, dplfBand, ndoBand,channel):
		criteria = currParams['fareAnchor']
		offset = currParams['fareOffset']
		strategy = 'NA'
  
		if (str(currParams['strategyFlag'])== '1'):
			dynamicD2List = self.getd1d2StrategyValue(currParams['Strategy'], dplfBand, ndoBand,channel)
			criteria = dynamicD2List['criteria'][0]
			offset = dynamicD2List['offset'][0]
			strategy = dynamicD2List['strategy'][0]
		
		return criteria, offset, strategy

	def getd1d2StrategyValue(self, strategy, dplfBand, ndoBand,channel):
		dynamicD1D2Query = "SELECT criteria, time_range, offset, strategy FROM d1_d2_strategies where ndo_band = "+ str(ndoBand) +" and dplf_band = "+ str(dplfBand) +" and strategy = '"+ strategy +"'"+f" and channel='{channel}'"
		try:
			dynamicList = pd.read_sql(dynamicD1D2Query, self.conn)
		except Exception as e:
			self.logger.error(e)
		return dynamicList

	def outputResults(self):
		# self.logger.info("Final Allocation Results:")
		# self.logger.info(self.allocResults)
		if self.inputSource[CONST_OUTPUT][CONST_AU_GRID] == FILE:
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
			AUFrame = pd.DataFrame(self.AUFileData, columns=AUlist)
			AUFrame.to_csv(CONST_AU_GRID_FILE_PATH, index=False)
   
		if self.inputSource[CONST_OUTPUT][CONST_SUMMARY_FILE] == FILE:
			self.allocResults.to_csv(CONST_SUMMARY_FILE_PATH, index=False)

	def insertRunStart(self, startTime, username):
		if self.inputSource[CONST_LOGS][CONST_RUN_AUDIT] == DB:
			currVersionQuery = "SELECT tableName, curr_version FROM currentVersion"
			try:
				currVersionValues = pd.read_sql(currVersionQuery, self.conn)
			except Exception as e:
				self.logger.error(e)

			table_versions = {}
			for index,currVersion in currVersionValues.iterrows():
				table_versions[currVersion['tableName']] = currVersion['curr_version']

			type = "Local Run"
			
			insert_query = " INSERT INTO allocation_run_audit (run_id, start_time, user, dtd_start, dtd_end, Curves, d1_d2_strategies, market_list, Fares, type, is_sftp_pushed, is_connections_required, update_navitaire_method) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
			try:
				self.wrconn.execute(insert_query,
							(self.runId, startTime.strftime(DATE_TIME_FORMAT), username, 0, 365, table_versions['Curves'], table_versions['d1_d2_strategies'], type, table_versions['Fares'], type, '0', '0', 'file'))
			except Exception as e:
				self.logger.error(e)

	def updateNavitaire(self, rec):
		auData = {"runId": self.runId, "type": rec[0], "flight": rec[1], "startDate": rec[2], "endDate": rec[3], "dayOfWeek": rec[4],
                  "aircraftType": rec[5], "aircraftSuffix": rec[6], "lid": rec[7], "capacity": rec[8], "status": rec[9], "b2cRunId": rec[10], "b2bRunId": rec[11], "auClasses": {}}

		gap = 11
		for i in range(self.AU_COLUMN_LENGTH):
			auClass = {"classOfService": rec[gap + i*7 + 1], "classType": rec[gap + i*7 + 2], "classNest": int(rec[gap + i*7 + 3]),
                       "classRank": int(rec[gap + i*7 + 4]), "classAU": int(rec[gap + i*7 + 5]), "classAllotted": int(rec[gap + i*7 + 6]), "apRestriction": int(rec[gap + i*7 + 7])}
			auData['auClasses'][auClass['classOfService']] = auClass

		auDataStr = json.dumps(auData)
		#self.logger.info(auDataStr)
        # Send to queue for the market
		self.sqs.send_message(
            QueueUrl=self.RBD_QUEUE_URL,
            MessageBody=auDataStr
        )

	def updateNavSFTP(self, rec):
		flightNo = rec[1].split(" ")[1][2:7]
		sector = rec[1].split(" ")[2]
		flightDate = datetime.strptime(str(rec[2]), "%m-%d-%Y").date().strftime(DATE_FORMAT_2)
		auData = {"type": rec[0], "sector1": sector, "flightNo1" : flightNo, "departureDate": flightDate, "auClasses": {}}
		auClass={}
		gap = 11
		for i in range(self.AU_COLUMN_LENGTH):
			auClass[rec[gap + i*7 + 1]] = int(rec[gap + i*7 + 5])
  
		auData['auClasses'] = auClass
		auDataStr = json.dumps(auData)
		self.insert_run_fltDate_audit(self.runId, flightNo, flightDate, auDataStr, "Ready", "Ready")

		# update_query = " UPDATE run_flight_date_audit SET b2cstatus = %s, b2bstatus = %s, result = %s WHERE runId = %s and flightNumber = %s and flightDate = %s"
		# try:
		# 	self.wrconn.execute(update_query, "Ready", "Ready", auDataStr, self.runId, flightNo, flightDate)
		# except Exception as e:
		# 		self.logger.error(e)

	def getCurrentDateTime(self):
		startTime = datetime.now()
		if self.inputSource["currDateTime"] != None:
			startTime = self.inputSource["currDateTime"]
		return startTime

	def getCurrentDate(self):
		currDate = date.today()
		if self.inputSource["currDate"] != None:
			currDate = self.inputSource["currDate"]
		return currDate

	def getStartEndDate(self, PerType, currDate, startNDO, endNDO):
		if(PerType == 'D'):
			startDate = datetime.strptime(startNDO, DATE_FORMAT).date()
			endDate = datetime.strptime(endNDO, DATE_FORMAT).date()

			#Reset the startNDO and endNDO to actually NDO
			startNDO = (startDate - currDate).days
			endNDO = (endDate - currDate).days
		else:
			startDate = currDate + timedelta(days=int(startNDO))
			endDate = currDate + timedelta(days=int(endNDO))
   
		return startDate, endDate, startNDO, endNDO

	def generateConstantValues(self):
		self.MIN_D3_D4_VALUE = MIN_D3_D4_VALUE
		self.LINEAR_JUMP_VALUE = LINEAR_JUMP_VALUE
		self.PLF_CURVE_VALUE = PLF_CURVE_VALUE
		self.B2B_FARE_PRICE_COMPARISON = B2B_FARE_PRICE_COMPARISON
		self.COVERAGE_PARAMETER = COVERAGE_PARAMETER
		self.AU_COLUMN_LENGTH = AU_COLUMN_LENGTH
		self.B2B_RDB_COUNT = B2B_RDB_COUNT
		self.LOWEST_B2B_RBD_VALUE = LOWEST_B2B_RBD_VALUE
		self.LOWEST_B2C_RBD_VALUE = LOWEST_B2C_RBD_VALUE
		self.HIGHEST_B2B_RBD_VALUE = HIGHEST_B2B_RBD_VALUE
		self.HIGHEST_B2C_RBD_VALUE = HIGHEST_B2C_RBD_VALUE
  
		parameterValueQuery = "SELECT parameterKey,parameterValue FROM rm_parameter_values"
		try:
			parameterValue = pd.read_sql(parameterValueQuery, self.conn)
		except Exception as e:
			self.logger.error(e)
		for index,val in parameterValue.iterrows():
			if val['parameterKey'] == 'MIN_D3_D4_VALUE':
				self.MIN_D3_D4_VALUE = float(val['parameterValue'])
			if val['parameterKey'] == 'LINEAR_JUMP_VALUE':
				self.LINEAR_JUMP_VALUE = float(val['parameterValue'])
			if val['parameterKey'] == 'PLF_CURVE_VALUE':
				self.PLF_CURVE_VALUE = float(val['parameterValue'])
			if val['parameterKey'] == 'B2B_FARE_PRICE_COMPARISON':
				self.B2B_FARE_PRICE_COMPARISON = float(val['parameterValue'])
			if val['parameterKey'] == 'COVERAGE_PARAMETER':
				self.COVERAGE_PARAMETER = float(val['parameterValue'])
			if val['parameterKey'] == 'AU_COLUMN_LENGTH':
				self.AU_COLUMN_LENGTH = str(val['parameterValue'])
			if val['parameterKey'] == 'B2B_RDB_COUNT':
				self.B2B_RDB_COUNT = float(val['parameterValue'])
			if val['parameterKey'] == 'LOWEST_B2B_RBD_VALUE':
				self.LOWEST_B2B_RBD_VALUE = str(val['parameterValue'])
			if val['parameterKey'] == 'LOWEST_B2C_RBD_VALUE':
				self.LOWEST_B2C_RBD_VALUE = str(val['parameterValue'])
			if val['parameterKey'] == 'HIGHEST_B2B_RBD_VALUE':
				self.HIGHEST_B2B_RBD_VALUE = str(val['parameterValue'])
			if val['parameterKey'] == 'HIGHEST_B2C_RBD_VALUE':
				self.HIGHEST_B2C_RBD_VALUE = str(val['parameterValue'])
	
	def getUniqueId(self):
		return str(uuid.uuid4())

	def generateRoundNumber(self, number):
		fractional_part = number - int(number)
		if fractional_part >= 0.5:
			return int(number) + 1
		elif fractional_part < 0.5:
			return int(number)

	def loadRunParams(self):
		#load the list of sectors
		if self.inputSource[CONST_INPUT_SOURCE][CONST_MARKET_LIST] == FILE:
			self.mktList = pd.read_csv(CONST_MARKET_LIST_FILE_PATH)
		else:
			marketListQuery = "SELECT Origin,Destin,FlightNumber,PerType,PerStart,PerEnd,DOW,strategyFlag,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusion,fareAnchor,fareOffset,FirstRBDAlloc,OtherRBDAlloc,B2BBackstop,B2CBackstop,B2BFactor,SkippingFactor,analystName,DaySpan,OwnFareFlag,AutoTimeRangeFlag, openingFares FROM market_list "
			try:
				self.mktList = pd.read_sql(marketListQuery, self.conn)
			except Exception as e:
				self.logger.error(e)
   
		if(self.debugMode):
			self.logger.info("Loaded Market List")
			self.logger.info(self.mktList)

	def getCurrentTime(self, currentTimeFlag, currParams, currFltDate):
		#load the list of sectors
		if currentTimeFlag:
			currFltDate = datetime.strptime(currFltDate, DATE_FORMAT_2).date()
			query = f''' 
   				SELECT runTime FROM config_runTime 
					where FlightNumber = '{currParams["FlightNumber"]}' 
					and Origin = '{currParams["Origin"]}' 
					and Destin = '{currParams["Destin"]}' 
					and FlightDate ='{currFltDate}'
   			'''
			currTime = pd.read_sql(query, self.conn)
			self.currentTime = currTime["runTime"][0]
		else:
   			# Convert UTC time to IST
			# Define the IST timezone
			ist_timezone = pytz.timezone('Asia/Kolkata')
			# Convert UTC time to IST
			cur_time = self.starttime.astimezone(ist_timezone).time().strftime("%H:%M")
			self.currentTime = cur_time

	def insert_run_fltDate_audit(self, run_id, flightNumber, flightDate, result, b2cstatus, b2bstatus):
		insert_query = " INSERT INTO run_flight_date_audit (runId, flightNumber, flightDate, result, b2cstatus, b2bstatus ,createdAt) VALUES (%s, %s, %s, %s, %s, %s, %s)"
		try:
			self.wrconn.execute(insert_query, (run_id, flightNumber, flightDate, result, b2cstatus, b2bstatus, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
		except Exception as e:
			self.logger.info(e)

	def updateRunAudit(self, b2cStatus, b2bStatus, flightNo, flightDate):
		if self.inputSource[CONST_LOGS][CONST_RUN_AUDIT] == DB:
			self.cache_client.incr(KEY_B2B_DONE_COUNT + self.runId)
			self.cache_client.incr(KEY_B2C_DONE_COUNT + self.runId)

			# update_query = " UPDATE run_flight_date_audit SET b2cstatus = %s, b2bstatus = %s WHERE runId = %s and flightNumber = %s and flightDate = %s"
			# try:
			# 	self.wrconn.execute(update_query, b2cStatus, b2bStatus,  self.runId, flightNo, flightDate)
			# except Exception as e:
			# 	self.logger.error(e)

	def updateRunAuditb2c(self, b2cStatus, flightNo, flightDate):
		pass
		# if self.inputSource[CONST_LOGS][CONST_RUN_AUDIT] == DB:
		# 	self.cache_client.incr(KEY_B2C_DONE_COUNT + self.runId)

			# update_query = " UPDATE run_flight_date_audit SET b2cstatus = %s WHERE runId = %s and flightNumber = %s and flightDate = %s"
			# try:
			# 	self.wrconn.execute(update_query, b2cStatus,  self.runId, flightNo, flightDate)
			# except Exception as e:
			# 	self.logger.error(e)
  
	def updateRunAuditb2b(self, b2bStatus, flightNo, flightDate):
		pass
		# if self.inputSource[CONST_LOGS][CONST_RUN_AUDIT] == DB:
		# 	self.cache_client.incr(KEY_B2B_DONE_COUNT + self.runId)

			# update_query = " UPDATE run_flight_date_audit SET b2bstatus = %s WHERE runId = %s and flightNumber = %s and flightDate = %s"
			# try:
			# 	self.wrconn.execute(update_query, b2bStatus,  self.runId, flightNo, flightDate)
			# except Exception as e:
			# 	self.logger.error(e)
  
	def updateRunEnd(self, endTime):
		if self.inputSource[CONST_LOGS][CONST_RUN_AUDIT] == DB:
			updateQuery = " UPDATE allocation_run_audit SET end_time = %s WHERE run_id = %s "
			try:
				self.wrconn.execute(updateQuery, (endTime.strftime(DATE_TIME_FORMAT), self.runId))
			except Exception as e:
				self.logger.error(e)

	def areAllInputsReady(self):
		if self.inputSource[CONST_INPUT_SOURCE][CONST_INPUT_STATUS] == DB:
			countQuery = "select count(*) as busyInputs from inputs_status WHERE is_running = b'1'"
			try:
				inputsData = pd.read_sql(countQuery, self.conn)
			except Exception as e:
				self.logger.error(e)
			return inputsData['busyInputs'][0] == 0

	def getLatestCodeVersion(self):
		lastestQuery = " SELECT code_version FROM config_code_version where isActive = 1 "
		try:
			inputsData = pd.read_sql(lastestQuery, self.conn)
		except Exception as e:
			self.logger.error(e)
		return inputsData['code_version'][0]

	def getDepartureTimeBands(self):
		result = self.conn.execute("SELECT StartTime, EndTime, TimeBand FROM departure_time_ranges")
		time_bands = [(self.parse_time(start), self.parse_time(end), time_band) for start, end, time_band in result]
		rr=self.conn.execute("""SELECT
		(SELECT EndTime FROM departure_time_ranges ORDER BY TimeBand ASC LIMIT 1) AS StartTime,
		(SELECT StartTime FROM departure_time_ranges ORDER BY TimeBand ASC LIMIT 1 OFFSET 1) AS EndTime""")
		start_end_range=[(start,end) for start, end in rr]
		return time_bands,start_end_range

	def parse_time(self,time_str):
		return datetime.strptime(time_str, '%H:%M').time()

	def find_time_range(self,input_time_str, time_bands,start_end_range):
		input_time = datetime.strptime(input_time_str, '%H:%M').time()
		date_today = datetime.today()
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

	def setAllRunStatus2Ready(self):
		b2bPendingCount = self.cache_client.get(KEY_B2B_PENDING_COUNT + self.runId)
		b2cPendingCount = self.cache_client.get(KEY_B2B_PENDING_COUNT + self.runId)
		self.cache_client.set(KEY_B2B_READY_COUNT + self.runId, b2bPendingCount)
		self.cache_client.set(KEY_B2C_READY_COUNT + self.runId, b2cPendingCount)
		
		# update_query = " UPDATE run_flight_date_audit SET b2cstatus = %s, b2bstatus = %s WHERE runId = %s "
		# try:
		# 	self.wrconn.execute(update_query, "Ready", "Ready", self.runId)
		# except Exception as e:
		# 	self.logger.error(e)
		
class OwnLogger:
    
	def __init__(self,runid,input):
		self.logger = logging.getLogger()
		self.logger.setLevel(logging.INFO)
		self.runid=runid
		self.input=input
	
	def info(self,messsage):
		if self.input==DB:
			self.logger.info(f"message:{messsage}  runid:{self.runid}")
		else:
			print(messsage)  
	
	def error(self,errormessage):
		if self.input==DB:
			self.logger.error(f"error:{errormessage} runid:{self.runid}")
		else:
			print(errormessage)
