import traceback
import pandas as pd
import uuid
import math
import os
import time
import json
import Constants
import warnings
import numpy as np
import boto3
warnings.filterwarnings('ignore')
import logging
from datetime import date
from datetime import timedelta
from datetime import datetime
from dateutil import parser

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
		self.logger=OwnLogger(self.runId,self.inputSource[Constants.CONST_LOGS][Constants.CONST_RUN_AUDIT])
		self.TABLE_MARKET_FARES = "market_fares"
		self.BookloadTableName = "bookload"
		self.WAIT_TIME = 30 # in seconds
		self.nochunks=0
		self.totalrecords=0
		self.starttime=datetime.now()
		self.MARKET_LIST_QUEUE_URL = self.inputSource["MARKET_LIST_QUEUE_URL"]
		self.RBD_QUEUE_URL = self.inputSource["RBD_QUEUE_URL"]
		self.AWS_SQS_QUEUE_URL=self.inputSource["AU_CHUNK_QUEUE_URL"]
		self.AWS_BUCKET_NAME = 'qp-applications'
		self.NAV_DB_NAME = self.inputSource["NAV_DB_NAME"]

		self.sqs = currParams["sqs"]
		self.s3=currParams["s3"]
		self.conn = currParams["dbconn"]
		self.wrconn = currParams["wrconn"]
		self.NavitaireConnection = currParams["NavitaireConnection"]

		if(inputData["latestCode"]):
			code_version = self.getLatestCodeVersion()
			if(code_version != LATEST_VERSION):
				self.logger.info("Please use latest code! This is an old version!")
				exit()

		self.generateConstantValues()

		#Load the Run Parameters
		self.loadRunParams()
		self.chunk_size = 4 * 1024 * 1024
		#Place Holders for Output
		self.allocResults = pd.DataFrame(columns=['Id','Origin','Destin','FltNum','DepDate','BookedLoad','TgtLoad','bookedPlf','Variance','anchorFare','OpenRBD','SellingFare','Channel','HowDetermined','RunId','CreatedAt','MktFare_Min','AirlineCode_Min','FlightNumber_Min','MktFare_Max','AirlineCode_Max','FlightNumber_Max','fareAnchor','fareOffset','TimeRange','startTime','endTime','Backstop','StrategyReference','dlfBand','ndoBand','analystName','allocationStatus'])
		self.AUFileData = []
		
		#Start the Allocation
		if (currParams["runSingleMarket"]):
			self.runDynamicAllocationSingleMarket(currParams, 1)
		else:
			self.runDynamicAllocation()

	def runDynamicAllocation(self):
		self.logger.info("Starting Dynamic Allocation")
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
		OwnFareFlag=currParams['OwnFareFlag']
		SkippingFactor = currParams['SkippingFactor']
		analystName = currParams['analystName']
		FlightNumber = str(currParams['FlightNumber'])
		currParams['Strategy'] = currParams['fareAnchor']
		DaySpan=currParams['DaySpan']
		startDate, endDate, startNDO, endNDO = self.getStartEndDate(PerType, currDate, startNDO, endNDO)
		
		self.logger.info("Allocation for " + origin + destination + " from " + str(startDate) + " to " + str(endDate))
		
		# check if inputs are available to be copied else wait and retry
		canCopy = False
		while canCopy == False:
			canCopy = self.areAllInputsReady()
			if canCopy == False:
				self.logger.info('Waiting as inputs are busy...')
				time.sleep(self.WAIT_TIME)
  
		bookedLoad = self.getCurrentBookedLoads(origin, destination, startDate, endDate)
  
		try: 
			# For each NDO
			for ndocounter in range(startNDO, endNDO+1):
				currDate = self.getCurrentDate()
				tgtDate = datetime.strftime(currDate + timedelta(days=ndocounter),"%Y-%m-%d")
				bookedLoad['DepartureDate'] = pd.to_datetime(bookedLoad['DepartureDate'])
				if(FlightNumber == '*'):
					bookedLoadSingleRow = bookedLoad[(bookedLoad['DepartureDate'] == tgtDate)]
				else:
					bookedLoadSingleRow = bookedLoad[(bookedLoad['DepartureDate'] == tgtDate) & (bookedLoad['FlightNumber'] == FlightNumber)]
				#Fetch the current booked load from Navitaire
				# If for whatever reason - unable to get the booked Load - Abort
				tgtBookedLoad = None
				if(bookedLoadSingleRow.shape[0] == 0):
					dateFormatted = datetime.strptime(str(tgtDate), Constants.DATE_FORMAT_2).date().strftime(Constants.DATE_FORMAT)
					self.logger.info("Unable to get the Booked Load - Aborting")
					self.allocResults = self.allocResults.append({"Id":self.getUniqueId(), "RunId":self.runId, "Origin":origin, "Destin":destination, "FltNum":str(FlightNumber),"DepDate":str(dateFormatted), "BookedLoad":"NA", "TgtLoad":str(tgtBookedLoad), 'TimeRange': 'NA', 'dlfBand': 'NA', 'ndoBand': 'NA', 'anchorFare': 'NA', "MktFare_Min":"NA","MktFare_Max":"NA", "fareOffset": "NA", "OpenRBD": "NA", "SellingFare":"NA", 'Channel':"NA", 'fareAnchor':"NA",  'HowDetermined':"unable to get Booked loads" , 'Backstop': 'NA', 'AirlineCode_Min': 'NA', 'AirlineCode_Max': 'NA', 'FlightNumber_Min': 'NA', 'FlightNumber_Max': 'NA', 'Variance': 'NA' , 'StrategyReference': 'NA', 'analystName': analystName ,'bookedPlf': 'NA', 'startTime':'NA', 'endTime': 'NA', 'allocationStatus': 0}, ignore_index = True)
					self.updateRunAudit("Done", "Done", FlightNumber, tgtDate)
					self.insertSummaryRow()
				else:
					tgtBookedLoadList = self.getTargetBookedLoadFromBenchmarkCurve(ndocounter, curveId)
					if (len(tgtBookedLoadList) > 0):
						tgtBookedLoad = float(tgtBookedLoadList['LF'][0])
						B2CAvgFare=float(tgtBookedLoadList['B2C_Avg_Fare'][0])
						B2BAvgFare=float(tgtBookedLoadList['B2B_Avg_Fare'][0])
						self.runDynamicAllocationSingleMarketSingleDay(ndocounter, bookedLoadSingleRow, tgtBookedLoad, currParams, B2CBackstop, B2BBackstop,B2CAvgFare,B2BAvgFare, SkippingFactor,DaySpan,OwnFareFlag)
					else:
						#Cannot get a reference curve - Abort
						self.logger.info("Unable to get a Target - Check the booking curves")
						self.allocResults = self.allocResults.append({"Id":self.getUniqueId(), "RunId":self.runId, "Origin":origin,"Destin":destination,"FltNum":str(FlightNumber),"DepDate":str(tgtDate), "BookedLoad":bookedLoadSingleRow["bookedPax"].values[0], "TgtLoad":str(tgtBookedLoad), 'TimeRange': 'NA', 'dlfBand': 'NA', 'ndoBand': 'NA', 'anchorFare': 'NA', "MktFare_Min":"NA","MktFare_Max":"NA", "fareOffset": "NA", "OpenRBD":"NA", "SellingFare":"NA", 'fareAnchor':"NA",  'Channel':"NA", 'HowDetermined':"No Reference Curve Found", 'Backstop': 'NA', 'AirlineCode_Min': 'NA', 'AirlineCode_Max': 'NA', 'FlightNumber_Min': 'NA', 'FlightNumber_Max': 'NA', 'Variance': 'NA' , 'StrategyReference': 'NA', 'analystName': analystName ,'bookedPlf': 'NA','startTime':'NA', 'endTime': 'NA', 'allocationStatus': 0}, ignore_index = True)
						self.updateRunAudit("Done", "Done", FlightNumber, tgtDate)
						self.insertSummaryRow()
		except Exception as e:
			self.logger.info("Exception occurred")
			traceback.print_exc()
			self.logger.info(e)

		if  (self.inputSource['isUpdateNavitaire'] == False):
			self.outputResults()

		if (self.inputSource['isUpdateNavitaire']) and (self.NavitaireUpdateMethod==Constants.CONST_SFTP):
			update_query = " UPDATE run_flight_date_audit SET b2cstatus = %s, b2bstatus = %s WHERE runId = %s "
			try:
				self.wrconn.execute(update_query, "Ready", "Ready", self.runId)
			except Exception as e:
				self.logger.error(e)


	def runDynamicAllocationSingleMarketSingleDay(self, ndocounter, bookedLoad, tgtBookedLoad, currParams, B2CBackstop, B2BBackstop,B2CAvgFare,B2BAvgFare, SkippingFactor,DaySpan,OwnFareFlag):
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
			currFltDate = datetime.strftime(ndoBookedLoad['DepartureDate'][fltCounter],Constants.DATE_FORMAT)

			self.runDynamicAllocationSingleFlight(currFlt, currBookedLoad, currCapacity, currFltDate, tgtBookedLoad, dow, currParams, ndocounter, B2CBackstop, B2BBackstop,B2CAvgFare,B2BAvgFare, SkippingFactor,DaySpan,OwnFareFlag)

	def runDynamicAllocationSingleFlight(self, currFlt, currBookedLoad, currCapacity, currFltDate, tgtBookedLoad, dow, currParams, ndocounter, B2CBackstop, B2BBackstop,B2CAvgFare,B2BAvgFare, SkippingFactor,DaySpan,OwnFareFlag):
		
		tgtBookloadFactor = tgtBookedLoad	
		tgtBookedLoad = tgtBookedLoad * currCapacity

		if(self.debugMode):
			self.logger.info("Ready to start Allocation")
			self.logger.info(currParams)
			self.logger.info("DOW is : " + str(dow))
			self.logger.info("Extract is " + str(currParams['DOW'])[dow:(dow+1)])

		dateFormatted = datetime.strptime(str(currFltDate), Constants.DATE_FORMAT).date().strftime(Constants.DATE_FORMAT_2)
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
   
			# Get Dynamic D1 (Time) Value

			b2b_startTime, b2b_endTime, b2b_time_range = self.getDynamicTime(currParams, dplfBand, ndoBand,DaySpan,"B2B")
			b2c_startTime, b2c_endTime, b2c_time_range = self.getDynamicTime(currParams, dplfBand, ndoBand,DaySpan,"B2C")
			# Get Dynamic D2 (Offset & Criteria) Value
			b2b_criteria, b2b_offset, b2b_strategy = self.getDynamicD2Value(currParams, dplfBand, ndoBand,"B2B")
			b2c_criteria, b2c_offset, b2c_strategy = self.getDynamicD2Value(currParams, dplfBand, ndoBand,"B2C")
			# assigning to the same field to maintain backward compatibility

			b2b_fareanchor=b2b_criteria
			b2c_fareanchor=b2c_criteria
			b2b_fareoffset=b2b_offset
			b2c_fareoffset=b2c_offset
			
			#Actual Work Happens here
			#Get the B2C and B2B Fare ranges
			mktFareRangeB2C = self.getMarketFareRange('B2C',currFltDate, currParams,DaySpan,b2c_startTime,b2c_endTime)
			mktFareRangeB2B = self.getMarketFareRange('B2B',currFltDate, currParams,DaySpan,b2b_startTime,b2b_endTime)
			mktfareRangeB2C_minomaxo = self.getMarketFareRangeminomaxo('B2C', currFltDate, currParams,DaySpan,b2c_startTime,b2c_endTime)
			mktfareRangeB2B_minomaxo = self.getMarketFareRangeminomaxo('B2B', currFltDate, currParams,DaySpan,b2b_startTime,b2b_endTime)


			#Does competetive fare exist
			#We only proceed if a B2C fare exists - If B2C does not exist, we will skip the whole thing even with a B2B fare
			# This is because, when we prepare the AU Grid, if we donr push inventory on to B2C RBD, then the flight will not be visible 
			# on OTA - Flag the error for correction here
			lowFareB2C = 0
			lowFareB2C_minomaxo = 0
			hiFareB2C = 0
			hiFareB2C_minomaxo = 0
			lowFareB2B = 0
			lowFareB2B_minomaxo = 0
			hiFareB2B = 0
			hiFareB2B_minomaxo = 0
			lowAirlineCodeB2C = "Market Fare not found"
			hiAirlineCodeB2C = "Market Fare not found"
			lowFilghtNumB2C = "Market Fare not found"
			hiFilghtNumB2C = "Market Fare not found"
			lowAirlineCodeB2B = "Market Fare not found"
			hiAirlineCodeB2B = "Market Fare not found"
			lowFilghtNumB2B = "Market Fare not found"
			hiFilghtNumB2B = "Market Fare not found"
			b2bRunId = self.getUniqueId()
			b2cRunId = self.getUniqueId()

			if(len(mktFareRangeB2C) > 0):
				lowFareB2C = mktFareRangeB2C['gross_fare'][0]
				hiFareB2C = mktFareRangeB2C['gross_fare'].iloc[-1]
				if (currParams['fareAnchor']=="MIN" or currParams['fareAnchor']=="MAX"):
					lowAirlineCodeB2C = mktFareRangeB2C['outbound_airline_code'][0]
					hiAirlineCodeB2C = mktFareRangeB2C['outbound_airline_code'].iloc[-1]
					lowFilghtNumB2C = mktFareRangeB2C['outbound_flight'][0]
					hiFilghtNumB2C = mktFareRangeB2C['outbound_flight'].iloc[-1]
				else:
					lowAirlineCodeB2C = mktfareRangeB2C_minomaxo['LowFare_airline_code'][0]
					hiAirlineCodeB2C = mktfareRangeB2C_minomaxo['HiFare_airline_code'][0]
					lowFilghtNumB2C = mktfareRangeB2C_minomaxo['LowFare_outbound_flight'][0]
					hiFilghtNumB2C = mktfareRangeB2C_minomaxo['HiFare_outbound_flight'].iloc[0]
			lowFareB2C_minomaxo = mktfareRangeB2C_minomaxo['LowFare'][0]
			hiFareB2C_minomaxo = mktfareRangeB2C_minomaxo['HiFare'][0]
			if(len(mktFareRangeB2B) > 0):
				lowFareB2B = mktFareRangeB2B['gross_fare'][0]
				hiFareB2B = mktFareRangeB2B['gross_fare'].iloc[-1]
				if (currParams['fareAnchor']=="MIN" or currParams['fareAnchor']=="MAX"):
					lowAirlineCodeB2B = mktFareRangeB2B['outbound_airline_code'][0]
					hiAirlineCodeB2B = mktFareRangeB2B['outbound_airline_code'].iloc[-1]
					lowFilghtNumB2B = mktFareRangeB2B['outbound_flight'][0]
					hiFilghtNumB2B = mktFareRangeB2B['outbound_flight'].iloc[-1]
				else:
					lowAirlineCodeB2B = mktfareRangeB2B_minomaxo['LowFare_airline_code'][0]
					hiAirlineCodeB2B = mktfareRangeB2B_minomaxo['HiFare_airline_code'][0]
					lowFilghtNumB2B = mktfareRangeB2B_minomaxo['LowFare_outbound_flight'][0]
					hiFilghtNumB2B = mktfareRangeB2B_minomaxo['HiFare_outbound_flight'].iloc[0]
			lowFareB2B_minomaxo = mktfareRangeB2B_minomaxo['LowFare'][0]
			hiFareB2B_minomaxo = mktfareRangeB2B_minomaxo['HiFare'][0]
   
			isB2BMarketFareAvailable = True
			isB2CMarketFareAvailable = True
			if(lowFareB2B == hiFareB2B == 0 and B2BAvgFare!=Constants.FARE_NOT_AVL):
				lowFareB2B = B2BAvgFare
				hiFareB2B=B2BAvgFare
			elif (lowFareB2B == hiFareB2B == 0 and B2BAvgFare==Constants.FARE_NOT_AVL):
				isB2BMarketFareAvailable=False
			if((lowFareB2C == hiFareB2C == 0) and B2CAvgFare!=Constants.FARE_NOT_AVL):
				lowFareB2C=B2CAvgFare
				hiFareB2C=B2CAvgFare
			elif(lowFareB2C == hiFareB2C == 0 and B2CAvgFare==Constants.FARE_NOT_AVL):
				isB2CMarketFareAvailable=False
		
			if(isB2CMarketFareAvailable):
				if (OwnFareFlag==Constants.YES_FLAG and B2CAvgFare!=Constants.FARE_NOT_AVL and b2c_fareanchor==Constants.OWNF_FARE_ANCHOR):
					self.logger.info("Market Fare Range for " + str(currFltDate) + " in the B2C Channel is :" + str(B2CAvgFare) + " to " + str(B2CAvgFare))
					openDataB2C = self.determineRBDToOpen(currParams['Origin'], currParams['Destin'], lowFareB2C,hiFareB2C,lowFareB2C_minomaxo, hiFareB2C_minomaxo, b2c_fareanchor,b2c_fareoffset,'B2C', B2CBackstop,B2CAvgFare, currParams['FlightNumber'], dateFormatted)
					openDataB2C['FareResult']="Matched and allocated at Own Fare"
				elif b2c_fareanchor==Constants.MIN_FARE_ANCHOR or b2c_fareanchor==Constants.MAX_FARE_ANCHOR:

					self.logger.info("Market Fare Range for " + str(currFltDate) + " in the B2C Channel is :" + str(lowFareB2C) + " to " + str(hiFareB2C))
					openDataB2C = self.determineRBDToOpen(currParams['Origin'], currParams['Destin'], lowFareB2C,hiFareB2C,lowFareB2C_minomaxo, hiFareB2C_minomaxo, b2c_fareanchor,b2c_fareoffset,'B2C', B2CBackstop,0, currParams['FlightNumber'], dateFormatted)
				elif (OwnFareFlag==Constants.YES_FLAG and B2CAvgFare==Constants.FARE_NOT_AVL and b2c_fareanchor==Constants.OWNF_FARE_ANCHOR):
					b2c_fareanchor=Constants.MINO_FARE_ANCHOR
					self.logger.info("Market Fare Range for " + str(currFltDate) + " in the B2C Channel is :" + str(B2CAvgFare) + " to " + str(B2CAvgFare))
					openDataB2C = self.determineRBDToOpen(currParams['Origin'], currParams['Destin'], lowFareB2C,hiFareB2C,lowFareB2C_minomaxo, hiFareB2C_minomaxo, b2c_fareanchor,b2c_fareoffset,'B2C', B2CBackstop,B2CAvgFare, currParams['FlightNumber'], dateFormatted)
					openDataB2C['FareResult']="Inconsistent inputs recheck-Y Flag/OWNF/Fare NA Allocated with MINO"
				elif (OwnFareFlag==Constants.NO_FLAG and b2c_fareanchor==Constants.OWNF_FARE_ANCHOR):
					b2c_fareanchor=Constants.MINO_FARE_ANCHOR
					self.logger.info("Market Fare Range for " + str(currFltDate) + " in the B2C Channel is :" + str(B2CAvgFare) + " to " + str(B2CAvgFare))
					openDataB2C = self.determineRBDToOpen(currParams['Origin'], currParams['Destin'], lowFareB2C,hiFareB2C,lowFareB2C_minomaxo, hiFareB2C_minomaxo, b2c_fareanchor,b2c_fareoffset,'B2C', B2CBackstop,B2CAvgFare, currParams['FlightNumber'], dateFormatted)
					openDataB2C['FareResult']="Inconsistent inputs recheck-N Flag/OWNF"
				else:
					self.logger.info("Market Fare Range for " + str(currFltDate) + " in the B2C Channel is :" + str(lowFareB2C_minomaxo) + " to " + str(hiFareB2C_minomaxo))
					openDataB2C = self.determineRBDToOpen(currParams['Origin'], currParams['Destin'], lowFareB2C,hiFareB2C,lowFareB2C_minomaxo, hiFareB2C_minomaxo, b2c_fareanchor,b2c_fareoffset,'B2C', B2CBackstop,0, currParams['FlightNumber'], dateFormatted)


			else:
				openDataB2C = {'RBD':['NA'], 'Fare':['NA'], 'FareResult':['B2C Market Data NA - Skipping allocation'],'anchorFare': ['NA'],'statusInd':[1], 'allocationStatus': [0]}
				self.updateRunAudit("Done", "Done", currParams['FlightNumber'], dateFormatted)
	
			if(isB2BMarketFareAvailable):
				if (OwnFareFlag==Constants.YES_FLAG and B2BAvgFare!=Constants.FARE_NOT_AVL and b2b_fareanchor==Constants.OWNF_FARE_ANCHOR):
					self.logger.info("Market Fare Range for " + str(currFltDate) + " in the B2B Channel is :" + str(B2BAvgFare) + " to " + str(B2BAvgFare))
					openDataB2B = self.determineRBDToOpen(currParams['Origin'], currParams['Destin'], lowFareB2B,hiFareB2B,lowFareB2B_minomaxo, hiFareB2B_minomaxo, b2b_fareanchor,b2b_fareoffset,'B2B', B2BBackstop,B2BAvgFare, currParams['FlightNumber'], dateFormatted)
				elif b2b_fareanchor==Constants.MIN_FARE_ANCHOR or b2b_fareanchor==Constants.MAX_FARE_ANCHOR:

					self.logger.info("Market Fare Range for " + str(currFltDate) + " in the B2B Channel is :" + str(lowFareB2B) + " to " + str(hiFareB2B))
					openDataB2B = self.determineRBDToOpen(currParams['Origin'], currParams['Destin'], lowFareB2B,hiFareB2B,lowFareB2B_minomaxo, hiFareB2B_minomaxo, b2b_fareanchor,b2b_fareoffset,'B2B', B2BBackstop,0, currParams['FlightNumber'], dateFormatted)
				elif (OwnFareFlag==Constants.YES_FLAG and B2BAvgFare==Constants.FARE_NOT_AVL and b2b_fareanchor==Constants.OWNF_FARE_ANCHOR):
					b2b_fareanchor=Constants.MINO_FARE_ANCHOR
					self.logger.info("Market Fare Range for " + str(currFltDate) + " in the B2B Channel is :" + str(B2BAvgFare) + " to " + str(B2BAvgFare))
					openDataB2B = self.determineRBDToOpen(currParams['Origin'], currParams['Destin'], lowFareB2B,hiFareB2B,lowFareB2B_minomaxo, hiFareB2B_minomaxo, b2b_fareanchor,b2b_fareoffset,'B2B', B2BBackstop,B2BAvgFare, currParams['FlightNumber'], dateFormatted)
					openDataB2B['FareResult']="Inconsistent inputs recheck-Y Flag/OWNF/Fare NA Allocated with MINO"
				elif (OwnFareFlag==Constants.NO_FLAG and b2b_fareanchor==Constants.OWNF_FARE_ANCHOR):
					b2b_fareanchor=Constants.MINO_FARE_ANCHOR
					self.logger.info("Market Fare Range for " + str(currFltDate) + " in the B2B Channel is :" + str(B2BAvgFare) + " to " + str(B2BAvgFare))
					openDataB2B = self.determineRBDToOpen(currParams['Origin'], currParams['Destin'], lowFareB2B,hiFareB2B,lowFareB2B_minomaxo, hiFareB2B_minomaxo, b2b_fareanchor,b2b_fareoffset,'B2B', B2BBackstop,B2BAvgFare, currParams['FlightNumber'], dateFormatted)
					openDataB2B['FareResult']="Inconsistent inputs recheck-N Flag/OWNF"
				else:
					self.logger.info("Market Fare Range for " + str(currFltDate) + " in the B2B Channel is :" + str(lowFareB2B_minomaxo) + " to " + str(hiFareB2B_minomaxo))
					openDataB2B = self.determineRBDToOpen(currParams['Origin'], currParams['Destin'], lowFareB2B,hiFareB2B,lowFareB2B_minomaxo, hiFareB2B_minomaxo, b2b_fareanchor,b2b_fareoffset,'B2B', B2BBackstop,0,currParams['FlightNumber'], dateFormatted)

			else:
				openDataB2B = {'RBD':['NA'], 'Fare':['NA'], 'FareResult':['B2B Market Data NA - Skipping allocation'],'anchorFare': ['NA'],'statusInd':[3], 'allocationStatus': [0]}
				self.updateRunAuditb2b("Done", currParams['FlightNumber'], dateFormatted)
			if b2b_fareanchor==Constants.MINO_FARE_ANCHOR or b2b_fareanchor==Constants.MAXO_FARE_ANCHOR:

					lowFareB2B=lowFareB2B_minomaxo
					hiFareB2B=hiFareB2B_minomaxo
			if b2c_fareanchor==Constants.MINO_FARE_ANCHOR or b2c_fareanchor==Constants.MAXO_FARE_ANCHOR:
					lowFareB2C=lowFareB2C_minomaxo
					hiFareB2C=hiFareB2C_minomaxo
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
				#Now add all the Z RBDs
				isUpsellFlag, b2bSkipFlag = self.B2BAllocation(currBookedLoad, currParams, openDataB2B, currRec, totalCapacity, dPLF, b2cAllocationsUnits)

				if(isUpsellFlag):
					openDataB2B = {'RBD':['NA'], 'Fare':['NA'], 'FareResult':['B2B allocation skipped - Flight in upsell'],'anchorFare': ['NA'],'statusInd':[1], 'allocationStatus': [1]}
					self.updateRunAuditb2b("Done", currParams['FlightNumber'], dateFormatted)
     
				if(b2bSkipFlag):
					openDataB2B = {'RBD':['NA'], 'Fare':['NA'], 'FareResult':['B2B allocation skipped - B2B Factor is -1'],'anchorFare': ['NA'],'statusInd':[1], 'allocationStatus': [1]}
					self.updateRunAuditb2b("Done", currParams['FlightNumber'], dateFormatted)
				
				#Ends statsusInd == 0
				if self.thresholdflag:
					currRec.extend(['L', '', -1, -1, 0, -1, -1, 'P', '', -1, -1, 0, -1, -1])
				else:
					currRec.extend(['L', '', -1, -1, -1, -1, -1, 'P', '', -1, -1, -1, -1, -1])
     
				self.AUFileData.append(currRec)
				
				if (self.inputSource['isUpdateNavitaire']) and (self.NavitaireUpdateMethod==Constants.CONST_API):
					self.updateNavitaire(currRec)
     
				if (self.inputSource['isUpdateNavitaire']) and (self.NavitaireUpdateMethod==Constants.CONST_SFTP):
					self.updateNavSFTP(currRec)


			self.allocResults = self.allocResults.append({"Id":b2cRunId, "RunId":self.runId,"Origin":currParams['Origin'],"Destin":currParams['Destin'],"FltNum":str(currFlt),"DepDate":str(currFltDate), "BookedLoad":str(currBookedLoad), "TgtLoad":str(tgtBookedLoad), 'TimeRange': str(b2c_time_range), 'dlfBand': str(dplfBand), 'ndoBand': str(ndoBand),  'anchorFare':str(openDataB2C["anchorFare"][0]), "MktFare_Min":str(round(float(lowFareB2C))), 'MktFare_Max': hiFareB2C, "OpenRBD":str(openDataB2C['RBD'][0]), "SellingFare":str(openDataB2C["Fare"][0]), 'Channel':'B2C', 'HowDetermined':str(openDataB2C['FareResult'][0]), 'fareAnchor': b2c_fareanchor, 'fareOffset': b2c_fareoffset, 'Backstop': B2CBackstop, 'AirlineCode_Min': str(lowAirlineCodeB2C), 'AirlineCode_Max': str(hiAirlineCodeB2C), 'FlightNumber_Min': str(lowFilghtNumB2C), 'FlightNumber_Max': str(hiFilghtNumB2C), 'Variance': str(curveReference) , 'StrategyReference': str(b2c_strategy),'analystName': currParams['analystName'] ,'bookedPlf': str(plf),'startTime': b2c_startTime, 'endTime':  b2c_endTime, 'allocationStatus': (openDataB2C['allocationStatus'][0])}, ignore_index = True)
			self.insertSummaryRow()
			
			self.allocResults = self.allocResults.append({"Id":b2bRunId, "RunId":self.runId,"Origin":currParams['Origin'],"Destin":currParams['Destin'],"FltNum":str(currFlt),"DepDate":str(currFltDate), "BookedLoad":str(currBookedLoad), "TgtLoad":str(tgtBookedLoad), 'TimeRange': str(b2b_time_range), 'dlfBand': str(dplfBand), 'ndoBand': str(ndoBand), 'anchorFare':str(openDataB2B["anchorFare"][0]), "MktFare_Min":str(round(float(lowFareB2B))), 'MktFare_Max': hiFareB2B, "OpenRBD":str(openDataB2B['RBD'][0]), "SellingFare":str(openDataB2B["Fare"][0]), 'Channel':'B2B', 'HowDetermined':str(openDataB2B['FareResult'][0]), 'fareAnchor': b2b_fareanchor, 'fareOffset': b2b_fareoffset, 'Backstop': B2BBackstop, 'AirlineCode_Min': str(lowAirlineCodeB2B), 'AirlineCode_Max': str(hiAirlineCodeB2B), 'FlightNumber_Min': str(lowFilghtNumB2B), 'FlightNumber_Max': str(hiFilghtNumB2B), 'Variance': str(curveReference), 'StrategyReference': str(b2b_strategy),'analystName': currParams['analystName'] ,'bookedPlf': str(plf),'startTime': b2b_startTime, 'endTime':  b2b_endTime, 'allocationStatus': (openDataB2C['allocationStatus'][0])}, ignore_index = True)
			self.insertSummaryRow()
				
		else:
			if (str(currParams['DOW'])[dow:(dow+1)] == "9"):
				self.updateRunAudit("Done", "Done", currParams['FlightNumber'], dateFormatted)
			self.logger.info("Run Parameters requested to skip - (FlightNumber, DOW, or Status)")

	def B2BAllocation(self, currBookedLoad, currParams, openDataB2B, currRec, totalCapacity, dPLF, b2cAllocationsUnits):
		isUpsellFlag = False
		b2bSkipFlag = False
		B2BRBDs = self.fareExtract(currParams['Origin'], currParams['Destin'], 'B2B', '')

		b2bFactor = float(currParams['B2BFactor'])
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
			
			if(int(b2bFactor) == -1):
				currRec.append(-1)
				b2bSkipFlag = True
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
		return isUpsellFlag, b2bSkipFlag

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
				elif(firstRDBAllocation == Constants.DYNAMIC_D3_D4):
					openRBDIndex, entryRDB, d3Allocation, d4Allocation, remainingRBD = self.d3d4Calculations(dPLF, remainingCapacity, ndocounter, maxRBDPrice, openingRBDPirce, openRBDIndex, entryRDB, otherRDBAllocation, remainingRBD, d3Allocation)
					openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b = self.jumpingAllocation(currBookedLoad, bookedDeficit, openRBDIndex, incremental, jumpPercent, price, nextPrice, d3Allocation, d4Allocation, jumpingRBDPrice, b2bFactor, incrementalb2b)

				#Negative -1 Allocation	
				elif(firstRDBAllocation == Constants.DEFICIT_ALLOCATION):
					bookedDef = int(round(bookedDeficit,0))
					if(bookedDef <= 0):
						bookedDef = 0
					openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b = self.jumpingAllocation(currBookedLoad, bookedDeficit, openRBDIndex, incremental, jumpPercent, price, nextPrice, bookedDef, otherRDBAllocation, jumpingRBDPrice, b2bFactor, incrementalb2b)
					
				#Negative -2 Allocation
				elif(firstRDBAllocation == Constants.LINEAR_ALLOCATION):
					allocation = int(round(remainingCapacity/otherRDBAllocation,0))
					openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b = self.jumpingAllocation(currBookedLoad, bookedDeficit, openRBDIndex, incremental, jumpPercent, price, nextPrice, allocation, allocation, jumpingRBDPrice, b2bFactor, incrementalb2b)

				#Negative -3 Allocation
				elif(firstRDBAllocation == Constants.LINEAR_ALLOCATION_WITH_JUMP):
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


	def determineRBDToOpen(self, origin, destin, lowFare, hiFare,lowFare_minomaxo, hifare_minomaxo, strategy, offset, channel, backstop,avgFare,flightNumber, currFltDate):

		openResult = pd.DataFrame(columns=['RBD', 'Fare', 'FareResult', 'anchorFare', 'statusInd', 'allocationStatus'])

		if (strategy == Constants.MAX_FARE_ANCHOR):
			anchorFare = hiFare
		if (strategy == Constants.MIN_FARE_ANCHOR):
			anchorFare = lowFare
		if (strategy == Constants.MAXO_FARE_ANCHOR):
			anchorFare = hifare_minomaxo
		if (strategy == Constants.MINO_FARE_ANCHOR):
			anchorFare = lowFare_minomaxo
		if (strategy==Constants.OWNF_FARE_ANCHOR and avgFare!=0):
			anchorFare = avgFare
		
		self.logger.info("Strategy applied for " + channel + " Channel is " + strategy )
		self.logger.info("Anchor Fare is " + str(anchorFare) + " with an offset of " + str(offset))

		RBDQuery = "select RBD, Total from Fares where Org = '" + origin + "' and Dst = '" + destin + "' "
		try:
			rbddata = pd.read_sql(RBDQuery, self.conn)
		except Exception as e:
			self.logger.error(e)
		if(rbddata.shape[0] == 0):
			self.logger.info('QP Fare not available. Please validate Fares table')
			openResult = openResult.append({'RBD':'NA', 'Fare':'NA', 'FareResult':'QP Fares not available','anchorFare':anchorFare,'statusInd':1, 'allocationStatus': 0 }, ignore_index = True)
			self.updateRunAudit("Done", "Done", flightNumber, currFltDate)
			return openResult
  
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

		if(rbddata.shape[0] == 0):
			self.logger.info("Market Fare is out of price ladder range - allocating at extreme values")
			#Fetch the Lowest or highest available fares

			defFareData = self.fetchExtremePublishedFare(origin, destin, channel, 'LOW', backstop)
			if (int(offset) >= 0):
				defFareData = self.fetchExtremePublishedFare(origin, destin, channel, "HI", backstop)

			# Edge case where backstop is placed at a very large value - allocates at Z0 or Y levels
			if (defFareData.shape[0] == 0):
				defFareData = self.fetchExtremePublishedFare(origin, destin, channel, 'HI', 0) 	
			if(defFareData.shape[0]!=0):
				if (channel == 'B2C'):
					openResult = openResult.append({'RBD':defFareData['RBD'][0], 'Fare':defFareData['Total'][0], 'FareResult':'Unable to Match Mkt - B2C allocation at extreme value','anchorFare':anchorFare,'statusInd':2, 'allocationStatus': 1}, ignore_index = True)
				if (channel == 'B2B'):
					if (defFareData['RBD'][0] == self.HIGHEST_B2B_RBD_VALUE):
						openResult = openResult.append({'RBD':'NA', 'Fare':'NA', 'FareResult':'Market Fare higher than '+ self.HIGHEST_B2B_RBD_VALUE +' levels - skipping B2B allocation','anchorFare':anchorFare,'statusInd':1, 'allocationStatus': 1}, ignore_index = True)
						self.updateRunAuditb2b("Done", flightNumber, currFltDate)
					else:
						openResult = openResult.append({'RBD':defFareData['RBD'][0], 'Fare':defFareData['Total'][0], 'FareResult':'Unable to Match Mkt - B2B allocation at extreme value','anchorFare':anchorFare,'statusInd':2, 'allocationStatus': 1}, ignore_index = True)
			else:
				openResult = openResult.append({'RBD':"NA", 'Fare':"NA", 'FareResult':'Unable to fetch extreme published Fares','anchorFare':"NA",'statusInd':2, 'allocationStatus': 0}, ignore_index = True)
		else:
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
					openResult = openResult.append({'RBD':'NA', 'Fare':'NA', 'FareResult':'Market Fare higher than '+ self.HIGHEST_B2B_RBD_VALUE +' levels - skipping B2B allocation','anchorFare':anchorFare,'statusInd':1, 'allocationStatus': 1}, ignore_index = True)
					self.updateRunAuditb2c("Done", flightNumber, currFltDate)
				else:
					if(offsetToFetch == rbddata.shape[0]):
						openResult = openResult.append({'RBD':RBDtoOpen, 'Fare':str(FareToOpen), 'FareResult':'Not Enough Levels available - taking the Max/Min Available','anchorFare':anchorFare,'statusInd':2, 'allocationStatus': 1}, ignore_index = True)
					else:
						if(extremeFareFlag):
							openResult = openResult.append({'RBD':RBDtoOpen, 'Fare':str(FareToOpen), 'FareResult':'Unable to Match Mkt - B2B allocation at extreme value','anchorFare':anchorFare, 'statusInd':0, 'allocationStatus': 1}, ignore_index = True)
						else:
							if(backstopFlag):
								openResult = openResult.append({'RBD':RBDtoOpen, 'Fare':str(FareToOpen), 'FareResult':'Matched and allocated - Backstop Hit','anchorFare':anchorFare,'statusInd':0, 'allocationStatus': 1}, ignore_index = True)
							else:
								openResult = openResult.append({'RBD':RBDtoOpen, 'Fare':str(FareToOpen), 'FareResult':'Matched and allocated','anchorFare':anchorFare,'statusInd':0, 'allocationStatus': 1}, ignore_index = True)
			else:
				if(offsetToFetch == rbddata.shape[0]):
					openResult = openResult.append({'RBD':RBDtoOpen, 'Fare':str(FareToOpen), 'FareResult':'Not Enough Levels available - taking the Max/Min Available','anchorFare':anchorFare,'statusInd':2, 'allocationStatus': 1}, ignore_index = True)
				else:
					if(extremeFareFlag):
							openResult = openResult.append({'RBD':RBDtoOpen, 'Fare':str(FareToOpen), 'FareResult':'Unable to Match Mkt - B2C allocation at extreme value','anchorFare':anchorFare,'statusInd':0, 'allocationStatus': 1}, ignore_index = True)
					else:
						if(backstopFlag):
							openResult = openResult.append({'RBD':RBDtoOpen, 'Fare':str(FareToOpen), 'FareResult':'Matched and allocated - Backstop Hit','anchorFare':anchorFare,'statusInd':0, 'allocationStatus': 1}, ignore_index = True)
						else:
							openResult = openResult.append({'RBD':RBDtoOpen, 'Fare':str(FareToOpen), 'FareResult':'Matched and allocated','anchorFare':anchorFare,'statusInd':0, 'allocationStatus': 1}, ignore_index = True)

		return openResult

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
		
		currFltDate = datetime.strptime(currFltDateStr, Constants.DATE_FORMAT).date()
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
		if(self.inputSource[Constants.CONST_INPUT_SOURCE][Constants.CONST_MARKET_FARES] != Constants.TEST_DATA):
			mktLowestQuery = mktLowestQuery + " AND created_at > NOW() - INTERVAL 2 DAY "
		mktLowestQuery = mktLowestQuery + " ORDER BY gross_fare ASC"
		try:
			mktFareRange = pd.read_sql(mktLowestQuery, self.conn)
		except Exception as e:
			self.logger.error(e)

		return mktFareRange


	def getMarketFareRangeminomaxo(self, channel, currFltDateStr, currParams,DaySpan,startTime,endTime):

		currFltDate = datetime.strptime(currFltDateStr, Constants.DATE_FORMAT).date()
		startDateTime=f"{currFltDate} {startTime}"
		enddateTime=f"{currFltDate} {endTime}"
		updateDate=currFltDate+timedelta(days=int(DaySpan))
		if currFltDate>updateDate:
			startDateTime=f"{updateDate} {startTime}"
			enddateTime=f"{currFltDate} {endTime}"
		elif currFltDate<updateDate:
			startDateTime=f"{currFltDate} {startTime}"
			enddateTime=f"{updateDate} {endTime}"
		mktLowestQuery = "Select gross_fare,outbound_airline_code,outbound_flight from " + self.TABLE_MARKET_FARES + " where source_airport = '"
		mktLowestQuery = mktLowestQuery + currParams['Origin'] + "' and destination_airport = '" + currParams['Destin'] + "'"
		mktLowestQuery = mktLowestQuery + f"AND CAST(CONCAT(departure_date, ' ', outbound_departuretime) AS DATETIME) <= CAST('{enddateTime}' AS DATETIME) AND CAST(CONCAT(departure_date, ' ', outbound_departuretime) AS DATETIME) >= CAST('{startDateTime}' AS DATETIME)"
		mktLowestQuery = mktLowestQuery + " and channel = '" + channel + "' "
		mktLowestQuery = mktLowestQuery + " and outbound_airline_code <> '" + currParams['CarrExlusion'] + "' AND outbound_airline_code != 'QP' AND outbound_airline_code!='G8' "
		if(self.inputSource[Constants.CONST_INPUT_SOURCE][Constants.CONST_MARKET_FARES] != Constants.TEST_DATA):
			mktLowestQuery = mktLowestQuery + " AND created_at > NOW() - INTERVAL 2 DAY "
		try:
			mktFareRange = pd.read_sql(mktLowestQuery, self.conn)
		except Exception as e:
			self.logger.error(e)
		data = pd.DataFrame(mktFareRange)
		fare_list = data["gross_fare"].tolist()
		if len(fare_list)>1:
			coveragevalue = int(self.COVERAGE_PARAMETER)
			MED = np.median(fare_list)
			MAD = np.median(np.abs(fare_list- np.median(fare_list)))
			for fare in fare_list:
				MZS = np.abs(fare - MED)/MAD
				if MZS > coveragevalue:
					fare_list.remove(fare)
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

	def getTargetBookedLoadFromBenchmarkCurve(self, ndocounter, curveID):
		tgtBookedLoad = []
  		#Fetch the Target Booked Load from the curve

		bookingCurveQuery = "Select LF,B2C_Avg_Fare,B2B_Avg_Fare from Curves where NDO = " + str(ndocounter) + " and CurveID = '" + curveID + "'"
		try:
			benchmark = pd.read_sql(bookingCurveQuery, self.conn)
		except Exception as e:
			self.logger.error(e)
		if(benchmark.shape[0] > 0):
			tgtBookedLoad = benchmark

		return tgtBookedLoad

	def deleteTableExtract(self, tableName):
		dropQuery = "DROP TABLE IF EXISTS " + tableName
		try:
			self.conn.execute(dropQuery)
		except Exception as e:
			self.logger.error(e)

	def insertSummaryRow(self):
		if self.inputSource[Constants.CONST_LOGS][Constants.CONST_RUN_SUMMARY] == Constants.DB:
			latest_row = self.allocResults.iloc[-1]
			insertQuery = " INSERT INTO run_summary ( Id, Origin, Destin, FltNum, DepDate, BookedLoad, TgtLoad, MktFare_Min, OpenRBD, SellingFare, Channel, HowDetermined, RunId, CreatedAt, MktFare_Max, fareAnchor, fareOffset, TimeRange, dlfBand, ndoBand, criteria, AirlineCode_Max, AirlineCode_Min, FlightNumber_Max, FlightNumber_Min, backstop, Variance, StrategyReference, analystName, bookedPlf, startTime, endTime, allocationStatus) "		
			insertQuery = insertQuery + " VALUES ('"+ latest_row['Id'] + "', '"+latest_row['Origin'] + "', '"+latest_row['Destin'] + "', '"+ latest_row['FltNum'] + "', '"+latest_row['DepDate'] + "', '"+latest_row['BookedLoad'] + "', '"+latest_row['TgtLoad'] + "', '"+str(latest_row['MktFare_Min']) + "', '"+latest_row['OpenRBD'] + "', '"+str(latest_row['SellingFare']) + "', '"+latest_row['Channel'] + "', '"+latest_row['HowDetermined'] + "', '"+latest_row['RunId'] + "', '" + self.getCurrentDateTime().strftime(Constants.DATE_TIME_FORMAT) +"','"+str(latest_row['MktFare_Max']) + "','"+str(latest_row['anchorFare']) + "','"+str(latest_row['fareOffset']) +"','"+str(latest_row['TimeRange']) +"','"+str(latest_row['dlfBand']) +"','"+str(latest_row['ndoBand']) + "','"+str(latest_row['fareAnchor']) + "','"+str(latest_row['AirlineCode_Max']) + "','"+str(latest_row['AirlineCode_Min']) + "','"+str(latest_row['FlightNumber_Max']) + "','"+str(latest_row['FlightNumber_Min']) + "','"+str(latest_row['Backstop']) + "','"+str(latest_row['Variance']) + "','"+str(latest_row['StrategyReference']) + "','"+str(latest_row['analystName']) + "','"+str(latest_row['bookedPlf']) + "','"+str(latest_row['startTime']) + "','"+str(latest_row['endTime']) + "','"+str(latest_row['allocationStatus']) + "')"
			try:
				self.wrconn.execute(insertQuery)
			except Exception as e:
				self.logger.error(e)


	def getCurrentBookedLoads(self, origin, destin, startDate, endDate):
		
		if self.inputSource[Constants.CONST_INPUT_SOURCE][Constants.CONST_BOOKLOAD] == Constants.TEST_DATA:
			NavitaireQuery = "SELECT Origin, Destination, FlightNumber, DepartureDate, bookedPax, capacity from " + self.BookloadTableName
			NavitaireQuery = NavitaireQuery + " where DepartureDate >= '" + str(startDate) + "' and DepartureDate <= '" +str(endDate) + "'"
			NavitaireQuery = NavitaireQuery + " and Origin = '" + origin + "' and Destination = '" + destin + "'"
			con = self.conn
   
		if self.inputSource[Constants.CONST_INPUT_SOURCE][Constants.CONST_BOOKLOAD] == Constants.DB:
			NavitaireQuery = "select Origin, Destination, FlightNumber, DepartureDate, count(PassengerID) as bookedPax, max(Lid) as capacity from ("
			NavitaireQuery = NavitaireQuery + "select BP.PassengerID, IL.DepartureStation as Origin, IL.ArrivalStation as Destination, IL.DepartureDate,IL.FlightNumber, IL.AdjustedCapacity, IL.Lid "
			NavitaireQuery = NavitaireQuery + " from "+self.NAV_DB_NAME+"[rez].InventoryLeg IL "
			NavitaireQuery = NavitaireQuery + " left outer join "+self.NAV_DB_NAME+"[rez].PassengerJourneyLeg PJL on  PJL.InventoryLegID = IL.InventoryLegID "
			NavitaireQuery = NavitaireQuery + " left outer join "+self.NAV_DB_NAME+"[rez].PassengerJourneySegment PJS on PJS.PassengerID = PJL.PassengerID and PJS.SegmentID = PJL.SegmentID and (PJS.FareBasis != 'SO7NBSOL' or PJS.FareBasis IS NULL) "
			NavitaireQuery = NavitaireQuery + " left outer join "+self.NAV_DB_NAME+"[rez].BookingPassenger BP on  BP.PassengerID = PJS.PassengerID "
			NavitaireQuery = NavitaireQuery + " where IL.Lid > 0 and IL.Status NOT IN (2) and IL.DepartureDate >= '" + str(startDate) + "' and IL.DepartureDate <= '" +str(endDate) + "' "
			NavitaireQuery = NavitaireQuery + " and IL.DepartureStation = '" + origin + "' and IL.ArrivalStation = '" + destin + "'"
			NavitaireQuery = NavitaireQuery + ") A group by Origin, Destination, FlightNumber, DepartureDate order by Origin, Destination, DepartureDate "
			con = self.NavitaireConnection
		try:
			bookedData = pd.read_sql(NavitaireQuery, con)
		except Exception as e:
			self.logger.error(e)
   
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

			if(rangeList['type'][0] == Constants.CONST_ABSOLUTE):
				if DaySpan<0:
					endTime = rangeList['end'][0]
				elif DaySpan>0:
					startTime=rangeList['start'][0]
				else:
					endTime = rangeList['end'][0]
					startTime=rangeList['start'][0]
     
			if(rangeList['type'][0] == Constants.CONST_RELATIVE):
				startTime, endTime = self.getRelativeTImeValue(currParams, rangeList,DaySpan,time_range)
		
			if(rangeList['type'][0] == Constants.CONST_REFERENCE):
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

	def getRelativeTImeValue(self, currParams, rangeList,DaySpan,time_range):
		start_datetime_object = datetime.strptime(currParams['PerStart'], Constants.DATE_FORMAT)
		end_datetime_object = datetime.strptime(currParams['PerEnd'], Constants.DATE_FORMAT)
		startDateTime = currParams['PerStart'] + ' ' + currParams['TimeWindowStart']
		endDateTime = currParams['PerEnd'] + ' ' + currParams['TimeWindowEnd']
		startTime = datetime.strptime(startDateTime, Constants.DATE_TIME_FORMAT2) + timedelta(minutes= int(rangeList['start'][0]))
		endTime = datetime.strptime(endDateTime, Constants.DATE_TIME_FORMAT2) + timedelta(minutes= int(rangeList['end'][0]))
		if DaySpan<0:
			startTime = datetime.strptime(startDateTime, Constants.DATE_TIME_FORMAT2)
		elif DaySpan>0:
			endTime = datetime.strptime(endDateTime, Constants.DATE_TIME_FORMAT2)
		startTime = max(startTime, start_datetime_object.replace(hour=int(Constants.CONST_START_TIME[:2]), minute=int(Constants.CONST_START_TIME[3:])))
		endTime = min(endTime, end_datetime_object.replace(hour=int(Constants.CONST_END_TIME[:2]), minute=int(Constants.CONST_END_TIME[3:])))
	
		startTime = str(startTime.hour) + ":" + str(startTime.minute)
		endTime = str(endTime.hour) + ":" + str(endTime.minute)
  
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
		
		AUlist=['Type','Flight','StartDate','EndDate','DayOfWeek','Aircraft Type','Aircraft Suffix','Lid','Capacity','Status']
		length = self.AU_COLUMN_LENGTH
		for i in range(length):
			AUlist.append(f'Class Of Service AU Class {i+1}')
			AUlist.append(f'Class Type AU Class {i+1}')
			AUlist.append(f'Class Nest AU Class {i+1}')
			AUlist.append(f'Class Rank AU Class {i+1}')
			AUlist.append(f'Class AU AU Class {i+1}')
			AUlist.append(f'Class Allotted AU Class {i+1}')
			AUlist.append(f'AP Restriction AU Class {i+1}')	
		AUFrame = pd.DataFrame(self.AUFileData, columns=AUlist)
		
		# self.logger.info("Final Allocation Results:")
		# self.logger.info(self.allocResults)
		if self.inputSource[Constants.CONST_OUTPUT][Constants.CONST_AU_GRID] == Constants.FILE:
			AUFrame.to_csv(Constants.CONST_AU_GRID_FILE_PATH, index=False)
   
		if self.inputSource[Constants.CONST_OUTPUT][Constants.CONST_SUMMARY_FILE] == Constants.FILE:
			self.allocResults.to_csv(Constants.CONST_SUMMARY_FILE_PATH, index=False)

	def insertRunStart(self, startTime, username):
		if self.inputSource[Constants.CONST_LOGS][Constants.CONST_RUN_AUDIT] == Constants.DB:
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
							(self.runId, startTime.strftime(Constants.DATE_TIME_FORMAT), username, 0, 365, table_versions['Curves'], table_versions['d1_d2_strategies'], type, table_versions['Fares'], type, '0', '0', 'file'))
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
		flightDate = datetime.strptime(str(rec[2]), "%m-%d-%Y").date().strftime(Constants.DATE_FORMAT_2)
		auData = {"type": rec[0], "sector1": sector, "flightNo1" : flightNo, "departureDate": flightDate, "auClasses": {}}
		auClass={}
		gap = 11
		for i in range(self.AU_COLUMN_LENGTH):
			auClass[rec[gap + i*7 + 1]] = int(rec[gap + i*7 + 5])
  
		auData['auClasses'] = auClass
		auDataStr = json.dumps(auData)
		update_query = " UPDATE run_flight_date_audit SET b2cstatus = %s, b2bstatus = %s, result = %s WHERE runId = %s and flightNumber = %s and flightDate = %s"
		try:
			self.wrconn.execute(update_query, "Ready", "Ready", auDataStr, self.runId, flightNo, flightDate)
		except Exception as e:
				self.logger.error(e)

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
			startDate = datetime.strptime(startNDO, Constants.DATE_FORMAT).date()
			endDate = datetime.strptime(endNDO, Constants.DATE_FORMAT).date()

			#Reset the startNDO and endNDO to actually NDO
			startNDO = (startDate - currDate).days
			endNDO = (endDate - currDate).days
		else:
			startDate = currDate + timedelta(days=int(startNDO))
			endDate = currDate + timedelta(days=int(endNDO))
   
		return startDate, endDate, startNDO, endNDO

	def generateConstantValues(self):
		self.MIN_D3_D4_VALUE = Constants.MIN_D3_D4_VALUE
		self.LINEAR_JUMP_VALUE = Constants.LINEAR_JUMP_VALUE
		self.PLF_CURVE_VALUE = Constants.PLF_CURVE_VALUE
		self.B2B_FARE_PRICE_COMPARISON = Constants.B2B_FARE_PRICE_COMPARISON
		self.COVERAGE_PARAMETER = Constants.COVERAGE_PARAMETER
		self.AU_COLUMN_LENGTH = Constants.AU_COLUMN_LENGTH
		self.B2B_RDB_COUNT = Constants.B2B_RDB_COUNT
		self.LOWEST_B2B_RBD_VALUE = Constants.LOWEST_B2B_RBD_VALUE
		self.LOWEST_B2C_RBD_VALUE = Constants.LOWEST_B2C_RBD_VALUE
		self.HIGHEST_B2B_RBD_VALUE = Constants.HIGHEST_B2B_RBD_VALUE
		self.HIGHEST_B2C_RBD_VALUE = Constants.HIGHEST_B2C_RBD_VALUE
  
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
		if self.inputSource[Constants.CONST_INPUT_SOURCE][Constants.CONST_MARKET_LIST] == Constants.FILE:
			self.mktList = pd.read_csv(Constants.CONST_MARKET_LIST_FILE_PATH)
		else:
			marketListQuery = "SELECT Origin,Destin,FlightNumber,PerType,PerStart,PerEnd,DOW,strategyFlag,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusion,fareAnchor,fareOffset,FirstRBDAlloc,OtherRBDAlloc,B2BBackstop,B2CBackstop,B2BFactor,SkippingFactor,analystName,DaySpan,OwnFareFlag FROM market_list "
			try:
				self.mktList = pd.read_sql(marketListQuery, self.conn)
			except Exception as e:
				self.logger.error(e)
   
		if(self.debugMode):
			self.logger.info("Loaded Market List")
			self.logger.info(self.mktList)

	def updateRunAudit(self, b2cStatus, b2bStatus, flightNo, flightDate):
		if self.inputSource[Constants.CONST_LOGS][Constants.CONST_RUN_AUDIT] == Constants.DB:
			update_query = " UPDATE run_flight_date_audit SET b2cstatus = %s, b2bstatus = %s WHERE runId = %s and flightNumber = %s and flightDate = %s"
			try:
				self.wrconn.execute(update_query, b2cStatus, b2bStatus,  self.runId, flightNo, flightDate)
			except Exception as e:
				self.logger.error(e)

	def updateRunAuditb2c(self, b2cStatus, flightNo, flightDate):
		if self.inputSource[Constants.CONST_LOGS][Constants.CONST_RUN_AUDIT] == Constants.DB:
			update_query = " UPDATE run_flight_date_audit SET b2cstatus = %s WHERE runId = %s and flightNumber = %s and flightDate = %s"
			try:
				self.wrconn.execute(update_query, b2cStatus,  self.runId, flightNo, flightDate)
			except Exception as e:
				self.logger.error(e)
  
	def updateRunAuditb2b(self, b2bStatus, flightNo, flightDate):
		if self.inputSource[Constants.CONST_LOGS][Constants.CONST_RUN_AUDIT] == Constants.DB:
			update_query = " UPDATE run_flight_date_audit SET b2bstatus = %s WHERE runId = %s and flightNumber = %s and flightDate = %s"
			try:
				self.wrconn.execute(update_query, b2bStatus,  self.runId, flightNo, flightDate)
			except Exception as e:
				self.logger.error(e)
  
	def updateRunEnd(self, endTime):
		if self.inputSource[Constants.CONST_LOGS][Constants.CONST_RUN_AUDIT] == Constants.DB:
			updateQuery = " UPDATE allocation_run_audit SET end_time = %s WHERE run_id = %s "
			try:
				self.wrconn.execute(updateQuery, (endTime.strftime(Constants.DATE_TIME_FORMAT), self.runId))
			except Exception as e:
				self.logger.error(e)

	def areAllInputsReady(self):
		if self.inputSource[Constants.CONST_INPUT_SOURCE][Constants.CONST_INPUT_STATUS] == Constants.DB:
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


class OwnLogger:
	def __init__(self,runid,input):
		self.logger = logging.getLogger()
		self.logger.setLevel(logging.INFO)
		self.runid=runid
		self.input=input
	def info(self,messsage):
		if self.input==Constants.DB:
			self.logger.info(f"message:{messsage}  runid:{self.runid}")
		else:
			print(messsage)  
	def error(self,errormessage):
		if self.input==Constants.DB:
			self.logger.error(f"error:{errormessage} runid:{self.runid}")
		else:
			print(errormessage)