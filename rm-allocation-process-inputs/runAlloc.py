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
warnings.filterwarnings('ignore')

from datetime import date
from datetime import timedelta
from datetime import datetime
from dateutil import parser

class DynAlloc:

	def __init__(self, inputData, currParams):
		self.inputSource = inputData
		#Set this to true for more verbose scree display during the run
		self.debugMode = self.inputSource["debugMode"]

		if (currParams["runId"]):
			self.runId = currParams["runId"]
		else:
			self.runId = self.getUniqueId()
  
		self.TABLE_MARKET_FARES = "market_fares"
		self.BookloadTableName = "bookload"
		self.WAIT_TIME = 30 # in seconds
		
		self.MARKET_LIST_QUEUE_URL = self.inputSource["MARKET_LIST_QUEUE_URL"]
		self.RBD_QUEUE_URL = self.inputSource["RBD_QUEUE_URL"]
		self.NAV_DB_NAME = self.inputSource["NAV_DB_NAME"]

		self.sqs = currParams["sqs"]
		self.conn = currParams["dbconn"]
		self.wrconn = currParams["wrconn"]
		self.NavitaireConnection = currParams["NavitaireConnection"]
  
		self.generateConstantValues()

		#Load the Run Parameters
		self.loadRunParams()

		#Place Holders for Output
		self.allocResults = pd.DataFrame(columns=['Id', 'RunId', 'Origin','Destin','FltNum','DepDate','BookedLoad','TgtLoad', 'anchorFare','MktFare_Min','MktFare_Max','OpenRBD','SellingFare', 'Channel', 'HowDetermined'])
		self.AUFileData = []
		
		#Start the Allocation
		if (currParams["runSingleMarket"]):
			self.runDynamicAllocationSingleMarket(currParams, 1)
		else:
			self.runDynamicAllocation()

	def runDynamicAllocation(self):
		print("Starting Dynamic Allocation")
		startTime = self.getCurrentDateTime()
		username = os.getlogin()
		
		self.insertRunStart(startTime, username)

		#The list of markets to run the allocation
		numMarkets = self.mktList.shape[0]
		print("Market List has " + str(numMarkets) + " Rows")

		if(self.debugMode):
			print("Market List : ")
			print(self.mktList)

		for mktCounter in range(numMarkets):
			currParams = self.mktList.iloc[mktCounter]
			self.runDynamicAllocationSingleMarket(currParams, mktCounter)

		self.outputResults()

		endTime = self.getCurrentDateTime()
		self.updateRunEnd(endTime)
		runTime = (endTime - startTime).total_seconds()
		print("Allocation Completed in " + str(runTime) + " seconds")

	def runDynamicAllocationSingleMarket(self, currParams, mtkIndex):
		print("**********************************************")
		print("Starting Allocation for row : " + str(mtkIndex+1))

		if(self.debugMode):
			print(currParams)
		
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
		currParams['Strategy'] = currParams['fareAnchor']

		startDate, endDate, startNDO, endNDO = self.getStartEndDate(PerType, currDate, startNDO, endNDO)
		
		print("Allocation for " + origin + destination + " from " + str(startDate) + " to " + str(endDate))
		
		#Fetch the current booked load from Navitaire
		bookedLoad = self.getCurrentBookedLoads(origin, destination, startDate, endDate)

		tgtBookedLoad = None
		# If for whatever reason - unable to get the booked Load - Abort
		if(bookedLoad.shape[0] == 0):
			print("Unable to get the Booked Load - Aborting")
			self.allocResults = self.allocResults.append({"Id":self.getUniqueId(), "RunId":self.runId, "Origin":origin, "Destin":destination, "FltNum":"NA","DepDate":str(startDate), "BookedLoad":"NA", "TgtLoad":str(tgtBookedLoad), 'TimeRange': 'NA', 'dlfBand': 'NA', 'ndoBand': 'NA', 'anchorFare': 'NA', "MktFare_Min":"NA","MktFare_Max":"NA", "fareOffset": "NA", "OpenRBD": "NA", "SellingFare":"NA", 'Channel':"NA", 'fareAnchor':"NA",  'HowDetermined':"unable to get Booked loads from " + str(startDate) + " to " + str(endDate)}, ignore_index = True)
			self.insertSummaryRow()
		else:
			# check if inputs are available to be copied else wait and retry
			canCopy = False
			while canCopy == False:
				canCopy = self.areAllInputsReady()
				if canCopy == False:
					print('Waiting as inputs are busy...')
					time.sleep(self.WAIT_TIME)
   
			print('Inputs (market fares, channel_map etc) are ready. Copying inputs...')
			# create a market_fares_extract based on org/dest/timerange
			# marketFaresExtractName = self.createTableExtract(origin, destination, startDate, endDate, startTime, endTime)
			marketFaresExtractName = self.TABLE_MARKET_FARES
			print('Inputs Copy completed.')

			try: 
				# For each NDO
				for ndocounter in range(startNDO, endNDO+1):
					currDate = self.getCurrentDate()
					tgtDate = datetime.strftime(currDate + timedelta(days=ndocounter),"%Y-%m-%d")
					tgtBookedLoadList = self.getTargetBookedLoadFromBenchmarkCurve(ndocounter, curveId)
					if (len(tgtBookedLoadList) > 0):
						tgtBookedLoad = float(tgtBookedLoadList['LF'][0])
						B2CAvgFare=float(tgtBookedLoadList['B2C_Avg_Fare'][0])
						B2BAvgFare=float(tgtBookedLoadList['B2B_Avg_Fare'][0])
						self.runDynamicAllocationSingleMarketSingleDay(ndocounter, bookedLoad, tgtBookedLoad, currParams, marketFaresExtractName, B2CBackstop, B2BBackstop,B2CAvgFare,B2BAvgFare)
					else:
						#Cannot get a reference curve - Abort
						print("Unable to get a Target - Check the booking curves")
						self.allocResults = self.allocResults.append({"Id":self.getUniqueId(), "RunId":self.runId, "Origin":origin,"Destin":destination,"FltNum":"NA","DepDate":str(tgtDate), "BookedLoad":"NA", "TgtLoad":str(tgtBookedLoad), 'TimeRange': 'NA', 'dlfBand': 'NA', 'ndoBand': 'NA', 'anchorFare': 'NA', "MktFare_Min":"NA","MktFare_Max":"NA", "fareOffset": "NA", "OpenRBD":"NA", "SellingFare":"NA", 'fareAnchor':"NA",  'Channel':"NA", 'HowDetermined':"No Reference Curve Found"}, ignore_index = True)
						self.insertSummaryRow()
			except Exception as e:
				print("Exception occurred")
				traceback.print_exc()
				print(e)

	def runDynamicAllocationSingleMarketSingleDay(self, ndocounter, bookedLoad, tgtBookedLoad, currParams, marketFaresExtractName, B2CBackstop, B2BBackstop,B2CAvgFare,B2BAvgFare):
		currDate = self.getCurrentDate()
		tgtDate = datetime.strftime(currDate + timedelta(days=ndocounter),"%Y-%m-%d")
		dow = int(datetime.strftime(currDate + timedelta(days=ndocounter),"%w"))
		
		print("Determining pricing for NDO " + str(ndocounter) + " on  " + str(tgtDate))
		print("Target Pax Load is :" + str(tgtBookedLoad))

		ndoBookedLoad = bookedLoad[bookedLoad['DepartureDate'].astype(str).str.contains(str(tgtDate))]
		ndoBookedLoad = ndoBookedLoad.reset_index(drop=True)
		
		numFlts = ndoBookedLoad.shape[0]
		if(self.debugMode):
			print("Found " + str(numFlts) + " Flights")
			print(ndoBookedLoad)

		for fltCounter in range(numFlts):
			currFlt = ndoBookedLoad['FlightNumber'][fltCounter]
			currBookedLoad = ndoBookedLoad["bookedPax"][fltCounter]
			currCapacity = ndoBookedLoad['capacity'][fltCounter]
			currFltDate = datetime.strftime(ndoBookedLoad['DepartureDate'][fltCounter],Constants.DATE_FORMAT)

			self.runDynamicAllocationSingleFlight(currFlt, currBookedLoad, currCapacity, currFltDate, tgtBookedLoad, dow, currParams, marketFaresExtractName, ndocounter, B2CBackstop, B2BBackstop,B2CAvgFare,B2BAvgFare)

	def runDynamicAllocationSingleFlight(self, currFlt, currBookedLoad, currCapacity, currFltDate, tgtBookedLoad, dow, currParams, marketFaresExtractName, ndocounter, B2CBackstop, B2BBackstop,B2CAvgFare,B2BAvgFare):
		
		tgtBookloadFactor = tgtBookedLoad	
		tgtBookedLoad = tgtBookedLoad * currCapacity

		if(self.debugMode):
			print("Ready to start Allocation")
			print(currParams)
			print("DOW is : " + str(dow))
			print("Extract is " + str(currParams['DOW'])[dow:(dow+1)])

		if ((str(currFlt) == str(currParams['FlightNumber'])) or (currParams['FlightNumber'] == '*')) and (str(currParams['DOW'])[dow:(dow+1)] == "1"):
			print("Actual Booked Load for " + str(currParams['FlightNumber']) + " on " + str(currFltDate) + " is :" + str(currBookedLoad))

			totalCapacity = currCapacity
			bookedDeficit = round(tgtBookedLoad - currBookedLoad)

			remainingCapacity = totalCapacity - currBookedLoad
   
			dPLF = (currBookedLoad/totalCapacity) - tgtBookloadFactor
			plfValue = dPLF * 100
			plf= (currBookedLoad/totalCapacity) * 100
			print("plfValue is " + str(plfValue))
			ndoBand = self.getNdoBand(ndocounter)
			dplfBand = self.getdpflBand(plfValue)
			thresholdValue = self.getPlfThreshhold(ndocounter,str(currParams['FlightNumber']))
			self.thresholdflag=False
			if len(thresholdValue)!=0:
				if plf>thresholdValue[0]:
					self.thresholdflag=True
   
			# Get Dynamic D1 (Time) Value
			startTime, endTime, time_range = self.getDynamicTime(currParams, dplfBand, ndoBand)
   
			# Get Dynamic D2 (Offset & Criteria) Value
			criteria, offset = self.getDynamicD2Value(currParams, dplfBand, ndoBand)
   
			# assigning to the same field to maintain backward compatibility
			currParams['TimeWindowStart'] = startTime
			currParams['TimeWindowEnd'] = endTime
			currParams['fareAnchor'] = criteria
			currParams['fareOffset'] = offset
   
			#Actual Work Happens here
			#Get the B2C and B2B Fare ranges
			mktFareRangeB2C = self.getMarketFareRange('B2C',currFltDate, currParams, marketFaresExtractName)
			mktFareRangeB2B = self.getMarketFareRange('B2B',currFltDate, currParams, marketFaresExtractName)
			mktfareRangeB2C_minomaxo = self.getMarketFareRangeminomaxo('B2C', currFltDate, currParams, marketFaresExtractName)
			mktfareRangeB2B_minomaxo = self.getMarketFareRangeminomaxo('B2B', currFltDate, currParams, marketFaresExtractName)

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
			b2bRunId = self.getUniqueId()
			b2cRunId = self.getUniqueId()

			lowFareB2C = mktFareRangeB2C['LowFare'][0]
			hiFareB2C = mktFareRangeB2C['HiFare'][0]
			lowFareB2C_minomaxo = mktfareRangeB2C_minomaxo['LowFare'][0]
			hiFareB2C_minomaxo = mktfareRangeB2C_minomaxo['HiFare'][0]
			
			lowFareB2B = mktFareRangeB2B['LowFare'][0]
			hiFareB2B = mktFareRangeB2B['HiFare'][0]
			lowFareB2B_minomaxo = mktfareRangeB2B_minomaxo['LowFare'][0]
			hiFareB2B_minomaxo = mktfareRangeB2B_minomaxo['HiFare'][0]
			isB2BMarketFareAvailable = True
			isB2CMarketFareAvailable = True
			if(lowFareB2B == hiFareB2B == 0 and B2BAvgFare!=-99):
				lowFareB2B=B2BAvgFare
				hiFareB2B=B2BAvgFare
			elif (lowFareB2B == hiFareB2B == 0 and B2BAvgFare==-99):
				isB2BMarketFareAvailable=False
			if((lowFareB2C == hiFareB2C == 0) and B2CAvgFare!=-99):
				lowFareB2C=B2CAvgFare
				hiFareB2C=B2CAvgFare
			elif(lowFareB2C == hiFareB2C == 0 and B2CAvgFare==-99):
				isB2CMarketFareAvailable=False
		
			if(isB2CMarketFareAvailable):
				if currParams['fareAnchor']=="MIN" or currParams['fareAnchor']=="MAX":
					print("Market Fare Range for " + str(currFltDate) + " in the B2C Channel is :" + str(lowFareB2C) + " to " + str(hiFareB2C))
				else:
					print("Market Fare Range for " + str(currFltDate) + " in the B2C Channel is :" + str(lowFareB2C_minomaxo) + " to " + str(hiFareB2C_minomaxo))
				openDataB2C = self.determineRBDToOpen(currParams['Origin'], currParams['Destin'], lowFareB2C,hiFareB2C,lowFareB2C_minomaxo, hiFareB2C_minomaxo, currParams['fareAnchor'],currParams['fareOffset'],'B2C', B2CBackstop)
			else:
				openDataB2C = {'RBD':['NA'], 'Fare':['NA'], 'FareResult':['B2C Market Data NA - Skipping allocation'],'anchorFare': ['NA'],'statusInd':[1]}
	
			if(isB2BMarketFareAvailable):
				if currParams['fareAnchor']=="MIN" or currParams['fareAnchor']=="MAX":
					print("Market Fare Range for " + str(currFltDate) + " in the B2B Channel is :" + str(lowFareB2B) + " to " + str(hiFareB2B))
				else:
					print("Market Fare Range for " + str(currFltDate) + " in the B2C Channel is :" + str(lowFareB2B_minomaxo) + " to " + str(hiFareB2B_minomaxo))
				openDataB2B = self.determineRBDToOpen(currParams['Origin'], currParams['Destin'], lowFareB2B,hiFareB2B,lowFareB2B_minomaxo, hiFareB2B_minomaxo, currParams['fareAnchor'],currParams['fareOffset'],'B2B', B2BBackstop)
			else:
				openDataB2B = {'RBD':['NA'], 'Fare':['NA'], 'FareResult':['B2B Market Data NA - Skipping allocation'],'anchorFare': ['NA'],'statusInd':[3]}
			if currParams['fareAnchor']=="MINO" or currParams['fareAnchor']=="MAXO":
					lowFareB2B=lowFareB2B_minomaxo
					lowFareB2C=lowFareB2C_minomaxo
					hiFareB2B=hiFareB2B_minomaxo
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
				if self.inputSource['isUpdateNavitaire']:
					currRec.append(b2cRunId) #10
					currRec.append(b2bRunId) #11

				# First - Adding all B2C RBD's
				b2cAllocationsUnits = self.B2CAllocation(currBookedLoad, currParams, openDataB2C, currRec, totalCapacity, bookedDeficit, dPLF, remainingCapacity, ndocounter)

				isUpsellFlag = False
				#Now add all the Z RBDs
				isUpsellFlag = self.B2BAllocation(currBookedLoad, currParams, openDataB2B, currRec, totalCapacity, dPLF, b2cAllocationsUnits)

				if(isUpsellFlag):
					openDataB2B = {'RBD':['NA'], 'Fare':['NA'], 'FareResult':['B2B allocation skipped - Flight in upsell'],'anchorFare': ['NA'],'statusInd':[1]}
				
				#Ends statsusInd == 0
				if self.thresholdflag:
					currRec.extend(['L', '', -1, -1, 0, -1, -1, 'P', '', -1, -1, 0, -1, -1])
				else:
					currRec.extend(['L', '', -1, -1, -1, -1, -1, 'P', '', -1, -1, -1, -1, -1])
     
				self.AUFileData.append(currRec)
				
				if self.inputSource['isUpdateNavitaire']:
					self.updateNavitaire(currRec,self.thresholdflag)
			
			self.allocResults = self.allocResults.append({"Id":b2cRunId, "RunId":self.runId,"Origin":currParams['Origin'],"Destin":currParams['Destin'],"FltNum":str(currFlt),"DepDate":str(currFltDate), "BookedLoad":str(currBookedLoad), "TgtLoad":str(tgtBookedLoad), 'TimeRange': str(time_range), 'dlfBand': str(dplfBand), 'ndoBand': str(ndoBand),  'anchorFare':str(openDataB2C["anchorFare"][0]), "MktFare_Min":str(round(float(lowFareB2C))), 'MktFare_Max': hiFareB2C, "OpenRBD":str(openDataB2C['RBD'][0]), "SellingFare":str(openDataB2C["Fare"][0]), 'Channel':'B2C', 'HowDetermined':str(openDataB2C['FareResult'][0]), 'fareAnchor': currParams['fareAnchor'], 'fareOffset': currParams['fareOffset'], 'Backstop': currParams['B2CBackstop']}, ignore_index = True)
			self.insertSummaryRow()
			
			self.allocResults = self.allocResults.append({"Id":b2bRunId, "RunId":self.runId,"Origin":currParams['Origin'],"Destin":currParams['Destin'],"FltNum":str(currFlt),"DepDate":str(currFltDate), "BookedLoad":str(currBookedLoad), "TgtLoad":str(tgtBookedLoad), 'TimeRange': str(time_range), 'dlfBand': str(dplfBand), 'ndoBand': str(ndoBand), 'anchorFare':str(openDataB2B["anchorFare"][0]), "MktFare_Min":str(round(float(lowFareB2B))), 'MktFare_Max': hiFareB2B, "OpenRBD":str(openDataB2B['RBD'][0]), "SellingFare":str(openDataB2B["Fare"][0]), 'Channel':'B2B', 'HowDetermined':str(openDataB2B['FareResult'][0]), 'fareAnchor': currParams['fareAnchor'], 'fareOffset': currParams['fareOffset'], 'Backstop': currParams['B2BBackstop']}, ignore_index = True)
			self.insertSummaryRow()
				
		else:
			print("Run Parameters requested to skip - (FlightNumber, DOW, or Status)")

	def B2BAllocation(self, currBookedLoad, currParams, openDataB2B, currRec, totalCapacity, dPLF, b2cAllocationsUnits):
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

	def B2CAllocation(self, currBookedLoad, currParams, openDataB2C, currRec, totalCapacity, bookedDeficit, dPLF, remainingCapacity, ndocounter):
		
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
		b2bMaxRDBCount = 10
		#reset
		#Loop Thru
		while b2cctr < B2CRBDs.shape[0] or b2bctr < b2bMaxRDBCount :
			
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
					incremental, incrementalb2b = self.incrementalAllocation(currBookedLoad, bookedDeficit, incremental, firstRDBAllocation, otherRDBAllocation, b2bFactor, incrementalb2b)

				#Negative -99 Allocation
				elif(firstRDBAllocation == Constants.DYNAMIC_D3_D4):
					d3Allocation, incremental, jumpingRBDPrice, openRBDIndex, entryRDB, incrementalb2b = self.negativeValueJumpingAllocation(remainingRBD, currBookedLoad, bookedDeficit, dPLF, remainingCapacity, ndocounter, maxRBDPrice, openingRBDPirce, openRBDIndex, incremental, entryRDB, otherRDBAllocation, price, nextPrice, d3Allocation, jumpingRBDPrice, b2bFactor, incrementalb2b)

				#Negative -1 Allocation	
				elif(firstRDBAllocation == Constants.DEFICIT_ALLOCATION):
					bookedDef = int(round(bookedDeficit,0))
					if(bookedDef <= 0):
						bookedDef = 0
					incremental, incrementalb2b = self.incrementalAllocation(currBookedLoad, bookedDeficit, incremental, bookedDef, otherRDBAllocation, b2bFactor, incrementalb2b)

				#Negative -2 Allocation
				elif(firstRDBAllocation == Constants.LINEAR_ALLOCATION):
					allocation = int(round(remainingCapacity/otherRDBAllocation,0))
					incremental, incrementalb2b = self.incrementalAllocation(currBookedLoad, bookedDeficit, incremental, allocation, allocation, b2bFactor, incrementalb2b)

				#Negative -3 Allocation
				elif(firstRDBAllocation == Constants.LINEAR_ALLOCATION_WITH_JUMP):
					allocation = int(round(remainingCapacity/otherRDBAllocation,0))
					jumpPercent = self.LINEAR_JUMP_VALUE
					openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b = self.jumpingAllocation(currBookedLoad, bookedDeficit, openRBDIndex, incremental, jumpPercent, price, nextPrice, allocation, allocation, jumpingRBDPrice, b2bFactor, incrementalb2b)
					
				if (incremental >= totalCapacity):
					incremental = totalCapacity
     
				if (incrementalb2b >= totalCapacity):
					incrementalb2b = totalCapacity
     
				if (RBDLabel == 'Y'):
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

	def incrementalAllocation(self, currBookedLoad, bookedDeficit, incremental, firstRDBAllocation, otherRDBAllocation, b2bFactor, incrementalb2b):
		if(incremental < 0):
			# First Alloctation
			if(firstRDBAllocation >= 0):
				#allocate the FirstRBD 
				incremental = currBookedLoad + firstRDBAllocation
				incrementalb2b = currBookedLoad + (firstRDBAllocation * b2bFactor)
			else:
				#negtive number means allocate the full deficit - Only if the deficit is positive - Lagging
				if(bookedDeficit > 0):
					incremental = currBookedLoad + bookedDeficit
				else:
					incremental = currBookedLoad + otherRDBAllocation
					incrementalb2b = currBookedLoad + (otherRDBAllocation * b2bFactor)
		else:
			incremental = incremental + otherRDBAllocation
			incrementalb2b = incrementalb2b + (otherRDBAllocation * b2bFactor)
		return incremental, incrementalb2b

	def jumpingAllocation(self, currBookedLoad, bookedDeficit, openRBDIndex, incremental, otherRDBAllocation, price, nextPrice, d3Allocation, d4Allocation, jumpingRBDPrice, b2bFactor, incrementalb2b):
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
			jumpingRBDPrice = price + (price * (otherRDBAllocation / 100))
		else:
			nextPriceDiff = abs(nextPrice - jumpingRBDPrice)
			currPriceDiff = abs(price - jumpingRBDPrice)
			if(jumpingRBDPrice >= nextPrice):
				pass
			elif(currPriceDiff <= nextPriceDiff):
				incremental = incremental + d4Allocation
				incrementalb2b = incrementalb2b + (d4Allocation * b2bFactor)
				jumpingRBDPrice = price + (price * (otherRDBAllocation / 100))
				openRBDIndex = openRBDIndex + 1
			elif(currPriceDiff > nextPriceDiff):
				pass
		return openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b

	def negativeValueJumpingAllocation(self, remainingRBD, currBookedLoad, bookedDeficit, dPLF, remainingCapacity, ndocounter, maxRBDPrice, openingRBDPirce, openRBDIndex, incremental, entryRDB, otherRDBAllocation, price, nextPrice, d3Allocation, jumpingRBDPrice, b2bFactor, incrementalb2b):
		openRBDIndex, entryRDB, d3Allocation, d4Allocation, remainingRBD = self.d3d4Calculations(dPLF, remainingCapacity, ndocounter, maxRBDPrice, openingRBDPirce, openRBDIndex, entryRDB, otherRDBAllocation, remainingRBD, d3Allocation)
		
		if(otherRDBAllocation == 0):
			incremental, incrementalb2b = self.incrementalAllocation(currBookedLoad, bookedDeficit, incremental, d3Allocation, d4Allocation, b2bFactor, incrementalb2b)
			openRBDIndex = openRBDIndex + 1
		else:
			openRBDIndex, incremental, jumpingRBDPrice, incrementalb2b = self.jumpingAllocation(currBookedLoad, bookedDeficit, openRBDIndex, incremental, otherRDBAllocation, price, nextPrice, d3Allocation, d4Allocation, jumpingRBDPrice, b2bFactor, incrementalb2b)
		return d3Allocation, incremental, jumpingRBDPrice, openRBDIndex, entryRDB, incrementalb2b

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
		queryResult = pd.read_sql(RBDQuery, self.conn)
 
		return queryResult

	def determineRBDToOpen(self, origin, destin, lowFare, hiFare,lowFare_minomaxo, hifare_minomaxo, strategy, offset, channel, backstop):

		openResult = pd.DataFrame(columns=['RBD', 'Fare', 'FareResult', 'anchorFare', 'statusInd'])

		if (strategy == 'MAX'):
			anchorFare = hiFare
		if (strategy == 'MIN'):
			anchorFare = lowFare
		if (strategy == 'MAXO'):
			anchorFare = hifare_minomaxo
		if (strategy == 'MINO'):
			anchorFare = lowFare_minomaxo
		
		print("Strategy applied for " + channel + " Channel is " + strategy )
		print("Anchor Fare is " + str(anchorFare) + " with an offset of " + str(offset))

		RBDQuery = "select RBD, Total from Fares where Org = '" + origin + "' and Dst = '" + destin + "' "
  
		rbddata = pd.read_sql(RBDQuery, self.conn)
		if(rbddata.shape[0] == 0):
			print('QP Fare not available. Please validate Fares table')
			openResult = openResult.append({'RBD':'NA', 'Fare':'NA', 'FareResult':'QP Fares not available','anchorFare':anchorFare,'statusInd':1}, ignore_index = True)
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
			print(RBDQuery)

		rbddata = pd.read_sql(RBDQuery, self.conn)

		if(rbddata.shape[0] == 0):
			print("Market Fare is out of price ladder range - allocating at extreme values")
			#Fetch the Lowest or highest available fares

			defFareData = self.fetchExtremePublishedFare(origin, destin, channel, 'LOW', backstop)
			if (offset >= 0):
				defFareData = self.fetchExtremePublishedFare(origin, destin, channel, "HI", backstop)

			# Edge case where backstop is placed at a very large value - allocates at Z0 or Y levels
			if (defFareData.shape[0] == 0):
				defFareData = self.fetchExtremePublishedFare(origin, destin, channel, 'HI', 0) 	

			if (channel == 'B2C'):
				openResult = openResult.append({'RBD':defFareData['RBD'][0], 'Fare':defFareData['Total'][0], 'FareResult':'Unable to Match Mkt - B2C allocation at extreme value','anchorFare':anchorFare,'statusInd':2}, ignore_index = True)
			if (channel == 'B2B'):
				if (defFareData['RBD'][0] == 'Z0'):
					openResult = openResult.append({'RBD':'NA', 'Fare':'NA', 'FareResult':'Market Fare higher than Z0 levels - skipping B2B allocation','anchorFare':anchorFare,'statusInd':1}, ignore_index = True)
				else:
					openResult = openResult.append({'RBD':defFareData['RBD'][0], 'Fare':defFareData['Total'][0], 'FareResult':'Unable to Match Mkt - B2B allocation at extreme value','anchorFare':anchorFare,'statusInd':2}, ignore_index = True)
		else:
			backstopFlag = False
			extremeFareFlag = False
			# We got back data purely on the basis of anchor fare - comparison with offset remains which is done in the subsequent section

			# Calculating the offset that is required based on entries remaining in the RBD table
			offsetToFetch = min(abs(offset), rbddata.shape[0] - 1) 
			if(offset < 0):
				offsetToFetch = min(abs(offset+1), rbddata.shape[0] - 1) 

			# Comparison with the backstop table - to ensure we are picking a value higher than the backstop
			defFareData = self.fetchExtremePublishedFare(origin, destin, channel, 'LOW', backstop)
   
			if(defFareData.shape[0] != 0):
				if(rbddata['Total'][0] < defFareData['Total'][0] and backstop > 0):
					backstopFlag = True
    
			# Edge case if the backstop values are placed very high - in which case it will place the backstop at Z0 and Y levels 
			if(defFareData.shape[0] == 0):
				defFareData = self.fetchExtremePublishedFare(origin, destin, channel, 'HI', 0)
		
			if(rbddata['RBD'][0] == 'Z9' or rbddata['RBD'][0] == 'V9'):
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
					if(rbddata['Total'][0] < defFareData['Total'][offsetToFetch] and backstop > 0):
						backstopFlag = True
					RBDtoOpen = defFareData['RBD'][0]
					FareToOpen = defFareData['Total'][0]
				else:
					RBDtoOpen = rbddata['RBD'][offsetToFetch]
					FareToOpen = rbddata['Total'][offsetToFetch]		
				
			print('Determining ' + channel + ' Fare ' + str(FareToOpen) + " " + str(RBDtoOpen))

			if (channel == 'B2B'):
				#Special handling for B2B - if the B2B Anchor fare is higher than Z0 - Skip B2B
				limitB2BFare_Data = self.fetchExtremePublishedFare(origin, destin, channel, 'HI', 0)
				limitB2BFare = float(limitB2BFare_Data['Total'][0])

				if (float(anchorFare) > limitB2BFare):
					#Outside of B2B Levels - stick with B2C
					openResult = openResult.append({'RBD':'NA', 'Fare':'NA', 'FareResult':'Market Fare higher than Z0 levels - skipping B2B allocation','anchorFare':anchorFare,'statusInd':1}, ignore_index = True)
				else:
					if(offsetToFetch == rbddata.shape[0]):
						openResult = openResult.append({'RBD':RBDtoOpen, 'Fare':str(FareToOpen), 'FareResult':'Not Enough Levels available - taking the Max/Min Available','anchorFare':anchorFare,'statusInd':2}, ignore_index = True)
					else:
						if(extremeFareFlag):
							openResult = openResult.append({'RBD':RBDtoOpen, 'Fare':str(FareToOpen), 'FareResult':'Unable to Match Mkt - B2B allocation at extreme value','anchorFare':anchorFare, 'statusInd':0}, ignore_index = True)
						else:
							if(backstopFlag):
								openResult = openResult.append({'RBD':RBDtoOpen, 'Fare':str(FareToOpen), 'FareResult':'Matched and allocated - Backstop Hit','anchorFare':anchorFare,'statusInd':0}, ignore_index = True)
							else:
								openResult = openResult.append({'RBD':RBDtoOpen, 'Fare':str(FareToOpen), 'FareResult':'Matched and allocated','anchorFare':anchorFare,'statusInd':0}, ignore_index = True)
			else:
				if(offsetToFetch == rbddata.shape[0]):
					openResult = openResult.append({'RBD':RBDtoOpen, 'Fare':str(FareToOpen), 'FareResult':'Not Enough Levels available - taking the Max/Min Available','anchorFare':anchorFare,'statusInd':2}, ignore_index = True)
				else:
					if(extremeFareFlag):
							openResult = openResult.append({'RBD':RBDtoOpen, 'Fare':str(FareToOpen), 'FareResult':'Unable to Match Mkt - B2C allocation at extreme value','anchorFare':anchorFare,'statusInd':0}, ignore_index = True)
					else:
						if(backstopFlag):
							openResult = openResult.append({'RBD':RBDtoOpen, 'Fare':str(FareToOpen), 'FareResult':'Matched and allocated - Backstop Hit','anchorFare':anchorFare,'statusInd':0}, ignore_index = True)
						else:
							openResult = openResult.append({'RBD':RBDtoOpen, 'Fare':str(FareToOpen), 'FareResult':'Matched and allocated','anchorFare':anchorFare,'statusInd':0}, ignore_index = True)

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
		defFareData = pd.read_sql(RBDQuery, self.conn)

		return defFareData

	def getMarketFareRange(self, channel, currFltDateStr, currParams, marketFaresExtractName):
	
		currFltDate = datetime.strptime(currFltDateStr, Constants.DATE_FORMAT).date()		
		mktLowestQuery = "Select ifnull(min(gross_fare),0) as LowFare, ifnull(max(gross_fare),0) as HiFare from " + marketFaresExtractName + " where source_airport = '" 
		mktLowestQuery = mktLowestQuery + currParams['Origin'] + "' and destination_airport = '" + currParams['Destin'] + "'"
		mktLowestQuery = mktLowestQuery + " and departure_date = '" + str(currFltDate) + "'"
		mktLowestQuery = mktLowestQuery + " and str_to_date(outbound_departuretime,'%%T') >= str_to_date('" + currParams['TimeWindowStart'] + "','%%T')"
		mktLowestQuery = mktLowestQuery + " and str_to_date(outbound_departuretime,'%%T') <= str_to_date('" + currParams['TimeWindowEnd'] + "','%%T')"
		mktLowestQuery = mktLowestQuery + " and channel = '" + channel + "' "
		mktLowestQuery = mktLowestQuery + " and outbound_airline_code <> '" + currParams['CarrExlusion'] + "' AND outbound_airline_code != 'QP' AND outbound_airline_code!='G8' "
		if(self.inputSource[Constants.CONST_INPUT_SOURCE][Constants.CONST_MARKET_FARES] != Constants.TEST_DATA):
			mktLowestQuery = mktLowestQuery + " AND created_at > NOW() - INTERVAL 2 DAY "
   
		mktFareRange = pd.read_sql(mktLowestQuery, self.conn)


		return mktFareRange

	def getMarketFareRangeminomaxo(self, channel, currFltDateStr, currParams, marketFaresExtractName):

		currFltDate = datetime.strptime(
			currFltDateStr, Constants.DATE_FORMAT).date()
		mktLowestQuery = "Select gross_fare from " + \
			marketFaresExtractName + " where source_airport = '"
		mktLowestQuery = mktLowestQuery + \
			currParams['Origin'] + "' and destination_airport = '" + \
			currParams['Destin'] + "'"
		mktLowestQuery = mktLowestQuery + \
			" and departure_date = '" + str(currFltDate) + "'"
		mktLowestQuery = mktLowestQuery + \
			" and str_to_date(outbound_departuretime,'%%T') >= str_to_date('" + \
			currParams['TimeWindowStart'] + "','%%T')"
		mktLowestQuery = mktLowestQuery + \
			" and str_to_date(outbound_departuretime,'%%T') <= str_to_date('" + \
			currParams['TimeWindowEnd'] + "','%%T')"
		mktLowestQuery = mktLowestQuery + " and channel = '" + channel + "' "
		mktLowestQuery = mktLowestQuery + " and outbound_airline_code <> '" + \
			currParams['CarrExlusion'] + \
			"' AND outbound_airline_code != 'QP' AND outbound_airline_code!='G8' "
		if(self.inputSource[Constants.CONST_INPUT_SOURCE][Constants.CONST_MARKET_FARES] != Constants.TEST_DATA):
			mktLowestQuery = mktLowestQuery + " AND created_at > NOW() - INTERVAL 2 DAY "
		mktFareRange = pd.read_sql(mktLowestQuery, self.conn)
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
		elif len(fare_list)==0:
			min_fare=0
			max_fare=0
		else:
			min_fare=fare_list[0]
			max_fare=fare_list[0]
		mktFareRange = pd.DataFrame(
			{'LowFare': [min_fare], 'HiFare': [max_fare]})
		return mktFareRange

	def getTargetBookedLoadFromBenchmarkCurve(self, ndocounter, curveID):
		tgtBookedLoad = []
  		#Fetch the Target Booked Load from the curve

		bookingCurveQuery = "Select LF,B2C_Avg_Fare,B2B_Avg_Fare from Curves where NDO = " + str(ndocounter) + " and CurveID = '" + curveID + "'"
		benchmark = pd.read_sql(bookingCurveQuery, self.conn)
		if(benchmark.shape[0] > 0):
			tgtBookedLoad = benchmark

		return tgtBookedLoad

	def deleteTableExtract(self, tableName):
		dropQuery = "DROP TABLE IF EXISTS " + tableName
		self.conn.execute(dropQuery)

	def insertSummaryRow(self):
		if self.inputSource[Constants.CONST_LOGS][Constants.CONST_RUN_SUMMARY] == Constants.DB:
			latest_row = self.allocResults.iloc[-1]
			insertQuery = " INSERT INTO run_summary ( Id, Origin, Destin, FltNum, DepDate, BookedLoad, TgtLoad, MktFare_Min, OpenRBD, SellingFare, Channel, HowDetermined, RunId, CreatedAt, MktFare_Max, fareAnchor, fareOffset, TimeRange, dlfBand, ndoBand) "		
			insertQuery = insertQuery + " VALUES ('"+ latest_row['Id'] + "', '"+latest_row['Origin'] + "', '"+latest_row['Destin'] + "', '"+ latest_row['FltNum'] + "', '"+latest_row['DepDate'] + "', '"+latest_row['BookedLoad'] + "', '"+latest_row['TgtLoad'] + "', '"+str(latest_row['MktFare_Min']) + "', '"+latest_row['OpenRBD'] + "', '"+str(latest_row['SellingFare']) + "', '"+latest_row['Channel'] + "', '"+latest_row['HowDetermined'] + "', '"+latest_row['RunId'] + "', '" + self.getCurrentDateTime().strftime(Constants.DATE_TIME_FORMAT) +"','"+str(latest_row['MktFare_Max']) + "','"+str(latest_row['anchorFare']) + "','"+str(latest_row['fareOffset']) +"','"+str(latest_row['TimeRange']) +"','"+str(latest_row['dlfBand']) +"','"+str(latest_row['ndoBand']) + "')"
			self.wrconn.execute(insertQuery)

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
			NavitaireQuery = NavitaireQuery + " left outer join "+self.NAV_DB_NAME+"[rez].PassengerJourneySegment PJS on PJS.PassengerID = PJL.PassengerID and PJS.SegmentID = PJL.SegmentID  "
			NavitaireQuery = NavitaireQuery + " left outer join "+self.NAV_DB_NAME+"[rez].BookingPassenger BP on  BP.PassengerID = PJS.PassengerID "
			NavitaireQuery = NavitaireQuery + " where IL.Lid > 0 and IL.Status NOT IN (2) and IL.DepartureDate >= '" + str(startDate) + "' and IL.DepartureDate <= '" +str(endDate) + "' and (PJS.FareBasis != 'SO7NBSOL' or PJS.FareBasis IS NULL)"
			NavitaireQuery = NavitaireQuery + " and IL.DepartureStation = '" + origin + "' and IL.ArrivalStation = '" + destin + "'"
			NavitaireQuery = NavitaireQuery + ") A group by Origin, Destination, FlightNumber, DepartureDate order by Origin, Destination, DepartureDate "
			con = self.NavitaireConnection
   
		bookedData = pd.read_sql(NavitaireQuery, con)
   
		if(self.debugMode):
			print(NavitaireQuery)
   
		return bookedData

	def getNdoBand(self, startNDO):
		ndoBandQuery = "SELECT ndo_band FROM ndo_bands where start <= " + str(startNDO) + " and end > " + str(startNDO) 
		ndoBandList = pd.read_sql(ndoBandQuery, self.conn)
		ndoBand = ndoBandList['ndo_band'][0]
		return ndoBand

	def getdpflBand(self, dPLF):
		dplfBandQuery = "SELECT dplf_band FROM dplf_bands where start <= " + str(dPLF) + " and end > " + str(dPLF) 
		dplfBandList = pd.read_sql(dplfBandQuery, self.conn)
		dplfBand = dplfBandList['dplf_band'][0]
		return dplfBand

	def getPlfThreshhold(self, ndocounter, flightNumber):
		pflThresholdQuery = "SELECT plf_threshold FROM config_pfl_threshold WHERE flightNumber = '"+ str(flightNumber) +"' AND NDO = '"+ str(ndocounter) +"' UNION " 
		pflThresholdQuery = pflThresholdQuery+ " SELECT plf_threshold FROM config_pfl_threshold WHERE flightNumber = '*' " 
		pflThresholdQuery = pflThresholdQuery+ " AND NOT EXISTS (SELECT 1 FROM config_pfl_threshold WHERE flightNumber = '"+ str(flightNumber) +"') AND NDO = '"+ str(ndocounter) +"' LIMIT 1" 
		pflThresholdList = pd.read_sql(pflThresholdQuery, self.conn)
		threshold = pflThresholdList['plf_threshold']
		return threshold
     
	def getDynamicTime(self, currParams, dplfBand, ndoBand):
		startTime = currParams['TimeWindowStart']
		endTime = currParams['TimeWindowEnd']
		time_range = '0'
		if (str(currParams['strategyFlag']) == '1'):
			dynamicTimeList = self.getd1d2StrategyValue(currParams['Strategy'], dplfBand, ndoBand)
			time_range = dynamicTimeList['time_range'][0]
			
			rangeList = self.getTimeRange(time_range)

			if(rangeList['type'][0] == Constants.CONST_ABSOLUTE):
				startTime = rangeList['start'][0]
				endTime = rangeList['end'][0]
	
			if(rangeList['type'][0] == Constants.CONST_RELATIVE):
				startTime, endTime = self.getRelativeTImeValue(currParams, rangeList)
		
			if(rangeList['type'][0] == Constants.CONST_REFERENCE):
				rangeList = self.getTimeReferenceValue(currParams)
				startTime = rangeList['ar_start'][0]
				endTime = rangeList['ar_end'][0]
   
		return str(startTime), str(endTime), str(time_range)

	def getTimeRange(self, time_range):
		timeRangeQuery = "SELECT type, start, end FROM time_ranges where time_range = '"+ time_range +"'"
		rangeList = pd.read_sql(timeRangeQuery, self.conn)
		return rangeList

	def getTimeReferenceValue(self, currParams):
		timeRangeQuery = "SELECT ar_start, ar_end FROM allocation_acceptable_range_d1 where airports = '"+ currParams['Origin'] +"'"
		rangeList = pd.read_sql(timeRangeQuery, self.conn)
		return rangeList

	def getRelativeTImeValue(self, currParams, rangeList):
		start_datetime_object = datetime.strptime(currParams['PerStart'], Constants.DATE_FORMAT)
		end_datetime_object = datetime.strptime(currParams['PerEnd'], Constants.DATE_FORMAT)
	
		startDateTime = currParams['PerStart'] + ' ' + currParams['TimeWindowStart']
		endDateTime = currParams['PerEnd'] + ' ' + currParams['TimeWindowEnd']
	
		startTime = datetime.strptime(startDateTime, Constants.DATE_TIME_FORMAT2) + timedelta(minutes= int(rangeList['start'][0]))
		endTime = datetime.strptime(endDateTime, Constants.DATE_TIME_FORMAT2) + timedelta(minutes= int(rangeList['end'][0]))

		startTime = max(startTime, start_datetime_object.replace(hour=int(Constants.CONST_START_TIME[:2]), minute=int(Constants.CONST_START_TIME[3:])))
		endTime = min(endTime, end_datetime_object.replace(hour=int(Constants.CONST_END_TIME[:2]), minute=int(Constants.CONST_END_TIME[3:])))
	
		startTime = str(startTime.hour) + ":" + str(startTime.minute)
		endTime = str(endTime.hour) + ":" + str(endTime.minute)
  
		return startTime, endTime

	def getDynamicD2Value(self, currParams, dplfBand, ndoBand):
		criteria = currParams['fareAnchor']
		offset = currParams['fareOffset']

		if (str(currParams['strategyFlag'])== '1'):
			dynamicD2List = self.getd1d2StrategyValue(currParams['Strategy'], dplfBand, ndoBand)
			criteria = dynamicD2List['criteria'][0]
			offset = dynamicD2List['offset'][0]
		
		return criteria, offset

	def getd1d2StrategyValue(self, strategy, dplfBand, ndoBand):
		dynamicD1D2Query = "SELECT criteria, time_range, offset FROM d1_d2_strategies where ndo_band = "+ str(ndoBand) +" and dplf_band = "+ str(dplfBand) +" and strategy = '"+ strategy +"'"
		dynamicList = pd.read_sql(dynamicD1D2Query, self.conn)
		return dynamicList

	def outputResults(self):
		
		AUlist=['Type','Flight','StartDate','EndDate','DayOfWeek','Aircraft Type','Aircraft Suffix','Lid','Capacity','Status']
		length=Constants.AU_COLUMN_LENGTH
		for i in range(length):
			AUlist.append(f'Class Of Service AU Class {i+1}')
			AUlist.append(f'Class Type AU Class {i+1}')
			AUlist.append(f'Class Nest AU Class {i+1}')
			AUlist.append(f'Class Rank AU Class {i+1}')
			AUlist.append(f'Class AU AU Class {i+1}')
			AUlist.append(f'Class Allotted AU Class {i+1}')
			AUlist.append(f'AP Restriction AU Class {i+1}')	
		AUFrame = pd.DataFrame(self.AUFileData, columns=AUlist)

		print("Final Allocation Results:")
		print(self.allocResults)
		if self.inputSource[Constants.CONST_OUTPUT][Constants.CONST_AU_GRID] == Constants.FILE:
			AUFrame.to_csv(Constants.CONST_AU_GRID_FILE_PATH, index=False)
   
		if self.inputSource[Constants.CONST_OUTPUT][Constants.CONST_SUMMARY_FILE] == Constants.FILE:
			self.allocResults.to_csv(Constants.CONST_SUMMARY_FILE_PATH, index=False)

	def insertRunStart(self, startTime, username):
		if self.inputSource[Constants.CONST_LOGS][Constants.CONST_RUN_AUDIT] == Constants.DB:
			currVersionQuery = "SELECT tableName, curr_version FROM currentVersion"
			currVersionValues = pd.read_sql(currVersionQuery, self.conn)

			table_versions = {}
			for index,currVersion in currVersionValues.iterrows():
				table_versions[currVersion['tableName']] = currVersion['curr_version']

			type = "Local Run"
			
			insert_query = " INSERT INTO allocation_run_audit (run_id, start_time, user, dtd_start, dtd_end, Curves, d1_d2_strategies, market_list, Fares, type) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
			self.wrconn.execute(insert_query,
						(self.runId, startTime.strftime(Constants.DATE_TIME_FORMAT), username, 0, 365, table_versions['Curves'], table_versions['d1_d2_strategies'], type, table_versions['Fares'], type))

	def updateNavitaire(self, rec,thresholdflag):
		auData = {"runId": self.runId, "type": rec[0], "flight": rec[1], "startDate": rec[2], "endDate": rec[3], "dayOfWeek": rec[4],
                  "aircraftType": rec[5], "aircraftSuffix": rec[6], "lid": rec[7], "capacity": rec[8], "status": rec[9], "b2cRunId": rec[10], "b2bRunId": rec[11], "auClasses": {}}

		gap = 11
		for i in range(Constants.AU_COLUMN_LENGTH-2):
			auClass = {"classOfService": rec[gap + i*7 + 1], "classType": rec[gap + i*7 + 2], "classNest": int(rec[gap + i*7 + 3]),
                       "classRank": int(rec[gap + i*7 + 4]), "classAU": int(rec[gap + i*7 + 5]), "classAllotted": int(rec[gap + i*7 + 6]), "apRestriction": int(rec[gap + i*7 + 7])}
			auData['auClasses'][auClass['classOfService']] = auClass
		if thresholdflag:
				auClass = {"classOfService": "L", "classType": '', "classNest": -1,
						"classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}
				auData['auClasses'][auClass['classOfService']] = auClass

				auClass = {"classOfService": "P", "classType": '', "classNest": -1,
						"classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}
				auData['auClasses'][auClass['classOfService']] = auClass
		else:
				auClass = {"classOfService": "L", "classType": '', "classNest": -1,
						"classRank": -1, "classAU": -1, "classAllotted": -1, "apRestriction": -1}
				auData['auClasses'][auClass['classOfService']] = auClass

				auClass = {"classOfService": "P", "classType": '', "classNest": -1,
						"classRank": -1, "classAU": -1, "classAllotted": -1, "apRestriction": -1}
				auData['auClasses'][auClass['classOfService']] = auClass

		auDataStr = json.dumps(auData)
		#print(auDataStr)
        # Send to queue for the market
		self.sqs.send_message(
            QueueUrl=self.RBD_QUEUE_URL,
            MessageBody=auDataStr
        )

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
  
		parameterValueQuery = "SELECT parameterKey,parameterValue FROM rm_parameter_values"
		parameterValue = pd.read_sql(parameterValueQuery, self.conn)
		for index,val in parameterValue.iterrows():
			if val['parameterKey'] == 'MIN_D3_D4_VALUE':
				self.MIN_D3_D4_VALUE = val['parameterValue']
			if val['parameterKey'] == 'LINEAR_JUMP_VALUE':
				self.LINEAR_JUMP_VALUE = val['parameterValue']
			if val['parameterKey'] == 'PLF_CURVE_VALUE':
				self.PLF_CURVE_VALUE = val['parameterValue']
			if val['parameterKey'] == 'B2B_FARE_PRICE_COMPARISON':
				self.B2B_FARE_PRICE_COMPARISON = val['parameterValue']
			if val['parameterKey'] == 'COVERAGE_PARAMETER':
				self.COVERAGE_PARAMETER = val['parameterValue']
	
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
		if self.inputSource[Constants.CONST_INPUT_SOURCE][Constants.CONST_MARKET_LIST] == Constants.TEST_DATA:
			marketListQuery = "SELECT Origin,Destin,FlightNumber,PerType,PerStart,PerEnd,DOW,strategyFlag,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusion,fareAnchor,fareOffset,FirstRBDAlloc,OtherRBDAlloc,B2BBackstop,B2CBackstop,B2BFactor FROM market_list "
			self.mktList = pd.read_sql(marketListQuery, self.conn)
   
		if self.inputSource[Constants.CONST_INPUT_SOURCE][Constants.CONST_MARKET_LIST] == Constants.DB:
			marketListQuery = "SELECT Origin,Destin,FlightNumber,PerType,PerStart,PerEnd,DOW,strategyFlag,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusion,fareAnchor,fareOffset,FirstRBDAlloc,OtherRBDAlloc,B2BBackstop,B2CBackstop,B2BFactor FROM market_list "
			self.mktList = pd.read_sql(marketListQuery, self.conn)

		if self.inputSource[Constants.CONST_INPUT_SOURCE][Constants.CONST_MARKET_LIST] == Constants.FILE:
			self.mktList = pd.read_csv(Constants.CONST_MARKET_LIST_FILE_PATH)
   
		if(self.debugMode):
			print("Loaded Market List")
			print(self.mktList)

	def updateRunEnd(self, endTime):
		if self.inputSource[Constants.CONST_LOGS][Constants.CONST_RUN_AUDIT] == Constants.DB:
			updateQuery = " UPDATE allocation_run_audit SET end_time = %s WHERE run_id = %s "		
			self.wrconn.execute(updateQuery, (endTime.strftime(Constants.DATE_TIME_FORMAT), self.runId))

	def areAllInputsReady(self):
		if self.inputSource[Constants.CONST_INPUT_SOURCE][Constants.CONST_INPUT_STATUS] == Constants.DB:
			countQuery = "select count(*) as busyInputs from inputs_status WHERE is_running = b'1'"
			inputsData = pd.read_sql(countQuery, self.conn)
			return inputsData['busyInputs'][0] == 0
