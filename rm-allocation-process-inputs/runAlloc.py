import pandas as pd
import uuid
import math
import os
import time
import json
import Constants
import warnings
warnings.filterwarnings('ignore')

from datetime import date
from datetime import timedelta
from datetime import datetime
from dateutil import parser

class DynAlloc:

	def __init__(self, inputData, currParams, NavitaireConnection, dbconn, wrconn, sqs):  
		self.inputSource = inputData
		#Set this to true for more verbose scree display during the run
		
		self.debugMode = self.inputSource["debugMode"]
		self.runId = currParams["runId"]
		
		self.TABLE_MARKET_FARES = "market_fares"
		self.BookloadTableName = "bookload"
		self.WAIT_TIME = 30 # in seconds
		
		self.MARKET_LIST_QUEUE_URL = self.inputSource["MARKET_LIST_QUEUE_URL"]
		self.RBD_QUEUE_URL = self.inputSource["RBD_QUEUE_URL"]
		self.NAV_DB_NAME = self.inputSource["NAV_DB_NAME"]
		self.sqs = sqs
   
		#Establish connection to DataLake
		self.conn = dbconn
		self.wrconn = wrconn

		#Establish Connection to Navitaire
		self.NavitaireConnection = NavitaireConnection

		#Load the Run Parameters
		self.loadRunParams()

		#Place Holders for Output
		self.allocResults = pd.DataFrame(columns=['Id', 'RunId', 'Origin','Destin','FltNum','DepDate','BookedLoad','TgtLoad','MktFare','OpenRBD','SellingFare', 'Channel', 'HowDetermined'])
		self.AUFileData = []
		
		#Start the Allocation
		self.runDynamicAllocationSingleMarket(currParams)
		# self.runDynamicAllocation()

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
			self.runDynamicAllocationSingleMarket(mktCounter)

		self.outputResults()

		endTime = self.getCurrentDateTime()
		self.updateRunEnd(endTime)
		runTime = (endTime - startTime).total_seconds()
		print("Allocation Completed in " + str(runTime) + " seconds")

	def runDynamicAllocationSingleMarket(self, currParams):
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
			self.allocResults = self.allocResults.append({"Id":self.getUniqueId(), "RunId":self.runId, "Origin":origin, "Destin":destination, "FltNum":"NA","DepDate":str(startDate), "BookedLoad":"NA", "TgtLoad":str(tgtBookedLoad), "MktFare":"NA", "OpenRBD":"NA", "SellingFare":"NA", 'Channel':"NA", 'HowDetermined':"unable to get Booked loads from " + str(startDate) + " to " + str(endDate)}, ignore_index = True)
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
				#For each NDO
				for ndocounter in range(startNDO, endNDO+1):
					currDate = self.getCurrentDate()
					tgtDate = datetime.strftime(currDate + timedelta(days=ndocounter),"%Y-%m-%d")
					tgtBookedLoadList = self.getTargetBookedLoadFromBenchmarkCurve(ndocounter, curveId)
					tgtBookedLoad = float(tgtBookedLoadList['LF'][0])

					if(tgtBookedLoad >= 0):
						self.runDynamicAllocationSingleMarketSingleDay(ndocounter, bookedLoad, tgtBookedLoad, currParams, marketFaresExtractName, B2CBackstop, B2BBackstop)
					else:
						#Cannot get a reference curve - Abort
						print("Unable to get a Target - Check the booking curves")
						self.allocResults = self.allocResults.append({"Id":self.getUniqueId(), "RunId":self.runId, "Origin":origin,"Destin":destination,"FltNum":"NA","DepDate":str(tgtDate), "BookedLoad":"NA", "TgtLoad":str(tgtBookedLoad), "MktFare":"NA", "OpenRBD":"NA", "SellingFare":"NA", 'Channel':"NA", 'HowDetermined':"No Reference Curve Found"}, ignore_index = True)
						self.insertSummaryRow()
			except Exception as e:
				print("Exception occurred")
				print(e)

	def runDynamicAllocationSingleMarketSingleDay(self, ndocounter, bookedLoad, tgtBookedLoad, currParams, marketFaresExtractName, B2CBackstop, B2BBackstop):
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

			self.runDynamicAllocationSingleFlight(currFlt, currBookedLoad, currCapacity, currFltDate, tgtBookedLoad, dow, currParams, marketFaresExtractName, ndocounter, B2CBackstop, B2BBackstop)

	def runDynamicAllocationSingleFlight(self, currFlt, currBookedLoad, currCapacity, currFltDate, tgtBookedLoad, dow, currParams, marketFaresExtractName, ndocounter, B2CBackstop, B2BBackstop):
		
		tgtBookloadFactor = tgtBookedLoad	
		tgtBookedLoad = tgtBookedLoad * currCapacity
		calcDeficit = tgtBookedLoad - currBookedLoad
		calcBookStatus = "M"
		if (calcDeficit > 0):
			calcBookStatus = "B"
		if (calcDeficit <= 0):
			calcBookStatus = "A"

		if(self.debugMode):
			print("Ready to start Allocation")
			print(currParams)
			print("DOW is : " + str(dow))
			print("Extract is " + str(currParams['DOW'])[dow:(dow+1)])

		if ((str(currFlt) == str(currParams['FlightNumber'])) or (currParams['FlightNumber'] == '*')) and (calcBookStatus == currParams['bookedStatus']) and (str(currParams['DOW'])[dow:(dow+1)] == "1"):
			print("Actual Booked Load for " + str(currParams['FlightNumber']) + " on " + str(currFltDate) + " is :" + str(currBookedLoad))

			totalCapacity = currCapacity
			bookedDeficit = round(tgtBookedLoad - currBookedLoad)

			remainingCapacity = totalCapacity - currBookedLoad
   
			dPLF = (currBookedLoad/totalCapacity) - tgtBookloadFactor
			plfValue = dPLF * 100
			print("plfValue is " + str(plfValue))

			ndoBand = self.getNdoBand(ndocounter)
			dplfBand = self.getdpflBand(plfValue)
   
			# Get Dynamic D1 (Time) Value
			startTime, endTime = self.getDynamicTime(currParams, dplfBand, ndoBand)
   
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

			#Does competetive fare exist
			#We only proceed if a B2C fare exists - If B2C does not exist, we will skip the whole thing even with a B2B fare
			# This is because, when we prepare the AU Grid, if we donr push inventory on to B2C RBD, then the flight will not be visible 
			# on OTA - Flag the error for correction here

			if(float(mktFareRangeB2C['LowFare'][0]) > 0):
				lowFareB2C = mktFareRangeB2C['LowFare'][0]
				hiFareB2C = mktFareRangeB2C['HiFare'][0]

				lowFareB2B = mktFareRangeB2B['LowFare'][0]
				hiFareB2B = mktFareRangeB2B['HiFare'][0]

				print("Market Fare Range for " + str(currFltDate) + " in the B2C Channel is :" + str(lowFareB2C) + " to " + str(hiFareB2C))
				print("Market Fare Range for " + str(currFltDate) + " in the B2B Channel is :" + str(lowFareB2B) + " to " + str(hiFareB2B))

				openDataB2C = self.determineRBDToOpen(currParams['Origin'], currParams['Destin'], lowFareB2C,hiFareB2C, currParams['fareAnchor'],currParams['fareOffset'],'B2C', B2CBackstop)
				openDataB2B = self.determineRBDToOpen(currParams['Origin'], currParams['Destin'], lowFareB2B,hiFareB2B, currParams['fareAnchor'],currParams['fareOffset'],'B2B', B2BBackstop)

				self.allocResults = self.allocResults.append({"Id":self.getUniqueId(), "RunId":self.runId,"Origin":currParams['Origin'],"Destin":currParams['Destin'],"FltNum":str(currFlt),"DepDate":str(currFltDate), "BookedLoad":str(currBookedLoad), "TgtLoad":str(tgtBookedLoad), "MktFare":str(round(float(lowFareB2C))), "OpenRBD":str(openDataB2C['RBD'][0]), "SellingFare":str(openDataB2C["Fare"][0]), 'Channel':'B2C', 'HowDetermined':str(openDataB2C['FareResult'][0])}, ignore_index = True)
				self.insertSummaryRow()
				
				self.allocResults = self.allocResults.append({"Id":self.getUniqueId(), "RunId":self.runId,"Origin":currParams['Origin'],"Destin":currParams['Destin'],"FltNum":str(currFlt),"DepDate":str(currFltDate), "BookedLoad":str(currBookedLoad), "TgtLoad":str(tgtBookedLoad), "MktFare":str(round(float(lowFareB2B))), "OpenRBD":str(openDataB2B['RBD'][0]), "SellingFare":str(openDataB2B["Fare"][0]), 'Channel':'B2B', 'HowDetermined':str(openDataB2B['FareResult'][0])}, ignore_index = True)
				self.insertSummaryRow()
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


					#First - Fill in the Z B2B Levels
					#get all the Z RBDs
					self.B2BAllocation(currBookedLoad, currParams, openDataB2B, openDataB2C, currRec, totalCapacity, bookedDeficit, dPLF, remainingCapacity, ndocounter)
					
					#Now Add All RBD Levels
					self.B2CAllocation(currBookedLoad, currParams, openDataB2B, openDataB2C, currRec, totalCapacity, bookedDeficit, dPLF, remainingCapacity, ndocounter)
					#End the B2C counter

					#Ends statsusInd == 0
					self.AUFileData.append(currRec)
					if self.inputSource['isUpdateNavitaire']:
						self.updateNavitaire(currRec)
			else:
				print("unable to get a Market Fare - Skipping")

				self.allocResults = self.allocResults.append({"Id":self.getUniqueId(), "RunId":self.runId,"Origin":currParams['Origin'],"Destin":currParams['Destin'],"FltNum":str(currFlt),"DepDate":str(currFltDate), "BookedLoad":str(currBookedLoad), "TgtLoad":str(tgtBookedLoad), "MktFare":"NA", "OpenRBD":"NA", "SellingFare":"NA", 'Channel':'B2C', 'HowDetermined':"Market Fare B2C Not Available - Skipping entire allocation"}, ignore_index = True)
				self.insertSummaryRow()
				
				self.allocResults = self.allocResults.append({"Id":self.getUniqueId(), "RunId":self.runId,"Origin":currParams['Origin'],"Destin":currParams['Destin'],"FltNum":str(currFlt),"DepDate":str(currFltDate), "BookedLoad":str(currBookedLoad), "TgtLoad":str(tgtBookedLoad), "MktFare":"NA", "OpenRBD":"NA", "SellingFare":"NA", 'Channel':'B2B', 'HowDetermined':"Market Fare B2C Not Available - Skipping entire allocation"}, ignore_index = True)
				self.insertSummaryRow()
				
		else:
			print("Run Parameters requested to skip - (FlightNumber, DOW, or Status)")

	def B2BAllocation(self, currBookedLoad, currParams, openDataB2B, openDataB2C, currRec, totalCapacity, bookedDeficit, dPLF, remainingCapacity, ndocounter):
		  
		B2BRBDs = self.fareExtract(currParams['Origin'], currParams['Destin'], 'B2B', '')
		openingB2CRBD = openDataB2C['RBD'][0]
  
		B2CRBDs = self.fareExtract(currParams['Origin'], currParams['Destin'], 'B2C', openingB2CRBD)

		openingB2CPrice = Constants.B2B_FARE_PRICE_COMPARISION * B2CRBDs['Total'][0]
		incremental = -1
		openbuketFlag = False
		maxRBDIndex = B2BRBDs['id'].iloc[-1]
		maxRBDPrice = B2BRBDs['Total'].iloc[-1]
		openingRBD = openDataB2B['RBD'][0]
		
		openingRBDPirce = 0
		openRBDIndex = 0				
		size = B2BRBDs.shape[0] - 1
		jumpingRBDPrice = 0
		entryRDB = True
		d3Allocation = 0
		PLF = currBookedLoad / totalCapacity

		#Loop Thru
		
		for b2bctr in range(B2BRBDs.shape[0]):
			RBDLabel = B2BRBDs['RBD'][b2bctr]
			remainingRBD = maxRBDIndex - openRBDIndex
			firstRDBAllocation = int(currParams['FirstRBDAlloc'])
			otherRDBAllocation = int(currParams['OtherRBDAlloc'])
			price = B2BRBDs['Total'][b2bctr]
			nextPrice = price
			b2bStatus = openDataB2B['statusInd'][0]
			
			if(size > b2bctr):
				nextPrice = B2BRBDs['Total'][b2bctr + 1]
    
			currRec.append(RBDLabel)
			currRec.append('')
			currRec.append(-1)
			currRec.append(-1)
			if RBDLabel == openingRBD:
				openbuketFlag = True
				openingRBDPirce = price
				
			if (openbuketFlag):
				#Allocation
				if((b2bStatus == 0) or (b2bStatus == 2)):
					if(dPLF < 0 and (PLF < Constants.PLF_CURVE_VALUE or openingB2CPrice <= openingRBDPirce)):
         
						#Add -1 to continue with previous value
						if(firstRDBAllocation >= 0):	
							incremental = self.B2BincrementalAllocation(currBookedLoad, bookedDeficit, incremental, firstRDBAllocation, otherRDBAllocation)
		
						elif(firstRDBAllocation == Constants.DYNAMIC_D3_D4):
							d3Allocation, incremental, jumpingRBDPrice, openRBDIndex, entryRDB = self.B2BnegativeValueJumpingAllocation(remainingRBD, currBookedLoad, bookedDeficit, dPLF, remainingCapacity, ndocounter, maxRBDPrice, openingRBDPirce, openRBDIndex, incremental, entryRDB, otherRDBAllocation, price, nextPrice, d3Allocation, jumpingRBDPrice, RBDLabel)
						
						#Negative -1 Allocation	
						elif(firstRDBAllocation == Constants.DEFICIT_ALLOCATION):
							bookedDef = int(round(bookedDeficit,0))
							if(bookedDef <= 0):
								bookedDef = 0
							incremental = self.B2BincrementalAllocation(currBookedLoad, bookedDeficit, incremental, bookedDef, otherRDBAllocation)
		
						#Negative -2 Allocation
						elif(firstRDBAllocation == Constants.LINEAR_ALLOCATION):
							allocation = int(round(remainingCapacity/otherRDBAllocation,0))
							incremental = self.B2BincrementalAllocation(currBookedLoad, bookedDeficit, incremental, allocation, allocation)
		
						#Negative -3 Allocation
						elif(firstRDBAllocation == Constants.LINEAR_ALLOCATION_WITH_JUMP):
							allocation = int(round(remainingCapacity/otherRDBAllocation,0))
							jumpPercent = Constants.LINEAR_JUMP_VALUE
							openRBDIndex, incremental, jumpingRBDPrice = self.B2BjumpingAllocation(currBookedLoad, bookedDeficit, openRBDIndex, incremental, jumpPercent, price, nextPrice, allocation, allocation, jumpingRBDPrice, RBDLabel)

						if (incremental > totalCapacity):
							incremental = totalCapacity
						currRec.append(int(round(incremental,0)))
      
					else:
						currRec.append(0)
				
				else:
					#No B2B Allocaion - Leave it to B2C Fare
					currRec.append(0)
			else:
				if openingRBD == 'NA':
					# This means B2B market fare was missing. Add -1 to continue with previous value
					currRec.append(-1)
				else:
					currRec.append(0)

			currRec.append(-1)
			currRec.append(-1)

	def B2CAllocation(self, currBookedLoad, currParams, openDataB2B, openDataB2C, currRec, totalCapacity, bookedDeficit, dPLF, remainingCapacity, ndocounter):
		
		B2CRBDs = self.fareExtract(currParams['Origin'], currParams['Destin'], 'B2C', '')
  
		maxRBDIndex = B2CRBDs['id'].iloc[-1]
		maxRBDPrice = B2CRBDs['Total'].iloc[-1]
		openingRBD = openDataB2C['RBD'][0]
		openbuketFlag = False
		openingRBDPirce = 0
		openRBDIndex = 0				
		incremental = -1
		size = B2CRBDs.shape[0] - 1
		jumpingRBDPrice = 0
		entryRDB = True
		d3Allocation = 0
		#reset
		#Loop Thru
		for b2cctr in range(B2CRBDs.shape[0]):
			RBDLabel = B2CRBDs['RBD'][b2cctr]
			remainingRBD = maxRBDIndex - openRBDIndex
			firstRDBAllocation = int(currParams['FirstRBDAlloc'])
			otherRDBAllocation = int(currParams['OtherRBDAlloc'])
			price = B2CRBDs['Total'][b2cctr]
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
					incremental = self.incrementalAllocation(currBookedLoad, bookedDeficit, incremental, firstRDBAllocation, otherRDBAllocation)

				#Negative -99 Allocation
				elif(firstRDBAllocation == Constants.DYNAMIC_D3_D4):
					d3Allocation, incremental, jumpingRBDPrice, openRBDIndex, entryRDB = self.negativeValueJumpingAllocation(remainingRBD, currBookedLoad, bookedDeficit, dPLF, remainingCapacity, ndocounter, maxRBDPrice, openingRBDPirce, openRBDIndex, incremental, entryRDB, otherRDBAllocation, price, nextPrice, d3Allocation, jumpingRBDPrice)

				#Negative -1 Allocation	
				elif(firstRDBAllocation == Constants.DEFICIT_ALLOCATION):
					bookedDef = int(round(bookedDeficit,0))
					if(bookedDef <= 0):
						bookedDef = 0
					incremental = self.incrementalAllocation(currBookedLoad, bookedDeficit, incremental, bookedDef, otherRDBAllocation)

				#Negative -2 Allocation
				elif(firstRDBAllocation == Constants.LINEAR_ALLOCATION):
					allocation = int(round(remainingCapacity/otherRDBAllocation,0))
					incremental = self.incrementalAllocation(currBookedLoad, bookedDeficit, incremental, allocation, allocation)

				#Negative -3 Allocation
				elif(firstRDBAllocation == Constants.LINEAR_ALLOCATION_WITH_JUMP):
					allocation = int(round(remainingCapacity/otherRDBAllocation,0))
					jumpPercent = Constants.LINEAR_JUMP_VALUE
					openRBDIndex, incremental, jumpingRBDPrice = self.jumpingAllocation(currBookedLoad, bookedDeficit, openRBDIndex, incremental, jumpPercent, price, nextPrice, allocation, allocation, jumpingRBDPrice)
					
				if (incremental >= totalCapacity):
					incremental = totalCapacity
				if (RBDLabel == 'Y'):
					incremental = totalCapacity
				currRec.append(incremental)
				
			else:
				currRec.append(0)
									
			currRec.append(-1)
			currRec.append(-1)

	def incrementalAllocation(self, currBookedLoad, bookedDeficit, incremental, firstRDBAllocation, otherRDBAllocation):
		if(incremental < 0):
			# First Alloctation
			if(firstRDBAllocation >= 0):
				#allocate the FirstRBD 
				incremental = currBookedLoad + firstRDBAllocation
			else:
				#negtive number means allocate the full deficit - Only if the deficit is positive - Lagging
				if(bookedDeficit > 0):
					incremental = currBookedLoad + bookedDeficit
				else:
					incremental = currBookedLoad + otherRDBAllocation
		else:
			incremental = incremental + otherRDBAllocation
		return incremental

	def B2BincrementalAllocation(self, currBookedLoad, bookedDeficit, incremental, firstRDBAllocation, otherRDBAllocation):
		if(incremental < 0):
			# First Alloctation
			if(firstRDBAllocation >= 0):
				#allocate the FirstRBD 
				incremental = currBookedLoad + firstRDBAllocation
			else:
				#negtive number means allocate the full deficit - Only if the deficit is positive - Lagging
				if(bookedDeficit > 0):
					incremental = currBookedLoad + bookedDeficit
				else:
					incremental = currBookedLoad + otherRDBAllocation
		else:
			incremental = incremental + otherRDBAllocation
		return round(incremental,0)

	def jumpingAllocation(self, currBookedLoad, bookedDeficit, openRBDIndex, incremental, otherRDBAllocation, price, nextPrice, d3Allocation, d4Allocation, jumpingRBDPrice):
		if(incremental < 0):
			if(d3Allocation >= 0):
				#allocate the FirstRBD 
				incremental = currBookedLoad + d3Allocation
			else:
				#negtive number means allocate the full deficit - Only if the deficit is positive - Lagging
				if(bookedDeficit > 0):
					incremental = currBookedLoad + bookedDeficit
				else:
					incremental = currBookedLoad + d4Allocation
			jumpingRBDPrice = price + (price * (otherRDBAllocation / 100))
		else:
			nextPriceDiff = abs(nextPrice - jumpingRBDPrice)
			currPriceDiff = abs(price - jumpingRBDPrice)
			if(jumpingRBDPrice >= nextPrice):
				pass
			elif(currPriceDiff <= nextPriceDiff):
				incremental = incremental + d4Allocation
				jumpingRBDPrice = price + (price * (otherRDBAllocation / 100))
				openRBDIndex = openRBDIndex + 1
			elif(currPriceDiff > nextPriceDiff):
				pass
		return openRBDIndex, incremental, jumpingRBDPrice

	def B2BjumpingAllocation(self, currBookedLoad, bookedDeficit, openRBDIndex, incremental, otherRDBAllocation, price, nextPrice, d3Allocation, d4Allocation, jumpingRBDPrice, RBDLabel):
		if(incremental < 0):
			if(d3Allocation >= 0):
				#allocate the FirstRBD 
				incremental = currBookedLoad + d3Allocation
			else:
				#negtive number means allocate the full deficit - Only if the deficit is positive - Lagging
				if(bookedDeficit > 0):
					incremental = currBookedLoad + bookedDeficit
				else:
					incremental = currBookedLoad + d4Allocation
			jumpingRBDPrice = price + (price * (otherRDBAllocation / 100))
		else:
			nextPriceDiff = abs(nextPrice - jumpingRBDPrice)
			currPriceDiff = abs(price - jumpingRBDPrice)
			if(jumpingRBDPrice >= nextPrice):
				pass
			elif(currPriceDiff <= nextPriceDiff):
				incremental = incremental + d4Allocation
				jumpingRBDPrice = price + (price * (otherRDBAllocation / 100))
				openRBDIndex = openRBDIndex + 1
			elif(currPriceDiff > nextPriceDiff):
				pass
			if(RBDLabel == 'Z0'):
				incremental = incremental + d4Allocation
		return openRBDIndex, round(incremental,0), jumpingRBDPrice

	def negativeValueJumpingAllocation(self, remainingRBD, currBookedLoad, bookedDeficit, dPLF, remainingCapacity, ndocounter, maxRBDPrice, openingRBDPirce, openRBDIndex, incremental, entryRDB, otherRDBAllocation, price, nextPrice, d3Allocation, jumpingRBDPrice):
		openRBDIndex, entryRDB, d3Allocation, d4Allocation, remainingRBD = self.d3d4Calculations(dPLF, remainingCapacity, ndocounter, maxRBDPrice, openingRBDPirce, openRBDIndex, entryRDB, otherRDBAllocation, remainingRBD, d3Allocation)
		
		if(otherRDBAllocation == 0):
			incremental = self.incrementalAllocation(currBookedLoad, bookedDeficit, incremental, d3Allocation, d4Allocation)
			openRBDIndex = openRBDIndex + 1
		else:
			openRBDIndex, incremental, jumpingRBDPrice = self.jumpingAllocation(currBookedLoad, bookedDeficit, openRBDIndex, incremental, otherRDBAllocation, price, nextPrice, d3Allocation, d4Allocation, jumpingRBDPrice)
		return d3Allocation, incremental, jumpingRBDPrice, openRBDIndex, entryRDB

	def B2BnegativeValueJumpingAllocation(self, remainingRBD, currBookedLoad, bookedDeficit, dPLF, remainingCapacity, ndocounter, maxRBDPrice, openingRBDPirce, openRBDIndex, incremental, entryRDB, otherRDBAllocation, price, nextPrice, d3Allocation, jumpingRBDPrice, RBDLabel):
		openRBDIndex, entryRDB, d3Allocation, d4Allocation, remainingRBD = self.d3d4Calculations(dPLF, remainingCapacity, ndocounter, maxRBDPrice, openingRBDPirce, openRBDIndex, entryRDB, otherRDBAllocation, remainingRBD, d3Allocation)
		
		if(otherRDBAllocation == 0):
			incremental = self.B2BincrementalAllocation(currBookedLoad, bookedDeficit, incremental, d3Allocation, d4Allocation)
			openRBDIndex = openRBDIndex + 1
		else:
			openRBDIndex, incremental, jumpingRBDPrice = self.B2BjumpingAllocation(currBookedLoad, bookedDeficit, openRBDIndex, incremental, otherRDBAllocation, price, nextPrice, d3Allocation, d4Allocation, jumpingRBDPrice, RBDLabel)
		return d3Allocation, incremental, jumpingRBDPrice, openRBDIndex, entryRDB

	def d3d4Calculations(self, dPLF, remainingCapacity, ndocounter, maxRBDPrice, openingRBDPirce, openRBDIndex, entryRDB, otherRDBAllocation, remainingRBD, d3Allocation):
		maxLimit = self.generateMaxValue(remainingCapacity, ndocounter)
		if(entryRDB):
			if(otherRDBAllocation > 0 and openingRBDPirce - maxRBDPrice != 0):
				remainingRBD = math.log(maxRBDPrice/openingRBDPirce, (1+otherRDBAllocation/100))

			d3Allocation = self.dynamicAllocationD3Formula(dPLF, remainingCapacity, remainingRBD, ndocounter)
			if(d3Allocation <= Constants.MIN_D3_D4_VALUE):
				d3Allocation = Constants.MIN_D3_D4_VALUE
			elif(d3Allocation >= maxLimit):
				d3Allocation = maxLimit
			entryRDB = False
			openRBDIndex = openRBDIndex + 1

		d4Allocation = self.dynamicAllocationD4Formula(dPLF, openRBDIndex, d3Allocation)

		if(d4Allocation <= Constants.MIN_D3_D4_VALUE):
			d4Allocation = Constants.MIN_D3_D4_VALUE
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

	def determineRBDToOpen(self, origin, destin, lowFare, hiFare, strategy, offset, channel, backstop):

		openResult = pd.DataFrame(columns=['RBD', 'Fare', 'FareResult','statusInd'])

		anchorFare = lowFare
		if (strategy == 'MAX'):
			anchorFare = hiFare
		
		print("Strategy applied for " + channel + " Channel is " + strategy )
		print("Anchor Fare is " + str(anchorFare) + " with an offset of " + str(offset))


		RBDQuery = "select RBD, Total from Fares where Org = '" + origin + "' and Dst = '" + destin + "' "
		
		if channel == 'B2C':
			RBDQuery = RBDQuery + " and LEFT(RBD,1) != 'Z'"
		if channel == 'B2B':
			RBDQuery = RBDQuery + " and LEFT(RBD,1) = 'Z'"

		sortorder = ' desc '

		if (offset < 0):
			RBDQuery = RBDQuery + " and Total < " + str(anchorFare)
			sortorder = ' desc '
		if (offset >= 0):
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
				openResult = openResult.append({'RBD':defFareData['RBD'][0], 'Fare':defFareData['Total'][0], 'FareResult':'Unable to Match Mkt - Revisit Fare Ladder','statusInd':2}, ignore_index = True)
			if (channel == 'B2B'):
				openResult = openResult.append({'RBD':'NA', 'Fare':defFareData['Total'][0], 'FareResult':'Unable to Match Mkt - B2B Allocation Skipped','statusInd':1}, ignore_index = True)
		else:
			# We got back data purely on the basis of anchor fare - comparison with offset remains which is done in the subsequent section

			# Calculating the offset that is required based on entries remaining in the RBD table
			offsetToFetch = min(abs(offset), rbddata.shape[0] - 1) 
			if(offset < 0):
				offsetToFetch = min(abs(offset+1), rbddata.shape[0] - 1) 

			# Comparison with the backstop table - to ensure we are picking a value higher than the backstop
			defFareData = self.fetchExtremePublishedFare(origin, destin, channel, 'LOW', backstop)

			# Edge case if the backstop values are placed very high - in which case it will place the backstop at Z0 and Y levels 
			if(defFareData.shape[0] == 0):
				defFareData = self.fetchExtremePublishedFare(origin, destin, channel, 'HI', 0)

			if(float(defFareData['Total'][0]) >= float(rbddata['Total'][offsetToFetch])):
				RBDtoOpen = defFareData['RBD'][0]
				FareToOpen = defFareData['Total'][0]
			else:			
				RBDtoOpen = rbddata['RBD'][offsetToFetch]
				FareToOpen = rbddata['Total'][offsetToFetch]

			print('Determining ' + channel + ' Fare ' + str(FareToOpen) + " " + str(RBDtoOpen) )

			if (channel == 'B2B'):
				#Special handling for B2B - if the B2B Anchor fare is higher than Z0 - Skip B2B
				limitB2BFare_Data = self.fetchExtremePublishedFare(origin, destin, channel, 'HI', 0)
				limitB2BFare = float(limitB2BFare_Data['Total'][0])

				if (float(anchorFare) > limitB2BFare):
					#Outside of B2B Levels - stick with B2C
					openResult = openResult.append({'RBD':'NA', 'Fare':'NA', 'FareResult':'Market Fare higher than Z0 levels - skipping B2B allocation','statusInd':1}, ignore_index = True)
				else:
					if(offsetToFetch == rbddata.shape[0]):
						openResult = openResult.append({'RBD':RBDtoOpen, 'Fare':str(FareToOpen), 'FareResult':'Not Enough Levels available - taking the Max/Min Available','statusInd':2}, ignore_index = True)
					else:
						openResult = openResult.append({'RBD':RBDtoOpen, 'Fare':str(FareToOpen), 'FareResult':'Matched and allocated','statusInd':0}, ignore_index = True)
			else:
				if(offsetToFetch == rbddata.shape[0]):
					openResult = openResult.append({'RBD':RBDtoOpen, 'Fare':str(FareToOpen), 'FareResult':'Not Enough Levels available - taking the Max/Min Available','statusInd':2}, ignore_index = True)
				else:
					openResult = openResult.append({'RBD':RBDtoOpen, 'Fare':str(FareToOpen), 'FareResult':'Matched and allocated','statusInd':0}, ignore_index = True)

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
		mktLowestQuery = mktLowestQuery + " and outbound_airline_code <> '" + currParams['CarrExlusion'] + "' AND outbound_airline_code != 'QP' AND outbound_airline_code!='G8' AND created_at > NOW() - INTERVAL 2 DAY "
		mktFareRange = pd.read_sql(mktLowestQuery, self.conn)

		return mktFareRange

	def getTargetBookedLoadFromBenchmarkCurve(self, ndocounter, curveID):
		tgtBookedLoad = []
  		#Fetch the Target Booked Load from the curve

		bookingCurveQuery = "Select LF from Curves where NDO = " + str(ndocounter) + " and CurveID = '" + curveID + "'"
		benchmark = pd.read_sql(bookingCurveQuery, self.conn)
		if(benchmark.shape[0] > 0):
			tgtBookedLoad = benchmark

		return tgtBookedLoad

	def createTableExtract(self, org, dest, perstart, perend, starttime, endtime):
		name = str(self.getUniqueId())
		name = name.replace("-", "_")
		tableName = "ext_" + name

		dropQuery = "DROP TABLE IF EXISTS " + tableName
		createQuery = "CREATE TABLE " + tableName + " LIKE " + self.TABLE_MARKET_FARES
		insertQuery = "INSERT INTO " + tableName + " SELECT * FROM " + self.TABLE_MARKET_FARES + \
			" WHERE source_airport = '" + org + "' AND destination_airport = '" + dest + "' AND departure_date >= '" + \
				perstart.strftime("%Y-%m-%d") + "' AND departure_date <= '" + perend.strftime("%Y-%m-%d") + \
					"' AND outbound_departuretime >= '" + parser.parse(starttime).strftime("%H:%M") + \
						"' AND outbound_departuretime <= '" + parser.parse(endtime).strftime("%H:%M") + \
							"' AND outbound_airline_code != 'QP' AND outbound_airline_code!='G8' AND created_at > NOW() - INTERVAL 2 DAY"
		# print(insertQuery)
		self.wrconn.execute(dropQuery)
		self.wrconn.execute(createQuery)
		self.wrconn.execute(insertQuery)
		return tableName

	def deleteTableExtract(self, tableName):
		dropQuery = "DROP TABLE IF EXISTS " + tableName
		self.conn.execute(dropQuery)

	def insertSummaryRow(self):
		if self.inputSource[Constants.CONST_LOGS][Constants.CONST_RUN_SUMMARY] == Constants.DB:
			latest_row = self.allocResults.iloc[-1]
			insertQuery = " INSERT INTO run_summary ( Id, Origin, Destin, FltNum, DepDate, BookedLoad, TgtLoad, MktFare, OpenRBD, SellingFare, Channel, HowDetermined, RunId, CreatedAt) "		
			insertQuery = insertQuery + " VALUES ('"+ latest_row['Id'] + "', '"+latest_row['Origin'] + "', '"+latest_row['Destin'] + "', '"+ latest_row['FltNum'] + "', '"+latest_row['DepDate'] + "', '"+latest_row['BookedLoad'] + "', '"+latest_row['TgtLoad'] + "', '"+latest_row['MktFare'] + "', '"+latest_row['OpenRBD'] + "', '"+str(latest_row['SellingFare']) + "', '"+latest_row['Channel'] + "', '"+latest_row['HowDetermined'] + "', '"+latest_row['RunId'] + "', '" + self.getCurrentDateTime().strftime(Constants.DATE_TIME_FORMAT) +"')"
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
			NavitaireQuery = NavitaireQuery + " where  IL.Lid > 0 and IL.Status NOT IN (2) and IL.DepartureDate >= '" + str(startDate) + "' and IL.DepartureDate <= '" +str(endDate) + "' and (PJS.FareBasis != 'SO7NBSOL' or PJS.FareBasis IS NULL)"
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

	def getDynamicTime(self, currParams, dplfBand, ndoBand):
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
   
		return str(startTime), str(endTime)

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
		dynamicD2List = self.getd1d2StrategyValue(currParams['Strategy'], dplfBand, ndoBand)
		criteria = dynamicD2List['criteria'][0]
		offset = dynamicD2List['offset'][0]
		return criteria, offset

	def getd1d2StrategyValue(self, strategy, dplfBand, ndoBand):
		dynamicD1D2Query = "SELECT criteria, time_range, offset FROM d1_d2_strategies where ndo_band = "+ str(ndoBand) +" and dplf_band = "+ str(dplfBand) +" and strategy = '"+ strategy +"'"
		dynamicList = pd.read_sql(dynamicD1D2Query, self.conn)
		return dynamicList

	def outputResults(self):
		
		AUFrame = pd.DataFrame(self.AUFileData, columns=['Type','Flight','StartDate','EndDate','DayOfWeek','Aircraft Type','Aircraft Suffix','Lid','Capacity','Status',
			'Class Of Service AU Class 1','Class Type AU Class 1','Class Nest AU Class 1','Class Rank AU Class 1','Class AU AU Class 1','Class Allotted AU Class 1','AP Restriction AU Class 1',
			'Class Of Service AU Class 2','Class Type AU Class 2','Class Nest AU Class 2','Class Rank AU Class 2','Class AU AU Class 2','Class Allotted AU Class 2','AP Restriction AU Class 2',
			'Class Of Service AU Class 3','Class Type AU Class 3','Class Nest AU Class 3','Class Rank AU Class 3','Class AU AU Class 3','Class Allotted AU Class 3','AP Restriction AU Class 3',
			'Class Of Service AU Class 4','Class Type AU Class 4','Class Nest AU Class 4','Class Rank AU Class 4','Class AU AU Class 4','Class Allotted AU Class 4','AP Restriction AU Class 4',
			'Class Of Service AU Class 5','Class Type AU Class 5','Class Nest AU Class 5','Class Rank AU Class 5','Class AU AU Class 5','Class Allotted AU Class 5','AP Restriction AU Class 5',
			'Class Of Service AU Class 6','Class Type AU Class 6','Class Nest AU Class 6','Class Rank AU Class 6','Class AU AU Class 6','Class Allotted AU Class 6','AP Restriction AU Class 6',
			'Class Of Service AU Class 7','Class Type AU Class 7','Class Nest AU Class 7','Class Rank AU Class 7','Class AU AU Class 7','Class Allotted AU Class 7','AP Restriction AU Class 7',
			'Class Of Service AU Class 8','Class Type AU Class 8','Class Nest AU Class 8','Class Rank AU Class 8','Class AU AU Class 8','Class Allotted AU Class 8','AP Restriction AU Class 8',
			'Class Of Service AU Class 9','Class Type AU Class 9','Class Nest AU Class 9','Class Rank AU Class 9','Class AU AU Class 9','Class Allotted AU Class 9','AP Restriction AU Class 9',
			'Class Of Service AU Class 10','Class Type AU Class 10','Class Nest AU Class 10','Class Rank AU Class 10','Class AU AU Class 10','Class Allotted AU Class 10','AP Restriction AU Class 10',
			'Class Of Service AU Class 11','Class Type AU Class 11','Class Nest AU Class 11','Class Rank AU Class 11','Class AU AU Class 11','Class Allotted AU Class 11','AP Restriction AU Class 11',
			'Class Of Service AU Class 12','Class Type AU Class 12','Class Nest AU Class 12','Class Rank AU Class 12','Class AU AU Class 12','Class Allotted AU Class 12','AP Restriction AU Class 12',
			'Class Of Service AU Class 13','Class Type AU Class 13','Class Nest AU Class 13','Class Rank AU Class 13','Class AU AU Class 13','Class Allotted AU Class 13','AP Restriction AU Class 13',
			'Class Of Service AU Class 14','Class Type AU Class 14','Class Nest AU Class 14','Class Rank AU Class 14','Class AU AU Class 14','Class Allotted AU Class 14','AP Restriction AU Class 14',
			'Class Of Service AU Class 15','Class Type AU Class 15','Class Nest AU Class 15','Class Rank AU Class 15','Class AU AU Class 15','Class Allotted AU Class 15','AP Restriction AU Class 15',
			'Class Of Service AU Class 16','Class Type AU Class 16','Class Nest AU Class 16','Class Rank AU Class 16','Class AU AU Class 16','Class Allotted AU Class 16','AP Restriction AU Class 16',
			'Class Of Service AU Class 17','Class Type AU Class 17','Class Nest AU Class 17','Class Rank AU Class 17','Class AU AU Class 17','Class Allotted AU Class 17','AP Restriction AU Class 17',
			'Class Of Service AU Class 18','Class Type AU Class 18','Class Nest AU Class 18','Class Rank AU Class 18','Class AU AU Class 18','Class Allotted AU Class 18','AP Restriction AU Class 18',
			'Class Of Service AU Class 19','Class Type AU Class 19','Class Nest AU Class 19','Class Rank AU Class 19','Class AU AU Class 19','Class Allotted AU Class 19','AP Restriction AU Class 19',
			'Class Of Service AU Class 20','Class Type AU Class 20','Class Nest AU Class 20','Class Rank AU Class 20','Class AU AU Class 20','Class Allotted AU Class 20','AP Restriction AU Class 20',
			'Class Of Service AU Class 21','Class Type AU Class 21','Class Nest AU Class 21','Class Rank AU Class 21','Class AU AU Class 21','Class Allotted AU Class 21','AP Restriction AU Class 21',
			'Class Of Service AU Class 22','Class Type AU Class 22','Class Nest AU Class 22','Class Rank AU Class 22','Class AU AU Class 22','Class Allotted AU Class 22','AP Restriction AU Class 22',
			'Class Of Service AU Class 23','Class Type AU Class 23','Class Nest AU Class 23','Class Rank AU Class 23','Class AU AU Class 23','Class Allotted AU Class 23','AP Restriction AU Class 23',
			'Class Of Service AU Class 24','Class Type AU Class 24','Class Nest AU Class 24','Class Rank AU Class 24','Class AU AU Class 24','Class Allotted AU Class 24','AP Restriction AU Class 24',
			'Class Of Service AU Class 25','Class Type AU Class 25','Class Nest AU Class 25','Class Rank AU Class 25','Class AU AU Class 25','Class Allotted AU Class 25','AP Restriction AU Class 25',
			'Class Of Service AU Class 26','Class Type AU Class 26','Class Nest AU Class 26','Class Rank AU Class 26','Class AU AU Class 26','Class Allotted AU Class 26','AP Restriction AU Class 26',
			'Class Of Service AU Class 27','Class Type AU Class 27','Class Nest AU Class 27','Class Rank AU Class 27','Class AU AU Class 27','Class Allotted AU Class 27','AP Restriction AU Class 27',
			'Class Of Service AU Class 28','Class Type AU Class 28','Class Nest AU Class 28','Class Rank AU Class 28','Class AU AU Class 28','Class Allotted AU Class 28','AP Restriction AU Class 28',
			'Class Of Service AU Class 29','Class Type AU Class 29','Class Nest AU Class 29','Class Rank AU Class 29','Class AU AU Class 29','Class Allotted AU Class 29','AP Restriction AU Class 29',
			'Class Of Service AU Class 30','Class Type AU Class 30','Class Nest AU Class 30','Class Rank AU Class 30','Class AU AU Class 30','Class Allotted AU Class 30','AP Restriction AU Class 30',
			'Class Of Service AU Class 31','Class Type AU Class 31','Class Nest AU Class 31','Class Rank AU Class 31','Class AU AU Class 31','Class Allotted AU Class 31','AP Restriction AU Class 31',
			'Class Of Service AU Class 32','Class Type AU Class 32','Class Nest AU Class 32','Class Rank AU Class 32','Class AU AU Class 32','Class Allotted AU Class 32','AP Restriction AU Class 32',
			'Class Of Service AU Class 33','Class Type AU Class 33','Class Nest AU Class 33','Class Rank AU Class 33','Class AU AU Class 33','Class Allotted AU Class 33','AP Restriction AU Class 33',
			'Class Of Service AU Class 34','Class Type AU Class 34','Class Nest AU Class 34','Class Rank AU Class 34','Class AU AU Class 34','Class Allotted AU Class 34','AP Restriction AU Class 34',
			'Class Of Service AU Class 35','Class Type AU Class 35','Class Nest AU Class 35','Class Rank AU Class 35','Class AU AU Class 35','Class Allotted AU Class 35','AP Restriction AU Class 35',
			'Class Of Service AU Class 36','Class Type AU Class 36','Class Nest AU Class 36','Class Rank AU Class 36','Class AU AU Class 36','Class Allotted AU Class 36','AP Restriction AU Class 36',
			'Class Of Service AU Class 37','Class Type AU Class 37','Class Nest AU Class 37','Class Rank AU Class 37','Class AU AU Class 37','Class Allotted AU Class 37','AP Restriction AU Class 37',
			'Class Of Service AU Class 38','Class Type AU Class 38','Class Nest AU Class 38','Class Rank AU Class 38','Class AU AU Class 38','Class Allotted AU Class 38','AP Restriction AU Class 38',
			'Class Of Service AU Class 39','Class Type AU Class 39','Class Nest AU Class 39','Class Rank AU Class 39','Class AU AU Class 39','Class Allotted AU Class 39','AP Restriction AU Class 39',
			'Class Of Service AU Class 40','Class Type AU Class 40','Class Nest AU Class 40','Class Rank AU Class 40','Class AU AU Class 40','Class Allotted AU Class 40','AP Restriction AU Class 40',
			'Class Of Service AU Class 41','Class Type AU Class 41','Class Nest AU Class 41','Class Rank AU Class 41','Class AU AU Class 41','Class Allotted AU Class 41','AP Restriction AU Class 41',
			'Class Of Service AU Class 42','Class Type AU Class 42','Class Nest AU Class 42','Class Rank AU Class 42','Class AU AU Class 42','Class Allotted AU Class 42','AP Restriction AU Class 42',
			'Class Of Service AU Class 43','Class Type AU Class 43','Class Nest AU Class 43','Class Rank AU Class 43','Class AU AU Class 43','Class Allotted AU Class 43','AP Restriction AU Class 43',
			'Class Of Service AU Class 44','Class Type AU Class 44','Class Nest AU Class 44','Class Rank AU Class 44','Class AU AU Class 44','Class Allotted AU Class 44','AP Restriction AU Class 44',
			'Class Of Service AU Class 45','Class Type AU Class 45','Class Nest AU Class 45','Class Rank AU Class 45','Class AU AU Class 45','Class Allotted AU Class 45','AP Restriction AU Class 45',
			'Class Of Service AU Class 46','Class Type AU Class 46','Class Nest AU Class 46','Class Rank AU Class 46','Class AU AU Class 46','Class Allotted AU Class 46','AP Restriction AU Class 46',
			'Class Of Service AU Class 47','Class Type AU Class 47','Class Nest AU Class 47','Class Rank AU Class 47','Class AU AU Class 47','Class Allotted AU Class 47','AP Restriction AU Class 47',
			'Class Of Service AU Class 48','Class Type AU Class 48','Class Nest AU Class 48','Class Rank AU Class 48','Class AU AU Class 48','Class Allotted AU Class 48','AP Restriction AU Class 48',
			'Class Of Service AU Class 49','Class Type AU Class 49','Class Nest AU Class 49','Class Rank AU Class 49','Class AU AU Class 49','Class Allotted AU Class 49','AP Restriction AU Class 49',
			'Class Of Service AU Class 50','Class Type AU Class 50','Class Nest AU Class 50','Class Rank AU Class 50','Class AU AU Class 50','Class Allotted AU Class 50','AP Restriction AU Class 50',
			'Class Of Service AU Class 51','Class Type AU Class 51','Class Nest AU Class 51','Class Rank AU Class 51','Class AU AU Class 51','Class Allotted AU Class 51','AP Restriction AU Class 51',
			'Class Of Service AU Class 52','Class Type AU Class 52','Class Nest AU Class 52','Class Rank AU Class 52','Class AU AU Class 52','Class Allotted AU Class 52','AP Restriction AU Class 52',
			'Class Of Service AU Class 53','Class Type AU Class 53','Class Nest AU Class 53','Class Rank AU Class 53','Class AU AU Class 53','Class Allotted AU Class 53','AP Restriction AU Class 53',
			'Class Of Service AU Class 54','Class Type AU Class 54','Class Nest AU Class 54','Class Rank AU Class 54','Class AU AU Class 54','Class Allotted AU Class 54','AP Restriction AU Class 54',
			'Class Of Service AU Class 55','Class Type AU Class 55','Class Nest AU Class 55','Class Rank AU Class 55','Class AU AU Class 55','Class Allotted AU Class 55','AP Restriction AU Class 55',
			'Class Of Service AU Class 56','Class Type AU Class 56','Class Nest AU Class 56','Class Rank AU Class 56','Class AU AU Class 56','Class Allotted AU Class 56','AP Restriction AU Class 56',
			'Class Of Service AU Class 57','Class Type AU Class 57','Class Nest AU Class 57','Class Rank AU Class 57','Class AU AU Class 57','Class Allotted AU Class 57','AP Restriction AU Class 57',
			'Class Of Service AU Class 58','Class Type AU Class 58','Class Nest AU Class 58','Class Rank AU Class 58','Class AU AU Class 58','Class Allotted AU Class 58','AP Restriction AU Class 58',
			'Class Of Service AU Class 59','Class Type AU Class 59','Class Nest AU Class 59','Class Rank AU Class 59','Class AU AU Class 59','Class Allotted AU Class 59','AP Restriction AU Class 59',
			'Class Of Service AU Class 60','Class Type AU Class 60','Class Nest AU Class 60','Class Rank AU Class 60','Class AU AU Class 60','Class Allotted AU Class 60','AP Restriction AU Class 60',
			'Class Of Service AU Class 61','Class Type AU Class 61','Class Nest AU Class 61','Class Rank AU Class 61','Class AU AU Class 61','Class Allotted AU Class 61','AP Restriction AU Class 61',
			'Class Of Service AU Class 62','Class Type AU Class 62','Class Nest AU Class 62','Class Rank AU Class 62','Class AU AU Class 62','Class Allotted AU Class 62','AP Restriction AU Class 62',
			'Class Of Service AU Class 63','Class Type AU Class 63','Class Nest AU Class 63','Class Rank AU Class 63','Class AU AU Class 63','Class Allotted AU Class 63','AP Restriction AU Class 63',
			'Class Of Service AU Class 64','Class Type AU Class 64','Class Nest AU Class 64','Class Rank AU Class 64','Class AU AU Class 64','Class Allotted AU Class 64','AP Restriction AU Class 64'
		])

		print("Final Allocation Results:")
		print(self.allocResults)
		if self.inputSource[Constants.CONST_OUTPUT][Constants.CONST_AU_GRID] == Constants.FILE:
			AUFrame.to_csv(Constants.CONST_AU_GRID_FILE_PATH, index=False)
   
		if self.inputSource[Constants.CONST_OUTPUT][Constants.CONST_SUMMARY_FILE] == Constants.FILE:
			self.allocResults.to_csv(Constants.CONST_SUMMARY_FILE_PATH, index=False)

	def insertRunStart(self, startTime, username):
		if self.inputSource[Constants.CONST_LOGS][Constants.CONST_RUN_AUDIT] == Constants.DB:
			insertQuery = " INSERT INTO allocation_run_audit ( run_id, start_time, user) VALUES (%s, %s, %s)"
			self.wrconn.execute(insertQuery, 
				(self.runId, startTime.strftime(Constants.DATE_TIME_FORMAT), username))

	def updateNavitaire(self, rec):
		auData = {"runId": self.runId, "type": rec[0], "flight": rec[1], "startDate": rec[2], "endDate": rec[3], "dayOfWeek": rec[4],
                  "aircraftType": rec[5], "aircraftSuffix": rec[6], "lid": rec[7], "capacity": rec[8], "status": rec[9], "auClasses": {}}

		gap = 9
		for i in range(0, 64):
			auClass = {"classOfService": rec[gap + i*7 + 1], "classType": rec[gap + i*7 + 2], "classNest": int(rec[gap + i*7 + 3]),
                       "classRank": int(rec[gap + i*7 + 4]), "classAU": int(rec[gap + i*7 + 5]), "classAllotted": int(rec[gap + i*7 + 6]), "apRestriction": int(rec[gap + i*7 + 7])}
			auData['auClasses'][auClass['classOfService']] = auClass

		auDataStr = json.dumps(auData)
		# print(auDataStr)
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


	def getUniqueId(self):
		return str(uuid.uuid4())

	def loadRunParams(self):
		#load the list of sectors
		if self.inputSource[Constants.CONST_INPUT_SOURCE][Constants.CONST_MARKET_LIST] == Constants.TEST_DATA:
			marketListQuery = "SELECT Origin,Destin,FlightNumber,PerType,PerStart,PerEnd,DOW,bookedStatus,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusion,fareAnchor,fareOffset,FirstRBDAlloc,OtherRBDAlloc,B2BBackstop,B2CBackstop FROM market_list "
			self.mktList = pd.read_sql(marketListQuery, self.conn)
   
		if self.inputSource[Constants.CONST_INPUT_SOURCE][Constants.CONST_MARKET_LIST] == Constants.DB:
			marketListQuery = "SELECT Origin,Destin,FlightNumber,PerType,PerStart,PerEnd,DOW,bookedStatus,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusion,fareAnchor,fareOffset,FirstRBDAlloc,OtherRBDAlloc,B2BBackstop,B2CBackstop FROM market_list "
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
