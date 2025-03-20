import ast
import copy
from decimal import Decimal
import traceback
import os
import json
import warnings

import time as time_sleep
import numpy as np
import pandas as pd

from datetime import timedelta
from datetime import datetime
from datetime import time

from Constants import *
from ownlogger import OwnLogger
from allocations import RBDAllocations
from get_dynamic_time_d2 import GetDynamicTimeD2
from market_fares_service import MarketFareService
from determine_rbd import DetermineRBD

from helper.redis_helper import RedisHelper
from helper.nav_api_helper import NavAPIHelper
from helper.rm_database_helper import RMDatabaseHelper
from helper.nav_database_helper import NavDatabaseHelper

from utilities.utils import Utils
from utilities.math_utils import MathUtils
from utilities.time_utils import TimeUtils
from utilities.constants_utils import ConstantsUtils

from models.market_list_row import MarketListRow
from models.input_source import InputSource
from models.output_summary import OutputSummary
from models.au_grid_row import AuGridRow

warnings.filterwarnings('ignore')


class DynAlloc:
	def __init__(self, inputData, currParams):
		self._initialize_params(inputData, currParams)

		# Start the Allocation
		if self.currParams.run_single_market:
			self.runDynamicAllocationSingleMarket(1)
		else:
			self.runDynamicAllocation()

	def runDynamicAllocation(self):
		self.logger.info("Starting Dynamic Allocation")
		# Load the Run Parameters
		self.mktList = self.rmdatabasehelper.loadRunParams()
		startTime = self.timeutils.getCurrentDateTime()
		username = os.getlogin()

		self.rmdatabasehelper.insertRunStart(startTime, username)

		# The list of markets to run the allocation
		numMarkets = self.mktList.shape[0]
		self.logger.info("Market List has " + str(numMarkets) + " Rows")

		if self.debugMode:
			self.logger.info("Market List : ")
			self.logger.info(self.mktList)

		for mktCounter in range(numMarkets):
			self.currParams = MarketListRow(self.mktList.iloc[mktCounter])
			self.runDynamicAllocationSingleMarket(mktCounter)

		self.outputResults()

		endTime = self.timeutils.getCurrentDateTime()
		self.rmdatabasehelper.updateRunEnd(endTime)
		runTime = (endTime - startTime).total_seconds()
		self.logger.info("Allocation Completed in " + str(runTime) + " seconds")

	def runDynamicAllocationSingleMarket(self, mtkIndex):

		self.logger.info("**********************************************")
		self.logger.info("Starting Allocation for row : " + str(mtkIndex+1))

		if self.debugMode:
			self.logger.info(self.currParams)

		# Convert NDO to run dates as necessary
		currDate = self.timeutils.getCurrentDate()

		origin = self.currParams.origin
		destination = self.currParams.destin
		startDate = currDate
		endDate = currDate
		startNDO = self.currParams.per_start
		endNDO = self.currParams.per_end
		curveId = self.currParams.curve_id
		analystName = self.currParams.analyst_name
		FlightNumber = str(self.currParams.flight_number)
		self.currParams.strategy = self.currParams.fare_anchor

		startDate, endDate, startNDO, endNDO = self.getStartEndDate(currDate, startNDO, endNDO)

		self.logger.info("Allocation for " + origin + destination + " from " + str(startDate) + " to " + str(endDate))

		self.wait_for_inputs()
		bookedLoad = []
		if self.inputSource.input_source[CONST_BOOKLOAD] == TEST_DATA:
			bookedLoad = self.rmdatabasehelper.getCurrentBookedLoads(origin, destination, startDate, endDate)
		if self.inputSource.input_source[CONST_BOOKLOAD] == DB:
			bookedLoad = self.navdatabasehelper.getCurrentBookedLoads(origin, destination, startDate, endDate)

		try:
			# For each NDO
			for ndocounter in range(startNDO, endNDO+1):
				self.outputSummaryRow_b2c = OutputSummary()
				self.outputSummaryRow_b2c.w0Flag = self.w0Flag
				currDate = self.timeutils.getCurrentDate()
				tgtDate = datetime.strftime(currDate + timedelta(days=ndocounter), DATE_FORMAT_2)
				if bookedLoad.shape[0] == 0:
					dateFormatted = datetime.strptime(str(startDate), DATE_FORMAT_2).date().strftime(DATE_FORMAT)
					self.logger.info("Unable to get the Booked Load - Aborting")
					self.outputSummaryRow_b2c.Id = self.utils.getUniqueId()
					self.outputSummaryRow_b2c.RunId = self.runId
					self.outputSummaryRow_b2c.Origin = origin
					self.outputSummaryRow_b2c.Destin = destination
					self.outputSummaryRow_b2c.FltNum = str(FlightNumber)
					self.outputSummaryRow_b2c.DepDate = str(dateFormatted)
					self.outputSummaryRow_b2c.HowDetermined = HD_NO_BOOKLOADS
					self.outputSummaryRow_b2c.analystName = analystName
					self.outputSummaryRow_b2c.allocationStatus = 0
					self.outputSummaryRow_b2c.Channel=B2C_CHANNEL
					self.outputSummaryRow_b2b= copy.deepcopy(self.outputSummaryRow_b2c)
					self.outputSummaryRow_b2b.Id = self.utils.getUniqueId()
					self.outputSummaryRow_b2b.Channel=B2B_CHANNEL
					self.allocResults.append(self.outputSummaryRow_b2c)
					self.rmdatabasehelper.insertSummaryRow(self.allocResults)
					self.allocResults.append(self.outputSummaryRow_b2b)
					self.rmdatabasehelper.insertSummaryRow(self.allocResults)
					self.redishelper.updateRunAudit()
					break
				bookedLoad['DepartureDate'] = pd.to_datetime(bookedLoad['DepartureDate'])
				if FlightNumber == '*':
					bookedLoadSingleRow = bookedLoad[(bookedLoad['DepartureDate'] == tgtDate)]
				else:
					bookedLoadSingleRow = bookedLoad[(bookedLoad['DepartureDate'] == tgtDate) & (bookedLoad['FlightNumber'] == FlightNumber) & (bookedLoad['Origin'] == origin) & (bookedLoad['Destination'] == destination)]
				# Fetch the current booked load from Navitaire
				# If for whatever reason - unable to get the booked Load - Abort
				tgtBookedLoad = None
				if bookedLoadSingleRow.shape[0] == 0:
					dateFormatted = datetime.strptime(str(tgtDate), DATE_FORMAT_2).date().strftime(DATE_FORMAT)
					self.logger.info("Unable to get the Booked Load - Aborting")
					self.outputSummaryRow_b2c.Id = self.utils.getUniqueId()
					self.outputSummaryRow_b2c.RunId = self.runId
					self.outputSummaryRow_b2c.Origin = origin
					self.outputSummaryRow_b2c.Destin = destination
					self.outputSummaryRow_b2c.FltNum = str(FlightNumber)
					self.outputSummaryRow_b2c.DepDate = str(dateFormatted)
					self.outputSummaryRow_b2c.TgtLoad = str(tgtBookedLoad)
					self.outputSummaryRow_b2c.HowDetermined = HD_NO_BOOKLOADS
					self.outputSummaryRow_b2c.analystName = analystName
					self.outputSummaryRow_b2c.allocationStatus = 0
					self.outputSummaryRow_b2c.Channel=B2C_CHANNEL
					self.outputSummaryRow_b2b=copy.deepcopy(self.outputSummaryRow_b2c)
					self.outputSummaryRow_b2b.Channel=B2B_CHANNEL
					self.outputSummaryRow_b2b.Id = self.utils.getUniqueId()
					self.allocResults.append(self.outputSummaryRow_b2c)
					self.rmdatabasehelper.insertSummaryRow(self.allocResults)
					self.allocResults.append(self.outputSummaryRow_b2b)
					self.rmdatabasehelper.insertSummaryRow(self.allocResults)
					self.redishelper.updateRunAudit()
				else:
					tgtBookedLoadList = self.rmdatabasehelper.getTargetBookedLoadFromBenchmarkCurve(ndocounter, curveId)
					qpFares = self.rmdatabasehelper.get_qp_fares(origin, destination)
					TBFExist = self.rmdatabasehelper.get_TBF_fares(origin, destination)
					print("TBF Flag: ", self.currParams.tbfFlag)
					if len(tgtBookedLoadList) > 0:
						if not qpFares.empty:
							if int(self.currParams.tbfFlag) == 1 and not TBFExist:
								self.logger.info("Unable to get TBF Fares - Check the fares table")
								self.outputSummaryRow_b2c.Id = self.utils.getUniqueId()
								self.outputSummaryRow_b2c.RunId = self.runId
								self.outputSummaryRow_b2c.Origin = origin
								self.outputSummaryRow_b2c.Destin = destination
								self.outputSummaryRow_b2c.FltNum = str(FlightNumber)
								self.outputSummaryRow_b2c.DepDate = str(tgtDate)
								self.outputSummaryRow_b2c.BookedLoad = bookedLoadSingleRow["bookedPax"].values[0]
								self.outputSummaryRow_b2c.TgtLoad = str(tgtBookedLoad)
								self.outputSummaryRow_b2c.HowDetermined = HD_NO_TBF_FARES
								self.outputSummaryRow_b2c.analystName = analystName
								self.outputSummaryRow_b2c.allocationStatus = 0
								self.outputSummaryRow_b2c.Channel = B2C_CHANNEL

								self.outputSummaryRow_b2b = copy.deepcopy(self.outputSummaryRow_b2c)
								self.outputSummaryRow_b2b.Id = self.utils.getUniqueId()
								self.outputSummaryRow_b2b.Channel = B2B_CHANNEL

								self.allocResults.append(self.outputSummaryRow_b2c)
								self.rmdatabasehelper.insertSummaryRow(self.allocResults)
								self.allocResults.append(self.outputSummaryRow_b2b)
								self.rmdatabasehelper.insertSummaryRow(self.allocResults)
								self.redishelper.updateRunAudit()
							else:
								tgtBookedLoadPercent = float(tgtBookedLoadList['LF'][0])
								self.runDynamicAllocationSingleMarketSingleDay(ndocounter, bookedLoadSingleRow, tgtBookedLoadPercent)
						else:
							self.logger.info("Unable to get QP Fares - Check the fares table")
							self.outputSummaryRow_b2c.Id = self.utils.getUniqueId()
							self.outputSummaryRow_b2c.RunId = self.runId
							self.outputSummaryRow_b2c.Origin = origin
							self.outputSummaryRow_b2c.Destin = destination
							self.outputSummaryRow_b2c.FltNum = str(FlightNumber)
							self.outputSummaryRow_b2c.DepDate = str(tgtDate)
							self.outputSummaryRow_b2c.BookedLoad = bookedLoadSingleRow["bookedPax"].values[0]
							self.outputSummaryRow_b2c.TgtLoad = str(tgtBookedLoad)
							self.outputSummaryRow_b2c.HowDetermined = HD_NO_QP_FARES
							self.outputSummaryRow_b2c.analystName = analystName
							self.outputSummaryRow_b2c.allocationStatus = 0
							self.outputSummaryRow_b2c.Channel = B2C_CHANNEL

							self.outputSummaryRow_b2b = copy.deepcopy(self.outputSummaryRow_b2c)
							self.outputSummaryRow_b2b.Id = self.utils.getUniqueId()
							self.outputSummaryRow_b2b.Channel = B2B_CHANNEL

							self.allocResults.append(self.outputSummaryRow_b2c)
							self.rmdatabasehelper.insertSummaryRow(self.allocResults)
							self.allocResults.append(self.outputSummaryRow_b2b)
							self.rmdatabasehelper.insertSummaryRow(self.allocResults)
							self.redishelper.updateRunAudit()
					else:
						# Cannot get a reference curve - Abort
						self.logger.info("Unable to get a Target - Check the booking curves")
						self.outputSummaryRow_b2c.Id = self.utils.getUniqueId()
						self.outputSummaryRow_b2c.RunId = self.runId
						self.outputSummaryRow_b2c.Origin = origin
						self.outputSummaryRow_b2c.Destin = destination
						self.outputSummaryRow_b2c.FltNum = str(FlightNumber)
						self.outputSummaryRow_b2c.DepDate = str(tgtDate)
						self.outputSummaryRow_b2c.BookedLoad = bookedLoadSingleRow["bookedPax"].values[0]
						self.outputSummaryRow_b2c.TgtLoad = str(tgtBookedLoad)
						self.outputSummaryRow_b2c.HowDetermined = HD_NO_REF_CURVE
						self.outputSummaryRow_b2c.analystName = analystName
						self.outputSummaryRow_b2c.allocationStatus = 0
						self.outputSummaryRow_b2c.Channel=B2C_CHANNEL
						self.outputSummaryRow_b2b=copy.deepcopy(self.outputSummaryRow_b2c)
						self.outputSummaryRow_b2b.Id = self.utils.getUniqueId()
						self.outputSummaryRow_b2b.Channel=B2B_CHANNEL
						self.allocResults.append(self.outputSummaryRow_b2c)
						self.rmdatabasehelper.insertSummaryRow(self.allocResults)
						self.allocResults.append(self.outputSummaryRow_b2b)
						self.rmdatabasehelper.insertSummaryRow(self.allocResults)
						self.redishelper.updateRunAudit()		
		except Exception as e:
			self.logger.info("Exception occurred")
			traceback.print_exc()
			self.logger.info(e)

		if not self.inputSource.isUpdateNavitaire:
			self.outputResults()

		if self.inputSource.isUpdateNavitaire and (self.NavitaireUpdateMethod == CONST_SFTP or self.NavitaireUpdateMethod == S3):
			self.redishelper.setAllRunStatus2Ready()

	def runDynamicAllocationSingleMarketSingleDay(self, ndocounter, bookedLoad, tgtBookedLoadPercent):
		currDate = self.timeutils.getCurrentDate()
		tgtDate = datetime.strftime(currDate + timedelta(days=ndocounter), DATE_FORMAT_2)
		dow = int(datetime.strftime(currDate + timedelta(days=ndocounter), WEEK_DAY))

		self.logger.info("Determining pricing for NDO " + str(ndocounter) + " on  " + str(tgtDate))
		self.logger.info("Target Pax Load Percent is :" + str(tgtBookedLoadPercent))
		ndoBookedLoad = bookedLoad[bookedLoad['DepartureDate'].astype(str).str.contains(str(tgtDate))].reset_index(drop=True)

		numFlts = ndoBookedLoad.shape[0]
		if self.debugMode:
			self.logger.info("Found " + str(numFlts) + " Flights")
			self.logger.info(ndoBookedLoad)

		for fltCounter in range(numFlts):
			currFlt = ndoBookedLoad['FlightNumber'][fltCounter]
			currBookedLoad = ndoBookedLoad["bookedPax"][fltCounter]
			currLid = ndoBookedLoad['capacity'][fltCounter]
			currAdjustedCapacity = ndoBookedLoad['adjustedCapacity'][fltCounter]
			currtbfBookedLoad = ndoBookedLoad['tbf_bookings'][fltCounter]
			currFltDate = datetime.strftime(ndoBookedLoad['DepartureDate'][fltCounter], DATE_FORMAT)
			currFltDepartureTime = ndoBookedLoad["DepartureTime"][fltCounter]

			self.runDynamicAllocationSingleFlight(currFlt, currBookedLoad, currLid, currFltDate, currFltDepartureTime, tgtBookedLoadPercent, dow, ndocounter, currAdjustedCapacity,currtbfBookedLoad)

	def runDynamicAllocationSingleFlight(self, currFlt, currBookedLoad, currLid, currFltDate, currFltDepartureTime, tgtBookedLoadPercent, dow, ndocounter, currAdjustedCapacity,currtbfBookedLoad):
		self.outputSummaryRow_b2b = OutputSummary()
		self.outputSummaryRow_b2c = OutputSummary()
		self.outputSummaryRow_b2c.w0Flag = int(self.w0Flag)
		self.outputSummaryRow_b2b.w0Flag = int(self.w0Flag)
		tgtBookloadFactor = tgtBookedLoadPercent
		tgtBookedLoad = tgtBookedLoadPercent * currLid

		if self.debugMode:
			self.logger.info("Ready to start Allocation")
			self.logger.info(self.currParams)
			self.logger.info("DOW is : " + str(dow))
			self.logger.info("Extract is " + str(self.currParams.dow)[dow:(dow+1)])

		dateFormatted = datetime.strptime(str(currFltDate), DATE_FORMAT).date().strftime(DATE_FORMAT_2)
		if ((str(currFlt) == str(self.currParams.flight_number)) or (self.currParams.flight_number == '*')) and (str(self.currParams.dow)[dow:(dow+1)] == "1"):
			orgBookedLoad = currBookedLoad
			self.actualplf = (currBookedLoad/currAdjustedCapacity) * 100
			if int(self.currParams.seriesBlock) == 1:
				currBookedLoad = self.getBookloadSeries(dateFormatted,currBookedLoad)
			self.logger.info("Actual Booked Load for " + str(self.currParams.flight_number) + " on " + str(currFltDate) + " is :" + str(currBookedLoad))
			self.currentTime = self.rmdatabasehelper.getCurrentTime(self.inputSource.currentTime, self.currParams, currFltDate, self.starttime)
			totalCapacity = currLid
			adjustedCapacity = currAdjustedCapacity
			bookedDeficit = round(tgtBookedLoad - currBookedLoad)

			remainingCapacity = totalCapacity - currBookedLoad

			# plf = (currBookedLoad/adjustedCapacity) * 100

			startEndOfDayDplfBand, bookingToday,BookingsInHour, endOfDayTgtBookedLoad, tgtAtCurrentHour, startOfDayBookLoad,startOfDayTgtBookedLoad = self.getStartOfDayValues(currBookedLoad, dateFormatted, ndocounter, tgtBookloadFactor, totalCapacity, currFltDepartureTime)
			self.outputSummaryRow_b2b.currentTgtLoad = tgtAtCurrentHour
			self.outputSummaryRow_b2c.currentTgtLoad = tgtAtCurrentHour
			self.outputSummaryRow_b2b.currentLoadFactor = (tgtAtCurrentHour / totalCapacity) * 100
			self.outputSummaryRow_b2c.currentLoadFactor = (tgtAtCurrentHour / totalCapacity) * 100
			self.outputSummaryRow_b2b.bookingToday = bookingToday
			self.outputSummaryRow_b2c.bookingToday = bookingToday
			self.outputSummaryRow_b2b.bookingLastHour = BookingsInHour
			self.outputSummaryRow_b2c.bookingLastHour = BookingsInHour
			self.outputSummaryRow_b2b.endOfDayTgtLoad = endOfDayTgtBookedLoad
			self.outputSummaryRow_b2c.endOfDayTgtLoad = endOfDayTgtBookedLoad
			self.outputSummaryRow_b2c.dlfBand = startEndOfDayDplfBand
			self.outputSummaryRow_b2b.dlfBand = startEndOfDayDplfBand

			variance = self.calculateVariance(currBookedLoad, totalCapacity, ndocounter, tgtAtCurrentHour, startOfDayBookLoad,startOfDayTgtBookedLoad)

			mappedMonth = self.rmdatabasehelper.getMappedMonth(currFltDate)
			time_object = datetime.strptime(currFltDepartureTime, TIME_FORMAT).time()
			deptTime = f"{time_object.hour:02d}:{time_object.minute:02d}"
			currentDate = datetime.strptime(currFltDate, DATE_FORMAT)
			dow = currentDate.strftime(DAY_OF_WEEK)
			month = currentDate.strftime(MONTH)
			if len(mappedMonth) != 0:
				month = mappedMonth['mappedMonth'][0]    
    
			self.logger.info("Total capacity/Lid is: "+ str(totalCapacity))
			self.logger.info("Adjusted capacity is: "+ str(adjustedCapacity))
		
			if totalCapacity >= adjustedCapacity:
				plf = (currBookedLoad / adjustedCapacity)*100
			if totalCapacity < adjustedCapacity:
				plf = (currBookedLoad / totalCapacity) * 100
			self.logger.info("PLF is: "+ str(plf))
			totalCapacity, b2boverbookingflag = self.overBooking(currFltDate, ndocounter, totalCapacity, adjustedCapacity, plf, month, deptTime, currBookedLoad)
			self.logger.info("Total capacity/Lid after ob(if applicable) is: "+ str(totalCapacity))
			overBookingCount = float(totalCapacity-adjustedCapacity)
			self.logger.info("variance is " + str(variance))
			ndoBand = self.rmdatabasehelper.getNdoBand(ndocounter)
			dplfBand = self.rmdatabasehelper.getdpflBand(variance)

			self.outputSummaryRow_b2c.endOfDaydlfBand = dplfBand
			self.outputSummaryRow_b2b.endOfDaydlfBand = dplfBand

			self.checkTreshold(currFlt, ndocounter, plf)

			# Get Dynamic D1 (Time) Value
			DaySpan, input_time_str = self.autoTimeRange(currFltDepartureTime)

			self.outputSummaryRow_b2b.startTime, self.outputSummaryRow_b2b.endTime, self.outputSummaryRow_b2b.TimeRange,hardstartTime,hardendTime = self.dynamictimed2.getDynamicTime(self.currParams, dplfBand, ndoBand, DaySpan, B2B_CHANNEL,plf)
			self.outputSummaryRow_b2c.startTime, self.outputSummaryRow_b2c.endTime, self.outputSummaryRow_b2c.TimeRange,hardstartTime,hardendTime = self.dynamictimed2.getDynamicTime(self.currParams, dplfBand, ndoBand, DaySpan, B2C_CHANNEL,plf)

			# Get Dynamic D2 (Offset & Criteria) Value
			self.outputSummaryRow_b2b.criteria,hardcriteria, self.outputSummaryRow_b2b.fareOffset,hardoffset, self.outputSummaryRow_b2b.StrategyReference,hardfareb2b = self.dynamictimed2.getDynamicD2Value(self.currParams, dplfBand, ndoBand, B2B_CHANNEL,plf)
			self.outputSummaryRow_b2c.criteria,hardcriteria, self.outputSummaryRow_b2c.fareOffset,hardoffset, self.outputSummaryRow_b2c.StrategyReference,hardfareb2c = self.dynamictimed2.getDynamicD2Value(self.currParams, dplfBand, ndoBand, B2C_CHANNEL,plf)

			self.outputSummaryRow_b2b.Id = self.utils.getUniqueId()
			self.outputSummaryRow_b2c.Id = self.utils.getUniqueId()

			# Actual Work Happens here
			# Get the B2C and B2B Fare ranges
			self.logger.info("Getting market Fares")

			self.outputSummaryRow_b2c.MktFare_Min, self.outputSummaryRow_b2c.MktFare_Max, self.outputSummaryRow_b2c.AirlineCode_Min, self.outputSummaryRow_b2c.AirlineCode_Max, self.outputSummaryRow_b2c.FlightNumber_Min, self.outputSummaryRow_b2c.FlightNumber_Max, isB2CMarketFareAvailable, self.outputSummaryRow_b2c.criteria, self.outputSummaryRow_b2c.startTime, self.outputSummaryRow_b2c.endTime,hardfareb2c = self.marketfaresservice.getMarketFareAnchorFare(self.currParams, currFltDate, B2C_CHANNEL, self.outputSummaryRow_b2c.startTime, self.outputSummaryRow_b2c.endTime, DaySpan, self.outputSummaryRow_b2c.criteria, input_time_str,hardcriteria,hardfareb2c,hardstartTime,hardendTime)
   

			self.outputSummaryRow_b2b.MktFare_Min, self.outputSummaryRow_b2b.MktFare_Max, self.outputSummaryRow_b2b.AirlineCode_Min, self.outputSummaryRow_b2b.AirlineCode_Max, self.outputSummaryRow_b2b.FlightNumber_Min, self.outputSummaryRow_b2b.FlightNumber_Max, isB2BMarketFareAvailable, self.outputSummaryRow_b2b.criteria, self.outputSummaryRow_b2b.startTime, self.outputSummaryRow_b2b.endTime,hardfareb2b = self.marketfaresservice.getMarketFareAnchorFare(self.currParams, currFltDate, B2B_CHANNEL, self.outputSummaryRow_b2b.startTime, self.outputSummaryRow_b2b.endTime, DaySpan, self.outputSummaryRow_b2b.criteria, input_time_str,hardcriteria,hardfareb2b,hardstartTime,hardendTime)

			
			actualPercentile = totalCapacity / PROFILE_FARES_QUARTILE
			bookloadPercentile = self.mathutils.getCeilNumber(currBookedLoad / actualPercentile)

			self.logger.info("actualPercentile: " + str(actualPercentile))
			self.logger.info("bookloadPercentile: " + str(bookloadPercentile))
			if bookloadPercentile > PROFILE_FARES_QUARTILE:
				bookloadPercentile = PROFILE_FARES_QUARTILE
			if bookloadPercentile < PROFILE_FARES_25PERCENT_QUARTILE:
				bookloadPercentile = PROFILE_FARES_25PERCENT_QUARTILE
			percentile = 'Q'+str(bookloadPercentile)

			self.logger.info("percentile: " + str(percentile))

			sector = self.currParams.origin + self.currParams.destin
			upsellValueData = self.rmdatabasehelper.getUpsellValue(currFltDate, sector)

			upsellValue = 0
			b2c_backstop = self.currParams.b2c_backstop
			b2b_backstop = self.currParams.b2b_backstop
			auto_b2b_backstop = "NA"
			auto_b2c_backstop = "NA"
			autoBackstopFlag = str(self.currParams.autoBackstopFlag)
			if(autoBackstopFlag == "1"):
				final_mkt_fare_b2b, final_mkt_fare_b2c = self.determineFinalMF(dplfBand, currFltDate, DaySpan, input_time_str)
				auto_b2b_backstop = self.auto_backstop(autoBackstopFlag, self.currParams.b2b_backstop, dplfBand, final_mkt_fare_b2b)
				auto_b2c_backstop = self.auto_backstop(autoBackstopFlag, self.currParams.b2c_backstop, dplfBand, final_mkt_fare_b2c)
				if(auto_b2c_backstop != "NA"):
					b2c_backstop = auto_b2c_backstop
				else:
					b2c_backstop = self.currParams.b2c_backstop
		
				if(auto_b2b_backstop != "NA"):
					b2b_backstop = auto_b2b_backstop
				else:
					b2b_backstop = self.currParams.b2b_backstop
			
			self.logger.info("auto_b2c_backstop: " + str(auto_b2c_backstop))
			self.logger.info("auto_b2b_backstop: " + str(auto_b2b_backstop))
    
			if len(upsellValueData) != 0 and upsellValueData.percentage[0] is not None:
				upsellValue = upsellValueData.percentage[0]

			if self.inputSource.input_source[CONST_LAST_SELLING_FARE] == TEST_DATA:
				farePrice = self.rmdatabasehelper.getLastSellingFare(currFltDate, self.currParams)
				last_seven_avg = CONST_NA
				if farePrice.shape[0] != 0:
					lastSoldFareb2c = farePrice["B2C_last3avg_fare"][0]
					lastSoldFareb2b = farePrice["B2B_last3avg_fare"][0]
					last_seven_avg = farePrice["last7avg"][0]
						
			if self.inputSource.input_source[CONST_LAST_SELLING_FARE] == DB:
				lastSoldFareb2c = CONST_NA
				lastSoldFareb2b = CONST_NA
				last_seven_avg = CONST_NA
				flightDate = datetime.strptime(currFltDate, DATE_FORMAT).date()	
				key = f"LastSoldFare:{self.currParams.origin}{self.currParams.destin}{self.currParams.flight_number}_{flightDate}"
				if self.cache_client.exists(key):
					fare_manifest = self.cache_client.get(key).decode('utf-8')
					last_seven_avg, lastsoldb2b, lastsoldb2c = self.calculate_averages(fare_manifest)
					lastSoldFareb2c = lastsoldb2c
					lastSoldFareb2b = lastsoldb2b
			b2b_discount_fare = False
			discount_amount = 0
			if isB2CMarketFareAvailable or last_seven_avg!=CONST_NA:
				
				if self.outputSummaryRow_b2c.criteria in [MAX_FARE_ANCHOR, MAXO_FARE_ANCHOR]:
					self.outputSummaryRow_b2c.anchorFare = self.outputSummaryRow_b2c.MktFare_Max
				if self.outputSummaryRow_b2c.criteria in [MIN_FARE_ANCHOR, MINO_FARE_ANCHOR]:
					self.outputSummaryRow_b2c.anchorFare = self.outputSummaryRow_b2c.MktFare_Min
				
				last7avg = False
				if not isB2CMarketFareAvailable:
					self.outputSummaryRow_b2c.lastSevenAvgFare = last_seven_avg
					self.outputSummaryRow_b2c.anchorFare = last_seven_avg
					last7avg = True

				self.logger.info("Market Fare Range for " + str(currFltDate) + " in the B2C Channel is :" + str(self.outputSummaryRow_b2c.MktFare_Min) + " to " + str(self.outputSummaryRow_b2c.MktFare_Max))
				openDataB2C, self.outputSummaryRow_b2c.lastSellingFare, self.outputSummaryRow_b2c.strategyFare, self.outputSummaryRow_b2c.profileFare, self.outputSummaryRow_b2c.hardAnchorFare,self.outputSummaryRow_b2c.upsell = self.determinerbd.determineRBDToOpen(self.outputSummaryRow_b2c.anchorFare, self.outputSummaryRow_b2c.fareOffset, B2C_CHANNEL, dateFormatted, dplfBand, ndocounter, currBookedLoad, totalCapacity, self.currParams, b2c_backstop, self.currentTime, startEndOfDayDplfBand, variance, month, deptTime, dow, percentile, upsellValue, self.currParams.b2c_tolerance,plf,float(totalCapacity-adjustedCapacity),hardfareb2c,hardoffset,BookingsInHour,lastSoldFareb2c,last7avg,b2b_discount_fare,discount_amount,self.w0Flag,self.currParams.obFare)

				B2CRBDs = self.rmdatabasehelper.fareExtract(self.currParams.origin, self.currParams.destin, B2C_CHANNEL, '', 'L')
				if openDataB2C['Fare'][0] != CONST_NA:
					B2CopeningPrice = float(openDataB2C['Fare'][0])
				else:
					B2CopeningPrice = None
				
				if  isB2BMarketFareAvailable:
					if self.outputSummaryRow_b2b.criteria in [MAX_FARE_ANCHOR, MAXO_FARE_ANCHOR]:
						self.outputSummaryRow_b2b.anchorFare = self.outputSummaryRow_b2b.MktFare_Max
					if self.outputSummaryRow_b2b.criteria in [MIN_FARE_ANCHOR, MINO_FARE_ANCHOR]:
						self.outputSummaryRow_b2b.anchorFare = self.outputSummaryRow_b2b.MktFare_Min

					self.logger.info("Market Fare Range for " + str(currFltDate) + " in the B2B Channel is :" + str(self.outputSummaryRow_b2b.MktFare_Min) + " to " + str(self.outputSummaryRow_b2b.MktFare_Max))
					openDataB2B, self.outputSummaryRow_b2b.lastSellingFare, self.outputSummaryRow_b2b.strategyFare, self.outputSummaryRow_b2b.profileFare, self.outputSummaryRow_b2b.hardAnchorFare,self.outputSummaryRow_b2b.upsell = self.determinerbd.determineRBDToOpen(self.outputSummaryRow_b2b.anchorFare, self.outputSummaryRow_b2b.fareOffset, B2B_CHANNEL, dateFormatted, dplfBand, ndocounter, currBookedLoad, totalCapacity, self.currParams, b2b_backstop, self.currentTime, startEndOfDayDplfBand, variance, month, deptTime, dow, percentile, upsellValue, self.currParams.b2b_tolerance,plf,float(totalCapacity-adjustedCapacity),hardfareb2b,hardoffset,BookingsInHour,lastSoldFareb2b,last7avg,b2b_discount_fare,discount_amount,self.w0Flag,self.currParams.obFare)
				elif B2CopeningPrice is not None and not self.B2B_threshold and dplfBand>=0 and dplfBand<=3:
					b2b_discount_fare = True
					last7avg = False
					discount_map = ast.literal_eval(self.constantsUtils.B2B_DISCOUNT_MAP)
					discount = discount_map[str(dplfBand)]
					if '%' in discount:
						# Calculate percentage discount
						discount_amount = B2CopeningPrice * (float(discount.strip('%')) / 100)
					else:
						# Use absolute discount amount
						discount_amount = float(discount)
					
					self.outputSummaryRow_b2b.anchorFare = B2CopeningPrice - discount_amount
					self.logger.info("Market Fare Range for " + str(currFltDate) + " in the B2B Channel is :" + str(self.outputSummaryRow_b2b.MktFare_Min) + " to " + str(self.outputSummaryRow_b2b.MktFare_Max))
					openDataB2B, self.outputSummaryRow_b2b.lastSellingFare, self.outputSummaryRow_b2b.strategyFare, self.outputSummaryRow_b2b.profileFare, self.outputSummaryRow_b2b.hardAnchorFare,self.outputSummaryRow_b2b.upsell = self.determinerbd.determineRBDToOpen(self.outputSummaryRow_b2b.anchorFare, self.outputSummaryRow_b2b.fareOffset, B2B_CHANNEL, dateFormatted, dplfBand, ndocounter, currBookedLoad, totalCapacity, self.currParams, b2b_backstop, self.currentTime, startEndOfDayDplfBand, variance, month, deptTime, dow, percentile, upsellValue, self.currParams.b2b_tolerance,plf,float(totalCapacity-adjustedCapacity),hardfareb2b,hardoffset,BookingsInHour,lastSoldFareb2b,last7avg,b2b_discount_fare,discount_amount,self.w0Flag,self.currParams.obFare) 
				else:
					openDataB2B = {'RBD': [CONST_NA], 'Fare': [CONST_NA], 'FareResult': ['B2B Market Data NA - Skipping allocation'], 'anchorFare': [CONST_NA], 'statusInd': [3], 'allocationStatus': [0]}
			else:
				openDataB2C = {'RBD': [CONST_NA], 'Fare': [CONST_NA], 'FareResult': ['B2C Market Data NA - Skipping allocation'], 'anchorFare': [CONST_NA], 'statusInd': [1], 'allocationStatus': [0]}
				openDataB2B = {'RBD': [CONST_NA], 'Fare': [CONST_NA], 'FareResult': ['B2C Market Data NA - Skipping allocation'], 'anchorFare': [CONST_NA], 'statusInd': [3], 'allocationStatus': [0]}
				self.redishelper.updateRunAudit()

			# statusInd
			# 0 - Allocation went as planned
			# 1 - Could not find market fares
			# 2 - Ran out of RBDs before the offset could be achieved

			if (openDataB2C['statusInd'][0] == 0) or (openDataB2C['statusInd'][0] == 2):
				# Legit Allocation - Write to AU File
				currRec = AuGridRow(self.AU_COLUMN_LENGTH)
				currRec.type = 'L'
				currRec.flight = ' QP' + str(currFlt) + ' ' + self.currParams.origin + self.currParams.destin
				currRec.start_date = datetime.strftime(datetime.strptime(currFltDate, DATE_FORMAT), DATE_FORMAT_3)
				currRec.end_date = datetime.strftime(datetime.strptime(currFltDate, DATE_FORMAT), DATE_FORMAT_3)
				currRec.day_of_week = 'Daily'
				# Condition Added since we don't need to update the AU Grid in Local we need the RunId's when we update Navitare via Server Run
				if self.inputSource.isUpdateNavitaire and not (self.NavitaireUpdateMethod == S3):
					currRec.b2crunid = self.outputSummaryRow_b2c.Id
					currRec.b2brunid = self.outputSummaryRow_b2b.Id

				profileExtract = self.rmdatabasehelper.profileFaresExtract(self.currParams.origin, self.currParams.destin, month, dow, deptTime)
				profileExtractMap = profileExtract
				profileExtract = profileExtract.transpose()
				profileExtract = profileExtract.values.flatten()
				profileExtract = sorted(profileExtract)
				# First - Adding all B2C RBD's
				b2cAllocationsUnits, brd_counter, status,_,B2CRBDs,openingRBDPrice = self.allocations.b2CAllocation(orgBookedLoad, openDataB2C, currRec, totalCapacity, bookedDeficit, variance, remainingCapacity, ndocounter, self.currParams, self.outputSummaryRow_b2c.profileFare, profileExtract, bookloadPercentile, upsellValue)
				alloc_df = self.allocations.b2CAllocations(currBookedLoad, openDataB2C, currRec, totalCapacity, bookedDeficit, variance, remainingCapacity, ndocounter, self.currParams, self.outputSummaryRow_b2c.profileFare, profileExtract, bookloadPercentile, upsellValue,openingRBDPrice)

				self.outputSummaryRow_b2c.status = status
				self.outputSummaryRow_b2b.status = status

				isUpsellFlag = False
				b2bSkipFlag = False

				B2BRBDs = self.rmdatabasehelper.fareExtract(self.currParams.origin, self.currParams.destin, B2B_CHANNEL, 'Z0', 'L')
				B2B_Z0_RBD = B2BRBDs['Total'][0]
    
				if(float(B2B_Z0_RBD)<float(self.currParams.b2b_backstop)):
					openDataB2B = {'RBD': [CONST_NA], 'Fare': [CONST_NA], 'FareResult': ['B2B allocation zero Backtop higher'], 'anchorFare': [CONST_NA], 'statusInd': [1], 'allocationStatus': [1]}
				if float(self.currParams.b2b_factor) == -1:
					b2bSkipFlag = True
				if float(totalCapacity-adjustedCapacity)>0:
					openDataB2B = {'RBD': [CONST_NA], 'Fare': [CONST_NA], 'FareResult': ['B2B allocation zero Overbooking'], 'anchorFare': [CONST_NA], 'statusInd': [1], 'allocationStatus': [1]}

				# Now add all the Z RBDs
				isUpsellFlag, brd_counter = self.allocations.b2BAllocation(orgBookedLoad, openDataB2B, currRec, totalCapacity, variance, b2cAllocationsUnits, b2bSkipFlag, b2boverbookingflag, brd_counter, openDataB2C, self.currParams,float(totalCapacity-adjustedCapacity))

				if isUpsellFlag:
					openDataB2B = {'RBD': [CONST_NA], 'Fare': [CONST_NA], 'FareResult': ['B2B allocation skipped - Flight in upsell'], 'anchorFare': [CONST_NA], 'statusInd': [1], 'allocationStatus': [1]}

				if b2bSkipFlag:
					openDataB2B = {'RBD': [CONST_NA], 'Fare': [CONST_NA], 'FareResult': ['B2B allocation skipped - B2B Factor is -1'], 'anchorFare': [CONST_NA], 'statusInd': [1], 'allocationStatus': [1]}

				# Ends statsusInd == 0
    
				self.l_rbd_allocation(dateFormatted, ndoBand, dplfBand, currRec, brd_counter)
				self.p_rbd_allocation(dateFormatted, ndoBand, dplfBand, currRec, brd_counter)
							

				brd_counter = self.w_rbd_allocation(plf, currRec, brd_counter)
				brd_counter = self.allocations.group_quote_alloc(profileExtractMap,brd_counter+1,currRec,self.currParams.autoGroup,adjustedCapacity,upsellValue,B2CRBDs,openingRBDPrice)
				self.allocations.tbf_allocation(alloc_df,brd_counter,currRec,self.currParams.tbfFlag,B2CRBDs,currtbfBookedLoad,currBookedLoad,adjustedCapacity,openingRBDPrice,month,sector)
				
    

				self.AUFileData.append(currRec)

				if self.inputSource.isUpdateNavitaire and (self.NavitaireUpdateMethod == CONST_API):
					self.updateNavitaire(currRec)

				if self.inputSource.isUpdateNavitaire and (self.NavitaireUpdateMethod == CONST_SFTP):
					self.updateNavSFTP(currRec)

				if self.inputSource.isUpdateNavitaire and (self.NavitaireUpdateMethod == S3):
					self.updateS3Push(currRec)

			if float(totalCapacity-adjustedCapacity)>0:
				openDataB2C['FareResult'][0] = openDataB2C['FareResult'][0]+' -Overbooking'
				openDataB2B['FareResult'][0] = openDataB2B['FareResult'][0]+' -Overbooking'

			self.logger.info("Anchor fare b2c is: " + str(openDataB2C["anchorFare"][0]))
			self.logger.info("Anchor fare b2b is: " + str(openDataB2B["anchorFare"][0]))

			if int(self.currParams.profileFares) == 0:
				self.outputSummaryRow_b2b.profileFare = CONST_NA
				self.outputSummaryRow_b2c.profileFare = CONST_NA
    
			self.outputSummaryRow_b2c.RunId = self.runId
			self.outputSummaryRow_b2c.Origin = self.currParams.origin
			self.outputSummaryRow_b2c.Destin = self.currParams.destin
			self.outputSummaryRow_b2c.FltNum = str(currFlt)
			self.outputSummaryRow_b2c.DepDate = str(currFltDate)
			self.outputSummaryRow_b2c.BookedLoad = str(currBookedLoad)
			self.outputSummaryRow_b2c.TgtLoad = str(startOfDayBookLoad * totalCapacity)
			self.outputSummaryRow_b2c.ndoBand = str(ndoBand)
			self.outputSummaryRow_b2c.anchorFare = str(openDataB2C["anchorFare"][0])
			self.outputSummaryRow_b2c.OpenRBD = str(openDataB2C['RBD'][0])
			self.outputSummaryRow_b2c.SellingFare = str(openDataB2C["Fare"][0])
			self.outputSummaryRow_b2c.Channel = B2C_CHANNEL
			self.outputSummaryRow_b2c.HowDetermined = str(openDataB2C['FareResult'][0])
			self.outputSummaryRow_b2c.Backstop = self.currParams.b2c_backstop
			self.outputSummaryRow_b2c.autoBackstop = auto_b2c_backstop
			self.outputSummaryRow_b2c.Variance = str(variance)
			self.outputSummaryRow_b2c.analystName = self.currParams.analyst_name
			self.outputSummaryRow_b2c.bookedPlf = str(plf)
			self.outputSummaryRow_b2c.allocationStatus = (openDataB2C['allocationStatus'][0])
			self.outputSummaryRow_b2c.overBookingCount = overBookingCount
			self.outputSummaryRow_b2c.actualbookedPlf = self.actualplf
			self.outputSummaryRow_b2c.recommendedActions = self.recommend_action(self.outputSummaryRow_b2c)
			
			self.allocResults.append(self.outputSummaryRow_b2c)
			self.rmdatabasehelper.insertSummaryRow(self.allocResults)

			self.outputSummaryRow_b2b.RunId = self.runId
			self.outputSummaryRow_b2b.Origin = self.currParams.origin
			self.outputSummaryRow_b2b.Destin = self.currParams.destin
			self.outputSummaryRow_b2b.FltNum = str(currFlt)
			self.outputSummaryRow_b2b.DepDate = str(currFltDate)
			self.outputSummaryRow_b2b.BookedLoad = str(currBookedLoad)
			self.outputSummaryRow_b2b.TgtLoad = str(startOfDayBookLoad * totalCapacity)
			self.outputSummaryRow_b2b.ndoBand = str(ndoBand)
			self.outputSummaryRow_b2b.anchorFare = str(openDataB2B["anchorFare"][0])
			self.outputSummaryRow_b2b.OpenRBD = str(openDataB2B['RBD'][0])
			self.outputSummaryRow_b2b.SellingFare = str(openDataB2B["Fare"][0])
			self.outputSummaryRow_b2b.Channel = B2B_CHANNEL
			self.outputSummaryRow_b2b.HowDetermined = str(openDataB2B['FareResult'][0])
			self.outputSummaryRow_b2b.Backstop = self.currParams.b2b_backstop
			self.outputSummaryRow_b2b.autoBackstop = auto_b2b_backstop
			self.outputSummaryRow_b2b.Variance = str(variance)
			self.outputSummaryRow_b2b.analystName = self.currParams.analyst_name
			self.outputSummaryRow_b2b.bookedPlf = str(plf)
			self.outputSummaryRow_b2b.allocationStatus = (openDataB2B['allocationStatus'][0])
			self.outputSummaryRow_b2b.overBookingCount = overBookingCount
			self.outputSummaryRow_b2b.actualbookedPlf = self.actualplf
			self.outputSummaryRow_b2b.recommendedActions = self.recommend_action(self.outputSummaryRow_b2b)

			self.allocResults.append(self.outputSummaryRow_b2b)
			self.rmdatabasehelper.insertSummaryRow(self.allocResults)

		else:
			if str(self.currParams.dow)[dow:(dow + 1)] == "9":
				self.redishelper.updateRunAudit()
			self.logger.info("Run Parameters requested to skip - (FlightNumber, DOW, or Status)")

	def w_rbd_allocation(self, plf, currRec, brd_counter):
		currRec.set_class_attribute(brd_counter+2, 'class_of_service', 'W9')
		currRec.set_class_attribute(brd_counter+3, 'class_of_service', 'W8')
		currRec.set_class_attribute(brd_counter+4, 'class_of_service', 'W7')
		currRec.set_class_attribute(brd_counter+5, 'class_of_service', 'W6')
		currRec.set_class_attribute(brd_counter+6, 'class_of_service', 'W5')
		currRec.set_class_attribute(brd_counter+7, 'class_of_service', 'W4')
		currRec.set_class_attribute(brd_counter+8, 'class_of_service', 'W3')
		currRec.set_class_attribute(brd_counter+9, 'class_of_service', 'W2')
		currRec.set_class_attribute(brd_counter+10, 'class_of_service', 'W1')
	
		if self.w9_thresholdflag:
			currRec.set_class_attribute(brd_counter+2, 'class_au', 0)
		else:
			currRec.set_class_attribute(brd_counter+2, 'class_au', -1)
		
		if self.w8_thresholdflag:
			currRec.set_class_attribute(brd_counter+3, 'class_au', 0)
		else:
			currRec.set_class_attribute(brd_counter+3, 'class_au', -1)
	
		if self.w7_thresholdflag:
			currRec.set_class_attribute(brd_counter+4, 'class_au', 0)
		else:
			currRec.set_class_attribute(brd_counter+4, 'class_au', -1)
	
		if self.w6_thresholdflag:
			currRec.set_class_attribute(brd_counter+5, 'class_au', 0)
		else:
			currRec.set_class_attribute(brd_counter+5, 'class_au', -1)

		if self.w5_thresholdflag:
			currRec.set_class_attribute(brd_counter+6, 'class_au', 0)
		else:
			currRec.set_class_attribute(brd_counter+6, 'class_au', -1)

		if self.w4_thresholdflag:
			currRec.set_class_attribute(brd_counter+7, 'class_au', 0)
		else:
			currRec.set_class_attribute(brd_counter+7, 'class_au', -1)

		if self.w3_thresholdflag:
			currRec.set_class_attribute(brd_counter+8, 'class_au', 0)
		else:
			currRec.set_class_attribute(brd_counter+8, 'class_au', -1)

		if self.w2_thresholdflag:
			currRec.set_class_attribute(brd_counter+9, 'class_au', 0)
		else:
			currRec.set_class_attribute(brd_counter+9, 'class_au', -1)

		if self.w1_thresholdflag:
			currRec.set_class_attribute(brd_counter+10, 'class_au', 0)
		else:
			currRec.set_class_attribute(brd_counter+10, 'class_au', -1)

		return brd_counter+10

	def p_rbd_allocation(self, dateFormatted, ndoBand, dplfBand, currRec, brd_counter):
		currRec.set_class_attribute(brd_counter + 1, 'class_of_service', 'P')
		if self.p_thresholdflag:
			currRec.set_class_attribute(brd_counter + 1, 'class_au', 0)
		elif int(self.currParams.distressInventoryFlag) == 1:
			self.allocate_distress_inventory(dateFormatted, ndoBand, dplfBand, currRec, brd_counter)
		else:
			currRec.set_class_attribute(brd_counter + 1, 'class_au', -1)

	def l_rbd_allocation(self, dateFormatted, ndoBand, dplfBand, currRec, brd_counter):
		currRec.set_class_attribute(brd_counter, 'class_of_service', 'L')
		if self.l_thresholdflag:
			currRec.set_class_attribute(brd_counter, 'class_au', 0)
		elif int(self.currParams.distressInventoryFlag) == 1:
			self.allocate_distress_inventory(dateFormatted, ndoBand, dplfBand, currRec, brd_counter)
		else:
			currRec.set_class_attribute(brd_counter , 'class_au', -1)


	def allocate_distress_inventory(self, dateFormatted, ndoBand, dplfBand, currRec, brd_counter):
		default_au, au_cap = self.rmdatabasehelper.getDefaultAu(dateFormatted)
		distress_au = self.rmdatabasehelper.getDistressAu(ndoBand, dplfBand)
		l_class_seats_sold, p_class_seats_sold= self.get_seats_sold(dateFormatted)
		l_class_rbd, p_class_rbd= self.get_current_rbds(dateFormatted)

		self.allocate_class_inventory(currRec, brd_counter, default_au, au_cap, distress_au,
									l_class_seats_sold, p_class_seats_sold,l_class_rbd, p_class_rbd)

	def get_seats_sold(self, dateFormatted):
		if self.inputSource.input_source[CONST_LAST_SELLING_FARE] == TEST_DATA:
			return self.rmdatabasehelper.get_seats_sold_by_class(str(self.currParams.flight_number),
																self.currParams.origin, self.currParams.destin,
																dateFormatted)
		elif self.inputSource.input_source[CONST_LAST_SELLING_FARE] == DB:
			return self.navdatabasehelper.get_seats_sold_by_class(str(self.currParams.flight_number),
																self.currParams.origin, self.currParams.destin,
																dateFormatted)

	def get_current_rbds(self, dateFormatted):
		if self.inputSource.input_source[CONST_LAST_SELLING_FARE] == TEST_DATA:
			return self.rmdatabasehelper.get_current_rbds_by_class(dateFormatted,
															str(self.currParams.flight_number),
															self.currParams.origin + self.currParams.destin)
		elif self.inputSource.input_source[CONST_LAST_SELLING_FARE] == DB:
			return self.navdatabasehelper.get_current_rbds_by_class(dateFormatted,
															str(self.currParams.flight_number),
															self.currParams.origin + self.currParams.destin)

	def allocate_class_inventory(self, currRec, brd_counter, default_au, au_cap, distress_au,
									l_class_seats_sold, p_class_seats_sold, l_class_rbd, p_class_rbd):
		self.allocate_individual_class(currRec, brd_counter, default_au, au_cap, distress_au,
										l_class_seats_sold, l_class_rbd, 'L')
		self.allocate_individual_class(currRec, brd_counter + 1, default_au, au_cap, distress_au,
										p_class_seats_sold, p_class_rbd, 'P')

	def allocate_individual_class(self, currRec, brd_counter, default_au, au_cap, distress_au,
								class_seats_sold, class_rbd, class_of_service):
		if class_seats_sold < default_au:
			currRec.set_class_attribute(brd_counter, 'class_au', default_au)
		elif class_seats_sold == class_rbd:
			new_au = class_rbd + distress_au
			currRec.set_class_attribute(brd_counter, 'class_au', min(new_au, au_cap))
		else:
			currRec.set_class_attribute(brd_counter, 'class_au', -1)
		currRec.set_class_attribute(brd_counter, 'class_of_service', class_of_service)


	def getStartOfDayValues(self, currBookedLoad, currFltDate, ndocounter, tgtBookloadFactor, totalCapacity,currFltDepartureTime):
		bookings = []
		if self.inputSource.input_source[CONST_LAST_SELLING_FARE] == TEST_DATA:
			bookings = self.rmdatabasehelper.getNumberOfBookings(currFltDate, self.currParams)
		if self.inputSource.input_source[CONST_LAST_SELLING_FARE] == DB:
			bookings = self.navdatabasehelper.getNumberOfBookings(currFltDate, self.currParams)

		lastBookloadTillMidnight = 0
		bookingToday = 0
		bookingLastHour = 0
		if bookings.shape[0] != 0:
			lastBookloadTillMidnight = bookings["BookingsCount"][0]
			bookingToday = bookings["BookingToday"][0]
			bookingLastHour = bookings["BookingsInHour"][0]
		self.logger.info("Number of Bookings is " + str(lastBookloadTillMidnight))

		curTime = datetime.strptime(str(self.currentTime), TIME_FORMAT)
		deptTime = datetime.strptime(str(currFltDepartureTime), TIME_FORMAT)

		area = self.rmdatabasehelper.getArea(curTime.hour)
		areaAtCurrentTime = area["sum"][0] / 100
		self.logger.info("Area is:" + str(areaAtCurrentTime))
  
		startOfDayTgtList = self.rmdatabasehelper.getTargetBookedLoadFromBenchmarkCurve(ndocounter + 1, self.currParams.curve_id)
		if startOfDayTgtList.shape[0] == 0:
			startOfDayTgtList = self.rmdatabasehelper.getTargetBookedLoadFromBenchmarkCurve(ndocounter, self.currParams.curve_id)

		startOfDayBookLoad = float(startOfDayTgtList['LF'][0])
		startOfDayTgtBookedLoad = startOfDayBookLoad * totalCapacity
  
		startOfdayFactor = ((lastBookloadTillMidnight/totalCapacity) - startOfDayBookLoad) * 100
		startEndOfDayDplfBand = self.rmdatabasehelper.getdpflBand(startOfdayFactor)

		self.logger.info("startEndOfDayDplfBand is: " + str(startEndOfDayDplfBand))
		endOfDayTgtBookedLoad = tgtBookloadFactor * totalCapacity
  
		if ndocounter == 0:
			endOfDayTgtBookedLoad = max((totalCapacity * self.constantsUtils.END_OF_DAY_LF_FACTOR), endOfDayTgtBookedLoad)
			hours = (deptTime - timedelta(hours=2)).hour
			area_current_time=self.rmdatabasehelper.getArea(curTime.hour)
			area_2hrs_departure=self.rmdatabasehelper.getArea(hours)
			area =area_current_time["sum"][0] /area_2hrs_departure["sum"][0]
			if area_current_time["sum"][0]>area_2hrs_departure["sum"][0]:
				area=1
			areaAtCurrentTime = area

		tgtAtCurrentHour = (endOfDayTgtBookedLoad - startOfDayTgtBookedLoad) * areaAtCurrentTime + startOfDayTgtBookedLoad
  
		self.logger.info("endOfDayTgtBookedLoad is: " + str(endOfDayTgtBookedLoad))

		return startEndOfDayDplfBand, bookingToday,bookingLastHour, endOfDayTgtBookedLoad, tgtAtCurrentHour, startOfDayBookLoad,startOfDayTgtBookedLoad

	def calculateVariance(self, currBookedLoad, totalCapacity, ndocounter, tgtAtCurrentHour, startOfDayBookLoad,startOfDayTgtBookedLoad):
		variance = ((currBookedLoad / totalCapacity) - (tgtAtCurrentHour / totalCapacity)) * 100
		if ndocounter >= 22:
			variance = ((currBookedLoad - (startOfDayTgtBookedLoad)) / totalCapacity) * 100
		return variance

	def calculateDplf(self, currBookedLoad, tgtBookloadFactor, totalCapacity):
		dPLF = (currBookedLoad/totalCapacity) - tgtBookloadFactor
		return dPLF

	def autoTimeRange(self, currFltDepartureTime):
		DaySpan = self.currParams.day_span
		input_time_str = currFltDepartureTime
		# This should be set according to your data
		time_bands, start_end_range = self.rmdatabasehelper.getDepartureTimeBands()
		time_range_start, time_range_end, dspan = self.find_time_range(input_time_str, time_bands, start_end_range)
		if int(self.currParams.auto_time_range_flag) == 1:
			self.currParams.time_window_start = time_range_start.strftime(TIME_FORMAT)
			self.currParams.time_window_end = time_range_end.strftime(TIME_FORMAT)
			DaySpan = dspan
		return DaySpan, input_time_str

	def checkTreshold(self, currFlt, ndocounter, plf):
		w9_thresholdValue, w8_thresholdValue, w7_thresholdValue, w6_thresholdValue, w5_thresholdValue, w4_thresholdValue, w3_thresholdValue, w2_thresholdValue, w1_thresholdValue, p_thresholdValue, l_thresholdValue, B2B_threshold = self.rmdatabasehelper.getPlfThreshhold(ndocounter, str(currFlt))
		
		self.w9_thresholdflag = False
		self.w8_thresholdflag = False
		self.w7_thresholdflag = False
		self.w6_thresholdflag = False
		self.w5_thresholdflag = False
		self.w4_thresholdflag = False
		self.w3_thresholdflag = False
		self.w2_thresholdflag = False
		self.w1_thresholdflag = False
		self.p_thresholdflag = False
		self.l_thresholdflag = False
		self.B2B_threshold = False
		
		if len(w9_thresholdValue) != 0:
			if w9_thresholdValue[0] is not None and w9_thresholdValue[0]!='':
				if plf > w9_thresholdValue[0]:
					self.w9_thresholdflag = True
		
		if len(w8_thresholdValue) != 0:
			if w8_thresholdValue[0] is not None and w8_thresholdValue[0]!='':
				if plf > w8_thresholdValue[0]:
					self.w8_thresholdflag = True
					
		if len(w7_thresholdValue) != 0:
			if w7_thresholdValue[0] is not None and w7_thresholdValue[0]!='':
				if plf > w7_thresholdValue[0]:
					self.w7_thresholdflag = True
					
		if len(w6_thresholdValue) != 0:
			if w6_thresholdValue[0] is not None and w6_thresholdValue[0]!='':
				if plf > w6_thresholdValue[0]:
					self.w6_thresholdflag = True
					
		if len(w5_thresholdValue) != 0:
			if w5_thresholdValue[0] is not None and w5_thresholdValue[0]!='':
				if plf > w5_thresholdValue[0]:
					self.w5_thresholdflag = True
					
		if len(w4_thresholdValue) != 0:
			if w4_thresholdValue[0] is not None and w4_thresholdValue[0]!='':
				if plf > w4_thresholdValue[0]:
					self.w4_thresholdflag = True
					
		if len(w3_thresholdValue) != 0:
			if w3_thresholdValue[0] is not None and w3_thresholdValue[0]!='':
				if plf > w3_thresholdValue[0]:
					self.w3_thresholdflag = True
					
		if len(w2_thresholdValue) != 0:
			if w2_thresholdValue[0] is not None and w2_thresholdValue[0]!='':
				if plf > w2_thresholdValue[0]:
					self.w2_thresholdflag = True
					
		if len(w1_thresholdValue) != 0:
			if w1_thresholdValue[0] is not None and w1_thresholdValue[0]!='':
				if plf > w1_thresholdValue[0]:
					self.w1_thresholdflag = True
					
		if len(p_thresholdValue) != 0:
			if p_thresholdValue[0] is not None and p_thresholdValue[0]!='':
				if plf > p_thresholdValue[0]:
					self.p_thresholdflag = True
					
		if len(l_thresholdValue) != 0:
			if l_thresholdValue[0] is not None and l_thresholdValue[0]!='':
				if plf > l_thresholdValue[0]:
					self.l_thresholdflag = True
					
		if len(B2B_threshold) != 0:
			if B2B_threshold[0] is not None and B2B_threshold[0]!='':
				if plf > B2B_threshold[0]:
					self.B2B_threshold = True

	def overBooking(self, currFltDate, ndocounter, totalCapacity, adjustedCapacity, plf, month, deptTime, currBookedLoad):
		b2boverbookingflag = False
		if self.inputSource.input_source[CONST_BOOKLOAD] == TEST_DATA:
			nextFlight = self.rmdatabasehelper.checkForNextFLight(currFltDate, self.currParams)
		if self.inputSource.input_source[CONST_BOOKLOAD] == DB:
			nextFlight = self.navdatabasehelper.checkForNextFLight(currFltDate, self.currParams)
		if self.currParams.obSeats == '' and self.currParams.obFare == '' or self.currParams.obSeats == None and self.currParams.obFare == None:
			self.logger.info("Entered system overbooking loop runalloc")
			overBookingCount = self.rmdatabasehelper.checkNowShowProb(self.currParams.origin+self.currParams.destin, self.constantsUtils.OVERBOOKING_PROB, month,deptTime)
			if self.canOverbook(ndocounter, totalCapacity, adjustedCapacity, plf, nextFlight,overBookingCount):
				totalCapacity = totalCapacity+overBookingCount
				if self.inputSource.input_source[CONST_OVERBOOKING] == DB:
					leg_key, nest_key, lid, nest_lid = self.navapihelper.get_leg_nest_details(currFltDate, AKASA_AIRLINE_CODE, self.currParams.flight_number, self.currParams.origin, self.currParams.destin)
					if not totalCapacity == lid:
						self.navapihelper.update_leg(leg_key, lid=totalCapacity)
					if not totalCapacity == nest_lid:
						self.navapihelper.update_leg_nest(leg_key, nest_key, totalCapacity)
				b2boverbookingflag = True
		else:
			self.logger.info("Entered manual overbooking loop runalloc")
			if self.canOverbookManual( ndocounter, totalCapacity, adjustedCapacity, plf, self.currParams.obSeats):
				if self.inputSource.input_source[CONST_OVERBOOKING] == DB:
					leg_key, nest_key, lid, nest_lid = self.navapihelper.get_leg_nest_details(currFltDate, AKASA_AIRLINE_CODE, self.currParams.flight_number, self.currParams.origin, self.currParams.destin)
					self.logger.info("Lid from nav is: "+ str(lid))
					lid = totalCapacity + int(self.currParams.obSeats)
					self.logger.info("Lid + OBSeats is: "+ str(lid))
					self.navapihelper.update_leg(leg_key, lid)
					self.navapihelper.update_leg_nest(leg_key, nest_key, lid)
					totalCapacity = lid
				else:
					start_date = datetime.strptime(self.currParams.per_start, "%m/%d/%Y").strftime('%Y-%m-%d')
					end_date = datetime.strptime(self.currParams.per_end, "%m/%d/%Y").strftime('%Y-%m-%d')
					bookload = self.rmdatabasehelper.getLid(self.currParams.origin, self.currParams.destin, start_date, end_date, self.currParams.flight_number)
					lid = bookload["Lid"][0] + int(self.currParams.obSeats)
					totalCapacity = lid
				b2boverbookingflag =True
		return totalCapacity, b2boverbookingflag

	def canOverbook(self, ndocounter, totalCapacity, adjustedCapacity, plf, nextFlight,overBookingCount):
		return int(self.currParams.over_booking) == 1 and nextFlight and plf > self.constantsUtils.OVERBOOKING_LF and self.constantsUtils.OVERBOOKING_END_NDO >= ndocounter >= self.constantsUtils.OVERBOOKING_START_NDO and int(adjustedCapacity) == int(totalCapacity) and overBookingCount>0
	
	def canOverbookManual(self, ndocounter, totalCapacity, adjustedCapacity, plf, overBookingCount):
		return int(self.currParams.over_booking) == 1 and plf > self.constantsUtils.OVERBOOKING_LF and self.constantsUtils.OVERBOOKING_END_NDO >= ndocounter >= self.constantsUtils.OVERBOOKING_START_NDO and int(adjustedCapacity) == int(totalCapacity) and float(overBookingCount)>0

	def outputResults(self):
		# self.logger.info("Final Allocation Results:")
		# self.logger.info(self.allocResults)
		if self.inputSource.output[CONST_AU_GRID] == FILE and len(self.AUFileData)!=0:
			AUlist = ['Type', 'Flight', 'StartDate', 'EndDate', 'DayOfWeek', 'Aircraft Type', 'Aircraft Suffix', 'Lid', 'Capacity', 'Status']
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
			AUFrame = AUFrame.drop(columns=["b2brunid", "b2crunid", "au_column_length"])
			AUFrame.columns = AUlist
			AUFrame.to_csv(CONST_AU_GRID_FILE_PATH, index=False)

		if self.inputSource.output[CONST_SUMMARY_FILE] == FILE:
			df = pd.DataFrame([vars(row) for row in self.allocResults])
			df = df.drop(columns=["Id"])
			df.to_csv(CONST_SUMMARY_FILE_PATH, index=False)

	def updateNavitaire(self, rec):
		auData = {"runId": self.runId, "type": rec.type, "flight": rec.flight, "startDate": rec.start_date, "endDate": rec.end_date, "dayOfWeek": rec.day_of_week, "aircraftType": rec.aircraft_type, "aircraftSuffix": rec.aircraft_suffix, "lid": rec.lid, "capacity": rec.capacity, "status": rec.status, "b2cRunId": rec.b2crunid, "b2bRunId": rec.b2brunid, "auClasses": {}}

		for i in range(self.AU_COLUMN_LENGTH):
			auClass = {"classOfService": getattr(rec, f"class_of_service_{i+1}", None), "classType": getattr(rec, f"class_type_{i+1}", None), "classNest": int(getattr(rec, f"class_nest_{i+1}", None)), "classRank": int(getattr(rec, f"class_rank_{i+1}", None)), "classAU": int(getattr(rec, f"class_au_{i+1}", None)), "classAllotted": int(getattr(rec, f"class_allotted_{i+1}", None)), "apRestriction": int(getattr(rec, f"ap_restriction_{i+1}", None))}
			auData['auClasses'][auClass['classOfService']] = auClass

		auDataStr = json.dumps(auData)
		# self.logger.info(auDataStr)
		# Send to queue for the market

		self.sqs.send_message(
			QueueUrl=self.RBD_QUEUE_URL,
			MessageBody=auDataStr
		)

	def updateNavSFTP(self, rec):
		flightNo = rec.flight.split(" ")[1][2:7]
		sector = rec.flight.split(" ")[2]
		flightDate = datetime.strptime(str(rec.start_date), DATE_FORMAT_3).date().strftime(DATE_FORMAT_2)
		auData = {"type": rec.type, "sector1": sector, "flightNo1": flightNo, "departureDate": flightDate, "auClasses": {}}
		auClass = {}
		for i in range(self.AU_COLUMN_LENGTH):
			auClass[getattr(rec, f"class_of_service_{i+1}", None)] = int(getattr(rec, f"class_au_{i+1}", None))

		auData['auClasses'] = auClass
		auDataStr = json.dumps(auData)
		self.rmdatabasehelper.insert_run_fltDate_audit(self.runId, flightNo, flightDate, auDataStr, "Ready", "Ready")

	def updateS3Push(self, rec):
		flightNo = rec.flight.split(" ")[1][2:7]
		flightDate = datetime.strptime(str(rec.start_date), DATE_FORMAT_3).date().strftime(DATE_FORMAT_2)
		records = vars(rec)

		auData = {"type": rec.type, "flight": rec.flight, "startDate": rec.start_date, "endDate": rec.end_date, "dayOfWeek": rec.day_of_week, "aircraftType": rec.aircraft_type, "aircraftSuffix": rec.aircraft_suffix, "lid": rec.lid, "capacity": rec.capacity, "status": rec.status, "auClasses": {}}

		for i in range(self.AU_COLUMN_LENGTH):
			auClass = {"classOfService": getattr(rec, f"class_of_service_{i+1}", None), "classType": getattr(rec, f"class_type_{i+1}", None), "classNest": int(getattr(rec, f"class_nest_{i+1}", None)), "classRank": int(getattr(rec, f"class_rank_{i+1}", None)), "classAU": int(getattr(rec, f"class_au_{i+1}", None)), "classAllotted": int(getattr(rec, f"class_allotted_{i+1}", None)), "apRestriction": int(getattr(rec, f"ap_restriction_{i+1}", None))}
			auData['auClasses'][auClass['classOfService']] = auClass

		auDataStr = json.dumps(auData)

		self.rmdatabasehelper.insert_run_fltDate_audit(self.runId, flightNo, flightDate, auDataStr, "Ready", "Ready")

	def getStartEndDate(self, currDate, startNDO, endNDO):
		startDate = datetime.strptime(startNDO, DATE_FORMAT).date()
		endDate = datetime.strptime(endNDO, DATE_FORMAT).date()
		# Reset the startNDO and endNDO to actually NDO
		startNDO = (startDate - currDate).days
		endNDO = (endDate - currDate).days

		return startDate, endDate, startNDO, endNDO

	def find_time_range(self, input_time_str, time_bands, start_end_range):
		input_time = datetime.strptime(input_time_str, TIME_FORMAT).time()
		date_today = self.timeutils.getCurrentDateTime()
		lower_time = (datetime.combine(date_today, input_time) - timedelta(hours=1, minutes=30)).time()
		upper_time = (datetime.combine(date_today, input_time) + timedelta(hours=1, minutes=30)).time()

		# Initialize the start and end times
		start_time = end_time = None
		dayspan = 0
		for start, end, time_band in time_bands:
			# Apply the logic based on the TimeBand value and the input time
			if start <= input_time <= end:
				if time_band == 1:
					if input_time < datetime.strptime(start_end_range[0][0], TIME_FORMAT).time():
						start_time = datetime.strptime(start_end_range[0][1], TIME_FORMAT).time()
						end_time = max(datetime.strptime(start_end_range[0][0], TIME_FORMAT).time(), upper_time)
						dayspan = -1
					else:
						start_time = min(datetime.strptime(start_end_range[0][1], TIME_FORMAT).time(), lower_time)
						end_time = datetime.strptime(start_end_range[0][0], TIME_FORMAT).time()
						dayspan = 1
					return start_time, end_time, dayspan
				else:
					start_time = start
					end_time = end
					return min(lower_time, start_time), max(upper_time, end_time), dayspan

	def _initialize_params(self, inputData, currParams):
		self.currParams = MarketListRow(currParams)
		self.inputSource = InputSource(inputData)
		self.debugMode = inputData["debugMode"]
		self.NavitaireUpdateMethod = self.currParams.navitaire_update_method
		self.TABLE_MARKET_FARES = TABLE_MARKET_FARES
		self.MARKET_LIST_QUEUE_URL = inputData["MARKET_LIST_QUEUE_URL"]
		self.RBD_QUEUE_URL = inputData["RBD_QUEUE_URL"]
		self.NAV_DB_NAME = inputData["NAV_DB_NAME"]
		self.NAV_DB_DW_NAME = inputData["NAV_DB_DW_NAME"]
		self.NAV_DB_WB_NAME = inputData["NAV_DB_WB_NAME"]
		self.NAV_DB_REF_NAME = inputData["NAV_DB_REF_NAME"]
		self.NAV_BASE_API_URL = inputData["NAV_BASE_API_URL"]
		self.NAV_OCP_SUBSCRIPTION_KEY = inputData["NAV_OCP_SUBSCRIPTION_KEY"]
		self.NAV_USERNAME = inputData["NAV_USERNAME"]
		self.NAV_PWD = inputData["NAV_PWD"]
		self.NAV_DOMAIN = inputData["NAV_DOMAIN"]

		self.sqs = self.currParams.sqs
		self.conn = self.currParams.dbconn
		self.wrconn = self.currParams.wrconn
		self.w0Flag = self.currParams.w0Flag
		self.cache_client = self.currParams.cache_client
		self.starttime = datetime.now()
		self.NavitaireConnection = self.currParams.navitaire_connection
		self.allocResults = []
		self.AUFileData = []
		if self.currParams.run_id:
			self.runId = self.currParams.run_id
		else:
			self.runId = self.utils.getUniqueId()
		self.log_time = self.currParams.time_logger
		if self.log_time is not None:
			self.log_time.update_run_id(self.currParams.run_id)
		self.logger = OwnLogger(self.runId, self.inputSource)
		self.rmdatabasehelper = RMDatabaseHelper(self.conn, self.wrconn, self.runId, self.inputSource, self.debugMode, self.cache_client,self.currParams)
		self.constantsUtils = ConstantsUtils(self.rmdatabasehelper)
		self.utils = Utils(self.inputSource)
		self.mathutils = MathUtils(self.inputSource)
		self.timeutils = TimeUtils(self.inputSource)
		self.navdatabasehelper = NavDatabaseHelper(self.NavitaireConnection, self.conn, self.inputSource, self.NAV_DB_NAME, self.NAV_DB_REF_NAME,self.runId, self.debugMode, self.NAV_DB_WB_NAME,self.currParams, self.log_time)
		self.redishelper = RedisHelper(self.runId, self.cache_client, self.inputSource)
		if self.inputSource.input_source[CONST_OVERBOOKING] == DB:
			self.navapihelper = NavAPIHelper(self.NAV_BASE_API_URL, self.NAV_OCP_SUBSCRIPTION_KEY, self.NAV_USERNAME, self.NAV_PWD, self.NAV_DOMAIN, self.runId, self.inputSource)
		self.dynamictimed2 = GetDynamicTimeD2(self.rmdatabasehelper)
		self.marketfaresservice = MarketFareService(self.rmdatabasehelper, self.navdatabasehelper,self.constantsUtils,self.timeutils)
		self.determinerbd = DetermineRBD(self.rmdatabasehelper, self.logger, self.inputSource, self.constantsUtils, self.navdatabasehelper, self.mathutils, self.cache_client)
		self.allocations = RBDAllocations(self.rmdatabasehelper, self.logger, self.constantsUtils, self.mathutils)

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

		if inputData["latestCode"]:
			code_version = self.rmdatabasehelper.getLatestCodeVersion()
			if code_version != LATEST_VERSION:
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

	def getBookloadSeries(self,dep_date,currBookedLoad):
		blocked_seats = self.rmdatabasehelper.getSeriesBlockedSeats(dep_date)
		if self.inputSource.input_source[CONST_LAST_SELLING_FARE] == TEST_DATA:
			booked_seats = self.rmdatabasehelper.get_series_seats(dep_date,self.currParams.flight_number,self.currParams.origin+self.currParams.destin)
		if self.inputSource.input_source[CONST_LAST_SELLING_FARE] == DB:
			booked_seats = self.navdatabasehelper.get_series_seats(dep_date,self.currParams.flight_number,self.currParams.origin+self.currParams.destin)
		if not booked_seats.empty:
			booked_seats['DepartureDate'] = booked_seats['DepartureDate'].astype(str)
			merged_df=pd.merge(blocked_seats, booked_seats, on=['DepartureDate', 'OrgCode'], how='left')
			merged_df['bookedPax'].fillna(0, inplace=True)
			first_row_values = booked_seats.iloc[0]
			for column in ['Origin', 'Destination', 'FlightNumber']:
				merged_df[column].fillna(first_row_values[column], inplace=True)
			merged_df['SeatsBlocked'] = merged_df['SeatsBlocked'].astype(int)
			merged_df['count'] = np.maximum(merged_df['SeatsBlocked'] - merged_df['bookedPax'], 0)
			total_count = merged_df['count'].sum()
		else:
			blocked_seats['SeatsBlocked'] = blocked_seats['SeatsBlocked'].astype(int)
			total_count = blocked_seats['SeatsBlocked'].sum()
		return currBookedLoad+total_count

	def recommend_action(self, row):
		try:
			def safe_float(value):
				if value is None or str(value).strip().upper() == 'NA':
					return 0.0
				return float(str(value).strip())

			mkt_fare_min = safe_float(row.MktFare_Min)
			backstop = safe_float(row.Backstop)
			variance = safe_float(row.Variance)
			selling_fare = safe_float(row.SellingFare)

			if mkt_fare_min < backstop:
				return "Revise Backstop"            
			elif variance > 12:
				if (mkt_fare_min - selling_fare) > 100:
					return "Push prices UP"
				else:
					return "NO action required"
			elif variance < -12:
				if (selling_fare - mkt_fare_min) > 100:
					return "Pull prices DOWN"
				else:
					return "NO action required"
			else:
				return "NO action required"
				
		except (ValueError, AttributeError, TypeError) as e:
			self.logger.error(f"Error in recommend_action: {str(e)}")
			return "NO action required"
		
  
	def determineFinalMF(self, dplfBand, currFltDate, DaySpan, input_time_str):
		min_10_b2c = 0
		min_10_b2b = 0
		final_mkt_fare_b2b = 0
		final_mkt_fare_b2c = 0
		if(dplfBand == 0):
			ab_start_time = "00:00"
			ab_end_time = "23:59"
			min_10_b2c=self.marketfaresservice.getMFinRange(self.currParams, currFltDate, B2C_CHANNEL, ab_start_time, ab_end_time, DaySpan, self.outputSummaryRow_b2c.criteria, input_time_str,dplfBand)
			min_10_b2b=self.marketfaresservice.getMFinRange(self.currParams, currFltDate, B2B_CHANNEL, ab_start_time, ab_end_time, DaySpan, self.outputSummaryRow_b2b.criteria, input_time_str,dplfBand)
			final_mkt_fare_b2b = min_10_b2b
			final_mkt_fare_b2c = min_10_b2c
		elif(dplfBand == 1 or dplfBand == 2):
			ab_start_time = "00:00"
			ab_end_time = "23:59"
			min_10_b2c=self.marketfaresservice.getMFinRange(self.currParams, currFltDate, B2C_CHANNEL, ab_start_time, ab_end_time, DaySpan, self.outputSummaryRow_b2c.criteria, input_time_str,dplfBand)
			min_10_b2b=self.marketfaresservice.getMFinRange(self.currParams, currFltDate, B2B_CHANNEL, ab_start_time, ab_end_time, DaySpan, self.outputSummaryRow_b2b.criteria, input_time_str,dplfBand)
			final_mkt_fare_b2b = min_10_b2b
			final_mkt_fare_b2c = min_10_b2c
		elif(dplfBand == 3):
			final_mkt_fare_b2b = self.outputSummaryRow_b2b.MktFare_Min
			final_mkt_fare_b2c = self.outputSummaryRow_b2c.MktFare_Min
		else:
			final_mkt_fare_b2b = min_10_b2b
			final_mkt_fare_b2c = min_10_b2c
   
		return final_mkt_fare_b2b, final_mkt_fare_b2c
    
	def auto_backstop(self, abf, backstop, dplfBand, mkt_fare):
		try:
			autoBackstopValue = 0

			def safe_float(value):
				if value is None or str(value).strip() == '' or str(value).strip().upper() == 'NA':
					return None
				try:
					return float(str(value).strip())
				except (ValueError, TypeError):
					return None

			autoBackstopFlag = str(abf)
			backstop = safe_float(backstop)
			mkt_fare = safe_float(mkt_fare)
			self.logger.info("AUTOBACKSTOP: Autobackstop Flag is: " + str(autoBackstopFlag))
			self.logger.info("AUTOBACKSTOP: Backstop is: " + str(backstop))
			self.logger.info("AUTOBACKSTOP: mkt_fare is: " + str(mkt_fare))
			self.logger.info("AUTOBACKSTOP: dplfBand is: " + str(dplfBand))
			try:
				dplfBand = int(str(dplfBand).strip())
			except (ValueError, TypeError, AttributeError):
				return 'NA'

			if autoBackstopFlag == '1':
				if dplfBand in [0, 1, 2, 3]:
					if None not in [backstop, mkt_fare] and mkt_fare < backstop and mkt_fare!=0:
						autoBackstopValue = mkt_fare - 50
						return autoBackstopValue
			return 'NA'

		except Exception as e:
			self.logger.error(f"Error in autoBackstop: {str(e)}")
			return 'NA'  # Changed from error message to 'NA' for consistency

	def calculate_last_seven_avg(self,fares):
		if not fares:
			return CONST_NA
		
		total_fares = sum(Decimal(str(fare['Fare'])) for fare in fares)
		return float(total_fares / len(fares))

	def calculate_average(self,fares):
		
		current_time = datetime.utcnow()
		recent_fares = [
			fare for fare in fares 
			if (current_time - datetime.fromisoformat(fare['BookingUTC'].replace('Z', ''))) <= timedelta(hours=self.constantsUtils.LAST_SOLD_FARE_THRESHOLD)
		]
		if len(recent_fares) < 3:
			return CONST_NA
		
		total_fares = sum(Decimal(str(fare['Fare'])) for fare in recent_fares)
		return float(total_fares / len(recent_fares))

	def calculate_averages(self,fares_manifest):
		# Parse the JSON string if it's not already a dictionary
		if isinstance(fares_manifest, str):
			fares_manifest = json.loads(fares_manifest)
		
		# Calculate last_seven_avg (always with all available fares)
		all_fares = fares_manifest.get('allfares', [])
		last_seven_avg = self.calculate_last_seven_avg(all_fares)
		
		# Calculate lastsoldb2b and lastsoldb2c (require at least 3 fares)
		b2b_fares = fares_manifest.get('b2bfares', [])
		b2c_fares = fares_manifest.get('b2cfares', [])
		
		lastsoldb2b = self.calculate_average(b2b_fares)
		lastsoldb2c = self.calculate_average(b2c_fares)
		
		return last_seven_avg, lastsoldb2b, lastsoldb2c