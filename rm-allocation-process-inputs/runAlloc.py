import ast
import copy
from decimal import Decimal
import math
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

from utilities.utils import Utils
from utilities.math_utils import MathUtils
from utilities.time_utils import TimeUtils
from constants_utils import ConstantsUtils

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
		bookedLoad = self.currParams.bookedPax

		try:
			# For each NDO
			for ndocounter in range(startNDO, endNDO+1):
				self.outputSummaryRow_b2c = OutputSummary()
				self.outputSummaryRow_b2c.w0Flag = self.w0Flag
				currDate = self.timeutils.getCurrentDate()
				tgtDate = datetime.strftime(currDate + timedelta(days=ndocounter), DATE_FORMAT_2)
				if pd.isna(bookedLoad):
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
					self.redishelper.updateRunAuditReady()
					break
				# bookedLoad['DepartureDate'] = pd.to_datetime(bookedLoad['DepartureDate'])
				# if FlightNumber == '*':
				# 	bookedLoadSingleRow = bookedLoad[(bookedLoad['DepartureDate'] == tgtDate)]
				# else:
				# 	bookedLoadSingleRow = bookedLoad[(bookedLoad['DepartureDate'] == tgtDate) & (bookedLoad['FlightNumber'] == FlightNumber) & (bookedLoad['Origin'] == origin) & (bookedLoad['Destination'] == destination)]
				# Fetch the current booked load from Navitaire
				# If for whatever reason - unable to get the booked Load - Abort
    
				tgtBookedLoad = None
				if pd.isna(bookedLoad):
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
					self.redishelper.updateRunAuditReady()
				else:
					tgtBookedLoadList = self.rmdatabasehelper.getTargetBookedLoadFromBenchmarkCurve(ndocounter, curveId)
					qpFares = self.rmdatabasehelper.get_qp_fares(origin, destination)
					TBFExist = self.rmdatabasehelper.get_TBF_fares(origin, destination)
					dateFormatted = datetime.strptime(str(tgtDate), DATE_FORMAT_2).date().strftime(DATE_FORMAT)
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
								self.outputSummaryRow_b2c.BookedLoad = bookedLoad
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
								self.redishelper.updateRunAuditReady()
							else:
								tgtBookedLoadPercent = float(tgtBookedLoadList['LF'][0])
								self.runDynamicAllocationSingleMarketSingleDay(ndocounter, bookedLoad, tgtBookedLoadPercent)
						else:
							self.logger.info("Unable to get QP Fares - Check the fares table")
							self.outputSummaryRow_b2c.Id = self.utils.getUniqueId()
							self.outputSummaryRow_b2c.RunId = self.runId
							self.outputSummaryRow_b2c.Origin = origin
							self.outputSummaryRow_b2c.Destin = destination
							self.outputSummaryRow_b2c.FltNum = str(FlightNumber)
							self.outputSummaryRow_b2c.DepDate = str(tgtDate)
							self.outputSummaryRow_b2c.BookedLoad = bookedLoad
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
							self.redishelper.updateRunAuditReady()
					else:
						# Cannot get a reference curve - Abort
						self.logger.info("Unable to get a Target - Check the booking curves")
						self.outputSummaryRow_b2c.Id = self.utils.getUniqueId()
						self.outputSummaryRow_b2c.RunId = self.runId
						self.outputSummaryRow_b2c.Origin = origin
						self.outputSummaryRow_b2c.Destin = destination
						self.outputSummaryRow_b2c.FltNum = str(FlightNumber)
						self.outputSummaryRow_b2c.DepDate = str(tgtDate)
						self.outputSummaryRow_b2c.BookedLoad = bookedLoad
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
						self.redishelper.updateRunAuditReady()
		except Exception as e:
			self.logger.info("Exception occurred")
			traceback.print_exc()
			self.logger.info(e)

		if not self.inputSource.isUpdateNavitaire:
			self.outputResults()

		# if self.inputSource.isUpdateNavitaire and (self.NavitaireUpdateMethod == CONST_SFTP or self.NavitaireUpdateMethod == S3):
		# 	self.redishelper.setAllRunStatus2Ready()

	def runDynamicAllocationSingleMarketSingleDay(self, ndocounter, bookedLoad, tgtBookedLoadPercent):
		currDate = self.timeutils.getCurrentDate()
		tgtDate = datetime.strftime(currDate + timedelta(days=ndocounter), DATE_FORMAT_2)
		dow = int(datetime.strftime(currDate + timedelta(days=ndocounter), WEEK_DAY))

		self.logger.info("Determining pricing for NDO " + str(ndocounter) + " on  " + str(tgtDate))
		self.logger.info("Target Pax Load Percent is :" + str(tgtBookedLoadPercent))
		ndoBookedLoad = bookedLoad

		numFlts = 1
		if self.debugMode:
			self.logger.info("Found " + str(numFlts) + " Flights")
			self.logger.info(ndoBookedLoad)

		for fltCounter in range(numFlts):
			currFlt = self.currParams.flight_number
			currBookedLoad = ndoBookedLoad
			currLid = self.currParams.capacity
			currAdjustedCapacity = self.currParams.adjustedCapacity
			currtbfBookedLoad = self.currParams.tbf_bookings
			currFltDate = self.currParams.per_start
			currFltDepartureTime = self.currParams.DepartureTime
			self.cug_bookings_dict = {
									'WA': self.currParams.WA_bookings if self.currParams.WA_bookings not in [None, 'NA', 'null'] else 0,
									'WB': self.currParams.WB_bookings if self.currParams.WB_bookings not in [None, 'NA', 'null'] else 0,
									'WC': self.currParams.WC_bookings if self.currParams.WC_bookings not in [None, 'NA', 'null'] else 0,
									'WD': self.currParams.WD_bookings if self.currParams.WD_bookings not in [None, 'NA', 'null'] else 0,
									'WE': self.currParams.WE_bookings if self.currParams.WE_bookings not in [None, 'NA', 'null'] else 0
								}

			self.runDynamicAllocationSingleFlight(currFlt, currBookedLoad, currLid, currFltDate, currFltDepartureTime, tgtBookedLoadPercent, dow, ndocounter, currAdjustedCapacity,currtbfBookedLoad)

	def runDynamicAllocationSingleFlight(self, currFlt, currBookedLoad, currLid, currFltDate, currFltDepartureTime,
										 tgtBookedLoadPercent, # Renamed from tgtBookloadPercent for consistency
										 dow, ndocounter, currAdjustedCapacity,currtbfBookedLoad):
		# currFltDate is an MM/DD/YYYY string
		# currFltDepartureTime is an HH:MM:SS string

		# --- Ensure outputSummaryRow objects are fresh for this flight-day ---
		self.outputSummaryRow_b2b = OutputSummary()
		self.outputSummaryRow_b2c = OutputSummary()

		current_w0Flag = 0 # Default
		# Safely get w0Flag from self, as it might be set in __init__ or earlier
		if hasattr(self, 'w0Flag') and pd.notna(self.w0Flag):
			try:
				current_w0Flag = int(self.w0Flag)
			except (ValueError, TypeError):
				self.logger.warning(f"Could not convert self.w0Flag ('{self.w0Flag}') to int. Defaulting to 0.")
		self.outputSummaryRow_b2c.w0Flag = current_w0Flag
		self.outputSummaryRow_b2b.w0Flag = current_w0Flag

		# --- Date Conversions (Essential) ---
		try:
			# DATE_FORMAT should be '%m/%d/%Y'
			currFltDate_dt = datetime.strptime(currFltDate, DATE_FORMAT)
			# DATE_FORMAT_2 should be '%Y-%m-%d'
			dateFormatted_yyyymmdd = currFltDate_dt.strftime(DATE_FORMAT_2)
		except ValueError as e:
			self.logger.error(f"CRITICAL: Could not parse currFltDate '{currFltDate}' with format '{DATE_FORMAT}'. Error: {e}. Aborting flight processing.")
			# Consider how to handle this error; for now, returning to prevent further issues.
			# You might want to populate error summary rows here before returning.
			return

		# --- Common Initializations (needed by both paths) ---
		initial_currBookedLoad = currBookedLoad # Preserve the value passed in

		# Actual PLF based on initial booked load and adjusted capacity
		self.actualplf = 0.0 # Default
		if pd.notna(currAdjustedCapacity) and currAdjustedCapacity > 0:
			try:
				self.actualplf = (float(initial_currBookedLoad) / float(currAdjustedCapacity)) * 100
			except (ValueError, TypeError):
				self.logger.warning("Could not calculate actualplf due to invalid numeric types for booked load or capacity.")

		# Adjust currBookedLoad for seriesBlock if applicable
		effective_series_block_flag = 0 # Default
		series_block_param = getattr(self.currParams, 'seriesBlock', '0') # Safely get, default to '0'
		if pd.notna(series_block_param):
			try:
				effective_series_block_flag = int(series_block_param)
			except (ValueError, TypeError):
				self.logger.warning(f"Invalid seriesBlock value: {series_block_param}. Assuming 0.")

		if effective_series_block_flag == 1:
			currBookedLoad = self.getBookloadSeries(dateFormatted_yyyymmdd, currBookedLoad) # getBookloadSeries needs YYYY-MM-DD

		self.logger.info(f"Effective Booked Load for Flt: {str(currFlt)} on Date: {str(currFltDate)} is: {str(currBookedLoad)}")

		# Current processing time (HH:MM:SS string) from helper
		self.currentTime = self.rmdatabasehelper.getCurrentTime(
			self.inputSource.currentTime, self.currParams, currFltDate, self.starttime # currFltDate is MM/DD/YYYY
		)

		# Capacities - ensure they are numeric
		base_totalCapacity = 0.0
		if pd.notna(currLid):
			try: base_totalCapacity = float(currLid)
			except (ValueError, TypeError): self.logger.warning(f"currLid '{currLid}' not convertible to float.")

		base_adjustedCapacity = 0.0
		if pd.notna(currAdjustedCapacity):
			try: base_adjustedCapacity = float(currAdjustedCapacity)
			except (ValueError, TypeError): self.logger.warning(f"currAdjustedCapacity '{currAdjustedCapacity}' not convertible to float.")


		# --- Overbooking Logic (affects totalCapacity, run before flag check) ---
		plf_before_ob = 0.0
		if base_adjustedCapacity > 0:
			plf_before_ob = (float(currBookedLoad) / base_adjustedCapacity) * 100
		elif base_totalCapacity > 0: # Fallback if adjustedCapacity is zero
			plf_before_ob = (float(currBookedLoad) / base_totalCapacity) * 100

		# self.overBooking needs YYYY-MM-DD date
		final_totalCapacity, self.b2boverbookingflag = self.overBooking(
			dateFormatted_yyyymmdd, ndocounter, base_totalCapacity, base_adjustedCapacity, plf_before_ob
		)
		self.logger.info(f"Total capacity/Lid after OB (if applicable) is: {final_totalCapacity}")

		# OverBookingCount relative to the original adjusted capacity
		overBookingCount = float(final_totalCapacity - base_adjustedCapacity)

		# --- PLF after potential overbooking (used for various thresholds) ---
		final_plf = 0.0 # Default
		if base_adjustedCapacity > 0:
			final_plf = (float(currBookedLoad) / base_adjustedCapacity) * 100
		elif final_totalCapacity > 0: # Use potentially updated totalCapacity if adjCap was 0
			final_plf = (float(currBookedLoad) / final_totalCapacity) * 100
		self.logger.info(f"PLF for downstream logic (based on adjCap mainly): {final_plf:.2f}%")

		# --- Start of Day Values & Distress Pax Count (needed by both paths) ---
		# getStartOfDayValues needs MM/DD/YYYY date AND tgtBookedLoadPercent
		sod_dplfBand, sod_bookingToday, sod_BookingsInHour, sod_eodTgtBookedLoad_abs, \
		sod_tgtAtCurrentHour_abs, sod_startOfDayBookLoad_factor, sod_startOfDayTgtBookedLoad_abs = \
			self.getStartOfDayValues(
				currBookedLoad, currFltDate, ndocounter,
				tgtBookedLoadPercent, # Passed parameter
				final_totalCapacity, currFltDepartureTime
			)

		distress_pax_count = round(sod_startOfDayTgtBookedLoad_abs - float(currBookedLoad))
		self.logger.info(f"Distress Pax Count (SOD_Target - CurrentBooked): {distress_pax_count}")

		# --- Date/Time strings for profileFaresExtract etc. (defined once, used by either path if needed) ---
		# These are always defined before the if/else split.
		# mappedMonth_df is used to potentially override month_str_for_profile
		mappedMonth_df = self.rmdatabasehelper.getMappedMonth(currFltDate) # currFltDate is MM/DD/YYYY

		try:
			# TIME_FORMAT should be '%H:%M:%S'
			time_object_dep = datetime.strptime(currFltDepartureTime, TIME_FORMAT).time()
			deptTime_str_hhmm = f"{time_object_dep.hour:02d}:{time_object_dep.minute:02d}"
		except ValueError as e:
			self.logger.error(f"CRITICAL: Could not parse currFltDepartureTime '{currFltDepartureTime}' with format '{TIME_FORMAT}'. Error: {e}. Using '00:00'.")
			deptTime_str_hhmm = "00:00" # Fallback

		# DAY_OF_WEEK should be '%a' (e.g., "Mon"), MONTH should be '%b' (e.g., "Jan")
		dow_str_for_profile = currFltDate_dt.strftime(DAY_OF_WEEK)
		month_str_for_profile = currFltDate_dt.strftime(MONTH)
		if not mappedMonth_df.empty and 'mappedMonth' in mappedMonth_df.columns and pd.notna(mappedMonth_df['mappedMonth'].iloc[0]):
			month_str_for_profile = mappedMonth_df['mappedMonth'].iloc[0]


		# ========================================================================
		# *** SALEINVFLAG Check: Determines which allocation path to take ***
		# ========================================================================
		

		if str(self.salesInv) == '1':
			self.logger.info(f"Running in Sales Inventory Only mode (Minimal AU Grid - W classes only) for Flt: {currFlt}, Date: {currFltDate}, NDO: {ndocounter}")
			sales_inv_target_rbd_names = ['W7','W6','W5','W4']
			self.AU_COLUMN_LENGTH = len(sales_inv_target_rbd_names)
			currRec_salesinv_minimal = AuGridRow(self.AU_COLUMN_LENGTH) # AU_COLUMN_LENGTH from _initialize_params
			currRec_salesinv_minimal.type = 'L'
			currRec_salesinv_minimal.flight = f' QP{str(currFlt)} {self.currParams.origin}{self.currParams.destin}'
			try:
				# DATE_FORMAT_3 e.g., '%Y%m%d' or '%d%m%Y'
				currRec_salesinv_minimal.start_date = currFltDate_dt.strftime(DATE_FORMAT_3)
				currRec_salesinv_minimal.end_date = currFltDate_dt.strftime(DATE_FORMAT_3)
			except ValueError as e:
				self.logger.error(f"Error formatting date for AU Grid with DATE_FORMAT_3 ('{DATE_FORMAT_3}'): {e}. Using YYYY-MM-DD.")
				currRec_salesinv_minimal.start_date = dateFormatted_yyyymmdd # Fallback
				currRec_salesinv_minimal.end_date = dateFormatted_yyyymmdd

			currRec_salesinv_minimal.day_of_week = 'Daily' # Standard valu # Standard Active status
			currRec_salesinv_minimal.capacity=currAdjustedCapacity
			currRec_salesinv_minimal.lid = currBookedLoad
			# Assign unique IDs for summary rows, link to AuGridRow if Navitaire update enabled
			self.outputSummaryRow_b2c.Id = self.utils.getUniqueId()
			self.outputSummaryRow_b2b.Id = self.utils.getUniqueId()

			current_nav_update_method = str(getattr(self, 'NavitaireUpdateMethod', '')) # Safely get and uppercase
			if self.inputSource.isUpdateNavitaire and current_nav_update_method != S3: # S3 is a constant
				currRec_salesinv_minimal.b2crunid = self.outputSummaryRow_b2c.Id
				currRec_salesinv_minimal.b2brunid = self.outputSummaryRow_b2b.Id

			
			 # The only RBDs to get actual allocation

			# Call sales_inv_allocation.
			# It should populate W7-W4 starting at index 0 in currRec_salesinv_minimal.
			try:
				# Assuming sales_inv_allocation handles brd_counter=-1 to mean "start at index 0 of currRec"
				# and sets all necessary attributes (class_of_service, class_au, type, nest, rank, allotted, ap_restriction)
				# for the sales_inv_target_rbd_names.
				self.allocations.sales_inv_allocation(
					distress_pax_count, # Already rounded
					1, # Indicates sales_inv_target_rbd_names start at currRec index 0
					currRec_salesinv_minimal,
					sales_inv_target_rbd_names,
					ndocounter,
					dateFormatted_yyyymmdd, # YYYY-MM-DD
					self.currParams.origin,
					self.currParams.destin
				)
    # Update count of filled slots
				self.logger.info(f"Sales inventory allocation applied for {sales_inv_target_rbd_names} in minimal AU grid.")
			except Exception as e:
				self.logger.error(f"Error during sales_inv_allocation in minimal AU Grid mode: {e}")
				# If error, _brd_idx_minimal_au might still be 0 or partially updated.
				# The loop below will fill remaining slots.


			self.AUFileData.append(currRec_salesinv_minimal) # Add to main list for later output
			# Update Navitaire/SFTP/S3 if enabled
			if self.inputSource.isUpdateNavitaire:
				if current_nav_update_method == CONST_API: self.updateNavitaire(currRec_salesinv_minimal)
				elif current_nav_update_method == CONST_SFTP: self.updateNavSFTP(currRec_salesinv_minimal)
				elif current_nav_update_method == S3: self.updateS3Push(currRec_salesinv_minimal)

			# --- Populate OutputSummaryRow for Sales Inventory Only (Minimal AU Grid) mode ---
			# Calculate variance for summary info (uses common variables already defined)
			variance_salesinv = self.calculateVariance(
				float(currBookedLoad), final_totalCapacity, ndocounter,
				sod_tgtAtCurrentHour_abs, sod_startOfDayBookLoad_factor, sod_startOfDayTgtBookedLoad_abs
			)
			current_ndo_band_salesinv = self.rmdatabasehelper.getNdoBand(ndocounter)
			current_eod_dplf_band_salesinv = self.rmdatabasehelper.getdpflBand(variance_salesinv)

			common_summary_attrs = {
				"RunId": self.runId, "Origin": self.currParams.origin, "Destin": self.currParams.destin,
				"FltNum": str(currFlt), "DepDate": str(currFltDate), # MM/DD/YYYY
				"BookedLoad": str(currBookedLoad),
				"TgtLoad": str(round(sod_startOfDayTgtBookedLoad_abs, 2)),
				"HowDetermined": "Sales Inventory Only (Minimal AU Grid - W classes only)",
				"OpenRBD": CONST_NA, "SellingFare": CONST_NA, "anchorFare": CONST_NA,
				"analystName": str(getattr(self.currParams, 'analyst_name', CONST_NA)),
				"allocationStatus": 1, # Mark as success for this specific mode
				"MktFare_Min": CONST_NA, "MktFare_Max": CONST_NA, "criteria": CONST_NA, "fareOffset": CONST_NA,
				"profileFare": CONST_NA, "strategyFare": CONST_NA, "upsell": CONST_NA,
				"Variance": str(round(variance_salesinv,2)),
				"bookedPlf": str(round(final_plf,2)), "overBookingCount": overBookingCount,
				"actualbookedPlf": round(self.actualplf,2),
				"currentTgtLoad": round(sod_tgtAtCurrentHour_abs,2),
				"currentLoadFactor": round((sod_tgtAtCurrentHour_abs / final_totalCapacity) * 100 if final_totalCapacity > 0 else 0, 2),
				"bookingToday": sod_bookingToday, "bookingLastHour": sod_BookingsInHour,
				"endOfDayTgtLoad": round(sod_eodTgtBookedLoad_abs,2),
				"dlfBand": sod_dplfBand, # Start of day DPLF band
				"ndoBand": str(current_ndo_band_salesinv),
				"endOfDaydlfBand": str(current_eod_dplf_band_salesinv) # End of day DPLF band (current variance)
			}

			for attr, value in common_summary_attrs.items():
				setattr(self.outputSummaryRow_b2c, attr, value)
				setattr(self.outputSummaryRow_b2b, attr, value)

			self.outputSummaryRow_b2c.Channel = B2C_CHANNEL
			self.outputSummaryRow_b2b.Channel = B2B_CHANNEL
			# IDs were already set for summary rows
			self.outputSummaryRow_b2c.recommendedActions = self.recommend_action(self.outputSummaryRow_b2c)
			self.outputSummaryRow_b2b.recommendedActions = self.recommend_action(self.outputSummaryRow_b2b)

			new_summary_rows_salesinv = [self.outputSummaryRow_b2c, self.outputSummaryRow_b2b]
			self.allocResults.extend(new_summary_rows_salesinv)
			# Database insert for summary rows
			if hasattr(self.rmdatabasehelper, 'insertSummaryRows'): # Ideal if this method exists
				self.rmdatabasehelper.insertSummaryRows(new_summary_rows_salesinv)
			else: # Fallback
				self.rmdatabasehelper.insertSummaryRow([self.outputSummaryRow_b2c])
				self.rmdatabasehelper.insertSummaryRow([self.outputSummaryRow_b2b])

			self.redishelper.updateRunAuditReady()
			return # IMPORTANT: End processing for this flight-day if saleinvflag == '1'

		# ========================================================================
		# *** ELSE: Full Dynamic Allocation Logic (self.currParams.saleinvflag != '1') ***
		# (This part remains as per your original full logic)
		# ========================================================================
		else: # effective_saleinvflag is '0' or other
			self.logger.info("Running in Full Dynamic Allocation mode.")

			# --- Start of Full Allocation Logic ---
			# All variables from the "Common Initializations" section are available here, including:
			# currBookedLoad, final_totalCapacity, base_adjustedCapacity, final_plf, overBookingCount,
			# sod_dplfBand, sod_bookingToday, sod_BookingsInHour, sod_eodTgtBookedLoad_abs,
			# sod_tgtAtCurrentHour_abs, sod_startOfDayBookLoad_factor, sod_startOfDayTgtBookedLoad_abs,
			# distress_pax_count,
			# dateFormatted_yyyymmdd, month_str_for_profile, dow_str_for_profile, deptTime_str_hhmm,
			# self.currentTime, self.actualplf
			# tgtBookedLoadPercent (parameter)

			param_dow_str = str(getattr(self.currParams, 'dow', '')) # Safely get dow string from params
			param_flight_number_str = str(getattr(self.currParams, 'flight_number', '')) # Safely get flight_number

			# DOW check: dow is an int (e.g., 0 for Mon, 1 for Tue if strftime('%w'), or 1-7 if strftime('%u'))
			# Ensure `param_dow_str` indexing is robust. `dow` should align with 0-based or 1-based indexing of this string.
			# Assuming `dow` is 0-6 and param_dow_str uses 1 at index 0 for Monday, index 1 for Tuesday, etc.
			# Original check: param_dow_str[dow:(dow+1)] == "1"
			# This requires `dow` to be the correct 0-based index for the day in the 7-char string.
			# Example: If dow is 0 (Monday), it checks param_dow_str[0:1]. If dow is 6 (Sunday), it checks param_dow_str[6:7].

			dow_condition_met = False
			if len(param_dow_str) == 7: # Ensure it's a 7-character string
				if 0 <= dow < 7: # Ensure dow is a valid index
					dow_condition_met = (param_dow_str[dow] == "1")
				else:
					self.logger.warning(f"Invalid DOW integer {dow} for indexing DOW string '{param_dow_str}'.")
			else:
				self.logger.warning(f"DOW string '{param_dow_str}' in params is not 7 characters long.")


			if ((str(currFlt) == param_flight_number_str) or (param_flight_number_str == '*')) and dow_condition_met:

				variance_full = self.calculateVariance(
					float(currBookedLoad), final_totalCapacity, ndocounter,
					sod_tgtAtCurrentHour_abs, sod_startOfDayBookLoad_factor, sod_startOfDayTgtBookedLoad_abs
				)
				self.logger.info(f"Variance (Full Mode): {variance_full:.2f}")

				ndoBand_full = self.rmdatabasehelper.getNdoBand(ndocounter)
				dplfBand_full = self.rmdatabasehelper.getdpflBand(variance_full)

				# Populate summary row fields specific to full allocation
				self.outputSummaryRow_b2c.currentTgtLoad = round(sod_tgtAtCurrentHour_abs,2)
				self.outputSummaryRow_b2b.currentTgtLoad = round(sod_tgtAtCurrentHour_abs,2)
				currentLoadFactor_val = (sod_tgtAtCurrentHour_abs / final_totalCapacity) * 100 if final_totalCapacity > 0 else 0.0
				self.outputSummaryRow_b2c.currentLoadFactor = round(currentLoadFactor_val,2)
				self.outputSummaryRow_b2b.currentLoadFactor = round(currentLoadFactor_val,2)
				self.outputSummaryRow_b2c.bookingToday = sod_bookingToday
				self.outputSummaryRow_b2b.bookingToday = sod_bookingToday
				self.outputSummaryRow_b2c.bookingLastHour = sod_BookingsInHour
				self.outputSummaryRow_b2b.bookingLastHour = sod_BookingsInHour
				self.outputSummaryRow_b2c.endOfDayTgtLoad = round(sod_eodTgtBookedLoad_abs,2)
				self.outputSummaryRow_b2b.endOfDayTgtLoad = round(sod_eodTgtBookedLoad_abs,2)
				self.outputSummaryRow_b2c.dlfBand = sod_dplfBand # Start of day DPLF band
				self.outputSummaryRow_b2b.dlfBand = sod_dplfBand
				self.outputSummaryRow_b2c.endOfDaydlfBand = dplfBand_full # End of day DPLF band (from current variance)
				self.outputSummaryRow_b2b.endOfDaydlfBand = dplfBand_full

				self.checkTreshold(currFlt, ndocounter, final_plf) # Sets self.wX_thresholdflag, etc.

				DaySpan_full, input_time_str_dep_full = self.autoTimeRange(currFltDepartureTime) # currFltDepartureTime is HH:MM:SS

				# Get Dynamic D1 (Time Window)
				self.outputSummaryRow_b2b.startTime, self.outputSummaryRow_b2b.endTime, self.outputSummaryRow_b2b.TimeRange, hardstartTimeB2B, hardendTimeB2B = \
					self.dynamictimed2.getDynamicTime(self.currParams, dplfBand_full, ndoBand_full, DaySpan_full, B2B_CHANNEL, final_plf)
				self.outputSummaryRow_b2c.startTime, self.outputSummaryRow_b2c.endTime, self.outputSummaryRow_b2c.TimeRange, hardstartTimeB2C, hardendTimeB2C = \
					self.dynamictimed2.getDynamicTime(self.currParams, dplfBand_full, ndoBand_full, DaySpan_full, B2C_CHANNEL, final_plf) # Typo was c2c

				# Get Dynamic D2 (Criteria, Offset, StrategyRef)
				self.outputSummaryRow_b2b.criteria, hardcriteriaB2B, self.outputSummaryRow_b2b.fareOffset, hardoffsetB2B, self.outputSummaryRow_b2b.StrategyReference, hardfareb2b = \
					self.dynamictimed2.getDynamicD2Value(self.currParams, dplfBand_full, ndoBand_full, B2B_CHANNEL, final_plf)
				self.outputSummaryRow_b2c.criteria, hardcriteriaB2C, self.outputSummaryRow_b2c.fareOffset, hardoffsetB2C, self.outputSummaryRow_b2c.StrategyReference, hardfareb2c = \
					self.dynamictimed2.getDynamicD2Value(self.currParams, dplfBand_full, ndoBand_full, B2C_CHANNEL, final_plf)

				# IDs for summary rows (already set in common init, but ensure unique if re-running parts)
				self.outputSummaryRow_b2b.Id = self.utils.getUniqueId()
				self.outputSummaryRow_b2c.Id = self.utils.getUniqueId()

				self.logger.info("Getting market Fares (Full Mode)")
				# getMarketFareAnchorFare expects currFltDate as MM/DD/YYYY
				mktfare_params_b2c = {
					"currParams": self.currParams, "currFltDate": currFltDate, "channel": B2C_CHANNEL,
					"startTime": self.outputSummaryRow_b2c.startTime, "endTime": self.outputSummaryRow_b2c.endTime,
					"DaySpan": DaySpan_full, "criteria": self.outputSummaryRow_b2c.criteria,
					"flightTime": input_time_str_dep_full, "hardcriteria": hardcriteriaB2C,
					"hardfare": hardfareb2c, "hardstartTime": hardstartTimeB2C, "hardendTime": hardendTimeB2C
				}
    
				self.outputSummaryRow_b2c.MktFare_Min, self.outputSummaryRow_b2c.MktFare_Max, \
				self.outputSummaryRow_b2c.AirlineCode_Min, self.outputSummaryRow_b2c.AirlineCode_Max, \
				self.outputSummaryRow_b2c.FlightNumber_Min, self.outputSummaryRow_b2c.FlightNumber_Max, \
				isB2CMarketFareAvailable, self.outputSummaryRow_b2c.criteria, \
				self.outputSummaryRow_b2c.startTime, self.outputSummaryRow_b2c.endTime, \
				hardfareb2c_updated,lowmktFileb2c,himktFileb2c = self.marketfaresservice.getMarketFareAnchorFare(**mktfare_params_b2c)
				hardfareb2c = hardfareb2c_updated # Update local var if function modifiesit    
				mktfare_params_b2b = {
					"currParams": self.currParams, "currFltDate": currFltDate, "channel": B2B_CHANNEL,
					"startTime": self.outputSummaryRow_b2b.startTime, "endTime": self.outputSummaryRow_b2b.endTime,
					"DaySpan": DaySpan_full, "criteria": self.outputSummaryRow_b2b.criteria,
					"flightTime": input_time_str_dep_full, "hardcriteria": hardcriteriaB2B,
					"hardfare": hardfareb2b, "hardstartTime": hardstartTimeB2B, "hardendTime": hardendTimeB2B
				}
    
				self.outputSummaryRow_b2b.MktFare_Min, self.outputSummaryRow_b2b.MktFare_Max, \
				self.outputSummaryRow_b2b.AirlineCode_Min, self.outputSummaryRow_b2b.AirlineCode_Max, \
				self.outputSummaryRow_b2b.FlightNumber_Min, self.outputSummaryRow_b2b.FlightNumber_Max, \
				isB2BMarketFareAvailable, self.outputSummaryRow_b2b.criteria, \
				self.outputSummaryRow_b2b.startTime, self.outputSummaryRow_b2b.endTime, \
				hardfareb2b_updated,lowmktFileb2b,himktFileb2b = self.marketfaresservice.getMarketFareAnchorFare(**mktfare_params_b2b)
				hardfareb2b = hardfareb2b_updated # Update local var

				# Profile Fare Quartile calculation
				actualPercentile_full = final_totalCapacity / PROFILE_FARES_QUARTILE if PROFILE_FARES_QUARTILE != 0 else final_totalCapacity # Avoid DivByZero
				bookloadPercentile_full = self.mathutils.getCeilNumber(float(currBookedLoad) / actualPercentile_full) if actualPercentile_full != 0 else PROFILE_FARES_QUARTILE # Avoid DivByZero
				
				if bookloadPercentile_full > PROFILE_FARES_QUARTILE: bookloadPercentile_full = PROFILE_FARES_QUARTILE
				if bookloadPercentile_full < PROFILE_FARES_25PERCENT_QUARTILE: bookloadPercentile_full = PROFILE_FARES_25PERCENT_QUARTILE
				percentile_str_full = f'Q{str(int(bookloadPercentile_full))}'

				# Upsell Value
				sector_full = self.currParams.origin + self.currParams.destin
				upsellValueData_full = self.rmdatabasehelper.getUpsellValue(currFltDate, sector_full) # currFltDate is MM/DD/YYYY
				upsellValue_full = 0.0
				if not upsellValueData_full.empty and 'percentage' in upsellValueData_full.columns and pd.notna(upsellValueData_full.percentage.iloc[0]):
					try: upsellValue_full = float(upsellValueData_full.percentage.iloc[0])
					except (ValueError, TypeError): self.logger.warning("Could not convert upsell percentage to float.")


				# Backstop Logic
				b2c_backstop_final = getattr(self.currParams, 'b2c_backstop', CONST_NA) # Safely get
				b2b_backstop_final = getattr(self.currParams, 'b2b_backstop', CONST_NA) # Safely get
				auto_b2b_backstop_str = "NA"
				auto_b2c_backstop_str = "NA"
				
				effective_autoBackstopFlag = str(getattr(self.currParams, 'autoBackstopFlag', '0')) # Safely get
				if effective_autoBackstopFlag == "1":
					# determineFinalMF needs MM/DD/YYYY date
					final_mkt_fare_b2b_for_ab, final_mkt_fare_b2c_for_ab = self.determineFinalMF(dplfBand_full, currFltDate, DaySpan_full, input_time_str_dep_full)
					auto_b2b_backstop_str = self.auto_backstop(effective_autoBackstopFlag, b2b_backstop_final, dplfBand_full, final_mkt_fare_b2b_for_ab)
					auto_b2c_backstop_str = self.auto_backstop(effective_autoBackstopFlag, b2c_backstop_final, dplfBand_full, final_mkt_fare_b2c_for_ab)
					if auto_b2c_backstop_str != "NA": b2c_backstop_final = auto_b2c_backstop_str
					if auto_b2b_backstop_str != "NA": b2b_backstop_final = auto_b2b_backstop_str
				
				# Last Selling Fare
				lastSoldFareb2c_val = CONST_NA; lastSoldFareb2b_val = CONST_NA; last_seven_avg_val = CONST_NA
				input_source_lsf = self.inputSource.input_source.get(CONST_LAST_SELLING_FARE, TEST_DATA) # Default to TEST_DATA if not set

				if input_source_lsf == TEST_DATA:
					farePrice_data = self.rmdatabasehelper.getLastSellingFare(currFltDate, self.currParams) # currFltDate is MM/DD/YYYY
					if not farePrice_data.empty:
						if "B2C_last3avg_fare" in farePrice_data.columns: lastSoldFareb2c_val = farePrice_data["B2C_last3avg_fare"].iloc[0]
						if "B2B_last3avg_fare" in farePrice_data.columns: lastSoldFareb2b_val = farePrice_data["B2B_last3avg_fare"].iloc[0]
						if "last7avg" in farePrice_data.columns: last_seven_avg_val = farePrice_data["last7avg"].iloc[0]
				elif input_source_lsf == DB:
					redis_key_lsf = f"LastSoldFare:{self.currParams.origin}{self.currParams.destin}{param_flight_number_str}_{currFltDate_dt.date()}"
					if self.cache_client.exists(redis_key_lsf):
						fare_manifest_str = self.cache_client.get(redis_key_lsf).decode('utf-8')
						last_seven_avg_val, lastSoldFareb2b_val, lastSoldFareb2c_val = self.calculate_averages(fare_manifest_str)
				
				# Determine RBD to Open
				b2b_discount_fare_active = False; current_discount_amount = 0.0; determined_overBookingFare = 0.0
				openDataB2C_details = {}; openDataB2B_details = {} # To store results from determineRBDToOpen
				
				param_obFare = getattr(self.currParams, 'obFare', None) # Get obFare safely
				param_b2c_tolerance = getattr(self.currParams, 'b2c_tolerance', 0) # Default tolerance if not set
				param_b2b_tolerance = getattr(self.currParams, 'b2b_tolerance', 0) # Default tolerance

				mkt_fare_flag = int(self.currParams.mkt_fare_flag)

				# Determine if we should process (enter the main block)
				should_process = False
				if mkt_fare_flag == 1:
					# Flag = 1: Original behavior - process if either market fare OR last_seven_avg available
					should_process = isB2CMarketFareAvailable or (pd.notna(last_seven_avg_val) and last_seven_avg_val != CONST_NA)
				elif mkt_fare_flag == 0:
					# Flag = 0: Only process if last_seven_avg is available
					should_process = pd.notna(last_seven_avg_val) and last_seven_avg_val != CONST_NA

				if should_process:
					self.outputSummaryRow_b2c.lastSevenAvgFare = last_seven_avg_val
					
					is_last7avg_anchor = False
					# Set B2C Anchor Fare based on criteria or L7D Avg
					if self.outputSummaryRow_b2c.criteria in [MAX_FARE_ANCHOR, MAXO_FARE_ANCHOR]:
						self.outputSummaryRow_b2c.anchorFare = self.outputSummaryRow_b2c.MktFare_Max
						self.outputSummaryRow_b2c.mktFile = himktFileb2c
					elif self.outputSummaryRow_b2c.criteria in [MIN_FARE_ANCHOR, MINO_FARE_ANCHOR]:
						self.outputSummaryRow_b2c.anchorFare = self.outputSummaryRow_b2c.MktFare_Min
						self.outputSummaryRow_b2c.mktFile = lowmktFileb2c
					
					# Determine when to override with last_seven_avg
					if mkt_fare_flag == 0:
						# Flag = 0: Always use last_seven_avg if available
						if pd.notna(last_seven_avg_val) and last_seven_avg_val != CONST_NA:
							self.outputSummaryRow_b2c.anchorFare = last_seven_avg_val
							is_last7avg_anchor = True
					elif mkt_fare_flag == 1:
						# Flag = 1: Original behavior - use last_seven_avg only if market fare not available
						if not isB2CMarketFareAvailable and (pd.notna(last_seven_avg_val) and last_seven_avg_val != CONST_NA):
							self.outputSummaryRow_b2c.anchorFare = last_seven_avg_val
							is_last7avg_anchor = True
					
					if is_last7avg_anchor: 
						self.outputSummaryRow_b2c.fareOffset = 0  # No offset if L7D avg is anchor
					
					# determineRBDToOpen needs YYYY-MM-DD date
					openDataB2C_details, self.outputSummaryRow_b2c.lastSellingFare, self.outputSummaryRow_b2c.strategyFare, \
					self.outputSummaryRow_b2c.profileFare, self.outputSummaryRow_b2c.hardAnchorFare, self.outputSummaryRow_b2c.upsell, \
					determined_overBookingFare = self.determinerbd.determineRBDToOpen(
													self.outputSummaryRow_b2c.anchorFare, self.outputSummaryRow_b2c.fareOffset, B2C_CHANNEL, 
													dateFormatted_yyyymmdd, dplfBand_full, ndocounter, float(currBookedLoad), final_totalCapacity, self.currParams, 
													b2c_backstop_final, self.currentTime, sod_dplfBand, variance_full, month_str_for_profile, deptTime_str_hhmm, 
													dow_str_for_profile, percentile_str_full, upsellValue_full, param_b2c_tolerance, final_plf,
													overBookingCount, hardfareb2c, hardoffsetB2C, sod_BookingsInHour, lastSoldFareb2c_val, 
													is_last7avg_anchor, b2b_discount_fare_active, current_discount_amount, current_w0Flag, param_obFare
												)

					# Get B2C Opening Price for potential B2B derivation
					B2C_opening_price_val = None
					# Check if 'Fare' key exists and the list is not empty before accessing iloc[0]
					if 'Fare' in openDataB2C_details and isinstance(openDataB2C_details['Fare'], list) and openDataB2C_details['Fare'] and openDataB2C_details['Fare'][0] != CONST_NA:
						try: 
							B2C_opening_price_val = float(openDataB2C_details['Fare'][0])
						except (ValueError, TypeError): 
							self.logger.warning(f"Could not parse B2C opening fare '{openDataB2C_details['Fare'][0]}' to float.")
					
					# B2B RBD determination
					if isB2BMarketFareAvailable:
						if self.outputSummaryRow_b2b.criteria in [MAX_FARE_ANCHOR, MAXO_FARE_ANCHOR]: 
							self.outputSummaryRow_b2b.anchorFare = self.outputSummaryRow_b2b.MktFare_Max
							self.outputSummaryRow_b2b.mktFile = himktFileb2b
						elif self.outputSummaryRow_b2b.criteria in [MIN_FARE_ANCHOR, MINO_FARE_ANCHOR]: 
							self.outputSummaryRow_b2b.anchorFare = self.outputSummaryRow_b2b.MktFare_Min
							self.outputSummaryRow_b2b.mktFile = lowmktFileb2b
						elif pd.notna(self.outputSummaryRow_b2b.MktFare_Min): 
							self.outputSummaryRow_b2b.anchorFare = self.outputSummaryRow_b2b.MktFare_Min  # Default
						
						openDataB2B_details, self.outputSummaryRow_b2b.lastSellingFare, self.outputSummaryRow_b2b.strategyFare, \
						self.outputSummaryRow_b2b.profileFare, self.outputSummaryRow_b2b.hardAnchorFare, self.outputSummaryRow_b2b.upsell, _  = \
							self.determinerbd.determineRBDToOpen(
								self.outputSummaryRow_b2b.anchorFare, self.outputSummaryRow_b2b.fareOffset, B2B_CHANNEL, 
								dateFormatted_yyyymmdd, dplfBand_full, ndocounter, float(currBookedLoad), final_totalCapacity, self.currParams, 
								b2b_backstop_final, self.currentTime, sod_dplfBand, variance_full, month_str_for_profile, deptTime_str_hhmm, 
								dow_str_for_profile, percentile_str_full, upsellValue_full, param_b2b_tolerance, final_plf,
								overBookingCount, hardfareb2b, hardoffsetB2B, sod_BookingsInHour, lastSoldFareb2b_val,
								False, b2b_discount_fare_active, current_discount_amount, current_w0Flag, param_obFare 
							)
					elif B2C_opening_price_val is not None and not self.B2B_threshold and 0 <= dplfBand_full <= 3:
						b2b_discount_fare_active = True 
						discount_map_config = ast.literal_eval(self.constantsUtils.B2B_DISCOUNT_MAP)  # B2B_DISCOUNT_MAP from ConstantsUtils
						discount_str_val = discount_map_config.get(str(dplfBand_full), "0") 
						
						if '%' in discount_str_val: 
							current_discount_amount = B2C_opening_price_val * (float(discount_str_val.strip('%')) / 100.0)
						else: 
							current_discount_amount = float(discount_str_val)
						
						derived_b2b_anchor_fare = B2C_opening_price_val - current_discount_amount
						self.outputSummaryRow_b2b.anchorFare = derived_b2b_anchor_fare
						self.outputSummaryRow_b2b.fareOffset = 0 

						openDataB2B_details, self.outputSummaryRow_b2b.lastSellingFare, self.outputSummaryRow_b2b.strategyFare, \
						self.outputSummaryRow_b2b.profileFare, self.outputSummaryRow_b2b.hardAnchorFare,self.outputSummaryRow_b2b.upsell, _ = \
							self.determinerbd.determineRBDToOpen(
								derived_b2b_anchor_fare, self.outputSummaryRow_b2b.fareOffset, B2B_CHANNEL, 
								dateFormatted_yyyymmdd, dplfBand_full, ndocounter, float(currBookedLoad), final_totalCapacity, self.currParams, 
								b2b_backstop_final, self.currentTime, sod_dplfBand, variance_full, month_str_for_profile, deptTime_str_hhmm, 
								dow_str_for_profile, percentile_str_full, upsellValue_full, param_b2b_tolerance,final_plf,
								overBookingCount, hardfareb2b,hardoffsetB2B,sod_BookingsInHour,lastSoldFareb2b_val,
								False, b2b_discount_fare_active, current_discount_amount, current_w0Flag, param_obFare
							) 
					else: 
						openDataB2B_details = {'RBD': [CONST_NA], 'Fare': [CONST_NA], 'FareResult': ['B2B Mkt Data NA & Not Derivable'], 'anchorFare': [CONST_NA], 'statusInd': [3], 'allocationStatus': [0]}
				else: 
					openDataB2C_details = {'RBD': [CONST_NA], 'Fare': [CONST_NA], 'FareResult': ['B2C Mkt Data & L7D Avg NA'], 'anchorFare': [CONST_NA], 'statusInd': [1], 'allocationStatus': [0]}
					openDataB2B_details = {'RBD': [CONST_NA], 'Fare': [CONST_NA], 'FareResult': ['B2C Mkt Data NA - Skip B2B'], 'anchorFare': [CONST_NA], 'statusInd': [3], 'allocationStatus': [0]}
					self.redishelper.updateRunAudit(); self.redishelper.updateRunAuditReady()

				# --- AU Grid Allocation (Full Mode) ---
				currRec_fullalloc = AuGridRow(self.AU_COLUMN_LENGTH) # AU_COLUMN_LENGTH from _initialize_params
				currRec_fullalloc.type = 'L'; currRec_fullalloc.flight = f' QP{str(currFlt)} {self.currParams.origin}{self.currParams.destin}'
				try:
					currRec_fullalloc.start_date = currFltDate_dt.strftime(DATE_FORMAT_3)
					currRec_fullalloc.end_date = currFltDate_dt.strftime(DATE_FORMAT_3)
				except ValueError: 
					currRec_fullalloc.start_date = dateFormatted_yyyymmdd 
					currRec_fullalloc.end_date = dateFormatted_yyyymmdd
				currRec_fullalloc.day_of_week = 'Daily'

				current_nav_update_method_full = str(getattr(self, 'NavitaireUpdateMethod', ''))
				if self.inputSource.isUpdateNavitaire and current_nav_update_method_full != S3:
					currRec_fullalloc.b2crunid = self.outputSummaryRow_b2c.Id
					currRec_fullalloc.b2brunid = self.outputSummaryRow_b2b.Id

				# Profile Fares list for allocations
				profileExtract_df_fullalloc = self.rmdatabasehelper.profileFaresExtract(
					self.currParams.origin, self.currParams.destin, month_str_for_profile, dow_str_for_profile, deptTime_str_hhmm
				)
				profileFares_list_sorted_fullalloc = []
				if not profileExtract_df_fullalloc.empty:
					profileFares_list_sorted_fullalloc = sorted(profileExtract_df_fullalloc.transpose().values.flatten().tolist())

				# Proceed with AU allocation if B2C determination was somewhat successful
				# Check if 'statusInd' key exists before trying to access its value
				b2c_status_ind = openDataB2C_details.get('statusInd', [1])[0] if isinstance(openDataB2C_details.get('statusInd'), list) and openDataB2C_details.get('statusInd') else 1

				if b2c_status_ind in [0, 2]:
					# Booked deficit for B2C allocation (target based on original Lid from params)
					bookedDeficit_val = round((tgtBookedLoadPercent * base_totalCapacity) - float(currBookedLoad))
					remainingCapacity_val = final_totalCapacity - float(currBookedLoad)

					b2cAllocUnits_val, brd_counter_after_b2c, b2c_status_val, _, B2CRBDs_list_val, b2c_opening_price_val = \
						self.allocations.b2CAllocation(
							float(initial_currBookedLoad), openDataB2C_details, currRec_fullalloc, final_totalCapacity, bookedDeficit_val, variance_full,
							remainingCapacity_val, ndocounter, self.currParams, self.outputSummaryRow_b2c.profileFare,
							profileFares_list_sorted_fullalloc, bookloadPercentile_full, upsellValue_full,
							determined_overBookingFare, base_adjustedCapacity
						)

					# This call also modifies currRec_fullalloc, for TBF.
					alloc_df_for_tbf = self.allocations.b2CAllocations(
						float(currBookedLoad), openDataB2C_details, currRec_fullalloc, final_totalCapacity, bookedDeficit_val, variance_full,
						remainingCapacity_val, ndocounter, self.currParams, self.outputSummaryRow_b2c.profileFare,
						profileFares_list_sorted_fullalloc, bookloadPercentile_full, upsellValue_full, b2c_opening_price_val, # Use opening price from b2CAllocation
						determined_overBookingFare, base_adjustedCapacity
					)

					self.outputSummaryRow_b2c.status = b2c_status_val
					self.outputSummaryRow_b2b.status = b2c_status_val # Mirror B2C status initially

					# B2B Allocation
					isUpsell_b2b = False; skip_b2b_factor = False
					current_brd_counter = brd_counter_after_b2c

					# B2B Z0 fare check vs backstop
					b2b_Z0_rbds_df = self.rmdatabasehelper.fareExtract(self.currParams.origin, self.currParams.destin, B2B_CHANNEL, 'Z0', 'L')
					b2b_Z0_fare = b2b_Z0_rbds_df['Total'].iloc[0] if not b2b_Z0_rbds_df.empty and 'Total' in b2b_Z0_rbds_df.columns else float('inf')

					if pd.notna(b2b_backstop_final) and b2b_backstop_final != CONST_NA and float(b2b_Z0_fare) < float(b2b_backstop_final):
						openDataB2B_details = {'RBD': [CONST_NA], 'Fare': [CONST_NA], 'FareResult': ['B2B: Z0 fare < B2B backstop'], 'anchorFare': [CONST_NA], 'statusInd': [1], 'allocationStatus': [1]}
					
					param_b2b_factor = getattr(self.currParams, 'b2b_factor', 0) # Default if not present
					if pd.notna(param_b2b_factor) and float(param_b2b_factor) == -1:
						skip_b2b_factor = True

					isUpsell_b2b, current_brd_counter = self.allocations.b2BAllocation(
						float(initial_currBookedLoad), openDataB2B_details, currRec_fullalloc, final_totalCapacity, variance_full, b2cAllocUnits_val,
						skip_b2b_factor, self.b2boverbookingflag, current_brd_counter, openDataB2C_details,
						self.currParams, overBookingCount
					)

					if isUpsell_b2b: openDataB2B_details = {'RBD': [CONST_NA], 'Fare': [CONST_NA], 'FareResult': ['B2B skipped - Flight in Upsell'], 'anchorFare': [CONST_NA], 'statusInd': [1], 'allocationStatus': [1]}
					if skip_b2b_factor: openDataB2B_details = {'RBD': [CONST_NA], 'Fare': [CONST_NA], 'FareResult': ['B2B skipped - B2B Factor is -1'], 'anchorFare': [CONST_NA], 'statusInd': [1], 'allocationStatus': [1]}

					# L, P, W, CUG, GQ, TBF allocations
					# Pass YYYY-MM-DD date to L, P allocations
					self.l_rbd_allocation(dateFormatted_yyyymmdd, ndoBand_full, dplfBand_full, currRec_fullalloc, current_brd_counter); 
					self.p_rbd_allocation(dateFormatted_yyyymmdd, ndoBand_full, dplfBand_full, currRec_fullalloc, current_brd_counter); 

					current_brd_counter = self.w_rbd_allocation(currRec_fullalloc, current_brd_counter) # Uses final_plf

					# CUG allocation
					B2CRBDs_df_for_cug_gq_tbf = self.rmdatabasehelper.fareExtract(self.currParams.origin, self.currParams.destin, B2C_CHANNEL, '', 'L')
					cug_bookings_dict_local = getattr(self, 'cug_bookings_dict', {}) # Use if it's a class member, else ensure defined

					param_cug_flag = getattr(self.currParams, 'cug_flag', 0) # Default
					param_cug_mkt_flag = getattr(self.currParams, 'cug_mkt_flag', 0) # Default

					current_brd_counter = self.allocations.cug_allocation(
						current_brd_counter + 1, self.currParams, currRec_fullalloc,
						B2CRBDs_df_for_cug_gq_tbf, 
						base_adjustedCapacity, self.outputSummaryRow_b2c.profileFare,
						dateFormatted_yyyymmdd, ndocounter, b2c_opening_price_val,
						cug_bookings_dict_local, param_cug_flag, param_cug_mkt_flag
					)

					# Group Quote allocation
					param_autoGroup = getattr(self.currParams, 'autoGroup', 0) # Default
					current_brd_counter = self.allocations.group_quote_alloc(
						profileExtract_df_fullalloc, current_brd_counter, currRec_fullalloc, param_autoGroup,
						base_adjustedCapacity, upsellValue_full, B2CRBDs_df_for_cug_gq_tbf, b2c_opening_price_val
					)

					# TBF allocation needs alloc_df from b2CAllocations (plural)
					param_tbfFlag = getattr(self.currParams, 'tbfFlag', 0) # Default
					current_brd_counter = self.allocations.tbf_allocation(
						alloc_df_for_tbf, current_brd_counter, currRec_fullalloc, param_tbfFlag, B2CRBDs_df_for_cug_gq_tbf,
						currtbfBookedLoad, float(currBookedLoad), base_adjustedCapacity, b2c_opening_price_val, month_str_for_profile, sector_full
					)

					# Sales Inventory for W-classes (distress_pax_count from common init)
					# w8_slot_for_sales_inv_full = -1 # Default if W8 not found
					# for i_slot in range(self.AU_COLUMN_LENGTH):
					# 	if getattr(currRec_fullalloc, f"class_of_service_{i_slot+1}", None) == 'W8':
					# 		w8_slot_for_sales_inv_full = i_slot
					# 		break
					# if w8_slot_for_sales_inv_full != -1:
					# 	self.allocations.sales_inv_allocation(
					# 		distress_pax_count, # Already rounded
					# 		w8_slot_for_sales_inv_full, # Pass index of W8
					# 		currRec_fullalloc, ['W7','W6','W5','W4'],
					# 		ndocounter,dateFormatted_yyyymmdd,self.currParams.origin,self.currParams.destin
					# 	)
					# else:
					# 	self.logger.warning("Could not find W8 slot for sales_inv_allocation in Full Mode AU Grid.")

					self.AUFileData.append(currRec_fullalloc)
					if self.inputSource.isUpdateNavitaire:
						if current_nav_update_method_full == CONST_API: self.updateNavitaire(currRec_fullalloc)
						elif current_nav_update_method_full == CONST_SFTP: self.updateNavSFTP(currRec_fullalloc)
						elif current_nav_update_method_full == S3: self.updateS3Push(currRec_fullalloc)
				else: # B2C determination was not successful enough for AU grid
					self.logger.info("B2C Open Data status not 0 or 2. AU Grid generation skipped for Full Allocation.")


				# --- Populate Final OutputSummaryRows (Full Mode) ---
				if overBookingCount > 0: # Add Overbooking note to FareResult
					if 'FareResult' in openDataB2C_details and isinstance(openDataB2C_details.get('FareResult'), list) and openDataB2C_details.get('FareResult'):
						openDataB2C_details['FareResult'][0] = str(openDataB2C_details['FareResult'][0]) + ' -Overbooking'
					if 'FareResult' in openDataB2B_details and isinstance(openDataB2B_details.get('FareResult'), list) and openDataB2B_details.get('FareResult'):
						openDataB2B_details['FareResult'][0] = str(openDataB2B_details['FareResult'][0]) + ' -Overbooking'

				# If profileFares param is 0, clear profileFare from summary
				param_profileFares = getattr(self.currParams, 'profileFares', 1) # Default to 1 (enabled)
				if pd.notna(param_profileFares) and int(param_profileFares) == 0:
					self.outputSummaryRow_b2b.profileFare = CONST_NA
					self.outputSummaryRow_b2c.profileFare = CONST_NA


				param_analyst_name_full = str(getattr(self.currParams, 'analyst_name', CONST_NA))
				param_b2c_backstop_full = str(b2c_backstop_final if b2c_backstop_final != CONST_NA else getattr(self.currParams, 'b2c_backstop', CONST_NA))
				param_b2b_backstop_full = str(b2b_backstop_final if b2b_backstop_final != CONST_NA else getattr(self.currParams, 'b2b_backstop', CONST_NA))


				# B2C Summary Row Population
				# Safely get values from openDataB2C_details dictionary
				def safe_get_from_dict(d, key, default=CONST_NA, index=0):
					val_list = d.get(key)
					if isinstance(val_list, list) and len(val_list) > index:
						return val_list[index]
					return default

				self.outputSummaryRow_b2c.RunId = self.runId; self.outputSummaryRow_b2c.Origin = self.currParams.origin
				self.outputSummaryRow_b2c.Destin = self.currParams.destin; self.outputSummaryRow_b2c.FltNum = str(currFlt)
				self.outputSummaryRow_b2c.DepDate = str(currFltDate); self.outputSummaryRow_b2c.BookedLoad = str(currBookedLoad)
				self.outputSummaryRow_b2c.TgtLoad = str(round(sod_startOfDayTgtBookedLoad_abs,2)); self.outputSummaryRow_b2c.ndoBand = str(ndoBand_full)
				self.outputSummaryRow_b2c.anchorFare = str(safe_get_from_dict(openDataB2C_details, "anchorFare"))
				self.outputSummaryRow_b2c.OpenRBD = str(safe_get_from_dict(openDataB2C_details, 'RBD'))
				self.outputSummaryRow_b2c.SellingFare = str(safe_get_from_dict(openDataB2C_details, "Fare")); self.outputSummaryRow_b2c.Channel = B2C_CHANNEL
				self.outputSummaryRow_b2c.HowDetermined = str(safe_get_from_dict(openDataB2C_details, 'FareResult', default='Error determining B2C fare'))
				self.outputSummaryRow_b2c.Backstop = param_b2c_backstop_full; self.outputSummaryRow_b2c.autoBackstop = auto_b2c_backstop_str
				self.outputSummaryRow_b2c.Variance = str(round(variance_full,2)); self.outputSummaryRow_b2c.analystName = param_analyst_name_full
				self.outputSummaryRow_b2c.bookedPlf = str(round(final_plf,2)); self.outputSummaryRow_b2c.allocationStatus = safe_get_from_dict(openDataB2C_details, 'allocationStatus', default=0)
				self.outputSummaryRow_b2c.overBookingCount = overBookingCount; self.outputSummaryRow_b2c.actualbookedPlf = round(self.actualplf,2)
				self.outputSummaryRow_b2c.recommendedActions = self.recommend_action(self.outputSummaryRow_b2c)

				# B2B Summary Row Population
				self.outputSummaryRow_b2b.RunId = self.runId; self.outputSummaryRow_b2b.Origin = self.currParams.origin
				self.outputSummaryRow_b2b.Destin = self.currParams.destin; self.outputSummaryRow_b2b.FltNum = str(currFlt)
				self.outputSummaryRow_b2b.DepDate = str(currFltDate); self.outputSummaryRow_b2b.BookedLoad = str(currBookedLoad)
				self.outputSummaryRow_b2b.TgtLoad = str(round(sod_startOfDayTgtBookedLoad_abs,2)); self.outputSummaryRow_b2b.ndoBand = str(ndoBand_full)
				self.outputSummaryRow_b2b.anchorFare = str(safe_get_from_dict(openDataB2B_details, "anchorFare"))
				self.outputSummaryRow_b2b.OpenRBD = str(safe_get_from_dict(openDataB2B_details, 'RBD'))
				self.outputSummaryRow_b2b.SellingFare = str(safe_get_from_dict(openDataB2B_details, "Fare")); self.outputSummaryRow_b2b.Channel = B2B_CHANNEL
				self.outputSummaryRow_b2b.HowDetermined = str(safe_get_from_dict(openDataB2B_details, 'FareResult', default='Error determining B2B fare'))
				self.outputSummaryRow_b2b.Backstop = param_b2b_backstop_full; self.outputSummaryRow_b2b.autoBackstop = auto_b2b_backstop_str
				self.outputSummaryRow_b2b.Variance = str(round(variance_full,2)); self.outputSummaryRow_b2b.analystName = param_analyst_name_full
				self.outputSummaryRow_b2b.bookedPlf = str(round(final_plf,2)); self.outputSummaryRow_b2b.allocationStatus = safe_get_from_dict(openDataB2B_details, 'allocationStatus', default=0)
				self.outputSummaryRow_b2b.overBookingCount = overBookingCount; self.outputSummaryRow_b2b.actualbookedPlf = round(self.actualplf,2)
				self.outputSummaryRow_b2b.recommendedActions = self.recommend_action(self.outputSummaryRow_b2b)

				new_summary_rows_fullalloc = [self.outputSummaryRow_b2c, self.outputSummaryRow_b2b]
				self.allocResults.extend(new_summary_rows_fullalloc)
				# Database insert
				if hasattr(self.rmdatabasehelper, 'insertSummaryRows'): self.rmdatabasehelper.insertSummaryRows(new_summary_rows_fullalloc)
				else: self.rmdatabasehelper.insertSummaryRow([self.outputSummaryRow_b2c]); self.rmdatabasehelper.insertSummaryRow([self.outputSummaryRow_b2b])

				self.redishelper.updateRunAuditReady()
			else: # Skipped due to DOW or flight number mismatch (Full Allocation Mode)
				# Check for specific DOW '9' for audit update
				dow_char_to_check = ''
				if len(param_dow_str) == 7 and 0 <= dow < 7:
					dow_char_to_check = param_dow_str[dow]

				if dow_char_to_check == "9":
					self.redishelper.updateRunAudit()
					self.redishelper.updateRunAuditReady()
				self.logger.info(f"Run Parameters requested to skip this flight-day (Flt: {currFlt}, DOW_int: {dow}, DOW_char_in_param: '{dow_char_to_check}') in Full Allocation Mode.")

	def w_rbd_allocation(self, currRec, brd_counter):
		currRec.set_class_attribute(brd_counter+2, 'class_of_service', 'G8')
		currRec.set_class_attribute(brd_counter+3, 'class_of_service', 'W9')
		currRec.set_class_attribute(brd_counter+4, 'class_of_service', 'W8')
		currRec.set_class_attribute(brd_counter+5, 'class_of_service', 'W7')
		currRec.set_class_attribute(brd_counter+6, 'class_of_service', 'W6')
		currRec.set_class_attribute(brd_counter+7, 'class_of_service', 'W5')
		currRec.set_class_attribute(brd_counter+8, 'class_of_service', 'W4')
		currRec.set_class_attribute(brd_counter+9, 'class_of_service', 'W3')
		currRec.set_class_attribute(brd_counter+10, 'class_of_service', 'W2')
		currRec.set_class_attribute(brd_counter+11, 'class_of_service', 'W1')
	
		if self.g8_thresholdflag:
			currRec.set_class_attribute(brd_counter+2, 'class_au', 0)
		else:
			currRec.set_class_attribute(brd_counter+2, 'class_au', -1)
	
		if self.w9_thresholdflag:
			currRec.set_class_attribute(brd_counter+3, 'class_au', 0)
		else:
			currRec.set_class_attribute(brd_counter+3, 'class_au', -1)
		
		if self.w8_thresholdflag:
			currRec.set_class_attribute(brd_counter+4, 'class_au', 0)
		else:
			currRec.set_class_attribute(brd_counter+4, 'class_au', -1)
	
		if self.w7_thresholdflag:
			currRec.set_class_attribute(brd_counter+5, 'class_au', 0)
		else:
			currRec.set_class_attribute(brd_counter+5, 'class_au', -1)
	
		if self.w6_thresholdflag:
			currRec.set_class_attribute(brd_counter+6, 'class_au', 0)
		else:
			currRec.set_class_attribute(brd_counter+6, 'class_au', -1)

		if self.w5_thresholdflag:
			currRec.set_class_attribute(brd_counter+7, 'class_au', 0)
		else:
			currRec.set_class_attribute(brd_counter+7, 'class_au', -1)

		if self.w4_thresholdflag:
			currRec.set_class_attribute(brd_counter+8, 'class_au', 0)
		else:
			currRec.set_class_attribute(brd_counter+8, 'class_au', -1)

		if self.w3_thresholdflag:
			currRec.set_class_attribute(brd_counter+9, 'class_au', 0)
		else:
			currRec.set_class_attribute(brd_counter+9, 'class_au', -1)

		if self.w2_thresholdflag:
			currRec.set_class_attribute(brd_counter+10, 'class_au', 0)
		else:
			currRec.set_class_attribute(brd_counter+10, 'class_au', -1)

		if self.w1_thresholdflag:
			currRec.set_class_attribute(brd_counter+11, 'class_au', 0)
		else:
			currRec.set_class_attribute(brd_counter+11, 'class_au', -1)

		return brd_counter+11

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
		l_class_seats_sold, p_class_seats_sold= self.currParams.seats_sold_L, self.currParams.seats_sold_P
		l_class_rbd, p_class_rbd= self.currParams.current_rbd_L, self.currParams.current_rbd_P

		self.allocate_class_inventory(currRec, brd_counter, default_au, au_cap, distress_au,
									l_class_seats_sold, p_class_seats_sold,l_class_rbd, p_class_rbd)



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


		lastBookloadTillMidnight = 0
		bookingToday = 0
		bookingLastHour = 0
		# if bookings.shape[0] != 0:
		if not pd.isna(self.currParams.BookingsCount):
			lastBookloadTillMidnight = int(self.currParams.BookingsCount)
		if not pd.isna(self.currParams.BookingToday):
			bookingToday = int(self.currParams.BookingToday)
		if not pd.isna(self.currParams.BookingsInHour):
			bookingLastHour = int(self.currParams.BookingsInHour)
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
		w9_thresholdValue, w8_thresholdValue, w7_thresholdValue, w6_thresholdValue, w5_thresholdValue, w4_thresholdValue, w3_thresholdValue, w2_thresholdValue, w1_thresholdValue, p_thresholdValue, l_thresholdValue, B2B_threshold, g8_thresholdValue = self.rmdatabasehelper.getPlfThreshhold(ndocounter, str(currFlt))
		
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
		self.g8_thresholdflag =False
		
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
		
		if len(g8_thresholdValue) != 0:
			if g8_thresholdValue[0] is not None and g8_thresholdValue[0]!='':
				if plf > g8_thresholdValue[0]:
					self.g8_thresholdflag = True

	def overBooking(self, currFltDate, ndocounter, totalCapacity, adjustedCapacity, plf):
		self.b2boverbookingflag = False
		nextFlight = self.currParams.NextFlight
		if (self.currParams.obSeats == '' or self.currParams.obSeats is None or (hasattr(self.currParams.obSeats, 'dtype') and math.isnan(self.currParams.obSeats))) and (self.currParams.obFare == '' or self.currParams.obFare is None or (hasattr(self.currParams.obFare, 'dtype') and math.isnan(self.currParams.obFare))):
			self.logger.info("Entered system overbooking loop runalloc")
			overBookingCount = self.rmdatabasehelper.getOverbookedSeats(self.currParams.origin, self.currParams.destin, self.currParams.flight_number, currFltDate)
			if(overBookingCount > OVERBOOKING_CAP):
				overBookingCount = OVERBOOKING_CAP
			if nextFlight == False and overBookingCount > OVERBOOKING_MAX_COUNT:
				self.logger.info("Reducing overbooking to max 2 seats")
				overBookingCount = OVERBOOKING_MAX_COUNT
			if self.canOverbook(ndocounter, totalCapacity, adjustedCapacity, plf, overBookingCount):
				totalCapacity = totalCapacity+overBookingCount
				if self.inputSource.input_source[CONST_OVERBOOKING] == DB:
					leg_key, nest_key, lid, nest_lid = self.navapihelper.get_leg_nest_details(currFltDate, AKASA_AIRLINE_CODE, self.currParams.flight_number, self.currParams.origin, self.currParams.destin)
					if not totalCapacity == lid:
						self.navapihelper.update_leg(leg_key, lid=totalCapacity)
					if not totalCapacity == nest_lid:
						self.navapihelper.update_leg_nest(leg_key, nest_key, totalCapacity)
				self.b2boverbookingflag = True
		else:
			self.logger.info("Entered manual overbooking loop runalloc")
			if self.canOverbookManual( ndocounter, totalCapacity, adjustedCapacity, plf, float(self.currParams.obSeats)):
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
				self.b2boverbookingflag =True
		return totalCapacity, self.b2boverbookingflag

	def canOverbook(self, ndocounter, totalCapacity, adjustedCapacity, plf, overBookingCount):
		return int(self.currParams.over_booking) == 1 and plf > self.constantsUtils.OVERBOOKING_LF and self.constantsUtils.OVERBOOKING_END_NDO >= ndocounter >= self.constantsUtils.OVERBOOKING_START_NDO and int(adjustedCapacity) == int(totalCapacity) and overBookingCount>0
	
	def canOverbookManual(self, ndocounter, totalCapacity, adjustedCapacity, plf, overBookingCount):
		return plf > self.constantsUtils.OVERBOOKING_LF and self.constantsUtils.OVERBOOKING_END_NDO >= ndocounter >= self.constantsUtils.OVERBOOKING_START_NDO and int(adjustedCapacity) == int(totalCapacity) and overBookingCount>0
	
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
			if self.salesInv==0:
				AUFrame.to_csv(CONST_AU_GRID_FILE_PATH, index=False)
			else:
				AUFrame.to_csv(CONST_AU_GRID_SALES_FILE_PATH, index=False)

		if self.inputSource.output[CONST_SUMMARY_FILE] == FILE:
			df = pd.DataFrame([vars(row) for row in self.allocResults])
			df = df.drop(columns=["Id"])
			if self.salesInv==0:
				df.to_csv(CONST_SUMMARY_FILE_PATH, index=False)
			else:
				df.to_csv(CONST_SUMMARY_SALES_FILE_PATH, index=False)

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
		self.dwconn = self.currParams.dwconn
		self.w0Flag = self.currParams.w0Flag
		if pd.isna(self.currParams.w0Flag):
			self.w0Flag = 0
		
		self.cache_client = self.currParams.cache_client
		self.starttime = datetime.now()
		self.allocResults = []
		self.AUFileData = []
		if self.currParams.run_id:
			self.runId = self.currParams.run_id
		else:
			self.runId = self.utils.getUniqueId()
		self.salesInv = self.currParams.saleinvflag
		self.log_time = self.currParams.time_logger
		if self.log_time is not None:
			self.log_time.update_run_id(self.currParams.run_id)
		self.logger = OwnLogger(self.runId, self.inputSource)
		self.rmdatabasehelper = RMDatabaseHelper(self.conn, self.wrconn,self.dwconn, self.runId, self.inputSource, self.debugMode, self.cache_client,self.currParams)
		self.constantsUtils = ConstantsUtils(self.conn)
		self.utils = Utils(self.inputSource)
		self.mathutils = MathUtils(self.inputSource)
		self.timeutils = TimeUtils(self.inputSource)
		# self.navdatabasehelper = NavDatabaseHelper(self.NavitaireConnection, self.conn, self.inputSource, self.NAV_DB_NAME, self.NAV_DB_REF_NAME,self.runId, self.debugMode, self.NAV_DB_WB_NAME,self.currParams, self.log_time)
		self.redishelper = RedisHelper(self.runId, self.cache_client, self.inputSource)
		if self.inputSource.input_source[CONST_OVERBOOKING] == DB:
			self.navapihelper = NavAPIHelper(self.NAV_BASE_API_URL, self.NAV_OCP_SUBSCRIPTION_KEY, self.NAV_USERNAME, self.NAV_PWD, self.NAV_DOMAIN, self.runId, self.inputSource)
		self.dynamictimed2 = GetDynamicTimeD2(self.rmdatabasehelper)
		self.marketfaresservice = MarketFareService(self.rmdatabasehelper,self.constantsUtils,self.timeutils)
		self.determinerbd = DetermineRBD(self.rmdatabasehelper, self.logger, self.inputSource, self.constantsUtils, self.mathutils, self.cache_client)
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
		booked_seats = self.currParams.total_series_seats
		total_count = 0
		if (booked_seats is not None) and (blocked_seats['blocked'][0] is not None) and (not pd.isna(booked_seats)):
			total_count = blocked_seats['blocked'][0]-booked_seats
		elif (blocked_seats['blocked'][0] is not None):
			total_count = blocked_seats['blocked'][0]
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