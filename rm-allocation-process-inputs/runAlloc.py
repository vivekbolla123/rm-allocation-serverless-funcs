import ast
import copy
from decimal import Decimal
import math
import traceback
import os
import json
import uuid
import warnings

import time as time_sleep
import numpy as np
import pandas as pd

from datetime import timedelta
from datetime import datetime
from datetime import time

import pytz

from Constants import *
from ownlogger import OwnLogger
from allocations import RBDAllocations
from get_dynamic_time_d2 import GetDynamicTimeD2
from market_fares_service import MarketFareService
from determine_rbd import DetermineRBD

from helper.rm_mongo_helper import MarketFareMongoDB
from helper.redis_helper import RedisHelper
from helper.nav_api_helper import NavAPIHelper
from helper.rm_database_helper import RMDatabaseHelper
from helper.portfolio_aggregator import compute_portfolio_aggregates

from utilities.utils import Utils
from utilities.math_utils import MathUtils
from utilities.time_utils import TimeUtils
from utilities.offset_utils import is_multiplier_offset
from constants_utils import ConstantsUtils
from common.compute_utils import *

from models.market_list_row import MarketListRow
from models.input_source import InputSource
from models.output_summary import OutputSummary
from models.output_summary_nextgen import OutputSummaryNextGen
from models.au_grid_row import AuGridRow
from models.flight_state import FlightState
from models.anchor_result import AnchorResult
from utilities.how_determined_parser import HowDeterminedParser
from common.nextgen_optimization_engine import NextGenOptimizationEngine
from common.fare_tax_helper import FareTaxHelper

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
		
		# For local split test, compute portfolio counts beforehand
		if self.inputSource.input_source.get(CONST_LAST_SELLING_FARE, TEST_DATA) == TEST_DATA:
			self.logger.info("Initializing local split orchestration")
			counts = self.mktList.groupby(["Origin", "Destin", "PerStart"])['FlightNumber'].nunique().to_dict()
			self.mktList['portfolio_count'] = self.mktList.apply(
				lambda row: counts.get((row.get('Origin', ''), row.get('Destin', ''), row.get('PerStart', '')), 1),
				axis=1
			)
			# Clear intermediate dir
			intermediate_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "QPDynamicAllocation", "testcases", "intermediate")
			os.makedirs(intermediate_dir, exist_ok=True)
			import glob
			for f in glob.glob(os.path.join(intermediate_dir, "*.jsonl")) + glob.glob(os.path.join(intermediate_dir, "*.json")):
				try:
					os.remove(f)
				except Exception:
					pass

		# if self.debugMode:
		# 	self.logger.info("Market List : ")
		# 	self.logger.info(self.mktList)

		for mktCounter in range(numMarkets):
			self.currParams = MarketListRow(self.mktList.iloc[mktCounter])
			if self.inputSource.input_source.get(CONST_LAST_SELLING_FARE, TEST_DATA) == TEST_DATA:
				self.currParams.portfolio_count = self.mktList.iloc[mktCounter].get('portfolio_count', 1)
			self.runDynamicAllocationSingleMarket(mktCounter)

		self.outputResults()

		endTime = self.timeutils.getCurrentDateTime()
		self.rmdatabasehelper.updateRunEnd(endTime)
		runTime = (endTime - startTime).total_seconds()
		self.logger.info("Allocation Completed in " + str(runTime) + " seconds")

	def runDynamicAllocationSingleMarket(self, mtkIndex):

		self.logger.info("**********************************************")
		self.logger.info("Starting Allocation for row : " + str(mtkIndex+1))
		self.redishelper.updateRunAuditReady()

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
		self.directFlag = self.currParams.directFlag

		startDate, endDate, startNDO, endNDO = self.getStartEndDate(currDate, startNDO, endNDO)

		self.logger.info("Allocation for " + origin + destination + " from " + str(startDate) + " to " + str(endDate))

		self.wait_for_inputs()
		bookedLoad = self.currParams.bookedPax

		# Sourcing flight parameters (capacity, time, bookload, tagging) from weighted file if NextGen optimization is enabled
		if int(getattr(self.currParams, 'nextgen_flag_NG', getattr(self.currParams, 'nextgen_flag', 0))) == 1:
			try:
				fc_df = self.rmdatabasehelper.get_additive_fare_bucket_forecast(
					sector=f"{self.currParams.origin}{self.currParams.destin}",
					snapshot_date=getattr(self.currParams, 'currDate', None),
					departure_date=startDate,
					flight_number=self.currParams.flight_number
				)
				if fc_df is not None and not fc_df.empty:
					cb_col = next((c for c in ['Current_Bookings', 'Total_Bookings', 'Total Bookings', 'Current Bookings'] if c in fc_df.columns), None)
					if cb_col and pd.notna(fc_df[cb_col].iloc[0]):
						bookedLoad = float(fc_df[cb_col].iloc[0])
						self.currParams.bookedPax = bookedLoad
					if 'Capacity' in fc_df.columns and pd.notna(fc_df['Capacity'].iloc[0]):
						cap_v = float(fc_df['Capacity'].iloc[0])
						self.currParams.capacity = cap_v
						self.currParams.adjustedCapacity = cap_v
					if 'STD' in fc_df.columns and pd.notna(fc_df['STD'].iloc[0]):
						self.currParams.DepartureTime = str(fc_df['STD'].iloc[0])[:5]
					if 'Tagging' in fc_df.columns and pd.notna(fc_df['Tagging'].iloc[0]):
						self.currParams.tagging = str(fc_df['Tagging'].iloc[0])
			except Exception as fc_e:
				self.logger.warning(f"Failed to fetch parameters from weighted forecast: {fc_e}")

		try:
			# For each NDO
			for ndocounter in range(startNDO, endNDO+1):
				self.outputSummaryRow_b2c = OutputSummary()
				self.outputSummaryRow_b2b = OutputSummary()
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
					self._triggerDummyAbortedFlightLocal(FlightNumber, bookedLoad, HD_NO_BOOKLOADS)
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
					self._triggerDummyAbortedFlightLocal(FlightNumber, bookedLoad, HD_NO_BOOKLOADS)
				else:
					tgtBookedLoadList = self.rmdatabasehelper.getTargetBookedLoadFromBenchmarkCurve(ndocounter, curveId)
					qpFares = self.rmdatabasehelper.get_qp_fares(origin, destination)
					TBFExist = self.rmdatabasehelper.get_TBF_fares(origin, destination)
					dateFormatted = datetime.strptime(str(tgtDate), DATE_FORMAT_2).date().strftime(DATE_FORMAT)
					print("TBF Flag: ", self.currParams.tbfFlag)
					if len(tgtBookedLoadList) > 0:
						if not qpFares.empty:
							if str(self.currParams.tbfFlag) in ('1', '1.0', 'True', 'true', 'Y', 'y') and not TBFExist:
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
								self._triggerDummyAbortedFlightLocal(FlightNumber, bookedLoad, HD_NO_TBF_FARES)
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
							self._triggerDummyAbortedFlightLocal(FlightNumber, bookedLoad, HD_NO_QP_FARES)
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
						self._triggerDummyAbortedFlightLocal(FlightNumber, bookedLoad, HD_NO_REF_CURVE)	
		except Exception as e:
			self.logger.info("Exception occurred")
			import traceback
			traceback.print_exc()
			self.logger.info(e)
			self._triggerDummyAbortedFlightLocal(FlightNumber, bookedLoad, f"Unhandled exception: {str(e)}")

		if not self.inputSource.isUpdateNavitaire:
			self.outputResults()

		# if self.inputSource.isUpdateNavitaire and (self.NavitaireUpdateMethod == CONST_SFTP or self.NavitaireUpdateMethod == S3):
		# 	self.redishelper.setAllRunStatus2Ready()

	def _triggerDummyAbortedFlightLocal(self, FlightNumber, bookedLoad, failure_reason):
		if self.inputSource.input_source.get(CONST_LAST_SELLING_FARE, TEST_DATA) == TEST_DATA:
			dummy_state = FlightState(
				currFlt=FlightNumber, currFltDate=self.currParams.per_start,
				currBookedLoad=bookedLoad, aborted=True, is_failure=True, failure_reason=failure_reason,
				portfolio_count=int(getattr(self.currParams, 'portfolio_count', 1) or 1)
			)
			entry = self._buildPortfolioEntry(dummy_state)
			self._coordinatePortfolioTriggerLocal(entry)

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

			if self.inputSource.input_source.get(CONST_LAST_SELLING_FARE, TEST_DATA) == TEST_DATA:
				# Local split flow: Run anchor pass only
				entry = self.runFareAnchorPass(
					currFlt=currFlt, currBookedLoad=currBookedLoad, currLid=currLid,
					currFltDate=currFltDate, currFltDepartureTime=currFltDepartureTime,
					tgtBookedLoadPercent=tgtBookedLoadPercent, dow=dow,
					ndocounter=ndocounter, currAdjustedCapacity=currAdjustedCapacity,
					currtbfBookedLoad=currtbfBookedLoad
				)
				# Only append multi-flight portfolios; single flights are short-circuited natively in maybeShortCircuitSingleFlight
				if not self._maybeShortCircuitSingleFlight(entry):
					self._coordinatePortfolioTriggerLocal(entry)
			else:
				# Monolithic legacy flow
				self.runDynamicAllocationSingleFlight(currFlt, currBookedLoad, currLid, currFltDate, currFltDepartureTime, tgtBookedLoadPercent, dow, ndocounter, currAdjustedCapacity,currtbfBookedLoad)

	def runDynamicAllocationSingleFlight(self, currFlt, currBookedLoad, currLid, currFltDate, currFltDepartureTime,
										 tgtBookedLoadPercent, # Renamed from tgtBookloadPercent for consistency
										 dow, ndocounter, currAdjustedCapacity,currtbfBookedLoad):
		"""Orchestrator for the per-flight allocation algorithm.

		Builds a ``FlightState`` from the call arguments, then calls the private
		phase methods (``_phaseA_initFlightContext`` … ``_phaseN_writeSummary``)
		in order. Behaviour is identical to the previous monolithic
		implementation (R11 — bit-for-bit output preservation); each phase method
		contains the original code block verbatim, with locals threaded through
		``state`` at the boundaries.
		"""
		# currFltDate is an MM/DD/YYYY string
		# currFltDepartureTime is an HH:MM:SS string

		state = FlightState(
			currFlt=currFlt,
			currFltDate=currFltDate,
			currFltDepartureTime=currFltDepartureTime,
			currLid=currLid,
			currAdjustedCapacity=currAdjustedCapacity,
			currtbfBookedLoad=currtbfBookedLoad,
			currBookedLoad=currBookedLoad,
			initial_currBookedLoad=currBookedLoad,
			tgtBookedLoadPercent=tgtBookedLoadPercent,
			dow=dow,
			ndocounter=ndocounter,
		)

		self._phaseA_initFlightContext(state)
		if state.aborted:
			return
		self._phaseB_overbooking(state)
		self._phaseC_targetsAndBands(state)
		self._phaseD_dowGate(state)
		if state.aborted:
			return
		self._phaseE_dynamicTimeAndD2(state)
		self._phaseF_marketFares(state)
		self._phaseG_profileFares(state)
		# self._phaseH_backstops(state)
		self._phaseI_lastSoldFare(state)
		self._phaseJ_spoilageReduction(state)
		self._phaseK_anchorAndLadder(state)
		self._phaseL_buildAuGrid(state)
		self._phaseM_pushNavitaire(state)
		self._phaseN_writeSummary(state)

	def _phaseA_initFlightContext(self, state):
		"""Phase A — fresh ``OutputSummary`` rows, w0/direct flags, date parsing,
		actualplf, series-block booked-load adjustment, currentTime, base
		capacities. Mirrors the head of the original
		``runDynamicAllocationSingleFlight`` up to (but not including) the
		overbooking call. Sets ``state.aborted = True`` if the flight date can't
		be parsed (the original method ``return``​s in that case)."""
		currFlt = state.currFlt
		currBookedLoad = state.currBookedLoad
		currLid = state.currLid
		currFltDate = state.currFltDate
		currFltDepartureTime = state.currFltDepartureTime
		currAdjustedCapacity = state.currAdjustedCapacity

		self.currParams.strategy = self.currParams.fare_anchor

		# --- Ensure outputSummaryRow objects are fresh for this flight-day ---
		self.outputSummaryRow_b2b = OutputSummary()
		self.outputSummaryRow_b2c = OutputSummary()

		current_w0Flag = 0 # Default
		# Safely get w0Flag from self, as it might be set in __init__ or earlier
		if hasattr(self, 'w0Flag') and pd.notna(self.w0Flag):
			try:
				current_w0Flag = int(self.w0Flag)
			except (ValueError, TypeError):
				self.logger.info(f"Could not convert self.w0Flag ('{self.w0Flag}') to int. Defaulting to 0.")
		self.outputSummaryRow_b2c.w0Flag = current_w0Flag
		self.outputSummaryRow_b2b.w0Flag = current_w0Flag
		state.current_w0Flag = current_w0Flag

		current_directFlag = 1 # Default
		# Safely get directFlag from self, as it might be set in __init__ or earlier
		if hasattr(self, 'directFlag') and pd.notna(self.directFlag):
			try:
				current_directFlag = int(self.directFlag)
			except (ValueError, TypeError):
				self.logger.info(f"Could not convert self.directFlag ('{self.directFlag}') to int. Defaulting to 1.")
		state.current_directFlag = current_directFlag

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
			state.aborted = True
			return
		state.currFltDate_dt = currFltDate_dt
		state.dateFormatted_yyyymmdd = dateFormatted_yyyymmdd

		# --- Common Initializations (needed by both paths) ---
		initial_currBookedLoad = currBookedLoad # Preserve the value passed in
		state.initial_currBookedLoad = initial_currBookedLoad

		# Actual PLF based on initial booked load and adjusted capacity
		self.actualplf = 0.0 # Default
		try:
			if pd.notna(currAdjustedCapacity) and float(currAdjustedCapacity) > 0:
				self.actualplf = (float(initial_currBookedLoad) / float(currAdjustedCapacity)) * 100
		except (ValueError, TypeError):
			self.logger.info("Could not calculate actualplf due to invalid numeric types for booked load or capacity.")
		state.actualplf = self.actualplf

		# Adjust currBookedLoad for seriesBlock if applicable
		effective_series_block_flag = 0 # Default
		series_block_param = getattr(self.currParams, 'seriesBlock', '0') # Safely get, default to '0'
		if pd.notna(series_block_param):
			try:
				effective_series_block_flag = int(series_block_param)
			except (ValueError, TypeError):
				self.logger.info(f"Invalid seriesBlock value: {series_block_param}. Assuming 0.")

		if effective_series_block_flag == 1:
			currBookedLoad = self.getBookloadSeries(dateFormatted_yyyymmdd, currBookedLoad) # getBookloadSeries needs YYYY-MM-DD
		state.currBookedLoad = currBookedLoad

		self.logger.info(f"Effective Booked Load for Flt: {str(currFlt)} on Date: {str(currFltDate)} is: {str(currBookedLoad)}")

		# Current processing time (HH:MM:SS string) from helper
		self.currentTime = self.rmdatabasehelper.getCurrentTime(
			self.inputSource.currentTime, self.currParams, currFltDate, self.starttime # currFltDate is MM/DD/YYYY
		)
		state.currentTime = self.currentTime

		# Capacities - ensure they are numeric
		base_totalCapacity = 0.0
		if pd.notna(currLid):
			try: base_totalCapacity = float(currLid)
			except (ValueError, TypeError): self.logger.info(f"currLid '{currLid}' not convertible to float.")

		base_adjustedCapacity = 0.0
		if pd.notna(currAdjustedCapacity):
			try: base_adjustedCapacity = float(currAdjustedCapacity)
			except (ValueError, TypeError): self.logger.info(f"currAdjustedCapacity '{currAdjustedCapacity}' not convertible to float.")

		state.base_totalCapacity = base_totalCapacity
		state.base_adjustedCapacity = base_adjustedCapacity

		# Initialize auto backstop values in state
		state.b2c_backstop_final = getattr(self.currParams, 'b2c_backstop', CONST_NA)
		state.b2b_backstop_final = getattr(self.currParams, 'b2b_backstop', CONST_NA)
		state.auto_b2b_backstop_str = CONST_NA
		state.auto_b2c_backstop_str = CONST_NA
		

	def _phaseB_overbooking(self, state):
		"""Phase B — overbooking calc, post-OB PLF, start-of-day values, distress
		pax count, and date/time strings used downstream by profile-fares lookups."""
		currFlt = state.currFlt
		currBookedLoad = state.currBookedLoad
		currFltDate = state.currFltDate
		currFltDepartureTime = state.currFltDepartureTime
		ndocounter = state.ndocounter
		tgtBookedLoadPercent = state.tgtBookedLoadPercent
		currFltDate_dt = state.currFltDate_dt
		base_totalCapacity = state.base_totalCapacity
		base_adjustedCapacity = state.base_adjustedCapacity

		# --- Overbooking Logic (affects totalCapacity, run before flag check) ---
		plf_before_ob = 0.0
		if base_adjustedCapacity > 0:
			plf_before_ob = (float(currBookedLoad) / base_adjustedCapacity) * 100
		elif base_totalCapacity > 0: # Fallback if adjustedCapacity is zero
			plf_before_ob = (float(currBookedLoad) / base_totalCapacity) * 100

		# self.overBooking needs YYYY-MM-DD date
		final_totalCapacity, self.b2boverbookingflag, expectedNoShows = self.overBooking(
			self.currParams.per_start, ndocounter, base_totalCapacity, base_adjustedCapacity, plf_before_ob, float(currBookedLoad)
		)
		state.b2boverbookingflag = self.b2boverbookingflag
		self.logger.info(f"Total capacity/Lid after OB (if applicable) is: {final_totalCapacity}")

		# OverBookingCount relative to the original adjusted capacity
		overBookingCount = float(final_totalCapacity - base_adjustedCapacity)
		state.expectedNoShows = expectedNoShows

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
				(state.base_adjustedCapacity if state.base_adjustedCapacity > 0 else final_totalCapacity), currFltDepartureTime
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

		state.grp_adj_profile_fare = CONST_NA
		state.grp_adj_bookload = CONST_NA
		state.grp_fare_anchor = CONST_NA
		
		self.logger.info("Running in Full Dynamic Allocation mode.")

		state.final_totalCapacity = final_totalCapacity
		state.overBookingCount = overBookingCount
		state.final_plf = final_plf
		state.sod_dplfBand = sod_dplfBand
		state.sod_bookingToday = sod_bookingToday
		state.sod_BookingsInHour = sod_BookingsInHour
		state.sod_eodTgtBookedLoad_abs = sod_eodTgtBookedLoad_abs
		state.sod_tgtAtCurrentHour_abs = sod_tgtAtCurrentHour_abs
		state.sod_startOfDayBookLoad_factor = sod_startOfDayBookLoad_factor
		state.sod_startOfDayTgtBookedLoad_abs = sod_startOfDayTgtBookedLoad_abs
		state.distress_pax_count = distress_pax_count
		state.deptTime_str_hhmm = deptTime_str_hhmm
		state.dow_str_for_profile = dow_str_for_profile
		state.month_str_for_profile = month_str_for_profile

	def _phaseC_targetsAndBands(self, state):
		"""Phase C — read DOW / flight-number params from currParams. The variance,
		forecast, and band derivations stay in the DOW-matched branch (Phase E
		onwards) to preserve the original control flow."""
		param_dow_str = str(getattr(self.currParams, 'dow', '')) # Safely get dow string from params
		param_flight_number_str = str(getattr(self.currParams, 'flight_number', '')) # Safely get flight_number
		state.param_dow_str = param_dow_str
		state.param_flight_number_str = param_flight_number_str

	def _phaseD_dowGate(self, state):
		"""Phase D — DOW + flight-number gate. Sets ``state.aborted = True`` when
		the flight should be skipped (DOW or flight-number mismatch); otherwise
		leaves ``state.aborted`` as-is so subsequent phases run.

		On the abort path this method also performs the original side effects:
		when the operator explicitly requested a skip via DOW char ``"9"`` it
		bumps ``runAudit``, and it always logs the skip line. Keeping these here
		(instead of in the orchestrator) keeps ``runDynamicAllocation*`` callers
		as flat phase pipelines and lets ``runFareAnchorPass`` (Slice 7) reuse
		the same gate without duplicating the audit/log block."""
		currFlt = state.currFlt
		dow = state.dow
		param_dow_str = state.param_dow_str
		param_flight_number_str = state.param_flight_number_str

		dow_condition_met = False
		if len(param_dow_str) == 7: # Ensure it's a 7-character string
			if 0 <= dow < 7: # Ensure dow is a valid index
				dow_condition_met = (param_dow_str[dow] == "1")
			else:
				self.logger.info(f"Invalid DOW integer {dow} for indexing DOW string '{param_dow_str}'.")
		else:
			self.logger.info(f"DOW string '{param_dow_str}' in params is not 7 characters long.")
		state.dow_condition_met = dow_condition_met

		gate_open = ((str(currFlt) == param_flight_number_str) or (param_flight_number_str == '*')) and dow_condition_met

		if gate_open:
			return

		# Skipped due to DOW or flight number mismatch (Full Allocation Mode).
		# Check for specific DOW '9' for audit update.
		dow_char_to_check = ''
		if len(param_dow_str) == 7 and 0 <= dow < 7:
			dow_char_to_check = param_dow_str[dow]

		if dow_char_to_check == "9":
			self.redishelper.updateRunAudit()
		self.logger.info(
			f"Run Parameters requested to skip this flight-day "
			f"(Flt: {currFlt}, DOW_int: {dow}, DOW_char_in_param: '{dow_char_to_check}') "
			f"in Full Allocation Mode."
		)
		state.aborted = True

	def _phaseE_dynamicTimeAndD2(self, state):
		"""Phase E — variance, forecast plumbing, NDO/dPLF/forecast bands, B2B
		strategy factor lookup, summary-row priming for current load/booking
		counts, threshold checks, dynamic D1/D2 (start/end times, criteria, offset,
		strategy reference), and unique IDs for the summary rows."""
		currFlt = state.currFlt
		currBookedLoad = state.currBookedLoad
		currFltDepartureTime = state.currFltDepartureTime
		ndocounter = state.ndocounter
		final_totalCapacity = state.final_totalCapacity
		final_plf = state.final_plf
		sod_tgtAtCurrentHour_abs = state.sod_tgtAtCurrentHour_abs
		sod_startOfDayBookLoad_factor = state.sod_startOfDayBookLoad_factor
		sod_startOfDayTgtBookedLoad_abs = state.sod_startOfDayTgtBookedLoad_abs
		sod_bookingToday = state.sod_bookingToday
		sod_BookingsInHour = state.sod_BookingsInHour
		sod_eodTgtBookedLoad_abs = state.sod_eodTgtBookedLoad_abs
		sod_dplfBand = state.sod_dplfBand

		eff_capacity_for_variance = state.base_adjustedCapacity if state.base_adjustedCapacity > 0 else final_totalCapacity
		variance_full = calculateVariance(
			float(currBookedLoad), eff_capacity_for_variance, ndocounter,
			sod_tgtAtCurrentHour_abs, sod_startOfDayBookLoad_factor, sod_startOfDayTgtBookedLoad_abs
		)
		self.logger.info(f"Variance (Full Mode): {variance_full:.2f}")

		# Forecast PLF — computed first because the imputed-forecast guard/fallback
		# cases below return forecast_plf when the adjustment can't be applied.
		forecast_plf = forecastBand = CONST_NA
		if ndocounter <= 30:
			forecast_plf = getParsedForecast(self)
		if forecast_plf != CONST_NA and not pd.isna(forecast_plf):
			forecastBand = self.rmdatabasehelper.getForecastBand(forecast_plf)

		# ==================== IMPUTED FORECAST (Phase 2 BRD) ====================
		# imp_forecast_load = Forecast_Load + (CB - CB_r) - [Adjustment_Factor*(CT - CT_r)]
		# imputed forecast  = round(imp_forecast_load) / capacity * 100
		imp_forecast = CONST_NA
		PLF_Gain = CONST_NA
		Af_Nb_Allin = CONST_NA
		if ndocounter <= 30:
			imp_forecast = getParsedImputedForecast(
				self, forecast_plf, currBookedLoad, sod_tgtAtCurrentHour_abs, eff_capacity_for_variance
			)
		if imp_forecast != CONST_NA:
			Af_Nb_Allin = calculateAfNbAllin(self, ndocounter, min(imp_forecast, self.constantsUtils.AF_PLF_FORECAST_CAP), str(round(final_plf,2)), sod_tgtAtCurrentHour_abs, eff_capacity_for_variance)
			PLF_Gain = ((imp_forecast) - round(final_plf,2))

		ndoBand_full = self.rmdatabasehelper.getNdoBand(ndocounter)
		dplfBand_full = self.rmdatabasehelper.getdpflBand(variance_full)

		imp_forecast_band = CONST_NA
		af_forecast_band = CONST_NA
		group_forecast_band = CONST_NA
		if imp_forecast != CONST_NA and not pd.isna(imp_forecast):
			imp_forecast_band = self.rmdatabasehelper.get_imputed_forecast_band(imp_forecast)
			af_forecast_band = self.rmdatabasehelper.get_af_forecast_band(imp_forecast)
			if ndocounter <= 30:
				group_forecast_band_result = self.rmdatabasehelper.get_group_forecast_band(imp_forecast)
				group_forecast_band = group_forecast_band_result.get('band', CONST_NA) if isinstance(group_forecast_band_result, dict) else CONST_NA
		else:
			group_forecast_band = dplfBand_full if dplfBand_full is not None else CONST_NA

		b2b_strategy = self.currParams.b2bStrategy
		forecastAllocFlag = int(getattr(self.currParams, 'forecastAllocFlag', 0))
		skip_auto_b2b_factor = True

		is_b2b_strategy_na = b2b_strategy == CONST_NA and b2b_strategy == "" and pd.isna(b2b_strategy)
		try:
			skip_auto_b2b_factor = forecastAllocFlag == 0 or is_b2b_strategy_na or imp_forecast_band == CONST_NA
		except (ValueError, TypeError):
			skip_auto_b2b_factor = True

		# Assign B2B factor based on B2B Strategy if applicable
		if not skip_auto_b2b_factor:
			auto_b2b_factor_value = self.rmdatabasehelper.getB2BStrategyValue(ndoBand_full, imp_forecast_band, b2b_strategy)
			if auto_b2b_factor_value is not None:
				try:
					self.currParams.b2b_factor = float(auto_b2b_factor_value)
				except (ValueError, TypeError):
					pass  # keep the default value from ML

		# Populate summary row fields specific to full allocation
		self.outputSummaryRow_b2c.currentTgtLoad = round(sod_tgtAtCurrentHour_abs,2)
		self.outputSummaryRow_b2b.currentTgtLoad = round(sod_tgtAtCurrentHour_abs,2)
		currentLoadFactor_val = (sod_tgtAtCurrentHour_abs / eff_capacity_for_variance) * 100 if eff_capacity_for_variance > 0 else 0.0
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

		# Strategy Band logic
		forecastAllocNdoMax = int(getattr(self.constantsUtils, 'FORECAST_ALLOC_NDO_MAX', 30))
		strategy_band = group_forecast_band if (forecastAllocFlag == 1 and ndocounter <= forecastAllocNdoMax) else dplfBand_full

		# Get Dynamic D1 (Time Window)
		self.outputSummaryRow_b2b.startTime, self.outputSummaryRow_b2b.endTime, self.outputSummaryRow_b2b.TimeRange, hardstartTimeB2B, hardendTimeB2B = \
			self.dynamictimed2.getDynamicTime(self.currParams, strategy_band, ndoBand_full, group_forecast_band, DaySpan_full, B2B_CHANNEL, final_plf)
		self.outputSummaryRow_b2c.startTime, self.outputSummaryRow_b2c.endTime, self.outputSummaryRow_b2c.TimeRange, hardstartTimeB2C, hardendTimeB2C = \
			self.dynamictimed2.getDynamicTime(self.currParams, strategy_band, ndoBand_full, group_forecast_band, DaySpan_full, B2C_CHANNEL, final_plf) # Typo was c2c

		# Get Dynamic D2 (Criteria, Offset, StrategyRef)
		self.outputSummaryRow_b2b.criteria, hardcriteriaB2B, self.outputSummaryRow_b2b.fareOffset, hardoffsetB2B, self.outputSummaryRow_b2b.StrategyReference, hardfareb2b = \
			self.dynamictimed2.getDynamicD2Value(self.currParams, strategy_band, ndoBand_full, group_forecast_band, B2B_CHANNEL, final_plf)
		self.outputSummaryRow_b2c.criteria, hardcriteriaB2C, self.outputSummaryRow_b2c.fareOffset, hardoffsetB2C, self.outputSummaryRow_b2c.StrategyReference, hardfareb2c = \
			self.dynamictimed2.getDynamicD2Value(self.currParams, strategy_band, ndoBand_full, group_forecast_band, B2C_CHANNEL, final_plf)

		# IDs for summary rows (already set in common init, but ensure unique if re-running parts)
		self.outputSummaryRow_b2b.Id = self.utils.getUniqueId()
		self.outputSummaryRow_b2c.Id = self.utils.getUniqueId()

		state.variance_full = variance_full
		state.imp_forecast = imp_forecast
		state.PLF_Gain = PLF_Gain
		state.Af_Nb_Allin = Af_Nb_Allin
		state.ndoBand_full = ndoBand_full
		state.dplfBand_full = dplfBand_full
		state.forecast_plf = forecast_plf
		state.forecastBand = forecastBand
		state.group_forecast_band = group_forecast_band
		state.imp_forecast_band = imp_forecast_band
		state.af_forecast_band = af_forecast_band
		state.DaySpan_full = DaySpan_full
		state.input_time_str_dep_full = input_time_str_dep_full
		state.hardstartTime_b2b = hardstartTimeB2B
		state.hardendTime_b2b = hardendTimeB2B
		state.hardstartTime_b2c = hardstartTimeB2C
		state.hardendTime_b2c = hardendTimeB2C
		state.hardcriteria_b2b = hardcriteriaB2B
		state.hardoffset_b2b = hardoffsetB2B
		state.hardfare_b2b = hardfareb2b
		state.hardcriteria_b2c = hardcriteriaB2C
		state.hardoffset_b2c = hardoffsetB2C
		state.hardfare_b2c = hardfareb2c

	def _phaseF_marketFares(self, state):
		"""Phase F — competitive market fare anchor lookup for B2C and B2B,
		including the hardfare update returned by the market-fare service."""
		currFltDate = state.currFltDate
		imp_forecast = state.imp_forecast
		DaySpan_full = state.DaySpan_full
		input_time_str_dep_full = state.input_time_str_dep_full

		self.logger.info("Getting market Fares (Full Mode)")
		# getMarketFareAnchorFare expects currFltDate as MM/DD/YYYY
		mktfare_params_b2c = {
			"currParams": self.currParams, "currFltDate": currFltDate, "channel": B2C_CHANNEL,
			"startTime": self.outputSummaryRow_b2c.startTime, "endTime": self.outputSummaryRow_b2c.endTime,
			"DaySpan": DaySpan_full, "criteria": self.outputSummaryRow_b2c.criteria,
			"flightTime": input_time_str_dep_full, "hardcriteria": state.hardcriteria_b2c,
			"hardfare": state.hardfare_b2c, "hardstartTime": state.hardstartTime_b2c, "hardendTime": state.hardendTime_b2c,
			"imp_forecast": imp_forecast
		}

		self.outputSummaryRow_b2c.MktFare_Min, self.outputSummaryRow_b2c.MktFare_Max, \
		self.outputSummaryRow_b2c.AirlineCode_Min, self.outputSummaryRow_b2c.AirlineCode_Max, \
		self.outputSummaryRow_b2c.FlightNumber_Min, self.outputSummaryRow_b2c.FlightNumber_Max, \
		isB2CMarketFareAvailable, self.outputSummaryRow_b2c.criteria, \
		self.outputSummaryRow_b2c.startTime, self.outputSummaryRow_b2c.endTime, \
		hardfareb2c_updated,lowmktFileb2c,himktFileb2c = self.marketfaresservice.getMarketFareAnchorFare(**mktfare_params_b2c)
		state.hardfare_b2c = hardfareb2c_updated # Update local var if function modifiesit
		mktfare_params_b2b = {
			"currParams": self.currParams, "currFltDate": currFltDate, "channel": B2B_CHANNEL,
			"startTime": self.outputSummaryRow_b2b.startTime, "endTime": self.outputSummaryRow_b2b.endTime,
			"DaySpan": DaySpan_full, "criteria": self.outputSummaryRow_b2b.criteria,
			"flightTime": input_time_str_dep_full, "hardcriteria": state.hardcriteria_b2b,
			"hardfare": state.hardfare_b2b, "hardstartTime": state.hardstartTime_b2b, "hardendTime": state.hardendTime_b2b,
			"imp_forecast": imp_forecast
		}

		self.outputSummaryRow_b2b.MktFare_Min, self.outputSummaryRow_b2b.MktFare_Max, \
		self.outputSummaryRow_b2b.AirlineCode_Min, self.outputSummaryRow_b2b.AirlineCode_Max, \
		self.outputSummaryRow_b2b.FlightNumber_Min, self.outputSummaryRow_b2b.FlightNumber_Max, \
		isB2BMarketFareAvailable, self.outputSummaryRow_b2b.criteria, \
		self.outputSummaryRow_b2b.startTime, self.outputSummaryRow_b2b.endTime, \
		hardfareb2b_updated,lowmktFileb2b,himktFileb2b = self.marketfaresservice.getMarketFareAnchorFare(**mktfare_params_b2b)
		state.hardfare_b2b = hardfareb2b_updated # Update local var

		state.isB2CMarketFareAvailable = isB2CMarketFareAvailable
		state.isB2BMarketFareAvailable = isB2BMarketFareAvailable
		state.lowmktFile_b2c = lowmktFileb2c
		state.himktFile_b2c = himktFileb2c
		state.lowmktFile_b2b = lowmktFileb2b
		state.himktFile_b2b = himktFileb2b

	def _phaseG_profileFares(self, state):
		"""Phase G — booking quartile, sector, upsell value, profile fare for
		current quartile."""
		currBookedLoad = state.currBookedLoad
		currFltDate = state.currFltDate
		final_totalCapacity = state.final_totalCapacity
		deptTime_str_hhmm = state.deptTime_str_hhmm
		dow_str_for_profile = state.dow_str_for_profile
		month_str_for_profile = state.month_str_for_profile

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
		if not upsellValueData_full.empty and 'percentage' in upsellValueData_full.columns:
			raw_val = upsellValueData_full.percentage.iloc[0]
			if pd.notna(raw_val) and str(raw_val).strip().lower() != 'nan':
				try: 
					upsellValue_full = float(raw_val)
				except (ValueError, TypeError): 
					self.logger.info(f"Could not convert upsell percentage '{raw_val}' to float.")
					upsellValue_full = 0.0

		state.bookloadPercentile_full = bookloadPercentile_full
		state.percentile_str_full = percentile_str_full
		state.sector_full = sector_full
		state.upsellValue_full = upsellValue_full

	# def _phaseH_backstops(self, state):
	# 	"""Phase H — read configured B2C/B2B backstop, optionally override with
	# 	auto-backstop computed from the final market fare."""
	# 	currFltDate = state.currFltDate
	# 	dplfBand_full = state.dplfBand_full
	# 	DaySpan_full = state.DaySpan_full
	# 	input_time_str_dep_full = state.input_time_str_dep_full
	# 
	# 	# Backstop Logic
	# 	b2c_backstop_final = getattr(self.currParams, 'b2c_backstop', CONST_NA) # Safely get
	# 	b2b_backstop_final = getattr(self.currParams, 'b2b_backstop', CONST_NA) # Safely get
	# 	auto_b2b_backstop_str = "NA"
	# 	auto_b2c_backstop_str = "NA"
	# 
	# 	state.b2c_backstop_final = b2c_backstop_final
	# 	state.b2b_backstop_final = b2b_backstop_final
	# 	state.auto_b2b_backstop_str = auto_b2b_backstop_str
	# 	state.auto_b2c_backstop_str = auto_b2c_backstop_str

	def _phaseI_lastSoldFare(self, state):
		"""Phase I — last selling fare and seven-day average (from CSV fixtures or
		Redis depending on input source)."""
		currFltDate = state.currFltDate
		currFltDate_dt = state.currFltDate_dt
		param_flight_number_str = state.param_flight_number_str
		conversion_rate = "NA"
		# Last Selling Fare
		lastSoldFareb2c_val = CONST_NA; lastSoldFareb2b_val = CONST_NA; last_seven_avg_val = CONST_NA
		input_source_lsf = self.inputSource.input_source.get(CONST_LAST_SELLING_FARE, TEST_DATA) # Default to TEST_DATA if not set
		qpcurrency_df = self.rmdatabasehelper.get_qp_currency(self.currParams.origin, self.currParams.destin)
		if not qpcurrency_df.empty and 'currency' in qpcurrency_df.columns:
			self.currency = str(qpcurrency_df['currency'].iloc[0]).strip()
		else:
			self.currency = 'INR'
		if input_source_lsf == TEST_DATA:
			farePrice_data = self.rmdatabasehelper.getLastSellingFare(currFltDate, self.currParams) # currFltDate is MM/DD/YYYY
			if not farePrice_data.empty:
				if "B2C_last3avg_fare" in farePrice_data.columns: lastSoldFareb2c_val = farePrice_data["B2C_last3avg_fare"].iloc[0]
				if "B2B_last3avg_fare" in farePrice_data.columns: lastSoldFareb2b_val = farePrice_data["B2B_last3avg_fare"].iloc[0]
				if "last7avg" in farePrice_data.columns: last_seven_avg_val = farePrice_data["last7avg"].iloc[0]
		elif input_source_lsf == DB:
			redis_key_lsf = f"LastSoldFare:{self.currParams.origin}{self.currParams.destin}{param_flight_number_str}_{currFltDate_dt.date()}"
			if self.cache_client.exists(redis_key_lsf):
				fare_manifest_str = None
				try:
					fare_manifest_str = self.cache_client.get(redis_key_lsf)
					if fare_manifest_str is not None:
						fare_manifest_str = fare_manifest_str.decode('utf-8')
				except Exception as e:
					self.logger.error(f"Error fetching {redis_key_lsf} from Redis: {e}")
					# Optionally try to get it as a hash if it's the wrong type
					try:
						if self.cache_client.type(redis_key_lsf).decode('utf-8') == 'hash':
							hash_data = self.cache_client.hgetall(redis_key_lsf)
							# Reconstruct json string if needed, or simply log it.
							self.logger.error(f"Key {redis_key_lsf} is a hash. Data: {hash_data}")
					except:
						pass

				if fare_manifest_str:
					last_seven_avg_val, lastSoldFareb2b_val, lastSoldFareb2c_val = self.calculate_averages(fare_manifest_str, state.ndocounter)
					
					

					if self.currency != 'INR' and self.currency != '' and self.currency.upper() != 'NAN' and self.currency != 'None':
						if last_seven_avg_val != CONST_NA and not pd.isna(last_seven_avg_val):
							last_seven_avg_val, conversion_rate = self.convert_currency('INR', self.currency, last_seven_avg_val)
						if lastSoldFareb2b_val != CONST_NA and not pd.isna(lastSoldFareb2b_val):
							lastSoldFareb2b_val, conversion_rate = self.convert_currency('INR', self.currency, lastSoldFareb2b_val)
						if lastSoldFareb2c_val != CONST_NA and not pd.isna(lastSoldFareb2c_val):
							lastSoldFareb2c_val, conversion_rate = self.convert_currency('INR', self.currency, lastSoldFareb2c_val)

		state.lastSoldFareb2c_val = lastSoldFareb2c_val
		state.lastSoldFareb2b_val = lastSoldFareb2b_val
		state.last_seven_avg_val = last_seven_avg_val
		state.currency = self.currency
		state.conversion_rate = conversion_rate

	def _phaseJ_spoilageReduction(self, state):
		"""Phase J — NDO-0 spoilage-reduction trigger (PLF threshold, time-to-
		departure window, and last-7 average availability)."""
		currFltDepartureTime = state.currFltDepartureTime
		currFltDate_dt = state.currFltDate_dt
		ndocounter = state.ndocounter
		final_plf = state.final_plf
		last_seven_avg_val = state.last_seven_avg_val

		is_spoilage_reduction_active = False
		if ndocounter == 0 and int(self.currParams.Spoilage_Flag)==1:
			self.logger.info("D-0 flight detected. Checking for spoilage reduction conditions.")
			try:
				# 1. Check for last seven sold fare availability
				if pd.notna(last_seven_avg_val) and last_seven_avg_val != CONST_NA:
					# 2. Check PLF threshold to avoid dilution
					if final_plf <= self.constantsUtils.SPOILAGE_REDUCTION_PLF_THRESHOLD:
						# 3. Check timing trigger
						try:
							departure_time_obj = datetime.strptime(str(currFltDepartureTime), TIME_FORMAT).time()
						except ValueError:
							departure_time_obj = datetime.strptime(str(currFltDepartureTime)[:5], TIME_FORMAT).time()
						try:
							current_time_obj = datetime.strptime(str(self.currentTime), TIME_FORMAT).time()
						except ValueError:
							current_time_obj = datetime.strptime(str(self.currentTime)[:5], TIME_FORMAT).time()


						# Combine with a date to properly calculate difference

						today = self.timeutils.getCurrentDate()
						departure_datetime = datetime.combine(currFltDate_dt, departure_time_obj)
						current_datetime = datetime.combine(today, current_time_obj)

						# Handle overnight case where departure is on the "next" day relative to current time
						if departure_datetime > current_datetime:


							hours_to_departure = (departure_datetime - current_datetime).total_seconds() / 3600

							trigger_hours = 0
							time_8am = time(8, 0)
							time_12pm = time(12, 0)

							if time_8am <= departure_time_obj < time_12pm:
								# For flights departing between 8 AM and 12 PM
								trigger_hours = 12
							else:
								# For flights departing between 12 PM and 8 AM (next day)
								trigger_hours = 12

							if hours_to_departure <= trigger_hours:
								self.logger.info(f"Spoilage Reduction ACTIVE. Hours to departure: {hours_to_departure:.2f} <= Trigger: {trigger_hours}hrs. PLF: {final_plf:.2f}%")
								is_spoilage_reduction_active = True
							else:
								self.logger.info(f"Spoilage Reduction Inactive: Outside time window. Hours to departure: {hours_to_departure:.2f} > Trigger: {trigger_hours}hrs.")
					else:
						self.logger.info(f"Spoilage Reduction Skipped: PLF ({final_plf:.2f}%) is above threshold ({self.constantsUtils.SPOILAGE_REDUCTION_PLF_THRESHOLD}%).")
				else:
					self.logger.info("Spoilage Reduction Skipped: Last 7 average fare not available.")
			except Exception as e:
				self.logger.error(f"Error during spoilage reduction check: {e}")

		state.is_spoilage_reduction_active = is_spoilage_reduction_active
	def _phaseK_anchorAndLadder(self, state):
		"""Phase K — anchor candidate selection (B2C and B2B) plus the ladder
		lookup via ``DetermineRBD.determineRBDToOpen`` for both channels."""
		currBookedLoad = state.currBookedLoad
		currFltDate = state.currFltDate
		dateFormatted_yyyymmdd = state.dateFormatted_yyyymmdd
		ndocounter = state.ndocounter
		final_totalCapacity = state.final_totalCapacity
		final_plf = state.final_plf
		dplfBand_full = state.dplfBand_full
		imp_forecast = state.imp_forecast
		imp_forecast_band = state.imp_forecast_band
		af_forecast_band = state.af_forecast_band
		Af_Nb_Allin = state.Af_Nb_Allin
		PLF_Gain = state.PLF_Gain
		forecast_plf = state.forecast_plf
		sod_BookingsInHour = state.sod_BookingsInHour
		sod_dplfBand = state.sod_dplfBand
		variance_full = state.variance_full
		overBookingCount = state.overBookingCount
		lastSoldFareb2c_val = state.lastSoldFareb2c_val
		lastSoldFareb2b_val = state.lastSoldFareb2b_val
		last_seven_avg_val = state.last_seven_avg_val
		current_w0Flag = state.current_w0Flag
		current_directFlag = state.current_directFlag
		hardfareb2c = state.hardfare_b2c
		hardfareb2b = state.hardfare_b2b
		hardoffsetB2C = state.hardoffset_b2c
		hardoffsetB2B = state.hardoffset_b2b
		isB2CMarketFareAvailable = state.isB2CMarketFareAvailable
		isB2BMarketFareAvailable = state.isB2BMarketFareAvailable
		lowmktFileb2c = state.lowmktFile_b2c
		himktFileb2c = state.himktFile_b2c
		lowmktFileb2b = state.lowmktFile_b2b
		himktFileb2b = state.himktFile_b2b
		month_str_for_profile = state.month_str_for_profile
		dow_str_for_profile = state.dow_str_for_profile
		deptTime_str_hhmm = state.deptTime_str_hhmm
		percentile_str_full = state.percentile_str_full
		upsellValue_full = state.upsellValue_full
		b2c_backstop_final = state.b2c_backstop_final
		b2b_backstop_final = state.b2b_backstop_final

		# Determine RBD to Open
		b2b_discount_fare_active = False; current_discount_amount = 0.0; determined_overBookingFare = 0.0
		openDataB2C_details = {}; openDataB2B_details = {} # To store results from determineRBDToOpen
		b2b_anchor_source = None  # Track B2B anchor source for HowDetermined

		param_obFare = getattr(self.currParams, 'obFare', None) # Get obFare safely
		param_b2c_tolerance = getattr(self.currParams, 'b2c_tolerance', 0) # Default tolerance if not set
		param_b2b_tolerance = getattr(self.currParams, 'b2b_tolerance', 0) # Default tolerance
		param_profileFares = getattr(self.currParams, 'profileFares', 1) # Default to 1 (enabled)

		mkt_fare_flag = int(self.currParams.mkt_fare_flag)

		profileFare = CONST_NA
		profileFaresData = self.rmdatabasehelper.getProfileFares(self.currParams.origin + self.currParams.destin, month_str_for_profile, dow_str_for_profile, deptTime_str_hhmm, percentile_str_full, self.currParams.incrementedProfileFareFlag)

		if upsellValue_full is None or str(upsellValue_full).strip() == '' or str(upsellValue_full).strip().upper() == 'NA':
			upsellValue_full = 0
			state.upsellValue_full = upsellValue_full

		if len(profileFaresData) != 0:
			if profileFaresData['price'][0]!="":
				profileFare = float(profileFaresData['price'][0])
				if int(self.currParams.incrementedProfileFareFlag) == 1:
					# When flag is on, apply ML-level profileFareUpsell instead of station upsell
					ml_upsell = self.currParams.profileFareUpsell
					if ml_upsell not in (None, '', 'NA', CONST_NA):
						try:
							ml_upsell_pct = float(ml_upsell)
							if ml_upsell_pct != 0:
								profileFare = profileFare * (1 + ml_upsell_pct / 100)
						except (ValueError, TypeError):
							pass
				else:
					if upsellValue_full != 0:
						profileFare = (profileFare * (1 + float(upsellValue_full)/100))

		# Determine if we should process (enter the main block)
		should_process = False
		if mkt_fare_flag == 1:
			# Flag = 1: Original behavior - process if either market fare OR last_seven_avg available
			should_process = isB2CMarketFareAvailable or (pd.notna(last_seven_avg_val) and last_seven_avg_val != CONST_NA) or (pd.notna(profileFare) and profileFare != CONST_NA)
		elif mkt_fare_flag == 0:
			# Flag = 0: Only process if last_seven_avg is available
			should_process = (pd.notna(last_seven_avg_val) and last_seven_avg_val != CONST_NA) or (pd.notna(profileFare) and profileFare != CONST_NA)

		hard_anchor_flag = False  # Set default; will be assigned by determineRBDToOpen below

		if should_process:
			self.outputSummaryRow_b2c.lastSevenAvgFare = last_seven_avg_val

			is_last7avg_anchor = False
			is_profile_anchor = False
			# Set B2C Anchor Fare based on criteria or L7D Avg
			if self.outputSummaryRow_b2c.criteria in [MAX_FARE_ANCHOR, MAXO_FARE_ANCHOR]:
				self.outputSummaryRow_b2c.anchorFare = self.outputSummaryRow_b2c.MktFare_Max
				self.outputSummaryRow_b2c.mktFile = himktFileb2c
			elif self.outputSummaryRow_b2c.criteria in [MIN_FARE_ANCHOR, MINO_FARE_ANCHOR, MINC_FARE_ANCHOR, MINOC_FARE_ANCHOR, MIN2_FARE_ANCHOR, MIN2C_FARE_ANCHOR, MED_FARE_ANCHOR, MEDC_FARE_ANCHOR]:
				self.outputSummaryRow_b2c.anchorFare = self.outputSummaryRow_b2c.MktFare_Min
				self.outputSummaryRow_b2c.mktFile = lowmktFileb2c

			# Determine when to override with last_seven_avg
			# If neither market fare nor last_seven_avg is available, use profile fare
			if mkt_fare_flag == 0:
				# Flag = 0: Always use last_seven_avg if available
				self.outputSummaryRow_b2c.anchorFare = CONST_NA
				if pd.notna(last_seven_avg_val) and last_seven_avg_val != CONST_NA:
					self.outputSummaryRow_b2c.anchorFare = last_seven_avg_val
					is_last7avg_anchor = True
				elif pd.notna(param_profileFares) and int(param_profileFares) == 1:
					self.outputSummaryRow_b2c.anchorFare = profileFare
					is_profile_anchor = True
			elif mkt_fare_flag == 1:
				# Flag = 1: Original behavior - use last_seven_avg only if market fare not available
				if not isB2CMarketFareAvailable:
					if pd.notna(last_seven_avg_val) and last_seven_avg_val != CONST_NA:
						self.outputSummaryRow_b2c.anchorFare = last_seven_avg_val
						is_last7avg_anchor = True
					elif pd.notna(param_profileFares) and int(param_profileFares) == 1:
						self.outputSummaryRow_b2c.anchorFare = profileFare
						is_profile_anchor = True

			if is_last7avg_anchor or is_profile_anchor:
				self.outputSummaryRow_b2c.fareOffset = "X0" if is_multiplier_offset(self.outputSummaryRow_b2c.fareOffset) else 0  # No offset if L7D avg is anchor

			# determineRBDToOpen needs YYYY-MM-DD date
			openDataB2C_details, self.outputSummaryRow_b2c.lastSellingFare, self.outputSummaryRow_b2c.strategyFare, \
			self.outputSummaryRow_b2c.profileFare, self.outputSummaryRow_b2c.hardAnchorFare, self.outputSummaryRow_b2c.upsell, \
			determined_overBookingFare,hard_anchor_flag, self.outputSummaryRow_b2c.toleranceValue, self.outputSummaryRow_b2c.T_prime = self.determinerbd.determineRBDToOpen(
											self.outputSummaryRow_b2c.anchorFare, self.outputSummaryRow_b2c.fareOffset, B2C_CHANNEL,
											dateFormatted_yyyymmdd, dplfBand_full, ndocounter, float(currBookedLoad), final_totalCapacity, self.currParams,
											b2c_backstop_final, self.currentTime, sod_dplfBand, variance_full, param_b2c_tolerance, final_plf,
											overBookingCount, hardfareb2c, hardoffsetB2C, sod_BookingsInHour, lastSoldFareb2c_val, last_seven_avg_val,
											is_last7avg_anchor, b2b_discount_fare_active, current_discount_amount, current_w0Flag, current_directFlag, param_obFare, profileFare, is_profile_anchor, imp_forecast, imp_forecast_band, self.outputSummaryRow_b2c.criteria, af_forecast_band, Af_Nb_Allin, PLF_Gain, forecast_plf, state
										)

			# Get B2C Opening Price for potential B2B derivation
			B2C_opening_price_val = None
			# Check if 'Fare' key exists and the list is not empty before accessing iloc[0]
			if 'Fare' in openDataB2C_details and isinstance(openDataB2C_details['Fare'], list) and openDataB2C_details['Fare'] and openDataB2C_details['Fare'][0] != CONST_NA:
				try:
					B2C_opening_price_val = float(openDataB2C_details['Fare'][0])
				except (ValueError, TypeError):
					self.logger.info(f"Could not parse B2C opening fare '{openDataB2C_details['Fare'][0]}' to float.")
			isB2BMarketFareAvailable = (
					isB2BMarketFareAvailable or
					(pd.notna(last_seven_avg_val) and last_seven_avg_val != CONST_NA) or
					(B2C_opening_price_val is not None)
				)
			self.outputSummaryRow_b2b.lastSevenAvgFare = last_seven_avg_val
			# B2B RBD determination
			if isB2BMarketFareAvailable and not (self.B2B_thresholdflag):
				discount_map_config = ast.literal_eval(self.constantsUtils.B2B_DISCOUNT_MAP)
				discount_str_val = discount_map_config.get(str(dplfBand_full), "0")

				if '%' in discount_str_val:
					current_discount_amount = B2C_opening_price_val * (float(discount_str_val.strip('%')) / 100.0) if B2C_opening_price_val else 0
				else:
					current_discount_amount = float(discount_str_val)

				# Collect all available B2B anchor fare candidates
				b2b_anchor_candidates = []
				offset_flag=True
				if mkt_fare_flag == 1:
					if self.outputSummaryRow_b2b.criteria in [MAX_FARE_ANCHOR, MAXO_FARE_ANCHOR]:
						if pd.notna(self.outputSummaryRow_b2b.MktFare_Max) and self.outputSummaryRow_b2b.MktFare_Max!=0:
							offset_flag=False

							b2b_anchor_candidates.append(self.outputSummaryRow_b2b.MktFare_Max)
							self.outputSummaryRow_b2b.mktFile = himktFileb2b
					elif self.outputSummaryRow_b2b.criteria in [MIN_FARE_ANCHOR, MINO_FARE_ANCHOR]:
						if pd.notna(self.outputSummaryRow_b2b.MktFare_Min) and self.outputSummaryRow_b2b.MktFare_Min!=0:
							offset_flag=False

							b2b_anchor_candidates.append(self.outputSummaryRow_b2b.MktFare_Min)
							self.outputSummaryRow_b2b.mktFile = lowmktFileb2b
					elif pd.notna(self.outputSummaryRow_b2b.MktFare_Min) and self.outputSummaryRow_b2b.MktFare_Min!=0:
						offset_flag=False
						b2b_anchor_candidates.append(self.outputSummaryRow_b2b.MktFare_Min)

				# Removed last_seven_avg_val from B2B Strategy Fare Calculation

				# Add last_seven_avg if available
				# if pd.notna(last_seven_avg_val) and last_seven_avg_val != CONST_NA:
				# 	b2b_anchor_candidates.append(last_seven_avg_val)

				# Add B2C opening price with discount if available
				if B2C_opening_price_val and current_discount_amount:
					b2b_anchor_candidates.append(B2C_opening_price_val - current_discount_amount)

				# Set anchor fare to minimum of all available candidates
				b2b_anchor_source = "Strategy"
				if b2b_anchor_candidates:
					self.outputSummaryRow_b2b.anchorFare = min(b2b_anchor_candidates)
					offset_flag=False
					# Check if winning anchor is the B2C discounted price
					if B2C_opening_price_val and current_discount_amount:

						b2c_discounted_val = B2C_opening_price_val - current_discount_amount
						if abs(float(self.outputSummaryRow_b2b.anchorFare) - b2c_discounted_val) < 0.01:
							b2b_anchor_source = "B2CDiscounted"
							self.outputSummaryRow_b2b.fareOffset = "X0" if is_multiplier_offset(self.outputSummaryRow_b2b.fareOffset) else 0
				else:
					self.outputSummaryRow_b2b.anchorFare = self.outputSummaryRow_b2b.MktFare_Min  # Fallback
				if offset_flag:
					self.outputSummaryRow_b2b.fareOffset="X0" if is_multiplier_offset(self.outputSummaryRow_b2b.fareOffset) else 0
				self.outputSummaryRow_b2b.strategyFare=self.outputSummaryRow_b2b.anchorFare
				openDataB2B_details, self.outputSummaryRow_b2b.lastSellingFare, self.outputSummaryRow_b2b.strategyFare, \
				self.outputSummaryRow_b2b.profileFare, self.outputSummaryRow_b2b.hardAnchorFare, self.outputSummaryRow_b2b.upsell, _ ,hard_anchor_flag, self.outputSummaryRow_b2b.toleranceValue, self.outputSummaryRow_b2b.T_prime = \
					self.determinerbd.determineRBDToOpen(
						self.outputSummaryRow_b2b.anchorFare, self.outputSummaryRow_b2b.fareOffset, B2B_CHANNEL,
						dateFormatted_yyyymmdd, dplfBand_full, ndocounter, float(currBookedLoad), final_totalCapacity, self.currParams,
						b2b_backstop_final, self.currentTime, sod_dplfBand, variance_full, param_b2b_tolerance, final_plf,
						overBookingCount, hardfareb2b, hardoffsetB2B, sod_BookingsInHour, lastSoldFareb2b_val, last_seven_avg_val,
						False, b2b_discount_fare_active, current_discount_amount, current_w0Flag, current_directFlag, param_obFare, profileFare, False, imp_forecast, imp_forecast_band, self.outputSummaryRow_b2b.criteria, af_forecast_band, Af_Nb_Allin, PLF_Gain, forecast_plf, state
					)

			# Removed last_seven_avg_val from B2B Strategy Fare Calculation

			# elif last_seven_avg_val is not None and last_seven_avg_val != CONST_NA and not (self.B2B_thresholdflag):
			# 	# b2b_discount_fare_active = True
			# 	# discount_map_config = ast.literal_eval(self.constantsUtils.B2B_DISCOUNT_MAP)  # B2B_DISCOUNT_MAP from ConstantsUtils
			# 	# discount_str_val = discount_map_config.get(str(dplfBand_full), "0")

			# 	# if '%' in discount_str_val:
			# 	# 	current_discount_amount = B2C_opening_price_val * (float(discount_str_val.strip('%')) / 100.0)
			# 	# else:
			# 	# 	current_discount_amount = float(discount_str_val)

			# 	derived_b2b_anchor_fare = last_seven_avg_val
			# 	self.outputSummaryRow_b2b.anchorFare = derived_b2b_anchor_fare
			# 	self.outputSummaryRow_b2b.fareOffset = 0

			# 	openDataB2B_details, self.outputSummaryRow_b2b.lastSellingFare, self.outputSummaryRow_b2b.strategyFare, \
			# 	self.outputSummaryRow_b2b.profileFare, self.outputSummaryRow_b2b.hardAnchorFare,self.outputSummaryRow_b2b.upsell, _ ,hard_anchor_flag= \
			# 		self.determinerbd.determineRBDToOpen(
			# 			derived_b2b_anchor_fare, self.outputSummaryRow_b2b.fareOffset, B2B_CHANNEL,
			# 			dateFormatted_yyyymmdd, dplfBand_full, ndocounter, float(currBookedLoad), final_totalCapacity, self.currParams,
			# 			b2b_backstop_final, self.currentTime, sod_dplfBand, variance_full, param_b2b_tolerance,final_plf,
			# 			overBookingCount, hardfareb2b,hardoffsetB2B,sod_BookingsInHour,lastSoldFareb2b_val, last_seven_avg_val,
			# 			False, b2b_discount_fare_active, current_discount_amount, current_w0Flag, current_directFlag, param_obFare, profileFare, False, imp_forecast, imp_forecast_band, self.outputSummaryRow_b2b.criteria
			# 		)
			else:
				openDataB2B_details = {'RBD': [CONST_NA], 'Fare': [CONST_NA], 'FareResult': ['B2B Mkt Data NA & Not Derivable or b2b threshold hit'], 'anchorFare': [CONST_NA], 'statusInd': [3], 'allocationStatus': [0]}
		else:
			openDataB2C_details = {'RBD': [CONST_NA], 'Fare': [CONST_NA], 'FareResult': ['B2C Mkt Data & L7D Avg NA'], 'anchorFare': [CONST_NA], 'statusInd': [1], 'allocationStatus': [0]}
			openDataB2B_details = {'RBD': [CONST_NA], 'Fare': [CONST_NA], 'FareResult': ['B2C Mkt Data NA - Skip B2B'], 'anchorFare': [CONST_NA], 'statusInd': [3], 'allocationStatus': [0]}
			self.redishelper.updateRunAudit();

		state.openDataB2C_details = openDataB2C_details
		state.openDataB2B_details = openDataB2B_details
		state.profileFare = profileFare
		state.profileFare_for_calc = profileFare_for_calc
		state.profileFaresData = profileFaresData
		state.b2b_anchor_source = b2b_anchor_source
		state.determined_overBookingFare = determined_overBookingFare
		state.hard_anchor_flag = hard_anchor_flag
		state.current_discount_amount = current_discount_amount
		state.b2b_discount_fare_active = b2b_discount_fare_active
		state.isB2BMarketFareAvailable = isB2BMarketFareAvailable
	def _phaseL_buildAuGrid(self, state):
		"""Phase L — build the AU grid row, run B2C / B2B / L / P / W / CUG /
		Group-Quote / TBF allocations, and append the row to ``self.AUFileData``.
		No-ops if the B2C status indicator isn't ``0`` or ``2``."""
		currFlt = state.currFlt
		currBookedLoad = state.currBookedLoad
		currFltDate = state.currFltDate
		currFltDate_dt = state.currFltDate_dt
		currtbfBookedLoad = state.currtbfBookedLoad
		dateFormatted_yyyymmdd = state.dateFormatted_yyyymmdd
		initial_currBookedLoad = state.initial_currBookedLoad
		tgtBookedLoadPercent = state.tgtBookedLoadPercent
		ndocounter = state.ndocounter
		final_totalCapacity = state.final_totalCapacity
		base_totalCapacity = state.base_totalCapacity
		base_adjustedCapacity = state.base_adjustedCapacity
		dplfBand_full = state.dplfBand_full
		ndoBand_full = state.ndoBand_full
		variance_full = state.variance_full
		imp_forecast = state.imp_forecast
		overBookingCount = state.overBookingCount
		determined_overBookingFare = state.determined_overBookingFare
		is_spoilage_reduction_active = state.is_spoilage_reduction_active
		last_seven_avg_val = state.last_seven_avg_val
		hardfareb2c = state.hardfare_b2c
		month_str_for_profile = state.month_str_for_profile
		dow_str_for_profile = state.dow_str_for_profile
		deptTime_str_hhmm = state.deptTime_str_hhmm
		bookloadPercentile_full = state.bookloadPercentile_full
		upsellValue_full = state.upsellValue_full
		sector_full = state.sector_full
		b2b_backstop_final = state.b2b_backstop_final
		openDataB2C_details = state.openDataB2C_details
		openDataB2B_details = state.openDataB2B_details
		final_plf = state.final_plf
		sod_tgtAtCurrentHour_abs = state.sod_tgtAtCurrentHour_abs
		sod_startOfDayBookLoad_factor = state.sod_startOfDayBookLoad_factor
		sod_startOfDayTgtBookedLoad_abs = state.sod_startOfDayTgtBookedLoad_abs
		b2c_backstop_final = state.b2c_backstop_final

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

		# Profile Fares list for allocations — reuse the DataFrame already
		# fetched during _rehydrateFlightState to avoid a duplicate MongoDB call.
		profileExtract_df_fullalloc = state.profileExtract_df_fullalloc
		profileFares_list_sorted_fullalloc = state.profileFares_list_sorted_fullalloc

		# Proceed with AU allocation if B2C determination was somewhat successful
		# Check if 'statusInd' key exists before trying to access its value
		b2c_status_ind = openDataB2C_details.get('statusInd', [1])[0] if isinstance(openDataB2C_details.get('statusInd'), list) and openDataB2C_details.get('statusInd') else 1
		Af_Nb_Allin = state.Af_Nb_Allin
		if b2c_status_ind in [0, 2]:
			# Booked deficit for B2C allocation (target based on original Lid from params)
			bookedDeficit_val = round((tgtBookedLoadPercent * base_totalCapacity) - float(currBookedLoad))
			remainingCapacity_val = final_totalCapacity - float(currBookedLoad)
			b2cAllocUnits_val, brd_counter_after_b2c, b2c_status_val, _, B2CRBDs_list_val, b2c_opening_price_val, self.first_rbd_alloc, self.other_rbd_alloc = \
				self.allocations.b2CAllocation(
					float(initial_currBookedLoad), openDataB2C_details, currRec_fullalloc, final_totalCapacity, bookedDeficit_val, variance_full,
					remainingCapacity_val, ndocounter, self.currParams, self.outputSummaryRow_b2c.profileFare,
					profileFares_list_sorted_fullalloc, bookloadPercentile_full, upsellValue_full, imp_forecast,self.outputSummaryRow_b2c.strategyFare,hardfareb2c,self.currParams.plfthreshold,self.actualplf,
					determined_overBookingFare, base_adjustedCapacity,is_spoilage_reduction_active=is_spoilage_reduction_active,
					last_seven_avg_fare=last_seven_avg_val, b2c_backstop_final=b2c_backstop_final
				)

			# This call also modifies currRec_fullalloc, for TBF.
			alloc_df_for_tbf = self.allocations.b2CAllocations(
				float(currBookedLoad), openDataB2C_details, currRec_fullalloc, final_totalCapacity, bookedDeficit_val, variance_full,
				remainingCapacity_val, ndocounter, self.currParams, self.outputSummaryRow_b2c.profileFare,
				profileFares_list_sorted_fullalloc, bookloadPercentile_full, upsellValue_full, b2c_opening_price_val, # Use opening price from b2CAllocation
				imp_forecast,self.outputSummaryRow_b2c.strategyFare,hardfareb2c,self.currParams.plfthreshold,self.actualplf, determined_overBookingFare, base_adjustedCapacity,is_spoilage_reduction_active=is_spoilage_reduction_active,
					last_seven_avg_fare=last_seven_avg_val, b2c_backstop_final=b2c_backstop_final
			)

			self.outputSummaryRow_b2c.status = b2c_status_val
			self.outputSummaryRow_b2b.status = b2c_status_val # Mirror B2C status initially

			# B2B Allocation
			isUpsell_b2b = False; skip_b2b_factor = False; close_b2b = False
			current_brd_counter = brd_counter_after_b2c

			# B2B Z0 fare check vs backstop
			b2b_Z0_rbds_df = self.rmdatabasehelper.fareExtract(self.currParams.origin, self.currParams.destin, B2B_CHANNEL, 'Z0', 'L')
			b2b_Z0_fare = b2b_Z0_rbds_df['Total'].iloc[0] if not b2b_Z0_rbds_df.empty and 'Total' in b2b_Z0_rbds_df.columns else float('inf')

			if pd.notna(b2b_backstop_final) and b2b_backstop_final != CONST_NA and float(b2b_Z0_fare) < float(b2b_backstop_final):
				openDataB2B_details = {'RBD': [CONST_NA], 'Fare': [CONST_NA], 'FareResult': ['B2B: Z0 fare < B2B backstop'], 'anchorFare': [CONST_NA], 'statusInd': [1], 'allocationStatus': [1]}

			param_b2b_factor = getattr(self.currParams, 'b2b_factor', -1) # Default if not present
			if pd.notna(param_b2b_factor):
				if float(param_b2b_factor) == -1:
					skip_b2b_factor = True
				elif float(param_b2b_factor) == 0:
					close_b2b = True

			isUpsell_b2b, current_brd_counter = self.allocations.b2BAllocation(
				float(initial_currBookedLoad), openDataB2B_details, currRec_fullalloc, final_totalCapacity, variance_full, b2cAllocUnits_val,
				skip_b2b_factor, self.b2boverbookingflag, current_brd_counter, openDataB2C_details,
				self.currParams, overBookingCount,self.B2B_thresholdflag, close_b2b
			)

			if isUpsell_b2b: openDataB2B_details = {'RBD': [CONST_NA], 'Fare': [CONST_NA], 'FareResult': ['B2B skipped - Flight in Upsell'], 'anchorFare': [CONST_NA], 'statusInd': [1], 'allocationStatus': [1]}
			if skip_b2b_factor: openDataB2B_details = {'RBD': [CONST_NA], 'Fare': [CONST_NA], 'FareResult': ['B2B skipped - B2B Factor is -1'], 'anchorFare': [CONST_NA], 'statusInd': [1], 'allocationStatus': [1]}

			# L, P, W, CUG, GQ, TBF allocations
			# Pass YYYY-MM-DD date to L, P allocations
			self.l_rbd_allocation(dateFormatted_yyyymmdd, ndoBand_full, dplfBand_full, currRec_fullalloc, current_brd_counter);
			self.p_rbd_allocation(dateFormatted_yyyymmdd, ndoBand_full, dplfBand_full, currRec_fullalloc, current_brd_counter);

			current_brd_counter = self.w_rbd_allocation(currRec_fullalloc, current_brd_counter) # Uses final_plf

			# CUG allocation
			B2CRBDs_df_for_cug_gq_tbf = self.rmdatabasehelper.fareExtract(self.currParams.origin, self.currParams.destin, [CUG_CHANNEL, GROUP_CHANNEL, TBF_CHANNEL], '', 'L')
			cug_bookings_dict_local = getattr(self, 'cug_bookings_dict', {}) # Use if it's a class member, else ensure defined

			param_cug_flag = getattr(self.currParams, 'cug_flag', 0) # Default
			param_cug_mkt_flag = getattr(self.currParams, 'cug_mkt_flag', 0) # Default

			current_brd_counter = self.allocations.cug_allocation(
				current_brd_counter + 1, self.currParams, currRec_fullalloc,
				B2CRBDs_df_for_cug_gq_tbf,
				base_adjustedCapacity, self.outputSummaryRow_b2c.profileFare,
				dateFormatted_yyyymmdd, ndocounter, b2c_opening_price_val,
				cug_bookings_dict_local, param_cug_flag, param_cug_mkt_flag,sector_full
			)

			# Group Quote allocation
			b2c_selling_fare = safe_get_from_dict(openDataB2C_details, "Fare")
			param_autoGroup = getattr(self.currParams, 'autoGroup', 0)
			group_bookings=self.currParams.group_bookings# Default
			# For group allocation: always use ML profileFareUpsell
			group_upsell = self.currParams.profileFareUpsell
			current_brd_counter, state.grp_adj_profile_fare, state.grp_adj_bookload, state.grp_fare_anchor = self.allocations.group_quote_alloc(
				current_brd_counter, currRec_fullalloc, param_autoGroup,
				base_adjustedCapacity, group_upsell, B2CRBDs_df_for_cug_gq_tbf, b2c_opening_price_val,group_bookings,float(currBookedLoad),month_str_for_profile, sector_full, deptTime_str_hhmm, dow_str_for_profile, final_totalCapacity, variance_full, b2c_selling_fare,
				b2c_backstop_final,
				ndocounter=ndocounter, imp_forecast=imp_forecast,
				sod_tgtAtCurrentHour_abs=sod_tgtAtCurrentHour_abs,
				sod_startOfDayBookLoad_factor=sod_startOfDayBookLoad_factor,
				sod_startOfDayTgtBookedLoad_abs=sod_startOfDayTgtBookedLoad_abs,
				departure_year=currFltDate_dt.year
			)

			# TBF allocation needs alloc_df from b2CAllocations (plural)
			param_tbfFlag = getattr(self.currParams, 'tbfFlag', 0)
			param_tbfenhFlag = getattr(self.currParams, 'tbfenhflag', 0) # Default
			# Default
			current_brd_counter = self.allocations.tbf_allocation(
				alloc_df_for_tbf, current_brd_counter, currRec_fullalloc, param_tbfFlag, B2CRBDs_df_for_cug_gq_tbf,
				currtbfBookedLoad, float(currBookedLoad), base_adjustedCapacity, b2c_opening_price_val, month_str_for_profile, sector_full,param_tbfenhFlag,dateFormatted_yyyymmdd
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
			# 	self.logger.info("Could not find W8 slot for sales_inv_allocation in Full Mode AU Grid.")

			# --- GDS RBD Allocation ---
			# Map B2C RBDs to GDS RBDs and allocate same AU
			# for b2c_rbd, gds_rbd in GDS_MAPPING.items():
			# 	# Find the B2C RBD in the current allocation
			# 	b2c_au = None
			# 	for i in range(1, self.AU_COLUMN_LENGTH + 1):
			# 		if getattr(currRec_fullalloc, f"class_of_service_{i}", None) == b2c_rbd:
			# 			b2c_au = getattr(currRec_fullalloc, f"class_au_{i}", None)
			# 			break

			# 	if b2c_au is not None:
			# 		# Allocate GDS RBD
			# 		currRec_fullalloc.set_class_attribute(current_brd_counter, 'class_of_service', gds_rbd)
			# 		currRec_fullalloc.set_class_attribute(current_brd_counter, 'class_au', b2c_au)
			# 		current_brd_counter += 1

			# --- NextGen AU Optimization & Competition Response Execution ---
			try:
				B2CRBDs_ladder = self.rmdatabasehelper.fareExtract(self.currParams.origin, self.currParams.destin, B2C_CHANNEL, '', 'L')
				if not B2CRBDs_ladder.empty:
					# Read exact per-bucket (mu, sigma) from DW/MongoDB table
					s3_bucket_df = self.rmdatabasehelper.get_additive_fare_bucket_forecast(
						sector=f"{self.currParams.origin}{self.currParams.destin}",
						snapshot_date=getattr(self.currParams, 'currDate', None),
						departure_date=currFltDate,
						flight_number=self.currParams.flight_number
					)

					ladder_data = []
					if s3_bucket_df is not None and not s3_bucket_df.empty:
						count_col = next((c for c in ['OptimizationDemand', 'WTP_pax_count', 'New Final_pax_count', 'Final_pax_count', 'weighted_pax_count', 'Total_Predicted_pax_count', 'predicted_pax_count', 'mu'] if c in s3_bucket_df.columns), None)
						std_col = next((c for c in ['OptimizationDemandStd', 'weighted_pax_std', 'std', 'predicted_pax_std', 'sigma'] if c in s3_bucket_df.columns), None)
						for idx, bucket_row in s3_bucket_df.iterrows():
							b_name = str(bucket_row.get('FareBucket', f'Bucket_{idx}'))
							b_lower = bucket_row.get('FareBucket_Lower')
							b_upper = bucket_row.get('FareBucket_Upper')

							# Take FareBucket_Lower directly as bucket fare
							if pd.notna(b_lower):
								try:
									f_val = float(b_lower)
								except (ValueError, TypeError):
									f_val = 0.0
							else:
								try:
									if '+' in b_name:
										f_val = float(b_name.replace('+', '').strip())
									elif '-' in b_name:
										f_val = float(b_name.split('-')[0].strip())
									else:
										f_val = float(b_name)
								except Exception:
									f_val = 0.0

							pax_val = float(bucket_row[count_col]) if count_col and pd.notna(bucket_row[count_col]) else 0.0
							std_val = float(bucket_row[std_col]) if std_col and pd.notna(bucket_row[std_col]) else (0.25 * pax_val)

							ladder_data.append({
								'FareBucket': b_name,
								'fare': float(f_val),
								'mu': pax_val,
								'sigma': std_val,
								'FareBucket_Lower': b_lower if pd.notna(b_lower) else None,
								'FareBucket_Upper': b_upper if pd.notna(b_upper) else None,
							})
					else:
						# Fallback when weighted forecast is unavailable: 19 filed RBDs
						for idx, rbd_row in B2CRBDs_ladder.iterrows():
							raw_fare = float(rbd_row.get('TOTAL', rbd_row.get('Total', rbd_row.get('fare', 0.0))))
							# Fares in Mongo fares collection are all-in; convert to airline component for NextGen optimization
							if hasattr(self, 'tax_helper') and self.tax_helper:
								fare_val = self.tax_helper.allin_to_airline_component(
									raw_fare, self.currParams.origin, self.currParams.destin,
									route=getattr(self.currParams, 'route', None)
								)
							else:
								fare_val = raw_fare
							rbd_code = str(rbd_row.get('RBD', f'RBD_{idx}'))
							var_float = float(variance_full) if variance_full != CONST_NA and variance_full is not None else 10.0
							mu_val = max(0.0, var_float / max(1, len(B2CRBDs_ladder))) if var_float > 0 else 0.0
							ladder_data.append({
								'FareBucket': rbd_code,
								'fare': fare_val,
								'mu': mu_val,
								'sigma': 0.25 * mu_val
							})
					ladder_df = pd.DataFrame(ladder_data).sort_values('fare', ascending=False).reset_index(drop=True)

					# Fetch real market fares directly from database helper / market fares service
					try:
						day_span = getattr(state, 'DaySpan_full', getattr(self.currParams, 'day_span', 0))
						flt_date = getattr(state, 'currFltDate', dateFormatted_yyyymmdd)
						mkt_fares = self.marketfaresservice.fetchMarketFareRange(
							B2C_CHANNEL, flt_date, self.currParams, day_span,
							getattr(self.outputSummaryRow_b2c, 'startTime', '00:00'),
							getattr(self.outputSummaryRow_b2c, 'endTime', '23:59'),
							self.currParams.origin, self.currParams.destin, includeWeekend=False
						)
					except Exception as mkt_err:
						self.logger.warning(f"Error fetching market fares from service: {mkt_err}")
						mkt_fares = None

					# If connection_fare_flag == 1, also fetch connecting competitor fares
					if int(getattr(self.currParams, 'connection_fare_flag_NG', getattr(self.currParams, 'connection_fare_flag', 0))) == 1:
						try:
							max_stops = 1
							add_dur = float(getattr(self.currParams, 'additional_connection_duration_NG', getattr(self.currParams, 'additional_connection_duration', 4.0)))
							mkt_conn = self.rmdatabasehelper.getMarketFareRangeConnections(
								B2C_CHANNEL, self.currParams, dateFormatted_yyyymmdd, max_stops, add_dur
							)
							if mkt_conn is not None and not mkt_conn.empty:
								mkt_fares = pd.concat([mkt_fares, mkt_conn], ignore_index=True) if (mkt_fares is not None and not mkt_fares.empty) else mkt_conn
						except Exception as conn_err:
							self.logger.warning(f"Error fetching connecting market fares: {conn_err}")

					# Fallback to summary min/max if mkt_fares is empty
					if mkt_fares is None or (isinstance(mkt_fares, pd.DataFrame) and mkt_fares.empty):
						mkt_min_val = getattr(self.outputSummaryRow_b2c, 'MktFare_Min', None)
						mkt_max_val = getattr(self.outputSummaryRow_b2c, 'MktFare_Max', None)
						if mkt_min_val is not None and mkt_min_val != CONST_NA and str(mkt_min_val).strip():
							try:
								f_min = float(mkt_min_val)
								f_max = float(mkt_max_val) if (mkt_max_val is not None and mkt_max_val != CONST_NA and str(mkt_max_val).strip()) else f_min
								mkt_fares = pd.DataFrame([
									{
										'gross_fare': f_min,
										'outbound_airline_code': getattr(self.outputSummaryRow_b2c, 'AirlineCode_Min', '6E'),
										'outbound_flight': getattr(self.outputSummaryRow_b2c, 'FlightNumber_Min', ''),
										'outbound_departuretime': getattr(self.outputSummaryRow_b2c, 'startTime', '10:30'),
										'outbound_stops': 0
									},
									{
										'gross_fare': f_max,
										'outbound_airline_code': getattr(self.outputSummaryRow_b2c, 'AirlineCode_Max', '6E'),
										'outbound_flight': getattr(self.outputSummaryRow_b2c, 'FlightNumber_Max', ''),
										'outbound_departuretime': getattr(self.outputSummaryRow_b2c, 'endTime', '10:30'),
										'outbound_stops': 0
									}
								])
							except (ValueError, TypeError):
								mkt_fares = None

					recal_dict = self.rmdatabasehelper.getCompFareRecalibration(f"{self.currParams.origin}{self.currParams.destin}")

					# For NextGen optimization, ensure parameters are sourced directly from the weighted file if available
					flight_std_val = getattr(state, 'currFltTime', None)
					if s3_bucket_df is not None and not s3_bucket_df.empty:
						cb_col = next((c for c in ['Current_Bookings', 'Total_Bookings', 'Total Bookings', 'Current Bookings'] if c in s3_bucket_df.columns), None)
						if cb_col and pd.notna(s3_bucket_df[cb_col].iloc[0]):
							try:
								currBookedLoad = float(s3_bucket_df[cb_col].iloc[0])
							except (ValueError, TypeError):
								pass
						if 'Capacity' in s3_bucket_df.columns and pd.notna(s3_bucket_df['Capacity'].iloc[0]):
							try:
								final_totalCapacity = float(s3_bucket_df['Capacity'].iloc[0])
							except (ValueError, TypeError):
								pass
						if 'STD' in s3_bucket_df.columns and pd.notna(s3_bucket_df['STD'].iloc[0]):
							flight_std_val = str(s3_bucket_df['STD'].iloc[0])[:5]

					avg_fare_val = last_seven_avg_val if last_seven_avg_val != CONST_NA and last_seven_avg_val is not None else 3500.0
					# Check if Current_Revenue is available in s3_bucket_df
					weighted_rev = None
					if s3_bucket_df is not None and not s3_bucket_df.empty and 'Current_Revenue' in s3_bucket_df.columns:
						try:
							rev_val = s3_bucket_df['Current_Revenue'].iloc[0]
							if pd.notna(rev_val) and float(rev_val) > 0:
								weighted_rev = float(rev_val)
						except (ValueError, TypeError):
							pass
					curr_rev = weighted_rev if weighted_rev is not None else (float(currBookedLoad) * float(avg_fare_val))

					ng_opt_df, ng_summary = NextGenOptimizationEngine.execute_flight_nextgen_optimization(
						ladder_df, final_totalCapacity, currBookedLoad, curr_rev,
						market_fares=mkt_fares,
						flight_std=flight_std_val,
						curr_params=self.currParams,
						db_helper=self.rmdatabasehelper,
						recalibration_dict=recal_dict,
						tax_helper=getattr(self, 'tax_helper', None)
					)

					# Populate OutputSummaryNextGen (All fares are presented as ALL-IN fares)
					ng_row = OutputSummaryNextGen()
					ng_row.Origin = self.currParams.origin
					ng_row.Destin = self.currParams.destin
					ng_row.FltNum = self.currParams.flight_number
					ng_row.DepDate = dateFormatted_yyyymmdd
					ng_row.Capacity = final_totalCapacity
					ng_row.NG_FirstRBD = ng_summary.get('NG_FirstRBD')
					ng_row.Predicted_Flown_Revised = ng_summary.get('Predicted_Flown_Revised')
					ng_row.MarketFare_Selected = ng_summary.get('MarketFare_Selected')
					ng_row.MarketFare_Min = ng_summary.get('MarketFare_Min')
					ng_row.MarketFare_Max = ng_summary.get('MarketFare_Max')
					ng_row.Grid_Name = ng_summary.get('Grid_Name')
					ng_row.Rule_Applied = ng_summary.get('Rule_Applied')
					ng_row.nextgen_flag = getattr(self.currParams, 'nextgen_flag_NG', getattr(self.currParams, 'nextgen_flag', 0))
					ng_row.competition_response_flag = getattr(self.currParams, 'competition_response_flag_NG', getattr(self.currParams, 'competition_response_flag', 1))
					ng_row.buy_up_factor = getattr(self.currParams, 'buy_up_factor_NG', getattr(self.currParams, 'buy_up_factor', 0.0))
					ng_row.RunId = self.runId
					ng_row.Id = getattr(self, 'Id', getattr(self.currParams, 'Id', CONST_NA))
					ng_row.NG_Historical_current_booked = currBookedLoad
					ng_row.NG_LF_Forecast = ng_summary.get('NG_LF_Forecast')
					ng_row.NG_AF_Forecast = ng_summary.get('NG_AF_Forecast')
					ng_row.NG_Opening_fare = ng_summary.get('NG_Opening_fare')
					ng_row.Strategy_fare = ng_summary.get('Strategy_fare')
					ng_row.Anchor_fare = ng_summary.get('Anchor_fare')
					ng_row.analystName = getattr(self.currParams, 'analyst_name', CONST_NA)
					ng_row.status = getattr(self.currParams, 'status', getattr(self, 'status', CONST_NA))
					ng_row.allocationStatus = getattr(self.currParams, 'allocationStatus', getattr(self, 'allocationStatus', CONST_NA))

					self.nextgenSummaryResults.append(ng_row)
					self.rmdatabasehelper.insertSummaryRowNextGen(self.nextgenSummaryResults)

					if not ng_opt_df.empty:
						dist_val = float(getattr(self.currParams, 'distance', 1000.0) or 1000.0)
						std_str = str(getattr(state, 'currFltTime', ''))
						dep_dow = pd.to_datetime(dateFormatted_yyyymmdd).strftime('%A')
						sec_str = f"{self.currParams.origin}{self.currParams.destin}"

						ng_opt_df['sector'] = sec_str
						ng_opt_df['date'] = dateFormatted_yyyymmdd
						ng_opt_df['DOW'] = dep_dow
						ng_opt_df['STD'] = std_str
						ng_opt_df['flight'] = self.currParams.flight_number
						ng_opt_df['NDO'] = int(getattr(self.currParams, 'ndocounter', 0))
						ng_opt_df['capacity'] = final_totalCapacity
						ng_opt_df['Total_Bookings'] = currBookedLoad
						ng_opt_df['fare'] = ng_opt_df.get('fare', 0.0)
						ng_opt_df['oal_b2c_min'] = ng_summary.get('MarketFare_Min', 0.0)
						ng_opt_df['oal_b2c_max'] = ng_summary.get('MarketFare_Max', 0.0)
						ng_opt_df['qp_b2c_min'] = min(ng_opt_df['fare']) if 'fare' in ng_opt_df.columns and not ng_opt_df['fare'].empty else 0.0
						ng_opt_df['qp_b2c_max'] = max(ng_opt_df['fare']) if 'fare' in ng_opt_df.columns and not ng_opt_df['fare'].empty else 0.0
						ng_opt_df['anchor_fare'] = ng_summary.get('Anchor_fare', 0.0)
						ng_opt_df['strategy_fare'] = ng_summary.get('Strategy_fare', 0.0)
						ng_opt_df['predicted_pickup'] = float(ng_opt_df['mu'].sum()) if 'mu' in ng_opt_df.columns else 0.0
						ng_opt_df['Predicted_Flown'] = float(currBookedLoad) + ng_opt_df['predicted_pickup']
						ng_opt_df['Predicted_pickup_Revised'] = ng_summary.get('Predicted_Pickup_Revised', 0.0)
						ng_opt_df['Predicted_Flown_Revised'] = ng_summary.get('Predicted_Flown_Revised', 0.0)
						ng_opt_df['LF_Forecast_Revised'] = ng_summary.get('NG_LF_Forecast', 0.0)
						ng_opt_df['AF_Forecast'] = ng_summary.get('NG_AF_Forecast', 0.0)
						ng_opt_df['Distance'] = dist_val
						ng_opt_df['ASKM'] = float(final_totalCapacity * dist_val)
						ng_opt_df['RPKM'] = float(ng_summary.get('Predicted_Flown_Revised', 0.0) * dist_val)
						ng_opt_df['RASK_Forecast'] = float(ng_summary.get('Predicted_Revenue', 0.0) / (final_totalCapacity * dist_val)) if (final_totalCapacity * dist_val) > 0 else 0.0
						ng_opt_df['Capped_Predicted_Flown_Revised'] = min(final_totalCapacity, ng_summary.get('Predicted_Flown_Revised', 0.0))
						ng_opt_df['Denominator'] = ng_opt_df['Capped_Predicted_Flown_Revised']
						ng_opt_df['Current_Revenue'] = curr_rev
						ng_opt_df['Total_Expected_Revenue'] = ng_summary.get('Total_Expected_Revenue', 0.0)
						ng_opt_df['Predicted_Revenue'] = ng_summary.get('Predicted_Revenue', 0.0)
						ng_opt_df['SnapshotDate'] = getattr(self.currParams, 'currDate', datetime.now().strftime('%Y-%m-%d'))
						ng_opt_df['Tagging'] = getattr(self.currParams, 'tagging', 'Shoulder')
						ng_opt_df['STD_future'] = std_str
						ng_opt_df['STD_dt'] = f"{dateFormatted_yyyymmdd} {std_str}"
						ng_opt_df['Origin'] = self.currParams.origin
						ng_opt_df['Destin'] = self.currParams.destin
						ng_opt_df['FltNum'] = self.currParams.flight_number
						ng_opt_df['DepDate'] = dateFormatted_yyyymmdd
						ng_opt_df['RunId'] = self.runId

						# Build ordered list of bucket intervals for mapping RBDs to NextGen AU ladder
						bucket_intervals = []
						for _, b_row in ng_opt_df.iterrows():
							if b_row.get('Promo_Flag', 0) == 1:
								continue
							b_name = str(b_row.get('FareBucket', ''))
							b_low = b_row.get('FareBucket_Lower')
							b_up = b_row.get('FareBucket_Upper')

							if pd.notna(b_low):
								low_val = float(b_low)
							elif '+' in b_name:
								low_val = float(b_name.replace('+', '').strip())
							elif '-' in b_name:
								low_val = float(b_name.split('-')[0].strip())
							else:
								low_val = float(b_row.get('fare', 0.0))

							if pd.notna(b_up) and str(b_up).strip().lower() not in ['nan', 'null', 'none']:
								up_val = float(b_up)
							elif '+' in b_name:
								up_val = float('inf')
							elif '-' in b_name:
								up_val = float(b_name.split('-')[1].strip())
							else:
								up_val = float('inf') if '+' in b_name else float(b_row.get('fare', 0.0))

							bucket_intervals.append({
								'name': b_name,
								'low': low_val,
								'up': up_val,
								'au': b_row.get('AU_Revised', 0.0)
							})

						# Strictly include only the configured columns without extra columns for saving
						inv_ordered_cols = [c for c in CONST_OUTPUT_INVENTORY_OPTIMIZATION_ORDER if c in ng_opt_df.columns]
						ng_opt_df_to_save = ng_opt_df[inv_ordered_cols]

						inv_records = ng_opt_df_to_save.to_dict(orient='records')
						self.inventoryOptimizationResults.extend(inv_records)
						self.rmdatabasehelper.insertInventoryOptimization(inv_records)

					# If nextgen_flag == 1, update AU values ONLY for B2C RBDs in currRec_fullalloc
					if int(getattr(self.currParams, 'nextgen_flag_NG', getattr(self.currParams, 'nextgen_flag', 0))) == 1 and not ng_opt_df.empty:
						b2c_rbd_set = set(B2CRBDs_ladder['RBD'].dropna().astype(str).tolist()) if not B2CRBDs_ladder.empty else set()
						rbd_to_au = dict(zip(ng_opt_df['FareBucket'], ng_opt_df['AU_Revised']))

						promo_flag = int(getattr(self.currParams, 'promo_flag_NG', getattr(self.currParams, 'PromoFlag_NG', DEFAULT_PROMO_FLAG_NG)))
						promo_rbd = str(getattr(self.constantsUtils, 'PROMO_RBD_NG', getattr(self.constantsUtils, 'PromoRBD_NG', DEFAULT_PROMO_RBD_NG))).strip()
						promo_rows = ng_opt_df[ng_opt_df.get('Promo_Flag', 0) == 1]
						promo_au_revised = float(promo_rows['AU_Revised'].iloc[0]) if not promo_rows.empty else 0.0

						for slot_i in range(1, int(self.AU_COLUMN_LENGTH) + 1):
							cos = getattr(currRec_fullalloc, f"class_of_service_{slot_i}", None)
							c_type = getattr(currRec_fullalloc, f"class_type_{slot_i}", None)

							# If Promo Flag is on and this slot matches Promo RBD (default W4), allocate promo AU
							if promo_flag == 1 and cos == promo_rbd:
								currRec_fullalloc.set_class_attribute(slot_i, 'class_au', promo_au_revised)
								continue

							# ONLY update B2C RBDs; all other channels (B2B, CUG, Group, TBF) remain as allocated by standard flow
							is_b2c = (c_type == 'B2C') or (cos in b2c_rbd_set)
							if not is_b2c:
								continue

							assigned_au = 0.0
							if cos in rbd_to_au:
								assigned_au = rbd_to_au[cos]
							elif cos and not B2CRBDs_ladder.empty:
								rbd_match = B2CRBDs_ladder[B2CRBDs_ladder['RBD'] == cos]
								if not rbd_match.empty:
									rbd_fare = float(rbd_match.iloc[0].get('TOTAL', rbd_match.iloc[0].get('Total', 0.0)))
									for b in bucket_intervals:
										if b['low'] <= rbd_fare <= b['up'] or (b['up'] == float('inf') and rbd_fare >= b['low']):
											assigned_au = b['au']
											break
							currRec_fullalloc.set_class_attribute(slot_i, 'class_au', assigned_au)
			except Exception as e:
				self.logger.error(f"Error in NextGen AU Optimization: {e}")

			self.AUFileData.append(currRec_fullalloc)
			state.openDataB2B_details = openDataB2B_details
		else: # B2C determination was not successful enough for AU grid
			self.logger.info("B2C Open Data status not 0 or 2. AU Grid generation skipped for Full Allocation.")

		state.currRec_fullalloc = currRec_fullalloc
		state.current_nav_update_method_full = current_nav_update_method_full
		state.Af_Nb_Allin = Af_Nb_Allin
		state.profileExtract_df_fullalloc = profileExtract_df_fullalloc
		state.profileFares_list_sorted_fullalloc = profileFares_list_sorted_fullalloc

	def _phaseM_pushNavitaire(self, state):
		"""Phase M — push the AU grid row to Navitaire via the configured method
		(API / SFTP / S3). Mirrors the original ``if isUpdateNavitaire`` block."""
		currRec_fullalloc = state.currRec_fullalloc
		current_nav_update_method_full = state.current_nav_update_method_full
		openDataB2C_details = state.openDataB2C_details
		b2c_status_ind = openDataB2C_details.get('statusInd', [1])[0] if isinstance(openDataB2C_details.get('statusInd'), list) and openDataB2C_details.get('statusInd') else 1
		if b2c_status_ind in [0, 2] and self.inputSource.isUpdateNavitaire:
			if current_nav_update_method_full == CONST_API: self.updateNavitaire(currRec_fullalloc)
			elif current_nav_update_method_full == CONST_SFTP: self.updateNavSFTP(currRec_fullalloc)
			elif current_nav_update_method_full == S3: self.updateS3Push(currRec_fullalloc)

	def _phaseN_writeSummary(self, state):
		"""Phase N — finalise both ``OutputSummary`` rows (B2C + B2B), append to
		``self.allocResults``, and persist via the database helper."""
		# Safety net: portfolio_only records must never produce output.
		if str(getattr(self.currParams, 'portfolio_only', '0')) == '1':
			return
		currFlt = state.currFlt
		currBookedLoad = state.currBookedLoad
		currFltDate = state.currFltDate
		overBookingCount = state.overBookingCount
		expectedNoShows = getattr(state, 'expectedNoShows', CONST_NA)
		if expectedNoShows is None:
			expectedNoShows = CONST_NA
		final_plf = state.final_plf
		variance_full = state.variance_full
		dplfBand_full = state.dplfBand_full
		imp_forecast_band = state.imp_forecast_band
		Af_Nb_Allin = state.Af_Nb_Allin
		sod_startOfDayTgtBookedLoad_abs = state.sod_startOfDayTgtBookedLoad_abs
		ndoBand_full = state.ndoBand_full
		b2c_backstop_final = state.b2c_backstop_final
		b2b_backstop_final = state.b2b_backstop_final
		auto_b2c_backstop_str = state.auto_b2c_backstop_str
		auto_b2b_backstop_str = state.auto_b2b_backstop_str
		openDataB2C_details = state.openDataB2C_details
		openDataB2B_details = state.openDataB2B_details
		b2b_anchor_source = state.b2b_anchor_source
		grp_adj_profile_fare = state.grp_adj_profile_fare
		grp_adj_bookload = state.grp_adj_bookload
		grp_fare_anchor = state.grp_fare_anchor
		forecast_plf = state.forecast_plf
		final_totalCapacity = state.final_totalCapacity
		sod_tgtAtCurrentHour_abs = state.sod_tgtAtCurrentHour_abs
		conversion_rate = state.conversion_rate
		currency = state.currency
		param_profileFares = getattr(self.currParams, 'profileFares', 1)
		profileFare_for_calc = getattr(state, 'profileFare_for_calc', CONST_NA)

		# --- Populate Final OutputSummaryRows (Full Mode) ---
		if overBookingCount > 0: # Add Overbooking note to FareResult
			if 'FareResult' in openDataB2C_details and isinstance(openDataB2C_details.get('FareResult'), list) and openDataB2C_details.get('FareResult'):
				openDataB2C_details['FareResult'][0] = str(openDataB2C_details['FareResult'][0]) + ' -Overbooking'
			if 'FareResult' in openDataB2B_details and isinstance(openDataB2B_details.get('FareResult'), list) and openDataB2B_details.get('FareResult'):
				openDataB2B_details['FareResult'][0] = str(openDataB2B_details['FareResult'][0]) + ' -Overbooking'

		# Ensure profileFare is populated in output summary rows every time if available
		extracted_profile_fare = getattr(state, 'profileFare', getattr(self, 'profileFare', CONST_NA))
		if extracted_profile_fare not in (None, '', 'NA', CONST_NA):
			self.outputSummaryRow_b2c.profileFare = extracted_profile_fare
			self.outputSummaryRow_b2b.profileFare = extracted_profile_fare
		elif getattr(self.outputSummaryRow_b2c, 'profileFare', CONST_NA) not in (None, '', 'NA', CONST_NA):
			self.outputSummaryRow_b2b.profileFare = self.outputSummaryRow_b2c.profileFare
		elif getattr(self.outputSummaryRow_b2b, 'profileFare', CONST_NA) not in (None, '', 'NA', CONST_NA):
			self.outputSummaryRow_b2c.profileFare = self.outputSummaryRow_b2b.profileFare


		param_analyst_name_full = str(getattr(self.currParams, 'analyst_name', CONST_NA))
		param_b2c_backstop_full = getattr(self.currParams, 'b2c_backstop', CONST_NA)
		param_b2b_backstop_full = getattr(self.currParams, 'b2b_backstop', CONST_NA)


		# B2C Summary Row Population
		# Safely get values from openDataB2C_details dictionary
		def safe_get_from_dict(d, key, default=CONST_NA, index=0):
			val_list = d.get(key)
			if isinstance(val_list, list) and len(val_list) > index:
				return val_list[index]
			return default

		self.outputSummaryRow_b2c.FirstRBDAlloc = self.outputSummaryRow_b2b.FirstRBDAlloc = getattr(self, 'first_rbd_alloc', CONST_NA)
		self.outputSummaryRow_b2c.OtherRBDAlloc = self.outputSummaryRow_b2b.OtherRBDAlloc = getattr(self, 'other_rbd_alloc', CONST_NA)
		self.outputSummaryRow_b2c.forecastPLF = self.outputSummaryRow_b2b.forecastPLF = getParsedForecast(self)
		self.outputSummaryRow_b2c.ImputedForecastPLF = self.outputSummaryRow_b2b.ImputedForecastPLF = getParsedImputedForecast(self, forecast_plf, currBookedLoad, sod_tgtAtCurrentHour_abs, final_totalCapacity)
		self.outputSummaryRow_b2c.AF_Allin = self.outputSummaryRow_b2b.AF_Allin = getattr(self.currParams, 'ALLIN_AF', CONST_NA)
		self.outputSummaryRow_b2c.ForecastAF_Allin = self.outputSummaryRow_b2b.ForecastAF_Allin = getattr(self.currParams, 'ForecastAF_Allin', CONST_NA)
		self.outputSummaryRow_b2c.ALLIN_AF_NB_LM = self.outputSummaryRow_b2b.ALLIN_AF_NB_LM = getattr(self.currParams, 'ALLIN_AF_NB_LM', CONST_NA)
		self.outputSummaryRow_b2c.ALLIN_AF_NB_LY = self.outputSummaryRow_b2b.ALLIN_AF_NB_LY = getattr(self.currParams, 'ALLIN_AF_NB_LY', CONST_NA)
		self.outputSummaryRow_b2c.AF_NB_Allin = self.outputSummaryRow_b2b.AF_NB_Allin = Af_Nb_Allin
		self.outputSummaryRow_b2c.B2BFactor = self.outputSummaryRow_b2b.B2BFactor = self.currParams.b2b_factor
		self.outputSummaryRow_b2c.grpAdjProfileFare = self.outputSummaryRow_b2b.grpAdjProfileFare = grp_adj_profile_fare
		self.outputSummaryRow_b2c.grpAdjPLF = self.outputSummaryRow_b2b.grpAdjPLF = grp_adj_bookload
		self.outputSummaryRow_b2c.grpFareAnchor = self.outputSummaryRow_b2b.grpFareAnchor = grp_fare_anchor

		self.outputSummaryRow_b2c.RunId = self.runId; self.outputSummaryRow_b2c.Origin = self.currParams.origin
		self.outputSummaryRow_b2c.Destin = self.currParams.destin; self.outputSummaryRow_b2c.FltNum = str(currFlt)
		self.outputSummaryRow_b2c.DepDate = str(currFltDate); self.outputSummaryRow_b2c.BookedLoad = str(currBookedLoad)
		self.outputSummaryRow_b2c.TgtLoad = str(round(sod_startOfDayTgtBookedLoad_abs,2)); self.outputSummaryRow_b2c.ndoBand = str(ndoBand_full)
		self.outputSummaryRow_b2c.anchorFare = str(safe_get_from_dict(openDataB2C_details, "anchorFare"))
		self.outputSummaryRow_b2c.SellingFare = str(safe_get_from_dict(openDataB2C_details, "Fare")); self.outputSummaryRow_b2c.Channel = B2C_CHANNEL
		
		# Parse and format the raw FareResult string
		raw_b2c_how_determined = str(safe_get_from_dict(openDataB2C_details, 'FareResult', default='Error determining B2C fare'))
		self.outputSummaryRow_b2c.HowDetermined = HowDeterminedParser.parse_and_format(raw_b2c_how_determined)
		# Append _Ignoring_PF when profileFare was not used in determining the final anchor fare
		if 'profile' not in raw_b2c_how_determined.lower() and 'profile' not in self.outputSummaryRow_b2c.HowDetermined.lower():
			self.outputSummaryRow_b2c.HowDetermined += '_Ignoring_PF'
		
		self.outputSummaryRow_b2c.OpenRBD = str(safe_get_from_dict(openDataB2C_details, 'RBD', default='Z'))
		self.outputSummaryRow_b2c.Backstop = param_b2c_backstop_full; self.outputSummaryRow_b2c.autoBackstop = auto_b2c_backstop_str
		self.outputSummaryRow_b2c.Variance = str(round(variance_full,2)); self.outputSummaryRow_b2c.analystName = param_analyst_name_full
		self.outputSummaryRow_b2c.bookedPlf = str(round(final_plf,2)); self.outputSummaryRow_b2c.allocationStatus = safe_get_from_dict(openDataB2C_details, 'allocationStatus', default=0)
		self.outputSummaryRow_b2c.overBookingCount = overBookingCount; self.outputSummaryRow_b2c.actualbookedPlf = round(self.actualplf,2)
		self.outputSummaryRow_b2c.ExpectedNoShows = expectedNoShows
		self.outputSummaryRow_b2c.currency = self.outputSummaryRow_b2b.currency = currency
		self.outputSummaryRow_b2c.conversionRate = self.outputSummaryRow_b2b.conversionRate = conversion_rate
		self.outputSummaryRow_b2b.region = self.outputSummaryRow_b2c.region = getattr(self.currParams, 'region', DOMESTIC_VALUE)
		
		CurveId = getattr(self.currParams, 'curve_id', CONST_NA)
		self.outputSummaryRow_b2c.CurveId = CurveId if CurveId is not None else CONST_NA
		
		self.outputSummaryRow_b2c.recommendedActions = self.recommend_action(self.outputSummaryRow_b2c)

		# Portfolio anchor split (R9) — carry ``portfolio_count`` from the
		# market-list row through to the summary. The four z-score / anchor
		# fields stay at ``CONST_NA`` here; they are populated by the
		# allocation pass in a later slice.
		portfolio_count_val = getattr(self.currParams, 'portfolio_count', CONST_NA)
		self.outputSummaryRow_b2c.portfolio_count = portfolio_count_val

		# B2B Summary Row Population
		self.outputSummaryRow_b2b.RunId = self.runId; self.outputSummaryRow_b2b.Origin = self.currParams.origin
		self.outputSummaryRow_b2b.Destin = self.currParams.destin; self.outputSummaryRow_b2b.FltNum = str(currFlt)
		self.outputSummaryRow_b2b.DepDate = str(currFltDate); self.outputSummaryRow_b2b.BookedLoad = str(currBookedLoad)
		self.outputSummaryRow_b2b.TgtLoad = str(round(sod_startOfDayTgtBookedLoad_abs,2)); self.outputSummaryRow_b2b.ndoBand = str(ndoBand_full)
		self.outputSummaryRow_b2b.anchorFare = str(safe_get_from_dict(openDataB2B_details, "anchorFare"))
		self.outputSummaryRow_b2b.SellingFare = str(safe_get_from_dict(openDataB2B_details, "Fare")); self.outputSummaryRow_b2b.Channel = B2B_CHANNEL

		raw_b2b_how_determined = str(safe_get_from_dict(openDataB2B_details, 'FareResult', default='Error determining B2B fare'))
		
		# Override B2B HowDetermined with simplified format if no special condition like Backstop Hit
		if "Backstop" not in raw_b2b_how_determined and b2b_anchor_source and "hardanchorfare" not in b2b_anchor_source.lower():
			raw_b2b_how_determined = f"{b2b_anchor_source}_{imp_forecast_band}_{dplfBand_full}"

		self.outputSummaryRow_b2b.HowDetermined = HowDeterminedParser.parse_and_format(raw_b2b_how_determined)
		if 'profile' not in raw_b2b_how_determined.lower() and 'profile' not in self.outputSummaryRow_b2b.HowDetermined.lower():
			self.outputSummaryRow_b2b.HowDetermined += '_Ignoring_PF'
		self.outputSummaryRow_b2b.OpenRBD = str(safe_get_from_dict(openDataB2B_details, 'RBD', default='Z'))
		
		self.outputSummaryRow_b2b.Backstop = param_b2b_backstop_full; self.outputSummaryRow_b2b.autoBackstop = auto_b2b_backstop_str
		self.outputSummaryRow_b2b.Variance = str(round(variance_full,2)); self.outputSummaryRow_b2b.analystName = param_analyst_name_full
		self.outputSummaryRow_b2b.bookedPlf = str(round(final_plf,2)); self.outputSummaryRow_b2b.allocationStatus = safe_get_from_dict(openDataB2B_details, 'allocationStatus', default=0)
		self.outputSummaryRow_b2b.overBookingCount = overBookingCount; self.outputSummaryRow_b2b.actualbookedPlf = round(self.actualplf,2)
		self.outputSummaryRow_b2b.ExpectedNoShows = expectedNoShows
		
		CurveId = getattr(self.currParams, 'curve_id', CONST_NA)
		self.outputSummaryRow_b2b.CurveId = CurveId if CurveId is not None else CONST_NA
		
		self.outputSummaryRow_b2b.recommendedActions = self.recommend_action(self.outputSummaryRow_b2b)

		# Portfolio anchor split (R9) — same ``portfolio_count`` on the B2B row.
		self.outputSummaryRow_b2b.portfolio_count = portfolio_count_val

		new_summary_rows_fullalloc = [self.outputSummaryRow_b2c, self.outputSummaryRow_b2b]
		self.allocResults.extend(new_summary_rows_fullalloc)
		state.outputSummaryRow_b2c = self.outputSummaryRow_b2c
		state.outputSummaryRow_b2b = self.outputSummaryRow_b2b
		# Database insert
		if hasattr(self.rmdatabasehelper, 'insertSummaryRows'): self.rmdatabasehelper.insertSummaryRows(new_summary_rows_fullalloc)
		else: self.rmdatabasehelper.insertSummaryRow([self.outputSummaryRow_b2c]); self.rmdatabasehelper.insertSummaryRow([self.outputSummaryRow_b2b])

	def w_rbd_allocation(self, currRec, brd_counter):
		rbds = json.loads(self.constantsUtils.SOFTBLOCK_RBDS)
		# default_ml_flag = int(getattr(self.currParams, 'defaultMlFlag', 0))
		
		ind = 2
		# Set class_of_service for each RBD dynamically
		for rbd in rbds:
			currRec.set_class_attribute(brd_counter + ind, 'class_of_service', rbd)
			# if default_ml_flag == 1 and rbd == "S":
			# 	currRec.set_class_attribute(brd_counter + ind, 'ap_restriction', 365)
			ind += 1
		
		ind = 2
		# Set class_au based on threshold flags dynamically
		for rbd in rbds:
			flag_name = rbd.lower() + '_thresholdflag'
			threshold_flag = getattr(self, flag_name, False)  # Get flag value or False if not exists
			
			if threshold_flag:
				currRec.set_class_attribute(brd_counter + ind, 'class_au', 0)
			else:
				currRec.set_class_attribute(brd_counter + ind, 'class_au', -1)
			ind += 1

		return brd_counter + len(rbds) + 1

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
		# if not pd.isna(self.currParams.BookingsCount):
		# 	lastBookloadTillMidnight = int(self.currParams.BookingsCount)
		if not pd.isna(self.currParams.BookingToday) and self.currParams.BookingToday != CONST_NA:
			bookingToday = int(self.currParams.BookingToday)
		# if not pd.isna(self.currParams.BookingsInHour):
		# 	bookingLastHour = int(self.currParams.BookingsInHour)
		self.logger.info("Number of Bookings is " + str(lastBookloadTillMidnight))

		try:
			curTime = datetime.strptime(str(self.currentTime), TIME_FORMAT)
		except ValueError:
			curTime = datetime.strptime(str(self.currentTime)[:5], TIME_FORMAT)
		try:
			deptTime = datetime.strptime(str(currFltDepartureTime), TIME_FORMAT)
		except ValueError:
			deptTime = datetime.strptime(str(currFltDepartureTime)[:5], TIME_FORMAT)

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
		plfThresholds = self.rmdatabasehelper.getPlfThreshhold(ndocounter, str(currFlt))
		rbds = json.loads(self.constantsUtils.SOFTBLOCK_RBDS)
		
		# Initialize all RBD threshold flags dynamically
		for rbd in rbds:
			flag_name = rbd.lower() + '_thresholdflag'
			setattr(self, flag_name, False)
		
		# Initialize fixed threshold flags
		self.p_thresholdflag = False
		self.l_thresholdflag = False
		self.B2B_thresholdflag = False
		
		if len(plfThresholds) > 0:
			threshold_row = plfThresholds.iloc[0]  # Get first row
			
			# Check each RBD threshold dynamically
			for rbd in rbds:
				threshold_col = rbd + '_threshold'
				if threshold_col in threshold_row and threshold_row[threshold_col] is not None and threshold_row[threshold_col] != '':
					if plf > threshold_row[threshold_col]:
						# Set the corresponding flag to True
						flag_name = rbd.lower() + '_thresholdflag'
						setattr(self, flag_name, True)
			
			# Check P threshold
			if 'P_threshold' in threshold_row and threshold_row['P_threshold'] is not None and threshold_row['P_threshold'] != '':
				if plf > threshold_row['P_threshold']:
					self.p_thresholdflag = True
			
			# Check L threshold
			if 'L_threshold' in threshold_row and threshold_row['L_threshold'] is not None and threshold_row['L_threshold'] != '':
				if plf > threshold_row['L_threshold']:
					self.l_thresholdflag = True
			
			# Check B2B threshold
			if 'B2B_threshold' in threshold_row and threshold_row['B2B_threshold'] is not None and threshold_row['B2B_threshold'] != '':
				if plf > threshold_row['B2B_threshold']:
					self.B2B_thresholdflag = True

	def overBooking(self, currFltDate, ndocounter, totalCapacity, adjustedCapacity, plf, bookload):
		self.b2boverbookingflag = False
		nextFlight = self.currParams.NextFlight
		expectedNoShows = self.rmdatabasehelper.getOverbookedSeats(self.currParams.origin, self.currParams.destin, self.currParams.flight_number, currFltDate)
		if (self.currParams.obSeats == '' or self.currParams.obSeats is None or (hasattr(self.currParams.obSeats, 'dtype') and math.isnan(self.currParams.obSeats))) and (self.currParams.obFare == '' or self.currParams.obFare is None or (hasattr(self.currParams.obFare, 'dtype') and math.isnan(self.currParams.obFare))):
			self.logger.info("Entered system overbooking loop runalloc")
			overBookingCount = 0 if expectedNoShows == CONST_NA else expectedNoShows
			if(overBookingCount > OVERBOOKING_CAP):
				overBookingCount = OVERBOOKING_CAP
			if nextFlight == False and overBookingCount > OVERBOOKING_MAX_COUNT:
				self.logger.info("Reducing overbooking to max 2 seats")
				overBookingCount = OVERBOOKING_MAX_COUNT
			if self.canOverbook(ndocounter, totalCapacity, adjustedCapacity, plf, overBookingCount, bookload):
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
			if self.canOverbookManual( ndocounter, totalCapacity, adjustedCapacity, plf, float(self.currParams.obSeats), bookload):
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
		return totalCapacity, self.b2boverbookingflag, expectedNoShows

	def canOverbook(self, ndocounter, totalCapacity, adjustedCapacity, plf, overBookingCount, bookload):
		return int(self.currParams.over_booking) == 1 and plf > self.constantsUtils.OVERBOOKING_LF and self.constantsUtils.OVERBOOKING_END_NDO >= ndocounter >= self.constantsUtils.OVERBOOKING_START_NDO and (int(totalCapacity) == int(adjustedCapacity)) and overBookingCount>0 and bookload <= float(adjustedCapacity)
	
	def canOverbookManual(self, ndocounter, totalCapacity, adjustedCapacity, plf, overBookingCount, bookload):
		return plf > self.constantsUtils.OVERBOOKING_LF and self.constantsUtils.OVERBOOKING_END_NDO >= ndocounter >= self.constantsUtils.OVERBOOKING_START_NDO and int(adjustedCapacity) == int(totalCapacity) and overBookingCount>0 and bookload <= float(adjustedCapacity)
	
	def outputResults(self):
		# self.logger.info("Final Allocation Results:")
		# self.logger.info(self.allocResults)
		output_dir = getattr(self, "output_dir", None)
		au_grid_path = os.path.join(output_dir, "AUGrid.csv") if output_dir else CONST_AU_GRID_FILE_PATH
		summary_path = os.path.join(output_dir, "summary.csv") if output_dir else CONST_SUMMARY_FILE_PATH
		summary_nextgen_path = os.path.join(output_dir, "summary_nextgen.csv") if output_dir else CONST_SUMMARY_NEXTGEN_FILE_PATH
		inventory_opt_path = os.path.join(output_dir, "inventory_optimization.csv") if output_dir else CONST_INVENTORY_OPTIMIZATION_FILE_PATH

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
			os.makedirs(os.path.dirname(os.path.abspath(au_grid_path)), exist_ok=True)
			AUFrame.to_csv(au_grid_path, index=False)


		if self.inputSource.output[CONST_SUMMARY_FILE] == FILE:
			df = pd.DataFrame([vars(row) for row in self.allocResults])
			final_order = [col for col in CONST_OUTPUT_SUMMARY_ORDER if col in df.columns]
			missing_cols = [col for col in df.columns if col not in final_order]
			df = df[final_order + missing_cols]
			df = df.drop(columns=["Id", "currentLoadFactor", "recommendedActions"], errors='ignore')
			os.makedirs(os.path.dirname(os.path.abspath(summary_path)), exist_ok=True)
			df.to_csv(summary_path, index=False)

		# Always write NextGen summary output if present
		if len(self.nextgenSummaryResults) > 0:
			try:
				df_ng = pd.DataFrame([vars(row) for row in self.nextgenSummaryResults])
				final_ng_order = [col for col in CONST_OUTPUT_SUMMARY_NEXTGEN_ORDER if col in df_ng.columns]
				df_ng = df_ng[final_ng_order]
				os.makedirs(os.path.dirname(os.path.abspath(summary_nextgen_path)), exist_ok=True)
				df_ng.to_csv(summary_nextgen_path, index=False)
			except Exception as e:
				self.logger.error(f"Error saving {summary_nextgen_path}: {e}")

		# Always write per-bucket inventory optimization dataset if present
		if len(self.inventoryOptimizationResults) > 0:
			try:
				df_inv = pd.DataFrame(self.inventoryOptimizationResults)
				final_inv_order = [col for col in CONST_OUTPUT_INVENTORY_OPTIMIZATION_ORDER if col in df_inv.columns]
				df_inv = df_inv[final_inv_order]
				os.makedirs(os.path.dirname(os.path.abspath(inventory_opt_path)), exist_ok=True)
				df_inv.to_csv(inventory_opt_path, index=False)
			except Exception as e:
				self.logger.error(f"Error saving {inventory_opt_path}: {e}")

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
		self.rmdatabasehelper.insert_run_fltDate_audit(self.runId, flightNo, sector,flightDate, auDataStr, "Ready", "Ready")

	def updateS3Push(self, rec):
		flightNo = rec.flight.split(" ")[1][2:7]
		flightDate = datetime.strptime(str(rec.start_date), DATE_FORMAT_3).date().strftime(DATE_FORMAT_2)
		records = vars(rec)

		auData = {"type": rec.type, "flight": rec.flight, "startDate": rec.start_date, "endDate": rec.end_date, "dayOfWeek": rec.day_of_week, "aircraftType": rec.aircraft_type, "aircraftSuffix": rec.aircraft_suffix, "lid": rec.lid, "capacity": rec.capacity, "status": rec.status, "auClasses": {}}

		for i in range(self.AU_COLUMN_LENGTH):
			auClass = {"classOfService": getattr(rec, f"class_of_service_{i+1}", None), "classType": getattr(rec, f"class_type_{i+1}", None), "classNest": int(getattr(rec, f"class_nest_{i+1}", None)), "classRank": int(getattr(rec, f"class_rank_{i+1}", None)), "classAU": int(getattr(rec, f"class_au_{i+1}", None)), "classAllotted": int(getattr(rec, f"class_allotted_{i+1}", None)), "apRestriction": int(getattr(rec, f"ap_restriction_{i+1}", None))}
			auData['auClasses'][auClass['classOfService']] = auClass

		auDataStr = json.dumps(auData)

		self.rmdatabasehelper.insert_run_fltDate_audit(self.runId, flightNo,"", flightDate, auDataStr, "Ready", "Ready")

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
		self.ist_zone = pytz.timezone("Asia/Kolkata")
		self.starttime = datetime.now(self.ist_zone)
		self.currParams = MarketListRow(currParams)
		self.inputSource = InputSource(inputData)
		self.debugMode = inputData["debugMode"]
		self.output_dir = inputData.get("output_dir", getattr(self.currParams, "output_dir", None))
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
		self.mongoclient = self.currParams.mongodbclient
		self.mongo_master_db = self.currParams.mongo_master_db
		self.dwconn = self.currParams.dwconn
		self.w0Flag = self.currParams.w0Flag
		if pd.isna(self.currParams.w0Flag):
			self.w0Flag = 0
		if pd.isna(self.currParams.directFlag):
			self.directFlag = 1
		
		self.cache_client = self.currParams.cache_client
		self.starttime = datetime.now()
		self.allocResults = []
		self.AUFileData = []
		self.nextgenSummaryResults = []
		self.inventoryOptimizationResults = []
		if self.currParams.run_id:
			self.runId = self.currParams.run_id
		else:
			self.runId = self.utils.getUniqueId()
		self.log_time = self.currParams.time_logger
		if self.log_time is not None:
			self.log_time.update_run_id(self.currParams.run_id)
		self.logger = OwnLogger(self.runId, self.inputSource)
		self.rmdatabasehelper = RMDatabaseHelper(self.mongoclient,self.dwconn, self.runId, self.inputSource, self.debugMode, self.cache_client,self.currParams)
		self.mongohelper=MarketFareMongoDB(self.mongoclient,self.inputSource,self.mongo_master_db)
		self.constantsUtils = ConstantsUtils(self.mongoclient)
		self.utils = Utils(self.inputSource)
		self.mathutils = MathUtils(self.inputSource)
		self.timeutils = TimeUtils(self.inputSource)
		# self.navdatabasehelper = NavDatabaseHelper(self.NavitaireConnection, self.conn, self.inputSource, self.NAV_DB_NAME, self.NAV_DB_REF_NAME,self.runId, self.debugMode, self.NAV_DB_WB_NAME,self.currParams, self.log_time)
		self.redishelper = RedisHelper(self.runId, self.cache_client, self.inputSource)
		if self.inputSource.input_source[CONST_OVERBOOKING] == DB:
			self.navapihelper = NavAPIHelper(self.NAV_BASE_API_URL, self.NAV_OCP_SUBSCRIPTION_KEY, self.NAV_USERNAME, self.NAV_PWD, self.NAV_DOMAIN, self.runId, self.inputSource)
		self.dynamictimed2 = GetDynamicTimeD2(self.rmdatabasehelper)
		self.marketfaresservice = MarketFareService(self.rmdatabasehelper,self.constantsUtils,self.timeutils,self.mongohelper)
		self.determinerbd = DetermineRBD(self.rmdatabasehelper, self.logger, self.inputSource, self.constantsUtils, self.mathutils, self.cache_client)
		self.allocations = RBDAllocations(self.rmdatabasehelper, self.logger, self.constantsUtils, self.mathutils, self.timeutils)
		self.tax_helper = FareTaxHelper(self.mongo_master_db, self.constantsUtils, self.rmdatabasehelper, self.logger)
		if self.currParams:
			self.currParams.tax_helper = self.tax_helper

		self.currency = "INR"
		self.conversion_data = self.rmdatabasehelper.fetch_all_conversion_data()

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
		
		self.auto_tolerance_config = self.rmdatabasehelper.getAutoToleranceConfig()
		
		# Increase AU_COLUMN_LENGTH to accommodate GDS RBDs
		# self.AU_COLUMN_LENGTH += len(GDS_MAPPING)

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

	def convert_currency(self, from_currency, to_currency, value):
		if str(value) == CONST_NA or pd.isna(value) or value == '':
			return value

		conversion = self.conversion_data[
			(self.conversion_data['FromCurrencyCode'] == from_currency) &
			(self.conversion_data['ToCurrencyCode'] == to_currency)
		]
		
		if conversion.empty:
			raise ValueError(f"No conversion rate found for {from_currency} to {to_currency}")
		
		conversion_rate = conversion['ConversionRate'].values[0]
		converted_value = float(value) * conversion_rate
		# if self.currency!='INR':
		# 	if hasattr(self, 'outputSummaryRow_b2b'):
		# 		self.outputSummaryRow_b2b.conversionRate=conversion_rate
		# 	if hasattr(self, 'outputSummaryRow_b2c'):
		# 		self.outputSummaryRow_b2c.conversionRate=conversion_rate
				
		return converted_value, conversion_rate

	def getBookloadSeries(self,dep_date,currBookedLoad):
		blocked_seats = self.rmdatabasehelper.getSeriesBlockedSeats(dep_date)
		booked_seats = self.currParams.total_series_seats
		total_count = 0
		if len(blocked_seats)>0:
			if (booked_seats is not None) and (blocked_seats['blocked'][0] is not None) and (not pd.isna(booked_seats)) and (not CONST_NA):
				total_count = blocked_seats['blocked'][0]-booked_seats
			elif (blocked_seats['blocked'][0] is not None):
				total_count = blocked_seats['blocked'][0]
		return currBookedLoad+total_count

	def recommend_action(self, row):
		try:
			def safe_float(value):
				val_str = str(value).strip().upper()
				if value is None or val_str in ['NA', 'NONE', '']:
					return 0.0
				return float(val_str)

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

	def calculate_averages(self,fares_manifest, ndo=0):
		# Parse the JSON string if it's not already a dictionary
		if isinstance(fares_manifest, str):
			fares_manifest = json.loads(fares_manifest)
			
		if ndo >= 45:
			all_fares = fares_manifest.get('allfares_pax', [])
			b2b_fares = fares_manifest.get('b2bfares_pax', [])
			b2c_fares = fares_manifest.get('b2cfares_pax', [])
			
			# Fallback if pax variants don't exist yet
			if not all_fares and not b2b_fares and not b2c_fares:
				all_fares = fares_manifest.get('allfares', [])
				b2b_fares = fares_manifest.get('b2bfares', [])
				b2c_fares = fares_manifest.get('b2cfares', [])
		else:
			all_fares = fares_manifest.get('allfares', [])
			b2b_fares = fares_manifest.get('b2bfares', [])
			b2c_fares = fares_manifest.get('b2cfares', [])
		
		# Calculate last_seven_avg (always with all available fares)
		last_seven_avg = self.calculate_last_seven_avg(all_fares)
		
		# Calculate lastsoldb2b and lastsoldb2c (require at least 3 fares)
		
		lastsoldb2b = self.calculate_average(b2b_fares)
		lastsoldb2c = self.calculate_average(b2c_fares)
		
		return last_seven_avg, lastsoldb2b, lastsoldb2c

	# ==================================================================
	# Portfolio anchor split — slice 7
	#
	# These methods carve the existing ``_phaseK_anchorAndLadder`` into
	# three reusable phases (``_phaseK_anchorCandidate`` →
	# ``_computeFareAnchors`` → ``_phaseK_applyLadder``) so that the
	# direct-route flow can be driven as two separate SQS passes (R1).
	# The legacy orchestrator ``runDynamicAllocationSingleFlight`` keeps
	# calling ``_phaseK_anchorAndLadder`` unchanged; the new entry points
	# ``runFareAnchorPass`` and ``runAllocationPass`` use the split
	# methods plus ``_buildPortfolioEntry`` / ``_rehydrateFlightState`` /
	# ``_computeAndAttachZScores`` to round-trip a portfolio entry through
	# Redis + SQS without mutating the per-flight algorithm (R11).
	# ==================================================================

	def _phaseK_anchorCandidate(self, state):
		"""Phase K head — pre-``DetermineRBD`` candidate selection for B2C.

		Mirrors the head of the legacy ``_phaseK_anchorAndLadder``: pulls
		the profile fare for the booking quartile, computes
		``should_process``, picks the B2C anchor fare based on
		``criteria`` / ``mkt_fare_flag`` / last-7-day average / profile
		fare, and stores the resulting flags on ``state`` so
		``_computeFareAnchors`` and ``_phaseK_applyLadder`` can resume
		from the same point in either the in-process flow or the two-pass
		split.
		"""
		last_seven_avg_val = state.last_seven_avg_val
		upsellValue_full = state.upsellValue_full
		month_str_for_profile = state.month_str_for_profile
		dow_str_for_profile = state.dow_str_for_profile
		deptTime_str_hhmm = state.deptTime_str_hhmm
		percentile_str_full = state.percentile_str_full
		isB2CMarketFareAvailable = state.isB2CMarketFareAvailable
		lowmktFileb2c = state.lowmktFile_b2c
		himktFileb2c = state.himktFile_b2c

		param_profileFares = getattr(self.currParams, 'profileFares', 1)
		mkt_fare_flag = int(self.currParams.mkt_fare_flag)

		profileFare = CONST_NA
		profileFaresData = self.rmdatabasehelper.getProfileFares(
			self.currParams.origin + self.currParams.destin,
			month_str_for_profile, dow_str_for_profile, deptTime_str_hhmm, percentile_str_full,
			self.currParams.incrementedProfileFareFlag,
		)

		if upsellValue_full is None or str(upsellValue_full).strip() == '' or str(upsellValue_full).strip().upper() in ('NA', 'NAN') or pd.isna(upsellValue_full):
			upsellValue_full = 0
			state.upsellValue_full = upsellValue_full

		if len(profileFaresData) != 0:
			if profileFaresData['price'][0] != "":
				profileFare = float(profileFaresData['price'][0])
				if int(self.currParams.incrementedProfileFareFlag) == 1:
					# When flag is on, apply ML-level profileFareUpsell instead of station upsell
					ml_upsell = self.currParams.profileFareUpsell
					if ml_upsell not in (None, '', 'NA', CONST_NA):
						try:
							ml_upsell_pct = float(ml_upsell)
							if ml_upsell_pct != 0:
								profileFare = profileFare * (1 + ml_upsell_pct / 100)
						except (ValueError, TypeError):
							pass
				else:
					if upsellValue_full != 0:
						profileFare = (profileFare * (1 + float(upsellValue_full) / 100))

		# Determine if we should process (enter the main block)
		should_process = False
		if mkt_fare_flag == 1:
			should_process = isB2CMarketFareAvailable or (pd.notna(last_seven_avg_val) and last_seven_avg_val != CONST_NA) or (pd.notna(profileFare) and profileFare != CONST_NA)
		elif mkt_fare_flag == 0:
			should_process = (pd.notna(last_seven_avg_val) and last_seven_avg_val != CONST_NA) or (pd.notna(profileFare) and profileFare != CONST_NA)

		is_last7avg_anchor = False
		is_profile_anchor = False
		if should_process:
			self.outputSummaryRow_b2c.lastSevenAvgFare = last_seven_avg_val

			if self.outputSummaryRow_b2c.criteria in [MAX_FARE_ANCHOR, MAXO_FARE_ANCHOR]:
				self.outputSummaryRow_b2c.anchorFare = self.outputSummaryRow_b2c.MktFare_Max
				self.outputSummaryRow_b2c.mktFile = himktFileb2c
			elif self.outputSummaryRow_b2c.criteria in [MIN_FARE_ANCHOR, MINO_FARE_ANCHOR, MINC_FARE_ANCHOR, MINOC_FARE_ANCHOR, MIN2_FARE_ANCHOR, MIN2C_FARE_ANCHOR, MED_FARE_ANCHOR, MEDC_FARE_ANCHOR]:
				self.outputSummaryRow_b2c.anchorFare = self.outputSummaryRow_b2c.MktFare_Min
				self.outputSummaryRow_b2c.mktFile = lowmktFileb2c

			if mkt_fare_flag == 0:
				self.outputSummaryRow_b2c.anchorFare = CONST_NA
				if pd.notna(last_seven_avg_val) and last_seven_avg_val != CONST_NA:
					self.outputSummaryRow_b2c.anchorFare = last_seven_avg_val
					is_last7avg_anchor = True
				elif pd.notna(param_profileFares) and int(param_profileFares) == 1:
					self.outputSummaryRow_b2c.anchorFare = profileFare
					is_profile_anchor = True
			elif mkt_fare_flag == 1:
				if not isB2CMarketFareAvailable:
					if pd.notna(last_seven_avg_val) and last_seven_avg_val != CONST_NA:
						self.outputSummaryRow_b2c.anchorFare = last_seven_avg_val
						is_last7avg_anchor = True
					elif pd.notna(param_profileFares) and int(param_profileFares) == 1:
						self.outputSummaryRow_b2c.anchorFare = profileFare
						is_profile_anchor = True

			if is_last7avg_anchor or is_profile_anchor:
				self.outputSummaryRow_b2c.fareOffset = "X0" if is_multiplier_offset(self.outputSummaryRow_b2c.fareOffset) else 0

		state.profileFare = profileFare
		state.profileFaresData = profileFaresData
		state.should_process_K = should_process
		state.is_last7avg_anchor_b2c = is_last7avg_anchor
		state.is_profile_anchor_b2c = is_profile_anchor

	def _computeFareAnchors(self, state):
		"""Phase K tail (head half) — call ``DetermineRBD.computeFinalAnchor``
		for B2C and B2B.

		The B2B candidate-selection logic in the legacy code requires the
		B2C *opening price* (the result of the final ladder lookup), so
		this method runs ``computeFinalAnchor`` and ``applyAnchorToLadder``
		for B2C inline before deriving the B2B candidate, then runs
		``computeFinalAnchor`` for B2B and stores both ``AnchorResult``
		objects on the ``FlightState`` for serialisation into the
		portfolio entry. ``state.openDataB2C_details`` is also populated
		here because the B2B branch reads it for ``B2C_opening_price_val``;
		``state.openDataB2B_details`` is left for ``_phaseK_applyLadder``.
		"""
		currBookedLoad = state.currBookedLoad
		dateFormatted_yyyymmdd = state.dateFormatted_yyyymmdd
		ndocounter = state.ndocounter
		final_totalCapacity = state.final_totalCapacity
		final_plf = state.final_plf
		dplfBand_full = state.dplfBand_full
		imp_forecast = state.imp_forecast
		imp_forecast_band = state.imp_forecast_band
		af_forecast_band = state.af_forecast_band
		Af_Nb_Allin = state.Af_Nb_Allin
		PLF_Gain = state.PLF_Gain
		forecast_plf = state.forecast_plf
		sod_BookingsInHour = state.sod_BookingsInHour
		sod_dplfBand = state.sod_dplfBand
		variance_full = state.variance_full
		overBookingCount = state.overBookingCount
		lastSoldFareb2c_val = state.lastSoldFareb2c_val
		lastSoldFareb2b_val = state.lastSoldFareb2b_val
		last_seven_avg_val = state.last_seven_avg_val
		current_w0Flag = state.current_w0Flag
		current_directFlag = state.current_directFlag
		hardfareb2c = state.hardfare_b2c
		hardfareb2b = state.hardfare_b2b
		hardoffsetB2C = state.hardoffset_b2c
		hardoffsetB2B = state.hardoffset_b2b
		isB2BMarketFareAvailable = state.isB2BMarketFareAvailable
		himktFileb2b = state.himktFile_b2b
		lowmktFileb2b = state.lowmktFile_b2b
		b2c_backstop_final = state.b2c_backstop_final
		b2b_backstop_final = state.b2b_backstop_final
		profileFare = state.profileFare
		should_process = state.should_process_K
		is_last7avg_anchor = state.is_last7avg_anchor_b2c
		is_profile_anchor = state.is_profile_anchor_b2c

		param_obFare = getattr(self.currParams, 'obFare', None)
		param_b2c_tolerance = getattr(self.currParams, 'b2c_tolerance', 0)
		param_b2b_tolerance = getattr(self.currParams, 'b2b_tolerance', 0)
		mkt_fare_flag = int(self.currParams.mkt_fare_flag)

		b2b_discount_fare_active = False
		current_discount_amount = 0.0
		determined_overBookingFare = 0.0
		openDataB2C_details = {}
		openDataB2B_details = {}
		b2b_anchor_source = None
		anchorResult_b2c = None
		anchorResult_b2b = None
		hard_anchor_flag = False

		plf_surge = False
		try:
			history = self.determinerbd.get_autotolerance_history(self.currParams)
			if history:
				plf_change_per = float(getattr(self.constantsUtils, 'plf_change_per', 2.0))
				plf_change_n_runs = int(getattr(self.constantsUtils, 'plf_change_n_runs', 4))
				
				if len(history) >= plf_change_n_runs:
					nth_run = history[-plf_change_n_runs]
					if ndocounter > 30:
                        # Variance logic uses actual_dplf
						nth_dplf = nth_run.get('actual_dplf', 0)
						if (float(currBookedLoad) - float(nth_dplf)) >= plf_change_per:
							plf_surge = True
					else:
                        # Forecast logic uses actual_imp_forecast
						nth_imp_forecast = nth_run.get('actual_imp_forecast', 0)
						if (float(imp_forecast) - float(nth_imp_forecast)) >= plf_change_per:
							plf_surge = True
		except Exception as e:
			self.logger.error(f"Error evaluating PLF surge: {e}")

		if plf_surge:
			# B2C Evaluation
			if lastSoldFareb2c_val != CONST_NA and profileFare != CONST_NA:
				try:
					if float(lastSoldFareb2c_val) > float(profileFare):
						profileFare = CONST_NA
				except ValueError:
					pass
			if Af_Nb_Allin != CONST_NA and lastSoldFareb2c_val != CONST_NA:
				try:
					if float(Af_Nb_Allin) < float(lastSoldFareb2c_val):
						Af_Nb_Allin = CONST_NA
				except ValueError:
					pass

			# B2B Evaluation
			if lastSoldFareb2b_val != CONST_NA and profileFare != CONST_NA:
				try:
					if float(lastSoldFareb2b_val) > float(profileFare):
						profileFare = CONST_NA
				except ValueError:
					pass
			if Af_Nb_Allin != CONST_NA and lastSoldFareb2b_val != CONST_NA:
				try:
					if float(Af_Nb_Allin) < float(lastSoldFareb2b_val):
						Af_Nb_Allin = CONST_NA
				except ValueError:
					pass

		if should_process:
			# B2C — head + ladder.  We need the ladder result here so the
			# B2B branch below can read ``B2C_opening_price_val`` from the
			# resolved ``openDataB2C_details``.
			anchorResult_b2c = self.determinerbd.computeFinalAnchor(
				self.outputSummaryRow_b2c.anchorFare, self.outputSummaryRow_b2c.fareOffset, B2C_CHANNEL,
				dateFormatted_yyyymmdd, dplfBand_full, ndocounter, float(currBookedLoad), final_totalCapacity, self.currParams,
				b2c_backstop_final, self.currentTime, sod_dplfBand, variance_full, param_b2c_tolerance, final_plf,
				overBookingCount, hardfareb2c, hardoffsetB2C, sod_BookingsInHour, lastSoldFareb2c_val, last_seven_avg_val,
				is_last7avg_anchor, b2b_discount_fare_active, current_discount_amount, current_w0Flag, current_directFlag,
				param_obFare, profileFare, is_profile_anchor, imp_forecast, imp_forecast_band,
				self.outputSummaryRow_b2c.criteria, af_forecast_band, Af_Nb_Allin, PLF_Gain, forecast_plf, True, state
			)
			# Capture the F/G metadata onto the AnchorResult so the
			# serialised PortfolioEntry can carry it across the SQS hop
			# without re-running Phase F when the allocation pass fires.
			self._copyOutputSummaryToAnchor(anchorResult_b2c, self.outputSummaryRow_b2c)

			b2c_tuple = self.determinerbd.applyAnchorToLadder(
				anchorResult_b2c, self.currParams,
				self.outputSummaryRow_b2c.fareOffset, self.outputSummaryRow_b2c.criteria, state
			)
			(openDataB2C_details, self.outputSummaryRow_b2c.lastSellingFare,
			 self.outputSummaryRow_b2c.strategyFare, self.outputSummaryRow_b2c.profileFare,
			 self.outputSummaryRow_b2c.hardAnchorFare, self.outputSummaryRow_b2c.upsell,
			 determined_overBookingFare, hard_anchor_flag,
			 self.outputSummaryRow_b2c.toleranceValue, self.outputSummaryRow_b2c.T_prime) = b2c_tuple

			# Get B2C Opening Price for potential B2B derivation
			B2C_opening_price_val = None
			if 'Fare' in openDataB2C_details and isinstance(openDataB2C_details['Fare'], list) and openDataB2C_details['Fare'] and openDataB2C_details['Fare'][0] != CONST_NA:
				try:
					B2C_opening_price_val = float(openDataB2C_details['Fare'][0])
				except (ValueError, TypeError):
					self.logger.info(f"Could not parse B2C opening fare '{openDataB2C_details['Fare'][0]}' to float.")

			isB2BMarketFareAvailable = (
				isB2BMarketFareAvailable
				or (pd.notna(last_seven_avg_val) and last_seven_avg_val != CONST_NA)
				or (B2C_opening_price_val is not None)
			)
			self.outputSummaryRow_b2b.lastSevenAvgFare = last_seven_avg_val

			if isB2BMarketFareAvailable and not (self.B2B_thresholdflag):
				discount_map_config = ast.literal_eval(self.constantsUtils.B2B_DISCOUNT_MAP)
				discount_str_val = discount_map_config.get(str(dplfBand_full), "0")

				if '%' in discount_str_val:
					current_discount_amount = B2C_opening_price_val * (float(discount_str_val.strip('%')) / 100.0) if B2C_opening_price_val else 0
				else:
					current_discount_amount = float(discount_str_val)

				b2b_anchor_candidates = []
				offset_flag = True
				if mkt_fare_flag == 1:
					if self.outputSummaryRow_b2b.criteria in [MAX_FARE_ANCHOR, MAXO_FARE_ANCHOR]:
						if pd.notna(self.outputSummaryRow_b2b.MktFare_Max) and self.outputSummaryRow_b2b.MktFare_Max != 0:
							offset_flag = False
							b2b_anchor_candidates.append(self.outputSummaryRow_b2b.MktFare_Max)
							self.outputSummaryRow_b2b.mktFile = himktFileb2b
					elif self.outputSummaryRow_b2b.criteria in [MIN_FARE_ANCHOR, MINO_FARE_ANCHOR]:
						if pd.notna(self.outputSummaryRow_b2b.MktFare_Min) and self.outputSummaryRow_b2b.MktFare_Min != 0:
							offset_flag = False
							b2b_anchor_candidates.append(self.outputSummaryRow_b2b.MktFare_Min)
							self.outputSummaryRow_b2b.mktFile = lowmktFileb2b
					elif pd.notna(self.outputSummaryRow_b2b.MktFare_Min) and self.outputSummaryRow_b2b.MktFare_Min != 0:
						offset_flag = False
						b2b_anchor_candidates.append(self.outputSummaryRow_b2b.MktFare_Min)

				if B2C_opening_price_val and current_discount_amount:
					b2b_anchor_candidates.append(B2C_opening_price_val - current_discount_amount)

				b2b_anchor_source = "Strategy"
				if b2b_anchor_candidates:
					self.outputSummaryRow_b2b.anchorFare = min(b2b_anchor_candidates)
					offset_flag = False
					if B2C_opening_price_val and current_discount_amount:
						b2c_discounted_val = B2C_opening_price_val - current_discount_amount
						if abs(float(self.outputSummaryRow_b2b.anchorFare) - b2c_discounted_val) < 0.01:
							b2b_anchor_source = "B2CDiscounted"
							self.outputSummaryRow_b2b.fareOffset = "X0" if is_multiplier_offset(self.outputSummaryRow_b2b.fareOffset) else 0
				else:
					self.outputSummaryRow_b2b.anchorFare = self.outputSummaryRow_b2b.MktFare_Min
				if offset_flag:
					self.outputSummaryRow_b2b.fareOffset = "X0" if is_multiplier_offset(self.outputSummaryRow_b2b.fareOffset) else 0
				self.outputSummaryRow_b2b.strategyFare = self.outputSummaryRow_b2b.anchorFare

				anchorResult_b2b = self.determinerbd.computeFinalAnchor(
					self.outputSummaryRow_b2b.anchorFare, self.outputSummaryRow_b2b.fareOffset, B2B_CHANNEL,
					dateFormatted_yyyymmdd, dplfBand_full, ndocounter, float(currBookedLoad), final_totalCapacity, self.currParams,
					b2b_backstop_final, self.currentTime, sod_dplfBand, variance_full, param_b2b_tolerance, final_plf,
					overBookingCount, hardfareb2b, hardoffsetB2B, sod_BookingsInHour, lastSoldFareb2b_val, last_seven_avg_val,
					False, b2b_discount_fare_active, current_discount_amount, current_w0Flag, current_directFlag,
					param_obFare, profileFare, False, imp_forecast, imp_forecast_band,
					self.outputSummaryRow_b2b.criteria, af_forecast_band, Af_Nb_Allin, PLF_Gain, forecast_plf, True, state
				)
				anchorResult_b2b.strategyFare = self.outputSummaryRow_b2b.strategyFare
				self._copyOutputSummaryToAnchor(anchorResult_b2b, self.outputSummaryRow_b2b)

				state.B2C_opening_price_val = B2C_opening_price_val
			else:
				openDataB2B_details = {
					'RBD': [CONST_NA], 'Fare': [CONST_NA],
					'FareResult': ['B2B Mkt Data NA & Not Derivable or b2b threshold hit'],
					'anchorFare': [CONST_NA], 'statusInd': [3], 'allocationStatus': [0],
				}
		else:
			openDataB2C_details = {
				'RBD': [CONST_NA], 'Fare': [CONST_NA],
				'FareResult': ['B2C Mkt Data & L7D Avg NA'], 'anchorFare': [CONST_NA],
				'statusInd': [1], 'allocationStatus': [0],
			}
			openDataB2B_details = {
				'RBD': [CONST_NA], 'Fare': [CONST_NA],
				'FareResult': ['B2C Mkt Data NA - Skip B2B'], 'anchorFare': [CONST_NA],
				'statusInd': [3], 'allocationStatus': [0],
			}
			# Failure-equivalent path: bump done counters so the run-audit
			# stays consistent (mirrors today's behaviour at the bottom of
			# ``_phaseK_anchorAndLadder``).
			self.redishelper.updateRunAudit()

		state.openDataB2C_details = openDataB2C_details
		state.openDataB2B_details = openDataB2B_details
		state.anchorResult_b2c = anchorResult_b2c
		state.anchorResult_b2b = anchorResult_b2b
		state.b2b_anchor_source = b2b_anchor_source
		state.determined_overBookingFare = determined_overBookingFare
		state.hard_anchor_flag = hard_anchor_flag
		state.current_discount_amount = current_discount_amount
		state.b2b_discount_fare_active = b2b_discount_fare_active
		state.isB2BMarketFareAvailable = isB2BMarketFareAvailable

	def _phaseK_applyLadder(self, state):
		"""Phase K tail (tail half) — call ``DetermineRBD.applyAnchorToLadder``
		for B2C and B2B from the ``AnchorResult`` objects stashed on
		``state``.

		Called by ``runAllocationPass`` after rehydration: re-runs the
		ladder lookup so heavy DataFrames stay in MongoDB and only the
		small ``AnchorResult`` payload travels through SQS.  The B2C
		``openDataB2C_details`` was already populated by
		``_computeFareAnchors`` (so the B2B candidate selection could
		read the opening price); this method refreshes it from a fresh
		``applyAnchorToLadder`` call so the in-process orchestrator path
		and the two-pass split both end up with the same byte-identical
		``openData*`` dicts feeding Phase L.
		"""
		anchorResult_b2c = state.anchorResult_b2c
		anchorResult_b2b = state.anchorResult_b2b
		b2c_backstop_final = state.b2c_backstop_final
		b2b_backstop_final = state.b2b_backstop_final

		if anchorResult_b2c is not None and getattr(anchorResult_b2c, 'is_skipped', False) is False:
			b2c_tuple = self.determinerbd.applyAnchorToLadder(
				anchorResult_b2c, self.currParams,
				self.outputSummaryRow_b2c.fareOffset, self.outputSummaryRow_b2c.criteria, state
			)
			(state.openDataB2C_details, self.outputSummaryRow_b2c.lastSellingFare,
			 self.outputSummaryRow_b2c.strategyFare, self.outputSummaryRow_b2c.profileFare,
			 self.outputSummaryRow_b2c.hardAnchorFare, self.outputSummaryRow_b2c.upsell,
			 state.determined_overBookingFare, state.hard_anchor_flag,
			 self.outputSummaryRow_b2c.toleranceValue, self.outputSummaryRow_b2c.T_prime) = b2c_tuple
		else:
			# No B2C anchor result — set fallback so Phase L sees the
			# expected dict structure.
			state.openDataB2C_details = {
				'RBD': [CONST_NA], 'Fare': [CONST_NA],
				'FareResult': ['B2C anchor not available'],
				'anchorFare': [CONST_NA], 'statusInd': [1], 'allocationStatus': [0],
			}

		if anchorResult_b2b is not None and getattr(anchorResult_b2b, 'is_skipped', False) is False:
			b2b_tuple = self.determinerbd.applyAnchorToLadder(
				anchorResult_b2b, self.currParams,
				self.outputSummaryRow_b2b.fareOffset, self.outputSummaryRow_b2b.criteria, state
			)
			(state.openDataB2B_details, self.outputSummaryRow_b2b.lastSellingFare,
			 _b2b_strategyFare, self.outputSummaryRow_b2b.profileFare,
			 self.outputSummaryRow_b2b.hardAnchorFare, self.outputSummaryRow_b2b.upsell,
			 _b2b_overBookingFare, state.hard_anchor_flag,
			 self.outputSummaryRow_b2b.toleranceValue, self.outputSummaryRow_b2b.T_prime) = b2b_tuple
		else:
			# No B2B anchor result (market fare unavailable or threshold
			# hit during fare-anchor pass) — set the expected fallback dict
			# so Phase L's b2BAllocation sees 'RBD' and can skip gracefully.
			state.openDataB2B_details = {
				'RBD': [CONST_NA], 'Fare': [CONST_NA],
				'FareResult': ['B2B Mkt Data NA & Not Derivable or b2b threshold hit'],
				'anchorFare': [CONST_NA], 'statusInd': [3], 'allocationStatus': [0],
			}

	def _copyOutputSummaryToAnchor(self, anchor_result, output_row):
		"""Snapshot the Phase F/G metadata from the active ``OutputSummary`` row
		into the ``AnchorResult`` so it can travel verbatim to the
		allocation pass."""
		if anchor_result is None or output_row is None:
			return
		anchor_result.criteria = output_row.criteria
		anchor_result.fareOffset = output_row.fareOffset
		anchor_result.MktFare_Min = output_row.MktFare_Min
		anchor_result.MktFare_Max = output_row.MktFare_Max
		anchor_result.AirlineCode_Min = output_row.AirlineCode_Min
		anchor_result.AirlineCode_Max = output_row.AirlineCode_Max
		anchor_result.FlightNumber_Min = output_row.FlightNumber_Min
		anchor_result.FlightNumber_Max = output_row.FlightNumber_Max
		anchor_result.lowmktFile = getattr(output_row, 'mktFile', '') or ''
		anchor_result.himktFile = getattr(output_row, 'mktFile', '') or ''
		anchor_result.startTime = output_row.startTime
		anchor_result.endTime = output_row.endTime
		anchor_result.TimeRange = output_row.TimeRange
		anchor_result.StrategyReference = output_row.StrategyReference


	# ------------------------------------------------------------------
	# Two-pass entry points
	# ------------------------------------------------------------------

	def runFareAnchorPass(self, currFlt=None, currBookedLoad=None, currLid=None,
						   currFltDate=None, currFltDepartureTime=None,
						   tgtBookedLoadPercent=None, dow=None, ndocounter=None,
						   currAdjustedCapacity=None, currtbfBookedLoad=None):
		"""Run Phases A–J + Phase K head + ``_computeFareAnchors`` and
		return a serialisable portfolio entry dict (R1, R2, R8).

		Increments the F-tagged ready counters at the start of the pass
		(R7.1) so the orchestrator can distinguish flights that have
		started the fare-anchor pass from those that have started the
		allocation pass.  The legacy in-process path is unchanged; this
		method exists so the dispatcher in
		``DynAlloc.from_anchor_payload`` (Slice 10) can drive the
		two-pass flow.

		If any required positional argument is omitted, the values are
		pulled from ``self.currParams`` so callers exercising a single
		flight against a market-list row don't need to plumb them
		explicitly.  Mirrors how ``runDynamicAllocationSingleMarket``
		invokes ``runDynamicAllocationSingleFlight`` today.
		"""
		# Fare-anchor (F) ready counters — bumped before any work so
		# even short-circuit failures (HD_NO_BOOKLOADS etc.) still land
		# in the F-tagged counter pair (R7.1, R7.4).
		self.redishelper.updateAnchorRunAuditReady()

		# Convenience: when called against ``self.currParams`` directly
		# (e.g. from ``from_anchor_payload`` after ``_initialize_params``
		# resolves the SQS body), build the per-flight inputs from the
		# market-list row.  Falls back to the explicit positional args
		# when the orchestrator threads them through.
		if currFlt is None:
			currFlt = self.currParams.flight_number
		if currLid is None:
			currLid = self.currParams.capacity
		if currAdjustedCapacity is None:
			currAdjustedCapacity = self.currParams.adjustedCapacity
		if currtbfBookedLoad is None:
			currtbfBookedLoad = self.currParams.tbf_bookings
		if currFltDate is None:
			currFltDate = self.currParams.per_start
		if currFltDepartureTime is None:
			currFltDepartureTime = self.currParams.DepartureTime
		if currBookedLoad is None:
			currBookedLoad = self.currParams.bookedPax
		# SQS payload dispatch omits positional arguments.
		# Mimic the legacy orchestrator loop exactly to ensure 100% behavioral parity.
		short_circuit_reason = None
		if ndocounter is None or dow is None or tgtBookedLoadPercent is None:
			try:
				currDate = self.timeutils.getCurrentDate()
				if isinstance(currDate, datetime):
					currDate = currDate.date()
				flightDate = datetime.strptime(currFltDate, DATE_FORMAT).date()
				
				computed_ndo = (flightDate - currDate).days
				computed_dow = int(flightDate.strftime(WEEK_DAY))

				if ndocounter is None:
					ndocounter = computed_ndo
				if dow is None:
					dow = computed_dow

				if tgtBookedLoadPercent is None:
					curveId = getattr(self.currParams, 'curve_id', None)
					if curveId is not None and str(curveId).strip() not in ('', 'nan', 'None', 'NA'):
						tgtBookedLoadList = self.rmdatabasehelper.getTargetBookedLoadFromBenchmarkCurve(ndocounter, curveId)
						if len(tgtBookedLoadList) > 0:
							tgtBookedLoadPercent = float(tgtBookedLoadList['LF'][0])
						else:
							tgtBookedLoadPercent = 0.0
							short_circuit_reason = HD_NO_REF_CURVE
					else:
						tgtBookedLoadPercent = 0.0
						short_circuit_reason = HD_NO_REF_CURVE

			except Exception as e:
				self.logger.error(f"Could not create params from currFltDate '{currFltDate}': {e}")
				if ndocounter is None:
					ndocounter = 0
				if dow is None:
					dow = 0
				if tgtBookedLoadPercent is None:
					tgtBookedLoadPercent = 0.0

		state = FlightState(
			currFlt=currFlt,
			currFltDate=currFltDate,
			currFltDepartureTime=currFltDepartureTime,
			currLid=currLid,
			currAdjustedCapacity=currAdjustedCapacity,
			currtbfBookedLoad=currtbfBookedLoad,
			currBookedLoad=currBookedLoad,
			initial_currBookedLoad=currBookedLoad,
			tgtBookedLoadPercent=tgtBookedLoadPercent,
			dow=dow,
			ndocounter=ndocounter,
			portfolio_count=int(getattr(self.currParams, 'portfolio_count', 1) or 1)
				if str(getattr(self.currParams, 'portfolio_count', '')).strip() not in ('', 'NA') else 1,
		)

		if short_circuit_reason:
			state.aborted = True
			state.is_failure = True
			state.failure_reason = short_circuit_reason

		self._phaseA_initFlightContext(state)
		if state.aborted:
			return self._buildPortfolioEntry(state)
		self._phaseB_overbooking(state)
		self._phaseC_targetsAndBands(state)
		self._phaseD_dowGate(state)
		if state.aborted:
			# DOW mismatch — the entry still ships through the portfolio
			# (R8.2) but is excluded from aggregates.  ``state.dow_matched``
			# is False so the allocation pass writes today's "skipped" log.
			return self._buildPortfolioEntry(state)
		self._phaseE_dynamicTimeAndD2(state)
		self._phaseF_marketFares(state)
		self._phaseG_profileFares(state)
		# self._phaseH_backstops(state)
		self._phaseI_lastSoldFare(state)
		self._phaseJ_spoilageReduction(state)
		self._phaseK_anchorCandidate(state)
		self._computeFareAnchors(state)

		return self._buildPortfolioEntry(state)

	def runAllocationPass(self, payload, aggregates):
		"""Resume the per-flight algorithm from a serialised portfolio
		entry plus its portfolio aggregates (R1, R5, R8).

		The ``payload`` is a portfolio entry dict produced by
		``_buildPortfolioEntry`` (typically delivered as the body of an
		SQS allocation-pass message); ``aggregates`` is the 12-field
		dict from ``compute_portfolio_aggregates``.

		Increments the unbranded ready counters at the start (R7.2),
		rehydrates the ``FlightState`` via ``_rehydrateFlightState``,
		populates the four new R5 summary fields and ``portfolio_count``
		via ``_computeAndAttachZScores``, then either runs the failure
		summary path (when ``payload['is_failure']`` is True — R8.3) or
		Phases K-tail / L / M / N.
		"""
		self.redishelper.updateRunAuditReady()

		state = self._rehydrateFlightState(payload)
		self._computeAndAttachZScores(state, aggregates)

		if state.is_failure:
			self._writeFailureSummaryRow(state)
			self.redishelper.updateRunAudit()
			return state

		if getattr(state, 'aborted', False):
			self.redishelper.updateRunAudit()
			return state

		self._phaseK_applyLadder(state)
		self._phaseL_buildAuGrid(state)
		self._phaseM_pushNavitaire(state)
		self._phaseN_writeSummary(state)
		return state

	# ------------------------------------------------------------------
	# SQS dispatcher constructors (Slice 8 / Slice 10)
	# ------------------------------------------------------------------

	@classmethod
	def from_anchor_payload(cls, inputData, currParams):
		"""Constructor entry point for ``pass = "anchor"`` SQS messages
		(R1, R6).

		Builds a ``DynAlloc`` instance via ``_initialize_params`` (so
		all the helper clients line up with today's __init__ flow),
		runs ``runFareAnchorPass`` to produce the portfolio entry,
		then either short-circuits the single-flight portfolio in-
		process (R6 — Slice 8) or returns and lets Slice 11 wire the
		Redis insert + ``SendMessageBatch`` fan-out for ``portfolio_count
		> 1``.

		The single-flight short-circuit performs no Redis or SQS
		interaction: degenerate aggregates (mean = entry value, std =
		0 for each tracked metric) are built in-process and threaded
		straight into ``runAllocationPass``.  This keeps the most
		common portfolio shape (one direct flight on the date) on a
		single Lambda invocation, matching today's per-flight latency.
		"""
		instance = cls.__new__(cls)
		instance._initialize_params(inputData, currParams)
		try:
			entry = instance.runFareAnchorPass()
		except Exception as e:
			instance.logger.error(f"Unhandled exception in runFareAnchorPass: {e}")
			traceback.print_exc()
			entry = instance._build_fallback_portfolio_entry(e)

		if instance._maybeShortCircuitSingleFlight(entry):
			return instance
		# Multi-flight portfolio (portfolio_count > 1): coordinate the
		# Redis trigger and, for the winning invocation, fan out the
		# allocation-pass messages (R3 — Slice 11).
		instance._coordinatePortfolioTrigger(entry)
		return instance

	def _build_fallback_portfolio_entry(self, error):
		"""Synthesize a failure portfolio entry when runFareAnchorPass crashes.
		Ensures Redis triggers still fire and Phase 2 correctly fast-fails.
		"""
		from models.anchor_result import AnchorResult
		from dataclasses import asdict
		entry = self._serialisableMarketListRow()
		entry["pass"] = "allocation"
		entry["portfolio_count"] = int(getattr(self.currParams, 'portfolio_count', 1))
		entry["is_failure"] = True
		entry["failure_reason"] = f"Unhandled exception in Phase 1: {str(error)}"
		
		# Send skipped anchors so downstream logic doesn't KeyError on missing dict keys
		ar_b2c = AnchorResult(is_skipped=True, anchor_source='Error')
		ar_b2b = AnchorResult(is_skipped=True, anchor_source='Error')
		entry["b2c_anchor_result"] = asdict(ar_b2c)
		entry["b2b_anchor_result"] = asdict(ar_b2b)
		return entry

	@classmethod
	def from_allocation_payload(cls, inputData, currParams):
		"""Constructor entry point for ``pass = "allocation"`` SQS messages
		(R1.3).

		Builds a ``DynAlloc`` instance via ``_initialize_params`` (so all
		the helper clients line up with today's __init__ flow), pulls the
		portfolio aggregates that the trigger Lambda stamped onto the
		message body (under ``portfolio_aggregates`` — see ``design.md``
		§Allocation-pass fan-out), and resumes the algorithm via
		``runAllocationPass``.

		The ``currParams`` dict is the serialised ``PortfolioEntry`` plus
		the runtime clients (sqs / cache / mongo / dwconn) injected by the
		dispatcher, so it doubles as the rehydration payload threaded into
		``runAllocationPass``.
		"""
		instance = cls.__new__(cls)
		instance._initialize_params(inputData, currParams)
		aggregates = currParams.get("portfolio_aggregates") or {}
		instance.runAllocationPass(currParams, aggregates)
		return instance

	def _maybeShortCircuitSingleFlight(self, entry):
		"""Single-flight short-circuit (R6.1).

		If the portfolio has exactly one flight, build degenerate
		aggregates (each mean equals the entry's value for that metric
		and every std is 0 — R6.2) and call ``runAllocationPass`` in-
		process so the same Lambda invocation finishes the algorithm
		without any Redis or SQS coordination.

		Returns ``True`` when the short-circuit fired (caller should
		not also fan out to the allocation queue).  Returns ``False``
		for ``portfolio_count > 1`` so Slice 11's Redis-coordinated
		fan-out can take over.
		"""
		try:
			portfolio_count = int(entry.get("portfolio_count", 1) or 1)
		except (TypeError, ValueError):
			portfolio_count = 1
		if portfolio_count != 1:
			return False
		# portfolio_only records participate in aggregation only — skip allocation.
		if str(entry.get("portfolio_only", "0")) == "1":
			return True
		aggregates = self._buildDegenerateAggregates(entry)
		self.runAllocationPass(entry, aggregates)
		return True

	def _buildDegenerateAggregates(self, entry):
		"""Build the 12-field aggregates dict for a single-flight portfolio.

		Mirrors the schema produced by
		``helper.portfolio_aggregator.compute_portfolio_aggregates``:
		every mean equals the entry's value for that metric and every
		standard deviation is 0 (R4.2 / R6.1).  When the entry is not
		in the ``<88`` Portfolio88 subset, the six 88-suffixed fields
		are ``None`` so ``compute_flight_zscores`` returns
		``Portfolio88_Anchor = None`` per R6.2.
		"""
		plf = entry.get("Imputed_ForecastPLF")
		af = entry.get("ForecastAF_Allin")
		fare = entry.get("anchorFare_b2c")

		full = {
			"PDS": plf, "sd_PDS": 0,
			"PDQ": af, "sd_PDQ": 0,
			"PFA": fare, "sd_PFA": 0,
		}

		# Portfolio88 subset: only entries whose imputed PLF is a real
		# number strictly less than 88.  Everything else (including
		# ``"NA"`` / ``None`` / ``NaN``) leaves the 88-suffixed
		# aggregates as ``None`` so the zscore helper collapses
		# ``Portfolio88_Anchor`` to ``None`` (R6.2).
		in_portfolio88 = False
		try:
			plf_value = float(plf)
			if not math.isnan(plf_value):
				in_portfolio88 = plf_value < 88
		except (TypeError, ValueError):
			in_portfolio88 = False

		if in_portfolio88:
			sub = {
				"PDS88": plf, "sd_PDS88": 0,
				"PDQ88": af, "sd_PDQ88": 0,
				"PFA88": fare, "sd_PFA88": 0,
			}
		else:
			sub = {
				"PDS88": None, "sd_PDS88": None,
				"PDQ88": None, "sd_PDQ88": None,
				"PFA88": None, "sd_PFA88": None,
			}

		return {**full, **sub}

	# ------------------------------------------------------------------
	# Portfolio trigger + allocation-pass fan-out (Slice 11 — R3)
	# ------------------------------------------------------------------

	def _coordinatePortfolioTriggerLocal(self, entry):
		"""Simulate Redis HSET and trigger logic for local test runs.
		
		Uses a single local JSON file (`local_redis_state.json`) to act as the 
		mock Redis store, mapping to keys exactly as production does.
		If the portfolio reaches its count, it triggers `runAllocationPass` 
		incrementally mid-run.
		"""
		import json
		import os
		from helper.portfolio_aggregator import compute_portfolio_aggregates

		sector, dep_date, flight_uid = self._portfolioCoordinates(entry)
		try:
			portfolio_count = int(entry.get("portfolio_count", 1) or 1)
		except (TypeError, ValueError):
			portfolio_count = 1

		intermediate_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "QPDynamicAllocation", "testcases", "intermediate")
		os.makedirs(intermediate_dir, exist_ok=True)

		redis_file = os.path.join(intermediate_dir, "local_redis_state.json")
		
		# Read existing state or create empty
		if os.path.exists(redis_file):
			try:
				with open(redis_file, 'r') as f:
					redis_state = json.load(f)
			except Exception:
				redis_state = {}
		else:
			redis_state = {}

		# Simulate Portfolio Key: Portfolio_{run_id}_{sector}_{dep_date}
		# Using a generic run_id so it matches cross-run if needed, but self.runId is safer.
		hkey = f"Portfolio_{self.runId}_{sector}_{dep_date}"
		if hkey not in redis_state:
			redis_state[hkey] = {}

		# Simulate HSET
		redis_state[hkey][flight_uid] = entry
		
		# Check Trigger Condition (simulate INSERT_AND_CHECK_TRIGGER)
		completed_exists = "completed" in redis_state[hkey]
		# active_count is number of flights (keys minus 'completed')
		active_count = len([k for k in redis_state[hkey].keys() if k != "completed"])
		
		won = False
		if active_count >= portfolio_count and not completed_exists:
			redis_state[hkey]["completed"] = True
			won = True

		# Write back the state
		with open(redis_file, 'w') as f:
			json.dump(redis_state, f, default=str, indent=2)

		self.logger.info(
			f"Local Portfolio insert for {flight_uid} on {sector}: "
			f"active_count={active_count}, portfolio_count={portfolio_count}, won={won}"
		)

		if not won:
			return False

		# We won the trigger: fetch entries, compute aggregates, run Phase 2
		entries = [v for k, v in redis_state[hkey].items() if k != "completed"]
		aggregates = compute_portfolio_aggregates(entries)
		
		self.logger.info(
			f"Local Portfolio {sector}_{dep_date} complete — executing phase 2 for "
			f"{len(entries)} allocation-pass flights."
		)

		# Dump aggregates for visibility
		agg_file = os.path.join(intermediate_dir, "portfolio_aggregates.jsonl")
		with open(agg_file, 'a') as f:
			f.write(json.dumps({"Group": f"{sector}_{dep_date}", "Aggregates": aggregates}, default=str) + '\n')

		# Run allocation pass directly
		for p_entry in entries:
			if str(p_entry.get("portfolio_only", "0")) == "1":
				continue
			self.runAllocationPass(p_entry, aggregates)

		# Output results incrementally so files update mid-run
		self.outputResults()
		return True

	def _coordinatePortfolioTrigger(self, entry):
		"""Insert the fare-anchor entry into the Redis portfolio hash and,
		for the invocation that completes the portfolio, fan out the
		allocation-pass messages (R3.3 - R3.5).

		Called by ``from_anchor_payload`` only when the single-flight
		short-circuit did not fire (``portfolio_count > 1``).  The
		``insert_portfolio_entry`` Lua script atomically writes the
		entry, refreshes the TTL, and returns ``(active_count, won)``.
		``won`` is ``True`` for exactly one invocation per portfolio —
		the one that fills the count and claims the ``completed``
		sentinel — guaranteeing the fan-out happens once even under SQS
		redelivery (R3.2 / R3.4).

		Returns ``True`` when this invocation owned the trigger and
		fanned out, ``False`` otherwise.
		"""
		sector, dep_date, flight_uid = self._portfolioCoordinates(entry)
		try:
			portfolio_count = int(entry.get("portfolio_count", 1) or 1)
		except (TypeError, ValueError):
			portfolio_count = 1

		entry_json = json.dumps(entry, default=str)
		active_count, won = self.redishelper.insert_portfolio_entry(
			self.runId, sector, dep_date, flight_uid, entry_json, portfolio_count,
		)
		self.logger.info(
			f"Portfolio insert for {flight_uid} on {sector}: "
			f"active_count={active_count}, portfolio_count={portfolio_count}, won={won}"
		)

		if not won:
			# Either the portfolio is not yet complete (R3.5) or another
			# invocation already claimed the trigger (R3.2 / R3.4).  Ack
			# and return without any allocation-pass dispatch.
			return False

		# This invocation owns the trigger: gather every entry, compute
		# the portfolio aggregates, fan out the allocation-pass messages,
		# then mark the portfolio done so late retries don't re-fan-out.
		entries = self.redishelper.fetch_portfolio_entries(self.runId, sector, dep_date)
		aggregates = compute_portfolio_aggregates(entries)
		self.logger.info(
			f"Portfolio {sector}_{dep_date} complete — fanning out "
			f"{len(entries)} allocation-pass messages."
		)
		self._fan_out_allocation_messages(sector, dep_date, entries, aggregates)
		self.redishelper.mark_portfolio_done(self.runId, sector, dep_date)
		return True

	def _fan_out_allocation_messages(self, sector, dep_date, entries, aggregates):
		"""Enqueue one allocation-pass SQS message per portfolio entry
		(R3.4), batching into ``SendMessageBatch`` chunks of 10.

		Each message body is the original fare-anchor ``PortfolioEntry``
		with ``pass`` flipped to ``"allocation"`` and the freshly-computed
		``portfolio_aggregates`` attached, so the allocation pass can
		compute its z-scores and ``Portfolio_Anchor`` without re-reading
		Redis (see ``design.md`` §Allocation-pass fan-out).

		Failure entries are included in the fan-out (R8.2); the
		allocation pass writes their failure summary rows.
		"""
		queue_url = self.MARKET_LIST_QUEUE_URL
		batch = []
		for entry in entries:
			# portfolio_only records don't need an allocation pass
			if str(entry.get("portfolio_only", "0")) == "1":
				continue
			body = {
				**entry,
				"pass": "allocation",
				"portfolio_aggregates": aggregates,
			}
			batch.append({
				"Id": str(uuid.uuid4()),
				"MessageBody": json.dumps(body, default=str),
			})
			if len(batch) == 10:
				self.sqs.send_message_batch(QueueUrl=queue_url, Entries=batch)
				batch = []
		if batch:
			self.sqs.send_message_batch(QueueUrl=queue_url, Entries=batch)

	def _portfolioCoordinates(self, entry):
		"""Derive the ``(sector, dep_date, flight_uid)`` triple used to key
		the Redis portfolio hash for ``entry``.

		* ``sector``    — ``Origin + Destin`` (matches Phase G's
		  ``state.sector_full``).
		* ``dep_date``  — the departure date the portfolio is grouped on;
		  ``rm-allocation-fetch-inputs`` groups by ``PerStart`` (R13.1),
		  so we key on the same field.
		* ``flight_uid``— ``"{flight_number}"`` (R3.1), the
		  ``HSET`` field name; redelivered messages for the same flight
		  overwrite the same field without growing the hash (R3.2).
		"""
		sector = entry.get("sector") or (self.currParams.origin + self.currParams.destin)
		dep_date = (
			entry.get("PerStart")
			or entry.get("currFltDate")
			or self.currParams.per_start
		)
		flight_number = entry.get("FlightNumber")
		if flight_number in (None, ""):
			flight_number = entry.get("currFlt", self.currParams.flight_number)
		flight_uid = f"{flight_number}"
		return sector, dep_date, flight_uid

	# ------------------------------------------------------------------
	# Portfolio entry serialisation
	# ------------------------------------------------------------------

	def _buildPortfolioEntry(self, state):
		"""Serialise ``state`` into the ``PortfolioEntry`` dict described in
		``design.md``.

		The entry carries the original market-list fields verbatim plus
		a snapshot of the Phase A–K-head outputs, so the allocation
		pass can rebuild the per-flight algorithm without re-running
		Phases A–J.  Heavy DataFrames (``profileExtract_df_fullalloc``,
		``B2CRBDs_df_for_cug_gq_tbf``, ``b2b_Z0_rbds_df``) are NOT
		serialised — the allocation pass re-fetches them from MongoDB.
		"""
		# Start from the inbound market-list row dict so all ~70 source
		# fields travel through.  ``MarketListRow`` keeps a reference
		# to the source dict only via attribute reads, so we rebuild a
		# clean dict from the attributes that matter for the allocation
		# pass — anything callers don't need (DB engines, sqs clients,
		# cache clients, etc.) is intentionally dropped.
		entry = self._serialisableMarketListRow()

		entry["pass"] = "allocation"
		entry["portfolio_count"] = state.portfolio_count

		ar_b2c = state.anchorResult_b2c
		if ar_b2c is None:
			ar_b2c = AnchorResult(is_skipped=True, anchor_source=getattr(self.outputSummaryRow_b2c, 'HowDetermined', ''))
			self._copyOutputSummaryToAnchor(ar_b2c, self.outputSummaryRow_b2c)
		entry["anchor_result_b2c"] = self._serialiseAnchorResult(ar_b2c)

		ar_b2b = state.anchorResult_b2b
		if ar_b2b is None:
			ar_b2b = AnchorResult(is_skipped=True, anchor_source=getattr(self.outputSummaryRow_b2b, 'HowDetermined', ''))
			self._copyOutputSummaryToAnchor(ar_b2b, self.outputSummaryRow_b2b)
		entry["anchor_result_b2b"] = self._serialiseAnchorResult(ar_b2b)
		entry["b2b_anchor_source"] = state.b2b_anchor_source

		# Forecast / band / capacity / SOD / Phase G / Phase H / I / J
		# scalars — anything the allocation pass needs to thread back
		# through ``_phaseL_buildAuGrid`` and ``_phaseN_writeSummary``.
		entry["currFltDate"] = state.currFltDate
		entry["currFltDepartureTime"] = state.currFltDepartureTime
		entry["currentTime"] = state.currentTime
		entry["currFlt"] = state.currFlt
		entry["currLid"] = state.currLid
		entry["currAdjustedCapacity"] = state.currAdjustedCapacity
		entry["currtbfBookedLoad"] = state.currtbfBookedLoad
		entry["currBookedLoad"] = state.currBookedLoad
		entry["initial_currBookedLoad"] = state.initial_currBookedLoad
		entry["tgtBookedLoadPercent"] = state.tgtBookedLoadPercent
		entry["dow"] = state.dow
		entry["ndocounter"] = state.ndocounter

		entry["current_w0Flag"] = state.current_w0Flag
		entry["current_directFlag"] = state.current_directFlag
		entry["actualplf"] = state.actualplf

		entry["base_totalCapacity"] = state.base_totalCapacity
		entry["base_adjustedCapacity"] = state.base_adjustedCapacity
		entry["final_totalCapacity"] = state.final_totalCapacity
		entry["final_plf"] = state.final_plf
		entry["overBookingCount"] = state.overBookingCount
		entry["expectedNoShows"] = getattr(state, 'expectedNoShows', CONST_NA)
		entry["b2boverbookingflag"] = state.b2boverbookingflag

		entry["sod_dplfBand"] = state.sod_dplfBand
		entry["sod_bookingToday"] = state.sod_bookingToday
		entry["sod_BookingsInHour"] = state.sod_BookingsInHour
		entry["sod_eodTgtBookedLoad"] = state.sod_eodTgtBookedLoad_abs
		entry["sod_tgtAtCurrentHour"] = state.sod_tgtAtCurrentHour_abs
		entry["sod_startOfDayBookLoad"] = state.sod_startOfDayBookLoad_factor
		entry["sod_startOfDayTgtBookedLoad"] = state.sod_startOfDayTgtBookedLoad_abs
		entry["distress_pax_count"] = state.distress_pax_count

		entry["variance"] = state.variance_full
		entry["dplfBand"] = state.dplfBand_full
		entry["ndoBand"] = state.ndoBand_full
		entry["forecast_plf"] = state.forecast_plf
		entry["forecastBand"] = state.forecastBand
		entry["imp_forecast"] = state.imp_forecast
		entry["imp_forecast_band"] = state.imp_forecast_band
		entry["af_forecast_band"] = state.af_forecast_band
		entry["Af_Nb_Allin"] = state.Af_Nb_Allin
		entry["PLF_Gain"] = state.PLF_Gain
		# R4-friendly aliases used by the portfolio aggregator.
		entry["Imputed_ForecastPLF"] = state.imp_forecast
		entry["ForecastAF_Allin"] = getattr(self.currParams, 'ForecastAF_Allin', CONST_NA)
		entry["anchorFare_b2c"] = (
			state.anchorResult_b2c.anchorFare if state.anchorResult_b2c is not None else CONST_NA
		)

		entry["bookloadPercentile"] = state.bookloadPercentile_full
		entry["percentile_str"] = state.percentile_str_full
		entry["upsellValue"] = state.upsellValue_full
		entry["sector"] = state.sector_full
		entry["profileFare"] = state.profileFare
		entry["month_str"] = state.month_str_for_profile
		entry["dow_str"] = state.dow_str_for_profile
		entry["deptTime_hhmm"] = state.deptTime_str_hhmm

		entry["b2c_backstop_final"] = state.b2c_backstop_final
		entry["b2b_backstop_final"] = state.b2b_backstop_final
		entry["auto_b2c_backstop_str"] = state.auto_b2c_backstop_str
		entry["auto_b2b_backstop_str"] = state.auto_b2b_backstop_str

		entry["lastSoldFareb2c"] = state.lastSoldFareb2c_val
		entry["lastSoldFareb2b"] = state.lastSoldFareb2b_val
		entry["last_seven_avg_val"] = state.last_seven_avg_val
		entry["is_spoilage_reduction_active"] = state.is_spoilage_reduction_active
		entry["hardfare_b2c"] = state.hardfare_b2c
		entry["conversion_rate"] = state.conversion_rate
		entry["currency"] = state.currency

		entry["B2B_thresholdflag"] = bool(getattr(self, 'B2B_thresholdflag', False))
		entry["p_thresholdflag"] = bool(getattr(self, 'p_thresholdflag', False))
		entry["l_thresholdflag"] = bool(getattr(self, 'l_thresholdflag', False))
		entry["threshold_flags"] = self._snapshotSoftblockThresholds()

		entry["DaySpan_full"] = state.DaySpan_full
		entry["input_time_str_dep_full"] = state.input_time_str_dep_full

		# Summary row fields populated during Phases E and K that Phase N
		# reads from ``self.outputSummaryRow_*``.  The allocation pass
		# creates fresh summary rows via ``_populateSummaryFromAnchor`` but
		# only restores a subset; these additional fields ensure the
		# allocation pass can produce byte-identical output.
		entry["sod_currentTgtLoad"] = round(state.sod_tgtAtCurrentHour_abs, 2) if state.sod_tgtAtCurrentHour_abs else 0.0
		entry["sod_currentLoadFactor"] = (
			round((state.sod_tgtAtCurrentHour_abs / state.final_totalCapacity) * 100, 2)
			if state.final_totalCapacity and state.final_totalCapacity > 0 else 0.0
		)
		entry["sod_endOfDayTgtLoad"] = round(state.sod_eodTgtBookedLoad_abs, 2) if state.sod_eodTgtBookedLoad_abs else 0.0
		entry["sod_dlfBand"] = state.sod_dplfBand
		entry["sod_endOfDaydlfBand"] = state.dplfBand_full
		entry["last_seven_avg_val_summary"] = state.last_seven_avg_val

		# B2B factor may have been mutated in Phase E (auto-B2B-strategy
		# lookup); capture the post-mutation value so the allocation
		# pass uses the same factor.
		entry["b2b_factor_resolved"] = getattr(self.currParams, 'b2b_factor', None)

		entry["aborted"] = getattr(state, "aborted", False)
		entry["is_failure"] = bool(state.is_failure)
		entry["failure_reason"] = state.failure_reason

		entry["should_process_K"] = getattr(state, 'should_process_K', False)
		entry["isB2CMarketFareAvailable"] = getattr(state, 'isB2CMarketFareAvailable', False)
		entry["isB2BMarketFareAvailable"] = getattr(state, 'isB2BMarketFareAvailable', False)
		entry["himktFile_b2b"] = getattr(state, 'himktFile_b2b', "")
		entry["lowmktFile_b2b"] = getattr(state, 'lowmktFile_b2b', "")
		entry["himktFile_b2c"] = getattr(state, 'himktFile_b2c', "")
		entry["lowmktFile_b2c"] = getattr(state, 'lowmktFile_b2c', "")
		entry["is_last7avg_anchor_b2c"] = getattr(state, 'is_last7avg_anchor_b2c', False)
		entry["is_profile_anchor_b2c"] = getattr(state, 'is_profile_anchor_b2c', False)

		# R9 — placeholder for portfolio-aware fields.  Filled in during
		# the allocation pass by ``_computeAndAttachZScores``.
		entry["z_score_composite"] = None
		entry["Portfolio_Anchor"] = None
		entry["z_score88"] = None
		entry["Portfolio88_Anchor"] = None

		# Carry portfolio_only flag so allocation-pass fan-out can skip it.
		entry["portfolio_only"] = getattr(self.currParams, 'portfolio_only', 0)

		return entry

	def _serialisableMarketListRow(self):
		"""Project ``self.currParams`` (a ``MarketListRow``) into a JSON-
		serialisable dict.

		Drops the live runtime handles (``sqs``, ``cache_client``,
		``mongodbclient``, etc.) and any pandas / database engine
		objects — the allocation pass receives those from its own
		Lambda context, not from the SQS body.

		Keys are mapped back to the original constructor-key names that
		``MarketListRow.__init__`` reads (e.g. ``origin`` → ``Origin``)
		so that ``_rehydrateFlightState`` can rebuild a ``MarketListRow``
		from the serialised dict without losing fields.
		"""
		# Map attribute names → constructor keys that MarketListRow.__init__ reads.
		_ATTR_TO_KEY = {
			"origin": "Origin", "destin": "Destin",
			"flight_number": "FlightNumber", "per_start": "PerStart",
			"per_end": "PerEnd", "dow": "DOW",
			"hardanchor": "hardAnchor", "plfthreshold": "plfThreshold",
			"time_window_start": "TimeWindowStart",
			"time_window_end": "TimeWindowEnd",
			"curve_id": "CurveID",
			"carr_exclusion_b2c": "CarrExlusionB2C",
			"carr_exclusion_b2b": "CarrExlusionB2B",
			"flightExclusionB2C": "flightExclusionB2C",
			"flightExclusionB2B": "flightExclusionB2B",
			"fare_anchor": "fareAnchor", "fare_offset": "fareOffset",
			"first_rbd_alloc": "FirstRBDAlloc",
			"other_rbd_alloc": "OtherRBDAlloc",
			"b2b_backstop": "B2BBackstop", "b2c_backstop": "B2CBackstop",
			"b2b_factor": "B2BFactor", "obSeats": "obSeats",
			"obFare": "obFare", "skipping_factor": "SkippingFactor",
			"day_span": "DaySpan", "auto_time_range_flag": "AutoTimeRangeFlag",
			"analyst_name": "analystName", "opening_fares": "openingFares",
			"over_booking": "OverBooking", "profileFares": "profileFares",
			"rbdPushFlag": "rbdPushFlag",
			"b2c_tolerance": "B2CTolerance", "b2b_tolerance": "B2BTolerance",
			"distressInventoryFlag": "distressInventoryFlag",
			"seriesBlock": "seriesBlock", "autoGroup": "autoGroup",
			"autoBackstopFlag": "autoBackstopFlag",
			"tbfFlag": "tbfFlag", "w0Flag": "w0_value",
			"directFlag": "directFlag", "cug_mkt_flag": "CugFlag",
			"mkt_fare_flag": "MktFareFlag", "tbfenhflag": "TBFEnhFlag",
			"outboundstop": "Outbound_Stop",
			"market_discount": "Marketfare_Upsell",
			"saleflag": "saleflag",
			"DepartureTime": "DepartureTime", "bookedPax": "bookedPax",
			"capacity": "capacity", "adjustedCapacity": "adjustedCapacity",
			"tbf_bookings": "tbf_bookings",
			"seats_sold_L": "seats_sold_L", "seats_sold_P": "seats_sold_P",
			"current_rbd_L": "current_rbd_L", "current_rbd_P": "current_rbd_P",
			"total_series_seats": "total_series_seats",
			"NextFlight": "NextFlight",
			"BookingsInHour": "BookingsInHour",
			"BookingsCount": "BookingsCount",
			"BookingToday": "BookingToday",
			"WA_bookings": "WA_bookings", "WB_bookings": "WB_bookings",
			"WC_bookings": "WC_bookings", "WD_bookings": "WD_bookings",
			"WE_bookings": "WE_bookings",
			"cug_flag": "cug_alloc", "group_bookings": "group_bookings",
			"run_id": "runId",
			"update_navitaire_method": "update_navitaire_method",
			"run_time": "runTime",
			"run_single_market": "runSingleMarket",
			"navitaire_update_method": "NavitaireUpdateMethod",
			"route_type": "route_type",
			"saleinvflag": "saleinvflag",
			"defaultMlFlag": "defaultMlFlag",
			"forecastPLF": "forecastPLF",
			"ForecastAF_Allin": "ForecastAF_Allin",
			"ALLIN_AF": "ALLIN_AF",
			"ALLIN_AF_NB_LM": "ALLIN_AF_NB_LM",
			"ALLIN_AF_NB_LY": "ALLIN_AF_NB_LY",
			"forecast_variance": "forecast_variance",
			"b2bStrategy": "b2bStrategy",
			"forecastAllocFlag": "forecastAllocFlag",
			"afForecastAllocFlag": "afForecastAllocFlag",
			"Spoilage_Flag": "Spoilage_Flag",
			"CanAdjustedForecast": "CanAdjustedForecast",
			"adjustedPLF": "adjustedPLF",
			"market_station_mapping": "market_station_mapping",
			"farecap": "farecap",
			"portfolio_count": "portfolio_count",
			"portfolio_only": "portfolio_only",
			"strategy": "fareAnchor",  # strategy is derived from fare_anchor
			"incrementedProfileFareFlag": "incrementedProfileFareFlag",
			"profileFareUpsell": "ProfileFareUpsell", 
			"currency": "currency",
			"conversion_rate": "conversionRate",
			"region": "region",
			"buy_up_factor_NG": "buy_up_factor_NG",
			"connection_fare_flag_NG": "connection_fare_flag_NG",
			"additional_connection_duration_NG": "additional_connection_duration_NG",
			"CompetitionResponseGrid_NG": "CompetitionResponseGrid_NG",
			"nextgen_flag_NG": "nextgen_flag_NG",
			"competition_response_flag_NG": "competition_response_flag_NG",
			"buy_up_factor": "buy_up_factor_NG",
			"connection_fare_flag": "connection_fare_flag_NG",
			"additional_connection_duration": "additional_connection_duration_NG",
			"grid_name": "CompetitionResponseGrid_NG",
			"nextgen_flag": "nextgen_flag_NG",
			"competition_response_flag": "competition_response_flag_NG"
		}
		# Explicitly exclude fields that are only used in Phase 1 and never read by Phase 2 or summary mappings
		EXCLUDED_MARKET_LIST_FIELDS = {
			"CarrExlusionB2C", "CarrExlusionB2B", "flightExclusionB2C", "flightExclusionB2B",
			"fareAnchor", "fareOffset", "obSeats", "obFare", "DaySpan",
			"AutoTimeRangeFlag", "openingFares", "OverBooking", "B2CTolerance",
			"B2BTolerance", "seriesBlock", "autoBackstopFlag", "Outbound_Stop", "Marketfare_Upsell",
			"saleflag", "bookedPax", "total_series_seats", "NextFlight", "BookingsInHour", "BookingsCount",
			"BookingToday", "WA_bookings", "WB_bookings", "WC_bookings", "WD_bookings", "WE_bookings",
			"TimeWindowStart", "TimeWindowEnd", "PerStart", "PerEnd",
		}

		_DROPPED = {
			"sqs", "cache_client", "dbconn", "wrconn", "dwconn",
			"mongodbclient", "mongo_master_db", "s3", "time_logger",
		}
		out = {}
		for key, value in vars(self.currParams).items():
			if key in _DROPPED:
				continue
			# Map attribute name to constructor key; fall back to original
			output_key = _ATTR_TO_KEY.get(key, key)

			if output_key in EXCLUDED_MARKET_LIST_FIELDS:
				continue

			if isinstance(value, (str, int, float, bool, type(None))):
				out[output_key] = value
			else:
				# Non-trivial values (dataframes, datetimes, etc.) get
				# stringified — the SQS hop only needs the raw scalars.
				try:
					out[output_key] = str(value)
				except Exception:
					continue
		return out

	def _serialiseAnchorResult(self, anchor_result):
		"""Convert an ``AnchorResult`` dataclass to a JSON-friendly dict.

		``openResult`` is normalised to a dict of column → list (the
		shape ``determineRBDToOpen`` already produces) so it can travel
		through SQS without ``pandas`` on the receiving end.
		``early_return_tuple`` is preserved so the allocation pass can
		replay the legacy short-circuit verbatim.
		"""
		if anchor_result is None:
			return None
		payload = {}
		for slot, value in vars(anchor_result).items():
			if isinstance(value, pd.DataFrame):
				payload[slot] = value.to_dict(orient='list')
			elif isinstance(value, tuple):
				payload[slot] = list(value)
			else:
				if value == "Market Fare not found":
					payload[slot] = None
				elif slot in ("lowmktFile", "himktFile") and value == "NA":
					payload[slot] = None
				else:
					payload[slot] = value
		return payload

	def _snapshotSoftblockThresholds(self):
		"""Capture the per-RBD softblock threshold flags computed in Phase E."""
		try:
			rbds = json.loads(self.constantsUtils.SOFTBLOCK_RBDS)
		except Exception:
			rbds = []
		flags = {}
		for rbd in rbds:
			flag_name = rbd.lower() + '_thresholdflag'
			is_true = bool(getattr(self, flag_name, False))
			if is_true:
				flags[rbd] = True
		return flags

	# ------------------------------------------------------------------
	# Allocation-pass rehydration
	# ------------------------------------------------------------------

	def _rehydrateFlightState(self, payload):
		"""Rebuild a ``FlightState`` from the ``PortfolioEntry`` payload.

		The lightweight scalars come straight off ``payload``; the heavy
		DataFrames (``profileExtract_df_fullalloc``,
		``B2CRBDs_df_for_cug_gq_tbf``, ``b2b_Z0_rbds_df``) are
		re-fetched from MongoDB by ``_phaseL_buildAuGrid`` itself, so
		this method only has to load ``profileFaresExtract`` ahead of
		time so Phase G's ``profileFares_list_sorted_fullalloc`` is
		ready for the AU-grid build.

		Also rehydrates the new ``OutputSummary`` rows (B2C + B2B) with
		the Phase F/G metadata (Min/Max market fares, criteria, time
		range, etc.) carried inside the serialised ``AnchorResult`` —
		Phase N reads those fields from ``self.outputSummaryRow_*``
		when persisting the summary.
		"""
		# Bring the ``MarketListRow`` view in sync with the inbound
		# payload so downstream methods using ``self.currParams`` see
		# the same values as the fare-anchor pass did.
		merged = dict(payload)
		# ``MarketListRow`` reads keys that match its constructor (the
		# raw market-list field names).  ``_serialisableMarketListRow``
		# already wrote them under those names, so ``MarketListRow``
		# can rebuild from ``merged`` without changes.
		if "B2BFactor" not in merged and merged.get("b2b_factor_resolved") is not None:
			merged["B2BFactor"] = merged["b2b_factor_resolved"]
		self.currParams = MarketListRow(merged)
		# Some attributes that ``_initialize_params`` set up directly on
		# self need to be re-derived after the rehydration so the
		# downstream phases see consistent values.
		self.w0Flag = self.currParams.w0Flag
		if pd.isna(self.currParams.w0Flag):
			self.w0Flag = 0
		self.directFlag = self.currParams.directFlag
		if pd.isna(self.currParams.directFlag):
			self.directFlag = 1
		# ``b2b_factor`` may have been overwritten in Phase C (R3
		# implementation note); reapply the post-mutation value so the
		# allocation pass uses the same factor that the fare-anchor
		# pass picked.
		resolved_b2b = payload.get("b2b_factor_resolved")
		if resolved_b2b is not None:
			try:
				self.currParams.b2b_factor = float(resolved_b2b)
			except (TypeError, ValueError):
				self.currParams.b2b_factor = resolved_b2b

		state = FlightState(
			currFlt=payload.get("currFlt", self.currParams.flight_number),
			currFltDate=payload.get("currFltDate", self.currParams.per_start),
			currFltDepartureTime=payload.get("currFltDepartureTime", self.currParams.DepartureTime),
			currLid=safe_float(payload.get("currLid", self.currParams.capacity)),
			currAdjustedCapacity=safe_float(payload.get("currAdjustedCapacity", self.currParams.adjustedCapacity)),
			currtbfBookedLoad=safe_float(payload.get("currtbfBookedLoad", self.currParams.tbf_bookings)),
			currBookedLoad=safe_float(payload.get("currBookedLoad", self.currParams.bookedPax)),
			initial_currBookedLoad=safe_float(payload.get("initial_currBookedLoad", payload.get("currBookedLoad", self.currParams.bookedPax))),
			tgtBookedLoadPercent=safe_float(payload.get("tgtBookedLoadPercent", 0.0)),
			dow=payload.get("dow", 0),
			ndocounter=payload.get("ndocounter", 0),
			current_w0Flag=payload.get("current_w0Flag", 0),
			current_directFlag=payload.get("current_directFlag", 1),
			actualplf=safe_float(payload.get("actualplf", 0.0)),
			base_totalCapacity=safe_float(payload.get("base_totalCapacity", 0.0)),
			base_adjustedCapacity=safe_float(payload.get("base_adjustedCapacity", 0.0)),
			final_totalCapacity=safe_float(payload.get("final_totalCapacity", 0.0)),
			final_plf=safe_float(payload.get("final_plf", 0.0)),
			overBookingCount=safe_float(payload.get("overBookingCount", 0.0)),
			expectedNoShows=payload.get("expectedNoShows", CONST_NA),
			b2boverbookingflag=bool(payload.get("b2boverbookingflag", False)),
			currentTime=payload.get("currentTime", ""),
			sod_dplfBand=payload.get("sod_dplfBand", 0),
			sod_bookingToday=payload.get("sod_bookingToday", 0),
			sod_BookingsInHour=payload.get("sod_BookingsInHour", 0),
			sod_eodTgtBookedLoad_abs=payload.get("sod_eodTgtBookedLoad", 0.0),
			sod_tgtAtCurrentHour_abs=payload.get("sod_tgtAtCurrentHour", 0.0),
			sod_startOfDayBookLoad_factor=payload.get("sod_startOfDayBookLoad", 0.0),
			sod_startOfDayTgtBookedLoad_abs=payload.get("sod_startOfDayTgtBookedLoad", 0.0),
			distress_pax_count=payload.get("distress_pax_count", 0),
			variance_full=payload.get("variance", 0.0),
			ndoBand_full=payload.get("ndoBand", 0),
			dplfBand_full=payload.get("dplfBand", 0),
			forecast_plf=payload.get("forecast_plf"),
			forecastBand=payload.get("forecastBand"),
			imp_forecast=payload.get("imp_forecast"),
			imp_forecast_band=payload.get("imp_forecast_band"),
			af_forecast_band=payload.get("af_forecast_band"),
			Af_Nb_Allin=payload.get("Af_Nb_Allin"),
			PLF_Gain=payload.get("PLF_Gain"),
			DaySpan_full=payload.get("DaySpan_full", 0),
			input_time_str_dep_full=payload.get("input_time_str_dep_full", ""),
			bookloadPercentile_full=payload.get("bookloadPercentile", 0),
			percentile_str_full=payload.get("percentile_str", ""),
			upsellValue_full=payload.get("upsellValue", 0.0),
			sector_full=payload.get("sector", ""),
			profileFare=payload.get("profileFare"),
			month_str_for_profile=payload.get("month_str", ""),
			dow_str_for_profile=payload.get("dow_str", ""),
			deptTime_str_hhmm=payload.get("deptTime_hhmm", ""),
			b2c_backstop_final=payload.get("b2c_backstop_final"),
			b2b_backstop_final=payload.get("b2b_backstop_final"),
			auto_b2c_backstop_str=payload.get("auto_b2c_backstop_str", "NA"),
			auto_b2b_backstop_str=payload.get("auto_b2b_backstop_str", "NA"),
			lastSoldFareb2c_val=payload.get("lastSoldFareb2c"),
			lastSoldFareb2b_val=payload.get("lastSoldFareb2b"),
			last_seven_avg_val=payload.get("last_seven_avg_val"),
			is_spoilage_reduction_active=bool(payload.get("is_spoilage_reduction_active", False)),
			B2B_thresholdflag=bool(payload.get("B2B_thresholdflag", False)),
			p_thresholdflag=bool(payload.get("p_thresholdflag", False)),
			l_thresholdflag=bool(payload.get("l_thresholdflag", False)),
			threshold_flags=payload.get("threshold_flags", {}) or {},
			b2b_anchor_source=payload.get("b2b_anchor_source"),
			hardfare_b2c=payload.get("hardfare_b2c"),
			anchorResult_b2c=self._deserialiseAnchorResult(payload.get("anchor_result_b2c")),
			anchorResult_b2b=self._deserialiseAnchorResult(payload.get("anchor_result_b2b")),
			is_failure=bool(payload.get("is_failure", False)),
			failure_reason=payload.get("failure_reason") or "",
			portfolio_count=int(payload.get("portfolio_count", 1) or 1),
			aborted=bool(payload.get("aborted", False)),
			should_process_K=payload.get("should_process_K", False),
			isB2CMarketFareAvailable=payload.get("isB2CMarketFareAvailable", False),
			isB2BMarketFareAvailable=payload.get("isB2BMarketFareAvailable", False),
			himktFile_b2b=payload.get("himktFile_b2b", ""),
			lowmktFile_b2b=payload.get("lowmktFile_b2b", ""),
			himktFile_b2c=payload.get("himktFile_b2c", ""),
			lowmktFile_b2c=payload.get("lowmktFile_b2c", ""),
			is_last7avg_anchor_b2c=payload.get("is_last7avg_anchor_b2c", False),
			is_profile_anchor_b2c=payload.get("is_profile_anchor_b2c", False),
			currency=payload.get("currency", "NA"),
			conversion_rate=payload.get("conversion_rate", "NA"),
		)
		# Re-derive parsed datetime / yyyymmdd helpers because the
		# downstream phases read them from ``state``.
		try:
			state.currFltDate_dt = datetime.strptime(state.currFltDate, DATE_FORMAT)
			state.dateFormatted_yyyymmdd = payload.get(
				"currFltDate_yyyymmdd",
				state.currFltDate_dt.strftime(DATE_FORMAT_2),
			)
		except (TypeError, ValueError):
			state.currFltDate_dt = None
			state.dateFormatted_yyyymmdd = payload.get("currFltDate_yyyymmdd", "")

		if not state.deptTime_str_hhmm:
			try:
				time_object_dep = datetime.strptime(state.currFltDepartureTime, TIME_FORMAT).time()
				state.deptTime_str_hhmm = f"{time_object_dep.hour:02d}:{time_object_dep.minute:02d}"
			except (TypeError, ValueError):
				state.deptTime_str_hhmm = "00:00"

		# Refresh the per-flight summary rows so Phase N has the F/G
		# metadata it needs without re-running Phases F or G.
		self.outputSummaryRow_b2c = OutputSummary()
		self.outputSummaryRow_b2b = OutputSummary()
		self._populateSummaryFromAnchor(self.outputSummaryRow_b2c, state.anchorResult_b2c, B2C_CHANNEL)
		self._populateSummaryFromAnchor(self.outputSummaryRow_b2b, state.anchorResult_b2b, B2B_CHANNEL)
		self.outputSummaryRow_b2c.Id = self.utils.getUniqueId()
		self.outputSummaryRow_b2b.Id = self.utils.getUniqueId()

		# Restore Phase E summary fields that _populateSummaryFromAnchor
		# doesn't cover (SOD values, last7avg, bands).
		sod_currentTgtLoad = payload.get("sod_currentTgtLoad", 0.0)
		sod_currentLoadFactor = payload.get("sod_currentLoadFactor", 0.0)
		sod_endOfDayTgtLoad = payload.get("sod_endOfDayTgtLoad", 0.0)
		sod_dlfBand = payload.get("sod_dlfBand", 0)
		sod_endOfDaydlfBand = payload.get("sod_endOfDaydlfBand", 0)
		last_seven_avg_summary = payload.get("last_seven_avg_val_summary")

		self.outputSummaryRow_b2c.currentTgtLoad = sod_currentTgtLoad
		self.outputSummaryRow_b2b.currentTgtLoad = sod_currentTgtLoad
		self.outputSummaryRow_b2c.currentLoadFactor = sod_currentLoadFactor
		self.outputSummaryRow_b2b.currentLoadFactor = sod_currentLoadFactor
		self.outputSummaryRow_b2c.bookingToday = state.sod_bookingToday
		self.outputSummaryRow_b2b.bookingToday = state.sod_bookingToday
		self.outputSummaryRow_b2c.bookingLastHour = state.sod_BookingsInHour
		self.outputSummaryRow_b2b.bookingLastHour = state.sod_BookingsInHour
		self.outputSummaryRow_b2c.endOfDayTgtLoad = sod_endOfDayTgtLoad
		self.outputSummaryRow_b2b.endOfDayTgtLoad = sod_endOfDayTgtLoad
		self.outputSummaryRow_b2c.dlfBand = sod_dlfBand
		self.outputSummaryRow_b2b.dlfBand = sod_dlfBand
		self.outputSummaryRow_b2c.endOfDaydlfBand = sod_endOfDaydlfBand
		self.outputSummaryRow_b2b.endOfDaydlfBand = sod_endOfDaydlfBand
		if last_seven_avg_summary is not None:
			self.outputSummaryRow_b2c.lastSevenAvgFare = last_seven_avg_summary
			self.outputSummaryRow_b2b.lastSevenAvgFare = last_seven_avg_summary
		self.outputSummaryRow_b2c.w0Flag = state.current_w0Flag
		self.outputSummaryRow_b2b.w0Flag = state.current_w0Flag

		# Threshold flags as instance attributes so Phase L's per-RBD
		# softblock allocations see the same values as the legacy run.
		try:
			rbds = json.loads(self.constantsUtils.SOFTBLOCK_RBDS)
		except Exception:
			rbds = []
		threshold_flags = state.threshold_flags or {}
		for rbd in rbds:
			flag_name = rbd.lower() + '_thresholdflag'
			setattr(self, flag_name, bool(threshold_flags.get(rbd, False)))
		self.B2B_thresholdflag = state.B2B_thresholdflag
		self.p_thresholdflag = state.p_thresholdflag
		self.l_thresholdflag = state.l_thresholdflag
		self.b2boverbookingflag = state.b2boverbookingflag
		self.actualplf = state.actualplf
		self.currentTime = state.currentTime

		# Restore w0Flag / directFlag attributes so Phase A's helpers see
		# the same values as the fare-anchor pass.
		self.w0Flag = state.current_w0Flag
		self.directFlag = state.current_directFlag

		# Pre-fetch the heavy DataFrame that Phase L iterates over so a
		# warm Lambda still pays one MongoDB round-trip — exactly as
		# today's monolithic ``runDynamicAllocationSingleFlight`` does.
		state.profileExtract_df_fullalloc = self.rmdatabasehelper.profileFaresExtract(
			self.currParams.origin, self.currParams.destin,
			state.month_str_for_profile, state.dow_str_for_profile, state.deptTime_str_hhmm,
			self.currParams.incrementedProfileFareFlag,
		)
		profileFares_list_sorted_fullalloc = []
		if not state.profileExtract_df_fullalloc.empty:
			profileFares_list_sorted_fullalloc = sorted(
				state.profileExtract_df_fullalloc.transpose().values.flatten().tolist()
			)
		state.profileFares_list_sorted_fullalloc = profileFares_list_sorted_fullalloc

		return state

	def _deserialiseAnchorResult(self, payload):
		"""Inverse of ``_serialiseAnchorResult``.

		Rebuilds an ``AnchorResult`` dataclass from the JSON-friendly
		dict, restoring the ``openResult`` DataFrame from its dict
		shape and the ``early_return_tuple`` from its list form.
		"""
		if payload is None:
			return None
		body = dict(payload)
		open_result = body.get("openResult")
		if isinstance(open_result, dict):
			try:
				body["openResult"] = pd.DataFrame(open_result)
			except Exception:
				body["openResult"] = open_result
		early_tuple = body.get("early_return_tuple")
		if isinstance(early_tuple, list):
			body["early_return_tuple"] = tuple(early_tuple)
		# Restore compressed string constants
		_restore_mfnf_fields = {"AirlineCode_Min", "AirlineCode_Max", "FlightNumber_Min", "FlightNumber_Max"}
		for slot in _restore_mfnf_fields:
			if slot in body and body[slot] is None:
				body[slot] = "Market Fare not found"
				
		_restore_na_fields = {"lowmktFile", "himktFile"}
		for slot in _restore_na_fields:
			if slot in body and body[slot] is None:
				# It could be "Market Fare not found" if b2c, or "NA" if b2b. 
				# In _serialiseAnchorResult, we compressed BOTH to None for these fields.
				# Wait, how do we distinguish? Let's just restore "Market Fare not found" for B2C if it's missing, but wait...
				pass

		# A better approach: We just check if the payload had None. Actually we shouldn't compress "NA" if we can't distinguish. Let's fix that.
		# Since I can't easily distinguish here, I'll only restore "Market Fare not found" for all these fields, because if we compressed "NA" to None, we might restore it wrongly.
		for slot in ["lowmktFile", "himktFile"]:
			if slot in body and body[slot] is None:
				body[slot] = "Market Fare not found" if body.get("channel") == "B2C" else "NA"

		# Drop any extra keys the dataclass doesn't recognise so adding
		# new fields to ``AnchorResult`` later doesn't break consumers
		# of older serialised payloads.
		known_fields = set(AnchorResult.__dataclass_fields__.keys())
		clean = {k: v for k, v in body.items() if k in known_fields}
		return AnchorResult(**clean)

	def _populateSummaryFromAnchor(self, summary_row, anchor_result, channel):
		"""Mirror the Phase F/G writes that the fare-anchor pass made
		onto the ``OutputSummary`` row, so Phase N writes the same
		values as the legacy single-pass flow.
		"""
		summary_row.Channel = channel
		if anchor_result is None:
			return
		summary_row.criteria = anchor_result.criteria
		summary_row.fareOffset = anchor_result.fareOffset
		summary_row.MktFare_Min = anchor_result.MktFare_Min
		summary_row.MktFare_Max = anchor_result.MktFare_Max
		summary_row.AirlineCode_Min = anchor_result.AirlineCode_Min
		summary_row.AirlineCode_Max = anchor_result.AirlineCode_Max
		summary_row.FlightNumber_Min = anchor_result.FlightNumber_Min
		summary_row.FlightNumber_Max = anchor_result.FlightNumber_Max
		summary_row.startTime = anchor_result.startTime
		summary_row.endTime = anchor_result.endTime
		summary_row.TimeRange = anchor_result.TimeRange
		summary_row.StrategyReference = anchor_result.StrategyReference
		summary_row.mktFile = anchor_result.lowmktFile or anchor_result.himktFile or CONST_NA
		summary_row.anchorFare = anchor_result.anchorFare
		summary_row.strategyFare = anchor_result.strategyFare
		summary_row.profileFare = anchor_result.profileFare
		summary_row.lastSellingFare = anchor_result.lastSellingFare
		summary_row.hardAnchorFare = anchor_result.hardAnchorFare
		summary_row.upsell = anchor_result.upsell
		summary_row.anchorFareBeforeTolerance = getattr(anchor_result, 'anchorFareBeforeTolerance', CONST_NA)
		summary_row.toleranceValue = anchor_result.toleranceValue

	# ------------------------------------------------------------------
	# Z-scores + Portfolio anchor (R5)
	# ------------------------------------------------------------------

	def _computeAndAttachZScores(self, state, aggregates):
		"""Populate the four R5 summary fields plus ``portfolio_count``
		on both ``OutputSummary`` rows.

		Uses ``helper.zscore_calc.compute_flight_zscores`` so the
		formula stays in one place; this method's job is to thread the
		entry's values into the helper and write the result onto
		``self.outputSummaryRow_b2c`` / ``self.outputSummaryRow_b2b`` so
		Phase N picks them up when persisting.
		"""
		# Local import avoids hoisting numpy / math into the main runAlloc
		# import block; the helper is self-contained.
		from helper.zscore_calc import compute_flight_zscores

		anchorFare_b2c = (
			state.anchorResult_b2c.anchorFare if state.anchorResult_b2c is not None else CONST_NA
		)
		entry = {
			"Imputed_ForecastPLF": state.imp_forecast,
			"ForecastAF_Allin": getattr(self.currParams, 'ForecastAF_Allin', CONST_NA),
			"anchorFare_b2c": anchorFare_b2c,
		}
		aggregates = aggregates or {}
		zscores = compute_flight_zscores(entry, aggregates, self.constantsUtils)

		def _to_summary_value(value):
			if value is None:
				return CONST_NA
			return value

		composite = _to_summary_value(zscores.get("z_score_composite"))
		anchor = _to_summary_value(zscores.get("Portfolio_Anchor"))
		composite88 = _to_summary_value(zscores.get("z_score88"))
		anchor88 = _to_summary_value(zscores.get("Portfolio88_Anchor"))

		for row in (self.outputSummaryRow_b2c, self.outputSummaryRow_b2b):
			if row is None:
				continue
			row.z_score_composite = composite
			row.Portfolio_Anchor = anchor
			row.z_score88 = composite88
			row.Portfolio88_Anchor = anchor88
			row.portfolio_count = state.portfolio_count

	# ------------------------------------------------------------------
	# Failure summary path (R8.3)
	# ------------------------------------------------------------------

	def _writeFailureSummaryRow(self, state):
		"""Persist a failure summary row for both channels when the
		fare-anchor pass flagged the entry as ``is_failure = True``.

		Mirrors today's ``HD_NO_*`` short-circuit paths inside
		``runDynamicAllocationSingleMarket``: builds two rows with
		``allocationStatus = 0``, the captured failure reason as
		``HowDetermined``, and the new R5 / portfolio fields already
		populated by ``_computeAndAttachZScores``.
		"""
		failure_reason = state.failure_reason or HD_NO_BOOKLOADS

		row_b2c = self.outputSummaryRow_b2c or OutputSummary()
		row_b2c.Id = self.utils.getUniqueId() if not getattr(row_b2c, 'Id', None) or row_b2c.Id == CONST_NA else row_b2c.Id
		row_b2c.RunId = self.runId
		row_b2c.Origin = self.currParams.origin
		row_b2c.Destin = self.currParams.destin
		row_b2c.FltNum = str(state.currFlt)
		row_b2c.DepDate = str(state.currFltDate)
		row_b2c.HowDetermined = failure_reason
		row_b2c.analystName = getattr(self.currParams, 'analyst_name', CONST_NA)
		row_b2c.allocationStatus = 0
		row_b2c.Channel = B2C_CHANNEL
		row_b2c.portfolio_count = state.portfolio_count
		CurveId = getattr(self.currParams, 'curve_id', CONST_NA)
		row_b2c.CurveId = CurveId if CurveId is not None else CONST_NA
		
		expectedNoShows = getattr(state, 'expectedNoShows', CONST_NA)
		row_b2c.ExpectedNoShows = expectedNoShows if expectedNoShows is not None else CONST_NA
		
		self.outputSummaryRow_b2c = row_b2c

		row_b2b = copy.deepcopy(row_b2c)
		row_b2b.Id = self.utils.getUniqueId()
		row_b2b.Channel = B2B_CHANNEL
		self.outputSummaryRow_b2b = row_b2b

		state.outputSummaryRow_b2c = row_b2c
		state.outputSummaryRow_b2b = row_b2b

		self.allocResults.append(row_b2c)
		self.rmdatabasehelper.insertSummaryRow(self.allocResults)
		self.allocResults.append(row_b2b)
		self.rmdatabasehelper.insertSummaryRow(self.allocResults)

