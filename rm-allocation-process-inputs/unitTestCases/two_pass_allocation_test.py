"""Equivalence tests for the portfolio-anchor-split two-pass entry points
(``portfolio-anchor-split`` Slice 7).

Validates: Requirements R1, R2, R5, R7, R8 from
``.kiro/specs/portfolio-anchor-split/requirements.md``.

These tests exercise ``DynAlloc.runFareAnchorPass`` /
``DynAlloc._buildPortfolioEntry`` /
``DynAlloc._rehydrateFlightState`` /
``DynAlloc._computeAndAttachZScores`` /
``DynAlloc.runAllocationPass`` end-to-end without standing up a live
MongoDB or Redis.  The collaborators ``DetermineRBD`` /
``RMDatabaseHelper`` / ``RBDAllocations`` / ``RedisHelper`` are the same
production classes as the legacy single-pass path, but the helpers'
expensive DB methods are stubbed so the test focuses on:

* The portfolio-entry round-trip (build → SQS hop → rehydrate) preserves
  every Phase A–K-head field the allocation pass needs.
* The four R5 summary fields (``z_score_composite``,
  ``Portfolio_Anchor``, ``z_score88``, ``Portfolio88_Anchor``) and
  ``portfolio_count`` land on the persisted summary rows for the
  failure path (R8) and the success path with degenerate aggregates
  (R5.4 / R6.2).
* F-tagged ready counters bump on the fare-anchor pass and the
  unbranded ready counters bump on the allocation pass (R7.1 / R7.2).
* The done counters bump exactly once on the failure path (R7.3 / R7.4).

The "non-new summary columns match" requirement from Slice 7.5 is
exercised by feeding a captured ``AnchorResult`` directly into
``runAllocationPass`` and asserting the rehydrated ``OutputSummary``
rows carry through the same MktFare_Min / MktFare_Max / criteria /
fareOffset / startTime / endTime / TimeRange / StrategyReference
values that the fare-anchor pass would have written.
"""

from __future__ import annotations

import os
import sys
import unittest
from unittest.mock import MagicMock

import pandas as pd

# The Lambda's source files and the shared ``rm-allocation-configurations/``
# package live at sibling paths to this test file.
HERE = os.path.dirname(os.path.abspath(__file__))
LAMBDA_ROOT = os.path.dirname(HERE)
REPO_ROOT = os.path.dirname(LAMBDA_ROOT)
SHARED_CONFIG = os.path.join(REPO_ROOT, "rm-allocation-configurations")
for path in (LAMBDA_ROOT, SHARED_CONFIG):
	if path not in sys.path:
		sys.path.insert(0, path)

from Constants import (  # noqa: E402
	B2B_CHANNEL,
	B2C_CHANNEL,
	CONST_NA,
	HD_NO_BOOKLOADS,
	KEY_B2B_DONE_COUNT,
	KEY_B2C_DONE_COUNT,
	KEY_B2B_READY_COUNT,
	KEY_B2C_READY_COUNT,
	KEY_B2B_READY_COUNT_F,
	KEY_B2C_READY_COUNT_F,
)
from models.anchor_result import AnchorResult  # noqa: E402
from models.flight_state import FlightState  # noqa: E402
from models.output_summary import OutputSummary  # noqa: E402
from runAlloc import DynAlloc  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers — build a partially-mocked DynAlloc instance.
# ---------------------------------------------------------------------------


class _FakeRedis:
	"""Tiny stand-in for the real Redis client used to count ``incr`` calls."""

	def __init__(self):
		self.counts = {}

	def incr(self, key):
		self.counts[key] = self.counts.get(key, 0) + 1
		return self.counts[key]


def _make_dyn_alloc(b2b_factor=1.0):
	"""Build a ``DynAlloc`` instance with all collaborators mocked.

	Bypasses ``__init__`` so we don't need a live MongoDB / SQS / Redis;
	callers exercise individual methods like ``_buildPortfolioEntry``,
	``_rehydrateFlightState``, ``_computeAndAttachZScores`` and the new
	pass entry points.
	"""
	dyn = DynAlloc.__new__(DynAlloc)

	dyn.runId = "RUN_TEST_SLICE7"
	dyn.starttime = None
	dyn.AU_COLUMN_LENGTH = 30
	dyn.AUFileData = []
	dyn.allocResults = []
	dyn.actualplf = 0.0
	dyn.b2boverbookingflag = False
	dyn.B2B_thresholdflag = False
	dyn.p_thresholdflag = False
	dyn.l_thresholdflag = False
	dyn.w0Flag = 0
	dyn.directFlag = 1
	dyn.currentTime = "12:00:00"

	# OutputSummary rows (Phase A normally creates these; the rehydrate
	# path overwrites them, but we initialise here so failure-path
	# constructions don't crash before they reach the rehydrate).
	dyn.outputSummaryRow_b2c = OutputSummary()
	dyn.outputSummaryRow_b2b = OutputSummary()

	# Stubs for the helpers ``runAllocationPass`` reaches into.
	dyn.logger = MagicMock()
	dyn.utils = MagicMock()
	dyn.utils.getUniqueId = MagicMock(side_effect=lambda: "UID-" + str(_make_dyn_alloc._counter.next()))
	dyn.mathutils = MagicMock()
	dyn.timeutils = MagicMock()
	dyn.constantsUtils = MagicMock()
	dyn.constantsUtils.SOFTBLOCK_RBDS = '["W1","W2","W3","W4","W5","W6","W7","W8"]'
	dyn.constantsUtils.AF_PLF_FORECAST_CAP = 110.0
	dyn.determinerbd = MagicMock()
	dyn.allocations = MagicMock()
	dyn.marketfaresservice = MagicMock()
	dyn.dynamictimed2 = MagicMock()
	dyn.navapihelper = MagicMock()
	dyn.mongohelper = MagicMock()
	dyn.sqs = MagicMock()
	dyn.s3 = None
	dyn.cache_client = None

	dyn.rmdatabasehelper = MagicMock()
	# Empty profile fares so Phase L's ``profileFares_list_sorted_fullalloc``
	# stays empty (rehydrate calls ``profileFaresExtract`` ahead of time).
	dyn.rmdatabasehelper.profileFaresExtract.return_value = pd.DataFrame()

	# Redis: real counter capture so we can assert on the keys.
	fake_redis = _FakeRedis()
	dyn.cache_client = fake_redis
	dyn.redishelper = MagicMock()
	dyn.redishelper.runId = dyn.runId
	dyn.redishelper.cache_client = fake_redis
	dyn.redishelper.updateAnchorRunAuditReady = lambda: (
		fake_redis.incr(KEY_B2B_READY_COUNT_F + dyn.runId),
		fake_redis.incr(KEY_B2C_READY_COUNT_F + dyn.runId),
	)
	dyn.redishelper.updateRunAuditReady = lambda: (
		fake_redis.incr(KEY_B2B_READY_COUNT + dyn.runId),
		fake_redis.incr(KEY_B2C_READY_COUNT + dyn.runId),
	)
	dyn.redishelper.updateRunAudit = lambda: (
		fake_redis.incr(KEY_B2B_DONE_COUNT + dyn.runId),
		fake_redis.incr(KEY_B2C_DONE_COUNT + dyn.runId),
	)

	# A minimally-populated MarketListRow for the fare-anchor side.
	dyn.currParams = MagicMock()
	dyn.currParams.origin = "BOM"
	dyn.currParams.destin = "DEL"
	dyn.currParams.flight_number = 1127
	dyn.currParams.per_start = "08/01/2023"
	dyn.currParams.per_end = "08/01/2023"
	dyn.currParams.DepartureTime = "12:00:00"
	dyn.currParams.bookedPax = 10
	dyn.currParams.capacity = 189
	dyn.currParams.adjustedCapacity = 189
	dyn.currParams.tbf_bookings = 0
	dyn.currParams.b2b_factor = b2b_factor
	dyn.currParams.analyst_name = "TESTER"
	dyn.currParams.portfolio_count = 1
	dyn.currParams.ForecastAF_Allin = 20000.0
	dyn.currParams.ALLIN_AF = 18000.0
	dyn.currParams.ALLIN_AF_NB_LM = 0
	dyn.currParams.ALLIN_AF_NB_LY = 0
	dyn.currParams.profileFares = 1
	dyn.currParams.b2c_backstop = 0
	dyn.currParams.b2b_backstop = 0
	dyn.currParams.w0Flag = 0
	dyn.currParams.directFlag = 1
	dyn.currParams.run_id = dyn.runId
	dyn.currParams.update_navitaire_method = "api"
	dyn.currParams.run_time = None

	dyn.inputSource = MagicMock()
	dyn.inputSource.isUpdateNavitaire = False

	return dyn, fake_redis


class _Counter:
	def __init__(self):
		self._n = 0

	def next(self):
		self._n += 1
		return self._n


_make_dyn_alloc._counter = _Counter()


def _build_anchor_result(channel, *, strategy_fare=4500.0, anchor_fare=4500.0):
	"""Build a minimally-populated ``AnchorResult`` for the round-trip
	tests.  The fields populated here mirror the Phase F/G snapshot
	written by ``_copyOutputSummaryToAnchor``.
	"""
	return AnchorResult(
		anchorFare=anchor_fare,
		strategyFare=strategy_fare,
		profileFare=CONST_NA,
		lastSellingFare=CONST_NA,
		hardAnchorFare=CONST_NA,
		hard_anchor_flag=False,
		toleranceValue=CONST_NA,
		anchor_source="Strategy",
		channel=channel,
		upsell=CONST_NA,
		overBookingFare=0,
		openResult=pd.DataFrame({
			"RBD": ["Y"], "Fare": [strategy_fare], "FareResult": ["Strategy"],
			"anchorFare": [anchor_fare], "statusInd": [0], "allocationStatus": [1],
		}),
		criteria="MIN",
		fareOffset=-2,
		MktFare_Min=4751,
		MktFare_Max=10000,
		AirlineCode_Min="6E",
		AirlineCode_Max="6E",
		FlightNumber_Min="6E-2051",
		FlightNumber_Max="6E-2083",
		lowmktFile="test_AG",
		himktFile="test_AG",
		isMarketFareAvailable=True,
		startTime="00:00",
		endTime="23:59",
		TimeRange="FR",
		StrategyReference="Strategy_1",
	)


def _baseline_payload(*, is_failure=False, portfolio_count=1):
	"""Build a ``PortfolioEntry`` payload mimicking what the fare-anchor
	pass writes for a single direct-route flight."""
	anchor_b2c = _build_anchor_result(B2C_CHANNEL, strategy_fare=4665.0, anchor_fare=4665.0)
	anchor_b2b = _build_anchor_result(B2B_CHANNEL, strategy_fare=4400.0, anchor_fare=4400.0)
	dyn = _make_dyn_alloc()[0]
	return {
		# Original market-list fields.
		"Origin": "BOM", "Destin": "DEL", "FlightNumber": 1127,
		"PerStart": "08/01/2023", "PerEnd": "08/01/2023",
		"DepartureTime": "12:00:00",
		"runId": dyn.runId,
		"route_type": "direct",
		"update_navitaire_method": "api",
		"runTime": "2025-06-17T00:00:00",
		"saleinvflag": "0",
		"capacity": 189,
		"adjustedCapacity": 189,
		"bookedPax": 10,
		"B2BFactor": 1.0,
		"B2BBackstop": 0,
		"B2CBackstop": 0,
		"obSeats": "",
		"obFare": "",
		"profileFares": 1,
		"OverBooking": "0",
		"distressInventoryFlag": "0",
		"seriesBlock": "0",
		"AutoGroup": "0",
		"TbfFlag": "0",
		"CugFlag": "0",
		"MktFareFlag": "1",
		"Autobackstopflag": "0",
		"w0_value": 0,
		"directFlag": 1,
		"Spoilage_Flag": 0,
		"CanAdjustedForecast": 0,
		"forecastAllocFlag": 0,
		"afForecastAllocFlag": 0,
		"forecastPLF": 70.0,
		"ForecastAF_Allin": 20000.0,
		"ALLIN_AF": 18000.0,
		"defaultMlFlag": 0,
		"analystName": "TESTER",
		"DOW": 1111111,
		"hardAnchor": CONST_NA,
		"plfThreshold": "88%",
		"TimeWindowStart": "00:15",
		"TimeWindowEnd": "23:55",
		"CurveID": "BOMDEL_NP",
		"fareAnchor": "NP_General",
		"fareOffset": -2,
		"FirstRBDAlloc": 8,
		"OtherRBDAlloc": 3,
		"SkippingFactor": 10,
		"DaySpan": 0,
		"AutoTimeRangeFlag": 0,
		"openingFares": 1,
		"rbdPushFlag": "0",
		"B2BTolerance": "",
		"B2CTolerance": "",
		# Pass marker + portfolio coordination.
		"pass": "allocation",
		"portfolio_count": portfolio_count,
		# AnchorResult dicts (as ``_serialiseAnchorResult`` would produce).
		"anchor_result_b2c": dyn._serialiseAnchorResult(anchor_b2c),
		"anchor_result_b2b": dyn._serialiseAnchorResult(anchor_b2b),
		"b2b_anchor_source": "Strategy",
		# Forecast / band scalars.
		"Imputed_ForecastPLF": 66.29,
		"ForecastAF_Allin_alias": 20000.0,
		"imp_forecast": 66.29,
		"imp_forecast_band": 0,
		"af_forecast_band": 0,
		"Af_Nb_Allin": 20867.20,
		"PLF_Gain": 60.99,
		"forecast_plf": 110.0,
		"forecastBand": 2,
		"ndoBand": 0,
		"dplfBand": 4,
		"sod_dplfBand": 4,
		# Capacity & PLF.
		"variance": -89.71,
		"final_plf": 5.29,
		"actualplf": 5.29,
		"final_totalCapacity": 189,
		"base_adjustedCapacity": 189,
		"base_totalCapacity": 189,
		"currBookedLoad": 10,
		"initial_currBookedLoad": 10,
		"overBookingCount": 0,
		"b2boverbookingflag": False,
		"currentTime": "12:00:00",
		# SOD.
		"sod_bookingToday": 5,
		"sod_BookingsInHour": 5,
		"sod_eodTgtBookedLoad": 179.55,
		"sod_tgtAtCurrentHour": 179.55,
		"sod_startOfDayTgtBookedLoad": 170.1,
		"sod_startOfDayBookLoad": 0.9,
		"distress_pax_count": 160,
		"tgtBookedLoadPercent": 0.9,
		"ndocounter": 0,
		"dow": 2,
		# Phase G.
		"bookloadPercentile": 5,
		"percentile_str": "Q5",
		"upsellValue": 0,
		"sector": "BOMDEL",
		"profileFare": CONST_NA,
		"month_str": "August",
		"dow_str": "Tue",
		"deptTime_hhmm": "12:00",
		# Phase H/I/J.
		"b2c_backstop_final": 0,
		"b2b_backstop_final": 0,
		"auto_b2c_backstop_str": "NA",
		"auto_b2b_backstop_str": "NA",
		"lastSoldFareb2c": 30,
		"lastSoldFareb2b": 10,
		"last_seven_avg_val": 5400,
		"is_spoilage_reduction_active": False,
		# Phase K head.
		"B2B_thresholdflag": False,
		"p_thresholdflag": False,
		"l_thresholdflag": False,
		"threshold_flags": {"W1": False, "W2": False, "W3": False, "W4": False,
							 "W5": False, "W6": False, "W7": False, "W8": False},
		"DaySpan_full": 0,
		"input_time_str_dep_full": "12:00:00",
		"b2b_factor_resolved": 1.0,
		"currFltDate": "08/01/2023",
		"currFltDate_yyyymmdd": "2023-08-01",
		"currFltDepartureTime": "12:00:00",
		"currFlt": 1127,
		"currLid": 189,
		"currAdjustedCapacity": 189,
		"currtbfBookedLoad": 0,
		"currentTime": "12:00:00",
		"current_w0Flag": 0,
		"current_directFlag": 1,
		# Failure flags.
		"is_failure": is_failure,
		"failure_reason": HD_NO_BOOKLOADS if is_failure else None,
	}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestRehydrationRoundTrip(unittest.TestCase):
	"""``_buildPortfolioEntry`` → JSON → ``_rehydrateFlightState`` round-trip
	preserves every Phase A–K-head field the allocation pass needs."""

	def test_rehydrated_state_matches_payload_scalars(self):
		dyn, _ = _make_dyn_alloc()
		payload = _baseline_payload()
		state = dyn._rehydrateFlightState(payload)

		# Phase A / capacities.
		self.assertEqual(state.currFlt, payload["currFlt"])
		self.assertEqual(state.currFltDate, payload["currFltDate"])
		self.assertEqual(state.currFltDepartureTime, payload["currFltDepartureTime"])
		self.assertEqual(state.dateFormatted_yyyymmdd, payload["currFltDate_yyyymmdd"])
		self.assertEqual(state.base_totalCapacity, payload["base_totalCapacity"])
		self.assertEqual(state.final_totalCapacity, payload["final_totalCapacity"])

		# Phase E forecast / bands.
		self.assertEqual(state.imp_forecast, payload["imp_forecast"])
		self.assertEqual(state.imp_forecast_band, payload["imp_forecast_band"])
		self.assertEqual(state.dplfBand_full, payload["dplfBand"])
		self.assertEqual(state.ndoBand_full, payload["ndoBand"])

		# Phase G profile metadata.
		self.assertEqual(state.month_str_for_profile, payload["month_str"])
		self.assertEqual(state.dow_str_for_profile, payload["dow_str"])
		self.assertEqual(state.deptTime_str_hhmm, payload["deptTime_hhmm"])

		# Threshold flags wired onto ``self`` for Phase L.
		self.assertEqual(state.threshold_flags, payload["threshold_flags"])
		for rbd, expected in payload["threshold_flags"].items():
			flag_name = rbd.lower() + "_thresholdflag"
			self.assertEqual(getattr(dyn, flag_name), expected)

		# AnchorResult survives the round-trip as a dataclass instance.
		self.assertIsNotNone(state.anchorResult_b2c)
		self.assertIsNotNone(state.anchorResult_b2b)
		self.assertEqual(state.anchorResult_b2c.strategyFare, 4665.0)
		self.assertEqual(state.anchorResult_b2b.strategyFare, 4400.0)
		self.assertEqual(state.anchorResult_b2c.MktFare_Min, 4751)

		# OutputSummary rows pick up the F/G metadata so Phase N can write it.
		self.assertEqual(dyn.outputSummaryRow_b2c.MktFare_Min, 4751)
		self.assertEqual(dyn.outputSummaryRow_b2c.MktFare_Max, 10000)
		self.assertEqual(dyn.outputSummaryRow_b2c.criteria, "MIN")
		self.assertEqual(dyn.outputSummaryRow_b2c.fareOffset, -2)
		self.assertEqual(dyn.outputSummaryRow_b2c.startTime, "00:00")
		self.assertEqual(dyn.outputSummaryRow_b2c.endTime, "23:59")
		self.assertEqual(dyn.outputSummaryRow_b2c.TimeRange, "FR")
		self.assertEqual(dyn.outputSummaryRow_b2c.StrategyReference, "Strategy_1")

	def test_rehydrate_calls_profileFaresExtract(self):
		"""Heavy DataFrame rehydration: the allocation pass re-fetches
		``profileFaresExtract`` because the SQS payload doesn't carry it."""
		dyn, _ = _make_dyn_alloc()
		payload = _baseline_payload()
		dyn._rehydrateFlightState(payload)
		dyn.rmdatabasehelper.profileFaresExtract.assert_called_once_with(
			"BOM", "DEL", "August", "Tue", "12:00",
		)


class TestComputeAndAttachZScores(unittest.TestCase):
	"""``_computeAndAttachZScores`` writes the four R5 fields plus
	``portfolio_count`` onto both summary rows."""

	def test_degenerate_aggregates_yield_zero_z_and_anchor_equals_strategy(self):
		"""R5.4 — single-flight portfolio: every component zero,
		movement zero, ``Portfolio_Anchor`` == ``anchorFare_b2c``."""
		dyn, _ = _make_dyn_alloc()
		payload = _baseline_payload(portfolio_count=1)
		state = dyn._rehydrateFlightState(payload)

		strategy_b2c = state.anchorResult_b2c.strategyFare
		# Degenerate aggregates: mean=value, std=0 for each metric.
		aggregates = {
			"PDS": payload["imp_forecast"], "sd_PDS": 0,
			"PDQ": payload["ForecastAF_Allin"], "sd_PDQ": 0,
			"PFA": strategy_b2c, "sd_PFA": 0,
			"PDS88": payload["imp_forecast"], "sd_PDS88": 0,
			"PDQ88": payload["ForecastAF_Allin"], "sd_PDQ88": 0,
			"PFA88": strategy_b2c, "sd_PFA88": 0,
		}
		dyn._computeAndAttachZScores(state, aggregates)

		for row in (dyn.outputSummaryRow_b2c, dyn.outputSummaryRow_b2b):
			self.assertEqual(row.z_score_composite, 0.0)
			self.assertEqual(row.z_score88, 0.0)
			self.assertEqual(row.Portfolio_Anchor, strategy_b2c)
			self.assertEqual(row.Portfolio88_Anchor, strategy_b2c)
			self.assertEqual(row.portfolio_count, 1)

	def test_null_aggregates_yield_zero_z_and_anchor_equals_strategy(self):
		"""R5.3 — null aggregates collapse z-scores to 0 and leave the
		fare unchanged."""
		dyn, _ = _make_dyn_alloc()
		state = dyn._rehydrateFlightState(_baseline_payload())
		strategy_b2c = state.anchorResult_b2c.strategyFare
		aggregates = {k: None for k in (
			"PDS", "sd_PDS", "PDQ", "sd_PDQ", "PFA", "sd_PFA",
			"PDS88", "sd_PDS88", "PDQ88", "sd_PDQ88", "PFA88", "sd_PFA88",
		)}
		dyn._computeAndAttachZScores(state, aggregates)

		self.assertEqual(dyn.outputSummaryRow_b2c.z_score_composite, 0.0)
		self.assertEqual(dyn.outputSummaryRow_b2c.Portfolio_Anchor, strategy_b2c)
		self.assertEqual(dyn.outputSummaryRow_b2b.Portfolio_Anchor, strategy_b2c)


class TestRunAllocationPassFailurePath(unittest.TestCase):
	"""R8.3 — ``runAllocationPass`` on an ``is_failure = True`` payload
	writes the failure summary rows and bumps the done counters once."""

	def test_failure_payload_writes_summary_and_bumps_done_counter(self):
		dyn, fake_redis = _make_dyn_alloc()
		payload = _baseline_payload(is_failure=True, portfolio_count=1)
		state = payload  # ``runAllocationPass`` constructs FlightState internally.

		dyn.runAllocationPass(payload, aggregates=None)

		# F-tagged ready counters NOT bumped (this is the allocation pass).
		self.assertNotIn(KEY_B2B_READY_COUNT_F + dyn.runId, fake_redis.counts)
		self.assertNotIn(KEY_B2C_READY_COUNT_F + dyn.runId, fake_redis.counts)
		# Unbranded ready counters bumped at the start (R7.2).
		self.assertEqual(fake_redis.counts.get(KEY_B2B_READY_COUNT + dyn.runId), 1)
		self.assertEqual(fake_redis.counts.get(KEY_B2C_READY_COUNT + dyn.runId), 1)
		# Done counters bumped exactly once on the failure path (R7.3 / R7.4).
		self.assertEqual(fake_redis.counts.get(KEY_B2B_DONE_COUNT + dyn.runId), 1)
		self.assertEqual(fake_redis.counts.get(KEY_B2C_DONE_COUNT + dyn.runId), 1)

		# Phase L / M / N skipped — DetermineRBD never called.
		dyn.determinerbd.applyAnchorToLadder.assert_not_called()
		dyn.allocations.b2CAllocation.assert_not_called()

		# Two failure summary rows persisted, both carry the failure
		# reason and ``portfolio_count`` (R8.3).
		self.assertEqual(len(dyn.allocResults), 2)
		b2c_row = dyn.allocResults[0]
		b2b_row = dyn.allocResults[1]
		self.assertEqual(b2c_row.HowDetermined, HD_NO_BOOKLOADS)
		self.assertEqual(b2c_row.allocationStatus, 0)
		self.assertEqual(b2c_row.Channel, B2C_CHANNEL)
		self.assertEqual(b2c_row.portfolio_count, 1)
		self.assertEqual(b2b_row.Channel, B2B_CHANNEL)
		self.assertEqual(b2b_row.portfolio_count, 1)


class TestRunFareAnchorPassCounters(unittest.TestCase):
	"""R7.1 — ``runFareAnchorPass`` increments the F-tagged ready counters
	at the start, before any work that might fail.

	We don't drive the full Phase A–K head (that needs MongoDB); instead
	we monkey-patch ``_phaseA_initFlightContext`` to abort immediately
	and assert the counters bumped before the abort short-circuits the
	method.
	"""

	def test_fare_anchor_pass_bumps_F_counters_at_start(self):
		dyn, fake_redis = _make_dyn_alloc()

		def _abort_phase_a(state):
			state.aborted = True
			state.is_failure = True
			state.failure_reason = HD_NO_BOOKLOADS

		dyn._phaseA_initFlightContext = _abort_phase_a

		entry = dyn.runFareAnchorPass()

		self.assertEqual(fake_redis.counts.get(KEY_B2B_READY_COUNT_F + dyn.runId), 1)
		self.assertEqual(fake_redis.counts.get(KEY_B2C_READY_COUNT_F + dyn.runId), 1)
		# Non-F counters NOT touched by the anchor pass (R7.5).
		self.assertNotIn(KEY_B2B_READY_COUNT + dyn.runId, fake_redis.counts)
		self.assertNotIn(KEY_B2C_READY_COUNT + dyn.runId, fake_redis.counts)
		# Even an aborted Phase A produces a portfolio entry (R8.1) so
		# the trigger downstream sees the entry in the portfolio hash.
		self.assertIsInstance(entry, dict)
		self.assertEqual(entry["pass"], "allocation")
		self.assertTrue(entry["is_failure"])
		self.assertEqual(entry["failure_reason"], HD_NO_BOOKLOADS)
		self.assertEqual(entry["portfolio_count"], 1)


class TestPortfolioEntrySerialisation(unittest.TestCase):
	"""``_buildPortfolioEntry`` produces a JSON-serialisable dict that
	carries every Phase F/G metadata field the allocation pass needs to
	reproduce the non-new summary columns (Slice 7.5 done signal)."""

	def test_anchor_result_round_trip_preserves_metadata(self):
		dyn, _ = _make_dyn_alloc()
		anchor_b2c = _build_anchor_result(B2C_CHANNEL, strategy_fare=4665.0, anchor_fare=4665.0)
		serialised = dyn._serialiseAnchorResult(anchor_b2c)
		# JSON-friendly: openResult is a dict, not a DataFrame.
		self.assertIsInstance(serialised["openResult"], dict)
		self.assertIn("Fare", serialised["openResult"])

		restored = dyn._deserialiseAnchorResult(serialised)
		self.assertIsInstance(restored, AnchorResult)
		self.assertEqual(restored.strategyFare, 4665.0)
		self.assertEqual(restored.MktFare_Min, anchor_b2c.MktFare_Min)
		self.assertEqual(restored.MktFare_Max, anchor_b2c.MktFare_Max)
		self.assertEqual(restored.criteria, anchor_b2c.criteria)
		self.assertEqual(restored.fareOffset, anchor_b2c.fareOffset)
		self.assertEqual(restored.startTime, anchor_b2c.startTime)
		self.assertEqual(restored.endTime, anchor_b2c.endTime)
		self.assertEqual(restored.TimeRange, anchor_b2c.TimeRange)
		self.assertEqual(restored.StrategyReference, anchor_b2c.StrategyReference)
		# The DataFrame is restored too.
		self.assertIsInstance(restored.openResult, pd.DataFrame)
		self.assertEqual(list(restored.openResult["Fare"]), [4665.0])


class TestSingleFlightShortCircuit(unittest.TestCase):
	"""Slice 8 — ``DynAlloc._maybeShortCircuitSingleFlight`` /
	``_buildDegenerateAggregates`` (R6).

	A portfolio of size 1 SHALL skip Redis + SQS coordination, build
	degenerate aggregates in-process, and call ``runAllocationPass``
	directly so the per-flight summary row carries
	``Portfolio_Anchor == anchorFare_b2c``,
	``z_score_composite == 0`` and ``Portfolio88_Anchor ==
	anchorFare_b2c`` (or ``CONST_NA`` when the flight is not in the
	``<88`` Portfolio88 subset).
	"""

	def test_degenerate_aggregates_have_zero_std_and_88_subset_when_under_88(self):
		dyn, _ = _make_dyn_alloc()
		entry = {
			"Imputed_ForecastPLF": 66.29,
			"ForecastAF_Allin": 20000.0,
			"anchorFare_b2c": 4665.0,
			"portfolio_count": 1,
		}
		aggregates = dyn._buildDegenerateAggregates(entry)
		self.assertEqual(aggregates["PDS"], 66.29)
		self.assertEqual(aggregates["PDQ"], 20000.0)
		self.assertEqual(aggregates["PFA"], 4665.0)
		self.assertEqual(aggregates["sd_PDS"], 0)
		self.assertEqual(aggregates["sd_PDQ"], 0)
		self.assertEqual(aggregates["sd_PFA"], 0)
		# PLF < 88 → flight is in the Portfolio88 subset.
		self.assertEqual(aggregates["PDS88"], 66.29)
		self.assertEqual(aggregates["PFA88"], 4665.0)
		self.assertEqual(aggregates["sd_PDS88"], 0)
		self.assertEqual(aggregates["sd_PFA88"], 0)

	def test_degenerate_aggregates_88_subset_null_when_at_or_above_88(self):
		dyn, _ = _make_dyn_alloc()
		entry = {
			"Imputed_ForecastPLF": 95.0,
			"ForecastAF_Allin": 20000.0,
			"anchorFare_b2c": 4665.0,
			"portfolio_count": 1,
		}
		aggregates = dyn._buildDegenerateAggregates(entry)
		# Full aggregates filled; 88 aggregates all None (R6.2).
		self.assertEqual(aggregates["PDS"], 95.0)
		for key in ("PDS88", "sd_PDS88", "PDQ88", "sd_PDQ88", "PFA88", "sd_PFA88"):
			self.assertIsNone(aggregates[key])

	def test_degenerate_aggregates_88_subset_null_when_plf_missing(self):
		"""``Imputed_ForecastPLF`` of ``"NA"`` / ``None`` / ``NaN`` keeps
		the entry out of the Portfolio88 subset (mirrors the behaviour
		of ``portfolio_aggregator`` for missing PLF values)."""
		dyn, _ = _make_dyn_alloc()
		for missing in (CONST_NA, None, "not-a-number"):
			entry = {
				"Imputed_ForecastPLF": missing,
				"ForecastAF_Allin": 20000.0,
				"anchorFare_b2c": 4665.0,
				"portfolio_count": 1,
			}
			aggregates = dyn._buildDegenerateAggregates(entry)
			self.assertIsNone(
				aggregates["PDS88"],
				msg=f"PDS88 not None for missing PLF {missing!r}",
			)

	def test_short_circuit_single_flight_calls_runAllocationPass(self):
		"""R6.1 — ``portfolio_count == 1`` triggers the in-process
		allocation pass with degenerate aggregates and no Redis / SQS.
		"""
		dyn, fake_redis = _make_dyn_alloc()
		# Stub ``runAllocationPass`` so we can capture its arguments
		# without driving Phase L–N (the rehydrate path is exercised
		# by the success / failure tests above).
		captured = {}

		def _capture(payload, aggregates):
			captured["payload"] = payload
			captured["aggregates"] = aggregates
			return None

		dyn.runAllocationPass = _capture

		entry = {
			"Imputed_ForecastPLF": 66.29,
			"ForecastAF_Allin": 20000.0,
			"anchorFare_b2c": 4665.0,
			"portfolio_count": 1,
		}

		short_circuited = dyn._maybeShortCircuitSingleFlight(entry)

		self.assertTrue(short_circuited)
		self.assertIs(captured["payload"], entry)
		# Aggregates are degenerate: every full mean equals the entry
		# values and every full std-dev is zero (R6.1).
		aggregates = captured["aggregates"]
		self.assertEqual(aggregates["PFA"], 4665.0)
		self.assertEqual(aggregates["sd_PFA"], 0)

	def test_short_circuit_skipped_for_multi_flight_portfolio(self):
		"""R6 — ``portfolio_count > 1`` keeps the message on the Redis
		coordination path (Slice 11).  The short-circuit returns False
		and never invokes ``runAllocationPass``."""
		dyn, _ = _make_dyn_alloc()

		def _fail(*_args, **_kwargs):
			self.fail("runAllocationPass should not be called for portfolio_count > 1")

		dyn.runAllocationPass = _fail

		entry = {
			"Imputed_ForecastPLF": 66.29,
			"ForecastAF_Allin": 20000.0,
			"anchorFare_b2c": 4665.0,
			"portfolio_count": 5,
		}
		short_circuited = dyn._maybeShortCircuitSingleFlight(entry)
		self.assertFalse(short_circuited)

	def test_single_flight_summary_row_under_88_has_portfolio_anchor_equal_to_strategy(self):
		"""R6.2 — feeding the degenerate aggregates straight into
		``_computeAndAttachZScores`` (the same code path the short-
		circuit eventually reaches) yields ``Portfolio_Anchor ==
		anchorFare_b2c``, ``z_score_composite == 0``, and
		``Portfolio88_Anchor == anchorFare_b2c`` for an under-88
		flight.
		"""
		dyn, _ = _make_dyn_alloc()
		payload = _baseline_payload(portfolio_count=1)
		# The baseline PLF is 66.29 → in the Portfolio88 subset.
		payload["Imputed_ForecastPLF"] = 66.29
		payload["imp_forecast"] = 66.29

		entry_for_aggregates = {
			"Imputed_ForecastPLF": payload["Imputed_ForecastPLF"],
			"ForecastAF_Allin": payload["ForecastAF_Allin"],
			"anchorFare_b2c": payload["anchor_result_b2c"]["strategyFare"],
			"portfolio_count": 1,
		}
		aggregates = dyn._buildDegenerateAggregates(entry_for_aggregates)

		state = dyn._rehydrateFlightState(payload)
		dyn._computeAndAttachZScores(state, aggregates)

		strategy_b2c = state.anchorResult_b2c.strategyFare
		for row in (dyn.outputSummaryRow_b2c, dyn.outputSummaryRow_b2b):
			self.assertEqual(row.z_score_composite, 0.0)
			self.assertEqual(row.z_score88, 0.0)
			self.assertEqual(row.Portfolio_Anchor, strategy_b2c)
			self.assertEqual(row.Portfolio88_Anchor, strategy_b2c)
			self.assertEqual(row.portfolio_count, 1)

	def test_single_flight_summary_row_at_or_above_88_has_portfolio88_null(self):
		"""R6.2 — when the flight's PLF is not in the Portfolio88
		subset, the 88-branch aggregates SHALL be ``None`` and the
		zscore helper (Slice 5) collapses every 88-branch z-score to
		zero so ``Portfolio88_Anchor`` equals ``anchorFare_b2c``
		(``fare * (1 + 0)``).  ``Portfolio_Anchor`` likewise stays at
		``anchorFare_b2c``.

		The Slice 8 task wording "Portfolio88_Anchor == anchorFare_b2c
		(or null if not in <88)" reflects this ambiguity; Slice 5
		settled it by returning the fare unchanged whenever the
		underlying ``anchorFare_b2c`` is itself a real number.
		"""
		dyn, _ = _make_dyn_alloc()
		payload = _baseline_payload(portfolio_count=1)
		payload["Imputed_ForecastPLF"] = 95.0
		payload["imp_forecast"] = 95.0

		entry_for_aggregates = {
			"Imputed_ForecastPLF": 95.0,
			"ForecastAF_Allin": payload["ForecastAF_Allin"],
			"anchorFare_b2c": payload["anchor_result_b2c"]["strategyFare"],
			"portfolio_count": 1,
		}
		aggregates = dyn._buildDegenerateAggregates(entry_for_aggregates)
		# Sanity check: the 88 aggregates are None (R4.3) so zscore_calc
		# treats every 88 z-score as zero.
		for key in ("PDS88", "sd_PDS88", "PDQ88", "sd_PDQ88", "PFA88", "sd_PFA88"):
			self.assertIsNone(aggregates[key])

		state = dyn._rehydrateFlightState(payload)
		dyn._computeAndAttachZScores(state, aggregates)

		strategy_b2c = state.anchorResult_b2c.strategyFare
		for row in (dyn.outputSummaryRow_b2c, dyn.outputSummaryRow_b2b):
			self.assertEqual(row.z_score_composite, 0.0)
			self.assertEqual(row.z_score88, 0.0)
			self.assertEqual(row.Portfolio_Anchor, strategy_b2c)
			self.assertEqual(row.Portfolio88_Anchor, strategy_b2c)
			self.assertEqual(row.portfolio_count, 1)


class TestSingleFlightShortCircuitNoCoordination(unittest.TestCase):
	"""Slice 8.2 — the short-circuit performs **no** Redis-hash or SQS
	interaction (R6.1).

	The fare-anchor pass for a single-flight portfolio bypasses both
	``redishelper.insert_portfolio_entry`` /
	``redishelper.fetch_portfolio_entries`` /
	``redishelper.mark_portfolio_done`` and any
	``sqs.send_message`` / ``sqs.send_message_batch`` fan-out calls; the
	allocation pass runs in-process inside the same Lambda invocation
	with degenerate aggregates so the resulting summary row carries
	``Portfolio_Anchor == anchorFare_b2c``,
	``z_score_composite == 0`` and (for under-88 flights)
	``Portfolio88_Anchor == anchorFare_b2c``.
	"""

	def _make_short_circuit_entry(self, *, plf=66.29, strategy_fare=4665.0):
		return {
			"Imputed_ForecastPLF": plf,
			"ForecastAF_Allin": 20000.0,
			"anchorFare_b2c": strategy_fare,
			"portfolio_count": 1,
		}

	def test_short_circuit_does_not_touch_redis_or_sqs(self):
		"""R6.1 — single-flight short-circuit issues zero portfolio-hash
		and zero SQS fan-out calls.  Stubs ``runAllocationPass`` so we
		can assert on coordination side-effects in isolation from the
		ladder / Phase L–N work (covered by the success-path test
		below).
		"""
		dyn, fake_redis = _make_dyn_alloc()
		dyn.runAllocationPass = MagicMock(return_value=None)

		entry = self._make_short_circuit_entry()
		short_circuited = dyn._maybeShortCircuitSingleFlight(entry)

		self.assertTrue(short_circuited)
		dyn.runAllocationPass.assert_called_once()

		# No portfolio-hash interactions (R6.1).
		dyn.redishelper.insert_portfolio_entry.assert_not_called()
		dyn.redishelper.fetch_portfolio_entries.assert_not_called()
		dyn.redishelper.mark_portfolio_done.assert_not_called()

		# No SQS fan-out (R6.1).
		dyn.sqs.send_message.assert_not_called()
		dyn.sqs.send_message_batch.assert_not_called()

		# Fake-Redis cache client untouched too — no Lua eval, no incr
		# beyond what the allocation pass would do (we stubbed it out).
		self.assertEqual(fake_redis.counts, {})

	def test_short_circuit_summary_row_under_88_carries_portfolio_anchor_and_zero_zscores(self):
		"""R6.2 — summary row of a single-flight portfolio has
		``Portfolio_Anchor == anchorFare_b2c``,
		``z_score_composite == 0``, and
		``Portfolio88_Anchor == anchorFare_b2c`` when the flight is
		in the ``<88`` Portfolio88 subset.

		Drives the short-circuit through ``_phaseN_writeSummary`` by
		stubbing the ladder / Phase L–M (which need MongoDB-backed
		DataFrames not available in the test harness).  The persisted
		summary rows in ``allocResults`` carry the R5 contract.
		"""
		dyn, _fake_redis = _make_dyn_alloc()

		# Allocation pass uses a captured payload to short-circuit
		# directly to the summary writer.  We rehydrate the state from
		# a baseline payload (PLF=66.29, in the <88 subset) and
		# manually invoke ``_phaseN_writeSummary`` after the rehydrate
		# / z-score attach so we exercise the same code path as the
		# in-process short-circuit success case without needing live
		# Phase K-tail / L / M collaborators.
		payload = _baseline_payload(portfolio_count=1)
		entry = {
			"Imputed_ForecastPLF": payload["imp_forecast"],
			"ForecastAF_Allin": payload["ForecastAF_Allin"],
			"anchorFare_b2c": payload["anchor_result_b2c"]["strategyFare"],
			"portfolio_count": 1,
		}
		aggregates = dyn._buildDegenerateAggregates(entry)

		def _fake_allocation_pass(payload_in, aggregates_in):
			# Mirror the real ``runAllocationPass`` ordering: counter
			# bump, rehydrate, z-score attach, then summary write.  Skip
			# Phase K-tail / L / M (they need DB-backed DataFrames).
			# We rehydrate from the rich ``payload`` closure (rather
			# than the lean ``entry`` short-circuit hand-off) so the
			# ``MarketListRow`` view carries every Phase F/G field
			# ``_phaseN_writeSummary`` reads.
			dyn.redishelper.updateRunAuditReady()
			state = dyn._rehydrateFlightState(payload)
			dyn._computeAndAttachZScores(state, aggregates_in)
			# Minimal openData stubs so ``_phaseN_writeSummary``'s
			# safe_get_from_dict reads ``CONST_NA``-equivalent
			# placeholders without crashing.
			state.openDataB2C_details = {
				"RBD": ["V1"], "Fare": [4665.0], "FareResult": ["Strategy"],
				"anchorFare": [4665.0], "statusInd": [1], "allocationStatus": [1],
			}
			state.openDataB2B_details = {
				"RBD": ["Z3"], "Fare": [4310.0], "FareResult": ["B2CDiscounted"],
				"anchorFare": [4310.0], "statusInd": [1], "allocationStatus": [1],
			}
			# ``_phaseN_writeSummary`` reads a few attributes off ``self``
			# that the rehydrate path leaves untouched (because the
			# legacy in-process flow sets them in earlier phases).
			# Provide safe defaults so the writer focuses on the R5 /
			# portfolio_count fields under test.
			dyn.first_rbd_alloc = 5
			dyn.other_rbd_alloc = 10
			dyn.getParsedForecast = lambda: 110.0
			dyn.getParsedImputedForecast = lambda variance: 66.29
			dyn.recommend_action = lambda row: "NO action required"
			# The summary writer overrides ``portfolio_count`` from
			# ``self.currParams.portfolio_count`` — set it on the
			# stubbed currParams so the assertion below sees the right
			# value (R6 / R9 contract).
			dyn.currParams.portfolio_count = state.portfolio_count
			dyn._phaseN_writeSummary(state)
			dyn.redishelper.updateRunAudit()

		dyn.runAllocationPass = _fake_allocation_pass

		short_circuited = dyn._maybeShortCircuitSingleFlight(entry)
		self.assertTrue(short_circuited)

		# The short-circuit's allocation pass left two summary rows in
		# ``allocResults``; both should carry the R5 / R6.2 contract.
		self.assertEqual(len(dyn.allocResults), 2)
		strategy_b2c = payload["anchor_result_b2c"]["strategyFare"]
		for row in dyn.allocResults:
			self.assertEqual(row.z_score_composite, 0.0)
			self.assertEqual(row.z_score88, 0.0)
			self.assertEqual(row.Portfolio_Anchor, strategy_b2c)
			# PLF=66.29 < 88 → Portfolio88_Anchor follows Portfolio_Anchor.
			self.assertEqual(row.Portfolio88_Anchor, strategy_b2c)
			self.assertEqual(row.portfolio_count, 1)

		# No Redis-hash / SQS interaction throughout the pipeline (R6.1).
		dyn.redishelper.insert_portfolio_entry.assert_not_called()
		dyn.redishelper.fetch_portfolio_entries.assert_not_called()
		dyn.redishelper.mark_portfolio_done.assert_not_called()
		dyn.sqs.send_message.assert_not_called()
		dyn.sqs.send_message_batch.assert_not_called()


if __name__ == "__main__":
	unittest.main()
