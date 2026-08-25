import os
import sys
import unittest
from unittest.mock import MagicMock

HERE = os.path.dirname(os.path.abspath(__file__))
LAMBDA_ROOT = os.path.dirname(HERE)
REPO_ROOT = os.path.dirname(LAMBDA_ROOT)
SHARED_CONFIG = os.path.join(REPO_ROOT, "rm-allocation-configurations")
for path in (LAMBDA_ROOT, SHARED_CONFIG):
	if path not in sys.path:
		sys.path.insert(0, path)

from Constants import CONST_NA
from models.flight_state import FlightState
from models.output_summary import OutputSummary
from models.anchor_result import AnchorResult
from runAlloc import DynAlloc


def _make_dyn_alloc_for_surge():
	dyn = DynAlloc.__new__(DynAlloc)
	dyn.logger = MagicMock()
	dyn.constantsUtils = MagicMock()
	dyn.constantsUtils.plf_change_per = 2.0
	dyn.constantsUtils.plf_change_n_runs = 4
	dyn.constantsUtils.B2B_DISCOUNT_MAP = "{}"
	dyn.determinerbd = MagicMock()
	dyn.determinerbd.computeFinalAnchor = MagicMock(return_value=AnchorResult(
		anchorFare=4000.0, anchor_source="Strategy", channel="B2C",
		toleranceValue=0, strategyFare=4000.0,
		T_prime="NA",
	))
	dyn.determinerbd.applyAnchorToLadder = MagicMock(return_value=(
		{"Fare": [4000.0]}, 4000.0, 4000.0, 4000.0, CONST_NA, 0, 0.0, False, CONST_NA, CONST_NA
	))
	dyn.outputSummaryRow_b2c = OutputSummary()
	dyn.outputSummaryRow_b2b = OutputSummary()
	dyn.currParams = MagicMock()
	dyn.currParams.mkt_fare_flag = 1
	dyn.currParams.b2c_tolerance = 0
	dyn.currParams.b2b_tolerance = 0
	dyn.currParams.origin = "BOM"
	dyn.currParams.destin = "DEL"
	dyn.currParams.flight_number = "101"
	dyn.currParams.per_start = "2023-08-01"
	dyn.currentTime = "12:00:00"
	dyn.B2B_thresholdflag = False
	return dyn


def _make_flight_state(ndocounter=10, imp_forecast=CONST_NA, currBookedLoad=50.0):
	state = FlightState()
	state.currBookedLoad = currBookedLoad
	state.dateFormatted_yyyymmdd = "2023-08-01"
	state.ndocounter = ndocounter
	state.final_totalCapacity = 180.0
	state.final_plf = 50.0
	state.dplfBand_full = 1
	state.imp_forecast = imp_forecast
	state.imp_forecast_band = 1
	state.af_forecast_band = 1
	state.Af_Nb_Allin = 3500.0
	state.PLF_Gain = 0.0
	state.forecast_plf = 50.0
	state.sod_BookingsInHour = 0
	state.sod_dplfBand = 1
	state.variance_full = 0.0
	state.overBookingCount = 0
	state.lastSoldFareb2c_val = 5000.0
	state.lastSoldFareb2b_val = 4500.0
	state.last_seven_avg_val = CONST_NA
	state.current_w0Flag = 0
	state.current_directFlag = 1
	state.hardfare_b2c = CONST_NA
	state.hardfare_b2b = CONST_NA
	state.hardoffset_b2c = 0
	state.hardoffset_b2b = 0
	state.isB2CMarketFareAvailable = True
	state.isB2BMarketFareAvailable = True
	state.himktFile_b2b = ""
	state.lowmktFile_b2b = ""
	state.b2c_backstop_final = 0.0
	state.b2b_backstop_final = 0.0
	state.profileFare = 4200.0
	state.should_process_K = True
	state.is_last7avg_anchor_b2c = False
	state.is_profile_anchor_b2c = False
	return state


class TestPlfSurgeEvaluation(unittest.TestCase):

	def test_imp_forecast_const_na_does_not_raise(self):
		"""When imp_forecast is CONST_NA ('NA'), no ValueError is raised and no error is logged."""
		dyn = _make_dyn_alloc_for_surge()
		dyn.determinerbd.get_autotolerance_history.return_value = [
			{"actual_imp_forecast": 50.0, "actual_dplf": 50.0},
			{"actual_imp_forecast": 51.0, "actual_dplf": 51.0},
			{"actual_imp_forecast": 52.0, "actual_dplf": 52.0},
			{"actual_imp_forecast": 53.0, "actual_dplf": 53.0},
		]
		state = _make_flight_state(ndocounter=10, imp_forecast=CONST_NA)

		dyn._computeFareAnchors(state)

		for call_args in dyn.logger.error.call_args_list:
			self.assertNotIn("Error evaluating PLF surge", str(call_args))

	def test_history_actual_imp_forecast_na_does_not_raise(self):
		"""When history has 'NA' for actual_imp_forecast, no ValueError is raised."""
		dyn = _make_dyn_alloc_for_surge()
		dyn.determinerbd.get_autotolerance_history.return_value = [
			{"actual_imp_forecast": "NA", "actual_dplf": "NA"},
			{"actual_imp_forecast": "NA", "actual_dplf": "NA"},
			{"actual_imp_forecast": "NA", "actual_dplf": "NA"},
			{"actual_imp_forecast": "NA", "actual_dplf": "NA"},
		]
		state = _make_flight_state(ndocounter=10, imp_forecast=60.0)

		dyn._computeFareAnchors(state)

		for call_args in dyn.logger.error.call_args_list:
			self.assertNotIn("Error evaluating PLF surge", str(call_args))

	def test_imp_forecast_surge_triggers_profile_fare_override(self):
		"""When imp_forecast surges (>= 2.0 above nth run), PLF surge is activated."""
		dyn = _make_dyn_alloc_for_surge()
		dyn.determinerbd.get_autotolerance_history.return_value = [
			{"actual_imp_forecast": 50.0, "actual_dplf": 50.0},
			{"actual_imp_forecast": 50.0, "actual_dplf": 50.0},
			{"actual_imp_forecast": 50.0, "actual_dplf": 50.0},
			{"actual_imp_forecast": 50.0, "actual_dplf": 50.0},
		]
		# imp_forecast = 55.0 (+5.0 >= plf_change_per 2.0)
		# lastSoldFareb2c_val = 5000.0 > profileFare 4200.0 => profileFare becomes CONST_NA
		# Af_Nb_Allin = 3500.0 < lastSoldFareb2c_val 5000.0 => Af_Nb_Allin becomes CONST_NA
		state = _make_flight_state(ndocounter=10, imp_forecast=55.0)

		dyn._computeFareAnchors(state)

		b2c_call_args = dyn.determinerbd.computeFinalAnchor.call_args_list[0]
		args = b2c_call_args[0]
		self.assertEqual(args[27], CONST_NA)
		self.assertEqual(args[33], CONST_NA)

	def test_ndocounter_above_30_variance_logic_safe(self):
		"""When ndocounter > 30, variance logic evaluates actual_dplf safely."""
		dyn = _make_dyn_alloc_for_surge()
		dyn.determinerbd.get_autotolerance_history.return_value = [
			{"actual_imp_forecast": 50.0, "actual_dplf": 50.0},
			{"actual_imp_forecast": 50.0, "actual_dplf": 50.0},
			{"actual_imp_forecast": 50.0, "actual_dplf": 50.0},
			{"actual_imp_forecast": 50.0, "actual_dplf": 50.0},
		]
		state = _make_flight_state(ndocounter=35, imp_forecast=CONST_NA, currBookedLoad=55.0)

		dyn._computeFareAnchors(state)

		for call_args in dyn.logger.error.call_args_list:
			self.assertNotIn("Error evaluating PLF surge", str(call_args))

		b2c_call_args = dyn.determinerbd.computeFinalAnchor.call_args_list[0]
		args = b2c_call_args[0]
		self.assertEqual(args[27], CONST_NA)
		self.assertEqual(args[33], CONST_NA)


if __name__ == "__main__":
	unittest.main()
