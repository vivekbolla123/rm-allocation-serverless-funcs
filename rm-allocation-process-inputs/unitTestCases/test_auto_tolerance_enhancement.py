import unittest
from unittest.mock import MagicMock
import pandas as pd
import sys
import os

sys.path.insert(
    0,
    os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "..", "rm-allocation-configurations")),
)
sys.path.insert(
    0,
    os.path.normpath(os.path.join(os.path.dirname(__file__), "..")),
)

from determine_rbd import DetermineRBD
from Constants import CONST_NA, B2C_CHANNEL


class TestAutoToleranceEnhancement(unittest.TestCase):
    def setUp(self):
        self.rmdb = MagicMock()
        self.rmdb.runId = "test_run_123"
        self.logger = MagicMock()
        self.inputsource = MagicMock()
        self.constantsutils = MagicMock()
        self.constantsutils.DILUTION_PLF_THRESHOLD = 999.0
        self.constantsutils.OVERBOOKING_FARE_LF = 100
        self.constantsutils.OVERBOOKING_LF = 100
        self.constantsutils.OVERBOOKING_MANUAL_LF = 100
        self.constantsutils.OVERBOOKING_FARE_PERCENT_VALUE = 0.1
        self.constantsutils.OVERBOOKING_FARE_ABSOLUTE_VALUE = 200
        self.constantsutils.B2B_FACTOR_MAP = {}
        self.constantsutils.lsf_fallback_n_runs = 4
        self.constantsutils.plf_change_n_runs = 4
        self.constantsutils.plf_change_per = 2.0
        self.mathutils = MagicMock()
        self.redishelper = MagicMock()

        # Mock auto_tolerance_config
        self.auto_tolerance_config = {
            "ndo_config": pd.DataFrame([
                {"ndo_min": 0, "ndo_max": 7, "n_value": 0.1},
                {"ndo_min": 8, "ndo_max": 14, "n_value": 0.1},
                {"ndo_min": 15, "ndo_max": 21, "n_value": 0.1},
                {"ndo_min": 22, "ndo_max": 45, "n_value": 4},
                {"ndo_min": 46, "ndo_max": 90, "n_value": 4},
                {"ndo_min": 91, "ndo_max": 120, "n_value": 5},
            ]),
            "delta_bands": pd.DataFrame([
                {"id": 1, "band_name": "Negative", "min_value": -9999.0, "max_value": -3.0},
                {"id": 2, "band_name": "No Change", "min_value": -2.0, "max_value": 2.0},
                {"id": 3, "band_name": "Positive", "min_value": 3.0, "max_value": 9999.0},
            ]),
            "grid_values": pd.DataFrame([
                {"band_id": "3", "delta_band_id": 3, "g_value": 0.5},
                {"band_id": "3", "delta_band_id": 2, "g_value": 0.0},
                {"band_id": "3", "delta_band_id": 1, "g_value": -0.2},
                {"band_id": "4", "delta_band_id": 3, "g_value": 0.4},
            ])
        }
        self.rmdb.getAutoToleranceConfig.return_value = self.auto_tolerance_config

        self.determinerbd = DetermineRBD(
            self.rmdb, self.logger, self.inputsource,
            self.constantsutils, self.mathutils, self.redishelper
        )

        self.currParams = MagicMock()
        self.currParams.origin = "BOM"
        self.currParams.destin = "DEL"
        self.currParams.flight_number = 1127
        self.currParams.per_start = "2026-10-01"
        self.currParams.auto_tolerance_flag = 1
        self.currParams.forecastAllocFlag = 0
        self.currParams.opening_fares = 0
        self.currParams.profileFares = 0
        self.currParams.market_discount = CONST_NA
        self.currParams.portfolio_only = 0

    def test_check_tolerance_not_hit_last_3_runs(self):
        # Case 1: Less than 3 runs
        hist_short = [
            {"selling_fare": 4800, "T_V": 4500, "tolerance_hit": False},
            {"selling_fare": 4700, "T_V": 4500, "tolerance_hit": False},
        ]
        self.assertFalse(self.determinerbd.check_tolerance_not_hit_last_3_runs(hist_short))

        # Case 2: 3 runs, all not hit
        hist_clean = [
            {"selling_fare": 4800, "T_V": 4500, "tolerance_hit": False},
            {"selling_fare": 4700, "T_V": 4500, "tolerance_hit": False},
            {"selling_fare": 4900, "T_V": 4500, "tolerance_hit": False},
        ]
        self.assertTrue(self.determinerbd.check_tolerance_not_hit_last_3_runs(hist_clean))

        # Case 3: 3 runs, 1 hit via boolean flag
        hist_hit_flag = [
            {"selling_fare": 4800, "T_V": 4500, "tolerance_hit": False},
            {"selling_fare": 4400, "T_V": 4500, "tolerance_hit": True},
            {"selling_fare": 4900, "T_V": 4500, "tolerance_hit": False},
        ]
        self.assertFalse(self.determinerbd.check_tolerance_not_hit_last_3_runs(hist_hit_flag))

        # Case 4: 3 runs, 1 hit via selling_fare <= T_V fallback
        hist_hit_fallback = [
            {"selling_fare": 4800, "T_V": 4500},
            {"selling_fare": 4500, "T_V": 4500},  # hit because <= T_V
            {"selling_fare": 4900, "T_V": 4500},
        ]
        self.assertFalse(self.determinerbd.check_tolerance_not_hit_last_3_runs(hist_hit_fallback))

    def test_get_current_scenario_grid_value(self):
        # delta_variance = 5 -> delta_band_id = 3 (min 3, max 9999)
        # target_band_id = "3" -> g_value = 0.5
        g_val = self.determinerbd.get_current_scenario_grid_value("3", 5.0)
        self.assertEqual(g_val, 0.5)

        # delta_variance = 0 -> delta_band_id = 2 -> g_value = 0.0
        g_val_zero = self.determinerbd.get_current_scenario_grid_value("3", 0.0)
        self.assertEqual(g_val_zero, 0.0)

        # delta_variance = -10 -> delta_band_id = 1 -> g_value = -0.2
        g_val_neg = self.determinerbd.get_current_scenario_grid_value("3", -10.0)
        self.assertEqual(g_val_neg, -0.2)

    def test_forecast_flow_triggers_enhancement(self):
        # 3 prior runs without tolerance hit; Nth run has T_prime = 450.0
        import json
        history = [
            {"run_id": "r1", "run_count": 1, "T": 500.0, "T_prime": 450.0, "T_V": 4550.0, "actual_dplf": 0.0, "actual_imp_forecast": 50.0, "selling_fare": 4800.0, "tolerance_hit": False},
            {"run_id": "r2", "run_count": 2, "T": 500.0, "T_prime": 450.0, "T_V": 4550.0, "actual_dplf": 0.0, "actual_imp_forecast": 52.0, "selling_fare": 4850.0, "tolerance_hit": False},
            {"run_id": "r3", "run_count": 3, "T": 500.0, "T_prime": 450.0, "T_V": 4550.0, "actual_dplf": 0.0, "actual_imp_forecast": 54.0, "selling_fare": 4900.0, "tolerance_hit": False},
        ]
        self.redishelper.hget.return_value = json.dumps(history)

        # Mock forecast_logic anchor output
        self.determinerbd.forecast_logic = MagicMock()
        self.determinerbd.forecast_logic.calculate_anchor.return_value = {
            "anchor_fare": 4800.0,
            "how_determined": "AllocationY_Test",
        }

        # Parameters
        strategyFare = 5000.0
        toleranceFare = 500.0
        dPLF = 5.0            # delta_variance = 5.0 - 0.0 = 5.0 -> delta_band_id = 3 -> g_value = 0.5 > 0
        imp_forecast_band = "3"
        ndocounter = 25        # NDO 25 (NDO 22-45)

        self.currParams.forecastAllocFlag = 1

        anchorFare, howDetermined, upsell, overBookingFare, toleranceValue, anchorFareBeforeTolerance, T_prime = \
            self.determinerbd.final_fare_anchor_forecast(
                strategyFare=strategyFare,
                profileFare=CONST_NA,
                lastSoldFare=CONST_NA,
                last7AvgVal=CONST_NA,
                hardAnchorFare=CONST_NA,
                currParams=self.currParams,
                plf=80.0,
                overBookingCount=0,
                BookingsInHour=0,
                last7avg=0,
                isProfileAnchor=False,
                b2b_discount_fare=False,
                anchorFareDiscounted=0,
                obfare=0,
                ndocounter=ndocounter,
                imp_forecast=59.0,
                imp_forecast_band=imp_forecast_band,
                af_forecast_band=CONST_NA,
                afForecastAllocFlag=0,
                Af_Nb_Allin=CONST_NA,
                PLF_Gain=-1,
                forecast_plf=CONST_NA,
                toleranceFare=toleranceFare
            )

        # Enhancement expected:
        # Nth run T' = 450.0
        # New T = 0.8 * 450.0 = 360.0
        # New Tolerance Floor = 5000 - 360 = 4640.0
        self.assertEqual(T_prime, 360.0)
        self.assertEqual(float(toleranceValue), 4640.0)
        self.assertEqual(anchorFareBeforeTolerance, 4800.0)
        self.assertEqual(anchorFare, 4800.0)  # 4800 >= 4640, so not clamped

    def test_dplf_flow_triggers_enhancement(self):
        # 3 prior runs without tolerance hit; Nth run has T_prime = 400.0
        import json
        history = [
            {"run_id": "r1", "run_count": 1, "T": 500.0, "T_prime": 400.0, "T_V": 4600.0, "actual_dplf": 0.0, "actual_imp_forecast": 0.0, "selling_fare": 4800.0, "tolerance_hit": False},
            {"run_id": "r2", "run_count": 2, "T": 500.0, "T_prime": 400.0, "T_V": 4600.0, "actual_dplf": 0.0, "actual_imp_forecast": 0.0, "selling_fare": 4850.0, "tolerance_hit": False},
            {"run_id": "r3", "run_count": 3, "T": 500.0, "T_prime": 400.0, "T_V": 4600.0, "actual_dplf": 0.0, "actual_imp_forecast": 0.0, "selling_fare": 4900.0, "tolerance_hit": False},
        ]
        self.redishelper.hget.return_value = json.dumps(history)

        strategyFare = 5000.0
        toleranceFare = 500.0
        dPLF = 5.0           # delta_variance = 5.0 - 0.0 = 5.0 -> delta_band_id = 3 -> g_value = 0.5 > 0
        dplfBand = 3
        ndocounter = 25

        anchorFare, howDetermined, upsell, overBookingFare, toleranceValue, anchorFareBeforeTolerance, T_prime = \
            self.determinerbd.finalFareAnchor(
                strategyFare=strategyFare,
                profileFare=CONST_NA,
                lastSoldFare=4800.0,
                hardAnchorFare=CONST_NA,
                value='A',
                dPLF=dPLF,
                toleranceFare=toleranceFare,
                currParams=self.currParams,
                plf=80.0,
                overBookingCount=0,
                dplfBand=dplfBand,
                BookingsInHour=0,
                last7avg=0,
                isProfileAnchor=False,
                b2b_discount_fare=False,
                anchorFareDiscounted=0,
                obfare=0,
                ndocounter=ndocounter
            )

        # Expected: T = 0.8 * 400.0 = 320.0
        # Floor = 5000 - 320 = 4680.0
        self.assertEqual(T_prime, 320.0)
        self.assertEqual(float(toleranceValue), 4680.0)

    def test_tolerance_hit_in_history_preserves_standard_behavior(self):
        # Run 2 had tolerance hit
        import json
        history = [
            {"run_id": "r1", "run_count": 1, "T": 500.0, "T_prime": "NA", "T_V": 4500.0, "actual_dplf": 0.0, "actual_imp_forecast": 50.0, "selling_fare": 4800.0, "tolerance_hit": False},
            {"run_id": "r2", "run_count": 2, "T": 500.0, "T_prime": "NA", "T_V": 4500.0, "actual_dplf": 0.0, "actual_imp_forecast": 52.0, "selling_fare": 4400.0, "tolerance_hit": True},
            {"run_id": "r3", "run_count": 3, "T": 500.0, "T_prime": "NA", "T_V": 4500.0, "actual_dplf": 0.0, "actual_imp_forecast": 54.0, "selling_fare": 4900.0, "tolerance_hit": False},
        ]
        self.redishelper.hget.return_value = json.dumps(history)

        self.determinerbd.forecast_logic = MagicMock()
        self.determinerbd.forecast_logic.calculate_anchor.return_value = {
            "anchor_fare": 4800.0,
            "how_determined": "AllocationY_Test",
        }

        strategyFare = 5000.0
        toleranceFare = 500.0
        dPLF = 5.0
        imp_forecast_band = "3"
        ndocounter = 25

        anchorFare, howDetermined, upsell, overBookingFare, toleranceValue, anchorFareBeforeTolerance, T_prime = \
            self.determinerbd.final_fare_anchor_forecast(
                strategyFare=strategyFare,
                profileFare=CONST_NA,
                lastSoldFare=CONST_NA,
                last7AvgVal=CONST_NA,
                hardAnchorFare=CONST_NA,
                currParams=self.currParams,
                plf=80.0,
                overBookingCount=0,
                BookingsInHour=0,
                last7avg=0,
                isProfileAnchor=False,
                b2b_discount_fare=False,
                anchorFareDiscounted=0,
                obfare=0,
                ndocounter=ndocounter,
                imp_forecast=None,
                imp_forecast_band=imp_forecast_band,
                af_forecast_band=CONST_NA,
                afForecastAllocFlag=0,
                Af_Nb_Allin=CONST_NA,
                PLF_Gain=-1,
                forecast_plf=CONST_NA,
                toleranceFare=toleranceFare
            )

        # Standard behavior: Not an execution run (run_count = 4, N=4, run_count % 5 != 0)
        # T remains baseline 500.0, T_prime is CONST_NA
        self.assertEqual(T_prime, CONST_NA)
        self.assertEqual(float(toleranceValue), 4500.0)

    def test_min_clamp_tolerance_at_100(self):
        # If 0.8 * T' < 100, clamped to 100
        import json
        history = [
            {"run_id": "r1", "run_count": 1, "T": 100.0, "T_prime": 100.0, "T_V": 4900.0, "actual_dplf": 0.0, "actual_imp_forecast": 50.0, "selling_fare": 4950.0, "tolerance_hit": False},
            {"run_id": "r2", "run_count": 2, "T": 100.0, "T_prime": 100.0, "T_V": 4900.0, "actual_dplf": 0.0, "actual_imp_forecast": 52.0, "selling_fare": 4950.0, "tolerance_hit": False},
            {"run_id": "r3", "run_count": 3, "T": 100.0, "T_prime": 100.0, "T_V": 4900.0, "actual_dplf": 0.0, "actual_imp_forecast": 54.0, "selling_fare": 4950.0, "tolerance_hit": False},
        ]
        self.redishelper.hget.return_value = json.dumps(history)

        self.determinerbd.forecast_logic = MagicMock()
        self.determinerbd.forecast_logic.calculate_anchor.return_value = {
            "anchor_fare": 4950.0,
            "how_determined": "AllocationY_Test",
        }

        strategyFare = 5000.0
        toleranceFare = 500.0
        dPLF = 5.0
        imp_forecast_band = "3"
        ndocounter = 25

        anchorFare, howDetermined, upsell, overBookingFare, toleranceValue, anchorFareBeforeTolerance, T_prime = \
            self.determinerbd.final_fare_anchor_forecast(
                strategyFare=strategyFare,
                profileFare=CONST_NA,
                lastSoldFare=CONST_NA,
                last7AvgVal=CONST_NA,
                hardAnchorFare=CONST_NA,
                currParams=self.currParams,
                plf=80.0,
                overBookingCount=0,
                BookingsInHour=0,
                last7avg=0,
                isProfileAnchor=False,
                b2b_discount_fare=False,
                anchorFareDiscounted=0,
                obfare=0,
                ndocounter=ndocounter,
                imp_forecast=None,
                imp_forecast_band=imp_forecast_band,
                af_forecast_band=CONST_NA,
                afForecastAllocFlag=0,
                Af_Nb_Allin=CONST_NA,
                PLF_Gain=-1,
                forecast_plf=CONST_NA,
                toleranceFare=toleranceFare
            )

        # 0.8 * 100.0 = 80.0 -> clamped to 100.0
        # Floor = 5000 - 100 = 4900.0
        self.assertEqual(T_prime, 100.0)
        self.assertEqual(float(toleranceValue), 4900.0)


if __name__ == "__main__":
    unittest.main()
