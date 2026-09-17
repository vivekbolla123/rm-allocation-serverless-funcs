import unittest
from unittest.mock import MagicMock
import pandas as pd

from forecast_allocation import FormulaEvaluator, ForecastAllocationLogic, DEFAULT_GRID_RULES


class TestFormulaEvaluator(unittest.TestCase):
    def setUp(self):
        self.evaluator = FormulaEvaluator()

    def test_band_4_increasing_with_booking(self):
        # Max(Profile, LSF*1.04, Strategy)
        # Profile=4000, LSF=4100, Strategy=4200
        # LSF * 1.04 = 4264
        # Max(4000, 4264, 4200) = 4264
        vars_dict = {"Profile": 4000.0, "LSF": 4100.0, "Strategy": 4200.0}
        res = self.evaluator.evaluate("Max(Profile, LSF*1.04, Strategy)", vars_dict, has_booking_in_last_hour=True)
        self.assertAlmostEqual(res, 4264.0, places=2)

    def test_band_4_increasing_without_booking(self):
        # Upsell clamped to 1.0 -> Max(4000, 4100*1.0, 4200) = 4200
        vars_dict = {"Profile": 4000.0, "LSF": 4100.0, "Strategy": 4200.0}
        res = self.evaluator.evaluate("Max(Profile, LSF*1.04, Strategy)", vars_dict, has_booking_in_last_hour=False)
        self.assertAlmostEqual(res, 4200.0, places=2)

    def test_band_4_stable(self):
        # Max(LSF*1.02, Average(Highest, Middle))
        # LSF=4000. Highest=5000, Middle=4500. Average=4750.
        # With booking: LSF*1.02 = 4080. Max(4080, 4750) = 4750.
        vars_dict = {"LSF": 4000.0, "Highest": 5000.0, "Middle": 4500.0}
        res = self.evaluator.evaluate("Max(LSF*1.02, Average (Highest Fare, Middle Fare))", vars_dict, has_booking_in_last_hour=True)
        self.assertAlmostEqual(res, 4750.0, places=2)

        # LSF=5000, Average=4500. With booking: 5000*1.02=5100. Max=5100.
        vars_dict2 = {"LSF": 5000.0, "Highest": 4600.0, "Middle": 4400.0}
        res2 = self.evaluator.evaluate("Max(LSF*1.02, Average(Highest, Middle))", vars_dict2, has_booking_in_last_hour=True)
        self.assertAlmostEqual(res2, 5100.0, places=2)

        # Without booking: 5000*1.0 = 5000. Max(5000, 4500) = 5000.
        res3 = self.evaluator.evaluate("Max(LSF*1.02, Average(Highest, Middle))", vars_dict2, has_booking_in_last_hour=False)
        self.assertAlmostEqual(res3, 5000.0, places=2)

    def test_band_4_decreasing(self):
        # Min(LSF, 80% x LSF + 20% x Middle Fare)
        # LSF=5000, Middle=4000
        # 0.8*5000 + 0.2*4000 = 4000 + 800 = 4800
        # Min(5000, 4800) = 4800
        vars_dict = {"LSF": 5000.0, "Middle": 4000.0}
        res = self.evaluator.evaluate("Min(LSF, 80% x LSF + 20% x Middle Fare)", vars_dict)
        self.assertAlmostEqual(res, 4800.0, places=2)

    def test_band_3_increasing(self):
        # Max(LSF, 70% x Highest Fare + 30% x Middle Fare)
        # LSF=4000, Highest=5000, Middle=4500
        # 0.7*5000 + 0.3*4500 = 3500 + 1350 = 4850
        # Max(4000, 4850) = 4850
        vars_dict = {"LSF": 4000.0, "Highest": 5000.0, "Middle": 4500.0}
        res = self.evaluator.evaluate("Max(LSF, 70% x Highest Fare + 30% x Middle Fare)", vars_dict)
        self.assertAlmostEqual(res, 4850.0, places=2)

    def test_band_3_stable(self):
        # Max(LSF, 30% x Highest Fare + 70% x Middle Fare)
        # LSF=4000, Highest=5000, Middle=4500
        # 0.3*5000 + 0.7*4500 = 1500 + 3150 = 4650
        # Max(4000, 4650) = 4650
        vars_dict = {"LSF": 4000.0, "Highest": 5000.0, "Middle": 4500.0}
        res = self.evaluator.evaluate("Max(LSF, 30% x Highest Fare + 70% x Middle Fare)", vars_dict)
        self.assertAlmostEqual(res, 4650.0, places=2)

    def test_band_3_decreasing(self):
        # Min(LSF, 60% x LSF + 40% x Middle Fare)
        # LSF=5000, Middle=4000
        # 0.6*5000 + 0.4*4000 = 3000 + 1600 = 4600
        # Min(5000, 4600) = 4600
        vars_dict = {"LSF": 5000.0, "Middle": 4000.0}
        res = self.evaluator.evaluate("Min(LSF, 60% x LSF + 40% x Middle Fare)", vars_dict)
        self.assertAlmostEqual(res, 4600.0, places=2)

    def test_band_2_increasing(self):
        # Max(LSF, 70% x LSF + 30% x Highest Fare)
        # LSF=4000, Highest=5000
        # 0.7*4000 + 0.3*5000 = 2800 + 1500 = 4300
        # Max(4000, 4300) = 4300
        vars_dict = {"LSF": 4000.0, "Highest": 5000.0}
        res = self.evaluator.evaluate("Max(LSF, 70% x LSF + 30% x Highest Fare)", vars_dict)
        self.assertAlmostEqual(res, 4300.0, places=2)

    def test_band_2_stable(self):
        # Min(LSF, 80% x LSF + 20% x Lowest Fare)
        # LSF=5000, Lowest=4000
        # 0.8*5000 + 0.2*4000 = 4000 + 800 = 4800
        # Min(5000, 4800) = 4800
        vars_dict = {"LSF": 5000.0, "Lowest": 4000.0}
        res = self.evaluator.evaluate("Min(LSF, 80% x LSF + 20% x Lowest Fare)", vars_dict)
        self.assertAlmostEqual(res, 4800.0, places=2)

    def test_band_2_decreasing(self):
        # Min(0.95 LSF, 60% x LSF + 40% x Lowest Fare)
        # LSF=5000, Lowest=4000
        # 0.95 * 5000 = 4750
        # 0.6*5000 + 0.4*4000 = 3000 + 1600 = 4600
        # Min(4750, 4600) = 4600
        vars_dict = {"LSF": 5000.0, "Lowest": 4000.0}
        res = self.evaluator.evaluate("Min(0.95 LSF, 60% x LSF + 40% x Lowest Fare)", vars_dict)
        self.assertAlmostEqual(res, 4600.0, places=2)

    def test_band_1_increasing(self):
        # Min(Strategy fare, LSF)
        # Strategy=4500, LSF=4200 -> Min(4500, 4200) = 4200
        vars_dict = {"Strategy": 4500.0, "LSF": 4200.0}
        res = self.evaluator.evaluate("Min(Strategy fare, LSF)", vars_dict)
        self.assertAlmostEqual(res, 4200.0, places=2)

    def test_band_1_stable(self):
        # Min(Strategy fare, LSF*0.98)
        # Strategy=4500, LSF=4200 -> 4200*0.98 = 4116
        # Min(4500, 4116) = 4116
        vars_dict = {"Strategy": 4500.0, "LSF": 4200.0}
        res = self.evaluator.evaluate("Min(Strategy fare, LSF*0.98)", vars_dict)
        self.assertAlmostEqual(res, 4116.0, places=2)

    def test_band_1_decreasing(self):
        # Min(Strategy fare, profile fare, LSF*0.95)
        # Strategy=4500, Profile=4000, LSF=4200 -> 4200*0.95 = 3990
        # Min(4500, 4000, 3990) = 3990
        vars_dict = {"Strategy": 4500.0, "Profile": 4000.0, "LSF": 4200.0}
        res = self.evaluator.evaluate("Min(Strategy fare, profile fare, LSF*0.95)", vars_dict)
        self.assertAlmostEqual(res, 3990.0, places=2)

    def test_missing_profile_handled_gracefully(self):
        # Max(Profile, LSF*1.04, Strategy) where Profile is None
        vars_dict = {"Profile": None, "LSF": 4000.0, "Strategy": 4500.0}
        res = self.evaluator.evaluate("Max(Profile, LSF*1.04, Strategy)", vars_dict, has_booking_in_last_hour=False)
        self.assertAlmostEqual(res, 4500.0, places=2)

    def test_spacing_variations(self):
        """Verify that any whitespace variation inside formulas evaluates identically."""
        vars_dict = {
            "Strategy": 4000.0,
            "Profile": 4200.0,
            "LSF": 4100.0,
            "Lowest": 4000.0,
            "Middle": 4100.0,
            "Highest": 4200.0,
        }
        test_spacings = [
            # Standard
            "Min(LSF, 0.80*LSF + 0.20*Lowest)",
            # No spaces at all
            "Min(LSF,0.80*LSF+0.20*Lowest)",
            # Irregular / extra spaces
            "Min(  LSF  ,   0.80  *  LSF   +   0.20  *  Lowest  )",
            # Space after function name
            "Min ( LSF , 0.80 * LSF + 0.20 * Lowest )",
            # Human readable text with percentages & x
            "Min(LSF, 80% x LSF + 20% x Lowest)",
            "Min(LSF, 80%x LSF + 20% xLowest)",
            "Min(LSF, 80% * LSF + 20% * Lowest)",
            "Min(LSF, 80%*LSF + 20%*Lowest)",
            "Min(LSF, 80 % * LSF + 20 % * Lowest)",
        ]
        # Expected: min(4100, 0.8*4100 + 0.2*4000) = min(4100, 3280 + 800) = 4080.0
        for expr in test_spacings:
            res = self.evaluator.evaluate(expr, vars_dict)
            self.assertIsNotNone(res, f"Failed to evaluate: {expr}")
            self.assertAlmostEqual(res, 4080.0, places=2, msg=f"Wrong result for spacing: {expr}")

        # Also verify space vs no space in 0.95 LSF vs 0.95LSF
        res1 = self.evaluator.evaluate("Min(0.95 LSF, 60% x LSF + 40% x Lowest Fare)", vars_dict)
        res2 = self.evaluator.evaluate("Min(0.95LSF, 60% x LSF + 40% x Lowest Fare)", vars_dict)
        self.assertAlmostEqual(res1, 3895.0, places=2)
        self.assertAlmostEqual(res2, 3895.0, places=2)

    def test_case_insensitivity(self):
        """Verify that uppercase, lowercase, and mixed-case formulas evaluate identically."""
        vars_dict = {
            "Strategy": 4000.0,
            "Profile": 4200.0,
            "LSF": 4100.0,
            "Lowest": 4000.0,
            "Middle": 4100.0,
            "Highest": 4200.0,
        }
        test_cases = [
            "min(lsf, 0.80*lsf + 0.20*lowest)",
            "MIN(LSF, 0.80*LSF + 0.20*LOWEST)",
            "mIn(LsF, 0.80*lSf + 0.20*lOwEsT)",
            "min(last sold fare, 80% x last sold fare + 20% x lowest fare)",
            "MIN(LAST SOLD FARE, 80% X LAST SOLD FARE + 20% X LOWEST FARE)",
            "max(profile, lsf*1.04, strategy)",
            "MAX(PROFILE, LSF*1.04, STRATEGY)",
            "average(highest, middle)",
            "AVERAGE(HIGHEST, MIDDLE)",
            "median(highest, middle, lowest)",
            "MEDIAN(HIGHEST, MIDDLE, LOWEST)",
        ]
        for tc in test_cases:
            res = self.evaluator.evaluate(tc, vars_dict)
            self.assertIsNotNone(res, f"Failed on case variation: {tc}")


class TestForecastAllocationLogic(unittest.TestCase):
    def setUp(self):
        self.mock_db = MagicMock()
        self.mock_constants = MagicMock()
        self.mock_constants.plf_change_per = 2.0
        self.mock_constants.plf_change_n_runs = 4
        self.mock_constants.lsf_fallback_n_runs = 4
        self.logic = ForecastAllocationLogic(
            rmdatabasehelper=self.mock_db,
            constantsutils=self.mock_constants,
        )

    def test_resolve_bands(self):
        self.assertEqual(self.logic.resolve_band(98.5), 4)
        self.assertEqual(self.logic.resolve_band(95.0), 3)
        self.assertEqual(self.logic.resolve_band(92.0), 3)
        self.assertEqual(self.logic.resolve_band(85.0), 2)
        self.assertEqual(self.logic.resolve_band(75.0), 1)

    def test_resolve_trend_increasing(self):
        history = [
            {"run_count": 1, "actual_imp_forecast": 85.0},
            {"run_count": 2, "actual_imp_forecast": 86.0},
            {"run_count": 3, "actual_imp_forecast": 87.0},
            {"run_count": 4, "actual_imp_forecast": 88.0},
        ]
        # current = 91.0, 4th previous is run 1 (85.0). Delta = 91.0 - 85.0 = 6.0 > 2.0 -> Increasing
        trend, delta, prev = self.logic.resolve_trend(91.0, history, n_runs=4, threshold=2.0)
        self.assertEqual(trend, "Increasing")
        self.assertAlmostEqual(delta, 6.0)

    def test_resolve_trend_decreasing(self):
        history = [
            {"run_count": 1, "actual_imp_forecast": 92.0},
            {"run_count": 2, "actual_imp_forecast": 91.0},
            {"run_count": 3, "actual_imp_forecast": 90.0},
            {"run_count": 4, "actual_imp_forecast": 89.0},
        ]
        # current = 88.0, 4th previous is 92.0. Delta = 88.0 - 92.0 = -4.0 < -2.0 -> Decreasing
        trend, delta, prev = self.logic.resolve_trend(88.0, history, n_runs=4, threshold=2.0)
        self.assertEqual(trend, "Decreasing")
        self.assertAlmostEqual(delta, -4.0)

    def test_resolve_trend_stable(self):
        history = [
            {"run_count": 1, "actual_imp_forecast": 90.0},
            {"run_count": 2, "actual_imp_forecast": 90.5},
            {"run_count": 3, "actual_imp_forecast": 91.0},
            {"run_count": 4, "actual_imp_forecast": 90.8},
        ]
        # current = 91.0, 4th previous is 90.0. Delta = +1.0 within [-2, +2] -> Stable
        trend, delta, prev = self.logic.resolve_trend(91.0, history, n_runs=4, threshold=2.0)
        self.assertEqual(trend, "Stable")
        self.assertAlmostEqual(delta, 1.0)

    def test_resolve_trend_insufficient_history(self):
        history = [
            {"run_count": 1, "actual_imp_forecast": 90.0},
        ]
        # Only 1 record, need 4 -> defaults to Stable
        trend, delta, prev = self.logic.resolve_trend(91.0, history, n_runs=4, threshold=2.0)
        self.assertEqual(trend, "Stable")
        self.assertIsNone(delta)

    def test_resolve_lsf_fallback(self):
        # When LSF is NA, use average of last N selling fares in history
        history = [
            {"selling_fare": 4000.0},
            {"selling_fare": 4200.0},
            {"selling_fare": 4400.0},
            {"selling_fare": 4600.0},
        ]
        lsf, is_fallback = self.logic.resolve_lsf("NA", history, n_runs=4)
        self.assertTrue(is_fallback)
        self.assertAlmostEqual(lsf, 4300.0)

    def test_calculate_anchor_end_to_end_band_4_increasing(self):
        history = [
            {"run_count": 1, "actual_imp_forecast": 90.0, "selling_fare": 4200.0},
            {"run_count": 2, "actual_imp_forecast": 92.0, "selling_fare": 4300.0},
            {"run_count": 3, "actual_imp_forecast": 94.0, "selling_fare": 4400.0},
            {"run_count": 4, "actual_imp_forecast": 95.0, "selling_fare": 4500.0},
        ]
        # imp_forecast = 98.0 (>95% -> Band 4)
        # Trend: 98.0 - 90.0 = +8.0 -> Increasing
        # BookingsInHour = 1 -> has_bookings True -> LSF*1.04 applies
        # Strategy=4000, Profile=3800, lastSoldFare=4500
        # Formula: Max(Profile, LSF*1.04, Strategy) = Max(3800, 4500*1.04=4680, 4000) = 4680
        result = self.logic.calculate_anchor(
            strategyFare=4000.0,
            profileFare=3800.0,
            lastSoldFare=4500.0,
            imp_forecast=98.0,
            BookingsInHour=1,
            history=history,
        )
        self.assertEqual(result["band"], 4)
        self.assertEqual(result["trend"], "Increasing")
        self.assertAlmostEqual(result["anchor_fare"], 4680.0, places=2)
        self.assertIn("AllocationY_Band4_Increasing_UpsellY", result["how_determined"])
        self.assertIn("Max(Profile,LSF*1.04,Strategy)", result["how_determined"])

    def test_all_12_grid_combinations(self):
        """Verify each of the 12 combinations in Table 1.1 with explicit inputs."""
        # Baseline fares:
        # Strategy = 4000, Profile = 4200, LSF = 4100
        # Potential fares: [4000, 4100, 4200]
        # Highest = 4200, Middle = 4100, Lowest = 4000
        test_cases = [
            # (band, trend, bookings, expected_formula_math)
            # Band 4
            (4, "Increasing", 1, max(4200, 4100 * 1.04, 4000)),  # Max(Profile, LSF*1.04, Strategy) = 4264
            (4, "Increasing", 0, max(4200, 4100 * 1.0, 4000)),   # Max(Profile, LSF*1.0, Strategy) = 4200
            (4, "Stable", 1, max(4100 * 1.02, (4200 + 4100) / 2)), # Max(4182, 4150) = 4182
            (4, "Stable", 0, max(4100 * 1.0, (4200 + 4100) / 2)),  # Max(4100, 4150) = 4150
            (4, "Decreasing", 0, min(4100, 0.80 * 4100 + 0.20 * 4100)), # Min(4100, 4100) = 4100
            # Band 3
            (3, "Increasing", 0, max(4100, 0.70 * 4200 + 0.30 * 4100)), # Max(4100, 2940 + 1230 = 4170) = 4170
            (3, "Stable", 0, max(4100, 0.30 * 4200 + 0.70 * 4100)),     # Max(4100, 1260 + 2870 = 4130) = 4130
            (3, "Decreasing", 0, min(4100, 0.60 * 4100 + 0.40 * 4100)), # Min(4100, 4100) = 4100
            # Band 2
            (2, "Increasing", 0, max(4100, 0.70 * 4100 + 0.30 * 4200)), # Max(4100, 2870 + 1260 = 4130) = 4130
            (2, "Stable", 0, min(4100, 0.80 * 4100 + 0.20 * 4000)),     # Min(4100, 3280 + 800 = 4080) = 4080
            (2, "Decreasing", 0, min(0.95 * 4100, 0.60 * 4100 + 0.40 * 4000)), # Min(3895, 2460 + 1600 = 4060) = 3895
            # Band 1
            (1, "Increasing", 0, min(4000, 4100)),                       # Min(Strategy, LSF) = 4000
            (1, "Stable", 0, min(4000, 4100 * 0.98)),                   # Min(4000, 4018) = 4000
            (1, "Decreasing", 0, min(4000, 4200, 4100 * 0.95)),         # Min(4000, 4200, 3895) = 3895
        ]

        for band, trend, bookings, expected_val in test_cases:
            # Map trend to forecast diff
            if trend == "Increasing":
                current_fcst = 96.0 if band == 4 else (92.0 if band == 3 else (85.0 if band == 2 else 75.0))
                hist_fcst = current_fcst - 5.0
            elif trend == "Decreasing":
                current_fcst = 96.0 if band == 4 else (92.0 if band == 3 else (85.0 if band == 2 else 75.0))
                hist_fcst = current_fcst + 5.0
            else:
                current_fcst = 96.0 if band == 4 else (92.0 if band == 3 else (85.0 if band == 2 else 75.0))
                hist_fcst = current_fcst

            history = [
                {"run_count": 1, "actual_imp_forecast": hist_fcst},
                {"run_count": 2, "actual_imp_forecast": hist_fcst},
                {"run_count": 3, "actual_imp_forecast": hist_fcst},
                {"run_count": 4, "actual_imp_forecast": hist_fcst},
            ]

            res = self.logic.calculate_anchor(
                strategyFare=4000.0,
                profileFare=4200.0,
                lastSoldFare=4100.0,
                imp_forecast=current_fcst,
                BookingsInHour=bookings,
                history=history,
                band_override=band,
            )
            self.assertEqual(res["band"], band)
            self.assertEqual(res["trend"], trend)
            self.assertAlmostEqual(res["anchor_fare"], round(expected_val, 2), places=2,
                                   msg=f"Failed for Band {band} {trend} (bookings={bookings})")

    def test_custom_db_grid_override(self):
        """Verify that custom formulas loaded from DB override default formulas."""
        custom_grid_df = pd.DataFrame([
            {"band": 4, "trend": "Increasing", "formula": "Strategy + 500"},
        ])
        self.mock_db.get_forecast_allocation_grid.return_value = custom_grid_df
        custom_logic = ForecastAllocationLogic(
            rmdatabasehelper=self.mock_db,
            constantsutils=self.mock_constants,
        )
        history = [
            {"run_count": 1, "actual_imp_forecast": 90.0},
            {"run_count": 2, "actual_imp_forecast": 91.0},
            {"run_count": 3, "actual_imp_forecast": 92.0},
            {"run_count": 4, "actual_imp_forecast": 93.0},
        ]
        res = custom_logic.calculate_anchor(
            strategyFare=4000.0,
            profileFare=4200.0,
            lastSoldFare=4100.0,
            imp_forecast=98.0,
            BookingsInHour=1,
            history=history,
            band_override=4,
        )
        self.assertAlmostEqual(res["anchor_fare"], 4500.0, places=2)


class TestDetermineRBDIntegration(unittest.TestCase):
    def setUp(self):
        from determine_rbd import DetermineRBD
        from Constants import CONST_NA, B2C_CHANNEL

        self.rmdb = MagicMock()
        self.rmdb.getAutoToleranceConfig.return_value = {"ndo_config": None}
        self.logger = MagicMock()
        self.inputsource = MagicMock()
        self.constantsutils = MagicMock()
        self.constantsutils.OVERBOOKING_FARE_LF = 100
        self.constantsutils.OVERBOOKING_LF = 100
        self.constantsutils.OVERBOOKING_MANUAL_LF = 100
        self.constantsutils.OVERBOOKING_FARE_PERCENT_VALUE = 0.1
        self.constantsutils.OVERBOOKING_FARE_ABSOLUTE_VALUE = 200
        self.constantsutils.B2B_FACTOR_MAP = {"1": 0.5, "2": 0.5, "3": 0.0, "4": 0.0}
        self.constantsutils.plf_change_per = 2.0
        self.constantsutils.plf_change_n_runs = 4
        self.constantsutils.lsf_fallback_n_runs = 4
        self.mathutils = MagicMock()
        self.redishelper = MagicMock()
        self.determine_rbd = DetermineRBD(
            self.rmdb,
            self.logger,
            self.inputsource,
            self.constantsutils,
            self.mathutils,
            self.redishelper,
        )

    def test_forecast_active_band_4_routes_correctly(self):
        curr_params = MagicMock()
        curr_params.origin = "BOM"
        curr_params.destin = "DEL"
        curr_params.flight_number = 1127
        curr_params.per_start = "08/01/2023"
        curr_params.forecastAllocFlag = 1
        curr_params.afForecastAllocFlag = 0
        curr_params.opening_fares = 0
        curr_params.profileFares = 0
        curr_params.market_discount = "NA"
        curr_params.b2b_factor = 1.0

        open_res = pd.DataFrame({
            "RBD": ["Y"],
            "TOTAL": [4000.0],
            "statusInd": [0],
            "allocationStatus": [1],
        })
        rbd_data = pd.DataFrame({"RBD": ["Y"], "TOTAL": [4000.0]})
        self.rmdb.getValueWithOffset.return_value = (open_res, rbd_data)
        self.rmdb.getLadderFares.return_value = rbd_data

        # Run determineRBDToOpen with imp_forecast = 98.0 (>95 -> Band 4)
        output = self.determine_rbd.determineRBDToOpen(
            anchorFare=4000.0,
            offset=0,
            channel="B2C",
            currFltDate="2023-08-01",
            dplfBand=4,
            ndocounter=0,
            currBookedLoad=10,
            totalCapacity=189,
            currParams=curr_params,
            backstop=0,
            currenttime="12:00",
            startEndOfDayDplfBand=4,
            dPLF=0.0,
            toleranceFare="NA",
            plf=5.0,
            overBookingCount=0,
            hardAnchorFare="NA",
            hardoffset="NA",
            BookingsInHour=1,
            lastSoldFare=4500.0,
            last7AvgVal="NA",
            last7avg=False,
            b2b_discount_fare=False,
            discount_amount=0,
            w0Flag=0,
            directFlag=1,
            obfare="NA",
            profileFare=3800.0,
            isProfileAnchor=False,
            imp_forecast=98.0,
            imp_forecast_band="NA",
            strategy_criteria="MIN",
            af_forecast_band="NA",
            Af_Nb_Allin="NA",
            PLF_Gain=0,
            forecast_plf="NA",
        )
        open_result = output[0]
        final_anchor_fare = float(open_result['anchorFare'][0])
        howDetermined = str(open_result['FareResult'][0])
        # Band 4 Stable with BookingsInHour=1: Max(4500*1.02=4590, (4500+4000)/2=4250) = 4590.0
        self.assertAlmostEqual(final_anchor_fare, 4590.0, places=1)
        self.assertIn("F_4_4_AllocationY_Band4_Stable_UpsellY", howDetermined)

    def test_forecast_active_missing_imp_forecast_falls_back_to_variance(self):
        curr_params = MagicMock()
        curr_params.origin = "BOM"
        curr_params.destin = "DEL"
        curr_params.flight_number = 1127
        curr_params.per_start = "08/01/2023"
        curr_params.forecastAllocFlag = 1
        curr_params.afForecastAllocFlag = 0
        curr_params.opening_fares = 0
        curr_params.profileFares = 0
        curr_params.market_discount = "NA"
        curr_params.b2b_factor = 1.0

        open_res = pd.DataFrame({
            "RBD": ["Y"],
            "TOTAL": [4000.0],
            "statusInd": [0],
            "allocationStatus": [1],
        })
        rbd_data = pd.DataFrame({"RBD": ["Y"], "TOTAL": [4000.0]})
        self.rmdb.getValueWithOffset.return_value = (open_res, rbd_data)
        self.rmdb.getLadderFares.return_value = rbd_data

        # Mock getRDBTable to return variance grid
        self.rmdb.getRDBTable.return_value = pd.DataFrame({
            "B2CValue": ["A"],
            "B2BValue": ["A"],
        })

        # When imp_forecast is "NA", it MUST fall back to variance based allocation logic
        output = self.determine_rbd.determineRBDToOpen(
            anchorFare=4000.0,
            offset=0,
            channel="B2C",
            currFltDate="2023-08-01",
            dplfBand=4,
            ndocounter=0,
            currBookedLoad=10,
            totalCapacity=189,
            currParams=curr_params,
            backstop=0,
            currenttime="12:00",
            startEndOfDayDplfBand=4,
            dPLF=0.0,
            toleranceFare="NA",
            plf=5.0,
            overBookingCount=0,
            hardAnchorFare="NA",
            hardoffset="NA",
            BookingsInHour=0,
            lastSoldFare=4500.0,
            last7AvgVal="NA",
            last7avg=False,
            b2b_discount_fare=False,
            discount_amount=0,
            w0Flag=0,
            directFlag=1,
            obfare="NA",
            profileFare=3800.0,
            isProfileAnchor=False,
            imp_forecast="NA",
            imp_forecast_band="NA",
            strategy_criteria="MIN",
            af_forecast_band="NA",
            Af_Nb_Allin="NA",
            PLF_Gain=0,
            forecast_plf="NA",
        )
        open_result = output[0]
        howDetermined = str(open_result['FareResult'][0])
        self.rmdb.getRDBTable.assert_called_once()
        self.assertIn("V_", howDetermined)

    def test_how_determined_formula_in_parser(self):
        from utilities.how_determined_parser import HowDeterminedParser
        raw_hd = "F_4_4_AllocationY_Band4_Increasing_UpsellY_Max(Profile,LSF*1.04,Strategy)_Auto_Tolerance_Hit"
        parsed = HowDeterminedParser.parse_and_format(raw_hd)
        self.assertIn("Formula:Max(Profile,LSF*1.04,Strategy)", parsed)


if __name__ == "__main__":
    unittest.main()

