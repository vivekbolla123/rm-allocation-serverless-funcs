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
        # Booking-in-last-hour upsell constraint has been removed (Change Notes – 3):
        # LSF*1.04 now applies regardless of booking velocity.
        # Max(4000, 4100*1.04=4264, 4200) = 4264
        vars_dict = {"Profile": 4000.0, "LSF": 4100.0, "Strategy": 4200.0}
        res = self.evaluator.evaluate("Max(Profile, LSF*1.04, Strategy)", vars_dict, has_booking_in_last_hour=False)
        self.assertAlmostEqual(res, 4264.0, places=2)

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

        # Upsell constraint removed: 5000*1.02 = 5100 applies even without bookings.
        # Max(5100, 4500) = 5100.
        res3 = self.evaluator.evaluate("Max(LSF*1.02, Average(Highest, Middle))", vars_dict2, has_booking_in_last_hour=False)
        self.assertAlmostEqual(res3, 5100.0, places=2)

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

    def test_interpolate_1(self):
        # Interpolate_1 = MaxOwn + ((Strategy - MaxOwn)/0.85) * (ImputedForecast/100 + 0.85)
        # MaxOwn=4000, Strategy=5000, ImputedForecast=86 -> 0.86
        # 4000 + ((1000)/0.85) * (0.86 + 0.85) = 4000 + 1176.4706 * 1.71 = 4000 + 2011.76 = 6011.76
        vars_dict = {"MaxOwn": 4000.0, "Strategy": 5000.0, "ImputedForecast": 86.0}
        res = self.evaluator.evaluate("Interpolate_1(MaxOwn, Strategy)", vars_dict)
        self.assertAlmostEqual(res, 4000.0 + (1000.0 / 0.85) * (0.86 + 0.85), places=2)

    def test_interpolate_2(self):
        # Interpolate_2 = MaxOwn + ((Strategy - MaxOwn)/0.80) * (ImputedForecast/100 + 0.80)
        # MaxOwn=4000, Strategy=5000, ImputedForecast=82 -> 0.82
        vars_dict = {"MaxOwn": 4000.0, "Strategy": 5000.0, "ImputedForecast": 82.0}
        res = self.evaluator.evaluate("Interpolate_2(MaxOwn, Strategy)", vars_dict)
        self.assertAlmostEqual(res, 4000.0 + (1000.0 / 0.80) * (0.82 + 0.80), places=2)

    def test_interpolate_accepts_fraction_imputed_forecast(self):
        # ImputedForecast already provided as a fraction (0.86) must behave identically.
        vars_dict = {"MaxOwn": 4000.0, "Strategy": 5000.0, "ImputedForecast": 0.86}
        res = self.evaluator.evaluate("Interpolate_1(MaxOwn, Strategy)", vars_dict)
        self.assertAlmostEqual(res, 4000.0 + (1000.0 / 0.85) * (0.86 + 0.85), places=2)

    def test_maxown_and_leading_scalar_multiplier(self):
        # 1.05 * Max(LSF, Average(Highest, Middle))
        # LSF=4000, Highest=5000, Middle=4000 -> Average=4500 -> Max=4500 -> *1.05 = 4725
        vars_dict = {"LSF": 4000.0, "Highest": 5000.0, "Middle": 4000.0}
        res = self.evaluator.evaluate("1.05*Max(LSF, Average(Highest, Middle))", vars_dict)
        self.assertAlmostEqual(res, 4725.0, places=2)

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
        self.mock_constants.plf_change_upper_per = 2.0
        self.mock_constants.plf_change_lower_per = -1.0
        self.mock_constants.plf_change_n_runs = 4
        self.mock_constants.lsf_fallback_n_runs = 4
        self.logic = ForecastAllocationLogic(
            rmdatabasehelper=self.mock_db,
            constantsutils=self.mock_constants,
        )

    def test_resolve_bands(self):
        # Seven bands: 7:>100, 6:95-100, 5:92-95, 4:88-92, 3:85-88, 2:80-85, 1:<80
        self.assertEqual(self.logic.resolve_band(101.0), 7)
        self.assertEqual(self.logic.resolve_band(98.5), 6)
        self.assertEqual(self.logic.resolve_band(95.0), 6)
        self.assertEqual(self.logic.resolve_band(93.0), 5)
        self.assertEqual(self.logic.resolve_band(92.0), 5)
        self.assertEqual(self.logic.resolve_band(90.0), 4)
        self.assertEqual(self.logic.resolve_band(88.0), 4)
        self.assertEqual(self.logic.resolve_band(86.0), 3)
        self.assertEqual(self.logic.resolve_band(85.0), 3)
        self.assertEqual(self.logic.resolve_band(82.0), 2)
        self.assertEqual(self.logic.resolve_band(80.0), 2)
        self.assertEqual(self.logic.resolve_band(75.0), 1)

    def test_resolve_trend_increasing(self):
        history = [
            {"run_count": 1, "actual_imp_forecast": 85.0},
            {"run_count": 2, "actual_imp_forecast": 86.0},
            {"run_count": 3, "actual_imp_forecast": 87.0},
            {"run_count": 4, "actual_imp_forecast": 88.0},
        ]
        # current = 91.0, 4th previous is run 1 (85.0). Delta = 91.0 - 85.0 = 6.0 > 2.0 -> Increasing
        trend, delta, prev = self.logic.resolve_trend(91.0, history, n_runs=4)
        self.assertEqual(trend, "Increasing")
        self.assertAlmostEqual(delta, 6.0)

    def test_resolve_trend_decreasing(self):
        history = [
            {"run_count": 1, "actual_imp_forecast": 92.0},
            {"run_count": 2, "actual_imp_forecast": 91.0},
            {"run_count": 3, "actual_imp_forecast": 90.0},
            {"run_count": 4, "actual_imp_forecast": 89.0},
        ]
        # current = 88.0, 4th previous is 92.0. Delta = 88.0 - 92.0 = -4.0 < -1.0 -> Decreasing
        trend, delta, prev = self.logic.resolve_trend(88.0, history, n_runs=4)
        self.assertEqual(trend, "Decreasing")
        self.assertAlmostEqual(delta, -4.0)

    def test_resolve_trend_stable(self):
        history = [
            {"run_count": 1, "actual_imp_forecast": 90.0},
            {"run_count": 2, "actual_imp_forecast": 90.5},
            {"run_count": 3, "actual_imp_forecast": 91.0},
            {"run_count": 4, "actual_imp_forecast": 90.8},
        ]
        # current = 91.0, 4th previous is 90.0. Delta = +1.0 within [-1, +2] -> Stable
        trend, delta, prev = self.logic.resolve_trend(91.0, history, n_runs=4)
        self.assertEqual(trend, "Stable")
        self.assertAlmostEqual(delta, 1.0)

    def test_resolve_trend_asymmetric_range(self):
        history = [
            {"run_count": 1, "actual_imp_forecast": 90.0},
            {"run_count": 2, "actual_imp_forecast": 90.0},
            {"run_count": 3, "actual_imp_forecast": 90.0},
            {"run_count": 4, "actual_imp_forecast": 90.0},
        ]
        # Range [-1.0, 2.0] is Stable; > 2.0 is Increasing; < -1.0 is Decreasing
        # Delta = +2.5 -> Increasing
        trend, delta, _ = self.logic.resolve_trend(92.5, history, n_runs=4, upper_threshold=2.0, lower_threshold=-1.0)
        self.assertEqual(trend, "Increasing")
        self.assertAlmostEqual(delta, 2.5)

        # Delta = +2.0 -> Stable (boundary condition)
        trend, delta, _ = self.logic.resolve_trend(92.0, history, n_runs=4, upper_threshold=2.0, lower_threshold=-1.0)
        self.assertEqual(trend, "Stable")
        self.assertAlmostEqual(delta, 2.0)

        # Delta = +0.5 -> Stable
        trend, delta, _ = self.logic.resolve_trend(90.5, history, n_runs=4, upper_threshold=2.0, lower_threshold=-1.0)
        self.assertEqual(trend, "Stable")
        self.assertAlmostEqual(delta, 0.5)

        # Delta = -1.0 -> Stable (boundary condition)
        trend, delta, _ = self.logic.resolve_trend(89.0, history, n_runs=4, upper_threshold=2.0, lower_threshold=-1.0)
        self.assertEqual(trend, "Stable")
        self.assertAlmostEqual(delta, -1.0)

        # Delta = -1.5 -> Decreasing (< -1.0)
        trend, delta, _ = self.logic.resolve_trend(88.5, history, n_runs=4, upper_threshold=2.0, lower_threshold=-1.0)
        self.assertEqual(trend, "Decreasing")
        self.assertAlmostEqual(delta, -1.5)

        # Test defaults (upper=2.0, lower=-1.0 without passing them explicitly)
        trend, delta, _ = self.logic.resolve_trend(88.5, history, n_runs=4)
        self.assertEqual(trend, "Decreasing")
        self.assertAlmostEqual(delta, -1.5)

    def test_resolve_trend_insufficient_history(self):
        history = [
            {"run_count": 1, "actual_imp_forecast": 90.0},
        ]
        # Only 1 record, need 4 -> defaults to Stable
        trend, delta, prev = self.logic.resolve_trend(91.0, history, n_runs=4)
        self.assertEqual(trend, "Stable")
        self.assertIsNone(delta)

    def test_resolve_trend_reads_from_constantsutils(self):
        # Configure mock_constants with custom thresholds and runs
        self.mock_constants.plf_change_upper_per = 3.0
        self.mock_constants.plf_change_lower_per = -0.5
        self.mock_constants.plf_change_n_runs = 2
        history = [
            {"run_count": 1, "actual_imp_forecast": 90.0},
            {"run_count": 2, "actual_imp_forecast": 90.0},
        ]
        # 2nd previous is 90.0, current is 92.5 -> delta = +2.5. Since upper is 3.0, it is Stable.
        trend, delta, _ = self.logic.resolve_trend(92.5, history)
        self.assertEqual(trend, "Stable")
        self.assertAlmostEqual(delta, 2.5)

        # Current 93.5 -> delta = 3.5 > 3.0 -> Increasing
        trend, delta, _ = self.logic.resolve_trend(93.5, history)
        self.assertEqual(trend, "Increasing")

        # Current 89.4 -> delta = -0.6 < -0.5 -> Decreasing
        trend, delta, _ = self.logic.resolve_trend(89.4, history)
        self.assertEqual(trend, "Decreasing")

    def test_resolve_trend_without_constantsutils(self):
        # When constantsutils is None, falls back to Constants.py defaults (upper=2.0, lower=-1.0, n_runs=4)
        logic_no_const = ForecastAllocationLogic(constantsutils=None)
        history = [
            {"run_count": 1, "actual_imp_forecast": 90.0},
            {"run_count": 2, "actual_imp_forecast": 90.0},
            {"run_count": 3, "actual_imp_forecast": 90.0},
            {"run_count": 4, "actual_imp_forecast": 90.0},
        ]
        trend, delta, _ = logic_no_const.resolve_trend(92.5, history)
        self.assertEqual(trend, "Increasing")
        self.assertAlmostEqual(delta, 2.5)

        trend, delta, _ = logic_no_const.resolve_trend(88.5, history)
        self.assertEqual(trend, "Decreasing")
        self.assertAlmostEqual(delta, -1.5)

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

    def test_calculate_anchor_end_to_end_band_6_increasing(self):
        history = [
            {"run_count": 1, "actual_imp_forecast": 90.0, "selling_fare": 4200.0},
            {"run_count": 2, "actual_imp_forecast": 92.0, "selling_fare": 4300.0},
            {"run_count": 3, "actual_imp_forecast": 94.0, "selling_fare": 4400.0},
            {"run_count": 4, "actual_imp_forecast": 95.0, "selling_fare": 4500.0},
        ]
        # imp_forecast = 98.0 (95-100% -> Band 6)
        # Trend: 98.0 - 90.0 = +8.0 -> Increasing
        # Strategy=4000, Profile=3800, last3=4500, last7=4300 -> LSF = max(4500, 4300) = 4500
        # Potential fares: [4000, 3800, 4500] -> Highest=4500, Middle=4000
        # Formula (Band 6 Increasing): 1.05 * Max(LSF, Average(Highest, Middle))
        #   Average(4500, 4000) = 4250 ; Max(4500, 4250) = 4500 ; *1.05 = 4725
        result = self.logic.calculate_anchor(
            strategyFare=4000.0,
            profileFare=3800.0,
            lastSoldFare=4500.0,
            imp_forecast=98.0,
            BookingsInHour=1,
            history=history,
            last3Fare=4500.0,
            last7Fare=4300.0,
        )
        self.assertEqual(result["band"], 6)
        self.assertEqual(result["trend"], "Increasing")
        self.assertAlmostEqual(result["lsf"], 4500.0, places=2)
        self.assertAlmostEqual(result["anchor_fare"], 4725.0, places=2)
        self.assertIn("AllocationY_Band6_Increasing_UpsellY", result["how_determined"])
        self.assertIn("1.05*Max(LSF,Average(Highest,Middle))", result["how_determined"])

    def test_calculate_anchor_max_own_capped_below_strategy(self):
        # Max Own = Max(Profile, Last 3), capped below Strategy.
        # Profile=4600, Last3=4400, Strategy=4500 -> Max(4600,4400)=4600 capped to 4500.
        history = [{"run_count": i, "actual_imp_forecast": 83.0} for i in range(1, 5)]
        result = self.logic.calculate_anchor(
            strategyFare=4500.0,
            profileFare=4600.0,
            lastSoldFare=4400.0,
            imp_forecast=83.0,   # Band 2 -> Interpolate_2(MaxOwn, Strategy)
            BookingsInHour=0,
            history=history,
            last3Fare=4400.0,
            last7Fare=4300.0,
        )
        self.assertEqual(result["band"], 2)
        self.assertAlmostEqual(result["max_own_fare"], 4500.0, places=2)
        # MaxOwn == Strategy -> Interpolate_2 collapses to Strategy regardless of divisor.
        self.assertAlmostEqual(result["anchor_fare"], 4500.0, places=2)
        self.assertIn("AllocationY_Band2_", result["how_determined"])
        self.assertIn("Interpolate_2(MaxOwn,Strategy)", result["how_determined"])

    def test_all_21_grid_combinations(self):
        """Verify each of the 21 combinations in the seven-band grid with explicit inputs."""
        # Baseline fares:
        #   Strategy = 4000, Profile = 4200, Last3 = 4100, Last7 = 4050
        #   LSF = Max(Last3, Last7) = 4100
        #   Potential fares: [4000, 4200, 4100] -> Highest = 4200, Middle = 4100, Lowest = 4000
        #   MaxOwn = Max(Profile, Last3) = 4200, capped below Strategy (4000) -> 4000
        STRAT, PROF, LAST3, LAST7 = 4000.0, 4200.0, 4100.0, 4050.0
        LSF = max(LAST3, LAST7)          # 4100
        HIGH, MID, LOW = 4200.0, 4100.0, 4000.0
        MAXOWN = min(max(PROF, LAST3), STRAT)  # 4000

        def interp(divisor, imp):
            return MAXOWN + ((STRAT - MAXOWN) / divisor) * (imp / 100.0 + divisor)

        # Imputed forecast used per band so resolve_band would also agree (band_override still set).
        band_imp = {7: 101.0, 6: 97.0, 5: 93.0, 4: 90.0, 3: 86.0, 2: 82.0, 1: 75.0}

        test_cases = [
            # Band 7
            (7, "Increasing", 1.05 * max(LSF, (HIGH + MID) / 2)),
            (7, "Stable", max(LSF, (HIGH + MID) / 2)),
            (7, "Decreasing", max(LSF, (HIGH + MID) / 2)),
            # Band 6
            (6, "Increasing", 1.05 * max(LSF, (HIGH + MID) / 2)),
            (6, "Stable", max(LSF, (HIGH + MID) / 2)),
            (6, "Decreasing", max(LSF, (HIGH + MID) / 2)),
            # Band 5
            (5, "Increasing", max(LSF, 0.70 * HIGH + 0.30 * MID)),
            (5, "Stable", max(LSF, 0.30 * HIGH + 0.70 * MID)),
            (5, "Decreasing", min(LSF, 0.30 * HIGH + 0.70 * MID)),
            # Band 4
            (4, "Increasing", max(LSF, 0.70 * LSF + 0.30 * HIGH)),
            (4, "Stable", min(LSF, 0.30 * HIGH + 0.70 * MID)),
            (4, "Decreasing", min(LSF, 0.60 * LSF + 0.40 * STRAT)),
            # Band 3 -> Interpolate_1(MaxOwn, Strategy); MaxOwn==Strategy so collapses to Strategy
            (3, "Increasing", interp(0.85, band_imp[3])),
            (3, "Stable", interp(0.85, band_imp[3])),
            (3, "Decreasing", interp(0.85, band_imp[3])),
            # Band 2 -> Interpolate_2(MaxOwn, Strategy)
            (2, "Increasing", interp(0.80, band_imp[2])),
            (2, "Stable", interp(0.80, band_imp[2])),
            (2, "Decreasing", interp(0.80, band_imp[2])),
            # Band 1 -> Min(Strategy, MaxOwn)
            (1, "Increasing", min(STRAT, MAXOWN)),
            (1, "Stable", min(STRAT, MAXOWN)),
            (1, "Decreasing", min(STRAT, MAXOWN)),
        ]

        for band, trend, expected_val in test_cases:
            current_fcst = band_imp[band]
            if trend == "Increasing":
                hist_fcst = current_fcst - 5.0
            elif trend == "Decreasing":
                hist_fcst = current_fcst + 5.0
            else:
                hist_fcst = current_fcst

            history = [
                {"run_count": 1, "actual_imp_forecast": hist_fcst},
                {"run_count": 2, "actual_imp_forecast": hist_fcst},
                {"run_count": 3, "actual_imp_forecast": hist_fcst},
                {"run_count": 4, "actual_imp_forecast": hist_fcst},
            ]

            res = self.logic.calculate_anchor(
                strategyFare=STRAT,
                profileFare=PROF,
                lastSoldFare=LAST3,
                imp_forecast=current_fcst,
                BookingsInHour=0,
                history=history,
                band_override=band,
                last3Fare=LAST3,
                last7Fare=LAST7,
            )
            self.assertEqual(res["band"], band)
            self.assertEqual(res["trend"], trend)
            self.assertAlmostEqual(res["anchor_fare"], round(expected_val, 2), places=2,
                                   msg=f"Failed for Band {band} {trend}")

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
        self.constantsutils.plf_change_upper_per = 2.0
        self.constantsutils.plf_change_lower_per = -1.0
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

    def test_forecast_active_band_6_routes_correctly(self):
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
        # imp_forecast 98 -> Band 6 (95-100%). No history -> Stable.
        # LSF = Max(Last3=4500, Last7=NA) = 4500; Highest=4500, Middle=4000.
        # Band 6 Stable: Max(LSF, Average(Highest, Middle)) = Max(4500, 4250) = 4500.0
        self.assertAlmostEqual(final_anchor_fare, 4500.0, places=1)
        self.assertIn("F_6_4_AllocationY_Band6_Stable_UpsellY", howDetermined)

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

