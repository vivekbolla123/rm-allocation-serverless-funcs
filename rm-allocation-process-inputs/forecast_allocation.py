import ast
import logging
import math
import re
import statistics
from typing import Any, Dict, List, Optional, Tuple, Union

try:
    from Constants import (
        CONST_NA,
        PLF_CHANGE_PER,
        PLF_CHANGE_UPPER_PER,
        PLF_CHANGE_LOWER_PER,
        PLF_CHANGE_N_RUNS,
        LSF_FALLBACK_N_RUNS,
    )
except ImportError:
    CONST_NA = "NA"
    PLF_CHANGE_PER = 2.0
    PLF_CHANGE_UPPER_PER = 2.0
    PLF_CHANGE_LOWER_PER = -1.0
    PLF_CHANGE_N_RUNS = 4
    LSF_FALLBACK_N_RUNS = 4

# Abbreviations understood inside grid formulas. Variables resolve to numeric fares;
# functions aggregate/transform their arguments. Keep this in sync with the Java
# ForecastAllocationValidator token list.
#
# The interpolation band formulas are written inline in DEFAULT_GRID_RULES so the
# user controls every part (bounds, denominator, offset) directly from the grid:
#   Interpolate = LowerBound + ((UpperBound - LowerBound)/0.08) * (ImputedForecastFraction - 0.80)
#   LowerBound  = MaxOwn
#   UpperBound  = Min(Strategy, Average(Highest, Middle))
FORMULA_ABBREVIATIONS = {
    # Variables
    "Strategy": "Strategy fare",
    "Profile": "Profile fare",
    "LSF": "Last Sold Fare (Max of Last 3 and Last 7)",
    "Last3": "Last 3 fare",
    "Last7": "Last 7 fare",
    "Highest": "Highest potential fare",
    "Middle": "Middle potential fare",
    "Lowest": "Lowest potential fare",
    "MaxOwn": "Max Own fare = highest of (Last 3, Profile) that is <= Strategy, else Strategy",
    "ImputedForecast": "Imputed forecast PLF (percentage, e.g. 86 for 86%)",
    "ImputedForecastFraction": "Imputed forecast PLF as a fraction (e.g. 0.86 for 86%)",
    # Functions
    "Max": "Maximum of arguments",
    "Min": "Minimum of arguments",
    "Average": "Mean of arguments",
    "Median": "Median of arguments",
}


# Seven-band Strategy Grid (Change Notes – 3, standardized with Group Forecast Bands).
# Each cell maps (band, trend) -> formula string evaluated by FormulaEvaluator.
DEFAULT_GRID_RULES = {
    # Band 7 (>100%)
    (7, "Increasing"): "1.05*Max(LSF, Average(Highest, Middle))",
    (7, "Stable"): "Max(LSF, Average(Highest, Middle))",
    (7, "Decreasing"): "Max(LSF, Average(Highest, Middle))",
    # Band 6 (95-100%)
    (6, "Increasing"): "1.05*Max(LSF, Average(Highest, Middle))",
    (6, "Stable"): "Max(LSF, Average(Highest, Middle))",
    (6, "Decreasing"): "Max(LSF, Average(Highest, Middle))",
    # Band 5 (92-95%)
    (5, "Increasing"): "Max(LSF, 0.70*Highest + 0.30*Middle)",
    (5, "Stable"): "Max(LSF, 0.30*Highest + 0.70*Middle)",
    (5, "Decreasing"): "Min(LSF, 0.30*Highest + 0.70*Middle)",
    # Band 4 (88-92%)
    (4, "Increasing"): "Max(LSF, 0.70*LSF + 0.30*Highest)",
    (4, "Stable"): "Min(LSF, 0.30*Highest + 0.70*Middle)",
    (4, "Decreasing"): "Min(LSF, 0.60*LSF + 0.40*Strategy)",
    # Band 3 (85-88%) -> inline interpolation (user controls bounds/denominator/offset)
    (3, "Increasing"): "MaxOwn + ((Min(Strategy, Average(Highest, Middle)) - MaxOwn)/0.08) * (ImputedForecastFraction - 0.80)",
    (3, "Stable"): "MaxOwn + ((Min(Strategy, Average(Highest, Middle)) - MaxOwn)/0.08) * (ImputedForecastFraction - 0.80)",
    (3, "Decreasing"): "MaxOwn + ((Min(Strategy, Average(Highest, Middle)) - MaxOwn)/0.08) * (ImputedForecastFraction - 0.80)",
    # Band 2 (80-85%) -> inline interpolation (user controls bounds/denominator/offset)
    (2, "Increasing"): "MaxOwn + ((Min(Strategy, Average(Highest, Middle)) - MaxOwn)/0.08) * (ImputedForecastFraction - 0.80)",
    (2, "Stable"): "MaxOwn + ((Min(Strategy, Average(Highest, Middle)) - MaxOwn)/0.08) * (ImputedForecastFraction - 0.80)",
    (2, "Decreasing"): "MaxOwn + ((Min(Strategy, Average(Highest, Middle)) - MaxOwn)/0.08) * (ImputedForecastFraction - 0.80)",
    # Band 1 (<80%)
    (1, "Increasing"): "Min(Strategy, MaxOwn)",
    (1, "Stable"): "Min(Strategy, MaxOwn)",
    (1, "Decreasing"): "Min(Strategy, MaxOwn)",
}

DEFAULT_BAND_THRESHOLDS = [
    {"band": 7, "start": 100.01, "end": 9999.00},
    {"band": 6, "start": 95.00, "end": 100.00},
    {"band": 5, "start": 92.00, "end": 94.99},
    {"band": 4, "start": 88.00, "end": 91.99},
    {"band": 3, "start": 85.00, "end": 87.99},
    {"band": 2, "start": 80.00, "end": 84.99},
    {"band": 1, "start": -9999.00, "end": 79.99},
]


def value_exist(val: Any) -> bool:
    """Check if a numeric or fare value is valid and not empty/NA."""
    if val is None:
        return False
    if isinstance(val, float) and math.isnan(val):
        return False
    s = str(val).strip()
    return s not in ("", CONST_NA, "None", "nan", "NAN", "null")


class FormulaEvaluator:
    """Safe AST-based evaluator for forecasting allocation grid formulas."""

    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)

    @staticmethod
    def normalize_formula(formula: str) -> str:
        """Standardize abbreviations, spacing, and math symbols for python AST parsing."""
        if not formula:
            return ""

        expr = formula.strip()

        # 1. Normalize variable name variations (e.g. strategy fare -> Strategy).
        #    Multi-word / compound tokens are handled first so that "max own fare"
        #    does not get partially rewritten by the "max" function handling below.
        replacements = [
            (r"\bmax\s*own\s*fare\b", "MaxOwn"),
            (r"\bmax\s*own\b", "MaxOwn"),
            (r"\bimputed\s*forecast\s*fraction\b", "ImputedForecastFraction"),
            (r"\bimputed\s*forecast\b(?!\s*fraction)", "ImputedForecast"),
            (r"\bstrategy\s+fare\b", "Strategy"),
            (r"\bprofile\s+fare\b", "Profile"),
            (r"\blast\s+3\s+fare\b", "Last3"),
            (r"\blast\s+7\s+fare\b", "Last7"),
            (r"\blast\s+sold\s+fare\b", "LSF"),
            (r"\blast\s+selling\s+fare\b", "LSF"),
            (r"\bhighest\s+fare\b", "Highest"),
            (r"\bmiddle\s+fare\b", "Middle"),
            (r"\blowest\s+fare\b", "Lowest"),
        ]
        for pattern, repl in replacements:
            expr = re.sub(pattern, repl, expr, flags=re.IGNORECASE)

        # 2. Normalize percentages with optional space: 80% or 80 % -> (80/100)
        expr = re.sub(r"(\d+(?:\.\d+)?)\s*%", r"(\1/100)", expr)

        # 3. Replace multiplication 'x' or 'X' with optional spaces: e.g. '80% x LSF', '80%x LSF', '0.80 x LSF'
        expr = re.sub(r"(\d+(?:\.\d+)?|\))\s*[xX]\s*", r"\1 * ", expr)

        # 4. Replace implicit multiplication like "0.95 LSF" or "0.95LSF" -> "0.95 * LSF"
        var_names = r"(?:LSF|Strategy|Profile|Highest|Middle|Lowest|MaxOwn|ImputedForecastFraction|ImputedForecast|Last3|Last7)"
        expr = re.sub(rf"(\d+(?:\.\d+)?|\))\s*({var_names})", r"\1 * \2", expr, flags=re.IGNORECASE)

        return expr

    def evaluate(
        self,
        formula: str,
        variables: Dict[str, Optional[float]],
        has_booking_in_last_hour: bool = False,
    ) -> Optional[float]:
        """Evaluate a mathematical expression safely using Python AST.

        Parameters
        ----------
        formula : str
            The formula string (e.g. 'Max(Profile, LSF*1.04, Strategy)').
        variables : dict
            Dict containing values for 'Profile', 'Strategy', 'LSF', 'Highest', 'Middle', 'Lowest'.
        has_booking_in_last_hour : bool
            If False, any multiplier on LSF > 1.0 (upsell) is clamped to 1.0.
        """
        normalized = self.normalize_formula(formula)
        if not normalized:
            return None

        # Build case-insensitive lookup
        var_map: Dict[str, Optional[float]] = {}
        for k, v in variables.items():
            val = float(v) if value_exist(v) else None
            var_map[k.lower()] = val
            var_map[k.upper()] = val
            var_map[k] = val

        try:
            tree = ast.parse(normalized, mode="eval")
            return self._eval_node(tree.body, var_map, has_booking_in_last_hour)
        except Exception as e:
            self.logger.error(f"Error evaluating formula '{formula}' (normalized: '{normalized}'): {e}")
            return None

    def _eval_node(
        self,
        node: ast.AST,
        var_map: Dict[str, Optional[float]],
        has_booking_in_last_hour: bool,
    ) -> Optional[float]:
        if isinstance(node, ast.Constant):
            if isinstance(node.value, (int, float)):
                return float(node.value)
            return None

        elif isinstance(node, ast.Name):
            name = node.id
            return var_map.get(name.lower())

        elif isinstance(node, ast.UnaryOp):
            operand = self._eval_node(node.operand, var_map, has_booking_in_last_hour)
            if operand is None:
                return None
            if isinstance(node.op, ast.UAdd):
                return +operand
            elif isinstance(node.op, ast.USub):
                return -operand
            return None

        elif isinstance(node, ast.BinOp):
            # NOTE: The booking-in-last-hour upsell constraint on LSF multipliers has
            # been removed (Change Notes – 3). LSF multipliers are now applied as-is,
            # regardless of booking velocity.
            left = self._eval_node(node.left, var_map, has_booking_in_last_hour)
            right = self._eval_node(node.right, var_map, has_booking_in_last_hour)

            if left is None or right is None:
                # If one side of an additive blend is None, fall back gracefully to the other side
                if isinstance(node.op, ast.Add):
                    return left if left is not None else right
                return None

            if isinstance(node.op, ast.Add):
                return left + right
            elif isinstance(node.op, ast.Sub):
                return left - right
            elif isinstance(node.op, ast.Mult):
                return left * right
            elif isinstance(node.op, ast.Div):
                return left / right if right != 0 else None
            return None

        elif isinstance(node, ast.Call):
            func_name = ""
            if isinstance(node.func, ast.Name):
                func_name = node.func.id.lower()

            args: List[float] = []
            for arg_node in node.args:
                v = self._eval_node(arg_node, var_map, has_booking_in_last_hour)
                if v is not None and not (isinstance(v, float) and math.isnan(v)):
                    args.append(v)

            if not args:
                return None

            if func_name in ("max", "highest"):
                return max(args)
            elif func_name in ("min", "lowest"):
                return min(args)
            elif func_name in ("average", "avg", "mean"):
                return sum(args) / len(args)
            elif func_name in ("median", "middle"):
                return statistics.median(args)
            else:
                self.logger.warning(f"Unknown function '{func_name}' in formula")
                return None

        return None


class ForecastAllocationLogic:
    """Core logic engine for Change Notes – 3 (Forecast Allocation Logic)."""

    def __init__(
        self,
        rmdatabasehelper=None,
        constantsutils=None,
        redishelper=None,
        logger: Optional[logging.Logger] = None,
    ):
        self.rmdatabasehelper = rmdatabasehelper
        self.constantsutils = constantsutils
        self.redishelper = redishelper
        self.logger = logger or logging.getLogger(__name__)
        self.evaluator = FormulaEvaluator(self.logger)

        self._grid_cache: Optional[Dict[Tuple[int, str], str]] = None
        self._bands_cache: Optional[List[Dict[str, Any]]] = None

    def get_grid_rules(self) -> Dict[Tuple[int, str], str]:
        """Fetch grid rules from DB or fallback to defaults."""
        if self._grid_cache is not None:
            return self._grid_cache

        rules = dict(DEFAULT_GRID_RULES)
        if self.rmdatabasehelper and hasattr(self.rmdatabasehelper, "get_forecast_allocation_grid"):
            try:
                grid_df = self.rmdatabasehelper.get_forecast_allocation_grid()
                if grid_df is not None and not grid_df.empty:
                    for _, row in grid_df.iterrows():
                        band = int(row["band"])
                        trend = str(row["trend"]).strip()
                        formula = str(row["formula"]).strip()
                        rules[(band, trend)] = formula
                        rules[(band, trend.capitalize())] = formula
                        rules[(band, trend.lower())] = formula
                        rules[(band, trend.upper())] = formula
            except Exception as e:
                self.logger.error(f"Error loading forecast allocation grid from DB: {e}")

        self._grid_cache = rules
        return rules

    def get_bands_config(self) -> List[Dict[str, Any]]:
        """Fetch bands configuration from DB or fallback to defaults."""
        if self._bands_cache is not None:
            return self._bands_cache

        bands = list(DEFAULT_BAND_THRESHOLDS)
        if self.rmdatabasehelper and hasattr(self.rmdatabasehelper, "get_forecast_allocation_bands"):
            try:
                bands_df = self.rmdatabasehelper.get_forecast_allocation_bands()
                if bands_df is not None and not bands_df.empty:
                    loaded_bands = []
                    for _, row in bands_df.iterrows():
                        loaded_bands.append({
                            "band": int(row["band"]),
                            "start": float(row["start"]),
                            "end": float(row["end"]),
                        })
                    if loaded_bands:
                        bands = loaded_bands
            except Exception as e:
                self.logger.error(f"Error loading forecast allocation bands from DB: {e}")

        self._bands_cache = bands
        return bands

    def resolve_band(self, imp_forecast: float) -> int:
        """Resolve Forecast Band (1 to 7) for a given imputed forecast PLF.

        Seven bands standardize the Strategy Grid with the Group Forecast Bands:
          7: >100%, 6: 95-100%, 5: 92-95%, 4: 88-92%,
          3: 85-88%, 2: 80-85%, 1: <80%.
        """
        if not value_exist(imp_forecast):
            return 1

        val = float(imp_forecast)
        bands = self.get_bands_config()
        # Sort descending by start to match upper bands first
        for b in sorted(bands, key=lambda x: x["start"], reverse=True):
            if val >= b["start"] and val <= b["end"]:
                return b["band"]

        # Default fallback (mirrors DEFAULT_BAND_THRESHOLDS)
        if val > 100.0:
            return 7
        elif val >= 95.0:
            return 6
        elif val >= 92.0:
            return 5
        elif val >= 88.0:
            return 4
        elif val >= 85.0:
            return 3
        elif val >= 80.0:
            return 2
        return 1

    def resolve_trend(
        self,
        current_imp_forecast: float,
        history: Optional[List[Dict[str, Any]]] = None,
        n_runs: Optional[int] = None,
        upper_threshold: Optional[float] = None,
        lower_threshold: Optional[float] = None,
    ) -> Tuple[str, Optional[float], Optional[float]]:
        """Determine trend (Increasing, Stable, Decreasing).

        Range within [lower_threshold, upper_threshold] is Stable.
        delta > upper_threshold -> Increasing.
        delta < lower_threshold -> Decreasing.

        Returns: (trend_name, delta, nth_prev_forecast)
        """
        if not value_exist(current_imp_forecast) or not history:
            return "Stable", None, None

        if n_runs is None:
            n_runs = getattr(self.constantsutils, "plf_change_n_runs", PLF_CHANGE_N_RUNS) if self.constantsutils else PLF_CHANGE_N_RUNS
        n_runs = int(n_runs)

        if upper_threshold is None:
            upper_threshold = getattr(self.constantsutils, "plf_change_upper_per", None) if self.constantsutils else None
            if upper_threshold is None:
                upper_threshold = getattr(self.constantsutils, "plf_change_per", PLF_CHANGE_PER) if self.constantsutils else PLF_CHANGE_PER
        upper_threshold = float(upper_threshold)

        if lower_threshold is None:
            lower_threshold = getattr(self.constantsutils, "plf_change_lower_per", PLF_CHANGE_LOWER_PER) if self.constantsutils else PLF_CHANGE_LOWER_PER
        lower_threshold = float(lower_threshold)

        current_val = float(current_imp_forecast)

        # Look back n_runs in history
        # History is chronological [run_1, run_2, ..., latest_prev_run]
        target_record = None
        if len(history) >= n_runs:
            target_record = history[-n_runs]
        else:
            # Check by run_count if available
            target_run_count = None
            if history and "run_count" in history[-1]:
                target_run_count = int(history[-1]["run_count"]) - n_runs + 1
                for r in history:
                    if r.get("run_count") == target_run_count:
                        target_record = r
                        break

        if target_record is None:
            return "Stable", None, None

        prev_val = target_record.get("actual_imp_forecast")
        if not value_exist(prev_val):
            return "Stable", None, None

        prev_val = float(prev_val)
        delta = current_val - prev_val

        if delta > upper_threshold:
            trend = "Increasing"
        elif delta < lower_threshold:
            trend = "Decreasing"
        else:
            trend = "Stable"

        return trend, delta, prev_val

    def resolve_lsf(
        self,
        lastSoldFare: Any,
        history: Optional[List[Dict[str, Any]]] = None,
        n_runs: Optional[int] = None,
        last3Fare: Any = None,
        last7Fare: Any = None,
    ) -> Tuple[Optional[float], bool]:
        """Resolve LSF.

        Per Change Notes – 3, LSF is the maximum of the Last 3 and Last 7 fares when
        either is supplied. When neither is available, falls back to ``lastSoldFare``
        and finally to the average of N previous run selling fares.

        Returns: (lsf_value, is_fallback_used)
        """
        # Preferred: Max(Last 3, Last 7)
        candidates: List[float] = []
        if value_exist(last3Fare) and float(last3Fare) > 0:
            candidates.append(float(last3Fare))
        if value_exist(last7Fare) and float(last7Fare) > 0:
            candidates.append(float(last7Fare))
        if candidates:
            return max(candidates), False

        if value_exist(lastSoldFare) and float(lastSoldFare) > 0:
            return float(lastSoldFare), False

        if n_runs is None:
            n_runs = getattr(self.constantsutils, "lsf_fallback_n_runs", LSF_FALLBACK_N_RUNS) if self.constantsutils else LSF_FALLBACK_N_RUNS
        n_runs = int(n_runs)

        # Fallback to average of N previous run selling fares
        # if history:
        #     selling_fares: List[float] = []
        #     for rec in reversed(history):
        #         s_fare = rec.get("selling_fare") or rec.get("anchorFare") or rec.get("T_V")
        #         if value_exist(s_fare) and float(s_fare) > 0:
        #             selling_fares.append(float(s_fare))
        #             if len(selling_fares) >= n_runs:
        #                 break
        #     if selling_fares:
        #         avg_lsf = sum(selling_fares) / len(selling_fares)
        #         return avg_lsf, True

        return None, False

    def compute_potential_fares(
        self,
        strategyFare: float,
        profileFare: Any,
        lsf: Optional[float],
    ) -> Tuple[float, float, float]:
        """Calculate Highest Fare, Middle Fare, and Lowest Fare from potential fares."""
        candidates = []
        if value_exist(strategyFare) and float(strategyFare) > 0:
            candidates.append(float(strategyFare))
        if value_exist(profileFare) and float(profileFare) > 0:
            candidates.append(float(profileFare))
        if value_exist(lsf) and float(lsf) > 0:
            candidates.append(float(lsf))

        if not candidates:
            strat = float(strategyFare) if value_exist(strategyFare) else 0.0
            return strat, strat, strat

        highest = max(candidates)
        lowest = min(candidates)
        middle = statistics.median(candidates)
        return highest, middle, lowest

    def compute_max_own(
        self,
        strategyFare: Any,
        profileFare: Any,
        last3Fare: Any,
    ) -> Optional[float]:
        """Max Own Fare = Max(Last Sold / Last 3, Profile) subject to being <= Strategy.

        Per Change Notes – 3, Max Own is the highest own fare (Last 3 and Profile)
        that is still at or below the Strategy fare. If no own fare is lower than
        the Strategy fare, the Strategy fare itself becomes Max Own.
        """
        own_candidates: List[float] = []
        if value_exist(profileFare) and float(profileFare) > 0:
            own_candidates.append(float(profileFare))
        if value_exist(last3Fare) and float(last3Fare) > 0:
            own_candidates.append(float(last3Fare))

        strat = float(strategyFare) if (value_exist(strategyFare) and float(strategyFare) > 0) else None

        if not own_candidates:
            return strat

        if strat is None:
            # No strategy cap available -> fall back to the highest own fare.
            return max(own_candidates)

        # Keep only own fares that do not exceed the Strategy fare.
        eligible = [f for f in own_candidates if f <= strat]
        if eligible:
            return max(eligible)

        # No own fare is lower than Strategy -> Strategy becomes Max Own.
        return strat

    def calculate_anchor(
        self,
        strategyFare: float,
        profileFare: Any,
        lastSoldFare: Any,
        imp_forecast: float,
        BookingsInHour: Any,
        history: Optional[List[Dict[str, Any]]] = None,
        band_override: Optional[int] = None,
        last3Fare: Any = None,
        last7Fare: Any = None,
    ) -> Dict[str, Any]:
        """Calculate anchor fare and diagnostic info according to Change Notes – 3.

        Returns dict with:
        - anchor_fare: float
        - band: int (1..7)
        - trend: str ('Increasing', 'Stable', 'Decreasing')
        - formula: str
        - lsf: Optional[float]            (Max of Last 3 / Last 7)
        - is_lsf_fallback: bool
        - has_bookings: bool
        - last3_fare / last7_fare: Optional[float]
        - max_own_fare: Optional[float]   (Max(Profile, Last 3) capped below Strategy)
        - highest_fare / middle_fare / lowest_fare: float
        - imputed_forecast: Optional[float]
        - strategy_fare / profile_fare: Optional[float]
        - how_determined: str
        """
        # 1. Band determination (now 1..7)
        band = band_override if band_override in (1, 2, 3, 4, 5, 6, 7) else self.resolve_band(imp_forecast)

        # 2. Trend determination
        trend, delta, prev_forecast = self.resolve_trend(imp_forecast, history)

        # 3. LSF determination — Max(Last 3, Last 7); falls back to lastSoldFare / history
        lsf, is_lsf_fallback = self.resolve_lsf(
            lastSoldFare, history, last3Fare=last3Fare, last7Fare=last7Fare
        )

        # 4. Potential fares (Highest / Middle / Lowest) and Max Own
        highest_fare, middle_fare, lowest_fare = self.compute_potential_fares(strategyFare, profileFare, lsf)
        max_own_fare = self.compute_max_own(strategyFare, profileFare, last3Fare)

        # 5. Booking velocity check (diagnostic only — no longer gates LSF upsell)
        has_bookings = False
        if value_exist(BookingsInHour):
            try:
                has_bookings = float(BookingsInHour) >= 1.0
            except (ValueError, TypeError):
                has_bookings = False

        # 6. Formula lookup
        rules = self.get_grid_rules()
        formula = (
            rules.get((band, trend))
            or rules.get((band, trend.capitalize()))
            or rules.get((band, trend.lower()))
            or rules.get((band, trend.upper()))
            or DEFAULT_GRID_RULES.get((band, trend.capitalize()), "Strategy")
        )

        # 7. Formula evaluation
        imp_forecast_val = float(imp_forecast) if value_exist(imp_forecast) else None
        variables = {
            "Strategy": float(strategyFare) if value_exist(strategyFare) else None,
            "Profile": float(profileFare) if value_exist(profileFare) else None,
            "LSF": lsf,
            "Highest": highest_fare,
            "Middle": middle_fare,
            "Lowest": lowest_fare,
            "MaxOwn": max_own_fare,
            "Last3": float(last3Fare) if value_exist(last3Fare) else None,
            "Last7": float(last7Fare) if value_exist(last7Fare) else None,
            "ImputedForecast": imp_forecast_val,
            # Fraction form (e.g. 0.86 for 86%) for inline interpolation formulas
            # that use terms like (ImputedForecastFraction - 0.80).
            "ImputedForecastFraction": (imp_forecast_val / 100.0) if imp_forecast_val is not None else None,
        }

        evaluated = self.evaluator.evaluate(
            formula=formula,
            variables=variables,
            has_booking_in_last_hour=has_bookings,
        )

        if evaluated is None or (isinstance(evaluated, float) and math.isnan(evaluated)) or evaluated <= 0:
            anchor_fare = float(strategyFare)
            audit_eval = "FallbackStrategy"
        else:
            anchor_fare = round(evaluated, 2)
            audit_eval = formula.replace(" ", "")

        # Upsell indicator — the top forecast bands (6/7) push competitive fares.
        upsell_str = "UpsellY" if (band in (6, 7) and trend in ("Increasing", "Stable")) else "UpsellN"

        how_determined = f"AllocationY_Band{band}_{trend}_{upsell_str}_{audit_eval}"

        return {
            "anchor_fare": anchor_fare,
            "band": band,
            "trend": trend,
            "delta": delta,
            "prev_forecast": prev_forecast,
            "formula": formula,
            "lsf": lsf,
            "is_lsf_fallback": is_lsf_fallback,
            "has_bookings": has_bookings,
            "last3_fare": float(last3Fare) if value_exist(last3Fare) else None,
            "last7_fare": float(last7Fare) if value_exist(last7Fare) else None,
            "max_own_fare": max_own_fare,
            "highest_fare": highest_fare,
            "middle_fare": middle_fare,
            "lowest_fare": lowest_fare,
            "imputed_forecast": imp_forecast_val,
            "strategy_fare": float(strategyFare) if value_exist(strategyFare) else None,
            "profile_fare": float(profileFare) if value_exist(profileFare) else None,
            "how_determined": how_determined,
        }
