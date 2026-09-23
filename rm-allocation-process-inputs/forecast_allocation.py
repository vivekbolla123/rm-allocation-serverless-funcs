import ast
import logging
import math
import re
import statistics
from typing import Any, Dict, List, Optional, Tuple, Union

try:
    from Constants import CONST_NA
except ImportError:
    CONST_NA = "NA"


DEFAULT_GRID_RULES = {
    (4, "Increasing"): "Max(Profile, LSF*1.04, Strategy)",
    (4, "Stable"): "Max(LSF*1.02, Average(Highest, Middle))",
    (4, "Decreasing"): "Min(LSF, 0.80*LSF + 0.20*Middle)",
    (3, "Increasing"): "Max(LSF, 0.70*Highest + 0.30*Middle)",
    (3, "Stable"): "Max(LSF, 0.30*Highest + 0.70*Middle)",
    (3, "Decreasing"): "Min(LSF, 0.60*LSF + 0.40*Middle)",
    (2, "Increasing"): "Max(LSF, 0.70*LSF + 0.30*Highest)",
    (2, "Stable"): "Min(LSF, 0.80*LSF + 0.20*Lowest)",
    (2, "Decreasing"): "Min(0.95*LSF, 0.60*LSF + 0.40*Lowest)",
    (1, "Increasing"): "Min(Strategy, LSF)",
    (1, "Stable"): "Min(Strategy, LSF*0.98)",
    (1, "Decreasing"): "Min(Strategy, Profile, LSF*0.95)",
}

DEFAULT_BAND_THRESHOLDS = [
    {"band": 4, "start": 95.01, "end": 9999.00},
    {"band": 3, "start": 90.00, "end": 95.00},
    {"band": 2, "start": 80.00, "end": 89.99},
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

        # 1. Normalize variable name variations (e.g. strategy fare -> Strategy)
        replacements = [
            (r"\bstrategy\s+fare\b", "Strategy"),
            (r"\bprofile\s+fare\b", "Profile"),
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
        var_names = r"(?:LSF|Strategy|Profile|Highest|Middle|Lowest)"
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
            # Check for LSF upsell multiplier pattern: LSF * factor or factor * LSF
            is_lsf_upsell = False
            left_is_lsf = isinstance(node.left, ast.Name) and node.left.id.lower() == "lsf"
            right_is_lsf = isinstance(node.right, ast.Name) and node.right.id.lower() == "lsf"

            if isinstance(node.op, ast.Mult):
                if left_is_lsf and isinstance(node.right, ast.Constant) and isinstance(node.right.value, (int, float)):
                    mult = float(node.right.value)
                    if mult > 1.0 and not has_booking_in_last_hour:
                        mult = 1.0
                    lsf_val = var_map.get("lsf")
                    return lsf_val * mult if lsf_val is not None else None
                elif right_is_lsf and isinstance(node.left, ast.Constant) and isinstance(node.left.value, (int, float)):
                    mult = float(node.left.value)
                    if mult > 1.0 and not has_booking_in_last_hour:
                        mult = 1.0
                    lsf_val = var_map.get("lsf")
                    return lsf_val * mult if lsf_val is not None else None

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
        """Resolve Forecast Band (1 to 4) for a given imputed forecast PLF."""
        if not value_exist(imp_forecast):
            return 1

        val = float(imp_forecast)
        bands = self.get_bands_config()
        # Sort descending by start to match upper bands first
        for b in sorted(bands, key=lambda x: x["start"], reverse=True):
            if val >= b["start"] and val <= b["end"]:
                return b["band"]

        # Default fallback
        if val > 95.0:
            return 4
        elif val >= 90.0:
            return 3
        elif val >= 80.0:
            return 2
        return 1

    def resolve_trend(
        self,
        current_imp_forecast: float,
        history: Optional[List[Dict[str, Any]]] = None,
        n_runs: int = 4,
        upper_threshold: float = 2.0,
        lower_threshold: float = -1.0,
        threshold: Optional[float] = None,
    ) -> Tuple[str, Optional[float], Optional[float]]:
        """Determine trend (Increasing, Stable, Decreasing).

        Range within [lower_threshold, upper_threshold] (default -1 to +2) is Stable.
        delta > upper_threshold -> Increasing.
        delta < lower_threshold -> Decreasing.

        Returns: (trend_name, delta, nth_prev_forecast)
        """
        if not value_exist(current_imp_forecast) or not history:
            return "Stable", None, None

        if threshold is not None:
            upper_threshold = threshold
            lower_threshold = -threshold

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
        n_runs: int = 4,
    ) -> Tuple[Optional[float], bool]:
        """Resolve LSF: uses lastSoldFare if available, else average of N previous run selling fares.

        Returns: (lsf_value, is_fallback_used)
        """
        if value_exist(lastSoldFare) and float(lastSoldFare) > 0:
            return float(lastSoldFare), False

        # Fallback to average of N previous run selling fares
        if history:
            selling_fares: List[float] = []
            for rec in reversed(history):
                s_fare = rec.get("selling_fare") or rec.get("anchorFare") or rec.get("T_V")
                if value_exist(s_fare) and float(s_fare) > 0:
                    selling_fares.append(float(s_fare))
                    if len(selling_fares) >= n_runs:
                        break
            if selling_fares:
                avg_lsf = sum(selling_fares) / len(selling_fares)
                return avg_lsf, True

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

    def calculate_anchor(
        self,
        strategyFare: float,
        profileFare: Any,
        lastSoldFare: Any,
        imp_forecast: float,
        BookingsInHour: Any,
        history: Optional[List[Dict[str, Any]]] = None,
        band_override: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Calculate anchor fare and diagnostic info according to Change Notes – 3.

        Returns dict with:
        - anchor_fare: float
        - band: int (1..4)
        - trend: str ('Increasing', 'Stable', 'Decreasing')
        - formula: str
        - lsf: Optional[float]
        - is_lsf_fallback: bool
        - has_bookings: bool
        - highest_fare: float
        - middle_fare: float
        - lowest_fare: float
        - how_determined: str
        """
        # 1. Band determination
        band = band_override if band_override in (1, 2, 3, 4) else self.resolve_band(imp_forecast)

        # 2. Trend determination
        n_runs = getattr(self.constantsutils, "plf_change_n_runs", 4) if self.constantsutils else 4
        upper_threshold = getattr(self.constantsutils, "plf_change_upper_per", None)
        if upper_threshold is None:
            upper_threshold = getattr(self.constantsutils, "plf_change_per", 2.0) if self.constantsutils else 2.0
        lower_threshold = getattr(self.constantsutils, "plf_change_lower_per", -1.0) if self.constantsutils else -1.0
        trend, delta, prev_forecast = self.resolve_trend(
            imp_forecast, history, n_runs=n_runs, upper_threshold=upper_threshold, lower_threshold=lower_threshold
        )

        # 3. LSF determination
        n_selling = getattr(self.constantsutils, "lsf_fallback_n_runs", 4) if self.constantsutils else 4
        lsf, is_lsf_fallback = self.resolve_lsf(lastSoldFare, history, n_runs=n_selling)

        # 4. Potential fares
        highest_fare, middle_fare, lowest_fare = self.compute_potential_fares(strategyFare, profileFare, lsf)

        # 5. Booking velocity check
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
        variables = {
            "Strategy": float(strategyFare) if value_exist(strategyFare) else None,
            "Profile": float(profileFare) if value_exist(profileFare) else None,
            "LSF": lsf,
            "Highest": highest_fare,
            "Middle": middle_fare,
            "Lowest": lowest_fare,
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

        # Upsell indicator
        upsell_str = "UpsellY" if (has_bookings and band == 4 and trend in ("Increasing", "Stable")) else "UpsellN"

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
            "highest_fare": highest_fare,
            "middle_fare": middle_fare,
            "lowest_fare": lowest_fare,
            "how_determined": how_determined,
        }
