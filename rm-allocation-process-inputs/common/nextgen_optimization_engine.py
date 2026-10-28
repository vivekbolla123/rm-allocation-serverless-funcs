import math
import re
import numpy as np
import pandas as pd
try:
    from scipy.stats import norm
except (ImportError, ModuleNotFoundError):
    from statistics import NormalDist

    class _NormAdapter:
        """
        Pure Python standard-library drop-in replacement for scipy.stats.norm
        (avoids heavy scipy dependency in AWS Lambda environments).
        """
        @staticmethod
        def ppf(p):
            if isinstance(p, (np.ndarray, list)):
                p = np.clip(p, 1e-12, 1.0 - 1e-12)
                return np.vectorize(lambda val: NormalDist().inv_cdf(float(val)))(p)
            p_clipped = float(np.clip(p, 1e-12, 1.0 - 1e-12))
            return NormalDist().inv_cdf(p_clipped)

        @staticmethod
        def pdf(x):
            if isinstance(x, (np.ndarray, list)):
                x = np.array(x, dtype=float)
                return (1.0 / math.sqrt(2.0 * math.pi)) * np.exp(-0.5 * x * x)
            x = float(x)
            return (1.0 / math.sqrt(2.0 * math.pi)) * math.exp(-0.5 * x * x)

        @staticmethod
        def cdf(x):
            if isinstance(x, (np.ndarray, list)):
                x = np.array(x, dtype=float)
                return 0.5 * (1.0 + np.vectorize(math.erf)(x / math.sqrt(2.0)))
            x = float(x)
            return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

    norm = _NormAdapter()
import logging

from Constants import (
    CONST_NA,
    DEFAULT_FIRST_RBD_SEATS,
    NEXTGEN_OTHERRBD,
    DEFAULT_STANDARD_DEVIATION_FACTOR,
    DEFAULT_PROMO_FARE_FACTOR,
    DEFAULT_FORECAST_THRESHOLD_PROMO_AU,
    DEFAULT_COVERAGE_PARAMETER,
    DEFAULT_ADDITIONAL_CONNECTION_DURATION,
    DEFAULT_TIME_RANGE,
    DEFAULT_PROMO_FLAG_NG,
    DEFAULT_PROMO_RBD_NG,
    DEFAULT_PROMO_FIRST_RBD_NG,
    DEFAULT_PROMO_RATIO_NG,
    DEFAULT_PMIN_THRESHOLD_FACTOR_NG
)

logger = logging.getLogger(__name__)


def to_float(val, default=0.0):
    if val is None:
        return default
    s = str(val).strip().upper()
    if s.endswith('%'):
        s = s[:-1].strip()
    elif '%' in s:
        s = s.replace('%', '').strip()
    if s in ['', 'NA', 'NAN', 'NONE', 'NULL']:
        return default
    try:
        return float(s)
    except (ValueError, TypeError):
        return default


def to_int(val, default=0):
    if val is None:
        return default
    s = str(val).strip().upper()
    if s in ['Y', 'YES', 'TRUE', 'T']:
        return 1
    if s in ['N', 'NO', 'FALSE', 'F']:
        return 0
    if s in ['', 'NA', 'NAN', 'NONE', 'NULL']:
        return default
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default


def is_flag_enabled(val):
    return to_int(val, 0) == 1


class NextGenOptimizationEngine:
    """
    Core algorithmic engine for NextGen AU Optimization, Competition Response,
    Demand Redistribution, and Closed-Form Revenue Forecasting according to
    BRD-AU_Opt_Competition_Response.

    Fully dynamic and parameter/database-driven:
    - Band lookups query ndo_bands and comp_response_plf_bands collections dynamically.
    - Grid rules query comp_response_grid dynamically.
    - Grid rule expressions are evaluated dynamically via a safe mathematical expression parser.
    - All operational thresholds and factors are sourced dynamically from parameters/DB.
    """
    to_float = staticmethod(to_float)
    to_int = staticmethod(to_int)
    is_flag_enabled = staticmethod(is_flag_enabled)

    @classmethod
    def get_plf_band_info(cls, plf_pct, db_helper=None):
        """
        Dynamically determines the PLF band ID for a given PLF percentage.
        Queries comp_response_plf_bands via db_helper if available, or matches range.
        Returns: (band_id, None)
        """
        val = to_float(plf_pct, 0.0)
        if 0.0 < val <= 1.5:
            val = val * 100.0

        band_doc = None
        if db_helper and hasattr(db_helper, 'getCompResponsePlfBandDoc'):
            band_doc = db_helper.getCompResponsePlfBandDoc(val)
        elif hasattr(db_helper, 'db') and hasattr(db_helper.db, 'comp_response_plf_bands'):
            try:
                band_doc = db_helper.db.comp_response_plf_bands.find_one({"start": {"$lte": val}, "end": {"$gt": val}})
            except Exception as e:
                logger.error(f"Error querying comp_response_plf_bands: {e}")

        if band_doc:
            band_id = band_doc.get("plf_band", band_doc.get("id"))
            try:
                band_id = int(band_id) if band_id is not None else None
            except Exception:
                pass
            return band_id, None

        if db_helper and hasattr(db_helper, 'getCompResponsePlfBand'):
            band = db_helper.getCompResponsePlfBand(val)
            if band is not None:
                return band, None

        # Standard range mapping fallback
        if val < 80.0:
            b_id = 0
        elif val < 85.0:
            b_id = 1
        elif val < 90.0:
            b_id = 2
        elif val < 95.0:
            b_id = 3
        elif val < 100.0:
            b_id = 4
        else:
            b_id = 5

        return b_id, None

    @classmethod
    def get_plf_band(cls, plf_pct, db_helper=None):
        """
        Dynamically determines the PLF band for a given PLF percentage.
        Queries comp_response_plf_bands via db_helper if available, or matches range.
        """
        band_id, _ = cls.get_plf_band_info(plf_pct, db_helper=db_helper)
        return band_id

    @classmethod
    def get_ndo_band(cls, ndo, db_helper=None):
        """
        Dynamically determines the NDO band for a given NDO.
        Queries ndo_bands via db_helper using (start, end] interval convention.
        """
        n = to_int(ndo, 0)
        if n <= 0:
            return 0
        elif n == 1:
            return 1

        if hasattr(db_helper, 'db') and hasattr(db_helper.db, 'ndo_bands'):
            try:
                rec = db_helper.db.ndo_bands.find_one({"start": {"$lt": n}, "end": {"$gte": n}})
                if rec:
                    return rec.get("ndo_band", rec.get("id"))
            except Exception as e:
                logger.error(f"Error querying ndo_bands: {e}")

        # Standard range mapping matching ndo_bands
        if n <= 0:
            return 0
        elif n == 1:
            return 1
        elif n <= 3:
            return 2
        elif n <= 7:
            return 3
        elif n <= 14:
            return 4
        elif n <= 30:
            return 5
        elif n <= 60:
            return 6
        else:
            return 7

    @staticmethod
    def _apply_stepup_cap(au_series, cap):
        """Cap the consecutive step-up across an AU_Revised series.

        ``au_series`` is ordered highest-fare (index 0) .. lowest-fare (last
        index), i.e. the same order ``AU_Revised`` is stored on ``final_df``.
        The opening bucket is the last strictly-positive entry. Walking from
        the opening bucket toward higher fares, each subsequent open bucket may
        exceed the previous (already-capped) bucket by at most ``cap``. Buckets
        whose raw value is already within the step are left unchanged (never
        forced up). Zeros (closed buckets) are skipped and are not part of the
        step chain. The opening bucket itself is never inflated.
        """
        vals = [float(x) for x in au_series]
        n = len(vals)
        if n == 0 or cap is None or cap <= 0:
            return vals
        open_idx = None
        for i in range(n - 1, -1, -1):
            if vals[i] > 0.0:
                open_idx = i
                break
        if open_idx is None:
            return vals
        prev_capped = vals[open_idx]
        for i in range(open_idx - 1, -1, -1):
            if vals[i] <= 0.0:
                continue
            allowed = prev_capped + cap
            if vals[i] > allowed:
                vals[i] = allowed
            prev_capped = vals[i]
        return vals

    @classmethod
    def _apply_filed_rbd_stepup_cap(cls, mapping, filed_levels, cap, capacity):
        """Apply the same consecutive ``+cap`` step to the filed B2C RBD map.

        ``filed_levels`` is ordered highest-fare -> lowest-fare (Step 5 sorts
        it that way), so the chain walks in reverse: from the opening
        (lowest-fare) RBD upward. Zero (closed) RBDs are skipped. The last open
        RBD of the filed ladder (highest fare) is anchored to the flight
        capacity so the top of the ladder always allocates full capacity.
        """
        if not mapping or not filed_levels or cap is None or cap <= 0:
            return mapping
        prev_capped = None
        last_open_rbd = None
        for fl in reversed(filed_levels):
            rbd = fl.get('RBD')
            if rbd not in mapping:
                continue
            au = to_float(mapping[rbd], 0.0)
            if au <= 0.0:
                continue
            if prev_capped is None:
                prev_capped = au
                last_open_rbd = rbd
                continue
            allowed = prev_capped + cap
            if au > allowed:
                au = allowed
                mapping[rbd] = au
            prev_capped = au
            last_open_rbd = rbd
        # Last open RBD of the ladder (highest fare) allocates full capacity.
        if capacity is not None and capacity > 0 and last_open_rbd is not None:
            mapping[last_open_rbd] = float(capacity)
        return mapping

    @classmethod
    def compute_initial_emsr_b(
        cls,
        ladder_df,
        capacity,
        current_bookings,
        buy_up_factor=0.0,
        sigma_factor=DEFAULT_STANDARD_DEVIATION_FACTOR,
        promo_ratio=DEFAULT_PROMO_RATIO_NG,
        promo_threshold=DEFAULT_FORECAST_THRESHOLD_PROMO_AU,
        promo_flag=DEFAULT_PROMO_FLAG_NG,
        pax_col='mu',
        std_col='sigma',
        fare_col='fare'
    ):
        """
        Phase 1: Initial EMSR-b Optimization.
        Computes protection limits (y_j) and AU levels for all filed classes plus promo class,
        and identifies the Anchor Fare (A).
        """
        df = ladder_df.copy()
        if df.empty:
            return df, 0.0, 0.0

        if fare_col not in df.columns and 'FareBucket_Lower' in df.columns:
            df[fare_col] = df['FareBucket_Lower']
        elif fare_col not in df.columns and 'FareMidpoint' in df.columns:
            df[fare_col] = df['FareMidpoint']
        elif fare_col not in df.columns and 'TOTAL' in df.columns:
            df[fare_col] = df['TOTAL']
        elif fare_col not in df.columns and 'Total' in df.columns:
            df[fare_col] = df['Total']

        df[fare_col] = pd.to_numeric(df[fare_col], errors='coerce').fillna(0.0)
        df = df.sort_values(fare_col, ascending=False).reset_index(drop=True)

        n = len(df)
        cap = to_int(capacity, 189)
        if cap <= 0:
            cap = 189
        cb = to_float(current_bookings, 0.0)

        if pax_col in df.columns:
            mu = pd.to_numeric(df[pax_col], errors='coerce').fillna(0.0).clip(lower=0.0).tolist()
        else:
            mu = [0.0] * n
        if std_col in df.columns:
            sigma = pd.to_numeric(df[std_col], errors='coerce').fillna(0.0).clip(lower=0.0).tolist()
        else:
            sigma = [0.0] * n
        fares = [to_float(f, 0.0) for f in df[fare_col].tolist()]

        f_lowest = fares[-1] if n > 0 else 0.0
        f_promo = to_float(promo_ratio, DEFAULT_PROMO_RATIO_NG) * f_lowest

        alpha = to_float(buy_up_factor, 0.0)
        if alpha > 1.0:
            alpha = alpha / 100.0
        alpha = float(np.clip(alpha, 0.0, 0.99))
        buy_up_mult = 1.0 / (1.0 - alpha)
        sig_fact = to_float(sigma_factor, DEFAULT_STANDARD_DEVIATION_FACTOR)

        mu_bar_list = []
        sig_bar_list = []
        F_j_list = []
        y_list = []

        for j in range(n):
            mu_bar = float(sum(mu[:j+1]))
            sig_raw = math.sqrt(sum(s**2 for s in sigma[:j+1]))
            sig_bar = min(sig_raw, sig_fact * mu_bar)
            F_j = (sum(mu[i] * fares[i] for i in range(j+1)) / mu_bar) if mu_bar > 0 else fares[j]

            if j < n - 1:
                f_next = fares[j+1]
            elif is_flag_enabled(promo_flag):
                f_next = f_promo
            else:
                f_next = None

            if f_next is not None and F_j > 0:
                prob = (1.0 - (f_next / F_j)) * buy_up_mult
                prob_clipped = float(np.clip(prob, 1e-6, 1.0 - 1e-6))
                z = float(norm.ppf(prob_clipped))
                y_j = max(0.0, float(mu_bar + z * sig_bar))
            elif y_list:
                y_j = y_list[-1]
            else:
                y_j = 0.0

            if y_list:
                y_j = max(y_j, y_list[-1])  # monotonic nesting

            mu_bar_list.append(mu_bar)
            sig_bar_list.append(sig_bar)
            F_j_list.append(F_j)
            y_list.append(y_j)

        # Authorization levels (AU)
        au_list = [float(cap)]
        for j in range(1, n):
            au_list.append(max(0.0, float(cap) - y_list[j-1]))

        # Promo AU (only calculated when promo_flag == 1)
        if is_flag_enabled(promo_flag):
            au_promo = max(0.0, float(cap) - y_list[-1]) if n > 0 else float(cap)
        else:
            au_promo = 0.0

        df['mu'] = mu
        df['sigma'] = sigma
        df['mu_bar'] = mu_bar_list
        df['mu_bar_orig'] = mu_bar_list
        df['sigma_bar'] = sig_bar_list
        df['F_j'] = F_j_list
        df['ProtectionLimit'] = y_list
        df['protection_limit_yj'] = y_list
        df['AU'] = au_list
        df['Promo_Flag'] = 0

        # Anchor Fare A: lowest filed fare where AU > current_bookings
        open_classes = df[df['AU'] > cb]
        if not open_classes.empty:
            anchor_fare = to_float(open_classes[fare_col].iloc[-1], 0.0)
        else:
            anchor_fare = to_float(df[fare_col].iloc[0], 0.0)

        return df, anchor_fare, au_promo

    @classmethod
    def filter_market_fares(
        cls,
        market_fares,
        flight_std=None,
        time_range_hours=DEFAULT_TIME_RANGE,
        carrier_exclusion=None,
        connection_fare_flag=0,
        direct_duration_mins=None,
        additional_connection_hours=DEFAULT_ADDITIONAL_CONNECTION_DURATION,
        coverage_param=DEFAULT_COVERAGE_PARAMETER
    ):
        """
        Phase 2a: Dynamically filters competitor market fares for normal direct flights:
        1. Carrier exclusion.
        2. Connecting vs Direct: If connection_fare_flag == 1, includes connecting competitor flights
           provided their total duration <= direct_duration + additional_connection_hours * 60 mins.
           If connection_fare_flag == 0, includes only direct flights.
        3. Departure time window: [flight_std - time_range_hours, flight_std + time_range_hours].
        4. Modified Z-score (MAD) outlier removal using coverage_param.
        """
        if market_fares is None:
            return None, None

        if isinstance(market_fares, list):
            df_mkt = pd.DataFrame(market_fares)
        elif isinstance(market_fares, pd.DataFrame):
            df_mkt = market_fares.copy()
        else:
            return None, None

        if df_mkt.empty:
            return None, None

        # 1. Exclude carriers
        carr_col = next((c for c in ['outbound_airline_code', 'AirlineCode', 'airline_code', 'Carrier', 'carrier'] if c in df_mkt.columns), None)
        if carr_col and carrier_exclusion is not None and not pd.isna(carrier_exclusion):
            if isinstance(carrier_exclusion, str):
                carr_list = [c.strip().upper() for c in carrier_exclusion.split(',') if c.strip() and c.strip().lower() != 'nan']
            elif isinstance(carrier_exclusion, (list, tuple, set)):
                carr_list = [str(c).strip().upper() for c in carrier_exclusion if str(c).strip() and str(c).strip().lower() != 'nan']
            else:
                carr_list = []
            if carr_list:
                df_mkt = df_mkt[~df_mkt[carr_col].astype(str).str.upper().isin(carr_list)]

        if df_mkt.empty:
            return None, None

        # 2. Connecting vs Direct filtering for normal flight
        stops_col = next((c for c in ['outbound_stops', 'stops', 'Stops', 'outbound_stop'] if c in df_mkt.columns), None)
        dur_col = next((c for c in ['outbound_duration', 'duration', 'Duration', 'flight_duration'] if c in df_mkt.columns), None)

        if stops_col and stops_col in df_mkt.columns:
            stops_series = pd.to_numeric(df_mkt[stops_col], errors='coerce').fillna(0)
            if not is_flag_enabled(connection_fare_flag):
                # Direct flights only
                df_mkt = df_mkt[stops_series == 0]
            else:
                # Include direct flights AND connecting flights with duration <= (direct_duration + additional_duration)
                if dur_col and direct_duration_mins:
                    direct_mins = to_float(direct_duration_mins, 0.0)
                    add_mins = to_float(additional_connection_hours, 4.0) * 60.0
                    max_allowed_duration = direct_mins + add_mins

                    def parse_duration_to_mins(v):
                        try:
                            if pd.isna(v): return direct_mins
                            v_str = str(v).strip()
                            if ':' in v_str:
                                parts = v_str.split(':')
                                return to_float(parts[0], 0.0) * 60.0 + to_float(parts[1], 0.0)
                            return to_float(v_str, direct_mins)
                        except Exception:
                            return direct_mins

                    dur_series = df_mkt[dur_col].apply(parse_duration_to_mins)
                    df_mkt = df_mkt[(stops_series == 0) | ((stops_series > 0) & (dur_series <= max_allowed_duration))]

        if df_mkt.empty:
            return None, None

        # 3. Departure time window filtering
        time_col = next((c for c in ['outbound_departuretime', 'DepTime', 'departure_time', 'dep_time', 'STD'] if c in df_mkt.columns), None)
        if time_col and flight_std and time_range_hours:
            try:
                def parse_std_time(t_str):
                    if not t_str or pd.isna(t_str): return None
                    s = str(t_str).strip()
                    if len(s) >= 5:
                        parts = s[:5].split(':')
                        return to_int(parts[0], 0) * 60 + to_int(parts[1], 0)
                    return None

                target_mins = parse_std_time(flight_std)
                if target_mins is not None:
                    window_mins = to_float(time_range_hours, 2.0) * 60.0
                    mkt_mins = df_mkt[time_col].apply(parse_std_time)
                    valid_time = mkt_mins.notna() & (abs(mkt_mins - target_mins) <= window_mins)
                    if valid_time.any():
                        df_mkt = df_mkt[valid_time]
            except Exception:
                pass

        if df_mkt.empty:
            return None, None

        # 4. Extract numeric fare
        fare_col = next((c for c in ['gross_fare', 'Fare', 'fare', 'TotalFare', 'total_fare', 'TOTAL', 'price', 'Total'] if c in df_mkt.columns), None)
        if not fare_col:
            return None, None

        fares = pd.to_numeric(df_mkt[fare_col], errors='coerce').dropna()
        fares = fares[fares > 0].tolist()

        if not fares:
            return None, None

        # 5. Outlier removal via Modified Z-score (MAD)
        cov_p = to_float(coverage_param, 3.0)
        med = float(np.median(fares))
        abs_dev = [abs(f - med) for f in fares]
        mad = float(np.median(abs_dev))
        if mad == 0:
            mad = 0.15 * med

        valid_fares = [f for f in fares if (abs(f - med) / mad) < cov_p] if mad > 0 else fares
        if not valid_fares:
            valid_fares = fares

        return float(min(valid_fares)), float(np.median(valid_fares))

    @classmethod
    def evaluate_grid_response(
        cls,
        grid_rule_str,
        anchor_fare,
        comp_min_fare,
        comp_med_fare=None,
        tolerance=None,
        threshold_factor=None
    ):
        """
        Phase 2b: Dynamically parses and evaluates ANY Competition Response Grid formula
        involving Anchor Fare (A) and Competitor Fare (C).
        Supports:
          - 'Min (1.2A, 0.8C)', 'Min (1.3A, 0.9C)', 'Min (1.5A, C)', 'Min (2A, C)', 'Min (xA, yC)'
          - 'Max (xA, yC)', 'C', '0.95C', 'A', '1.1A', etc.
        Also applies tolerance floor if provided:
          final_response = max(thresh_fare, rule_result)
        """
        A = to_float(anchor_fare, 0.0)
        C = to_float(comp_min_fare, 0.0)
        med_C = to_float(comp_med_fare, C) if comp_med_fare is not None else C

        rule = str(grid_rule_str).strip()
        if not rule or rule == CONST_NA:
            rule_resp = C
        else:
            # Determine whether to use median or minimum competitor fare
            C_val = med_C if re.search(r'\bmed\b', rule, re.IGNORECASE) else C

            # Clean and extract mathematical expression
            expr = rule
            if '/' in expr:
                expr = expr.split('/')[-1].strip()

            # Standardize operators and variables
            expr = re.sub(r'(?i)\bmin\b', 'min', expr)
            expr = re.sub(r'(?i)\bmax\b', 'max', expr)
            # Convert numeric multipliers like 1.2A -> 1.2*A and 0.8C -> 0.8*C
            expr = re.sub(r'(\d+(?:\.\d+)?)\s*[aA]\b', r'\1*A', expr)
            expr = re.sub(r'(\d+(?:\.\d+)?)\s*[cC]\b', r'\1*C', expr)
            expr = re.sub(r'(?i)\bA\b', 'A', expr)
            expr = re.sub(r'(?i)\bC\b', 'C', expr)

            safe_env = {
                "A": A,
                "C": C_val,
                "min": min,
                "max": max,
                "round": round
            }

            try:
                rule_resp = float(eval(expr, {"__builtins__": {}}, safe_env))
            except Exception as e:
                logger.warning(f"Error evaluating grid rule expression '{expr}': {e}. Defaulting to competitor fare.")
                rule_resp = C_val

        # Apply tolerance / threshold_factor: max(tolerance_fare, whatever response rule comes from grid)
        tol_param = tolerance if tolerance is not None else threshold_factor
        if tol_param is not None and pd.notna(tol_param) and str(tol_param).strip().upper() not in ['', 'NA', 'NAN']:
            tf = to_float(tol_param, 0.0)
            if tf > 100.0:
                rule_resp = max(tf, rule_resp)
            else:
                if tf > 1.5:
                    tf = tf / 100.0  # normalize if passed as percentage e.g. 80 -> 0.80
                if tf > 0:
                    c_for_thresh = med_C if re.search(r'\bmed\b', rule, re.IGNORECASE) else C
                    thresh_fare = tf * to_float(c_for_thresh, 0.0)
                    rule_resp = max(thresh_fare, rule_resp)

        return rule_resp

    @classmethod
    def determine_strategy_fare(cls, response_fare, ladder_df=None, fare_col='fare'):
        """
        Strategy Fare C is the highest ladder fare lower than (or equal to) the response fare.
        Ensures strategy fare is always chosen among the discrete filed ladder fares.
        """
        if response_fare is None or pd.isna(response_fare) or str(response_fare).strip().upper() in ['', 'NA', 'NAN']:
            return None

        resp_f = to_float(response_fare, 0.0)

        if ladder_df is None:
            return resp_f

        if isinstance(ladder_df, (list, tuple)):
            fares = sorted([to_float(x, 0.0) for x in ladder_df if pd.notna(x) and str(x).strip().upper() not in ['', 'NA', 'NAN']], reverse=True)
        elif isinstance(ladder_df, pd.Series):
            fares = pd.to_numeric(ladder_df, errors='coerce').dropna().sort_values(ascending=False).tolist()
        elif isinstance(ladder_df, pd.DataFrame):
            if ladder_df.empty:
                return resp_f
            if fare_col not in ladder_df.columns:
                fare_col = next((c for c in ['fare', 'FareBucket_Lower', 'FareMidpoint', 'TOTAL', 'Total'] if c in ladder_df.columns), None)
                if not fare_col:
                    return resp_f
            fares = pd.to_numeric(ladder_df[fare_col], errors='coerce').dropna().sort_values(ascending=False).tolist()
        else:
            return resp_f

        if not fares:
            return resp_f

        valid = [f for f in fares if f <= resp_f]
        if valid:
            return float(valid[0])
        return float(fares[-1])

    @classmethod
    def redistribute_demand_and_compute_revised_au(
        cls,
        ladder_df,
        anchor_fare,
        strategy_fare,
        capacity,
        current_bookings,
        buy_up_factor=0.0,
        sigma_factor=0.25,
        first_rbd=20,
        promo_ratio=DEFAULT_PROMO_RATIO_NG,
        backstop=None,
        hardanchor=None,
        promo_au=0,
        promo_flag=DEFAULT_PROMO_FLAG_NG,
        promo_first_rbd=DEFAULT_PROMO_FIRST_RBD_NG,
        promo_threshold=DEFAULT_FORECAST_THRESHOLD_PROMO_AU,
        pmin_threshold_factor=DEFAULT_PMIN_THRESHOLD_FACTOR_NG,
        plf_threshold=DEFAULT_FORECAST_THRESHOLD_PROMO_AU,
        fare_col='fare',
        tax_helper=None,
        origin=None,
        destin=None,
        route=None
    ):
        """
        Phase 3: Demand Redistribution & Revised AU Calculation.
        If A < C, warps the demand curve via P_eq equivalent price mapping, computes mu_2, sigma_2,
        AU_2, and final AU_revised with FirstRBD cap, Backstop floor, and Hardanchor cap.
        When promo_flag == 1, recomputes promo inventory during competition response using
        strategy fare and 80% of strategy fare, capped by PromoFirstRBD_NG.
        """
        df = ladder_df.copy()
        if df.empty:
            return df

        if fare_col not in df.columns:
            fare_col = next((c for c in ['fare', 'FareBucket_Lower', 'FareMidpoint', 'TOTAL', 'Total'] if c in df.columns), 'fare')

        df[fare_col] = pd.to_numeric(df[fare_col], errors='coerce').fillna(0.0)
        df = df.sort_values(fare_col, ascending=False).reset_index(drop=True)
        fares = [to_float(f, 0.0) for f in df[fare_col].tolist()]
        n = len(df)

        cap = to_int(capacity, 189)
        if cap <= 0:
            cap = 189
        cb = to_float(current_bookings, 0.0)
        A = to_float(anchor_fare, 0.0)
        C = to_float(strategy_fare, A)
        sig_fact = to_float(sigma_factor, DEFAULT_STANDARD_DEVIATION_FACTOR)
        first_rbd_seats = to_float(first_rbd, DEFAULT_FIRST_RBD_SEATS)

        # Convert backstop and hardanchor to airline component
        b_val = to_float(backstop, None)
        if b_val is not None and b_val > 0 and tax_helper and origin and destin:
            try:
                b_val = tax_helper.allin_to_airline_component(float(b_val), origin, destin, route=route)
            except Exception as e:
                logger.warning(f"Error converting backstop {b_val} to airline component: {e}")

        h_val = to_float(hardanchor, None)
        if h_val is not None and h_val > 0 and tax_helper and origin and destin:
            try:
                h_val = tax_helper.allin_to_airline_component(float(h_val), origin, destin, route=route)
            except Exception as e:
                logger.warning(f"Error converting hardanchor {h_val} to airline component: {e}")

        strat_candidates = [f for f in fares if f <= C]
        C_ladder = strat_candidates[0] if strat_candidates else fares[-1]

        anchor_candidates = [f for f in fares if f <= A]
        a_ladder = anchor_candidates[0] if anchor_candidates else fares[-1]

        # If C_ladder <= a_ladder or Comp missing, no demand redistribution
        if C_ladder <= a_ladder or strategy_fare is None or pd.isna(strategy_fare):
            df.attrs['p_min_old'] = A
            df['P_eq'] = np.nan
            df['mu_bar_2'] = df['mu_bar']
            df['mu_2'] = df['mu']
            if 'sigma' in df.columns:
                df['sigma_2'] = df['sigma'].copy()
                df['sigma_3'] = df['sigma'].copy()
            else:
                df['sigma_2'] = sig_fact * df['mu_2']
                df['sigma_3'] = df['sigma_2']
            df['sigma_bar_3'] = df['sigma_bar']
            au_2_list = df['AU'].copy().tolist()
            anchor_idx = fares.index(a_ladder)
            for j in range(anchor_idx + 1, n):
                au_2_list[j] = 0.0
            
            # Floor protection: if AU_2 at Anchor Fare < current_bookings, elevate
            if au_2_list[anchor_idx] < cb:
                elevate_au = cb
                for k in range(anchor_idx, -1, -1):
                    if au_2_list[k] >= cb:
                        elevate_au = au_2_list[k]
                        break
                for k in range(anchor_idx, -1, -1):
                    if au_2_list[k] < elevate_au:
                        au_2_list[k] = elevate_au

            df['AU_2'] = au_2_list
            
            au_rev = df['AU_2'].copy().tolist()
            for j in range(anchor_idx + 1, n):
                au_rev[j] = 0.0

            # Apply backstop floor
            if b_val is not None and b_val > 0:
                for i, f in enumerate(fares):
                    if f < b_val:
                        au_rev[i] = 0.0

            # 1. FirstRBD logic (20-seat threshold) applied at opening bucket
            open_idx = None
            for i in range(n - 1, -1, -1):
                if au_rev[i] > 0:
                    open_idx = i
                    break

            first_rbd_seats = to_float(first_rbd, DEFAULT_FIRST_RBD_SEATS)
            if first_rbd_seats <= 0:
                first_rbd_seats = float(DEFAULT_FIRST_RBD_SEATS)

            if open_idx is not None:
                au_rev[open_idx] = min(au_rev[open_idx], float(cb + first_rbd_seats))

            # 2. Hardanchor with PLF threshold: applied after FirstRBD logic
            if h_val is not None and h_val > 0:
                lower_candidates = [f for f in fares if f <= h_val]
                h_ladder = lower_candidates[0] if lower_candidates else fares[-1]
                ha_idx = fares.index(h_ladder)

                plf_th = to_float(plf_threshold if plf_threshold is not None else promo_threshold, DEFAULT_FORECAST_THRESHOLD_PROMO_AU)
                plf_pct = (plf_th / 100.0) if plf_th > 1.5 else plf_th
                plf_target = float(round(plf_pct * cap, 2))
                plf_target = min(float(cap), max(0.0, plf_target))

                if au_rev[ha_idx] < plf_target:
                    au_rev[ha_idx] = plf_target

                for i in range(ha_idx):
                    if au_rev[i] < au_rev[ha_idx]:
                        au_rev[i] = plf_target

                if b_val is not None and b_val > 0:
                    for i, f in enumerate(fares):
                        if f < b_val:
                            au_rev[i] = 0.0

            df['AU_Revised'] = au_rev

            f_lowest = fares[-1] if n > 0 else 0.0
            f_promo_init = to_float(promo_ratio, DEFAULT_PROMO_RATIO_NG) * f_lowest
            if is_flag_enabled(promo_flag):
                au_2_promo = to_float(promo_au, 0.0)
                promo_thresh = float(cb + to_float(promo_first_rbd, DEFAULT_PROMO_FIRST_RBD_NG))
                if au_2_promo > promo_thresh:
                    au_revised_promo = promo_thresh
                else:
                    au_revised_promo = float(au_2_promo)
                if b_val is not None and f_promo_init < b_val:
                    au_revised_promo = 0.0
            else:
                au_2_promo = 0.0
                au_revised_promo = 0.0

            df.attrs['promo_info'] = {
                'AU': to_float(promo_au, 0.0) if is_flag_enabled(promo_flag) else 0.0,
                'AU_2': float(au_2_promo),
                'AU_Revised': float(au_revised_promo),
                'fare': f_promo_init
            }
            df.attrs['anchor_fare'] = float(A)
            return df

        # --- C_ladder > a_ladder: Demand Redistribution Triggers ---
        P_max = fares[0]
        strat_idx = fares.index(C_ladder)
        a_idx = fares.index(a_ladder)

        # Calculate Pmin_old by taking sum of mu in original demand distribution
        # from anchor fare till Strategy Fare (C). The fare at which this sum crosses
        # the threshold of pmin_threshold_factor * Capacity (default 0.1) is Pmin_Old.
        # If it doesn't cross the threshold even at C then C itself will be Pmin_old.
        cum_mu = 0.0
        p_min_old = C_ladder
        threshold = to_float(pmin_threshold_factor, DEFAULT_PMIN_THRESHOLD_FACTOR_NG) * float(cap)
        for idx in range(a_idx, strat_idx - 1, -1):
            cum_mu += to_float(df['mu'].iloc[idx], 0.0)
            if cum_mu >= threshold:
                p_min_old = float(fares[idx])
                break

        df.attrs['p_min_old'] = p_min_old

        orig_mu_bars = df['mu_bar'].tolist()
        P_eq_list = []
        mu_bar_2_list = []

        for i, p in enumerate(fares):
            if p < C_ladder:
                peq = 0.0
                m_bar_2 = 0.0
            elif p == C_ladder:
                # P_eq for C remains Anchor fare
                peq = A
                m_bar_2 = orig_mu_bars[a_idx]
            elif p >= P_max:
                # P_eq for max fare remains max fare
                peq = P_max
                m_bar_2 = orig_mu_bars[0]
            else:
                # Intermediate fares (C_ladder < p < P_max) use Pmin_old
                denom = (P_max - C_ladder)
                peq = p_min_old + ((p - C_ladder) / denom) * (P_max - p_min_old) if denom > 0 else p_min_old

                # Piecewise uniform interpolation of mu_bar
                if peq >= fares[0]:
                    m_bar_2 = orig_mu_bars[0]
                elif peq <= fares[-1]:
                    m_bar_2 = orig_mu_bars[-1]
                else:
                    m_bar_2 = 0.0
                    for k in range(len(fares) - 1):
                        if fares[k] >= peq >= fares[k+1]:
                            frac = (fares[k] - peq) / (fares[k] - fares[k+1]) if (fares[k] - fares[k+1]) > 0 else 0.0
                            m_bar_2 = orig_mu_bars[k] + frac * (orig_mu_bars[k+1] - orig_mu_bars[k])
                            break

            P_eq_list.append(peq)
            mu_bar_2_list.append(m_bar_2)

        # Derive mu_2 by consecutive differencing
        mu_2_list = []
        for i in range(n):
            if fares[i] < C_ladder:
                mu_2_list.append(0.0)
            elif i == 0:
                mu_2_list.append(mu_bar_2_list[0])
            else:
                mu_2_list.append(max(0.0, mu_bar_2_list[i] - mu_bar_2_list[i-1]))

        # For redistributed demand, standard deviation is parameterized by sig_fact * mu_2
        sigma_2_list = [sig_fact * m for m in mu_2_list]

        df['P_eq'] = P_eq_list
        df['mu_bar_2'] = mu_bar_2_list
        df['mu_2'] = mu_2_list
        df['sigma_2'] = sigma_2_list
        df['sigma_3'] = sigma_2_list
        df['sigma_bar_3'] = [sig_fact * sum(mu_2_list[:j+1]) for j in range(n)]

        # Recompute EMSR-b on mu_2, sigma_2
        f_promo = to_float(promo_ratio, DEFAULT_PROMO_RATIO_NG) * fares[-1]
        alpha = to_float(buy_up_factor, 0.0)
        if alpha > 1.0:
            alpha = alpha / 100.0
        alpha = float(np.clip(alpha, 0.0, 0.99))
        buy_up_mult = 1.0 / (1.0 - alpha)

        y_2_list = []
        for j in range(n):
            if fares[j] < C_ladder:
                y_2_list.append(float(cap))
                continue
            m_bar = sum(mu_2_list[:j+1])
            s_bar = sig_fact * m_bar
            F_j_2 = (sum(mu_2_list[i] * fares[i] for i in range(j+1)) / m_bar) if m_bar > 0 else fares[j]
            f_next = fares[j+1] if j < n - 1 else f_promo

            if F_j_2 > 0:
                prob = (1.0 - (f_next / F_j_2)) * buy_up_mult
            else:
                prob = 0.5
            prob_clipped = float(np.clip(prob, 1e-6, 1.0 - 1e-6))
            z = float(norm.ppf(prob_clipped))
            y_j = max(0.0, float(m_bar + z * s_bar))
            if y_2_list and y_2_list[-1] < cap:
                y_j = max(y_j, y_2_list[-1])
            y_2_list.append(y_j)

        df['ProtectionLimit_3'] = y_2_list

        au_2_list = [float(cap)]
        for j in range(1, n):
            if fares[j] < C_ladder:
                au_2_list.append(0.0)
            else:
                au_2_list.append(max(0.0, float(cap) - y_2_list[j-1]))

        # Floor protection: if AU_2 at Strategy Fare < current_bookings, elevate
        if au_2_list[strat_idx] < cb:
            elevate_au = cb
            for k in range(strat_idx, -1, -1):
                if au_2_list[k] >= cb:
                    elevate_au = au_2_list[k]
                    break
            for k in range(strat_idx, -1, -1):
                if au_2_list[k] < elevate_au:
                    au_2_list[k] = elevate_au

        df['AU_2'] = au_2_list

        # AU_revised: 1. Close buckets below strat_idx
        au_revised = au_2_list.copy()
        for j in range(strat_idx + 1, n):
            au_revised[j] = 0.0

        # Apply backstop floor
        if b_val is not None and b_val > 0:
            for i, f in enumerate(fares):
                if f < b_val:
                    au_revised[i] = 0.0

        # 1. FirstRBD logic (20-seat threshold) applied at opening bucket
        open_idx = None
        for i in range(n - 1, -1, -1):
            if au_revised[i] > 0:
                open_idx = i
                break

        first_rbd_seats = to_float(first_rbd, DEFAULT_FIRST_RBD_SEATS)
        if first_rbd_seats <= 0:
            first_rbd_seats = float(DEFAULT_FIRST_RBD_SEATS)

        if open_idx is not None:
            au_revised[open_idx] = min(au_revised[open_idx], float(cb + first_rbd_seats))

        # 2. Hardanchor with PLF threshold: applied after FirstRBD logic
        if h_val is not None and h_val > 0:
            lower_candidates = [f for f in fares if f <= h_val]
            h_ladder = lower_candidates[0] if lower_candidates else fares[-1]
            ha_idx = fares.index(h_ladder)

            plf_th = to_float(plf_threshold if plf_threshold is not None else promo_threshold, DEFAULT_FORECAST_THRESHOLD_PROMO_AU)
            plf_pct = (plf_th / 100.0) if plf_th > 1.5 else plf_th
            plf_target = float(round(plf_pct * cap, 2))
            plf_target = min(float(cap), max(0.0, plf_target))

            if au_revised[ha_idx] < plf_target:
                au_revised[ha_idx] = plf_target

            for i in range(ha_idx):
                if au_revised[i] < au_revised[ha_idx]:
                    au_revised[i] = plf_target

            if b_val is not None and b_val > 0:
                for i, f in enumerate(fares):
                    if f < b_val:
                        au_revised[i] = 0.0

        df['AU_Revised'] = au_revised

        # During competition response, Promo inventory calculation is done again using strategy fare and 80% of strategy fare
        f_promo_comp = to_float(promo_ratio, DEFAULT_PROMO_RATIO_NG) * float(fares[strat_idx])
        if is_flag_enabled(promo_flag):
            m_bar_strat = sum(mu_2_list[:strat_idx + 1])
            s_bar_strat = sig_fact * m_bar_strat
            F_strat = (sum(mu_2_list[i] * fares[i] for i in range(strat_idx + 1)) / m_bar_strat) if m_bar_strat > 0 else fares[strat_idx]
            prob_strat = (1.0 - (f_promo_comp / F_strat)) * buy_up_mult if F_strat > 0 else 0.5
            prob_clipped = float(np.clip(prob_strat, 1e-6, 1.0 - 1e-6))
            z_strat = float(norm.ppf(prob_clipped))
            y_promo_comp = max(0.0, float(m_bar_strat + z_strat * s_bar_strat))
            if strat_idx > 0 and len(y_2_list) > strat_idx - 1:
                y_promo_comp = max(y_promo_comp, y_2_list[strat_idx - 1])

            au_2_promo = max(0.0, float(cap) - y_promo_comp)

            # Promo threshold: current_bookings + promo_first_rbd. If AU_2 crossed threshold make it threshold, otherwise take AU_2
            promo_thresh = float(cb + to_float(promo_first_rbd, DEFAULT_PROMO_FIRST_RBD_NG))
            if au_2_promo > promo_thresh:
                au_revised_promo = promo_thresh
            else:
                au_revised_promo = float(au_2_promo)
            if b_val is not None and f_promo_comp < b_val:
                au_revised_promo = 0.0
        else:
            au_2_promo = 0.0
            au_revised_promo = 0.0

        df.attrs['promo_info'] = {
            'AU': to_float(promo_au, 0.0) if is_flag_enabled(promo_flag) else 0.0,
            'AU_2': float(au_2_promo),
            'AU_Revised': float(au_revised_promo),
            'fare': f_promo_comp
        }
        df.attrs['anchor_fare'] = float(A)
        return df

    @classmethod
    def compute_closed_form_revenue_forecast(
        cls,
        ladder_df,
        capacity,
        current_bookings,
        current_revenue,
        fare_col='fare',
        mu_col='mu_2',
        sigma_col='sigma_2',
        au_col='AU_2',
        anchor_fare=None
    ):
        """
        Phase 4: Closed-Form Revenue Forecasting using standard normal distribution integrals.
        """
        df = ladder_df.copy()
        if df.empty:
            return 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, df

        if fare_col not in df.columns:
            fare_col = next((c for c in ['fare', 'FareBucket_Lower', 'FareMidpoint', 'TOTAL', 'Total'] if c in df.columns), 'fare')

        df = df.sort_values(fare_col, ascending=False).reset_index(drop=True)
        fares = [to_float(f, 0.0) for f in df[fare_col].tolist()]
        mu_raw = df[mu_col].tolist() if mu_col in df.columns else (df['mu'].tolist() if 'mu' in df.columns else [0.0] * len(df))
        mu_2 = [to_float(m, 0.0) for m in mu_raw]
        if sigma_col in df.columns:
            sigma_2 = [to_float(s, 0.0) for s in df[sigma_col].tolist()]
        elif 'sigma' in df.columns:
            sigma_2 = [to_float(s, 0.0) for s in df['sigma'].tolist()]
        else:
            sigma_2 = [0.25 * m for m in mu_2]

        cap = to_int(capacity, 189)
        if cap <= 0:
            cap = 189
        cb = to_float(current_bookings, 0.0)
        curr_rev = to_float(current_revenue, 0.0)

        au_col_to_use = au_col if au_col in df.columns else ('AU_2' if 'AU_2' in df.columns else ('AU_Revised' if 'AU_Revised' in df.columns else 'AU'))
        au_raw = df[au_col_to_use].tolist() if au_col_to_use in df.columns else [float(cap)] * len(df)
        au_vals = [to_float(a, float(cap)) for a in au_raw]
        n = len(df)

        # Resolve anchor fare if not provided
        if anchor_fare is None:
            if 'anchor_fare' in df.attrs:
                anchor_fare = to_float(df.attrs['anchor_fare'], 0.0)
            elif 'anchor_fare' in df.columns and pd.notna(df['anchor_fare'].iloc[0]):
                anchor_fare = to_float(df['anchor_fare'].iloc[0], 0.0)
            elif 'AU' in df.columns:
                open_classes = df[df['AU'] > cb]
                anchor_fare = to_float(open_classes[fare_col].iloc[-1], 0.0) if not open_classes.empty else to_float(df[fare_col].iloc[0], 0.0)
            elif au_col_to_use in df.columns:
                open_classes = df[df[au_col_to_use] > cb]
                anchor_fare = to_float(open_classes[fare_col].iloc[-1], 0.0) if not open_classes.empty else to_float(df[fare_col].iloc[0], 0.0)
            else:
                anchor_fare = to_float(fares[-1], 0.0) if fares else 0.0

        anchor_val = to_float(anchor_fare, 0.0)

        A_cap = [0.0] * n   # Capped expected bookings per bucket (A_hat)
        ER = [0.0] * n      # Expected revenue per bucket
        A_list = [0.0] * n  # Available capacity limit (helper column A)

        # Process bottom-up from lowest fare to highest fare
        for i in reversed(range(n)):
            if fares[i] < (anchor_val - 1e-5):
                A_list[i] = 0.0
                A_cap[i] = 0.0
                ER[i] = 0.0
                continue

            sum_lower = sum(A_cap[i+1:])
            Ai = max(0.0, float(au_vals[i]) - cb - sum_lower)
            A_list[i] = Ai
            m_i = float(mu_2[i])
            s_i = float(sigma_2[i])

            if s_i > 0:
                zi = (Ai - m_i) / s_i
                phi_z = float(norm.pdf(zi))
                Phi_z = float(norm.cdf(zi))
                A_hat_i = m_i * Phi_z - s_i * phi_z + Ai * (1.0 - Phi_z)
            else:
                A_hat_i = min(m_i, Ai)

            A_cap[i] = max(0.0, A_hat_i)
            ER[i] = fares[i] * A_cap[i]

        total_expected_revenue = float(sum(ER))
        predicted_revenue = curr_rev + total_expected_revenue

        df['A'] = A_list
        df['A_cap'] = A_cap
        df['Expected_Bookings'] = A_cap
        df['Expected_Revenue'] = ER
        df['Total_Expected_Revenue'] = total_expected_revenue
        df['Predicted_Revenue'] = predicted_revenue

        # For LF_Forecast_Revised, sum mu from anchor fare and above
        mu_for_pickup = df['mu'] if 'mu' in df.columns else df[mu_col]
        anchor_mask = df[fare_col] >= (anchor_val - 1e-5)
        pred_pickup_rev = float(mu_for_pickup[anchor_mask].sum())

        pred_flown_rev = cb + pred_pickup_rev

        denom = min(cap, pred_flown_rev) if pred_flown_rev > 0 else cap
        af_forecast = (predicted_revenue / denom) if denom > 0 else 0.0
        lf_forecast_rev = (pred_flown_rev / cap) if cap > 0 else 0.0

        return predicted_revenue, total_expected_revenue, pred_pickup_rev, pred_flown_rev, af_forecast, lf_forecast_rev, df

    @classmethod
    def execute_flight_nextgen_optimization(
        cls,
        flight_ladder_df,
        capacity,
        current_bookings,
        current_revenue,
        market_fares=None,
        flight_std=None,
        curr_params=None,
        db_helper=None,
        recalibration_dict=None,
        tax_helper=None,
        filed_51_ladder_df=None
    ):
        """
        Executes end-to-end NextGen AU optimization and revenue forecasting for a single flight.
        Dynamically queries DB for grid rules, bands, parameters, and competitor recalibrations.
        Converts all-in competitor market fares to airline component for fair comparison against
        the weighted forecast ladder, and ensures all summary fares are presented as all-in fares.
        """
        # Dynamic parameter extraction from curr_params / market_list / DB
        def _get_val(obj, *keys, default=None):
            if obj is None:
                return default
            for k in keys:
                if hasattr(obj, k):
                    v = getattr(obj, k)
                    if type(v).__name__ != 'MagicMock' and v is not None:
                        s = str(v).strip().upper()
                        if s not in ['', 'NA', 'NAN', 'NONE', 'NULL']:
                            return v
                elif isinstance(obj, dict) and k in obj:
                    v = obj[k]
                    if v is not None:
                        s = str(v).strip().upper()
                        if s not in ['', 'NA', 'NAN', 'NONE', 'NULL']:
                            return v
            return default

        origin = _get_val(curr_params, 'origin', 'Origin', default=None)
        destin = _get_val(curr_params, 'destin', 'destination', 'Destin', default=None)
        route = _get_val(curr_params, 'route', default=None)

        # Resolve tax helper if not passed explicitly
        if tax_helper is None:
            tax_helper = _get_val(curr_params, 'tax_helper', default=getattr(db_helper, 'tax_helper', None))

        # Resolve 51 filed B2C fare levels in airline component early
        filed_51_df = None
        if filed_51_ladder_df is not None and not filed_51_ladder_df.empty:
            filed_51_df = filed_51_ladder_df.copy()
        elif db_helper and hasattr(db_helper, 'fareExtract') and origin and destin:
            try:
                b2c_fares = db_helper.fareExtract(origin, destin, 'B2C', '', route or 'L')
                if b2c_fares is not None and not b2c_fares.empty:
                    filed_51_df = b2c_fares.copy()
            except Exception as e:
                logger.warning(f"Could not extract 51 filed B2C fares: {e}")

        filed_levels = []
        if filed_51_df is not None and not filed_51_df.empty:
            total_col = next((c for c in ['TOTAL', 'Total', 'allin', 'All_in'] if c in filed_51_df.columns), None)
            comp_col = next((c for c in ['airline_fare', 'fare', 'Fare', 'airline_comp'] if c in filed_51_df.columns), None)

            for _, r in filed_51_df.iterrows():
                rbd_code = str(r.get('RBD', '')).strip()
                if comp_col:
                    comp_f = to_float(r.get(comp_col), 0.0)
                elif total_col and tax_helper and origin and destin:
                    comp_f = tax_helper.allin_to_airline_component(to_float(r.get(total_col), 0.0), origin, destin, route=route)
                else:
                    comp_f = to_float(r.get(total_col), 0.0) if total_col else 0.0

                if total_col:
                    allin_f = to_float(r.get(total_col), 0.0)
                elif tax_helper and origin and destin:
                    allin_f = tax_helper.airline_component_to_allin(comp_f, origin, destin, route=route)
                else:
                    allin_f = comp_f

                filed_levels.append({
                    'RBD': rbd_code,
                    'fare': comp_f,
                    'allin': allin_f
                })
            filed_levels = sorted(filed_levels, key=lambda x: x['fare'], reverse=True)

        const_utils = getattr(db_helper, 'constants_utils', getattr(db_helper, 'constantsUtils', None))
        first_rbd_const = getattr(const_utils, 'DEFAULT_FIRST_RBD_SEATS', getattr(const_utils, 'FIRST_RBD_SEATS', getattr(const_utils, 'FirstRBDAlloc', DEFAULT_FIRST_RBD_SEATS)))
        promo_threshold_const = getattr(const_utils, 'DEFAULT_FORECAST_THRESHOLD_PROMO_AU', DEFAULT_FORECAST_THRESHOLD_PROMO_AU)
        sigma_factor_const = getattr(const_utils, 'EMSR_SIGMA_FACTOR', getattr(const_utils, 'DEFAULT_STANDARD_DEVIATION_FACTOR', DEFAULT_STANDARD_DEVIATION_FACTOR))
        time_range_const = getattr(const_utils, 'DEFAULT_TIME_RANGE', DEFAULT_TIME_RANGE)
        cov_param_const = getattr(const_utils, 'COVERAGE_PARAMETER', getattr(const_utils, 'DEFAULT_COVERAGE_PARAMETER', DEFAULT_COVERAGE_PARAMETER))
        add_conn_dur_const = getattr(const_utils, 'DEFAULT_ADDITIONAL_CONNECTION_DURATION', DEFAULT_ADDITIONAL_CONNECTION_DURATION)
        pmin_thresh_const = getattr(const_utils, 'PMIN_THRESHOLD_FACTOR_NG', getattr(const_utils, 'pmin_threshold_factor_NG', DEFAULT_PMIN_THRESHOLD_FACTOR_NG))
        # Maximum consecutive step-up (seats) allowed between successive
        # AU_Revised allocations / filed B2C RBD allocations.
        nextgen_otherrbd = to_float(
            getattr(const_utils, 'NEXTGEN_OTHERRBD', NEXTGEN_OTHERRBD),
            NEXTGEN_OTHERRBD,
        )
        if nextgen_otherrbd <= 0:
            nextgen_otherrbd = float(NEXTGEN_OTHERRBD)

        raw_buy_up = to_float(_get_val(curr_params, 'buy_up_factor_NG', 'buy_up_factor', default=0.0), 0.0)
        buy_up = (raw_buy_up / 100.0) if raw_buy_up > 1.0 else raw_buy_up
        comp_flag = 1 if is_flag_enabled(_get_val(curr_params, 'competition_response_flag_NG', 'competition_response_flag', default=0)) else 0
        time_range = to_float(_get_val(curr_params, 'time_range', default=time_range_const), to_float(time_range_const, DEFAULT_TIME_RANGE))
        carr_excl = _get_val(curr_params, 'carr_exclusion_b2c', 'CarrExlusionB2C', 'carrier_exclusion', default=None)
        grid_name = str(_get_val(curr_params, 'CompetitionResponseGrid_NG', 'grid_name', default='DEFAULT')).strip() or 'DEFAULT'
        # In NextGen, use raw B2C backstop from Market List directly (do not use autobackstop / b2c_backstop_final)
        raw_bs = _get_val(curr_params, 'b2c_backstop', 'B2CBackstop', 'price_floor', default=None)
        backstop = to_float(raw_bs, None)
        if backstop is not None and backstop <= 0:
            backstop = None

        hardanchor = None
        ndo = to_int(_get_val(curr_params, 'ndocounter', 'NDO', default=0), 0)
        # First RBD seat threshold for NextGen comes from parameter table / ConstantsUtils / Constants.py, NOT from MarketList FirstRBDAlloc
        first_rbd = to_float(first_rbd_const, DEFAULT_FIRST_RBD_SEATS)
        if first_rbd <= 0:
            first_rbd = float(DEFAULT_FIRST_RBD_SEATS)
        promo_threshold = to_float(_get_val(curr_params, 'plfthreshold', default=promo_threshold_const), to_float(promo_threshold_const, DEFAULT_FORECAST_THRESHOLD_PROMO_AU))
        sigma_factor = to_float(_get_val(curr_params, 'sigma_factor', default=sigma_factor_const), to_float(sigma_factor_const, DEFAULT_STANDARD_DEVIATION_FACTOR))
        # Promo inventory parameters: PromoFlag_NG from Market List; others come from constants (ConstantsUtils / DB / Constants.py)
        promo_ratio_const = getattr(const_utils, 'PROMO_RATIO_NG', getattr(const_utils, 'PromoRatio_NG', DEFAULT_PROMO_RATIO_NG))
        promo_rbd_const = str(getattr(const_utils, 'PROMO_RBD_NG', getattr(const_utils, 'PromoRBD_NG', DEFAULT_PROMO_RBD_NG))).strip() or DEFAULT_PROMO_RBD_NG
        promo_first_rbd_const = getattr(const_utils, 'PROMO_FIRST_RBD_NG', getattr(const_utils, 'PromoFirstRBD_NG', DEFAULT_PROMO_FIRST_RBD_NG))

        promo_flag = 1 if is_flag_enabled(_get_val(curr_params, 'promo_flag_NG', 'promo_flag', 'PromoFlag_NG', default=DEFAULT_PROMO_FLAG_NG)) else 0
        promo_ratio = to_float(_get_val(curr_params, 'promo_ratio_NG', 'promo_ratio', 'PromoRatio_NG', 'promo_fare_factor', default=promo_ratio_const), to_float(promo_ratio_const, DEFAULT_PROMO_RATIO_NG))
        promo_rbd = str(_get_val(curr_params, 'promo_rbd_NG', 'promo_rbd', 'PromoRBD_NG', default=promo_rbd_const)).strip() or promo_rbd_const
        promo_first_rbd = to_float(_get_val(curr_params, 'promo_first_rbd_NG', 'promo_first_rbd', 'PromoFirstRBD_NG', default=promo_first_rbd_const), to_float(promo_first_rbd_const, DEFAULT_PROMO_FIRST_RBD_NG))
        cov_param = to_float(_get_val(curr_params, 'coverage_parameter', default=cov_param_const), to_float(cov_param_const, DEFAULT_COVERAGE_PARAMETER))

        cap_val = to_int(capacity, 189)
        if cap_val <= 0:
            cap_val = 189
        cb_val = to_float(current_bookings, 0.0)
        curr_rev_val = to_float(current_revenue, 0.0)

        # Step 1: Initial EMSR-b
        init_df, anchor_fare, promo_au = cls.compute_initial_emsr_b(
            flight_ladder_df, cap_val, cb_val,
            buy_up_factor=buy_up,
            sigma_factor=sigma_factor,
            promo_ratio=promo_ratio,
            promo_threshold=promo_threshold,
            promo_flag=promo_flag
        )

        # Step 2: Competition Response & Strategy Fare
        conn_fare_flag = 1 if is_flag_enabled(_get_val(curr_params, 'connection_fare_flag_NG', 'connection_fare_flag', default=0)) else 0
        add_conn_dur = to_float(_get_val(curr_params, 'additional_connection_duration_NG', 'additional_connection_duration', default=add_conn_dur_const), to_float(add_conn_dur_const, DEFAULT_ADDITIONAL_CONNECTION_DURATION))
        dir_dur = getattr(curr_params, 'flight_duration', getattr(curr_params, 'outbound_duration', None)) if curr_params else None

        # Apply Competitor Fare Recalibration per airline before filtering
        recal_market_fares = market_fares
        if market_fares is not None and recalibration_dict:
            recal_map = {}
            if isinstance(recalibration_dict, dict):
                for k, v in recalibration_dict.items():
                    k_str = str(k).strip().upper()
                    if k_str not in ['_ID', 'SECTOR', 'ID']:
                        try:
                            recal_map[k_str] = float(v)
                        except (ValueError, TypeError):
                            pass

            if recal_map:
                if isinstance(market_fares, list):
                    recal_market_fares = pd.DataFrame(market_fares)
                elif isinstance(market_fares, pd.DataFrame):
                    recal_market_fares = market_fares.copy()
                else:
                    recal_market_fares = pd.DataFrame()

                if not recal_market_fares.empty:
                    carr_col = next((c for c in ['outbound_airline_code', 'AirlineCode', 'airline_code', 'Carrier', 'carrier'] if c in recal_market_fares.columns), None)
                    fare_col = next((c for c in ['gross_fare', 'Fare', 'fare', 'TotalFare', 'total_fare', 'TOTAL', 'price', 'Total'] if c in recal_market_fares.columns), None)
                    if carr_col and fare_col:
                        recal_market_fares[fare_col] = recal_market_fares.apply(
                            lambda r: to_float(r[fare_col], 0.0) + recal_map.get(str(r[carr_col]).strip().upper(), recal_map.get('DEFAULT', 0.0))
                            if pd.notna(r[fare_col]) else r[fare_col],
                            axis=1
                        )
                    elif fare_col and 'DEFAULT' in recal_map:
                        recal_market_fares[fare_col] = pd.to_numeric(recal_market_fares[fare_col], errors='coerce').fillna(0.0) + recal_map['DEFAULT']

        # Convert market fares to AIRLINE COMPONENT before calculation
        c_min_allin, c_med_allin = cls.filter_market_fares(
            recal_market_fares, flight_std,
            time_range_hours=time_range,
            carrier_exclusion=carr_excl,
            connection_fare_flag=conn_fare_flag,
            direct_duration_mins=dir_dur,
            additional_connection_hours=add_conn_dur,
            coverage_param=cov_param
        )

        converted_market_fares = recal_market_fares
        if recal_market_fares is not None:
            if isinstance(recal_market_fares, list):
                converted_market_fares = pd.DataFrame(recal_market_fares)
            elif isinstance(recal_market_fares, pd.DataFrame):
                converted_market_fares = recal_market_fares.copy()
            else:
                converted_market_fares = pd.DataFrame()

            if not converted_market_fares.empty:
                fare_col = next((c for c in ['gross_fare', 'Fare', 'fare', 'TotalFare', 'total_fare', 'TOTAL', 'price', 'Total'] if c in converted_market_fares.columns), None)
                if fare_col and tax_helper and origin and destin:
                    converted_market_fares[fare_col] = converted_market_fares[fare_col].apply(
                        lambda f: tax_helper.allin_to_airline_component(float(f), origin, destin, route=route) if pd.notna(f) else f
                    )

        c_min_comp, c_med_comp = cls.filter_market_fares(
            converted_market_fares, flight_std,
            time_range_hours=time_range,
            carrier_exclusion=carr_excl,
            connection_fare_flag=conn_fare_flag,
            direct_duration_mins=dir_dur,
            additional_connection_hours=add_conn_dur,
            coverage_param=cov_param
        )

        # Fallback if converted_market_fares was None or tax_helper not run
        if c_min_comp is None and c_min_allin is not None:
            if tax_helper and origin and destin:
                c_min_comp = tax_helper.allin_to_airline_component(c_min_allin, origin, destin, route=route)
            else:
                c_min_comp = c_min_allin
        if c_med_comp is None and c_med_allin is not None:
            if tax_helper and origin and destin:
                c_med_comp = tax_helper.allin_to_airline_component(c_med_allin, origin, destin, route=route)
            else:
                c_med_comp = c_med_allin
        if c_min_allin is None and c_min_comp is not None:
            if tax_helper and origin and destin:
                c_min_allin = tax_helper.airline_component_to_allin(c_min_comp, origin, destin, route=route)
            else:
                c_min_allin = c_min_comp
        if c_med_allin is None and c_med_comp is not None:
            if tax_helper and origin and destin:
                c_med_allin = tax_helper.airline_component_to_allin(c_med_comp, origin, destin, route=route)
            else:
                c_med_allin = c_med_comp

        # Resolve hardanchor
        # Only evaluate hardanchor if explicitly configured in Market List (hardanchor/price_cap)
        # or passed via resolved hard_anchor_fare.
        raw_ha = _get_val(curr_params, 'hardanchor', 'hardAnchor', 'price_cap', default=None)
        if raw_ha in ('NA', 'NAN', 'NONE', 'None', '', None, CONST_NA):
            raw_ha = None
        explicit_ha = _get_val(curr_params, 'hard_anchor_fare', 'hardAnchorFare', default=None)
        if explicit_ha in ('NA', 'NAN', 'NONE', 'None', '', None, CONST_NA):
            explicit_ha = None

        plf_thresh = to_float(_get_val(curr_params, 'plfthreshold', 'plfThreshold', default=95.0), 95.0)
        curr_plf = (cb_val / cap_val) * 100.0 if cap_val > 0 else 0.0
        is_plf_below_thresh = (curr_plf < plf_thresh)

        hardanchor = None
        if is_plf_below_thresh and (raw_ha is not None or explicit_ha is not None):
            resolved_ha_num = to_float(explicit_ha, None)
            if resolved_ha_num is not None and resolved_ha_num > 0:
                hardanchor = resolved_ha_num
            elif raw_ha is not None:
                raw_ha_str = str(raw_ha).strip().upper()
                if raw_ha_str in ['MIN', 'MAX', 'MINO', 'MAXO', 'MED', 'MEDC', 'MIN2', 'MIN2C']:
                    ha_mkt_val = None
                    if recal_market_fares is not None and not (isinstance(recal_market_fares, pd.DataFrame) and recal_market_fares.empty):
                        fare_col = next((c for c in ['gross_fare', 'Fare', 'fare', 'TotalFare', 'total_fare', 'TOTAL', 'price', 'Total'] if c in recal_market_fares.columns), None)
                        if fare_col:
                            fares_s = pd.to_numeric(recal_market_fares[fare_col], errors='coerce').dropna()
                            fares_s = fares_s[fares_s > 0]
                            if not fares_s.empty:
                                if 'MIN' in raw_ha_str:
                                    ha_mkt_val = float(fares_s.min())
                                elif 'MAX' in raw_ha_str:
                                    ha_mkt_val = float(fares_s.max())
                                elif 'MED' in raw_ha_str:
                                    ha_mkt_val = float(fares_s.median())
                    elif c_min_allin is not None:
                        if 'MIN' in raw_ha_str:
                            ha_mkt_val = float(c_min_allin)
                        elif 'MED' in raw_ha_str and c_med_allin is not None:
                            ha_mkt_val = float(c_med_allin)

                    if ha_mkt_val is not None and ha_mkt_val > 0:
                        ha_offset = to_int(_get_val(curr_params, 'fare_offset', 'fareOffset', 'hardoffset', default=0), 0)
                        if filed_levels:
                            sorted_fl_asc = sorted(filed_levels, key=lambda x: x['allin'])
                            cand_idx = None
                            for idx, fl in enumerate(sorted_fl_asc):
                                if fl['allin'] >= ha_mkt_val - 1.0:
                                    cand_idx = idx
                                    break
                            if cand_idx is not None:
                                target_idx = min(len(sorted_fl_asc) - 1, max(0, cand_idx + ha_offset))
                                hardanchor = float(sorted_fl_asc[target_idx]['allin'])
                            else:
                                hardanchor = float(ha_mkt_val)
                        else:
                            hardanchor = float(ha_mkt_val)
                else:
                    num_ha = to_float(raw_ha, None)
                    if num_ha is not None and num_ha > 0:
                        hardanchor = num_ha

        rule_applied = "N/A"
        strategy_fare = anchor_fare
        response_fare = anchor_fare

        # Rule: In cases where oal_b2c_min is less than anchor fare, or market fares not found:
        # we don't do AU recalculate and demand redistribution and make AUs below anchor fare 0.
        is_market_found = (c_min_comp is not None and not (isinstance(c_min_comp, float) and np.isnan(c_min_comp)))
        is_oal_below_anchor = (is_market_found and c_min_comp < anchor_fare)

        if not is_market_found or is_oal_below_anchor or comp_flag != 1:
            # Case 1: Bypass recalculation and demand redistribution
            if not is_market_found:
                rule_applied = "No Competition Response (Market Fares Not Found)"
            elif is_oal_below_anchor:
                rule_applied = "No Competition Response (oal_b2c_min < anchor_fare)"
            else:
                rule_applied = "N/A"
            strategy_fare = anchor_fare
            response_fare = anchor_fare

            redistrib_df = init_df.copy()
            redistrib_df['P_eq'] = np.nan
            redistrib_df['mu_bar_2'] = redistrib_df['mu_bar']
            redistrib_df['mu_2'] = redistrib_df['mu']
            if 'sigma' in redistrib_df.columns:
                redistrib_df['sigma_2'] = redistrib_df['sigma'].copy()
                redistrib_df['sigma_3'] = redistrib_df['sigma'].copy()
            else:
                redistrib_df['sigma_2'] = sigma_factor * redistrib_df['mu_2']
                redistrib_df['sigma_3'] = redistrib_df['sigma_2']
            redistrib_df['sigma_bar_3'] = redistrib_df['sigma_bar']

            # Initial AU retained; make AUs below anchor fare 0
            fares_init = [to_float(f, 0.0) for f in redistrib_df['fare'].tolist()]
            au_init = redistrib_df['AU'].copy().tolist()
            au_rev = au_init.copy()
            for j, f in enumerate(fares_init):
                if f < anchor_fare - 1e-4:
                    au_init[j] = 0.0
                    au_rev[j] = 0.0

            # Apply backstop floor
            bs_val = None
            if backstop is not None and backstop > 0:
                bs_val = backstop
                if tax_helper and origin and destin:
                    try:
                        bs_val = tax_helper.allin_to_airline_component(float(backstop), origin, destin, route=route)
                    except Exception:
                        pass
                for i, f in enumerate(fares_init):
                    if f < bs_val:
                        au_rev[i] = 0.0

            # 1. FirstRBD logic (20-seat threshold) applied at opening bucket
            open_idx = None
            for i in range(len(fares_init) - 1, -1, -1):
                if au_rev[i] > 0:
                    open_idx = i
                    break

            first_rbd_seats = to_float(first_rbd, DEFAULT_FIRST_RBD_SEATS)
            if first_rbd_seats <= 0:
                first_rbd_seats = float(DEFAULT_FIRST_RBD_SEATS)

            if open_idx is not None:
                au_rev[open_idx] = min(au_rev[open_idx], float(cb_val + first_rbd_seats))

            # 2. Hardanchor with PLF threshold: applied after FirstRBD logic
            if hardanchor is not None and hardanchor > 0:
                ha_val = hardanchor
                if tax_helper and origin and destin:
                    try:
                        ha_val = tax_helper.allin_to_airline_component(float(hardanchor), origin, destin, route=route)
                    except Exception:
                        pass
                lower_candidates = [f for f in fares_init if f <= ha_val]
                h_ladder = lower_candidates[0] if lower_candidates else fares_init[-1]
                ha_idx = fares_init.index(h_ladder)

                plf_th = to_float(_get_val(curr_params, 'plfthreshold', 'plfThreshold', default=promo_threshold_const), to_float(promo_threshold_const, DEFAULT_FORECAST_THRESHOLD_PROMO_AU))
                plf_pct = (plf_th / 100.0) if plf_th > 1.5 else plf_th
                plf_target = float(round(plf_pct * cap_val, 2))
                plf_target = min(float(cap_val), max(0.0, plf_target))

                if au_rev[ha_idx] < plf_target:
                    au_rev[ha_idx] = plf_target

                for i in range(ha_idx):
                    if au_rev[i] < au_rev[ha_idx]:
                        au_rev[i] = plf_target

                if bs_val is not None and bs_val > 0:
                    for i, f in enumerate(fares_init):
                        if f < bs_val:
                            au_rev[i] = 0.0

            redistrib_df['AU_2'] = au_init
            redistrib_df['AU_Revised'] = au_rev
            redistrib_df.attrs['p_min_old'] = anchor_fare
            redistrib_df.attrs['anchor_fare'] = float(anchor_fare)
            if promo_flag == 1:
                promo_thresh = float(cb_val + to_float(promo_first_rbd, DEFAULT_PROMO_FIRST_RBD_NG))
                if float(promo_au) > promo_thresh:
                    promo_revised = promo_thresh
                else:
                    promo_revised = float(promo_au)
            else:
                promo_revised = 0.0
            redistrib_df.attrs['promo_info'] = {
                'AU': float(promo_au),
                'AU_2': float(promo_au),
                'AU_Revised': float(promo_revised),
                'fare': float(promo_ratio) * float(strategy_fare) if strategy_fare else 0.0
            }
        else:
            # Case 2: Competition response triggers (oal_b2c_min >= anchor_fare)
            pred_pickup_init = sum(init_df['mu'])
            plf_pct = ((cb_val + pred_pickup_init) / float(cap_val)) * 100.0 if cap_val > 0 else 0.0

            ndo_band = cls.get_ndo_band(ndo, db_helper=db_helper)
            plf_band = cls.get_plf_band(plf_pct, db_helper=db_helper)

            grid_doc = None
            if db_helper and hasattr(db_helper, 'getCompResponseGrid'):
                grid_doc = db_helper.getCompResponseGrid(grid_name, ndo_band, plf_band)

            if grid_doc:
                rule_applied = grid_doc.get("response_rule", grid_doc.get("rule", "C"))
                criteria = grid_doc.get("criteria", "Min")
            else:
                rule_applied = "Min (1.2A, 0.8C)" if plf_band == 0 else ("Min (1.3A, 0.9C)" if plf_band == 1 else "C")
                criteria = "Min"

            # 1. Fare obtained from grid calculation
            rule_calc_resp = cls.evaluate_grid_response(
                rule_applied, anchor_fare, c_min_comp, c_med_comp,
                tolerance=None
            )

            # 2. Tolerance in the grid
            grid_tol = None
            if grid_doc:
                grid_tol = grid_doc.get("tolerance", grid_doc.get("tolernace"))

            c_for_thresh = c_med_comp if (c_med_comp is not None and re.search(r'\bmed\b', str(rule_applied), re.IGNORECASE)) else c_min_comp
            grid_tol_fare = 0.0
            if grid_tol is not None and pd.notna(grid_tol) and str(grid_tol).strip().upper() not in ['', 'NA', 'NAN']:
                try:
                    gt = float(grid_tol)
                    if gt > 100.0:
                        grid_tol_fare = gt
                    elif gt > 1.5:
                        if c_for_thresh is not None:
                            grid_tol_fare = (gt / 100.0) * float(c_for_thresh)
                    elif gt > 0.0:
                        if c_for_thresh is not None:
                            grid_tol_fare = gt * float(c_for_thresh)
                except (ValueError, TypeError):
                    pass

            # 3. Tolerance in ML input
            ml_tol_fare = 0.0
            ml_tol = _get_val(curr_params, 'tolerance', 'B2CTolerance', 'b2c_tolerance', 'b2cTolerance', default=None)
            if ml_tol is not None and pd.notna(ml_tol) and str(ml_tol).strip().upper() not in ['', 'NA', 'NAN']:
                try:
                    mt = float(ml_tol)
                    if mt > 100.0:
                        if tax_helper and origin and destin:
                            try:
                                ml_tol_fare = float(tax_helper.allin_to_airline_component(mt, origin, destin, route=route))
                            except Exception:
                                ml_tol_fare = mt
                        else:
                            ml_tol_fare = mt
                    elif mt > 1.5:
                        if c_for_thresh is not None:
                            ml_tol_fare = (mt / 100.0) * float(c_for_thresh)
                    elif mt > 0.0:
                        if c_for_thresh is not None:
                            ml_tol_fare = mt * float(c_for_thresh)
                except (ValueError, TypeError):
                    pass

            # Higher fare considered as tolerance
            chosen_tolerance_fare = max(grid_tol_fare, ml_tol_fare)

            # Max of tolerance fare and grid calculation fare is Strategy fare
            response_fare = max(rule_calc_resp, chosen_tolerance_fare)
            strategy_fare = cls.determine_strategy_fare(response_fare, init_df)

            pmin_thresh_factor = to_float(_get_val(curr_params, 'pmin_threshold_factor_NG', 'pmin_threshold_factor', default=pmin_thresh_const), to_float(pmin_thresh_const, DEFAULT_PMIN_THRESHOLD_FACTOR_NG))
            plf_thresh = to_float(_get_val(curr_params, 'plfthreshold', 'plfThreshold', default=promo_threshold_const), to_float(promo_threshold_const, DEFAULT_FORECAST_THRESHOLD_PROMO_AU))

            redistrib_df = cls.redistribute_demand_and_compute_revised_au(
                init_df, anchor_fare, strategy_fare, cap_val, cb_val,
                buy_up_factor=buy_up,
                sigma_factor=sigma_factor,
                first_rbd=first_rbd,
                promo_ratio=promo_ratio,
                backstop=backstop,
                hardanchor=hardanchor,
                promo_au=promo_au,
                promo_flag=promo_flag,
                promo_first_rbd=promo_first_rbd,
                promo_threshold=promo_threshold,
                pmin_threshold_factor=pmin_thresh_factor,
                plf_threshold=plf_thresh,
                tax_helper=tax_helper,
                origin=origin,
                destin=destin,
                route=route
            )

        # Step 4: Closed-Form Revenue Forecasting
        pred_rev, tot_er, pickup_rev, flown_rev, af_fc, lf_fc_rev, final_df = cls.compute_closed_form_revenue_forecast(
            redistrib_df, cap_val, cb_val, curr_rev_val,
            au_col='AU_2',
            anchor_fare=anchor_fare
        )

        # Consecutive step-up cap across the AU_Revised bucket series. Applying
        # it once here (before Step 5 reads AU_Revised) keeps the inventory CSV
        # and the filed B2C RBD mapping consistent with the same capped series.
        _capped_au_revised = cls._apply_stepup_cap(
            [to_float(a, 0.0) for a in final_df['AU_Revised'].tolist()],
            nextgen_otherrbd,
        )
        final_df['AU_Revised'] = _capped_au_revised

        # Step 5: 51 Fare Levels (airline component) Mapping & Opening Fare
        fc_fares = [to_float(f, 0.0) for f in final_df['fare'].tolist()]
        fc_aus = [to_float(a, 0.0) for a in final_df['AU_Revised'].tolist()]
        eff_response_fare = response_fare if response_fare is not None else anchor_fare

        # Determine backstop and hardanchor values in airline component and all-in
        bs_comp = None
        bs_allin = None
        if backstop is not None and backstop > 0:
            if tax_helper and origin and destin:
                try:
                    bs_comp = tax_helper.allin_to_airline_component(float(backstop), origin, destin, route=route)
                    bs_allin = float(backstop)
                except Exception:
                    bs_comp = float(backstop)
                    bs_allin = float(backstop)
            else:
                bs_comp = float(backstop)
                bs_allin = float(backstop)

        ha_comp = None
        ha_allin = None
        if hardanchor is not None and hardanchor > 0:
            if tax_helper and origin and destin:
                try:
                    ha_comp = tax_helper.allin_to_airline_component(float(hardanchor), origin, destin, route=route)
                    ha_allin = float(hardanchor)
                except Exception:
                    ha_comp = float(hardanchor)
                    ha_allin = float(hardanchor)
            else:
                ha_comp = float(hardanchor)
                ha_allin = float(hardanchor)

        # The effective floor on opening fare is max of backstop and hardanchor
        eff_floor_comp = max([v for v in [bs_comp, ha_comp] if v is not None], default=None)
        eff_floor_allin = max([v for v in [bs_allin, ha_allin] if v is not None], default=None)

        # Effective target response fare considers floor (backstop / hardanchor)
        eff_target_fare = eff_response_fare
        if eff_floor_comp is not None:
            eff_target_fare = max(eff_target_fare, eff_floor_comp)

        # Only forecast buckets with AU_Revised > 0 are considered open
        open_fc = [(f, a) for f, a in zip(fc_fares, fc_aus) if a > 0.0]

        # Nearest fare among open forecast fares below effective target response fare
        if open_fc:
            fc_below = [(f, a) for f, a in open_fc if f <= eff_target_fare + 1e-4]
            if fc_below:
                target_fc_fare, target_fc_au = fc_below[0]
            else:
                target_fc_fare, target_fc_au = open_fc[-1]
        else:
            fc_below = [(f, a) for f, a in zip(fc_fares, fc_aus) if f <= eff_target_fare + 1e-4]
            if fc_below:
                target_fc_fare, target_fc_au = fc_below[0]
            else:
                target_fc_fare, target_fc_au = fc_fares[-1], fc_aus[-1]

        # Find nearest lower among the 51 fare levels (airline component)
        open_filed = None
        if filed_levels:
            cand_51 = [fl for fl in filed_levels if fl['fare'] <= eff_response_fare + 1e-4]
            if cand_51:
                open_filed = cand_51[0]
            else:
                open_filed = filed_levels[-1]

            # Enforce floor on opening fare if floor is higher
            if eff_floor_comp is not None and open_filed['fare'] < eff_floor_comp:
                floor_cand = [fl for fl in filed_levels if fl['fare'] >= eff_floor_comp - 1e-4]
                open_filed = floor_cand[-1] if floor_cand else open_filed

            opening_fare = float(open_filed['fare'])
            opening_rbd = open_filed['RBD']
        else:
            opening_fare = float(target_fc_fare)
            if eff_floor_comp is not None and opening_fare < eff_floor_comp:
                opening_fare = eff_floor_comp
            if open_fc:
                open_cand = final_df[final_df['fare'] >= open_fc[-1][0] - 1e-4]
                opening_rbd = str(open_cand.iloc[-1]['FareBucket']) if not open_cand.empty else str(final_df['FareBucket'].iloc[-1])
            else:
                opening_rbd = str(final_df['FareBucket'].iloc[-1])

        opening_fare_allin = tax_helper.airline_component_to_allin(opening_fare, origin, destin, route=route) if tax_helper and origin and destin else opening_fare
        if eff_floor_allin is not None and opening_fare_allin < eff_floor_allin:
            opening_fare_allin = eff_floor_allin

        # Allocate inventory for each of the 51 fare levels:
        # QP fare RBDs below opening fare or below floor get AU=0.
        # The inventory at nearest forecast fare below competition response is assigned to opening 51 level.
        filed_51_au_mapping = {}
        if filed_levels:
            min_open_au = open_fc[-1][1] if open_fc else 0.0
            for fl in filed_levels:
                f_comp = fl['fare']
                r_code = fl['RBD']
                if f_comp < opening_fare - 1e-4 or (eff_floor_comp is not None and f_comp < eff_floor_comp - 1e-4):
                    alloc_au = 0.0
                else:
                    matched_au = None
                    if open_fc:
                        for fc_f, au_val in open_fc:
                            if f_comp >= fc_f - 1e-4:
                                matched_au = au_val
                                break
                        if matched_au is None:
                            matched_au = min_open_au
                    else:
                        for fc_f, au_val in zip(fc_fares, fc_aus):
                            if f_comp >= fc_f - 1e-4:
                                matched_au = au_val
                                break
                    alloc_au = matched_au if matched_au is not None else 0.0
                filed_51_au_mapping[r_code] = alloc_au

            # Assign inventory at nearest forecast fare below competition response to opening RBD
            opening_allocated_au = max(to_float(target_fc_au, 0.0), min_open_au)
            filed_51_au_mapping[opening_rbd] = opening_allocated_au

            # Enforce non-decreasing monotonicity (higher fare cannot have lower AU)
            for i in range(len(filed_levels) - 2, -1, -1):
                high_rbd = filed_levels[i]['RBD']
                low_rbd = filed_levels[i+1]['RBD']
                if filed_levels[i]['fare'] >= opening_fare - 1e-4:
                    filed_51_au_mapping[high_rbd] = max(filed_51_au_mapping[high_rbd], filed_51_au_mapping[low_rbd])

        # While mapping, follow the same consecutive +cap rule: the AU assigned
        # to each successive filed B2C RBD may exceed the previous RBD's AU by
        # at most NEXTGEN_OTHERRBD. The last RBD of the ladder allocates full
        # capacity. This is applied on top of the existing Step-5 mapping (the
        # opening RBD keeps its opening allocation; closed RBDs stay zero).
        nextgen_otherrbd_bypass = bool(filed_levels) and nextgen_otherrbd > 0
        if nextgen_otherrbd_bypass:
            cls._apply_filed_rbd_stepup_cap(
                filed_51_au_mapping, filed_levels, nextgen_otherrbd, cap_val
            )
        filed_51_au_mapping_out = filed_51_au_mapping

        opening_au = filed_51_au_mapping.get(opening_rbd, target_fc_au) if filed_levels else target_fc_au
        seats_on_opening = float(int(round(opening_au)))

        # Add 'opening fare' and 'opening fare_All_in' as columns in inventory file
        final_df['opening fare'] = float(opening_fare)
        final_df['opening fare_All_in'] = float(opening_fare_allin)

        # When promo_flag == 1, append Promo row to final_df
        if promo_flag == 1:
            promo_info = redistrib_df.attrs.get('promo_info', {})
            promo_row = final_df.iloc[-1].to_dict()
            promo_row['FareBucket'] = 'Promo'
            promo_row['Promo_Flag'] = 1
            promo_row['fare'] = to_float(promo_info.get('fare'), to_float(promo_ratio) * to_float(strategy_fare))
            promo_row['mu'] = 0.0
            promo_row['mu_2'] = 0.0
            promo_row['mu_bar_orig'] = 0.0
            promo_row['mu_bar_2'] = 0.0
            promo_row['AU'] = to_float(promo_info.get('AU', 0.0), 0.0)
            promo_row['AU_2'] = to_float(promo_info.get('AU_2', 0.0), 0.0)
            promo_row['AU_Revised'] = to_float(promo_info.get('AU_Revised', 0.0), 0.0)
            promo_row['P_eq'] = np.nan
            promo_row['Expected_Bookings'] = 0.0
            promo_row['Expected_Revenue'] = 0.0
            promo_row['Total_Expected_Revenue'] = to_float(tot_er, 0.0)
            promo_row['Predicted_Revenue'] = to_float(pred_rev, 0.0)
            promo_row['A'] = 0.0
            promo_row['A_cap'] = 0.0
            promo_row['opening fare'] = opening_fare
            final_df = pd.concat([final_df, pd.DataFrame([promo_row])], ignore_index=True)

        # Sourcing pre-calculated LF_Forecast_Revised and AF_Forecast from daily_next_gen table (add_model_inventory_ng)
        daily_lf = None
        daily_af = None
        if db_helper is not None:
            try:
                sec_val = f"{origin}{destin}" if origin and destin else None
                flt_val = _get_val(curr_params, 'flight_number', 'flt_num', 'flight', default=None)
                date_val = _get_val(curr_params, 'dep_date', 'departure_date', 'currFltDate', 'date', default=None)
                inv_ng = db_helper.get_additive_inventory_ng(sector=sec_val, date=date_val, flight=flt_val)
                if inv_ng is not None and not inv_ng.empty:
                    if 'LF_Forecast_Revised' in inv_ng.columns and pd.notna(inv_ng['LF_Forecast_Revised'].iloc[0]):
                        daily_lf = to_float(inv_ng['LF_Forecast_Revised'].iloc[0], None)
                        # LF_Forecast_Revised is a 0-1 fraction; scale to 0-100%.
                        # PLF can exceed 100% (capped ~110%), so a fraction may be
                        # > 1.0 (e.g. 1.01 == 101%). Treat values <= 2.0 as
                        # fractions; higher values are already percentages.
                        if daily_lf is not None and 0.0 < daily_lf <= 2.0:
                            daily_lf = daily_lf * 100.0
                    if 'AF_Forecast' in inv_ng.columns and pd.notna(inv_ng['AF_Forecast'].iloc[0]):
                        daily_af = to_float(inv_ng['AF_Forecast'].iloc[0], None)
            except Exception as e:
                logger.warning(f"Error reading pre-calculated forecast from add_model_inventory_ng: {e}")

        # Convert airline component fares to ALL-IN for NextGen output summary and inventory optimization
        if open_filed and 'allin' in open_filed and open_filed['allin']:
            opening_fare_allin = float(open_filed['allin'])
        elif tax_helper and origin and destin:
            opening_fare_allin = tax_helper.airline_component_to_allin(opening_fare, origin, destin, route=route)
        else:
            opening_fare_allin = opening_fare
        # Strategy fare for viewing/display: show the fare from grid calculation (eff_response_fare), not ladder-mapped
        display_strategy_fare = eff_response_fare
        display_strategy_fare_allin = tax_helper.airline_component_to_allin(display_strategy_fare, origin, destin, route=route) if tax_helper and origin and destin else display_strategy_fare
        anchor_fare_allin = tax_helper.airline_component_to_allin(anchor_fare, origin, destin, route=route) if tax_helper and origin and destin else anchor_fare

        # Populate fare columns with both Airline Component and All_in values on final_df
        final_df['opening fare'] = float(opening_fare)
        final_df['opening fare_All_in'] = float(opening_fare_allin)
        if tax_helper and origin and destin:
            final_df['fare_All_in'] = final_df['fare'].apply(
                lambda f: tax_helper.airline_component_to_allin(float(f), origin, destin, route=route) if pd.notna(f) else f
            )
        else:
            final_df['fare_All_in'] = final_df['fare']
        final_df['anchor_fare'] = float(anchor_fare)
        final_df['anchor_fare_All_in'] = float(anchor_fare_allin)
        final_df['strategy_fare'] = float(display_strategy_fare)
        final_df['strategy_fare_All_in'] = float(display_strategy_fare_allin)
        final_df['oal_b2c_min'] = to_float(c_min_comp, 0.0)
        final_df['oal_b2c_min_All_in'] = to_float(c_min_allin, 0.0)

        summary = {
            'NG_Historical_current_booked': cb_val,
            'NG_LF_Forecast': daily_lf if daily_lf is not None else to_float(lf_fc_rev * 100.0, 0.0),
            'NG_AF_Forecast': int(round(daily_af)) if daily_af is not None else int(round(to_float(af_fc, 0.0))),
            'NG_Opening_fare': int(round(to_float(opening_fare_allin, 0.0))),
            'NG_FirstRBD': to_float(seats_on_opening, 0.0),
            'NG_Strategy_fare': int(round(to_float(display_strategy_fare_allin, 0.0))),
            'NG_Anchor_fare': int(round(to_float(anchor_fare_allin, 0.0))),
            'Strategy_fare': int(round(to_float(display_strategy_fare_allin, 0.0))),
            'Anchor_fare': int(round(to_float(anchor_fare_allin, 0.0))),
            'Pmin_Old': to_float(redistrib_df.attrs.get('p_min_old', anchor_fare), 0.0),
            'Current_Revenue': curr_rev_val,
            'Total_Expected_Revenue': to_float(tot_er, 0.0),
            'Predicted_Revenue': to_float(pred_rev, 0.0),
            'Predicted_Pickup_Revised': to_float(pickup_rev, 0.0),
            'Predicted_Flown_Revised': to_float(flown_rev, 0.0),
            'MarketFare_Selected': int(round(to_float(c_min_allin, 0.0))) if c_min_allin is not None else None,
            'MarketFare_Min': int(round(to_float(c_min_allin, 0.0))) if c_min_allin is not None else None,
            'MarketFare_Max': int(round(to_float(c_med_allin, 0.0))) if c_med_allin is not None else None,
            'MarketFare_Min_AllIn': int(round(to_float(c_min_allin, 0.0))) if c_min_allin is not None else None,
            'MarketFare_Max_AllIn': int(round(to_float(c_med_allin, 0.0))) if c_med_allin is not None else None,
            'MarketFare_Min_Comp': int(round(to_float(c_min_comp, 0.0))) if c_min_comp is not None else None,
            'Grid_Name': grid_name,
            'Rule_Applied': rule_applied,
            'nextgen_flag': 1 if is_flag_enabled(_get_val(curr_params, 'nextgen_flag_NG', 'nextgen_flag', default=0)) else 0,
            'competition_response_flag': 1 if is_flag_enabled(comp_flag) else 0,
            'buy_up_factor': buy_up,
            'opening_fare_airline_comp': to_float(opening_fare, 0.0),
            'strategy_fare_airline_comp': to_float(display_strategy_fare, 0.0),
            'anchor_fare_airline_comp': to_float(anchor_fare, 0.0),
            'opening_fare_allin': to_float(opening_fare_allin, 0.0),
            'strategy_fare_allin': to_float(display_strategy_fare_allin, 0.0),
            'anchor_fare_allin': to_float(anchor_fare_allin, 0.0),
            'oal_b2c_min_airline_comp': to_float(c_min_comp, None) if c_min_comp is not None else None,
            'oal_b2c_min_allin': to_float(c_min_allin, None) if c_min_allin is not None else None,
            'opening_rbd': opening_rbd,
            'filed_51_au_mapping': filed_51_au_mapping_out,
            'nextgen_otherrbd': float(nextgen_otherrbd),
            'nextgen_otherrbd_bypass': nextgen_otherrbd_bypass
        }

        return final_df, summary
