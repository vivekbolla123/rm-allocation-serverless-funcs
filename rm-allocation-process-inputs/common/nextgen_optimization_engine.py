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
    DEFAULT_PROMO_FLAG_NG,
    DEFAULT_PROMO_RBD_NG,
    DEFAULT_PROMO_FIRST_RBD_NG,
    DEFAULT_PROMO_RATIO_NG
)

logger = logging.getLogger(__name__)


def to_float(val, default=0.0):
    if val is None:
        return default
    s = str(val).strip().upper()
    if s in ['', 'NA', 'NAN', 'NONE', 'NULL']:
        return default
    try:
        return float(val)
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
        Dynamically determines the PLF band ID and threshold_factor for a given PLF percentage.
        Queries comp_response_plf_bands via db_helper if available, or matches range.
        Returns: (band_id, threshold_factor)
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
            threshold_factor = to_float(band_doc.get("threshold_factor", 0.0), 0.0)
            return band_id, threshold_factor

        if db_helper and hasattr(db_helper, 'getCompResponsePlfBand'):
            band = db_helper.getCompResponsePlfBand(val)
            if band is not None:
                tf = 0.0
                if hasattr(db_helper, 'getCompResponsePlfBandById'):
                    b_doc = db_helper.getCompResponsePlfBandById(band)
                    if b_doc:
                        tf = to_float(b_doc.get("threshold_factor", 0.0), 0.0)
                return band, tf

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

        return b_id, 0.0

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

    @classmethod
    def compute_initial_emsr_b(
        cls,
        ladder_df,
        capacity,
        current_bookings,
        buy_up_factor=0.0,
        sigma_factor=0.25,
        promo_ratio=0.80,
        promo_threshold=95.0,
        promo_flag=0,
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
        sig_fact = to_float(sigma_factor, 0.25)

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
        time_range_hours=2.0,
        carrier_exclusion=None,
        connection_fare_flag=0,
        direct_duration_mins=None,
        additional_connection_hours=4.0,
        coverage_param=3.0
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
        threshold_factor=None
    ):
        """
        Phase 2b: Dynamically parses and evaluates ANY Competition Response Grid formula
        involving Anchor Fare (A) and Competitor Fare (C).
        Supports:
          - 'Min (1.2A, 0.8C)', 'Min (1.3A, 0.9C)', 'Min (1.5A, C)', 'Min (2A, C)', 'Min (xA, yC)'
          - 'Max (xA, yC)', 'C', '0.95C', 'A', '1.1A', etc.
        Also applies threshold_factor floor if provided:
          final_response = max(threshold_factor * C, rule_result)
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

        # Apply threshold_factor: max(threshold_factor * C, whatever response rule comes from grid)
        if threshold_factor is not None:
            tf = to_float(threshold_factor, 0.0)
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
        promo_ratio=0.80,
        backstop=None,
        hardanchor=None,
        promo_au=0,
        promo_flag=0,
        promo_first_rbd=10,
        promo_threshold=95.0,
        pmin_threshold_factor=0.1,
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
        sig_fact = to_float(sigma_factor, 0.25)
        first_rbd_seats = to_float(first_rbd, 20.0)

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
            au_rev[anchor_idx] = min(au_rev[anchor_idx], cb + first_rbd_seats)
            for j in range(anchor_idx + 1, n):
                au_rev[j] = 0.0

            # Apply backstop floor
            if b_val is not None and b_val > 0:
                for i, f in enumerate(fares):
                    if f < b_val:
                        au_rev[i] = 0.0

            # Apply hardanchor cap: make AU equal to capacity starting from the nearest fare lower than or equal to hardanchor
            if h_val is not None and h_val > 0:
                lower_candidates = [f for f in fares if f <= h_val]
                h_ladder = lower_candidates[0] if lower_candidates else fares[-1]
                for i, f in enumerate(fares):
                    if f >= h_ladder:
                        if b_val is not None and b_val > 0 and f < b_val:
                            continue
                        au_rev[i] = float(cap)

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
        threshold = to_float(pmin_threshold_factor, 0.1) * float(cap)
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

        # AU_revised
        au_revised = au_2_list.copy()
        au_revised[strat_idx] = min(au_revised[strat_idx], float(cb + first_rbd_seats))

        # Backstop
        if b_val is not None and b_val > 0:
            for i, f in enumerate(fares):
                if f < b_val:
                    au_revised[i] = 0.0

        # Hardanchor: make AU equal to capacity starting from the nearest fare lower than or equal to hardanchor
        if h_val is not None and h_val > 0:
            lower_candidates = [f for f in fares if f <= h_val]
            h_ladder = lower_candidates[0] if lower_candidates else fares[-1]
            for i, f in enumerate(fares):
                if f >= h_ladder:
                    if b_val is not None and b_val > 0 and f < b_val:
                        continue
                    au_revised[i] = float(cap)

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

        raw_buy_up = to_float(_get_val(curr_params, 'buy_up_factor_NG', 'buy_up_factor', default=0.0), 0.0)
        buy_up = (raw_buy_up / 100.0) if raw_buy_up > 1.0 else raw_buy_up
        comp_flag = 1 if is_flag_enabled(_get_val(curr_params, 'competition_response_flag_NG', 'competition_response_flag', default=1)) else 0
        time_range = to_float(_get_val(curr_params, 'time_range', default=2.0), 2.0)
        carr_excl = _get_val(curr_params, 'carr_exclusion_b2c', 'CarrExlusionB2C', 'carrier_exclusion', default=None)
        grid_name = str(_get_val(curr_params, 'CompetitionResponseGrid_NG', 'grid_name', default='DEFAULT')).strip() or 'DEFAULT'
        raw_bs = _get_val(curr_params, 'b2c_backstop_final', 'b2c_backstop', 'B2CBackstop', 'price_floor', default=None)
        backstop = to_float(raw_bs, None)
        if backstop is not None and backstop <= 0:
            backstop = None

        raw_ha = _get_val(curr_params, 'hardanchor', 'hardAnchor', 'price_cap', default=None)
        if raw_ha is not None and str(raw_ha).strip().upper() in ['MIN', 'MAX', 'MED', '**', '', 'NONE', 'NAN', 'NA']:
            hardanchor = None
        else:
            hardanchor = to_float(raw_ha, None)
            if hardanchor is not None and hardanchor <= 0:
                hardanchor = None
        ndo = to_int(_get_val(curr_params, 'ndocounter', 'NDO', default=0), 0)
        first_rbd = to_float(_get_val(curr_params, 'first_rbd_alloc', default=20), 20.0)
        promo_threshold = to_float(_get_val(curr_params, 'plfthreshold', default=95.0), 95.0)
        sigma_factor = to_float(_get_val(curr_params, 'sigma_factor', default=0.25), 0.25)
        # Promo inventory parameters: PromoFlag_NG from Market List; others come from constants (ConstantsUtils / DB / Constants.py)
        const_utils = getattr(db_helper, 'constants_utils', getattr(db_helper, 'constantsUtils', None))
        promo_ratio_const = getattr(const_utils, 'PROMO_RATIO_NG', getattr(const_utils, 'PromoRatio_NG', DEFAULT_PROMO_RATIO_NG))
        promo_rbd_const = str(getattr(const_utils, 'PROMO_RBD_NG', getattr(const_utils, 'PromoRBD_NG', DEFAULT_PROMO_RBD_NG))).strip() or DEFAULT_PROMO_RBD_NG
        promo_first_rbd_const = getattr(const_utils, 'PROMO_FIRST_RBD_NG', getattr(const_utils, 'PromoFirstRBD_NG', DEFAULT_PROMO_FIRST_RBD_NG))

        promo_flag = 1 if is_flag_enabled(_get_val(curr_params, 'promo_flag_NG', 'promo_flag', 'PromoFlag_NG', default=DEFAULT_PROMO_FLAG_NG)) else 0
        promo_ratio = to_float(_get_val(curr_params, 'promo_ratio_NG', 'promo_ratio', 'PromoRatio_NG', 'promo_fare_factor', default=promo_ratio_const), to_float(promo_ratio_const, DEFAULT_PROMO_RATIO_NG))
        promo_rbd = str(_get_val(curr_params, 'promo_rbd_NG', 'promo_rbd', 'PromoRBD_NG', default=promo_rbd_const)).strip() or promo_rbd_const
        promo_first_rbd = to_float(_get_val(curr_params, 'promo_first_rbd_NG', 'promo_first_rbd', 'PromoFirstRBD_NG', default=promo_first_rbd_const), to_float(promo_first_rbd_const, DEFAULT_PROMO_FIRST_RBD_NG))
        cov_param = to_float(_get_val(curr_params, 'coverage_parameter', default=3.0), 3.0)

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
        add_conn_dur = to_float(_get_val(curr_params, 'additional_connection_duration_NG', 'additional_connection_duration', default=4.0), 4.0)
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

        rule_applied = "N/A"
        strategy_fare = anchor_fare
        response_fare = anchor_fare
        threshold_factor = 0.0

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
            anchor_idx = 0
            for idx, f in enumerate(fares_init):
                if f <= anchor_fare:
                    anchor_idx = idx
                    break

            au_rev = au_init.copy()
            au_rev[anchor_idx] = min(au_rev[anchor_idx], float(cb_val + first_rbd))
            for j in range(anchor_idx + 1, len(fares_init)):
                au_init[j] = 0.0
                au_rev[j] = 0.0

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
            plf_band, threshold_factor = cls.get_plf_band_info(plf_pct, db_helper=db_helper)

            override_tf = _get_val(curr_params, 'threshold_factor_NG', 'threshold_factor', default=None)
            if override_tf is not None:
                threshold_factor = to_float(override_tf, threshold_factor)

            grid_doc = None
            if db_helper and hasattr(db_helper, 'getCompResponseGrid'):
                grid_doc = db_helper.getCompResponseGrid(grid_name, ndo_band, plf_band)

            if grid_doc:
                rule_applied = grid_doc.get("response_rule", grid_doc.get("rule", "C"))
                criteria = grid_doc.get("criteria", "Min")
            else:
                rule_applied = "Min (1.2A, 0.8C)" if plf_band == 0 else ("Min (1.3A, 0.9C)" if plf_band == 1 else "C")
                criteria = "Min"

            response_fare = cls.evaluate_grid_response(
                rule_applied, anchor_fare, c_min_comp, c_med_comp,
                threshold_factor=threshold_factor
            )
            strategy_fare = cls.determine_strategy_fare(response_fare, init_df)

            pmin_thresh_factor = to_float(_get_val(curr_params, 'pmin_threshold_factor_NG', 'pmin_threshold_factor', default=0.1), 0.1)

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

        # Step 5: 51 Fare Levels (airline component) Mapping & Opening Fare
        # Resolve 51 filed B2C fare levels in airline component
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
            fare_col = next((c for c in ['airline_fare', 'fare', 'Fare', 'TOTAL', 'Total'] if c in filed_51_df.columns), None)
            for _, r in filed_51_df.iterrows():
                rbd_code = str(r.get('RBD', '')).strip()
                raw_f = to_float(r.get(fare_col), 0.0) if fare_col else 0.0
                if fare_col in ['TOTAL', 'Total'] and tax_helper and origin and destin:
                    comp_f = tax_helper.allin_to_airline_component(raw_f, origin, destin, route=route)
                else:
                    comp_f = raw_f
                filed_levels.append({
                    'RBD': rbd_code,
                    'fare': comp_f,
                    'allin': raw_f if fare_col in ['TOTAL', 'Total'] else (tax_helper.airline_component_to_allin(comp_f, origin, destin, route=route) if tax_helper and origin and destin else comp_f)
                })
            filed_levels = sorted(filed_levels, key=lambda x: x['fare'], reverse=True)

        # Nearest forecast fare below competition response fare (or anchor fare)
        fc_fares = [to_float(f, 0.0) for f in final_df['fare'].tolist()]
        fc_aus = [to_float(a, 0.0) for a in final_df['AU_Revised'].tolist()]
        fc_below = [(f, a) for f, a in zip(fc_fares, fc_aus) if f <= response_fare]
        if fc_below:
            target_fc_fare, target_fc_au = fc_below[0]
        else:
            target_fc_fare, target_fc_au = fc_fares[-1], fc_aus[-1]

        # "The inventory at nearest fare (among the fares from forecast file) below the fare obtained from
        # competition response should be assigned to the nearest lower among the 51 fare levels (airline component)."
        # "And add that as another column in inventory file as opening fare"
        if filed_levels:
            cand_51 = [fl for fl in filed_levels if fl['fare'] <= target_fc_fare]
            open_filed = cand_51[0] if cand_51 else filed_levels[-1]
            opening_fare = float(open_filed['fare'])
            opening_rbd = open_filed['RBD']
        else:
            open_classes = final_df[(final_df['AU_Revised'] > cb_val) & (final_df.get('Promo_Flag', 0) == 0)]
            if not open_classes.empty:
                opening_fare = to_float(open_classes['fare'].iloc[-1], 0.0)
            else:
                filed_classes = final_df[final_df.get('Promo_Flag', 0) == 0]
                opening_fare = to_float(filed_classes['fare'].iloc[0], 0.0) if not filed_classes.empty else to_float(final_df['fare'].iloc[0], 0.0)
            opening_rbd = str(final_df['FareBucket'].iloc[0])

        # Allocate inventory for each of the 51 fare levels:
        # "If qp fares changed after morning so forecast fare levels and actual fare levels will be different
        # then allocate inventory to nearest fare for each fare level."
        filed_51_au_mapping = {}
        if filed_levels:
            for fl in filed_levels:
                f_val = fl['fare']
                r_code = fl['RBD']
                if f_val < opening_fare - 1e-4:
                    # Below opening fare: inventory is 0
                    alloc_au = 0.0
                elif abs(f_val - opening_fare) < 1e-4:
                    # At opening fare: assigned the competition response inventory (capped at cb + first_rbd)
                    alloc_au = min(target_fc_au, float(cb_val + first_rbd))
                else:
                    # Above opening fare: allocate inventory from nearest forecast fare
                    nearest_idx = int(np.argmin([abs(f_val - fc_f) for fc_f in fc_fares]))
                    alloc_au = fc_aus[nearest_idx]
                filed_51_au_mapping[r_code] = alloc_au

            # Enforce non-decreasing monotonicity (higher fare cannot have lower AU)
            for i in range(len(filed_levels) - 2, -1, -1):
                high_rbd = filed_levels[i]['RBD']
                low_rbd = filed_levels[i+1]['RBD']
                if filed_levels[i]['fare'] >= opening_fare - 1e-4:
                    filed_51_au_mapping[high_rbd] = max(filed_51_au_mapping[high_rbd], filed_51_au_mapping[low_rbd])

        seats_on_opening = max(0.0, target_fc_au - cb_val)

        # Add 'opening fare' as another column in inventory file
        final_df['opening fare'] = opening_fare

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
                    if 'AF_Forecast' in inv_ng.columns and pd.notna(inv_ng['AF_Forecast'].iloc[0]):
                        daily_af = to_float(inv_ng['AF_Forecast'].iloc[0], None)
            except Exception as e:
                logger.warning(f"Error reading pre-calculated forecast from add_model_inventory_ng: {e}")

        # Convert airline component fares to ALL-IN for NextGen output summary and inventory optimization
        opening_fare_allin = tax_helper.airline_component_to_allin(opening_fare, origin, destin, route=route) if tax_helper and origin and destin else opening_fare
        strategy_fare_allin = tax_helper.airline_component_to_allin(strategy_fare, origin, destin, route=route) if tax_helper and origin and destin else strategy_fare
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
        final_df['strategy_fare'] = float(strategy_fare)
        final_df['strategy_fare_All_in'] = float(strategy_fare_allin)
        final_df['oal_b2c_min'] = to_float(c_min_comp, 0.0)
        final_df['oal_b2c_min_All_in'] = to_float(c_min_allin, 0.0)

        summary = {
            'NG_Historical_current_booked': cb_val,
            'NG_LF_Forecast': daily_lf if daily_lf is not None else to_float(lf_fc_rev * 100.0, 0.0),
            'NG_AF_Forecast': daily_af if daily_af is not None else to_float(af_fc, 0.0),
            'NG_Opening_fare': to_float(opening_fare_allin, 0.0),
            'NG_FirstRBD': to_float(seats_on_opening, 0.0),
            'NG_Strategy_fare': to_float(strategy_fare_allin, 0.0),
            'NG_Anchor_fare': to_float(anchor_fare_allin, 0.0),
            'Strategy_fare': to_float(strategy_fare_allin, 0.0),
            'Anchor_fare': to_float(anchor_fare_allin, 0.0),
            'Pmin_Old': to_float(redistrib_df.attrs.get('p_min_old', anchor_fare), 0.0),
            'Current_Revenue': curr_rev_val,
            'Total_Expected_Revenue': to_float(tot_er, 0.0),
            'Predicted_Revenue': to_float(pred_rev, 0.0),
            'Predicted_Pickup_Revised': to_float(pickup_rev, 0.0),
            'Predicted_Flown_Revised': to_float(flown_rev, 0.0),
            'MarketFare_Selected': to_float(c_min_allin, None) if c_min_allin is not None else None,
            'MarketFare_Min': to_float(c_min_allin, None) if c_min_allin is not None else None,
            'MarketFare_Max': to_float(c_med_allin, None) if c_med_allin is not None else None,
            'MarketFare_Min_AllIn': to_float(c_min_allin, None) if c_min_allin is not None else None,
            'MarketFare_Max_AllIn': to_float(c_med_allin, None) if c_med_allin is not None else None,
            'MarketFare_Min_Comp': to_float(c_min_comp, None) if c_min_comp is not None else None,
            'Grid_Name': grid_name,
            'Rule_Applied': rule_applied,
            'nextgen_flag': 1 if is_flag_enabled(_get_val(curr_params, 'nextgen_flag_NG', 'nextgen_flag', default=0)) else 0,
            'competition_response_flag': 1 if is_flag_enabled(comp_flag) else 0,
            'buy_up_factor': buy_up,
            'threshold_factor': to_float(threshold_factor, 0.0),
            'opening_fare_airline_comp': to_float(opening_fare, 0.0),
            'strategy_fare_airline_comp': to_float(strategy_fare, 0.0),
            'anchor_fare_airline_comp': to_float(anchor_fare, 0.0),
            'opening_fare_allin': to_float(opening_fare_allin, 0.0),
            'strategy_fare_allin': to_float(strategy_fare_allin, 0.0),
            'anchor_fare_allin': to_float(anchor_fare_allin, 0.0),
            'oal_b2c_min_airline_comp': to_float(c_min_comp, None) if c_min_comp is not None else None,
            'oal_b2c_min_allin': to_float(c_min_allin, None) if c_min_allin is not None else None,
            'filed_51_au_mapping': filed_51_au_mapping
        }

        return final_df, summary
