import math
import re
import numpy as np
import pandas as pd
from scipy.stats import norm
import logging

from Constants import CONST_NA

logger = logging.getLogger(__name__)


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

    @classmethod
    def get_plf_band(cls, plf_pct, db_helper=None):
        """
        Dynamically determines the PLF band for a given PLF percentage.
        Queries comp_response_plf_bands via db_helper if available, or matches range.
        """
        val = float(plf_pct) if plf_pct is not None else 0.0
        if 0.0 < val <= 1.5:
            val = val * 100.0

        if db_helper and hasattr(db_helper, 'getCompResponsePlfBand'):
            band = db_helper.getCompResponsePlfBand(val)
            if band is not None:
                return band

        # Generic dynamic fallback from database band table if query not available
        if hasattr(db_helper, 'db') and hasattr(db_helper.db, 'comp_response_plf_bands'):
            try:
                rec = db_helper.db.comp_response_plf_bands.find_one({"start": {"$lte": val}, "end": {"$gt": val}})
                if rec:
                    return rec.get("plf_band", rec.get("id"))
            except Exception as e:
                logger.error(f"Error querying comp_response_plf_bands: {e}")

        # Standard range mapping
        if val < 80.0:
            return 0
        elif val < 85.0:
            return 1
        elif val < 90.0:
            return 2
        elif val < 95.0:
            return 3
        elif val < 100.0:
            return 4
        else:
            return 5

    @classmethod
    def get_ndo_band(cls, ndo, db_helper=None):
        """
        Dynamically determines the NDO band for a given NDO.
        Queries ndo_bands via db_helper.
        """
        n = int(ndo) if ndo is not None else 0
        if db_helper and hasattr(db_helper, 'getNdoBand'):
            band = db_helper.getNdoBand(n)
            if band is not None:
                return band

        if hasattr(db_helper, 'db') and hasattr(db_helper.db, 'ndo_bands'):
            try:
                rec = db_helper.db.ndo_bands.find_one({"start": {"$lte": n}, "end": {"$gt": n}})
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

        if fare_col not in df.columns and 'FareMidpoint' in df.columns:
            df[fare_col] = df['FareMidpoint']
        elif fare_col not in df.columns and 'TOTAL' in df.columns:
            df[fare_col] = df['TOTAL']
        elif fare_col not in df.columns and 'Total' in df.columns:
            df[fare_col] = df['Total']

        df[fare_col] = pd.to_numeric(df[fare_col], errors='coerce').fillna(0.0)
        df = df.sort_values(fare_col, ascending=False).reset_index(drop=True)

        n = len(df)
        cap = int(capacity) if capacity and capacity > 0 else 189
        cb = float(current_bookings) if current_bookings is not None else 0.0

        mu = pd.to_numeric(df.get(pax_col, 0.0), errors='coerce').fillna(0.0).clip(lower=0.0).tolist()
        sigma = pd.to_numeric(df.get(std_col, 0.0), errors='coerce').fillna(0.0).clip(lower=0.0).tolist()
        fares = df[fare_col].tolist()

        f_lowest = fares[-1] if n > 0 else 0.0
        f_promo = round(float(promo_ratio) * f_lowest, 2)

        alpha = float(buy_up_factor or 0.0)
        alpha = float(np.clip(alpha, 0.0, 0.99))
        buy_up_mult = 1.0 / (1.0 - alpha)
        sig_fact = float(sigma_factor) if sigma_factor is not None else 0.25

        mu_bar_list = []
        sig_bar_list = []
        F_j_list = []
        y_list = []

        for j in range(n):
            mu_bar = float(sum(mu[:j+1]))
            sig_raw = math.sqrt(sum(s**2 for s in sigma[:j+1]))
            sig_bar = min(sig_raw, sig_fact * mu_bar)
            F_j = (sum(mu[i] * fares[i] for i in range(j+1)) / mu_bar) if mu_bar > 0 else fares[j]
            f_next = fares[j+1] if j < n - 1 else f_promo

            if F_j > 0:
                prob = (1.0 - (f_next / F_j)) * buy_up_mult
            else:
                prob = 0.5
            prob_clipped = float(np.clip(prob, 1e-6, 1.0 - 1e-6))
            z = float(norm.ppf(prob_clipped))

            y_j = max(0, math.ceil(mu_bar + z * sig_bar))
            if y_list:
                y_j = max(y_j, y_list[-1])  # monotonic nesting

            mu_bar_list.append(mu_bar)
            sig_bar_list.append(sig_bar)
            F_j_list.append(F_j)
            y_list.append(y_j)

        # Authorization levels (AU)
        au_list = [cap]
        for j in range(1, n):
            au_list.append(max(0, cap - y_list[j-1]))

        # Promo AU
        au_promo_calc = max(0, cap - y_list[-1]) if n > 0 else cap
        pred_pickup_initial = sum(mu)
        lf_forecast_initial = ((cb + pred_pickup_initial) / cap) * 100.0 if cap > 0 else 0.0
        au_promo = 0 if lf_forecast_initial >= float(promo_threshold) else au_promo_calc

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
            anchor_fare = float(open_classes[fare_col].iloc[-1])
        else:
            anchor_fare = float(df[fare_col].iloc[0])

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
            if int(connection_fare_flag or 0) == 0:
                # Direct flights only
                df_mkt = df_mkt[stops_series == 0]
            else:
                # Include direct flights AND connecting flights with duration <= (direct_duration + additional_duration)
                if dur_col and direct_duration_mins:
                    direct_mins = float(direct_duration_mins)
                    add_mins = float(additional_connection_hours or 4.0) * 60.0
                    max_allowed_duration = direct_mins + add_mins

                    def parse_duration_to_mins(v):
                        try:
                            if pd.isna(v): return direct_mins
                            v_str = str(v).strip()
                            if ':' in v_str:
                                parts = v_str.split(':')
                                return float(parts[0]) * 60.0 + float(parts[1])
                            return float(v_str)
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
                        return int(parts[0]) * 60 + int(parts[1])
                    return None

                target_mins = parse_std_time(flight_std)
                if target_mins is not None:
                    window_mins = float(time_range_hours) * 60.0
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
        cov_p = float(coverage_param) if coverage_param is not None else 3.0
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
        comp_med_fare=None
    ):
        """
        Phase 2b: Dynamically parses and evaluates ANY Competition Response Grid formula
        involving Anchor Fare (A) and Competitor Fare (C).
        Supports:
          - 'Min (1.2A, 0.8C)', 'Min (1.3A, 0.9C)', 'Min (1.5A, C)', 'Min (2A, C)', 'Min (xA, yC)'
          - 'Max (xA, yC)', 'C', '0.95C', 'A', '1.1A', etc.
        """
        A = float(anchor_fare)
        C = float(comp_min_fare)
        med_C = float(comp_med_fare) if comp_med_fare is not None else C

        rule = str(grid_rule_str).strip()
        if not rule or rule == CONST_NA:
            return C

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
            return float(eval(expr, {"__builtins__": {}}, safe_env))
        except Exception as e:
            logger.warning(f"Error evaluating grid rule expression '{expr}': {e}. Defaulting to competitor fare.")
            return C_val

    @classmethod
    def determine_strategy_fare(cls, response_fare, ladder_df, fare_col='fare'):
        """
        Strategy Fare C is the highest ladder fare lower than (or equal to) the response fare.
        """
        if response_fare is None or pd.isna(response_fare):
            return None

        if fare_col not in ladder_df.columns:
            fare_col = next((c for c in ['fare', 'FareMidpoint', 'TOTAL', 'Total'] if c in ladder_df.columns), None)
            if not fare_col:
                return float(response_fare)

        fares = pd.to_numeric(ladder_df[fare_col], errors='coerce').dropna().sort_values(ascending=False).tolist()
        valid = [f for f in fares if f <= float(response_fare)]
        if valid:
            return float(valid[0])
        return float(fares[-1]) if fares else float(response_fare)

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
        fare_col='fare'
    ):
        """
        Phase 3: Demand Redistribution & Revised AU Calculation.
        If A < C, warps the demand curve via P_eq equivalent price mapping, computes mu_2, sigma_2,
        AU_2, and final AU_revised with FirstRBD cap, Backstop floor, and Hardanchor cap.
        """
        df = ladder_df.copy()
        if df.empty:
            return df

        if fare_col not in df.columns:
            fare_col = next((c for c in ['fare', 'FareMidpoint', 'TOTAL', 'Total'] if c in df.columns), 'fare')

        df[fare_col] = pd.to_numeric(df[fare_col], errors='coerce').fillna(0.0)
        df = df.sort_values(fare_col, ascending=False).reset_index(drop=True)
        fares = df[fare_col].tolist()
        n = len(df)

        cap = int(capacity) if capacity and capacity > 0 else 189
        cb = float(current_bookings) if current_bookings is not None else 0.0
        A = float(anchor_fare)
        C = float(strategy_fare) if strategy_fare is not None else A
        sig_fact = float(sigma_factor) if sigma_factor is not None else 0.25
        first_rbd_seats = float(first_rbd) if first_rbd is not None else 20.0

        # If Anchor >= Strategy or Comp missing, no demand redistribution
        if A >= C or strategy_fare is None or pd.isna(strategy_fare):
            df['P_eq'] = np.nan
            df['mu_bar_2'] = df['mu_bar']
            df['mu_2'] = df['mu']
            df['sigma_2'] = sig_fact * df['mu_2']
            df['sigma_3'] = df['sigma_2']
            df['sigma_bar_3'] = df['sigma_bar']
            df['ProtectionLimit_3'] = df['ProtectionLimit']
            df['AU_2'] = df['AU']
            
            au_rev = df['AU_2'].copy().tolist()
            strat_idx = next((i for i, f in enumerate(fares) if f <= C), 0)
            au_rev[strat_idx] = min(au_rev[strat_idx], cb + first_rbd_seats)

            # Apply backstop floor
            if backstop is not None and pd.notna(backstop) and str(backstop) != CONST_NA:
                try:
                    b_val = float(backstop)
                    for i, f in enumerate(fares):
                        if f < b_val:
                            au_rev[i] = 0
                except ValueError:
                    pass

            # Apply hardanchor cap
            if hardanchor is not None and pd.notna(hardanchor) and str(hardanchor) != CONST_NA:
                try:
                    h_val = float(hardanchor)
                    for i, f in enumerate(fares):
                        if f >= h_val:
                            au_rev[i] = cap
                except ValueError:
                    pass

            df['AU_Revised'] = au_rev
            return df

        # --- A < C: Demand Redistribution Triggers ---
        P_max = fares[0]
        strat_candidates = [f for f in fares if f <= C]
        C_ladder = strat_candidates[0] if strat_candidates else fares[-1]
        strat_idx = fares.index(C_ladder)

        orig_mu_bars = df['mu_bar'].tolist()
        P_eq_list = []
        mu_bar_2_list = []

        for i, p in enumerate(fares):
            if p < C_ladder:
                peq = 0.0
                m_bar_2 = 0.0
            elif p == C_ladder:
                peq = A
                anchor_candidates = [f for f in fares if f <= A]
                a_ladder = anchor_candidates[0] if anchor_candidates else fares[-1]
                a_idx = fares.index(a_ladder)
                m_bar_2 = orig_mu_bars[a_idx]
            elif p >= P_max:
                peq = P_max
                m_bar_2 = orig_mu_bars[0]
            else:
                denom = (P_max - C_ladder)
                peq = A + ((p - C_ladder) / denom) * (P_max - A) if denom > 0 else A

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

        sigma_2_list = [sig_fact * m for m in mu_2_list]
        df['P_eq'] = P_eq_list
        df['mu_bar_2'] = mu_bar_2_list
        df['mu_2'] = mu_2_list
        df['sigma_2'] = sigma_2_list
        df['sigma_3'] = sigma_2_list
        df['sigma_bar_3'] = [sig_fact * sum(mu_2_list[:j+1]) for j in range(n)]

        # Recompute EMSR-b on mu_2, sigma_2
        f_promo = round(float(promo_ratio) * fares[-1], 2)
        alpha = float(buy_up_factor or 0.0)
        buy_up_mult = 1.0 / (1.0 - np.clip(alpha, 0.0, 0.99))

        y_2_list = []
        for j in range(n):
            if fares[j] < C_ladder:
                y_2_list.append(cap)
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
            y_j = max(0, math.ceil(m_bar + z * s_bar))
            if y_2_list and y_2_list[-1] < cap:
                y_j = max(y_j, y_2_list[-1])
            y_2_list.append(y_j)

        df['ProtectionLimit_3'] = y_2_list

        au_2_list = [cap]
        for j in range(1, n):
            if fares[j] < C_ladder:
                au_2_list.append(0)
            else:
                au_2_list.append(max(0, cap - y_2_list[j-1]))

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
        au_revised[strat_idx] = min(au_revised[strat_idx], cb + first_rbd_seats)

        # Backstop
        if backstop is not None and pd.notna(backstop) and str(backstop) != CONST_NA:
            try:
                b_val = float(backstop)
                for i, f in enumerate(fares):
                    if f < b_val:
                        au_revised[i] = 0
            except ValueError:
                pass

        # Hardanchor
        if hardanchor is not None and pd.notna(hardanchor) and str(hardanchor) != CONST_NA:
            try:
                h_val = float(hardanchor)
                for i, f in enumerate(fares):
                    if f >= h_val:
                        au_revised[i] = cap
            except ValueError:
                pass

        df['AU_Revised'] = au_revised
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
        au_col='AU_Revised'
    ):
        """
        Phase 4: Closed-Form Revenue Forecasting using standard normal distribution integrals.
        """
        df = ladder_df.copy()
        if df.empty:
            return 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, df

        if fare_col not in df.columns:
            fare_col = next((c for c in ['fare', 'FareMidpoint', 'TOTAL', 'Total'] if c in df.columns), 'fare')

        df = df.sort_values(fare_col, ascending=False).reset_index(drop=True)
        fares = df[fare_col].tolist()
        mu_2 = df[mu_col].tolist()
        sigma_2 = df[sigma_col].tolist()
        au_rev = df[au_col].tolist()
        n = len(df)

        cap = int(capacity) if capacity and capacity > 0 else 189
        cb = float(current_bookings) if current_bookings is not None else 0.0
        curr_rev = float(current_revenue) if current_revenue is not None else 0.0

        A_hat = [0.0] * n
        ER = [0.0] * n
        Ai_list = [0.0] * n

        # Process bottom-up from lowest fare to highest fare
        for i in reversed(range(n)):
            sum_lower = sum(A_hat[i+1:])
            Ai = max(0.0, float(au_rev[i]) - cb - sum_lower)
            Ai_list[i] = Ai
            m_i = float(mu_2[i])
            s_i = float(sigma_2[i])

            if s_i > 0:
                zi = (Ai - m_i) / s_i
                phi_z = float(norm.pdf(zi))
                Phi_z = float(norm.cdf(zi))
                A_hat_i = m_i * Phi_z - s_i * phi_z + Ai * (1.0 - Phi_z)
            else:
                A_hat_i = min(m_i, Ai)

            A_hat[i] = max(0.0, A_hat_i)
            ER[i] = fares[i] * A_hat[i]

        df['A_cap'] = Ai_list
        df['A'] = A_hat
        df['Expected_Bookings'] = A_hat
        df['Expected_Revenue'] = ER

        total_expected_revenue = float(sum(ER))
        predicted_revenue = curr_rev + total_expected_revenue
        pred_pickup_rev = float(sum(mu_2))
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
        recalibration_dict=None
    ):
        """
        Executes end-to-end NextGen AU optimization and revenue forecasting for a single flight.
        Dynamically queries DB for grid rules, bands, parameters, and competitor recalibrations.
        """
        # Dynamic parameter extraction from curr_params / market_list / DB
        buy_up = getattr(curr_params, 'buy_up_factor', 0.0) if curr_params else 0.0
        comp_flag = getattr(curr_params, 'competition_response_flag', 1) if curr_params else 1
        time_range = getattr(curr_params, 'time_range', 2.0) if curr_params else 2.0
        carr_excl = getattr(curr_params, 'carrier_exclusion', None) if curr_params else None
        grid_name = str(getattr(curr_params, 'grid_name', 'DEFAULT')).strip() or 'DEFAULT'
        backstop = getattr(curr_params, 'price_floor', getattr(curr_params, 'b2c_backstop', None)) if curr_params else None
        hardanchor = getattr(curr_params, 'price_cap', getattr(curr_params, 'hardanchor', None)) if curr_params else None
        ndo = getattr(curr_params, 'ndocounter', getattr(curr_params, 'NDO', 0)) if curr_params else 0
        first_rbd = getattr(curr_params, 'first_rbd_alloc', 20) if curr_params else 20
        promo_threshold = getattr(curr_params, 'plfthreshold', 95.0) if curr_params else 95.0
        sigma_factor = getattr(curr_params, 'sigma_factor', 0.25) if curr_params else 0.25
        promo_ratio = getattr(curr_params, 'promo_fare_factor', 0.80) if curr_params else 0.80
        cov_param = getattr(curr_params, 'coverage_parameter', 3.0) if curr_params else 3.0

        # Step 1: Initial EMSR-b
        init_df, anchor_fare, promo_au = cls.compute_initial_emsr_b(
            flight_ladder_df, capacity, current_bookings,
            buy_up_factor=buy_up,
            sigma_factor=sigma_factor,
            promo_ratio=promo_ratio,
            promo_threshold=promo_threshold
        )

        # Step 2: Competition Response & Strategy Fare
        conn_fare_flag = getattr(curr_params, 'connection_fare_flag', 0) if curr_params else 0
        add_conn_dur = getattr(curr_params, 'additional_connection_duration', 4.0) if curr_params else 4.0
        dir_dur = getattr(curr_params, 'flight_duration', getattr(curr_params, 'outbound_duration', None)) if curr_params else None

        c_min, c_med = cls.filter_market_fares(
            market_fares, flight_std,
            time_range_hours=time_range,
            carrier_exclusion=carr_excl,
            connection_fare_flag=conn_fare_flag,
            direct_duration_mins=dir_dur,
            additional_connection_hours=add_conn_dur,
            coverage_param=cov_param
        )

        # Apply Recalibration if present
        if c_min is not None and recalibration_dict:
            c_min = c_min + recalibration_dict.get('default', 0.0)

        rule_applied = "N/A"
        strategy_fare = anchor_fare

        if comp_flag == 1 and c_min is not None:
            # Dynamic Band Lookups from DB
            pred_pickup_init = sum(init_df['mu'])
            plf_pct = ((float(current_bookings) + pred_pickup_init) / float(capacity)) * 100.0 if capacity > 0 else 0.0

            ndo_band = cls.get_ndo_band(ndo, db_helper=db_helper)
            plf_band = cls.get_plf_band(plf_pct, db_helper=db_helper)

            # Dynamic Grid Lookup from DB
            grid_doc = None
            if db_helper and hasattr(db_helper, 'getCompResponseGrid'):
                grid_doc = db_helper.getCompResponseGrid(grid_name, ndo_band, plf_band)

            if grid_doc:
                rule_applied = grid_doc.get("response_rule", grid_doc.get("rule", "C"))
                criteria = grid_doc.get("criteria", "Min")
            else:
                rule_applied = "Min (1.2A, 0.8C)" if plf_band == 0 else ("Min (1.3A, 0.9C)" if plf_band == 1 else "C")
                criteria = "Min"

            response_fare = cls.evaluate_grid_response(rule_applied, anchor_fare, c_min, c_med)
            strategy_fare = cls.determine_strategy_fare(response_fare, init_df)
        else:
            strategy_fare = anchor_fare

        # Step 3: Demand Redistribution & Revised AU
        redistrib_df = cls.redistribute_demand_and_compute_revised_au(
            init_df, anchor_fare, strategy_fare, capacity, current_bookings,
            buy_up_factor=buy_up,
            sigma_factor=sigma_factor,
            first_rbd=first_rbd,
            promo_ratio=promo_ratio,
            backstop=backstop,
            hardanchor=hardanchor,
            promo_au=promo_au
        )

        # Step 4: Closed-Form Revenue Forecasting
        pred_rev, tot_er, pickup_rev, flown_rev, af_fc, lf_fc_rev, final_df = cls.compute_closed_form_revenue_forecast(
            redistrib_df, capacity, current_bookings, current_revenue
        )

        # Find opening fare & seats on opening fare
        open_classes = final_df[final_df['AU_Revised'] > float(current_bookings)]
        if not open_classes.empty:
            opening_fare = float(open_classes['fare'].iloc[-1])
            opening_au = float(open_classes['AU_Revised'].iloc[-1])
            seats_on_opening = max(0.0, opening_au - float(current_bookings))
        else:
            opening_fare = float(final_df['fare'].iloc[0])
            seats_on_opening = max(0.0, float(final_df['AU_Revised'].iloc[0]) - float(current_bookings))

        summary = {
            'NG_Historical_current_booked': current_bookings,
            'NG_LF_Forecast': round(lf_fc_rev * 100.0, 2),
            'NG_AF_Forecast': round(af_fc, 2),
            'NG_Opening_fare': opening_fare,
            'NG_FirstRBD': round(seats_on_opening, 2),
            'Strategy_fare': strategy_fare,
            'Anchor_fare': anchor_fare,
            'Current_Revenue': round(current_revenue, 2),
            'Total_Expected_Revenue': round(tot_er, 2),
            'Predicted_Revenue': round(pred_rev, 2),
            'Predicted_Pickup_Revised': round(pickup_rev, 4),
            'Predicted_Flown_Revised': round(flown_rev, 4),
            'MarketFare_Selected': c_min,
            'MarketFare_Min': c_min,
            'MarketFare_Max': c_med,
            'Grid_Name': grid_name,
            'Rule_Applied': rule_applied,
            'nextgen_flag': getattr(curr_params, 'nextgen_flag', 0) if curr_params else 0,
            'competition_response_flag': comp_flag,
            'buy_up_factor': buy_up
        }

        return final_df, summary
