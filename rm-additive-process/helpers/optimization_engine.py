"""
helpers/optimization_engine.py
=============================================================================
AU Optimization & Competition Response Engine (NextGen RM)
Implements all mathematical and business logic per BRD-AU_Opt_Competition_Response:

  1. EMSR-b Inventory Optimization (Protection Limits y_j & Authorization Levels AU_j)
  2. Promo Fare (80% lowest filed fare, W RBD) Allocation
  3. Anchor Fare (A) Determination
  4. Market Fare Ingestion, Outlier Filtering (Modified Z-Score / MAD), & Recalibration
  5. Competition Response Grid & Strategy Fare (C) Selection
  6. Demand Redistribution (P_eq mapping & uniform interpolation)
  7. Revised Authorization Levels (AU_2 & AU_revised with FirstRBD cap, Backstop, Hardanchor)
  8. Closed-Form Expected Accepted Bookings (A_hat) & Revenue Forecast (AF_Forecast)
"""

import math
import numpy as np
import pandas as pd
from datetime import time, timedelta

from helpers.utils.common_utils import (
    norm_cdf, norm_pdf, norm_ppf, filter_outliers_mad
)


class AUOptimizationEngine:
    """Core mathematical engine for NextGen AU Optimization and Competition Response."""

    def __init__(self, logger=None):
        self.logger = logger

    def _log_debug(self, msg):
        if self.logger:
            self.logger.debug(msg)

    def _log_info(self, msg):
        if self.logger:
            self.logger.info(msg)

    def _log_warning(self, msg):
        if self.logger:
            self.logger.warning(msg)

    # =========================================================================
    # STEP 1: INITIAL EMSR-b INVENTORY OPTIMIZATION
    # =========================================================================
    @staticmethod
    def compute_emsr_b(mu_vec, sigma_vec, fares_vec, capacity,
                       buy_up_factor=0.0, promo_factor=0.80,
                       standard_dev_factor=0.25,
                       forecast_threshold_promo=95.0,
                       current_bookload=0):
        """
        Compute EMSR-b protection limits and authorization levels across 19 fare classes + Promo.

        Args:
            mu_vec: array-like of length 19, demand per class sorted DESCENDING by fare (f_1 > f_2 > ... > f_19)
            sigma_vec: array-like of length 19, standard deviation per class
            fares_vec: array-like of length 19, fare values (airline component) sorted DESCENDING
            capacity: flight capacity (int/float)
            buy_up_factor: alpha in [0, 1) (default 0.0)
            promo_factor: multiplier on lowest filed fare for Promo class (default 0.80)
            standard_dev_factor: gamma factor capping sigma_bar <= gamma * mu_bar (default 0.25)
            forecast_threshold_promo: LF threshold above which Promo AU is set to 0
            current_bookload: current bookings on the flight

        Returns:
            dict containing:
                - 'mu_bar': list of length 19
                - 'sigma_bar': list of length 19
                - 'F_j': list of length 19 (weighted average fare for aggregated higher classes)
                - 'prob': list of length 19 (marginal protection probability)
                - 'z': list of length 19
                - 'y': list of length 19 (protection limits)
                - 'au': list of length 19 (authorization levels)
                - 'promo_fare': float (80% of lowest filed fare)
                - 'promo_prob': float
                - 'promo_z': float
                - 'promo_y': int (protection limit for promo)
                - 'promo_au': int (authorization level for promo)
        """
        n = len(mu_vec)
        if n == 0 or len(fares_vec) != n:
            raise ValueError(f"mu_vec and fares_vec must be non-empty and same length ({n} vs {len(fares_vec)})")

        mu = np.array([max(0.0, float(m)) if pd.notna(m) else 0.0 for m in mu_vec])
        sigma = np.array([max(0.0, float(s)) if pd.notna(s) else 0.0 for s in sigma_vec])
        fares = np.array([float(f) for f in fares_vec])
        K = float(capacity)
        alpha = min(max(float(buy_up_factor), 0.0), 0.999)
        prob_scale = 1.0 / (1.0 - alpha)

        mu_bar = np.zeros(n)
        sigma_bar = np.zeros(n)
        F_j = np.zeros(n)
        prob = np.zeros(n)
        z = np.zeros(n)
        y = np.zeros(n, dtype=int)
        au = np.zeros(n, dtype=int)

        # 1. Cumulative demand, dispersion, and weighted fare (for higher classes 0..j)
        cum_mu = 0.0
        cum_var = 0.0
        cum_rev = 0.0

        for j in range(n):
            cum_mu += mu[j]
            cum_var += (sigma[j] ** 2)
            cum_rev += (mu[j] * fares[j])

            mu_bar[j] = cum_mu
            raw_sigma = math.sqrt(cum_var)
            sigma_bar[j] = min(raw_sigma, standard_dev_factor * cum_mu) if cum_mu > 0 else 0.0
            F_j[j] = (cum_rev / cum_mu) if cum_mu > 0 else fares[j]

        # 2. Protection limits and probabilities for classes 0..(n-2) protecting against next lower class
        for j in range(n - 1):
            f_next = fares[j + 1]
            raw_p = (1.0 - (f_next / F_j[j])) if F_j[j] > 0 else 0.5
            p = min(max(raw_p * prob_scale, 1e-6), 1.0 - 1e-6)
            prob[j] = p
            z_val = norm_ppf(p)
            z[j] = z_val

            prot = mu_bar[j] + (z_val * sigma_bar[j])
            y[j] = max(0, math.ceil(prot))

        # 3. Promo class protecting against lowest filed class (j = n-1)
        f_promo = promo_factor * fares[n - 1]
        raw_p_promo = (1.0 - (f_promo / F_j[n - 1])) if F_j[n - 1] > 0 else 0.5
        p_promo = min(max(raw_p_promo * prob_scale, 1e-6), 1.0 - 1e-6)
        prob[n - 1] = p_promo
        z_promo = norm_ppf(p_promo)
        z[n - 1] = z_promo
        prot_promo = mu_bar[n - 1] + (z_promo * sigma_bar[n - 1])
        y_promo = max(0, math.ceil(prot_promo))
        y[n - 1] = y_promo

        # 4. Authorization levels (AU): AU for highest class = Capacity, AU for class j+1 = Capacity - y_j
        au[0] = int(K)
        for j in range(1, n):
            au[j] = max(0, int(K - y[j - 1]))

        # Promo AU (Capacity - y_19)
        initial_flown = float(current_bookload) + float(mu_bar[n - 1])
        initial_lf = (initial_flown / K * 100.0) if K > 0 else 0.0
        if initial_lf >= forecast_threshold_promo:
            promo_au = 0
        else:
            promo_au = max(0, int(K - y_promo))

        return {
            'mu_bar': mu_bar.tolist(),
            'sigma_bar': sigma_bar.tolist(),
            'F_j': F_j.tolist(),
            'prob': prob.tolist(),
            'z': z.tolist(),
            'y': y.tolist(),
            'au': au.tolist(),
            'promo_fare': float(f_promo),
            'promo_prob': float(p_promo),
            'promo_z': float(z_promo),
            'promo_y': int(y_promo),
            'promo_au': int(promo_au),
        }

    # =========================================================================
    # STEP 2: ANCHOR FARE DETERMINATION
    # =========================================================================
    @staticmethod
    def determine_anchor_fare(au_ladder, fares_ladder, current_bookload):
        """
        Anchor Fare = Lowest filed fare where AU is strictly greater than Current Bookload.
        Fares and AU are sorted DESCENDING by fare (f_1 > f_2 > ... > f_19).
        """
        cb = float(current_bookload)
        # Scan from lowest fare class (index n-1) up to highest (index 0)
        for j in range(len(fares_ladder) - 1, -1, -1):
            if au_ladder[j] > cb:
                return float(fares_ladder[j]), j

        # Fallback if full/overbooked: highest filed fare class
        return float(fares_ladder[0]), 0

    # =========================================================================
    # STEP 3: MARKET FARE FILTERING, OUTLIER REMOVAL & RECALIBRATION
    # =========================================================================
    @staticmethod
    def _time_diff_minutes(t1, t2):
        """Absolute time difference in minutes with 24h wrap-around."""
        if t1 is None or t2 is None:
            return 9999.0
        if isinstance(t1, str):
            t1 = pd.to_datetime(t1).time()
        if isinstance(t2, str):
            t2 = pd.to_datetime(t2).time()
        m1 = t1.hour * 60 + t1.minute
        m2 = t2.hour * 60 + t2.minute
        diff = abs(m1 - m2)
        return min(diff, 1440 - diff)

    def filter_and_process_market_fares(self, mkt_fares_df, flight_std, departure_date,
                                        time_range_hours=2.0,
                                        carrier_exclusions=None,
                                        connection_fare_flag=False,
                                        direct_flight_duration_hours=2.0,
                                        additional_connection_duration=4.0,
                                        coverage_param=3.0,
                                        recalibration_amount=0.0):
        """
        Filter competitor fares by departure date, time window, carrier exclusion,
        connection duration, and remove outliers via MAD modified Z-score.

        Returns:
            tuple: (min_comp_gross_fare, median_comp_gross_fare, selected_row_dict, all_valid_fares_df)
        """
        if mkt_fares_df is None or mkt_fares_df.empty:
            return None, None, None, pd.DataFrame()

        df = mkt_fares_df.copy()
        carrier_exclusions = set(carrier_exclusions or [])

        # 1. Date match
        if 'departure_date' in df.columns:
            df['departure_date'] = pd.to_datetime(df['departure_date']).dt.normalize()
            flt_date = pd.to_datetime(departure_date).normalize()
            df = df[df['departure_date'] == flt_date]
        elif 'DepartureDate' in df.columns:
            df['DepartureDate'] = pd.to_datetime(df['DepartureDate']).dt.normalize()
            flt_date = pd.to_datetime(departure_date).normalize()
            df = df[df['DepartureDate'] == flt_date]

        if df.empty:
            return None, None, None, pd.DataFrame()

        # 2. Carrier exclusion
        carr_col = next((c for c in ('airline_code', 'AirlineCode', 'outbound_airline_code', 'CarrierCode')
                         if c in df.columns), None)
        if carr_col:
            df = df[~df[carr_col].isin(carrier_exclusions)]
            # Exclude own carrier (QP) for competition response
            df = df[df[carr_col] != 'QP']

        if df.empty:
            return None, None, None, pd.DataFrame()

        # 3. Connection vs Direct filtering
        stops_col = next((c for c in ('stops', 'Stops', 'outBoundStop', 'is_connecting') if c in df.columns), None)
        dur_col = next((c for c in ('duration', 'Duration', 'outboundDuration', 'journey_duration') if c in df.columns), None)

        if not connection_fare_flag and stops_col:
            df = df[df[stops_col].isin([0, '0', False, 'False', 'DIRECT', 'Direct'])]
        elif connection_fare_flag and dur_col:
            max_allowed_dur = float(direct_flight_duration_hours) + float(additional_connection_duration)
            df = df[pd.to_numeric(df[dur_col], errors='coerce').fillna(0) <= (max_allowed_dur * 60.0 if df[dur_col].max() > 24 else max_allowed_dur)]

        if df.empty:
            return None, None, None, pd.DataFrame()

        # 4. Time range filtering (STD +/- time_range_hours)
        std_col = next((c for c in ('std', 'STD', 'departure_time', 'DepartureTime', 'flightTime') if c in df.columns), None)
        if std_col and flight_std is not None:
            max_diff_min = float(time_range_hours) * 60.0
            df['_time_diff'] = df[std_col].apply(lambda t: self._time_diff_minutes(t, flight_std))
            df = df[df['_time_diff'] <= max_diff_min]

        if df.empty:
            return None, None, None, pd.DataFrame()

        # 5. Outlier removal on gross fare
        fare_col = next((c for c in ('gross_fare', 'GrossFare', 'fare', 'Fare', 'TOTAL') if c in df.columns), None)
        if not fare_col:
            return None, None, None, pd.DataFrame()

        df[fare_col] = pd.to_numeric(df[fare_col], errors='coerce')
        df = df.dropna(subset=[fare_col])
        if df.empty:
            return None, None, None, pd.DataFrame()

        gross_fares = df[fare_col].tolist()
        filtered_fares = filter_outliers_mad(gross_fares, coverage_param=coverage_param)
        if not filtered_fares:
            filtered_fares = gross_fares

        df_valid = df[df[fare_col].isin(filtered_fares)].sort_values(by=fare_col, ascending=True)
        if df_valid.empty:
            return None, None, None, pd.DataFrame()

        min_fare = float(df_valid[fare_col].iloc[0])
        med_fare = float(np.median(df_valid[fare_col]))

        # Apply sector recalibration (discount/upsell)
        recal_fare = max(0.0, min_fare + float(recalibration_amount))
        selected_row = df_valid.iloc[0].to_dict()
        selected_row['recalibrated_fare'] = recal_fare

        return recal_fare, med_fare, selected_row, df_valid

    # =========================================================================
    # STEP 4: COMPETITION RESPONSE GRID & STRATEGY FARE SELECTION
    # =========================================================================
    @staticmethod
    def evaluate_competition_response_grid(anchor_fare, comp_fare, grid_name='DEFAULT', grid_rules=None):
        """
        Evaluate Competition Response Grid: Response Fare = Grid(A, C_comp).
        A = Anchor Fare (airline component)
        C = Selected Market Fare (airline component)
        """
        A = float(anchor_fare)
        if comp_fare is None or pd.isna(comp_fare):
            return A

        C = float(comp_fare)

        # 1. Custom grid rules evaluation if provided
        if grid_rules and isinstance(grid_rules, list):
            for rule in grid_rules:
                if isinstance(rule, dict):
                    min_a = rule.get('min_A', -float('inf'))
                    max_a = rule.get('max_A', float('inf'))
                    min_c = rule.get('min_C', -float('inf'))
                    max_c = rule.get('max_C', float('inf'))
                    if min_a <= A <= max_a and min_c <= C <= max_c:
                        # Evaluate formula or multiplier
                        mult_c = rule.get('multiplier_C', 1.0)
                        mult_a = rule.get('multiplier_A', 0.0)
                        offset = rule.get('offset', 0.0)
                        return float(mult_c * C + mult_a * A + offset)

        # 2. Default Grid Behavior per BRD:
        # If competitor fare C is lower than anchor fare A, respond to competition C; else hold A
        if C < A:
            return C
        else:
            return A

    @staticmethod
    def determine_strategy_fare(response_fare, ladder_fares):
        """
        Strategy Fare (C) is the highest fare class strictly lower than Response Fare
        among the 19 filed ladder fares.
        ladder_fares is sorted DESCENDING (f_1 > f_2 > ... > f_19).
        """
        rf = float(response_fare)
        # Find all ladder fares strictly lower than response_fare
        lower_fares = [float(f) for f in ladder_fares if float(f) < rf]
        if lower_fares:
            return max(lower_fares)  # highest among those lower

        # If response fare is lower than all filed fares, return lowest ladder fare
        return float(ladder_fares[-1])

    # =========================================================================
    # STEP 5: DEMAND RE-DISTRIBUTION & REVISED AU CALCULATION
    # =========================================================================
    @staticmethod
    def redistribute_demand_and_compute_au2(
            anchor_fare, strategy_fare, ladder_fares,
            mu_vec, sigma_vec, capacity, current_bookload,
            standard_dev_factor=0.25, first_rbd_seats=20,
            buy_up_factor=0.0, promo_factor=0.80,
            forecast_threshold_promo=95.0,
            initial_promo_au=0,
            backstop=None, hardanchor=None):
        """
        Redistribute forecast demand when Anchor Fare < Strategy Fare,
        interpolate P_eq, recompute EMSR-b AU_2, apply FirstRBD capping, Backstop, and Hardanchor.

        ladder_fares is sorted DESCENDING (f_1 > f_2 > ... > f_19).
        """
        n = len(ladder_fares)
        fares_desc = np.array([float(f) for f in ladder_fares])
        P_max = fares_desc[0]  # highest fare class
        P_min = fares_desc[-1] # lowest fare class

        A = float(anchor_fare)
        C = float(strategy_fare)
        K = float(capacity)
        CB = float(current_bookload)

        # Compute initial cumulative demand mu_bar (descending from highest to lowest class)
        initial_mu = np.array([max(0.0, float(m)) for m in mu_vec])
        initial_mu_bar = np.cumsum(initial_mu)

        # If Anchor >= Strategy Fare, no redistribution occurs (BRD rule)
        if A >= C:
            # Recompute baseline EMSR-b
            opt1 = AUOptimizationEngine.compute_emsr_b(
                initial_mu, sigma_vec, fares_desc, capacity=K,
                buy_up_factor=buy_up_factor, promo_factor=promo_factor,
                standard_dev_factor=standard_dev_factor,
                forecast_threshold_promo=forecast_threshold_promo,
                current_bookload=CB)

            au_2 = list(opt1['au'])
            au_revised = list(au_2)

            # Apply Backstop & Hardanchor to au_revised
            au_revised = AUOptimizationEngine.apply_backstop_and_hardanchor(
                au_revised, fares_desc, capacity=K, backstop=backstop, hardanchor=hardanchor)

            revised_pickup = float(np.sum(initial_mu))
            revised_flown = CB + revised_pickup
            revised_lf = (revised_flown / K) if K > 0 else 0.0

            return {
                'mu_2': initial_mu.tolist(),
                'sigma_2': [standard_dev_factor * m for m in initial_mu],
                'mu_bar_2': initial_mu_bar.tolist(),
                'AU_2': au_2,
                'AU_revised': au_revised,
                'Predicted_Pickup_Revised': revised_pickup,
                'Predicted_Flown_Revised': revised_flown,
                'Revised_LF_Forecast': revised_lf,
            }

        # --- Active Demand Redistribution (A < C) ---
        # 1. Map cumulative demand above price P: build interpolator from (fares_ascending, mu_bar_ascending)
        # In descending array: index 0 is P_max, index n-1 is P_min
        # Cumulative demand above price f_j is initial_mu_bar[j]
        fares_asc = fares_desc[::-1]
        mu_bar_asc = initial_mu_bar[::-1] # mu_bar_asc[0] is at P_min (total demand), mu_bar_asc[-1] is at P_max

        def get_initial_mu_bar(p_val):
            """Piecewise uniform linear interpolation of cumulative demand above price p_val."""
            if p_val <= P_min:
                return float(mu_bar_asc[0])
            if p_val >= P_max:
                return float(mu_bar_asc[-1])
            # Find interval [P_k, P_{k+1}] in ascending ladder
            idx = np.searchsorted(fares_asc, p_val)
            p_low = fares_asc[idx - 1]
            p_high = fares_asc[idx]
            mu_bar_low = mu_bar_asc[idx - 1]
            mu_bar_high = mu_bar_asc[idx]

            # Linear interpolation (demand decreases as price increases)
            ratio = (p_val - p_low) / (p_high - p_low) if p_high > p_low else 0.0
            return float(mu_bar_low + ratio * (mu_bar_high - mu_bar_low))

        # 2. Compute mu_bar_2 for all 19 ladder classes (descending)
        mu_bar_2 = np.zeros(n)
        mu_bar_anchor = get_initial_mu_bar(A)

        for j in range(n):
            P = fares_desc[j]
            if P < C:
                mu_bar_2[j] = 0.0
            elif math.isclose(P, C, rel_tol=1e-5):
                mu_bar_2[j] = mu_bar_anchor
            elif math.isclose(P, P_max, rel_tol=1e-5):
                mu_bar_2[j] = initial_mu_bar[0]
            else:
                # P_eq = Anchor + [(P - C) / (P_max - C)] * [P_max - Anchor]
                denom = (P_max - C) if (P_max - C) > 0 else 1.0
                P_eq = A + ((P - C) / denom) * (P_max - A)
                mu_bar_2[j] = get_initial_mu_bar(P_eq)

        # Ensure monotonicity of mu_bar_2 (descending order of fares must have non-decreasing mu_bar_2)
        for j in range(1, n):
            if fares_desc[j] >= C:
                mu_bar_2[j] = max(mu_bar_2[j], mu_bar_2[j - 1])

        # 3. Derive individual class demands mu_2 and standard deviations sigma_2
        mu_2 = np.zeros(n)
        mu_2[0] = mu_bar_2[0]
        for j in range(1, n):
            if fares_desc[j] >= C:
                mu_2[j] = max(0.0, mu_bar_2[j] - mu_bar_2[j - 1])
            else:
                mu_2[j] = 0.0

        sigma_2 = standard_dev_factor * mu_2

        # 4. Recompute EMSR-b using mu_2 and sigma_2
        opt2 = AUOptimizationEngine.compute_emsr_b(
            mu_2, sigma_2, fares_desc, capacity=K,
            buy_up_factor=buy_up_factor, promo_factor=promo_factor,
            standard_dev_factor=standard_dev_factor,
            forecast_threshold_promo=forecast_threshold_promo,
            current_bookload=CB)

        au_2 = np.array(opt2['au'])

        # AU_2 below strategy fare = 0
        for j in range(n):
            if fares_desc[j] < C:
                au_2[j] = 0

        # Current bookload floor protection:
        # If AU_2 for Strategy fare is lower than current bookload,
        # find lowest fare with AU_2 >= CB and elevate intermediate classes
        c_idx = next((j for j in range(n) if math.isclose(fares_desc[j], C, rel_tol=1e-4) or fares_desc[j] <= C), n - 1)
        if au_2[c_idx] < CB:
            # Find lowest fare class above C where au_2 >= CB
            elevate_idx = None
            for j in range(c_idx, -1, -1):
                if au_2[j] >= CB:
                    elevate_idx = j
                    break
            if elevate_idx is not None:
                elevated_val = au_2[elevate_idx]
                for j in range(elevate_idx, c_idx + 1):
                    au_2[j] = elevated_val
            else:
                # If even highest class has AU_2 < CB, elevate all classes >= C to CB
                for j in range(0, c_idx + 1):
                    au_2[j] = int(CB)

        # 5. Compute AU_revised: cap AU at Strategy Fare to (CB + FirstRBD)
        au_revised = au_2.copy()
        strat_cap = int(CB + float(first_rbd_seats))
        au_revised[c_idx] = min(int(au_2[c_idx]), strat_cap)

        # 6. Apply Backstop (Price Floor) & Hardanchor (Price Cap)
        au_revised_list = AUOptimizationEngine.apply_backstop_and_hardanchor(
            au_revised.tolist(), fares_desc, capacity=K, backstop=backstop, hardanchor=hardanchor)

        revised_pickup = float(np.sum(mu_2))
        revised_flown = CB + revised_pickup
        revised_lf = (revised_flown / K) if K > 0 else 0.0

        return {
            'mu_2': mu_2.tolist(),
            'sigma_2': sigma_2.tolist(),
            'mu_bar_2': mu_bar_2.tolist(),
            'AU_2': au_2.tolist(),
            'AU_revised': au_revised_list,
            'Predicted_Pickup_Revised': revised_pickup,
            'Predicted_Flown_Revised': revised_flown,
            'Revised_LF_Forecast': revised_lf,
        }

    # =========================================================================
    # STEP 6: PRICE FLOOR (BACKSTOP) & PRICE CAP (HARDANCHOR)
    # =========================================================================
    @staticmethod
    def apply_backstop_and_hardanchor(au_ladder, fares_ladder, capacity, backstop=None, hardanchor=None):
        """
        Adjust AU ladder for Backstop and Hardanchor:
          - Fares below Backstop -> AU = 0
          - Fares at/above highest fare lower than Hardanchor -> AU = Capacity
        """
        au = list(au_ladder)
        fares = [float(f) for f in fares_ladder]
        K = int(capacity)

        # Backstop: floor
        if backstop is not None and pd.notna(backstop):
            bs = float(backstop)
            for j in range(len(fares)):
                if fares[j] < bs:
                    au[j] = 0

        # Hardanchor: cap
        if hardanchor is not None and pd.notna(hardanchor):
            ha = float(hardanchor)
            # Find highest fare lower than hardanchor
            lower_fares = [fares[j] for j in range(len(fares)) if fares[j] < ha]
            if lower_fares:
                target_fare = max(lower_fares)
                for j in range(len(fares)):
                    if fares[j] >= target_fare:
                        au[j] = K

        return au

    # =========================================================================
    # STEP 7: CLOSED-FORM REVENUE FORECAST (AF_FORECAST)
    # =========================================================================
    @staticmethod
    def calculate_revenue_forecast(mu_2_vec, sigma_2_vec, ladder_fares,
                                   au_revised_vec, current_bookload,
                                   current_revenue, capacity,
                                   predicted_flown_revised):
        """
        Calculate expected accepted bookings (A_hat_i) and expected revenue (ER_i)
        processed bottom-up from lowest fare class to highest fare class.

        Formula:
          A_i = max(0, BL_i - CB - sum_{k=i+1}^n A_hat_k)
          z_i = (A_i - mu_i) / sigma_i
          A_hat_i = mu_i * Phi(z_i) - sigma_i * phi(z_i) + A_i * (1 - Phi(z_i))
          ER_i = f_i * A_hat_i
          AF_Forecast = Predicted Revenue / min(Capacity, Predicted Flown Revised)
        """
        n = len(ladder_fares)
        fares = np.array([float(f) for f in ladder_fares]) # descending
        mu = np.array([max(0.0, float(m)) for m in mu_2_vec])
        sigma = np.array([max(0.0, float(s)) for s in sigma_2_vec])
        au_rev = np.array([int(a) for a in au_revised_vec])
        CB = float(current_bookload)
        K = float(capacity)
        curr_rev = max(0.0, float(current_revenue)) if pd.notna(current_revenue) else 0.0

        A_avail = np.zeros(n)
        A_hat = np.zeros(n)
        ER = np.zeros(n)

        # Process bottom-up from lowest class (index n-1) to highest class (index 0)
        sum_lower_accepted = 0.0

        for j in range(n - 1, -1, -1):
            BL_i = float(au_rev[j])
            avail = max(0.0, BL_i - CB - sum_lower_accepted)
            A_avail[j] = avail

            if avail <= 0 or (mu[j] <= 0 and sigma[j] <= 0):
                exp_accepted = 0.0
            elif sigma[j] > 1e-6:
                z_i = (avail - mu[j]) / sigma[j]
                cdf_val = norm_cdf(z_i)
                pdf_val = norm_pdf(z_i)
                exp_accepted = (mu[j] * cdf_val) - (sigma[j] * pdf_val) + (avail * (1.0 - cdf_val))
                exp_accepted = max(0.0, min(exp_accepted, avail))
            else:
                exp_accepted = min(mu[j], avail)

            A_hat[j] = exp_accepted
            ER[j] = fares[j] * exp_accepted
            sum_lower_accepted += exp_accepted

        total_expected_revenue = float(np.sum(ER))
        predicted_revenue = curr_rev + total_expected_revenue
        flown_denom = min(K, float(predicted_flown_revised)) if predicted_flown_revised > 0 else K
        af_forecast = (predicted_revenue / flown_denom) if flown_denom > 0 else 0.0

        return {
            'Available_Inventory': A_avail.tolist(),
            'Expected_Bookings': A_hat.tolist(),
            'Expected_Revenue': ER.tolist(),
            'Total_Expected_Revenue': total_expected_revenue,
            'Predicted_Revenue': predicted_revenue,
            'AF_Forecast': af_forecast,
        }
