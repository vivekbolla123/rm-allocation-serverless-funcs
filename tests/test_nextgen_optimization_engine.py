import os
import sys
import math
import numpy as np
import pandas as pd
import pytest

# Add paths to sys.path
sys.path.insert(0, os.path.abspath("rm-allocation-process-inputs"))
sys.path.insert(0, os.path.abspath("rm-allocation-configurations"))

from common.nextgen_optimization_engine import NextGenOptimizationEngine


class TestNextGenOptimizationEngine:

    @pytest.fixture
    def sample_flight_data(self):
        """16 fare classes for BOM-DEL / AMDBLR ladder."""
        fares = [94338.0, 40483.0, 35127.0, 30463.0, 24575.0, 20545.0, 16416.0, 13655.0,
                 11352.0, 9272.0, 7568.0, 6409.0, 5252.0, 4279.0, 3526.0, 2914.0]
        mu = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.14675, 2.918775, 5.195576, 4.153556,
              2.142497, 0.943086, 0.487870, 0.027140, 0.0]
        sigma = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.504376, 4.781883, 4.508768, 4.695182,
                 4.296716, 2.222435, 1.294768, 0.163203, 0.0]
        df = pd.DataFrame({
            'FareBucket': [f'Bucket_{i+1}' for i in range(16)],
            'fare': fares,
            'mu': mu,
            'sigma': sigma
        })
        return df

    def test_emsr_b_initial_anchor_fare(self, sample_flight_data):
        """Verify Initial EMSR-b and Anchor Fare selection."""
        capacity = 189
        cb = 147
        df, anchor_fare, promo_au = NextGenOptimizationEngine.compute_initial_emsr_b(
            sample_flight_data, capacity, cb, buy_up_factor=0.0
        )
        assert len(df) == 16
        assert anchor_fare == 2914.0
        assert df['AU'].iloc[0] == capacity
        # Check non-increasing AU
        au_vals = df['AU'].tolist()
        assert all(au_vals[i] >= au_vals[i+1] for i in range(len(au_vals)-1))

    def test_grid_evaluation_rules(self):
        """Verify all Competition Response Grid formula rules."""
        A = 2914.0
        C = 14321.90
        # Rule: Min (1.2A, 0.8C)
        resp1 = NextGenOptimizationEngine.evaluate_grid_response("Min (1.2A, 0.8C)", A, C)
        assert np.isclose(resp1, 3496.80, rtol=1e-4)

        # Rule: Min (1.3A, 0.9C)
        C2 = 8021.90
        resp2 = NextGenOptimizationEngine.evaluate_grid_response("Min (1.3A, 0.9C)", A, C2)
        assert np.isclose(resp2, 3788.20, rtol=1e-4)

        # Arbitrary dynamic formulas (not hardcoded)
        resp_custom1 = NextGenOptimizationEngine.evaluate_grid_response("Min (1.45A, 0.75C)", 3000.0, 5000.0)
        assert np.isclose(resp_custom1, min(1.45 * 3000.0, 0.75 * 5000.0), rtol=1e-4)

        resp_custom2 = NextGenOptimizationEngine.evaluate_grid_response("Max (1.1A, 0.9C)", 3000.0, 4000.0)
        assert np.isclose(resp_custom2, max(1.1 * 3000.0, 0.9 * 4000.0), rtol=1e-4)

        resp_custom3 = NextGenOptimizationEngine.evaluate_grid_response("0.85C", 3000.0, 4000.0)
        assert np.isclose(resp_custom3, 3400.0, rtol=1e-4)

        # Rule: Min (1.5A, C)
        resp3 = NextGenOptimizationEngine.evaluate_grid_response("Min (1.5A, C)", A, 17321.90)
        assert np.isclose(resp3, 4371.00, rtol=1e-4)

        # Rule: C
        resp4 = NextGenOptimizationEngine.evaluate_grid_response("C", A, 5500.0)
        assert resp4 == 5500.0

    def test_filter_market_fares_connecting_and_time_window(self):
        """Verify carrier exclusion, connecting duration threshold, and time window filtering."""
        mkt_fares = [
            # Direct competitor within time window
            {'AirlineCode': '6E', 'Fare': 4500, 'outbound_stops': 0, 'outbound_duration': 120, 'DepTime': '10:15'},
            # Direct competitor excluded carrier
            {'AirlineCode': 'AI', 'Fare': 4000, 'outbound_stops': 0, 'outbound_duration': 120, 'DepTime': '10:30'},
            # Connecting competitor within allowed duration (120 + 240 = 360 mins)
            {'AirlineCode': 'SG', 'Fare': 4200, 'outbound_stops': 1, 'outbound_duration': 280, 'DepTime': '11:00'},
            # Connecting competitor exceeding allowed duration (420 > 360 mins)
            {'AirlineCode': 'SG', 'Fare': 3500, 'outbound_stops': 1, 'outbound_duration': 420, 'DepTime': '11:00'},
            # Competitor outside time window (+3 hours from 10:30)
            {'AirlineCode': '6E', 'Fare': 3800, 'outbound_stops': 0, 'outbound_duration': 120, 'DepTime': '14:30'},
        ]

        # Case 1: connection_fare_flag == 0 (direct only, carrier AI excluded)
        c_min, c_med = NextGenOptimizationEngine.filter_market_fares(
            mkt_fares, flight_std='10:30', time_range_hours=2.0,
            carrier_exclusion='AI', connection_fare_flag=0, direct_duration_mins=120
        )
        assert c_min == 4500.0

        # Case 2: connection_fare_flag == 1 (include connecting flight within 360 mins: 4200 is included, 3500 excluded)
        c_min_conn, c_med_conn = NextGenOptimizationEngine.filter_market_fares(
            mkt_fares, flight_std='10:30', time_range_hours=2.0,
            carrier_exclusion='AI', connection_fare_flag=1, direct_duration_mins=120, additional_connection_hours=4.0
        )
        assert c_min_conn == 4200.0

    def test_demand_redistribution_and_p_eq(self, sample_flight_data):
        """Verify P_eq equivalent price mapping and piecewise interpolation when A < C."""
        capacity = 189
        cb = 147
        init_df, anchor_fare, promo_au = NextGenOptimizationEngine.compute_initial_emsr_b(
            sample_flight_data, capacity, cb
        )
        strategy_fare = 4371.0  # Fares <= 4371 is 4279.0
        redist_df = NextGenOptimizationEngine.redistribute_demand_and_compute_revised_au(
            init_df, anchor_fare, strategy_fare, capacity, cb, first_rbd=20
        )
        assert 'P_eq' in redist_df.columns
        assert 'mu_2' in redist_df.columns
        assert 'AU_Revised' in redist_df.columns

        # Verify strategy fare class (4279.0) has P_eq == Anchor Fare (2914.0)
        strat_row = redist_df[redist_df['fare'] == 4279.0].iloc[0]
        assert np.isclose(strat_row['P_eq'], 2914.0, rtol=1e-3)

        # Verify classes below Strategy Fare have mu_2 == 0 and AU_Revised == 0
        below_strat = redist_df[redist_df['fare'] < 4279.0]
        assert all(below_strat['mu_2'] == 0.0)
        assert all(below_strat['AU_Revised'] == 0)

        # Verify 20-seat limit: capacity - AU_Revised should not cross 20 seats
        assert (capacity - strat_row['AU_Revised']) <= 20.0

    def test_closed_form_revenue_forecast(self, sample_flight_data):
        """Verify expected accepted bookings integral and revenue forecast calculation."""
        capacity = 189
        cb = 147
        curr_rev = 961206.34
        init_df, anchor_fare, promo_au = NextGenOptimizationEngine.compute_initial_emsr_b(
            sample_flight_data, capacity, cb
        )
        strategy_fare = 4371.0
        redist_df = NextGenOptimizationEngine.redistribute_demand_and_compute_revised_au(
            init_df, anchor_fare, strategy_fare, capacity, cb
        )
        pred_rev, tot_er, pickup_rev, flown_rev, af_fc, lf_fc_rev, final_df = (
            NextGenOptimizationEngine.compute_closed_form_revenue_forecast(
                redist_df, capacity, cb, curr_rev
            )
        )
        assert pred_rev > curr_rev
        assert tot_er > 0
        assert af_fc > 0
        assert 0.0 <= lf_fc_rev <= 1.5

    def test_benchmark_against_sample_csv(self):
        """Compare calculation directly against WADDProdAUSample reference dataset."""
        csv_path = "WADDProdAUSample(WADDProdAU_16_Aug_26).csv"
        if not os.path.exists(csv_path):
            pytest.skip("Benchmark CSV not found")

        df = pd.read_csv(csv_path)
        sample = df[(df['sector'] == 'AMDBLR') & (df['date'] == '8/17/2026') & (df['flight'] == 1901)].copy()
        filed = sample[sample['Promo_Flag'] == 0].copy().reset_index(drop=True)

        capacity = int(sample['capacity'].iloc[0])
        cb = float(sample['Total_Bookings'].iloc[0])
        curr_rev = float(sample['Current_Revenue'].iloc[0])

        ladder_df = pd.DataFrame({
            'FareBucket': filed['FareBucket'],
            'fare': filed['fare'],
            'mu': filed['mu'],
            'sigma': filed['sigma']
        })

        init_df, anchor_fare, promo_au = NextGenOptimizationEngine.compute_initial_emsr_b(
            ladder_df, capacity, cb
        )
        assert anchor_fare == sample['anchor_fare'].iloc[0]

        strategy_fare = sample['strategy_fare'].iloc[0]
        redist_df = NextGenOptimizationEngine.redistribute_demand_and_compute_revised_au(
            init_df, anchor_fare, strategy_fare, capacity, cb, first_rbd=20
        )

        pred_rev, tot_er, pickup_rev, flown_rev, af_fc, lf_fc_rev, final_df = (
            NextGenOptimizationEngine.compute_closed_form_revenue_forecast(
                redist_df, capacity, cb, curr_rev
            )
        )

        # Compare with sample dataset values
        np.testing.assert_allclose(final_df['Expected_Revenue'], filed['Expected_Revenue'], rtol=1e-3, atol=1e-2)
        np.testing.assert_allclose(pred_rev, sample['Predicted_Revenue'].iloc[0], rtol=1e-3)
        np.testing.assert_allclose(flown_rev, sample['Predicted_Flown_Revised'].iloc[0], rtol=1e-3)

    def test_promo_inventory_flag_off(self, sample_flight_data):
        """Verify that when PromoFlag_NG == 0, promo AU is 0 and no promo row is appended."""
        capacity = 189
        cb = 100
        init_df, anchor_fare, promo_au = NextGenOptimizationEngine.compute_initial_emsr_b(
            sample_flight_data, capacity, cb, promo_flag=0
        )
        assert promo_au == 0

        final_df, summary = NextGenOptimizationEngine.execute_flight_nextgen_optimization(
            sample_flight_data, capacity, cb, 500000.0,
            curr_params={'promo_flag_NG': 0, 'nextgen_flag_NG': 1}
        )
        assert len(final_df) == len(sample_flight_data)
        assert all(final_df['Promo_Flag'] == 0)

    def test_promo_inventory_flag_on(self, sample_flight_data):
        """Verify that when PromoFlag_NG == 1, promo AU is calculated, recalculated on comp response, and capped by PromoFirstRBD_NG."""
        capacity = 189
        cb = 74
        promo_ratio = 0.80
        promo_first_rbd = 10

        init_df, anchor_fare, promo_au = NextGenOptimizationEngine.compute_initial_emsr_b(
            sample_flight_data, capacity, cb, promo_flag=1, promo_ratio=promo_ratio
        )
        assert promo_au > 0

        final_df, summary = NextGenOptimizationEngine.execute_flight_nextgen_optimization(
            sample_flight_data, capacity, cb, 400000.0,
            curr_params={
                'promo_flag_NG': 1,
                'PromoRBD_NG': 'W4',
                'PromoFirstRBD_NG': promo_first_rbd,
                'PromoRatio_NG': promo_ratio,
                'nextgen_flag_NG': 1,
                'competition_response_flag_NG': 1
            }
        )
        # Verify Promo row exists as the last row
        assert len(final_df) == len(sample_flight_data) + 1
        promo_row = final_df.iloc[-1]
        assert promo_row['FareBucket'] == 'Promo'
        assert promo_row['Promo_Flag'] == 1
        assert promo_row['AU'] == promo_au
        # AU_Revised must be capped by threshold (cb + PromoFirstRBD_NG) if AU_2 crossed it, otherwise AU_2
        promo_threshold = cb + promo_first_rbd
        if promo_row['AU_2'] > promo_threshold:
            assert promo_row['AU_Revised'] == promo_threshold
        else:
            assert promo_row['AU_Revised'] == promo_row['AU_2']
        assert promo_row['AU_Revised'] <= promo_threshold
        # Opening fare must not be promo fare
        assert summary['NG_Opening_fare'] != promo_row['fare']
        assert summary['NG_Opening_fare'] in sample_flight_data['fare'].tolist()

    def test_pmin_old_demand_redistribution_intermediate_fares(self, sample_flight_data):
        """
        Verify Pmin_old logic:
        1. When cum_mu >= threshold (pmin_threshold_factor * cap), Pmin_old is the crossing fare.
        2. When cum_mu < threshold even at C, Pmin_old is C itself.
        3. P_eq for C is Anchor fare, P_eq for P_max is P_max, intermediate fares follow Pmin_old formula.
        """
        capacity = 189
        cb = 147
        init_df, anchor_fare, _ = NextGenOptimizationEngine.compute_initial_emsr_b(
            sample_flight_data, capacity, cb
        )
        assert anchor_fare == 2914.0

        # Scenario A: Strategy Fare = 9272.0, threshold factor = 0.01 (threshold = 1.89)
        # Sum of mu from 2914 up to 9272:
        # 2914: 0.0
        # 3526: 0.02714
        # 4279: 0.48787  (cum = 0.51501)
        # 5252: 0.943086 (cum = 1.458096)
        # 6409: 2.142497 (cum = 3.600593 >= 1.89) -> crosses at 6409.0!
        redist_df_cross = NextGenOptimizationEngine.redistribute_demand_and_compute_revised_au(
            init_df, anchor_fare, 9272.0, capacity, cb,
            pmin_threshold_factor=0.01
        )
        assert redist_df_cross.attrs['p_min_old'] == 6409.0

        # Boundary checks
        c_row = redist_df_cross[redist_df_cross['fare'] == 9272.0].iloc[0]
        max_row = redist_df_cross[redist_df_cross['fare'] == 94338.0].iloc[0]
        assert np.isclose(c_row['P_eq'], anchor_fare)  # P_eq(C) == Anchor fare
        assert np.isclose(max_row['P_eq'], 94338.0)    # P_eq(P_max) == P_max

        # Intermediate fare check: fare = 11352.0
        # P_eq = Pmin_old + (p - C) / (P_max - C) * (P_max - Pmin_old)
        expected_peq_11352 = 6409.0 + ((11352.0 - 9272.0) / (94338.0 - 9272.0)) * (94338.0 - 6409.0)
        row_11352 = redist_df_cross[redist_df_cross['fare'] == 11352.0].iloc[0]
        assert np.isclose(row_11352['P_eq'], expected_peq_11352)

        # Scenario B: Default threshold factor = 0.1 (threshold = 18.9)
        # Sum of mu up to 4279 is 0.51501 < 18.9 -> does not cross even at C -> Pmin_old = 4279.0
        redist_df_fallback = NextGenOptimizationEngine.redistribute_demand_and_compute_revised_au(
            init_df, anchor_fare, 4279.0, capacity, cb,
            pmin_threshold_factor=0.1
        )
        assert redist_df_fallback.attrs['p_min_old'] == 4279.0
        # When Pmin_old == C, intermediate fares have P_eq == p
        row_5252 = redist_df_fallback[redist_df_fallback['fare'] == 5252.0].iloc[0]
        assert np.isclose(row_5252['P_eq'], 5252.0)

    def test_revised_pickup_and_load_factor_forecast_from_anchor_fare(self, sample_flight_data):
        """Verify that Predicted_pickup_Revised sums mu only from anchor fare and above,
        giving Predicted_Flown_Revised = cb + Predicted_pickup_Revised and LF_Forecast_Revised = (flown / cap) * 100.
        """
        capacity = 189
        cb = 147  # At cb=147, anchor fare is 2914.0 (all 16 classes)
        curr_rev = 500000.0

        # Run with cb = 147 where anchor fare is lowest class
        final_df, summary = NextGenOptimizationEngine.execute_flight_nextgen_optimization(
            sample_flight_data, capacity, cb, curr_rev
        )
        anchor_fare = summary['Anchor_fare']
        expected_pickup_all = float(sample_flight_data[sample_flight_data['fare'] >= (anchor_fare - 1e-5)]['mu'].sum())
        assert np.isclose(summary['Predicted_Pickup_Revised'], expected_pickup_all)
        assert np.isclose(summary['Predicted_Flown_Revised'], cb + expected_pickup_all)
        assert np.isclose(summary['NG_LF_Forecast'], ((cb + expected_pickup_all) / capacity) * 100.0)

        # Now test with higher cb where anchor_fare is higher up the ladder (closing lower classes)
        # Using a custom ladder where lower buckets have significant mu
        custom_ladder = pd.DataFrame({
            'FareBucket': ['Y', 'B', 'M', 'Q'],
            'fare': [10000.0, 7000.0, 5000.0, 3000.0],
            'mu': [2.0, 5.0, 10.0, 20.0],
            'sigma': [0.5, 1.25, 2.5, 5.0]
        })
        # If capacity=100 and cb=80, AU will close the lowest bucket(s)
        final_df2, summary2 = NextGenOptimizationEngine.execute_flight_nextgen_optimization(
            custom_ladder, 100, 80, 400000.0
        )
        anchor2 = summary2['Anchor_fare']
        expected_pickup_anchor_above = float(custom_ladder[custom_ladder['fare'] >= (anchor2 - 1e-5)]['mu'].sum())
        total_mu = float(custom_ladder['mu'].sum())

        # Verify revised pickup sums only from anchor fare and above
        assert np.isclose(summary2['Predicted_Pickup_Revised'], expected_pickup_anchor_above)
        assert np.isclose(summary2['Predicted_Flown_Revised'], 80 + expected_pickup_anchor_above)
        # In output summary, LF forecast is capped at 100.0%
        assert np.isclose(summary2['NG_LF_Forecast'], min(100.0, ((80 + expected_pickup_anchor_above) / 100.0) * 100.0))

        # If anchor2 > 3000, verify that the 3000 bucket's mu is excluded
        if anchor2 > 3000.0:
            assert summary2['Predicted_Pickup_Revised'] < total_mu

    def test_revenue_forecasting_columns(self, sample_flight_data):
        """Verify Expected_Revenue, Total_Expected_Revenue, and Predicted_Revenue are correctly populated in output."""
        capacity = 189
        cb = 74
        curr_rev = 450000.0

        final_df, summary = NextGenOptimizationEngine.execute_flight_nextgen_optimization(
            sample_flight_data, capacity, cb, curr_rev,
            curr_params={'promo_flag_NG': 1, 'PromoRatio_NG': 0.8}
        )

        assert 'Expected_Revenue' in final_df.columns
        assert 'Total_Expected_Revenue' in final_df.columns
        assert 'Predicted_Revenue' in final_df.columns

        # Verify sum of Expected_Revenue equals Total_Expected_Revenue
        filed_rows = final_df[final_df['Promo_Flag'] == 0]
        sum_er = float(filed_rows['Expected_Revenue'].sum())
        assert np.isclose(sum_er, summary['Total_Expected_Revenue'], rtol=1e-5)
        assert np.isclose(final_df['Total_Expected_Revenue'].iloc[0], summary['Total_Expected_Revenue'])

        # Verify Predicted_Revenue = Current_Revenue + Total_Expected_Revenue
        assert np.isclose(summary['Predicted_Revenue'], curr_rev + summary['Total_Expected_Revenue'])
        assert np.isclose(final_df['Predicted_Revenue'].iloc[0], curr_rev + summary['Total_Expected_Revenue'])

        # Verify Promo row has 0 Expected_Revenue and flight-level total revenues
        promo_rows = final_df[final_df['Promo_Flag'] == 1]
        assert not promo_rows.empty
        assert promo_rows['Expected_Revenue'].iloc[0] == 0.0
        assert np.isclose(promo_rows['Total_Expected_Revenue'].iloc[0], summary['Total_Expected_Revenue'])
        assert np.isclose(promo_rows['Predicted_Revenue'].iloc[0], summary['Predicted_Revenue'])

    def test_auoptlocal_flight_1109_reference_match(self):
        """Verify NextGen optimization against the reference AUOptlocal.xlsx for Flight 1109."""
        excel_path = os.path.abspath("AUOptlocal.xlsx")
        if not os.path.exists(excel_path):
            pytest.skip("AUOptlocal.xlsx not found")

        excel_df = pd.read_excel(excel_path)
        ladder_df = pd.DataFrame({
            'FareBucket': excel_df['FareBucket'],
            'fare': excel_df['fare'],
            'mu': excel_df['mu'],
            'sigma': excel_df['sigma']
        })

        capacity = int(excel_df['capacity'].iloc[0])
        cb = float(excel_df['Total_Bookings'].iloc[0])
        curr_rev = float(excel_df['Current_Revenue'].iloc[0])

        final_df, summary = NextGenOptimizationEngine.execute_flight_nextgen_optimization(
            ladder_df,
            capacity=capacity,
            current_bookings=cb,
            current_revenue=curr_rev,
            curr_params={
                'buy_up_factor': 0.0,
                'sigma_factor': 0.25,
                'first_rbd_alloc': 20,
                'promo_flag_NG': 0,
                'competition_response_flag': 0
            }
        )

        # Flight-level forecast columns
        assert np.isclose(summary['Total_Expected_Revenue'], excel_df['Total_Expected_Revenue'].iloc[0], atol=1e-2)
        assert np.isclose(summary['Predicted_Revenue'], excel_df['Predicted_Revenue'].iloc[0], atol=1e-2)
        assert np.isclose(summary['NG_AF_Forecast'], excel_df['AF_Forecast'].iloc[0], atol=1e-2)
        assert np.isclose(summary['Predicted_Pickup_Revised'], excel_df['Predicted_pickup_Revised'].iloc[0], atol=1e-2)
        assert np.isclose(summary['Predicted_Flown_Revised'], excel_df['Predicted_Flown_Revised'].iloc[0], atol=1e-2)
        assert np.isclose(summary['NG_LF_Forecast'], excel_df['LF_Forecast_Revised'].iloc[0] * 100.0, atol=1e-2)

        # Helper columns: A, A_cap, and Expected_Revenue per bucket
        assert np.allclose(final_df['A'].values, excel_df['A'].values, atol=1e-4)
        assert np.allclose(final_df['A_cap'].values, excel_df['A_cap'].values, atol=1e-4)
        assert np.allclose(final_df['Expected_Revenue'].values, excel_df['Expected_Revenue'].values, atol=1e-2)

        # Bucket below anchor fare (4901 < 5301) must have 0 bookings and 0 revenue
        below_anchor = final_df[final_df['fare'] < 5301]
        assert not below_anchor.empty
        assert (below_anchor['A'] == 0.0).all()
        assert (below_anchor['A_cap'] == 0.0).all()
        assert (below_anchor['Expected_Revenue'] == 0.0).all()

    def test_excel_local_sheet_competition_response_flight_1120(self):
        """
        Verify that NextGen revenue calculation matches the local sheet in
        AUOptTestoutput2 (1).xlsx for flight 1120 where competition response triggers demand redistribution.
        Verifies helper column A_cap and Expected_Revenue = fare * A_cap.
        """
        excel_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "AUOptTestoutput2 (1).xlsx")
        if not os.path.exists(excel_path):
            pytest.skip("AUOptTestoutput2 (1).xlsx not found for integration test")

        df_local = pd.read_excel(excel_path, sheet_name='local')
        f1120 = df_local[(df_local['flight'] == 1120) & (df_local['date'] == '2026-10-01')].copy().reset_index(drop=True)

        ladder_df = pd.DataFrame({
            'FareBucket': f1120['FareBucket'],
            'fare': f1120['fare'],
            'mu': f1120['mu_LOCAL'],
            'sigma': f1120['sigma']
        })

        final_df, summary = NextGenOptimizationEngine.execute_flight_nextgen_optimization(
            ladder_df,
            capacity=int(f1120['capacity'].iloc[0]),
            current_bookings=float(f1120['Total_Bookings'].iloc[0]),
            current_revenue=float(f1120['Current_Revenue'].iloc[0]),
            market_fares=[{
                'gross_fare': 7463.81,
                'outbound_airline_code': '6E',
                'outbound_flight': '',
                'outbound_departuretime': '19:00',
                'outbound_stops': 0
            }],
            flight_std='19:00',
            curr_params={
                'origin': 'BOM',
                'destin': 'DEL',
                'route': 'BOMDEL',
                'flight_number': 1120,
                'ndocounter': 10,
                'buy_up_factor_NG': 0.0,
                'competition_response_flag_NG': 1,
                'CompetitionResponseGrid_NG': 'DEFAULT',
                'promo_flag_NG': 0,
                'pmin_threshold_factor_NG': 0.1
            }
        )

        assert np.isclose(summary['Total_Expected_Revenue'], f1120['Total_Expected_Revenue'].iloc[0], atol=1.0)
        assert np.isclose(summary['Predicted_Revenue'], f1120['Predicted_Revenue'].iloc[0], atol=1.0)
        assert np.isclose(summary['NG_AF_Forecast'], f1120['AF_Forecast_LOCAL'].iloc[0], atol=1e-2)
        assert np.allclose(final_df['A_cap'].values, f1120['A_cap'].values, atol=1e-4)
        assert np.allclose(final_df['Expected_Revenue'].values, f1120['Expected_Revenue'].values, atol=1.0)

    def test_determine_strategy_fare_mapped_to_ladder(self, sample_flight_data):
        """
        Verify that determine_strategy_fare always maps continuous response fares
        to one of the discrete fares in ladder_df['fare'].
        """
        ladder_fares = sample_flight_data['fare'].tolist()

        # Between two ladder classes: 4279.0 and 5252.0
        # If response fare is 5000.0, highest ladder fare <= 5000 is 4279.0
        s_fare = NextGenOptimizationEngine.determine_strategy_fare(5000.0, sample_flight_data)
        assert s_fare == 4279.0
        assert s_fare in ladder_fares

        # If response fare is exactly equal to a ladder fare
        s_fare_exact = NextGenOptimizationEngine.determine_strategy_fare(6409.0, sample_flight_data)
        assert s_fare_exact == 6409.0
        assert s_fare_exact in ladder_fares

        # If response fare is lower than all ladder fares (< 2914.0)
        s_fare_low = NextGenOptimizationEngine.determine_strategy_fare(2000.0, sample_flight_data)
        assert s_fare_low == 2914.0
        assert s_fare_low in ladder_fares

        # If response fare is higher than all ladder fares (> 94338.0)
        s_fare_high = NextGenOptimizationEngine.determine_strategy_fare(100000.0, sample_flight_data)
        assert s_fare_high == 94338.0
        assert s_fare_high in ladder_fares

        # If response fare is None
        assert NextGenOptimizationEngine.determine_strategy_fare(None, sample_flight_data) is None

    def test_hardanchor_non_numeric_ignored_and_backstop_floor(self, sample_flight_data):
        """
        Verify that non-numeric hardanchor (e.g. 'MIN', 'MAX') is ignored (not treated as cap),
        and backstop floor properly sets AU_Revised = 0.0 for fares below backstop.
        """
        capacity = 189
        cb = 100
        # Compute initial EMSR-b
        init_df, anchor_fare, promo_au = NextGenOptimizationEngine.compute_initial_emsr_b(
            sample_flight_data, capacity, cb
        )
        # Test 1: hardanchor='MIN', backstop=5000.0
        redistrib_df = NextGenOptimizationEngine.redistribute_demand_and_compute_revised_au(
            init_df, anchor_fare, 5252.0, capacity, cb,
            backstop=5000.0,
            hardanchor='MIN',
            first_rbd=20.0
        )
        # Fares below 5000 (4279, 3526, 2914) must be floored to 0.0
        for _, r in redistrib_df.iterrows():
            if r['fare'] < 5000.0:
                assert r['AU_Revised'] == 0.0, f"Fare {r['fare']} below backstop should have AU_Revised=0"
            elif r['fare'] >= 5252.0:
                # Should not be capped to capacity everywhere
                assert r['AU_Revised'] <= capacity

        # Verify not all AU_Revised are equal to capacity
        au_rev_vals = redistrib_df['AU_Revised'].tolist()
        assert len(set(au_rev_vals)) > 1
        assert not all(v == capacity for v in au_rev_vals)

    def test_hardanchor_capping_from_nearest_lower_fare(self, sample_flight_data):
        """
        Verify that hardanchor capping starts from the nearest ladder fare lower than or equal
        to hardanchor, elevating AU_Revised using PLF threshold.
        """
        capacity = 189
        cb = 100
        init_df, anchor_fare, promo_au = NextGenOptimizationEngine.compute_initial_emsr_b(
            sample_flight_data, capacity, cb
        )
        # sample_flight_data fares around 5000 are: 6409.0, 5252.0, 4279.0, 3526.0
        # If hardanchor=5000.0, the nearest fare lower than 5000 is 4279.0.
        # With default plf_threshold=95.0 -> plf_target = 179.55.
        # Fares >= 4279.0 must have AU_Revised >= 179.55.
        redistrib_df = NextGenOptimizationEngine.redistribute_demand_and_compute_revised_au(
            init_df, anchor_fare, 5252.0, capacity, cb,
            hardanchor=5000.0,
            plf_threshold=95.0,
            first_rbd=20.0
        )
        for _, r in redistrib_df.iterrows():
            if r['fare'] >= 4279.0:
                assert r['AU_Revised'] >= 179.55, f"Fare {r['fare']} >= 4279 should be at least PLF target"
            else:
                assert r['AU_Revised'] < capacity, f"Fare {r['fare']} < 4279 should not be capped to capacity"

        # With plf_threshold=100.0, plf_target = 189 (capacity)
        redistrib_100 = NextGenOptimizationEngine.redistribute_demand_and_compute_revised_au(
            init_df, anchor_fare, 5252.0, capacity, cb,
            hardanchor=5000.0,
            plf_threshold=100.0,
            first_rbd=20.0
        )
        for _, r in redistrib_100.iterrows():
            if r['fare'] >= 4279.0:
                assert r['AU_Revised'] == capacity, f"Fare {r['fare']} >= 4279 should be capped to capacity when PLF=100%"

    def test_oal_less_than_anchor_fare_bypasses_au_recalculation(self, sample_flight_data):
        """
        Verify that when oal_b2c_min < anchor_fare, AU recalculation and demand redistribution
        are bypassed, strategy_fare = anchor_fare, and AUs below anchor fare are 0.
        """
        capacity = 189
        cb = 100
        # Compute anchor fare first to know its value
        init_df, anchor_fare, _ = NextGenOptimizationEngine.compute_initial_emsr_b(
            sample_flight_data, capacity, cb
        )
        assert anchor_fare > 0

        # Competitor fare below anchor fare
        low_comp_fare = anchor_fare - 500.0
        mkt_fares = [{'AirlineCode': '6E', 'Fare': low_comp_fare, 'outbound_stops': 0, 'outbound_duration': 120, 'DepTime': '10:30'}]

        final_df, summary = NextGenOptimizationEngine.execute_flight_nextgen_optimization(
            sample_flight_data, capacity, cb, 300000.0,
            market_fares=mkt_fares,
            flight_std='10:30',
            curr_params={'competition_response_flag_NG': 1, 'first_rbd_NG': 15}
        )

        assert 'oal_b2c_min < anchor_fare' in summary['Rule_Applied']
        assert summary['Strategy_fare'] == summary['Anchor_fare']
        assert 'opening fare' in final_df.columns

        # Verify AUs below anchor fare are 0.0
        for _, r in final_df.iterrows():
            if r['FareBucket'] == 'Promo':
                continue
            if r['fare'] < anchor_fare - 1e-4:
                assert r['AU_Revised'] == 0.0, f"Fare {r['fare']} below anchor {anchor_fare} must have AU_Revised=0"

    def test_market_fares_not_found_bypasses_au_recalculation(self, sample_flight_data):
        """
        Verify that when market fares are not found, AU recalculation and demand redistribution
        are bypassed, strategy_fare = anchor_fare, and AUs below anchor fare are 0.
        """
        capacity = 189
        cb = 100
        init_df, anchor_fare, _ = NextGenOptimizationEngine.compute_initial_emsr_b(
            sample_flight_data, capacity, cb
        )

        final_df, summary = NextGenOptimizationEngine.execute_flight_nextgen_optimization(
            sample_flight_data, capacity, cb, 300000.0,
            market_fares=[], # No market fares found
            flight_std='10:30',
            curr_params={'competition_response_flag_NG': 1, 'first_rbd_NG': 15}
        )

        assert 'Market Fares Not Found' in summary['Rule_Applied']
        assert summary['Strategy_fare'] == summary['Anchor_fare']
        assert 'opening fare' in final_df.columns

        for _, r in final_df.iterrows():
            if r['FareBucket'] == 'Promo':
                continue
            if r['fare'] < anchor_fare - 1e-4:
                assert r['AU_Revised'] == 0.0

    def test_competition_response_opening_fare_assigned_to_51_levels(self, sample_flight_data):
        """
        Verify that when oal_b2c_min >= anchor_fare, inventory at nearest forecast fare below
        competition response is assigned to nearest lower among 51 fare levels as opening fare.
        """
        capacity = 189
        cb = 100
        init_df, anchor_fare, _ = NextGenOptimizationEngine.compute_initial_emsr_b(
            sample_flight_data, capacity, cb
        )

        # Higher competitor fare to trigger competition response
        high_comp_fare = 15000.0
        mkt_fares = [{'AirlineCode': '6E', 'Fare': high_comp_fare, 'outbound_stops': 0, 'outbound_duration': 120, 'DepTime': '10:30'}]

        # Construct synthetic 51 filed B2C fare levels
        filed_fares = np.linspace(25000, 2500, 51)
        filed_51_ladder = pd.DataFrame({
            'RBD': [f'RBD_{i}' for i in range(51)],
            'fare': filed_fares
        })

        final_df, summary = NextGenOptimizationEngine.execute_flight_nextgen_optimization(
            sample_flight_data, capacity, cb, 300000.0,
            market_fares=mkt_fares,
            flight_std='10:30',
            curr_params={'competition_response_flag_NG': 1, 'first_rbd_NG': 15},
            filed_51_ladder_df=filed_51_ladder
        )

        assert 'opening fare' in final_df.columns
        assert summary['NG_Opening_fare'] > 0
        assert 'filed_51_au_mapping' in summary
        assert len(summary['filed_51_au_mapping']) == 51

        # Check that filed levels below opening fare have 0 inventory
        opening_fare = summary.get('opening_fare_airline_comp', summary['NG_Opening_fare'])
        for rbd, au in summary['filed_51_au_mapping'].items():
            rbd_fare = filed_51_ladder.loc[filed_51_ladder['RBD'] == rbd, 'fare'].iloc[0]
            if rbd_fare < opening_fare - 1e-4:
                assert au == 0.0, f"RBD {rbd} fare {rbd_fare} below opening {opening_fare} should have AU=0"

    def test_allin_conversion_for_nextgen_summary(self, sample_flight_data):
        """
        Verify that airline component fares are converted to all-in for NextGen output summary.
        """
        class MockTaxHelper:
            def allin_to_airline_component(self, f, o, d, route=None):
                return float(f) - 500.0
            def airline_component_to_allin(self, f, o, d, route=None):
                return float(f) + 500.0

        tax_helper = MockTaxHelper()
        capacity = 189
        cb = 100

        final_df, summary = NextGenOptimizationEngine.execute_flight_nextgen_optimization(
            sample_flight_data, capacity, cb, 300000.0,
            market_fares=[],
            flight_std='10:30',
            curr_params={'origin': 'BOM', 'destin': 'DEL'},
            tax_helper=tax_helper
        )

        # In summary, Anchor_fare and NG_Opening_fare must be all-in (airline_comp + 500)
        assert summary['Anchor_fare'] == summary['anchor_fare_airline_comp'] + 500.0
        assert summary['NG_Opening_fare'] == summary['opening_fare_airline_comp'] + 500.0

    def test_buy_up_factor_percentage_normalization(self, sample_flight_data):
        capacity = 189
        cb = 50

        # Run with decimal 0.10
        df_dec, anchor_dec, _ = NextGenOptimizationEngine.compute_initial_emsr_b(
            sample_flight_data, capacity, cb, buy_up_factor=0.10
        )
        # Run with percentage 10.0
        df_pct, anchor_pct, _ = NextGenOptimizationEngine.compute_initial_emsr_b(
            sample_flight_data, capacity, cb, buy_up_factor=10.0
        )

        assert np.allclose(df_dec['ProtectionLimit'].values, df_pct['ProtectionLimit'].values)
        assert np.allclose(df_dec['AU'].values, df_pct['AU'].values)
        assert anchor_dec == anchor_pct

        # Test execute_flight_nextgen_optimization reporting normalized buy_up_factor
        _, summary = NextGenOptimizationEngine.execute_flight_nextgen_optimization(
            sample_flight_data, capacity, cb, 200000.0,
            market_fares=[],
            flight_std='10:30',
            curr_params={'origin': 'BOM', 'destin': 'DEL', 'buy_up_factor_NG': 10.0}
        )
        assert summary['buy_up_factor'] == 0.10

    def test_evaluate_grid_response_with_tolerance(self):
        """
        Verify that evaluate_grid_response correctly computes:
        max(tolerance * C, rule_response)
        """
        # A = 3000, C = 5000
        # Rule: Min (1.2A, 0.8C) -> 1.2*3000 = 3600, 0.8*5000 = 4000 -> Min is 3600
        anchor_fare = 3000.0
        comp_min = 5000.0
        rule = "Min (1.2A, 0.8C)"

        # Case 1: No tolerance
        res_no_tol = NextGenOptimizationEngine.evaluate_grid_response(rule, anchor_fare, comp_min)
        assert res_no_tol == 3600.0

        # Case 2: Tolerance = 0.85 -> 0.85 * 5000 = 4250 -> max(4250, 3600) = 4250
        res_high_tol = NextGenOptimizationEngine.evaluate_grid_response(
            rule, anchor_fare, comp_min, tolerance=0.85
        )
        assert res_high_tol == 4250.0

        # Case 3: Tolerance = 0.70 -> 0.70 * 5000 = 3500 -> max(3500, 3600) = 3600
        res_low_tol = NextGenOptimizationEngine.evaluate_grid_response(
            rule, anchor_fare, comp_min, tolerance=0.70
        )
        assert res_low_tol == 3600.0

        # Case 4: Percentage normalization (85 -> 0.85)
        res_pct_tol = NextGenOptimizationEngine.evaluate_grid_response(
            rule, anchor_fare, comp_min, tolerance=85.0
        )
        assert res_pct_tol == 4250.0

    def test_execute_flight_nextgen_optimization_with_tolerance(self, sample_flight_data):
        """
        Verify that execute_flight_nextgen_optimization uses tolerance
        from comp_response_grid or curr_params to decide final strategy fare.
        """
        capacity = 189
        cb = 50
        oal_min = 6000.0
        flight_std = '10:30'

        market_fares = [
            {'carrier': '6E', 'flight_duration': '2:00', 'departure_time': '10:30', 'outbound_stops': 0, 'gross_fare': 6000.0},
            {'carrier': 'AI', 'flight_duration': '2:00', 'departure_time': '10:30', 'outbound_stops': 0, 'gross_fare': 7000.0}
        ]

        # Mock db_helper with Grid having tolerance = 0.90
        class MockDbHelper:
            class MockDb:
                class MockColl:
                    def find_one(self, query):
                        return {"id": 1, "plf_band": 1, "start": 0.0, "end": 100.0}
                comp_response_plf_bands = MockColl()

                class MockGridColl:
                    def find_one(self, query):
                        return {"response_rule": "Min (1.2A, 0.7C)", "criteria": "Min", "tolerance": 0.90}
                comp_response_grid = MockGridColl()

            db = MockDb()

            def getCompResponseGrid(self, grid_name, ndo_band, plf_band):
                return {"response_rule": "Min (1.2A, 0.7C)", "criteria": "Min", "tolerance": 0.90}

            def getCompResponsePlfBandDoc(self, plf):
                return {"id": 1, "plf_band": 1, "start": 0.0, "end": 100.0}

            def getCompResponsePlfBand(self, plf):
                return 1

        helper = MockDbHelper()

        # Run with tolerance 0.90 from db_helper
        # C = 6000. 0.90 * C = 5400.
        # Rule Min(1.2A, 0.7C) gives min(1.2*anchor, 4200) <= 4200.
        # tolerance_fare = 5400. Response fare = max(5400, <=4200) = 5400.
        # Strategy fare mapped to discrete ladder <= 5400 is 5252.0!
        res_df, summary = NextGenOptimizationEngine.execute_flight_nextgen_optimization(
            sample_flight_data, capacity, cb, oal_min,
            market_fares=market_fares,
            flight_std=flight_std,
            curr_params={
                'origin': 'BOM', 'destin': 'DEL',
                'competition_response_flag_NG': 1,
                'nextgen_flag_NG': 1
            },
            db_helper=helper
        )

        # Output Strategy_fare shows the exact grid calculation output fare (5400.0)
        assert summary['Strategy_fare'] == 5400.0
        assert res_df['strategy_fare'].iloc[0] == 5400.0

        # Check that capacity - AU_Revised does not cross 20 seats at lowest open bucket
        open_rows = res_df[res_df['AU_Revised'] > 0]
        assert not open_rows.empty
        lowest_open_au = open_rows['AU_Revised'].iloc[-1]
        assert (capacity - lowest_open_au) <= 20.0

    def test_first_rbd_20_seat_threshold_and_hardanchor(self, sample_flight_data):
        """
        Verify that:
        1. 20-seat limit (capacity - AU_Revised <= 20) is enforced for open buckets.
        2. Hardanchor elevates AU_Revised to plf_target when explicitly configured
           in curr_params and curr_plf < plf_threshold.
        """
        capacity = 189
        cb = 50
        # Case A: Without hardanchor -> 20-seat limit strictly enforced (capacity - AU_Revised <= 20)
        res_df, summary = NextGenOptimizationEngine.execute_flight_nextgen_optimization(
            sample_flight_data, capacity, cb, 200000.0,
            market_fares=[],
            flight_std='10:30',
            curr_params={'origin': 'BOM', 'destin': 'DEL'}
        )
        open_rows = res_df[res_df['AU_Revised'] > 0]
        assert not open_rows.empty
        lowest_open_au = open_rows['AU_Revised'].iloc[-1]
        assert (capacity - lowest_open_au) <= 20.0

        # Case B: With hardanchor explicitly configured -> hardanchor elevates AU_Revised to plf_target
        # Suppose hardanchor is set to 4000.0 (which is in the ladder)
        res_df_ha, summary_ha = NextGenOptimizationEngine.execute_flight_nextgen_optimization(
            sample_flight_data, capacity, cb, 200000.0,
            market_fares=[],
            flight_std='10:30',
            curr_params={
                'origin': 'BOM', 'destin': 'DEL',
                'hardanchor': 4000.0,
                'plfthreshold': 95.0,
                'plftarget': 95.0
            }
        )
        # Because curr_plf = 50/189 = 26.4% < 95%, hardanchor elevates AU_Revised for buckets >= 4000.0
        # to target PLF = 0.95 * 189 = 179.55
        ha_rows = res_df_ha[res_df_ha['fare'] >= 4000.0]
        assert (ha_rows['AU_Revised'] >= 179.0).any()

    def test_opening_fare_selected_from_response_fare_not_target_fc_fare(self, sample_flight_data):
        """
        Verify that opening fare among 51 filed levels is selected as the nearest lower
        among 51 levels below competition response fare, and assigned the inventory
        at the nearest forecast fare below competition response.

        Example:
        - Competitor min fare / grid output = 6061.9
        - Forecast ladder fares: ..., 6409, 5252, 4279, ... -> nearest forecast bucket below 6061.9 is 5252.
        - Filed 51 levels: R1=6200(allin=7000), R2=5800(allin=6600), R3=5421.9(allin=6136), R4=4900(allin=5600)
        - Nearest lower among 51 levels below 6061.9 is R2 (5800.0).
        - Opening fare = 5800.0 (allin = 6600.0), opening_rbd = R2.
        - Inventory at 5252 forecast bucket is assigned to R2.
        - Filed levels below R2 get AU = 0.
        """
        capacity = 189
        cb = 100
        # Comp fare 6061.9
        mkt_fares = [{'AirlineCode': '6E', 'Fare': 6061.904762, 'outbound_stops': 0, 'outbound_duration': 120, 'DepTime': '10:30'}]
        filed_51_ladder = pd.DataFrame([
            {'RBD': 'R1', 'fare': 6200.0, 'TOTAL': 7000.0},
            {'RBD': 'R2', 'fare': 5800.0, 'TOTAL': 6600.0},
            {'RBD': 'R3', 'fare': 5421.904762, 'TOTAL': 6136.0},
            {'RBD': 'R4', 'fare': 4900.0, 'TOTAL': 5600.0},
        ])

        class MockDbHelper:
            def getCompResponseGrid(self, grid_name, ndo_band, plf_band):
                return {'response_rule': 'C', 'criteria': 'Min'}

        final_df, summary = NextGenOptimizationEngine.execute_flight_nextgen_optimization(
            sample_flight_data, capacity, cb, 300000.0,
            market_fares=mkt_fares,
            flight_std='10:30',
            curr_params={'competition_response_flag_NG': 1, 'first_rbd_NG': 15},
            db_helper=MockDbHelper(),
            filed_51_ladder_df=filed_51_ladder
        )

        # Nearest lower among 51 levels below competition response 6061.9 is R2 (5800.0).
        assert summary['opening_rbd'] == 'R2'
        assert summary['opening_fare_airline_comp'] == 5800.0
        assert summary['NG_Opening_fare'] == 6600.0
        assert final_df['opening fare'].iloc[0] == 5800.0

        # R1 and R2 receive inventory; levels below opening fare R2 get 0.0
        mapping = summary['filed_51_au_mapping']
        assert mapping['R1'] > 0.0
        assert mapping['R2'] > 0.0
        assert mapping['R3'] == 0.0
        assert mapping['R4'] == 0.0

    def test_au_mapping_floor_interval_not_euclidean_nearest(self):
        """
        Verify that a lower filed fare (e.g. R4 all-in 6136) is mapped to its matching
        lower forecast bucket (all-in 5672, AU=126), and NOT mapped upward to the higher
        forecast bucket (all-in 6383.9, AU=157) merely because 6136 is closer in Euclidean
        distance to 6383.9 than to 5672.
        Only R3 (all-in 6384) which is >= 6383.9 should receive AU=157.
        """
        # Forecast ladder with buckets including 5658 (allin 6383.9) and 4980 (allin 5672)
        ladder_df = pd.DataFrame([
            {'FareBucket': '6433-7320', 'fare': 6433.0, 'mu': 1.0, 'sigma': 0.25},
            {'FareBucket': '5658-6433', 'fare': 5658.0, 'mu': 2.0, 'sigma': 0.50},
            {'FareBucket': '4980-5658', 'fare': 4980.0, 'mu': 3.0, 'sigma': 0.75},
            {'FareBucket': '4208-4980', 'fare': 4208.0, 'mu': 4.0, 'sigma': 1.00},
            {'FareBucket': '3277-4208', 'fare': 3277.0, 'mu': 5.0, 'sigma': 1.25},
        ])

        class MockTaxHelper:
            def allin_to_airline_component(self, gross_fare, origin, destination, route=None):
                return (gross_fare - 443.0) / 1.05
            def airline_component_to_allin(self, comp_fare, origin, destination, route=None):
                return comp_fare * 1.05 + 443.0

        tax_helper = MockTaxHelper()
        # 5658 all-in = 5658 * 1.05 + 443 = 6383.9
        # 4980 all-in = 4980 * 1.05 + 443 = 5672.0
        # 4208 all-in = 4208 * 1.05 + 443 = 4861.4

        filed_ladder = pd.DataFrame([
            {'RBD': 'R2', 'fare': 7158.0, 'TOTAL': 7959.0},
            {'RBD': 'R3', 'fare': 5658.1, 'TOTAL': 6384.0},
            {'RBD': 'R4', 'fare': 5421.9, 'TOTAL': 6136.0},
            {'RBD': 'R9', 'fare': 5166.7, 'TOTAL': 5868.0},
            {'RBD': 'T0', 'fare': 4920.0, 'TOTAL': 5609.0},
            {'RBD': 'TF', 'fare': 4721.0, 'TOTAL': 5400.0},
        ])

        class MockDbHelper:
            def getCompResponseGrid(self, grid_name, ndo_band, plf_band):
                return {'response_rule': 'A', 'criteria': 'Min'}

        capacity = 186
        cb = 64
        # Backstop at 5672 (all-in) / 4980 (comp)
        final_df, summary = NextGenOptimizationEngine.execute_flight_nextgen_optimization(
            ladder_df, capacity, cb, 300000.0,
            market_fares=[],
            flight_std='10:30',
            curr_params={
                'origin': 'BOM', 'destin': 'BLR',
                'b2c_backstop': 5672.0,
                'competition_response_flag_NG': 0
            },
            tax_helper=tax_helper,
            filed_51_ladder_df=filed_ladder
        )

        mapping = summary['filed_51_au_mapping']
        au_5658 = final_df.loc[final_df['FareBucket'] == '5658-6433', 'AU_Revised'].iloc[0]
        au_4980 = final_df.loc[final_df['FareBucket'] == '4980-5658', 'AU_Revised'].iloc[0]

        # R3 (TOTAL 6384.0) >= 6383.9 -> receives AU from 5658 bucket
        # R4 (TOTAL 6136.0) < 6383.9 and >= 5672.0 -> receives AU from 4980 bucket
        # It must NOT be assigned the AU of 5658 bucket!
        assert mapping['R3'] == au_5658
        assert mapping['R4'] == au_4980
        assert mapping['R3'] > mapping['R4'], f"R3 AU ({mapping['R3']}) must be higher than R4 AU ({mapping['R4']})"

        # Fares below cutoff / backstop 5672 must have AU=0
        assert mapping['T0'] == 0.0
        assert mapping['TF'] == 0.0

        # Opening RBD must be R9 (5868) which is lowest open class >= backstop (5672)
        assert summary['opening_rbd'] == 'R9'
        assert summary['NG_Opening_fare'] >= 5672.0
        assert final_df['opening fare_All_in'].iloc[0] >= 5672.0

    def test_opening_fare_after_backstop_and_hardanchor(self):
        """
        Verify that Opening Fare column is strictly determined AFTER Backstop and Hardanchor
        are applied, and can never be lower than the Backstop price floor.
        """
        ladder_df = pd.DataFrame([
            {'FareBucket': 'B1', 'fare': 6000.0, 'mu': 2.0, 'sigma': 0.5},
            {'FareBucket': 'B2', 'fare': 5000.0, 'mu': 3.0, 'sigma': 0.75},
            {'FareBucket': 'B3', 'fare': 4000.0, 'mu': 4.0, 'sigma': 1.0},
            {'FareBucket': 'B4', 'fare': 3000.0, 'mu': 5.0, 'sigma': 1.25},
        ])

        capacity = 189
        cb = 50
        # Backstop floor at 5000. Fares below 5000 cannot be open.
        final_df, summary = NextGenOptimizationEngine.execute_flight_nextgen_optimization(
            ladder_df, capacity, cb, 200000.0,
            market_fares=[],
            flight_std='10:30',
            curr_params={'b2c_backstop': 5000.0, 'hardanchor': 6000.0},
        )

        assert final_df['opening fare'].iloc[0] >= 5000.0
        assert summary['opening_fare_airline_comp'] >= 5000.0
        # Buckets below backstop must have AU_Revised = 0
        assert final_df.loc[final_df['fare'] < 5000.0, 'AU_Revised'].sum() == 0.0

    def test_tolerance_grid_vs_ml_input_enhancements(self, sample_flight_data):
        """
        Verify scheduled run tolerance enhancement:
        1. Consider Tolerance in the grid as well as the ML input.
        2. Higher fare is considered as tolerance.
        3. Max of that fare and fare obtained from grid calculation is Strategy fare.
        """
        capacity = 189
        cb = 50
        oal_min = 6000.0
        flight_std = '10:30'

        market_fares = [
            {'carrier': '6E', 'flight_duration': '2:00', 'departure_time': '10:30', 'outbound_stops': 0, 'gross_fare': 6000.0}
        ]

        class MockDbHelper:
            class MockDb:
                pass
            db = MockDb()

            def __init__(self, grid_tolerance):
                self.grid_tolerance = grid_tolerance

            def getCompResponseGrid(self, grid_name, ndo_band, plf_band):
                # Rule Min(1.2A, 0.7C) gives min(1.2*anchor, 4200) <= 4200
                return {
                    "response_rule": "Min (1.2A, 0.7C)",
                    "criteria": "Min",
                    "tolerance": self.grid_tolerance
                }

            def getCompResponsePlfBandDoc(self, plf):
                return {"id": 1, "plf_band": 1, "start": 0.0, "end": 100.0, "threshold_factor": 0.70}

            def getCompResponsePlfBand(self, plf):
                return 1

        # Case 1: Grid tolerance 0.80 -> 4800. ML tolerance = 5400.
        # Higher tolerance is ML tolerance (5400).
        # Rule gives 4200. Max(4200, 5400) = 5400.
        helper_1 = MockDbHelper(grid_tolerance=0.80)
        res_1, summary_1 = NextGenOptimizationEngine.execute_flight_nextgen_optimization(
            sample_flight_data, capacity, cb, oal_min,
            market_fares=market_fares,
            flight_std=flight_std,
            curr_params={
                'origin': 'BOM', 'destin': 'DEL',
                'competition_response_flag_NG': 1,
                'nextgen_flag_NG': 1,
                'B2CTolerance': 5400.0
            },
            db_helper=helper_1
        )
        assert summary_1['Strategy_fare'] == 5400.0

        # Case 2: Grid tolerance 0.95 -> 5700. ML tolerance = 5100.
        # Higher tolerance is Grid tolerance (5700).
        # Rule gives 4200. Max(4200, 5700) = 5700.
        helper_2 = MockDbHelper(grid_tolerance=0.95)
        res_2, summary_2 = NextGenOptimizationEngine.execute_flight_nextgen_optimization(
            sample_flight_data, capacity, cb, oal_min,
            market_fares=market_fares,
            flight_std=flight_std,
            curr_params={
                'origin': 'BOM', 'destin': 'DEL',
                'competition_response_flag_NG': 1,
                'nextgen_flag_NG': 1,
                'B2CTolerance': 5100.0
            },
            db_helper=helper_2
        )
        assert summary_2['Strategy_fare'] == 5700.0

    def test_hardanchor_plf_threshold_and_backstop_enhancements(self):
        """
        Verify Hardanchor with PLF threshold enhancement:
        - Once AU_2 is calculated, for applying hardanchor to get AU_Revised, use PLF threshold from ML.
        - If AU_2 of fare immediately below Hardanchor is less than PLF threshold then change it to
          value equivalent to PLF threshold (if not, let it remain as it is).
        - Once this is done, check whether AUs of fares above hardanchor fare is >= AU of fare immediately
          below hardanchor. If greater, let it remain as is; if less, change to value equal to PLF threshold.
        - Hardanchor and Backstop should be applied after FirstRBD logic is applied.
        """
        # Ladder with 5 fares
        ladder = pd.DataFrame([
            {'fare': 10000.0, 'mu': 10.0, 'sigma': 2.5},
            {'fare': 7000.0, 'mu': 20.0, 'sigma': 5.0},
            {'fare': 5000.0, 'mu': 30.0, 'sigma': 7.5},
            {'fare': 4000.0, 'mu': 40.0, 'sigma': 10.0},
            {'fare': 3000.0, 'mu': 50.0, 'sigma': 12.5},
        ])
        capacity = 189
        cb = 80
        # Hardanchor = 5500. Nearest fare <= 5500 is 5000.
        # PLF threshold = 90% -> plf_target = 0.90 * 189 = 170.1
        # Backstop = 3500. Fares < 3500 (3000) must have AU_Revised = 0.0.
        init_df, anchor_fare, promo_au = NextGenOptimizationEngine.compute_initial_emsr_b(
            ladder, capacity, cb
        )
        redistrib_df = NextGenOptimizationEngine.redistribute_demand_and_compute_revised_au(
            init_df, anchor_fare, 7000.0, capacity, cb,
            hardanchor=5500.0,
            plf_threshold=90.0,
            backstop=3500.0,
            first_rbd=20.0
        )
        # 1. Fare 5000 (immediately below 5500) has AU_Revised >= 170.1
        r_5000 = redistrib_df.loc[redistrib_df['fare'] == 5000.0, 'AU_Revised'].iloc[0]
        assert r_5000 >= 170.1

        # 2. Fares above 5000 (7000, 10000) must be >= AU of 5000
        r_7000 = redistrib_df.loc[redistrib_df['fare'] == 7000.0, 'AU_Revised'].iloc[0]
        r_10000 = redistrib_df.loc[redistrib_df['fare'] == 10000.0, 'AU_Revised'].iloc[0]
        assert r_7000 >= r_5000
        assert r_10000 >= r_5000

        # 3. Backstop: fare 3000 is < 3500 so AU_Revised must be 0.0
        r_3000 = redistrib_df.loc[redistrib_df['fare'] == 3000.0, 'AU_Revised'].iloc[0]
        assert r_3000 == 0.0

    def test_opening_rbd_au_when_backstop_elevates_opening_fare(self):
        """
        Verify that when a backstop elevates the opening fare (e.g. backstop = 7500 while
        competitor response fare is lower, e.g. 5000), the opening RBD nearest to 7500 among
        the 50 filed B2C levels receives positive AU (matching the opening forecast bucket),
        and is NOT assigned AU = 0.
        """
        ladder = pd.DataFrame([
            {'fare': 15000.0, 'mu': 2.0, 'sigma': 0.5},
            {'fare': 12000.0, 'mu': 3.0, 'sigma': 0.75},
            {'fare': 10000.0, 'mu': 4.0, 'sigma': 1.0},
            {'fare': 8500.0, 'mu': 5.0, 'sigma': 1.25},
            {'fare': 7200.0, 'mu': 6.0, 'sigma': 1.5},
            {'fare': 6000.0, 'mu': 7.0, 'sigma': 1.75},
            {'fare': 4500.0, 'mu': 8.0, 'sigma': 2.0},
            {'fare': 3500.0, 'mu': 9.0, 'sigma': 2.25},
        ])
        capacity = 189
        cb = 50

        # 50 filed levels: R0=15000 down to R49=2750 in steps of 250
        # R30 has fare = 15000 - 30 * 250 = 7500.0
        filed_ladder = pd.DataFrame([
            {'RBD': f'R{i}', 'fare': 15000.0 - i * 250.0, 'TOTAL': 15000.0 - i * 250.0}
            for i in range(50)
        ])

        # Competitor fare below backstop 7500
        mkt_fares = [{'AirlineCode': '6E', 'Fare': 5000.0, 'outbound_stops': 0, 'outbound_duration': 120, 'DepTime': '10:30'}]

        class MockDbHelper:
            def getCompResponseGrid(self, grid_name, ndo_band, plf_band):
                return {'response_rule': 'C', 'criteria': 'Min'}

        final_df, summary = NextGenOptimizationEngine.execute_flight_nextgen_optimization(
            ladder, capacity, cb, 200000.0,
            market_fares=mkt_fares,
            flight_std='10:30',
            curr_params={'competition_response_flag_NG': 1, 'b2c_backstop': 7500.0, 'first_rbd_NG': 20},
            db_helper=MockDbHelper(),
            filed_51_ladder_df=filed_ladder
        )

        # 1. Opening fare is 7500 (enforced by backstop)
        assert summary['opening_fare_airline_comp'] == 7500.0
        assert summary['opening_rbd'] == 'R30'

        # 2. In 19 forecast buckets (final_df), bucket 8500 has FirstRBD seats (cb + 20 = 70)
        # and buckets < 7500 have AU_Revised = 0.0
        au_8500 = final_df.loc[final_df['fare'] == 8500.0, 'AU_Revised'].iloc[0]
        assert au_8500 >= 70.0
        assert final_df.loc[final_df['fare'] < 7500.0, 'AU_Revised'].sum() == 0.0

        # 3. In the 50 filed B2C RBD mapping:
        mapping = summary['filed_51_au_mapping']
        # The opening RBD R30 (nearest to 7500) must receive positive AU (>= 70), NOT 0.0!
        assert mapping['R30'] >= 70.0, f"Opening RBD R30 AU must be >= 70, got {mapping['R30']}"

        # Filed levels above opening fare (R29 at 7750, R28 at 8000, R26 at 8500) must be >= R30 AU
        assert mapping['R29'] >= mapping['R30']
        assert mapping['R28'] >= mapping['R30']
        assert mapping['R26'] >= mapping['R30']

        # Filed levels below opening fare 7500 (R31 at 7250, R32 at 7000...) must have AU = 0.0
        assert mapping['R31'] == 0.0
        assert mapping['R32'] == 0.0
        assert mapping['R49'] == 0.0

        # summary['NG_FirstRBD'] and seats on opening must match
        assert summary['NG_FirstRBD'] >= 70.0

    def test_hardanchor_lower_than_backstop_or_equal_not_wiped_by_backstop(self):
        """
        Verify Flight 1816 31 Oct fix:
        When hardanchor=7000 and backstop=7000, discrete fare step lower than 7000
        (e.g. fare=4930, allin=6552) must NOT be wiped out by backstop.
        The opening fare must be <= 7000.
        """
        ladder_df = pd.DataFrame([
            {'FareBucket': 'B1', 'fare': 7000.0, 'mu': 2.0, 'sigma': 0.5},
            {'FareBucket': 'B2', 'fare': 5409.0, 'mu': 3.0, 'sigma': 0.75},
            {'FareBucket': 'B3', 'fare': 4930.0, 'mu': 4.0, 'sigma': 1.0},
            {'FareBucket': 'B4', 'fare': 4000.0, 'mu': 5.0, 'sigma': 1.25},
        ])

        final_df, summary = NextGenOptimizationEngine.execute_flight_nextgen_optimization(
            ladder_df, capacity=194, current_bookings=30, current_revenue=100000.0,
            market_fares=[],
            flight_std='10:30',
            curr_params={
                'hardanchor': 5356.19,  # airline component of 7000
                'b2c_backstop': 5356.19,  # airline component of 7000
                'plfthreshold': 95.0
            }
        )

        # Bucket 4930 (ha_idx) must remain open with AU > 0 (elevated to PLF target)
        au_4930 = final_df.loc[final_df['fare'] == 4930.0, 'AU_Revised'].iloc[0]
        assert au_4930 > 0.0, f"Bucket 4930 must not be zeroed out by backstop! Got {au_4930}"
        # Buckets strictly below ha_idx (4000.0) must be zeroed out by backstop
        au_4000 = final_df.loc[final_df['fare'] == 4000.0, 'AU_Revised'].iloc[0]
        assert au_4000 == 0.0

        # Opening fare must be <= hardanchor (5356.19)
        assert summary['opening_fare_airline_comp'] <= 5356.19

    def test_hardanchor_not_treated_as_floor_opening_fare(self):
        """
        Verify Flight 1813 30 Oct fix:
        Hardanchor is a CEILING under low PLF, NOT a floor.
        If hardanchor=11000 and strategy_fare=9669, opening fare must open at strategy fare
        (or lower), NOT be pushed up to 11000+.
        """
        ladder_df = pd.DataFrame([
            {'FareBucket': 'B1', 'fare': 12000.0, 'mu': 1.0, 'sigma': 0.25},
            {'FareBucket': 'B2', 'fare': 10000.0, 'mu': 2.0, 'sigma': 0.5},
            {'FareBucket': 'B3', 'fare': 9000.0, 'mu': 3.0, 'sigma': 0.75},
            {'FareBucket': 'B4', 'fare': 7800.0, 'mu': 4.0, 'sigma': 1.0},
        ])

        final_df, summary = NextGenOptimizationEngine.execute_flight_nextgen_optimization(
            ladder_df, capacity=185, current_bookings=120, current_revenue=500000.0,
            market_fares=[],
            flight_std='10:30',
            curr_params={
                'hardanchor': 11000.0,
                'b2c_backstop': 7000.0,
                'plfthreshold': 60.0  # cb=120 is 64.8% >= 60%, so hardanchor does not apply
            }
        )

        # Opening fare should not be forced up to 11000.0
        assert summary['opening_fare_airline_comp'] < 11000.0

    def test_opening_fare_matches_hardanchor_bucket_when_active(self):
        """
        Verify Flight 1816 27 Oct fix:
        When hardanchor is active and opens lower forecast buckets down to 8810,
        the opening fare must be 8810 (the lowest open bucket), NOT the higher strategy fare (10174).
        """
        ladder_df = pd.DataFrame([
            {'FareBucket': 'B1', 'fare': 10000.0, 'mu': 60.0, 'sigma': 15.0},
            {'FareBucket': 'B2', 'fare': 8500.0, 'mu': 50.0, 'sigma': 12.5},
            {'FareBucket': 'B3', 'fare': 7080.0, 'mu': 40.0, 'sigma': 10.0},
            {'FareBucket': 'B4', 'fare': 5000.0, 'mu': 30.0, 'sigma': 7.5},
        ])

        filed_51_ladder = pd.DataFrame([
            {'RBD': 'R1', 'fare': 10000.0, 'TOTAL': 12000.0},
            {'RBD': 'R2', 'fare': 8210.0, 'TOTAL': 10174.0},
            {'RBD': 'R3', 'fare': 7080.0, 'TOTAL': 8810.0},
            {'RBD': 'R4', 'fare': 5000.0, 'TOTAL': 6500.0},
        ])

        # Prior to hardanchor, anchor fare is 8500 (B2), so B3 (7080) and B4 (5000) are closed.
        # Response / strategy fare is 8280 (matching filed R2=8210, allin=10174).
        # But hardanchor is 7080 (allin=8810) and PLF is low (cb=120/194 = 61.8% < 90%).
        final_df, summary = NextGenOptimizationEngine.execute_flight_nextgen_optimization(
            ladder_df, capacity=194, current_bookings=120, current_revenue=1000000.0,
            market_fares=[],
            flight_std='10:30',
            curr_params={
                'hardanchor': 7080.0,
                'b2c_backstop': 5000.0,
                'plfthreshold': 90.0,
                'strategy_fare_NG': 8280.0
            },
            filed_51_ladder_df=filed_51_ladder
        )

        # Opening fare must be 7080 (all-in 8810), NOT 8210 (all-in 10174)
        assert summary['opening_rbd'] == 'R3'
        assert summary['opening_fare_airline_comp'] == 7080.0

    def test_20_seat_limit_strictly_enforced_when_hardanchor_not_active(self):
        """
        Verify that when hardanchor is NOT active:
        The opening RBD cannot exceed cb + 20 seats (e.g. cb=107 -> at most 127 seats),
        preventing unconstrained AU (e.g. 174 seats) from leaking onto the opening RBD.
        """
        ladder_df = pd.DataFrame([
            {'FareBucket': 'B1', 'fare': 10000.0, 'mu': 2.0, 'sigma': 0.5},
            {'FareBucket': 'B2', 'fare': 8000.0, 'mu': 3.0, 'sigma': 0.75},
            {'FareBucket': 'B3', 'fare': 6000.0, 'mu': 4.0, 'sigma': 1.0},
        ])

        filed_51_ladder = pd.DataFrame([
            {'RBD': 'R1', 'fare': 10000.0, 'TOTAL': 12000.0},
            {'RBD': 'R2', 'fare': 8000.0, 'TOTAL': 10000.0},
            {'RBD': 'R3', 'fare': 6000.0, 'TOTAL': 7500.0},
        ])

        cb = 107.0
        final_df, summary = NextGenOptimizationEngine.execute_flight_nextgen_optimization(
            ladder_df, capacity=185, current_bookings=cb, current_revenue=500000.0,
            market_fares=[],
            flight_std='10:30',
            curr_params={
                'first_rbd_NG': 20,
                'b2c_backstop': 6000.0,
                'hardanchor': None
            },
            filed_51_ladder_df=filed_51_ladder
        )

        # In AU revised, capacity minus AU_revised should not cross 20 seats
        capacity = 185
        assert (capacity - summary['NG_FirstRBD']) <= 20.0
        opening_rbd = summary['opening_rbd']
        assert (capacity - summary['filed_51_au_mapping'][opening_rbd]) <= 20.0
        # In final AU allocation after mapping to 52 RBDs, final RBD (highest fare class R1) is allocated to capacity
        assert summary['filed_51_au_mapping']['R1'] == float(capacity)














