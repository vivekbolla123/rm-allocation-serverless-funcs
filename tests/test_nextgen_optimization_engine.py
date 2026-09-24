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

        # Verify FirstRBD cap: AU_Revised at strategy fare <= cb + 20
        assert strat_row['AU_Revised'] <= (cb + 20)

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
        # AU_Revised must be capped by PromoFirstRBD_NG (10)
        assert promo_row['AU_Revised'] <= promo_first_rbd
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
        assert np.isclose(summary2['NG_LF_Forecast'], ((80 + expected_pickup_anchor_above) / 100.0) * 100.0)

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
        to hardanchor, ensuring no seats are only available at fares higher than hardanchor.
        """
        capacity = 189
        cb = 100
        init_df, anchor_fare, promo_au = NextGenOptimizationEngine.compute_initial_emsr_b(
            sample_flight_data, capacity, cb
        )
        # sample_flight_data fares around 5000 are: 6409.0, 5252.0, 4279.0, 3526.0
        # If hardanchor=5000.0, the nearest fare lower than 5000 is 4279.0.
        # Fares >= 4279.0 (4279, 5252, 6409, ...) must have AU_Revised = capacity.
        # Fares < 4279.0 (3526, 2914) must have AU_Revised < capacity (or floored).
        redistrib_df = NextGenOptimizationEngine.redistribute_demand_and_compute_revised_au(
            init_df, anchor_fare, 5252.0, capacity, cb,
            hardanchor=5000.0,
            first_rbd=20.0
        )
        for _, r in redistrib_df.iterrows():
            if r['fare'] >= 4279.0:
                assert r['AU_Revised'] == capacity, f"Fare {r['fare']} >= 4279 should be capped to capacity"
            else:
                assert r['AU_Revised'] < capacity, f"Fare {r['fare']} < 4279 should not be capped to capacity"

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










