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

