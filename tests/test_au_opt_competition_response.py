"""
tests/test_au_opt_competition_response.py
=============================================================================
Unit & Integration Test Suite for NextGen AU Optimization & Competition Response
(BRD-AU_Opt_Competition_Response)

Validates:
  1. EMSR-b Protection Limits (y_j) and Authorization Levels (AU_j)
  2. Promo Fare (80% lowest filed fare, W RBD) Allocation
  3. Anchor Fare (A) Determination
  4. Market Fare Filtering & Outlier Removal (Modified Z-Score / MAD)
  5. Competition Response Grid & Strategy Fare (C) Selection
  6. Demand Redistribution (P_eq mapping & interpolation)
  7. Current Bookload Floor Protection & FirstRBD Seat Capping
  8. Backstop (Price Floor) & Hardanchor (Price Cap)
  9. Closed-Form Expected Accepted Bookings (A_hat) & Expected Revenue (ER)
 10. Average Fare Forecast (AF_Forecast) & Load Factor Forecast (NG_LF Forecast)
"""

import sys
import os
import math
import pytest
import numpy as np
import pandas as pd

# Add paths to sys.path for direct imports
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROCESS_DIR = os.path.join(BASE_DIR, 'rm-additive-process')
CONFIG_DIR = os.path.join(BASE_DIR, 'rm-allocation-configurations')
if PROCESS_DIR not in sys.path:
    sys.path.insert(0, PROCESS_DIR)
if CONFIG_DIR not in sys.path:
    sys.path.insert(0, CONFIG_DIR)

from helpers.optimization_engine import AUOptimizationEngine
from helpers.utils.common_utils import (
    norm_cdf, norm_pdf, norm_ppf, filter_outliers_mad
)


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def opt_engine():
    return AUOptimizationEngine()


@pytest.fixture
def sample_19_ladder():
    """Realistic 19-fare ladder (airline component) descending from F1 to F19."""
    fares_desc = [
        18500.0, 16000.0, 14000.0, 12200.0, 10800.0,
        9600.0, 8500.0, 7600.0, 6800.0, 6100.0,
        5500.0, 4950.0, 4450.0, 4000.0, 3600.0,
        3250.0, 2950.0, 2700.0, 2500.0
    ]
    # Forecast demand per bucket
    mu_desc = [
        2.0, 3.0, 4.0, 5.0, 6.0,
        7.0, 8.0, 9.0, 10.0, 12.0,
        14.0, 16.0, 18.0, 20.0, 22.0,
        24.0, 26.0, 28.0, 30.0
    ]
    sigma_desc = [0.25 * m for m in mu_desc]
    return fares_desc, mu_desc, sigma_desc


# =============================================================================
# 1. TEST STATISTICAL HELPERS
# =============================================================================

def test_statistical_helpers():
    """Verify normal CDF, PDF, PPF, and MAD outlier filter."""
    assert math.isclose(norm_cdf(0.0), 0.5, abs_tol=1e-5)
    assert math.isclose(norm_pdf(0.0), 1.0 / math.sqrt(2 * math.pi), abs_tol=1e-5)
    assert math.isclose(norm_ppf(0.5), 0.0, abs_tol=1e-5)
    assert math.isclose(norm_ppf(0.8413447), 1.0, abs_tol=1e-4)

    # Outlier filter with MAD
    fares = [4500, 4600, 4550, 4700, 4650, 4520, 25000] # 25000 is an outlier
    filtered = filter_outliers_mad(fares, coverage_param=3.0)
    assert 25000 not in filtered
    assert len(filtered) == 6


# =============================================================================
# 2. TEST INITIAL EMSR-b INVENTORY OPTIMIZATION
# =============================================================================

def test_emsr_b_optimization(opt_engine, sample_19_ladder):
    fares_desc, mu_desc, sigma_desc = sample_19_ladder
    capacity = 180
    cb = 60

    res = opt_engine.compute_emsr_b(
        mu_vec=mu_desc, sigma_vec=sigma_desc, fares_vec=fares_desc,
        capacity=capacity, buy_up_factor=0.0, promo_factor=0.80,
        standard_dev_factor=0.25, current_bookload=cb
    )

    # 1. Check outputs length
    assert len(res['mu_bar']) == 19
    assert len(res['sigma_bar']) == 19
    assert len(res['F_j']) == 19
    assert len(res['prob']) == 19
    assert len(res['y']) == 19
    assert len(res['au']) == 19

    # 2. Cumulative demand must be monotonically increasing from class 1 to 19
    for j in range(1, 19):
        assert res['mu_bar'][j] >= res['mu_bar'][j - 1]

    # 3. Standard deviation must be capped at 0.25 * mu_bar
    for j in range(19):
        assert res['sigma_bar'][j] <= 0.25 * res['mu_bar'][j] + 1e-6

    # 4. AU must be monotonically non-increasing (AU_1 >= AU_2 >= ... >= AU_19)
    assert res['au'][0] == capacity
    for j in range(1, 19):
        assert res['au'][j - 1] >= res['au'][j]
        assert res['au'][j] >= 0

    # 5. Promo fare must be 80% of lowest filed fare
    assert math.isclose(res['promo_fare'], 0.80 * fares_desc[-1], rel_tol=1e-5)
    assert res['promo_au'] >= 0


# =============================================================================
# 3. TEST ANCHOR FARE DETERMINATION
# =============================================================================

def test_anchor_fare_determination(opt_engine, sample_19_ladder):
    fares_desc, mu_desc, sigma_desc = sample_19_ladder
    capacity = 180

    opt = opt_engine.compute_emsr_b(mu_desc, sigma_desc, fares_desc, capacity=capacity)
    au_ladder = opt['au']

    # Bookload = 50 -> lowest fare with AU > 50
    anchor_fare, anchor_idx = opt_engine.determine_anchor_fare(au_ladder, fares_desc, current_bookload=50)
    assert au_ladder[anchor_idx] > 50
    if anchor_idx < 18:
        assert au_ladder[anchor_idx + 1] <= 50

    # Bookload = 200 (flight overbooked) -> fallback to highest fare class
    anchor_fare_full, full_idx = opt_engine.determine_anchor_fare(au_ladder, fares_desc, current_bookload=200)
    assert full_idx == 0
    assert anchor_fare_full == fares_desc[0]


# =============================================================================
# 4. TEST MARKET FARE FILTERING & COMPETITOR RECALIBRATION
# =============================================================================

def test_market_fare_filtering(opt_engine):
    mkt_data = pd.DataFrame([
        {'departure_date': '2026-09-01', 'outbound_airline_code': '6E', 'stops': 0, 'std': '08:00:00', 'gross_fare': 5200},
        {'departure_date': '2026-09-01', 'outbound_airline_code': 'AI', 'stops': 0, 'std': '08:30:00', 'gross_fare': 5400},
        {'departure_date': '2026-09-01', 'outbound_airline_code': 'QP', 'stops': 0, 'std': '08:15:00', 'gross_fare': 4800}, # own carrier -> exclude
        {'departure_date': '2026-09-01', 'outbound_airline_code': '6E', 'stops': 1, 'std': '08:10:00', 'gross_fare': 3000}, # connecting -> exclude if direct-only
        {'departure_date': '2026-09-01', 'outbound_airline_code': '6E', 'stops': 0, 'std': '15:00:00', 'gross_fare': 4100}, # out of 2h window -> exclude
        {'departure_date': '2026-09-01', 'outbound_airline_code': 'SG', 'stops': 0, 'std': '07:45:00', 'gross_fare': 5300},
    ])

    min_fare, med_fare, sel_row, valid_df = opt_engine.filter_and_process_market_fares(
        mkt_data, flight_std='08:15:00', departure_date='2026-09-01',
        time_range_hours=2.0, carrier_exclusions=['SG'], # Exclude SG
        connection_fare_flag=False, recalibration_amount=100.0
    )

    # Valid competitors should be only 6E (08:00) and AI (08:30)
    assert len(valid_df) == 2
    # Lowest gross fare before recalibration is 5200 (6E) + 100 recalibration = 5300
    assert min_fare == 5300.0


def test_market_fares_rg_ag_union():
    """Verify load_market_fares combines RG and AG collections."""
    import logging
    from helpers.db_helpers.additive_mongo_helper import AdditiveMongoHelper

    logger = logging.getLogger('test_union')

    class MockMasterDB:
        def __init__(self):
            self.market_fares_rg = self.MockCollection([
                {'sector': 'BOMDEL', 'departure_date': '2026-09-01', 'outbound_airline_code': '6E', 'std': '08:00:00', 'gross_fare': 5200, 'stops': 0}
            ])
            self.market_fares_ag = self.MockCollection([
                {'sector': 'BOMDEL', 'departure_date': '2026-09-01', 'airline_code': 'AI', 'std': '08:30:00', 'gross_fare': 5400, 'stops': 0}
            ])

        def list_collection_names(self):
            return ['market_fares_rg', 'market_fares_ag']

        class MockCollection:
            def __init__(self, data):
                self._data = data

            def find(self, query, projection=None):
                return self._data

    helper = AdditiveMongoHelper(mongo_db=None, master_db=MockMasterDB(), logger=logger)
    helper.mode = 'PROD'

    combined = helper.load_market_fares(['BOMDEL'], start_date='2026-09-01', end_date='2026-09-01')

    assert not combined.empty
    assert len(combined) == 2
    assert set(combined['data_source']) == {'RG', 'AG'}



# =============================================================================
# 5. TEST COMPETITION RESPONSE GRID & STRATEGY FARE
# =============================================================================

def test_competition_response_and_strategy_fare(opt_engine, sample_19_ladder):
    fares_desc, _, _ = sample_19_ladder

    anchor_fare = 4000.0
    comp_fare = 5200.0

    # Default grid: comp_fare >= anchor_fare -> hold anchor
    resp_fare = opt_engine.evaluate_competition_response_grid(anchor_fare, comp_fare)
    assert resp_fare == anchor_fare

    # If comp_fare < anchor_fare -> response is comp_fare
    comp_fare_low = 3500.0
    resp_fare_low = opt_engine.evaluate_competition_response_grid(anchor_fare, comp_fare_low)
    assert resp_fare_low == comp_fare_low

    # Strategy fare: highest ladder fare strictly lower than response fare
    strat_fare = opt_engine.determine_strategy_fare(response_fare=5200.0, ladder_fares=fares_desc)
    assert strat_fare < 5200.0
    assert strat_fare == 4950.0  # 4950 is highest ladder fare < 5200


# =============================================================================
# 6. TEST DEMAND REDISTRIBUTION & REVISED AU (AU_2, AU_revised)
# =============================================================================

def test_demand_redistribution_and_revised_au(opt_engine, sample_19_ladder):
    fares_desc, mu_desc, sigma_desc = sample_19_ladder
    capacity = 180
    cb = 60

    # 1. No-op case: Anchor >= Strategy Fare
    res_noop = opt_engine.redistribute_demand_and_compute_au2(
        anchor_fare=6100.0, strategy_fare=5500.0, ladder_fares=fares_desc,
        mu_vec=mu_desc, sigma_vec=sigma_desc, capacity=capacity,
        current_bookload=cb, standard_dev_factor=0.25, first_rbd_seats=20
    )
    assert math.isclose(res_noop['Predicted_Pickup_Revised'], sum(mu_desc), rel_tol=1e-4)

    # 2. Active redistribution case: Anchor (3250) < Strategy Fare (5500)
    anchor_fare = 3250.0
    strategy_fare = 5500.0

    res = opt_engine.redistribute_demand_and_compute_au2(
        anchor_fare=anchor_fare, strategy_fare=strategy_fare,
        ladder_fares=fares_desc, mu_vec=mu_desc, sigma_vec=sigma_desc,
        capacity=capacity, current_bookload=cb, standard_dev_factor=0.25,
        first_rbd_seats=20, backstop=3000.0, hardanchor=15000.0
    )

    # Check demand redistribution:
    # All classes below strategy fare must have mu_2 = 0 and AU_2 = 0
    for j, f in enumerate(fares_desc):
        if f < strategy_fare:
            assert res['mu_2'][j] == 0.0
            assert res['AU_2'][j] == 0

    # Strategy fare index
    strat_idx = [j for j, f in enumerate(fares_desc) if math.isclose(f, strategy_fare, rel_tol=1e-4)][0]

    # AU_revised at Strategy Fare must be capped at min(AU_2(C), CB + FirstRBD) = min(AU_2(C), 60 + 20 = 80)
    assert res['AU_revised'][strat_idx] <= (cb + 20)

    # Backstop check: all fares below backstop (3000) must have AU_revised = 0
    for j, f in enumerate(fares_desc):
        if f < 3000.0:
            assert res['AU_revised'][j] == 0

    # Hardanchor check: classes >= highest fare < 15000 (which is 14000) must have AU_revised = capacity
    for j, f in enumerate(fares_desc):
        if f >= 14000.0:
            assert res['AU_revised'][j] == capacity


# =============================================================================
# 7. TEST CLOSED-FORM REVENUE FORECAST (AF_FORECAST)
# =============================================================================

def test_closed_form_revenue_forecast(opt_engine, sample_19_ladder):
    fares_desc, mu_desc, sigma_desc = sample_19_ladder
    capacity = 180
    cb = 60
    curr_rev = 240000.0  # 60 pax * 4000

    redist = opt_engine.redistribute_demand_and_compute_au2(
        anchor_fare=3250.0, strategy_fare=4950.0, ladder_fares=fares_desc,
        mu_vec=mu_desc, sigma_vec=sigma_desc, capacity=capacity,
        current_bookload=cb, standard_dev_factor=0.25, first_rbd_seats=20
    )

    rev_fc = opt_engine.calculate_revenue_forecast(
        mu_2_vec=redist['mu_2'], sigma_2_vec=redist['sigma_2'],
        ladder_fares=fares_desc, au_revised_vec=redist['AU_revised'],
        current_bookload=cb, current_revenue=curr_rev,
        capacity=capacity, predicted_flown_revised=redist['Predicted_Flown_Revised']
    )

    # Check revenue metrics
    assert len(rev_fc['Available_Inventory']) == 19
    assert len(rev_fc['Expected_Bookings']) == 19
    assert len(rev_fc['Expected_Revenue']) == 19

    assert rev_fc['Total_Expected_Revenue'] > 0
    assert rev_fc['Predicted_Revenue'] == curr_rev + rev_fc['Total_Expected_Revenue']
    assert rev_fc['AF_Forecast'] > 0
    # Expected average fare should be reasonable (e.g. ₹3,000 to ₹10,000)
    assert 2500 <= rev_fc['AF_Forecast'] <= 15000


# =============================================================================
# 8. TEST INTEGRATION IN ADDITIVE PROCESSOR
# =============================================================================

def test_batch_inventory_optimization_integration():
    """Verify _run_inventory_optimization in AdditivePickupModelProcessor."""
    import logging
    from additive_model import AdditivePickupModelProcessor

    logger = logging.getLogger('test_opt')
    params = {
        'mongo': None,
        'mongo_master': None,
        'rdEsbConn': None,
        's3': None
    }
    overrides = {'mode': 'TEST', 'sectors': ['BOMDEL']}
    processor = AdditivePickupModelProcessor(logger, params, overrides, None)

    # Mock tax master data in cache
    processor.cache.data['tax_master_dom'] = pd.DataFrame([
        {'sector': 'BOMDEL', 'psf': 100, 'udf': 250, 'cute': 50, 'rcs': 50, 'gst_rate': 0.05}
    ])
    processor.cache.data['pricing_gst_sectors'] = {'BOMDEL'}
    processor.cache.data['pricing_rcs_sectors'] = {'BOMDEL'}
    processor.cache.data['pricing_duf_sectors'] = {}

    # Mock future flight
    future_df = pd.DataFrame([{
        'Sector': 'BOMDEL',
        'DepartureDate_future': pd.Timestamp('2026-09-01'),
        'FlightNumber_future': 1101,
        'Capacity': 180,
        'Total Bookings': 45,
        'DOW': 'Tue',
        'STD_future': '06:00:00',
        'Tagging': 'Peak'
    }])

    # Mock 19 fare buckets
    buckets = []
    ladder_lowers = [
        18000, 15500, 13500, 11800, 10400,
        9200, 8100, 7200, 6400, 5700,
        5100, 4600, 4100, 3700, 3300,
        2950, 2650, 2400, 2200
    ]
    ladder_uppers = [
        None, 18000, 15500, 13500, 11800,
        10400, 9200, 8100, 7200, 6400,
        5700, 5100, 4600, 4100, 3700,
        3300, 2950, 2650, 2400
    ]

    for i in range(19):
        lo = ladder_lowers[i]
        up = ladder_uppers[i]
        basis = f"{lo}+" if up is None else f"{lo}-{up}"
        buckets.append({
            'Sector': 'BOMDEL',
            'DepartureDate_future': pd.Timestamp('2026-09-01'),
            'FlightNumber_future': 1101,
            'Capacity': 180,
            'Total Bookings': 45,
            'DOW': 'Tue',
            'STD_future': '06:00:00',
            'Tagging': 'Peak',
            'FareBasis': basis,
            'FareBucket_Lower': float(lo),
            'FareBucket_Upper': float(up) if up else np.nan,
            'WTP_pax_count': 5.0 + i * 1.5,
            'predicted_pax_std': 0.25 * (5.0 + i * 1.5)
        })

    bucket_df = pd.DataFrame(buckets)
    booked_df = pd.DataFrame([
        {'Sector': 'BOMDEL', 'DepartureDate': pd.Timestamp('2026-09-01'), 'FlightNumber': 1101, 'Fare': 180000.0, 'Pax Count': 45}
    ])
    farebasis_df = pd.DataFrame([{'Sector': 'BOMDEL', 'FareBasis': b['FareBasis']} for b in buckets])

    opt_df, metrics_df = processor._run_inventory_optimization(
        bucket_df, future_df, farebasis_df, booked_df,
        count_col='WTP_pax_count', std_col='predicted_pax_std'
    )

    # Verify outputs
    assert not opt_df.empty
    assert not metrics_df.empty
    assert len(metrics_df) == 1
    # 19 ladder buckets + 1 promo bucket = 20 rows
    assert len(opt_df) == 20

    # Verify metrics columns
    expected_metric_cols = [
        'Sector', 'DepartureDate_future', 'FlightNumber_future',
        'NG_LF Forecast', 'NG_AF Forecast', 'NG_Historical current booked',
        'NG_Opening fare', 'NG_FirstRBD', 'Strategy fare', 'Anchor fare'
    ]
    for col in expected_metric_cols:
        assert col in metrics_df.columns

    # Verify optimization dataset columns
    expected_opt_cols = [
        'SnapshotDate', 'Sector', 'DepartureDate', 'FlightNumber',
        'Capacity', 'Current_Bookings', 'FareBucket',
        'FareMidpoint', 'FareMidpoint_AllIn',
        'predicted_pax_count', 'predicted_pax_std',
        'ProtectionLimit', 'AU', 'AnchorFare_Flag', 'StrategyFare_Flag',
        'mu_2', 'sigma_2', 'mu_bar_2', 'AU_2', 'AU_revised',
        'Available_Inventory', 'Expected_Bookings', 'Expected_Revenue'
    ]
    for col in expected_opt_cols:
        assert col in opt_df.columns


if __name__ == '__main__':
    pytest.main(['-v', __file__])

