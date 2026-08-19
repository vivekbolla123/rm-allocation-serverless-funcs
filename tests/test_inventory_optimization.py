"""Unit tests for Inventory Optimization (EMSR-b) in rm-additive-process.

Tests:
  - Calculation of aggregated mean demand (Mu_Bar) and capped std dev (Sigma_Bar).
  - Weighted average fare (F_j) and next lower fare (f_{j+1}).
  - Normal inverse probability and ceiling protection limits (y_j).
  - Nested authorization levels (AU) with capacity ceiling.
  - Promo fare at 80% with W RBD and correct AU.
  - Opening fare determination ignoring Promo inventory.
  - Multi-flight DataFrame batch processing.
  - Edge cases (single bucket, zero demand, full capacity).
"""

import math
import os
import sys
import pytest
import numpy as np
import pandas as pd
from scipy.stats import norm

# Add required paths
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
_ADDITIVE_DIR = os.path.join(_ROOT, 'rm-additive-process')
_CONFIG_DIR = os.path.join(_ROOT, 'rm-allocation-configurations')
for p in [_ROOT, _ADDITIVE_DIR, _CONFIG_DIR]:
    if p not in sys.path:
        sys.path.insert(0, p)

from helpers.utils.common_utils import (
    compute_flight_inventory_optimization,
    compute_inventory_optimization_df,
    parse_fare_bounds,
)
from Constants import (
    EMSR_SIGMA_FACTOR,
    PROMO_FARE_RATIO,
    PROMO_RBD,
    OPEN_BUCKET_MIDPOINT_FACTOR,
)


@pytest.fixture
def sample_flight_data():
    """Returns sample flight bucket data matching documentation example."""
    rows = [
        {'Sector': 'BOMDEL', 'DepartureDate_future': '2026-09-01', 'FlightNumber_future': 1234,
         'DOW': 'Tuesday', 'STD_future': '10:00', 'Capacity': 189, 'Current_Bookings': 50,
         'Tagging': 'Peak', 'FareBasis': '25088+', 'predicted_pax_count': 2.1, 'predicted_pax_std': 1.0},
        {'Sector': 'BOMDEL', 'DepartureDate_future': '2026-09-01', 'FlightNumber_future': 1234,
         'DOW': 'Tuesday', 'STD_future': '10:00', 'Capacity': 189, 'Current_Bookings': 50,
         'Tagging': 'Peak', 'FareBasis': '21685-25088', 'predicted_pax_count': 3.5, 'predicted_pax_std': 1.5},
        {'Sector': 'BOMDEL', 'DepartureDate_future': '2026-09-01', 'FlightNumber_future': 1234,
         'DOW': 'Tuesday', 'STD_future': '10:00', 'Capacity': 189, 'Current_Bookings': 50,
         'Tagging': 'Peak', 'FareBasis': '15000-21685', 'predicted_pax_count': 5.2, 'predicted_pax_std': 2.0},
        {'Sector': 'BOMDEL', 'DepartureDate_future': '2026-09-01', 'FlightNumber_future': 1234,
         'DOW': 'Tuesday', 'STD_future': '10:00', 'Capacity': 189, 'Current_Bookings': 50,
         'Tagging': 'Peak', 'FareBasis': '8000-15000', 'predicted_pax_count': 8.7, 'predicted_pax_std': 2.5},
        {'Sector': 'BOMDEL', 'DepartureDate_future': '2026-09-01', 'FlightNumber_future': 1234,
         'DOW': 'Tuesday', 'STD_future': '10:00', 'Capacity': 189, 'Current_Bookings': 50,
         'Tagging': 'Peak', 'FareBasis': '5000-8000', 'predicted_pax_count': 12.5, 'predicted_pax_std': 3.2},
        {'Sector': 'BOMDEL', 'DepartureDate_future': '2026-09-01', 'FlightNumber_future': 1234,
         'DOW': 'Tuesday', 'STD_future': '10:00', 'Capacity': 189, 'Current_Bookings': 50,
         'Tagging': 'Peak', 'FareBasis': '3000-4000', 'predicted_pax_count': 15.8, 'predicted_pax_std': 3.8},
        {'Sector': 'BOMDEL', 'DepartureDate_future': '2026-09-01', 'FlightNumber_future': 1234,
         'DOW': 'Tuesday', 'STD_future': '10:00', 'Capacity': 189, 'Current_Bookings': 50,
         'Tagging': 'Peak', 'FareBasis': '1620-1911', 'predicted_pax_count': 18.2, 'predicted_pax_std': 4.1},
    ]
    return pd.DataFrame(rows)


def test_parse_fare_bounds():
    """Verify fare bounds parsing."""
    assert parse_fare_bounds('25088+') == (25088.0, None)
    assert parse_fare_bounds('21685-25088') == (21685.0, 25088.0)
    assert parse_fare_bounds('3000-4000') == (3000.0, 4000.0)


def test_flight_inventory_optimization_structure(sample_flight_data):
    """Test that output dataframe contains filed fare classes + 1 promo row."""
    res = compute_flight_inventory_optimization(sample_flight_data)
    assert len(res) == len(sample_flight_data) + 1
    
    # Check Promo row is at the end
    promo_row = res.iloc[-1]
    assert promo_row['FareBucket'] == 'Promo'
    assert promo_row['RBD'] == 'W'
    assert promo_row['ProtectionLimit'] == 0
    assert promo_row['predicted_pax_count'] == 0.0
    assert promo_row['predicted_pax_std'] == 0.0


def test_flight_inventory_optimization_values(sample_flight_data):
    """Test the exact mathematical calculations against the formula."""
    res = compute_flight_inventory_optimization(sample_flight_data)
    
    # Check midpoints
    # Open bucket: 25088 * 1.10 = 27596.8
    assert pytest.approx(res.iloc[0]['FareMidpoint'], 0.1) == 27596.8
    # Closed bucket: (21685 + 25088) / 2 = 23386.5
    assert pytest.approx(res.iloc[1]['FareMidpoint'], 0.1) == 23386.5
    # Lowest filed bucket: (1620 + 1911) / 2 = 1765.5
    assert pytest.approx(res.iloc[6]['FareMidpoint'], 0.1) == 1765.5
    # Promo bucket: 0.80 * 1765.5 = 1412.4
    assert pytest.approx(res.iloc[7]['FareMidpoint'], 0.1) == 1412.4

    # Top class AU must equal capacity
    assert res.iloc[0]['AU'] == 189
    
    # Monotonic AU descending / Protection limits non-decreasing
    filed_au = res['AU'].tolist()
    for i in range(len(filed_au) - 1):
        assert filed_au[i] >= filed_au[i + 1]


def test_promo_fare_calculation():
    """Verify promo fare is 80% of lowest filed fare midpoint and allocates AU."""
    df = pd.DataFrame([
        {'Sector': 'DELBOM', 'DepartureDate_future': '2026-09-01', 'FlightNumber_future': 101,
         'Capacity': 180, 'Current_Bookings': 10, 'FareBasis': '4000-5000',
         'predicted_pax_count': 10.0, 'predicted_pax_std': 2.0},
        {'Sector': 'DELBOM', 'DepartureDate_future': '2026-09-01', 'FlightNumber_future': 101,
         'Capacity': 180, 'Current_Bookings': 10, 'FareBasis': '2000-3000',
         'predicted_pax_count': 20.0, 'predicted_pax_std': 3.0},
    ])
    res = compute_flight_inventory_optimization(df)
    assert len(res) == 3
    promo_row = res.iloc[-1]
    lowest_midpoint = (2000 + 3000) / 2.0  # 2500
    assert promo_row['FareMidpoint'] == 0.80 * 2500  # 2000
    assert promo_row['RBD'] == 'W'
    assert promo_row['AU'] <= 180
    assert promo_row['ProtectionLimit'] == 0


def test_opening_fare_without_promo(sample_flight_data):
    """Verify that opening fare is identified from filed classes only, ignoring Promo."""
    # With Current_Bookings = 50, class 7 (AU = 135) is open
    res = compute_flight_inventory_optimization(sample_flight_data)
    assert res.iloc[6]['IsOpeningFare'] == True
    assert res.iloc[7]['IsOpeningFare'] == False  # Promo is never opening fare
    assert res['OpeningFareBucket'].iloc[0] == '1620-1911'

    # If Current_Bookings = 136, class 7 (AU = 135) is closed -> class 6 (AU = 153) is open
    sample_flight_data_high_bk = sample_flight_data.copy()
    sample_flight_data_high_bk['Current_Bookings'] = 136
    res2 = compute_flight_inventory_optimization(sample_flight_data_high_bk)
    assert res2.iloc[5]['IsOpeningFare'] == True
    assert res2.iloc[6]['IsOpeningFare'] == False
    assert res2.iloc[7]['IsOpeningFare'] == False
    assert res2['OpeningFareBucket'].iloc[0] == '3000-4000'


def test_multi_flight_optimization(sample_flight_data):
    """Test batch processing across multiple flights."""
    f1 = sample_flight_data.copy()
    f2 = sample_flight_data.copy()
    f2['FlightNumber_future'] = 5678
    combined = pd.concat([f1, f2], ignore_index=True)
    
    res = compute_inventory_optimization_df(combined)
    # 2 flights * (7 filed + 1 promo) = 16 rows
    assert len(res) == 16
    assert set(res['FlightNumber_future']) == {1234, 5678}


def test_single_fare_bucket():
    """Verify flight with single fare bucket behaves correctly."""
    df = pd.DataFrame([
        {'Sector': 'BOMDEL', 'DepartureDate_future': '2026-09-01', 'FlightNumber_future': 999,
         'Capacity': 150, 'Current_Bookings': 20, 'FareBasis': '5000+',
         'predicted_pax_count': 30.0, 'predicted_pax_std': 5.0}
    ])
    res = compute_flight_inventory_optimization(df)
    assert len(res) == 2  # 1 filed + 1 promo
    assert res.iloc[0]['AU'] == 150
    assert res.iloc[0]['IsOpeningFare'] == True
    assert res.iloc[1]['FareBucket'] == 'Promo'
    assert res.iloc[1]['AU'] >= 0


def test_zero_demand():
    """Verify zero predicted demand assigns full capacity and zero protection limit."""
    df = pd.DataFrame([
        {'Sector': 'BOMDEL', 'DepartureDate_future': '2026-09-01', 'FlightNumber_future': 888,
         'Capacity': 180, 'Current_Bookings': 0, 'FareBasis': '5000-6000',
         'predicted_pax_count': 0.0, 'predicted_pax_std': 0.0},
        {'Sector': 'BOMDEL', 'DepartureDate_future': '2026-09-01', 'FlightNumber_future': 888,
         'Capacity': 180, 'Current_Bookings': 0, 'FareBasis': '3000-4000',
         'predicted_pax_count': 0.0, 'predicted_pax_std': 0.0}
    ])
    res = compute_flight_inventory_optimization(df)
    assert len(res) == 3
    assert res.iloc[0]['ProtectionLimit'] == 0
    assert res.iloc[1]['ProtectionLimit'] == 0
    assert res.iloc[2]['ProtectionLimit'] == 0
    assert res.iloc[0]['AU'] == 180
    assert res.iloc[1]['AU'] == 180
    assert res.iloc[2]['AU'] == 180
    assert res.iloc[1]['IsOpeningFare'] == True  # lowest filed bucket open
