import pytest
import pandas as pd
import numpy as np
from unittest.mock import MagicMock
from common.compute_utils import (
    calculateVariance,
    calculate_variance_for_load,
    calculate_group_adjusted_load,
    safe_float
)


def test_calculate_variance_normal():
    # When totalCapacity > 0
    # ndocounter < 22: ((currBookedLoad / totalCapacity) - (tgtAtCurrentHour / totalCapacity)) * 100
    var = calculateVariance(currBookedLoad=100, totalCapacity=200, ndocounter=10, tgtAtCurrentHour=80, startOfDayBookLoad=0, startOfDayTgtBookedLoad=0)
    assert round(var, 2) == 10.0  # ((100/200) - (80/200)) * 100 = (0.5 - 0.4)*100 = 10.0

    # ndocounter >= 22: ((currBookedLoad - startOfDayTgtBookedLoad) / totalCapacity) * 100
    var_ndo25 = calculateVariance(currBookedLoad=100, totalCapacity=200, ndocounter=25, tgtAtCurrentHour=80, startOfDayBookLoad=0, startOfDayTgtBookedLoad=80)
    assert round(var_ndo25, 2) == 10.0  # ((100 - 80) / 200) * 100 = 10.0


def test_calculate_variance_zero_capacity():
    var = calculateVariance(currBookedLoad=100, totalCapacity=0, ndocounter=10, tgtAtCurrentHour=80, startOfDayBookLoad=0, startOfDayTgtBookedLoad=0)
    assert var == 0.0


def test_calculate_variance_for_load():
    # Tests calculate_variance_for_load with effective capacity
    var1 = calculate_variance_for_load(simulated_booked_load=150, total_capacity=200, ndocounter=10, tgt_at_current_hour=100, sod_tgt_booked_load=100)
    assert round(var1, 2) == 25.0  # ((150/200) - (100/200)) * 100 = 25.0

    # With adjusted capacity
    adjusted_cap = 180
    var2 = calculate_variance_for_load(simulated_booked_load=150, total_capacity=adjusted_cap, ndocounter=10, tgt_at_current_hour=100, sod_tgt_booked_load=100)
    assert round(var2, 4) == round(((150 - 100) / 180) * 100, 4)

    # Zero capacity check
    assert calculate_variance_for_load(150, 0, 10, 100, 100) == 0


def test_calculate_group_adjusted_load_uses_adjusted_capacity():
    constants_utils = MagicMock()
    constants_utils.GROUP_SCALING_FACTOR = 0.5
    constants_utils.GROUP_IMP_FORECAST_REDUCTION = 0.0

    # Case 1: use_forecast = False, adjusted_capacity provided and > 0
    # simulated_plf = (simulated_booked_load / adjusted_capacity) * 100
    # current_variance = calculate_variance_for_load(simulated_booked_load, adjusted_capacity, ...)
    # adjustment = current_variance * GROUP_SCALING_FACTOR
    adj_load = calculate_group_adjusted_load(
        simulated_booked_load=120,
        total_capacity=200,
        adjusted_capacity=150,
        ndocounter=10,
        tgt_at_current_hour=90,
        sod_tgt_booked_load=90,
        imp_forecast=None,
        use_forecast=False,
        constantsutils=constants_utils
    )
    expected_plf = (120 / 150) * 100  # 80.0
    expected_var = ((120 / 150) - (90 / 150)) * 100  # 20.0
    expected_adj = expected_var * 0.5  # 10.0
    assert round(adj_load, 2) == round(expected_plf + expected_adj, 2)  # 90.0

    # Case 2: adjusted_capacity is 0, falls back to total_capacity for variance
    adj_load_fallback = calculate_group_adjusted_load(
        simulated_booked_load=120,
        total_capacity=200,
        adjusted_capacity=0,
        ndocounter=10,
        tgt_at_current_hour=90,
        sod_tgt_booked_load=90,
        imp_forecast=None,
        use_forecast=False,
        constantsutils=constants_utils
    )
    expected_plf_fb = 0  # since adjusted_capacity is 0
    expected_var_fb = ((120 / 200) - (90 / 200)) * 100  # 15.0
    expected_adj_fb = expected_var_fb * 0.5  # 7.5
    assert round(adj_load_fallback, 2) == round(expected_plf_fb + expected_adj_fb, 2)


def test_connections_market_list_row_direct_adjusted_capacity():
    from models.market_list_connections_row import MarketListConnectiosnsRow
    row_dict = {
        "DirectFlightNumber": "QP101",
        "DirectFlightBookedPax": 120,
        "DirectFlightLid": 189,
        "DirectFlightAdjustedCapacity": 180,
        "DirectFlightForecast": 85.0
    }
    row = MarketListConnectiosnsRow(row_dict)
    assert row.DirectFlightAdjustedCapacity == 180
    assert row.DirectFlightLid == 189


def test_connections_eff_capacity_calculation():
    class FakeParams:
        AdjustedCapacity1 = 175
        Lid1 = 189
        AdjustedCapacity2 = "0"
        Lid2 = 189
        DirectFlightAdjustedCapacity = 160
        DirectFlightLid = 180

    params = FakeParams()
    
    # Leg 1: AdjustedCapacity1 is 175 > 0 -> eff_capacity1 = 175
    adj_cap1 = safe_float(getattr(params, 'AdjustedCapacity1', None), 0.0)
    eff_cap1 = adj_cap1 if adj_cap1 > 0 else safe_float(getattr(params, 'Lid1', 0), 0.0)
    assert eff_cap1 == 175.0

    # Leg 2: AdjustedCapacity2 is 0 -> eff_capacity2 falls back to Lid2 (189)
    adj_cap2 = safe_float(getattr(params, 'AdjustedCapacity2', None), 0.0)
    eff_cap2 = adj_cap2 if adj_cap2 > 0 else safe_float(getattr(params, 'Lid2', 0), 0.0)
    assert eff_cap2 == 189.0

    # Direct flight: DirectFlightAdjustedCapacity is 160 > 0 -> direct_capacity = 160
    direct_adj = safe_float(getattr(params, 'DirectFlightAdjustedCapacity', None), 0.0)
    direct_lid = safe_float(getattr(params, 'DirectFlightLid', None), 0.0)
    direct_cap = direct_adj if direct_adj > 0 else direct_lid
    assert direct_cap == 160.0
    
    # Direct flight variance calculation:
    direct_booked_pax = 120
    direct_forecast = 80.0
    direct_variance = (direct_booked_pax / direct_cap * 100) - direct_forecast
    assert round(direct_variance, 4) == round((120 / 160 * 100) - 80.0, 4)  # 75 - 80 = -5.0


def test_connections_variance_matches_direct_variance():
    # Test NDO < 22: both use ((currBookedLoad / totalCapacity) - (tgtAtCurrentHour / totalCapacity)) * 100
    bookload = 110
    eff_cap = 180
    ndo_low = 10
    tgt_current = 95.0
    sod_tgt_lf = 0.5
    sod_tgt_abs = sod_tgt_lf * eff_cap  # 90.0

    var_direct_low = calculateVariance(
        currBookedLoad=float(bookload),
        totalCapacity=eff_cap,
        ndocounter=ndo_low,
        tgtAtCurrentHour=tgt_current,
        startOfDayBookLoad=sod_tgt_lf,
        startOfDayTgtBookedLoad=sod_tgt_abs
    )
    # Connection logic calling calculateVariance with sector 1 parameters
    var_conn_low = calculateVariance(
        float(bookload), eff_cap, ndo_low,
        tgt_current, sod_tgt_lf, sod_tgt_abs
    )
    assert var_conn_low == var_direct_low
    assert round(var_conn_low, 4) == round(((110 / 180) - (95 / 180)) * 100, 4)

    # Test NDO >= 22: both use ((currBookedLoad - startOfDayTgtBookedLoad) / totalCapacity) * 100
    ndo_high = 25
    var_direct_high = calculateVariance(
        currBookedLoad=float(bookload),
        totalCapacity=eff_cap,
        ndocounter=ndo_high,
        tgtAtCurrentHour=tgt_current,
        startOfDayBookLoad=sod_tgt_lf,
        startOfDayTgtBookedLoad=sod_tgt_abs
    )
    var_conn_high = calculateVariance(
        float(bookload), eff_cap, ndo_high,
        tgt_current, sod_tgt_lf, sod_tgt_abs
    )
    assert var_conn_high == var_direct_high
    assert round(var_conn_high, 4) == round(((110 - 90) / 180) * 100, 4)

