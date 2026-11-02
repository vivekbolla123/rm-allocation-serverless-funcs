"""Focused tests for INR -> home-currency conversion of weighted-file fares and
the two new ConversionRate / Currency output columns (Repo B serverless engine).

Direction under test (documented in runAlloc.resolve_home_conversion):
    home_value = inr_value * ConversionRate   where row is (From='INR', To=<home>)

The fixture QPDynamicAllocation/testcases/input/config_conversion_rate.csv carries
both directions as reciprocal rows (e.g. INR->AED = 0.0438596, AED->INR = 22.8).
"""
import os
import sys
import types
import logging

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'rm-allocation-process-inputs'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'rm-allocation-configurations'))

import runAlloc
from Constants import (
    CONST_OUTPUT_SUMMARY_NEXTGEN_ORDER,
    CONST_OUTPUT_INVENTORY_OPTIMIZATION_ORDER,
)
from models.output_summary_nextgen import OutputSummaryNextGen


def _fixture_conversion_data():
    path = os.path.join(
        os.path.dirname(__file__), '..',
        'QPDynamicAllocation', 'testcases', 'input', 'config_conversion_rate.csv'
    )
    return pd.read_csv(path)


def _make_resolver():
    """Build a minimal object exposing the real convert_currency /
    resolve_home_conversion bound methods over the fixture conversion table."""
    obj = types.SimpleNamespace()
    obj.conversion_data = _fixture_conversion_data()
    obj.logger = logging.getLogger('test_currency_conversion_nextgen')
    obj.convert_currency = runAlloc.DynAlloc.convert_currency.__get__(obj)
    obj.resolve_home_conversion = runAlloc.DynAlloc.resolve_home_conversion.__get__(obj)
    return obj


# --- Direction / resolver semantics ------------------------------------------

def test_inr_sector_rate_is_one_and_currency_inr():
    obj = _make_resolver()
    for code in ['INR', 'inr', '', None, 'nan', 'None']:
        rate, curr = obj.resolve_home_conversion(code)
        assert rate == 1.0
        assert curr == 'INR'


def test_non_inr_sector_multiplies_by_inr_to_home_rate():
    obj = _make_resolver()
    rate, curr = obj.resolve_home_conversion('AED')
    assert curr == 'AED'
    # Fixture: INR->AED = 0.0438596 (the multiply convention).
    assert abs(rate - 0.0438596) < 1e-7
    # A 10000 INR fare becomes 10000 * rate AED.
    assert abs(10000.0 * rate - 438.596) < 1e-2


def test_missing_rate_falls_back_to_one_and_inr():
    obj = _make_resolver()
    # 'XYZ' is absent in both directions -> safe default, never crashes.
    rate, curr = obj.resolve_home_conversion('XYZ')
    assert rate == 1.0
    assert curr == 'INR'


def test_reverse_row_fallback_inverts_rate():
    """When only the (home -> INR) row exists, rate falls back to 1/rate."""
    obj = types.SimpleNamespace()
    obj.conversion_data = pd.DataFrame([
        {'FromCurrencyCode': 'AED', 'ToCurrencyCode': 'INR', 'ConversionRate': 22.8},
    ])
    obj.logger = logging.getLogger('test_currency_conversion_nextgen')
    obj.convert_currency = runAlloc.DynAlloc.convert_currency.__get__(obj)
    obj.resolve_home_conversion = runAlloc.DynAlloc.resolve_home_conversion.__get__(obj)

    rate, curr = obj.resolve_home_conversion('AED')
    assert curr == 'AED'
    assert abs(rate - (1.0 / 22.8)) < 1e-9


# --- New output columns present ----------------------------------------------

def test_new_columns_in_nextgen_summary_model_and_order():
    row = OutputSummaryNextGen()
    assert row.ConversionRate == 1.0
    assert row.Currency == 'INR'
    d = row.to_dict()
    assert 'ConversionRate' in d
    assert 'Currency' in d
    assert CONST_OUTPUT_SUMMARY_NEXTGEN_ORDER[-2:] == ['ConversionRate', 'Currency']


def test_new_columns_in_inventory_optimization_order():
    assert CONST_OUTPUT_INVENTORY_OPTIMIZATION_ORDER[-2:] == ['ConversionRate', 'Currency']


# --- Conversion applied to weighted fares, NOT to market fares ---------------

def test_weighted_fares_converted_market_fares_untouched():
    """Mirror the runAlloc inventory-optimization conversion block: weighted-file
    (INR) money columns scale by the rate; oal_b2c_min (market, already home) does not."""
    obj = _make_resolver()
    ng_rate, ng_curr = obj.resolve_home_conversion('AED')

    ng_opt_df = pd.DataFrame({
        'fare': [10000.0, 8000.0],
        'anchor_fare': [9000.0, 9000.0],
        'strategy_fare': [8500.0, 8500.0],
        'opening fare': [8400.0, 8400.0],
        'oal_b2c_min': [5000.0, 5000.0],          # market, home currency already
        'oal_b2c_min_All_in': [5500.0, 5500.0],   # market, home currency already
        'Current_Revenue': [100000.0, 100000.0],
        'Predicted_Revenue': [150000.0, 150000.0],
    })
    inr_money_cols = [
        'fare', 'anchor_fare', 'strategy_fare', 'opening fare',
        'Current_Revenue', 'Predicted_Revenue',
    ]
    expected = {c: (ng_opt_df[c] * ng_rate).tolist() for c in inr_money_cols}
    market_before = {c: ng_opt_df[c].tolist() for c in ['oal_b2c_min', 'oal_b2c_min_All_in']}

    if ng_rate != 1.0:
        for c in inr_money_cols:
            ng_opt_df[c] = pd.to_numeric(ng_opt_df[c], errors='coerce') * ng_rate
    ng_opt_df['ConversionRate'] = ng_rate
    ng_opt_df['Currency'] = ng_curr

    for c in inr_money_cols:
        assert ng_opt_df[c].tolist() == pytest.approx(expected[c])
    for c in ['oal_b2c_min', 'oal_b2c_min_All_in']:
        assert ng_opt_df[c].tolist() == market_before[c]
    assert ng_opt_df['Currency'].iloc[0] == 'AED'
    assert ng_opt_df['ConversionRate'].iloc[0] == ng_rate


def test_inr_sector_leaves_fares_unchanged():
    obj = _make_resolver()
    ng_rate, ng_curr = obj.resolve_home_conversion('INR')
    assert ng_rate == 1.0 and ng_curr == 'INR'

    ng_opt_df = pd.DataFrame({'fare': [10000.0], 'Current_Revenue': [100000.0]})
    before = ng_opt_df.copy()
    if ng_rate != 1.0:
        ng_opt_df['fare'] = ng_opt_df['fare'] * ng_rate
    assert ng_opt_df['fare'].tolist() == before['fare'].tolist()
