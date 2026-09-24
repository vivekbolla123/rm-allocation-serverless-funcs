import pytest
import sys
import os
import pandas as pd
import numpy as np

# Add workspace directories to sys.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'rm-allocation-process-inputs'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'rm-allocation-configurations'))

from common.fare_tax_helper import FareTaxHelper
from common.nextgen_optimization_engine import NextGenOptimizationEngine
from tests.test_fare_tax_helper import MockMongoMasterDb, MockConstantsUtils, MockRMDatabaseHelper


class MockCurrParams:
    def __init__(self, origin='BOM', destin='DEL', route='D'):
        self.origin = origin
        self.destin = destin
        self.route = route
        self.flight_number = 'QP1101'
        self.buy_up_factor_NG = 0.0
        self.competition_response_flag_NG = 1
        self.time_range = 2.0
        self.CompetitionResponseGrid_NG = 'DEFAULT'
        self.ndocounter = 10
        self.first_rbd_alloc = 20
        self.plfthreshold = 95.0
        self.sigma_factor = 0.25
        self.promo_fare_factor = 0.80
        self.coverage_parameter = 3.0
        self.connection_fare_flag_NG = 0
        self.additional_connection_duration_NG = 4.0
        self.analyst_name = 'TestAnalyst'


def test_nextgen_market_fare_tax_conversion():
    mongo_mock = MockMongoMasterDb()
    const_mock = MockConstantsUtils()
    rm_db_mock = MockRMDatabaseHelper()

    tax_helper = FareTaxHelper(
        mongo_master_db=mongo_mock,
        constants_utils=const_mock,
        rmdatabasehelper=rm_db_mock
    )

    curr_params = MockCurrParams('BOM', 'DEL', 'D')
    curr_params.tax_helper = tax_helper

    # Ladder in AIRLINE COMPONENT (base fares: 2600 to 9000)
    ladder_data = [
        {'FareBucket': 'B1', 'fare': 9000.0, 'mu': 5.0, 'sigma': 1.25},
        {'FareBucket': 'B2', 'fare': 7500.0, 'mu': 8.0, 'sigma': 2.0},
        {'FareBucket': 'B3', 'fare': 6000.0, 'mu': 12.0, 'sigma': 3.0},
        {'FareBucket': 'B4', 'fare': 4500.0, 'mu': 18.0, 'sigma': 4.5},
        {'FareBucket': 'B5', 'fare': 3500.0, 'mu': 25.0, 'sigma': 6.25},
        {'FareBucket': 'B6', 'fare': 2600.0, 'mu': 35.0, 'sigma': 8.75},
    ]
    ladder_df = pd.DataFrame(ladder_data)

    capacity = 189
    current_bookings = 80
    current_revenue = 80 * 4000.0

    # Market fares in ALL-IN fare (e.g. 5000 min, 6500 max)
    market_fares = pd.DataFrame([
        {
            'gross_fare': 5000.0,
            'outbound_airline_code': '6E',
            'outbound_flight': '6E101',
            'outbound_departuretime': '10:30',
            'outbound_stops': 0
        },
        {
            'gross_fare': 6500.0,
            'outbound_airline_code': 'AI',
            'outbound_flight': 'AI202',
            'outbound_departuretime': '11:00',
            'outbound_stops': 0
        }
    ])

    final_df, summary = NextGenOptimizationEngine.execute_flight_nextgen_optimization(
        ladder_df,
        capacity,
        current_bookings,
        current_revenue,
        market_fares=market_fares,
        flight_std='10:30',
        curr_params=curr_params,
        tax_helper=tax_helper
    )

    # 1. Output summary MarketFare is All-In for summary display
    assert summary['MarketFare_Min'] == 5000.0
    assert summary['MarketFare_Selected'] == 5000.0
    assert summary['MarketFare_Min_AllIn'] == 5000.0

    # 2. Output summary Anchor_fare, Strategy_fare, NG_Opening_fare are converted to ALL-IN for next gen output summary
    assert summary['Anchor_fare'] > 0
    assert summary['Strategy_fare'] > 0
    assert summary['NG_Opening_fare'] > 0
    assert summary['NG_Strategy_fare'] == summary['Strategy_fare']
    assert summary['NG_Anchor_fare'] == summary['Anchor_fare']
    # Airline component versions preserved
    assert summary['anchor_fare_airline_comp'] in [2600.0, 3500.0, 4500.0, 6000.0, 7500.0, 9000.0]
    assert summary['strategy_fare_airline_comp'] in [2600.0, 3500.0, 4500.0, 6000.0, 7500.0, 9000.0]
    assert summary['opening_fare_airline_comp'] in [2600.0, 3500.0, 4500.0, 6000.0, 7500.0, 9000.0]

    # 3. Final DF fare column is normal airline component and AU is unrounded
    assert (final_df['fare'].isin([2600.0, 3500.0, 4500.0, 6000.0, 7500.0, 9000.0])).all()
    assert pd.api.types.is_float_dtype(final_df['AU'])
    assert pd.api.types.is_float_dtype(final_df['AU_Revised'])


def test_nextgen_without_tax_helper_graceful_fallback():
    curr_params = MockCurrParams('BOM', 'DEL', 'D')

    ladder_data = [
        {'FareBucket': 'B1', 'fare': 5000.0, 'mu': 10.0, 'sigma': 2.5},
        {'FareBucket': 'B2', 'fare': 3000.0, 'mu': 20.0, 'sigma': 5.0},
    ]
    ladder_df = pd.DataFrame(ladder_data)

    final_df, summary = NextGenOptimizationEngine.execute_flight_nextgen_optimization(
        ladder_df,
        189,
        50,
        150000.0,
        curr_params=curr_params,
        tax_helper=None
    )

    assert summary['Anchor_fare'] in [3000.0, 5000.0]
    assert summary['Strategy_fare'] in [3000.0, 5000.0]
    assert (final_df['fare'].isin([3000.0, 5000.0])).all()


def test_nextgen_five_fare_metrics_airline_component_and_allin():
    """
    Verify that the 5 fare metrics:
    1. fare & fare_All_in
    2. oal_b2c_min & oal_b2c_min_All_in
    3. anchor_fare & anchor_fare_All_in
    4. strategy_fare & strategy_fare_All_in
    5. opening fare & opening fare_All_in
    correctly maintain airline component in original header and all-in in _All_in header.
    """
    tax_helper = FareTaxHelper(
        MockMongoMasterDb(),
        MockConstantsUtils(),
        MockRMDatabaseHelper(),
        logger=None
    )
    curr_params = MockCurrParams('BOM', 'DEL', 'D')
    curr_params.tax_helper = tax_helper

    ladder_data = [
        {'FareBucket': 'B1', 'fare': 9000.0, 'mu': 2.0, 'sigma': 0.5},
        {'FareBucket': 'B2', 'fare': 7500.0, 'mu': 5.0, 'sigma': 1.25},
        {'FareBucket': 'B3', 'fare': 6000.0, 'mu': 10.0, 'sigma': 2.5},
        {'FareBucket': 'B4', 'fare': 4500.0, 'mu': 15.0, 'sigma': 3.75},
        {'FareBucket': 'B5', 'fare': 3500.0, 'mu': 20.0, 'sigma': 5.0},
        {'FareBucket': 'B6', 'fare': 2600.0, 'mu': 30.0, 'sigma': 7.5}
    ]
    ladder_df = pd.DataFrame(ladder_data)
    market_fares = pd.DataFrame([
        {
            'gross_fare': 5000.0,
            'outbound_airline_code': '6E',
            'outbound_flight': '6E101',
            'outbound_departuretime': '10:30',
            'outbound_stops': 0
        }
    ])

    final_df, summary = NextGenOptimizationEngine.execute_flight_nextgen_optimization(
        ladder_df,
        189,
        50,
        250000.0,
        market_fares=market_fares,
        flight_std='10:30',
        curr_params=curr_params,
        tax_helper=tax_helper
    )

    # Check 1: fare & fare_All_in
    assert 'fare' in final_df.columns
    assert 'fare_All_in' in final_df.columns
    for _, row in final_df.iterrows():
        comp = row['fare']
        allin = row['fare_All_in']
        assert allin > comp
        assert abs(allin - tax_helper.airline_component_to_allin(comp, 'BOM', 'DEL', route='D')) < 1e-4

    # Check 2: oal_b2c_min & oal_b2c_min_All_in
    assert 'oal_b2c_min' in final_df.columns
    assert 'oal_b2c_min_All_in' in final_df.columns
    assert final_df['oal_b2c_min_All_in'].iloc[0] == 5000.0
    assert final_df['oal_b2c_min'].iloc[0] < 5000.0
    assert abs(final_df['oal_b2c_min_All_in'].iloc[0] - tax_helper.airline_component_to_allin(final_df['oal_b2c_min'].iloc[0], 'BOM', 'DEL', route='D')) < 1e-4

    # Check 3: anchor_fare & anchor_fare_All_in
    assert 'anchor_fare' in final_df.columns
    assert 'anchor_fare_All_in' in final_df.columns
    assert final_df['anchor_fare_All_in'].iloc[0] > final_df['anchor_fare'].iloc[0]
    assert abs(final_df['anchor_fare_All_in'].iloc[0] - tax_helper.airline_component_to_allin(final_df['anchor_fare'].iloc[0], 'BOM', 'DEL', route='D')) < 1e-4

    # Check 4: strategy_fare & strategy_fare_All_in
    assert 'strategy_fare' in final_df.columns
    assert 'strategy_fare_All_in' in final_df.columns
    assert final_df['strategy_fare_All_in'].iloc[0] > final_df['strategy_fare'].iloc[0]
    assert abs(final_df['strategy_fare_All_in'].iloc[0] - tax_helper.airline_component_to_allin(final_df['strategy_fare'].iloc[0], 'BOM', 'DEL', route='D')) < 1e-4

    # Check 5: opening fare & opening fare_All_in
    assert 'opening fare' in final_df.columns
    assert 'opening fare_All_in' in final_df.columns
    assert final_df['opening fare_All_in'].iloc[0] > final_df['opening fare'].iloc[0]
    assert abs(final_df['opening fare_All_in'].iloc[0] - tax_helper.airline_component_to_allin(final_df['opening fare'].iloc[0], 'BOM', 'DEL', route='D')) < 1e-4

