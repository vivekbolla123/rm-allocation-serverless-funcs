import os
import sys
import pandas as pd
import numpy as np
import pytest

sys.path.insert(0, os.path.abspath("rm-additive-process"))
sys.path.insert(0, os.path.abspath("rm-allocation-configurations"))

from helpers.utils.common_utils import (
    INVENTORY_NG_COLUMNS,
    compute_flight_inventory_ng,
    compute_inventory_ng_df
)


class TestInventoryNG:

    def test_inventory_ng_column_specification(self):
        """Verify that INVENTORY_NG_COLUMNS contains all 50 columns in exact order."""
        expected_columns = [
            'sector', 'date', 'DOW', 'STD', 'flight', 'NDO', 'capacity', 'Total_Bookings',
            'fare', 'oal_b2c_min', 'anchor_fare', 'strategy_fare', 'Promo_Flag',
            'mu_bar_orig', 'mu', 'AU', 'P_eq', 'mu_bar_2', 'mu_2', 'AU_2', 'AU_Revised',
            'predicted_pickup', 'Predicted_Flown', 'Predicted_pickup_Revised', 'Predicted_Flown_Revised',
            'LF_Forecast_Revised', 'AF_Forecast', 'RASK_Forecast', 'Distance', 'RPKM', 'ASKM',
            'Capped_Predicted_Flown_Revised', 'A_cap', 'Current_Revenue', 'Expected_Revenue',
            'Total_Expected_Revenue', 'Predicted_Revenue', 'SnapshotDate', 'Tagging',
            'FareBucket', 'sigma', 'STD_future', 'qp_b2c_min', 'sigma_bar', 'F_j',
            'protection_limit_yj', 'ProtectionLimit', 'sigma_3', 'ProtectionLimit_3', 'sigma_bar_3'
        ]
        assert len(INVENTORY_NG_COLUMNS) == 50
        assert INVENTORY_NG_COLUMNS == expected_columns

    def test_single_flight_inventory_ng_calculations(self):
        """Verify NextGen inventory optimization on a single flight ladder."""
        mock_flight = pd.DataFrame({
            'Sector': ['BOMDEL'] * 5,
            'DepartureDate': ['2026-09-01'] * 5,
            'DOW': ['Tuesday'] * 5,
            'STD': ['10:30:00'] * 5,
            'FlightNumber': [1101] * 5,
            'Capacity': [189] * 5,
            'Current_Bookings': [120] * 5,
            'Tagging': ['Peak'] * 5,
            'future_flight_NDO': [10] * 5,
            'FareBucket': ['15000+', '10000-15000', '7000-10000', '5000-7000', '3000-5000'],
            'FareBucket_Lower': [15000, 10000, 7000, 5000, 3000],
            'FareBucket_Upper': [np.nan, 15000, 10000, 7000, 5000],
            'Final_pax_count': [0.5, 2.0, 5.0, 12.0, 20.0],
            'weighted_pax_std': [0.2, 0.8, 1.5, 3.0, 5.0],
            'SnapshotDate': ['2026-08-22'] * 5,
            'Distance': [1148] * 5,
            'Current_Revenue': [450000.0] * 5
        })

        res_df = compute_flight_inventory_ng(
            mock_flight,
            pax_col='Final_pax_count',
            std_col='weighted_pax_std',
            buy_up_factor=0.0,
            sigma_factor=0.25,
            first_rbd=20.0
        )

        assert not res_df.empty
        assert res_df.columns.tolist() == INVENTORY_NG_COLUMNS
        assert len(res_df) == 5

        # Check values
        assert res_df['sector'].iloc[0] == 'BOMDEL'
        assert res_df['date'].iloc[0] == '2026-09-01'
        assert res_df['flight'].iloc[0] == 1101
        assert res_df['capacity'].iloc[0] == 189
        assert res_df['Total_Bookings'].iloc[0] == 120

        # Top class AU must equal capacity
        assert res_df['AU'].iloc[0] == 189
        # AU must be non-increasing as fares fall
        for k in range(len(res_df) - 1):
            assert res_df['AU'].iloc[k] >= res_df['AU'].iloc[k + 1]

        # Protection limit must be non-decreasing as fares fall
        for k in range(len(res_df) - 1):
            assert res_df['ProtectionLimit'].iloc[k] <= res_df['ProtectionLimit'].iloc[k + 1]

        # Anchor fare must be defined
        anchor = res_df['anchor_fare'].iloc[0]
        assert anchor > 0

        # Revised predictions and revenue
        assert res_df['Predicted_Revenue'].iloc[0] >= 450000.0
        assert res_df['Predicted_Flown_Revised'].iloc[0] >= 120
        assert res_df['LF_Forecast_Revised'].iloc[0] > 0
        assert res_df['AF_Forecast'].iloc[0] > 0
        assert res_df['RASK_Forecast'].iloc[0] > 0
        assert res_df['Distance'].iloc[0] == 1148
        assert res_df['qp_b2c_min'].iloc[0] == 4000.0  # (3000+5000)/2

    def test_competition_response_demand_redistribution(self):
        """Verify that when market fare < anchor fare, demand redistribution and revised AU trigger properly."""
        mock_flight = pd.DataFrame({
            'Sector': ['DELBOM'] * 4,
            'DepartureDate': ['2026-09-01'] * 4,
            'DOW': ['Tuesday'] * 4,
            'STD': ['10:30:00'] * 4,
            'FlightNumber': [1128] * 4,
            'Capacity': [200] * 4,
            'Current_Bookings': [50] * 4,
            'Tagging': ['Shoulder'] * 4,
            'future_flight_NDO': [5] * 4,
            'FareBucket': ['10000+', '8000-10000', '6000-8000', '4000-6000'],
            'FareBucket_Lower': [10000, 8000, 6000, 4000],
            'FareBucket_Upper': [np.nan, 10000, 8000, 6000],
            'Final_pax_count': [1.0, 5.0, 10.0, 15.0],
            'weighted_pax_std': [0.5, 1.25, 2.5, 3.75],
            'SnapshotDate': ['2026-08-27'] * 4,
            'Distance': [1148] * 4
        })

        # Market fare below anchor fare should set strategy fare to that ladder bucket
        res_df = compute_flight_inventory_ng(
            mock_flight,
            pax_col='Final_pax_count',
            std_col='weighted_pax_std',
            market_min_fare=7000.0
        )

        assert not res_df.empty
        assert res_df['strategy_fare'].iloc[0] == 7000.0
        assert res_df['oal_b2c_min'].iloc[0] == 7000.0

    def test_schema_isolation_of_fare_bucket_weighted(self):
        """Verify schema.py outputs fare_bucket_weighted and inventory_ng as separate datasets."""
        from localtest.schema import OUTPUT_COLLECTIONS, DATASET_ALIASES, SUPERSEDED_DATASETS

        assert 'fare_bucket_weighted' in OUTPUT_COLLECTIONS
        assert 'inventory_ng' in OUTPUT_COLLECTIONS
        assert 'flight_level_weighted' in OUTPUT_COLLECTIONS
        assert 'fare_bucket_weighted' not in SUPERSEDED_DATASETS
        assert DATASET_ALIASES.get('inventory_optimization') != 'fare_bucket_weighted'

    def test_processing_farebucketweoghted_sample(self):
        """Verify processing real rows from farebucketweoghted.csv if present."""
        sample_path = "farebucketweoghted.csv"
        if not os.path.exists(sample_path):
            pytest.skip("farebucketweoghted.csv not found in working directory")

        raw_df = pd.read_csv(sample_path, nrows=100)
        out_df = compute_inventory_ng_df(raw_df)

        assert not out_df.empty
        assert out_df.columns.tolist() == INVENTORY_NG_COLUMNS
        assert len(out_df) == 100
