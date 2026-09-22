import os
import sys
import io
import pytest
import pandas as pd
import numpy as np
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath("rm-allocation-scheduled-jobs"))
sys.path.insert(0, os.path.abspath("rm-allocation-configurations"))

import daily_next_gen
from daily_next_gen import (
    DailyNextGen,
    INVENTORY_NG_COLUMNS,
    compute_flight_inventory_ng,
    compute_inventory_ng_df,
    _NormAdapter
)


class TestDailyNextGen:

    @pytest.fixture
    def sample_fare_bucket_df(self):
        return pd.DataFrame({
            'Sector': ['BOMDEL'] * 4,
            'DepartureDate': ['2026-10-01'] * 4,
            'FlightNumber': [1101] * 4,
            'Capacity': [189] * 4,
            'Current_Bookings': [100] * 4,
            'Tagging': ['Normal'] * 4,
            'future_flight_NDO': [10] * 4,
            'FareBucket': ['10000+', '7000-10000', '5000-7000', '3000-5000'],
            'Final_pax_count': [1.5, 4.0, 10.0, 25.0],
            'weighted_pax_std': [0.5, 1.2, 2.5, 5.0],
            'SnapshotDate': ['2026-09-22'] * 4,
            'Distance': [1148] * 4,
            'Current_Revenue': [400000.0] * 4
        })

    def test_norm_adapter(self):
        adapter = _NormAdapter()
        # Test scalar
        ppf_val = adapter.ppf(0.5)
        assert abs(ppf_val) < 1e-4

        # Test array
        arr = np.array([0.5, 0.8413447])
        arr_ppf = adapter.ppf(arr)
        assert len(arr_ppf) == 2
        assert abs(arr_ppf[0]) < 1e-4
        assert abs(arr_ppf[1] - 1.0) < 0.01

        # Test pdf and cdf
        assert abs(adapter.pdf(0.0) - 0.39894228) < 1e-4
        assert abs(adapter.cdf(0.0) - 0.5) < 1e-4

    def test_overrides_case_insensitivity(self):
        # Uppercase
        job_upper = DailyNextGen(overrides={'ENV': 'uat', 'SNAPSHOT_DATE': '2026-09-22', 'WRITE_MONGO': 'false'})
        assert job_upper.env == 'uat'
        assert job_upper.snapshot_date_arg == '2026-09-22'
        assert job_upper.write_mongo is False

        # Lowercase
        job_lower = DailyNextGen(overrides={'env': 'uat', 'snapshot_date': '2026-09-22', 'write_mongo': 'false'})
        assert job_lower.env == 'uat'
        assert job_lower.snapshot_date_arg == '2026-09-22'
        assert job_lower.write_mongo is False

    def test_compute_flight_inventory_ng_structure(self, sample_fare_bucket_df):
        res = compute_flight_inventory_ng(sample_fare_bucket_df)
        assert not res.empty
        assert len(res) == 4
        assert res.columns.tolist() == INVENTORY_NG_COLUMNS
        assert res['sector'].iloc[0] == 'BOMDEL'
        assert res['flight'].iloc[0] == 1101
        assert res['capacity'].iloc[0] == 189.0
        assert res['AU'].iloc[0] == 189.0
        assert res['AU_Revised'].iloc[0] == 189.0

    def test_compute_flight_inventory_ng_robust_inputs(self):
        # String flight number, float NDO, dirty FareBucket
        df = pd.DataFrame({
            'Sector': ['DEL-BLR'] * 3,
            'DepartureDate': ['2026-10-05'] * 3,
            'FlightNumber': ['QP-1109.0'] * 3,
            'Capacity': ['186'] * 3,
            'Total_Bookings': ['95.0'] * 3,
            'future_flight_NDO': ['12.0'] * 3,
            'FareBucket': ['12000+', '8000-12000', '4000-8000'],
            'OptimizationDemand': [1.0, 5.0, 15.0],
            'OptimizationDemandStd': [0.5, 1.0, 3.0]
        })
        res = compute_flight_inventory_ng(df)
        assert not res.empty
        assert len(res) == 3
        assert res['flight'].iloc[0] == 1109
        assert res['NDO'].iloc[0] == 12
        assert res['capacity'].iloc[0] == 186.0
        assert res['Total_Bookings'].iloc[0] == 95.0

    def test_compute_inventory_ng_df_grouping(self):
        df = pd.DataFrame({
            'Sector': ['BOMDEL'] * 3 + ['DELBOM'] * 3,
            'DepartureDate': ['2026-10-01'] * 6,
            'FlightNumber': [1101] * 3 + [1102] * 3,
            'Capacity': [189] * 6,
            'Current_Bookings': [50] * 6,
            'FareBucket': ['9000+', '6000-9000', '3000-6000'] * 2,
            'Final_pax_count': [1.0, 4.0, 10.0] * 2,
            'weighted_pax_std': [0.5, 1.0, 2.0] * 2
        })
        res = compute_inventory_ng_df(df)
        assert len(res) == 6
        assert set(res['sector'].unique()) == {'BOMDEL', 'DELBOM'}
        assert set(res['flight'].unique()) == {1101, 1102}

    def test_discover_latest_snapshot_date(self):
        job = DailyNextGen()
        mock_s3 = MagicMock()
        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                'CommonPrefixes': [
                    {'Prefix': 'prod/additive_model/datasets/fare_bucket_weighted/snapshotdate=2026-09-18/'},
                    {'Prefix': 'prod/additive_model/datasets/fare_bucket_weighted/snapshotdate=2026-09-22/'},
                    {'Prefix': 'prod/additive_model/datasets/fare_bucket_weighted/snapshotdate=2026-09-20/'},
                ]
            }
        ]
        job.s3_client = mock_s3
        latest = job.discover_latest_snapshot_date('fare_bucket_weighted')
        assert latest == '2026-09-22'

    def test_read_partitioned_parquet_hyphenated_sector(self):
        job = DailyNextGen()
        mock_s3 = MagicMock()
        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                'Contents': [
                    {'Key': 'prod/additive_model/datasets/fare_bucket_weighted/snapshotdate=2026-09-22/sector=BOM-DEL/data.parquet'},
                    {'Key': 'prod/additive_model/datasets/fare_bucket_weighted/snapshotdate=2026-09-22/sector=DEL_BLR/data.parquet'}
                ]
            }
        ]

        def mock_get_object(Bucket, Key):
            # Return parquet bytes
            df = pd.DataFrame({'FareBucket': ['5000+'], 'Final_pax_count': [2.0], 'weighted_pax_std': [1.0]})
            buf = io.BytesIO()
            df.to_parquet(buf, index=False)
            return {'Body': io.BytesIO(buf.getvalue())}

        mock_s3.get_object.side_effect = mock_get_object
        job.s3_client = mock_s3

        res_df = job.read_partitioned_parquet('fare_bucket_weighted', '2026-09-22')
        assert not res_df.empty
        assert len(res_df) == 2
        assert 'BOM-DEL' in res_df['Sector'].values
        assert 'DEL_BLR' in res_df['Sector'].values

    def test_write_to_mongo_collection_bson_cleaning(self):
        job = DailyNextGen()
        mock_col = MagicMock()
        mock_db = MagicMock()
        mock_db.__getitem__.return_value = mock_col
        job.get_mongo_db = MagicMock(return_value=mock_db)

        # DataFrame containing pd.NA, np.nan, inf, datetimes
        dirty_df = pd.DataFrame({
            'Sector': ['BOMDEL', 'DELBOM'],
            'DepartureDate': pd.to_datetime(['2026-10-01', '2026-10-02']),
            'FlightNumber': [1101, 1102],
            'Nullable_Col': [pd.NA, 'valid_val'],
            'Inf_Col': [np.inf, 42.0],
            'Nan_Col': [np.nan, 3.14]
        })

        job.write_to_mongo_collection(dirty_df, 'test_collection')

        mock_col.drop.assert_called_once()
        mock_col.insert_many.assert_called_once()
        mock_col.create_index.assert_called_once()

        inserted_docs = mock_col.insert_many.call_args[0][0]
        assert len(inserted_docs) == 2
        # pd.NA must be converted to None for BSON
        assert inserted_docs[0]['Nullable_Col'] is None
        assert inserted_docs[1]['Nullable_Col'] == 'valid_val'
        # inf must be converted to None/nan
        assert inserted_docs[0]['Inf_Col'] is None or np.isnan(inserted_docs[0]['Inf_Col'])

    def test_run_pipeline_success(self, sample_fare_bucket_df):
        job = DailyNextGen(overrides={'WRITE_MONGO': 'true', 'WRITE_S3_SNAPSHOTS': 'true', 'COMPUTE_INVENTORY_NG': 'true'})

        job.discover_latest_snapshot_date = MagicMock(return_value='2026-09-22')
        job.read_partitioned_parquet = MagicMock(return_value=sample_fare_bucket_df)
        job.upload_snapshot_to_s3 = MagicMock()
        job.write_to_mongo_collection = MagicMock()

        result = job.run()
        assert result['status'] == 'success'
        assert result['snapshot_date'] == '2026-09-22'
        assert result['fare_bucket_rows'] == 4
        assert result['inventory_ng_rows'] == 4
        assert job.upload_snapshot_to_s3.call_count >= 1
        assert job.write_to_mongo_collection.call_count >= 1
