import os
import sys
import io
import pytest
import pandas as pd
import numpy as np
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath("rm-additive-process/glue"))
sys.path.insert(0, os.path.abspath("rm-allocation-process-inputs"))
sys.path.insert(0, os.path.abspath("rm-allocation-configurations"))

import additive_model_dw_sync as glue_sync
from helper.rm_database_helper import RMDatabaseHelper


class TestAdditiveGlueSync:

    @pytest.fixture
    def sample_fare_bucket_df(self):
        return pd.DataFrame({
            'Sector': ['BOMDEL'] * 4,
            'DepartureDate': ['2026-10-01'] * 4,
            'FlightNumber': [1101] * 4,
            'Capacity': [189] * 4,
            'Current_Bookings': [100] * 4,
            'Tagging': ['Normal'] * 4,
            'FareBucket': ['10000+', '7000-10000', '5000-7000', '3000-5000'],
            'Final_pax_count': [1.5, 4.0, 10.0, 25.0],
            'weighted_pax_std': [0.5, 1.2, 2.5, 5.0],
            'SnapshotDate': ['2026-09-03'] * 4,
            'Distance': [1148] * 4,
            'Current_Revenue': [400000.0] * 4
        })

    def test_inventory_ng_columns_count_and_order(self, sample_fare_bucket_df):
        """Verify that the Glue script produces all 52 INVENTORY_NG_COLUMNS in exact order."""
        out_df = glue_sync.compute_inventory_ng_df(sample_fare_bucket_df)
        assert len(out_df.columns) == 52
        assert out_df.columns.tolist() == glue_sync.INVENTORY_NG_COLUMNS
        assert len(out_df) == 4
        assert out_df['sector'].iloc[0] == 'BOMDEL'
        assert out_df['flight'].iloc[0] == 1101
        assert 'AU_Revised' in out_df.columns
        assert 'ProtectionLimit' in out_df.columns

    def test_bucket_bounds_parsing(self):
        """Verify parsing of FareBucket string ranges into Lower and Upper bounds."""
        df = pd.DataFrame({
            'FareBucket': ['15000+', '10000-15000', '3000-5000']
        })
        df['FareBucket_Lower'] = (
            df['FareBucket'].astype(str).str.replace('+', '', regex=False)
            .str.split('-').str[0].astype(float)
        )
        df['FareBucket_Upper'] = pd.to_numeric(
            df['FareBucket'].astype(str).str.split('-').str[1], errors='coerce'
        )

        assert df['FareBucket_Lower'].tolist() == [15000.0, 10000.0, 3000.0]
        assert np.isnan(df['FareBucket_Upper'].iloc[0])
        assert df['FareBucket_Upper'].iloc[1] == 15000.0
        assert df['FareBucket_Upper'].iloc[2] == 5000.0

    def test_discover_latest_snapshot_date(self):
        """Verify latest snapshot date is discovered correctly from S3 common prefixes."""
        mock_s3 = MagicMock()
        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                'CommonPrefixes': [
                    {'Prefix': 'prod/additive_model/datasets/fare_bucket_weighted/snapshotdate=2026-08-30/'},
                    {'Prefix': 'prod/additive_model/datasets/fare_bucket_weighted/snapshotdate=2026-09-02/'},
                    {'Prefix': 'prod/additive_model/datasets/fare_bucket_weighted/snapshotdate=2026-09-01/'},
                ]
            }
        ]

        with patch.object(glue_sync, 's3_client', mock_s3):
            latest_date = glue_sync.discover_latest_snapshot_date('fare_bucket_weighted')
            assert latest_date == '2026-09-02'

    def test_upload_snapshot_to_s3(self, sample_fare_bucket_df):
        """Verify daily snapshot archival uploads both parquet and csv."""
        mock_s3 = MagicMock()
        with patch.object(glue_sync, 's3_client', mock_s3):
            glue_sync.upload_snapshot_to_s3(sample_fare_bucket_df, 'test_dataset', '2026-09-03', '120000')
            assert mock_s3.put_object.call_count == 2
            calls = mock_s3.put_object.call_args_list
            keys = [c[1]['Key'] for c in calls]
            assert any(k.endswith('.parquet') for k in keys)
            assert any(k.endswith('.csv') for k in keys)
            assert any('2026/09/03' in k for k in keys)

    def test_write_to_mongo_collection(self, sample_fare_bucket_df):
        """Verify MongoDB writing replaces collection and creates compound index."""
        mock_col = MagicMock()
        mock_db = MagicMock()
        mock_db.__getitem__.return_value = mock_col

        with patch.object(glue_sync, 'get_mongo_db', return_value=mock_db):
            glue_sync.write_to_mongo_collection(sample_fare_bucket_df, 'add_model_fare_bucket_level_weighted')
            mock_col.drop.assert_called_once()
            mock_col.insert_many.assert_called_once()
            mock_col.create_index.assert_called_once()
            inserted_docs = mock_col.insert_many.call_args[0][0]
            assert len(inserted_docs) == 4
            assert inserted_docs[0]['Sector'] == 'BOMDEL'
            assert inserted_docs[0]['FlightNumber'] == 1101

    def test_main_stores_weighted_file_and_nextgen_outputs_in_mongo(self, sample_fare_bucket_df):
        """Verify main() stores weighted file and NextGen outputs in MongoDB."""
        sample_flight_df = pd.DataFrame({
            'Sector': ['BOMDEL'],
            'DepartureDate': ['2026-10-01'],
            'FlightNumber': [1101],
            'Forecast_LF': [85.0]
        })

        def mock_read_parquet(dataset_name, *args, **kwargs):
            if dataset_name == 'fare_bucket_weighted':
                return sample_fare_bucket_df
            elif dataset_name == 'flight_level_weighted':
                return sample_flight_df
            return pd.DataFrame()

        with patch.object(glue_sync, 'discover_latest_snapshot_date', return_value='2026-09-03'), \
             patch.object(glue_sync, 'read_partitioned_parquet', side_effect=mock_read_parquet), \
             patch.object(glue_sync, 'upload_snapshot_to_s3'), \
             patch.object(glue_sync, 'write_to_mongo_collection') as mock_write_mongo:

            glue_sync.main()

            # Verify weighted file & NextGen outputs were written to Mongo
            mongo_col_names = [call[0][1] for call in mock_write_mongo.call_args_list]
            assert 'add_model_fare_bucket_level_weighted' in mongo_col_names
            assert 'add_model_inventory_ng' in mongo_col_names
            assert 'add_model_flight_level_weighted' in mongo_col_names


    def test_rm_allocation_process_inputs_reads_from_mongo_primary(self, sample_fare_bucket_df):
        """Verify that rm-allocation-process-inputs reads from MongoDB add_model_fare_bucket_level_weighted collection."""
        curr_params = MagicMock()
        curr_params.s3 = None
        curr_params.dwconn = None

        mock_mongo_db = MagicMock()
        mock_mongo_db.add_model_fare_bucket_level_weighted.find.return_value = sample_fare_bucket_df.to_dict('records')

        db_helper = RMDatabaseHelper(mock_mongo_db, None, "test_run", MagicMock(), False, None, curr_params)
        forecast_df = db_helper.get_additive_fare_bucket_forecast(
            sector="BOMDEL",
            departure_date="2026-10-01",
            flight_number=1101
        )

        assert forecast_df is not None
        assert not forecast_df.empty
        assert 'Final_pax_count' in forecast_df.columns
        assert 'weighted_pax_std' in forecast_df.columns
        assert 'FareBucket' in forecast_df.columns
        assert len(forecast_df) == 4
        mock_mongo_db.add_model_fare_bucket_level_weighted.find.assert_called_once()

    def test_rm_allocation_process_inputs_reads_inventory_ng_from_mongo(self):
        """Verify reading NextGen inventory optimization output from MongoDB."""
        curr_params = MagicMock()
        mock_mongo_db = MagicMock()
        mock_mongo_db.add_model_inventory_ng.find.return_value = [
            {'sector': 'BOMDEL', 'date': '2026-10-01', 'flight': 1101, 'AU_Revised': 189.0}
        ]

        db_helper = RMDatabaseHelper(mock_mongo_db, None, "test_run", MagicMock(), False, None, curr_params)
        df = db_helper.get_additive_inventory_ng(sector="BOMDEL", date="2026-10-01", flight=1101)

        assert df is not None
        assert len(df) == 1
        assert df['AU_Revised'].iloc[0] == 189.0
        mock_mongo_db.add_model_inventory_ng.find.assert_called_once()

    def test_rm_allocation_process_inputs_reads_flight_level_weighted_from_mongo(self):
        """Verify reading flight-level weighted forecast from MongoDB."""
        curr_params = MagicMock()
        mock_mongo_db = MagicMock()
        mock_mongo_db.add_model_flight_level_weighted.find.return_value = [
            {'Sector': 'BOMDEL', 'DepartureDate': '2026-10-01', 'FlightNumber': 1101, 'Forecast_LF': 85.0}
        ]

        db_helper = RMDatabaseHelper(mock_mongo_db, None, "test_run", MagicMock(), False, None, curr_params)
        df = db_helper.get_additive_flight_level_weighted(sector="BOMDEL", departure_date="2026-10-01", flight_number=1101)

        assert df is not None
        assert len(df) == 1
        assert df['Forecast_LF'].iloc[0] == 85.0
        mock_mongo_db.add_model_flight_level_weighted.find.assert_called_once()
