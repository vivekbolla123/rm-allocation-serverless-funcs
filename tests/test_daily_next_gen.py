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
    _NormAdapter,
    parse_time_to_minutes,
    filter_market_fares_by_time,
    get_ndo_band,
    get_plf_band,
    evaluate_grid_response,
    load_comp_response_grid,
    fetch_market_fares_b2c,
    load_market_list,
    match_market_list_config
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
        assert len(res) == 5  # 4 filed classes + 1 promo row
        assert res.columns.tolist() == INVENTORY_NG_COLUMNS
        assert res['sector'].iloc[0] == 'BOMDEL'
        assert res['flight'].iloc[0] == 1101
        assert res['capacity'].iloc[0] == 189.0
        assert res['AU'].iloc[0] == 189.0
        assert res['AU_Revised'].iloc[0] == 189.0
        promo = res.iloc[-1]
        assert promo['FareBucket'] == 'Promo'
        assert promo['Promo_Flag'] == 1
        assert promo['fare'] == round(0.80 * res['strategy_fare'].iloc[0], 2)

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
        assert len(res) == 4  # 3 filed classes + 1 promo row
        assert res['flight'].iloc[0] == 1109
        assert res['NDO'].iloc[0] == 12
        assert res['capacity'].iloc[0] == 186.0
        assert res['Total_Bookings'].iloc[0] == 95.0
        assert res.iloc[-1]['FareBucket'] == 'Promo'
        assert res.iloc[-1]['Promo_Flag'] == 1

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
        assert len(res) == 8  # 2 flights * (3 filed classes + 1 promo row)
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
        assert result['inventory_ng_rows'] == 5  # 4 filed + 1 promo row
        assert job.upload_snapshot_to_s3.call_count >= 1
        assert job.write_to_mongo_collection.call_count >= 1

    def test_write_to_mongo_collection_parallel_chunks(self):
        job = DailyNextGen()
        mock_col = MagicMock()
        mock_db = MagicMock()
        mock_db.__getitem__.return_value = mock_col
        job.get_mongo_db = MagicMock(return_value=mock_db)

        # 25 rows, chunksize=10 -> should produce 3 chunks (10, 10, 5)
        df = pd.DataFrame({
            'Sector': ['BOMDEL'] * 25,
            'DepartureDate': ['2026-10-01'] * 25,
            'FlightNumber': list(range(1001, 1026)),
            'Value': [float(i) for i in range(25)]
        })

        inserted_batches = []
        def mock_insert_many(batch, ordered=False):
            inserted_batches.append(len(batch))

        mock_col.insert_many.side_effect = mock_insert_many

        job.write_to_mongo_collection(df, 'test_parallel_col', chunksize=10, max_workers=2)

        mock_col.drop.assert_called_once()
        assert mock_col.insert_many.call_count == 3
        assert sum(inserted_batches) == 25
        assert set(inserted_batches) == {10, 5}
        mock_col.create_index.assert_called_once()

    def test_write_to_mongo_collection_retry(self):
        job = DailyNextGen()
        mock_col = MagicMock()
        mock_db = MagicMock()
        mock_db.__getitem__.return_value = mock_col
        job.get_mongo_db = MagicMock(return_value=mock_db)

        df = pd.DataFrame({
            'Sector': ['BOMDEL'],
            'DepartureDate': ['2026-10-01'],
            'FlightNumber': [1101]
        })

        call_state = {'count': 0}
        def flaky_insert(batch, ordered=False):
            call_state['count'] += 1
            if call_state['count'] == 1:
                raise ConnectionError("Simulated network blip")
            return None

        mock_col.insert_many.side_effect = flaky_insert

        job.write_to_mongo_collection(df, 'test_retry_col', max_retries=2, retry_delay=0.01)

        assert call_state['count'] == 2
        assert mock_col.drop.call_count == 2
        mock_col.create_index.assert_called_once()

    def test_run_pipeline_parallel_mongo_collections(self, sample_fare_bucket_df):
        job = DailyNextGen(overrides={
            'WRITE_MONGO': 'true',
            'WRITE_S3_SNAPSHOTS': 'false',
            'COMPUTE_INVENTORY_NG': 'true',
            'PARALLEL_MONGO_COLLECTIONS': 'true'
        })

        job.discover_latest_snapshot_date = MagicMock(return_value='2026-09-22')
        job.read_partitioned_parquet = MagicMock(return_value=sample_fare_bucket_df)
        job.write_to_mongo_collection = MagicMock()

        result = job.run()
        assert result['status'] == 'success'
        assert job.write_to_mongo_collection.call_count >= 2

    def test_parse_time_to_minutes(self):
        assert parse_time_to_minutes('14:30') == 870
        assert parse_time_to_minutes('02:30 AM') == 150
        assert parse_time_to_minutes('11:15 PM') == 1395
        assert parse_time_to_minutes('12:05 AM') == 5
        assert parse_time_to_minutes('12:15 PM') == 735
        assert parse_time_to_minutes(None) is None

    def test_filter_market_fares_time_rules_fr(self):
        # Sample market fares with varying departure times
        fares_df = pd.DataFrame({
            'gross_fare': [4000, 4200, 4500, 4800, 5000],
            'outbound_departuretime': ['01:30', '03:45', '10:00', '20:15', '23:30']
        })

        # Day flight (STD = 10:00 AM = 600 mins):
        # Rule: full 24 hrs except 11pm to 4am (23:00 to 04:00)
        # Included: 10:00 (600), 20:15 (1215)
        # Excluded: 01:30 (90), 03:45 (225), 23:30 (1410)
        res_day = filter_market_fares_by_time(fares_df, '10:00', time_range='FR')
        assert len(res_day) == 2
        assert set(res_day['gross_fare'].tolist()) == {4500, 4800}

        # Night flight (STD = 23:45 PM = 1425 mins or 02:00 AM = 120 mins):
        # Rule: if flight itself from 11pm to 4am then use whole 24 hrs
        res_night = filter_market_fares_by_time(fares_df, '23:45', time_range='FR')
        assert len(res_night) == 5

        res_early_morning = filter_market_fares_by_time(fares_df, '02:15', time_range='FR')
        assert len(res_early_morning) == 5

    def test_filter_market_fares_time_rules_tr_and_extr(self):
        fares_df = pd.DataFrame({
            'gross_fare': [4100, 4900, 5200],
            'outbound_departuretime': ['06:00', '15:00', '21:00']
        })

        # TR for STD = 10:00 AM:
        # +/- 2h window [08:00, 12:00] has no fares
        # +/- 4h window [06:00, 14:00] has '06:00' (4100)!
        res_tr = filter_market_fares_by_time(fares_df, '10:00', time_range='TR')
        assert len(res_tr) == 1
        assert res_tr['gross_fare'].iloc[0] == 4100

        # ExTR starts at +/- 4h [06:00, 14:00] directly:
        res_extr = filter_market_fares_by_time(fares_df, '10:00', time_range='ExTR')
        assert len(res_extr) == 1
        assert res_extr['gross_fare'].iloc[0] == 4100

    def test_bands_and_grid_response(self):
        assert get_ndo_band(0) == 0
        assert get_ndo_band(1) == 1
        assert get_ndo_band(2) == 2
        assert get_ndo_band(6) == 3
        assert get_ndo_band(12) == 4
        assert get_ndo_band(25) == 5
        assert get_ndo_band(45) == 6
        assert get_ndo_band(75) == 7

        assert get_plf_band(75.0) == 0
        assert get_plf_band(82.0) == 1
        assert get_plf_band(88.0) == 2
        assert get_plf_band(92.0) == 3
        assert get_plf_band(97.0) == 4
        assert get_plf_band(102.0) == 5

        # Test evaluate_grid_response
        assert abs(evaluate_grid_response('0.95*C', anchor_fare=4000, comp_fare=5000) - 4750.0) < 1e-4
        assert abs(evaluate_grid_response('min(1.05*C, A)', anchor_fare=4000, comp_fare=5000) - 4000.0) < 1e-4
        assert abs(evaluate_grid_response('C', anchor_fare=4000, comp_fare=5200) - 5200.0) < 1e-4

    def test_compute_flight_inventory_ng_competition_response(self, sample_fare_bucket_df):
        # Case 1: Market fare < anchor fare (3000-5000 bucket, anchor = 3000)
        # Competitor fare = 2500 -> oal_b2c_min < anchor_fare -> strategy_fare = anchor_fare, no demand redistribution
        market_fares_low = pd.DataFrame({
            'gross_fare': [2500.0],
            'outbound_departuretime': ['10:00']
        })
        grid_doc = {
            'time_range': 'TR',
            'criteria': 'Min',
            'response_rule': 'C'
        }
        res_low = compute_flight_inventory_ng(
            sample_fare_bucket_df,
            grid_doc=grid_doc,
            market_fares_df=market_fares_low
        )
        assert res_low['oal_b2c_min'].iloc[0] == 2500.0
        assert res_low['strategy_fare'].iloc[0] == res_low['anchor_fare'].iloc[0]

        # Case 2: Market fare >= anchor fare
        # Competitor fare = 7500 -> strategy_fare maps to 7000 (nearest <= 7500)
        # Demand redistribution occurs: AU_2 and AU_Revised are computed
        market_fares_high = pd.DataFrame({
            'gross_fare': [7500.0],
            'outbound_departuretime': ['10:00']
        })
        res_high = compute_flight_inventory_ng(
            sample_fare_bucket_df,
            grid_doc=grid_doc,
            market_fares_df=market_fares_high
        )
        assert res_high['oal_b2c_min'].iloc[0] == 7500.0
        assert res_high['strategy_fare'].iloc[0] == 7000.0
        assert res_high['strategy_fare'].iloc[0] > res_high['anchor_fare'].iloc[0]
        # Buckets below strategy fare (e.g. 5000 and 3000) have mu_2 == 0 and AU_Revised == 0
        assert res_high['AU_Revised'].iloc[-1] == 0.0
        assert res_high['AU_Revised'].iloc[-2] == 0.0

    def test_connection_fare_flag_and_duration_filtering(self):
        # 3 competitor market fares:
        # 1: Direct flight (stops=0, duration=120 mins) -> gross_fare 4000
        # 2: Connecting flight within duration limit (stops=1, duration=300 mins) -> gross_fare 3500
        # 3: Connecting flight exceeding duration limit (stops=1, duration=600 mins) -> gross_fare 3000
        fares_df = pd.DataFrame({
            'gross_fare': [4000.0, 3500.0, 3000.0],
            'outbound_departuretime': ['12:00', '12:30', '13:00'],
            'outbound_stops': [0, 1, 1],
            'outbound_duration': ['2:00', '5:00', '10:00']
        })

        # When connection_fare_flag = 0 (disabled), only direct flight is kept
        res_direct = filter_market_fares_by_time(
            fares_df, flight_std='12:00', time_range='FR',
            connection_fare_flag=0, direct_duration_mins=120.0, additional_connection_duration=4.0
        )
        assert len(res_direct) == 1
        assert res_direct['gross_fare'].iloc[0] == 4000.0

        # When connection_fare_flag = 1 (enabled), connecting within direct + 4h (120 + 240 = 360m) is kept, but 600m is excluded
        res_conn = filter_market_fares_by_time(
            fares_df, flight_std='12:00', time_range='FR',
            connection_fare_flag=1, direct_duration_mins=120.0, additional_connection_duration=4.0
        )
        assert len(res_conn) == 2
        assert set(res_conn['gross_fare']) == {4000.0, 3500.0}

    def test_carrier_and_flight_exclusion(self):
        fares_df = pd.DataFrame({
            'gross_fare': [3000.0, 4000.0, 5000.0],
            'outbound_departuretime': ['12:00', '12:00', '12:00'],
            'outbound_airline_code': ['6E', 'AI', 'SG'],
            'outbound_flight_no': ['101', '202', '303']
        })

        # Exclude 6E
        res_carr = filter_market_fares_by_time(
            fares_df, flight_std='12:00', time_range='FR',
            carrier_exclusion='6E'
        )
        assert set(res_carr['outbound_airline_code']) == {'AI', 'SG'}

        # Exclude flight 202
        res_flt = filter_market_fares_by_time(
            fares_df, flight_std='12:00', time_range='FR',
            flight_exclusion='202'
        )
        assert set(res_flt['outbound_flight_no']) == {'101', '303'}

    def test_competition_response_flag_disabled(self, sample_fare_bucket_df):
        market_fares_high = pd.DataFrame({
            'gross_fare': [7500.0],
            'outbound_departuretime': ['10:00']
        })
        grid_doc = {
            'time_range': 'TR',
            'criteria': 'Min',
            'response_rule': 'C'
        }
        # With competition_response_enabled=False (from competition_response_flag_NG=0),
        # strategy_fare remains anchor_fare even when market fare > anchor fare
        res = compute_flight_inventory_ng(
            sample_fare_bucket_df,
            grid_doc=grid_doc,
            market_fares_df=market_fares_high,
            competition_response_enabled=False
        )
        assert res['strategy_fare'].iloc[0] == res['anchor_fare'].iloc[0]

    def test_promo_flag_ng(self, sample_fare_bucket_df):
        res_promo_0 = compute_flight_inventory_ng(sample_fare_bucket_df, promo_flag=0)
        assert (res_promo_0['Promo_Flag'].iloc[:-1] == 0).all()
        promo_0 = res_promo_0.iloc[-1]
        assert promo_0['FareBucket'] == 'Promo'
        assert promo_0['Promo_Flag'] == 1
        assert promo_0['fare'] == round(0.80 * res_promo_0['strategy_fare'].iloc[0], 2)
        assert promo_0['AU_Revised'] == 0.0

        res_promo_1 = compute_flight_inventory_ng(sample_fare_bucket_df, promo_flag=1)
        assert (res_promo_1['Promo_Flag'].iloc[:-1] == 0).all()
        promo_1 = res_promo_1.iloc[-1]
        assert promo_1['FareBucket'] == 'Promo'
        assert promo_1['Promo_Flag'] == 1
        assert promo_1['fare'] == round(0.80 * res_promo_1['strategy_fare'].iloc[0], 2)
        assert promo_1['AU_Revised'] > 0.0

    def test_promo_fare_regardless_of_flag_with_competition(self, sample_fare_bucket_df):
        """Verifies promo fare is always calculated as promo_ratio * strategy_fare regardless of flag, under competition."""
        market_fares_high = pd.DataFrame({
            'gross_fare': [8000.0, 9000.0],
            'outbound_departuretime': ['10:00', '10:15'],
            'outbound_airline_code': ['SG', '6E'],
            'outbound_stops': [0, 0]
        })
        grid_doc = {
            'time_range': 'TR',
            'criteria': 'Min',
            'response_rule': 'C'
        }
        # Flag = 0
        res_comp_0 = compute_flight_inventory_ng(
            sample_fare_bucket_df,
            grid_doc=grid_doc,
            market_fares_df=market_fares_high,
            competition_response_enabled=True,
            promo_flag=0
        )
        assert res_comp_0['strategy_fare'].iloc[0] == 7000.0
        promo_row_0 = res_comp_0.iloc[-1]
        assert promo_row_0['FareBucket'] == 'Promo'
        assert promo_row_0['Promo_Flag'] == 1
        # Promo fare is 80% of strategy fare (7000 * 0.8 = 5600)
        assert promo_row_0['fare'] == 5600.0
        assert promo_row_0['AU_Revised'] == 0.0

        # Flag = 1
        res_comp_1 = compute_flight_inventory_ng(
            sample_fare_bucket_df,
            grid_doc=grid_doc,
            market_fares_df=market_fares_high,
            competition_response_enabled=True,
            promo_flag=1
        )
        promo_row_1 = res_comp_1.iloc[-1]
        assert promo_row_1['FareBucket'] == 'Promo'
        assert promo_row_1['Promo_Flag'] == 1
        assert promo_row_1['fare'] == 5600.0
        # Since strategy_fare is 7000 and lowest filed fare is 3000, promo is below strategy floor
        assert promo_row_1['AU_2'] == 0.0
        assert promo_row_1['AU_Revised'] == 0.0

    def test_first_rbd_alloc(self, sample_fare_bucket_df):
        # When first_rbd=5, au_revised at the opening bucket is capped at tot_bookings (100) + 5 = 105
        res = compute_flight_inventory_ng(sample_fare_bucket_df, first_rbd=5.0)
        assert res['AU_Revised'].iloc[3] == 105.0

    def test_compute_inventory_ng_df_with_market_list(self, sample_fare_bucket_df):
        # Create a mock market_list with flight configuration
        market_list_mock = pd.DataFrame([
            {
                'Origin': 'BOM',
                'Destin': 'DEL',
                'FlightNumber': 1101,
                'PerStart': '2026-10-01',
                'PerEnd': '2026-10-01',
                'DOW': '1111111',
                'buy_up_factor_NG': 0.1,
                'connection_fare_flag_NG': 1,
                'additional_connection_duration_NG': 3.0,
                'CompetitionResponseGrid_NG': 'DEFAULT',
                'competition_response_flag_NG': 1,
                'PromoFlag_NG': 1,
                'FirstRBDAlloc': 8.0,
                'CarrExlusionB2C': 'SG'
            }
        ])

        # Market fares with SG and AI
        mkt_fares_mock = pd.DataFrame({
            'gross_fare': [7000.0, 8000.0],
            'outbound_departuretime': ['10:00', '10:00'],
            'outbound_airline_code': ['SG', 'AI'],
            'outbound_stops': [0, 0]
        })

        with patch('daily_next_gen.fetch_market_fares_b2c', return_value=mkt_fares_mock):
            with patch('daily_next_gen.load_comp_response_grid', return_value={}):
                res_df = compute_inventory_ng_df(
                    sample_fare_bucket_df,
                    market_list_df=market_list_mock
                )
                assert not res_df.empty
                # Filed classes have Promo_Flag = 0
                assert (res_df['Promo_Flag'].iloc[:-1] == 0).all()
                # Promo row has Promo_Flag = 1 and promo fare calculated regardless of flag
                promo_row = res_df.iloc[-1]
                assert promo_row['FareBucket'] == 'Promo'
                assert promo_row['Promo_Flag'] == 1
                assert promo_row['fare'] == round(0.80 * res_df['strategy_fare'].iloc[0], 2)
                # FirstRBDAlloc was 8 -> capped at 100 + 8 = 108
                assert res_df['AU_Revised'].iloc[3] == 108.0

    def test_backstop_floor(self, sample_fare_bucket_df):
        """Verify backstop floor sets AU_Revised=0 for fares below backstop and floors opening fare."""
        # Fares in sample: 10000, 7000, 5000, 3000
        res = compute_flight_inventory_ng(sample_fare_bucket_df, backstop=6000.0)
        assert not res.empty
        # Fares < 6000 (5000, 3000) must have AU_Revised = 0
        assert res.loc[res['fare'] < 6000.0, 'AU_Revised'].sum() == 0.0
        # Fares >= 6000 (10000, 7000) have non-zero AU_Revised
        assert res.loc[res['fare'] >= 6000.0, 'AU_Revised'].iloc[0] > 0
        # Opening fare cannot be lower than backstop
        assert res['opening fare'].iloc[0] >= 6000.0
        # Promo row fare (0.8 * 3000 = 2400) is below backstop, so AU_Revised must be 0
        promo = res.iloc[-1]
        assert promo['fare'] < 6000.0
        assert promo['AU_Revised'] == 0.0

    def test_hardanchor_capping(self, sample_fare_bucket_df):
        """Verify hardanchor caps AU_Revised with PLF threshold starting from nearest fare <= hardanchor."""
        # Bookings=100, Capacity=189 -> PLF ~ 52.9% < 95%
        # Nearest fare <= 6000 is 5000
        # PLF threshold = 95% -> plf_target = 179.55
        res = compute_flight_inventory_ng(sample_fare_bucket_df, hardanchor=6000.0, plf_threshold=95.0)
        assert not res.empty
        # Classes >= 5000 (10000, 7000, 5000) have AU_Revised >= 179.55
        fares_elevated = res.loc[res['fare'] >= 5000.0, 'AU_Revised'].tolist()
        assert all(au >= 179.55 for au in fares_elevated)

        # With 100% PLF threshold, all classes >= 5000 become 189.0
        res_100 = compute_flight_inventory_ng(sample_fare_bucket_df, hardanchor=6000.0, plf_threshold=100.0)
        fares_100 = res_100.loc[res_100['fare'] >= 5000.0, 'AU_Revised'].tolist()
        assert all(au == 189.0 for au in fares_100)

    def test_tolerance_grid_vs_ml_input(self, sample_fare_bucket_df):
        """Verify that higher fare between Grid tolerance and ML tolerance is chosen as tolerance,
        and max of tolerance fare and grid calculation fare determines Strategy fare."""
        mkt_fares = pd.DataFrame({
            'gross_fare': [6000.0],
            'outbound_departuretime': ['10:00'],
            'outbound_airline_code': ['6E'],
            'outbound_stops': [0]
        })
        grid_doc = {
            'response_rule': 'Min (2A, 0.8C)',
            'criteria': 'Min',
            'tolerance': 0.90
        }
        # Case A: Grid tolerance 0.90 -> 5400 > ML tolerance 5000 -> chosen tolerance = 5400
        res = compute_flight_inventory_ng(
            sample_fare_bucket_df,
            market_fares_df=mkt_fares,
            grid_doc=grid_doc,
            tolerance=5000.0
        )
        assert res['strategy_fare'].iloc[0] == 5000.0

        # Case B: ML tolerance 5600 > Grid tolerance 5400 -> chosen tolerance = 5600
        res_ml = compute_flight_inventory_ng(
            sample_fare_bucket_df,
            market_fares_df=mkt_fares,
            grid_doc=grid_doc,
            tolerance=5600.0
        )
        assert res_ml['strategy_fare'].iloc[0] == 5000.0

    def test_tax_conversion_allin_to_airline_component(self, sample_fare_bucket_df):
        """Verify that competitor market fares are converted from All-In to Airline Component via tax_helper."""
        mock_tax_helper = MagicMock()
        mock_tax_helper.allin_to_airline_component.side_effect = lambda f, o, d, route=None: f * 0.90  # 10% tax deduction

        mkt_fares = pd.DataFrame({
            'gross_fare': [8000.0],
            'outbound_departuretime': ['10:00'],
            'outbound_airline_code': ['6E'],
            'outbound_stops': [0]
        })

        res = compute_flight_inventory_ng(
            sample_fare_bucket_df,
            market_fares_df=mkt_fares,
            tax_helper=mock_tax_helper,
            origin='BOM',
            destin='DEL'
        )
        assert not res.empty
        # Gross fare 8000 converted to 7200 in airline component
        assert res['oal_b2c_min'].iloc[0] == 7200.0
        assert mock_tax_helper.allin_to_airline_component.called



