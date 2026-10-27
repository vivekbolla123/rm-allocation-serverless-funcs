import os
import sys
import pandas as pd
import numpy as np
import pytest
import datetime
from unittest.mock import MagicMock

sys.path.insert(0, os.path.abspath("rm-allocation-process-inputs"))
sys.path.insert(0, os.path.abspath("rm-allocation-configurations"))

import Constants
from models.market_list_row import MarketListRow
from models.output_summary_nextgen import OutputSummaryNextGen
from common.nextgen_optimization_engine import NextGenOptimizationEngine


class TestNextGenLocalIntegration:

    @pytest.fixture
    def mock_db_helper(self):
        helper = MagicMock()
        helper.getCompResponseGrid.return_value = {
            "grid_name": "DEFAULT", "ndo_band": 0, "plf_band": 0,
            "time_range": "TR", "criteria": "Min", "response_rule": "Min (1.2A, 0.8C)"
        }
        helper.getCompResponsePlfBand.return_value = 0
        helper.getCompFareRecalibration.return_value = {"Sector": "BOMDEL", "6E": 100, "SG": 200, "AI": -60}
        helper.fareExtract.return_value = pd.DataFrame({
            'RBD': ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'Q', 'T', 'V'],
            'Total': [94338.0, 40483.0, 35127.0, 30463.0, 24575.0, 20545.0, 16416.0, 13655.0,
                      11352.0, 9272.0, 7568.0, 6409.0, 5252.0, 4279.0, 3526.0, 2914.0]
        })
        return helper

    def test_market_list_row_parsing(self):
        """Verify that MarketListRow parses all NextGen input fields and reuses existing columns correctly."""
        raw_dict = {
            "Origin": "BOM",
            "Destin": "DEL",
            "FlightNumber": 1127,
            "PerStart": "08/01/2026",
            "PerEnd": "08/01/2026",
            "DOW": "1111111",
            "B2CBackstop": 3500.0,
            "hardAnchor": 30000.0,
            "CarrExlusionB2C": "6E,SG",
            "buy_up_factor_NG": 0.15,
            "connection_fare_flag_NG": 1,
            "additional_connection_duration_NG": 3.5,
            "CompetitionResponseGrid_NG": "DEFAULT",
            "nextgen_flag_NG": 1,
            "competition_response_flag_NG": 1
        }
        row = MarketListRow(raw_dict)
        assert row.origin == "BOM"
        assert row.destin == "DEL"
        assert row.flight_number == 1127
        assert row.price_floor == 3500.0
        assert row.b2c_backstop == 3500.0
        assert row.price_cap == 30000.0
        assert row.hardanchor == 30000.0
        assert row.carrier_exclusion == "6E,SG"
        assert row.carr_exclusion_b2c == "6E,SG"
        assert row.buy_up_factor_NG == 0.15
        assert row.buy_up_factor == 0.15
        assert row.connection_fare_flag_NG == 1
        assert row.connection_fare_flag == 1
        assert row.additional_connection_duration_NG == 3.5
        assert row.additional_connection_duration == 3.5
        assert row.CompetitionResponseGrid_NG == "DEFAULT"
        assert row.grid_name == "DEFAULT"
        assert row.nextgen_flag_NG == 1
        assert row.nextgen_flag == 1
        assert row.competition_response_flag_NG == 1
        assert row.competition_response_flag == 1

    def test_end_to_end_flight_nextgen_execution(self, mock_db_helper):
        """Verify full flight execution produces valid ladder, revised AU, and NextGen summary."""
        ladder_df = pd.DataFrame({
            'FareBucket': ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'Q', 'T', 'V'],
            'fare': [94338.0, 40483.0, 35127.0, 30463.0, 24575.0, 20545.0, 16416.0, 13655.0,
                     11352.0, 9272.0, 7568.0, 6409.0, 5252.0, 4279.0, 3526.0, 2914.0],
            'mu': [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.15, 2.92, 5.20, 4.15, 2.14, 0.94, 0.49, 0.03, 0.0],
            'sigma': [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.50, 4.78, 4.51, 4.70, 4.30, 2.22, 1.29, 0.16, 0.0]
        })
        market_fares = [
            {'AirlineCode': '6E', 'fare': 4500.0, 'DepTime': '10:30'},
            {'AirlineCode': 'SG', 'fare': 4200.0, 'DepTime': '11:00'},
            {'AirlineCode': 'AI', 'fare': 5100.0, 'DepTime': '10:00'},
        ]
        curr_params = MagicMock()
        curr_params.buy_up_factor_NG = 0.0
        curr_params.buy_up_factor = 0.0
        curr_params.competition_response_flag_NG = 1
        curr_params.competition_response_flag = 1
        curr_params.time_range = 2.0
        curr_params.carr_exclusion_b2c = None
        curr_params.carrier_exclusion = None
        curr_params.CompetitionResponseGrid_NG = "DEFAULT"
        curr_params.grid_name = "DEFAULT"
        curr_params.b2c_backstop = 3000.0
        curr_params.price_floor = 3000.0
        curr_params.hardanchor = 40000.0
        curr_params.price_cap = 40000.0
        curr_params.nextgen_flag_NG = 1
        curr_params.nextgen_flag = 1
        curr_params.ndocounter = 2

        capacity = 189
        cb = 147
        curr_rev = 961206.34

        opt_df, summary = NextGenOptimizationEngine.execute_flight_nextgen_optimization(
            ladder_df, capacity, cb, curr_rev,
            market_fares=market_fares,
            flight_std=datetime.datetime(2026, 8, 17, 10, 30),
            curr_params=curr_params,
            recalibration_dict={"default": 0}
        )

        assert not opt_df.empty
        assert 'AU_Revised' in opt_df.columns
        assert 'Expected_Revenue' in opt_df.columns
        assert summary['Anchor_fare'] == 2914.0
        assert summary['Strategy_fare'] >= summary['Anchor_fare']
        assert summary['Predicted_Revenue'] > curr_rev
        assert summary['NG_LF_Forecast'] > 0
        assert summary['NG_AF_Forecast'] > 0
        assert summary['nextgen_flag'] == 1
        assert summary['competition_response_flag'] == 1

    def test_mysql_dw_fare_bucket_weighted_reader(self):
        """Verify that get_additive_fare_bucket_forecast queries MySQL DW table RM_DW_DATA.add_model_fare_bucket_level_weighted."""
        from helper.rm_database_helper import RMDatabaseHelper
        from unittest.mock import patch

        curr_params = MagicMock()
        curr_params.s3 = None
        mock_dw_engine = MagicMock()
        curr_params.dwconn = mock_dw_engine

        mock_dw_data = pd.DataFrame({
            'Sector': ['AMDBLR'] * 2,
            'DepartureDate': ['2026-10-01', '2026-10-01'],
            'FlightNumber': [1905, 1905],
            'FareBucket': ['2914-3526', '3526-4279'],
            'Final_pax_count': [3.20, 42.19],
            'weighted_pax_std': [2.89, 14.92]
        })

        db_helper = RMDatabaseHelper(None, mock_dw_engine, "test_run", MagicMock(), False, None, curr_params)
        with patch('pandas.read_sql', return_value=mock_dw_data):
            read_df = db_helper.get_additive_fare_bucket_forecast(
                sector="AMDBLR",
                departure_date="2026-10-01",
                flight_number=1905
            )

        assert read_df is not None
        assert len(read_df) == 2
        assert 'Final_pax_count' in read_df.columns
        assert read_df['Final_pax_count'].iloc[0] == 3.20

    def test_local_mongodb_fare_bucket_weighted_reader(self):
        """Verify that get_additive_fare_bucket_forecast reads from MongoDB collection add_model_fare_bucket_level_weighted for local test runs."""
        from helper.rm_database_helper import RMDatabaseHelper

        curr_params = MagicMock()
        curr_params.s3 = None
        curr_params.dwconn = None

        mock_mongo_db = MagicMock()
        mock_mongo_db.add_model_fare_bucket_level_weighted.find.return_value = [
            {
                'Sector': 'AMDBLR',
                'DepartureDate': '2026-10-01 00:00:00',
                'FlightNumber': 1905,
                'FareBucket': '2914-3526',
                'Final_pax_count': 3.20,
                'weighted_pax_std': 2.89
            },
            {
                'Sector': 'AMDBLR',
                'DepartureDate': '2026-10-01 00:00:00',
                'FlightNumber': 1905,
                'FareBucket': '3526-4279',
                'Final_pax_count': 42.19,
                'weighted_pax_std': 14.92
            }
        ]

        db_helper = RMDatabaseHelper(mock_mongo_db, None, "test_run", MagicMock(), False, None, curr_params)
        db_helper.dwconn = None
        read_df = db_helper.get_additive_fare_bucket_forecast(
            sector="AMDBLR",
            departure_date="2026-10-01",
            flight_number=1905
        )

        assert read_df is not None
        assert len(read_df) == 2
        assert 'Final_pax_count' in read_df.columns
        assert read_df['Final_pax_count'].iloc[0] == 3.20

    def test_all_inventory_optimization_columns(self):
        """Verify that inventory_optimization generates all required columns as per CONST_OUTPUT_INVENTORY_OPTIMIZATION_ORDER."""
        from Constants import CONST_OUTPUT_INVENTORY_OPTIMIZATION_ORDER
        expected_columns = CONST_OUTPUT_INVENTORY_OPTIMIZATION_ORDER

        ladder_df = pd.DataFrame({
            'FareBucket': ['Y', 'B', 'M'],
            'fare': [9000.0, 5000.0, 3000.0],
            'mu': [2.0, 5.0, 10.0],
            'sigma': [0.5, 1.25, 2.5],
            'FareBucket_Upper': [10000.0, 6000.0, 4000.0]
        })

        curr_params = MagicMock()
        curr_params.origin = 'BOM'
        curr_params.destin = 'DEL'
        curr_params.flight_number = 1127
        curr_params.distance = 1148.0
        curr_params.currDate = '2026-08-16'
        curr_params.tagging = 'Peak'
        curr_params.ndocounter = 5

        opt_df, summary = NextGenOptimizationEngine.execute_flight_nextgen_optimization(
            ladder_df, 189, 100, 350000.0,
            curr_params=curr_params
        )

        # Mock runAlloc column assignment
        dist_val = float(getattr(curr_params, 'distance', 1000.0) or 1000.0)
        std_str = '10:30'
        dep_dow = 'Saturday'
        sec_str = 'BOMDEL'
        dateFormatted_yyyymmdd = '2026-08-20'
        curr_rev = 350000.0

        opt_df['sector'] = sec_str
        opt_df['date'] = dateFormatted_yyyymmdd
        opt_df['DOW'] = dep_dow
        opt_df['STD'] = std_str
        opt_df['flight'] = curr_params.flight_number
        opt_df['NDO'] = int(getattr(curr_params, 'ndocounter', 0))
        opt_df['capacity'] = 189
        opt_df['Total_Bookings'] = 100
        opt_df['fare'] = opt_df.get('fare', 0.0)
        opt_df['fare_All_in'] = opt_df.get('fare_All_in', opt_df['fare'])
        opt_df['oal_b2c_min'] = summary.get('MarketFare_Min_Comp', summary.get('MarketFare_Min', 0.0))
        opt_df['oal_b2c_min_All_in'] = summary.get('MarketFare_Min_AllIn', summary.get('MarketFare_Min', 0.0))
        opt_df['oal_b2c_max'] = summary.get('MarketFare_Max', 0.0)
        opt_df['qp_b2c_min'] = min(opt_df['fare'])
        opt_df['qp_b2c_max'] = max(opt_df['fare'])
        opt_df['anchor_fare'] = summary.get('anchor_fare_airline_comp', summary.get('Anchor_fare', 0.0))
        opt_df['anchor_fare_All_in'] = summary.get('anchor_fare_allin', summary.get('Anchor_fare', 0.0))
        opt_df['strategy_fare'] = summary.get('strategy_fare_airline_comp', summary.get('Strategy_fare', 0.0))
        opt_df['strategy_fare_All_in'] = summary.get('strategy_fare_allin', summary.get('Strategy_fare', 0.0))
        opt_df['opening fare'] = summary.get('opening_fare_airline_comp', opt_df.get('opening fare', 0.0))
        opt_df['opening fare_All_in'] = summary.get('opening_fare_allin', opt_df.get('opening fare_All_in', 0.0))
        opt_df['predicted_pickup'] = sum(opt_df.get('mu', [0.0]))
        opt_df['Predicted_Flown'] = 100 + opt_df['predicted_pickup']
        opt_df['Predicted_pickup_Revised'] = summary.get('Predicted_Pickup_Revised', 0.0)
        opt_df['Predicted_Flown_Revised'] = summary.get('Predicted_Flown_Revised', 0.0)
        opt_df['LF_Forecast_Revised'] = summary.get('NG_LF_Forecast', 0.0)
        opt_df['AF_Forecast'] = summary.get('NG_AF_Forecast', 0.0)
        opt_df['Distance'] = dist_val
        opt_df['ASKM'] = round(189 * dist_val, 2)
        opt_df['RPKM'] = round(summary.get('Predicted_Flown_Revised', 0.0) * dist_val, 2)
        opt_df['RASK_Forecast'] = round(summary.get('Predicted_Revenue', 0.0) / (189 * dist_val), 4)
        opt_df['Capped_Predicted_Flown_Revised'] = min(189, summary.get('Predicted_Flown_Revised', 0.0))
        opt_df['Denominator'] = opt_df['Capped_Predicted_Flown_Revised']
        opt_df['Current_Revenue'] = curr_rev
        opt_df['Total_Expected_Revenue'] = summary.get('Total_Expected_Revenue', 0.0)
        opt_df['Predicted_Revenue'] = summary.get('Predicted_Revenue', 0.0)
        opt_df['SnapshotDate'] = getattr(curr_params, 'currDate', '2026-08-16')
        opt_df['Tagging'] = getattr(curr_params, 'tagging', 'Shoulder')
        opt_df['RunId'] = 'test_run_123'
        opt_df['STD_future'] = std_str
        opt_df['STD_dt'] = f"{dateFormatted_yyyymmdd} {std_str}"

        for col in expected_columns:
            assert col in opt_df.columns, f"Missing required column in inventory_optimization: {col}"

    def test_db_helper_insert_summary_and_inventory_optimization(self):
        """Verify insertSummaryRowNextGen and insertInventoryOptimization handle DB operations cleanly."""
        from helper.rm_database_helper import RMDatabaseHelper
        from Constants import CONST_OUTPUT_SUMMARY_NEXTGEN_ORDER, CONST_OUTPUT_INVENTORY_OPTIMIZATION_ORDER

        mock_db = MagicMock()
        inserted_summary = []
        inserted_run_summary = []
        inserted_inv = []

        def mock_insert_one_summary(doc):
            doc['_id'] = 'mock_obj_id_1'
            inserted_summary.append(doc.copy())

        def mock_insert_one_run(doc):
            # If doc already has _id from previous insert, it could collide unless popped
            inserted_run_summary.append(doc.copy())

        def mock_insert_many_inv(docs):
            inserted_inv.extend([d.copy() for d in docs])

        mock_db.summary_nextgen.insert_one.side_effect = mock_insert_one_summary
        mock_db.run_summary_nextgen.insert_one.side_effect = mock_insert_one_run
        mock_db.inventory_optimization.insert_many.side_effect = mock_insert_many_inv

        db_helper = RMDatabaseHelper(mock_db, None, "run_123", MagicMock(), False, None, MagicMock())

        # Test insertSummaryRowNextGen
        test_summary_row = {col: f"val_{col}" for col in CONST_OUTPUT_SUMMARY_NEXTGEN_ORDER}
        test_summary_row['extra_dummy_col'] = 'should_be_filtered'
        test_summary_row['nextgen_flag'] = 'Y'
        test_summary_row['competition_response_flag'] = 'Y'

        db_helper.insertSummaryRowNextGen([test_summary_row])

        assert len(inserted_summary) == 1
        assert len(inserted_run_summary) == 1
        assert 'extra_dummy_col' not in inserted_summary[0]
        assert inserted_summary[0]['nextgen_flag'] == 'Y'
        # Ensure run_summary_nextgen insert didn't fail due to retained _id
        assert '_id' not in inserted_run_summary[0] or inserted_run_summary[0]['_id'] != 'mock_obj_id_1'

        # Test insertInventoryOptimization
        test_inv_row = {col: f"inv_{col}" for col in CONST_OUTPUT_INVENTORY_OPTIMIZATION_ORDER}
        test_inv_row['unwanted_col'] = 'strip_me'
        db_helper.insertInventoryOptimization([test_inv_row])

        assert len(inserted_inv) == 1
        assert 'unwanted_col' not in inserted_inv[0]
        assert inserted_inv[0]['sector'] == 'inv_sector'
        assert inserted_inv[0]['RunId'] == 'inv_RunId'
        assert inserted_inv[0]['runid'] == 'inv_RunId'

    def test_robust_type_coercion_edge_cases(self):
        """Verify to_float, to_int, and is_flag_enabled against problematic strings."""
        from common.nextgen_optimization_engine import to_float, to_int, is_flag_enabled

        # to_float edge cases
        assert to_float('', 0.0) == 0.0
        assert to_float('   ', 5.5) == 5.5
        assert to_float('NA', None) is None
        assert to_float('nan', 0.0) == 0.0
        assert to_float('None', 10.0) == 10.0
        assert to_float('null', 0.0) == 0.0
        assert to_float('123.45', 0.0) == 123.45
        assert to_float(None, -1.0) == -1.0

        # to_int edge cases
        assert to_int('', 0) == 0
        assert to_int('NA', 5) == 5
        assert to_int('Y', 0) == 1
        assert to_int('YES', 0) == 1
        assert to_int('true', 0) == 1
        assert to_int('N', 1) == 0
        assert to_int('no', 1) == 0
        assert to_int('false', 1) == 0
        assert to_int('42', 0) == 42
        assert to_int(None, 99) == 99

        # is_flag_enabled edge cases
        assert is_flag_enabled('Y') is True
        assert is_flag_enabled('yes') is True
        assert is_flag_enabled('1') is True
        assert is_flag_enabled(1) is True
        assert is_flag_enabled('N') is False
        assert is_flag_enabled('0') is False
        assert is_flag_enabled(0) is False
        assert is_flag_enabled('') is False
        assert is_flag_enabled('NA') is False
        assert is_flag_enabled(None) is False

    def test_fallback_18_gp_fare_buckets_with_79_rbds(self):
        """
        Verify that when 79 filed B2C RBDs are present and forecast is missing,
        _generate_fallback_18_gp_fare_buckets produces exactly 18 interval buckets
        (with actual interval strings, never raw RBD codes) and NextGen optimization
        with promo produces exactly 19 records (18 intervals + 1 promo).
        """
        from runAlloc import DynAlloc
        from common.nextgen_optimization_engine import NextGenOptimizationEngine

        # Simulate 79 filed RBDs as present in B2C_RBD_VALUES
        rbds = [f"V{i}" for i in range(10)] + [f"U{i}" for i in range(10)] + [f"T{i}" for i in range(10)] + \
               [f"R{i}" for i in range(10)] + [f"Q{i}" for i in range(10)] + [f"P{i}" for i in range(5)] + \
               [f"O{i}" for i in range(5)] + [f"N{i}" for i in range(5)] + ['M', 'K', 'J', 'I', 'H', 'F', 'E', 'D', 'C', 'B', 'Y']
        # Map to realistic fares between 4000 and 25000
        fares = np.linspace(4000, 25000, len(rbds))
        b2c_ladder_df = pd.DataFrame({
            'RBD': rbds,
            'fare': fares,
            'TOTAL': fares
        })

        alloc = DynAlloc.__new__(DynAlloc)
        fallback_data = alloc._generate_fallback_18_gp_fare_buckets(b2c_ladder_df, variance_full=36.0)

        # 1. Must produce exactly 18 buckets
        assert len(fallback_data) == 18, f"Expected 18 fallback buckets, got {len(fallback_data)}"

        # 2. FareBucket must be intervals ('lower-upper' or 'lower+'), never raw RBD codes
        raw_rbd_set = set(rbds)
        for b in fallback_data:
            fb = b['FareBucket']
            assert fb not in raw_rbd_set, f"FareBucket contains raw RBD code: {fb}"
            assert ('-' in fb or '+' in fb), f"FareBucket is not an interval: {fb}"
            assert b['fare'] > 0
            assert b['FareBucket_Lower'] > 0

        # Top bucket must end with '+'
        assert fallback_data[-1]['FareBucket'].endswith('+')

        # 3. NextGen optimization with this ladder and promo_flag=1 must produce exactly 19 records
        ladder_df = pd.DataFrame(fallback_data).sort_values('fare', ascending=False).reset_index(drop=True)
        curr_params = MagicMock()
        curr_params.promo_flag_NG = 1
        curr_params.promo_ratio_NG = 0.8
        curr_params.nextgen_flag_NG = 1
        curr_params.competition_response_flag_NG = 1
        curr_params.buy_up_factor_NG = 0.0

        opt_df, summary = NextGenOptimizationEngine.execute_flight_nextgen_optimization(
            flight_ladder_df=ladder_df,
            capacity=189.0,
            current_bookings=100.0,
            current_revenue=400000.0,
            market_fares=pd.DataFrame(),
            flight_std="12:00",
            curr_params=curr_params,
            db_helper=MagicMock(),
            filed_51_ladder_df=b2c_ladder_df
        )

        assert len(opt_df) == 19, f"Expected 19 records including promo, got {len(opt_df)}"
        # Exactly 1 Promo row
        promo_rows = opt_df[opt_df['FareBucket'] == 'Promo']
        assert len(promo_rows) == 1
        # Exactly 18 interval rows
        non_promo = opt_df[opt_df['FareBucket'] != 'Promo']
        assert len(non_promo) == 18
        for _, r in non_promo.iterrows():
            assert r['FareBucket'] not in raw_rbd_set
            assert ('-' in r['FareBucket'] or '+' in r['FareBucket'])

