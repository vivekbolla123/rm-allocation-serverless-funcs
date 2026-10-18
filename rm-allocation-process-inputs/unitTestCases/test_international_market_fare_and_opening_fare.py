import json
import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
from datetime import datetime

import Constants
from constants_utils import ConstantsUtils
from market_fares_service import MarketFareService
from helper.rm_database_helper import RMDatabaseHelper


class TestInternationalMarketFareAndOpeningFare(unittest.TestCase):
    def setUp(self):
        self.mock_db = MagicMock()
        self.mock_db.runId = "test_run_123"

    def test_constants_definitions(self):
        """Verify new constants exist with expected default values."""
        self.assertTrue(hasattr(Constants, 'MARKET_FARES_THRESHOLD_INTERNATIONAL'))
        self.assertTrue(hasattr(Constants, 'MARKET_FARES_THRESHOLD_INTERNATIONAL_KEY'))
        self.assertTrue(hasattr(Constants, 'LAST_RUN_OPENING_FARE_SECTORS'))
        self.assertTrue(hasattr(Constants, 'LAST_RUN_OPENING_FARE_SECTORS_KEY'))

        self.assertEqual(Constants.MARKET_FARES_THRESHOLD_INTERNATIONAL_KEY, 'MARKET_FARES_THRESHOLD_INTERNATIONAL')
        self.assertEqual(Constants.LAST_RUN_OPENING_FARE_SECTORS_KEY, 'LAST_RUN_OPENING_FARE_SECTORS')
        self.assertIn("BOMJED", Constants.LAST_RUN_OPENING_FARE_SECTORS)

    def test_constants_utils_parsing(self):
        """Verify ConstantsUtils parses international threshold and sectors properly."""
        # Test default init
        cu = ConstantsUtils(MagicMock())
        self.assertIn("BOMJED", cu.LAST_RUN_OPENING_FARE_SECTORS)

        # Test sector parsing helper
        self.assertEqual(cu._parse_sectors_json('["BOMJED", "delbom"]'), ["BOMJED", "DELBOM"])
        self.assertEqual(cu._parse_sectors_json('BOMJED, DELDXB'), ["BOMJED", "DELDXB"])
        self.assertEqual(cu._parse_sectors_json(['bomjed', 'CCUJED']), ["BOMJED", "CCUJED"])
        self.assertEqual(cu._parse_sectors_json(None), [])

        # Test generateConstantValues with custom parameter values
        mock_db_with_params = MagicMock()
        param_df = pd.DataFrame([
            {"parameterKey": "MARKET_FARES_THRESHOLD_INTERNATIONAL", "parameterValue": '[ {"startNdo": 0, "endNdo": 5, "value": 12} ]'},
            {"parameterKey": "LAST_RUN_OPENING_FARE_SECTORS", "parameterValue": '["BOMJED", "DELDXB"]'}
        ])
        mock_db_with_params.rm_parameter_values.find.return_value = param_df.to_dict('records')
        cu2 = ConstantsUtils(mock_db_with_params)
        self.assertEqual(cu2.MARKET_FARES_THRESHOLD_INTERNATIONAL, '[ {"startNdo": 0, "endNdo": 5, "value": 12} ]')
        self.assertEqual(cu2.LAST_RUN_OPENING_FARE_SECTORS, ["BOMJED", "DELDXB"])

    def test_market_fare_service_is_international(self):
        """Verify MarketFareService detects international flights correctly."""
        cu = ConstantsUtils(MagicMock())
        time_utils = MagicMock()
        mfs = MarketFareService(self.mock_db, cu, time_utils, MagicMock())

        # 1. When params.region is International (case-insensitive)
        p1 = MagicMock()
        p1.region = 'International'
        self.assertTrue(mfs.is_international(p1))

        p2 = MagicMock()
        p2.region = 'international'
        self.assertTrue(mfs.is_international(p2))

        # 2. When params.region is Domestic or other
        p3 = MagicMock()
        p3.region = 'Domestic'
        self.assertFalse(mfs.is_international(p3))

        p4 = MagicMock()
        p4.region = ''
        self.assertFalse(mfs.is_international(p4))

    def test_market_fare_service_fetch_threshold_selection(self):
        """Verify fetchMarketFareRange picks international vs domestic threshold."""
        cu = ConstantsUtils(MagicMock())
        cu.MARKET_FARES_THRESHOLD = '[ {"startNdo": 0, "endNdo": 7, "value": 2}, {"startNdo": 8, "endNdo": 14, "value": 8} ]'
        cu.MARKET_FARES_THRESHOLD_INTERNATIONAL = '[ {"startNdo": 0, "endNdo": 7, "value": 16}, {"startNdo": 8, "endNdo": 14, "value": 24} ]'

        time_utils = MagicMock()
        time_utils.calculate_ndo.return_value = 5  # NDO = 5

        mongo_helper = MagicMock()
        mfs = MarketFareService(self.mock_db, cu, time_utils, mongo_helper)

        # Call for International sector BOM-JED
        params_intl = MagicMock()
        params_intl.mongodb = '0'
        params_intl.region = 'International'
        params_intl.origin = 'BOM'
        params_intl.destin = 'JED'

        mfs.fetchMarketFareRange("B2C", "2026-04-01", params_intl, 0, "10:00", "12:00", "BOM", "JED", False)
        # Check hrs passed to rmdatabasehelper.getMarketFareRange (argument #8 is hrs)
        call_args = self.mock_db.getMarketFareRange.call_args[0]
        hrs_passed = call_args[7]
        self.assertEqual(hrs_passed, 16)  # From international threshold!

        # Call for Domestic sector DEL-BOM
        params_dom = MagicMock()
        params_dom.mongodb = '0'
        params_dom.region = 'Domestic'
        params_dom.origin = 'DEL'
        params_dom.destin = 'BOM'

        self.mock_db.reset_mock()
        self.mock_db.master_db.config_station_master.find.return_value = []
        mfs.fetchMarketFareRange("B2C", "2026-04-01", params_dom, 0, "10:00", "12:00", "DEL", "BOM", False)
        call_args_dom = self.mock_db.getMarketFareRange.call_args[0]
        hrs_passed_dom = call_args_dom[7]
        self.assertEqual(hrs_passed_dom, 2)  # From domestic threshold!

    def test_run_alloc_get_last_run_opening_fare_from_redis(self):
        """Verify _get_last_run_opening_fare retrieves the selling_fare strictly from Redis."""
        from runAlloc import DynAlloc

        run_alloc = DynAlloc.__new__(DynAlloc)
        run_alloc.logger = MagicMock()
        run_alloc.currParams = MagicMock()
        run_alloc.currParams.origin = 'BOM'
        run_alloc.currParams.destin = 'JED'
        run_alloc.currParams.flight_number = '1101'
        run_alloc.currParams.per_start = '2026-04-01'

        mock_state = MagicMock()
        mock_state.currFlt = '1101'
        mock_state.currFltDate = '2026-04-01'

        # Test Case 1: determinerbd.get_autotolerance_history returns runs
        run_alloc.determinerbd = MagicMock()
        run_alloc.determinerbd.get_autotolerance_history.return_value = [
            {"run_id": 1, "selling_fare": 7100.0},
            {"run_id": 2, "selling_fare": 7350.0}
        ]
        fare = run_alloc._get_last_run_opening_fare(mock_state)
        self.assertEqual(fare, 7350.0)

        # Test Case 2: determinerbd returns empty, fallback to cache_client.hget
        run_alloc.determinerbd.get_autotolerance_history.return_value = []
        run_alloc.cache_client = MagicMock()
        history_data = json.dumps([{"selling_fare": 7600.0}])
        run_alloc.cache_client.hget.return_value = history_data.encode('utf-8')

        fare2 = run_alloc._get_last_run_opening_fare(mock_state)
        self.assertEqual(fare2, 7600.0)
        run_alloc.cache_client.hget.assert_called_with("run_history:BOM_JED_1101_2026-04-01", "autotolerance_history")

    @patch('runAlloc.DynAlloc.calculate_averages', return_value=(4000, 4200, 4100))
    def test_run_alloc_phase_i_last_sold_fare_bomjed_fallback(self, mock_calc_avg):
        """Verify BOMJED uses last run opening fare from Redis when market fare is missing."""
        from runAlloc import DynAlloc
        from models.flight_state import FlightState
        from models.output_summary import OutputSummary

        cu = ConstantsUtils(MagicMock())
        cu.LAST_RUN_OPENING_FARE_SECTORS = ["BOMJED"]

        run_alloc = DynAlloc.__new__(DynAlloc)
        run_alloc.constantsUtils = cu
        run_alloc.logger = MagicMock()
        run_alloc.rmdatabasehelper = MagicMock()
        run_alloc.cache_client = MagicMock()
        run_alloc.cache_client.exists.return_value = False
        run_alloc.inputSource = MagicMock()
        run_alloc.inputSource.input_source = {"LAST_SELLING_FARE": "TEST_DATA"}
        run_alloc.currency = 'SAR'
        run_alloc.runId = 'test_run'

        # Set currParams
        curr_params = MagicMock()
        curr_params.origin = 'BOM'
        curr_params.destin = 'JED'
        curr_params.flight_number = '1101'
        curr_params.per_start = '2026-04-01'
        run_alloc.currParams = curr_params

        # Mock getLastSellingFare for test data (actual last sold fare)
        mock_lsf_df = pd.DataFrame([{
            "B2C_last3avg_fare": 5000,
            "B2B_last3avg_fare": 4900,
            "last7avg": 4800
        }])
        run_alloc.rmdatabasehelper.getLastSellingFare.return_value = mock_lsf_df
        run_alloc.rmdatabasehelper.get_qp_currency.return_value = pd.DataFrame([{"currency": "SAR"}])

        # Mock last run opening fare from Redis via determinerbd
        run_alloc.determinerbd = MagicMock()
        run_alloc.determinerbd.get_autotolerance_history.return_value = [
            {"run_id": 1, "selling_fare": 7200.0}
        ]

        # Output summaries
        run_alloc.outputSummaryRow_b2c = OutputSummary()
        run_alloc.outputSummaryRow_b2b = OutputSummary()

        # CASE 1: Market fare IS NOT available for BOMJED -> Override with last run opening fare from Redis
        state1 = FlightState(
            currFlt=1101,
            currBookedLoad=50,
            currLid=186,
            currFltDate="04/01/2026",
            currFltDepartureTime="12:00",
            currAdjustedCapacity=186,
            currtbfBookedLoad=0,
            tgtBookedLoadPercent=80,
            dow=3,
            ndocounter=10,
        )
        state1.currFltDate_dt = datetime(2026, 4, 1)
        state1.param_flight_number_str = "1101"
        state1.isB2CMarketFareAvailable = False
        state1.isB2BMarketFareAvailable = False
        run_alloc.outputSummaryRow_b2c.MktFare_Min = 0
        run_alloc.outputSummaryRow_b2b.MktFare_Min = 0

        run_alloc._phaseI_lastSoldFare(state1)
        self.assertEqual(state1.lastSoldFareb2c_val, 7200.0)
        self.assertEqual(state1.lastSoldFareb2b_val, 7200.0)

        # CASE 2: Market fare IS available for BOMJED -> Do NOT override, keep actual last sold fare
        state2 = FlightState(
            currFlt=1101,
            currBookedLoad=50,
            currLid=186,
            currFltDate="04/01/2026",
            currFltDepartureTime="12:00",
            currAdjustedCapacity=186,
            currtbfBookedLoad=0,
            tgtBookedLoadPercent=80,
            dow=3,
            ndocounter=10,
        )
        state2.currFltDate_dt = datetime(2026, 4, 1)
        state2.param_flight_number_str = "1101"
        state2.isB2CMarketFareAvailable = True
        state2.isB2BMarketFareAvailable = True
        run_alloc.outputSummaryRow_b2c.MktFare_Min = 6500
        run_alloc.outputSummaryRow_b2b.MktFare_Min = 6400

        run_alloc._phaseI_lastSoldFare(state2)
        self.assertEqual(state2.lastSoldFareb2c_val, 5000)
        self.assertEqual(state2.lastSoldFareb2b_val, 4900)

        # CASE 3: Sector is DEL-BOM (NOT in LAST_RUN_OPENING_FARE_SECTORS) -> Do NOT override even if market fare is missing
        curr_params.origin = 'DEL'
        curr_params.destin = 'BOM'
        state3 = FlightState(
            currFlt=101,
            currBookedLoad=50,
            currLid=186,
            currFltDate="04/01/2026",
            currFltDepartureTime="12:00",
            currAdjustedCapacity=186,
            currtbfBookedLoad=0,
            tgtBookedLoadPercent=80,
            dow=3,
            ndocounter=10,
        )
        state3.currFltDate_dt = datetime(2026, 4, 1)
        state3.param_flight_number_str = "101"
        state3.isB2CMarketFareAvailable = False
        state3.isB2BMarketFareAvailable = False
        run_alloc.outputSummaryRow_b2c.MktFare_Min = 0
        run_alloc.outputSummaryRow_b2b.MktFare_Min = 0

        run_alloc._phaseI_lastSoldFare(state3)
        self.assertEqual(state3.lastSoldFareb2c_val, 5000)
        self.assertEqual(state3.lastSoldFareb2b_val, 4900)

        # CASE 4: BOMJED, determinerbd empty, but Redis cache_client has selling_fare
        curr_params.origin = 'BOM'
        curr_params.destin = 'JED'
        run_alloc.determinerbd.get_autotolerance_history.return_value = []
        run_alloc.cache_client.hget.return_value = json.dumps([
            {"run_id": 1, "selling_fare": 7450.0}
        ]).encode('utf-8')

        state4 = FlightState(
            currFlt=1101,
            currBookedLoad=50,
            currLid=186,
            currFltDate="04/01/2026",
            currFltDepartureTime="12:00",
            currAdjustedCapacity=186,
            currtbfBookedLoad=0,
            tgtBookedLoadPercent=80,
            dow=3,
            ndocounter=10,
        )
        state4.currFltDate_dt = datetime(2026, 4, 1)
        state4.param_flight_number_str = "1101"
        state4.isB2CMarketFareAvailable = False
        state4.isB2BMarketFareAvailable = False
        run_alloc.outputSummaryRow_b2c.MktFare_Min = 0
        run_alloc.outputSummaryRow_b2b.MktFare_Min = 0

        run_alloc._phaseI_lastSoldFare(state4)
        self.assertEqual(state4.lastSoldFareb2c_val, 7450.0)
        self.assertEqual(state4.lastSoldFareb2b_val, 7450.0)


if __name__ == '__main__':
    unittest.main()
