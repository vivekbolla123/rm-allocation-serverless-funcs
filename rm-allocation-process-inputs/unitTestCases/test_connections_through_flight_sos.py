import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
from datetime import datetime

from runAlloc_Connections import DynAllocConnections
from models.market_list_connections_row import MarketListConnectiosnsRow
from constants_utils import ConstantsUtils
import Constants


class TestConnectionsThroughFlightSOS(unittest.TestCase):
    def setUp(self):
        self.mock_db = MagicMock()
        self.mock_db.runId = "test_run_conn_123"

    def test_is_through_flight(self):
        """Verify is_through_flight detects through flights when Flight1 and Flight2 are same."""
        conn = DynAllocConnections.__new__(DynAllocConnections)
        
        # Case 1: Identical numeric strings
        conn.currParams = MagicMock()
        conn.currParams.flight1 = "1101"
        conn.currParams.flight2 = "1101"
        conn.currParams.route_type = ""
        self.assertTrue(conn.is_through_flight())

        # Case 2: Identical integers
        conn.currParams.flight1 = 1101
        conn.currParams.flight2 = 1101
        self.assertTrue(conn.is_through_flight())

        # Case 3: QP prefix variations
        conn.currParams.flight1 = "QP 1101"
        conn.currParams.flight2 = "QP1101"
        self.assertTrue(conn.is_through_flight())

        # Case 4: Different flight numbers
        conn.currParams.flight1 = "1331"
        conn.currParams.flight2 = "1606"
        self.assertFalse(conn.is_through_flight())

        # Case 5: Different flight numbers but route_type is 'Through'
        conn.currParams.route_type = "Through"
        self.assertTrue(conn.is_through_flight())

    def test_is_sos_active(self):
        """Verify is_sos_active correctly parses sosFlag."""
        conn = DynAllocConnections.__new__(DynAllocConnections)
        conn.currParams = MagicMock()

        conn.currParams.sosFlag = 1
        self.assertTrue(conn.is_sos_active())

        conn.currParams.sosFlag = "1"
        self.assertTrue(conn.is_sos_active())

        conn.currParams.sosFlag = 1.0
        self.assertTrue(conn.is_sos_active())

        conn.currParams.sosFlag = 0
        self.assertFalse(conn.is_sos_active())

        conn.currParams.sosFlag = "0"
        self.assertFalse(conn.is_sos_active())

        conn.currParams.sosFlag = None
        self.assertFalse(conn.is_sos_active())

        conn.currParams.sosFlag = Constants.CONST_NA
        self.assertFalse(conn.is_sos_active())

    def test_calculate_phase2_anchor_fare_through_flight_sos_market_fare_higher(self):
        """When Through Flight + SOS ON and Market Fare > (L1 + L2), Fare Anchor = Market Fare."""
        conn = DynAllocConnections.__new__(DynAllocConnections)
        conn.currParams = MagicMock()
        conn.currParams.flight1 = "1101"
        conn.currParams.flight2 = "1101"
        conn.currParams.sosFlag = 1
        conn.currParams.route_type = ""

        # L1 = 3000, L2 = 4000 => L1 + L2 = 7000. Market Fare = 8500.
        anchor_fare, how_determined = conn._calculate_phase2_anchor_fare(
            channel="B2C",
            selling_fare1=3000.0,
            selling_fare2=4000.0,
            market_fare=8500.0,
            strategy_fare=8500.0,
            sector1="BOMDEL",
            sector2="DELIXC",
            is_sector1_domestic=True,
            is_sector2_domestic=True,
            forecasted_plf1=50.0,
            forecasted_plf2=50.0,
            bookload1=50,
            bookload2=50,
            ndocounter=10
        )
        self.assertEqual(anchor_fare, 8500.0)
        self.assertIn("Through Flight SOS Active", how_determined)

    def test_calculate_phase2_anchor_fare_through_flight_sos_l1_l2_higher(self):
        """When Through Flight + SOS ON and (L1 + L2) > Market Fare, Fare Anchor = L1 + L2."""
        conn = DynAllocConnections.__new__(DynAllocConnections)
        conn.currParams = MagicMock()
        conn.currParams.flight1 = "1101"
        conn.currParams.flight2 = "1101"
        conn.currParams.sosFlag = 1
        conn.currParams.route_type = ""

        # L1 = 4500, L2 = 5000 => L1 + L2 = 9500. Market Fare = 8000.
        anchor_fare, how_determined = conn._calculate_phase2_anchor_fare(
            channel="B2C",
            selling_fare1=4500.0,
            selling_fare2=5000.0,
            market_fare=8000.0,
            strategy_fare=8000.0,
            sector1="BOMDEL",
            sector2="DELIXC",
            is_sector1_domestic=True,
            is_sector2_domestic=True,
            forecasted_plf1=50.0,
            forecasted_plf2=50.0,
            bookload1=50,
            bookload2=50,
            ndocounter=10
        )
        self.assertEqual(anchor_fare, 9500.0)
        self.assertIn("Through Flight SOS Active", how_determined)

    def test_calculate_phase2_anchor_fare_through_flight_sos_no_market_fare(self):
        """When Through Flight + SOS ON and Market Fare is missing/0, Fare Anchor = L1 + L2."""
        conn = DynAllocConnections.__new__(DynAllocConnections)
        conn.currParams = MagicMock()
        conn.currParams.flight1 = "1101"
        conn.currParams.flight2 = "1101"
        conn.currParams.sosFlag = 1
        conn.currParams.route_type = ""

        # L1 = 4000, L2 = 3500 => L1 + L2 = 7500. Market Fare = 0.
        anchor_fare, how_determined = conn._calculate_phase2_anchor_fare(
            channel="B2C",
            selling_fare1=4000.0,
            selling_fare2=3500.0,
            market_fare=0,
            strategy_fare=0,
            sector1="BOMDEL",
            sector2="DELIXC",
            is_sector1_domestic=True,
            is_sector2_domestic=True,
            forecasted_plf1=50.0,
            forecasted_plf2=50.0,
            bookload1=50,
            bookload2=50,
            ndocounter=10
        )
        self.assertEqual(anchor_fare, 7500.0)
        self.assertIn("Through Flight SOS Active", how_determined)

    def test_non_through_flight_sos_does_not_use_through_logic(self):
        """When Flight1 != Flight2, standard Phase 2 threshold logic is executed instead."""
        conn = DynAllocConnections.__new__(DynAllocConnections)
        conn.currParams = MagicMock()
        conn.currParams.flight1 = "1331"
        conn.currParams.flight2 = "1606"
        conn.currParams.sosFlag = 1
        conn.currParams.route_type = ""
        conn.currParams.DirectFlightOpeningRBDB2C = Constants.CONST_NA
        conn.currParams.DirectFlightForecast = Constants.CONST_NA

        cu = ConstantsUtils(MagicMock())
        cu.DOMESTIC_PLF_THRESHOLD = 75.0
        conn.constantsUtils = cu

        conn.rmdatabasehelper = MagicMock()
        conn.rmdatabasehelper.get_international_bookload_threshold.return_value = 80.0
        conn.rmdatabasehelper.get_stage_length.return_value = 1000.0

        # Neither leg meets threshold (50% < 75%)
        anchor_fare, how_determined = conn._calculate_phase2_anchor_fare(
            channel="B2C",
            selling_fare1=3000.0,
            selling_fare2=4000.0,
            market_fare=6000.0,
            strategy_fare=6000.0,
            sector1="BLRAMD",
            sector2="AMDDEL",
            is_sector1_domestic=True,
            is_sector2_domestic=True,
            forecasted_plf1=50.0,
            forecasted_plf2=50.0,
            bookload1=50,
            bookload2=50,
            ndocounter=10
        )
        # Should NOT use "Through Flight SOS Active"
        self.assertNotIn("Through Flight SOS Active", how_determined)
        self.assertIn("Both Legs Didn't Meet Threshold", how_determined)


if __name__ == '__main__':
    unittest.main()
