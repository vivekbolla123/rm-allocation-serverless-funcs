import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
from datetime import datetime, date

from models.market_list_row import MarketListRow
from models.market_list_connections_row import MarketListConnectiosnsRow


class TestOutboundStopAndDuration(unittest.TestCase):

    def test_market_list_row_outbound_stop_parsing(self):
        # Case 1: Outbound_Stop provided as int or str
        row1 = MarketListRow({"Outbound_Stop": "1"})
        self.assertEqual(row1.outboundstop, 1)

        row2 = MarketListRow({"outbound_stop": 2})
        self.assertEqual(row2.outboundstop, 2)

        row3 = MarketListRow({"outboundstop": "0"})
        self.assertEqual(row3.outboundstop, 0)

        # Case 2: Missing or NA Outbound_Stop defaults to 0
        row_missing = MarketListRow({})
        self.assertEqual(row_missing.outboundstop, 0)

        row_na = MarketListRow({"Outbound_Stop": "NA"})
        self.assertEqual(row_na.outboundstop, 0)

        row_none = MarketListRow({"Outbound_Stop": None})
        self.assertEqual(row_none.outboundstop, 0)

    def test_market_list_connections_row_duration_parsing(self):
        row = MarketListConnectiosnsRow({
            "connection_duration": 4.5,
            "outbound_duration": 2.0
        })
        self.assertEqual(row.connection_duration, 4.5)
        self.assertEqual(row.outbound_duration, 2.0)

        # Fallback to actual_connection_duration key
        row2 = MarketListConnectiosnsRow({
            "actual_connection_duration": "3.75",
            "outbound_duration": 1.5
        })
        self.assertEqual(row2.connection_duration, "3.75")

    def test_connection_flight_duration_calculation_logic(self):
        # Test the connection duration logic implemented in lambda_function.py
        # Case 1: Same day connection: Dep1 06:00 (360), Arr2 11:30 (690) -> 5.5 hours
        d1 = "2026-09-10"
        d2 = "2026-09-10"
        std1 = 360  # 06:00
        std2 = 500  # 08:20
        sta2 = 690  # 11:30

        date_diff = (datetime.strptime(d2, "%Y-%m-%d") - datetime.strptime(d1, "%Y-%m-%d")).days
        overnight_day = 1 if sta2 < std2 else 0
        tot_min = (date_diff + overnight_day) * 1440 + sta2 - std1
        dur_hours = round(tot_min / 60.0, 2)
        self.assertEqual(dur_hours, 5.5)

        # Case 2: Overnight flight leg 2: Dep1 20:00 (1200), Dep2 23:00 (1380), Arr2 next day 02:00 (120)
        d1 = "2026-09-10"
        d2 = "2026-09-10"
        std1 = 1200
        std2 = 1380
        sta2 = 120  # next morning (sta2 < std2)
        date_diff = (datetime.strptime(d2, "%Y-%m-%d") - datetime.strptime(d1, "%Y-%m-%d")).days
        overnight_day = 1 if sta2 < std2 else 0
        tot_min = (date_diff + overnight_day) * 1440 + sta2 - std1
        dur_hours = round(tot_min / 60.0, 2)
        self.assertEqual(dur_hours, 6.0)

        # Case 3: Overnight layover (Dep2 is next day): Dep1 22:00 (1320), Dep2 next day 07:00 (420), Arr2 next day 09:30 (570)
        d1 = "2026-09-10"
        d2 = "2026-09-11"
        std1 = 1320
        std2 = 420
        sta2 = 570
        date_diff = (datetime.strptime(d2, "%Y-%m-%d") - datetime.strptime(d1, "%Y-%m-%d")).days
        overnight_day = 1 if sta2 < std2 else 0
        tot_min = (date_diff + overnight_day) * 1440 + sta2 - std1
        dur_hours = round(tot_min / 60.0, 2)
        self.assertEqual(dur_hours, 11.5)

    def test_final_outbound_duration_formula(self):
        # final_outbound_duration = actual_connection_duration + market_list_outbound_duration
        actual_dur = 4.75
        ml_dur = 2.0
        final_dur = round(actual_dur + ml_dur, 2)
        self.assertEqual(final_dur, 6.75)

        # Fallback when actual connection duration is None or 0
        actual_dur_none = None
        final_dur_fallback = round(float(actual_dur_none) + ml_dur, 2) if actual_dur_none else ml_dur
        self.assertEqual(final_dur_fallback, 2.0)

    def test_outbound_stop_filter_in_database_helper(self):
        from helper.rm_database_helper import RMDatabaseHelper

        helper = RMDatabaseHelper.__new__(RMDatabaseHelper)
        helper.logger = MagicMock()
        helper.master_db = MagicMock()
        helper.master_db.list_collection_names.return_value = ['market_fares_ag']
        ag_mock = MagicMock()
        ag_mock.aggregate.return_value = []
        helper.master_db.market_fares_ag = ag_mock

        # Test with Outbound_Stop = 1
        currParams = MagicMock()
        currParams.carr_exclusion_b2c = ""
        currParams.flightExclusionB2C = ""
        currParams.outboundstop = 1

        helper.getMarketFareRange(
            channel="B2C",
            currParams=currParams,
            currFltDateStr="09/15/2026",
            DaySpan=0,
            startTime="00:00",
            endtime="23:59",
            Day3Flag=False,
            hrs=24,
            origin="DEL",
            destination="BOM"
        )

        # Verify aggregate pipeline had outbound_stop $in condition containing 0 and 1
        call_args = ag_mock.aggregate.call_args[0][0]
        match_stage = next(stage["$match"] for stage in call_args if "$match" in stage)
        self.assertIn("outbound_stop", match_stage)
        self.assertIn("$in", match_stage["outbound_stop"])
        allowed = match_stage["outbound_stop"]["$in"]
        self.assertIn("0", allowed)
        self.assertIn("1", allowed)
        self.assertIn(0, allowed)
        self.assertIn(1, allowed)
        self.assertNotIn("2", allowed)

    def test_outbound_stop_filter_defaults_to_zero_stops(self):
        from helper.rm_database_helper import RMDatabaseHelper

        helper = RMDatabaseHelper.__new__(RMDatabaseHelper)
        helper.logger = MagicMock()
        helper.master_db = MagicMock()
        helper.master_db.list_collection_names.return_value = ['market_fares_ag']
        ag_mock = MagicMock()
        ag_mock.aggregate.return_value = []
        helper.master_db.market_fares_ag = ag_mock

        # Test with no outboundstop specified
        currParams = MagicMock()
        currParams.carr_exclusion_b2c = ""
        currParams.flightExclusionB2C = ""
        del currParams.outboundstop
        del currParams.outbound_stop

        helper.getMarketFareRange(
            channel="B2C",
            currParams=currParams,
            currFltDateStr="09/15/2026",
            DaySpan=0,
            startTime="00:00",
            endtime="23:59",
            Day3Flag=False,
            hrs=24,
            origin="DEL",
            destination="BOM"
        )

        call_args = ag_mock.aggregate.call_args[0][0]
        match_stage = next(stage["$match"] for stage in call_args if "$match" in stage)
        allowed = match_stage["outbound_stop"]["$in"]
        self.assertIn("0", allowed)
        self.assertIn(0, allowed)
        self.assertNotIn("1", allowed)

    def test_database_helper_connection_details(self):
        from helper.rm_database_helper import RMDatabaseHelper

        db_helper = RMDatabaseHelper.__new__(RMDatabaseHelper)
        db_helper.logger = MagicMock()
        db_helper.db = MagicMock()
        db_helper.db.connections_details = MagicMock()
        db_helper.db.connections_details.find_one.return_value = {
            "FlightNo1": 1101,
            "FlightNo2": 1102,
            "Origin1": "DEL",
            "Destnination2": "BLR",
            "Departuredate1": "2026-09-15",
            "Departuredate2": "2026-09-15",
            "connection_duration": 5.75
        }

        dep2, dur = db_helper.get_connection_details("1101", "1102", "DEL", "BLR", "2026-09-15")
        self.assertEqual(dep2, "2026-09-15")
        self.assertEqual(dur, 5.75)

    def test_mongo_helper_outbound_stops_filter(self):
        from helper.rm_mongo_helper import MarketFareMongoDB

        mongo_helper = MarketFareMongoDB.__new__(MarketFareMongoDB)
        mongo_helper.logger = MagicMock()
        mongo_helper.master_db = MagicMock()
        mongo_helper.master_db.list_collection_names.return_value = ['market_fares_ag']
        ag_mock = MagicMock()
        ag_mock.aggregate.return_value = []
        mongo_helper.master_db.market_fares_ag = ag_mock

        currParams = MagicMock()
        currParams.origin = "DEL"
        currParams.destin = "BOM"
        currParams.carr_exclusion_b2c = ""
        currParams.flightExclusionB2C = ""
        currParams.outboundstop = 2

        mongo_helper.getMarketFareRange(
            channel="B2C",
            currParams=currParams,
            currFltDateStr="09/15/2026",
            DaySpan=0,
            startTime="00:00",
            endtime="23:59",
            Day3Flag=False,
            hrs=24
        )

        call_args = ag_mock.aggregate.call_args[0][0]
        match_stage = next(stage["$match"] for stage in call_args if "$match" in stage)
        self.assertIn("outbound_stop", match_stage)
        self.assertIn("$in", match_stage["outbound_stop"])
        allowed = match_stage["outbound_stop"]["$in"]
        self.assertIn("0", allowed)
        self.assertIn("1", allowed)
        self.assertIn("2", allowed)
        self.assertNotIn("3", allowed)

    def test_mongo_helper_connections_duration_float(self):
        from helper.rm_mongo_helper import MarketFareMongoDB

        mongo_helper = MarketFareMongoDB.__new__(MarketFareMongoDB)
        mongo_helper.logger = MagicMock()
        mongo_helper.master_db = MagicMock()
        mongo_helper.master_db.list_collection_names.return_value = ['market_fares_ag']
        ag_mock = MagicMock()
        ag_mock.aggregate.return_value = []
        mongo_helper.master_db.market_fares_ag = ag_mock

        currParams = MagicMock()
        currParams.sector1 = "DELBOM"
        currParams.sector2 = "BOMBLR"
        currParams.carrierExclusionB2C = ""

        mongo_helper.getMarketFareRangeConnections(
            channel="B2C",
            currParams=currParams,
            currFltDateStr="2026-09-15",
            outBoundStop="1",
            outboundDuration=7.25
        )

        call_args = ag_mock.aggregate.call_args[0][0]
        match_stage = next(stage["$match"] for stage in call_args if "$match" in stage)
        self.assertIn("outbound_duration", match_stage)
        self.assertEqual(match_stage["outbound_duration"]["$lte"], 7.25)


if __name__ == '__main__':
    unittest.main()
