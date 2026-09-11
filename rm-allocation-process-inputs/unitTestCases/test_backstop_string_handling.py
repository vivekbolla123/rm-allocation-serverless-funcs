import unittest
from unittest.mock import MagicMock
import os
import sys
import pandas as pd

sys.path.insert(
    0,
    os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "..", "rm-allocation-configurations")),
)
sys.path.insert(
    0,
    os.path.normpath(os.path.join(os.path.dirname(__file__), "..")),
)

from Constants import CONST_NA, B2B_CHANNEL, B2C_CHANNEL
from determine_rbd import DetermineRBD
from allocations import RBDAllocations
from helper.rm_database_helper import RMDatabaseHelper


class TestBackstopStringHandling(unittest.TestCase):

    def setUp(self):
        self.rmdb = MagicMock()
        self.logger = MagicMock()
        self.constantsutils = MagicMock()
        self.mathutils = MagicMock()
        self.redishelper = MagicMock()
        self.timeutils = MagicMock()

        self.constantsutils.LOWEST_B2B_RBD_VALUE = "Z9"
        self.constantsutils.LOWEST_B2C_RBD_VALUE = "U"
        self.constantsutils.GROUP_AU_BATCH = 5
        self.constantsutils.GROUP_QUOTATION = '{"RBD1": "G1", "RBD2": "G2"}'

        self.determinerbd = DetermineRBD(
            self.rmdb, self.logger, MagicMock(), self.constantsutils, self.mathutils, self.redishelper
        )
        self.allocations = RBDAllocations(
            self.rmdb, self.logger, self.constantsutils, self.mathutils, self.timeutils
        )

    def test_get_fares_with_string_backstop_comparison(self):
        """Verify that getFaresWithBackstop does not raise TypeError when backstop is a string."""
        # Simulated defFareData: lowest published fare >= backstop
        self.rmdb.fetchExtremePublishedFare.return_value = pd.DataFrame({
            "RBD": ["V1"], "TOTAL": [4700.0]
        })
        rbddata = pd.DataFrame({"RBD": ["V2"], "TOTAL": [4600.0]})
        openResult = {
            "RBD": ["V2"], "Fare": [4600.0], "FareResult": ["Strategy"],
            "anchorFare": [4600.0], "statusInd": [0], "allocationStatus": [1]
        }

        # Case 1: String numeric backstop (e.g. "4500")
        res1 = self.determinerbd.getFaresWithBackstop(
            "BOM", "DEL", 4600.0, 0, B2C_CHANNEL, "4500", openResult, rbddata, "Strategy", "L"
        )
        self.assertIsNotNone(res1)
        self.assertIn("Backstop Hit", res1["FareResult"][0])

        # Case 2: String "0"
        self.rmdb.fetchExtremePublishedFare.return_value = pd.DataFrame({
            "RBD": ["V2"], "TOTAL": [4600.0]
        })
        res2 = self.determinerbd.getFaresWithBackstop(
            "BOM", "DEL", 4600.0, 0, B2C_CHANNEL, "0", openResult, rbddata, "Strategy", "L"
        )
        self.assertIsNotNone(res2)

        # Case 3: String "NA"
        res3 = self.determinerbd.getFaresWithBackstop(
            "BOM", "DEL", 4600.0, 0, B2C_CHANNEL, "NA", openResult, rbddata, "Strategy", "L"
        )
        self.assertIsNotNone(res3)

        # Case 4: None
        res4 = self.determinerbd.getFaresWithBackstop(
            "BOM", "DEL", 4600.0, 0, B2C_CHANNEL, None, openResult, rbddata, "Strategy", "L"
        )
        self.assertIsNotNone(res4)

    def test_extreme_fares_skipping_with_string_backstop(self):
        """Verify extremeFaresSkipping handles string backstop safely."""
        self.rmdb.fetchExtremePublishedFare.return_value = pd.DataFrame({
            "RBD": ["Y"], "TOTAL": [9000.0]
        })
        openResult = {}
        res = self.determinerbd.extremeFaresSkipping(
            "BOM", "DEL", 4500.0, 0, B2C_CHANNEL, "4500", openResult, "L"
        )
        self.assertEqual(res["RBD"][0], "Y")

    def test_group_quote_alloc_with_string_b2c_backstop_final(self):
        """Verify group_quote_alloc does not raise '<' not supported between 'float' and 'str'."""
        currRec = MagicMock()
        B2CRBDs = pd.DataFrame({
            "RBD": ["G1", "G2"],
            "TOTAL": [4400.0, 4800.0]
        })

        self.rmdb.profileFaresExtract.return_value = pd.DataFrame({
            "Q1": [4300.0], "Q2": [4500.0], "Q3": [4700.0], "Q4": [4900.0]
        })
        self.rmdb.get_group_potential_seats.return_value = 10
        self.rmdb.get_group_profile_fare_increments.return_value = {}
        self.rmdb.get_all_group_forecast_bands.return_value = [
            {"Band": 0, "b2c_discount": 0, "fare_anchor_strategy": "MIN", "b2c_fare_weight": 0.5, "profile_fare_weight": 0.5}
        ]
        self.rmdb.getdpflBand.return_value = 0

        # Pass b2c_backstop_final as string "4500"
        # Should execute without throwing '<' not supported between float and str
        counter, profile_fare, bookload, fare_anchor = self.allocations.group_quote_alloc(
            brd_counter=0,
            currRec=currRec,
            autoGroup=1,
            adjustedCapacity=180,
            upsell=0,
            B2CRBDs=B2CRBDs,
            openingRBDPrice=4000.0,
            groupbookload=0,
            currBookedLoad=50,
            month="May",
            sector="BOMDEL",
            deptTime_str_hhmm="12:00",
            dow_str_for_profile="Mon",
            final_totalCapacity=180,
            variance_full=0.0,
            b2c_selling_fare=4600.0,
            b2c_backstop_final="4500",  # String from input/state
        )

        # Logger should NOT have logged any '<' not supported error
        for call in self.logger.error.call_args_list:
            self.assertNotIn("Error in group_quote_alloc", str(call))

    def test_group_quote_alloc_with_na_backstop(self):
        """Verify group_quote_alloc handles 'NA' backstop safely."""
        currRec = MagicMock()
        B2CRBDs = pd.DataFrame({
            "RBD": ["G1", "G2"],
            "TOTAL": [4400.0, 4800.0]
        })

        self.rmdb.profileFaresExtract.return_value = pd.DataFrame({
            "Q1": [4300.0], "Q2": [4500.0], "Q3": [4700.0], "Q4": [4900.0]
        })
        self.rmdb.get_group_potential_seats.return_value = 10
        self.rmdb.get_group_profile_fare_increments.return_value = {}
        self.rmdb.get_all_group_forecast_bands.return_value = [
            {"Band": 0, "b2c_discount": 0, "fare_anchor_strategy": "MIN", "b2c_fare_weight": 0.5, "profile_fare_weight": 0.5}
        ]
        self.rmdb.getdpflBand.return_value = 0

        counter, profile_fare, bookload, fare_anchor = self.allocations.group_quote_alloc(
            brd_counter=0,
            currRec=currRec,
            autoGroup=1,
            adjustedCapacity=180,
            upsell=0,
            B2CRBDs=B2CRBDs,
            openingRBDPrice=4000.0,
            groupbookload=0,
            currBookedLoad=50,
            month="May",
            sector="BOMDEL",
            deptTime_str_hhmm="12:00",
            dow_str_for_profile="Mon",
            final_totalCapacity=180,
            variance_full=0.0,
            b2c_selling_fare=4600.0,
            b2c_backstop_final="NA",  # CONST_NA
        )

        for call in self.logger.error.call_args_list:
            self.assertNotIn("Error in group_quote_alloc", str(call))


if __name__ == "__main__":
    unittest.main()
