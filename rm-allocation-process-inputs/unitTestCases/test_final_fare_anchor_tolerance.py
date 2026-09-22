import json
import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
from Constants import CONST_NA
from determine_rbd import DetermineRBD
from constants_utils import ConstantsUtils


class TestFinalFareAnchorTolerance(unittest.TestCase):

    def _create_determiner(self, mock_redis=None, auto_tolerance_flag=1):
        if mock_redis is None:
            mock_redis = MagicMock()
            mock_redis.hget.return_value = None

        mock_db = MagicMock()
        mock_db.runId = 200
        mock_db.getAutoToleranceConfig.return_value = {
            "ndo_config": pd.DataFrame([{"ndo_min": 0, "ndo_max": 999, "n_value": 1}]),
            "delta_bands": pd.DataFrame([{"id": 1, "min_value": -999.0, "max_value": 999.0}]),
            "grid_values": pd.DataFrame([{"band_id": "3", "delta_band_id": "1", "g_value": 0.5}]),
        }

        mock_conn = MagicMock()
        mock_conn.execute.return_value = []
        constantsutils = ConstantsUtils(mock_conn)

        determiner = DetermineRBD(
            rmdatabasehelper=mock_db,
            logger=MagicMock(),
            inputsource=MagicMock(),
            constantsutils=constantsutils,
            mathutils=MagicMock(),
            redisHelper=mock_redis,
        )

        curr_params = MagicMock()
        curr_params.origin = "DEL"
        curr_params.destin = "BOM"
        curr_params.flight_number = "101"
        curr_params.per_start = "10"
        curr_params.auto_tolerance_flag = auto_tolerance_flag
        curr_params.portfolio_only = "0"
        curr_params.opening_fares = 1
        curr_params.profileFares = 1

        return determiner, curr_params, mock_redis

    def test_final_fare_anchor_last_sold_ge_strategy_execution_run_no_crash(self):
        """
        Verify that when lastSoldFare >= strategyFare and auto_tolerance_flag=1
        on an execution run, finalFareAnchor does not raise ValueError: could not convert string to float: 'NA'.
        """
        mock_redis = MagicMock()
        # Mock history in Redis having run_count=1, so next run is run_count=2.
        # With N=1, run_count=2 is an execution run (2 % 2 == 0) and target_run_count=1 is found in history.
        history_in_redis = [
            {
                "run_id": 199,
                "run_count": 1,
                "T": 400.0,
                "T_prime": "NA",
                "T_V": 3600.0,
                "actual_dplf": 2.0,
                "actual_imp_forecast": 90.0,
                "b2c_tolerance": "400.0",
            }
        ]
        mock_redis.hget.return_value = json.dumps(history_in_redis)

        determiner, curr_params, _ = self._create_determiner(mock_redis=mock_redis, auto_tolerance_flag=1)

        # Value 'A', lastSoldFare (5000.0) >= strategyFare (4000.0)
        # Previously, this branch did not set anchorFareBeforeTolerance, causing calculate_tolerance_value('NA') to crash.
        result = determiner.finalFareAnchor(
            strategyFare=4000.0,
            profileFare=4200.0,
            lastSoldFare=5000.0,
            hardAnchorFare=CONST_NA,
            value='A',
            dPLF=0,
            toleranceFare="400.0",
            currParams=curr_params,
            plf=85.0,
            overBookingCount=0,
            dplfBand=3,
            BookingsInHour=0,
            last7avg=4500.0,
            isProfileAnchor=False,
            b2b_discount_fare=False,
            anchorFareDiscounted=0,
            obfare=CONST_NA,
            ndocounter=15,
            imp_forecast=90.0,
        )

        anchorFare, howDetermined, upsell, overBookingFare, toleranceValue, anchorFareBeforeTolerance, T_prime = result
        self.assertEqual(anchorFare, 5000.0)
        self.assertEqual(anchorFareBeforeTolerance, 5000.0)
        self.assertNotEqual(toleranceValue, CONST_NA)
        self.assertTrue(mock_redis.hset.called)

    def test_final_fare_anchor_value_b_strategy_fare_execution_run(self):
        """
        Verify that value 'B' when max_value == strategyFare does not crash
        on an execution run and sets anchorFareBeforeTolerance.
        """
        mock_redis = MagicMock()
        history_in_redis = [
            {
                "run_id": 199,
                "run_count": 1,
                "T": 400.0,
                "T_prime": "NA",
                "T_V": 3600.0,
                "actual_dplf": 2.0,
                "actual_imp_forecast": 90.0,
                "b2c_tolerance": "400.0",
            }
        ]
        mock_redis.hget.return_value = json.dumps(history_in_redis)

        determiner, curr_params, _ = self._create_determiner(mock_redis=mock_redis, auto_tolerance_flag=1)

        result = determiner.finalFareAnchor(
            strategyFare=4000.0,
            profileFare=CONST_NA,
            lastSoldFare=4000.0,
            hardAnchorFare=CONST_NA,
            value='B',
            dPLF=0,
            toleranceFare="400.0",
            currParams=curr_params,
            plf=85.0,
            overBookingCount=0,
            dplfBand=3,
            BookingsInHour=0,
            last7avg=CONST_NA,
            isProfileAnchor=False,
            b2b_discount_fare=False,
            anchorFareDiscounted=0,
            obfare=CONST_NA,
            ndocounter=15,
            imp_forecast=90.0,
        )

        anchorFare, howDetermined, upsell, overBookingFare, toleranceValue, anchorFareBeforeTolerance, T_prime = result
        self.assertEqual(anchorFare, 4000.0)
        self.assertEqual(anchorFareBeforeTolerance, 4000.0)
        self.assertNotEqual(toleranceValue, CONST_NA)

    def test_final_fare_anchor_value_c_skips_tolerance(self):
        """
        Verify that value 'C' sets anchorFare=0, leaves toleranceValue as CONST_NA,
        and does not attempt to calculate tolerance or write history.
        """
        mock_redis = MagicMock()
        determiner, curr_params, _ = self._create_determiner(mock_redis=mock_redis, auto_tolerance_flag=1)

        result = determiner.finalFareAnchor(
            strategyFare=4000.0,
            profileFare=CONST_NA,
            lastSoldFare=CONST_NA,
            hardAnchorFare=CONST_NA,
            value='C',
            dPLF=0,
            toleranceFare="400.0",
            currParams=curr_params,
            plf=85.0,
            overBookingCount=0,
            dplfBand=3,
            BookingsInHour=0,
            last7avg=CONST_NA,
            isProfileAnchor=False,
            b2b_discount_fare=False,
            anchorFareDiscounted=0,
            obfare=CONST_NA,
            ndocounter=15,
            imp_forecast=90.0,
        )

        anchorFare, howDetermined, upsell, overBookingFare, toleranceValue, anchorFareBeforeTolerance, T_prime = result
        self.assertEqual(anchorFare, 0)
        self.assertEqual(toleranceValue, CONST_NA)
        self.assertFalse(mock_redis.hset.called)

    def test_calculate_tolerance_defensive_against_na_pre_anchor(self):
        """
        Verify that even if anchorFareBeforeTolerance is explicitly 'NA',
        finalFareAnchor does not raise ValueError.
        """
        mock_redis = MagicMock()
        history_in_redis = [
            {
                "run_id": 199,
                "run_count": 1,
                "T": 400.0,
                "T_prime": "NA",
                "T_V": 3600.0,
                "actual_dplf": 2.0,
                "actual_imp_forecast": 90.0,
                "b2c_tolerance": "400.0",
            }
        ]
        mock_redis.hget.return_value = json.dumps(history_in_redis)

        determiner, curr_params, _ = self._create_determiner(mock_redis=mock_redis, auto_tolerance_flag=1)

        # Force a case where anchorFare is 0 and anchorFareBeforeTolerance stays CONST_NA
        with patch.object(determiner, "calculate_overbooking_fare_and_status", return_value=(0, "test", CONST_NA, 0)):
            try:
                determiner.finalFareAnchor(
                    strategyFare=4000.0,
                    profileFare=CONST_NA,
                    lastSoldFare=CONST_NA,
                    hardAnchorFare=CONST_NA,
                    value='A',
                    dPLF=0,
                    toleranceFare="400.0",
                    currParams=curr_params,
                    plf=85.0,
                    overBookingCount=0,
                    dplfBand=3,
                    BookingsInHour=0,
                    last7avg=CONST_NA,
                    isProfileAnchor=False,
                    b2b_discount_fare=False,
                    anchorFareDiscounted=0,
                    obfare=CONST_NA,
                    ndocounter=15,
                    imp_forecast=90.0,
                )
            except ValueError as e:
                self.fail(f"finalFareAnchor raised ValueError unexpectedly: {e}")


if __name__ == "__main__":
    unittest.main()
