import json
import unittest
from unittest.mock import MagicMock, patch
from Constants import CONST_NA


class TestSellingFareRedisHistory(unittest.TestCase):

    def test_determine_rbd_does_not_set_premature_selling_fare(self):
        """Verify that apply_tolerance_logic in DetermineRBD does not set selling_fare on new_record."""
        from determine_rbd import DetermineRBD

        mock_db = MagicMock()
        mock_db.runId = 101
        mock_db.getAutoToleranceConfig.return_value = {}
        mock_redis = MagicMock()
        mock_redis.hget.return_value = None

        from constants_utils import ConstantsUtils
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
        curr_params.auto_tolerance_flag = 0
        curr_params.portfolio_only = "0"

        # Mock out calculate_anchor to return known values
        determiner.forecast_logic = MagicMock()
        determiner.forecast_logic.calculate_anchor.return_value = {
            "anchor_fare": 4532.8,
            "how_determined": "AllocationY_Band3_Stable_UpsellN_Formula",
        }

        with patch.object(determiner, "get_autotolerance_history", return_value=[]):
            determiner.final_fare_anchor_forecast(
                strategyFare=4000.0,
                profileFare=4200.0,
                lastSoldFare=4100.0,
                last7AvgVal=4000.0,
                hardAnchorFare="NA",
                currParams=curr_params,
                plf=85.0,
                overBookingCount=0,
                BookingsInHour=1,
                last7avg=4000.0,
                isProfileAnchor=False,
                b2b_discount_fare=False,
                anchorFareDiscounted=4000.0,
                obfare="NA",
                ndocounter=10,
                imp_forecast=92.0,
                imp_forecast_band=3,
                af_forecast_band="NA",
                afForecastAllocFlag=0,
                Af_Nb_Allin="NA",
                PLF_Gain=0,
                forecast_plf=90.0,
                toleranceFare="NA",
            )

        # Verify what was written to Redis via hset
        self.assertTrue(mock_redis.hset.called)
        call_args = mock_redis.hset.call_args[0]
        redis_key, hash_field, payload_json = call_args
        self.assertEqual(redis_key, "run_history:DEL_BOM_101_10")
        self.assertEqual(hash_field, "autotolerance_history")

        history = json.loads(payload_json)
        self.assertEqual(len(history), 1)
        # selling_fare must NOT be present in the newly created tolerance record
        self.assertNotIn("selling_fare", history[0])

    def test_update_selling_fare_in_history_success(self):
        """Verify _update_selling_fare_in_history updates history[-1]['selling_fare'] in Redis."""
        from runAlloc import DynAlloc

        dyn = DynAlloc.__new__(DynAlloc)
        dyn.logger = MagicMock()
        dyn.rmdatabasehelper = MagicMock()
        dyn.rmdatabasehelper.runId = 101

        curr_params = MagicMock()
        curr_params.origin = "DEL"
        curr_params.destin = "BOM"
        curr_params.flight_number = "101"
        curr_params.per_start = "10"
        curr_params.portfolio_only = "0"
        dyn.currParams = curr_params

        existing_history = [
            {
                "run_id": 100,
                "run_count": 1,
                "T": 400.0,
                "selling_fare": 4200.0,
            },
            {
                "run_id": 101,
                "run_count": 2,
                "T": 400.0,
                # selling_fare not set yet for current run
            }
        ]

        mock_redis = MagicMock()
        mock_redis.hget.return_value = json.dumps(existing_history)
        dyn.redishelper = mock_redis

        dyn.outputSummaryRow_b2c = MagicMock()
        dyn.outputSummaryRow_b2c.SellingFare = "4600.0"

        state = MagicMock()
        dyn._update_selling_fare_in_history(state)

        # Verify hset was called with updated selling_fare on the latest record
        self.assertTrue(mock_redis.hset.called)
        call_args = mock_redis.hset.call_args[0]
        redis_key, hash_field, payload_json = call_args

        self.assertEqual(redis_key, "run_history:DEL_BOM_101_10")
        self.assertEqual(hash_field, "autotolerance_history")

        updated_history = json.loads(payload_json)
        self.assertEqual(len(updated_history), 2)
        self.assertEqual(updated_history[0]["selling_fare"], 4200.0)
        self.assertEqual(updated_history[1]["selling_fare"], 4600.0)

    def test_update_selling_fare_in_history_na_skips(self):
        """Verify _update_selling_fare_in_history does nothing when SellingFare is NA."""
        from runAlloc import DynAlloc

        dyn = DynAlloc.__new__(DynAlloc)
        dyn.logger = MagicMock()
        dyn.currParams = MagicMock()
        dyn.currParams.portfolio_only = "0"

        mock_redis = MagicMock()
        dyn.redishelper = mock_redis

        dyn.outputSummaryRow_b2c = MagicMock()
        dyn.outputSummaryRow_b2c.SellingFare = CONST_NA

        state = MagicMock()
        dyn._update_selling_fare_in_history(state)

        # hset should not be called
        self.assertFalse(mock_redis.hset.called)


if __name__ == "__main__":
    unittest.main()
