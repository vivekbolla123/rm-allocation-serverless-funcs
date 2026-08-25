import os
import sys
import unittest
from unittest.mock import MagicMock, patch
import requests
import json

HERE = os.path.dirname(os.path.abspath(__file__))
LAMBDA_ROOT = os.path.dirname(HERE)
REPO_ROOT = os.path.dirname(LAMBDA_ROOT)
SHARED_CONFIG = os.path.join(REPO_ROOT, "rm-allocation-configurations")
for path in (LAMBDA_ROOT, SHARED_CONFIG):
	if path not in sys.path:
		sys.path.insert(0, path)

from Constants import CONST_NA, CONST_OVERBOOKING, DB, AKASA_AIRLINE_CODE
from helper.nav_api_helper import NavAPIHelper
from runAlloc import DynAlloc


class TestNavAPIHelperSafety(unittest.TestCase):

	def setUp(self):
		self.helper = NavAPIHelper.__new__(NavAPIHelper)
		self.helper.logger = MagicMock()
		self.helper.NAV_BASE_API_URL = "https://mocknav.api"
		self.helper.NAV_OCP_SUBSCRIPTION_KEY = "mock_sub_key"
		self.helper.NAV_USERNAME = "user"
		self.helper.NAV_PWD = "pwd"
		self.helper.NAV_DOMAIN = "domain"
		self.helper.base_url = "https://mocknav.api/api"
		self.helper.session = MagicMock()
		self.helper.token = None

	def test_get_token_json_decode_error(self):
		# When token endpoint returns 500 / non-JSON / empty string
		mock_resp = MagicMock()
		mock_resp.status_code = 500
		mock_resp.text = "Internal Server Error HTML"
		mock_resp.json.side_effect = requests.exceptions.JSONDecodeError("Expecting value", "doc", 0)
		self.helper.session.post.return_value = mock_resp

		self.helper.get_token()
		self.assertIsNone(self.helper.token)

	def test_get_leg_nest_details_json_decode_error(self):
		# When manifest endpoint returns 502 Bad Gateway / invalid JSON
		self.helper.token = "valid_token"
		mock_resp = MagicMock()
		mock_resp.status_code = 502
		mock_resp.text = "<html>502 Bad Gateway</html>"
		mock_resp.json.side_effect = requests.exceptions.JSONDecodeError("Expecting value", "doc", 0)
		self.helper.session.get.return_value = mock_resp

		leg_key, nest_key, lid, nest_lid = self.helper.get_leg_nest_details(
			"09/01/2026", AKASA_AIRLINE_CODE, "1383", "BOM", "DEL"
		)
		self.assertIsNone(leg_key)
		self.assertIsNone(nest_key)
		self.assertIsNone(lid)
		self.assertIsNone(nest_lid)

	def test_get_leg_nest_details_when_token_is_none(self):
		self.helper.token = None
		# get_token will also fail to acquire token
		self.helper.get_token = MagicMock()

		leg_key, nest_key, lid, nest_lid = self.helper.get_leg_nest_details(
			"09/01/2026", AKASA_AIRLINE_CODE, "1383", "BOM", "DEL"
		)
		self.assertIsNone(leg_key)
		self.assertIsNone(nest_key)

	def test_get_nest_key_json_decode_error(self):
		self.helper.token = "valid_token"
		mock_resp = MagicMock()
		mock_resp.status_code = 200
		mock_resp.json.side_effect = requests.exceptions.JSONDecodeError("Expecting value", "doc", 0)
		self.helper.session.get.return_value = mock_resp

		nest_key, lid, nest_lid = self.helper.get_nest_key("leg_123")
		self.assertIsNone(nest_key)
		self.assertIsNone(lid)
		self.assertIsNone(nest_lid)

	def test_update_leg_safe_on_none(self):
		# Calling update_leg with None should not make any HTTP call
		self.helper.update_leg(None, 180)
		self.helper.session.patch.assert_not_called()

	def test_overbooking_pass_safe_when_navapi_fails(self):
		# Ensure DynAlloc.overBooking does not crash when navapi returns None or fails
		dyn = DynAlloc.__new__(DynAlloc)
		dyn.logger = MagicMock()
		dyn.constantsUtils = MagicMock()
		dyn.constantsUtils.OVERBOOKING_LF = 40.0
		dyn.constantsUtils.OVERBOOKING_END_NDO = 30
		dyn.constantsUtils.OVERBOOKING_START_NDO = 0
		dyn.currParams = MagicMock()
		dyn.currParams.NextFlight = True
		dyn.currParams.origin = "BOM"
		dyn.currParams.destin = "DEL"
		dyn.currParams.flight_number = "1383"
		dyn.currParams.obSeats = None
		dyn.currParams.obFare = None
		dyn.currParams.over_booking = 1
		dyn.rmdatabasehelper = MagicMock()
		dyn.rmdatabasehelper.getOverbookedSeats.return_value = 5
		dyn.inputSource = MagicMock()
		dyn.inputSource.input_source = {CONST_OVERBOOKING: DB}
		dyn.navapihelper = MagicMock()
		# Mock navapihelper get_leg_nest_details raising an exception or returning None
		dyn.navapihelper.get_leg_nest_details.return_value = (None, None, None, None)

		totalCapacity, b2b_flag, expected_no_shows = dyn.overBooking(
			"09/01/2026", 10, 180, 180, 50.0, 50
		)
		self.assertEqual(totalCapacity, 184)
		self.assertTrue(b2b_flag)
		self.assertEqual(expected_no_shows, 5)


if __name__ == '__main__':
	unittest.main()
