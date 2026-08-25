import os
import sys
import unittest
from unittest.mock import MagicMock
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
LAMBDA_ROOT = os.path.dirname(HERE)
REPO_ROOT = os.path.dirname(LAMBDA_ROOT)
SHARED_CONFIG = os.path.join(REPO_ROOT, "rm-allocation-configurations")
for path in (LAMBDA_ROOT, SHARED_CONFIG):
	if path not in sys.path:
		sys.path.insert(0, path)

from Constants import CONST_NA, B2C_CHANNEL, B2B_CHANNEL
from allocations import RBDAllocations
from determine_rbd import DetermineRBD
from helper.rm_database_helper import RMDatabaseHelper
from models.au_grid_row import AuGridRow


class TestConnectionsRBDKeyError(unittest.TestCase):

	def setUp(self):
		self.allocations = RBDAllocations.__new__(RBDAllocations)
		self.allocations.rmdatabasehelper = MagicMock()
		self.allocations.logger = MagicMock()
		self.allocations.constantsutils = MagicMock()
		self.allocations.constantsutils.TBF_RBD = "T,B,F"
		self.allocations.constantsutils.CUG_RBD = "C,U,G"
		self.allocations.constantsutils.B2B_RDB_COUNT = 10
		self.allocations.mathutils = MagicMock()
		self.allocations.mathutils.generateRoundNumber = MagicMock(return_value=10)

		self.currParams = MagicMock()
		self.currParams.sector1 = "BOM-DEL"
		self.currParams.sector2 = "DEL-BLR"
		self.currParams.origin = "BOM"
		self.currParams.destin = "BLR"
		self.currParams.first_rbd_alloc = 10
		self.currParams.other_rbd_alloc = 5
		self.currParams.b2b_factor = 1.0
		self.currParams.skipping_factor = 0

	def test_b2CAllocation_connections_empty_fares(self):
		# When fareExtract returns empty DataFrame (no published fares for route)
		self.allocations.rmdatabasehelper.fareExtract = MagicMock(
			return_value=pd.DataFrame(columns=['RBD', 'TOTAL', 'Org', 'Dst', 'route'])
		)
		openDataB2C = {'RBD': ['NA'], 'Fare': ['NA'], 'FareResult': ['Unable to fetch extreme published Fares'], 'statusInd': [1], 'allocationStatus': [0]}
		currRec = AuGridRow(50)

		units, brd_counter = self.allocations.b2CAllocation_connections(
			openDataB2C, currRec, 180, 50, 0, self.currParams, False
		)
		self.assertEqual(units, [])
		self.assertEqual(brd_counter, 1)

	def test_b2CAllocation_connections_no_columns_dataframe(self):
		# When fareExtract returns completely blank DataFrame pd.DataFrame([])
		self.allocations.rmdatabasehelper.fareExtract = MagicMock(
			return_value=pd.DataFrame()
		)
		openDataB2C = {'RBD': ['NA'], 'Fare': ['NA'], 'FareResult': ['Unable to fetch extreme published Fares'], 'statusInd': [1], 'allocationStatus': [0]}
		currRec = AuGridRow(50)

		units, brd_counter = self.allocations.b2CAllocation_connections(
			openDataB2C, currRec, 180, 50, 0, self.currParams, False
		)
		self.assertEqual(units, [])
		self.assertEqual(brd_counter, 1)

	def test_b2BAllocation_connections_empty_fares(self):
		# When fareExtract returns empty DataFrame for B2B
		self.allocations.rmdatabasehelper.fareExtract = MagicMock(
			return_value=pd.DataFrame(columns=['RBD', 'TOTAL', 'Org', 'Dst', 'route'])
		)
		openDataB2B = {'RBD': ['NA'], 'Fare': ['NA'], 'FareResult': ['Unable to fetch extreme published Fares'], 'statusInd': [1], 'allocationStatus': [0]}
		currRec = AuGridRow(50)

		brd_counter, updatedRec = self.allocations.b2BAllocation_connections(
			openDataB2B, currRec, [], 1, self.currParams
		)
		self.assertEqual(brd_counter, 1)

	def test_extremeFaresSkipping_empty_db(self):
		# When DB has no published fares at all
		determinerbd = DetermineRBD.__new__(DetermineRBD)
		determinerbd.rmdatabasehelper = MagicMock()
		determinerbd.rmdatabasehelper.fetchExtremePublishedFare = MagicMock(
			return_value=pd.DataFrame(columns=['RBD', 'TOTAL', 'Org', 'Dst', 'route'])
		)
		determinerbd.logger = MagicMock()

		openResult = pd.DataFrame(columns=['RBD', 'Fare', 'FareResult', 'anchorFare', 'statusInd', 'allocationStatus'])
		res = determinerbd.extremeFaresSkipping('BOM', 'BLR', 5000, 0, 'B2C', 0, openResult, 'R')

		self.assertEqual(res['RBD'], [CONST_NA])
		self.assertEqual(res['Fare'], [CONST_NA])
		self.assertEqual(res['statusInd'], [1])
		self.assertEqual(res['allocationStatus'], [0])

	def test_fareExtract_empty_mongo_results(self):
		# Verify RMDatabaseHelper.fareExtract defaults safely when find() is empty
		helper = RMDatabaseHelper.__new__(RMDatabaseHelper)
		helper.db = MagicMock()
		helper.db.fares.find.return_value.sort.return_value = []
		helper.logger = MagicMock()
		helper.rbd_list_builder = MagicMock(return_value=['Y', 'B', 'M'])

		df = helper.fareExtract('BOM', 'BLR', 'B2C', '', 'R')
		self.assertTrue(df.empty)
		self.assertIn('RBD', df.columns)
		self.assertIn('TOTAL', df.columns)


if __name__ == '__main__':
	unittest.main()
