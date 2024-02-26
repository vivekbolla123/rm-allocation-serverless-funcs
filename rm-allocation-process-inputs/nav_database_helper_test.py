# test_NavDatabaseHelper.py
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch
import pandas as pd
from models.input_source import InputSource
from  helper.nav_database_helper import NavDatabaseHelper

class TestNavDatabaseHelper(unittest.TestCase):
    def setUp(self):
        # Mocking necessary dependencies for testing
        self.mock_connection = MagicMock()
        self.mock_navitaire_connection = MagicMock()
        self.mock_logger = MagicMock()
        self.mock_timeutils = MagicMock()

        self.nav_helper = NavDatabaseHelper(
            navconn=self.mock_navitaire_connection,
            conn=self.mock_connection,
            inputSource=InputSource({}),
            nav_db_name="test_db",
            runId="test_run",
            debugMode=False,
            nav_db_wb_name="test_wb"
        )

        # Mocking a sample DataFrame for testing
        self.sample_dataframe = pd.DataFrame({
            'DepartureDate': ['2024-02-10', '2024-02-11'],
            'FlightNumber': ['123', '456'],
            'Origin': ['ABC', 'DEF'],
            'Destination': ['XYZ', 'GHI'],
            'bookedPax':['174','174']
        })

    def test_getCurrentBookedLoads(self):
        with patch('pandas.read_sql', return_value=self.sample_dataframe) as mock_read_sql:
            result = self.nav_helper.getCurrentBookedLoads(
                origin='ABC',
                destin='XYZ',
                startDate=datetime(2024, 2, 10),
                endDate=datetime(2024, 2, 11)
            )
            mock_read_sql.assert_called_once()
            self.assertEqual(result.shape[0], self.sample_dataframe.shape[0])  

    def test_checkForNextFLight(self):
        with patch('pandas.read_sql', return_value=self.sample_dataframe) as mock_read_sql:
            result = self.nav_helper.checkForNextFLight(
                currFltDateStr='02/10/2024',
                currParams=MagicMock(origin='ABC', destin='XYZ', flight_number='123')
            )
            mock_read_sql.assert_called_once()
            self.assertTrue(result,True) 
    def test_getLastSellingFare(self):
        with patch('pandas.read_sql', return_value=self.sample_dataframe) as mock_read_sql:
            result = self.nav_helper.getLastSellingFare(
                currFltDateStr='2024-02-10',
                currParams=MagicMock(origin='ABC', destin='XYZ', flight_number='123')
            )
            mock_read_sql.assert_called_once()
            self.assertIsNotNone(result)  

    def test_getNumberOfBookings(self):
        with patch('pandas.read_sql', return_value=self.sample_dataframe) as mock_read_sql:
            result = self.nav_helper.getNumberOfBookings(
                currFltDateStr='2024-02-10',
                currParams=MagicMock(origin='ABC', destin='XYZ', flight_number='123')
            )
            mock_read_sql.assert_called_once()
            self.assertIsNotNone(result)  
if __name__ == '__main__':
    unittest.main()
