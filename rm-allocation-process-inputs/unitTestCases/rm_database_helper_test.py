import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta,timezone
import pandas as pd
from models.input_source import InputSource

# Import the class to be tested
from helper.rm_database_helper import RMDatabaseHelper

class TestRMDatabaseHelper(unittest.TestCase):

    def setUp(self):
        # Mock the necessary objects (e.g., database connections, logger, etc.)
        self.conn = MagicMock()
        self.wrconn = MagicMock()
        self.cache_client = MagicMock()
        self.logger = MagicMock()
        self.inputSource = InputSource({'inputSource':{'Curves': 'DB', 'Market_list': 'DB', 'Market_fares': 'DB', 'Fares': 'DB', 'inputs_status': 'DB', 'bookload': 'DB', 'ndo_bands': 'DB', 'dplf_bands': 'DB', 'd1_d2_strategies': 'DB', 'lastSellingFare': 'DB', 'overbooking': 'DB'},'logFiles':{'run_summary':'DB','allocation_run_audit':'DB'}})
        self.debugmode = MagicMock()

        # Create an instance of RMDatabaseHelper for testing
        self.rm_db_helper = RMDatabaseHelper(self.conn, self.wrconn, "runId", self.inputSource, self.debugmode, self.cache_client)

    def test_getLatestCodeVersion(self):
        # Mock the database query result
        pd.read_sql = MagicMock(return_value=pd.DataFrame({'code_version': [1]}))

        # Call the method to be tested
        result = self.rm_db_helper.getLatestCodeVersion()

        # Assert the expected result
        self.assertEqual(result, 1)
        
    def test_loadRunParams(self):
        # Mock the database query result for DB input source
        pd.read_sql = MagicMock(return_value=pd.DataFrame({'col1': [1, 2], 'col2': ['A', 'B']}))

        # Call the method to be tested
        result = self.rm_db_helper.loadRunParams()

        # Assert the expected result
        self.assertEqual(result['col1'].tolist(), [1, 2])
        self.assertEqual(result['col2'].tolist(), ['A', 'B'])

    @patch('helper.redis_helper.RedisHelper')
    def test_insertSummaryRow(self, mock_redishelper):
        # Mock the RedisHelper class
        mock_redishelper.updateRunAudit.return_value = None

        # Mock the database execution
        self.rm_db_helper.wrconn.execute = MagicMock()

        # Mock data for testing
        allocResults = MagicMock({'Id': [1], 'Origin': ['A'], 'Destin': ['B'], 'FltNum': ['F123'],
                                      'DepDate': ['2024-01-01'], 'BookedLoad': [100], 'TgtLoad': [120],
                                      'MktFare_Min': [150.0], 'OpenRBD': ['Y'], 'SellingFare': [160.0],
                                      'Channel': ['B2C'], 'HowDetermined': ['Manual'], 'RunId': ['run123'],
                                      'CreatedAt': ['2024-01-01 10:00:00']})

        # Call the method to be tested
        self.rm_db_helper.insertSummaryRow(allocResults)

        # Assert the expected calls
        self.rm_db_helper.wrconn.execute.assert_called_once()
        
    def test_insertRunStart(self):
        # Mock the database execution
        self.wrconn.execute = MagicMock()
        pd.read_sql = MagicMock(return_value=pd.DataFrame({'curr_version': [1,1,1,1],'tableName':['test','Curves','d1_d2_strategies','Fares']}))
        # Call the method to be tested
        self.rm_db_helper.insertRunStart(datetime.now(), 'test_user')

        # Assert that the execute method was called with the expected arguments
        self.wrconn.execute.assert_called_once()

    def test_updateRunEnd(self):
        # Mock the database execution
        self.wrconn.execute = MagicMock()

        # Call the method to be tested
        self.rm_db_helper.updateRunEnd(datetime.now())

        # Assert that the execute method was called with the expected arguments
        self.wrconn.execute.assert_called_once()
        
    def test_areAllInputsReady(self):
        # Mock the database query result
        pd.read_sql = MagicMock(return_value=pd.DataFrame({'busyInputs': [0]}))

        # Set the input source to DB
        # self.inputSource.input_source = {CONST_INPUT_STATUS: DB}

        # Call the method to be tested
        result = self.rm_db_helper.areAllInputsReady()

        # Assert the expected result
        self.assertTrue(result)
        
    def test_getTargetBookedLoadFromBenchmarkCurve(self):
        # Mock the database query result
        pd.read_sql = MagicMock(return_value=pd.DataFrame({'LF': [0.8], 'B2C_Avg_Fare': [100], 'B2B_Avg_Fare': [90]}))

        # Set input parameters
        ndocounter = 5
        curveID = 'ABC123'

        # Call the method to be tested
        result = self.rm_db_helper.getTargetBookedLoadFromBenchmarkCurve(ndocounter, curveID)

        # Assert the expected result
        expected_result = pd.DataFrame({'LF': [0.8], 'B2C_Avg_Fare': [100], 'B2B_Avg_Fare': [90]})
        pd.testing.assert_frame_equal(result, expected_result)
        
    def test_checkNowShowProb(self):
        # Mock the database query result
        pd.read_sql = MagicMock(return_value=pd.DataFrame({'OverBookingCount': [2]}))

        # Set input parameters
        sector = 'TestSector'
        flight_no = 'ABC123'
        constants_utils = MagicMock()
        constants_utils.OVERBOOKING_PROB = 0.8

        # Call the method to be tested
        result = self.rm_db_helper.checkNowShowProb(sector, flight_no, constants_utils)

        # Assert the expected result
        expected_result = 2
        self.assertEqual(result, expected_result)

    def test_getNdoBand(self):
        # Mock the database query result
        pd.read_sql = MagicMock(return_value=pd.DataFrame({'ndo_band': ['BandA']}))

        # Set input parameters
        ndo = 5

        # Call the method to be tested
        result = self.rm_db_helper.getNdoBand(ndo)

        # Assert the expected result
        expected_result = 'BandA'
        self.assertEqual(result, expected_result)

    def test_getdpflBand(self):
        # Mock the database query result
        pd.read_sql = MagicMock(return_value=pd.DataFrame({'dplf_band': ['BandB']}))

        # Set input parameters
        dPLF = 0.75

        # Call the method to be tested
        result = self.rm_db_helper.getdpflBand(dPLF)

        # Assert the expected result
        expected_result = 'BandB'
        self.assertEqual(result, expected_result)
        
    def test_getPlfThreshhold(self):
        # Mock the database query result
        pd.read_sql = MagicMock(return_value=pd.DataFrame({'plf_threshold': [0.8]}))

        # Set input parameters
        ndocounter = 1
        flightNumber = 'ABC123'

        # Call the method to be tested
        result = self.rm_db_helper.getPlfThreshhold(ndocounter, flightNumber)

        # Assert the expected result
        expected_result = 0.8
        self.assertEqual(result[0], expected_result)

    

    # def test_getDepartureTimeBands(self):
    #     # Mock the database query result for the first query
    #     mock_result = pd.DataFrame({
    #         'StartTime': ['08:00', '12:00'],
    #         'EndTime': ['10:00', '14:00'],
    #         'TimeBand': ['Morning', 'Afternoon']
    #     })
    #     self.conn.execute = MagicMock(return_value=mock_result)

    #     # Mock the database query result for the second query
    #     mock_result_offset = pd.DataFrame({
    #         'StartTime': ['10:00:00'],
    #         'EndTime': ['12:00:00']
    #     })
    #     self.conn.execute.side_effect = [mock_result, mock_result_offset]

    #     # Call the method to be tested
    #     time_bands, start_end_range = self.rm_db_helper.getDepartureTimeBands()

    #     # Assert the expected results
    #     expected_time_bands = [
    #         (self.timeutils.parse_time('08:00:00'), self.timeutils.parse_time('10:00:00'), 'Morning'),
    #         (self.timeutils.parse_time('12:00:00'), self.timeutils.parse_time('14:00:00'), 'Afternoon')
    #     ]
    #     expected_start_end_range = [(self.timeutils.parse_time('10:00:00'), self.timeutils.parse_time('12:00:00'))]

    #     self.assertEqual(time_bands, expected_time_bands)
    #     self.assertEqual(start_end_range, expected_start_end_range)


    def test_getd1d2StrategyValue(self):
        # Mock the database query result
        pd.read_sql = MagicMock(return_value=pd.DataFrame({'criteria': ['CriteriaA'], 'time_range': ['Morning'], 'offset': [1], 'strategy': ['StrategyA']}))

        # Set input parameters
        strategy = 'StrategyA'
        dplfBand = 'BandB'
        ndoBand = 'BandA'
        channel = 'ChannelA'

        # Call the method to be tested
        result = self.rm_db_helper.getd1d2StrategyValue(strategy, dplfBand, ndoBand, channel)

        # Assert the expected result
        expected_result = pd.DataFrame({'criteria': ['CriteriaA'], 'time_range': ['Morning'], 'offset': [1], 'strategy': ['StrategyA']})
        pd.testing.assert_frame_equal(result, expected_result)

    def test_getTimeRange(self):
        # Mock the database query result
        pd.read_sql = MagicMock(return_value=pd.DataFrame({'type': ['TypeA'], 'start': ['08:00'], 'end': ['10:00']}))

        # Set input parameters
        time_range = 'Morning'

        # Call the method to be tested
        result = self.rm_db_helper.getTimeRange(time_range)

        # Assert the expected result
        expected_result = pd.DataFrame({'type': ['TypeA'], 'start': ['08:00'], 'end': ['10:00']})
        pd.testing.assert_frame_equal(result, expected_result)
        
    def test_getTimeReferenceValue(self):
        # Mock the result of the SQL query
        expected_result = pd.DataFrame({
            'ar_start': [datetime(2022, 1, 1, 8, 0, 0)],
            'ar_end': [datetime(2022, 1, 1, 18, 0, 0)]
        })
        pd.read_sql = MagicMock(return_value=expected_result)

        # Call the method with some parameters
        currParams = MagicMock(origin='ABC')
        result = self.rm_db_helper.getTimeReferenceValue(currParams)

        # Check if the method returns the expected result
        
        pd.testing.assert_frame_equal(result, expected_result)

   
    # def test_getMarketFareRange(self):
    #     # Mocking necessary dependencies
        
    #     channel = 'some_channel'
    #     curr_params = MagicMock(origin= 'ABC', destin= 'XYZ', carr_exclusion= 'EXCL')
    #     curr_flt_date_str = '02/13/2024'
    #     day_span = 2
    #     start_time = '08:00:00'
    #     end_time = '18:00:00'
    #     day3_flag = False

    #     # Call the method and assert the expected output
    #     result = self.rm_db_helper.getMarketFareRange(channel, curr_params, curr_flt_date_str, day_span, start_time, end_time, day3_flag)
    #     self.assertIsNotNone(result)
    #     self.assertIsInstance(result, pd.DataFrame) 
    
    def test_getCurrentTime(self):
        # Mocking the SQL query result
        mock_query_result = pd.DataFrame({"runTime": ["12:30"]})
        pd.read_sql = MagicMock(return_value=mock_query_result)

        # Setting up test data
        currentTimeFlag = True
        currParams = MagicMock(flight_number="ABC123", origin="ORI", destin="DEST")
        currFltDate = "02/13/2024"
        starttime = datetime(2024, 2, 13, 10, 30, tzinfo=timezone.utc)

        # Calling the method
        result = self.rm_db_helper.getCurrentTime(currentTimeFlag, currParams, currFltDate, starttime)

        # Assertions
        self.assertEqual(result, "12:30")
        
    def test_getValueWithOffset(self):
        # Arrange
        origin = 'OriginA'
        destin = 'DestinB'
        anchorFare = 100.0
        offset = 50
        channel = 'B2C'
        openResult = {}

        # Mock the return value of pd.read_sql
        pd.read_sql = MagicMock(return_value=pd.DataFrame({'RBD': ['A', 'B'], 'Total': [75.0, 80.0]}))

        # Act
        result, rbddata = self.rm_db_helper.getValueWithOffset(origin, destin, anchorFare, offset, channel, openResult)

        # Assert
        
        self.assertEqual(result, openResult)
        self.assertIsNotNone(rbddata)
        self.assertEqual(rbddata.shape[0], 2)
        
   
    def test_fetchExtremePublishedFare(self):
      
        # Mock the read_sql method to return a specific DataFrame
        mock_data = pd.DataFrame({'RBD': ['A', 'B', 'C'], 'Total': [150, 120, 90]})
        
        pd.read_sql = MagicMock(return_value=mock_data)

        # Test case 1: Check if the method returns data for valid parameters
        origin = "Origin1"
        destin = "Destin1"
        channel = "B2C"
        type = "LOW"
        backstop = 50
        defFareData = self.rm_db_helper.fetchExtremePublishedFare(origin, destin, channel, type, backstop)
        self.assertIsNotNone(defFareData)
        self.assertEqual(defFareData.shape, mock_data.shape)

    
    def test_getHourlyOwnFareData(self):

        # Set up the mock return value for pd.read_sql
        mock_data = pd.DataFrame({'value': [10, 20, 30]})
        pd.read_sql = MagicMock(return_value=mock_data)# Adjust this based on your expected data
        

        # Call the method with mock data
        ndo = 42  # replace with actual value
        hour = 10  # replace with actual value
        result = self.rm_db_helper.getHourlyOwnFareData(ndo, hour)

        # Assertions
        self.assertIsNotNone(result)
        pd.testing.assert_frame_equal(result, mock_data)
        # Add more assertions based on your expected behavior
        
    def test_getGridOwnFareData(self):

        # Set up the mock return value for pd.read_sql
        mock_data = pd.DataFrame({'value': [10, 20, 30]})
        pd.read_sql = MagicMock(return_value=mock_data)# Adjust this based on your expected data
        

        # Call the method with mock data
        ndoband = 42  # replace with actual value
        dlfband = 10  # replace with actual value
        channel="B2c"
        result = self.rm_db_helper.getGridOwnFareData(ndoband, dlfband,channel)

        # Assertions
        self.assertIsNotNone(result)
        pd.testing.assert_frame_equal(result, mock_data)
        # Add more assertions based on your expected behavior
        
    def test_getArea(self):
        # Set up the mock return value for pd.read_sql
        mock_data = {'sum': [100]}  # Adjust this based on your expected data
        pd.read_sql = MagicMock(return_value=mock_data)
        # Call the method with mock data
        currentHour = 5  # replace with actual value
        result = self.rm_db_helper.getArea(currentHour)

        # Assertions
        self.assertIsNotNone(result)

    # Add more test methods for other functions in RMDatabaseHelper

if __name__ == '__main__':
    unittest.main()
