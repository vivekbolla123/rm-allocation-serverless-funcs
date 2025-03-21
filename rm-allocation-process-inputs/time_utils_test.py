import unittest
from datetime import datetime, date,timedelta
from utilities.time_utils import TimeUtils
class InputSourceMock:
    def __init__(self, currDate=None, currDateTime=None):
        self.currDate = currDate
        self.currDateTime = currDateTime

class TestTimeUtils(unittest.TestCase):

    def setUp(self):
        # Initialize TimeUtils object with a sample input source
        input_source_mock = InputSourceMock(currDate=date(2022, 1, 1), currDateTime=datetime(2022, 1, 1, 12, 0, 0))
        self.time_utils = TimeUtils(inputsource=input_source_mock)

    def test_parse_time(self):
        result = self.time_utils.parse_time('14:30')
        self.assertEqual(result.hour, 14)
        self.assertEqual(result.minute, 30)

    def test_getCurrentDate_with_input(self):
        # Test when currDate is provided in the input source
        result = self.time_utils.getCurrentDate()
        self.assertEqual(result, date(2022, 1, 1))

    def test_getCurrentDate_without_input(self):
        # Test when currDate is not provided in the input source
        input_source_mock = InputSourceMock()
        time_utils = TimeUtils(inputsource=input_source_mock)
        result = time_utils.getCurrentDate()
        self.assertEqual(result.strftime('%Y-%m-%d'), (datetime.utcnow()+timedelta(hours=5, minutes=30)).strftime('%Y-%m-%d'))

    def test_getCurrentDateTime_with_input(self):
        # Test when currDateTime is provided in the input source
        result = self.time_utils.getCurrentDateTime()
        self.assertEqual(result, datetime(2022, 1, 1, 12, 0, 0))


if __name__ == '__main__':
    unittest.main()
