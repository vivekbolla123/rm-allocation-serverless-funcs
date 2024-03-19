import unittest
from unittest.mock import Mock
from models.input_source import InputSource
# Assuming RedisHelper is in a module named "redis_helper"
from helper.redis_helper import RedisHelper

class TestRedisHelper(unittest.TestCase):

    def setUp(self):
        # Mocking the cache client to avoid actual interactions with the cache
        self.mock_cache_client = Mock()

        # Creating a sample input source for testing
        self.input_source = InputSource({'inputSource':{'Curves': 'DB', 'Market_list': 'DB', 'Market_fares': 'DB', 'Fares': 'DB', 'inputs_status': 'DB', 'bookload': 'DB', 'ndo_bands': 'DB', 'dplf_bands': 'DB', 'd1_d2_strategies': 'DB', 'lastSellingFare': 'DB', 'overbooking': 'DB'},'logFiles':{'run_summary':'DB','allocation_run_audit':'DB'}})

        # Creating a RedisHelper instance for testing
        self.redis_helper = RedisHelper(runid='test_run_id', cache_client=self.mock_cache_client, input_source=self.input_source)

    def test_updateRunAudit(self):
        # Mocking the cache_client.incr method
        self.mock_cache_client.incr = Mock()

        # Calling the method to test
        self.redis_helper.updateRunAudit()

        # Asserting that cache_client.incr was called with the expected arguments
        self.mock_cache_client.incr.assert_has_calls([
    unittest.mock.call('b2b_done_count_test_run_id'),
    unittest.mock.call('b2c_done_count_test_run_id')
], any_order=False)


    def test_setAllRunStatus2Ready(self):
        # Mocking the cache_client.get and cache_client.set methods
        self.mock_cache_client.get = Mock(side_effect=[10, 20])  # Mocking return values for B2B and B2C counts
        self.mock_cache_client.set = Mock()

        # Calling the method to test
        self.redis_helper.setAllRunStatus2Ready()

        # Asserting that cache_client.get and cache_client.set were called with the expected arguments
        self.mock_cache_client.get.assert_has_calls([
    unittest.mock.call('b2b_pending_count_test_run_id'),
    unittest.mock.call('b2c_pending_count_test_run_id')
], any_order=False)
        self.mock_cache_client.set.assert_has_calls([
    unittest.mock.call('b2b_ready_count_test_run_id',10),
    unittest.mock.call('b2c_ready_count_test_run_id',20)
], any_order=False)
       

if __name__ == '__main__':
    unittest.main()
