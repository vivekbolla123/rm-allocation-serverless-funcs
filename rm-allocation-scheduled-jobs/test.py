import configsettings
from redis import Redis

def test():
    KEY_B2B_PENDING_COUNT = 'b2b_pending_count_'
    KEY_B2C_PENDING_COUNT = 'b2c_pending_count_'
    KEY_B2B_READY_COUNT = 'b2b_ready_count_'
    KEY_B2C_READY_COUNT = 'b2c_ready_count_'
    KEY_B2B_DONE_COUNT = 'b2b_done_count_'
    KEY_B2C_DONE_COUNT = 'b2c_done_count_'
    CACHE_ENDPOINT = configsettings.RM_CACHE_ENDPOINT
    CACHE_PORT = configsettings.RM_CACHE_PORT
    CACHE_USERNAME = configsettings.RM_CACHE_USERNAME
    CACHE_PASSWORD = configsettings.RM_CACHE_PASSWORD
    
    cache_client = Redis(host=CACHE_ENDPOINT, port=CACHE_PORT, username=CACHE_USERNAME, password=CACHE_PASSWORD)
    print("Connected")
    run_id="857980f7-4551-4d6f-b99f-3d145cee0299"
    b2bPendingCount = cache_client.get(KEY_B2B_PENDING_COUNT + run_id)
    b2cPendingCount = cache_client.get(KEY_B2C_PENDING_COUNT + run_id)
    b2bDoneCount = cache_client.get(KEY_B2B_DONE_COUNT + run_id)
    b2cDoneCount = cache_client.get(KEY_B2C_DONE_COUNT + run_id)
    is_complete = int(b2bDoneCount) + int(b2cDoneCount) >= int(b2bPendingCount) + int(b2cPendingCount)
    print(is_complete)
    if is_complete:
        print("success")

test()