"""
Mock configsettings for testing.
This replaces the real configsettings.py to avoid AWS dependencies during testing.
"""

# Mock database settings for testing
NAV_DB_NAME = "[TEST_NAV]."
NAV_DB_DW_NAME = "[TEST_DW]."
NAV_DB_WB_NAME = "[TEST_WB]."
NAV_DB_HOST = "localhost"
NAV_DB_USER = "test_user"
NAV_DB_PASSWORD = "test_password"

# Mock MongoDB settings
MONGO_USERNAME = "test_mongo_user"
MONGO_PASSWORD = "test_mongo_password"
MONGO_URL = "localhost"
MONGO_AUTH_SOURCE = "admin"

# Mock ESB settings
ESB_DB_USERNAME = "test_esb_user"
ESB_DB_PASSWORD = "test_esb_password"
ESB_DB_WR_APPUSER_URL = "localhost"
ESB_NAV_SCHEMA_NAME = "TEST_ESB_SCHEMA"

# Other mock settings
RM_DB_SCHEMA_NAME = "TEST_RM_SCHEMA"
env = "test"

print("[TEST MODE] Using mock configsettings")