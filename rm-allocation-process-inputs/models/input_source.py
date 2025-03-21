from Constants import *

class InputSource:
    def __init__(self, input_data):
        self.debugMode = input_data.get("debugMode", False)
        self.isUpdateNavitaire = input_data.get("isUpdateNavitaire", False)
        self.currDate = input_data.get("currDate",None)
        self.currDateTime = input_data.get("currDateTime", None)
        self.MARKET_LIST_QUEUE_URL = input_data.get("MARKET_LIST_QUEUE_URL", None)
        self.RBD_QUEUE_URL = input_data.get("RBD_QUEUE_URL", None)
        self.NAV_DB_NAME = input_data.get("NAV_DB_NAME", None)
        self.NAV_DB_DW_NAME = input_data.get("NAV_DB_DW_NAME", None)
        self.NAV_DB_WB_NAME = input_data.get("NAV_DB_WB_NAME", None)
        self.NAV_BASE_API_URL = input_data.get("NAV_BASE_API_URL", None)
        self.NAV_OCP_SUBSCRIPTION_KEY = input_data.get("NAV_OCP_SUBSCRIPTION_KEY", None)
        self.NAV_USERNAME = input_data.get("NAV_USERNAME", None)
        self.NAV_PWD = input_data.get("NAV_PWD", None)
        self.NAV_DOMAIN = input_data.get("NAV_DOMAIN", None)
        self.input_source = input_data.get(CONST_INPUT_SOURCE, TESTING_INPUT)
        self.logs = input_data.get(CONST_LOGS, TESTING_LOGS)
        self.output = input_data.get(CONST_OUTPUT, TESTING_OUTPUT)
        self.API_KEY = input_data.get("API_KEY", None)
        self.API_URL = input_data.get("API_URL", None)
        self.latestCode = input_data.get("latestCode", False)
        self.currentTime = input_data.get("currentTime", True)

