"""
Mock constants_utils for testing.
This provides a simplified version without database dependencies.
"""

class ConstantsUtils:
    """Mock version of ConstantsUtils that doesn't require database connection."""
    
    def __init__(self, db=None):
        # Mock all the constants with default values
        self.MIN_D3_D4_VALUE = 2
        self.LINEAR_JUMP_VALUE = 5
        self.PLF_CURVE_VALUE = 0.80
        self.B2B_FARE_PRICE_COMPARISON = 0.9
        self.COVERAGE_PARAMETER = 3
        self.AU_COLUMN_LENGTH = 118
        self.B2B_RDB_COUNT = 20
        self.LOWEST_B2B_RBD_VALUE = 'ZJ'
        self.LOWEST_B2C_RBD_VALUE = 'V9'
        self.HIGHEST_B2B_RBD_VALUE = 'Z0'
        self.HIGHEST_B2C_RBD_VALUE = 'Y'
        self.OWN_FARE_DELTA_B = 30
        self.OWN_FARE_DELTA_A = 15
        self.OVERBOOKING_LF = 95
        self.OVERBOOKING_START_NDO = 0
        self.OVERBOOKING_END_NDO = 1
        self.OVERBOOKING_PROB = 0.75
        self.OVERBOOKING_FARE_LF = 96
        self.OVERBOOKING_FARE_PERCENT_VALUE = 0.1
        self.OVERBOOKING_FARE_ABSOLUTE_VALUE = 1000
        self.OVERBOOKING_MANUAL_LF = 100
        self.OVERBOOKING_MAX_COUNT = 2
        self.OVERBOOKING_CAP = 4
        self.S_CLASS_PLF_THRESHOLD = 75
        self.END_OF_DAY_LF_FACTOR = 0.95
        self.AU_COLUMN_LENGTH_CONNECTIONS = 84
        self.MARKET_FARES_THRESHOLD = '[ {"startNdo": 0, "endNdo": 7, "value": 2} ]'
        self.GROUP_QUOTATION = '{ "Q2": "GJ", "Q4": "GI" }'
        self.TBF_RBD = "TF,TE,TD,TC,TB,TY"
        self.CUG_RBD = "WA,WB,WC,WD,WE"
        self.CUG_B2C_OPENING_THRESHOLD = 0.3
        self.CUG_SEATS_PERCENT = 0.25
        self.LAST_SOLD_FARE_THRESHOLD = 72
        self.LAST_N_AVG_FARE = 7
        self.MFMIN0 = 0
        self.MFMIN1 = 1
        self.CHECK_THRESHOLD_PROCESS_INPUTS = 5
        self.GROUP_DIFF_FARE = 5000
        self.TBF_DIFF_FARE = 2000
        self.PROF_OPEN_COUNT = 10
        self.B2B_DISCOUNT_MAP = '{ "0": "450", "1": "350" }'
        self.TBF_SEATS = 20
        self.ADHOC_BATCH_SIZE = 4000
        self.CHECK_THRESHOLD_FETCH_INPUTS = 5
        self.TIME_BEFORE_DEPARTURE = 30
        self.THRESHOLD_TIME = 12
        self.PF_UPSELL_VALUE = 47
        self.CHECK_THRESHOLD_UPDATE_RBD = 20
        self.BOOKINGS_THRESHOLD = 10
        self.SOFTBLOCK_RBDS = '["G8","W9","W8"]'
        self.NAVODS_ALERT_SECTOR_LIST = '["BOMDEL1127"]'
        self.NAVODS_ALERT_MAX_NDO = 3
        self.TBF_ORG_CODE = "QPBDQ0903T"
        self.TBF_OPEN_SEATS = 8
        self.DEFAULT_MARKET_LIST = '{"Origin": "ABC"}'
        self.DEFAULT_MARKET_LIST_DAYS = 30
        self.PRICING_DUF_SECTORS = {"DEL": "66", "BOM": "89"}
        self.PRICING_RCS_SECTORS = ["GAU","IXA"]
        self.PRICING_CARR_EXCL = "9I,IC"
        self.PRICING_GST_SECTORS = ["IXB","GAU"]
        self.DOMESTIC_PLF_THRESHOLD = 95
        self.INTERNATIONAL_PLF_THRESHOLD = 80
        self.CUG_DISCOUNT = 5
        self.SPOILAGE_REDUCTION_PLF_THRESHOLD = 100
        self.IMP_FORECAST_DEDUCTION = 2
        self.TOLERANCE_MAP = {"4": -1, "3": 10}
        self.B2B_FACTOR_MAP = {"4": 0, "3": 0}
        self.DEFAULT_D3_VALUE = 4
        self.D3_MIN_RULES = {"L": {"forecast_start": -9999}}
        self.D3_MAX_RULES = {"L": {"forecast_start": -9999}}
        self.RM_ALERT_MAIL_ID = 'test@example.com'
        self.CLOSED_RM_ALERT_MAIL_ID = 'test@example.com'
        self.UP_B2C = 5
        self.UP_B2B = 5
        self.MARKET_FLAG = True
        self.B2B_THRESHOLD = 100
        
        print("[TEST MODE] Using mock ConstantsUtils")
    
    def generateConstantValues(self):
        """Mock method - no database operations needed in test mode."""
        pass
    
    def get_parameters(self):
        """Mock method - returns empty DataFrame."""
        import pandas as pd
        return pd.DataFrame()