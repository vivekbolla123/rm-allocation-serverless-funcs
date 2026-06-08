import json
import logging
import pandas as pd
from Constants import *

class ConstantsUtils:
    def __init__(self, conn, master_conn=None):
        self.MIN_D3_D4_VALUE = MIN_D3_D4_VALUE
        self.LINEAR_JUMP_VALUE = LINEAR_JUMP_VALUE
        self.PLF_CURVE_VALUE = PLF_CURVE_VALUE
        self.B2B_FARE_PRICE_COMPARISON = B2B_FARE_PRICE_COMPARISON
        self.COVERAGE_PARAMETER = COVERAGE_PARAMETER
        self.AU_COLUMN_LENGTH = AU_COLUMN_LENGTH
        self.B2B_RDB_COUNT = B2B_RDB_COUNT
        self.LOWEST_B2B_RBD_VALUE = LOWEST_B2B_RBD_VALUE
        self.LOWEST_B2C_RBD_VALUE = LOWEST_B2C_RBD_VALUE
        self.HIGHEST_B2B_RBD_VALUE = HIGHEST_B2B_RBD_VALUE
        self.HIGHEST_B2C_RBD_VALUE = HIGHEST_B2C_RBD_VALUE
        self.OWN_FARE_DELTA_B = OWN_FARE_DELTA_B
        self.OWN_FARE_DELTA_A = OWN_FARE_DELTA_A
        self.OVERBOOKING_LF = OVERBOOKING_LF
        self.OVERBOOKING_START_NDO = OVERBOOKING_START_NDO
        self.OVERBOOKING_END_NDO = OVERBOOKING_END_NDO
        self.OVERBOOKING_PROB = OVERBOOKING_PROB
        self.OVERBOOKING_FARE_LF = OVERBOOKING_FARE_LF
        self.OVERBOOKING_FARE_PERCENT_VALUE = OVERBOOKING_FARE_PERCENT_VALUE
        self.OVERBOOKING_FARE_ABSOLUTE_VALUE = OVERBOOKING_FARE_ABSOLUTE_VALUE
        self.OVERBOOKING_MANUAL_LF = OVERBOOKING_MANUAL_LF
        self.OVERBOOKING_MAX_COUNT = OVERBOOKING_MAX_COUNT
        self.OVERBOOKING_CAP = OVERBOOKING_CAP
        self.S_CLASS_PLF_THRESHOLD = S_CLASS_PLF_THRESHOLD
        self.END_OF_DAY_LF_FACTOR = END_OF_DAY_LF_FACTOR
        self.AU_COLUMN_LENGTH_CONNECTIONS=AU_COLUMN_LENGTH_CONNECTIONS
        self.MARKET_FARES_THRESHOLD=MARKET_FARES_THRESHOLD
        self.GROUP_QUOTATION = GROUP_QUOTATION
        self.TBF_RBD = TBF_RBD
        self.CUG_RBD = CUG_RBD
        self.CUG_B2C_OPENING_THRESHOLD = CUG_B2C_OPENING_THRESHOLD
        self.CUG_SEATS_PERCENT = CUG_SEATS_PERCENT
        self.LAST_SOLD_FARE_THRESHOLD = LAST_SOLD_FARE_THRESHOLD
        self.LAST_N_AVG_FARE = LAST_N_AVG_FARE
        self.MFMIN0 = MFMIN0
        self.MFMIN1 = MFMIN1
        self.CHECK_THRESHOLD_PROCESS_INPUTS = CHECK_THRESHOLD_PROCESS_INPUTS
        self.GROUP_DIFF_FARE = GROUP_DIFF_FARE
        self.TBF_DIFF_FARE = TBF_DIFF_FARE
        self.PROF_OPEN_COUNT = PROF_OPEN_COUNT
        self.B2B_DISCOUNT_MAP = B2B_DISCOUNT_MAP
        self.TBF_SEATS = TBF_SEATS
        self.ADHOC_BATCH_SIZE = ADHOC_BATCH_SIZE
        self.CHECK_THRESHOLD_FETCH_INPUTS = ADHOC_BATCH_SIZE
        self.TIME_BEFORE_DEPARTURE = ADHOC_BATCH_SIZE
        self.PREFETCH_CHUNK_SIZE = PREFETCH_CHUNK_SIZE
        self.THRESHOLD_TIME = THRESHOLD_TIME
        self.PF_UPSELL_VALUE = PF_UPSELL_VALUE
        self.CHECK_THRESHOLD_UPDATE_RBD = CHECK_THRESHOLD_UPDATE_RBD
        self.BOOKINGS_THRESHOLD = BOOKINGS_THRESHOLD
        self.SOFTBLOCK_RBDS = SOFTBLOCK_RBDS
        self.NAVODS_ALERT_SECTOR_LIST = NAVODS_ALERT_SECTOR_LIST
        self.NAVODS_ALERT_MAX_NDO = NAVODS_ALERT_MAX_NDO
        self.TBF_ORG_CODE = TBF_ORG_CODE
        self.TBF_OPEN_SEATS = TBF_OPEN_SEATS
        self.DEFAULT_MARKET_LIST = DEFAULT_MARKET_LIST
        self.DEFAULT_MARKET_LIST_DAYS = DEFAULT_MARKET_LIST_DAYS
        self.PRICING_DUF_SECTORS = PRICING_DUF_SECTORS
        self.PRICING_RCS_SECTORS = PRICING_RCS_SECTORS
        self.PRICING_CARR_EXCL = PRICING_CARR_EXCL
        self.CONNECTION_CARRIER_INCLUSION = json.loads(CONNECTION_CARRIER_INCLUSION)
        self.PRICING_GST_SECTORS = PRICING_GST_SECTORS
        self.DOMESTIC_PLF_THRESHOLD = DOMESTIC_PLF_THRESHOLD
        self.INTERNATIONAL_PLF_THRESHOLD = INTERNATIONAL_PLF_THRESHOLD
        self.CUG_DISCOUNT = CUG_DISCOUNT
        self.SPOILAGE_REDUCTION_PLF_THRESHOLD = 100
        self.IMP_FORECAST_DEDUCTION = IMP_FORECAST_DEDUCTION
        self.TOLERANCE_MAP = json.loads(TOLERANCE_MAP)
        self.B2B_FACTOR_MAP = json.loads(B2B_FACTOR_MAP)        
        self.AF_PLF_FORECAST_CAP = AF_PLF_FORECAST_CAP
        self.AF_PLF_GAIN_MAX_ZERO = AF_PLF_GAIN_MAX_ZERO
        self.AF_CONSTANT_BAND_2 = AF_CONSTANT_BAND_2
        self.AF_CONSTANT_BAND_3 = AF_CONSTANT_BAND_3
        self.AF_PROFILE_FARE_PLF_THRESHOLD = AF_PROFILE_FARE_PLF_THRESHOLD
        self.AF_PROFILE_FARE_MULTIPLIER = AF_PROFILE_FARE_MULTIPLIER
        self.AF_BAND_1_MULTIPLIERS = json.loads(AF_BAND_1_MULTIPLIERS)
        self.AF_BAND_2_MULTIPLIERS = json.loads(AF_BAND_2_MULTIPLIERS)
        self.DEFAULT_D3_VALUE = DEFAULT_D3_VALUE
        self.D3_MIN_RULES = json.loads(D3_MIN_RULES)
        self.D3_MAX_RULES = json.loads(D3_MAX_RULES)
        self.RM_ALERT_MAIL_ID = RM_ALERT_MAIL_ID
        self.CLOSED_RM_ALERT_MAIL_ID = CLOSED_RM_ALERT_MAIL_ID
        self.UP_B2C = UP_B2C
        self.UP_B2B = UP_B2B
        self.MARKET_FLAG_DOMESTIC = MARKET_FLAG_DOMESTIC
        self.MARKET_FLAG_INTERNATIONAL = MARKET_FLAG_INTERNATIONAL
        self.MARKET_FARE_MAX_THRESHOLD_DOMESTIC_B2B = MARKET_FARE_MAX_THRESHOLD_DOMESTIC_B2B
        self.MARKET_FARE_MAX_THRESHOLD_INTERNATIONAL_B2B = MARKET_FARE_MAX_THRESHOLD_INTERNATIONAL_B2B
        self.MARKET_FARE_MAX_THRESHOLD_DOMESTIC_B2C = MARKET_FARE_MAX_THRESHOLD_DOMESTIC_B2C
        self.MARKET_FARE_MAX_THRESHOLD_INTERNATIONAL_B2C = MARKET_FARE_MAX_THRESHOLD_INTERNATIONAL_B2C
        self.MARKET_FARE_MIN_THRESHOLD_DOMESTIC_B2B = MARKET_FARE_MIN_THRESHOLD_DOMESTIC_B2B
        self.MARKET_FARE_MIN_THRESHOLD_INTERNATIONAL_B2B = MARKET_FARE_MIN_THRESHOLD_INTERNATIONAL_B2B
        self.MARKET_FARE_MIN_THRESHOLD_DOMESTIC_B2C = MARKET_FARE_MIN_THRESHOLD_DOMESTIC_B2C
        self.MARKET_FARE_MIN_THRESHOLD_INTERNATIONAL_B2C = MARKET_FARE_MIN_THRESHOLD_INTERNATIONAL_B2C
        self.MARKET_FARE_MAX_THRESHOLD_CONNECTION_B2B = MARKET_FARE_MAX_THRESHOLD_CONNECTION_B2B
        self.MARKET_FARE_MAX_THRESHOLD_CONNECTION_B2C = MARKET_FARE_MAX_THRESHOLD_CONNECTION_B2C
        self.MARKET_FARE_MIN_THRESHOLD_CONNECTION_B2B = MARKET_FARE_MIN_THRESHOLD_CONNECTION_B2B
        self.MARKET_FARE_MIN_THRESHOLD_CONNECTION_B2C = MARKET_FARE_MIN_THRESHOLD_CONNECTION_B2C
        self.BOOKINGS_FARE_MAX_THRESHOLD_DOMESTIC_B2B = BOOKINGS_FARE_MAX_THRESHOLD_DOMESTIC_B2B
        self.BOOKINGS_FARE_MAX_THRESHOLD_INTERNATIONAL_B2B = BOOKINGS_FARE_MAX_THRESHOLD_INTERNATIONAL_B2B
        self.BOOKINGS_FARE_MAX_THRESHOLD_DOMESTIC_B2C = BOOKINGS_FARE_MAX_THRESHOLD_DOMESTIC_B2C
        self.BOOKINGS_FARE_MAX_THRESHOLD_INTERNATIONAL_B2C = BOOKINGS_FARE_MAX_THRESHOLD_INTERNATIONAL_B2C
        self.FARECAP_DISTANCE_CONFIG = self._parse_farecap_distance_config(FARECAP_DISTANCE_CONFIG)
        self.YQ_DISTANCE_CONFIG = self._parse_farecap_distance_config(YQ_DISTANCE_CONFIG)
        self.CURRENCY_ROUND_OFF_LIST = CURRENCY_ROUND_OFF_LIST
        self.GDS_RBD_VALUES = GDS_RBD_VALUES
        self.GRACE_PERIOD_MINUTES = GRACE_PERIOD_MINUTES
        self.WINDOW_MINUTES = WINDOW_MINUTES
        self.EQT_RECIPIENTS = EQT_RECIPIENTS
        
        # Phase 2 Constants
        self.INTERNATIONAL_BOOKLOAD_THRESHOLDS = self._parse_intl_thresholds(INTERNATIONAL_BOOKLOAD_THRESHOLDS)
        self.YIELD_FACTOR = YIELD_FACTOR
        self.MARKET_FARE_MIN_YIELD = MARKET_FARE_MIN_YIELD
        self.DIRECT_FLIGHT_UPSELL_PERCENT = DIRECT_FLIGHT_UPSELL_PERCENT
        self.DIRECT_FLIGHT_UPSELL_AMOUNT = DIRECT_FLIGHT_UPSELL_AMOUNT
        self.RBD_VALUES = RBD_VALUES
        self.B2B_RBD_VALUES = B2B_RBD_VALUES
        self.B2C_RBD_VALUES = B2C_RBD_VALUES
        self.GROUP_RBD_VALUES = GROUP_RBD_VALUES
        self.DISTRESS_RBD_VALUES = DISTRESS_RBD_VALUES
        self.B2B_THRESHOLD = B2B_THRESHOLD
        self.B2B_THRESHOLD_INTERNATIONAL = B2B_THRESHOLD_INTERNATIONAL
        self.B2C_DISTINCT_FARES_COUNT = B2C_DISTINCT_FARES_COUNT
        self.MARKET_FARE_FORECAST_THRESHOLD = MARKET_FARE_FORECAST_THRESHOLD
        self.ACTIVE_SECTORS_DEFAULT_FIELDS = ACTIVE_SECTORS_DEFAULT_FIELDS
        self.CWT_UPSELL = CWT_UPSELL
        self.GROUP_SCALING_FACTOR = GROUP_SCALING_FACTOR
        self.GROUP_AU_BATCH = GROUP_AU_BATCH
        self.GROUP_IMP_FORECAST_REDUCTION = GROUP_IMP_FORECAST_REDUCTION
        self.W_PLF = W_PLF
        self.W_AF = W_AF
        self.W_ANCHOR = W_ANCHOR
        self.MOVEMENT_GAIN = MOVEMENT_GAIN
        self.MOVEMENT_FLOOR_PCT = MOVEMENT_FLOOR_PCT
        self.MOVEMENT_CEIL_PCT = MOVEMENT_CEIL_PCT


        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)
        self.db = conn
        self.logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")
        # Generate constants from database values
        self.generateConstantValues()

        # Calculate rbd counts and column lengths based on RBD values
        self.B2B_RDB_COUNT = len(self.B2B_RBD_VALUES.split(','))
        self.B2C_RDB_COUNT = len(self.B2C_RBD_VALUES.split(','))
        self.AU_COLUMN_LENGTH = self.B2B_RDB_COUNT + self.B2C_RDB_COUNT + len(self.GROUP_RBD_VALUES.split(',')) + len(self.DISTRESS_RBD_VALUES.split(',')) + len(self.TBF_RBD.split(',')) + len(self.CUG_RBD.split(',')) + len(json.loads(self.SOFTBLOCK_RBDS))
        self.AU_COLUMN_LENGTH_CONNECTIONS = self.B2B_RDB_COUNT + self.B2C_RDB_COUNT

    def generateConstantValues(self):
        parameterValue = self.get_parameters()
        
        for index, val in parameterValue.iterrows():
            parameter_key = val['parameterKey']
            parameter_value = val['parameterValue']

            if parameter_key == MIN_D3_D4_VALUE_KEY:
                self.MIN_D3_D4_VALUE = float(parameter_value)
            elif parameter_key == LINEAR_JUMP_VALUE_KEY:
                self.LINEAR_JUMP_VALUE = float(parameter_value)
            elif parameter_key == PLF_CURVE_VALUE_KEY:
                self.PLF_CURVE_VALUE = float(parameter_value)
            elif parameter_key == B2B_FARE_PRICE_COMPARISON_KEY:
                self.B2B_FARE_PRICE_COMPARISON = float(parameter_value)
            elif parameter_key == COVERAGE_PARAMETER_KEY:
                self.COVERAGE_PARAMETER = float(parameter_value)
            elif parameter_key == AU_COLUMN_LENGTH_KEY:
                self.AU_COLUMN_LENGTH = int(parameter_value)
            elif parameter_key == B2B_RDB_COUNT_KEY:
                self.B2B_RDB_COUNT = float(parameter_value)
            elif parameter_key == LOWEST_B2B_RBD_VALUE_KEY:
                self.LOWEST_B2B_RBD_VALUE = str(parameter_value)
            elif parameter_key == LOWEST_B2C_RBD_VALUE_KEY:
                self.LOWEST_B2C_RBD_VALUE = str(parameter_value)
            elif parameter_key == HIGHEST_B2B_RBD_VALUE_KEY:
                self.HIGHEST_B2B_RBD_VALUE = str(parameter_value)
            elif parameter_key == HIGHEST_B2C_RBD_VALUE_KEY:
                self.HIGHEST_B2C_RBD_VALUE = str(parameter_value)
            elif parameter_key == OWN_FARE_DELTA_A_KEY:
                self.OWN_FARE_DELTA_A = float(parameter_value)
            elif parameter_key == OWN_FARE_DELTA_B_KEY:
                self.OWN_FARE_DELTA_B = float(parameter_value)
            elif parameter_key == OVERBOOKING_LF_KEY:
                self.OVERBOOKING_LF = float(parameter_value)
            elif parameter_key == OVERBOOKING_START_NDO_KEY:
                self.OVERBOOKING_START_NDO = int(parameter_value)
            elif parameter_key == OVERBOOKING_END_NDO_KEY:
                self.OVERBOOKING_END_NDO = int(parameter_value)
            elif parameter_key == OVERBOOKING_PROB_KEY:
                self.OVERBOOKING_PROB = float(parameter_value)
            elif parameter_key == S_CLASS_PLF_THRESHOLD_KEY:
                self.S_CLASS_PLF_THRESHOLD = float(parameter_value)
            elif parameter_key == END_OF_DAY_LF_FACTOR_KEY:
                self.END_OF_DAY_LF_FACTOR = float(parameter_value)
            elif parameter_key == AU_COLUMN_LENGTH_CONNECTIONS_KEY:
                self.AU_COLUMN_LENGTH_CONNECTIONS = int(parameter_value)
            elif parameter_key == OVERBOOKING_FARE_LF_KEY:
                self.OVERBOOKING_FARE_LF = float(parameter_value)
            elif parameter_key == OVERBOOKING_FARE_PERCENT_VALUE_KEY:
                self.OVERBOOKING_FARE_PERCENT_VALUE = float(parameter_value)
            elif parameter_key == OVERBOOKING_FARE_ABSOLUTE_VALUE_KEY:
                self.OVERBOOKING_FARE_ABSOLUTE_VALUE = float(parameter_value) 
            elif parameter_key == MARKET_FARES_THRESHOLD_KEY:
                self.MARKET_FARES_THRESHOLD = parameter_value
            elif parameter_key == GROUP_QUOTATION_KEY:
                self.GROUP_QUOTATION = parameter_value
            elif parameter_key == TBF_RBD_KEY:
                self.TBF_RBD = parameter_value
            elif parameter_key == CUG_RBD_KEY:
                self.CUG_RBD = parameter_value
            elif parameter_key == CUG_B2C_OPENING_THRESHOLD_KEY:
                self.CUG_B2C_OPENING_THRESHOLD = float(parameter_value)
            elif parameter_key == CUG_SEATS_PERCENT_KEY:
                self.CUG_SEATS_PERCENT = float(parameter_value)
            elif parameter_key == LAST_SOLD_FARE_THRESHOLD_KEY:
                self.LAST_SOLD_FARE_THRESHOLD = int(parameter_value)
            elif parameter_key == LAST_N_AVG_FARE_KEY:
                self.LAST_N_AVG_FARE = int(parameter_value)
            elif parameter_key == MFMIN0_KEY:
                self.MFMIN0 = int(parameter_value)
            elif parameter_key == MFMIN1_KEY:
                self.MFMIN1 = int(parameter_value)
            elif parameter_key == CHECK_THRESHOLD_PROCESS_INPUTS_KEY:
                self.CHECK_THRESHOLD_PROCESS_INPUTS = int(parameter_value)
            elif parameter_key == GROUP_DIFF_FARE_KEY:
                self.GROUP_DIFF_FARE = int(parameter_value)
            elif parameter_key == TBF_DIFF_FARE_KEY:
                self.TBF_DIFF_FARE = int(parameter_value)
            elif parameter_key == B2B_DISCOUNT_MAP_KEY:
                self.B2B_DISCOUNT_MAP = parameter_value
            elif parameter_key == OVERBOOKING_MANUAL_LF_KEY:
                self.OVERBOOKING_MANUAL_LF = int(parameter_value)
            elif parameter_key == OVERBOOKING_MAX_COUNT_KEY:
                self.OVERBOOKING_MAX_COUNT = int(parameter_value)
            elif parameter_key == OVERBOOKING_CAP_KEY:
                self.OVERBOOKING_CAP = int(parameter_value)
            elif parameter_key == PROF_OPEN_COUNT_KEY:
                self.PROF_OPEN_COUNT = int(parameter_value)
            elif parameter_key == TBF_SEATS_KEY:
                self.TBF_SEATS = int(parameter_value)
            elif parameter_key == ADHOC_BATCH_SIZE_KEY:
                self.ADHOC_BATCH_SIZE = int(parameter_value)
            elif parameter_key == CHECK_THRESHOLD_FETCH_INPUTS_KEY:
                self.CHECK_THRESHOLD_FETCH_INPUTS = int(parameter_value)
            elif parameter_key == TIME_BEFORE_DEPARTURE_KEY:
                self.TIME_BEFORE_DEPARTURE = int(parameter_value)
            elif parameter_key == PREFETCH_CHUNK_SIZE_KEY:
                self.PREFETCH_CHUNK_SIZE = int(parameter_value)
            elif parameter_key == THRESHOLD_TIME_KEY:
                self.THRESHOLD_TIME = int(parameter_value)   
            elif parameter_key == PF_UPSELL_VALUE_KEY:
                self.PF_UPSELL_VALUE = int(parameter_value)
            elif parameter_key == CHECK_THRESHOLD_UPDATE_RBD_KEY:
                self.CHECK_THRESHOLD_UPDATE_RBD = int(parameter_value)
            elif parameter_key == BOOKINGS_THRESHOLD_KEY:
                self.BOOKINGS_THRESHOLD = int(parameter_value)
            elif parameter_key == SOFTBLOCK_RBDS_KEY:
                self.SOFTBLOCK_RBDS = parameter_value
            elif parameter_key == NAVODS_ALERT_SECTOR_LIST_KEY:
                self.NAVODS_ALERT_SECTOR_LIST = parameter_value
            elif parameter_key == NAVODS_ALERT_MAX_NDO_KEY:
                self.NAVODS_ALERT_MAX_NDO = parameter_value
            elif parameter_key == TBF_ORG_CODE_KEY:
                self.TBF_ORG_CODE = parameter_value
            elif parameter_key == TBF_OPEN_SEATS_KEY:
                self.TBF_OPEN_SEATS = int(parameter_value)
            elif parameter_key == DEFAULT_MARKET_LIST_KEY:
                self.DEFAULT_MARKET_LIST = parameter_value
            elif parameter_key == DEFAULT_MARKET_LIST_DAYS_KEY:
                self.DEFAULT_MARKET_LIST_DAYS = int(parameter_value)
            elif parameter_key == PRICING_DUF_SECTORS_KEY:
                self.PRICING_DUF_SECTORS = json.loads(parameter_value)
            elif parameter_key == PRICING_RCS_SECTORS_KEY:
                self.PRICING_RCS_SECTORS = json.loads(parameter_value)
            elif parameter_key == PRICING_CARR_EXCL_KEY:
                self.PRICING_CARR_EXCL = parameter_value
            elif parameter_key == PRICING_GST_SECTORS_KEY:
                self.PRICING_GST_SECTORS = json.loads(parameter_value)
            elif parameter_key == CONNECTION_CARRIER_INCLUSION_KEY:
                self.CONNECTION_CARRIER_INCLUSION = json.loads(parameter_value)
            elif parameter_key == DOMESTIC_PLF_THRESHOLD_KEY:
                self.DOMESTIC_PLF_THRESHOLD = int(parameter_value)
            elif parameter_key == INTERNATIONAL_PLF_THRESHOLD_KEY:
                self.INTERNATIONAL_PLF_THRESHOLD = int(parameter_value)
            elif parameter_key == CUG_DISCOUNT_KEY:
                self.CUG_DISCOUNT = int(parameter_value)
            elif parameter_key == IMP_FORECAST_DEDUCTION_KEY:
                self.IMP_FORECAST_DEDUCTION = int(parameter_value)
            elif parameter_key == TOLERANCE_MAP_KEY:
                self.TOLERANCE_MAP = json.loads(parameter_value)
            elif parameter_key == B2B_FACTOR_MAP_KEY:
                self.B2B_FACTOR_MAP = json.loads(parameter_value)
            elif parameter_key == DEFAULT_D3_VALUE_KEY:
                self.DEFAULT_D3_VALUE = int(parameter_value)
            elif parameter_key == D3_MIN_RULES_KEY:
                self.D3_MIN_RULES = json.loads(parameter_value)
            elif parameter_key == D3_MAX_RULES_KEY:
                self.D3_MAX_RULES = json.loads(parameter_value)
            elif parameter_key == RM_ALERT_MAIL_ID_KEY:
                self.RM_ALERT_MAIL_ID = parameter_value
            elif parameter_key == CLOSED_RM_ALERT_MAIL_ID_KEY:
                self.CLOSED_RM_ALERT_MAIL_ID = parameter_value
            elif parameter_key == UP_B2C_KEY:
                self.UP_B2C = int(parameter_value)
            elif parameter_key == UP_B2B_KEY:
                self.UP_B2B = int(parameter_value)
            elif parameter_key == MARKET_FLAG_DOMESTIC_KEY:
                self.MARKET_FLAG_DOMESTIC = parameter_value
            elif parameter_key == MARKET_FLAG_INTERNATIONAL_KEY:
                self.MARKET_FLAG_INTERNATIONAL = parameter_value
            elif parameter_key == RBD_VALUES_KEY:
                self.RBD_VALUES = parameter_value
            elif parameter_key == B2B_RBD_VALUES_KEY:
                self.B2B_RBD_VALUES = parameter_value
            elif parameter_key == B2C_RBD_VALUES_KEY:
                self.B2C_RBD_VALUES = parameter_value
            elif parameter_key == GROUP_RBD_VALUES_KEY:
                self.GROUP_RBD_VALUES = parameter_value
            elif parameter_key == DISTRESS_RBD_VALUES_KEY:
                self.DISTRESS_RBD_VALUES = parameter_value
            elif parameter_key == B2B_THRESHOLD_KEY:
                self.B2B_THRESHOLD = float(parameter_value)
            elif parameter_key == B2B_THRESHOLD_INTERNATIONAL_KEY:
                self.B2B_THRESHOLD_INTERNATIONAL = float(parameter_value)
            elif parameter_key == B2C_DISTINCT_FARES_COUNT_KEY:
                self.B2C_DISTINCT_FARES_COUNT = int(parameter_value)
            elif parameter_key == DIRECT_FLIGHT_UPSELL_PERCENT_KEY:
                self.DIRECT_FLIGHT_UPSELL_PERCENT = float(parameter_value)
            elif parameter_key == DIRECT_FLIGHT_UPSELL_AMOUNT_KEY:
                self.DIRECT_FLIGHT_UPSELL_AMOUNT = float(parameter_value)
            elif parameter_key == INTERNATIONAL_BOOKLOAD_THRESHOLDS_KEY:
                self.INTERNATIONAL_BOOKLOAD_THRESHOLDS = self._parse_intl_thresholds(parameter_value)
            elif parameter_key == YIELD_FACTOR_KEY:
                self.YIELD_FACTOR = float(parameter_value)
            elif parameter_key == MARKET_FARE_MIN_YIELD_KEY:
                self.MARKET_FARE_MIN_YIELD = float(parameter_value)
            elif parameter_key == MARKET_FARE_FORECAST_THRESHOLD_KEY:
                self.MARKET_FARE_FORECAST_THRESHOLD = float(parameter_value)
            elif parameter_key == MARKET_FARE_MAX_THRESHOLD_DOMESTIC_B2B_KEY:
                self.MARKET_FARE_MAX_THRESHOLD_DOMESTIC_B2B = float(parameter_value)
            elif parameter_key == MARKET_FARE_MAX_THRESHOLD_INTERNATIONAL_B2B_KEY:
                self.MARKET_FARE_MAX_THRESHOLD_INTERNATIONAL_B2B = float(parameter_value)
            elif parameter_key == MARKET_FARE_MAX_THRESHOLD_DOMESTIC_B2C_KEY:
                self.MARKET_FARE_MAX_THRESHOLD_DOMESTIC_B2C = float(parameter_value)
            elif parameter_key == MARKET_FARE_MAX_THRESHOLD_INTERNATIONAL_B2C_KEY:
                self.MARKET_FARE_MAX_THRESHOLD_INTERNATIONAL_B2C = float(parameter_value)
            elif parameter_key == MARKET_FARE_MIN_THRESHOLD_DOMESTIC_B2B_KEY:
                self.MARKET_FARE_MIN_THRESHOLD_DOMESTIC_B2B = float(parameter_value)
            elif parameter_key == MARKET_FARE_MIN_THRESHOLD_INTERNATIONAL_B2B_KEY:
                self.MARKET_FARE_MIN_THRESHOLD_INTERNATIONAL_B2B = float(parameter_value)
            elif parameter_key == MARKET_FARE_MIN_THRESHOLD_DOMESTIC_B2C_KEY:
                self.MARKET_FARE_MIN_THRESHOLD_DOMESTIC_B2C = float(parameter_value)
            elif parameter_key == MARKET_FARE_MIN_THRESHOLD_INTERNATIONAL_B2C_KEY:
                self.MARKET_FARE_MIN_THRESHOLD_INTERNATIONAL_B2C = float(parameter_value)
            elif parameter_key == MARKET_FARE_MAX_THRESHOLD_CONNECTION_B2B_KEY:
                self.MARKET_FARE_MAX_THRESHOLD_CONNECTION_B2B = float(parameter_value)
            elif parameter_key == MARKET_FARE_MAX_THRESHOLD_CONNECTION_B2C_KEY:
                self.MARKET_FARE_MAX_THRESHOLD_CONNECTION_B2C = float(parameter_value)
            elif parameter_key == MARKET_FARE_MIN_THRESHOLD_CONNECTION_B2B_KEY:
                self.MARKET_FARE_MIN_THRESHOLD_CONNECTION_B2B = float(parameter_value)
            elif parameter_key == MARKET_FARE_MIN_THRESHOLD_CONNECTION_B2C_KEY:
                self.MARKET_FARE_MIN_THRESHOLD_CONNECTION_B2C = float(parameter_value)
            elif parameter_key == BOOKINGS_FARE_MAX_THRESHOLD_DOMESTIC_B2B_KEY:
                self.BOOKINGS_FARE_MAX_THRESHOLD_DOMESTIC_B2B = float(parameter_value)
            elif parameter_key == BOOKINGS_FARE_MAX_THRESHOLD_INTERNATIONAL_B2B_KEY:
                self.BOOKINGS_FARE_MAX_THRESHOLD_INTERNATIONAL_B2B = float(parameter_value)
            elif parameter_key == BOOKINGS_FARE_MAX_THRESHOLD_DOMESTIC_B2C_KEY:
                self.BOOKINGS_FARE_MAX_THRESHOLD_DOMESTIC_B2C = float(parameter_value)
            elif parameter_key == BOOKINGS_FARE_MAX_THRESHOLD_INTERNATIONAL_B2C_KEY:
                self.BOOKINGS_FARE_MAX_THRESHOLD_INTERNATIONAL_B2C = float(parameter_value)
            elif parameter_key == CURRENCY_ROUND_OFF_LIST_KEY:
                self.CURRENCY_ROUND_OFF_LIST = json.loads(parameter_value)
            elif parameter_key == ACTIVE_SECTORS_DEFAULT_FIELDS_KEY:
                self.ACTIVE_SECTORS_DEFAULT_FIELDS = parameter_value
            elif parameter_key == FARECAP_DISTANCE_CONFIG_KEY:
                self.FARECAP_DISTANCE_CONFIG = self._parse_farecap_distance_config(parameter_value)
            elif parameter_key == YQ_DISTANCE_CONFIG_KEY:
                self.YQ_DISTANCE_CONFIG = self._parse_farecap_distance_config(parameter_value)
            elif parameter_key == GDS_RBD_VALUES_KEY:
                self.GDS_RBD_VALUES = parameter_value
            elif parameter_key == GRACE_PERIOD_MINUTES_KEY:
                self.GRACE_PERIOD_MINUTES = parameter_value
            elif parameter_key == WINDOW_MINUTES_KEY:
                self.WINDOW_MINUTES = parameter_value
            elif parameter_key == AF_PLF_FORECAST_CAP_KEY:
                self.AF_PLF_FORECAST_CAP = float(parameter_value)
            elif parameter_key == AF_PLF_GAIN_MAX_ZERO_KEY:
                self.AF_PLF_GAIN_MAX_ZERO = float(parameter_value)
            elif parameter_key == AF_CONSTANT_BAND_2_KEY:
                self.AF_CONSTANT_BAND_2 = float(parameter_value)
            elif parameter_key == AF_CONSTANT_BAND_3_KEY:
                self.AF_CONSTANT_BAND_3 = float(parameter_value)
            elif parameter_key == AF_PROFILE_FARE_PLF_THRESHOLD_KEY:
                self.AF_PROFILE_FARE_PLF_THRESHOLD = float(parameter_value)
            elif parameter_key == AF_PROFILE_FARE_MULTIPLIER_KEY:
                self.AF_PROFILE_FARE_MULTIPLIER = float(parameter_value)
            elif parameter_key == AF_BAND_1_MULTIPLIERS_KEY:
                self.AF_BAND_1_MULTIPLIERS = json.loads(parameter_value)
            elif parameter_key == AF_BAND_2_MULTIPLIERS_KEY:
                self.AF_BAND_2_MULTIPLIERS = json.loads(parameter_value)
            elif parameter_key == CWT_UPSELL_KEY:
                self.CWT_UPSELL = json.loads(parameter_value)
            elif parameter_key == EQT_RECIPIENTS_KEY:
                self.EQT_RECIPIENTS = parameter_value
            elif parameter_key == GROUP_SCALING_FACTOR_KEY:
                self.GROUP_SCALING_FACTOR = float(parameter_value)
            elif parameter_key == GROUP_AU_BATCH_KEY:
                self.GROUP_AU_BATCH = float(parameter_value)
            elif parameter_key == GROUP_IMP_FORECAST_REDUCTION_KEY:
                self.GROUP_IMP_FORECAST_REDUCTION = float(parameter_value)
            elif parameter_key == W_PLF_KEY:
                self.W_PLF = float(parameter_value)
            elif parameter_key == W_AF_KEY:
                self.W_AF = float(parameter_value)
            elif parameter_key == W_ANCHOR_KEY:
                self.W_ANCHOR = float(parameter_value)
            elif parameter_key == MOVEMENT_GAIN_KEY:
                self.MOVEMENT_GAIN = float(parameter_value)
            elif parameter_key == MOVEMENT_FLOOR_PCT_KEY:
                self.MOVEMENT_FLOOR_PCT = float(parameter_value)
            elif parameter_key == MOVEMENT_CEIL_PCT_KEY:
                self.MOVEMENT_CEIL_PCT = float(parameter_value)
                
    def _parse_intl_thresholds(self, json_str):
        """Parse international threshold JSON string into dict with (start, end) tuple keys."""
        try:
            raw_data = json.loads(json_str)
            parsed_data = {}
            for key, value in raw_data.items():
                start_str, end_str = key.split('-')
                parsed_data[(int(start_str), int(end_str))] = float(value)
            return parsed_data
        except Exception as e:
            self.logger.error(f"Error parsing international bookload thresholds: {e}")
            # Return default hardcoded if parsing fails
            return {(0, 3): 80, (4, 7): 60, (8, 14): 50, (15, 365): 40}
    
    def _parse_farecap_distance_config(self, json_str):
        """Parse farecap distance config JSON into sorted list of (min_km, max_km, cap_value) tuples.
        
        Input format: {"0-500": 7500, "500-1000": 12000, "1000-1500": 15000, "1500+": 20000}
        Output: [(0, 500, 7500), (500, 1000, 12000), (1000, 1500, 15000), (1500, inf, 20000)]
        """
        try:
            raw_data = json.loads(json_str)
            parsed = []
            for key, value in raw_data.items():
                if '+' in key:
                    start = float(key.replace('+', ''))
                    parsed.append((start, float('inf'), float(value)))
                else:
                    parts = key.split('-')
                    parsed.append((float(parts[0]), float(parts[1]), float(value)))
            parsed.sort(key=lambda x: x[0])
            return parsed
        except Exception as e:
            self.logger.error(f"Error parsing farecap distance config: {e}")
            return [(0, 500, 7500), (500, 1000, 12000), (1000, 1500, 15000), (1500, float('inf'), 20000)]
                
    def get_parameters(self):
        parameterValue = None  # Initialize with a default value
        try:
            results = list(self.db.rm_parameter_values.find({}, {"parameterKey": 1, "parameterValue": 1, "_id": 0}))
            parameterValue = pd.DataFrame(results)
        except Exception as e:
            self.logger.error(e)
            return pd.DataFrame()
        
        return parameterValue