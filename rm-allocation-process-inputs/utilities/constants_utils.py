from Constants import *
class ConstantsUtils:
    def __init__(self, rmdatabasehelper):
        self.rmdatabasehelper = rmdatabasehelper
        
        # Initialize all possible parameters with default values or None
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
        self.S_CLASS_PLF_THRESHOLD = S_CLASS_PLF_THRESHOLD
        self.END_OF_DAY_LF_FACTOR = END_OF_DAY_LF_FACTOR
        self.AU_COLUMN_LENGTH_CONNECTIONS=AU_COLUMN_LENGTH_CONNECTIONS
        self.MARKET_FARES_THRESHOLD=MARKET_FARES_THRESHOLD
        self.GROUP_QUOTATION = GROUP_QUOTATION
        self.TBF_RBD = TBF_RBD
        self.LAST_SOLD_FARE_THRESHOLD = LAST_SOLD_FARE_THRESHOLD
        self.LAST_N_AVG_FARE = LAST_N_AVG_FARE
        self.MFMIN0 = MFMIN0
        self.MFMIN1 = MFMIN1
        self.CHECK_THRESHOLD_PROCESS_INPUTS = CHECK_THRESHOLD_PROCESS_INPUTS
        self.GROUP_DIFF_FARE = GROUP_DIFF_FARE
        self.TBF_DIFF_FARE = TBF_DIFF_FARE
        self.PROF_OPEN_COUNT = PROF_OPEN_COUNT
        self.B2B_DISCOUNT_MAP = B2B_DISCOUNT_MAP
        
        # Generate constants from database values
        self.generateConstantValues()

    def generateConstantValues(self):
        parameterValue = self.rmdatabasehelper.get_parameters()
        
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
            elif parameter_key == PROF_OPEN_COUNT_KEY:
                self.PROF_OPEN_COUNT = int(parameter_value)

