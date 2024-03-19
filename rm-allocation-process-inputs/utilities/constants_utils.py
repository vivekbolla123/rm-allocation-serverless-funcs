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
        self.OWN_FARE_DELTA = OWN_FARE_DELTA
        self.OVERBOOKING_LF=OVERBOOKING_LF
        self.OVERBOOKING_START_NDO=OVERBOOKING_START_NDO
        self.OVERBOOKING_END_NDO=OVERBOOKING_END_NDO
        self.OVERBOOKING_PROB=OVERBOOKING_PROB
        self.S_CLASS_PLF_THRESHOLD=S_CLASS_PLF_THRESHOLD
        
        # Generate constants from database values
        self.generateConstantValues()

    def generateConstantValues(self):
        parameterValue = self.rmdatabasehelper.get_parameters()
        
        for index, val in parameterValue.iterrows():
            parameter_key = val['parameterKey']
            parameter_value = val['parameterValue']
            
            if parameter_key == 'MIN_D3_D4_VALUE':
                self.MIN_D3_D4_VALUE = float(parameter_value)
            elif parameter_key == 'LINEAR_JUMP_VALUE':
                self.LINEAR_JUMP_VALUE = float(parameter_value)
            elif parameter_key == 'PLF_CURVE_VALUE':
                self.PLF_CURVE_VALUE = float(parameter_value)
            elif parameter_key == 'B2B_FARE_PRICE_COMPARISON':
                self.B2B_FARE_PRICE_COMPARISON = float(parameter_value)
            elif parameter_key == 'COVERAGE_PARAMETER':
                self.COVERAGE_PARAMETER = float(parameter_value)
            elif parameter_key == 'AU_COLUMN_LENGTH':
                self.AU_COLUMN_LENGTH = int(parameter_value)
            elif parameter_key == 'B2B_RDB_COUNT':
                self.B2B_RDB_COUNT = float(parameter_value)
            elif parameter_key == 'LOWEST_B2B_RBD_VALUE':
                self.LOWEST_B2B_RBD_VALUE = str(parameter_value)
            elif parameter_key == 'LOWEST_B2C_RBD_VALUE':
                self.LOWEST_B2C_RBD_VALUE = str(parameter_value)
            elif parameter_key == 'HIGHEST_B2B_RBD_VALUE':
                self.HIGHEST_B2B_RBD_VALUE = str(parameter_value)
            elif parameter_key == 'HIGHEST_B2C_RBD_VALUE':
                self.HIGHEST_B2C_RBD_VALUE = str(parameter_value)
            elif parameter_key == 'OWN_FARE_DELTA':
                self.OWN_FARE_DELTA = float(parameter_value)
                
            elif parameter_key == 'OVERBOOKING_LF':
                self.OVERBOOKING_LF = float(parameter_value)
            elif parameter_key == 'OVERBOOKING_START_NDO':
                self.OVERBOOKING_START_NDO = int(parameter_value)
            elif parameter_key == 'OVERBOOKING_END_NDO':
                self.OVERBOOKING_END_NDO = int(parameter_value)
            elif parameter_key == 'OVERBOOKING_PROB':
                self.OVERBOOKING_PROB = float(parameter_value)
            elif parameter_key=='S_CLASS_PLF_THRESHOLD':
                self.S_CLASS_PLF_THRESHOLD=float(parameter_value)