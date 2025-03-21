import pandas as pd


class ConstantsUtils:
    def __init__(self, rd_conn):
        self.rd_conn = rd_conn

        # Initialize all possible parameters with default values or None
        self.THRESHOLD_TIME = 12
        self.PF_UPSELL_VALUE = '47%'
        # Generate constants from database values
        self.generateConstantValues()

    def generateConstantValues(self):
        parameterValue = self.get_parameters()

        for index, val in parameterValue.iterrows():
            parameter_key = val['parameterKey']
            parameter_value = val['parameterValue']

            if parameter_key == "THRESHOLD_TIME":
                self.THRESHOLD_TIME = int(parameter_value)
                
            if parameter_key == "PF_UPSELL_VALUE":
                self.PF_UPSELL_VALUE = int(parameter_value)

    def get_parameters(self):
        parameterValueQuery = "SELECT parameterKey,parameterValue FROM rm_parameter_values"
        try:
            parameterValue = pd.read_sql(parameterValueQuery, self.rd_conn)
        except Exception as e:
            self.logger.error(e)

        return parameterValue
