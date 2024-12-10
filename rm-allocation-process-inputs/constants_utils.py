import pandas as pd


class ConstantsUtils:
    def __init__(self, rd_conn):
        self.rd_conn = rd_conn

        self.CHECK_THRESHOLD_PROCESS_INPUTS = 5
        # Generate constants from database values
        self.generateConstantValues()

    def generateConstantValues(self):
        parameterValue = self.get_parameters()

        for index, val in parameterValue.iterrows():
            parameter_key = val['parameterKey']
            parameter_value = val['parameterValue']
            
            if parameter_key == "CHECK_THRESHOLD_PROCESS_INPUTS":
                self.CHECK_THRESHOLD_PROCESS_INPUTS = int(parameter_value)

    def get_parameters(self):
        parameterValueQuery = "SELECT parameterKey,parameterValue FROM rm_parameter_values"
        try:
            parameterValue = pd.read_sql(parameterValueQuery, self.rd_conn)
        except Exception as e:
            self.logger.error(e)

        return parameterValue