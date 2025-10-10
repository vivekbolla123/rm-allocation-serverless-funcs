from os import getenv
from boto3 import client

def get_multiple_credentials(parameter_names):
    ssm_client = client('ssm', region_name='ap-south-1')
    parameters = {}

    try:
        for i in range(0, len(parameter_names), 10):
            chunk = parameter_names[i:i+10]
            response = ssm_client.get_parameters(
                Names=chunk,
                WithDecryption=True
            )

            # Process the results into a dictionary
            for param in response['Parameters']:
                param_name = param['Name']
                parameters[param_name] = param['Value']

            # Check if any parameters were not found
            if response['InvalidParameters']:
                print(f"Parameters not found: {response['InvalidParameters']}")

    except Exception as e:
        print(f"Error fetching parameters: {e}")
        return {}

    return parameters

env = getenv('env', default='uat')

# List all parameter names you need to fetch
param_names = [
    f'/rm/{env}/rm-allocation/RM_DB_APPUSER_PASSWORD',
    f'/rm/{env}/rm-allocation/RM_CACHE_PASSWORD',
    f'/copilot/qp/{env}/secrets/RM_WEBSERVICE_AWS_ACCESS_KEY_NEW_PROD',
    f'/copilot/qp/{env}/secrets/RM_WEBSERVICE_AWS_SECRET_KEY_NEW_PROD',
]


# Get all credentials at once
credentials = get_multiple_credentials(param_names)


if env == 'prod':
    RM_DB_APPUSER_USERNAME = "rmappuser"
    RM_DB_APPUSER_URL = "prod-rm-db.cz6mmsw6mnag.ap-south-1.rds.amazonaws.com"

    S3_BUCKET_NAME = 'qp-applications'
    S3_FILE_PATH_NAME = 'prod/rm-admin/'
    
if env == 'uat':
    RM_DB_APPUSER_USERNAME = "app_esb"
    RM_DB_APPUSER_URL = "172.24.87.43"

    S3_BUCKET_NAME = 'uat-qp-applications'
    S3_FILE_PATH_NAME = "test/rm-admin/"

RM_DB_SCHEMA_NAME = "QP_DW_PRIAUT"
RM_DB_APPUSER_PASSWORD = credentials.get('/rm/' + env + '/rm-allocation/RM_DB_APPUSER_PASSWORD', '')

S3_PREFIX = f"{S3_FILE_PATH_NAME}upload-pricing/"
RM_WEBSERVICE_AWS_ACCESS_KEY_NEW_PROD = credentials.get('/copilot/qp/' + env + '/secrets/RM_WEBSERVICE_AWS_ACCESS_KEY_NEW_PROD', '')
RM_WEBSERVICE_AWS_SECRET_KEY_NEW_PROD = credentials.get('/copilot/qp/' + env + '/secrets/RM_WEBSERVICE_AWS_SECRET_KEY_NEW_PROD', '')
