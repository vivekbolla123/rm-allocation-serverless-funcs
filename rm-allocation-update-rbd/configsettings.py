from os import getenv
from boto3 import client

def get_parameters(parameter_names, region='ap-south-1'):
    ssm_client = client('ssm', region_name=region)
    response = ssm_client.get_parameters(
        Names=parameter_names,
        WithDecryption=True
    )
    
    # Convert the response to a dictionary for easy access
    parameters = {}
    for param in response['Parameters']:
        # Extract just the parameter name from the full path
        name = param['Name']
        parameters[name] = param['Value']
    
    return parameters

env = getenv('env', default='uat')

# Define environment-specific non-secret values
if env == 'prod':
    RM_DB_APPUSER_USERNAME = "rmappuser"
    RM_DB_APPUSER_URL = "prod-rm-db.cz6mmsw6mnag.ap-south-1.rds.amazonaws.com"
    NAV_BASE_URL = "https://dotrezapi.prod.0k.navitaire.com/0k/dotrez"
    RM_CACHE_ENDPOINT = "ip-10-16-6-51.ap-south-1.compute.internal"
else:  # Default to UAT
    RM_DB_APPUSER_USERNAME = "rm-user"
    RM_DB_APPUSER_URL = "uat-esb-db.cfwwkweqog18.ap-south-1.rds.amazonaws.com"
    NAV_BASE_URL = "https://qpecomapp-tbnk-reyalrb.qp.akasaair.com/test"
    RM_CACHE_ENDPOINT = "ip-172-24-72-8.ap-south-1.compute.internal"

# Fixed values
RM_CACHE_USERNAME = "default"
RM_CACHE_PORT = 6379
NAV_USERNAME = "aumaster1"
NAV_DOMAIN = "TOP"
RM_DB_SCHEMA_NAME = "QP_DW_RMALLOC"

# Define the list of parameters to fetch
parameter_names = [
    f'/rm/{env}/rm-allocation/RM_CACHE_PASSWORD',
    f'/rm/{env}/rm-allocation/RM_DB_APPUSER_PASSWORD',
    f'/rm/{env}/rm-allocation/NAV_API_PASSWORD',
    f'/rm/{env}/rm-allocation/NAV_API_SUBSCRIPTION_KEY'
]

# Get all parameters at once
parameters = get_parameters(parameter_names)

# Map parameters to variables
RM_CACHE_PASSWORD = parameters.get(f'/rm/{env}/rm-allocation/RM_CACHE_PASSWORD','')
RM_DB_APPUSER_PASSWORD = parameters.get(f'/rm/{env}/rm-allocation/RM_DB_APPUSER_PASSWORD','')
NAV_PWD = parameters.get(f'/rm/{env}/rm-allocation/NAV_API_PASSWORD','')
NAV_OCP_SUBSCRIPTION_KEY = parameters.get(f'/rm/{env}/rm-allocation/NAV_API_SUBSCRIPTION_KEY','')