from os import getenv
from boto3 import client

def get_multiple_credentials(parameter_names):
    ssm_client = client('ssm', region_name='ap-south-1')
    
    try:
        # Get multiple parameters at once
        response = ssm_client.get_parameters(
            Names=parameter_names,
            WithDecryption=True
        )
        
        # Process the results into a dictionary
        parameters = {}
        for param in response['Parameters']:
            parameters[param['Name']] = param['Value']
            
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
    f'/rm/{env}/rm-allocation/NAV_DB_PASSWORD',
    f'/rm/{env}/rm-allocation/NAV_API_PASSWORD',
    f'/rm/{env}/rm-allocation/NAV_API_SUBSCRIPTION_KEY',
    f'/rm/{env}/rm-allocation/RM_GW_APIKEY'
]


# Get all credentials at once
credentials = get_multiple_credentials(param_names)

if env == 'prod':
    RM_DB_APPUSER_USERNAME = "rmappuser"
    RM_DB_APPUSER_URL = "prod-rm-db.cz6mmsw6mnag.ap-south-1.rds.amazonaws.com"
    RM_DB_APPUSER_USERNAME_READ = "rmappuser"
    RM_DB_APPUSER_URL_READ = "prod-rm-db-replica.cz6mmsw6mnag.ap-south-1.rds.amazonaws.com"
    RM_DB_APPUSER_PASSWORD_READ = credentials.get('/rm/prod/rm-allocation/RM_DB_APPUSER_PASSWORD', '')
    NAV_DB_HOST = "10.154.12.13:52900"
    NAV_DB_NAME = "[REZ0KOD01]."
    NAV_DB_DW_NAME = "[REZ0KDW01]."
    NAV_DB_WB_NAME = "[REZ0KWB01]."
    NAV_DB_REF_NAME = "[REF0KOD01]."
    NAV_DB_PASSWORD = credentials.get(f'/rm/{env}/rm-allocation/NAV_DB_PASSWORD', '')
    MARKET_LIST_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/471112573018/rm-allocation-queues_rm-allocation-market-list"
    RBD_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/471112573018/rm-allocation-queues_rm-allocation-rbds"
    NAV_API_URL = "https://8trj4yffqf.execute-api.ap-south-1.amazonaws.com/default/navitaire-webservice/v1/queries/output"
    NAV_BASE_URL = "https://dotrezapi.prod.0k.navitaire.com/0k/dotrez"
    NAV_DB_USER = "0KODSUSER2"
    RM_CACHE_ENDPOINT = "ip-10-16-6-51.ap-south-1.compute.internal"
    RM_CACHE_PASSWORD = credentials.get(f'/rm/{env}/rm-allocation/RM_CACHE_PASSWORD', '')
    
if env == 'uat':
    RM_DB_APPUSER_USERNAME = "rm-user"
    RM_DB_APPUSER_URL = "uat-esb-db.cfwwkweqog18.ap-south-1.rds.amazonaws.com"
    RM_DB_APPUSER_USERNAME_READ = "rm-user"
    RM_DB_APPUSER_URL_READ = "uat-esb-db.cfwwkweqog18.ap-south-1.rds.amazonaws.com"
    RM_DB_APPUSER_PASSWORD_READ = credentials.get(f'/rm/{env}/rm-allocation/RM_DB_APPUSER_PASSWORD', '')
    NAV_DB_HOST = "10.154.12.19:52900"
    NAV_DB_NAME = "[TST0KOD01]."
    NAV_DB_DW_NAME = "[TST0KDW01]."
    NAV_DB_WB_NAME = "[TST0KWB01]."
    NAV_DB_REF_NAME = "[TSTREF0KOD01]."
    NAV_DB_USER = "0KODSUSER"
    NAV_DB_PASSWORD = credentials.get(f'/rm/{env}/rm-allocation/NAV_DB_PASSWORD', '')
    # Commented out original code
    # RM_DB_APPUSER_USERNAME_READ = "rmappuser"
    # RM_DB_APPUSER_URL_READ = "prod-rm-db-replica.cz6mmsw6mnag.ap-south-1.rds.amazonaws.com"
    # RM_DB_APPUSER_PASSWORD_READ = get_credentials('/rm/prod/rm-allocation/RM_DB_APPUSER_PASSWORD')
    # NAV_DB_USER = "0KODSUSER2"
    # NAV_DB_HOST = "10.154.12.13:52900"
    # NAV_DB_NAME = "[REZ0KOD01]."
    # NAV_DB_DW_NAME = "[REZ0KDW01]."
    # NAV_DB_WB_NAME = "[REZ0KWB01]."
    # NAV_DB_PASSWORD = get_credentials('/rm/prod/rm-allocation/NAV_DB_PASSWORD')
    MARKET_LIST_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/891377165721/test-rm-allocation-queues-rm-allocation-market-list"
    RBD_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/891377165721/test-rm-allocation-queues-rm-allocation-rbds"
    NAV_API_URL = "https://aanegtyddf.execute-api.ap-south-1.amazonaws.com/default/navitaire-webservice/v1/queries/output"
    NAV_BASE_URL = "https://qpecomapp-tbnk-reyalrb.qp.akasaair.com/test"
    RM_CACHE_ENDPOINT = "ip-172-24-72-8.ap-south-1.compute.internal"
    # RM_CACHE_ENDPOINT = "172.24.72.8"
    RM_CACHE_PASSWORD = credentials.get(f'/rm/{env}/rm-allocation/RM_CACHE_PASSWORD', '')

RM_CACHE_USERNAME = "default"
RM_CACHE_PORT = 6379

RM_DB_SCHEMA_NAME = "QP_DW_RMALLOC"
RM_DB_APPUSER_PASSWORD = credentials.get(f'/rm/{env}/rm-allocation/RM_DB_APPUSER_PASSWORD', '')

NAV_USERNAME = "aumaster1"
NAV_DOMAIN = "TOP"
NAV_PWD = credentials.get(f'/rm/{env}/rm-allocation/NAV_API_PASSWORD', '')
NAV_OCP_SUBSCRIPTION_KEY = credentials.get(f'/rm/{env}/rm-allocation/NAV_API_SUBSCRIPTION_KEY', '')

RM_GW_API_KEY = credentials.get(f'/rm/{env}/rm-allocation/RM_GW_APIKEY', '')