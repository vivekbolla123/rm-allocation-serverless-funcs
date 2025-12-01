from os import getenv
from boto3 import client

def get_multiple_credentials(parameter_names):
    ssm_client = client('ssm', region_name='ap-south-1')
    parameters = {}

    try:
        # Split the parameter names into chunks of 10
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

creenv=env

if env=="preprod":
    creenv="prod"

# List all parameter names you need to fetch
param_names = [
    f'/rm/{creenv}/rm-allocation/RM_DB_APPUSER_PASSWORD',
    f'/rm/{creenv}/rm-allocation/RM_CACHE_PASSWORD',
    f'/rm/{creenv}/rm-allocation/NAV_DB_PASSWORD',
    f'/rm/{creenv}/rm-allocation/NAV_API_PASSWORD',
    f'/rm/{creenv}/rm-allocation/NAV_API_SUBSCRIPTION_KEY',
    f'/rm/{creenv}/rm-allocation/NAV_SFTP_PASSWORD',
    f'/rm/{creenv}/rm-allocation/RM_GW_APIKEY',
    f'/rm/{creenv}/rm-allocation/ESB_DB_PASSWORD',
    f'/copilot/qp/{creenv}/secrets/RM_WEBSERVICE_LAMBDA_ACCESS_KEY',
    f'/glue_job/{creenv}/DW_DB_APPUSER_USERNAME',
    f'/glue_job/{creenv}/DW_DB_APPUSER_PASSWORD',
    f'/copilot/qp/{creenv}/secrets/smtp/QP_ALERTS_PWD',
    f'/copilot/qp/{creenv}/secrets/Qp-Forecast-DB-User',
    f'/copilot/qp/{creenv}/secrets/Qp-Forecast-DB-Pass',
    f'/copilot/qp/{creenv}/secrets/RM_WEBSERVICE_AWS_ACCESS_KEY_NEW_PROD',
    f'/copilot/qp/{creenv}/secrets/RM_WEBSERVICE_AWS_SECRET_KEY_NEW_PROD',
    f'/rm/{creenv}/rm-allocation/RM_MONGO_PASSWORD',
  
]


# Get all credentials at once
credentials = get_multiple_credentials(param_names)


if env == 'prod':
    RM_DB_APPUSER_USERNAME = "rmappuser"
    RM_DB_APPUSER_URL = "prod-rm-db.cz6mmsw6mnag.ap-south-1.rds.amazonaws.com"
    RM_DB_APPUSER_USERNAME_READ = "rmappuser"
    RM_DB_APPUSER_URL_READ = "prod-rm-db-replica.cz6mmsw6mnag.ap-south-1.rds.amazonaws.com"
    RM_DB_APPUSER_PASSWORD_READ = credentials.get('/rm/' + env + '/rm-allocation/RM_DB_APPUSER_PASSWORD', '')
    NAV_DB_HOST = "10.154.12.13:52900"
    NAV_DB_NAME = "[REZ0KOD01]."
    NAV_DB_DW_NAME = "[REZ0KDW01]."
    NAV_DB_WB_NAME = "[REZ0KWB01]."
    NAV_DB_REF_NAME = "[REF0KOD01]."
    NAV_DB_PASSWORD = credentials.get('/rm/' + env + '/rm-allocation/NAV_DB_PASSWORD', '')
    MARKET_LIST_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/471112573018/rm-allocation-queues_rm-allocation-market-list"
    RBD_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/471112573018/rm-allocation-queues_rm-allocation-rbds"
    NAV_API_URL="https://8trj4yffqf.execute-api.ap-south-1.amazonaws.com/default/navitaire-webservice/v1/queries/output"
    NAV_BASE_URL= "https://dotrezapi.prod.0k.navitaire.com/0k/dotrez"
    NAV_DB_USER = "0KODSUSER2"
    RM_CACHE_ENDPOINT = "ip-10-16-6-51.ap-south-1.compute.internal"
    RM_CACHE_PASSWORD = credentials.get('/rm/' + env + '/rm-allocation/RM_CACHE_PASSWORD', '')
    DW_DB_HOST="prod-qp-dw.cz6mmsw6mnag.ap-south-1.rds.amazonaws.com"
    DW_DB_PORT='3306'
    DW_DB_USER=credentials.get('/glue_job/' + env + '/DW_DB_APPUSER_USERNAME', '')
    DW_DB_PASSWORD= credentials.get('/glue_job/' + env + '/DW_DB_APPUSER_PASSWORD', '')
    DW_DB_WRITE_SCHEMA='RM_DW_DATA'
    DW_DB_READ_SCHEMA='QP_DW_NAVFACT'
 
    ESB_HOST='p-esb.cz6mmsw6mnag.ap-south-1.rds.amazonaws.com'
    ESB_PORT='3306'
    ESB_USER=credentials.get('/copilot/qp/' + env + '/secrets/Qp-Forecast-DB-User', '')
    ESB_PASSWORD=credentials.get('/copilot/qp/' + env + '/secrets/Qp-Forecast-DB-Pass', '')
    ESB_READ_SCHEMA='QP_DL_NAVDATA'
 
 
    RM_DB_WR_APPUSER_URL = "prod-rm-db.cz6mmsw6mnag.ap-south-1.rds.amazonaws.com"
    SFTP_RBD_QUEUE_URL = "rm-allocation-queues_rm-allocation-rbds-ftp"
    ESB_DB_WR_APPUSER_URL = "p-esb.cz6mmsw6mnag.ap-south-1.rds.amazonaws.com"
    ESB_DB_USERNAME = "appuser-rm"
    S3_FILE_PATH = 'prod/'
    S3_BUCKET_NAME = 'qp-applications'
    S3_RM_BUCKET_NAME = "rm-systems"
    FETCH_INPUTS_API_URL = "http://prod-internal-nlb-b7a4a0b9d3b5ddcb.elb.ap-south-1.amazonaws.com/lambda/rm-allocation-serverless-funcs_rm-allocation-fetch-inputs"
    CALCULATE_COMPONENTS_QUEUE_URL = "rm-allocation-queues_rm-allocation-scheduled-jobs"
    NAV_PWD = credentials.get('/rm/' + env + '/rm-allocation/NAV_API_PASSWORD', '')
    NAV_OCP_SUBSCRIPTION_KEY = credentials.get('/rm/' + env + '/rm-allocation/NAV_API_SUBSCRIPTION_KEY', '')

    ESB_DB_PASSWORD = credentials.get("/rm/" + env + "/rm-allocation/ESB_DB_PASSWORD")
    RM_GW_API_KEY = credentials.get('/rm/' + env + '/rm-allocation/RM_GW_APIKEY', '')
    FETCH_INPUTS_ACCESS_KEY = credentials.get('/copilot/qp/' + env + '/secrets/RM_WEBSERVICE_LAMBDA_ACCESS_KEY', '')
    RM_DB_APPUSER_PASSWORD = credentials.get('/rm/' + env + '/rm-allocation/RM_DB_APPUSER_PASSWORD', '')
    
    SFTP_USERNAME = '0KProdr4x_sftp'
    AWS_SQS_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/471112573018/rm-allocation-queues_rm-allocation-rbds-ftp"
    SFTP_PASSWORD = credentials.get('/rm/' + env + '/rm-allocation/NAV_SFTP_PASSWORD', '')
    
    FETCH_INPUTS_LAMBDA = "rm-allocation-serverless-funcs_rm-allocation-fetch-inputs"
    RM_WEBSERVICE_AWS_ACCESS_KEY_NEW_PROD = credentials.get('/copilot/qp/' + env + '/secrets/RM_WEBSERVICE_AWS_ACCESS_KEY_NEW_PROD', '')
    RM_WEBSERVICE_AWS_SECRET_KEY_NEW_PROD = credentials.get('/copilot/qp/' + env + '/secrets/RM_WEBSERVICE_AWS_SECRET_KEY_NEW_PROD', '')

    SENDER_PASSWORD= credentials.get('/copilot/qp/' + env + '/secrets/smtp/QP_ALERTS_PWD')
    
    MONGO_USERNAME = "rmuser"
    MONGO_DB = "qp_dw_rmalloc"
    MONGO_PASSWORD = credentials.get('/rm/' + env + '/rm-allocation/RM_MONGO_PASSWORD', '')
    MONGO_URL = "10.16.6.179"
    MONGO_AUTH_SOURCE="admin"

    SENDER_PASSWORD= credentials.get('/copilot/qp/' + env + '/secrets/smtp/QP_ALERTS_PWD')
    
    PA_BUCKET_NAME = "qp-s3-fs"
    PA_FILE_PATH = 'prod/PA/'
    
if env == 'uat':
    RM_DB_APPUSER_USERNAME = "app_esb"
    RM_DB_APPUSER_URL = "172.24.87.43"
    RM_DB_APPUSER_USERNAME_READ = "app_esb"
    RM_DB_APPUSER_URL_READ = "172.24.87.43"
    RM_DB_APPUSER_PASSWORD_READ=credentials.get('/rm/uat/rm-allocation/RM_DB_APPUSER_PASSWORD', '')
    NAV_DB_HOST = "10.154.12.19:52900"
    NAV_DB_NAME = "[TST0KOD01]."
    NAV_DB_DW_NAME = "[TST0KDW01]."
    NAV_DB_WB_NAME = "[TST0KWB01]."
    NAV_DB_REF_NAME = "[TSTREF0KOD01]."
    NAV_DB_USER = "0KODSUSER"
    NAV_DB_PASSWORD = credentials.get('/rm/' + env + '/rm-allocation/NAV_DB_PASSWORD', '')
    # RM_DB_APPUSER_USERNAME_READ = "rmappuser"
    # RM_DB_APPUSER_URL_READ = "prod-rm-db-replica.cz6mmsw6mnag.ap-south-1.rds.amazonaws.com"
    # RM_DB_APPUSER_PASSWORD_READ = credentials.get('/rm/prod/rm-allocation/RM_DB_APPUSER_PASSWORD', '')
    # NAV_DB_USER = "0KODSUSER2"
    # NAV_DB_HOST = "10.154.12.13:52900"
    # NAV_DB_NAME = "[REZ0KOD01]."
    # NAV_DB_DW_NAME = "[REZ0KDW01]."
    # NAV_DB_WB_NAME = "[REZ0KWB01]."
    # NAV_DB_PASSWORD = credentials.get('/rm/prod/rm-allocation/NAV_DB_PASSWORD', '')
    MARKET_LIST_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/891377165721/test-rm-allocation-queues-rm-allocation-market-list"
    RBD_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/891377165721/test-rm-allocation-queues-rm-allocation-rbds"
    NAV_API_URL="https://aanegtyddf.execute-api.ap-south-1.amazonaws.com/default/navitaire-webservice/v1/queries/output"
    NAV_BASE_URL= "https://qpecomapp-tbnk-reyalrb.qp.akasaair.com/test"
    RM_CACHE_ENDPOINT = "ip-172-24-72-8.ap-south-1.compute.internal"
    # RM_CACHE_ENDPOINT = "172.24.72.8"
    RM_CACHE_PASSWORD = credentials.get('/rm/' + env + '/rm-allocation/RM_CACHE_PASSWORD', '')
    DW_DB_HOST='172.24.87.10'
    DW_DB_PORT='3306'
    DW_DB_USER=credentials.get('/glue_job/' + env + '/DW_DB_APPUSER_USERNAME', '')
    DW_DB_PASSWORD= credentials.get('/glue_job/' + env + '/DW_DB_APPUSER_PASSWORD', '')
    DW_DB_WRITE_SCHEMA='RM_DW_DATA'
    DW_DB_READ_SCHEMA='QP_DW_NAVFACT'
 
    ESB_HOST='172.24.87.43'
    ESB_PORT='3306'
    ESB_USER=credentials.get('/copilot/qp/' + env + '/secrets/Qp-Forecast-DB-User', '')
    ESB_PASSWORD=credentials.get('/copilot/qp/' + env + '/secrets/Qp-Forecast-DB-Pass', '')
    ESB_READ_SCHEMA='QP_DL_NAVDATA'
    
    RM_DB_WR_APPUSER_URL = "172.24.87.43"
    ESB_DB_WR_APPUSER_URL = "172.24.87.43"
    ESB_DB_USERNAME = "app_esb"
    # ESB_DB_WR_APPUSER_URL = "p-esb.cz6mmsw6mnag.ap-south-1.rds.amazonaws.com"
    # ESB_DB_USERNAME = "appuser-rm"
    SFTP_RBD_QUEUE_URL = "test-rm-allocation-queues-rm-allocation-rbds-ftp"
    S3_FILE_PATH = 'test/'
    S3_BUCKET_NAME = 'uat-qp-applications'
    S3_RM_BUCKET_NAME = "uat-rm-systems"
    FETCH_INPUTS_API_URL = "https://uat-intsvc.qp.akasaair.com/rm-allocation-serverless-funcs_rm-allocation-fetch-inputs"
    CALCULATE_COMPONENTS_QUEUE_URL = 'rm-allocation-queues_test-rm-allocation-scheduled-jobs'
    
    NAV_PWD = credentials.get('/rm/' + env + '/rm-allocation/NAV_API_PASSWORD', '')
    NAV_OCP_SUBSCRIPTION_KEY = credentials.get('/rm/' + env + '/rm-allocation/NAV_API_SUBSCRIPTION_KEY', '')

    ESB_DB_PASSWORD = credentials.get("/rm/" + env + "/rm-allocation/ESB_DB_PASSWORD")
    RM_GW_API_KEY = credentials.get('/rm/' + env + '/rm-allocation/RM_GW_APIKEY', '')
    FETCH_INPUTS_ACCESS_KEY = credentials.get('/copilot/qp/' + env + '/secrets/RM_WEBSERVICE_LAMBDA_ACCESS_KEY', '')
    RM_DB_APPUSER_PASSWORD = credentials.get('/rm/' + env + '/rm-allocation/RM_DB_APPUSER_PASSWORD', '')
    
    SFTP_USERNAME = '0KTestr4x_sftp'
    AWS_SQS_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/891377165721/test-rm-allocation-queues-rm-allocation-rbds-ftp"
    SFTP_PASSWORD = credentials.get('/rm/' + env + '/rm-allocation/NAV_SFTP_PASSWORD', '')
    
    FETCH_INPUTS_LAMBDA = "uat-rm-allocation-serverless-funcs-rm-allocation-fetch-inputs"
    RM_WEBSERVICE_AWS_ACCESS_KEY_NEW_PROD = credentials.get('/copilot/qp/' + env + '/secrets/RM_WEBSERVICE_AWS_ACCESS_KEY_NEW_PROD', '')
    RM_WEBSERVICE_AWS_SECRET_KEY_NEW_PROD = credentials.get('/copilot/qp/' + env + '/secrets/RM_WEBSERVICE_AWS_SECRET_KEY_NEW_PROD', '')

    SENDER_PASSWORD= credentials.get('/copilot/qp/' + env + '/secrets/smtp/QP_ALERTS_PWD')
    MONGO_USERNAME = "testuser"
    MONGO_DB = "qp_dw_rmalloc"
    MONGO_PASSWORD = credentials.get('/rm/' + env + '/rm-allocation/RM_MONGO_PASSWORD', '')
    MONGO_URL = "172.24.72.35"
    MONGO_AUTH_SOURCE="admin"

    SENDER_PASSWORD= credentials.get('/copilot/qp/' + env + '/secrets/smtp/QP_ALERTS_PWD')
    
    PA_BUCKET_NAME = "uat-qp-s3-fs"
    PA_FILE_PATH = 'uat/PA/'
    
if env == 'preprod':
    credenv='prod'
    RM_DB_APPUSER_USERNAME = "rmappuser"
    RM_DB_APPUSER_URL = "prod-rm-db.cz6mmsw6mnag.ap-south-1.rds.amazonaws.com"
    RM_DB_APPUSER_USERNAME_READ = "rmappuser"
    RM_DB_APPUSER_URL_READ = "prod-rm-db-replica.cz6mmsw6mnag.ap-south-1.rds.amazonaws.com"
    RM_DB_APPUSER_PASSWORD_READ = credentials.get('/rm/' + credenv + '/rm-allocation/RM_DB_APPUSER_PASSWORD', '')
    NAV_DB_HOST = "10.154.12.13:52900"
    NAV_DB_NAME = "[REZ0KOD01]."
    NAV_DB_DW_NAME = "[REZ0KDW01]."
    NAV_DB_WB_NAME = "[REZ0KWB01]."
    NAV_DB_REF_NAME = "[REF0KOD01]."
    NAV_DB_PASSWORD = credentials.get('/rm/' + credenv + '/rm-allocation/NAV_DB_PASSWORD', '')
    MARKET_LIST_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/471112573018/preprod-rm-allocation-queues_rm-allocation-market-list"
    RBD_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/471112573018/preprod-rm-allocation-queues_rm-allocation-rbds"
    NAV_API_URL="https://8trj4yffqf.execute-api.ap-south-1.amazonaws.com/default/navitaire-webservice/v1/queries/output"
    NAV_BASE_URL= "https://dotrezapi.prod.0k.navitaire.com/0k/dotrez"
    NAV_DB_USER = "0KODSUSER2"
    RM_CACHE_ENDPOINT = "ip-10-16-6-51.ap-south-1.compute.internal"
    RM_CACHE_PASSWORD = credentials.get('/rm/' + credenv + '/rm-allocation/RM_CACHE_PASSWORD', '')
    DW_DB_HOST="prod-qp-dw.cz6mmsw6mnag.ap-south-1.rds.amazonaws.com"
    DW_DB_PORT='3306'
    DW_DB_USER=credentials.get('/glue_job/' + credenv + '/DW_DB_APPUSER_USERNAME', '')
    DW_DB_PASSWORD= credentials.get('/glue_job/' + credenv + '/DW_DB_APPUSER_PASSWORD', '')
    DW_DB_WRITE_SCHEMA='RM_DW_DATA'
    DW_DB_READ_SCHEMA='QP_DW_NAVFACT'
 
    ESB_HOST='p-esb.cz6mmsw6mnag.ap-south-1.rds.amazonaws.com'
    ESB_PORT='3306'
    ESB_USER=credentials.get('/copilot/qp/' + credenv + '/secrets/Qp-Forecast-DB-User', '')
    ESB_PASSWORD=credentials.get('/copilot/qp/' + credenv + '/secrets/Qp-Forecast-DB-Pass', '')
    ESB_READ_SCHEMA='QP_DL_NAVDATA'
 
 
    RM_DB_WR_APPUSER_URL = "prod-rm-db.cz6mmsw6mnag.ap-south-1.rds.amazonaws.com"
    SFTP_RBD_QUEUE_URL = "preprod-rm-allocation-queues_rm-allocation-rbds-ftp"
    ESB_DB_WR_APPUSER_URL = "p-esb.cz6mmsw6mnag.ap-south-1.rds.amazonaws.com"
    ESB_DB_USERNAME = "appuser-rm"
    S3_FILE_PATH = 'prod/'
    S3_BUCKET_NAME = 'qp-applications'
    S3_RM_BUCKET_NAME = "rm-systems"
    FETCH_INPUTS_API_URL = "http://prod-internal-nlb-b7a4a0b9d3b5ddcb.elb.ap-south-1.amazonaws.com/lambda/rm-allocation-serverless-funcs_rm-allocation-fetch-inputs"
    CALCULATE_COMPONENTS_QUEUE_URL = "rm-allocation-queues_rm-allocation-scheduled-jobs"
    NAV_PWD = credentials.get('/rm/' + credenv + '/rm-allocation/NAV_API_PASSWORD', '')
    NAV_OCP_SUBSCRIPTION_KEY = credentials.get('/rm/' + credenv + '/rm-allocation/NAV_API_SUBSCRIPTION_KEY', '')

    ESB_DB_PASSWORD = credentials.get("/rm/" + credenv + "/rm-allocation/ESB_DB_PASSWORD")
    RM_GW_API_KEY = credentials.get('/rm/' + credenv + '/rm-allocation/RM_GW_APIKEY', '')
    FETCH_INPUTS_ACCESS_KEY = credentials.get('/copilot/qp/' + credenv + '/secrets/RM_WEBSERVICE_LAMBDA_ACCESS_KEY', '')
    RM_DB_APPUSER_PASSWORD = credentials.get('/rm/' + credenv + '/rm-allocation/RM_DB_APPUSER_PASSWORD', '')
    
    SFTP_USERNAME = '0KTestr4x_sftp'
    AWS_SQS_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/471112573018/rm-allocation-queues_rm-allocation-rbds-ftp"
    SFTP_PASSWORD = credentials.get('/rm/' + env + '/rm-allocation/NAV_SFTP_PASSWORD', '')
    
    FETCH_INPUTS_LAMBDA = "rm-allocation-serverless-funcs_rm-allocation-fetch-inputs"
    RM_WEBSERVICE_AWS_ACCESS_KEY_NEW_PROD = credentials.get('/copilot/qp/' + credenv + '/secrets/RM_WEBSERVICE_AWS_ACCESS_KEY_NEW_PROD', '')
    RM_WEBSERVICE_AWS_SECRET_KEY_NEW_PROD = credentials.get('/copilot/qp/' + credenv + '/secrets/RM_WEBSERVICE_AWS_SECRET_KEY_NEW_PROD', '')

    SENDER_PASSWORD= credentials.get('/copilot/qp/' + credenv + '/secrets/smtp/QP_ALERTS_PWD')
    
    MONGO_USERNAME = "rmuser"
    MONGO_DB = "qp_dw_rmalloc"
    MONGO_PASSWORD = credentials.get('/rm/' + credenv + '/rm-allocation/RM_MONGO_PASSWORD', '')
    MONGO_URL = "10.16.6.179"
    MONGO_AUTH_SOURCE="admin"

    SENDER_PASSWORD= credentials.get('/copilot/qp/' + credenv + '/secrets/smtp/QP_ALERTS_PWD')
    
    PA_BUCKET_NAME = "qp-s3-fs"
    PA_FILE_PATH = 'prod/PA/'

RM_IT_ALERT_EMAIL = 'closed.rm.italerts@akasaair.com'
SENDER_EMAIL = 'qp.alerts@akasaair.com'
SMTP_SERVER = 'smtp.office365.com'
SMTP_PORT = 587

RM_CACHE_USERNAME = "default"
RM_CACHE_PORT = 6379

RM_DB_SCHEMA_NAME = "QP_DW_RMALLOC"

NAV_USERNAME = "aumaster1"
NAV_DOMAIN = "TOP"


RM_REPORTS_SCHEMA_NAME = "QP_RM_REPORTS"
ESB_NAV_SCHEMA_NAME = "QP_DL_NAVDATA"

S3_PARQUET_FILE_PATH = "inventory-optimization/rateGain/paraquet/"


SFTP_DES_DIR = '/'
SFTP_HOST = 'sftp.akasaair.com'
SFTP_PORT = 10022
SFTP_TARG_FILE = 'AUUPDATE'
TEMP_FILE_NAME = "/tmp/AUUPDATE"
EXP_TIME = 300000
MAX_FILE_SIZE = 40*1024*1024
