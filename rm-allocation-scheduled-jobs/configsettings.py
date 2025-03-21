from os import getenv
from boto3 import client


def get_credentials(parameter_name):
    ssm_client = client('ssm', region_name='ap-south-1')
    response = ssm_client.get_parameter(Name=parameter_name, WithDecryption=True)
    parameter_value = response['Parameter']['Value']
    return parameter_value


env = getenv('env', default='uat')

if env == 'prod':
    FETCH_INPUTS_LAMBDA = "rm-allocation-serverless-funcs_rm-allocation-scheduled-jobs"
    RM_DB_APPUSER_USERNAME = "rmappuser"
    RM_DB_APPUSER_URL = "prod-rm-db-replica.cz6mmsw6mnag.ap-south-1.rds.amazonaws.com"
    RM_DB_WR_APPUSER_URL = "prod-rm-db.cz6mmsw6mnag.ap-south-1.rds.amazonaws.com"
    SFTP_RBD_QUEUE_URL = "rm-allocation-queues_rm-allocation-rbds-ftp"
    ESB_DB_WR_APPUSER_URL = "p-esb.cz6mmsw6mnag.ap-south-1.rds.amazonaws.com"
    ESB_DB_USERNAME = "appuser-rm"
    NAV_DB_HOST = "10.154.12.13:52900"
    NAV_DB_NAME = "[REZ0KOD01]."
    NAV_DB_DW_NAME = "[REZ0KDW01]."
    NAV_DB_WB_NAME = "[REZ0KWB01]."
    NAV_API_URL = "https://8trj4yffqf.execute-api.ap-south-1.amazonaws.com/default/navitaire-webservice/v1/queries/output"
    S3_FILE_PATH = 'prod/'
    S3_BUCKET_NAME = 'qp-applications'
    S3_RM_BUCKET_NAME = "rm-systems"
    RM_CACHE_ENDPOINT = "ip-10-16-6-51.ap-south-1.compute.internal"
    FETCH_INPUTS_API_URL = "http://prod-internal-nlb-b7a4a0b9d3b5ddcb.elb.ap-south-1.amazonaws.com/lambda/rm-allocation-serverless-funcs_rm-allocation-fetch-inputs"
    CALCULATE_COMPONENTS_QUEUE_URL = "rm-allocation-queues_rm-allocation-scheduled-jobs"
    MARKET_LIST_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/471112573018/rm-allocation-queues_rm-allocation-market-list"
    NAV_DB_USER = "0KODSUSER2"
    

if env == 'uat':
    FETCH_INPUTS_LAMBDA = "uat-rm-allocation-serverless-funcs-rm-allocation-fetch-inputs"
    RM_DB_APPUSER_USERNAME = "rm-user"
    RM_DB_APPUSER_URL = "uat-esb-db.cfwwkweqog18.ap-south-1.rds.amazonaws.com"
    RM_DB_WR_APPUSER_URL = "uat-esb-db.cfwwkweqog18.ap-south-1.rds.amazonaws.com"
    ESB_DB_WR_APPUSER_URL = "uat-esb-db.cfwwkweqog18.ap-south-1.rds.amazonaws.com"
    ESB_DB_USERNAME = "DBSAAdmin"
    # ESB_DB_WR_APPUSER_URL = "p-esb.cz6mmsw6mnag.ap-south-1.rds.amazonaws.com"
    # ESB_DB_USERNAME = "appuser-rm"
    SFTP_RBD_QUEUE_URL = "test-rm-allocation-queues-rm-allocation-rbds-ftp"
    NAV_DB_HOST = "10.154.12.19:52900"
    NAV_DB_NAME = "[TST0KOD01]."
    NAV_DB_DW_NAME = "[TST0KDW01]."
    NAV_DB_WB_NAME = "[TST0KWB01]."
    NAV_API_URL = "https://aanegtyddf.execute-api.ap-south-1.amazonaws.com/default/navitaire-webservice/v1/queries/output"
    S3_FILE_PATH = 'test/'
    S3_BUCKET_NAME = 'uat-qp-applications'
    S3_RM_BUCKET_NAME = "uat-rm-systems"
    RM_CACHE_ENDPOINT = "ip-172-24-72-8.ap-south-1.compute.internal"
    FETCH_INPUTS_API_URL = "https://uat-intsvc.qp.akasaair.com/rm-allocation-serverless-funcs_rm-allocation-fetch-inputs"
    CALCULATE_COMPONENTS_QUEUE_URL = 'rm-allocation-queues_test-rm-allocation-scheduled-jobs'
    MARKET_LIST_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/891377165721/test-rm-allocation-queues-rm-allocation-market-list"
    NAV_DB_USER = "0KODSUSER"
    

RM_CACHE_USERNAME = "default"
RM_CACHE_PORT = 6379

NAV_DB_PASSWORD = get_credentials('/rm/' + env + '/rm-allocation/NAV_DB_PASSWORD')
ESB_DB_PASSWORD = get_credentials("/rm/" + env + "/rm-allocation/ESB_DB_PASSWORD")
RM_CACHE_PASSWORD = get_credentials('/rm/' + env + '/rm-allocation/RM_CACHE_PASSWORD')
RM_DB_APPUSER_PASSWORD = get_credentials('/rm/' + env + '/rm-allocation/RM_DB_APPUSER_PASSWORD')
RM_GW_API_KEY = get_credentials('/rm/' + env + '/rm-allocation/RM_GW_APIKEY')
FETCH_INPUTS_ACCESS_KEY = get_credentials('/copilot/qp/' + env + '/secrets/RM_WEBSERVICE_LAMBDA_ACCESS_KEY')

RM_DB_SCHEMA_NAME = "QP_DW_RMALLOC"
RM_REPORTS_SCHEMA_NAME = "QP_RM_REPORTS"
ESB_NAV_SCHEMA_NAME = "QP_DL_NAVDATA"

S3_PARQUET_FILE_PATH = "inventory-optimization/rateGain/paraquet/"

EXP_TIME = 120000