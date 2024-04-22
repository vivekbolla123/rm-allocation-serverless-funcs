from os import getenv
from boto3 import client

def get_credentials(parameter_name):
    ssm_client = client('ssm', region_name='ap-south-1')
    response = ssm_client.get_parameter(Name=parameter_name, WithDecryption=True)
    parameter_value = response['Parameter']['Value']
    return parameter_value

env = getenv('env', default=None)

if env == 'prod':
    RM_DB_APPUSER_USERNAME = "rmappuser"
    RM_DB_APPUSER_URL = "prod-rm-db-replica.cz6mmsw6mnag.ap-south-1.rds.amazonaws.com"
    RM_DB_WR_APPUSER_URL = "prod-rm-db.cz6mmsw6mnag.ap-south-1.rds.amazonaws.com"
    connection_queue_url = "rm-allocation-queues_rm-allocation-connections-trigger"
    sftp_rbd_queue_url = "rm-allocation-queues_rm-allocation-rbds-ftp"
    NAV_DB_NAME = "[REZ0KOD01]."
    NAV_API_URL = "https://8trj4yffqf.execute-api.ap-south-1.amazonaws.com/default/navitaire-webservice/v1/queries/output"
    S3_FILE_PATH = 'prod/'
    S3_BUCKET_NAME = 'qp-applications'
    RM_CACHE_ENDPOINT = "ip-10-16-6-51.ap-south-1.compute.internal"
    RM_CACHE_PASSWORD = "bRT__04M})0"

if env == 'uat':
    RM_DB_APPUSER_USERNAME = "rm-user"
    RM_DB_APPUSER_URL = "uat-esb-db.cfwwkweqog18.ap-south-1.rds.amazonaws.com"
    RM_DB_WR_APPUSER_URL = "uat-esb-db.cfwwkweqog18.ap-south-1.rds.amazonaws.com"
    connection_queue_url = "rm-allocation-queues_test-rm-allocation-connections-trigger"
    sftp_rbd_queue_url = "rm-allocation-queues_test-rm-allocation-rbds-ftp"
    NAV_DB_NAME = "[TST0KWB01]."
    NAV_API_URL = "https://aanegtyddf.execute-api.ap-south-1.amazonaws.com/default/navitaire-webservice/v1/queries/output"
    S3_FILE_PATH = 'test/'
    S3_BUCKET_NAME = 'uat-qp-applications'
    RM_CACHE_PASSWORD = get_credentials('/rm/' + env + '/rm-allocation/RM_CACHE_PASSWORD')
    RM_CACHE_ENDPOINT = "ip-172-24-72-8.ap-south-1.compute.internal"

RM_CACHE_USERNAME="default"
RM_CACHE_PORT=6379

RM_GW_API_KEY = get_credentials('/rm/' + env + '/rm-allocation/RM_GW_APIKEY')
RM_DB_APPUSER_PASSWORD = get_credentials('/rm/' + env + '/rm-allocation/RM_DB_APPUSER_PASSWORD')
RM_DB_SCHEMA_NAME = "QP_DW_RMALLOC"
RM_REPORTS_SCHEMA_NAME = "QP_RM_REPORTS"

EXP_TIME = 120000
