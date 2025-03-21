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
    RM_DB_APPUSER_URL = "prod-rm-db.cz6mmsw6mnag.ap-south-1.rds.amazonaws.com"
    SFTP_USERNAME = '0KProdr4x_sftp'
    AWS_SQS_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/471112573018/rm-allocation-queues_rm-allocation-rbds-ftp"
    RM_CACHE_ENDPOINT = "ip-10-16-6-51.ap-south-1.compute.internal"
    RM_CACHE_PASSWORD = get_credentials('/rm/' + env + '/rm-allocation/RM_CACHE_PASSWORD')
       
if env == 'uat':
    RM_DB_APPUSER_USERNAME = "rm-user"
    RM_DB_APPUSER_URL = "uat-esb-db.cfwwkweqog18.ap-south-1.rds.amazonaws.com"
    SFTP_USERNAME = '0KTestr4x_sftp'
    AWS_SQS_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/891377165721/test-rm-allocation-queues-rm-allocation-rbds-ftp"
    RM_CACHE_PASSWORD = get_credentials('/rm/' + env + '/rm-allocation/RM_CACHE_PASSWORD')
    RM_CACHE_ENDPOINT = "ip-172-24-72-8.ap-south-1.compute.internal"

RM_DB_APPUSER_PASSWORD = get_credentials('/rm/' + env + '/rm-allocation/RM_DB_APPUSER_PASSWORD')
RM_DB_SCHEMA_NAME = "QP_DW_RMALLOC"

RM_CACHE_USERNAME = "default"
RM_CACHE_PORT = 6379

SFTP_PASSWORD = get_credentials('/rm/' + env + '/rm-allocation/NAV_SFTP_PASSWORD')
SFTP_DES_DIR = '/'
SFTP_HOST = 'sftp.akasaair.com'
SFTP_PORT = 10022
SFTP_TARG_FILE = 'AUUPDATE'
TEMP_FILE_NAME = "/tmp/AUUPDATE"
EXP_TIME = 300000
MAX_FILE_SIZE = 40*1024*1024