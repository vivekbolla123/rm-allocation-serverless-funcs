from os import getenv
from boto3 import client

def get_credentials(parameter_name):
    ssm_client = client('ssm', region_name='ap-south-1')
    response = ssm_client.get_parameter(Name=parameter_name, WithDecryption=True)
    parameter_value = response['Parameter']['Value']
    return parameter_value

env = getenv('env', default=None)

if env == 'prod':
    RM_DB_APPUSER_USERNAME="rmappuser"
    RM_DB_APPUSER_URL="rm-prod-db.cd7glmg3mmed.ap-south-1.rds.amazonaws.com"
    SFTP_USERNAME = '0K_MFT'
    SFTP_DES_DIR = '/PROD/ESC'
    AWS_SQS_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/525872668219/rm-allocation-queues_rm-allocation-au-chunks"
    
if env == 'test':
    RM_DB_APPUSER_USERNAME="DBSAAdmin"
    RM_DB_APPUSER_URL="esb-db.cd7glmg3mmed.ap-south-1.rds.amazonaws.com"
    SFTP_USERNAME = '0KTestr4x_sftp'
    SFTP_DES_DIR = '/'
    AWS_SQS_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/525872668219/rm-allocation-queues_test-rm-allocation-au-chunks"


RM_DB_APPUSER_PASSWORD=get_credentials('/rm/' + env + '/rm-allocation/RM_DB_APPUSER_PASSWORD')
SFTP_PASSWORD = get_credentials('/rm/' + env + '/rm-allocation/NAV_SFTP_PASSWORD')

RM_DB_SCHEMA_NAME="QP_DW_RMALLOC"

SFTP_HOST = 'sftp.akasaair.com'
SFTP_PORT = 10022
SFTP_TARG_FILE = 'AUUPDATE'
TEMP_FILE_NAME="/tmp/AUUPDATE"
