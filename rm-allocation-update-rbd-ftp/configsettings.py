import boto3

def get_credentials(parameter_name):
    ssm_client = boto3.client('ssm', region_name='ap-south-1')
    response = ssm_client.get_parameter(Name=parameter_name, WithDecryption=True)
    parameter_value = response['Parameter']['Value']
    return parameter_value


RM_DB_APPUSER_PASSWORD=get_credentials('/rm/prod/rm-allocation/RM_DB_APPUSER_PASSWORD')
RM_DB_APPUSER_USERNAME="rmappuser"
RM_DB_APPUSER_URL="rm-prod-db.cd7glmg3mmed.ap-south-1.rds.amazonaws.com"
RM_DB_SCHEMA_NAME="QP_DW_RMALLOC"


SFTP_HOST = 'sftp.akasaair.com'
SFTP_PORT = 10022
SFTP_USERNAME = '0K_MFT'
SFTP_PASSWORD = get_credentials('/rm/prod/rm-allocation/NAV_SFTP_PASSWORD')
SFT_DES_DIR = '/PROD/ESC'
SFTP_TARG_FILE = 'AUUPDATE.csv'

AWS_BUCKET_NAME = 'qp-applications'
QUEUE_NAME = "rm-allocation-queues_rm-allocation-au-chunks"
AWS_SQS_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/525872668219/rm-allocation-queues_rm-allocation-au-chunks"
AWS_BUCKET_CHUNKS_PATH="prod/rm-au-chunks"