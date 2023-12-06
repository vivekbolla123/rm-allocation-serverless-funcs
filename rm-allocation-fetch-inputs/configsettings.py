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
    MARKET_LIST_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/525872668219/rm-allocation-queues_rm-allocation-market-list"
    RM_DB_APPUSER_USERNAME_READ="rmappuser"
    RM_DB_APPUSER_URL_READ="rm-prod-db-replica.cd7glmg3mmed.ap-south-1.rds.amazonaws.com"
    RM_DB_APPUSER_PASSWORD_READ=get_credentials('/rm/prod/rm-allocation/RM_DB_APPUSER_PASSWORD')
    
if env == 'test':
    RM_DB_APPUSER_USERNAME="rm-user"
    RM_DB_APPUSER_URL="esb-db.cd7glmg3mmed.ap-south-1.rds.amazonaws.com"
    MARKET_LIST_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/525872668219/rm-allocation-queues_test-rm-allocation-market-list"
    RM_DB_APPUSER_USERNAME_READ="rmappuser"
    RM_DB_APPUSER_URL_READ="rm-prod-db-replica.cd7glmg3mmed.ap-south-1.rds.amazonaws.com"
    RM_DB_APPUSER_PASSWORD_READ=get_credentials('/rm/prod/rm-allocation/RM_DB_APPUSER_PASSWORD')

RM_DB_SCHEMA_NAME="QP_DW_RMALLOC"
RM_DB_APPUSER_PASSWORD=get_credentials('/rm/' + env + '/rm-allocation/RM_DB_APPUSER_PASSWORD')
