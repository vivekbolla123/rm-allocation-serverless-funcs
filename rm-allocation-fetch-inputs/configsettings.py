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
    RM_DB_APPUSER_URL="prod-rm-db.cz6mmsw6mnag.ap-south-1.rds.amazonaws.com"
    MARKET_LIST_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/471112573018/rm-allocation-queues_rm-allocation-market-list"
    RM_DB_APPUSER_USERNAME_READ="rmappuser"
    RM_DB_APPUSER_URL_READ="prod-rm-db-replica.cz6mmsw6mnag.ap-south-1.rds.amazonaws.com"
    RM_DB_APPUSER_PASSWORD_READ=get_credentials('/rm/prod/rm-allocation/RM_DB_APPUSER_PASSWORD')
    RM_CACHE_ENDPOINT = "ip-10-16-6-51.ap-south-1.compute.internal"
    RM_CACHE_PASSWORD = "bRT__04M})0"

if env == 'uat':
    RM_DB_APPUSER_USERNAME="rm-user"
    RM_DB_APPUSER_URL="uat-esb-db.cfwwkweqog18.ap-south-1.rds.amazonaws.com"
    MARKET_LIST_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/891377165721/test-rm-allocation-queues-rm-allocation-market-list"
    RM_DB_APPUSER_USERNAME_READ="rm-user"
    RM_DB_APPUSER_URL_READ="uat-esb-db.cfwwkweqog18.ap-south-1.rds.amazonaws.com"
    RM_DB_APPUSER_PASSWORD_READ=get_credentials('/rm/uat/rm-allocation/RM_DB_APPUSER_PASSWORD')
    # RM_DB_APPUSER_USERNAME_READ="rmappuser"
    # RM_DB_APPUSER_URL_READ="prod-rm-db-replica.cz6mmsw6mnag.ap-south-1.rds.amazonaws.com"
    # RM_DB_APPUSER_PASSWORD_READ=get_credentials('/rm/prod/rm-allocation/RM_DB_APPUSER_PASSWORD')
    RM_CACHE_PASSWORD = get_credentials('/rm/' + env + '/rm-allocation/RM_CACHE_PASSWORD')
    RM_CACHE_ENDPOINT = "ip-172-24-72-8.ap-south-1.compute.internal"

RM_CACHE_USERNAME = "default"
RM_CACHE_PORT = 6379

RM_DB_SCHEMA_NAME = "QP_DW_RMALLOC"
RM_DB_APPUSER_PASSWORD = get_credentials('/rm/' + env + '/rm-allocation/RM_DB_APPUSER_PASSWORD')
