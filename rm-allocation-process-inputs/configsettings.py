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
    RM_REPLICA_DB_APPUSER_URL="rm-prod-db-replica.cd7glmg3mmed.ap-south-1.rds.amazonaws.com"
    NAV_DB_NAME = "[10.154.12.13,52900].[REZ0KOD01]."
    MARKET_LIST_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/525872668219/rm-allocation-queues_rm-allocation-market-list"
    RBD_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/525872668219/rm-allocation-queues_rm-allocation-rbds"
    AUCHUNKS_QUEUE_URL="https://sqs.ap-south-1.amazonaws.com/525872668219/rm-allocation-queues_rm-allocation-au-chunks"
if env == 'test':
    RM_DB_APPUSER_USERNAME="DBSAAdmin"
    RM_DB_APPUSER_URL="esb-db.cd7glmg3mmed.ap-south-1.rds.amazonaws.com"
    RM_REPLICA_DB_APPUSER_URL="esb-db.cd7glmg3mmed.ap-south-1.rds.amazonaws.com"
    NAV_DB_NAME = "[10.154.12.19,52900].[TST0KOD01]."
    MARKET_LIST_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/525872668219/rm-allocation-queues_test-rm-allocation-market-list"
    RBD_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/525872668219/rm-allocation-queues_test-rm-allocation-rbds"
    AUCHUNKS_QUEUE_URL="https://sqs.ap-south-1.amazonaws.com/525872668219/rm-allocation-queues_test-rm-allocation-au-chunks"

RM_DB_SCHEMA_NAME="QP_DW_RMALLOC"
NAV_DB_HOST="43.204.99.181:1433"
NAV_DB_USER="crm_navods_read"
NAV_DB_PASSWORD=get_credentials('/rm/' + env + '/rm-allocation/NAV_DB_PASSWORD')
RM_DB_APPUSER_PASSWORD=get_credentials('/rm/' + env + '/rm-allocation/RM_DB_APPUSER_PASSWORD')
