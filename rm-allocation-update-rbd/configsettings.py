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

    NAV_BASE_URL= "https://dotrezapi.prod.0k.navitaire.com/0k/dotrez"


if env == 'test':
    RM_DB_APPUSER_USERNAME="DBSAAdmin"
    RM_DB_APPUSER_URL="esb-db.cd7glmg3mmed.ap-south-1.rds.amazonaws.com"

    NAV_BASE_URL= "https://qpecomapp-tbnk-reyalrb.qp.akasaair.com/test"

NAV_USERNAME = "aumaster1"
NAV_DOMAIN = "TOP"
RM_DB_APPUSER_PASSWORD=get_credentials('/rm/' + env + '/rm-allocation/RM_DB_APPUSER_PASSWORD')
NAV_PWD = get_credentials('/rm/' + env + '/rm-allocation/NAV_API_PASSWORD')
NAV_OCP_SUBSCRIPTION_KEY = get_credentials('/rm/' + env + '/rm-allocation/NAV_API_SUBSCRIPTION_KEY')

RM_DB_SCHEMA_NAME="QP_DW_RMALLOC"
