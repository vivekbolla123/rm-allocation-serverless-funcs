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

NAV_BASE_URL= "https://dotrezapi.prod.0k.navitaire.com/0k/dotrez"
NAV_USERNAME = "aumaster1"
NAV_PWD = get_credentials('/rm/prod/rm-allocation/NAV_API_PASSWORD')
NAV_DOMAIN = "TOP"
NAV_OCP_SUBSCRIPTION_KEY = get_credentials('/rm/prod/rm-allocation/NAV_API_SUBSCRIPTION_KEY')