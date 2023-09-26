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

RM_REPLICA_DB_APPUSER_URL="rm-prod-db-replica.cd7glmg3mmed.ap-south-1.rds.amazonaws.com"

NAV_DB_NAME = "[10.154.12.13,52900].[REZ0KOD01]."

NAV_DB_HOST="43.204.99.181:1433"
NAV_DB_USER="crm_navods_read"
NAV_DB_PASSWORD=get_credentials("/rm/prod/rm-allocation/NAV_DB_PASSWORD")