import os
import subprocess
from boto3 import client


def get_credentials(parameter_name):
    ssm_client = client('ssm', region_name='ap-south-1')
    response = ssm_client.get_parameter(Name=parameter_name, WithDecryption=True)
    parameter_value = response['Parameter']['Value']
    return parameter_value
            
env = os.getenv('ENVN', default="uat")

if env == "uat":
    RM_DB_URL = "jdbc:mysql://uat-esb-db.cfwwkweqog18.ap-south-1.rds.amazonaws.com:3306/QP_DW_RMALLOC"
    RM_DB_USERNAME = "rm-user"
    FILE_PATH_NAME = "test/rm-admin/"
    S3_BUCKET_NAME = "uat-qp-applications"
    ENV_NAME_LAMBDA = "uat-"
    AWS_ACCOUNT_NUMBER = "891377165721"

if env == "prod":
    RM_DB_URL = "jdbc:mysql://prod-rm-db.cz6mmsw6mnag.ap-south-1.rds.amazonaws.com:3306/QP_DW_RMALLOC"
    RM_DB_USERNAME = "rmappuser"
    FILE_PATH_NAME = "prod/rm-admin/"
    S3_BUCKET_NAME = "qp-applications"
    ENV_NAME_LAMBDA = ""
    AWS_ACCOUNT_NUMBER = "471112573018"

RM_DB_PASSWORD = get_credentials('/rm/' + env + '/rm-allocation/RM_DB_APPUSER_PASSWORD')

# Set environment variables
docker_build_command = f'''docker build --no-cache -t {ENV_NAME_LAMBDA}rm-allocation-s3-inputs-download-image --build-arg RM_DB_PASSWORD="{RM_DB_PASSWORD}" --build-arg RM_DB_URL={RM_DB_URL} --build-arg RM_DB_USERNAME={RM_DB_USERNAME} --build-arg S3_BUCKET_NAME={S3_BUCKET_NAME} --build-arg FILE_PATH_NAME={FILE_PATH_NAME} --build-arg ENVN={env} .'''

docker_tag_command = f"docker tag {ENV_NAME_LAMBDA}rm-allocation-s3-inputs-download-image {AWS_ACCOUNT_NUMBER}.dkr.ecr.ap-south-1.amazonaws.com/{ENV_NAME_LAMBDA}rm-allocation-serverless-funcs-rm-allocation-s3-inputs-download:latest"

docker_push_command = f"docker push {AWS_ACCOUNT_NUMBER}.dkr.ecr.ap-south-1.amazonaws.com/{ENV_NAME_LAMBDA}rm-allocation-serverless-funcs-rm-allocation-s3-inputs-download:latest"

# Execute each Docker command using subprocess
try:
    # Execute docker build command
    subprocess.run(docker_build_command, shell=True, check=True) 
    # Execute docker tag command
    subprocess.run(docker_tag_command, shell=True, check=True)  
    # Execute docker push command
    subprocess.run(docker_push_command, shell=True, check=True)

    print("Docker commands executed successfully!")
except subprocess.CalledProcessError as e:
    print("Error executing Docker command:", e)