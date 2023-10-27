import logging
import boto3
import configsettings
import sys
import json


from sqlalchemy import create_engine
from pushRBDToSFTP import PushRBDtoSFTP
from runConnentions import RunConnections
from allocation_summary import AllocationSummaryReport
from markRunCompleted import MarkRunCompleted
from cleanup import CleanUpConfig

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

RM_ALLOC_DB = f"mysql+mysqldb://{configsettings.RM_DB_APPUSER_USERNAME}:{configsettings.RM_DB_APPUSER_PASSWORD}@{configsettings.RM_DB_APPUSER_URL}/{configsettings.RM_DB_SCHEMA_NAME}"
RM_REPORTS_DB = f"mysql+mysqldb://{configsettings.RM_DB_APPUSER_USERNAME}:{configsettings.RM_DB_APPUSER_PASSWORD}@{configsettings.RM_DB_WR_APPUSER_URL}/{configsettings.RM_REPORTS_SCHEMA_NAME}"

try:
    rm_rd_conn = create_engine(RM_ALLOC_DB)
    rm_wr_conn = create_engine(RM_REPORTS_DB)
    # Initialize AWS resources
    sqs = boto3.client('sqs')
except Exception as e:
    logger.error(e)
    sys.exit()

def lambda_handler(event, context):
    body = event['body']
    data = json.loads(body)
    
    if data['mark_run_completed']:
        markRunCompleted = MarkRunCompleted(logger, rm_rd_conn, rm_wr_conn)
        markRunCompleted.run()
    
    if data['sftp_push']:
        sftp_push = PushRBDtoSFTP(sqs, logger, rm_rd_conn)
        sftp_push.run()
    
    if data['connections']:
        connection = RunConnections(sqs, logger, rm_rd_conn)
        connection.run()

    ## All reporting modules
    if data['alloc_summary']:
        alloc_summary = AllocationSummaryReport(rm_rd_conn, rm_wr_conn, logger)
        alloc_summary.run()
    
    if data['clean_up']:
        clean_up = CleanUpConfig(rm_rd_conn, rm_wr_conn, logger)
        clean_up.run()
        
    return {
        'statusCode': 200,
        'body': 'Cron job executed successfully.'
    }


# DO COMMENT THE BELOW LINE AFTER TESTING          
# lambda_handler(None,None)