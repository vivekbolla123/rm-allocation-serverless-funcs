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
RM_REPORTS_DB = f"mysql+mysqldb://{configsettings.RM_DB_APPUSER_USERNAME}:{configsettings.RM_DB_APPUSER_PASSWORD}@{configsettings.RM_DB_WR_APPUSER_URL}/{configsettings.RM_DB_SCHEMA_NAME}"

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
    
    if 'mark_run_completed' in data:
        markRunCompleted = MarkRunCompleted(logger, rm_rd_conn, rm_wr_conn)
        logger.info("mark_run_completed :: START")
        markRunCompleted.run()
        logger.info("mark_run_completed :: END")
    
    if 'sftp_push' in data:
        sftp_push = PushRBDtoSFTP(sqs, logger, rm_rd_conn)
        logger.info("sftp_push :: START")
        sftp_push.run()
        logger.info("sftp_push :: END")
    
    if 'connections' in data:
        connection = RunConnections(sqs, logger, rm_rd_conn)
        logger.info("connections :: START")
        connection.run()
        logger.info("connections :: END")

    ## All reporting modules
    if 'alloc_summary' in data:
        alloc_summary = AllocationSummaryReport(rm_rd_conn, rm_wr_conn, logger)
        logger.info("alloc_summary :: START")
        alloc_summary.run()
        logger.info("alloc_summary :: END")

    if 'clean_up' in data:
        clean_up = CleanUpConfig(rm_rd_conn, rm_wr_conn, logger)
        logger.info("clean_up :: START")
        clean_up.run()
        logger.info("clean_up :: END")
        
    return {
        'statusCode': 200,
        'body': 'Cron job executed successfully.'
    }


# DO COMMENT THE BELOW LINE AFTER TESTING          
# lambda_handler(None,None)