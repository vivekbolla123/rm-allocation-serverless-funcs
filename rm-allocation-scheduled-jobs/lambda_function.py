import logging
import boto3
import configsettings
import sys
import json


from sqlalchemy import create_engine
from datetime import datetime

from pushRBDToSFTP import PushRBDtoSFTP
from runConnentions import RunConnections
from allocation_summary import AllocationSummaryReport
from markRunCompleted import MarkRunCompleted
from cleanup import CleanUpConfig
from sftp_error_rate import SFTPErrorRate
from scheduled_job_run_audit import AuditScheduledJob

logger = logging.getLogger()
logger.setLevel(logging.INFO)

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
    
    if 'modules' not in data:
        logger.info("Error! 'modules' key missing in body ")
        return False
    
    modules = data['modules']

    if 'mark_run_completed' in modules:
        markRunCompleted = MarkRunCompleted(logger, rm_rd_conn, rm_wr_conn)

        logger.info("mark_run_completed :: START")

        audit_schedule_run = AuditScheduledJob(rm_wr_conn, logger, 'mark_run_completed', datetime.now())
        markRunCompleted.run()

        logger.info("mark_run_completed :: END")
        audit_schedule_run.add_scheduled_job_audit(datetime.now())
    
    if 'sftp_push' in modules:
        sftp_push = PushRBDtoSFTP(sqs, logger, rm_rd_conn)

        logger.info("sftp_push :: START")

        audit_schedule_run = AuditScheduledJob(rm_wr_conn, logger, 'sftp_push', datetime.now())
        sftp_push.run()

        logger.info("sftp_push :: END")
        audit_schedule_run.add_scheduled_job_audit(datetime.now())
    
    if 'connections' in modules:
        connection = RunConnections(sqs, logger, rm_rd_conn)

        logger.info("connections :: START")
        
        audit_schedule_run = AuditScheduledJob(rm_wr_conn, logger, 'connections', datetime.now())
        connection.run()

        logger.info("connections :: END")
        audit_schedule_run.add_scheduled_job_audit(datetime.now())

    ## All reporting modules
    if 'alloc_summary' in modules:
        alloc_summary = AllocationSummaryReport(rm_rd_conn, rm_wr_conn, logger)
        
        logger.info("alloc_summary :: START")

        audit_schedule_run = AuditScheduledJob(rm_wr_conn, logger, 'alloc_summary', datetime.now())
        alloc_summary.run()

        logger.info("alloc_summary :: END")
        audit_schedule_run.add_scheduled_job_audit(datetime.now())

    if 'clean_up' in modules:
        clean_up = CleanUpConfig(rm_rd_conn, rm_wr_conn, logger)

        logger.info("clean_up :: START")
        
        audit_schedule_run = AuditScheduledJob(rm_wr_conn, logger,'clean_up', datetime.now())
        clean_up.run()
        
        logger.info("clean_up :: END")
        audit_schedule_run.add_scheduled_job_audit(datetime.now())
    
    if 'sftp_error' in modules:
        stfp = SFTPErrorRate(rm_rd_conn, rm_wr_conn, logger)
        logger.info("sftp_error :: START")

        audit_schedule_run = AuditScheduledJob(rm_wr_conn, logger,'sftp_error', datetime.now())
        stfp.run()

        logger.info("sftp_error :: END")
        audit_schedule_run.add_scheduled_job_audit(datetime.now())
        
    return {
        'statusCode': 200,
        'body': 'Cron job executed successfully.'
    }


# DO COMMENT THE BELOW LINE AFTER TESTING
# lambda_handler(None,None)