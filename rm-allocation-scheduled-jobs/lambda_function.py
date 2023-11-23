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

module_map = { "mark_run_completed": MarkRunCompleted, 'sftp_push': PushRBDtoSFTP, 'connections': RunConnections, 
              'alloc_summary': AllocationSummaryReport, 'clean_up': CleanUpConfig, 'sftp_error': SFTPErrorRate
              }

def lambda_handler(event, context):
    body = event['body']
    data = json.loads(body)
    
    if 'modules' not in data:
        logger.info("Error! 'modules' key missing in body ")
        return False
    
    modules = data['modules']

    overrides ={}
    params={}

    if 'overrides' in data:
        overrides = data['overrides']

    for module_key in modules.keys():
        params['rdconn'] = rm_rd_conn
        params['wrconn'] = rm_wr_conn
        params['sqs'] = sqs

        Module = module_map[module_key]
        Module_obj = Module(logger, params, overrides)

        logger.info(f"{module_key} :: START")
        audit_schedule_run = AuditScheduledJob(rm_wr_conn, logger, module_key, datetime.now())
        
        Module_obj.run()

        audit_schedule_run.add_scheduled_job_audit(datetime.now())
        logger.info(f"{module_key} :: END")
        
    return {
        'statusCode': 200,
        'body': 'Cron job executed successfully.'
    }


# DO COMMENT THE BELOW LINE AFTER TESTING
# lambda_handler(None,None)