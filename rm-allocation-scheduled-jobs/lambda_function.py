import logging
import boto3
import pymssql
import configsettings
import sys
import json

from redis import Redis
from datetime import datetime
from sqlalchemy import create_engine

from constants_utils import ConstantsUtils

from pushRBDToSFTP import PushRBDtoSFTP
from runConnections import RunConnections
from allocation_summary import AllocationSummaryReport
from markRunCompleted import MarkRunCompleted
from run_adhocs_class import SchedulerHandler
from cleanup import CleanUpConfig
from sftp_error_rate import SFTPErrorRate
from scheduled_job_run_audit import AuditScheduledJob
from pushRBDToS3 import pushRBDToS3
from daily_rolling import DailyRolling
from faresUpdate import Fares
from historicFares import HistoricFaresModule
from profileFares import ProfileFaresModule
from noShowProb import NoShowProbs
from qcTableGen import QCProcess
from rangeCheck import QCCheck
from calendarUpdate import CalendarUpdate

logger = logging.getLogger()
logger.setLevel(logging.INFO)

RM_ALLOC_DB = f"mysql+mysqldb://{configsettings.RM_DB_APPUSER_USERNAME}:{configsettings.RM_DB_APPUSER_PASSWORD}@{configsettings.RM_DB_APPUSER_URL}/{configsettings.RM_DB_SCHEMA_NAME}"
RM_REPORTS_DB = f"mysql+mysqldb://{configsettings.RM_DB_APPUSER_USERNAME}:{configsettings.RM_DB_APPUSER_PASSWORD}@{configsettings.RM_DB_WR_APPUSER_URL}/{configsettings.RM_DB_SCHEMA_NAME}"
ESB_DB = f"mysql+mysqldb://{configsettings.ESB_DB_USERNAME}:{configsettings.ESB_DB_PASSWORD}@{configsettings.ESB_DB_WR_APPUSER_URL}/{configsettings.ESB_NAV_SCHEMA_NAME}"

CACHE_ENDPOINT = configsettings.RM_CACHE_ENDPOINT
CACHE_PORT = configsettings.RM_CACHE_PORT
CACHE_USERNAME = configsettings.RM_CACHE_USERNAME
CACHE_PASSWORD = configsettings.RM_CACHE_PASSWORD
cache_client = None

try:
    rm_rd_conn = create_engine(RM_ALLOC_DB)
    rm_wr_conn = create_engine(RM_REPORTS_DB)
    rm_rd_esb_conn = create_engine(ESB_DB)


    NavitaireConnection = pymssql.connect(
            host=configsettings.NAV_DB_HOST,
            user=configsettings.NAV_DB_USER,
            password=configsettings.NAV_DB_PASSWORD
        )
    
    cache_client = Redis(host=CACHE_ENDPOINT, port=CACHE_PORT, username=CACHE_USERNAME, password=CACHE_PASSWORD)
    # Initialize AWS resources
    sqs = boto3.client('sqs')
    
    s3 = boto3.client("s3")

except Exception as e:
    logger.error(e)
    sys.exit()

module_map = {
    "mark_run_completed": MarkRunCompleted,
    "sftp_push": PushRBDtoSFTP,
    "connections": RunConnections,
    "alloc_summary": AllocationSummaryReport,
    "clean_up": CleanUpConfig,
    "sftp_error": SFTPErrorRate,
    "s3_push": pushRBDToS3,
    "daily_rolling": DailyRolling,
    "fares": Fares,
    "historicFares": HistoricFaresModule,
    "profileFares": ProfileFaresModule,
    "noShowProb" : NoShowProbs,
    "run_adhocs_queue":SchedulerHandler,
    "gen_qc_fares_table": QCProcess,
    "qc_fares_check": QCCheck,
    "update_calendar": CalendarUpdate
}


def lambda_handler(event, context):
    events = boto3.client('events')
    if "Records" in event:
        events = event["Records"]
        for i in events:
            body = i["body"]
    else:
        body = event["body"]

    data = json.loads(body)
    print(data)

    if "modules" not in data:
        logger.info("Error! 'modules' key missing in body ")
        return False

    modules = data["modules"]

    overrides = {}
    params = {}

    if "overrides" in data:
        overrides = data["overrides"]

    for module_key in modules.keys():
        params["rdEsbConn"] = rm_rd_esb_conn
        params['rdconn'] = rm_rd_conn
        params['wrconn'] = rm_wr_conn
        params['navconn'] = NavitaireConnection
        params['sqs'] = sqs
        params['s3'] = s3
        params['cache_client'] = cache_client
        params['events'] = events 
        params['constantutils'] = ConstantsUtils(rm_rd_conn)
        
        Module = module_map[module_key]
        Module_obj = Module(logger, params, overrides, context)

        logger.info(f"{module_key} :: START")
        audit_schedule_run = AuditScheduledJob(rm_wr_conn, logger, module_key, datetime.now())

        Module_obj.run()

        audit_schedule_run.add_scheduled_job_audit(datetime.now())
        logger.info(f"{module_key} :: END")

    return {
        "statusCode": 200, 
        "body": "Cron job executed successfully."
    }


# DO COMMENT THE BELOW LINE AFTER TESTING
# messageBody = {
#     "overrides": {
#         "ndo_bands": [
#             [7, 60],
#             [6, 30],
#             [5, 14],
#             [4, 7],
#             [3, 3],
#             [2, 2],
#             [1, 1],
#             [0, 0],
#         ],
#         "rg_paths": [
#             "inventory-optimization/rateGain/paraquet/Rategain_FareTrend_Apr24.parquet"
#         ],
#         "cluster": '{"cluster":{"0":0,"1":1,"2":2,"3":3,"4":4,"5":5},"startTime":{"0":"02:30:00","1":"07:30:00","2":"11:00:00","3":"14:30:00","4":"18:00:00","5":"21:00:00"},"endTime":{"0":"07:30:00","1":"11:00:00","2":"14:30:00","3":"18:00:00","4":"21:00:00","5":"02:30:00"}}',
#         "sector": "AMDBLR",
#     },
#     "modules": {"historicFares": 1}
# }

# messageBody = {
#   "body": "{\"modules\":{\"update_calendar\": 1}}"
# }

# event = {'Records':[{'body':json.dumps(messageBody)}]}
# lambda_handler(messageBody,None)
