import logging
import boto3
import configsettings
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from common_utils import instrument_engine
import sys
import json

from redis import Redis
from datetime import datetime
from pymongo import MongoClient

from constants_utils import ConstantsUtils

from pushRBDToSFTP import PushRBDtoSFTP
from runConnections import RunConnections
from allocation_summary import AllocationSummaryReport
from markRunCompleted import MarkRunCompleted
from run_adhocs_class import SchedulerHandler
from cleanup import CleanUpConfig
from sftp_error_rate import SFTPErrorRate
from scheduled_job_run_audit import AuditScheduledJob
from daily_rolling import DailyRolling
from qcTableGen import QCProcess
from rangeCheck import QCCheck
from calendarUpdate import CalendarUpdate
from noShowExtract import ExpectedNoShows
from schedule30 import ScheduleThirty
from input_audits import S3AuditProcessor
from navOdsAlert import NavOdsAlert
from calendarUpsell import CalUpsell
from defaultMarketlist import defaultMarketlist
from updateImpForecast import update_imputed_forecast
from cugReport import CugReport
from scheduled_run_fail_alert import ScheduledRunFailureAlert
from heuristicCalc import FareStatisticalAnalysis
from run_paused_scheduled_runs import Run_Paused_Runs
from active_sectors import ActiveSectors
from forecast_date_config import ForecastDateConfig
from mlForecastScore import MLForecastScore
from mkt_cleanup import MarketListCleanup

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# Keep ESB and DW connections if still needed
ESB_DB = f"mysql+pymysql://{configsettings.ESB_USER}:{configsettings.ESB_PASSWORD}@{configsettings.ESB_HOST}/{configsettings.ESB_READ_SCHEMA}"
DB_DW_WR_CONN_STRING = f"mysql+pymysql://{configsettings.DW_DB_USER}:{configsettings.DW_DB_PASSWORD}@{configsettings.DW_DB_HOST}/{configsettings.DW_DB_WRITE_SCHEMA}"

CACHE_ENDPOINT = configsettings.RM_CACHE_ENDPOINT
CACHE_PORT = configsettings.RM_CACHE_PORT
CACHE_USERNAME = configsettings.RM_CACHE_USERNAME
CACHE_PASSWORD = configsettings.RM_CACHE_PASSWORD
cache_client = None

try:
    # MongoDB connection (single connection for both read and write)
    mongo_client = MongoClient(
            host=configsettings.MONGO_URL,
            port=27017,
            username=configsettings.MONGO_USERNAME,
            password=configsettings.MONGO_PASSWORD,
            authSource=configsettings.MONGO_AUTH_SOURCE
        )
    mongo_db = mongo_client["qp_dw_rmalloc"]
    
    # Keep other connections if still needed
    rm_rd_esb_conn = create_engine(ESB_DB) if hasattr(configsettings, 'ESB_USER') else None
    dw_conn = create_engine(DB_DW_WR_CONN_STRING) if hasattr(configsettings, 'DW_DB_USER') else None

    NavitaireConnection = create_engine(
        f"mssql+pymssql://{configsettings.NAV_DB_USER}:"
        f"{quote_plus(configsettings.NAV_DB_PASSWORD)}@"
        f"{configsettings.NAV_DB_HOST}",
        pool_pre_ping=True,
        pool_recycle=3600,
        pool_size=5,
        max_overflow=10
    )
    instrument_engine(NavitaireConnection)
    
    cache_client = Redis(host=CACHE_ENDPOINT, port=CACHE_PORT, username=CACHE_USERNAME, password=CACHE_PASSWORD)
    
    # Initialize AWS resources
    sqs = boto3.client('sqs')
    s3 = boto3.client("s3")
    s3_old = boto3.client(
        's3',
        aws_access_key_id=configsettings.RM_WEBSERVICE_AWS_ACCESS_KEY_OLD_PROD,
        aws_secret_access_key=configsettings.RM_WEBSERVICE_AWS_SECRET_KEY_OLD_PROD,
        region_name='ap-south-1'
    )

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
    "daily_rolling": DailyRolling,
    "run_adhocs_queue": SchedulerHandler,
    "gen_qc_fares_table": QCProcess,
    "qc_fares_check": QCCheck,
    "update_calendar": CalendarUpdate,
    "expected_no_shows": ExpectedNoShows,
    "schedule_thirty": ScheduleThirty,
    "input_audits": S3AuditProcessor,
    "navods_alert": NavOdsAlert,
    "calendar_upsell": CalUpsell,
    "default_ml": defaultMarketlist,
    "imputed_forecast": update_imputed_forecast,
    "cug_report": CugReport,
    "schd_run_fail_alert": ScheduledRunFailureAlert,
    "calc": FareStatisticalAnalysis,
    "run_paused_runs": Run_Paused_Runs,
    "active_sectors": ActiveSectors,
    "forecast_date_config": ForecastDateConfig,
    "ml_forecast_score": MLForecastScore,
    "mkt_cleanup": MarketListCleanup
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
        # Single MongoDB connection for both read and write
        params["mongoconn"] = mongo_client
        
        # Keep other connections if still needed
        params["dwconn"] = dw_conn
        params["rdEsbConn"] = rm_rd_esb_conn
        params['navconn'] = NavitaireConnection
        params['sqs'] = sqs
        params['s3'] = s3
        params['s3_old'] = s3_old
        params['cache_client'] = cache_client
        params['events'] = events 
        params['constantutils'] = ConstantsUtils(mongo_db)
        
        Module = module_map[module_key]
        Module_obj = Module(logger, params, overrides, context)

        logger.info(f"{module_key} :: START")
        audit_schedule_run = AuditScheduledJob(mongo_client, logger, module_key, datetime.now())

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
#     "overrides": {
#         "start_time": 2,
#         "end_time": 3
#     },
#     "modules": {"expected_no_shows": 1}
# }


# messageBody = {
#   "body": "{\"modules\":{\"active_sectors\": 1}}"
# }

# event = {'Records':[{'body':json.dumps(messageBody)}]}
# lambda_handler(messageBody,None)