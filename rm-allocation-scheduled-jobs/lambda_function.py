import logging
import boto3
import pandas as pd
import configsettings
import sys
from pushRBDToSFTP import PushRBDtoSFTP
from runConnentions import RunConnections
from allocation_summary import AllocationSummaryReport
from sqlalchemy import create_engine

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

RM_ALLOC_DB = f"mysql+mysqldb://{configsettings.RM_DB_APPUSER_USERNAME}:{configsettings.RM_DB_APPUSER_PASSWORD}@{configsettings.RM_DB_APPUSER_URL}/{configsettings.RM_DB_SCHEMA_NAME}"
RM_REPORTS_DB = f"mysql+pymysql://{configsettings.RM_DB_APPUSER_USERNAME}:{configsettings.RM_DB_APPUSER_PASSWORD}@{configsettings.RM_DB_WR_APPUSER_URL}/{configsettings.RM_REPORTS_SCHEMA_NAME}"

try:
    rm_rd_conn = create_engine(RM_ALLOC_DB)
    rm_wr_conn = create_engine(RM_REPORTS_DB)
    # Initialize AWS resources
    sqs = boto3.client('sqs')
except Exception as e:
    logger.error(e)
    sys.exit()

def lambda_handler(event, context):

    sftp_push = PushRBDtoSFTP(sqs, logger)
    connection = RunConnections(sqs, logger)
    alloc_summary = AllocationSummaryReport(rm_rd_conn, rm_wr_conn, logger)
    # Insert run_ids and other details into allocation_summary_report table
    alloc_summary.create_allocation_summary_report()
    # Fetch runIds from allocation_run_audit table where is_connections_started is null
    query = "SELECT run_id,dtd_start,dtd_end,is_connections_required FROM allocation_run_audit WHERE is_connections_started IS NULL"
    rows = pd.read_sql(query, rm_rd_conn)
    for index,row in rows.iterrows():
        run_id = row["run_id"] 
        dtd_start = row["dtd_start"] 
        dtd_end = row["dtd_end"] 
        is_connections_required = row["is_connections_required"] 
        connection.runConnentions(rm_rd_conn, run_id, dtd_start, dtd_end, is_connections_required)
        
    query = "SELECT run_id FROM allocation_run_audit WHERE update_navitaire_method = 'sftp' and is_sftp_pushed IS NULL "
    rows = pd.read_sql(query, rm_rd_conn)
    for index,row in rows.iterrows():
        run_id = row["run_id"]
        sftp_push.pushRBDToSFTP(rm_rd_conn, run_id)

    query = "SELECT run_id FROM allocation_run_audit WHERE is_run_completed = '0' "
    rows = pd.read_sql(query, rm_rd_conn)
    for index,row in rows.iterrows():
        run_id = row["run_id"]
        checkIfRunCompleted(rm_rd_conn, rm_wr_conn, run_id)
        
    return {
        'statusCode': 200,
        'body': 'Cron job executed successfully.'
    }

def checkIfRunCompleted(rdconn, wrconn, run_id):
    query = f"SELECT COUNT(*) as total FROM run_flight_date_audit WHERE runId = '{run_id}' AND status <> 'Done'"
    count_ready = pd.read_sql(query, rdconn)["total"][0]
    logger.info(f"no of records are not ready  {count_ready}")
    if count_ready == 0:
        # Update the value to 1 if all flight are ready
        update_query = " UPDATE allocation_run_audit SET is_run_completed = %s WHERE run_id = %s "
        wrconn.execute(update_query, ('1', run_id))
        logger.info(f"Updated value for {run_id} since run is completed")

# DO COMMENT THE BELOW LINE AFTER TESTING          
# lambda_handler(None,None)