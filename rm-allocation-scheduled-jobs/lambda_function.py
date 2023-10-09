import json
import logging
import boto3
import pandas as pd
import configsettings
from sqlalchemy import create_engine

logger = logging.getLogger()
logger.setLevel(logging.INFO)
# Initialize AWS resources
sqs = boto3.client('sqs')

RM_ALLOC_DB = f"mysql+mysqldb://{configsettings.RM_DB_APPUSER_USERNAME}:{configsettings.RM_DB_APPUSER_PASSWORD}@{configsettings.RM_DB_APPUSER_URL}/{configsettings.RM_DB_SCHEMA_NAME}"
conn = create_engine(RM_ALLOC_DB)

def lambda_handler(event, context):
    
    # Fetch runIds from allocation_run_audit table where is_connections_started is null
    query = "SELECT run_id,dtd_start,dtd_end,is_connections_required FROM allocation_run_audit WHERE is_connections_started IS NULL"
    rows = pd.read_sql(query, conn)
    for index,row in rows.iterrows():
        run_id = row["run_id"] 
        dtd_start = row["dtd_start"] 
        dtd_end = row["dtd_end"] 
        is_connections_required = row["is_connections_required"] 
        runConnentions(conn, run_id, dtd_start, dtd_end, is_connections_required)
        
    query = "SELECT run_id FROM allocation_run_audit WHERE update_navitaire_method = 'sftp' and is_sftp_pushed IS NULL "
    rows = pd.read_sql(query, conn)
    for index,row in rows.iterrows():
        run_id = row["run_id"]
        pushRBDToSFTP(conn, run_id)
        
    return {
        'statusCode': 200,
        'body': 'Cron job executed successfully.'
    }

def pushRBDToSFTP(conn, run_id):
    query2 = f"SELECT COUNT(*) as total FROM run_flight_date_audit WHERE runId = '{run_id}' AND status <> 'Ready'"
    count_ready = pd.read_sql(query2, conn)["total"][0]
    logger.info(f"no of records are not ready  {count_ready}")
    if count_ready == 0:
            # All flights are ready, push run_id to the update_rbd_ftp
        sqs.send_message(
                QueueUrl=configsettings.sftp_rbd_queue_url,
                MessageBody=json.dumps(
                    {
                        'run_id': run_id
                    }),
            )
        logger.info(f"{run_id} is pushed into queue for sftp")

def runConnentions(conn, run_id, dtd_start, dtd_end, is_connections_required):
    if(is_connections_required == 1):
            # Check run_flight_date_audit table if all are done for the current run_id
        query1 = f"SELECT COUNT(*) as total FROM run_flight_date_audit WHERE runId = '{run_id}' AND status <> 'Done'"
        count_done = pd.read_sql(query1, conn)["total"][0]
        if count_done == 0:
                # All flights are done, push run_id to the connection_queue
            sqs.send_message(
                    QueueUrl=configsettings.connection_queue_url,
                    MessageBody=json.dumps(
                        {
                            'run_id': run_id, 
                            'start_interval': dtd_start, 
                            'end_interval': dtd_end, 
                            'run_type': "Scheduled_Connections", 
                            "market_list_table_name": "market_list_connections"
                         }),
                )

# DO COMMENT THE BELOW LINE AFTER TESTING          
# lambda_handler(None,None)