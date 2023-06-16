import sys
import logging
import boto3
from uuid import uuid4
from sqlalchemy import create_engine
from pandas import read_sql
from datetime import datetime


# sqs setting
sqs = boto3.client('sqs')  # client is required to interact with

# rds settings
# DB_CONN_STRING = "mysql+mysqldb://rmadmin:8rP#h1HQ5ysM@rm-prod-db.cd7glmg3mmed.ap-south-1.rds.amazonaws.com/QP_DW_RMALLOC"
DB_CONN_STRING = "mysql+mysqldb://dbadmin_prod:cxvyFv^^e4bd7b33@p-esb.cd7glmg3mmed.ap-south-1.rds.amazonaws.com/QP_DW_RMALLOC"
MARKET_LIST_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/525872668219/rm-allocation-queues_rm-allocation-market-list"
USERNAME = "SYS2"

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# create the database connection outside of the handler to allow connections to be
# re-used by subsequent function invocations.
try:
    conn = create_engine(DB_CONN_STRING)
except Exception as e:
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")


def lambda_handler(event, context):
    """
    This function fetches the market_list and inserts it into the queue
    """
    market_list_query = "SELECT Origin,Destin,FlightNumber,PerType,PerStart,PerEnd,DOW,bookedStatus,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusion,fareAnchor,fareOffset,FirstRBDAlloc,OtherRBDAlloc,B2BBackstop,B2CBackstop FROM market_list "

    mkt_list = read_sql(market_list_query, conn)
    markets_count = mkt_list.shape[0]
    logger.info(markets_count)

    start_time = datetime.now()
    run_id = str(uuid4())
    insertRunStart(start_time, run_id)

    for mkt_index in range(markets_count):
        data = mkt_list.iloc[mkt_index]
        data["runId"] = run_id

        data_str = data.to_json()
        logger.info(data_str)

        sqs.send_message(
            QueueUrl=MARKET_LIST_QUEUE_URL,
            MessageBody=data_str
        )

    end_time = datetime.now()
    updateRunEnd(end_time, run_id)

    return {
        'statusCode': 200,
        'body': markets_count
    }


def insertRunStart(startTime, runId):
    insertQuery = " INSERT INTO allocation_run_audit ( run_id, start_time, user) VALUES (%s, %s, %s)"
    conn.execute(insertQuery,
                 (runId, startTime.strftime('%Y-%m-%d %H:%M:%S'), USERNAME))
    logger.info("STARTED ALLOCATION RUN")


def updateRunEnd(endTime, runId):
    updateQuery = " UPDATE allocation_run_audit SET end_time = %s WHERE run_id = %s "
    conn.execute(updateQuery,
                 (endTime.strftime('%Y-%m-%d %H:%M:%S'), runId))
    logger.info("STOPPED ALLOCATION RUN")
