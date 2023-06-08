import sys
import logging
import boto3

import pandas as pd
from sqlalchemy import create_engine


# sqs setting
sqs = boto3.client('sqs')  # client is required to interact with

# rds settings
DB_CONN_STRING = "mysql+mysqldb://dbadmin_prod:cxvyFv^^e4bd7b33@p-esb.cd7glmg3mmed.ap-south-1.rds.amazonaws.com/QP_DW_RMALLOC"
MARKET_LIST_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/525872668219/rm-allocation-queues_rm-allocation-market-list"

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
    market_list_query = "SELECT Origin,Destin,FlightNumber,PerType,PerStart,PerEnd,DOW,bookedStatus,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusion,fareAnchor,fareOffset,FirstRBDAlloc,OtherRBDAlloc FROM market_list "

    mkt_list = pd.read_sql(market_list_query, conn)
    markets_count = mkt_list.shape[0]
    logger.info(markets_count)

    for mkt_index in range(markets_count):
        data = mkt_list.iloc[mkt_index].to_json()
        logger.info(data)

        sqs.send_message(
            QueueUrl=MARKET_LIST_QUEUE_URL,
            MessageBody=data
        )

    return {
        'statusCode': 200,
        'body': markets_count
    }
