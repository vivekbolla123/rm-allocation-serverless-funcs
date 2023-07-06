import sys
import logging
import boto3
import json

from uuid import uuid4
from sqlalchemy import create_engine
from pandas import read_sql
from datetime import datetime
from datetime import timedelta
from datetime import date

# sqs setting
sqs = boto3.client('sqs')  # client is required to interact with

# rds settings
# DB_CONN_STRING = "mysql+mysqldb://rmadmin:8rP#h1HQ5ysM@rm-prod-db.cd7glmg3mmed.ap-south-1.rds.amazonaws.com/QP_DW_RMALLOC"
DB_CONN_STRING = "mysql+mysqldb://rmadmin:8rP#h1HQ5ysM@rm-prod-db.cd7glmg3mmed.ap-south-1.rds.amazonaws.com/QP_DW_RMALLOC"
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
    # keys
    start_interval_key = "start_interval"
    end_interval_key = "end_interval"
    queue_key = "queue"
    market_list_key = "market_list_table_name"

    print(event)

    body = event['body']
    # Parse the JSON body
    data = json.loads(body)
    
    # defaults
    start_interval = 30 #0
    end_interval = 90 #365
    market_list_table = "market_list"
    queue_name = MARKET_LIST_QUEUE_URL

    # overrides
    if (queue_key in data):
        queue_name = data[queue_key]

    if (start_interval_key in data):
        start_interval = data[start_interval_key]

    if (end_interval_key in data):
        end_interval = data[end_interval_key]

    if (market_list_key in data):
        market_list_table = data[market_list_key]

    # start program
    curr_date = date.today()
    start_period = datetime.strftime(
        curr_date + timedelta(days=start_interval), "%Y-%m-%d"
    )
    end_period = datetime.strftime(
        curr_date + timedelta(days=end_interval), "%Y-%m-%d"
    )

    market_list_query = f"""
    SELECT Origin,Destin,FlightNumber,PerType, 
    date_format(greatest(str_to_date(PerStart, '%%m/%%d/%%Y'), str_to_date('{start_period}', '%%Y-%%m-%%d')), '%%m/%%d/%%Y') as PerStart, 
    date_format(least(str_to_date(PerEnd, '%%m/%%d/%%Y'), str_to_date('{end_period}', '%%Y-%%m-%%d')), '%%m/%%d/%%Y') as PerEnd, 
    DOW,bookedStatus,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusion,fareAnchor,fareOffset,FirstRBDAlloc,OtherRBDAlloc,B2BBackstop,B2CBackstop FROM {market_list_table} 
    WHERE str_to_date('{start_period}', '%%Y-%%m-%%d') <= str_to_date(PerEnd, '%%m/%%d/%%Y') AND 
    str_to_date('{end_period}', '%%Y-%%m-%%d') >= str_to_date(PerStart, '%%m/%%d/%%Y')
    """

    mkt_list = read_sql(market_list_query, conn)
    markets_count = mkt_list.shape[0]
    logger.info(markets_count)

    start_time = datetime.now()
    run_id = str(uuid4())
    insert_run_start(start_time, run_id)

    for mkt_index in range(markets_count):
        data = mkt_list.iloc[mkt_index]
        data["runId"] = run_id

        data_str = data.to_json()
        logger.info(data_str)

        sqs.send_message(
            QueueUrl=queue_name,
            MessageBody=data_str
        )

    end_time = datetime.now()
    update_run_end(end_time, run_id)

    return {
        'statusCode': 200,
        'body': markets_count
    }


def insert_run_start(start_time, run_id):
    insert_query = " INSERT INTO allocation_run_audit ( run_id, start_time, user) VALUES (%s, %s, %s)"
    conn.execute(insert_query,
                 (run_id, start_time.strftime('%Y-%m-%d %H:%M:%S'), USERNAME))
    logger.info("STARTED ALLOCATION RUN")


def update_run_end(end_time, run_id):
    update_query = " UPDATE allocation_run_audit SET end_time = %s WHERE run_id = %s "
    conn.execute(update_query,
                 (end_time.strftime('%Y-%m-%d %H:%M:%S'), run_id))
    logger.info("STOPPED ALLOCATION RUN")
