import sys
import logging

import json
from sqlalchemy import create_engine
import pymssql

import boto3

import Constants
from runAlloc import DynAlloc

# sqs setting
sqs = boto3.client('sqs')  # client is required to interact with

# DB settings
TABLE_MARKET_FARES = "market_fares"
WAIT_TIME = 30  # in seconds
NAV_DB_NAME = "[10.154.12.13,52900].[REZ0KOD01]."

LOGS_ON = False

# rds settings
# DB_CONN_STRING = "mysql+mysqldb://root:root@localhost/QP_DW_RMALLOC"
DB_CONN_STRING = "mysql+mysqldb://rmadmin:8rP#h1HQ5ysM@rm-prod-db.cd7glmg3mmed.ap-south-1.rds.amazonaws.com/QP_DW_RMALLOC"

# Queue details
MARKET_LIST_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/525872668219/rm-allocation-queues_rm-allocation-market-list"
RBD_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/525872668219/rm-allocation-queues_rm-allocation-rbds"

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# create the database connection outside of the handler to allow connections to be
# re-used by subsequent function invocations.
try:
    conn = create_engine(DB_CONN_STRING)
    NavitaireConnection = pymssql.connect(
        host="43.204.99.181:1433",
        user="crm_navods_read",
        password="P3WjeMw4463nwm8"
        # database="[10.154.12.13,52900][REZ0KDW01]"
    )
except Exception as e:
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")


def lambda_handler(event, context):
    """
    This function fetches the market_list and inserts it into the queue
    """
    records = event['Records']

    input_data = {
        "debugMode" : False,
        "isUpdateNavitaire" : True,
        "currDate": None,
        "currDateTime": None,
        "MARKET_LIST_QUEUE_URL": MARKET_LIST_QUEUE_URL,
        "RBD_QUEUE_URL": RBD_QUEUE_URL,
        "NAV_DB_NAME": NAV_DB_NAME,
        Constants.CONST_INPUT_SOURCE : Constants.SERVER_INPUT,
        Constants.CONST_LOGS : Constants.SERVER_LOGS,
        Constants.CONST_OUTPUT : Constants.SERVER_OUTPUT
    }

    for record in records:
        content = record['body']
        data = json.loads(content)
        DynAlloc(input_data, data, NavitaireConnection, conn, sqs)

    return {
        'statusCode': 200,
        'body': 'success'
    }
