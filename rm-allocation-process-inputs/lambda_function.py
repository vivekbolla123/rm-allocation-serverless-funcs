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
DB_WR_CONN_STRING = "mysql+mysqldb://rmadmin:8rP#h1HQ5ysM@rm-prod-db.cd7glmg3mmed.ap-south-1.rds.amazonaws.com/QP_DW_RMALLOC"
DB_RD_CONN_STRING = "mysql+mysqldb://rmadmin:8rP#h1HQ5ysM@rm-prod-db-replica.cd7glmg3mmed.ap-south-1.rds.amazonaws.com/QP_DW_RMALLOC"

# Queue details
MARKET_LIST_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/525872668219/rm-allocation-queues_rm-allocation-market-list"
RBD_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/525872668219/rm-allocation-queues_rm-allocation-rbds"

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# create the database connection outside of the handler to allow connections to be
# re-used by subsequent function invocations.
try:
    wrconn = create_engine(DB_WR_CONN_STRING)
    rdconn = create_engine(DB_RD_CONN_STRING)

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
        data["NavitaireConnection"] = NavitaireConnection
        data["dbconn"] = rdconn
        data["wrconn"] = wrconn
        data["sqs"] = sqs
        data["runSingleMarket"] = True

        DynAlloc(input_data, data)

    return {
        'statusCode': 200,
        'body': 'success'
    }

# DO COMMENT THE BELOW LINE AFTER TESTING
# messageBody = {"Origin":"DEL","Destin":"BOM","FlightNumber":1119,"PerType":"D","PerStart":"08/20/2023","PerEnd":"08/25/2023","DOW":1111111,"strategyFlag":"1","TimeWindowStart":"07:00","TimeWindowEnd":"10:30","CurveID":"DELBOM","CarrExlusion":"**","fareAnchor":"Strategy_2","fareOffset":0.0,"FirstRBDAlloc":-99,"OtherRBDAlloc":10,"B2BBackstop":4200,"B2CBackstop":4710,"B2BFactor":"0.8","runId":"a7831b2b-96ad-4b7b-9753-b774d8e19a6a"}
# event = {'Records':[{'body':json.dumps(messageBody)}]}
# lambda_handler(event,None)