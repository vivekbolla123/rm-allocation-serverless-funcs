import sys,os
path = os.path.abspath("rm-allocation-process-inputs")
sys.path.insert(1,path)
import Constants
import datetime
from testcontainers.mysql import MySqlContainer

from sqlalchemy import create_engine
from load_tests_db_data import Schema
from runAlloc import DynAlloc

def testCase1():
    mysql_container = MySqlContainer('mysql:latest')
    mysql_container.get_container_host_ip = lambda: 'localhost'
    mysql_container.start()
    conn = create_engine(mysql_container.get_connection_url())

    inputData = {
        "debugMode": False,
        "isUpdateNavitaire": False,
        "currDate": datetime.date(2023, 8, 1),
        "currDateTime": datetime.datetime(2023, 8, 1, 12, 12, 12),
        "MARKET_LIST_QUEUE_URL": None,
        "AU_CHUNK_QUEUE_URL": None,
        "RBD_QUEUE_URL": None,
        "NAV_DB_NAME": None,
        Constants.CONST_INPUT_SOURCE: Constants.TESTING_INPUT,
        Constants.CONST_LOGS: Constants.TESTING_LOGS,
        Constants.CONST_OUTPUT: Constants.TESTING_OUTPUT,
        "API_KEY": None,
        "API_URL": None,
        "latestCode": False,
        "currentTime": True
    }

    currParams = {
        "runId": "Test12345",
        "runSingleMarket": False,
        "NavitaireConnection": None,
        "dbconn": conn,
        "wrconn": conn,
        "sqs": None,
        "NavitaireUpdateMethod":Constants.CONST_SKIP,
        "s3":None,
        "runTime":None
    }
    outputData = {}

    schema = Schema(conn)
    schema.initializTables()

    DynAlloc(inputData, currParams)

testCase1()
