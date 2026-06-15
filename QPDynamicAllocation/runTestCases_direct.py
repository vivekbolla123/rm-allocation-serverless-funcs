import sys,os

log_file = open('test_output.log', 'w')
sys.stdout = log_file
sys.stderr = log_file
# Get the absolute path of the directory
path = os.path.abspath("rm-allocation-process-inputs")
sys.path.append('./rm-allocation-configurations')
sys.path.insert(1, path)
import Constants
import datetime
# from testcontainers.mysql import MySqlContainer

from sqlalchemy import create_engine
from load_tests_db_data import Schema
from runAlloc import DynAlloc
from runAlloc_SalesInv import DynAllocSalesInv
from pymongo import MongoClient

def testCase1():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['rm_database']
    master_db = client['qp_rm_master']
    
    
    inputData = {
        "debugMode": False,
        "isUpdateNavitaire": False,
        "currDate": datetime.date(2023, 8, 1),
        "currDateTime": datetime.datetime(2023, 8, 1, 12, 12, 12),
        "MARKET_LIST_QUEUE_URL": None,
        "RBD_QUEUE_URL": None,
        "NAV_DB_NAME": None,
        "NAV_DB_DW_NAME": None,
        "NAV_DB_WB_NAME": None,
        "NAV_DB_REF_NAME":None,
        "NAV_BASE_API_URL":None,
        "NAV_OCP_SUBSCRIPTION_KEY": None,
        "NAV_USERNAME": None,
        "NAV_PWD": None,
        "NAV_DOMAIN": None,
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
        "dbconn": None,
        "wrconn": None,
        "dwconn":None,
        "mongodbclient":db,
        "mongo_master_db":master_db,
        "sqs": None,
        "NavitaireUpdateMethod":Constants.S3,
        "s3":None,
        "runTime":None,
        "cache_client":None,
        "route_type":"direct",
        "timelogger":None,
    }
    outputData = {}

    schema = Schema(db, master_db)
    schema.initializTables()

    DynAlloc(inputData, currParams)
    # DynAllocSalesInv(inputData, currParams)

testCase1()
