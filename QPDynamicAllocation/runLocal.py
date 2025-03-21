import sys,os
path = os.path.abspath("rm-allocation-process-inputs")
sys.path.insert(1,path)
from runAlloc import DynAlloc
from sqlalchemy import create_engine
import pymssql
import Constants
import uuid
from datetime import datetime

def localRun():
    
    NavitaireConnection = pymssql.connect(
        host="10.154.12.13:52900",
        user="0KODSUSER",
        password="fruFR79on7TR"
        # database="[10.154.12.13,52900][REZ0KDW01]"
    )

    RM_ALLOC_DB_URL = "mysql+mysqldb://"+Constants.DB_USERNAME+":"+Constants.DB_PASSWORD+"@"+Constants.DB_URL
    RM_ALLOC_DB = RM_ALLOC_DB_URL+"/QP_DW_RMALLOC"
    conn = create_engine(RM_ALLOC_DB)
    
    inputData = {
        "debugMode" : False,
        "isUpdateNavitaire" : True,
        "currDate": None,
        "currDateTime": None,
        "MARKET_LIST_QUEUE_URL": None,
        "RBD_QUEUE_URL": None,
        "NAV_DB_NAME": "[REZ0KOD01].",
        "NAV_DB_DW_NAME": "[REZ0KDW01].",
        "NAV_DB_WB_NAME": "[REZ0KWB01].",
        "NAV_BASE_API_URL":None,
        "NAV_OCP_SUBSCRIPTION_KEY": None,
        "NAV_USERNAME": None,
        "NAV_PWD": None,
        "NAV_DOMAIN": None,
        Constants.CONST_INPUT_SOURCE : Constants.LOCAL_INPUT,
        Constants.CONST_LOGS : Constants.LOCAL_LOGS,
        Constants.CONST_OUTPUT : Constants.LOCAL_OUTPUT,
        "API_KEY": None,
        "API_URL": None,
        "latestCode":True,
        "currentTime": False
    }

    currParams = {
        "runId": str(uuid.uuid4()),
        "runSingleMarket": False,
        "NavitaireConnection": NavitaireConnection,
        "dbconn": conn,
        "wrconn": conn,
        "NavitaireUpdateMethod":Constants.S3,
        "sqs": None,
        "cache_client":None,
        "s3":None,
        "runTime":None
    }

    outputData = {}
    DynAlloc(inputData, currParams)
    
localRun()