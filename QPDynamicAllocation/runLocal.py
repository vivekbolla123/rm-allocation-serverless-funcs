import sys,os
path = os.path.abspath("rm-allocation-process-inputs")
sys.path.insert(1,path)
from runAlloc import DynAlloc
from sqlalchemy import create_engine
import pymssql
import Constants
import uuid

def localRun():
    
    NavitaireConnection = pymssql.connect(
        host="43.204.99.181:1433",
        user="crm_navods_read",
        password="P3WjeMw4463nwm8"
        # database="[10.154.12.13,52900][REZ0KDW01]"
    )

    RM_ALLOC_DB_URL = "mysql+mysqldb://"+Constants.DB_USERNAME+":"+Constants.DB_PASSWORD+"@"+Constants.DB_URL
    RM_ALLOC_DB = RM_ALLOC_DB_URL+"/QP_DW_RMALLOC"
    conn = create_engine(RM_ALLOC_DB)
    
    inputData = {
        "debugMode" : False,
        "isUpdateNavitaire" : False,
        "currDate": None,
        "currDateTime": None,
        "MARKET_LIST_QUEUE_URL": None,
        "AU_CHUNK_QUEUE_URL": None,
        "RBD_QUEUE_URL": None,
        "NAV_DB_NAME": "[10.154.12.13,52900].[REZ0KOD01].",
        Constants.CONST_INPUT_SOURCE : Constants.LOCAL_INPUT,
        Constants.CONST_LOGS : Constants.LOCAL_LOGS,
        Constants.CONST_OUTPUT : Constants.LOCAL_OUTPUT,
        "latestCode":True
    }

    currParams = {
        "runId": str(uuid.uuid4()),
        "runSingleMarket": False,
        "NavitaireConnection": NavitaireConnection,
        "dbconn": conn,
        "wrconn": conn,
        "NavitaireUpdateMethod":Constants.CONST_SKIP,
        "sqs": None,
        "s3":None,
    }

    outputData = {}
    DynAlloc(inputData, currParams)
    
localRun()