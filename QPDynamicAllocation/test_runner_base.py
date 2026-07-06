import sys, os
import io
import contextlib
import datetime
from pymongo import MongoClient
from sqlalchemy import create_engine
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

console = Console()

path = os.path.abspath("rm-allocation-process-inputs")
sys.path.append('./rm-allocation-configurations')
sys.path.insert(1, path)
import Constants

from load_tests_db_data import Schema
from test_runner_utils import run_with_animation
from runAlloc import DynAlloc
from runAlloc_Connections import DynAllocConnections

def run_test_cases(run_direct=False, run_connections=False):
    DB_WR_CONN_STRING = f"mysql+pymysql://root:12345678@localhost/localtest"
    conn = create_engine(DB_WR_CONN_STRING)
    
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
        "NAV_DB_REF_NAME": None,
        "NAV_BASE_API_URL": None,
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
        "dbconn": conn,
        "wrconn": conn,
        "dwconn": conn,
        "mongodbclient": db,
        "mongo_master_db": master_db,
        "sqs": None,
        "NavitaireUpdateMethod": Constants.S3,
        "s3": None,
        "runTime": None,
        "cache_client": None,
        "route_type": "direct",
        "timelogger": None,
    }

    schema = Schema(db, master_db)
    
    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), transient=True, console=console) as p:
        p.add_task(description="Loading tables to database...", total=None)
        with contextlib.redirect_stdout(io.StringIO()):
            schema.initializTables()
            
    console.print("[green]✔ All tables loaded to database![/green]")

    base_dir = os.path.dirname(os.path.abspath(__file__))

    if run_direct:
        currParams["route_type"] = "direct"
        run_with_animation("Processing for direct", DynAlloc, os.path.join(base_dir, 'logs', 'direct_output.log'), inputData, currParams)
        
    if run_connections:
        currParams["route_type"] = "connections"
        run_with_animation("Processing for connections", DynAllocConnections, os.path.join(base_dir, 'logs', 'connections_output.log'), inputData, currParams)

