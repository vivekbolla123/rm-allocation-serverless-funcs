
import logging
import boto3
import json
import pymssql
from sqlalchemy import create_engine
from datetime import datetime
import os
import sys

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from ml_forecasting import MLForecasting

# Setup Logging
def create_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(levelname)-8s - %(message)s  ")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


logger = create_logger()

# DB Credentials (should be in env vars or configsettings)
# Assuming typical setup or placeholders for now
try:
    import configsettings
except ImportError:
    # Handle if configsettings is not in path, purely for local or docker simulation
    class ConfigSettings:
        DW_DB_USER = os.getenv("DW_DB_USER", "user")
        DW_DB_PASSWORD = os.getenv("DW_DB_PASSWORD", "pass")
        DW_DB_HOST = os.getenv("DW_DB_HOST", "host")
        DW_DB_WRITE_SCHEMA = os.getenv("DW_DB_WRITE_SCHEMA", "schema")
        ESB_USER = os.getenv("ESB_USER", "esb_user")
        ESB_PASSWORD = os.getenv("ESB_PASSWORD", "esb_pass")
        ESB_HOST = os.getenv("ESB_HOST", "esb_host")
        ESB_READ_SCHEMA = os.getenv("ESB_READ_SCHEMA", "esb_schema")
    configsettings = ConfigSettings()

def lambda_handler(event, context):
    try:
        if "body" in event:
            body = json.loads(event["body"])
        else:
            body = event # Direct invocation
            
        logger.info(f"Received event: {body}")
        
        modules = body.get("modules", {})
        overrides = body.get("overrides", {})
        
        if "ml_forecasting" not in modules:
            logger.info("ml_forecasting module not requested. Exiting.")
            return {"statusCode": 200, "body": "Skipped"}
            
        # Setup Resources
        s3 = boto3.client("s3")
        
        # DB Connection
        conn_str = f"mysql+pymysql://Aarav_d:Aardby#1345@prod-qp-dw.cz6mmsw6mnag.ap-south-1.rds.amazonaws.com/RM_DW_DATA"
        dwconn = create_engine(conn_str)
        
        esb_conn_str = f"mysql+pymysql://comm_dl:7^yq*N7Wm76y8ccu@p-esb.cz6mmsw6mnag.ap-south-1.rds.amazonaws.com/QP_DL_NAVDATA"
        esbconn = create_engine(esb_conn_str)
        
        params = {
            "dwconn": dwconn,
            "esbconn": esbconn,
            "s3": s3
        }
        
        # Run ML Forecasting
        ml_module = MLForecasting(logger, params, overrides, context)
        ml_module.run()
        
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "ML Forecasting executed successfully"})
        }
        
    except Exception as e:
        logger.error(f"Lambda execution failed: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

# DO COMMENT THE BELOW LINE AFTER TESTING
messageBody = {
    "modules": {"ml_forecasting": 1},
    "overrides": {"action": "train"} 
}
lambda_handler(messageBody, None)

# messageBody = {
#     "modules": {"ml_forecasting": 1},
#     "overrides": {"action": "score"} 
# }
# lambda_handler(messageBody, None)
