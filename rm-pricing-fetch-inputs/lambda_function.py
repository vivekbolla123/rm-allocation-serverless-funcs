"""
rm-pricing-fetch-inputs — Orchestrator Lambda for ladder generation.

Reads the market list collection for the given route, splits ladder codes
into batches, and sends one SQS message per batch to rm-pricing-triggers.
Each message triggers a parallel Lambda invocation that processes its batch.
"""

import sys
import logging
import boto3
import json
import uuid
import math

from pymongo import MongoClient
import configsettings

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Connections (established once at cold start)
# ---------------------------------------------------------------------------
try:
    mongo_client = MongoClient(
        host=configsettings.MONGO_URL,
        port=27017,
        username=configsettings.MONGO_USERNAME,
        password=configsettings.MONGO_PASSWORD,
        authSource=configsettings.MONGO_AUTH_SOURCE,
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=10000,
        socketTimeoutMS=10000,
        retryWrites=True,
        retryReads=True,
    )
    mongo_db = mongo_client["qp_dw_priaut"]

    sqs = boto3.client('sqs')

    logger.info("SUCCESS: All connections established")
except Exception as e:
    logger.error(f"Failed to establish connections: {str(e)}")
    sys.exit()

# ---------------------------------------------------------------------------
# Route -> MongoDB collection mapping
# ---------------------------------------------------------------------------
COLLECTION_MAP = {
    'D':   'market_list',
    'I':   'market_list_international',
    'CNX': 'market_list_connections',
}

# Default batch size (number of ladder codes per SQS message)
DEFAULT_BATCH_SIZE = 5


# ---------------------------------------------------------------------------
# Lambda Handler
# ---------------------------------------------------------------------------
def lambda_handler(event, context):
    """
    Reads the market list for the given route, splits into batches,
    and dispatches SQS messages to rm-pricing-triggers.

    Expected input (direct invoke or API Gateway):
    {
        "route": "D",                   # D, I, or CNX
        "analyst": "Kenneth Menezes",
        "batch_size": 5                 # optional, defaults to 5
    }

    Can also be triggered via SQS (Records wrapper) or API Gateway (body wrapper).
    """
    # --- Parse event ---
    if "Records" in event:
        body = event["Records"][0]["body"]
    elif "body" in event:
        body = event["body"]
    else:
        body = json.dumps(event)

    data = json.loads(body)
    logger.info(f"Received: {json.dumps(data)}")

    route = data.get("route")
    analyst = data.get("analyst")
    batch_size = int(data.get("batch_size", DEFAULT_BATCH_SIZE))

    if not route or not analyst:
        logger.error("Missing required fields: 'route' and 'analyst'")
        return {"statusCode": 400, "body": "Missing 'route' or 'analyst'"}

    # --- Read ladder codes from MongoDB ---
    collection_name = COLLECTION_MAP.get(route)
    if not collection_name:
        logger.error(f"Unknown route '{route}'. Expected: {list(COLLECTION_MAP.keys())}")
        return {"statusCode": 400, "body": f"Unknown route '{route}'"}

    docs = list(mongo_db[collection_name].find({}, {"LadderCode": 1, "_id": 0}))
    ladder_codes = [
        str(doc["LadderCode"]).strip()
        for doc in docs
        if doc.get("LadderCode") and str(doc["LadderCode"]).strip()
    ]

    if not ladder_codes:
        logger.warning(f"No ladder codes found in '{collection_name}'")
        return {"statusCode": 200, "body": "No ladder codes found"}

    logger.info(f"Found {len(ladder_codes)} ladder codes in '{collection_name}'")

    # --- Generate shared run_id ---
    run_id = str(uuid.uuid4())

    # --- Split into batches ---
    total_batches = math.ceil(len(ladder_codes) / batch_size)
    batches = [
        ladder_codes[i:i + batch_size]
        for i in range(0, len(ladder_codes), batch_size)
    ]

    # --- Get target queue URL ---
    queue_url = configsettings.PRICING_TRIGGERS_QUEUE_URL
    if not queue_url:
        logger.error("PRICING_TRIGGERS_QUEUE_URL not configured")
        return {"statusCode": 500, "body": "Queue URL not configured"}

    # --- Send one SQS message per batch ---
    for idx, batch in enumerate(batches):
        message = {
            "route": route,
            "analyst": analyst,
            "run_id": run_id,
            "batch_index": idx + 1,
            "batch_total": total_batches,
            "ladder_codes": batch,
            "modules": {"ladder_gen": 1}
        }

        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message)
        )

        logger.info(
            f"Sent batch {idx + 1}/{total_batches}: "
            f"{len(batch)} ladder codes: {batch}"
        )

    logger.info(
        f"Dispatched {total_batches} batches for run_id={run_id}, "
        f"route={route}, total_ladder_codes={len(ladder_codes)}"
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "run_id": run_id,
            "route": route,
            "total_ladder_codes": len(ladder_codes),
            "batches_sent": total_batches,
            "batch_size": batch_size
        })
    }


# ---------------------------------------------------------------------------
# Local testing
# ---------------------------------------------------------------------------
# DO COMMENT THE BELOW LINES AFTER TESTING

# messageBody = {
#     "route": "D",
#     "analyst": "Kenneth Menezes",
#     "batch_size": 5
# }

# event = {'body': json.dumps(messageBody)}
# lambda_handler(event, None)
