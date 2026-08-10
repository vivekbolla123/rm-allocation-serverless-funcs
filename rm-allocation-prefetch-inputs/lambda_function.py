import sys
import logging
import boto3
import json
import pytz
import pandas as pd
from datetime import datetime, timedelta
import uuid
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError, AutoReconnect
import configsettings
# from Constants import PREFETCH_CHUNK_SIZE

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ==========================
# MongoDB Connection
# ==========================

def create_mongo_connection():
    """Create MongoDB connection"""
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
        mongo_client.admin.command('ping')
        logger.info("MongoDB connection established successfully")
        return mongo_client
    except Exception as e:
        logger.error(f"MongoDB connection error: {str(e)}")
        raise

try:
    mongo_client = create_mongo_connection()
    mongo_db = mongo_client[configsettings.MONGO_DB]
    sqs = boto3.client('sqs')
    logger.info("SUCCESS: All connections established")
except Exception as e:
    logger.error(f"Failed to establish connections: {str(e)}")
    sys.exit()

# ==========================
# Helper Functions
# ==========================

def get_parameter(key, default=None):
    """Read a single parameter from MongoDB rm_parameter_values"""
    try:
        doc = mongo_db['rm_parameter_values'].find_one(
            {'parameterKey': key},
            {'parameterValue': 1, '_id': 0}
        )
        if doc:
            return doc['parameterValue']
        return default
    except Exception as e:
        logger.error(f"Error reading parameter {key}: {str(e)}")
        return default


def count_records_per_ndo(market_list_table, start_period, end_period):
    """
    Use MongoDB aggregation to count records per NDO day.
    Since PerStart == PerEnd, we only consider PerStart.
    Pipeline: parse PerStart → filter date range → group by date → count.
    """
    ist = pytz.timezone('Asia/Kolkata')
    curr_date = datetime.now(ist).date()
    # Use UTC-aware datetimes so MongoDB $gte/$lte aligns with UTC-midnight stored dates
    start_dt = pytz.utc.localize(datetime.strptime(start_period, "%Y-%m-%d"))
    end_dt = pytz.utc.localize(datetime.strptime(end_period, "%Y-%m-%d"))

    pipeline = [
        # Parse PerStart string to date
        {
            '$addFields': {
                'perstart_date': {
                    '$dateFromString': {'dateString': '$PerStart', 'format': '%m/%d/%Y'}
                }
            }
        },
        # Filter by date range
        {
            '$match': {
                'perstart_date': {
                    '$gte': start_dt,
                    '$lte': end_dt
                }
            }
        },
        # Group by departure date and count
        {
            '$group': {
                '_id': '$perstart_date',
                'record_count': {'$sum': 1}
            }
        },
        # Sort by date
        {'$sort': {'_id': 1}}
    ]

    try:
        results = list(mongo_db[market_list_table].aggregate(pipeline, allowDiskUse=True))
    except Exception as e:
        logger.error(f"Aggregation error: {str(e)}")
        raise

    if not results:
        logger.info(f"No records found in {market_list_table} for period {start_period} to {end_period}")
        return pd.DataFrame(columns=['ndo', 'record_count'])

    # Convert to DataFrame and compute NDO
    counts_df = pd.DataFrame(results)
    counts_df['ndo'] = counts_df['_id'].apply(
        lambda d: (pytz.utc.localize(d).astimezone(ist).date() - curr_date).days
    )
    counts_df = counts_df[['ndo', 'record_count']].sort_values('ndo').reset_index(drop=True)

    logger.info(f"Total NDO days with records: {len(counts_df)}, total records: {counts_df['record_count'].sum()}")
    return counts_df


def bin_ndo_into_chunks(counts_df, records_per_chunk):
    """
    Greedy bin consecutive NDO days into chunks, each targeting ~records_per_chunk records.
    """
    chunks = []
    chunk_start = None
    running_total = 0

    for _, row in counts_df.iterrows():
        ndo = int(row['ndo'])
        count = int(row['record_count'])

        if chunk_start is None:
            chunk_start = ndo
            running_total = count
        elif running_total + count > records_per_chunk:
            chunks.append({
                'start_interval': chunk_start,
                'end_interval': ndo - 1,
                'estimated_records': running_total
            })
            chunk_start = ndo
            running_total = count
        else:
            running_total += count

    # Flush the last chunk
    if chunk_start is not None:
        chunks.append({
            'start_interval': chunk_start,
            'end_interval': int(counts_df['ndo'].iloc[-1]),
            'estimated_records': running_total
        })

    return chunks


# ==========================
# Lambda Handler
# ==========================

def lambda_handler(event, context):
    """
    prefetchLambda: counts records per NDO day, bins into chunks,
    and sends one SQS message per chunk to trigger fetchInputs.

    Expected EventBridge input:
    {
        "overall_start_interval": 0,
        "overall_end_interval": 90,
        "market_list_table_name": "market_list",
        "run_type": "Scheduled",
        "region_method": "Domestic",
        "update_navitaire_method": "api",
        "route_type": "direct",
        "cug_alloc": "0",
        "mongodb": "0"
    }
    """
    
    #logger.info(f"prefetchLambda invoked with event: {json.dumps(event)}")

    # Unwrap nested body if event came from SQS/EventBridge wrapping
    if 'body' in event and isinstance(event['body'], str):
        event = json.loads(event['body'])
    elif 'body' in event and isinstance(event['body'], dict):
        event = event['body']

    # Generate a single run_id to trace all chunks together
    run_id = event.get('run_id') or str(uuid.uuid4())
    logger.info(f"prefetchLambda start | run_id: {run_id} | event: {json.dumps(event)}")

    logger.info(f"Parsed event body: {json.dumps(event)}")

    # Parse input
    overall_start = int(event.get('start_interval', 0))
    overall_end = int(event.get('end_interval', 7))
    market_list_table = event.get('market_list_table_name', 'market_list')

    # Read config — run types that trigger sibling enrichment get smaller chunks
    run_type = event.get('run_type', 'Adhoc')
    DEFAULT_PREFETCH_CHUNK_SIZE = '{"with_siblings": 500, "without_siblings": 8000}'
    chunk_size_map = json.loads(get_parameter('PREFETCH_CHUNK_SIZE', DEFAULT_PREFETCH_CHUNK_SIZE))
    sibling_run_types = ('Scheduled_Thirty', 'Adhoc', 'Equipment_Change')
    if run_type in sibling_run_types:
        chunk_size = int(chunk_size_map['with_siblings'])
    else:
        chunk_size = int(chunk_size_map['without_siblings'])
    queue_url = configsettings.FETCH_INPUTS_TRIGGER_QUEUE_URL

    if not queue_url:
        logger.error("FETCH_INPUTS_TRIGGER_QUEUE_URL not configured in configsettings")
        raise ValueError("FETCH_INPUTS_TRIGGER_QUEUE_URL not configured")

    logger.info(f"Config: chunk_size={chunk_size}, queue_url={queue_url}")

    # Compute date range from NDO intervals
    ist = pytz.timezone('Asia/Kolkata')
    curr_date = datetime.now(ist).date()
    start_period = (curr_date + timedelta(days=overall_start)).strftime("%Y-%m-%d")
    end_period = (curr_date + timedelta(days=overall_end)).strftime("%Y-%m-%d")

    logger.info(f"NDO range: {overall_start}-{overall_end}, date range: {start_period} to {end_period}")

    # Step 1: Count records per NDO day
    counts_df = count_records_per_ndo(market_list_table, start_period, end_period)

    if counts_df.empty:
        logger.info("No records found for the given NDO range. Nothing to dispatch.")
        return {
            'statusCode': 200,
            'body': json.dumps({'chunks_sent': 0, 'message': 'No records found'})
        }

    # Step 2: Bin NDO days into chunks
    chunks = bin_ndo_into_chunks(counts_df, chunk_size)
    logger.info(f"Created {len(chunks)} chunks from {len(counts_df)} NDO days")

    # Step 3: Build base payload — pass through ALL fields from the event,
    # only strip the interval keys (those will be set per-chunk below)
    PREFETCH_ONLY_KEYS = {'start_interval', 'end_interval'}
    base_payload = {k: v for k, v in event.items() if k not in PREFETCH_ONLY_KEYS}


    base_payload['run_id'] = run_id
    base_payload['overall_start'] = overall_start
    base_payload['overall_end'] = overall_end

    # Step 4: Send one SQS message per chunk
    for i, chunk in enumerate(chunks):
        payload = {**base_payload}
        payload['start_interval'] = chunk['start_interval']
        payload['end_interval'] = chunk['end_interval']

        message_body = json.dumps({'body': json.dumps(payload)})

        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=message_body
        )

        logger.info(
            f"Sent chunk {i+1}/{len(chunks)}: "
            f"NDO {chunk['start_interval']}-{chunk['end_interval']}, "
            f"~{chunk['estimated_records']} records"
        )

    total_records = counts_df['record_count'].sum()
    logger.info(f"Done. Dispatched {len(chunks)} chunks covering ~{total_records} total records.")

    logger.info(f"prefetchLambda complete | run_id: {run_id}")
    return {
        'statusCode': 200,
        'body': json.dumps({
            'chunks_sent': len(chunks),
            'total_estimated_records': int(total_records),
            'chunks': chunks
        })
    }


# if __name__ == "__main__":
#     # For local testing; adjust event as needed
#     event = {
#         "body": json.dumps({
#             "start_interval": 0,
#             "end_interval": 7,
#             "market_list_table_name": "market_list",
#             "run_type": "Scheduled",
#             "cug_alloc":0
#         })
#     }
#     context = {}
#     result = lambda_handler(event, context)
#     print(result)