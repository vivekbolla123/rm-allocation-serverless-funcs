import sys
import logging
import json
import warnings
import time
from uuid import uuid4
from datetime import datetime
from lambda_multiprocessing import Pool
from sqlalchemy import create_engine
import requests
import configsettings
from redis import Redis
from time_logger import TimeLogger
from constants_utils import ConstantsUtils

warnings.filterwarnings('ignore')

# rds settings
DB_CONN_STRING = f"mysql+mysqldb://{configsettings.RM_DB_APPUSER_USERNAME}:{configsettings.RM_DB_APPUSER_PASSWORD}@{configsettings.RM_DB_APPUSER_URL}/{configsettings.RM_DB_SCHEMA_NAME}"

CACHE_ENDPOINT = configsettings.RM_CACHE_ENDPOINT
CACHE_PORT = configsettings.RM_CACHE_PORT
CACHE_USERNAME = configsettings.RM_CACHE_USERNAME
CACHE_PASSWORD = configsettings.RM_CACHE_PASSWORD

cache_client = Redis(host=CACHE_ENDPOINT, port=CACHE_PORT, username=CACHE_USERNAME, password=CACHE_PASSWORD)
if cache_client.ping():
    print("Connected to Redis")

KEY_B2B_DONE_COUNT = 'b2b_done_count_'
KEY_B2C_DONE_COUNT = 'b2c_done_count_'

def get_nav_token_api():
    """
        desc
    """
    token_url = f"{configsettings.NAV_BASE_URL}/api/nsk/v2/token"
    token_body = {
        "credentials": {
            "username": configsettings.NAV_USERNAME,
            "password": configsettings.NAV_PWD,
            "domain": configsettings.NAV_DOMAIN
        }
    }
    response = session.post(token_url, json=token_body)
    return response.json()

def retry_if_api_broken(api_call,*args):
    retry_count=0
    condition = True
    while condition and retry_count<4:
        try:
            if(len(args)>1):
                response=api_call(args[0],json = args[1], verify=False)
            else:
                response=api_call(args[0], verify=False)
            condition = False
        except Exception as e:
            retry_count=retry_count+1
            
            session = requests.Session()
            session.headers.update(
                {"Ocp-Apim-Subscription-Key": configsettings.NAV_OCP_SUBSCRIPTION_KEY}
            )
            token_response = get_nav_token_api()
            access_token = token_response["data"]["token"]
            session.headers.update({'Authorization': f"Bearer {access_token}"})
            logger.error(f"Retrying after token generation: {e} : {retry_count}")
            time.sleep(2 * retry_count)

            if (len(args) > 1):
                api_call = session.patch
            else:
                api_call = session.get
            
    if (retry_count >= 4):
        raise Exception("retry exceeded")

    return response
        

def get_nav_trip_info_simple_api(start_date, end_date, origin, destination, identifier, carrier_code):
    """
        desc
    """
    trip_info_simple_url = f"{configsettings.NAV_BASE_URL}/api/nsk/v2/trip/info/simple?BeginDate={start_date}&EndDate={end_date}&CarrierCode={carrier_code}&Identifier={identifier}&Origin={origin}&Destination={destination}"
    try:
        response = session.get(trip_info_simple_url, verify=False)
    except Exception as e:
        logger.error(f"retrying  trip_info again with new token: {e}")
        response = retry_if_api_broken(session.get,trip_info_simple_url)
    
    if(not check_status(response)):
        raise Exception("Trip Info", str(response.reason), str(response.status_code), str(response.json()))

    return response.json()


def get_nav_dcs_inventory_api(leg_key):
    """
        desc
    """
    dcs_get_inventory_url = f"{configsettings.NAV_BASE_URL}/api/dcs/v2/inventory/legs/{leg_key}"
    try:
        response = session.get(dcs_get_inventory_url, verify=False)
    except Exception as e:
        logger.error(f"retrying Inventory Leg again with new token: {e}")
        response = retry_if_api_broken(session.get,dcs_get_inventory_url)
        
    if(not check_status(response)):
        raise Exception("Inventory Leg", str(response.reason), str(response.status_code), str(response.json()))
    return response.json()


def patch_nav_dcs_inventory_api(args):
    """
        desc
    """
    leg_key, nest_key, class_key, target_authorised_units, class_of_service,curr_authorised_units = args
    try:
        dcs_patch_inventory_url = f"{configsettings.NAV_BASE_URL}/api/dcs/v1/inventory/legs/{leg_key}/nests/{nest_key}/classes/{class_key}"
        logger.info(f"Attempting update {dcs_patch_inventory_url},target_authorised_units: {target_authorised_units}, current_authorised_units: {curr_authorised_units}")

        req_body = {
            "authorizedUnits": target_authorised_units
        }
        try:    
            response = session.patch(dcs_patch_inventory_url, json=req_body, verify=False)
        except Exception as e:
            logger.error(f"retrying Navitaire update again with new token: {e}")
            response = retry_if_api_broken(session.patch,dcs_patch_inventory_url,req_body)
            
        if (not check_status(response)):
            logger.error(f"Navitaire update failed : status check  {str(response.reason)},  {str(response.status_code)}, {str(response.json())}")
            target_authorised_units = -3
        return [class_of_service , target_authorised_units]
    except Exception as err:
        logger.error(f"Navitaire update failed :{err}")
        return [class_of_service , -3]

def format_date(date_str):
    """
        desc
    """
    date_obj = datetime.strptime(date_str, "%m-%d-%Y")
    date_str = date_obj.strftime("%Y-%m-%dT%H:%M:%S")
    return date_str


def get_leg_keys(start_date, end_date, origin, destination, identifier, carrier_code):
    """
        desc
    """
    response = get_nav_trip_info_simple_api(
        start_date, end_date, origin, destination, identifier, carrier_code
    )
    leg_keys = []
    if "data" in response:
        journeys = response["data"][0]["journeys"]
        for journey in journeys:
            segments = journey["segments"]
            for segment in segments:
                legs = segment["legs"]
                for leg in legs:
                    leg_keys.append(leg["legKey"])

    return leg_keys


def get_dcs_inventories(leg_keys):
    """
        desc
    """
    inventory_dict = {}
    for leg_key in leg_keys:
        response = get_nav_dcs_inventory_api(leg_key)
        if "data" in response:
            inventory_nests = response["data"]["nests"]
            inventory_dict[leg_key] = inventory_nests

    return inventory_dict


def update_au_in_navitaire(inventory_dict, au_classes, run_id):
    """
        desc
    """
    allocation_result = {}
    log_time.start()
    for leg_key, inventory_nests in inventory_dict.items():
        for inventory_nest in inventory_nests:
            nest_key = inventory_nest["nestKey"]
            classes = inventory_nest["classes"]
            args = []
            with Pool(1) as p:
                for _class in classes:
                    class_key = _class["classKey"]
                    class_of_service = _class["classOfService"]
                    curr_authorised_units = _class["authorizedUnits"]

                    if class_of_service in au_classes:
                        au_class = au_classes[class_of_service]
                        target_authorised_units = au_class["classAU"]

                        if target_authorised_units != curr_authorised_units and target_authorised_units > -1:
                            # update nav class with target AU
                            args.append(
                                [leg_key, nest_key, class_key, target_authorised_units, class_of_service,curr_authorised_units])
                            print(
                                f"leg_key: {leg_key}, nest_key: {nest_key}, class_key: {class_key}, target_authorised_units: {target_authorised_units},current_authorised_units:{curr_authorised_units}"
                            )
                        else:
                            print(
                                f"skipping {class_of_service} : {target_authorised_units} : {curr_authorised_units}")

                        allocation_result[class_of_service] = target_authorised_units
                    else:
                        allocation_result[class_of_service] = -2
                        print(f"missing class {class_of_service}")
                arr = p.map(patch_nav_dcs_inventory_api, args)
                for key, value in arr:
                    allocation_result[key] = value
    log_time.end("update_au_in_navitaire")
    return allocation_result


logger = logging.getLogger()
logger.setLevel(logging.INFO)

# create the database connection outside of the handler to allow connections to be
# re-used by subsequent function invocations.
try:
    conn = create_engine(DB_CONN_STRING)
except Exception as e:
    logger.error(e)
    sys.exit()
constantsutils = ConstantsUtils(conn)
log_time= TimeLogger(conn, constantsutils.CHECK_THRESHOLD_UPDATE_RBD)
logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")

session = requests.Session()

session.headers.update(
    {"Ocp-Apim-Subscription-Key": configsettings.NAV_OCP_SUBSCRIPTION_KEY}
)
token_response = get_nav_token_api()
access_token = token_response["data"]["token"]
session.headers.update({'Authorization': f"Bearer {access_token}"})


def lambda_handler(event, context):
    """
        description
    """
    records = event['Records']
    for record in records:
        content = record['body']
        logger.info(type(content))

        allocationStartTime = datetime.now()

        allocation_data = json.loads(content)
        # allocation_data = content
        logger.info(allocation_data)
        run_id = allocation_data["runId"]
        au_classes = allocation_data['auClasses']
        start_date = allocation_data['startDate']
        end_date = allocation_data['endDate']
        flight = allocation_data['flight']

        identifier = flight.split(" ")[1][2:]        
        carrier_code = flight.split(" ")[1][:2]
        origin = flight.split(" ")[2][:3]
        destination = flight.split(" ")[2][3:]

        start_date_f = format_date(start_date)
        end_date_f = format_date(end_date)
        log_time.update_run_id(run_id)
        countOfFailures = 0
        countOfTotal = 0
        inventory_dict = {}
        try:
        # First fetch the legKey for the market using tripInfo API
            leg_keys = get_leg_keys(start_date_f, end_date_f,
                                    origin, destination, identifier, carrier_code)

            # Fetch dcs inventory for each leg key
            inventory_dict = get_dcs_inventories(leg_keys)
            
        except Exception as e:
            logger.error(e)
            countOfFailures = len(au_classes)
            countOfTotal = len(au_classes)

        # Now we have all the data needed to update the AUs. For fetched inventory classes, set the allotted units
        allocation_result = update_au_in_navitaire(inventory_dict, au_classes, run_id)

        for _, v in allocation_result.items():
            if(v==-3):
                countOfFailures+=1
            countOfTotal+=1
        
        allocationEndTime = datetime.now()
        insertNavAudit(str(uuid4()), allocationStartTime, allocationEndTime,
                       content, json.dumps(allocation_result), run_id, allocation_data["b2cRunId"], 
                       allocation_data["b2bRunId"], countOfFailures, countOfTotal)
        updateRunAudit(run_id, start_date_f, identifier, json.dumps(allocation_result))
        updateAllocationAudit(allocationEndTime, run_id)
        log_time.send_time_log_db()

    return {
        'statusCode': 200,
        'body': 'ok'
    }

def check_status(response):
    return response.status_code >= 200 and response.status_code < 300

def insertNavAudit(id, startTime, endTime, request, result, runId, b2cRunId, b2bRunId, countOfFailures, countOfTotal):
    insertQuery = " INSERT INTO navitaire_allocation_audit (id, start_time, end_time, request, result, run_id, b2cRunId, b2bRunId, failures, totalApiCalls) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    conn.execute(insertQuery,
                 (id, startTime.strftime('%Y-%m-%d %H:%M:%S'),
                  endTime.strftime('%Y-%m-%d %H:%M:%S'), request, result, runId, b2cRunId, b2bRunId, int(countOfFailures), int(countOfTotal))
                 )
    print("ADDED ALLOCATION RUN")

def updateRunAudit(run_id, start_date, flight, result):
    dateFormatted = start_date[:10]
    insert_run_fltDate_audit(run_id, flight, dateFormatted, result)

    cache_client.incr(KEY_B2B_DONE_COUNT + run_id, 1)
    cache_client.incr(KEY_B2C_DONE_COUNT + run_id, 1)

    # update_query = " UPDATE run_flight_date_audit SET b2cstatus = %s ,b2bstatus = %s , result = %s WHERE runId = %s and flightNumber = %s and flightDate = %s"
    # conn.execute(update_query, ("Done", "Done", result, run_id, flight, dateFormatted))

def insert_run_fltDate_audit(run_id, flightNumber, flightDate, result):
    insert_query = " INSERT INTO run_flight_date_audit (runId, flightNumber, flightDate, result, b2cstatus, b2bstatus ,createdAt) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    try:
        conn.execute(insert_query, (run_id, flightNumber, flightDate, result, "Done", "Done", datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    except Exception as e:
        logger.info(e)

def updateAllocationAudit(endTime, run_id):
    update_query = " UPDATE allocation_run_audit SET actual_end_time = %s WHERE run_id = %s"
    conn.execute(update_query, (endTime.strftime('%Y-%m-%d %H:%M:%S'), run_id))

# messageBody = {'runId': '80ee0d91-3493-4907-be1d-6181742daee7', 'type': 'L', 'flight': ' QP1410 BOMDEL', 'startDate': '08-27-2024', 'endDate': '08-27-2024', 'dayOfWeek': 'Daily', 'aircraftType': '', 'aircraftSuffix': '', 'lid': '', 'capacity': '', 'status': '', 'b2cRunId': 'e4bfe37b-440c-4ccf-95ea-b88112c6e84b', 'b2bRunId': '9460355b-799f-4a8b-97b9-ce661aacc580', 'rowId': '6d988a82-03b9-45f4-b994-6b503b9cc176', 'auClasses': {}}
# event = {'Records': [{'body': json.dumps(messageBody)}]}
# lambda_handler(event, None)
