import sys
import logging
import json
import warnings
import time
from uuid import uuid4
from datetime import datetime
from pymongo import MongoClient
import requests
import configsettings
from redis import Redis
from time_logger import TimeLogger
from constants_utils import ConstantsUtils

warnings.filterwarnings('ignore')

# MongoDB settings

CACHE_ENDPOINT = configsettings.RM_CACHE_ENDPOINT
CACHE_PORT = configsettings.RM_CACHE_PORT
CACHE_USERNAME = configsettings.RM_CACHE_USERNAME
CACHE_PASSWORD = configsettings.RM_CACHE_PASSWORD

try:
    cache_client = Redis(host=CACHE_ENDPOINT, port=CACHE_PORT, username=CACHE_USERNAME, password=CACHE_PASSWORD)
    if cache_client.ping():
        print("Connected to Redis")
except Exception as e:
    print(f"Redis connection skipped or failed: {e}")
    cache_client = None

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

def refresh_nav_token():
    """
    Fetches a new Navitaire authentication token and updates session headers.
    """
    session.headers.update(
        {"Ocp-Apim-Subscription-Key": configsettings.NAV_OCP_SUBSCRIPTION_KEY}
    )
    token_response = get_nav_token_api()
    access_token = token_response["data"]["token"]
    session.headers.update({'Authorization': f"Bearer {access_token}"})
    return access_token

def ensure_authenticated():
    """
    Ensures that the requests session has a valid Authorization header.
    """
    if 'Authorization' not in session.headers or not session.headers.get('Authorization'):
        refresh_nav_token()

def retry_if_api_broken(api_call, *args):
    global session
    retry_count = 0
    condition = True
    response = None
    while condition and retry_count < 4:
        try:
            ensure_authenticated()
            if len(args) > 1:
                response = api_call(args[0], json=args[1], verify=False)
            else:
                response = api_call(args[0], verify=False)

            if response.status_code == 401:
                raise Exception(f"HTTP 401 Unauthorized for {args[0]}")

            condition = False
        except Exception as e:
            retry_count = retry_count + 1
            logger.error(f"Retrying after token generation: {e} : {retry_count}")
            try:
                refresh_nav_token()
            except Exception as token_err:
                logger.error(f"Token refresh failed: {token_err}")

            time.sleep(2 * retry_count)

            if len(args) > 1:
                api_call = session.patch
            else:
                api_call = session.get

    if retry_count >= 4 or condition:
        raise Exception("retry exceeded")

    return response


def get_nav_trip_info_simple_api(start_date, end_date, origin, destination, identifier, carrier_code):
    """
        desc
    """
    trip_info_simple_url = f"{configsettings.NAV_BASE_URL}/api/nsk/v2/trip/info/simple?BeginDate={start_date}&EndDate={end_date}&CarrierCode={carrier_code}&Identifier={identifier}&Origin={origin}&Destination={destination}"
    try:
        ensure_authenticated()
        response = session.get(trip_info_simple_url, verify=False)
        if response.status_code == 401:
            raise Exception("HTTP 401 Unauthorized")
    except Exception as e:
        logger.error(f"retrying trip_info again with new token: {e}")
        response = retry_if_api_broken(session.get, trip_info_simple_url)

    if not check_status(response):
        raise Exception("Trip Info", str(response.reason), str(response.status_code), str(response.json()))

    return response.json()


def get_nav_dcs_inventory_api(leg_key):
    """
        desc
    """
    dcs_get_inventory_url = f"{configsettings.NAV_BASE_URL}/api/dcs/v2/inventory/legs/{leg_key}"
    try:
        ensure_authenticated()
        response = session.get(dcs_get_inventory_url, verify=False)
        if response.status_code == 401:
            raise Exception("HTTP 401 Unauthorized")
    except Exception as e:
        logger.error(f"retrying Inventory Leg again with new token: {e}")
        response = retry_if_api_broken(session.get, dcs_get_inventory_url)

    if not check_status(response):
        raise Exception("Inventory Leg", str(response.reason), str(response.status_code), str(response.json()))
    return response.json()


def patch_nav_dcs_inventory_api(args, send_avs_messages=False):
    """
        desc
    """
    req_body = {}
    if send_avs_messages:
        req_body = {'sendAvsMessages': True}

    for leg_key, nest_key, class_key, target_authorised_units, class_of_service,curr_authorised_units,target_ap_restrictions in args:
        nests = req_body.setdefault('Nests', {})

        # Ensure nest_key exists
        nest = nests.setdefault(nest_key, {})

        # Ensure 'Classes' exists under the nest
        classes = nest.setdefault('Classes', {})

        # Ensure class_key exists
        class_info = classes.setdefault(class_key, {})

        if target_authorised_units != -1:
            class_info['AuthorizedUnits'] = target_authorised_units
        if target_ap_restrictions != -1:
            class_info['LatestAdvancedReservation'] = target_ap_restrictions

    try:
        dcs_patch_inventory_url = f"{configsettings.NAV_BASE_URL}/api/dcs/v2/inventory/legs/{args[0][0]}"

        try:
            ensure_authenticated()
            response = session.patch(dcs_patch_inventory_url, json=req_body, verify=False)
            if response.status_code == 401:
                raise Exception("HTTP 401 Unauthorized")
        except Exception as e:
            logger.error(f"retrying Navitaire update again with new token: {e}")
            response = retry_if_api_broken(session.patch, dcs_patch_inventory_url, req_body)

        if not check_status(response):
            logger.error(f"Navitaire update failed : status check  {str(response.reason)},  {str(response.status_code)}, {str(response.json())}")
            return False
        return True
    except Exception as err:
        logger.error(f"Navitaire update failed :{err}")
        raise err

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
    is_updated = False
    is_gds_updated = False
    log_time.start()
    GDS_RBD_LIST = [rbd.strip() for rbd in constantsutils.GDS_RBD_VALUES.split(",")]

    for leg_key, inventory_nests in inventory_dict.items():
        for inventory_nest in inventory_nests:
            nest_key = inventory_nest["nestKey"]
            classes = inventory_nest["classes"]
            args = []
            gds_args = []
            is_updated = True
            is_gds_updated = True
            for _class in classes:
                class_key = _class["classKey"]
                class_of_service = _class["classOfService"]
                curr_authorised_units = _class["authorizedUnits"]
                curr_ap_restrictions = _class["latestAdvancedReservation"]

                if class_of_service in au_classes:
                    au_class = au_classes[class_of_service]
                    target_authorised_units = au_class["classAU"]
                    target_ap_restrictions = au_class["apRestriction"]

                    if (target_authorised_units != curr_authorised_units and target_authorised_units > -1) or (target_ap_restrictions != curr_ap_restrictions and target_ap_restrictions > -1):
                        # update nav class with target AU
                        if class_of_service in GDS_RBD_LIST:
                            gds_args.append(
                                [leg_key, nest_key, class_key, target_authorised_units, class_of_service,curr_authorised_units,target_ap_restrictions])
                            print(
                                f"leg_key: {leg_key}, nest_key: {nest_key}, class_key: {class_key}, target_authorised_units: {target_authorised_units},current_authorised_units:{curr_authorised_units}, sendAVSMessages: True"
                            )
                        else:
                            args.append(
                            [leg_key, nest_key, class_key, target_authorised_units, class_of_service,curr_authorised_units,target_ap_restrictions])
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
            if args:
                is_updated = patch_nav_dcs_inventory_api(args)
                if not is_updated: 
                    for _, _, _, _, class_of_service,_,_ in args:
                        allocation_result[class_of_service] = -3

            if gds_args:
                is_gds_updated = patch_nav_dcs_inventory_api(gds_args, send_avs_messages=True)

    log_time.end("update_au_in_navitaire")
    return allocation_result, is_updated, is_gds_updated


logger = logging.getLogger()
logger.setLevel(logging.INFO)

# create the database connection outside of the handler to allow connections to be
# re-used by subsequent function invocations.
try:
    mongo_client = MongoClient(
            host=configsettings.MONGO_URL,
            port=27017,
            username=configsettings.MONGO_USERNAME,
            password=configsettings.MONGO_PASSWORD,
            authSource=configsettings.MONGO_AUTH_SOURCE
        )
    db = mongo_client["qp_dw_rmalloc"]
    
    # Collections
    navitaire_allocation_audit_col = db['navitaire_allocation_audit']
    run_flight_date_audit_col = db['run_flight_date_audit']
    allocation_run_audit_col = db['allocation_run_audit']
    
except Exception as e:
    logger.error(e)
    sys.exit()

constantsutils = ConstantsUtils(db)
log_time= TimeLogger(db, constantsutils.CHECK_THRESHOLD_UPDATE_RBD)
logger.info("SUCCESS: Connection to MongoDB instance succeeded")

session = requests.Session()

session.headers.update(
    {"Ocp-Apim-Subscription-Key": configsettings.NAV_OCP_SUBSCRIPTION_KEY}
)
try:
    refresh_nav_token()
except Exception as e:
    logger.error(f"Could not fetch initial Navitaire token at startup: {e}")


def lambda_handler(event, context):
    """
        description
    """
    records = event['Records']
    for record in records:
        content = record['body']
        # logger.info(type(content))

        allocationStartTime = datetime.now()

        allocation_data = json.loads(content)
        # allocation_data = content
        # logger.info(allocation_data)
        run_id = allocation_data["runId"]
        logger.info(f"updateRBDLambda start | run_id: {run_id}")
        
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
        countOfTotal = 2

        # First fetch the legKey for the market using tripInfo API
        leg_keys = get_leg_keys(start_date_f, end_date_f,
                                origin, destination, identifier, carrier_code)

        # Fetch dcs inventory for each leg key
        inventory_dict = get_dcs_inventories(leg_keys)

        # Now we have all the data needed to update the AUs. For fetched inventory classes, set the allotted units
        allocation_result, is_nav_updated, is_gds_updated = update_au_in_navitaire(inventory_dict, au_classes, run_id)

        if not is_nav_updated:
            countOfFailures += 1
        if not is_gds_updated:
            countOfFailures += 1
        
        allocationEndTime = datetime.now()
        nav_audit_id = allocation_data.get("navAuditId")
        rbds = allocation_data.get("rbds")
        sector_val = allocation_data.get("sector") or (origin + destination)
        flight_number_val = allocation_data.get("flightNumber") or identifier
        dept_date_val = allocation_data.get("departureDate") or start_date_f[:10]

        insertNavAudit(str(uuid4()), allocationStartTime, allocationEndTime,
                       content, json.dumps(allocation_result), run_id, allocation_data["b2cRunId"], 
                       allocation_data["b2bRunId"], countOfFailures, countOfTotal,
                       navAuditId=nav_audit_id, sector=sector_val, flightNumber=flight_number_val,
                       departureDate=dept_date_val, flight=flight, rbds=rbds)
        updateRunAudit(run_id, start_date_f,origin+destination, identifier, json.dumps(allocation_result))
        updateAllocationAudit(allocationEndTime, run_id)
        log_time.send_time_log_db()
        
        logger.info(f"updateRBDLambda complete | run_id: {run_id}")

        if countOfFailures > 0:
            raise Exception(f"updateRBDLambda completed with {countOfFailures} failure(s)")

    return {
        'statusCode': 200,
        'body': 'ok'
    }

def check_status(response):
    return response.status_code >= 200 and response.status_code < 300

def insertNavAudit(id, startTime, endTime, request, result, runId, b2cRunId, b2bRunId, countOfFailures, countOfTotal,
                   navAuditId=None, sector=None, flightNumber=None, departureDate=None, flight=None, rbds=None, **kwargs):
    try:
        filter_doc = None
        if navAuditId:
            filter_doc = {"_id": navAuditId}
        elif sector and flightNumber and departureDate:
            flight_variants = [str(flightNumber)]
            if str(flightNumber).isdigit():
                flight_variants.append(str(int(flightNumber)))
            flight_variants.append(f"QP{flightNumber}")
            filter_doc = {
                "run_id": runId,
                "sector": sector,
                "flight_number": {"$in": flight_variants},
                "departure_date": departureDate
            }

        existing = None
        if filter_doc and navitaire_allocation_audit_col is not None:
            existing = navitaire_allocation_audit_col.find_one(filter_doc)

        update_fields = {
            "start_time": startTime,
            "end_time": endTime,
            "request": request,
            "result": result,
            "run_id": runId,
            "b2cRunId": b2cRunId,
            "b2bRunId": b2bRunId,
            "failures": int(countOfFailures),
            "totalApiCalls": int(countOfTotal)
        }
        if sector:
            update_fields["sector"] = sector
        if flightNumber:
            update_fields["flight_number"] = str(flightNumber)
        if departureDate:
            update_fields["departure_date"] = departureDate
        if flight:
            update_fields["flight"] = flight
        if rbds is not None:
            update_fields["rbds"] = rbds

        if existing:
            navitaire_allocation_audit_col.update_one({"_id": existing["_id"]}, {"$set": update_fields})
            print("UPDATED EXISTING ALLOCATION RUN")
        elif filter_doc and not navAuditId:
            set_on_insert = {"_id": id or str(uuid4())}
            navitaire_allocation_audit_col.update_one(filter_doc, {"$set": update_fields, "$setOnInsert": set_on_insert}, upsert=True)
            print("UPSERTED ALLOCATION RUN")
        else:
            doc_id = navAuditId or id or str(uuid4())
            update_fields["_id"] = doc_id
            navitaire_allocation_audit_col.insert_one(update_fields)
            print("ADDED ALLOCATION RUN")
    except Exception as e:
        logger.error(f"Error in insertNavAudit: {e}")
        raise e

def updateRunAudit(run_id, start_date,sector, flight, result):
    dateFormatted = start_date[:10]
    insert_run_fltDate_audit(run_id, sector,flight, dateFormatted, result)

    if cache_client is not None:
        try:
            cache_client.incr(KEY_B2B_DONE_COUNT + run_id, 1)
            cache_client.incr(KEY_B2C_DONE_COUNT + run_id, 1)
        except Exception as e:
            logger.error(f"Redis incr failed: {e}")

def insert_run_fltDate_audit(run_id,sector, flightNumber, flightDate, result):
    document = {
        "runId": run_id,
        "flightNumber": flightNumber,
        "sector": sector,
        "flightDate": flightDate,
        "result": result,
        "b2cstatus": "Done",
        "b2bstatus": "Done",
        "createdAt": datetime.now()
    }
    try:
        run_flight_date_audit_col.insert_one(document)
    except Exception as e:
        logger.error(f"Error in insert_run_fltDate_audit: {e}")
        raise e

def updateAllocationAudit(endTime, run_id):
    allocation_run_audit_col.update_one(
        {"run_id": run_id},
        {"$set": {"actual_end_time": endTime}}
    )

# messageBody = {"runId": "be76c956-f7d2-44cb-9f27-8f69f27f2cb0", "type": "L", "flight": " QP1955 BOMBLR", "startDate": "10-19-2025", "endDate": "10-19-2025", "dayOfWeek": "Daily", "aircraftType": "", "aircraftSuffix": "", "lid": "", "capacity": "", "status": "", "b2cRunId": "f4ed4831-14d7-41da-99e7-4e69a92bcb74", "b2bRunId": "6a35a631-e365-49e1-811e-948666f62dc3", "auClasses": {"V9": {"classOfService": "V9", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "V8": {"classOfService": "V8", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "V7": {"classOfService": "V7", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "V6": {"classOfService": "V6", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "V5": {"classOfService": "V5", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "V4": {"classOfService": "V4", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "V3": {"classOfService": "V3", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "V2": {"classOfService": "V2", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "V1": {"classOfService": "V1", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "V0": {"classOfService": "V0", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "U9": {"classOfService": "U9", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "U8": {"classOfService": "U8", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "U7": {"classOfService": "U7", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "U6": {"classOfService": "U6", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "U5": {"classOfService": "U5", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "U4": {"classOfService": "U4", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "U3": {"classOfService": "U3", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "U2": {"classOfService": "U2", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "U1": {"classOfService": "U1", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "U0": {"classOfService": "U0", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "T9": {"classOfService": "T9", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "T8": {"classOfService": "T8", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "T7": {"classOfService": "T7", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "T6": {"classOfService": "T6", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "T5": {"classOfService": "T5", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "T4": {"classOfService": "T4", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "T3": {"classOfService": "T3", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "T2": {"classOfService": "T2", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "T1": {"classOfService": "T1", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "T0": {"classOfService": "T0", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "R9": {"classOfService": "R9", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "R8": {"classOfService": "R8", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "R7": {"classOfService": "R7", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "R6": {"classOfService": "R6", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "R5": {"classOfService": "R5", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "R4": {"classOfService": "R4", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "R3": {"classOfService": "R3", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "R2": {"classOfService": "R2", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "R1": {"classOfService": "R1", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "R0": {"classOfService": "R0", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "Q9": {"classOfService": "Q9", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "Q8": {"classOfService": "Q8", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "Q7": {"classOfService": "Q7", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "Q6": {"classOfService": "Q6", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "Q5": {"classOfService": "Q5", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "Q4": {"classOfService": "Q4", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "Q3": {"classOfService": "Q3", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "Q2": {"classOfService": "Q2", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "Q1": {"classOfService": "Q1", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "Q0": {"classOfService": "Q0", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "Q": {"classOfService": "Q", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "O": {"classOfService": "O", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "N": {"classOfService": "N", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "M": {"classOfService": "M", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "K": {"classOfService": "K", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "J": {"classOfService": "J", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "I": {"classOfService": "I", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "H": {"classOfService": "H", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "F": {"classOfService": "F", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "E": {"classOfService": "E", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "D": {"classOfService": "D", "classType": "", "classNest": -1, "classRank": -1, "classAU": 164, "classAllotted": -1, "apRestriction": -1}, "C": {"classOfService": "C", "classType": "", "classNest": -1, "classRank": -1, "classAU": 164, "classAllotted": -1, "apRestriction": -1}, "B": {"classOfService": "B", "classType": "", "classNest": -1, "classRank": -1, "classAU": 164, "classAllotted": -1, "apRestriction": -1}, "Y": {"classOfService": "Y", "classType": "", "classNest": -1, "classRank": -1, "classAU": 194, "classAllotted": -1, "apRestriction": -1}, "ZJ": {"classOfService": "ZJ", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "ZI": {"classOfService": "ZI", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "ZH": {"classOfService": "ZH", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "ZG": {"classOfService": "ZG", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "ZF": {"classOfService": "ZF", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "ZE": {"classOfService": "ZE", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "ZD": {"classOfService": "ZD", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "ZC": {"classOfService": "ZC", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "ZB": {"classOfService": "ZB", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "ZA": {"classOfService": "ZA", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "Z9": {"classOfService": "Z9", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "Z8": {"classOfService": "Z8", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "Z7": {"classOfService": "Z7", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "Z6": {"classOfService": "Z6", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "Z5": {"classOfService": "Z5", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "Z4": {"classOfService": "Z4", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "Z3": {"classOfService": "Z3", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "Z2": {"classOfService": "Z2", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "Z1": {"classOfService": "Z1", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "Z0": {"classOfService": "Z0", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "L": {"classOfService": "L", "classType": "", "classNest": -1, "classRank": -1, "classAU": -1, "classAllotted": -1, "apRestriction": -1}, "P": {"classOfService": "P", "classType": "", "classNest": -1, "classRank": -1, "classAU": -1, "classAllotted": -1, "apRestriction": -1}, "G8": {"classOfService": "G8", "classType": "", "classNest": -1, "classRank": -1, "classAU": -1, "classAllotted": -1, "apRestriction": -1}, "W9": {"classOfService": "W9", "classType": "", "classNest": -1, "classRank": -1, "classAU": -1, "classAllotted": -1, "apRestriction": -1}, "W8": {"classOfService": "W8", "classType": "", "classNest": -1, "classRank": -1, "classAU": -1, "classAllotted": -1, "apRestriction": -1}, "W7": {"classOfService": "W7", "classType": "", "classNest": -1, "classRank": -1, "classAU": -1, "classAllotted": -1, "apRestriction": -1}, "W6": {"classOfService": "W6", "classType": "", "classNest": -1, "classRank": -1, "classAU": -1, "classAllotted": -1, "apRestriction": -1}, "W5": {"classOfService": "W5", "classType": "", "classNest": -1, "classRank": -1, "classAU": -1, "classAllotted": -1, "apRestriction": -1}, "W4": {"classOfService": "W4", "classType": "", "classNest": -1, "classRank": -1, "classAU": -1, "classAllotted": -1, "apRestriction": -1}, "W3": {"classOfService": "W3", "classType": "", "classNest": -1, "classRank": -1, "classAU": -1, "classAllotted": -1, "apRestriction": -1}, "W2": {"classOfService": "W2", "classType": "", "classNest": -1, "classRank": -1, "classAU": -1, "classAllotted": -1, "apRestriction": -1}, "W1": {"classOfService": "W1", "classType": "", "classNest": -1, "classRank": -1, "classAU": -1, "classAllotted": -1, "apRestriction": -1}, "S": {"classOfService": "S", "classType": "", "classNest": -1, "classRank": -1, "classAU": -1, "classAllotted": -1, "apRestriction": -1}, "WA": {"classOfService": "WA", "classType": "", "classNest": -1, "classRank": -1, "classAU": -1, "classAllotted": -1, "apRestriction": -1}, "WB": {"classOfService": "WB", "classType": "", "classNest": -1, "classRank": -1, "classAU": -1, "classAllotted": -1, "apRestriction": -1}, "WC": {"classOfService": "WC", "classType": "", "classNest": -1, "classRank": -1, "classAU": -1, "classAllotted": -1, "apRestriction": -1}, "WD": {"classOfService": "WD", "classType": "", "classNest": -1, "classRank": -1, "classAU": -1, "classAllotted": -1, "apRestriction": -1}, "WE": {"classOfService": "WE", "classType": "", "classNest": -1, "classRank": -1, "classAU": -1, "classAllotted": -1, "apRestriction": -1}, "GJ": {"classOfService": "GJ", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "GI": {"classOfService": "GI", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "GH": {"classOfService": "GH", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "GG": {"classOfService": "GG", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "GF": {"classOfService": "GF", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "GE": {"classOfService": "GE", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "GD": {"classOfService": "GD", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "GC": {"classOfService": "GC", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "GB": {"classOfService": "GB", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "GA": {"classOfService": "GA", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "TF": {"classOfService": "TF", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "TE": {"classOfService": "TE", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "TD": {"classOfService": "TD", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "TC": {"classOfService": "TC", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "TB": {"classOfService": "TB", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "TY": {"classOfService": "TY", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}}}
# event = {'Records': [{'body': json.dumps(messageBody)}]}
# lambda_handler(event, None)