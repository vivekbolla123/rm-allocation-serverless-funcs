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

warnings.filterwarnings('ignore')

# rds settings
DB_CONN_STRING = f"mysql+mysqldb://{configsettings.RM_DB_APPUSER_USERNAME}:{configsettings.RM_DB_APPUSER_PASSWORD}@{configsettings.RM_DB_APPUSER_URL}/{configsettings.RM_DB_SCHEMA_NAME}"


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


def update_au_in_navitaire(inventory_dict, au_classes):
    """
        desc
    """
    allocation_result = {}

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

        au_classes = allocation_data['auClasses']
        start_date = allocation_data['startDate']
        end_date = allocation_data['endDate']
        flight = allocation_data['flight']

        carrier_code = flight[1:3]
        identifier = flight[3:7]
        origin = flight[8:11]
        destination = flight[11:14]

        start_date_f = format_date(start_date)
        end_date_f = format_date(end_date)

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
        allocation_result = update_au_in_navitaire(inventory_dict, au_classes)

        for _, v in allocation_result.items():
            if(v==-3):
                countOfFailures+=1
            countOfTotal+=1
        
        allocationEndTime = datetime.now()
        insertNavAudit(str(uuid4()), allocationStartTime, allocationEndTime,
                       content, json.dumps(allocation_result), allocation_data["runId"], allocation_data["b2cRunId"], 
                       allocation_data["b2bRunId"], countOfFailures, countOfTotal)

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


# messageBody = {"runId": "63e2768f-c3c5-4277-8b57-b85e0922b62d", "type": "L", "flight": " QP1359 BLRDEL", "startDate": "09-21-2023", "endDate": "09-21-2023", "dayOfWeek": "Daily", "aircraftType": "", "aircraftSuffix": "", "lid": "", "capacity": "", "status": "", "b2cRunId": "a348d78f-5897-4be5-a815-8ec88307c7ec", "b2bRunId": "124519ef-9e10-46ef-b492-d7b575868997", "auClasses": {"V9": {"classOfService": "V9", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "V8": {"classOfService": "V8", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "V7": {"classOfService": "V7", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "V6": {"classOfService": "V6", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "V5": {"classOfService": "V5", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "V4": {"classOfService": "V4", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "V3": {"classOfService": "V3", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "V2": {"classOfService": "V2", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "V1": {"classOfService": "V1", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "V0": {"classOfService": "V0", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "U9": {"classOfService": "U9", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "U8": {"classOfService": "U8", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "U7": {"classOfService": "U7", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "U6": {"classOfService": "U6", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "U5": {"classOfService": "U5", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "U4": {"classOfService": "U4", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "U3": {"classOfService": "U3", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "U2": {"classOfService": "U2", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "U1": {"classOfService": "U1", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "U0": {"classOfService": "U0", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "T9": {"classOfService": "T9", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "T8": {"classOfService": "T8", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "T7": {"classOfService": "T7", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "T6": {"classOfService": "T6", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "T5": {"classOfService": "T5", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "T4": {"classOfService": "T4", "classType": "", "classNest": -1, "classRank": -1, "classAU": 132, "classAllotted": -1, "apRestriction": -1}, "T3": {"classOfService": "T3", "classType": "", "classNest": -1, "classRank": -1, "classAU": 132, "classAllotted": -1, "apRestriction": -1}, "T2": {"classOfService": "T2", "classType": "", "classNest": -1, "classRank": -1, "classAU": 132, "classAllotted": -1, "apRestriction": -1}, "T1": {"classOfService": "T1", "classType": "", "classNest": -1, "classRank": -1, "classAU": 132, "classAllotted": -1, "apRestriction": -1}, "T0": {"classOfService": "T0", "classType": "", "classNest": -1, "classRank": -1, "classAU": 135, "classAllotted": -1, "apRestriction": -1}, "R9": {"classOfService": "R9", "classType": "", "classNest": -1, "classRank": -1, "classAU": 135, "classAllotted": -1, "apRestriction": -1}, "R8": {"classOfService": "R8", "classType": "", "classNest": -1, "classRank": -1, "classAU": 138, "classAllotted": -1, "apRestriction": -1}, "R7": {"classOfService": "R7", "classType": "", "classNest": -1, "classRank": -1, "classAU": 138, "classAllotted": -1, "apRestriction": -1}, "R6": {"classOfService": "R6", "classType": "", "classNest": -1, "classRank": -1, "classAU": 138, "classAllotted": -1, "apRestriction": -1}, "R5": {"classOfService": "R5", "classType": "", "classNest": -1, "classRank": -1, "classAU": 141, "classAllotted": -1, "apRestriction": -1}, "R4": {"classOfService": "R4", "classType": "", "classNest": -1, "classRank": -1, "classAU": 141, "classAllotted": -1, "apRestriction": -1}, "R3": {"classOfService": "R3", "classType": "", "classNest": -1, "classRank": -1, "classAU": 144, "classAllotted": -1, "apRestriction": -1}, "R2": {"classOfService": "R2", "classType": "", "classNest": -1, "classRank": -1, "classAU": 144, "classAllotted": -1, "apRestriction": -1}, "R1": {"classOfService": "R1", "classType": "", "classNest": -1, "classRank": -1, "classAU": 147, "classAllotted": -1, "apRestriction": -1}, "R0": {"classOfService": "R0", "classType": "", "classNest": -1, "classRank": -1, "classAU": 150, "classAllotted": -1, "apRestriction": -1}, "Q": {"classOfService": "Q", "classType": "", "classNest": -1, "classRank": -1, "classAU": 153, "classAllotted": -1, "apRestriction": -1}, "O": {"classOfService": "O", "classType": "", "classNest": -1, "classRank": -1, "classAU": 156, "classAllotted": -1, "apRestriction": -1}, "N": {"classOfService": "N", "classType": "", "classNest": -1, "classRank": -1, "classAU": 159, "classAllotted": -1, "apRestriction": -1}, "M": {"classOfService": "M", "classType": "", "classNest": -1, "classRank": -1, "classAU": 162, "classAllotted": -1, "apRestriction": -1}, "K": {"classOfService": "K", "classType": "", "classNest": -1, "classRank": -1, "classAU": 165, "classAllotted": -1, "apRestriction": -1}, "J": {"classOfService": "J", "classType": "", "classNest": -1, "classRank": -1, "classAU": 168, "classAllotted": -1, "apRestriction": -1}, "I": {"classOfService": "I", "classType": "", "classNest": -1, "classRank": -1, "classAU": 171, "classAllotted": -1, "apRestriction": -1}, "H": {"classOfService": "H", "classType": "", "classNest": -1, "classRank": -1, "classAU": 174, "classAllotted": -1, "apRestriction": -1}, "F": {"classOfService": "F", "classType": "", "classNest": -1, "classRank": -1, "classAU": 174, "classAllotted": -1, "apRestriction": -1}, "E": {"classOfService": "E", "classType": "", "classNest": -1, "classRank": -1, "classAU": 174, "classAllotted": -1, "apRestriction": -1}, "D": {"classOfService": "D", "classType": "", "classNest": -1, "classRank": -1, "classAU": 174, "classAllotted": -1, "apRestriction": -1}, "C": {"classOfService": "C", "classType": "", "classNest": -1, "classRank": -1, "classAU": 174, "classAllotted": -1, "apRestriction": -1}, "B": {"classOfService": "B", "classType": "", "classNest": -1, "classRank": -1, "classAU": 174, "classAllotted": -1, "apRestriction": -1}, "Y": {"classOfService": "Y", "classType": "", "classNest": -1, "classRank": -1, "classAU": 174, "classAllotted": -1, "apRestriction": -1}, "Z9": {"classOfService": "Z9", "classType": "", "classNest": -1, "classRank": -1, "classAU": -1, "classAllotted": -1, "apRestriction": -1}, "Z8": {"classOfService": "Z8", "classType": "", "classNest": -1, "classRank": -1, "classAU": -1, "classAllotted": -1, "apRestriction": -1}, "Z7": {"classOfService": "Z7", "classType": "", "classNest": -1, "classRank": -1, "classAU": -1, "classAllotted": -1, "apRestriction": -1}, "Z6": {"classOfService": "Z6", "classType": "", "classNest": -1, "classRank": -1, "classAU": -1, "classAllotted": -1, "apRestriction": -1}, "Z5": {"classOfService": "Z5", "classType": "", "classNest": -1, "classRank": -1, "classAU": -1, "classAllotted": -1, "apRestriction": -1}, "Z4": {"classOfService": "Z4", "classType": "", "classNest": -1, "classRank": -1, "classAU": -1, "classAllotted": -1, "apRestriction": -1}, "Z3": {"classOfService": "Z3", "classType": "", "classNest": -1, "classRank": -1, "classAU": -1, "classAllotted": -1, "apRestriction": -1}, "Z2": {"classOfService": "Z2", "classType": "", "classNest": -1, "classRank": -1, "classAU": -1, "classAllotted": -1, "apRestriction": -1}, "Z1": {"classOfService": "Z1", "classType": "", "classNest": -1, "classRank": -1, "classAU": -1, "classAllotted": -1, "apRestriction": -1}, "Z0": {"classOfService": "Z0", "classType": "", "classNest": -1, "classRank": -1, "classAU": -1, "classAllotted": -1, "apRestriction": -1}, "L": {"classOfService": "L", "classType": "", "classNest": -1, "classRank": -1, "classAU": -1, "classAllotted": -1, "apRestriction": -1}, "P": {"classOfService": "P", "classType": "", "classNest": -1, "classRank": -1, "classAU": -1, "classAllotted": -1, "apRestriction": -1}}}
# event = {'Records': [{'body': json.dumps(messageBody)}]}
# lambda_handler(event, None)
