import sys
import logging
import json
from uuid import uuid4
from datetime import datetime
from lambda_multiprocessing import Pool
from sqlalchemy import create_engine
import requests

# nav creds
nav_base_url = "https://dotrezapi.prod.0k.navitaire.com/0k/dotrez"
username = "aumaster1"
pwd = "Jun@2023"
domain = "TOP"
ocp_subscription_key = "4aaea8e1179e478e9d08b25326b46656"


# rds settings
DB_CONN_STRING = "mysql+mysqldb://rmappuser:p6VeHNdvAHZKkjR@rm-prod-db.cd7glmg3mmed.ap-south-1.rds.amazonaws.com/QP_DW_RMALLOC"


def get_nav_token_api():
    """
        desc
    """
    token_url = f"{nav_base_url}/api/nsk/v2/token"
    token_body = {
        "credentials": {
            "username": username,
            "password": pwd,
            "domain": domain
        }
    }
    response = session.post(token_url, json=token_body)
    return response.json()


def get_nav_trip_info_simple_api(start_date, end_date, origin, destination, identifier, carrier_code):
    """
        desc
    """
    trip_info_simple_url = f"{nav_base_url}/api/nsk/v2/trip/info/simple?BeginDate={start_date}&EndDate={end_date}&CarrierCode={carrier_code}&Identifier={identifier}&Origin={origin}&Destination={destination}"
    response = session.get(trip_info_simple_url)
    return response.json()


def get_nav_dcs_inventory_api(leg_key):
    """
        desc
    """
    dcs_get_inventory_url = f"{nav_base_url}/api/dcs/v2/inventory/legs/{leg_key}"
    response = session.get(dcs_get_inventory_url)
    return response.json()


def patch_nav_dcs_inventory_api(args):
    """
        desc
    """
    leg_key, nest_key, class_key, target_authorised_units, class_of_service = args
    try:
        dcs_patch_inventory_url = f"{nav_base_url}/api/dcs/v1/inventory/legs/{leg_key}/nests/{nest_key}/classes/{class_key}"
        logger.info(f"Attempting update {dcs_patch_inventory_url}")
        req_body = {
            "authorizedUnits": target_authorised_units
        }
        response = session.patch(dcs_patch_inventory_url, json=req_body)
        return [class_of_service , target_authorised_units]
    except Exception as err:
        logger.error(f"Navitaire update failed {err}")
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

                        if target_authorised_units != curr_authorised_units or target_authorised_units > -1:
                            # update nav class with target AU
                            args.append(
                                [leg_key, nest_key, class_key, target_authorised_units, class_of_service])
                            print(
                                f"leg_key: {leg_key}, nest_key: {nest_key}, class_key: {class_key}, target_authorised_units: {target_authorised_units}"
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
    {"Ocp-Apim-Subscription-Key": ocp_subscription_key}
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

        # First fetch the legKey for the market using tripInfo API
        leg_keys = get_leg_keys(start_date_f, end_date_f,
                                origin, destination, identifier, carrier_code)

        # Fetch dcs inventory for each leg key
        inventory_dict = get_dcs_inventories(leg_keys)

        # Now we have all the data needed to update the AUs. For fetched inventory classes, set the allotted units
        allocation_result = update_au_in_navitaire(inventory_dict, au_classes)

        countOfFailures=0
        countOfTotal = 0
        for _, v in allocation_result.items():
            if(v==-3):
                countOfFailures+=1
            if(v > -1):
                countOfTotal+=1
        
        allocationEndTime = datetime.now()
        insertNavAudit(str(uuid4()), allocationStartTime, allocationEndTime,
                       content, json.dumps(allocation_result), allocation_data["runId"], allocation_data["b2cRunId"], 
                       allocation_data["b2bRunId"], countOfFailures, countOfTotal)

    return {
        'statusCode': 200,
        'body': 'ok'
    }


def insertNavAudit(id, startTime, endTime, request, result, runId, b2cRunId, b2bRunId, countOfFailures, countOfTotal):
    insertQuery = " INSERT INTO navitaire_allocation_audit (id, start_time, end_time, request, result, run_id, b2cRunId, b2bRunId, failures, totalApiCalls) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    conn.execute(insertQuery,
                 (id, startTime.strftime('%Y-%m-%d %H:%M:%S'),
                  endTime.strftime('%Y-%m-%d %H:%M:%S'), request, result, runId, b2cRunId, b2bRunId, int(countOfFailures), int(countOfTotal))
                 )
    print("ADDED ALLOCATION RUN")


# messageBody = {"runId": "a7831b2b-96ad-4b7b-9753-b774d8e19a6a", "type": "L", "flight": " QP1119 DELBOM", "startDate": "08-20-2023", "endDate": "08-20-2023", "dayOfWeek": "Daily", "aircraftType": "", "aircraftSuffix": "", "lid": "", "capacity": "", "status": "", "b2cRunId": "c18f4d29-6e32-4694-93f2-cb3639f1cfec", "b2bRunId": "5ec5f0e6-227e-4354-af1f-8b5793c590c7", "auClasses": {"V9": {"classOfService": "V9", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "V8": {"classOfService": "V8", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "V7": {"classOfService": "V7", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "V6": {"classOfService": "V6", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "V5": {"classOfService": "V5", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "V4": {"classOfService": "V4", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "V3": {"classOfService": "V3", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "V2": {"classOfService": "V2", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "V1": {"classOfService": "V1", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "V0": {"classOfService": "V0", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "U9": {"classOfService": "U9", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "U8": {"classOfService": "U8", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "U7": {"classOfService": "U7", "classType": "", "classNest": -1, "classRank": -1, "classAU": 16, "classAllotted": -1, "apRestriction": -1}, "U6": {"classOfService": "U6", "classType": "", "classNest": -1, "classRank": -1, "classAU": 16, "classAllotted": -1, "apRestriction": -1}, "U5": {"classOfService": "U5", "classType": "", "classNest": -1, "classRank": -1, "classAU": 16, "classAllotted": -1, "apRestriction": -1}, "U4": {"classOfService": "U4", "classType": "", "classNest": -1, "classRank": -1, "classAU": 16, "classAllotted": -1, "apRestriction": -1}, "U3": {"classOfService": "U3", "classType": "", "classNest": -1, "classRank": -1, "classAU": 16, "classAllotted": -1, "apRestriction": -1}, "U2": {"classOfService": "U2", "classType": "", "classNest": -1, "classRank": -1, "classAU": 27, "classAllotted": -1, "apRestriction": -1}, "U1": {"classOfService": "U1", "classType": "", "classNest": -1, "classRank": -1, "classAU": 27, "classAllotted": -1, "apRestriction": -1}, "U0": {"classOfService": "U0", "classType": "", "classNest": -1, "classRank": -1, "classAU": 27, "classAllotted": -1, "apRestriction": -1}, "T9": {"classOfService": "T9", "classType": "", "classNest": -1, "classRank": -1, "classAU": 27, "classAllotted": -1, "apRestriction": -1}, "T8": {"classOfService": "T8", "classType": "", "classNest": -1, "classRank": -1, "classAU": 36, "classAllotted": -1, "apRestriction": -1}, "T7": {"classOfService": "T7", "classType": "", "classNest": -1, "classRank": -1, "classAU": 36, "classAllotted": -1, "apRestriction": -1}, "T6": {"classOfService": "T6", "classType": "", "classNest": -1, "classRank": -1, "classAU": 36, "classAllotted": -1, "apRestriction": -1}, "T5": {"classOfService": "T5", "classType": "", "classNest": -1, "classRank": -1, "classAU": 43, "classAllotted": -1, "apRestriction": -1}, "T4": {"classOfService": "T4", "classType": "", "classNest": -1, "classRank": -1, "classAU": 43, "classAllotted": -1, "apRestriction": -1}, "T3": {"classOfService": "T3", "classType": "", "classNest": -1, "classRank": -1, "classAU": 43, "classAllotted": -1, "apRestriction": -1}, "T2": {"classOfService": "T2", "classType": "", "classNest": -1, "classRank": -1, "classAU": 49, "classAllotted": -1, "apRestriction": -1}, "T1": {"classOfService": "T1", "classType": "", "classNest": -1, "classRank": -1, "classAU": 49, "classAllotted": -1, "apRestriction": -1}, "T0": {"classOfService": "T0", "classType": "", "classNest": -1, "classRank": -1, "classAU": 49, "classAllotted": -1, "apRestriction": -1}, "R9": {"classOfService": "R9", "classType": "", "classNest": -1, "classRank": -1,
#                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                "classAU": 49, "classAllotted": -1, "apRestriction": -1}, "R8": {"classOfService": "R8", "classType": "", "classNest": -1, "classRank": -1, "classAU": 54, "classAllotted": -1, "apRestriction": -1}, "R7": {"classOfService": "R7", "classType": "", "classNest": -1, "classRank": -1, "classAU": 54, "classAllotted": -1, "apRestriction": -1}, "R6": {"classOfService": "R6", "classType": "", "classNest": -1, "classRank": -1, "classAU": 54, "classAllotted": -1, "apRestriction": -1}, "R5": {"classOfService": "R5", "classType": "", "classNest": -1, "classRank": -1, "classAU": 58, "classAllotted": -1, "apRestriction": -1}, "R4": {"classOfService": "R4", "classType": "", "classNest": -1, "classRank": -1, "classAU": 58, "classAllotted": -1, "apRestriction": -1}, "R3": {"classOfService": "R3", "classType": "", "classNest": -1, "classRank": -1, "classAU": 58, "classAllotted": -1, "apRestriction": -1}, "R2": {"classOfService": "R2", "classType": "", "classNest": -1, "classRank": -1, "classAU": 61, "classAllotted": -1, "apRestriction": -1}, "R1": {"classOfService": "R1", "classType": "", "classNest": -1, "classRank": -1, "classAU": 61, "classAllotted": -1, "apRestriction": -1}, "R0": {"classOfService": "R0", "classType": "", "classNest": -1, "classRank": -1, "classAU": 61, "classAllotted": -1, "apRestriction": -1}, "Q": {"classOfService": "Q", "classType": "", "classNest": -1, "classRank": -1, "classAU": 64, "classAllotted": -1, "apRestriction": -1}, "O": {"classOfService": "O", "classType": "", "classNest": -1, "classRank": -1, "classAU": 64, "classAllotted": -1, "apRestriction": -1}, "N": {"classOfService": "N", "classType": "", "classNest": -1, "classRank": -1, "classAU": 66, "classAllotted": -1, "apRestriction": -1}, "M": {"classOfService": "M", "classType": "", "classNest": -1, "classRank": -1, "classAU": 66, "classAllotted": -1, "apRestriction": -1}, "K": {"classOfService": "K", "classType": "", "classNest": -1, "classRank": -1, "classAU": 68, "classAllotted": -1, "apRestriction": -1}, "J": {"classOfService": "J", "classType": "", "classNest": -1, "classRank": -1, "classAU": 68, "classAllotted": -1, "apRestriction": -1}, "I": {"classOfService": "I", "classType": "", "classNest": -1, "classRank": -1, "classAU": 70, "classAllotted": -1, "apRestriction": -1}, "H": {"classOfService": "H", "classType": "", "classNest": -1, "classRank": -1, "classAU": 72, "classAllotted": -1, "apRestriction": -1}, "F": {"classOfService": "F", "classType": "", "classNest": -1, "classRank": -1, "classAU": 74, "classAllotted": -1, "apRestriction": -1}, "E": {"classOfService": "E", "classType": "", "classNest": -1, "classRank": -1, "classAU": 76, "classAllotted": -1, "apRestriction": -1}, "D": {"classOfService": "D", "classType": "", "classNest": -1, "classRank": -1, "classAU": 78, "classAllotted": -1, "apRestriction": -1}, "C": {"classOfService": "C", "classType": "", "classNest": -1, "classRank": -1, "classAU": 80, "classAllotted": -1, "apRestriction": -1}, "B": {"classOfService": "B", "classType": "", "classNest": -1, "classRank": -1, "classAU": 82, "classAllotted": -1, "apRestriction": -1}, "Y": {"classOfService": "Y", "classType": "", "classNest": -1, "classRank": -1, "classAU": 174, "classAllotted": -1, "apRestriction": -1}, "Z9": {"classOfService": "Z9", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "Z8": {"classOfService": "Z8", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "Z7": {"classOfService": "Z7", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "Z6": {"classOfService": "Z6", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "Z5": {"classOfService": "Z5", "classType": "", "classNest": -1, "classRank": -1, "classAU": 0, "classAllotted": -1, "apRestriction": -1}, "Z4": {"classOfService": "Z4", "classType": "", "classNest": -1, "classRank": -1, "classAU": 13, "classAllotted": -1, "apRestriction": -1}, "Z3": {"classOfService": "Z3", "classType": "", "classNest": -1, "classRank": -1, "classAU": 13, "classAllotted": -1, "apRestriction": -1}, "Z2": {"classOfService": "Z2", "classType": "", "classNest": -1, "classRank": -1, "classAU": 13, "classAllotted": -1, "apRestriction": -1}, "Z1": {"classOfService": "Z1", "classType": "", "classNest": -1, "classRank": -1, "classAU": 13, "classAllotted": -1, "apRestriction": -1}, "Z0": {"classOfService": "Z0", "classType": "", "classNest": -1, "classRank": -1, "classAU": 13, "classAllotted": -1, "apRestriction": -1}}}
# event = {'Records': [{'body': json.dumps(messageBody)}]}
# lambda_handler(event, None)
