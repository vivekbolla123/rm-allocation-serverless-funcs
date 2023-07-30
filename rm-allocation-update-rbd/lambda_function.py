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
DB_CONN_STRING = "mysql+mysqldb://rmadmin:8rP#h1HQ5ysM@rm-prod-db.cd7glmg3mmed.ap-south-1.rds.amazonaws.com/QP_DW_RMALLOC"


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
    trip_info_simple_url = f"{nav_base_url}/api/nsk/v1/trip/info/simple?BeginDate={start_date}&EndDate={end_date}&CarrierCode={carrier_code}&Identifier={identifier}&Origin={origin}&Destination={destination}"
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
    try:
        leg_key, nest_key, class_key, target_authorised_units = args
        dcs_patch_inventory_url = f"{nav_base_url}/api/dcs/v1/inventory/legs/{leg_key}/nests/{nest_key}/classes/{class_key}"
        logger.info(f"Attempting update {dcs_patch_inventory_url}")
        req_body = {
            "authorizedUnits": target_authorised_units
        }
        response = session.patch(dcs_patch_inventory_url, json=req_body)
        return response.json()
    except Exception as err:
        logger.error(f"Navitaire update failed {err}")


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
    journeys = response["data"][0]["journeys"]
    leg_keys = []
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
                            args.append([leg_key, nest_key, class_key, target_authorised_units])
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
                p.map(patch_nav_dcs_inventory_api, args)

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

        allocationEndTime = datetime.now()
        insertNavAudit(str(uuid4()), allocationStartTime, allocationEndTime,
                       content, json.dumps(allocation_result), allocation_data["runId"])

    return {
        'statusCode': 200,
        'body': 'ok'
    }


def insertNavAudit(id, startTime, endTime, request, result, runId):
    insertQuery = " INSERT INTO navitaire_allocation_audit (id, start_time, end_time, request, result, run_id) VALUES (%s, %s, %s, %s, %s, %s)"
    conn.execute(insertQuery,
                 (id, startTime.strftime('%Y-%m-%d %H:%M:%S'),
                  endTime.strftime('%Y-%m-%d %H:%M:%S'), request, result, runId)
                 )
    print("ADDED ALLOCATION RUN")