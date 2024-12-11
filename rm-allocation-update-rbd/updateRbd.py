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
from ownlogger import OwnLogger


class UpdateRBDClass:
    def __init__(self, run_id):
        warnings.filterwarnings('ignore')

        # RDS settings
        self.DB_CONN_STRING = f"mysql+mysqldb://{configsettings.RM_DB_APPUSER_USERNAME}:{configsettings.RM_DB_APPUSER_PASSWORD}@{configsettings.RM_DB_APPUSER_URL}/{configsettings.RM_DB_SCHEMA_NAME}"

        # Cache settings
        self.CACHE_ENDPOINT = configsettings.RM_CACHE_ENDPOINT
        self.CACHE_PORT = configsettings.RM_CACHE_PORT
        self.CACHE_USERNAME = configsettings.RM_CACHE_USERNAME
        self.CACHE_PASSWORD = configsettings.RM_CACHE_PASSWORD

        # Constants
        self.KEY_B2B_DONE_COUNT = 'b2b_done_count_'
        self.KEY_B2C_DONE_COUNT = 'b2c_done_count_'
        self.logger = OwnLogger(run_id)

        # Initialize components
        self.setup_cache_client()
        self.setup_database_connection()
        self.setup_navitaire_session()

    def setup_cache_client(self):
        self.cache_client = Redis(
            host=self.CACHE_ENDPOINT, 
            port=self.CACHE_PORT, 
            username=self.CACHE_USERNAME, 
            password=self.CACHE_PASSWORD
        )
        if self.cache_client.ping():
            print("Connected to Redis")

    def setup_database_connection(self):
        try:
            self.conn = create_engine(self.DB_CONN_STRING)
            self.constantsutils = ConstantsUtils(self.conn)
            self.log_time = TimeLogger(self.conn, self.constantsutils.CHECK_THRESHOLD_UPDATE_RBD)
            print("SUCCESS: Connection to RDS MySQL instance succeeded")
        except Exception as e:
            print(f"Database connection error: {e}")
            sys.exit()

    def setup_navitaire_session(self):
        self.session = requests.Session()
        self.session.headers.update({
            "Ocp-Apim-Subscription-Key": configsettings.NAV_OCP_SUBSCRIPTION_KEY
        })
        token_response = self.get_nav_token_api()
        access_token = token_response["data"]["token"]
        self.session.headers.update({'Authorization': f"Bearer {access_token}"})

    def get_nav_token_api(self):
        token_url = f"{configsettings.NAV_BASE_URL}/api/nsk/v2/token"
        token_body = {
            "credentials": {
                "username": configsettings.NAV_USERNAME,
                "password": configsettings.NAV_PWD,
                "domain": configsettings.NAV_DOMAIN
            }
        }
        response = self.session.post(token_url, json=token_body)
        return response.json()

    def retry_if_api_broken(self, api_call, *args):
        retry_count = 0
        condition = True
        while condition and retry_count < 4:
            try:
                if len(args) > 1:
                    response = api_call(args[0], json=args[1], verify=False)
                else:
                    response = api_call(args[0], verify=False)
                condition = False
            except Exception as e:
                retry_count += 1
                
                self.session = requests.Session()
                self.session.headers.update({
                    "Ocp-Apim-Subscription-Key": configsettings.NAV_OCP_SUBSCRIPTION_KEY
                })
                token_response = self.get_nav_token_api()
                access_token = token_response["data"]["token"]
                self.session.headers.update({'Authorization': f"Bearer {access_token}"})
                
                self.logger.error(f"Retrying after token generation: {e} : {retry_count}")
                time.sleep(2 * retry_count)

                api_call = self.session.patch if len(args) > 1 else self.session.get
                
        if retry_count >= 4:
            raise Exception("retry exceeded")

        return response

    def get_nav_trip_info_simple_api(self, start_date, end_date, origin, destination, identifier, carrier_code):
        trip_info_simple_url = f"{configsettings.NAV_BASE_URL}/api/nsk/v2/trip/info/simple?BeginDate={start_date}&EndDate={end_date}&CarrierCode={carrier_code}&Identifier={identifier}&Origin={origin}&Destination={destination}"
        try:
            response = self.session.get(trip_info_simple_url, verify=False)
        except Exception as e:
            self.logger.error(f"Retrying trip_info again with new token: {e}")
            response = self.retry_if_api_broken(self.session.get, trip_info_simple_url)
        
        if not self.check_status(response):
            raise Exception("Trip Info", str(response.reason), str(response.status_code), str(response.json()))

        return response.json()

    def get_nav_dcs_inventory_api(self, leg_key):
        dcs_get_inventory_url = f"{configsettings.NAV_BASE_URL}/api/dcs/v2/inventory/legs/{leg_key}"
        try:
            response = self.session.get(dcs_get_inventory_url, verify=False)
        except Exception as e:
            self.logger.error(f"Retrying Inventory Leg again with new token: {e}")
            response = self.retry_if_api_broken(self.session.get, dcs_get_inventory_url)
            
        if not self.check_status(response):
            raise Exception("Inventory Leg", str(response.reason), str(response.status_code), str(response.json()))
        return response.json()

    def patch_nav_dcs_inventory_api(self, args):
        leg_key, nest_key, class_key, target_authorised_units, class_of_service, curr_authorised_units, run_id = args
        try:
            dcs_patch_inventory_url = f"{configsettings.NAV_BASE_URL}/api/dcs/v1/inventory/legs/{leg_key}/nests/{nest_key}/classes/{class_key}"
            self.logger.info(f"Attempting update {dcs_patch_inventory_url}, target_authorised_units: {target_authorised_units}, current_authorised_units: {curr_authorised_units}")
            req_body = {
                "authorizedUnits": target_authorised_units
            }
            try:    
                response = self.session.patch(dcs_patch_inventory_url, json=req_body, verify=False)
            except Exception as e:
                self.logger.error(f"Retrying Navitaire update again with new token: {e}")
                response = self.retry_if_api_broken(self.session.patch, dcs_patch_inventory_url, req_body)
                
            if not self.check_status(response):
                self.logger.error(f"Navitaire update failed: status check {str(response.reason)}, {str(response.status_code)}, {str(response.json())}")
                target_authorised_units = -3
            return [class_of_service, target_authorised_units]
        except Exception as err:
            self.logger.error(f"Navitaire update failed: {err}")
            return [class_of_service, -3]

    def format_date(self, date_str):
        date_obj = datetime.strptime(date_str, "%m-%d-%Y")
        return date_obj.strftime("%Y-%m-%dT%H:%M:%S")

    def get_leg_keys(self, start_date, end_date, origin, destination, identifier, carrier_code):
        response = self.get_nav_trip_info_simple_api(
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

    def get_dcs_inventories(self, leg_keys):
        inventory_dict = {}
        for leg_key in leg_keys:
            response = self.get_nav_dcs_inventory_api(leg_key)
            if "data" in response:
                inventory_nests = response["data"]["nests"]
                inventory_dict[leg_key] = inventory_nests

        return inventory_dict

    def update_au_in_navitaire(self, inventory_dict, au_classes, run_id):
        allocation_result = {}
        self.log_time.start()
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
                                    [leg_key, nest_key, class_key, target_authorised_units, class_of_service, curr_authorised_units, run_id])
                                print(
                                    f"leg_key: {leg_key}, nest_key: {nest_key}, class_key: {class_key}, target_authorised_units: {target_authorised_units}, current_authorised_units:{curr_authorised_units}"
                                )
                            else:
                                print(
                                    f"skipping {class_of_service} : {target_authorised_units} : {curr_authorised_units}")

                            allocation_result[class_of_service] = target_authorised_units
                        else:
                            allocation_result[class_of_service] = -2
                            print(f"missing class {class_of_service}")
                    arr = p.map(self.patch_nav_dcs_inventory_api, args)
                    for key, value in arr:
                        allocation_result[key] = value
        self.log_time.end("update_au_in_navitaire")
        return allocation_result

    def check_status(self, response):
        return response.status_code >= 200 and response.status_code < 300

    def insertNavAudit(self, id, startTime, endTime, request, result, runId, b2cRunId, b2bRunId, countOfFailures, countOfTotal):
        insertQuery = " INSERT INTO navitaire_allocation_audit (id, start_time, end_time, request, result, run_id, b2cRunId, b2bRunId, failures, totalApiCalls) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        self.conn.execute(insertQuery,
                          (id, startTime.strftime('%Y-%m-%d %H:%M:%S'),
                           endTime.strftime('%Y-%m-%d %H:%M:%S'), request, result, runId, b2cRunId, b2bRunId, int(countOfFailures), int(countOfTotal))
                          )
        print("ADDED ALLOCATION RUN")


    def updateRunAudit(self, run_id, start_date, flight, result):
        dateFormatted = start_date[:10]
        self.insert_run_fltDate_audit(run_id, flight, dateFormatted, result)

        self.cache_client.incr(self.KEY_B2B_DONE_COUNT + run_id, 1)
        self.cache_client.incr(self.KEY_B2C_DONE_COUNT + run_id, 1)

        # update_query = " UPDATE run_flight_date_audit SET b2cstatus = %s ,b2bstatus = %s , result = %s WHERE runId = %s and flightNumber = %s and flightDate = %s"
        # conn.execute(update_query, ("Done", "Done", result, run_id, flight, dateFormatted))

    def insert_run_fltDate_audit(self, run_id, flightNumber, flightDate, result):
        insert_query = " INSERT INTO run_flight_date_audit (runId, flightNumber, flightDate, result, b2cstatus, b2bstatus ,createdAt) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        try:
            self.conn.execute(insert_query, (run_id, flightNumber, flightDate, result, "Done", "Done", datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        except Exception as e:
            self.logger.info(e)

    def updateAllocationAudit(self, endTime, run_id):
        update_query = " UPDATE allocation_run_audit SET actual_end_time = %s WHERE run_id = %s"
        self.conn.execute(update_query, (endTime.strftime('%Y-%m-%d %H:%M:%S'), run_id))