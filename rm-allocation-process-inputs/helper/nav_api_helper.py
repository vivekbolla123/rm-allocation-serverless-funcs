from datetime import datetime
import pandas as pd
import requests
from ownlogger import OwnLogger
from Constants import *

class NavAPIHelper:
    def __init__(self,NAV_BASE_API_URL,NAV_OCP_SUBSCRIPTION_KEY,NAV_USERNAME,NAV_PWD,NAV_DOMAIN, runId, inputRunAudit):
        self.logger= OwnLogger(runId, inputRunAudit)
        self.NAV_BASE_API_URL=NAV_BASE_API_URL
        self.NAV_OCP_SUBSCRIPTION_KEY=NAV_OCP_SUBSCRIPTION_KEY
        self.NAV_USERNAME=NAV_USERNAME
        self.NAV_PWD=NAV_PWD
        self.NAV_DOMAIN=NAV_DOMAIN
        self.base_url = f"{self.NAV_BASE_API_URL}/api"
        self.session = requests.Session()
        self.token = None
        self.session.headers.update(
    			 {"Ocp-Apim-Subscription-Key": self.NAV_OCP_SUBSCRIPTION_KEY}
		)
        self.get_token()

    def get_token(self):
        try:
            url = f"{self.base_url}/nsk/v2/token"
            credentials = {
                "credentials": {
                    "username": self.NAV_USERNAME,
                    "password": self.NAV_PWD,
                    "domain": self.NAV_DOMAIN
                }
            }
            response = self.session.post(url, json=credentials, timeout=10)
            if response.status_code == 201:
                try:
                    resp_json = response.json()
                    self.token = resp_json.get('data', {}).get('token') if isinstance(resp_json, dict) else None
                    if self.token:
                        self.session.headers.update({"Authorization": f"Bearer {self.token}"})
                    else:
                        self.logger.info("Token not found in response data")
                except Exception as e:
                    self.logger.error(f"Failed to parse token response JSON: {e}")
            else:
                self.logger.info(f"Failed to get token, status: {response.status_code}")
        except Exception as e:
            self.logger.error(f"Exception during get_token: {e}")

    def get_leg_nest_details(self, dep_date, carrier_code, identifier, origin, destin):
        try:
            if not getattr(self, 'token', None):
                self.get_token()
            if not getattr(self, 'token', None):
                self.logger.info("No valid token available for get_leg_nest_details")
                return None, None, None, None

            dep_date = datetime.strptime(str(dep_date), DATE_FORMAT).date().strftime(DATE_FORMAT_2)
            url = f"{self.base_url}/nsk/v2/manifest?BeginDate={dep_date}&endDate={dep_date}&CarrierCode={carrier_code}&Identifier={identifier}&FlightType=3"
            response = self.session.get(url, timeout=10)
            if response.status_code != 200:
                self.logger.info(f"Failed to get leg and nest keys. Status: {response.status_code}")
                return None, None, None, None

            try:
                resp_json = response.json()
            except Exception as e:
                self.logger.error(f"JSON decode error in get_leg_nest_details: {e}")
                return None, None, None, None

            data = resp_json.get('data', []) if isinstance(resp_json, dict) else []
            if isinstance(data, list):
                for item in data:
                    if not isinstance(item, dict):
                        continue
                    for journey in item.get('journeys', []):
                        if not isinstance(journey, dict):
                            continue
                        designator = journey.get('designator', {})
                        if designator.get('destination') == destin and designator.get('origin') == origin:
                            for segment in journey.get('segments', []):
                                if not isinstance(segment, dict):
                                    continue
                                for leg in segment.get('legs', []):
                                    if not isinstance(leg, dict):
                                        continue
                                    leg_key = leg.get('legKey')
                                    nest_key, lid, nest_lid = self.get_nest_key(leg_key)
                                    return leg_key, nest_key, lid, nest_lid

            self.logger.info("No matching leg found in manifest response")
        except Exception as e:
            self.logger.error(f"Exception in get_leg_nest_details: {e}")
        return None, None, None, None
    
    def get_nest_key(self, leg_key):
        try:
            if not leg_key:
                return None, None, None
            headers = {'Authorization': f'Bearer {self.token}'} if self.token else {}
            url = f'{self.base_url}/dcs/v2/inventory/legs/{leg_key}'
            response = self.session.get(url, headers=headers, timeout=10)

            if response.status_code != 200:
                self.logger.info(f"Failed to get nest key. Status: {response.status_code}")
                return None, None, None

            try:
                resp_json = response.json()
            except Exception as e:
                self.logger.error(f"JSON decode error in get_nest_key: {e}")
                return None, None, None

            if isinstance(resp_json, dict):
                data = resp_json.get('data', {})
                if isinstance(data, dict):
                    nests = data.get('nests', [])
                    lid = data.get('legInformation', {}).get('lid') if isinstance(data.get('legInformation'), dict) else None
                    if isinstance(nests, list):
                        for nest in nests:
                            if isinstance(nest, dict):
                                nest_key = nest.get('nestKey')
                                nest_lid = nest.get('lid')
                                return nest_key, lid, nest_lid
        except Exception as e:
            self.logger.error(f"Exception in get_nest_key: {e}")
        return None, None, None
    
    def update_leg(self, leg_key, lid):
        try:
            if not leg_key or lid is None:
                self.logger.info(f"Cannot update leg: invalid leg_key={leg_key} or lid={lid}")
                return
            url = f"{self.base_url}/dcs/v1/inventory/legs/{leg_key}"
            data = {"lid": int(lid)}
            self.logger.info('lid api called '+ str(url))
            self.logger.info('payload sent '+ str(data))
            
            response = self.session.patch(url, json=data, timeout=10)
            if response.status_code == 200:
                self.logger.info('lid changed sucessfully to '+ str(lid))
            else:
                self.logger.info(f"Failed to update leg, status: {response.status_code}")
        except Exception as e:
            self.logger.error(f"Exception in update_leg: {e}")
           
        
    def update_leg_nest(self, leg_key, nest_key, lid):
        try:
            if not leg_key or not nest_key or lid is None:
                self.logger.info(f"Cannot update leg nest: invalid leg_key={leg_key}, nest_key={nest_key}, or lid={lid}")
                return
            url = f"{self.base_url}/dcs/v1/inventory/legs/{leg_key}/nests/{nest_key}"
            data = {"lid": int(lid)}
            response = self.session.patch(url, json=data, timeout=10)
            if response.status_code == 200:
                self.logger.info('nest lid changed sucessfully to '+ str(lid))
            else:
                self.logger.info(f"Failed to update leg nest, status: {response.status_code}")
        except Exception as e:
            self.logger.error(f"Exception in update_leg_nest: {e}")
    
    
    def get_second_flight_dep(self,flightno1,flightno2,org,destin,depDate):
        departure = ""
        try:
            data = {
                "origin": org,
                "destination": destin,
                "searchDestinationMacs": True,
                "searchOriginMacs": True,
                "beginDate": str(depDate),
                "endDate": None,
                "getAllDetails": True,
                "taxesAndFees": "Taxes",
                "passengers": {
                    "types": [
                        {
                            "type": "ADT",
                            "count": 1
                        }
                    ]
                },
                "codes": {
                    "currencyCode": "INR",
                    "promotionCode": ""
                },
                "numberOfFaresPerJourney": 4,
                "filters": {
                    "compressionType": 1,
                    "groupByDate": False,
                    "carrierCode": "QP",
                    "identifier": str(flightno1),
                    "type": "ALL",
                    "maxConnections": 4,
                    "productClasses": [
                        "EC",
                        "AV",
                        "SP"
                    ],
                    "sortOptions": [
                        "NoSort"
                    ],
                    "fareTypes": [
                        "R",
                        "V",
                        "S"
                    ]
                }
            }

            trip_info_simple_url = f"{self.base_url}/nsk/v4/availability/search/simple"
            response = self.session.post(trip_info_simple_url, json=data, verify=False, timeout=10)
            if response.status_code == 200:
                try:
                    data = response.json()
                except Exception as e:
                    self.logger.error(f"JSON decode error in get_second_flight_dep: {e}")
                    return ""

                if isinstance(data, dict) and data.get("data") is not None and not pd.isna(data.get("data")) and not pd.isnull(data.get("data")):
                    for result in data["data"].get("results", []):
                        if not isinstance(result, dict):
                            continue
                        for trip in result.get("trips", []):
                            if not isinstance(trip, dict):
                                continue
                            for journey in trip.get("journeysAvailableByMarket", {}).values():
                                if not isinstance(journey, list):
                                    continue
                                for j in journey:
                                    if not isinstance(j, dict):
                                        continue
                                    for segment in j.get("segments", []):
                                        if not isinstance(segment, dict):
                                            continue
                                        identifier = segment.get("identifier", {}).get("identifier")
                                        if identifier == str(flightno2):  # Check if identifier matches
                                            dep = segment.get("designator", {}).get("departure")
                                            if dep:
                                                departure = datetime.fromisoformat(dep)
                                                departure = departure.date().isoformat()
        except Exception as e:
            self.logger.error(f"Exception in get_second_flight_dep: {e}")
        return departure