from datetime import datetime
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
        self.session.headers.update(
   			 {"Ocp-Apim-Subscription-Key": self.NAV_OCP_SUBSCRIPTION_KEY}
		)
        self.get_token()

    def get_token(self):
        url = f"{self.base_url}/nsk/v2/token"
        credentials = {
            "credentials": {
                "username": self.NAV_USERNAME,
                "password": self.NAV_PWD,
                "domain": self.NAV_DOMAIN
            }
        }
        response = self.session.post(url, json=credentials)
        if response.status_code == 201:
            self.token = response.json().get('data', {}).get('token')
            self.session.headers.update({"Authorization": f"Bearer {self.token}"})
            
        else:
            self.logger.info("Failed to get token:"+ response.text)

    def get_leg_nest_details(self,  dep_date, carrier_code, identifier, origin, destin ):
        dep_date=datetime.strptime(str(dep_date), DATE_FORMAT).date().strftime(DATE_FORMAT_2)
        url = f"{self.base_url}/nsk/v2/manifest?BeginDate={dep_date}&endDate={dep_date}&CarrierCode={carrier_code}&Identifier={identifier}&FlightType=3"
        response = self.session.get(url)
        data = response.json().get('data', [])
        for item in data:
            for journey in item.get('journeys', []):
                if journey['designator']['destination'] == destin and journey['designator']['origin'] == origin:
                    for segment in journey.get('segments', []):
                        for leg in segment['legs']:
                            leg_key = leg['legKey']
                            nest_key,lid,nest_lid = self.get_nest_key(leg_key)
                            return leg_key,nest_key,lid,nest_lid
        else:
            self.logger.info("Failed to get leg and nest keys:"+ response.text)
        return None,None,None,None
    
    def get_nest_key(self, leg_key):
        headers = {'Authorization': f'Bearer {self.token}'}
        url = f'{self.base_url}/dcs/v2/inventory/legs/{leg_key}'
        response = self.session.get(url, headers=headers)

        if response.status_code != 200:
            return None,None,None

        nests = response.json().get('data', {}).get('nests', [])
        lid = response.json().get('data', {}).get('legInformation', {}).get('lid')
        for nest in nests:
            nest_key = nest.get('nestKey')
            nest_lid = nest.get('lid')

            return nest_key,lid,nest_lid
        return None,None,None
    
    def update_leg(self, leg_key, lid):
        url = f"{self.base_url}/dcs/v1/inventory/legs/{leg_key}"
        data = {"lid": int(lid)}
        self.logger.info('lid api called '+ str(url))
        self.logger.info('payload sent '+ str(data))
        
        response = self.session.patch(url, json=data)
        if response.status_code == 200:
            self.logger.info('lid changed sucessfully to '+ str(lid))
            self.logger.info("Response :"+ response.text)
        else:
            self.logger.info("Failed to update leg :"+ response.text)
           
        
    def update_leg_nest(self, leg_key, nest_key, lid):
        url = f"{self.base_url}/dcs/v1/inventory/legs/{leg_key}/nests/{nest_key}"
        data = {"lid": int(lid)}
        response = self.session.patch(url, json=data)
        if response.status_code == 200:
            self.logger.info('nest lid changed sucessfully to '+ str(lid))
        else:
            self.logger.info("Failed to update leg nest:"+ response.text)
    
    
    def get_second_flight_dep(self,flightno1,flightno2,org,destin,depDate):
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
        response = self.session.post(trip_info_simple_url, json=data, verify=False)
        data = response.json()
        departure=""
        for result in data["data"]["results"]:
            for trip in result["trips"]:
                for journey in trip["journeysAvailableByMarket"].values():
                    for j in journey:
                        for segment in j["segments"]:
                            identifier = segment["identifier"]["identifier"]
                            if identifier == str(flightno2):  # Check if identifier is '2312'
                                # origin = segment["designator"]["origin"]
                                # destination = segment["designator"]["destination"]
                                departure = segment["designator"]["departure"]
                                departure=datetime.fromisoformat(departure)
                                departure=departure.date().isoformat()
        return departure