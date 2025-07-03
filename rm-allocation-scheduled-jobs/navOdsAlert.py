import json
import configsettings
import pandas as pd
import smtplib
from email.message import EmailMessage
from datetime import datetime, timezone, timedelta
from sqlalchemy import text
from constants_utils import ConstantsUtils
import traceback
import requests
from Constants import AKASA_AIRLINE_CODE

class NavOdsAlert:

    def __init__(self, logger, params, overrides, context):
        self.context = context
        self.logger = logger
        self.navconn = params['navconn']
        self.rm_rd_conn = params['wrconn']
        self.cache_client = params['cache_client']
        self.constants = ConstantsUtils(self.rm_rd_conn)
        self.sector_list = json.loads(self.constants.NAVODS_ALERT_SECTOR_LIST)
        self.max_ndo = float(self.constants.NAVODS_ALERT_MAX_NDO)
        self.session = requests.Session()
        self.session.headers.update(
   			 {"Ocp-Apim-Subscription-Key": configsettings.NAV_OCP_SUBSCRIPTION_KEY}
		)
        self.get_token()

    def run(self):
        try:
            self.logger.info("Starting the run method.")
            currDate = datetime.now(timezone.utc)
            futureDate = currDate + timedelta(days=self.max_ndo)
            
            df = pd.DataFrame({'flight': self.sector_list})

            # Getting Bookedloads from Nav API
            for flight in self.sector_list:
                seatsSold = 0
                response = legKeyData = None
                getLegKeyUrl = f"{configsettings.NAV_BASE_URL}/api/nsk/v2/trip/info/simple?Origin={flight[:3]}&Destination={flight[3:6]}&Identifier={flight[6:]}&BeginDate={currDate.date()}&endDate={futureDate.date()}&CarrierCode={AKASA_AIRLINE_CODE}&FlightType=3"

                response = self.session.get(getLegKeyUrl)
                legKeyData = response.json().get('data', [])

                if not legKeyData or len(legKeyData) == 0:
                    self.logger.info("No data found in Nav API")

                for date in range(len(legKeyData)):
                    for journey in range(len(legKeyData[date]['journeys'])):
                        legKey = legKeyData[date]['journeys'][journey]['segments'][0]['legs'][0]['legKey']

                        getSoldSeatsUrl = f"{configsettings.NAV_BASE_URL}/api/nsk/v1/manifest/{legKey}"

                        response = self.session.get(getSoldSeatsUrl)
                        soldSeatsData = response.json().get('data', [])
                        seatsSold += soldSeatsData['passengerCounts']['manifested']
                
                df.loc[df['flight'] == flight, 'api_sold_seats'] = seatsSold

            # Getting Bookedloads from Nav ODS
            NavitaireQuery = f""" EXEC {configsettings.NAV_DB_WB_NAME}[dbo].[SP_Bookeddata]
                                @DepartureDate_from = N'{str(currDate)}',
                                @DepartureDate_to = N'{str(futureDate)}'
                            """
            bookedData = pd.read_sql(NavitaireQuery, self.navconn)

            if bookedData.empty or len(bookedData) == 0:
                self.logger.info("Run method completed. No data found in Nav ODS.")
                return
            
            bookedData['flight'] = bookedData['Origin'] + bookedData['Destination'] + bookedData['FlightNumber']
            groupedBookedData = bookedData.groupby(['flight']).agg({
                'bookedPax': 'sum'
            }).reset_index()
            groupedBookedData = groupedBookedData.rename(columns={'bookedPax': 'ods_sold_seats'})

            filteredBookedData = groupedBookedData[groupedBookedData['flight'].isin(self.sector_list)]

            # Comparing Bookedloads
            df = df.merge(filteredBookedData[['flight', 'ods_sold_seats']], on='flight', how='left')
            df['ods_sold_seats'] = df['ods_sold_seats'].fillna(0)

            is_ods_down = (df['ods_sold_seats'] < df['api_sold_seats']).any()
            self.logger.info(df)

            if (is_ods_down):
                self.send_email_alert(
                    "Nav ODS Alert", 
                    "Bookedload from Nav ODS is lesser than the bookedload from Nav API.\nPlease check Nav ODS.", 
                    configsettings.RM_IT_ALERT_EMAIL
                    )

            self.logger.info("Run method completed.")
        except Exception as e:
            print(traceback.format_exc())
            self.logger.error(f"An error occurred in the run method: {str(e)}")
    
    def send_email_alert(self, subject, body, to_email):
        msg = EmailMessage()
        msg.set_content(body)
        msg['Subject'] = subject
        msg['From'] = configsettings.SENDER_EMAIL
        msg['To'] = to_email

        try:
            with smtplib.SMTP(configsettings.SMTP_SERVER, configsettings.SMTP_PORT) as server:
                server.starttls()
                server.login(configsettings.SENDER_EMAIL, configsettings.SENDER_PASSWORD)
                server.send_message(msg)
                self.logger.info(f"Alert email sent to {to_email}")

        except Exception as e:
            self.logger.error(f"Failed to send email to {to_email}: {e}")
    
    def get_token(self):
        url = f"{configsettings.NAV_BASE_URL}/api/nsk/v2/token"
        credentials = {
            "credentials": {
                "username": configsettings.NAV_USERNAME,
                "password": configsettings.NAV_PWD,
                "domain": configsettings.NAV_DOMAIN
            }
        }
        response = self.session.post(url, json=credentials)
        if response.status_code == 201:
            self.token = response.json().get('data', {}).get('token')
            self.session.headers.update({"Authorization": f"Bearer {self.token}"})
            
        else:
            self.logger.error("Failed to get token:"+ response.text)
