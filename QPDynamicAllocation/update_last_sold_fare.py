import logging
from sys import exit
import json

import pandas as pd
import pymssql
from redis import Redis
from datetime import datetime
from boto3 import client


class LastSoldFare:

    def __init__(self):
        self.NavitaireConnection = None
        self.logger = None
        self.cache_client = None
        self.generateConfigs()

        self.get_last_sold_fare()

    def generateConfigs(self):
        CACHE_ENDPOINT = "ip-10-16-6-51.ap-south-1.compute.internal"
        # CACHE_ENDPOINT = "ip-172-24-72-8.ap-south-1.compute.internal"
        # CACHE_ENDPOINT = "172.24.72.8"
        CACHE_PORT = 6379
        CACHE_USERNAME = "default"
        CACHE_PASSWORD = self.get_credentials('/rm/prod/rm-allocation/RM_CACHE_PASSWORD')
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)
        try:

            self.cache_client = Redis(host=CACHE_ENDPOINT, port=CACHE_PORT, username=CACHE_USERNAME, password=CACHE_PASSWORD)
            if self.cache_client.ping():
                print("Connected to Redis")

            self.NavitaireConnection = pymssql.connect(
                host="10.154.12.13:52900",
                user="0KODSUSER2",
                password=self.get_credentials('/rm/prod/rm-allocation/NAV_DB_PASSWORD')
            )
        except Exception as e:
            self.logger.error(e)
            exit()

    def get_last_sold_fare(self):
        time = None
        if self.cache_client.exists("LastSoldFare:executionTime"):
            time = self.cache_client.get("LastSoldFare:executionTime").decode('utf-8')
        
        whereCondition = f"'{str(time)}'"
        if time is None:
            whereCondition = "DATEADD(HOUR, -168, GETDATE())"

        df = self.lastSoldQuery(whereCondition)
        if df.shape[0] != 0:
            df['FareBasis'] = df.groupby(['BookingID', 'PassengerID'])['FareBasis'].apply(
                lambda x: x.replace('', pd.NA).ffill().bfill().replace(pd.NA, ''))

            # Convert BookingUTC to datetime
            df['BookingUTC'] = pd.to_datetime(df['BookingUTC'])

            # Sort the DataFrame by BookingUTC
            df = df.sort_values(by='BookingUTC')
            df = df.loc[df.groupby('BookingID')['BookingUTC'].idxmax()]
            # Group by the required columns and get the last booking based on BookingUTC
            last_booking_df = df.groupby(['DepartureStation', 'ArrivalStation', 'FlightNumber', 'DepartureDate', 'FareBasis']).apply(
                lambda x: x.nlargest(7, 'BookingUTC')).reset_index(drop=True)

            last_booking_df = last_booking_df[
                last_booking_df['FareBasis'].notna() & (last_booking_df['FareBasis'] != '')]

            self.updateValuesToRedis(last_booking_df)

        self.cache_client.set("LastSoldFare:executionTime", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        print("completed")

    def updateValuesToRedis(self, last_booking_df):
        for (departure, arrival, flight, date), group in last_booking_df.groupby(['DepartureStation', 'ArrivalStation', 'FlightNumber', 'DepartureDate']):
            # Generate the composite key
            key = f"LastSoldFare:{departure}{arrival}{flight}_{date}"
            
            # Prepare all fares list
            new_all_fares = group.sort_values('BookingUTC', ascending=False).apply(lambda row: {
                'BookingID': row['BookingID'],
                'FareBasis': row['FareBasis'],
                'Fare': row['Fare'],
                'BookingUTC': row['BookingUTC'].strftime('%Y-%m-%dT%H:%M:%S.%f')
            }, axis=1).tolist()
            
            # Check if key exists in Redis
            existing_data = self.cache_client.get(key)
            
            if existing_data:
                # Parse existing data
                existing_dict = json.loads(existing_data.decode('utf-8'))
                
                # Merge and deduplicate all fares
                merged_all_fares = self._merge_and_limit_entries(existing_dict.get('allfares', []), new_all_fares, 7)
                
                # Separate and merge B2B and B2C fares
                b2b_fares = self._merge_and_limit_entries(
                    existing_dict.get('b2bfares', []), 
                    [fare for fare in new_all_fares if fare['FareBasis'] == 'M'], 
                    3
                )
                
                b2c_fares = self._merge_and_limit_entries(
                    existing_dict.get('b2cfares', []), 
                    [fare for fare in new_all_fares if fare['FareBasis'] in ['R', 'C', 'V', 'Z']], 
                    3
                )
                
                # Prepare final dictionary
                final_data = {
                    'allfares': merged_all_fares,
                    'b2bfares': b2b_fares,
                    'b2cfares': b2c_fares
                }
            else:
                # If no existing data, create new lists
                final_data = {
                    'allfares': new_all_fares[:7],
                    'b2bfares': [fare for fare in new_all_fares if fare['FareBasis'] == 'M'][:3],
                    'b2cfares': [fare for fare in new_all_fares if fare['FareBasis'] in ['R', 'C', 'V', 'Z']][:3]
                }

            # Store in Redis as JSON
            self.cache_client.set(key, json.dumps(final_data))
    
    def _merge_and_limit_entries(self, existing_list, new_list, limit):
        # Combine lists and remove duplicates
        combined_list = []
        seen_bookings = set()
        
        # Combine new and existing entries
        for item in new_list + existing_list:
            if item['BookingID'] not in seen_bookings:
                combined_list.append(item)
                seen_bookings.add(item['BookingID'])
        
        # Sort by BookingUTC and keep top entries
        return sorted(combined_list, 
                     key=lambda x: datetime.strptime(x['BookingUTC'], '%Y-%m-%dT%H:%M:%S.%f'), 
                     reverse=True)[:limit]



    def lastSoldQuery(self, whereCondition):
        query = f"""
        Select 
            BookingID, BookingUTC, PassengerID, DepartureStation, ArrivalStation, DepartureDate, FlightNumber, 
            SUBSTRING(FareBasis, 5, 1) as FareBasis, Fare
            from 
            (
              Select
                BookingID, BookingUTC, PassengerID, SegmentID, InventoryLegID, DepartureStation, ArrivalStation, DepartureDate, FlightNumber, FareBasis, RecordLocator,
                CAST(sum(BaseFare+AirportCharges+Taxes) over (Partition by PassengerID,TripNumber,JourneyNumber,ModSegmentNumber)*(SQRT(Distance))/SUM(SQRT(Distance)) OVER (Partition by PassengerID,TripNumber,JourneyNumber,ModSegmentNumber) as decimal(19,4)) as Fare
                from 
                (
                  SELECT
                    B.RecordLocator, B.BookingID, B.BookingUTC, BP.PassengerID, PJS.SegmentID, IL.InventoryLegID, 
                    IL.DepartureStation, IL.ArrivalStation, IL.DepartureDate, IL.FlightNumber,
                    PJS.TripNumber, PJS.JourneyNumber, PJS.SegmentNumber, PJS.FareComponentNumber, PJS.FareJourneyType, PJS.FareBasis,
                    Case when PJS.FareJourneyType in (1,4) then SUM(Case when PJC.ChargeType in (0,1,2,3) then (PJC.ChargeAmount*CTM.PositiveNegativeFlag*CCV.ConversionRate) else 0 end) 
                      when PJS.FareJourneyType=2 then SUM(Case when PJC.ChargeType in (0,1,2,3) and PJS.SegmentNumber=PJL.LegNumber then (PJC.ChargeAmount*CTM.PositiveNegativeFlag*CCV.ConversionRate) else 0 end) else 0 end as BaseFare,
                    Case when PJS.FareJourneyType in (1,4) then SUM(Case when PJC.ChargeType=4 then (PJC.ChargeAmount*CTM.PositiveNegativeFlag*CCV.ConversionRate) else 0 end) 
                      when PJS.FareJourneyType=2 then SUM(Case when PJC.ChargeType=4 and PJS.SegmentNumber=PJL.LegNumber then (PJC.ChargeAmount*CTM.PositiveNegativeFlag*CCV.ConversionRate) else 0 end) else 0 end as AirportCharges,
                    Case when PJS.FareJourneyType in (1,4) then SUM(Case when PJC.ChargeType=5 then (PJC.ChargeAmount*CTM.PositiveNegativeFlag*CCV.ConversionRate) else 0 end) 
                    + SUM(Case when PJC.ChargeType=3 and CTM.IncludedTaxFlag=1  then (PJC.ChargeAmount*CCV.ConversionRate) else 0 end)
                    when PJS.FareJourneyType=2 then SUM(Case when PJC.ChargeType=5 and PJS.SegmentNumber=PJL.LegNumber then (PJC.ChargeAmount*CTM.PositiveNegativeFlag*CCV.ConversionRate) else 0 end) 
                    + SUM(Case when PJC.ChargeType=3 and CTM.IncludedTaxFlag=1  then (PJC.ChargeAmount*CCV.ConversionRate) else 0 end)
                    else 0 end as Taxes,
                    case when PJS.FareBasis='' then 1 else SegmentNumber end as ModSegmentNumber,
                    avg(CP.ActualDistance) as Distance
                  FROM
                    REZ0KWB01.rez.Booking B
                    INNER JOIN REZ0KWB01.rez.BookingPassenger BP ON BP.BookingID = B.BookingID
                    INNER JOIN REZ0KWB01.rez.PassengerJourneySegment PJS ON PJS.PassengerID = BP.PassengerID AND PJS.ClassOfService NOT IN ('G', 'S') 
                    INNER JOIN REZ0KWB01.rez.PassengerJourneyLeg PJL ON PJL.PassengerID = PJS.PassengerID AND PJL.SegmentID = PJS.SegmentID
                    INNER JOIN REZ0KWB01.rez.InventoryLeg IL ON IL.InventoryLegId = PJL.InventoryLegID AND IL.Status NOT IN (2) AND IL.Lid > 0
                    LEFT JOIN REZ0KWB01.rez.PassengerJourneyCharge PJC ON PJC.PassengerID = PJL.PassengerID AND PJC.SegmentID = PJL.SegmentID
                    LEFT JOIN REZ0KWB01.dw.ChargeTypeMatrix CTM ON CTM.ChargeTypeID = PJC.ChargeType
                    Left join REZ0KWB01.dw.CurrencyConversionVersion CCV ON CCV.FromCurrencyCode=PJC.CurrencyCode and CCV.ToCurrencyCode='INR' and CCV.ClientCode='0K' and CCV.VersionStartUTC<=PJC.CreatedUTC and CCV.VersionEndUTC>PJC.CreatedUTC
                    inner join REZ0KWB01.dw.CityPair CP ON CP.DepartureStation = IL.DepartureStation and CP.ArrivalStation = IL.ArrivalStation 
                  WHERE
                    BookingUTC >= {whereCondition}
                    AND SUBSTRING(FareBasis, 5, 1) IN ('M', 'R', 'C', 'V', 'Z')
                  GROUP BY
                    B.BookingID, BP.PassengerID, PJS.SegmentID, IL.InventoryLegID, IL.DepartureStation, IL.ArrivalStation, IL.DepartureDate, IL.FlightNumber,
                    PJS.TripNumber, PJS.JourneyNumber, PJS.SegmentNumber, PJS.FareComponentNumber, PJS.FareJourneyType, PJS.FareBasis, B.BookingUTC, B.RecordLocator
                ) X
              ) Y
              WHERE
                SUBSTRING(FareBasis, 3, 1) != 'C'
        """
        df = pd.read_sql(query, self.NavitaireConnection)
        return df

    def get_credentials(self, parameter_name):
        ssm_client = client('ssm', region_name='ap-south-1')
        try:
            response = ssm_client.get_parameter(Name=parameter_name, WithDecryption=True)
        except ssm_client.exceptions.ParameterNotFound:
            print("not found")
        parameter_value = response['Parameter']['Value']
        return parameter_value


if __name__ == "__main__":
    LastSoldFare()