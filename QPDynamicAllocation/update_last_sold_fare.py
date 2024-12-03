import logging
from sys import exit


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
        
        whereCondition = "DATEADD(HOUR, -48, GETDATE())"

        df = self.lastSoldQuery(whereCondition)
        if df.shape[0] != 0:
            df['FareBasis'] = df.groupby(['BookingID', 'PassengerID'])['FareBasis'].apply(
                lambda x: x.replace('', pd.NA).ffill().bfill().replace(pd.NA, ''))

            # Convert BookingUTC to datetime
            df['BookingUTC'] = pd.to_datetime(df['BookingUTC'])

            # Sort the DataFrame by BookingUTC
            df = df.sort_values(by='BookingUTC')

            # Group by the required columns and get the last three bookings
            last_three_bookings = df.groupby(['FareBasis', 'DepartureStation', 'ArrivalStation', 'FlightNumber', 'DepartureDate']) \
                .apply(lambda x: x.nlargest(3, 'BookingUTC')) \
                .reset_index(drop=True)

            # Calculate average fare for groups with at least three bookings
            avg_fares = last_three_bookings.groupby(['FareBasis', 'DepartureStation', 'ArrivalStation', 'FlightNumber', 'DepartureDate']) \
    .apply(lambda x: x['Fare'].mean() if len(x) >= 3 else None) \
    .reset_index(name='AverageFare')

            # Remove None values
            avg_fares = avg_fares.dropna()

            # Rename columns for clarity
            avg_fares.columns = ['FareBasis', 'DepartureStation', 'ArrivalStation', 'FlightNumber', 'DepartureDate', 'AverageFare']

            # Filter out groups with fewer than 3 bookings
            avg_fares = avg_fares[avg_fares['FareBasis'].notna() & (avg_fares['FareBasis'] != '')]

            self.updateValuesToRedis(avg_fares)

        self.cache_client.set("LastSoldFare:executionTime", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        print("completed")

    def updateValuesToRedis(self, avg_fares):
        for index, row in avg_fares.iterrows():
            # Generate the composite key
            key = self.generate_key(row)
            # Get the average fare value
            value = row["AverageFare"]
            # Set the key in Redis with a 48-hour expiration
            self.cache_client.set(key, value, 172800)

    def lastSoldQuery(self, whereCondition):
        query = f"""
        Select 
            BookingID, BookingUTC, PassengerID, DepartureStation, ArrivalStation, DepartureDate, FlightNumber, SUBSTRING(FareBasis, 5, 1) as FareBasis, Fare
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
                  GROUP BY
                    B.BookingID, BP.PassengerID, PJS.SegmentID, IL.InventoryLegID, IL.DepartureStation, IL.ArrivalStation, IL.DepartureDate, IL.FlightNumber,
                    PJS.TripNumber, PJS.JourneyNumber, PJS.SegmentNumber, PJS.FareComponentNumber, PJS.FareJourneyType, PJS.FareBasis, B.BookingUTC, B.RecordLocator
                ) X
              ) Y
              WHERE
                (SUBSTRING(FareBasis, 5, 1) IN ('M', 'R') ) AND SUBSTRING(FareBasis, 3, 1) != 'C'
        """
        df = pd.read_sql(query, self.NavitaireConnection)
        return df

    def generate_key(self, row):
        return f"LastSoldFare:{row['DepartureStation']}{row['ArrivalStation']}{row['FlightNumber']}_{row['DepartureDate']}_{row['FareBasis']}"

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