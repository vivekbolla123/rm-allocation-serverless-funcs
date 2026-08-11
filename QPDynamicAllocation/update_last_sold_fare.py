import logging
from sys import exit
import json
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine
from redis import Redis
from datetime import datetime, timezone, timedelta
from boto3 import client
from pymongo import MongoClient
from common_utils import instrument_engine

class LastSoldFare:

    def __init__(self):
        self.NavitaireConnection = None
        self.rmrdconn = None  # This will now be MongoDB client
        self.rm_db = None  # MongoDB database
        self.logger = None
        self.cache_client = None
        self.generateConfigs()
        self.get_filter_configs(['allfares', 'b2bfares', 'b2cfares'])
        self.get_last_sold_fare()

    def generateConfigs(self):
        ISPROD = True
        if ISPROD:
            CACHE_ENDPOINT = "ip-10-16-6-51.ap-south-1.compute.internal"
            CACHE_PORT = 6379
            CACHE_USERNAME = "default"
            CACHE_PASSWORD = self.get_credentials('/rm/prod/rm-allocation/RM_CACHE_PASSWORD')
            self.logger = logging.getLogger()
            self.logger.setLevel(logging.INFO)
            self.NAV_DB_NAME = "REZ0KWB01"
            
            # MongoDB connection
            RM_RD_PASSWORD = self.get_credentials('/rm/prod/rm-allocation/RM_MONGO_PASSWORD')
            encoded_password = quote_plus(RM_RD_PASSWORD)
            MONGO_CONN_STRING = f"mongodb://rmuser:{encoded_password}@10.16.6.179:27017/"
            # Or if using MongoDB Atlas or other format:
            # MONGO_CONN_STRING = f"mongodb+srv://rmappuser:{RM_RD_PASSWORD}@your-cluster.mongodb.net/"

            try:
                self.cache_client = Redis(host=CACHE_ENDPOINT, port=CACHE_PORT, username=CACHE_USERNAME, password=CACHE_PASSWORD)
                if self.cache_client.ping():
                    print("Connected to Redis")

                self.NavitaireConnection = create_engine(
                    f"mssql+pymssql://0KODSUSER2:"
                    f"{quote_plus(self.get_credentials('/rm/prod/rm-allocation/NAV_DB_PASSWORD'))}@"
                    f"10.154.12.13:52900"
                )
                instrument_engine(self.NavitaireConnection)

                # MongoDB connection
                self.rmrdconn = MongoClient(MONGO_CONN_STRING)
                self.rm_db = self.rmrdconn['qp_dw_rmalloc']  # Database name
                print("Connected to MongoDB")
                
            except Exception as e:
                self.logger.error(e)
                exit()
                
        else:
            CACHE_ENDPOINT = "172.24.72.8"
            CACHE_PORT = 6379
            CACHE_USERNAME = "default"
            CACHE_PASSWORD = self.get_credentials('/rm/uat/rm-allocation/RM_CACHE_PASSWORD')
            self.logger = logging.getLogger()
            self.logger.setLevel(logging.INFO)
            self.NAV_DB_NAME = "TST0KWB01"
            
            # MongoDB connection
            RM_RD_PASSWORD = self.get_credentials('/rm/uat/rm-allocation/RM_MONGO_PASSWORD')
            encoded_password = quote_plus(RM_RD_PASSWORD)
            MONGO_CONN_STRING = f"mongodb://testuser:{encoded_password}@172.24.72.35:27017/"
            
            try:
                self.cache_client = Redis(host=CACHE_ENDPOINT, port=CACHE_PORT, username=CACHE_USERNAME, password=CACHE_PASSWORD)
                if self.cache_client.ping():
                    print("Connected to Redis")

                self.NavitaireConnection = create_engine(
                    f"mssql+pymssql://0KODSUSER2:"
                    f"{quote_plus(self.get_credentials('/rm/uat/rm-allocation/NAV_DB_PASSWORD'))}@"
                    f"10.154.12.19:52900"
                )
                instrument_engine(self.NavitaireConnection)
                
                # MongoDB connection
                self.rmrdconn = MongoClient(MONGO_CONN_STRING)
                self.rm_db = self.rmrdconn['qp_dw_rmalloc']  # Database name
                print("Connected to MongoDB")
                
            except Exception as e:
                self.logger.error(e)
                exit()
                
    def get_filter_configs(self, filter_names):
        # MongoDB query instead of SQL
        collection = self.rm_db['rm_parameter_values']
        
        # Build filter parameter keys
        filter_keys = [f'fare_filter_{name}' for name in filter_names]
        
        # Query MongoDB
        cursor = collection.find({
            'parameterKey': {'$in': filter_keys}
        })
        
        # Convert to list of documents
        documents = list(cursor)
        
        # Build configs dictionary
        configs = {name: {} for name in filter_names}
        for doc in documents:
            filter_name = doc['parameterKey'].replace('fare_filter_', '')
            param_value = doc.get('parameterValue')
            configs[filter_name] = json.loads(param_value) if param_value else {}
        
        self.filter_config = configs
    
    def apply_filter(self, fares, filter_name):
        config = self.filter_config.get(filter_name, {})
        excluded_prefixes = config.get('excluded_class_prefixes', [])
        excluded_classes = config.get('excluded_classes', [])
        included_fare_basis = config.get('included_fare_basis', [])
        
        filtered_fares = []
        for fare in fares:
            # Check class prefix exclusions - G
            if any(fare['ClassOfService'].startswith(prefix) for prefix in excluded_prefixes):
                continue
            
            # Check class exclusions - G7
            if fare['ClassOfService'] in excluded_classes:
                continue
            
            # Check fare basis inclusion
            if included_fare_basis and fare['FareBasis'] not in included_fare_basis:
                continue
            
            filtered_fares.append(fare)
        return filtered_fares

    def get_last_sold_fare(self):
        time = None
        lastExecTime = None
        if self.cache_client.exists("LastSoldFare:executionTime"):
            lastExecTime = self.cache_client.get("LastSoldFare:executionTime").decode('utf-8')

        curr_minus_20 = datetime.now(timezone.utc) - timedelta(minutes=20)
        time = curr_minus_20.replace(second=0, microsecond=0, tzinfo=None)

        try:
            if lastExecTime:
                lastExecTime = datetime.strptime(lastExecTime, '%Y-%m-%d %H:%M:%S').replace(second=0, tzinfo=None)

                if lastExecTime < time:
                    time = lastExecTime - timedelta(minutes=20)

        except Exception as e:
            self.logger.error(f"Error parsing last execution time: {e}")
            time = curr_minus_20.replace(second=0)

        time = time.strftime("%Y-%m-%d %H:%M:%S")
        
        whereCondition = f"'{str(time)}'"
        if lastExecTime is None:
            whereCondition = "DATEADD(HOUR, -960, GETDATE())"  # 40 days

        self.cache_client.set("LastSoldFare:executionTime", datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'))
        
        df = self.lastSoldQuery(whereCondition)
        if df.shape[0] != 0:
            df['FareBasis'] = df.groupby(['BookingID', 'PassengerID'])['FareBasis'].apply(
                lambda x: x.replace('', pd.NA).ffill().bfill().replace(pd.NA, ''))

            # Convert BookingUTC to datetime
            df['BookingUTC'] = pd.to_datetime(df['BookingUTC'])

            # Sort the DataFrame by BookingUTC
            df = df.sort_values(by='BookingUTC')
            df = df.loc[df.groupby(['BookingID', 'PassengerID'])['BookingUTC'].idxmax()]

            df = df[
                df['FareBasis'].notna() & (df['FareBasis'] != '')]

            self.updateValuesToRedis(df)

        print("completed")

    def updateValuesToRedis(self, last_booking_df):
        last_booking_df['FlightNumber'] = last_booking_df['FlightNumber'].astype(str).str.strip()
        for (departure, arrival, flight, date), group in last_booking_df.groupby(['DepartureStation', 'ArrivalStation', 'FlightNumber', 'DepartureDate']):
            # Generate the composite key
            key = f"LastSoldFare:{departure}{arrival}{flight}_{date}"
            
            # Prepare all fares list
            new_all_fares = group.sort_values('BookingUTC', ascending=False).apply(lambda row: {
                'BookingID': row['BookingID'],
                'PassengerID': row['PassengerID'],
                'ClassOfService': row['ClassOfService'],
                'FareBasis': row['FareBasis'],
                'Fare': row['Fare'],
                'BookingUTC': row['BookingUTC'].strftime('%Y-%m-%dT%H:%M:%S.%f')
            }, axis=1).tolist()

            filtered_all_fares = self.apply_filter(new_all_fares, 'allfares')
            filtered_b2b_fares = self.apply_filter(new_all_fares, 'b2bfares')
            filtered_b2c_fares = self.apply_filter(new_all_fares, 'b2cfares')
            
            # Check if key exists in Redis
            existing_data = self.cache_client.get(key)
            
            if existing_data:
                # Parse existing data
                existing_dict = json.loads(existing_data.decode('utf-8'))
                
                # Merge and deduplicate all fares (Booking Level)
                merged_all_fares = self._merge_and_limit_entries(existing_dict.get('allfares', []), filtered_all_fares, 7, False)
                b2b_fares = self._merge_and_limit_entries(existing_dict.get('b2bfares', []), filtered_b2b_fares, 3, False)
                b2c_fares = self._merge_and_limit_entries(existing_dict.get('b2cfares', []), filtered_b2c_fares, 3, False)
                
                # Merge and deduplicate all fares (Passenger Level)
                merged_all_fares_pax = self._merge_and_limit_entries(existing_dict.get('allfares_pax', []), filtered_all_fares, 7, True)
                b2b_fares_pax = self._merge_and_limit_entries(existing_dict.get('b2bfares_pax', []), filtered_b2b_fares, 3, True)
                b2c_fares_pax = self._merge_and_limit_entries(existing_dict.get('b2cfares_pax', []), filtered_b2c_fares, 3, True)
                
                # Prepare final dictionary
                final_data = {
                    'allfares': merged_all_fares,
                    'b2bfares': b2b_fares,
                    'b2cfares': b2c_fares,
                    'allfares_pax': merged_all_fares_pax,
                    'b2bfares_pax': b2b_fares_pax,
                    'b2cfares_pax': b2c_fares_pax
                }
            else:
                # If no existing data, create new lists using the new merge function to respect limits and deduplication
                final_data = {
                    'allfares': self._merge_and_limit_entries([], filtered_all_fares, 7, False),
                    'b2bfares': self._merge_and_limit_entries([], filtered_b2b_fares, 3, False),
                    'b2cfares': self._merge_and_limit_entries([], filtered_b2c_fares, 3, False),
                    'allfares_pax': self._merge_and_limit_entries([], filtered_all_fares, 7, True),
                    'b2bfares_pax': self._merge_and_limit_entries([], filtered_b2b_fares, 3, True),
                    'b2cfares_pax': self._merge_and_limit_entries([], filtered_b2c_fares, 3, True)
                }

            # Store in Redis as JSON
            self.cache_client.set(key, json.dumps(final_data))
    
    def _merge_and_limit_entries(self, existing_list, new_list, limit, dedup_by_pax):
        # Combine lists and remove duplicates
        combined_list = []
        seen_bookings = set()
        
        # Combine new and existing entries
        for item in new_list + existing_list:
            unique_key = item['BookingID']
            if dedup_by_pax:
                pax_id = item.get('PassengerID', '')
                unique_key = f"{item['BookingID']}_{pax_id}"
                
            if unique_key not in seen_bookings:
                combined_list.append(item)
                seen_bookings.add(unique_key)
        
        # Sort by BookingUTC and keep top entries
        return sorted(combined_list, 
                     key=lambda x: datetime.strptime(x['BookingUTC'], '%Y-%m-%dT%H:%M:%S.%f'), 
                     reverse=True)[:limit]

    def lastSoldQuery(self, whereCondition):
        # This still uses MSSQL for Navitaire database
        query = f"""
        Select 
            BookingID, BookingUTC, PassengerID, DepartureStation, ArrivalStation, DepartureDate, FlightNumber, 
            CASE 
                WHEN LEFT(FareBasis, 1) = 'G' THEN 'G' 
                ELSE SUBSTRING(FareBasis, 5, 1) 
            END as FareBasis, ClassOfService,
            Fare
            from 
            (
              Select
                BookingID, BookingUTC, PassengerID, SegmentID, InventoryLegID, DepartureStation, ArrivalStation, DepartureDate, FlightNumber, FareBasis, RecordLocator,
                CAST(sum(BaseFare+AirportCharges+Taxes) over (Partition by PassengerID,TripNumber,JourneyNumber,ModSegmentNumber)*(SQRT(Distance))/SUM(SQRT(Distance)) OVER (Partition by PassengerID,TripNumber,JourneyNumber,ModSegmentNumber) as decimal(19,4)) as Fare, ClassOfService
                from 
                (
                  SELECT
                    B.RecordLocator, B.BookingID, B.BookingUTC, B.Status, BP.PassengerID, PJS.SegmentID, IL.InventoryLegID, 
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
                    avg(CP.ActualDistance) as Distance,
                    PJS.ClassOfService
                  FROM
                    {self.NAV_DB_NAME}.rez.Booking B
                    INNER JOIN {self.NAV_DB_NAME}.rez.BookingPassenger BP ON BP.BookingID = B.BookingID
                    INNER JOIN {self.NAV_DB_NAME}.rez.PassengerJourneySegment PJS ON PJS.PassengerID = BP.PassengerID AND PJS.ClassOfService NOT IN ('S') 
                    INNER JOIN {self.NAV_DB_NAME}.rez.PassengerJourneyLeg PJL ON PJL.PassengerID = PJS.PassengerID AND PJL.SegmentID = PJS.SegmentID
                    INNER JOIN {self.NAV_DB_NAME}.rez.InventoryLeg IL ON IL.InventoryLegId = PJL.InventoryLegID AND IL.Status NOT IN (2) AND IL.Lid > 0
                    LEFT JOIN {self.NAV_DB_NAME}.rez.PassengerJourneyCharge PJC ON PJC.PassengerID = PJL.PassengerID AND PJC.SegmentID = PJL.SegmentID
                    LEFT JOIN {self.NAV_DB_NAME}.dw.ChargeTypeMatrix CTM ON CTM.ChargeTypeID = PJC.ChargeType
                    Left join {self.NAV_DB_NAME}.dw.CurrencyConversionVersion CCV ON CCV.FromCurrencyCode=PJC.CurrencyCode and CCV.ToCurrencyCode='INR' and CCV.ClientCode='0K' and CCV.VersionStartUTC<=PJC.CreatedUTC and CCV.VersionEndUTC>PJC.CreatedUTC
                    inner join {self.NAV_DB_NAME}.dw.CityPair CP ON CP.DepartureStation = IL.DepartureStation and CP.ArrivalStation = IL.ArrivalStation 
                  WHERE
                    BookingUTC >= {whereCondition}
                    AND B.Status = 2
                    AND (SUBSTRING(FareBasis, 5, 1) IN ('M', 'R', 'C', 'V', 'Z')
		            OR LEFT(FareBasis, 1) = 'G')
                  GROUP BY
                    B.BookingID, B.Status, BP.PassengerID, PJS.SegmentID, IL.InventoryLegID, IL.DepartureStation, IL.ArrivalStation, IL.DepartureDate, IL.FlightNumber,
                    PJS.TripNumber, PJS.JourneyNumber, PJS.SegmentNumber, PJS.FareComponentNumber, PJS.FareJourneyType, PJS.FareBasis, B.BookingUTC, B.RecordLocator, PJS.ClassOfService
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