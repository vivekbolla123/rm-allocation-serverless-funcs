from datetime import datetime,timedelta
import pandas as pd
from Constants import *
from ownlogger import OwnLogger
import logging
from utilities.time_utils import TimeUtils
from time_logger import TimeLogger

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class NavDatabaseHelper:
    def __init__(self,navconn,conn,inputSource,nav_db_name,nav_db_ref_name,runId,debugMode,nav_db_wb_name,CurrParams, log_time):   
       self.NavitaireConnection = navconn
       self.conn = conn
       self.runId = runId
       self.logger = OwnLogger(runId, inputSource)
       self.inputSource = inputSource
       self.NAV_DB_NAME = nav_db_name
       self.NAV_DB_WB_NAME = nav_db_wb_name
       self.NAV_DB_REF_NAME = nav_db_ref_name
       self.debugMode = debugMode
       self.timeutils=TimeUtils(inputSource)
       self.currParams=CurrParams
       self.log_time = log_time
        
    def getCurrentBookedLoads(self, origin, destin, startDate, endDate):
        self.log_time.start()
        NavitaireQuery = f""" EXEC {self.NAV_DB_WB_NAME}[dbo].[SP_Bookeddata]
								@DepartureDate_from = N'{str(startDate)}',
								@DepartureDate_to = N'{str(endDate)}',
								@DepartureStation = N'{origin}',
								@ArrivalStation = N'{destin}' 
        					"""

        bookedData = pd.read_sql(NavitaireQuery, self.NavitaireConnection)
        self.log_time.end("getCurrentBookedLoads")
        if(self.debugMode):
            self.logger.info(NavitaireQuery)

        return bookedData
    
    def checkForNextFLight(self, currFltDateStr, currParams):
        self.log_time.start()
        currFltDate = datetime.strptime(currFltDateStr, DATE_FORMAT).date()	

        query=query=f'''
					EXEC {self.NAV_DB_WB_NAME}[dbo].[SP_DailyFlights]
						@DepartureDate = N'{currFltDate}',
						@DepartureStation = N'{currParams.origin}',
						@ArrivalStation = N'{currParams.destin}'
   					'''
        
        df=pd.read_sql(query, self.NavitaireConnection)
        
        df['FlightNumber'] = df['FlightNumber'].astype(str)
        row_index = df[((df['FlightNumber'])== str(currParams.flight_number)) &(df['Origin'] == currParams.origin) & (df['Destination'] == currParams.destin)].index
        self.log_time.end("checkForNextFLight")
        return len(row_index) > 0 and row_index[0] < len(df) - 1
    
    def getLastSellingFare(self, currFltDateStr, currParams):
        self.log_time.start()
        currFltDate = datetime.strptime(currFltDateStr, DATE_FORMAT_2).date()	
        query = f"""
                EXEC {self.NAV_DB_WB_NAME}[dbo].[SP_LastSoldFare]
                    @DepartureDate = N'{currFltDate}',
                    @FlightNumber = N'{currParams.flight_number}',	
                    @DepartureStation = N'{currParams.origin}',	
                    @ArrivalStation = N'{currParams.destin}'
        """
        farePrice = pd.read_sql(query, self.NavitaireConnection)
        self.log_time.end("getLastSellingFare")
        return farePrice

    def getNumberOfBookings(self, currFltDateStr, currParams):
        self.log_time.start()
        currFltDate = datetime.strptime(currFltDateStr, DATE_FORMAT_2).date()	
        
        currDate = self.timeutils.getCurrentDate()
        currDateTime = self.timeutils.getCurrentDateTime() - timedelta(hours=1)
        currDateTimeStr = currDateTime.strftime("%Y-%m-%d %H:%M:%S")
        query = f"""
                EXEC {self.NAV_DB_WB_NAME}[dbo].[SP_LastBookingsCount]
                    @DepartureDate = N'{currFltDate}',
                    @FlightNumber = N'{currParams.flight_number}',
                    @DepartureStation = N'{currParams.origin}',
                    @ArrivalStation = N'{currParams.destin}',
                    @ComparisonDate = N'{currDate}',
                    @ComparisonDateTime = N'{currDateTimeStr}'
        """
        farePrice = pd.read_sql(query, self.NavitaireConnection)
        self.log_time.end("getNumberOfBookings")
        return farePrice
    
    def getcurrentRBD(self, departuredate, flightnumber, channel, sector,bookload):
        self.log_time.start()
        if channel == 'B2C':
            RBDQuery = "Substring(ClassOfService,1,1) != 'Z'"
        if channel == 'B2B':
            RBDQuery = "Substring(ClassOfService,1,1) = 'Z'"
        query = f'''
            SELECT top 1 ILC.ClassOfService, IL.STD
FROM {self.NAV_DB_NAME}[Rez].[InventoryLegClass] ILC
JOIN {self.NAV_DB_NAME}[Rez].[InventoryLeg] IL ON IL.[InventoryLegID] = ILC.[InventoryLegID]
where IL.DepartureDate = '{departuredate}'
and trim(IL.FlightNumber) ='{flightnumber}'
and IL.DepartureStation + IL.ArrivalStation ='{sector}'
and ClassType = ''
and ClassAu != '0'
and (ClassAU-{str(bookload)})>0
and {RBDQuery}
order by ILC.ModifiedUTC desc, ILC.ClassRank
        '''
        
        
        rbdsallocated = pd.read_sql(query,self.NavitaireConnection)
        self.log_time.end("getcurrentRBD")
        return rbdsallocated
    
    def get_seats_sold_by_class(self, flight_number, origin, destination, departure_date):
        self.log_time.start()
        query = f'''
        SELECT TOP 4
            ILCS.[ClassOfService] as class_code,
            ILCS.[ClassSold] as seats_sold
        FROM {self.NAV_DB_NAME}[Rez].[InventoryLegVersion] ILV
        JOIN {self.NAV_DB_NAME}[Rez].[InventoryLegClassSold] ILCS ON ILV.InventoryLegID = ILCS.InventoryLegID
        WHERE
            ILV.FlightNumber = '{str(flight_number)}'
            AND ILV.DepartureDate = '{str(departure_date)}'
            AND ILV.DepartureStation = '{origin}'
            AND ILV.ArrivalStation = '{destination}'
            AND (ILCS.ClassOfService = 'P' or ILCS.ClassOfService = 'L')
        ORDER BY ILV.ModifiedUTC DESC
        '''
        df = pd.read_sql(query, self.NavitaireConnection)
        self.log_time.end("get_seats_sold_by_class")
        
        l_class_seats_sold = df[df['class_code'] == 'L']['seats_sold'].values[0]
        p_class_seats_sold = df[df['class_code'] == 'P']['seats_sold'].values[0]
        return l_class_seats_sold, p_class_seats_sold

    def get_current_rbds_by_class(self, departure_date, flight_number, sector):
            self.log_time.start()
            query = f"""
            SELECT TOP 4
                ILC.ClassOfService as class_code,
                ILC.ClassAu as current_availability
            FROM {self.NAV_DB_NAME}[Rez].[InventoryLegClass] ILC
            JOIN {self.NAV_DB_NAME}[Rez].[InventoryLeg] IL ON IL.[InventoryLegID] = ILC.[InventoryLegID]
            WHERE
                IL.DepartureDate = '{departure_date}'
                AND TRIM(IL.FlightNumber) = '{str(flight_number)}'
                AND IL.DepartureStation + IL.ArrivalStation = '{str(sector)}'
                AND (ILC.ClassOfService = 'P' or ILC.ClassOfService = 'L')
            ORDER BY ILC.ModifiedUTC DESC
            """
            df = pd.read_sql(query, self.NavitaireConnection)
            self.log_time.end("get_current_rbds_by_class")
            l_class_rbd = df[df['class_code'] == 'L']['current_availability'].values[0]
            p_class_rbd = df[df['class_code'] == 'P']['current_availability'].values[0]
            return l_class_rbd, p_class_rbd
        
    def get_series_seats(self,departure_date, flight_number, sector):
        self.log_time.start()
        query=f"""
                select
                Origin,
                Destination,
                TRIM(FlightNumber) as FlightNumber,
                DepartureDate,
                count(PassengerID) as bookedPax,
                SourceOrganizationCode as OrgCode

            from
                (
                    select
                        BP.PassengerID,
                        IL.DepartureStation as Origin,
                        IL.ArrivalStation as Destination,
                        IL.DepartureDate,
                        IL.FlightNumber,
                        PJS.SourceOrganizationCode,
                        PJS.ClassOfService
                    from
                        {self.NAV_DB_NAME}[Rez].InventoryLeg IL
                        left outer join {self.NAV_DB_NAME}[Rez].PassengerJourneyLeg PJL on PJL.InventoryLegID = IL.InventoryLegID
                        left outer join {self.NAV_DB_NAME}[Rez].PassengerJourneySegment PJS on PJS.PassengerID = PJL.PassengerID and PJS.SegmentID = PJL.SegmentID and (RIGHT(PJS.FareBasis, 3) = 'SER' ) 
                        left outer join {self.NAV_DB_NAME}[Rez].BookingPassenger BP on BP.PassengerID = PJS.PassengerID 
                    where
                        IL.Lid > 0
                        and IL.Status NOT IN (2)
                        and IL.DepartureDate >='{departure_date}'
                        and IL.DepartureDate <= '{departure_date}'
                        and CONCAT( IL.DepartureStation,IL.ArrivalStation) = '{sector}'
                        and IL.FlightNumber = '{flight_number}'
                        and PJS.SourceOrganizationCode is NOT NULL
                        and PJS.ClassOfService='G'
                ) A
            group by
                Origin, Destination, FlightNumber, DepartureDate,SourceOrganizationCode 
            order by
                Origin, Destination, DepartureDate,FlightNumber
        """
        df = pd.read_sql(query, self.NavitaireConnection)
        self.log_time.end("get_series_seats")
        return df
    
    def fetch_all_conversion_data(self):
        self.log_time.start()
        query = f"SELECT FromCurrencyCode, ToCurrencyCode, ConversionRate FROM {self.NAV_DB_REF_NAME}[Reference].CurrencyConversion"
        df = pd.read_sql(query, self.NavitaireConnection)
        self.log_time.end("fetch_all_conversion_data")
        return df
