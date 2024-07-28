from datetime import datetime
import pandas as pd
from Constants import *
from ownlogger import OwnLogger
from utilities.time_utils import TimeUtils

class NavDatabaseHelper:
    def __init__(self,navconn,conn,inputSource,nav_db_name,runId,debugMode,nav_db_wb_name,CurrParams):   
       self.NavitaireConnection = navconn
       self.conn = conn
       self.logger = OwnLogger(runId, inputSource)
       self.inputSource = inputSource
       self.NAV_DB_NAME = nav_db_name
       self.NAV_DB_WB_NAME = nav_db_wb_name
       self.debugMode = debugMode
       self.timeutils=TimeUtils(inputSource)
       self.currParams=CurrParams
        
    def getCurrentBookedLoads(self, origin, destin, startDate, endDate):

        NavitaireQuery = f""" EXEC {self.NAV_DB_WB_NAME}[dbo].[SP_Bookeddata]
								@DepartureDate_from = N'{str(startDate)}',
								@DepartureDate_to = N'{str(endDate)}',
								@DepartureStation = N'{origin}',
								@ArrivalStation = N'{destin}' 
        					"""

        bookedData = pd.read_sql(NavitaireQuery, self.NavitaireConnection)

        if(self.debugMode):
            self.logger.info(NavitaireQuery)

        return bookedData
    
    def checkForNextFLight(self, currFltDateStr, currParams):
	
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

        return len(row_index) > 0 and row_index[0] < len(df) - 1
    
    def getLastSellingFare(self, currFltDateStr, currParams):
	
        currFltDate = datetime.strptime(currFltDateStr, DATE_FORMAT_2).date()	
        query = f"""
                EXEC {self.NAV_DB_WB_NAME}[dbo].[SP_LastSoldFare]
                    @DepartureDate = N'{currFltDate}',
                    @FlightNumber = N'{currParams.flight_number}',	
                    @DepartureStation = N'{currParams.origin}',	
                    @ArrivalStation = N'{currParams.destin}'
        """
        farePrice = pd.read_sql(query, self.NavitaireConnection)

        return farePrice

    def getNumberOfBookings(self, currFltDateStr, currParams):
	
        currFltDate = datetime.strptime(currFltDateStr, DATE_FORMAT_2).date()	
        
        currDate = self.timeutils.getCurrentDate()
        query = f"""
                EXEC {self.NAV_DB_WB_NAME}[dbo].[SP_LastBookingsCount]
                    @DepartureDate = N'{currFltDate}',
                    @FlightNumber = N'{currParams.flight_number}',
                    @DepartureStation = N'{currParams.origin}',
                    @ArrivalStation = N'{currParams.destin}',
                    @ComparisonDate = N'{currDate}'
        """
        farePrice = pd.read_sql(query, self.NavitaireConnection)

        return farePrice
    
    def getcurrentRBD(self, departuredate, flightnumber, channel, sector,bookload):
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
        return rbdsallocated
    
    def get_seats_sold_by_class(self, flight_number, origin, destination, departure_date):
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
            AND (ILCS.ClassOfService = 'P' or ILCS.ClassOfService = 'L' or ILCS.ClassOfService = 'G8' or ILCS.ClassOfService = 'G9')
        ORDER BY ILV.ModifiedUTC DESC
        '''
        df = pd.read_sql(query, self.NavitaireConnection)
        l_class_seats_sold = df[df['class_code'] == 'L']['seats_sold'].values[0]
        p_class_seats_sold = df[df['class_code'] == 'P']['seats_sold'].values[0]
        G8_class_seats_sold = df[df['class_code'] == 'G8']['seats_sold'].values[0]
        G9_class_seats_sold = df[df['class_code'] == 'G9']['seats_sold'].values[0]
        return l_class_seats_sold, p_class_seats_sold, G8_class_seats_sold, G9_class_seats_sold

    def get_current_rbds_by_class(self, departure_date, flight_number, sector):
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
                AND (ILC.ClassOfService = 'P' or ILC.ClassOfService = 'L' or ILC.ClassOfService = 'G8' or ILC.ClassOfService = 'G9')
            ORDER BY ILC.ModifiedUTC DESC
            """
            df = pd.read_sql(query, self.NavitaireConnection)
            l_class_rbd = df[df['class_code'] == 'L']['current_availability'].values[0]
            p_class_rbd = df[df['class_code'] == 'P']['current_availability'].values[0]
            G8_class_rbd = df[df['class_code'] == 'G8']['current_availability'].values[0]
            G9_class_rbd = df[df['class_code'] == 'G9']['current_availability'].values[0]
            return l_class_rbd, p_class_rbd, G8_class_rbd, G9_class_rbd
        
    def get_series_seats(self,departure_date, flight_number, sector):
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
        return df
    
    def convert_currency(self,from_currency,to_currency,value):
        query = f"SELECT '{from_currency}' AS FromCurrency, '{to_currency}' AS ToCurrency, {value} AS OriginalValue, {value} * ConversionRate AS ConvertedValue FROM {self.NAV_DB_WB_NAME}[Reference].[CurrencyConversion] WHERE FromCurrencyCode = '{from_currency}' AND ToCurrencyCode = '{to_currency}'"
        df = pd.read_sql(query, self.conn)
        return df['ConvertedValue'][0]
