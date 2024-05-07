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