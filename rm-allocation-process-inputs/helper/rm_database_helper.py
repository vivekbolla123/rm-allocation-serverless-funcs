from datetime import datetime,timedelta
import pandas as pd
import pytz
from ownlogger import OwnLogger
from Constants import *
from utilities.time_utils import TimeUtils 
from helper.redis_helper import RedisHelper

class RMDatabaseHelper:
    def __init__(self,conn,wrconn,runId,inputSource,debugmode,cache_client):
        self.conn=conn
        self.wrconn=wrconn
        self.runId=runId
        self.logger= OwnLogger(runId, inputSource)
        self.inputSource=inputSource
        self.debugMode=debugmode
        self.timeutils=TimeUtils(inputSource)
        self.redishelper=RedisHelper(self.runId,cache_client,inputSource)

    
    def getLatestCodeVersion(self):
        lastestQuery = " SELECT code_version FROM config_code_version where isActive = 1 "
        try:
            inputsData = pd.read_sql(lastestQuery, self.conn)
        except Exception as e:
            self.logger.error(e)
        return inputsData['code_version'][0]
    
    def loadRunParams(self):
		#load the list of sectors
        if self.inputSource.input_source[CONST_MARKET_LIST] == FILE:
            self.mktList = pd.read_csv(CONST_MARKET_LIST_FILE_PATH)
        else:
            marketListQuery = "SELECT Origin,Destin,FlightNumber,PerType,PerStart,PerEnd,DOW,strategyFlag,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusion,fareAnchor,fareOffset,FirstRBDAlloc,OtherRBDAlloc,B2BBackstop,B2CBackstop,B2BFactor,SkippingFactor,analystName,DaySpan,OwnFareFlag,AutoTimeRangeFlag, openingFares,OverBooking FROM market_list "
            try:
                self.mktList = pd.read_sql(marketListQuery, self.conn)
            except Exception as e:
                self.logger.error(e)

        if(self.debugMode):
            self.logger.info("Loaded Market List")
            self.logger.info(self.mktList)
        return self.mktList
            
    def insertRunStart(self, startTime, username):
        if self.inputSource.logs[CONST_RUN_AUDIT] == DB:
            currVersionQuery = "SELECT tableName, curr_version FROM currentVersion"
            try:
                currVersionValues = pd.read_sql(currVersionQuery, self.conn)
            except Exception as e:
                self.logger.error(e)

            table_versions = {}
            for index,currVersion in currVersionValues.iterrows():
                table_versions[currVersion['tableName']] = currVersion['curr_version']

            type = "Local Run"
            
            insert_query = " INSERT INTO allocation_run_audit (run_id, start_time, user, dtd_start, dtd_end, Curves, d1_d2_strategies, market_list, Fares, type, is_sftp_pushed, is_connections_required, update_navitaire_method) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            try:
                self.wrconn.execute(insert_query,
                            (self.runId, startTime.strftime(DATE_TIME_FORMAT), username, 0, 365, table_versions['Curves'], table_versions['d1_d2_strategies'], type, table_versions['Fares'], type, '0', '0', 'file'))
            except Exception as e:
                self.logger.error(e)
    
    def updateRunEnd(self, endTime):
        if self.inputSource.logs[CONST_RUN_AUDIT] == DB:
            updateQuery = " UPDATE allocation_run_audit SET end_time = %s WHERE run_id = %s "
            try:
                self.wrconn.execute(updateQuery, (endTime.strftime(DATE_TIME_FORMAT), self.runId))
            except Exception as e:
                self.logger.error(e)
                
    def areAllInputsReady(self):
        if self.inputSource.input_source[CONST_INPUT_STATUS] == DB:
            countQuery = "select count(*) as busyInputs from inputs_status WHERE is_running = b'1'"
            try:
                inputsData = pd.read_sql(countQuery, self.conn)
            except Exception as e:
                self.logger.error(e)
            return inputsData['busyInputs'][0] == 0
        
    def insertSummaryRow(self,allocResults):
        if self.inputSource.logs[CONST_RUN_SUMMARY] == DB:
            latest_row = allocResults[-1]

            insertQuery = f""" 
                INSERT INTO run_summary 
                (Id, Origin, Destin, FltNum, DepDate, BookedLoad, TgtLoad, MktFare_Min, OpenRBD, SellingFare, Channel, HowDetermined, RunId, CreatedAt, MktFare_Max, fareAnchor, fareOffset, TimeRange, dlfBand, ndoBand, criteria, AirlineCode_Max, AirlineCode_Min, FlightNumber_Max, FlightNumber_Min, backstop, Variance, StrategyReference, analystName, bookedPlf, startTime, endTime, allocationStatus, endOfDaydlfBand, endOfDayTgtLoad, strategyFare, lastSellingFare, upsell, bookingToday, expectedEndOfDayBooking, expectedEndOfDayLoadFactor, historicOwnFare, historicFare, timeSectorDrop, gridDrop, overBookingCount, marketFaresSummary)  
                VALUES (
                    '{latest_row.Id}', 
                    '{latest_row.Origin}', 
                    '{latest_row.Destin}', 
                    '{latest_row.FltNum}', 
                    '{latest_row.DepDate}', 
                    '{latest_row.BookedLoad}', 
                    '{latest_row.TgtLoad}', 
                    '{str(latest_row.MktFare_Min)}', 
                    '{latest_row.OpenRBD}', 
                    '{str(latest_row.SellingFare)}', 
                    '{latest_row.Channel}', 
                    '{latest_row.HowDetermined}', 
                    '{latest_row.RunId}', 
                    '{self.timeutils.getCurrentDateTime().strftime(DATE_TIME_FORMAT)}',
                    '{str(latest_row.MktFare_Max)}',
                    '{str(latest_row.anchorFare)}',
                    '{str(latest_row.fareOffset)}',
                    '{str(latest_row.TimeRange)}',
                    '{str(latest_row.dlfBand)}',
                    '{str(latest_row.ndoBand)}',
                    '{str(latest_row.criteria)}',
                    '{str(latest_row.AirlineCode_Max)}',
                    '{str(latest_row.AirlineCode_Min)}',
                    '{str(latest_row.FlightNumber_Max)}',
                    '{str(latest_row.FlightNumber_Min)}',
                    '{str(latest_row.Backstop)}',
                    '{str(latest_row.Variance)}',
                    '{str(latest_row.StrategyReference)}',
                    '{str(latest_row.analystName)}',
                    '{str(latest_row.bookedPlf)}',
                    '{str(latest_row.startTime)}',
                    '{str(latest_row.endTime)}',
                    '{str(latest_row.allocationStatus)}',
                    '{str(latest_row.endOfDaydlfBand)}',
                    '{str(latest_row.endOfDayTgtLoad)}',
                    '{str(latest_row.strategyFare)}',
                    '{str(latest_row.lastSellingFare)}',
                    '{str(latest_row.upsell)}',
                    '{str(latest_row.bookingToday)}',
                    '{str(latest_row.expectedEndOfDayBooking)}',
                    '{str(latest_row.expectedEndOfDayLoadFactor)}',
                    '{str(latest_row.historicOwnFare)}',
                    '{str(latest_row.state)}',
                    '{str(latest_row.timeSectorDrop)}',
                    '{str(latest_row.gridDrop)}',
                    '{str(latest_row.overBookingCount)}',
                    '{str(latest_row.marketFaresSummary)}'
                )
            """
            try:
                self.wrconn.execute(insertQuery)
            except Exception as e:
                self.logger.error(e)
                
    def getTargetBookedLoadFromBenchmarkCurve(self, ndocounter, curveID):
        tgtBookedLoad = []
        #Fetch the Target Booked Load from the curve
        if ndocounter<0:
            ndocounter=0
        bookingCurveQuery = "Select LF,B2C_Avg_Fare,B2B_Avg_Fare from Curves where NDO = " + str(ndocounter) + " and CurveID = '" + curveID + "'"
        try:
            benchmark = pd.read_sql(bookingCurveQuery, self.conn)
        except Exception as e:
            self.logger.error(e)
        if(benchmark.shape[0] > 0):
            tgtBookedLoad = benchmark

        return tgtBookedLoad

    def checkNowShowProb(self,sector,flighnno,constantsutils):
        query=f'''SELECT CASE WHEN Prob1 >= '{constantsutils.OVERBOOKING_PROB}' AND Prob2 >='{constantsutils.OVERBOOKING_PROB}' THEN 2 ELSE 1 END AS OverBookingCount FROM no_show_probabilities WHERE (Sector = '{sector}' AND `FlightNumber` = '{flighnno}') AND (Prob1 >= '{constantsutils.OVERBOOKING_PROB}' OR Prob2 >= '{constantsutils.OVERBOOKING_PROB}');'''
        result=pd.read_sql(query,self.conn)
        if result.empty:
            count=0
        else:
            count=int(result["OverBookingCount"][0])
        return count
    
    def getNdoBand(self, ndo):
        ndoBandQuery = "SELECT ndo_band FROM ndo_bands where start <= " + str(ndo) + " and end > " + str(ndo) 
        try:
            ndoBandList = pd.read_sql(ndoBandQuery, self.conn)
        except Exception as e:
            self.logger.error(e)
        ndoBand = ndoBandList['ndo_band'][0]
        return ndoBand
    
    def getdpflBand(self, dPLF):
        dplfBandQuery = "SELECT dplf_band FROM dplf_bands where start <= " + str(dPLF) + " and end > " + str(dPLF) 
        try:
            dplfBandList = pd.read_sql(dplfBandQuery, self.conn)
        except Exception as e:
            self.logger.error(e)
        dplfBand = dplfBandList['dplf_band'][0]
        return dplfBand
    
    def getPlfThreshhold(self, ndocounter, flightNumber):
        pflThresholdQuery = "SELECT plf_threshold FROM config_pfl_threshold WHERE flightNumber = '"+ str(flightNumber) +"' AND NDO = '"+ str(ndocounter) +"' UNION " 
        pflThresholdQuery = pflThresholdQuery+ " SELECT plf_threshold FROM config_pfl_threshold WHERE flightNumber = '*' " 
        pflThresholdQuery = pflThresholdQuery+ " AND NOT EXISTS (SELECT 1 FROM config_pfl_threshold WHERE flightNumber = '"+ str(flightNumber) +"') AND NDO = '"+ str(ndocounter) +"' LIMIT 1" 
        try:
            pflThresholdList = pd.read_sql(pflThresholdQuery, self.conn)
        except Exception as e:
            self.logger.error(e)
        threshold = pflThresholdList['plf_threshold']
        return threshold
    
    def getDepartureTimeBands(self):
        result = self.conn.execute("SELECT StartTime, EndTime, TimeBand FROM departure_time_ranges")
        time_bands = [(self.timeutils.parse_time(start), self.timeutils.parse_time(end), time_band) for start, end, time_band in result]
        rr=self.conn.execute("""SELECT
        (SELECT EndTime FROM departure_time_ranges ORDER BY TimeBand ASC LIMIT 1) AS StartTime,
        (SELECT StartTime FROM departure_time_ranges ORDER BY TimeBand ASC LIMIT 1 OFFSET 1) AS EndTime""")
        start_end_range=[(start,end) for start, end in rr]
        return time_bands,start_end_range
    
    def getd1d2StrategyValue(self, strategy, dplfBand, ndoBand,channel):
        dynamicD1D2Query = "SELECT criteria, time_range, offset, strategy FROM d1_d2_strategies where ndo_band = "+ str(ndoBand) +" and dplf_band = "+ str(dplfBand) +" and strategy = '"+ strategy +"'"+f" and channel='{channel}'"
        try:
            dynamicList = pd.read_sql(dynamicD1D2Query, self.conn)
        except Exception as e:
            self.logger.error(e)
        return dynamicList
    
    def getTimeRange(self, time_range):
        timeRangeQuery = "SELECT type, start, end FROM time_ranges where time_range = '"+ time_range +"'"
        try:
            rangeList = pd.read_sql(timeRangeQuery, self.conn)
        except Exception as e:
            self.logger.error(e)
        return rangeList

    def getTimeReferenceValue(self, currParams):
        timeRangeQuery = "SELECT ar_start, ar_end FROM allocation_acceptable_range_d1 where airports = '"+ currParams.origin +"'"
        try:
            rangeList = pd.read_sql(timeRangeQuery, self.conn)
        except Exception as e:
            self.logger.error(e)
        return rangeList
    
    def getMarketFareRange(self, channel, currParams,currFltDateStr, DaySpan, startTime, endtime, Day3Flag):
		
        currFltDate = datetime.strptime(currFltDateStr, DATE_FORMAT).date()
        updateDate=currFltDate+timedelta(days=int(DaySpan))
        if Day3Flag == False:
            startDateTime=f"{currFltDate} {startTime}"
            enddateTime=f"{currFltDate} {endtime}"
            updateDate=currFltDate+timedelta(days=int(DaySpan))
            if currFltDate>updateDate:
                startDateTime=f"{updateDate} {startTime}"
                enddateTime=f"{currFltDate} {endtime}"
            elif currFltDate<updateDate:
                startDateTime=f"{currFltDate} {startTime}"
                enddateTime=f"{updateDate} {endtime}"
            
        origin = currParams.origin
        destination = currParams.destin
        routedSector = self.getRoutedDestination(origin, destination)

        if(len(routedSector) > 0):
            origin = routedSector['routedSector'][0][:3]
            destination = routedSector['routedSector'][0][3:]   

        mktLowestQuery = f"""Select gross_fare, outbound_airline_code, outbound_flight 
            from market_fares where source_airport = '{origin}' and destination_airport = '{destination}' 
            and channel = '{channel}' and outbound_airline_code <> '{currParams.carr_exclusion}' AND outbound_stop = 0
            AND outbound_airline_code != 'QP' AND outbound_airline_code!='G8' 
        """
        if Day3Flag:
            mktLowestQuery = mktLowestQuery + f""" AND CAST(departure_date AS DATE) >= CAST('{currFltDate}' AS DATE)
                AND CAST(departure_date AS DATE) <= CAST('{updateDate}' AS DATE)
                AND CAST(outbound_departuretime AS TIME) >= CAST('{startTime}' AS TIME)
                AND CAST(outbound_departuretime AS TIME) <= CAST('{endtime}' AS TIME) """
        else:
            mktLowestQuery = mktLowestQuery + f""" AND CAST(CONCAT(departure_date, ' ', outbound_departuretime) AS DATETIME) <= CAST('{enddateTime}' AS DATETIME) 
            AND CAST(CONCAT(departure_date, ' ', outbound_departuretime) AS DATETIME) >= CAST('{startDateTime}' AS DATETIME)  """

        if(self.inputSource.input_source[CONST_MARKET_FARES] != TEST_DATA):
            mktLowestQuery = mktLowestQuery + " AND created_at > NOW() - INTERVAL 2 DAY "
        mktLowestQuery = mktLowestQuery + " ORDER BY gross_fare ASC"
        try:
            mktFareRange = pd.read_sql(mktLowestQuery, self.conn)
        except Exception as e:
            self.logger.error(e)

        return mktFareRange
    
    def getCurrentTime(self, currentTimeFlag, currParams, currFltDate,starttime):
		#load the list of sectors
        if currentTimeFlag:
            currFltDate = datetime.strptime(currFltDate, DATE_FORMAT).date()
            query = f''' 
                SELECT runTime FROM config_runTime 
                    where FlightNumber = '{currParams.flight_number}' 
                    and Origin = '{currParams.origin}' 
                    and Destin = '{currParams.destin}' 
                    and FlightDate ='{currFltDate}'
            '''
            currTime = pd.read_sql(query, self.conn)
            self.currentTime = currTime["runTime"][0]
        else:
            # Convert UTC time to IST
            # Define the IST timezone
            ist_timezone = pytz.timezone('Asia/Kolkata')
            # Convert UTC time to IST
            cur_time = starttime.astimezone(ist_timezone).time().strftime("%H:%M")
            self.currentTime = cur_time
        return self.currentTime
            
    def getValueWithOffset(self, origin, destin, anchorFare, offset, channel, openResult):
        self.logger.info("Anchor Fare is " + str(anchorFare) + " with an offset of " + str(offset))

        RBDQuery = "select RBD, Total from Fares where Org = '" + origin + "' and Dst = '" + destin + "' "
        try:
            rbddata = pd.read_sql(RBDQuery, self.conn)
        except Exception as e:
            self.logger.error(e)
        if(rbddata.shape[0] == 0):
            self.logger.info('QP Fare not available. Please validate Fares table')
            openResult = {'RBD':['NA'], 'Fare':['NA'], 'FareResult':['QP Fares not available'],'anchorFare': [anchorFare],'statusInd':[1], 'allocationStatus': [1]}
            self.redishelper.updateRunAudit()
            return openResult, None

        if channel == 'B2C':
            RBDQuery = RBDQuery + " and LEFT(RBD,1) != 'Z'"
        if channel == 'B2B':
            RBDQuery = RBDQuery + " and LEFT(RBD,1) = 'Z'"

        sortorder = ' desc '

        if (int(offset) < 0):
            RBDQuery = RBDQuery + " and Total < " + str(anchorFare)
            sortorder = ' desc '
        if (int(offset) >= 0):
            anchorFare = float(anchorFare)
            if (anchorFare > 0):
                RBDQuery = RBDQuery + " and Total >= " + str(anchorFare)
            else:
                # Below condition forces zero records to be returned
                RBDQuery = RBDQuery + " and Total >= 1000000"
            sortorder = ' asc '

        RBDQuery = RBDQuery + " order by Total " + sortorder
        if(self.debugMode):
            self.logger.info(RBDQuery)

        try:
            rbddata = pd.read_sql(RBDQuery, self.conn)
        except Exception as e:
            self.logger.error(e)
        return openResult, rbddata
    
    def fetchExtremePublishedFare(self, origin, destin, channel, type, backstop):

        RBDQuery = "select RBD, Total from Fares where Org = '" + origin + "' and Dst = '" + destin + "'" 
        if channel == 'B2C':
            if type == 'LOW':
                RBDQuery = RBDQuery + " and LEFT(RBD,1) != 'Z' and Total >= '" + str(backstop) + "' order by Total asc" 
            else:
                RBDQuery = RBDQuery + " and LEFT(RBD,1) != 'Z' and Total >= '" + str(backstop) + "' order by Total desc"
        else:
            if type == 'LOW':
                RBDQuery = RBDQuery + " and LEFT(RBD,1) = 'Z' and Total >= '" + str(backstop) + "' order by Total asc"
            else:
                RBDQuery = RBDQuery + " and LEFT(RBD,1) = 'Z' and Total >= '" + str(backstop) + "' order by Total desc"
        try:
            defFareData = pd.read_sql(RBDQuery, self.conn)
        except Exception as e:
            self.logger.error(e)

        return defFareData
    
    def getHourlyOwnFareData(self, ndo, hour):
        query = f''' SELECT value FROM config_ownfares_hourly where {ndo} between ndoStart and ndoEnd and hour = {int(hour)} '''
        try:
            hourlyData = pd.read_sql(query, self.conn)
        except Exception as e:
            self.logger.error(e)

        return hourlyData
    
    def getGridOwnFareData(self, ndoBand, dlfBand, channel):

        query = f''' SELECT value FROM config_ownfare_grid where ndoBand={ndoBand} and dlfBand={dlfBand} and channel='{channel}' '''
        try:
            gridData = pd.read_sql(query, self.conn)
        except Exception as e:
            self.logger.error(e)

        return gridData
    
    def getFareHistoryData(self, ndo, sector, month, dow, channel, currTime):

        query = f''' SELECT value FROM config_ndo_range where start <= {ndo} and end > {ndo} '''
        try:
            ndoRange = pd.read_sql(query, self.conn)
        except Exception as e:
            self.logger.error(e)

        currTime =  currTime + ':00'

        query = f''' 
                select `{ndoRange['value'][0]}` as price from (
                    SELECT *, CASE
                        WHEN TIME(startTime) = '02:30:00' AND TIME(endTime) = '07:30:00' THEN 0
                        WHEN TIME(startTime) = '07:30:00' AND TIME(endTime) = '11:00:00' THEN 1
                        WHEN TIME(startTime) = '11:00:00' AND TIME(endTime) = '14:30:00' THEN 2
                        WHEN TIME(startTime) = '14:30:00' AND TIME(endTime) = '18:00:00' THEN 3
                        WHEN TIME(startTime) = '18:00:00' AND TIME(endTime) = '21:00:00' THEN 4
                        ELSE 5 end AS Cluster
                    FROM config_ownFares
                    WHERE sector='{sector}' and Month='{month}' and channel='{channel}' and DOW = '{dow}' 
                ) A where A.Cluster = 
                CASE 
                    WHEN TIME('{currTime}') >= '02:30:00' AND TIME('{currTime}') < '07:30:00' THEN 0
                    WHEN TIME('{currTime}') >= '07:30:00' AND TIME('{currTime}') < '11:00:00' THEN 1
                    WHEN TIME('{currTime}') >= '11:00:00' AND TIME('{currTime}') < '14:30:00' THEN 2
                    WHEN TIME('{currTime}') >= '14:30:00' AND TIME('{currTime}') < '18:00:00' THEN 3
                    WHEN TIME('{currTime}') >= '18:00:00' AND TIME('{currTime}') < '21:00:00' THEN 4
                    ELSE 5 end 
                '''
        try:
            historicData = pd.read_sql(query, self.conn)
        except Exception as e:
            self.logger.error(e)

        return historicData
    
    def getArea(self, currentHour):
        query = f''' SELECT sum(shareOfBooking) as sum FROM config_share_of_booking where hours <= {currentHour} '''
        return pd.read_sql(query, self.conn)
    
    def getOpeningFareValue(self, dplfBand, endOfDayDplfBand, currentTime, ndocounter, loadFactor, departureTime):
	
        query = f'''
            SELECT B2CValue, B2BValue 
            FROM config_opening_fares 
            WHERE 1 = 1 
            AND IFNULL(TIME(JSON_UNQUOTE(JSON_EXTRACT(conditions, '$.start_time'))), '00:00') <= '{currentTime}' 
            AND IFNULL(TIME(JSON_UNQUOTE(JSON_EXTRACT(conditions, '$.end_time'))), '23:59') >= '{currentTime}' 
            AND FIND_IN_SET('{ndocounter}',JSON_UNQUOTE(JSON_EXTRACT(conditions, '$.ndo_band'))) 
            AND FIND_IN_SET('{dplfBand}',IFNULL(JSON_UNQUOTE(JSON_EXTRACT(conditions, '$.start_of_day_dlf_band')), '{dplfBand}')) 
            AND FIND_IN_SET('{endOfDayDplfBand}',IFNULL(JSON_UNQUOTE(JSON_EXTRACT(conditions, '$.end_of_day_dlf_band')), '{endOfDayDplfBand}')) 
            AND IFNULL(JSON_UNQUOTE(JSON_EXTRACT(conditions, '$.min_load_factor')), 0) <= {loadFactor} 
            AND IFNULL(JSON_UNQUOTE(JSON_EXTRACT(conditions, '$.max_load_factor')), 100) >= {loadFactor} 
            AND IFNULL(JSON_UNQUOTE(JSON_EXTRACT(conditions, '$.departure_start_time')), '00:00') <= '{departureTime}' 
            AND IFNULL(JSON_UNQUOTE(JSON_EXTRACT(conditions, '$.departure_end_time')), '23:59') >= '{departureTime}' 
        '''
        # self.logger.info(query)
        return pd.read_sql(query, self.conn)
    
    def fareExtract(self, origin, destination, channel, openingRBD):

        RBDQuery = "select ROW_NUMBER() OVER (ORDER BY TOTAL) AS id, RBD, Total from Fares where Org = '" + origin + "' and Dst = '" + destination + "' "
        
        if channel == 'B2C':
            RBDQuery = RBDQuery + " and LEFT(RBD,1) != 'Z' "
        if channel == 'B2B':
            RBDQuery = RBDQuery + " and LEFT(RBD,1) = 'Z' "
        if openingRBD != '':
            RBDQuery = RBDQuery + " and RBD = '" + openingRBD + "'"

        RBDQuery = RBDQuery + " order by Total asc"
        try:
            queryResult = pd.read_sql(RBDQuery, self.conn)
        except Exception as e:
            self.logger.error(e)

        return queryResult
    
    def insert_run_fltDate_audit(self, run_id, flightNumber, flightDate, result, b2cstatus, b2bstatus):
        insert_query = " INSERT INTO run_flight_date_audit (runId, flightNumber, flightDate, result, b2cstatus, b2bstatus ,createdAt) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        try:
            self.wrconn.execute(insert_query, (run_id, flightNumber, flightDate, result, b2cstatus, b2bstatus, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        except Exception as e:
            self.logger.info(e)
            
    def getCurrentBookedLoads(self, origin, destin, startDate, endDate):
        
        NavitaireQuery = f"""SELECT Origin, Destination, FlightNumber, DepartureDate, bookedPax, capacity ,DepartureTime, adjustedCapacity from bookload
            where DepartureDate >= '{str(startDate)}' and DepartureDate <= '{str(endDate)}'
            and Origin = '{origin}' and Destination = '{destin}' """
        bookedData = pd.read_sql(NavitaireQuery, self.conn)

        if(self.debugMode):
            self.logger.info(NavitaireQuery)

        return bookedData
    
    def checkForNextFLight(self, currFltDateStr, currParams):
	
        currFltDate = datetime.strptime(currFltDateStr, DATE_FORMAT).date()	
       
        query=f'''
            select STD,Origin,Destin as Destination,FlightNumber from booking_fare_data where FlightDate = '{currFltDate}' AND Origin = '{currParams.origin}' AND Destin = '{currParams.destin}' order by STD ASC
        '''
        conn=self.conn
        df = pd.read_sql(query, conn)

        df['FlightNumber'] = df['FlightNumber'].astype(str)
        row_index = df[((df['FlightNumber'])== str(currParams.flight_number)) &(df['Origin'] == currParams.origin) & (df['Destination'] == currParams.destin)].index

        return len(row_index) > 0 and row_index[0] < len(df) - 1
    
    def getLastSellingFare(self, currFltDateStr, currParams):
	
        currFltDate = datetime.strptime(currFltDateStr, DATE_FORMAT_2).date()
        	
        query=f'''
            select B2Bfare,B2Cfare from booking_fare_data where FlightDate = '{currFltDate}' AND FlightNumber = '{currParams.flight_number}' AND Origin = '{currParams.origin}' AND Destin = '{currParams.destin}'
        '''
        conn=self.conn
        farePrice = pd.read_sql(query, conn)

        return farePrice
    
    def getNumberOfBookings(self, currFltDateStr, currParams):
	
        currFltDate = datetime.strptime(currFltDateStr, DATE_FORMAT_2).date()	
        
        query=f'''
            select BookingsCount,STD from booking_fare_data where FlightDate = '{currFltDate}' AND FlightNumber = '{currParams.flight_number}' AND Origin = '{currParams.origin}' AND Destin = '{currParams.destin}'
        '''
        conn=self.conn
        farePrice = pd.read_sql(query, conn)

        return farePrice
    
    def get_parameters(self):
        parameterValueQuery = "SELECT parameterKey,parameterValue FROM rm_parameter_values"
        try:
            parameterValue = pd.read_sql(parameterValueQuery, self.conn)
        except Exception as e:
            self.logger.error(e)
            
        return parameterValue
    
    def getRoutedDestination(self, origin, destination):
        query = f''' SELECT routedSector FROM config_marketFares_rerouting where sector = concat('{origin}','{destination}') '''
        try:
            routedData = pd.read_sql(query, self.conn)
        except Exception as e:
            self.logger.error(e)

        return routedData
            
    
