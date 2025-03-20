from datetime import datetime,timedelta
import json
import pandas as pd
import pytz
from ownlogger import OwnLogger
from Constants import *
from utilities.time_utils import TimeUtils 
from helper.redis_helper import RedisHelper

class RMDatabaseHelper:
    def __init__(self,conn,wrconn,runId,inputSource,debugmode,cache_client,currParams):
        self.conn=conn
        self.wrconn=wrconn
        self.runId=runId
        self.logger= OwnLogger(runId, inputSource)
        self.currParams=currParams
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
        # load the list of sectors

        if self.inputSource.input_source[CONST_MARKET_LIST] == FILE:
            if self.currParams.route_type=="direct":
                self.mktList = pd.read_csv(CONST_MARKET_LIST_FILE_PATH)
            else:
                self.mktList = pd.read_csv(CONST_MARKET_LIST_CONNECTIONS_FILE_PATH)
        else:
            if self.currParams.route_type=="direct":
                marketListQuery = "SELECT Origin,Destin,FlightNumber,PerType,PerStart,PerEnd,DOW,plfthreshold,hardAnchor,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusionB2C,CarrExlusionB2B,flightExclusionB2C,flightExclusionB2B,fareAnchor,fareOffset,FirstRBDAlloc,OtherRBDAlloc,B2BBackstop,B2CBackstop,B2BFactor,obSeats,obFare,SkippingFactor,analystName,DaySpan,AutoTimeRangeFlag, openingFares,OverBooking,profileFares,rbdPushFlag,B2BTolerance,B2CTolerance,distressInventoryFlag,seriesBlock,autoGroup,tbfFlag,autoBackstopFlag FROM market_list "
            else:
                marketListQuery = "SELECT Sector1,Flight1,Sector2,Flight2,Outbound_stop,PerStart,PerEnd,DOW,Price_Strategy,Discount_Value,FirstRBDAlloc,OtherRBDAlloc,B2BBackstop,B2CBackstop,B2BFactor,SkippingFactor,Outbound_duration,Currency,fareAnchor,Offset,DiscountFlag, analystName FROM market_list_connections "
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
            countQuery = f"""select count(*) as busyInputs from inputs_status 
            WHERE is_running = b'1' 
            AND name not like 'market_list%%' """
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
                (Id, Origin, Destin, FltNum, DepDate, BookedLoad, TgtLoad, MktFare_Min, OpenRBD, SellingFare, Channel, HowDetermined, RunId, CreatedAt, MktFare_Max, fareAnchor, fareOffset, TimeRange, dlfBand, ndoBand, criteria, AirlineCode_Max, AirlineCode_Min, FlightNumber_Max, FlightNumber_Min, backstop, autoBackstop, Variance, StrategyReference, analystName, bookedPlf,actualbookedPlf, startTime, endTime, allocationStatus, endOfDaydlfBand, endOfDayTgtLoad, strategyFare, lastSellingFare,lastSevenAvgFare,hardAnchorFare, upsell, bookingToday,bookingInHour, currentTgtLoad, currentLoadFactor, overBookingCount, recommendedActions, profileFare, status,w0Flag)  
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
                    '{str(latest_row.autoBackstop)}',
                    '{str(latest_row.Variance)}',
                    '{str(latest_row.StrategyReference)}',
                    '{str(latest_row.analystName)}',
                    '{str(latest_row.bookedPlf)}',
                    '{str(latest_row.actualbookedPlf)}',
                    '{str(latest_row.startTime)}',
                    '{str(latest_row.endTime)}',
                    '{str(latest_row.allocationStatus)}',
                    '{str(latest_row.endOfDaydlfBand)}',
                    '{str(latest_row.endOfDayTgtLoad)}',
                    '{str(latest_row.strategyFare)}',
                    '{str(latest_row.lastSellingFare)}',
                    '{str(latest_row.lastSevenAvgFare)}',
                    '{str(latest_row.hardAnchorFare)}',
                    '{str(latest_row.upsell)}',
                    '{str(latest_row.bookingToday)}',
                    '{str(latest_row.bookingLastHour)}',
                    '{str(latest_row.currentTgtLoad)}',
                    '{str(latest_row.currentLoadFactor)}',
                    '{str(latest_row.overBookingCount)}',
                    '{str(latest_row.recommendedActions)}',
                    '{str(latest_row.profileFare)}',
                    '{str(latest_row.status)}',
                    '{str(latest_row.w0Flag)}'
                )
            """
            try:
                self.wrconn.execute(insertQuery)
            except Exception as e:
                self.logger.error(e)

    def getTargetBookedLoadFromBenchmarkCurve(self, ndocounter, curveID):
        tgtBookedLoad = []
        # Fetch the Target Booked Load from the curve
        if ndocounter<0:
            ndocounter=0
        bookingCurveQuery = "Select LF from Curves where NDO = " + str(ndocounter) + " and CurveID = '" + curveID + "'"
        try:
            benchmark = pd.read_sql(bookingCurveQuery, self.conn)
        except Exception as e:
            self.logger.error(e)
        if(benchmark.shape[0] > 0):
            tgtBookedLoad = benchmark

        return tgtBookedLoad

    def checkNowShowProb(self, sector, probability, month, deptTime):
        query = f"""
        SELECT
        OBCount AS OverBookingCount
        FROM no_show_probabilities
        JOIN config_clusters c ON c.cluster = no_show_probabilities.Cluster
        WHERE Sector = '{sector}' AND Month = '{month}'
        AND Prob >= '{probability}' 
        AND TIME(c.startTime) <= '{deptTime}' AND TIME(c.endTime) > '{deptTime}' AND N>20
        order by OBCount desc
        """
        result = pd.read_sql(query, self.conn)
        if result.empty:
            count = 0
        else:
            count = int(result["OverBookingCount"][0])
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
        pflThresholdQuery = "SELECT W9_threshold,W8_threshold,W7_threshold,W6_threshold,W5_threshold,W4_threshold,W3_threshold,W2_threshold,W1_threshold,P_threshold,L_threshold,B2B_threshold FROM config_pfl_threshold WHERE flightNumber = '"+ str(flightNumber) +"' AND NDO = '"+ str(ndocounter) +"' UNION " 
        pflThresholdQuery = pflThresholdQuery+ " SELECT W9_threshold,W8_threshold,W7_threshold,W6_threshold,W5_threshold,W4_threshold,W3_threshold,W2_threshold,W1_threshold,P_threshold,L_threshold,B2B_threshold FROM config_pfl_threshold WHERE flightNumber = '*' " 
        pflThresholdQuery = pflThresholdQuery+ " AND NOT EXISTS (SELECT 1 FROM config_pfl_threshold WHERE flightNumber = '"+ str(flightNumber) +"' and NDO = '"+str(ndocounter)+"') AND NDO = '"+ str(ndocounter) +"' LIMIT 1" 
        try:
            pflThresholdList = pd.read_sql(pflThresholdQuery, self.conn)
        except Exception as e:
            self.logger.error(e)
        w9_threshold = pflThresholdList['W9_threshold']
        w8_threshold = pflThresholdList['W8_threshold']
        w7_threshold = pflThresholdList['W7_threshold']
        w6_threshold = pflThresholdList['W6_threshold']
        w5_threshold = pflThresholdList['W5_threshold']
        w4_threshold = pflThresholdList['W4_threshold']
        w3_threshold = pflThresholdList['W3_threshold']
        w2_threshold = pflThresholdList['W2_threshold']
        w1_threshold = pflThresholdList['W1_threshold']
        p_threshold = pflThresholdList['P_threshold']
        l_threshold = pflThresholdList['L_threshold']
        B2B_threshold = pflThresholdList['B2B_threshold']
        
        return w9_threshold,w8_threshold,w7_threshold,w6_threshold,w5_threshold,w4_threshold,w3_threshold,w2_threshold,w1_threshold,p_threshold,l_threshold,B2B_threshold

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

    def getMarketFareRange(self, channel, currParams,currFltDateStr, DaySpan, startTime, endtime, Day3Flag,hrs):
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
        if channel==B2B_CHANNEL:
            carr_exclusion = currParams.carr_exclusion_b2b
            flightExclusion = currParams.flightExclusionB2B
        else:
            carr_exclusion = currParams.carr_exclusion_b2c
            flightExclusion = currParams.flightExclusionB2C
    
        carr_exclusion_str = str(tuple(carr_exclusion.split('/')))  
        if len(tuple(carr_exclusion.split('/'))) == 1:
            carr_exclusion_str=carr_exclusion_str.replace(",", "")
        origin = currParams.origin
        destination = currParams.destin
        routedSector = self.getRoutedDestination(origin, destination)

        if(len(routedSector) > 0):
            origin = routedSector['routedSector'][0][:3]
            destination = routedSector['routedSector'][0][3:]   

        mktLowestQuery = f"""Select gross_fare, outbound_airline_code, outbound_flight 
            from market_fares where source_airport = '{origin}' and destination_airport = '{destination}' 
            and channel = '{channel}' and outbound_airline_code NOT IN {carr_exclusion_str} AND outbound_stop = 0
            AND outbound_airline_code != 'QP' AND outbound_airline_code!='G8' AND outbound_flight!='{flightExclusion}'
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
            mktLowestQuery = mktLowestQuery + f" AND created_at > NOW() - INTERVAL {hrs} HOUR "
        mktLowestQuery = mktLowestQuery + " ORDER BY gross_fare ASC"
        try:
            mktFareRange = pd.read_sql(mktLowestQuery, self.conn)
        except Exception as e:
            self.logger.error(e)

        return mktFareRange
    
    def getMarketFareRangeConnections(self, channel, currParams,currFltDateStr, outBoundStop,outboundDuration):
        
        currFltDate = currFltDateStr
        origin = currParams.sector1[:3]
        destination = currParams.sector2[3:]
        
        mktLowestQuery = f"""Select gross_fare, outbound_airline_code, outbound_flight ,currency
            from market_fares where source_airport = '{origin}' and destination_airport = '{destination}' 
            and channel = '{channel}' AND outbound_stop <= '{outBoundStop}' and outbound_duration <= '{outboundDuration}'
            AND outbound_airline_code != 'QP' AND outbound_airline_code!='G8' 
        """
        
        mktLowestQuery = mktLowestQuery + f""" AND departure_date  = '{currFltDate}' """

        if(self.inputSource.input_source[CONST_MARKET_FARES] != TEST_DATA):
            mktLowestQuery = mktLowestQuery + " AND created_at > NOW() - INTERVAL 2 DAY "
        mktLowestQuery = mktLowestQuery + " ORDER BY gross_fare ASC"
        try:
            mktFareRange = pd.read_sql(mktLowestQuery, self.conn)
        except Exception as e:
            self.logger.error(e)

        return mktFareRange

    def getCurrentTime(self, currentTimeFlag, currParams, currFltDate,starttime):
        # load the list of sectors
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
    def get_qp_currency(self,origin, destin):
        query= "select currency from Fares where Org = '" + origin + "' and Dst = '" + destin + "'"
        try:
            rbddata = pd.read_sql(query, self.conn)
        except Exception as e:
            self.logger.error(e) 
        return rbddata["currency"][0]
    
    def get_qp_fares(self,origin, destin):
        query= "select * from Fares where Org = '" + origin + "' and Dst = '" + destin + "'"
        try:
            rbddata = pd.read_sql(query, self.conn)
        except Exception as e:
            self.logger.error(e)
        return rbddata

    def get_TBF_fares(self, origin, destin):
        is_valid = False     
        query = f"""
            SELECT TOTAL 
            FROM Fares 
            WHERE Org = '{origin}' 
            AND Dst = '{destin}'
            AND RBD IN ({TBF_RBD_QUERY})
        """
        
        try:
            rbddata = pd.read_sql(query, self.conn)
            is_valid = (
                len(rbddata) == TBF_COUNT 
                and not (rbddata['TOTAL'] <= 0).any() 
                and not rbddata['TOTAL'].isnull().any()
            )
            
        except Exception as e:
            self.logger.error(e)
        
        return is_valid
    
    def getValueWithOffset(self, origin, destin, anchorFare, offset, channel, openResult,route):
        self.logger.info("Anchor Fare is " + str(anchorFare) + " with an offset of " + str(offset))

        RBDQuery = "select RBD, Total from Fares where Org = '" + origin + "' and Dst = '" + destin + "' and route = '"+route+"'"
        try:
            rbddata = pd.read_sql(RBDQuery, self.conn)
        except Exception as e:
            self.logger.error(e)
        if(rbddata.shape[0] == 0):
            self.logger.info('QP Fare not available. Please validate Fares table')
            openResult = {'RBD':[CONST_NA], 'Fare':[CONST_NA], 'FareResult':['QP Fares not available'],'anchorFare': [anchorFare],'statusInd':[1], 'allocationStatus': [1]}
            self.redishelper.updateRunAudit()
            return openResult, rbddata
        exclude_rbd_list = TBF_RBD.split(',')
        exclude_rbd_list.extend(list(json.loads(GROUP_QUOTATION).values()))

    # Convert the list of RBDs to a comma-separated string for the SQL query
        exclude_rbd_str = ",".join(["'{}'".format(rbd) for rbd in exclude_rbd_list])
        RBDQuery += f" AND RBD NOT IN ({exclude_rbd_str})"
        
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

    def fetchExtremePublishedFare(self, origin, destin, channel, type, backstop,route):
        exclude_rbd_list = TBF_RBD.split(',')
        exclude_rbd_list.extend(list(json.loads(GROUP_QUOTATION).values()))
    # Convert the list of RBDs to a comma-separated string for the SQL query
        exclude_rbd_str = ",".join(["'{}'".format(rbd) for rbd in exclude_rbd_list])
        RBDQuery = "select RBD, Total from Fares where Org = '" + origin + "' and Dst = '" + destin + "' and route = '"+route+"'"
        RBDQuery += f" AND RBD NOT IN ({exclude_rbd_str})"
        
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

    def getProfileFares(self, sector, month, dow, currTime, percentile):
        query = f''' 
                select {percentile} as price from (
                    SELECT *
                    FROM config_profile_fares
                    WHERE sector='{sector}' and Month='{month}' and DOW = '{dow}' 
                ) A where Cluster = 
                CASE 
                    WHEN TIME('{currTime}') >= '02:30:00' AND TIME('{currTime}') < '07:30:00' THEN 0
                    WHEN TIME('{currTime}') >= '07:30:00' AND TIME('{currTime}') < '11:00:00' THEN 1
                    WHEN TIME('{currTime}') >= '11:00:00' AND TIME('{currTime}') < '14:30:00' THEN 2
                    WHEN TIME('{currTime}') >= '14:30:00' AND TIME('{currTime}') < '18:00:00' THEN 3
                    WHEN TIME('{currTime}') >= '18:00:00' AND TIME('{currTime}') < '21:00:00' THEN 4
                    ELSE 5 end 
                '''
        try:
            profileData = pd.read_sql(query, self.conn)
        except Exception as e:
            self.logger.error(e)
        return profileData

    def profileFaresExtract(self, origin, destin, month, dow, currTime):
        query = f''' 
                SELECT Q0,Q1,Q2,Q3,Q4,Q5,Q6,Q7,Q8,Q9,Q10,Q11,Q12,Q13,Q14,Q15,Q16,Q17,Q18,Q19,Q20
                FROM config_profile_fares
                WHERE sector = CONCAT('{origin}','{destin}') and Month = '{month}' and DOW = '{dow}'  
                AND Cluster = 
                CASE 
                    WHEN TIME('{currTime}') >= '02:30:00' AND TIME('{currTime}') < '07:30:00' THEN 0
                    WHEN TIME('{currTime}') >= '07:30:00' AND TIME('{currTime}') < '11:00:00' THEN 1
                    WHEN TIME('{currTime}') >= '11:00:00' AND TIME('{currTime}') < '14:30:00' THEN 2
                    WHEN TIME('{currTime}') >= '14:30:00' AND TIME('{currTime}') < '18:00:00' THEN 3
                    WHEN TIME('{currTime}') >= '18:00:00' AND TIME('{currTime}') < '21:00:00' THEN 4
                    ELSE 5 end 
                '''
        try:
            profileData = pd.read_sql(query, self.conn)
        except Exception as e:
            self.logger.error(e)
        return profileData

    def getMappedMonth(self, currFltDate):
        query = f''' SELECT mappedMonth FROM config_date_event where date = '{currFltDate}' '''
        return pd.read_sql(query, self.conn)

    def getUpsellValue(self, currFltDate, sector):
        query = f''' SELECT 
                        REPLACE(value,'%%','') AS percentage
                    FROM
                        config_profile_fare_station_upsell
                    WHERE
                        date = '{currFltDate}'
                        AND sector = '{sector}' '''

        return pd.read_sql(query, self.conn)

    def getArea(self, currentHour):
        query = f''' SELECT sum(shareOfBooking) as sum FROM config_share_of_booking where hours <= {currentHour} '''
        return pd.read_sql(query, self.conn)

    def getRDBTable(self, dplfBand, endOfDayDplfBand, currentTime, ndoBand):
        query = f'''
            SELECT B2CValue, B2BValue 
            FROM config_determine_fares 
            WHERE 1 = 1 
            AND IFNULL(TIME(JSON_UNQUOTE(JSON_EXTRACT(conditions, '$.start_time'))), '00:00') <= '{currentTime}' 
            AND IFNULL(TIME(JSON_UNQUOTE(JSON_EXTRACT(conditions, '$.end_time'))), '23:59') >= '{currentTime}' 
            AND FIND_IN_SET('{dplfBand}',IFNULL(JSON_UNQUOTE(JSON_EXTRACT(conditions, '$.start_of_day_dlf_band')), '{dplfBand}')) 
            AND FIND_IN_SET('{endOfDayDplfBand}',IFNULL(JSON_UNQUOTE(JSON_EXTRACT(conditions, '$.end_of_day_dlf_band')), '{endOfDayDplfBand}')) 
            AND IFNULL(FIND_IN_SET('{ndoBand}',JSON_UNQUOTE(JSON_EXTRACT(conditions, '$.ndo_band'))), '{ndoBand}')
        '''
        return pd.read_sql(query, self.conn)

    def fareExtract(self, origin, destination, channel, openingRBD, route):

        RBDQuery = "select ROW_NUMBER() OVER (ORDER BY TOTAL) AS id, RBD, Total from Fares where Org = '" + origin + "' and Dst = '" + destination + "' and Route= '"+route+"'"

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

        NavitaireQuery = f"""SELECT Origin, Destination, FlightNumber, DepartureDate, bookedPax, capacity ,DepartureTime, adjustedCapacity,tbf_bookings from bookload
            where DepartureDate >= '{str(startDate)}' and DepartureDate <= '{str(endDate)}'
            and Origin = '{origin}' and Destination = '{destin}' """
        bookedData = pd.read_sql(NavitaireQuery, self.conn)

        if(self.debugMode):
            self.logger.info(NavitaireQuery)

        return bookedData

    def getLid(self, origin, destin, startDate, endDate, FltNumber):

        NavitaireQuery = f"""SELECT Lid from bookload
            where DepartureDate >= '{str(startDate)}' and DepartureDate <= '{str(endDate)}'
            and Origin = '{origin}' and Destination = '{destin}' and FlightNumber='{FltNumber}'"""
        bookedData = pd.read_sql(NavitaireQuery, self.conn)

        if(self.debugMode):
            self.logger.info(NavitaireQuery)

        return bookedData
    

    def checkForNextFLight(self, currFltDateStr, currParams):

        currFltDate = datetime.strptime(currFltDateStr, DATE_FORMAT).date()	

        query=f'''
            select DepartureTime,Origin,Destination,FlightNumber from bookload where DepartureDate = '{currFltDate}' AND Origin = '{currParams.origin}' AND Destination = '{currParams.destin}' order by DepartureTime ASC
        '''
        conn=self.conn
        df = pd.read_sql(query, conn)

        df['FlightNumber'] = df['FlightNumber'].astype(str)
        row_index = df[((df['FlightNumber'])== str(currParams.flight_number)) &(df['Origin'] == currParams.origin) & (df['Destination'] == currParams.destin)].index

        return len(row_index) > 0 and row_index[0] < len(df) - 1

    def getLastSellingFare(self, currFltDateStr, currParams):

        currFltDate = datetime.strptime(currFltDateStr, DATE_FORMAT).date()

        query=f'''
            select B2B_last3avg_fare,B2C_last3avg_fare,last7avg from booking_fare_data where FlightDate = '{currFltDate}' AND FlightNumber = '{currParams.flight_number}' AND Origin = '{currParams.origin}' AND Destin = '{currParams.destin}'
        '''
        conn=self.conn
        farePrice = pd.read_sql(query, conn)

        return farePrice

    def getNumberOfBookings(self, currFltDateStr, currParams):

        currFltDate = datetime.strptime(currFltDateStr, DATE_FORMAT_2).date()	

        query=f'''
            select BookingsCount,STD,BookingToday,BookingsInHour from booking_fare_data where FlightDate = '{currFltDate}' AND FlightNumber = '{currParams.flight_number}' AND Origin = '{currParams.origin}' AND Destin = '{currParams.destin}'
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

    def getFinalPrice(self, rbd_sector1, rbd_sector2, sector1, sector2, channel):

        if channel == 'B2C':
            condition = " and LEFT(RBD,1) != 'Z'"
        if channel == 'B2B':
            condition = " and LEFT(RBD,1) = 'Z'"

        sector1_RBDquery = f"select Total ,currency from Fares where sector = '{sector1}' and RBD = '{rbd_sector1['ClassOfService'][0]}' and route = 'L' {condition}"
        sector2_RBDquery = f"select Total ,currency from Fares where sector = '{sector2}' and RBD = '{rbd_sector2['ClassOfService'][0]}' and route = 'L' {condition}"

        sector1_result = pd.read_sql(sector1_RBDquery, self.conn)
        sector2_result = pd.read_sql(sector2_RBDquery, self.conn)

        if (sector1_result.shape[0] == 0 or sector2_result.shape[0] == 0):
            total = 0
            sector1_total=0
            sector2_total=0
            currency1=""
            currency2=""
        else:
            sector1_total = sector1_result["Total"][0]
            sector2_total = sector2_result["Total"][0]
            currency1=sector1_result["currency"][0]
            currency2=sector2_result["currency"][0]
            total = sector1_total + sector2_total
        return total,sector1_total,sector2_total,currency1,currency2

    def getSellingPrice(self, rbd, sector, channel,route):

        if channel == 'B2C':
            condition = " and LEFT(RBD,1) != 'Z'"
        if channel == 'B2B':
            condition = " and LEFT(RBD,1) = 'Z'"

        sector1_RBDquery = f"select Total,currency from Fares where sector = '{sector}' and RBD = '{rbd}' and route = '{route}' {condition}"

        sector1_result = pd.read_sql(sector1_RBDquery, self.conn)

        if (sector1_result.shape[0] == 0):

            sector1_total=0
            currency=""

        else:
            sector1_total = sector1_result["Total"][0]
            currency = sector1_result["currency"][0]
        return sector1_total,currency
    def insertSummaryRow_connections(self,allocresults):
        if self.inputSource.logs[CONST_RUN_SUMMARY] == DB:
            latest_row = allocresults[-1]
            insertQuery = f""" INSERT INTO run_summary_connections 
                (Id, RunId ,Sector1, Sector2, FltNum1, FltNum2, DepDate1,DepDate2, BookedLoad,BookedLoad1,SellingFare1,SellingFare2,BookedLoad2,OpenRBD1,OpenRBD2,Discount, anchorFare, OpenRBD, SellingFare, Channel, HowDetermined,allocationStatus, Backstop,CreatedAt,analystName , Variance1,Variance2,strategyFare,criteria,fareOffset,MktFare_Min,AirlineCode_Min,FlightNumber_Min,MktFare_Max,AirlineCode_Max,FlightNumber_Max,outBoundStop,outboundDuration,DiscountedFare,Currency,conversionRate) 
                VALUES (
                    '{latest_row.Id}', 
                    '{latest_row.RunId}', 
                    '{latest_row.Sector1}', 
                    '{latest_row.Sector2}', 
                    '{str(latest_row.FltNum1)}', 
                    '{str(latest_row.FltNum2)}', 
                    '{latest_row.DepDate1}', 
                    '{latest_row.DepDate2}', 
                    '{str(latest_row.BookedLoad)}',
                    '{str(latest_row.Bookedload1)}',
                    '{str(latest_row.SellingFare1)}',
                    '{str(latest_row.SellingFare2)}',
                    '{str(latest_row.Bookedload2)}',
                    '{str(latest_row.OpenRBD1)}',
                    '{str(latest_row.OpenRBD2)}',
                    '{str(latest_row.Discount)}',
                    '{latest_row.anchorFare}', 
                    '{str(latest_row.OpenRBD)}', 
                    '{str(latest_row.SellingFare)}', 
                    '{latest_row.Channel}', 
                    '{latest_row.HowDetermined}', 
                    '{latest_row.allocationStatus}', 
                    '{str(latest_row.Backstop)}',
                    '{self.timeutils.getCurrentDateTime().strftime(DATE_TIME_FORMAT)}',
                    '{str(latest_row.analystName)}',
                    '{str(latest_row.variance1)}',
                    '{str(latest_row.variance2)}',
                    '{str(latest_row.strategyfare)}',
                    '{str(latest_row.criteria)}',
                    '{str(latest_row.fareOffset)}',
                    '{str(latest_row.MktFare_Min)}',
                    '{str(latest_row.AirlineCode_Min)}',
                    '{str(latest_row.FlightNumber_Min)}',
                    '{str(latest_row.MktFare_Max)}',
                    '{str(latest_row.AirlineCode_Max)}',
                    '{str(latest_row.FlightNumber_Max)}',
                    '{str(latest_row.outBoundStop)}',
                    '{str(latest_row.outboundDuration)}',
                    '{str(latest_row.discountedFare)}',
                    '{str(latest_row.currency)}',
                    '{str(latest_row.conversionRate)}'
                    
                )"""
            self.wrconn.execute(insertQuery)

    def getcurrentRBD(self,current_date, flight, channel, sector,bookload):

        parameterValueQuery = f"""SELECT OpeningRBD as ClassOfService  FROM current_opening_rbd where DepartureDate = '{str(current_date)}' and concat(Origin,Destination)='{sector}' and flightno='{str(flight)}' and Channel='{channel}'"""
        try:
            parameterValue = pd.read_sql(parameterValueQuery, self.conn)
        except Exception as e:
            self.logger.error(e)

        return parameterValue

    def getDefaultAu(self, departure_date):
        month_name = datetime.strptime(departure_date, '%Y-%m-%d').strftime('%B')
        query = f"""
        SELECT default_au, au_cap
        FROM default_distress_inventory
        WHERE month='{str(month_name)}'
        """
        try:
            parameter_value = pd.read_sql(query, self.conn)
        except Exception as e:
            self.logger.error(e)
        return parameter_value["default_au"][0], parameter_value["au_cap"][0]

    def getDistressAu(self, ndo_band, dplf_band):
        query = f"""
        SELECT au_value
        FROM distress_inventory_strategy
        WHERE ndo_band='{str(ndo_band)}' AND dplf_band='{str(dplf_band)}'
        """
        try:
            parameter_value = pd.read_sql(query, self.conn)
        except Exception as e:
            self.logger.error(e)
        return parameter_value["au_value"][0]

    def get_seats_sold_by_class(self, flight_number, origin, destination, departure_date):
        query = f'''
        SELECT 
           ClassOfService as class_code,
           ClassSold as seats_sold
        FROM current_rbd
        WHERE
            FlightNumber = "{str(flight_number)}"
            AND DepartureDate = "{str(departure_date)}"
            AND Origin = "{origin}"
            AND Destin = "{destination}"
            AND (ClassOfService = 'P' or ClassOfService = 'L')
        '''
        df = pd.read_sql(query, self.conn)
        if df.empty:
            return -1, -1
        l_class_seats_sold = df[df['class_code'] == 'L']['seats_sold'].values[0]
        p_class_seats_sold = df[df['class_code'] == 'P']['seats_sold'].values[0]
        return l_class_seats_sold, p_class_seats_sold

    def get_current_rbds_by_class(self, departure_date, flight_number, sector):
        query = f"""
            SELECT
                ClassOfService as class_code,
                ClassAu as current_availability
            FROM current_rbd
            WHERE
                DepartureDate = "{departure_date}"
                AND FlightNumber = "{str(flight_number)}"
                AND Origin + Destin = "{str(sector)}"
                AND (ClassOfService = 'P' or ClassOfService = 'L' )
            """
        df = pd.read_sql(query, self.conn)
        if df.empty:
            return -1, -1
        l_class_rbd = df[df['class_code'] == 'L']['current_availability'].values[0]
        p_class_rbd = df[df['class_code'] == 'P']['current_availability'].values[0]
        return l_class_rbd, p_class_rbd
    
    def getSeriesBlockedSeats(self,departure_date):
        query=f"""select * from series_blocking where DepartureDate = '{departure_date}'"""
        df = pd.read_sql(query, self.conn)
        return df
    def get_series_seats(self,departure_date, flight_number, sector):
        query = f"""select Origin,Destination,FlightNumber,DepartureDate,bookedPax,OrgCode from series_booked where FlightNumber='{flight_number}' and DepartureDate = '{departure_date}' and concat(Origin,Destination)='{sector}'"""
        df = pd.read_sql(query, self.conn)
        return df
    
    def fetch_all_conversion_data(self):
        query = f"SELECT FromCurrencyCode, ToCurrencyCode, ConversionRate FROM config_conversion_rate"
        df = pd.read_sql(query, self.conn)
        return df
    
    def get_curveids(self,sector1,sector2,fltno1,fltno2,dep1,dep2):
        
        query = f"SELECT * FROM market_list ml WHERE (CONCAT(ml.Origin,ml.Destin) = '{sector1}' AND STR_TO_DATE(ml.PerStart, '%%m/%%d/%%Y') = '{dep1}' AND ml.FlightNumber = '{fltno1}') OR (CONCAT(ml.Origin,ml.Destin) = '{sector2}' AND STR_TO_DATE(ml.PerStart, '%%m/%%d/%%Y') = '{dep2}' AND ml.FlightNumber = '{fltno2}') UNION SELECT * FROM market_list_international mli WHERE (CONCAT(mli.Origin, mli.Destin) = '{sector1}' AND STR_TO_DATE(mli.PerStart, '%%m/%%d/%%Y') = '{dep1}' AND mli.FlightNumber = '{fltno1}') OR (CONCAT(mli.Origin, mli.Destin) = '{sector2}' AND STR_TO_DATE(mli.PerStart, '%%m/%%d/%%Y') = '{dep2}' AND mli.FlightNumber = '{fltno2}');"
        
        df = pd.read_sql(query, self.conn)
        return df
    
    def get_second_flight_dep(self,flightno1,flightno2,org,destin,depDate):
        query = f"""select Departuredate2 from connections_details where FlightNo1='{flightno1}' and FlightNo2 = '{flightno2}' and Origin1='{org}' and Destnination2='{destin}' and Departuredate1='{depDate}'"""
        df = pd.read_sql(query, self.conn)
        if len(df)==0:
            return ""
        return df['Departuredate2'][0]
    
    def get_tbf_discount(self,month,sector):
        query = f"""
        SELECT 
    TF, TE, TD, TC, TB, TY
FROM
    tbf_discount_grid
WHERE
    (Sector = '{sector}' OR Sector = '*')
        AND (Month = '{month}' OR Month = '*')
ORDER BY CASE
    WHEN Sector = '{sector}' THEN 0
    ELSE 1
END , CASE
    WHEN Month = '{month}' THEN 0
    ELSE 1
END
LIMIT 1
        """
        
        df = pd.read_sql(query, self.conn)
        return df