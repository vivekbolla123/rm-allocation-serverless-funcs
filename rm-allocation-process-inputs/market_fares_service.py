from Constants import *
import numpy as np
import pandas as pd


class MarketFareService:
    # Assuming other parts of the class are defined elsewhere
    def __init__(self,rm_database_helper,constantsutils):
        self.rmdatabasehelper=rm_database_helper
        self.constantsutils=constantsutils
        
    def getMarketFareAnchorFare(self, currParams,currFltDate, channel, startTime, endTime, DaySpan, criteria, flightTime):
        startTime, endTime, criteria, mktFareRange, mktfareRange_minomaxo, summary = self.getMarketFares(currFltDate, currParams, channel, startTime, endTime, DaySpan, criteria, flightTime)

        #Does competetive fare exist
        #We only proceed if a B2C fare exists - If B2C does not exist, we will skip the whole thing even with a B2B fare
        # This is because, when we prepare the AU Grid, if we donr push inventory on to B2C RBD, then the flight will not be visible 
        # on OTA - Flag the error for correction here
        lowFare = 0
        hiFare = 0
        lowAirlineCode = "Market Fare not found"
        hiAirlineCode = "Market Fare not found"
        lowFilghtNum = "Market Fare not found"
        hiFilghtNum = "Market Fare not found"

        if(len(mktFareRange) > 0):
            lowFare = mktFareRange['gross_fare'][0]
            hiFare = mktFareRange['gross_fare'].iloc[-1]
            lowAirlineCode = mktFareRange['outbound_airline_code'][0]
            hiAirlineCode = mktFareRange['outbound_airline_code'].iloc[-1]
            lowFilghtNum = mktFareRange['outbound_flight'][0]
            hiFilghtNum = mktFareRange['outbound_flight'].iloc[-1]
            if ((criteria == MINO_FARE_ANCHOR or criteria == MAXO_FARE_ANCHOR) and (len(mktFareRange)>1)):
                lowAirlineCode = mktfareRange_minomaxo['LowFare_airline_code'][0]
                hiAirlineCode = mktfareRange_minomaxo['HiFare_airline_code'][0]
                lowFilghtNum = mktfareRange_minomaxo['LowFare_outbound_flight'][0]
                hiFilghtNum = mktfareRange_minomaxo['HiFare_outbound_flight'][0]
                lowFare = mktfareRange_minomaxo['LowFare'][0]
                hiFare = mktfareRange_minomaxo['HiFare'][0]
        
            isMarketFareAvailable = True

        return lowFare,hiFare,lowAirlineCode,hiAirlineCode,lowFilghtNum,hiFilghtNum,isMarketFareAvailable, criteria, summary, startTime, endTime

    def getMarketFares(self, currFltDate, currParams, channel, startTime, endTime, DaySpan, criteria, flightTime):
        mktFareRange = self.fetchMarketFareRange(
            channel, currFltDate, currParams, DaySpan, startTime, endTime, includeWeekend=False
        )
    
        summary = "MF_Org"
        
        if(len(mktFareRange) == 0):
            if(flightTime <= '14:00'):
                start_time="00:00"
                end_time="14:00"
            else:
                start_time="14:01"
                end_time="23:59"
            criteria = "MINO"
            summary = "MF_HalfDay"
            mktFareRange = self.fetchMarketFareRange(channel,currFltDate, currParams,DaySpan,start_time,end_time,False)
       
                        
            if(len(mktFareRange) == 0):
                start_time="00:00"
                end_time="23:59"
                summary = "MF_Fullday"
                mktFareRange = self.fetchMarketFareRange(channel,currFltDate, currParams,DaySpan,start_time,end_time,False)

                if(len(mktFareRange) == 0):
                    DaySpan = 3
                    summary = "MF_TR_3Days"
                    start_time=startTime
                    end_time=endTime
                    mktFareRange = self.fetchMarketFareRange(channel,currFltDate, currParams,DaySpan, startTime, endTime,True)

                    if(len(mktFareRange) == 0):
                        DaySpan=3
                        start_time="00:00"
                        end_time="23:59"
                        summary = "MF_FR_3Days"
                        mktFareRange = self.fetchMarketFareRange(channel,currFltDate, currParams,DaySpan,start_time,end_time,False)

                        if(len(mktFareRange) == 0):
                            summary = "MF_NA"
                            start_time = startTime
                            end_time = endTime
            
            startTime = start_time
            endTime = end_time

        return self.prepareResponse(startTime, endTime, criteria, mktFareRange, summary)
    
    def fetchMarketFareRange(self, channel, flightDate, params, daySpan, start_time, end_time, includeWeekend):
        
        return self.rmdatabasehelper.getMarketFareRange(
            channel, params,flightDate, daySpan, start_time, end_time, includeWeekend
        )

    
    def getMarketFareRangeminomaxo(self, mktFareRange):
        data = pd.DataFrame(mktFareRange)
        fare_l1 = data["gross_fare"].tolist()
        fare_list = []
        if len(fare_l1)>1:
            coveragevalue = int(self.constantsutils.COVERAGE_PARAMETER)
            MED = np.median(fare_l1)
            MAD = np.median(np.abs(fare_l1- np.median(fare_l1)))
            if MAD==0:
                MAD=0.15*MED
            for fare in fare_l1:
                MZS = np.abs(fare - MED)/MAD
                if MZS < coveragevalue:
                    fare_list.append(fare)
            min_fare = min(fare_list)
            max_fare = max(fare_list)
            min_fare_row = data[data["gross_fare"] == min_fare].iloc[0]
            max_fare_row = data[data["gross_fare"] == max_fare].iloc[0]
            min_airline_code = min_fare_row["outbound_airline_code"]
            min_outbound_flight = min_fare_row["outbound_flight"]
            max_airline_code = max_fare_row["outbound_airline_code"]
            max_outbound_flight = max_fare_row["outbound_flight"]
        elif len(fare_list)==0:
            min_fare=0
            max_fare=0
            min_airline_code = 'Market Fare not found'
            min_outbound_flight = 'Market Fare not found'
            max_airline_code = 'Market Fare not found'
            max_outbound_flight = 'Market Fare not found'
        else:
            min_fare=fare_list[0]
            max_fare=fare_list[0]
            min_fare_row = data[data["gross_fare"] == min_fare].iloc[0]
            max_fare_row = data[data["gross_fare"] == max_fare].iloc[0]
            min_airline_code = min_fare_row["outbound_airline_code"]
            min_outbound_flight = min_fare_row["outbound_flight"]
            max_airline_code = max_fare_row["outbound_airline_code"]
            max_outbound_flight = max_fare_row["outbound_flight"]
        mktFareRange = pd.DataFrame(
            {'LowFare': [min_fare], 'HiFare': [max_fare],'LowFare_airline_code': [min_airline_code], 'LowFare_outbound_flight': [min_outbound_flight], 'HiFare_airline_code': [max_airline_code], 'HiFare_outbound_flight': [max_outbound_flight]})
        return mktFareRange

    def prepareResponse(self, startTime, endTime, criteria, marketFareRange, summary):
        minMaxFare = self.getMarketFareRangeminomaxo(marketFareRange)
        return startTime, endTime, criteria, marketFareRange, minMaxFare, summary


