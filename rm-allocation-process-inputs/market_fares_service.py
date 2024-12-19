import json
import math
from Constants import *
import numpy as np
import pandas as pd
import logging


class MarketFareService:
    # Assuming other parts of the class are defined elsewhere
    def __init__(self,rm_database_helper,nav_database_helper,constantsutils,timeutils):
        self.rmdatabasehelper=rm_database_helper
        self.navdatabasehelper=nav_database_helper
        self.constantsutils=constantsutils
        self.timeutils=timeutils
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)    
    
    def getMarketFareAnchorFare(self, currParams,currFltDate, channel, startTime, endTime, DaySpan, criteria, flightTime,hardcriteria,hardfare,hardstartTime,hardendTime):
        startTime, endTime, criteria, mktFareRange, mktfareRange_minomaxo = self.getMarketFares(currFltDate, currParams, channel, startTime, endTime, DaySpan, criteria, flightTime)
        _, _, _, hardmktFareRange, hardmktfareRange_minomaxo= self.getMarketFares(currFltDate, currParams, channel, hardstartTime, hardendTime, 0, criteria, flightTime)

        #Does competetive fare exist
        #We only proceed if a B2C fare exists - If B2C does not exist, we will skip the whole thing even with a B2B fare
        # This is because, when we prepare the AU Grid, if we donr push inventory on to B2C RBD, then the flight will not be visible 
        # on OTA - Flag the error for correction here
        isMarketFareAvailable = False
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
            if hardcriteria !=CONST_NA:
                if ((hardcriteria == MINO_FARE_ANCHOR ) and (len(hardmktfareRange_minomaxo)>0)):
                    hardfare = hardmktfareRange_minomaxo['LowFare'][0]
                if ((hardcriteria == MAXO_FARE_ANCHOR ) and (len(hardmktfareRange_minomaxo)>0)):
                    hardfare = hardmktfareRange_minomaxo['HiFare'][0]
                if ((hardcriteria == MIN_FARE_ANCHOR ) and (len(hardmktFareRange)>0)):
                    hardfare = hardmktFareRange['gross_fare'][0]
                if ((hardcriteria == MAX_FARE_ANCHOR ) and (len(hardmktFareRange)>0)):
                    hardfare = hardmktFareRange['gross_fare'].iloc[-1]

            isMarketFareAvailable = True
        
        return lowFare,hiFare,lowAirlineCode,hiAirlineCode,lowFilghtNum,hiFilghtNum,isMarketFareAvailable, criteria, startTime, endTime,hardfare

    def getMarketFares(self, currFltDate, currParams, channel, startTime, endTime, DaySpan, criteria, flightTime):
        mktFareRange = self.fetchMarketFareRange(
            channel, currFltDate, currParams, DaySpan, startTime, endTime, includeWeekend=False
        )
            

        return self.prepareResponse(startTime, endTime, criteria, mktFareRange)
    
    def fetchMarketFareRange(self, channel, flightDate, params, daySpan, start_time, end_time, includeWeekend):
        ndo=self.timeutils.calculate_ndo(flightDate)
        hrs=self.get_value_for_ndo(self.constantsutils.MARKET_FARES_THRESHOLD,ndo)
        return self.rmdatabasehelper.getMarketFareRange(
            channel, params,flightDate, daySpan, start_time, end_time, includeWeekend,hrs
        )

    def get_value_for_ndo(self,json_string, ndo):
        
        ranges = json.loads(json_string)
        
        
        for range_dict in ranges:
            if int(range_dict['startNdo']) <= ndo <= int(range_dict['endNdo']):
                return range_dict['value']
        
        return None 
    
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

    def prepareResponse(self, startTime, endTime, criteria, marketFareRange):
        minMaxFare = self.getMarketFareRangeminomaxo(marketFareRange)
        return startTime, endTime, criteria, marketFareRange, minMaxFare
    
    def fetchMarketFareRangeConnections(self, channel, flightDate, params, outBoundStop,outboundDuration):
        
        return self.rmdatabasehelper.getMarketFareRangeConnections(
            channel, params,flightDate, outBoundStop,outboundDuration
        )
    
    def getMarketFareAnchorFareConnections(self,channel, params,flightDate, outBoundStop,outboundDuration,criteria,inputSource,convert_currency):
        
        mktFareRange = self.fetchMarketFareRangeConnections(channel,flightDate,params, outBoundStop,outboundDuration)
        isMarketFareAvailable = False
        lowFare = 0
        hiFare = 0
        lowAirlineCode = "Market Fare not found"
        hiAirlineCode = "Market Fare not found"
        lowFilghtNum = "Market Fare not found"
        hiFilghtNum = "Market Fare not found"
        if len(mktFareRange)!=0:
            isMarketFareAvailable = True
            for index, row in mktFareRange.iterrows():
                if row['currency'] != 'INR':
                    converted_fare = convert_currency(row['currency'], 'INR', row['gross_fare'])
                    mktFareRange.at[index, 'gross_fare'] = converted_fare
                    mktFareRange.at[index, 'currency'] = 'INR'
            mktFareRange=mktFareRange.sort_values(by='gross_fare', ascending=True)
            mktfareRange_minomaxo = self.getMarketFareRangeminomaxo(mktFareRange)
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
        
        return lowFare,hiFare,lowAirlineCode,hiAirlineCode,lowFilghtNum,hiFilghtNum,isMarketFareAvailable, criteria


    def getMFinRange(self, currParams,currFltDate, channel, startTime, endTime, DaySpan, criteria, flightTime, dplfBand):
        startTime, endTime, criteria, mktFareRange, mktfareRange_minomaxo = self.getMarketFares(currFltDate, currParams, channel, startTime, endTime, DaySpan, criteria, flightTime)
        min_10 = 0
        if(dplfBand == 0):
            if(len(mktFareRange) > 0):
                min_10 = mktFareRange['gross_fare'][self.constantsutils.MFMIN0]
        elif(dplfBand == 1 or dplfBand == 2):
            if(len(mktFareRange) > 1):
                min_10 = mktFareRange['gross_fare'][self.constantsutils.MFMIN1]
            elif(len(mktFareRange) == 1):
                min_10 = mktFareRange['gross_fare'][self.constantsutils.MFMIN0]
        return min_10
