from datetime import datetime, timedelta

import pandas as pd
from Constants import *
import numpy as np

class GetDynamicTimeD2:
    def __init__(self, database_helper):
        self.rmdatabasehelper = database_helper

    def getDynamicTime(self, currParams, dplfBand, ndoBand, DaySpan, channel,plf):
        startTime = currParams.time_window_start
        endTime = currParams.time_window_end
        hardstartTime = currParams.time_window_start
        hardendTime = currParams.time_window_end
        time_range = '0'
        
        if currParams.plfthreshold != '' and pd.notna(currParams.plfthreshold):
            threshold_value = float(str(currParams.plfthreshold).replace('%', ''))
            
            if int(plf) < threshold_value:
                hardstartTime = currParams.time_window_start
                hardendTime = currParams.time_window_end
                hard_time_range = '0'
                
        dynamicTimeList = self.rmdatabasehelper.getd1d2StrategyValue(currParams.strategy, dplfBand, ndoBand, channel)
        time_range = dynamicTimeList['time_range'][0]

        rangeList = self.rmdatabasehelper.getTimeRange(time_range)

        if rangeList['type'][0] == CONST_ABSOLUTE:
            if DaySpan < 0:
                endTime = rangeList['end'][0]
            elif DaySpan > 0:
                startTime = rangeList['start'][0]
            else:
                endTime = rangeList['end'][0]
                startTime = rangeList['start'][0]

        if rangeList['type'][0] == CONST_RELATIVE:
            startTime, endTime = self.getRelativeTimeValue(currParams, rangeList, DaySpan)

        if rangeList['type'][0] == CONST_REFERENCE:
            rangeList = self.rmdatabasehelper.getTimeReferenceValue(currParams)
            startTime = rangeList['ar_start'][0]
            endTime = rangeList['ar_end'][0]
                
            
            

        return str(startTime), str(endTime), str(time_range),hardstartTime,hardendTime

    def getRelativeTimeValue(self, currParams, rangeList, DaySpan):
        start_datetime_object = datetime.strptime(currParams.per_start, DATE_FORMAT)
        end_datetime_object = datetime.strptime(currParams.per_end, DATE_FORMAT)

        startDateTime = currParams.per_start + ' ' + currParams.time_window_start
        endDateTime = currParams.per_end + ' ' + currParams.time_window_end

        startTime = datetime.strptime(startDateTime, DATE_TIME_FORMAT2) + timedelta(minutes=int(rangeList['start'][0]))
        endTime = datetime.strptime(endDateTime, DATE_TIME_FORMAT2) + timedelta(minutes=int(rangeList['end'][0]))
        if DaySpan < 0:
            startTime = datetime.strptime(startDateTime, DATE_TIME_FORMAT2)
        elif DaySpan > 0:
            endTime = datetime.strptime(endDateTime, DATE_TIME_FORMAT2)
        startTime = max(startTime, start_datetime_object.replace(hour=int(CONST_START_TIME[:2]), minute=int(CONST_START_TIME[3:])))
        endTime = min(endTime, end_datetime_object.replace(hour=int(CONST_END_TIME[:2]), minute=int(CONST_END_TIME[3:])))

        startTime = f"{startTime.hour:02d}:{startTime.minute:02d}"
        endTime = f"{endTime.hour:02d}:{endTime.minute:02d}"

        return startTime, endTime
    
    def getDynamicD2Value(self, currParams, dplfBand, ndoBand, channel, plf):
        hardcriteria = CONST_NA
        hardoffset = CONST_NA
        fare = CONST_NA
        
        if currParams.plfthreshold != '' and pd.notna(currParams.plfthreshold) and not self.check_int(currParams.hardanchor):
            threshold_value = float(str(currParams.plfthreshold).replace('%', ''))
            if int(plf) < threshold_value:
                hardcriteria = currParams.hardanchor
                hardoffset = currParams.fare_offset
                
        dynamicD2List = self.rmdatabasehelper.getd1d2StrategyValue(currParams.strategy, dplfBand, ndoBand, channel)
        criteria = dynamicD2List['criteria'][0]
        offset = dynamicD2List['offset'][0]
        strategy = dynamicD2List['strategy'][0]
        
        if self.check_int(currParams.hardanchor):
            threshold_value = float(str(currParams.plfthreshold).replace('%', ''))
            if int(plf) < threshold_value:
                fare = currParams.hardanchor
        
        return criteria, hardcriteria, offset, hardoffset, strategy, fare
    
    
    def check_int(self,value):
        try:
            int(value)
            return True
        except:
            return False
