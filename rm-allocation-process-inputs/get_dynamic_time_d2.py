from datetime import datetime, timedelta
from Constants import *

class GetDynamicTimeD2:
    def __init__(self, database_helper):
        self.rmdatabasehelper = database_helper

    def getDynamicTime(self, currParams, dplfBand, ndoBand, DaySpan, channel):
        startTime = currParams.time_window_start
        endTime = currParams.time_window_end
        time_range = '0'
        if str(currParams.strategy_flag) == '1':
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

        return str(startTime), str(endTime), str(time_range)

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
    
    def getDynamicD2Value(self, currParams, dplfBand, ndoBand,channel):
        criteria = currParams.fare_anchor
        offset = currParams.fare_offset
        strategy = 'NA'

        if (str(currParams.strategy_flag)== '1'):
            dynamicD2List = self.rmdatabasehelper.getd1d2StrategyValue(currParams.strategy, dplfBand, ndoBand,channel)
            criteria = dynamicD2List['criteria'][0]
            offset = dynamicD2List['offset'][0]
            strategy = dynamicD2List['strategy'][0]
        
        return criteria, offset, strategy
