from datetime import date, datetime,timedelta

import pytz

from Constants import DATE_FORMAT

class TimeUtils:
    def __init__(self,inputsource) :
        self.inputSource=inputsource
        
    
    def parse_time(self,time_str):
        return datetime.strptime(time_str, '%H:%M').time()
    
    def getCurrentDate(self):
        if self.inputSource.currDate != None:
            return self.inputSource.currDate
        currDate = self.getCurrentDateTime()
        currDate = currDate.date()
        return currDate
    
    def getCurrentDateTime(self):
        if self.inputSource.currDateTime != None:
            return  self.inputSource.currDateTime
        current_utc_time = datetime.utcnow()
        # Define the UTC time zone
        utc_timezone = pytz.timezone('UTC')
        # Set the time zone to IST (Indian Standard Time)
        ist_timezone = pytz.timezone('Asia/Kolkata')
        # Convert the UTC time to IST
        startTime = current_utc_time.replace(tzinfo=utc_timezone).astimezone(ist_timezone)
        return startTime
    
    def subtract_hours(self,time_str, hours):
        time_obj = datetime.strptime(time_str, '%H:%M')
        new_time = (time_obj - timedelta(hours=hours)).time()
        return str(new_time)[:5]
    def add_hours(self,time_str, hours):
        time_obj = datetime.strptime(time_str, '%H:%M')
        new_time = (time_obj + timedelta(hours=hours)).time()
        return str(new_time)[:5]
    
    def calculate_ndo(self,departure_date):
        if self.inputSource.currDate != None:
            today = self.inputSource.currDate
        else:
            today = date.today()
        departure = datetime.strptime(departure_date, DATE_FORMAT).date()
        
        ndo = (departure - today).days
        
        return max(ndo, 0)
