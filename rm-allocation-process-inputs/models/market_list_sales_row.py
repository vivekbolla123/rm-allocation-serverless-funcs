from Constants import *


class MarketListSalesRow:

    def __init__(self, currParams):
        self.sector = currParams.get("sector", CONST_NA)
        self.flight_number = currParams.get("flight_number", CONST_NA)
        self.departure_date = currParams.get("departure_date", CONST_NA)
        self.series_value = currParams.get("series_value", CONST_NA)
        self.mapped_month = currParams.get("mapped_month", CONST_NA)
        self.target_base_load = currParams.get("target_base_load", CONST_NA)
        self.hist_curr_plf = currParams.get("hist_curr_plf", CONST_NA)
        self.hist_base_plf = currParams.get("hist_base_plf", CONST_NA)
        self.seats_sold_W4 = currParams.get("seats_sold_W4", CONST_NA)
        self.seats_sold_W5 = currParams.get("seats_sold_W5", CONST_NA)
        self.seats_sold_W6 = currParams.get("seats_sold_W6", CONST_NA)
        self.booked_pax = currParams.get("bookedPax", CONST_NA)
        self.adjusted_capacity = currParams.get("adjustedCapacity", CONST_NA)
        
        self.run_id = currParams.get("runId", CONST_NA)
        self.update_navitaire_method = currParams.get("update_navitaire_method", CONST_NA)
        self.run_time = currParams.get("runTime", CONST_NA)
        self.dbconn = currParams.get("dbconn", CONST_NA)
        self.wrconn = currParams.get("wrconn", CONST_NA)
        self.dwconn = currParams.get("dwconn", CONST_NA)
        self.sqs = currParams.get("sqs", CONST_NA)
        self.run_single_market = currParams.get("runSingleMarket", CONST_NA)
        self.s3 = currParams.get("s3", CONST_NA)
        self.cache_client = currParams.get("cache_client", CONST_NA)
        self.navitaire_update_method = currParams.get("NavitaireUpdateMethod", CONST_NA)
        self.time_logger = currParams.get("timelogger", CONST_NA)        
        self.route_type=currParams.get("route_type",CONST_NA)
        self.mongodb=currParams.get("mongodb",'0')
        self.mongodbclient=currParams.get("mongodbclient",CONST_NA)


