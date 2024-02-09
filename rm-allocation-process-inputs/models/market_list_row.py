class MarketListRow:
    def __init__(self,currParams):
        self.origin = currParams.get('Origin',"NA")
        self.destin = currParams.get('Destin',"NA")
        self.flight_number = currParams.get('FlightNumber',"NA")
        self.per_start =currParams.get('PerStart',"NA")
        self.per_end = currParams.get('PerEnd',"NA")
        self.dow = currParams.get('DOW',"NA")
        self.strategy_flag = currParams.get('strategyFlag',"NA")
        self.time_window_start = currParams.get('TimeWindowStart',"NA")
        self.time_window_end = currParams.get('TimeWindowEnd',"NA")
        self.curve_id = currParams.get("CurveID","NA")
        self.carr_exclusion = currParams.get('CarrExlusion',"NA")
        self.fare_anchor = currParams.get("fareAnchor","NA")
        self.fare_offset = currParams.get("fareOffset","NA")
        self.first_rbd_alloc = currParams.get("FirstRBDAlloc","NA")
        self.other_rbd_alloc = currParams.get("OtherRBDAlloc","NA")
        self.b2b_backstop = currParams.get("B2BBackstop","NA")
        self.b2c_backstop = currParams.get("B2CBackstop","NA")
        self.b2b_factor = currParams.get("B2BFactor","NA")
        self.skipping_factor = currParams.get("SkippingFactor","NA")
        self.own_fare_flag = currParams.get("OwnFareFlag","NA")
        self.day_span = currParams.get("DaySpan","NA")
        self.auto_time_range_flag = currParams.get("AutoTimeRangeFlag","NA")
        self.analyst_name = currParams.get("analystName","NA")
        self.opening_fares = currParams.get("openingFares","NA")
        self.over_booking = currParams.get("OverBooking","NA")
        self.run_id = currParams.get("runId","NA")
        self.update_navitaire_method = currParams.get("update_navitaire_method","NA")
        self.run_time = currParams.get("runTime","NA")
        self.navitaire_connection = currParams.get("NavitaireConnection","NA")
        self.dbconn = currParams.get("dbconn","NA")
        self.wrconn = currParams.get("wrconn","NA")
        self.sqs = currParams.get("sqs","NA")
        self.run_single_market = currParams.get("runSingleMarket","NA")
        self.s3 = currParams.get("s3","NA")
        self.cache_client = currParams.get("cache_client","NA")
        self.navitaire_update_method = currParams.get("NavitaireUpdateMethod","NA")
        self.strategy="NA"

    # # Getters and Setters
    # @property
    # def origin(self):
    #     return self._origin

    # @origin.setter
    # def origin(self, value):
    #     self._origin = value

    # @property
    # def destin(self):
    #     return self._destin

    # @destin.setter
    # def destin(self, value):
    #     self._destin = value

    # @property
    # def flight_number(self):
    #     return self._flight_number

    # @flight_number.setter
    # def flight_number(self, value):
    #     self._flight_number = value

    # @property
    # def per_type(self):
    #     return self._per_type

    # @per_type.setter
    # def per_type(self, value):
    #     self._per_type = value

    # @property
    # def per_start(self):
    #     return self._per_start

    # @per_start.setter
    # def per_start(self, value):
    #     self._per_start = value

    # @property
    # def per_end(self):
    #     return self._per_end

    # @per_end.setter
    # def per_end(self, value):
    #     self._per_end = value

    # @property
    # def dow(self):
    #     return self._dow

    # @dow.setter
    # def dow(self, value):
    #     self._dow = value

    # @property
    # def strategy_flag(self):
    #     return self._strategy_flag

    # @strategy_flag.setter
    # def strategy_flag(self, value):
    #     self._strategy_flag = value

    # @property
    # def time_window_start(self):
    #     return self._time_window_start

    # @time_window_start.setter
    # def time_window_start(self, value):
    #     self._time_window_start = value

    # @property
    # def time_window_end(self):
    #     return self._time_window_end

    # @time_window_end.setter
    # def time_window_end(self, value):
    #     self._time_window_end = value

    # @property
    # def curve_id(self):
    #     return self._curve_id

    # @curve_id.setter
    # def curve_id(self, value):
    #     self._curve_id = value

    # @property
    # def carr_exclusion(self):
    #     return self._carr_exclusion

    # @carr_exclusion.setter
    # def carr_exclusion(self, value):
    #     self._carr_exclusion = value

    # @property
    # def fare_anchor(self):
    #     return self._fare_anchor

    # @fare_anchor.setter
    # def fare_anchor(self, value):
    #     self._fare_anchor = value

    # @property
    # def fare_offset(self):
    #     return self._fare_offset

    # @fare_offset.setter
    # def fare_offset(self, value):
    #     self._fare_offset = value

    # @property
    # def first_rbd_alloc(self):
    #     return self._first_rbd_alloc

    # @first_rbd_alloc.setter
    # def first_rbd_alloc(self, value):
    #     self._first_rbd_alloc = value

    # @property
    # def other_rbd_alloc(self):
    #     return self._other_rbd_alloc

    # @other_rbd_alloc.setter
    # def other_rbd_alloc(self, value):
    #     self._other_rbd_alloc = value

    # @property
    # def b2b_backstop(self):
    #     return self._b2b_backstop

    # @b2b_backstop.setter
    # def b2b_backstop(self, value):
    #     self._b2b_backstop = value

    # @property
    # def b2c_backstop(self):
    #     return self._b2c_backstop

    # @b2c_backstop.setter
    # def b2c_backstop(self, value):
    #     self._b2c_backstop = value

    # @property
    # def b2b_factor(self):
    #     return self._b2b_factor

    # @b2b_factor.setter
    # def b2b_factor(self, value):
    #     self._b2b_factor = value

    # @property
    # def skipping_factor(self):
    #     return self._skipping_factor

    # @skipping_factor.setter
    # def skipping_factor(self, value):
    #     self._skipping_factor = value

    # @property
    # def own_fare_flag(self):
    #     return self._own_fare_flag

    # @own_fare_flag.setter
    # def own_fare_flag(self, value):
    #     self._own_fare_flag = value

    # @property
    # def day_span(self):
    #     return self._day_span

    # @day_span.setter
    # def day_span(self, value):
    #     self._day_span = value

    # @property
    # def auto_time_range_flag(self):
    #     return self._auto_time_range_flag

    # @auto_time_range_flag.setter
    # def auto_time_range_flag(self, value):
    #     self._auto_time_range_flag = value

    # @property
    # def analyst_name(self):
    #     return self._analyst_name

    # @analyst_name.setter
    # def analyst_name(self, value):
    #     self._analyst_name = value

    # @property
    # def opening_fares(self):
    #     return self._opening_fares

    # @opening_fares.setter
    # def opening_fares(self, value):
    #     self._opening_fares = value

    # @property
    # def over_booking(self):
    #     return self._over_booking

    # @over_booking.setter
    # def over_booking(self, value):
    #     self._over_booking = value

    # @property
    # def run_id(self):
    #     return self._run_id

    # @run_id.setter
    # def run_id(self, value):
    #     self._run_id = value

    # @property
    # def update_navitaire_method(self):
    #     return self._update_navitaire_method

    # @update_navitaire_method.setter
    # def update_navitaire_method(self, value):
    #     self._update_navitaire_method = value

    # @property
    # def run_time(self):
    #     return self._run_time

    # @run_time.setter
    # def run_time(self, value):
    #     self._run_time = value

    # @property
    # def navitaire_connection(self):
    #     return self._navitaire_connection

    # @navitaire_connection.setter
    # def navitaire_connection(self, value):
    #     self._navitaire_connection = value

    # @property
    # def dbconn(self):
    #     return self._dbconn

    # @dbconn.setter
    # def dbconn(self, value):
    #     self._dbconn = value

    # @property
    # def wrconn(self):
    #     return self._wrconn

    # @wrconn.setter
    # def wrconn(self, value):
    #     self._wrconn = value

    # @property
    # def sqs(self):
    #     return self._sqs

    # @sqs.setter
    # def sqs(self, value):
    #     self._sqs = value

    # @property
    # def run_single_market(self):
    #     return self._run_single_market

    # @run_single_market.setter
    # def run_single_market(self, value):
    #     self._run_single_market = value

    # @property
    # def s3(self):
    #     return self._s3

    # @s3.setter
    # def s3(self, value):
    #     self._s3 = value

    # @property
    # def cache_client(self):
    #     return self._cache_client

    # @cache_client.setter
    # def cache_client(self, value):
    #     self._cache_client = value

    # @property
    # def navitaire_update_method(self):
    #     return self._navitaire_update_method

    # @navitaire_update_method.setter
    # def navitaire_update_method(self, value):
    #     self._navitaire_update_method = value
