from Constants import *
from common.compute_utils import parse_numeric, parse_int_flag


class MarketListRow:

    def __init__(self, currParams):
        self.origin = currParams.get("Origin", CONST_NA)
        self.destin = currParams.get("Destin", CONST_NA)
        self.flight_number = currParams.get("FlightNumber", CONST_NA)
        self.per_start = currParams.get("PerStart", CONST_NA)
        self.per_end = currParams.get("PerEnd", CONST_NA)
        self.dow = currParams.get("DOW", CONST_NA)
        self.hardanchor = currParams.get("hardAnchor", CONST_NA)
        self.plfthreshold = currParams.get("plfThreshold", CONST_NA)
        self.time_window_start = currParams.get("TimeWindowStart", CONST_NA)
        self.time_window_end = currParams.get("TimeWindowEnd", CONST_NA)
        self.curve_id = currParams.get("CurveID", CONST_NA)
        self.carr_exclusion_b2c = currParams.get("CarrExlusionB2C", CONST_NA)
        self.carr_exclusion_b2b = currParams.get("CarrExlusionB2B", CONST_NA)
        self.flightExclusionB2C = currParams.get("flightExclusionB2C", CONST_NA)
        self.flightExclusionB2B = currParams.get("flightExclusionB2B", CONST_NA)
        self.fare_anchor = currParams.get("fareAnchor", CONST_NA)
        self.fare_offset = currParams.get("fareOffset", CONST_NA)
        self.first_rbd_alloc = currParams.get("FirstRBDAlloc", CONST_NA)
        self.other_rbd_alloc = currParams.get("OtherRBDAlloc", CONST_NA)
        self.b2b_backstop = currParams.get("B2BBackstop", CONST_NA)
        self.b2c_backstop = currParams.get("B2CBackstop", CONST_NA)
        self.b2b_factor = currParams.get("B2BFactor", CONST_NA)
        self.obSeats = currParams.get("obSeats", CONST_NA)
        self.obFare = currParams.get("obFare", CONST_NA)
        self.skipping_factor = currParams.get("SkippingFactor", CONST_NA)
        self.day_span = currParams.get("DaySpan", CONST_NA)
        self.auto_time_range_flag = parse_int_flag(currParams.get("AutoTimeRangeFlag"), CONST_NA)
        self.analyst_name = currParams.get("analystName", CONST_NA)
        self.opening_fares = currParams.get("openingFares", CONST_NA)
        self.over_booking = currParams.get("OverBooking", CONST_NA)
        self.profileFares = currParams.get("profileFares", CONST_NA)
        self.rbdPushFlag = parse_int_flag(currParams.get("rbdPushFlag"), CONST_NA)
        self.b2c_tolerance = currParams.get("B2CTolerance", CONST_NA)
        self.b2b_tolerance = currParams.get("B2BTolerance", CONST_NA)
        self.auto_tolerance_flag = parse_int_flag(currParams.get("auto_tolerance_flag", currParams.get("autoToleranceFlag", 0)), 0)
        self.distressInventoryFlag = parse_int_flag(currParams.get("distressInventoryFlag"), CONST_NA)
        self.seriesBlock = currParams.get("seriesBlock", CONST_NA)
        self.autoGroup = currParams.get("autoGroup", CONST_NA)
        self.autoBackstopFlag = parse_int_flag(currParams.get("autoBackstopFlag"), CONST_NA)
        self.tbfFlag = parse_int_flag(currParams.get("tbfFlag"), CONST_NA)
        self.w0Flag = parse_int_flag(currParams.get("w0_value"), 0)
        self.directFlag = parse_int_flag(currParams.get("directFlag"), 1)
        self.cug_mkt_flag = parse_int_flag(currParams.get("CugFlag"), CONST_NA)
        self.mkt_fare_flag = parse_int_flag(currParams.get("MktFareFlag"), CONST_NA)
        self.tbfenhflag = parse_int_flag(currParams.get("TBFEnhFlag"), CONST_NA)
        self.outboundstop = parse_int_flag(currParams.get("Outbound_Stop"), CONST_NA)
        self.market_discount = parse_int_flag(currParams.get("Marketfare_Upsell"), CONST_NA)
        
        self.saleflag = parse_int_flag(currParams.get("saleflag"), CONST_NA)
        
        self.DepartureTime = currParams.get("DepartureTime",CONST_NA)
        self.bookedPax = currParams.get("bookedPax",CONST_NA)
        self.capacity = currParams.get("capacity",CONST_NA)
        self.adjustedCapacity = currParams.get("adjustedCapacity",CONST_NA)
        self.tbf_bookings = currParams.get("tbf_bookings",CONST_NA)

        self.seats_sold_L = parse_numeric(currParams.get("seats_sold_L", 0))
        self.seats_sold_P = parse_numeric(currParams.get("seats_sold_P", 0))
        self.current_rbd_L = parse_numeric(currParams.get("current_rbd_L", 0))
        self.current_rbd_P = parse_numeric(currParams.get("current_rbd_P", 0))
        self.total_series_seats = currParams.get("total_series_seats",CONST_NA)
        self.NextFlight = currParams.get("NextFlight",CONST_NA)
        self.BookingsInHour = currParams.get("BookingsInHour",CONST_NA)
        self.BookingsCount = currParams.get("BookingsCount",CONST_NA)
        self.BookingToday = currParams.get("BookingToday",CONST_NA)
        self.WA_bookings = currParams.get("WA_bookings",CONST_NA)
        self.WB_bookings = currParams.get("WB_bookings",CONST_NA)
        self.WC_bookings = currParams.get("WC_bookings",CONST_NA)
        self.WD_bookings = currParams.get("WD_bookings",CONST_NA)
        self.WE_bookings = currParams.get("WE_bookings",CONST_NA)	
        self.cug_flag = parse_int_flag(currParams.get("cug_alloc"), 1)	
        self.group_bookings = currParams.get("group_bookings",CONST_NA)
        
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
        self.strategy = CONST_NA
        self.mongodb = parse_int_flag(currParams.get("mongodb"), 0)
        self.mongodbclient=currParams.get("mongodbclient",CONST_NA)
        self.mongo_master_db=currParams.get("mongo_master_db",CONST_NA)
        
        self.forecast_run_plf = currParams.get("forecast_run_plf", CONST_NA)
        
        self.saleinvflag = parse_int_flag(currParams.get("saleinvflag"), 0)
        self.defaultMlFlag = parse_int_flag(currParams.get("defaultMlFlag"), 0)
        self.forecastPLF = currParams.get("forecastPLF",CONST_NA)
        self.ForecastAF_Allin = currParams.get("ForecastAF_Allin", 0)
        self.ALLIN_AF = currParams.get("ALLIN_AF", 0)
        self.ALLIN_AF_NB_LM = currParams.get("ALLIN_AF_NB_LM", 0)
        self.ALLIN_AF_NB_LY = currParams.get("ALLIN_AF_NB_LY", 0)
        self.forecast_variance = currParams.get("forecast_variance",CONST_NA)
        self.Adjustment_Factor = currParams.get("Adjustment_Factor", CONST_NA)
        self.Expected_PLF = currParams.get("Expected_PLF", CONST_NA)
        self.Flown_Target_Load = currParams.get("Flown_Target_Load", CONST_NA)
        self.forecast_model = currParams.get("forecast_model", CONST_NA)
        self.b2bStrategy = currParams.get("b2bStrategy",CONST_NA)
        self.forecastAllocFlag = parse_int_flag(currParams.get("forecastAllocFlag"), 0)
        self.afForecastAllocFlag = parse_int_flag(currParams.get("afForecastAllocFlag"), 0)
        self.Spoilage_Flag = parse_int_flag(currParams.get("Spoilage_Flag"), 0)
        self.CanAdjustedForecast = parse_int_flag(currParams.get("CanAdjustedForecast"), 0)
        self.adjustedPLF = currParams.get("adjustedPLF", CONST_NA)
        self.market_station_mapping = parse_int_flag(currParams.get("market_station_mapping"), 0)
        self.farecap = currParams.get("farecap", CONST_NA)
        self.region = currParams.get("region", "Domestic")

        # Portfolio count
        self.portfolio_count = currParams.get("portfolio_count", CONST_NA)

        # Portfolio-only flag — set to 1 for sibling records used only for portfolio calc and not allocated
        self.portfolio_only = parse_int_flag(currParams.get("portfolio_only"), 0)

        # Flag: when 1, use increment-factor-adjusted profile fares (from group_incremented_profile_fares)
        # for normal B2C allocations instead of config_profile_fares
        self.incrementedProfileFareFlag = parse_int_flag(currParams.get("incrementedProfileFareFlag"), 0)

        # Profile fare upsell percentage — applied when incrementedProfileFareFlag is 1
        # (replaces the standard station upsell with a market-list-level override)
        self.profileFareUpsell = currParams.get("ProfileFareUpsell", CONST_NA)

        # --- NextGen AU Optimization & Competition Response Inputs (BRD-AU_Opt_Competition_Response) ---
        # Price floor and price cap reuse existing B2CBackstop and hardAnchor
        self.price_floor = self.b2c_backstop if self.b2c_backstop != CONST_NA and self.b2c_backstop is not None else currParams.get("price_floor", currParams.get("priceFloor", CONST_NA))
        self.price_cap = self.hardanchor if self.hardanchor != CONST_NA and self.hardanchor is not None else currParams.get("price_cap", currParams.get("priceCap", CONST_NA))
        self.carrier_exclusion = self.carr_exclusion_b2c if self.carr_exclusion_b2c != CONST_NA and self.carr_exclusion_b2c is not None else currParams.get("carrier_exclusion", currParams.get("carrierExclusion", currParams.get("CarrierExclusion", [])))
        self.time_range = parse_numeric(currParams.get("time_range", currParams.get("timeRange", currParams.get("TimeRange", 2.0))))

        # Renamed NextGen fields with backward-compatible aliases
        raw_buy_up = parse_numeric(currParams.get("buy_up_factor_NG", currParams.get("buy_up_factor", currParams.get("buyUpFactor", currParams.get("BuyUpFactor", 0.0)))))
        self.buy_up_factor_NG = (raw_buy_up / 100.0) if raw_buy_up > 1.0 else raw_buy_up
        self.buy_up_factor = self.buy_up_factor_NG
        self.connection_fare_flag_NG = parse_int_flag(currParams.get("connection_fare_flag_NG", currParams.get("connection_fare_flag", currParams.get("connectionFareFlag", currParams.get("ConnectionFareFlag", 0)))), 0)
        self.connection_fare_flag = self.connection_fare_flag_NG
        self.additional_connection_duration_NG = parse_numeric(currParams.get("additional_connection_duration_NG", currParams.get("additional_connection_duration", currParams.get("additionalConnectionDuration", currParams.get("AdditionalConnectionDuration", 4.0)))))
        self.additional_connection_duration = self.additional_connection_duration_NG
        self.CompetitionResponseGrid_NG = str(currParams.get("CompetitionResponseGrid_NG", currParams.get("grid_name", currParams.get("gridName", currParams.get("GridName", "DEFAULT"))))).strip() or "DEFAULT"
        self.grid_name = self.CompetitionResponseGrid_NG
        self.nextgen_flag_NG = parse_int_flag(currParams.get("nextgen_flag_NG", currParams.get("nextgen_flag", currParams.get("nextGenFlag", currParams.get("NextGenFlag", 0)))), 0)
        self.nextgen_flag = self.nextgen_flag_NG
        self.competition_response_flag_NG = parse_int_flag(currParams.get("competition_response_flag_NG", currParams.get("competition_response_flag", currParams.get("competitionResponseFlag", currParams.get("CompetitionResponseFlag", 1)))), 1)
        self.competition_response_flag = self.competition_response_flag_NG
        # NextGen Promo Inventory Flag (only PromoFlag_NG comes from Market List; others come from constants)
        self.promo_flag_NG = parse_int_flag(currParams.get("PromoFlag_NG", currParams.get("promo_flag_NG", currParams.get("promoFlag_NG", DEFAULT_PROMO_FLAG_NG))), DEFAULT_PROMO_FLAG_NG)
        self.promo_flag = self.promo_flag_NG
