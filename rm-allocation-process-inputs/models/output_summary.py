from Constants import *

class OutputSummary:
    def __init__(self):
        self.Id = CONST_NA
        self.Origin = CONST_NA
        self.Destin = CONST_NA
        self.FltNum = CONST_NA
        self.DepDate = CONST_NA
        self.BookedLoad = CONST_NA
        self.analystName = CONST_NA
        self.TgtLoad = CONST_NA
        self.bookedPlf = CONST_NA
        self.actualbookedPlf = CONST_NA
        self.Variance = CONST_NA
        self.endOfDayTgtLoad = CONST_NA
        self.currentLoadFactor = CONST_NA
        self.currentTgtLoad = CONST_NA
        self.bookingToday = CONST_NA
        self.bookingLastHour = CONST_NA
        self.hardAnchorFare = CONST_NA
        self.strategyFare = CONST_NA
        self.lastSellingFare = CONST_NA
        self.profileFare = CONST_NA
        self.lastSevenAvgFare = CONST_NA
        self.upsell = CONST_NA
        self.anchorFareBeforeTolerance = CONST_NA
        self.anchorFare = CONST_NA
        self.OpenRBD = CONST_NA
        self.SellingFare = CONST_NA
        self.Channel = CONST_NA
        self.HowDetermined = CONST_NA
        self.RunId = CONST_NA
        self.MktFare_Min = CONST_NA
        self.AirlineCode_Min = CONST_NA
        self.FlightNumber_Min = CONST_NA
        self.MktFare_Max = CONST_NA
        self.AirlineCode_Max = CONST_NA
        self.FlightNumber_Max = CONST_NA
        self.criteria = CONST_NA
        self.fareOffset = CONST_NA
        self.TimeRange = CONST_NA
        self.startTime = CONST_NA
        self.endTime = CONST_NA
        self.Backstop = CONST_NA
        self.autoBackstop = CONST_NA
        self.StrategyReference = CONST_NA
        self.dlfBand = CONST_NA
        self.endOfDaydlfBand = CONST_NA        
        self.ndoBand = CONST_NA
        self.overBookingCount = CONST_NA
        self.recommendedActions = CONST_NA
        self.allocationStatus = CONST_NA
        self.status = CONST_NA
        self.w0Flag = CONST_NA
        self.mktFile=CONST_NA
        self.CurveId = CONST_NA
        self.ExpectedNoShows = CONST_NA
        
        self.forecastPLF=CONST_NA
        self.ImputedForecastPLF=CONST_NA
        self.ForecastAF_Allin=CONST_NA
        self.AF_Allin=CONST_NA
        self.AF_NB_Allin=CONST_NA
        self.ALLIN_AF_NB_LM=CONST_NA
        self.ALLIN_AF_NB_LY=CONST_NA
        self.FirstRBDAlloc=CONST_NA
        self.OtherRBDAlloc=CONST_NA
        self.B2BFactor=CONST_NA
        self.toleranceValue=CONST_NA
        self.T_prime=CONST_NA
        self.grpAdjProfileFare=CONST_NA
        self.grpAdjPLF=CONST_NA
        self.grpFareAnchor=CONST_NA

        # Portfolio anchor split (R9) — populated for direct-route rows by
        # ``DynAlloc._phaseN_writeSummary``; left as ``CONST_NA`` for
        # connections / sales-inventory rows.
        self.z_score_composite = CONST_NA
        self.Portfolio_Anchor = CONST_NA
        self.z_score88 = CONST_NA
        self.Portfolio88_Anchor = CONST_NA
        self.portfolio_count = CONST_NA
        
        self.conversionRate = CONST_NA
        self.currency = CONST_NA
        self.region = DOMESTIC_VALUE
        self.DepDateObj = CONST_NA