from Constants import *


class OutputSummaryNextGen:
    """Output Summary row representing NextGen AU Optimization and Competition Response results."""

    def __init__(self):
        self.Id = CONST_NA
        self.Origin = CONST_NA
        self.Destin = CONST_NA
        self.FltNum = CONST_NA
        self.DepDate = CONST_NA
        self.SnapshotDate = CONST_NA
        self.Capacity = CONST_NA

        # NextGen Metrics from BRD
        self.NG_Historical_current_booked = CONST_NA  # BookedLoad
        self.NG_LF_Forecast = CONST_NA
        self.NG_AF_Forecast = CONST_NA
        self.NG_Opening_fare = CONST_NA
        self.NG_FirstRBD = CONST_NA
        self.NG_Strategy_fare = CONST_NA
        self.NG_Anchor_fare = CONST_NA

        # Revenue & Demand Metrics
        self.Current_Revenue = CONST_NA
        self.Total_Expected_Revenue = CONST_NA
        self.Predicted_Revenue = CONST_NA
        self.Predicted_Pickup_Revised = CONST_NA
        self.Predicted_Flown_Revised = CONST_NA

        # Market Fare & Grid details
        self.MarketFare_Selected = CONST_NA
        self.MarketFare_Min = CONST_NA
        self.MarketFare_Max = CONST_NA
        # Which competitor carrier / flight produced the selected (minimum) market fare,
        # and how many candidate fares the time-window / outlier filters removed. These
        # make SG-exclusion and "market fare too high" issues debuggable from the summary.
        self.MarketFare_Selected_Carrier = CONST_NA
        self.MarketFare_Selected_Flight = CONST_NA
        self.MarketFare_Candidates_Raw = CONST_NA
        self.MarketFare_Window_Dropped = CONST_NA
        self.MarketFare_Outlier_Dropped = CONST_NA
        self.Grid_Name = CONST_NA
        self.Rule_Applied = CONST_NA

        # Flags & Config
        self.nextgen_flag = 0
        self.competition_response_flag = 1
        self.buy_up_factor = 0.0
        self.RunId = CONST_NA
        self.analystName = CONST_NA
        self.status = CONST_NA
        self.allocationStatus = CONST_NA

        # Currency conversion applied to the weighted-file-derived (INR) fares.
        # ConversionRate is the INR -> home-currency multiplier (1.0 when no
        # conversion); Currency is the resolved home-currency code ('INR' default).
        self.ConversionRate = 1.0
        self.Currency = 'INR'

    def to_dict(self):
        return {col: getattr(self, col, CONST_NA) for col in CONST_OUTPUT_SUMMARY_NEXTGEN_ORDER}
