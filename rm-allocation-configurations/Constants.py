import sys
import os
import json

DYNAMIC_D3_D4 = -99
FORECAST_ALLOCATION = -1
# LINEAR_ALLOCATION = -2
# LINEAR_ALLOCATION_WITH_JUMP = -3
PROFILE_FARES_QUARTILE = 20
PROFILE_FARES_25PERCENT_QUARTILE = 5
TBF_SEATS=20
GROUP_QUOTATION =  '{ "Q2": "GJ", "Q4": "GI", "Q6": "GH", "Q8": "GG", "Q10": "GF", "Q12": "GE", "Q14": "GD", "Q16": "GC", "Q18": "GB", "Q20": "GA" }'
B2B_DISCOUNT_MAP = '{ "0": "450", "1": "350", "2": "250", "3": "150" }'

MARKET_FARES_THRESHOLD = '[ {"startNdo": 0, "endNdo": 7, "value": 2}, {"startNdo": 8, "endNdo": 14, "value": 8}, {"startNdo": 15, "endNdo": 21, "value": 48}, {"startNdo": 22, "endNdo": 90, "value": 48}, {"startNdo": 91, "endNdo": 365, "value": 48} ]'
SOFTBLOCK_RBDS = '["G8","W9","W8","W7","W6","W5","W4","W3", "W2", "W1", "S"]'
CONNECTION_CARRIER_INCLUSION = '["6E","AI","IX","XY","FZ","GF","J9","KU","FD","SG","TG","WY","VZ","SV","F3"]'

NAVODS_ALERT_SECTOR_LIST = '["BOMDEL1127", "BOMDEL1120", "DELBOM1719", "DELBOM1411", "BOMDBR1529", "DBRBOM1530", "CCUBOM1125", "CCUBOM1173"]'
NAVODS_ALERT_MAX_NDO = 3

DEFAULT_MARKET_LIST = '{"Origin": "ABC","Destin": "XYZ","PerStart": "mm/dd/yyyy","PerEnd": "mm/dd/yyyy","FlightNumber": "1234","analystName": "System","DOW": "9999999","PerType": "D","TimeWindowStart": "17:00","TimeWindowEnd": "20:00","CurveID": "BOMDEL_C4","CarrExlusionB2C": "**","CarrExlusionB2B": "**","flightExclusionB2C": "","flightExclusionB2B": "","fareAnchor": "METRO_STG_MED1","hardAnchor": "","plfThreshold": "","fareOffset": "","FirstRBDAlloc": "-99","OtherRBDAlloc": "10","SkippingFactor": "5","B2BTolerance": "","B2CTolerance": "","B2BBackstop": "4300","B2CBackstop": "4500","B2BFactor": "0","obSeats": "","obFare": "","DaySpan": "0","AutoTimeRangeFlag": "0","openingFares": "1","OverBooking": "1","profileFares": "1","rbdPushFlag": "0","distressInventoryFlag": "0","seriesBlock": "0","autoGroup": "0","autoBackstopFlag": "1","tbfFlag": "1","CugFlag": "0","MktFareFlag": "1","directFlag": "1","TBFEnhFlag": "0", "UUID": "uuid", "defaultMlFlag": "1", "incrementedProfileFareFlag": "0", "ProfileFareUpsell": "0", "buy_up_factor_NG": "0.0", "connection_fare_flag_NG": "0", "additional_connection_duration_NG": "4.0", "CompetitionResponseGrid_NG": "DEFAULT", "nextgen_flag_NG": "0", "competition_response_flag_NG": "1", "PromoFlag_NG": "0"}'
DEFAULT_MARKET_LIST_DAYS = 30

TBF_RBD = "TF,TE,TD,TC,TB,TY"
W_RBD = "W4,W5,W6"
CUG_RBD = "WA,WB,WC,WD,WE"
CUG_B2C_OPENING_THRESHOLD=0.3
CUG_SEATS_PERCENT = 0.25
MIN_D3_D4_VALUE = 2
LINEAR_JUMP_VALUE = 5
PLF_CURVE_VALUE = 0.80
B2B_FARE_PRICE_COMPARISON = 0.9
COVERAGE_PARAMETER = 3
OWN_FARE_DELTA_B = 30
OWN_FARE_DELTA_A = 15
END_OF_DAY_LF_FACTOR = 0.95
PROF_OPEN_COUNT = 10
OVERBOOKING_LF = 95
OVERBOOKING_FARE_LF = 96
OVERBOOKING_FARE_PERCENT_VALUE = 0.1
OVERBOOKING_FARE_ABSOLUTE_VALUE = 1000
OVERBOOKING_START_NDO = 0
OVERBOOKING_END_NDO = 1
OVERBOOKING_PROB = 0.75
OVERBOOKING_MANUAL_LF = 100
OVERBOOKING_MAX_COUNT = 2
OVERBOOKING_CAP = 4
GRACE_PERIOD_MINUTES = 20
WINDOW_MINUTES = 25


S_CLASS_PLF_THRESHOLD = 75
LAST_SOLD_FARE_THRESHOLD = 72
LAST_SOLD_FARE_THRESHOLD_NDO45 = 96
LAST_N_AVG_FARE = 7

ADHOC_BATCH_SIZE = 4000
CHECK_THRESHOLD_FETCH_INPUTS = 5
TIME_BEFORE_DEPARTURE = 30
FORECAST_ALLOC_NDO_MAX = 30
WADD_FORECAST_NDO_MAX = 180
DILUTION_PLF_THRESHOLD = 99
AUTO_BACKSTOP_FORECAST_THRESHOLD = 85

# Portfolio sibling enrichment: max unique departure dates processed per window
PORTFOLIO_SIBLING_BATCH_SIZE = 7

THRESHOLD_TIME = 12
PF_UPSELL_VALUE = '47'

CHECK_THRESHOLD_UPDATE_RBD = 20

BOOKINGS_THRESHOLD = 10

PRICING_DUF_SECTORS = {
    "SAG": "319",
    "TRV": "425",
    "DEL": "66",
    "IXE": "372",
    "BOM": "89",
    "GOX": "425",
    "GAU": "313",
    "JAI": "407"
}
PRICING_RCS_SECTORS = ["GAU","IXA","IXZ","SXR"]
PRICING_GST_SECTORS = ["IXB","GAU","IXA","AJL"]
PRICING_CARR_EXCL = ('9I', 'IC', 'S9', 'S5')
UP_B2C = 5
UP_B2B = 5
DIRECT_FLIGHT_UPSELL_PERCENT = 10
DIRECT_FLIGHT_UPSELL_AMOUNT = 200

RBD_VALUES = "ZJ,ZI,ZH,ZG,ZF,ZE,ZD,ZC,ZB,ZA,Z9,Z8,Z7,Z6,Z5,Z4,Z3,Z2,Z1,Z0,V9,V8,V7,V6,V5,V4,V3,V2,V1,V0,U9,U8,U7,U6,U5,U4,U3,U2,U1,U0,T9,T8,T7,T6,T5,T4,T3,T2,T1,T0,R9,R8,R7,R6,R5,R4,R3,R2,R1,R0,Q9,Q8,Q7,Q6,Q5,Q4,Q3,Q2,Q1,Q0,QA,O0,N0,M0,K0,J0,I0,H0,F0,E0,D0,C0,B0,Y0"
B2C_RBD_VALUES = "V4,V3,V2,V1,V0,U4,U3,U2,U1,U0,T4,T3,T2,T1,T0,R4,R3,R2,R1,R0,Q4,Q3,Q2,Q1,Q0,P4,P3,P2,P1,P0,O4,O3,O2,O1,O,N4,N3,N2,N1,N,M,K,J,I,H,F,E,D,C,B,Y"
QP_B2C_RBD_VALUES = B2C_RBD_VALUES
B2B_RBD_VALUES = "Z9,Z8,Z7,Z6,Z5,Z4,Z3,Z2,Z1,Z0"
GROUP_RBD_VALUES = "GJ,GI,GH,GG,GF,GE,GD,GC,GB,GA"
DISTRESS_RBD_VALUES = "P,L"
B2B_RDB_COUNT = len(B2B_RBD_VALUES.split(','))
B2C_RDB_COUNT = len(B2C_RBD_VALUES.split(','))
AU_COLUMN_LENGTH = B2B_RDB_COUNT + B2C_RDB_COUNT + len(GROUP_RBD_VALUES.split(',')) + len(DISTRESS_RBD_VALUES.split(',')) + len(TBF_RBD.split(',')) + len(CUG_RBD.split(',')) + len(json.loads(SOFTBLOCK_RBDS))
AU_COLUMN_LENGTH_CONNECTIONS = B2B_RDB_COUNT + B2C_RDB_COUNT 

MARKET_FLAG_DOMESTIC = "True"
MARKET_FLAG_INTERNATIONAL = "True"
B2B_THRESHOLD = 100
B2B_THRESHOLD_INTERNATIONAL = 100
INTERNATIONAL_BOOKLOAD_THRESHOLDS = '{"0-3": 80, "4-7": 60, "8-14": 50, "15-365": 40}'
YIELD_FACTOR = 1.4
MARKET_FARE_MIN_YIELD = 1.5
B2C_DISTINCT_FARES_COUNT = 150
MARKET_FARE_FORECAST_THRESHOLD = 88
MARKET_FARE_MAX_THRESHOLD_DOMESTIC_B2B = 11000
MARKET_FARE_MAX_THRESHOLD_INTERNATIONAL_B2B = 25000
MARKET_FARE_MAX_THRESHOLD_DOMESTIC_B2C = 22000
MARKET_FARE_MAX_THRESHOLD_INTERNATIONAL_B2C = 65000
MARKET_FARE_MIN_THRESHOLD_DOMESTIC_B2B = 3000
MARKET_FARE_MIN_THRESHOLD_INTERNATIONAL_B2B = 12000
MARKET_FARE_MIN_THRESHOLD_DOMESTIC_B2C = 3000
MARKET_FARE_MIN_THRESHOLD_INTERNATIONAL_B2C = 12000
MARKET_FARE_MAX_THRESHOLD_CONNECTION_B2B = 25000
MARKET_FARE_MAX_THRESHOLD_CONNECTION_B2C = 65000
MARKET_FARE_MIN_THRESHOLD_CONNECTION_B2B = 12000
MARKET_FARE_MIN_THRESHOLD_CONNECTION_B2C = 12000
BOOKINGS_FARE_MAX_THRESHOLD_DOMESTIC_B2B = 11000
BOOKINGS_FARE_MAX_THRESHOLD_INTERNATIONAL_B2B = 25000
BOOKINGS_FARE_MAX_THRESHOLD_DOMESTIC_B2C = 22000
BOOKINGS_FARE_MAX_THRESHOLD_INTERNATIONAL_B2C = 65000
FARECAP_DISTANCE_CONFIG = '{"0-500": 7500, "500-1000": 12000, "1000-1500": 15000, "1500+": 20000}'
YQ_DISTANCE_CONFIG = '{"0-500": 199, "500-1000": 299, "1000-1500": 449, "1500+": 549}'
CURRENCY_ROUND_OFF_LIST = {"SAR":"2","QAR":"2","KWD":"3"}
CWT_UPSELL = {
    '5001': {'type': 'PERCENTAGE', 'value': 5, 'min_value': None, 'max_value': None},
    '5002': {'type': 'PERCENTAGE', 'value': 5, 'min_value': None, 'max_value': None},
    '5006': {'type': 'PERCENTAGE', 'value': 5, 'min_value': None, 'max_value': None},
}
GENERATE_CWT_RULES = "True"

W_PLF = 0.7
W_AF = 0.3
W_ANCHOR = -0.25

MOVEMENT_GAIN = 6
MOVEMENT_FLOOR_PCT = -12
MOVEMENT_CEIL_PCT = 0

MIN_D3_D4_VALUE_KEY = 'MIN_D3_D4_VALUE'
LINEAR_JUMP_VALUE_KEY = 'LINEAR_JUMP_VALUE'
PLF_CURVE_VALUE_KEY = 'PLF_CURVE_VALUE'
B2B_FARE_PRICE_COMPARISON_KEY = 'B2B_FARE_PRICE_COMPARISON'
COVERAGE_PARAMETER_KEY = 'COVERAGE_PARAMETER'
AU_COLUMN_LENGTH_KEY = 'AU_COLUMN_LENGTH'
B2B_RDB_COUNT_KEY = 'B2B_RDB_COUNT'
LOWEST_B2B_RBD_VALUE_KEY = 'LOWEST_B2B_RBD_VALUE'
LOWEST_B2C_RBD_VALUE_KEY = 'LOWEST_B2C_RBD_VALUE'
HIGHEST_B2B_RBD_VALUE_KEY = 'HIGHEST_B2B_RBD_VALUE'
HIGHEST_B2C_RBD_VALUE_KEY = 'HIGHEST_B2C_RBD_VALUE'
OWN_FARE_DELTA_A_KEY = 'OWN_FARE_DELTA_A'
OWN_FARE_DELTA_B_KEY = 'OWN_FARE_DELTA_B'
OVERBOOKING_LF_KEY = 'OVERBOOKING_LF'
END_OF_DAY_LF_FACTOR_KEY = 'END_OF_DAY_LF_FACTOR'
OVERBOOKING_START_NDO_KEY = 'OVERBOOKING_START_NDO'
CUG_RBD_KEY = 'CUG_RBD'
CUG_B2C_OPENING_THRESHOLD_KEY = 'CUG_B2C_OPENING_THRESHOLD'
CUG_SEATS_PERCENT_KEY = 'CUG_SEATS_PERCENT'
OVERBOOKING_END_NDO_KEY = 'OVERBOOKING_END_NDO'
OVERBOOKING_PROB_KEY = 'OVERBOOKING_PROB'
S_CLASS_PLF_THRESHOLD_KEY = 'S_CLASS_PLF_THRESHOLD'
AU_COLUMN_LENGTH_CONNECTIONS_KEY='AU_COLUMN_LENGTH_CONNECTIONS'
OVERBOOKING_FARE_PERCENT_VALUE_KEY='OVERBOOKING_FARE_PERCENT_VALUE'
OVERBOOKING_FARE_ABSOLUTE_VALUE_KEY = 'OVERBOOKING_FARE_ABSOLUTE_VALUE'
OVERBOOKING_FARE_LF_KEY = 'OVERBOOKING_FARE_LF'
GROUP_QUOTATION_KEY = 'GROUP_QUOTATION'
TBF_RBD_KEY = 'TBF_RBD'
W_RBD_KEY = 'W_RBD'
LAST_SOLD_FARE_THRESHOLD_KEY = 'LAST_SOLD_FARE_THRESHOLD'
LAST_SOLD_FARE_THRESHOLD_NDO45_KEY = 'LAST_SOLD_FARE_THRESHOLD_NDO45'
LAST_N_AVG_FARE_KEY = 'LAST_N_AVG_FARE'
GROUP_DIFF_FARE_KEY = 'GROUP_DIFF_FARE'
TBF_DIFF_FARE_KEY = 'TBF_DIFF_FARE'
B2B_DISCOUNT_MAP_KEY = 'B2B_DISCOUNT_MAP'
PROF_OPEN_COUNT_KEY = 'PROF_OPEN_COUNT'
ADHOC_BATCH_SIZE_KEY = 'ADHOC_BATCH_SIZE'
CHECK_THRESHOLD_FETCH_INPUTS_KEY = 'CHECK_THRESHOLD_FETCH_INPUTS'
TIME_BEFORE_DEPARTURE_KEY = 'TIME_BEFORE_DEPARTURE'
PREFETCH_CHUNK_SIZE_KEY = 'PREFETCH_CHUNK_SIZE'
PORTFOLIO_SIBLING_BATCH_SIZE_KEY = 'PORTFOLIO_SIBLING_BATCH_SIZE'
FETCH_INPUTS_TRIGGER_QUEUE_URL_KEY = 'FETCH_INPUTS_TRIGGER_QUEUE_URL'
MFMIN0_KEY = 'MFMIN0'
MFMIN1_KEY = 'MFMIN1'
TBF_SEATS_KEY = 'TBF_SEATS'
CHECK_THRESHOLD_PROCESS_INPUTS_KEY = 'CHECK_THRESHOLD_PROCESS_INPUTS'
OVERBOOKING_MANUAL_LF_KEY = 'OVERBOOKING_MANUAL_LF'
OVERBOOKING_MAX_COUNT_KEY = 'OVERBOOKING_MAX_COUNT'
OVERBOOKING_CAP_KEY = 'OVERBOOKING_CAP'
CUG_DISCOUNT_KEY = 'CUG_DISCOUNT'
SPOILAGE_REDUCTION_PLF_THRESHOLD_KEY = 'SPOILAGE_REDUCTION_PLF_THRESHOLD'
SPOILAGE_REDUCTION_TRIGGER_BANDS_KEY = 'SPOILAGE_REDUCTION_TRIGGER_BANDS'
SPOILAGE_REDUCTION_DEFAULT_TRIGGER_HOURS_KEY = 'SPOILAGE_REDUCTION_DEFAULT_TRIGGER_HOURS'
IMP_FORECAST_DEDUCTION_ML_KEY = 'IMP_FORECAST_DEDUCTION_ML'
IMP_FORECAST_DEDUCTION_LB_KEY = 'IMP_FORECAST_DEDUCTION_LB'
IMP_FORECAST_DEDUCTION_KEY = 'IMP_FORECAST_DEDUCTION'
TOLERANCE_MAP_KEY = 'TOLERANCE_MAP'
B2B_FACTOR_MAP_KEY = 'B2B_FACTOR_MAP'
B2B_DIRECT_OUTPUT_SUMMARY_FLAG_KEY = 'B2B_DIRECT_OUTPUT_SUMMARY_FLAG'
AF_PLF_FORECAST_CAP_KEY = 'AF_PLF_FORECAST_CAP'
AF_PLF_GAIN_MAX_ZERO_KEY = 'AF_PLF_GAIN_MAX_ZERO'
AF_CONSTANT_BAND_2_KEY = 'AF_CONSTANT_BAND_2'
AF_CONSTANT_BAND_3_KEY = 'AF_CONSTANT_BAND_3'
AF_PROFILE_FARE_PLF_THRESHOLD_KEY = 'AF_PROFILE_FARE_PLF_THRESHOLD'
AF_PROFILE_FARE_MULTIPLIER_KEY = 'AF_PROFILE_FARE_MULTIPLIER'
AF_BAND_1_MULTIPLIERS_KEY = 'AF_BAND_1_MULTIPLIERS'
AF_BAND_2_MULTIPLIERS_KEY = 'AF_BAND_2_MULTIPLIERS'
DEFAULT_D3_VALUE_KEY = 'DEFAULT_D3_VALUE'
D3_MIN_RULES_KEY = 'D3_MIN_RULES'
D3_MAX_RULES_KEY = 'D3_MAX_RULES'
RM_ALERT_MAIL_ID_KEY = 'RM_ALERT_MAIL_ID'
CLOSED_RM_ALERT_MAIL_ID_KEY = 'CLOSED_RM_ALERT_MAIL_ID'
TARIFF_ALERT_MAIL_ID_KEY = 'TARIFF_ALERT_MAIL_ID'
DEFAULT_ML_ALERT_MAIL_ID_KEY = 'DEFAULT_ML_ALERT_MAIL_ID'

MARKET_FARES_THRESHOLD_KEY = 'MARKET_FARES_THRESHOLD'
FORECAST_ALLOC_NDO_MAX_KEY = 'FORECAST_ALLOC_NDO_MAX'
WADD_FORECAST_NDO_MAX_KEY = 'WADD_FORECAST_NDO_MAX'

THRESHOLD_TIME_KEY = "THRESHOLD_TIME"
PF_UPSELL_VALUE_KEY = "PF_UPSELL_VALUE"

CHECK_THRESHOLD_UPDATE_RBD_KEY = "CHECK_THRESHOLD_UPDATE_RBD"

BOOKINGS_THRESHOLD_KEY = "BOOKINGS_THRESHOLD"

SOFTBLOCK_RBDS_KEY = 'SOFTBLOCK_RBDS'
CONNECTION_CARRIER_INCLUSION_KEY = 'CONNECTION_CARRIER_INCLUSION'

NAVODS_ALERT_SECTOR_LIST_KEY = 'NAVODS_ALERT_SECTOR_LIST'
NAVODS_ALERT_MAX_NDO_KEY = 'NAVODS_ALERT_MAX_NDO'

TBF_ORG_CODE_KEY = "TBF_ORG_CODE"
TBF_OPEN_SEATS_KEY = "TBF_OPEN_SEATS"
DILUTION_PLF_THRESHOLD_KEY = "DILUTION_PLF_THRESHOLD"
AUTO_BACKSTOP_FORECAST_THRESHOLD_KEY = "AUTO_BACKSTOP_FORECAST_THRESHOLD"

DEFAULT_MARKET_LIST_KEY = "DEFAULT_MARKET_LIST"
DEFAULT_MARKET_LIST_DAYS_KEY = "DEFAULT_MARKET_LIST_DAYS"

DOMESTIC_PLF_THRESHOLD_KEY = "DOMESTIC_PLF_THRESHOLD"
INTERNATIONAL_PLF_THRESHOLD_KEY = "INTERNATIONAL_PLF_THRESHOLD"

PRICING_DUF_SECTORS_KEY = "PRICING_DUF_SECTORS"
PRICING_RCS_SECTORS_KEY = "PRICING_RCS_SECTORS"
PRICING_CARR_EXCL_KEY = 'PRICING_CARR_EXCL'
PRICING_GST_SECTORS_KEY = 'PRICING_GST_SECTORS'
UP_B2C_KEY = "UP_B2C"
UP_B2B_KEY = "UP_B2B"
DIRECT_FLIGHT_UPSELL_PERCENT_KEY = "DIRECT_FLIGHT_UPSELL_PERCENT"
DIRECT_FLIGHT_UPSELL_AMOUNT_KEY = "DIRECT_FLIGHT_UPSELL_AMOUNT"
MARKET_FLAG_DOMESTIC_KEY = "MARKET_FLAG_DOMESTIC"
MARKET_FLAG_INTERNATIONAL_KEY = "MARKET_FLAG_INTERNATIONAL"
RBD_VALUES_KEY = "RBD_VALUES"
B2B_RBD_VALUES_KEY = "B2B_RBD_VALUES"
B2C_RBD_VALUES_KEY = "B2C_RBD_VALUES"
QP_B2C_RBD_VALUES_KEY = "QP_B2C_RBD_VALUES"
GROUP_RBD_VALUES_KEY = "GROUP_RBD_VALUES"
DISTRESS_RBD_VALUES_KEY = "DISTRESS_RBD_VALUES"
B2B_THRESHOLD_KEY = "B2B_THRESHOLD"
B2B_THRESHOLD_INTERNATIONAL_KEY = "B2B_THRESHOLD_INTERNATIONAL"
INTERNATIONAL_BOOKLOAD_THRESHOLDS_KEY = "INTERNATIONAL_BOOKLOAD_THRESHOLDS"
YIELD_FACTOR_KEY = "YIELD_FACTOR"
MARKET_FARE_MIN_YIELD_KEY = "MARKET_FARE_MIN_YIELD"
B2C_DISTINCT_FARES_COUNT_KEY = "B2C_DISTINCT_FARES_COUNT"
MARKET_FARE_FORECAST_THRESHOLD_KEY = "MARKET_FARE_FORECAST_THRESHOLD"
MARKET_FARE_MAX_THRESHOLD_DOMESTIC_B2B_KEY = "MARKET_FARE_MAX_THRESHOLD_DOMESTIC_B2B"
MARKET_FARE_MAX_THRESHOLD_INTERNATIONAL_B2B_KEY = "MARKET_FARE_MAX_THRESHOLD_INTERNATIONAL_B2B"
MARKET_FARE_MAX_THRESHOLD_DOMESTIC_B2C_KEY = "MARKET_FARE_MAX_THRESHOLD_DOMESTIC_B2C"
MARKET_FARE_MAX_THRESHOLD_INTERNATIONAL_B2C_KEY = "MARKET_FARE_MAX_THRESHOLD_INTERNATIONAL_B2C"
MARKET_FARE_MIN_THRESHOLD_DOMESTIC_B2B_KEY = "MARKET_FARE_MIN_THRESHOLD_DOMESTIC_B2B"
MARKET_FARE_MIN_THRESHOLD_INTERNATIONAL_B2B_KEY = "MARKET_FARE_MIN_THRESHOLD_INTERNATIONAL_B2B"
MARKET_FARE_MIN_THRESHOLD_DOMESTIC_B2C_KEY = "MARKET_FARE_MIN_THRESHOLD_DOMESTIC_B2C"
MARKET_FARE_MIN_THRESHOLD_INTERNATIONAL_B2C_KEY = "MARKET_FARE_MIN_THRESHOLD_INTERNATIONAL_B2C"
MARKET_FARE_MAX_THRESHOLD_CONNECTION_B2B_KEY = "MARKET_FARE_MAX_THRESHOLD_CONNECTION_B2B"
MARKET_FARE_MAX_THRESHOLD_CONNECTION_B2C_KEY = "MARKET_FARE_MAX_THRESHOLD_CONNECTION_B2C"
MARKET_FARE_MIN_THRESHOLD_CONNECTION_B2B_KEY = "MARKET_FARE_MIN_THRESHOLD_CONNECTION_B2B"
MARKET_FARE_MIN_THRESHOLD_CONNECTION_B2C_KEY = "MARKET_FARE_MIN_THRESHOLD_CONNECTION_B2C"
BOOKINGS_FARE_MAX_THRESHOLD_DOMESTIC_B2B_KEY = "BOOKINGS_FARE_MAX_THRESHOLD_DOMESTIC_B2B"
BOOKINGS_FARE_MAX_THRESHOLD_INTERNATIONAL_B2B_KEY = "BOOKINGS_FARE_MAX_THRESHOLD_INTERNATIONAL_B2B"
BOOKINGS_FARE_MAX_THRESHOLD_DOMESTIC_B2C_KEY = "BOOKINGS_FARE_MAX_THRESHOLD_DOMESTIC_B2C"
BOOKINGS_FARE_MAX_THRESHOLD_INTERNATIONAL_B2C_KEY = "BOOKINGS_FARE_MAX_THRESHOLD_INTERNATIONAL_B2C"
FARECAP_DISTANCE_CONFIG_KEY = "FARECAP_DISTANCE_CONFIG"
YQ_DISTANCE_CONFIG_KEY = "YQ_DISTANCE_CONFIG"
CURRENCY_ROUND_OFF_LIST_KEY = "CURRENCY_ROUND_OFF_LIST"
ACTIVE_SECTORS_DEFAULT_FIELDS_KEY = "ACTIVE_SECTORS_DEFAULT_FIELDS"
NAV_CACHE_CONFIG_KEY = "NAV_CACHE_CONFIG"
DEFAULT_ML_CONFIG_KEY = "DEFAULT_ML_CONFIG"
DEFAULT_ML_CALC_CONFIG_KEY = "DEFAULT_ML_CALC_CONFIG"
PLF_CHANGE_PER_KEY = "plf_change_per"
PLF_CHANGE_UPPER_PER_KEY = "plf_change_upper_per"
PLF_CHANGE_LOWER_PER_KEY = "plf_change_lower_per"
PLF_CHANGE_N_RUNS_KEY = "plf_change_n_runs"
LSF_FALLBACK_N_RUNS_KEY = "lsf_fallback_n_runs"
GDS_RBD_VALUES_KEY = "GDS_RBD_VALUES"
GRACE_PERIOD_MINUTES_KEY = "GRACE_PERIOD_MINUTES"
WINDOW_MINUTES_KEY = "WINDOW_MINUTES"
CWT_UPSELL_KEY = "CWT_UPSELL"
GENERATE_CWT_RULES_KEY = "GENERATE_CWT_RULES"
GROUP_SCALING_FACTOR_KEY = "GROUP_SCALING_FACTOR"
GROUP_AU_BATCH_KEY = "GROUP_AU_BATCH"
GROUP_IMP_FORECAST_REDUCTION_KEY = "GROUP_IMP_FORECAST_REDUCTION"
W_PLF_KEY = 'W_PLF'
W_AF_KEY = 'W_AF'
W_ANCHOR_KEY = 'W_ANCHOR'
MOVEMENT_GAIN_KEY = 'MOVEMENT_GAIN'
MOVEMENT_FLOOR_PCT_KEY = 'MOVEMENT_FLOOR_PCT'
MOVEMENT_CEIL_PCT_KEY = 'MOVEMENT_CEIL_PCT'
EMSR_SIGMA_FACTOR_KEY = 'EMSR_SIGMA_FACTOR'
PROMO_FARE_RATIO_KEY = 'PROMO_FARE_RATIO'
PROMO_RBD_KEY = 'PROMO_RBD'
PROMO_FLAG_NG_KEY = 'PromoFlag_NG'
PROMO_RBD_NG_KEY = 'PromoRBD_NG'
PROMO_FIRST_RBD_NG_KEY = 'PromoFirstRBD_NG'
PROMO_RATIO_NG_KEY = 'PromoRatio_NG'
PROMO_FIRST_RBD_KEY = 'PROMO_FIRST_RBD'
OPEN_BUCKET_MIDPOINT_FACTOR_KEY = 'OPEN_BUCKET_MIDPOINT_FACTOR'
BUY_UP_FACTOR_KEY = 'BUY_UP_FACTOR'
DEFAULT_FIRST_RBD_SEATS_KEY = 'DEFAULT_FIRST_RBD_SEATS'
FIRST_RBD_ALLOC_KEY = 'FirstRBDAlloc'
DEFAULT_STANDARD_DEVIATION_FACTOR_KEY = 'DEFAULT_STANDARD_DEVIATION_FACTOR'
DEFAULT_PROMO_FARE_FACTOR_KEY = 'DEFAULT_PROMO_FARE_FACTOR'
DEFAULT_FORECAST_THRESHOLD_PROMO_AU_KEY = 'DEFAULT_FORECAST_THRESHOLD_PROMO_AU'
DEFAULT_COVERAGE_PARAMETER_KEY = 'DEFAULT_COVERAGE_PARAMETER'
DEFAULT_ADDITIONAL_CONNECTION_DURATION_KEY = 'DEFAULT_ADDITIONAL_CONNECTION_DURATION'
DEFAULT_TIME_RANGE_KEY = 'DEFAULT_TIME_RANGE'
DEFAULT_PMIN_THRESHOLD_FACTOR_NG_KEY = 'DEFAULT_PMIN_THRESHOLD_FACTOR_NG'
PMIN_THRESHOLD_FACTOR_NG_KEY = 'pmin_threshold_factor_NG'
ML_FLAG_COLUMNS_KEY = 'ML_FLAG_COLUMNS'
LATEST_VERSION = '4.0.0'

TABLE_MARKET_FARES = "market_fares"

WAIT_TIME = 30  # in seconds

AKASA_AIRLINE_CODE = 'QP'

B2C_CHANNEL = "B2C"
B2B_CHANNEL = "B2B"
TBF_CHANNEL = "TBF"
CUG_CHANNEL = "CUG"
GROUP_CHANNEL = "GROUP"

LOWEST_B2B_RBD_VALUE = 'ZJ'
LOWEST_B2C_RBD_VALUE = 'V9'
HIGHEST_B2B_RBD_VALUE = 'Z0'
HIGHEST_B2C_RBD_VALUE = 'Y'

CONST_ABSOLUTE = 'ABS'
CONST_RELATIVE = 'REL'
CONST_REFERENCE = 'REF'
CONST_NA = 'NA'
DOMESTIC_VALUE = 'Domestic'

DATE_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
DATE_TIME_FORMAT2 = '%m/%d/%Y %H:%M'
DATE_FORMAT = "%m/%d/%Y"
DATE_FORMAT_2 = "%Y-%m-%d"
DATE_FORMAT_3 = '%m-%d-%Y'
TIME_FORMAT = '%H:%M'
WEEK_DAY = "%w"
DAY_OF_WEEK = '%A'
MONTH = '%B'
TIME_10_AM = '10:00'
CONST_START_TIME = "00:00"
CONST_END_TIME = "23:59"

TEST_DATA = "TESTDATA"
DB = "DB"
FILE = "FILE"
S3 = "S3"
SKIP = "SKIP"

CONST_API = "api"
CONST_SFTP = "sftp"
CONST_SKIP = "skip"

MIN_FARE_ANCHOR = "MIN"
MAX_FARE_ANCHOR = "MAX"
MINO_FARE_ANCHOR = "MINO"
MAXO_FARE_ANCHOR = "MAXO"
OWNF_FARE_ANCHOR = "OWNF"
MINC_FARE_ANCHOR = "MINC"
MINOC_FARE_ANCHOR = "MINOC"
MIN2_FARE_ANCHOR = "MIN2"
MIN2C_FARE_ANCHOR = "MIN2C"
MED_FARE_ANCHOR = "MED"
MEDC_FARE_ANCHOR = "MEDC"

YES_FLAG = 'Y'
NO_FLAG = 'N'
FARE_NOT_AVL = -99

CONST_LOGS = "logFiles"
CONST_INPUT_SOURCE = "inputSource"
CONST_OUTPUT = "outputSource"
CONST_CURVES = "Curves"
CONST_MARKET_FARES = "Market_fares"
CONST_MARKET_LIST = "Market_list"
CONST_FARES = "Fares"
CONST_INPUT_STATUS = "inputs_status"
CONST_BOOKLOAD = "bookload"
CONST_OVERBOOKING = "overbooking"
CONST_NDO_BAND = "ndo_bands"
CONST_DPLF_BAND = "dplf_bands"
CONST_DYNAMIC_D1_D2 = "d1_d2_strategies"
CONST_LAST_SELLING_FARE = "lastSellingFare"
CONST_COMP_RESPONSE_GRID = "comp_response_grid"
CONST_COMP_RESPONSE_PLF_BANDS = "comp_response_plf_bands"
CONST_COMP_FARE_RECALIBRATION = "comp_fare_recalibration"

CONST_RUN_SUMMARY = "run_summary"
CONST_RUN_AUDIT = "allocation_run_audit"
CONST_SUMMARY_FILE = "Summary"
CONST_SUMMARY_NEXTGEN_FILE = "Summary_NextGen"
CONST_AU_GRID = "AUGrid"

_ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_TESTCASES_DIR = os.path.join(_ROOT_DIR, "QPDynamicAllocation", "testcases")

CONST_MARKET_LIST_FILE_PATH = os.path.join(_ROOT_DIR, "params", "MarketList.csv")
CONST_MARKET_LIST_CONNECTIONS_FILE_PATH = os.path.join(_ROOT_DIR, "params", "MarketList_Connections.csv")
CONST_AU_GRID_FILE_PATH = os.path.join(_TESTCASES_DIR, "output", "generated_output", "AUGrid.csv")
CONST_SUMMARY_FILE_PATH = os.path.join(_TESTCASES_DIR, "output", "generated_output", "summary.csv")
CONST_NEXTGEN_OUTPUT_DIR = os.path.join(_TESTCASES_DIR, "outputnexgen", "generated_output")
CONST_SUMMARY_NEXTGEN_FILE_PATH = os.path.join(CONST_NEXTGEN_OUTPUT_DIR, "summary_nextgen.csv")
CONST_INVENTORY_OPTIMIZATION_FILE_PATH = os.path.join(CONST_NEXTGEN_OUTPUT_DIR, "inventory_optimization.csv")
CONST_AU_GRID_SALES_FILE_PATH = os.path.join(_TESTCASES_DIR, "output", "generated_output", "AUGrid_sales.csv")
CONST_SUMMARY_SALES_FILE_PATH = os.path.join(_TESTCASES_DIR, "output", "generated_output", "summary_sales.csv")
CONST_AU_GRID_CONNECTIONS_FILE_PATH = os.path.join(_TESTCASES_DIR, "output", "generated_output", "AUGrid_Connections.csv")
CONST_SUMMARY_CONNECTIONS_FILE_PATH = os.path.join(_TESTCASES_DIR, "output", "generated_output", "summary_Connections.csv")

# NextGen AU Optimization & Competition Response Parameters
DEFAULT_FIRST_RBD_SEATS = 20
DEFAULT_STANDARD_DEVIATION_FACTOR = 0.25
DEFAULT_PROMO_FARE_FACTOR = 0.80
DEFAULT_FORECAST_THRESHOLD_PROMO_AU = 95.0
DEFAULT_COVERAGE_PARAMETER = 3.0
DEFAULT_ADDITIONAL_CONNECTION_DURATION = 4.0
DEFAULT_TIME_RANGE = 2.0
DEFAULT_PROMO_FLAG_NG = 0
DEFAULT_PROMO_RBD_NG = "W4"
DEFAULT_PROMO_FIRST_RBD_NG = 10
DEFAULT_PROMO_RATIO_NG = 0.80
DEFAULT_PMIN_THRESHOLD_FACTOR_NG = 0.1

CONST_OUTPUT_SUMMARY_ORDER = [
    'Origin', 'Destin', 'FltNum', 'DepDate', 'BookedLoad', 'analystName', 'Day',
    'bookingLastHour', 'bookedPlf', 'actualbookedPlf', 'currentTgtLoad',
    'TgtLoad', 'endOfDayTgtLoad', 'Variance', 'forecastPLF', 'ImputedForecastPLF',
    'endOfDaydlfBand', 'bookingToday', 'Backstop', 'hardAnchorFare', 'strategyFare',
    'lastSellingFare', 'profileFare', 'lastSevenAvgFare', 'anchorFareBeforeTolerance', 'toleranceValue',
    'anchorFare', 'OpenRBD', 'SellingFare', 'Channel', 'HowDetermined', 'RunId',
    'criteria', 'fareOffset', 'FlightNumber_Min', 'MktFare_Max', 'FlightNumber_Max',
    'TimeRange', 'startTime', 'endTime', 'FirstRBDAlloc', 'OtherRBDAlloc',
    'autoBackstop', 'StrategyReference', 'CurveId', 'dlfBand', 'ndoBand',
    'overBookingCount', 'ExpectedNoShows', 'status', 'w0Flag', 'mktFile',
    'ForecastAF_Allin', 'AF_Allin', 'AF_NB_Allin', 'ALLIN_AF_NB_LM', 'ALLIN_AF_NB_LY',
    'B2BFactor', 'portfolio_count', 'z_score88', 'grpAdjPLF', 'z_score_composite',
    'AirlineCode_Max', 'grpAdjProfileFare', 'allocationStatus', 'Portfolio_Anchor',
    'AirlineCode_Min', 'grpFareAnchor', 'Portfolio88_Anchor', 'upsell', 'currentLoadFactor',
    'NG_LF_Forecast', 'NG_AF_Forecast', 'NG_Historical_current_booked',
    'NG_Opening_fare', 'NG_FirstRBD', 'NG_Strategy_fare', 'NG_Anchor_fare'
]

CONST_OUTPUT_SUMMARY_NEXTGEN_ORDER = [
    'Origin', 'Destin', 'FltNum', 'DepDate', 'Capacity',
    'NG_FirstRBD', 'Predicted_Flown_Revised',
    'MarketFare_Selected', 'MarketFare_Min', 'MarketFare_Max',
    'Grid_Name', 'Rule_Applied', 'nextgen_flag', 'competition_response_flag',
    'buy_up_factor', 'RunId', 'Id',
    'NG_Historical_current_booked', 'NG_LF_Forecast', 'NG_AF_Forecast',
    'NG_Opening_fare', 'NG_Strategy_fare', 'NG_Anchor_fare',
    'analystName', 'status', 'allocationStatus'
]

CONST_OUTPUT_INVENTORY_OPTIMIZATION_ORDER = [
    'sector', 'date', 'DOW', 'STD', 'flight', 'NDO', 'capacity', 'Total_Bookings',
    'fare', 'fare_All_in',
    'oal_b2c_min', 'oal_b2c_min_All_in',
    'anchor_fare', 'anchor_fare_All_in',
    'strategy_fare', 'strategy_fare_All_in',
    'opening fare', 'opening fare_All_in',
    'Promo_Flag',
    'mu_bar_orig', 'mu', 'AU', 'P_eq', 'mu_bar_2', 'mu_2', 'AU_2', 'AU_Revised',
    'predicted_pickup', 'Predicted_Flown', 'Predicted_pickup_Revised', 'Predicted_Flown_Revised',
    'LF_Forecast_Revised', 'AF_Forecast', 'Distance', 'ASKM', 'RPKM', 'RASK_Forecast', 'Current_Revenue',
    'Expected_Revenue', 'Total_Expected_Revenue', 'Predicted_Revenue',
    'SnapshotDate', 'Tagging', 'FareBucket', 'RunId'
]


KEY_B2B_PENDING_COUNT = 'b2b_pending_count_'
KEY_B2C_PENDING_COUNT = 'b2c_pending_count_'
KEY_B2B_READY_COUNT = 'b2b_ready_count_'
KEY_B2C_READY_COUNT = 'b2c_ready_count_'
KEY_B2B_DONE_COUNT = 'b2b_done_count_'
KEY_B2C_DONE_COUNT = 'b2c_done_count_'

# Fare-anchor (F) pass counters — direct-route two-pass split.
# See `.kiro/specs/portfolio-anchor-split/requirements.md` R7.
KEY_B2B_READY_COUNT_F = 'b2b_ready_count_F_'
KEY_B2C_READY_COUNT_F = 'b2c_ready_count_F_'

# Portfolio coordination hash key prefix and TTL (R3).
# Hash key format: f"{PORTFOLIO_KEY_PREFIX}{run_id}_{sector}_{dep_date}".
PORTFOLIO_KEY_PREFIX = 'Portfolio:'
PORTFOLIO_TTL_SEC = 21600  # 6 hours

TESTING_INPUT = {
    CONST_CURVES: TEST_DATA,
    CONST_MARKET_LIST: TEST_DATA,
    CONST_MARKET_FARES: TEST_DATA,
    CONST_FARES: TEST_DATA,
    CONST_INPUT_STATUS: TEST_DATA,
    CONST_BOOKLOAD: TEST_DATA,
    CONST_NDO_BAND: TEST_DATA,
    CONST_DPLF_BAND: TEST_DATA,
    CONST_DYNAMIC_D1_D2: TEST_DATA,
    CONST_LAST_SELLING_FARE: TEST_DATA,
    CONST_OVERBOOKING: TEST_DATA
}
TESTING_LOGS = {
    CONST_RUN_SUMMARY: SKIP,
    CONST_RUN_AUDIT: SKIP
}
TESTING_OUTPUT = {
    CONST_AU_GRID: FILE,
    CONST_SUMMARY_FILE: FILE
}
TESTING_NEXTGEN_OUTPUT = {
    CONST_AU_GRID: FILE,
    CONST_SUMMARY_FILE: FILE
}
SERVER_INPUT = {
    CONST_CURVES: DB,
    CONST_MARKET_LIST: DB,
    CONST_MARKET_FARES: DB,
    CONST_FARES: DB,
    CONST_INPUT_STATUS: DB,
    CONST_BOOKLOAD: DB,
    CONST_NDO_BAND: DB,
    CONST_DPLF_BAND: DB,
    CONST_DYNAMIC_D1_D2: DB,
    CONST_LAST_SELLING_FARE: DB,
    CONST_OVERBOOKING: DB
}
SERVER_LOGS = {
    CONST_RUN_SUMMARY: DB,
    CONST_RUN_AUDIT: DB
}
SERVER_OUTPUT = {
    CONST_AU_GRID: SKIP,
    CONST_SUMMARY_FILE: SKIP
}
LOCAL_INPUT = {
    CONST_CURVES: DB,
    CONST_MARKET_LIST: FILE,
    CONST_MARKET_FARES: DB,
    CONST_FARES: DB,
    CONST_INPUT_STATUS: SKIP,
    CONST_BOOKLOAD: DB,
    CONST_NDO_BAND: DB,
    CONST_DPLF_BAND: DB,
    CONST_DYNAMIC_D1_D2: DB,
    CONST_LAST_SELLING_FARE: DB,
    CONST_OVERBOOKING: DB
}
LOCAL_LOGS = {
    CONST_RUN_SUMMARY: SKIP,
    CONST_RUN_AUDIT: SKIP
}
LOCAL_OUTPUT = {
    CONST_AU_GRID: FILE,
    CONST_SUMMARY_FILE: FILE
}

MFMIN0 = 0
MFMIN1 = 1
CHECK_THRESHOLD_PROCESS_INPUTS = 5
GROUP_DIFF_FARE = 5000
TBF_DIFF_FARE = 2000

HD_NO_REF_CURVE = "No Reference Curve Found"
HD_NO_QP_FARES = "No QP Fares Found"
HD_NO_TBF_FARES = "TBF fares are missing or not found"
HD_NO_REF_CURVE_ID = "unable to get CurevId"
HD_NO_BOOKLOADS = "unable to get Booked loads"
HD_INVALID_CONNECTION = "Connection is invalid"
HD_W0_IS_ONE = "W0 RBD is 1 aborting"


TBF_RBD_QUERY = "'TF','TE','TD','TC','TB','TY'"
TBF_COUNT = 6

CUG_RBD_QUERY = "'WA','WB','WC','WD','WE'"
CUG_COUNT=5

TBF_ORG_CODE = "QPBDQ0903T"
TBF_OPEN_SEATS = 8

DOMESTIC_PLF_THRESHOLD = 95.0
INTERNATIONAL_PLF_THRESHOLD = 80.0
CUG_DISCOUNT = 5

# --- D-0 Spoilage Reduction ---------------------------------------------------
# PLF (%) above which spoilage reduction is skipped to avoid dilution.
SPOILAGE_REDUCTION_PLF_THRESHOLD = 100
# Departure-time bands (inclusive, HH:MM) mapped to the number of hours before
# departure at which the D-0 spoilage reduction becomes active.
# Current policy:
#   Departures 00:01 - 11:00 -> 16 hours before departure
#   Departures 11:01 - 23:59 -> 10 hours before departure
# A band may wrap past midnight by setting startTime > endTime.
SPOILAGE_REDUCTION_TRIGGER_BANDS = '[{"startTime": "00:01", "endTime": "11:00", "triggerHours": 16}, {"startTime": "11:01", "endTime": "23:59", "triggerHours": 10}]'
# Used when a departure time falls outside every configured band (e.g. 00:00).
SPOILAGE_REDUCTION_DEFAULT_TRIGGER_HOURS = 12

IMP_FORECAST_DEDUCTION_LB = 0
IMP_FORECAST_DEDUCTION_ML = 0
IMP_FORECAST_DEDUCTION = 0
TOLERANCE_MAP = '{"4": -1, "3": 10, "2": 15, "1": 25, "0": 25}'
B2B_FACTOR_MAP = '{"4": 0, "3": 0, "2": 0.5, "1": 1, "0": 1}'
# Controls whether B2B output-summary records are inserted for direct flights,
# keyed by region. When the region's value is false, B2B summary rows for
# direct flights are skipped/blocked.
B2B_DIRECT_OUTPUT_SUMMARY_FLAG = '{"Domestic": true, "International": true}'

AF_PLF_FORECAST_CAP = 105
AF_PLF_GAIN_MAX_ZERO = 60
AF_CONSTANT_BAND_2 = 2
AF_CONSTANT_BAND_3 = 5
AF_PROFILE_FARE_PLF_THRESHOLD = 97
AF_PROFILE_FARE_MULTIPLIER = 1.2
AF_BAND_1_MULTIPLIERS = '[1.1, 1.2]'
AF_BAND_2_MULTIPLIERS = '[1.3, 1.4]'

DEFAULT_D3_VALUE = 4
D3_MIN_RULES = '{"L": {"forecast_start":-9999, "forecast_end":69, "min_value": 5}, "M": {"forecast_start":70, "forecast_end":94, "min_value": 4}, "H": {"forecast_start":95, "forecast_end":9999, "min_value": 3}}'
D3_MAX_RULES = '{"L": {"forecast_start":-9999, "forecast_end":84, "max_value": 15}, "M": {"forecast_start":85, "forecast_end":95, "max_value": 10}, "H1": {"forecast_start":96, "forecast_end":9999, "ndo_start":0, "ndo_end":3, "max_value": 7}, "H2": {"forecast_start":96, "forecast_end":9999, "ndo_start":4, "ndo_end":9999, "max_value": 7}}'

RM_ALERT_MAIL_ID = 'rm.italerts@akasaair.com'
CLOSED_RM_ALERT_MAIL_ID = 'closed.rm.italerts@akasaair.com'
TARIFF_ALERT_MAIL_ID = 'pa.it.alerts@akasaair.com'
DEFAULT_ML_ALERT_MAIL_ID = '{"to": ["closed.rm.italerts@akasaair.com"], "cc": []}'

GDS_MAPPING = {
    'Y': 'YT', 'B': 'BT', 'C': 'CT', 'D': 'DT', 'E': 'ET', 'F': 'FT',
    'J': 'HT', 'N': 'IT', 'Q3': 'JT', 'Q7': 'KT', 'R2': 'MT', 'R7': 'NT',
    'T3': 'OT', 'T6': 'QT', 'U2': 'RT', 'U7': 'TT', 'V2': 'UT', 'V7': 'VT'
}

ACTIVE_SECTORS_DEFAULT_FIELDS = '{"forecast_model": "LB", "farecap": "30000"}'

NAV_CACHE_CONFIG = {"BATCH_DAYS": 30, "MONGO_BATCH_SIZE": 5000}
GDS_RBD_VALUES = "Y,B,C,D,E,F,H,I,J,K,M,N2,O2,P2,Q2,R2,T2,U2,V2,Z4,W6,L,X,A"

EQT_RECIPIENTS = "FlightAnalyst@akasaair.com,rmsupport@akasaair.com,closed.rm.it@akasaair.com"
EQT_RECIPIENTS_KEY = "EQT_RECIPIENTS"

# Payload restriction (RM support LID / adjusted-capacity update) recipients
PAYLOAD_RESTRICTION_RECIPIENTS = "rmsupport@akasaair.com,closed.rm.it@akasaair.com"
PAYLOAD_RESTRICTION_RECIPIENTS_KEY = "PAYLOAD_RESTRICTION_RECIPIENTS"
GROUP_SCALING_FACTOR = 0.7
GROUP_AU_BATCH = 5
GROUP_IMP_FORECAST_REDUCTION = 95
DEFAULT_ML_CONFIG = '{ "top_rbd_count": { "B2C": 8, "B2B": 3 }, "top_gp_factor": 1.2 }'
DEFAULT_ML_CALC_CONFIG = '{ "backstop_distance_multiplier": 6, "q_fare_level": "10" }'
PLF_CHANGE_PER = 2.0
PLF_CHANGE_UPPER_PER = 2.0
PLF_CHANGE_LOWER_PER = -1.0
PLF_CHANGE_N_RUNS = 4
LSF_FALLBACK_N_RUNS = 4
CONFIG_FORECAST_ALLOCATION_GRID_COLLECTION = "config_forecast_allocation_grid"
CONFIG_FORECAST_ALLOCATION_BANDS_COLLECTION = "config_forecast_allocation_bands"

STRATEGY_OFFSET_GUARDRAILS = '{"min_shift": 100, "max_shift": 2000}'
STRATEGY_OFFSET_GUARDRAILS_KEY = "strategy_offset_guardrails"

#HOLD_BOOKING_EMAIL_SUBJECT = "Hold Booking Alert"
#HOLD_BOOKING_EMAIL_RECIPIENTS = "jashubhai.chaudhary@akasaair.com"
#HOLD_BOOKING_ALERT_THRESHOLD = 20
#HOLD_BOOKING_ALERT_NDO = 7
#
#HOLD_BOOKING_EMAIL_SUBJECT_KEY = "HOLD_BOOKING_EMAIL_SUBJECT"
#HOLD_BOOKING_EMAIL_RECIPIENTS_KEY = "HOLD_BOOKING_EMAIL_RECIPIENTS"
#HOLD_BOOKING_ALERT_THRESHOLD_KEY = "HOLD_BOOKING_ALERT_THRESHOLD"
#HOLD_BOOKING_ALERT_NDO_KEY = "HOLD_BOOKING_ALERT_NDO"

HOLD_BOOKING_ALERT_CONFIGURATION = {"email_subject":"Hold Booking Alert","email_recipients": ["closed.rm.italerts@akasaair.com"], "email_cc": [],"alert_threshold":20,"alert_ndo":7}
HOLD_BOOKING_ALERT_CONFIGURATION_KEY = "HOLD_BOOKING_ALERT_CONFIGURATION"


# Master data cached once per run (survives per-sector cache resets).
RUN_SCOPED_CACHE_KEYS = (
    'tax_mast_dom', 'tax_mast_intl',
    'pricing_duf_sectors', 'pricing_gst_sectors', 'pricing_rcs_sectors',
)

# =============================================================================
# BACKFILL / REFRESH CONTROLS (overridable via overrides)
# =============================================================================
# FORCE_BACKFILL: 'true' -> ignore cache, refetch full window for the sectors.
FORCE_BACKFILL = ''
# REFRESH_SPEC: '' none | '3' last-3-months | '2025-03,2025-11' explicit months.
REFRESH_SPEC = ''
# In the Lambda, the assigned bin IS the unit of work; sectors are processed one
# at a time for accurate per-sector timing, so this only bounds a standalone run.
SECTOR_BATCH_SIZE = 6

# =============================================================================
# MODEL PARAMETERS
# =============================================================================
NDO_RANGE = list(range(181))           # 0..180
PREDICTION_HORIZON_DAYS = 180
MIN_FLIGHT_MATCHES = 13
# Upper bound on matched historical flights kept per future flight (Step 9).
# Coarse matching-ladder levels (Sector-all-taggings) can return the whole
# sector history; Step 10 then explodes to (future x buckets x matches x 181
# NDO cols) — the OOM source. Keeping the closest-STD / most-recent N caps that
# product with negligible effect on the averaged prediction. 0 disables the cap.
MAX_FLIGHT_MATCHES = 60
WEIGHT_ALPHA = 0.99                     # recency decay for FLOWN matches (weighted branch)
WEIGHT_ALPHA_FUTURE = 0                 # recency decay for FUTURE/unflown matches:
                                        # 0 -> future matches get weight 0 (excluded);
                                        # set to 0.99 to restore the original decayed weight.
WEIGHT_FUTURE_X = 1                     # full weight when snapshot < departure
PARQUET_ROW_THRESHOLD = 250_000
# Steps 10+11 are processed this many future-flight keys at a time so the
# exploded (future x buckets x matches x NDO) frame is never fully materialized.
# Purely a memory knob — output is identical for any value (every reduction is
# per-future-key). Lower it if a heavy sector still approaches the memory limit.
PREDICT_CHUNK_KEYS = 300
# Audit: max number of unmatched future-flight keys stored per sector in
# sector_stats.<SECTOR>.unmatched_keys. The exact count is always recorded
# (unmatched_count); this only caps the stored key list so a data-starved sector
# cannot bloat the single per-run audit document (Mongo 16 MB limit). 0 = no cap.
AUDIT_UNMATCHED_KEYS_CAP = 500

# Inventory Optimization (EMSR-b) parameters
EMSR_SIGMA_FACTOR = 0.25               # Cap Sigma_Bar at Factor * Mu_Bar (default 0.25)
PROMO_FARE_RATIO = 0.80                # Promo fare is 80% of lowest filed fare
PROMO_RBD = 'W'                        # RBD for promo fare
PROMO_FLAG_NG = DEFAULT_PROMO_FLAG_NG
PROMO_RBD_NG = DEFAULT_PROMO_RBD_NG
PROMO_FIRST_RBD_NG = DEFAULT_PROMO_FIRST_RBD_NG
PROMO_RATIO_NG = DEFAULT_PROMO_RATIO_NG
PMIN_THRESHOLD_FACTOR_NG = DEFAULT_PMIN_THRESHOLD_FACTOR_NG
OPEN_BUCKET_MIDPOINT_FACTOR = 1.10     # Midpoint factor for upper open-ended fare bucket (e.g. 25088+)
# Buy-up factor (alpha) applied to the EMSR-b sell-up probability:
#   Prob_j = (1 - f_(j+1) / F_j) * 1 / (1 - alpha)
# Raising alpha raises every probability, hence z, hence the protection limits, so
# more seats are held back for higher fares. Initial value is 0 (no buy-up), which
# leaves the ladder identical to the un-factored formula.
BUY_UP_FACTOR = 0.0
# --- Additive-model LOGIC constants (subject to tuning) -----------------------
# Geometric-progression fare-bucket count. GP generates this many boundary
# points; consecutive duplicates collapse, so a sector may end with FEWER
# buckets than this when actual fares are clustered.
GP_FARE_BUCKET_POINTS = 19
# Lowest fare bucket's lower bound is reduced by this factor to catch fares
# just under the boundary (0.8 => -20%).
FARE_BUCKET_LOWER_FACTOR = 0.8
# Future-tagging ladder: a grouping level with fewer than this many rows is
# dropped so the fill falls through to a coarser, better-supported level.
FUTURE_TAG_MIN_ENTRIES = 12
# Flight matching: +-N hour band applied on the DOW-Tagging ladder levels.
TIME_BAND_HOURS = 2

# --- Additive-model parameter KEYS (DB overrides via rm_parameter_values) -----
PREDICTION_HORIZON_DAYS_KEY = "PREDICTION_HORIZON_DAYS"
MIN_FLIGHT_MATCHES_KEY = "MIN_FLIGHT_MATCHES"
MAX_FLIGHT_MATCHES_KEY = "MAX_FLIGHT_MATCHES"
WEIGHT_ALPHA_KEY = "WEIGHT_ALPHA"
WEIGHT_ALPHA_FUTURE_KEY = "WEIGHT_ALPHA_FUTURE"
WEIGHT_FUTURE_X_KEY = "WEIGHT_FUTURE_X"
PARQUET_ROW_THRESHOLD_KEY = "PARQUET_ROW_THRESHOLD"
GP_FARE_BUCKET_POINTS_KEY = "GP_FARE_BUCKET_POINTS"
FARE_BUCKET_LOWER_FACTOR_KEY = "FARE_BUCKET_LOWER_FACTOR"
FUTURE_TAG_MIN_ENTRIES_KEY = "FUTURE_TAG_MIN_ENTRIES"
TIME_BAND_HOURS_KEY = "TIME_BAND_HOURS"

REQUIRED_RBDS = [
    'B', 'C', 'D', 'E', 'F', 'H', 'I', 'J', 'K',
    'M', 'N', 'N0', 'N1', 'N2', 'N3', 'N4', 'O',
    'O0', 'O1', 'O2', 'O3', 'O4', 'P0', 'P1', 'P2', 'P3',
    'P4', 'Q', 'Q0', 'Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q6', 'Q7',
    'Q8', 'Q9', 'R0', 'R1', 'R2', 'R3', 'R4', 'R5', 'R6', 'R7',
    'R8', 'R9', 'T0', 'T1', 'T2', 'T3', 'T4', 'T5', 'T6', 'T7',
    'T8', 'T9', 'U0', 'U1', 'U2', 'U3', 'U4', 'U5', 'U6', 'U7',
    'U8', 'U9', 'V0', 'V1', 'V2', 'V3', 'V4', 'V5', 'V6', 'V7',
    'V8', 'V9', 'Y', 'Z0', 'Z1', 'Z2', 'Z3', 'Z4', 'Z5', 'Z6',
    'Z7', 'Z8', 'Z9', 'ZA', 'ZB', 'ZC', 'ZD', 'ZE', 'ZF', 'ZH',
    'ZI', 'ZJ',
]

# RBDs used exclusively for fare bucket creation in _fetch_fare_buckets
FARE_BUCKET_RBDS = [
    'Y', 'B', 'C', 'D', 'E', 'F', 'H', 'I', 'J', 'K',
    'M', 'N0', 'N1', 'N2', 'N3', 'N4', 'O0', 'O1', 'O2', 'O3', 'O4',
    'P0', 'P1', 'P2', 'P3', 'P4', 'Q0', 'Q1', 'Q2', 'Q3', 'Q4',
    'R0', 'R1', 'R2', 'R3', 'R4', 'T0', 'T1', 'T2', 'T3', 'T4',
    'U0', 'U1', 'U2', 'U3', 'U4', 'V0', 'V1', 'V2', 'V3', 'V4',
]

SUMMARY_FARE_COLUMNS = [
    'Backstop', 'hardAnchorFare', 'strategyFare', 'lastSellingFare', 'profileFare',
    'lastSevenAvgFare', 'anchorFareBeforeTolerance', 'toleranceValue', 'anchorFare',
    'SellingFare', 'ForecastAF_Allin', 'AF_Allin', 'AF_NB_Allin', 'ALLIN_AF_NB_LM',
    'ALLIN_AF_NB_LY', 'grpAdjProfileFare', 'Portfolio_Anchor', 'grpFareAnchor',
    'Portfolio88_Anchor', 'upsell', 'NG_AF_Forecast', 'NG_Opening_fare',
    'NG_Strategy_fare', 'NG_Anchor_fare', 'MktFare_Min', 'MktFare_Max', 'autoBackstop'
]

NEXTGEN_SUMMARY_FARE_COLUMNS = [
    'MarketFare_Selected', 'MarketFare_Min', 'MarketFare_Max',
    'NG_AF_Forecast', 'NG_Opening_fare', 'NG_Strategy_fare', 'NG_Anchor_fare'
]


ML_AUDIT_COLNAME_TO_MARKET_LIST_FIELD = {
    "OBFare": "obFare",
    "OBSeats": "obSeats",
    "b2bOutbound_Stop": "Outbound_Stop",
    "carrExclusionB2B": "CarrExlusionB2B",
    "carrExclusionB2C": "CarrExlusionB2C",
    "curveId": "CurveID",
    "startTime": "TimeWindowStart",
    "endTime": "TimeWindowEnd",
    "firstAllocation": "FirstRBDAlloc",
    "otherAllocation": "OtherRBDAlloc",
    "skippingFactor": "SkippingFactor",
    "b2bBackstop": "B2BBackstop",
    "b2cBackstop": "B2CBackstop",
    "b2bFactor": "B2BFactor",
    "b2bTolerance": "B2BTolerance",
    "b2cTolerance": "B2CTolerance",
    "tbfEnhFlag": "TBFEnhFlag",
    "marketfare_Upsell": "Marketfare_Upsell",
    "spoilage_Flag": "Spoilage_Flag",
    "canAdjustedForecast": "CanAdjustedForecast",
    "profileFareUpsell": "ProfileFareUpsell",
    "daySpan": "DaySpan",
    "autoTimeRangeFlag": "AutoTimeRangeFlag",
    "overBooking": "OverBooking",
    "cugFlag": "CugFlag",
    "mktFareFlag": "MktFareFlag",
    # NextGen (_NG) fields where the Java bean name differs from @Column(name):
    "competitionResponseGrid_NG": "CompetitionResponseGrid_NG",
    "promoFlag_NG": "PromoFlag_NG",
}

ML_FLAG_COLUMNS = {
    "openingFares",
    "AutoTimeRangeFlag",
    "OverBooking",
    "profileFares",
    "rbdPushFlag",
    "distressInventoryFlag",
    "seriesBlock",
    "autoGroup",
    "autoBackstopFlag",
    "tbfFlag",
    "CugFlag",
    "MktFareFlag",
    "directFlag",
    "TBFEnhFlag",
    "defaultMlFlag",
    "forecastAllocFlag",
    "afForecastAllocFlag",
    "auto_tolerance_flag",
    "incrementedProfileFareFlag",
    "Spoilage_Flag",
    "CanAdjustedForecast",
    "market_station_mapping",
}
