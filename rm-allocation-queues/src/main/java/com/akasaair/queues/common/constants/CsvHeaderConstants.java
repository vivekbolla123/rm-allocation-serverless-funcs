package com.akasaair.queues.common.constants;

public interface CsvHeaderConstants {
    int ROW_START_COUNT = 1;
    int CSV_HEADER_ORIGIN = 0;
    int CSV_HEADER_DESTINATION = 1;
    int CSV_HEADER_FLIGHT_NO = 2;
    int CSV_HEADER_START_DATE = 3;
    int CSV_HEADER_END_DATE = 4;
    int CSV_HEADER_DAY_OF_WEEK = 5;
    int CSV_HEADER_START_TIME = 6;
    int CSV_HEADER_END_TIME = 7;
    int CSV_HEADER_CURVE_ID = 8;
    int CSV_HEADER_CARR_EXCLUSION_B2C = 9;
    int CSV_HEADER_CARR_EXCLUSION_B2B = 10;
    int CSV_HEADER_FLIGHT_EXCLUSION_B2C = 11;
    int CSV_HEADER_FLIGHT_EXCLUSION_B2B = 12;
    int CSV_HEADER_FARE_ANCHOR = 13;
    int CSV_HEADER_HARD_ANCHOR = 14;
    int CSV_HEADER_PLF_THRESHOLD = 15;
    int CSV_HEADER_FARE_OFFSET = 16;
    int CSV_HEADER_FIRST_ALLOCATION = 17;
    int CSV_HEADER_OTHER_ALLOCATION = 18;
    int CSV_HEADER_SKIPPING_FACTOR = 19;
    int CSV_HEADER_B2B_TOLERANCE = 20;
    int CSV_HEADER_B2C_TOLERANCE = 21;
    int CSV_HEADER_B2B_BACKSTOP = 22;
    int CSV_HEADER_B2C_BACKSTOP = 23;
    int CSV_HEADER_B2B_FACTOR = 24;
    int CSV_HEADER_OB_SEATS = 25;
    int CSV_HEADER_OB_FARE = 26;
    int CSV_HEADER_DAY_SPAN = 27;
    int CSV_HEADER_AUTO_TIME_RANGE_FLAG = 28;
    int CSV_HEADER_OPENING_FARES = 29;
    int CSV_HEADER_OVER_BOOKING = 30;
    int CSV_HEADER_PROFILE_FARES = 31;
    int CSV_HEADER_RBD_PUSH_FLAG = 32;
    int CSV_HEADER_DISTRESS_INVENTORY_FLAG = 33;
    int CSV_HEADER_SERIES_BLOCK_FLAG = 34;
    int CSV_HEADER_AUTO_GROUP = 35;
    int CSV_HEADER_AUTO_BACKSTOP_FLAG = 36;
    int CSV_HEADER_TBF_FLAG = 37;
    int CSV_HEADER_CUG_FLAG = 38;
    int CSV_HEADER_MKT_FARE_FLAG = 39;
    int CSV_HEADER_DIRECT_FLAG = 40;
    int CSV_HEADER_TBF_ENH_FLAG = 41;
    int CSV_HEADER_DEFAULT_ML_FLAG = 42;
    int CSV_HEADER_B2B_STRATEGY = 43;
    int CSV_HEADER_OUTBOUNDSTOP = 44;
    int CSV_HEADER_MKTFARE_UPSELL = 45;
    int CSV_HEADER_FORECAST_ALLOC_FLAG = 46;
    int CSV_HEADER_SPOILAGE_FLAG = 47;
    int CSV_HEADER_CAN_ADJUSTED_FORECAST = 48;
    int CSV_HEADER_MKT_ORIGIN_MAPPING = 49;
    int CSV_HEADER_AF_FORECAST_ALLOC_FLAG = 50;
    int CSV_HEADER_PROFILE_FARE_UPSELL = 51;
    int CSV_HEADER_INCREMENTED_PROFILE_FARE_FLAG = 52;
    int CSV_HEADER_AUTO_TOLERANCE_FLAG = 53;
    int CSV_HEADER_BUY_UP_FACTOR_NG = 54;
    int CSV_HEADER_CONNECTION_FARE_FLAG_NG = 55;
    int CSV_HEADER_ADDITIONAL_CONNECTION_DURATION_NG = 56;
    int CSV_HEADER_COMPETITION_RESPONSE_GRID_NG = 57;
    int CSV_HEADER_NEXTGEN_FLAG_NG = 58;
    int CSV_HEADER_COMPETITION_RESPONSE_FLAG_NG = 59;
    int CSV_HEADER_PROMO_FLAG_NG = 60;



    public static final int CSV_SALES_STARTDATE_IDX = 0; // Assuming "startdate" is the first column
    public static final int CSV_SALES_ENDDATE_IDX = 1;   // Assuming "enddate" is the second column
    public static final int CSV_SALES_MINPAX_IDX = 2;    // Assuming "minpax" is the third column
    public static final int CSV_SALES_MAXPAX_IDX = 3;    // Assuming "maxpax" is the fourth column
    public static final int CSV_SALES_SEATSALLOC_IDX = 4;// Assuming "seatsalloc" is the fifth column

    String CSV_HEADER_TIME = "Time";
    String CSV_HEADER_CRITERIA = "Criteria";
    String CSV_HEADER_OFFSET = "Offset";
    // Connections
    int CSV_HEADER_CONNECTIONS_SECTOR1 = 0;
    int CSV_HEADER_CONNECTIONS_FLIGHT1 = 1;
    int CSV_HEADER_CONNECTIONS_SECTOR2 = 2;
    int CSV_HEADER_CONNECTIONS_FLIGHT2 = 3;
    int CSV_HEADER_CONNECTIONS_PERSTART = 4;
    int CSV_HEADER_CONNECTIONS_PEREND = 5;
    int CSV_HEADER_CONNECTIONS_DOW = 6;
    int CSV_HEADER_CONNECTIONS_OUTBOUNDSTOP = 7;
    int CSV_HEADER_CONNECTIONS_OUTBOUNDDURATION = 8;
    public static final int CSV_HEADER_CONNECTIONS_CARRIEREXCLB2B = 9;   // New
    public static final int CSV_HEADER_CONNECTIONS_CARRIEREXCLB2C = 10;
    int CSV_HEADER_CONNECTIONS_PRICESTRATEGY = 11;
    int CSV_HEADER_CONNECTIONS_DISCOUNTFLAG = 12;
    int CSV_HEADER_CONNECTIONS_DISCOUNTVALUE = 13;
    int CSV_HEADER_CONNECTIONS_FAREANCHOR = 14;
    int CSV_HEADER_CONNECTIONS_OFFSET = 15;
    int CSV_HEADER_CONNECTIONS_FIRSTRBDALLOC = 16;
    int CSV_HEADER_CONNECTIONS_OTHERRBDALLOC = 17;
    int CSV_HEADER_CONNECTIONS_SKIPPINGFACTOR = 18;
    int CSV_HEADER_CONNECTIONS_CURRENCY = 19;
    int CSV_HEADER_CONNECTIONS_B2BBACKSTOP = 20;
    int CSV_HEADER_CONNECTIONS_B2CBACKSTOP = 21;
    int CSV_HEADER_CONNECTIONS_B2BFACTOR = 22;
    int CSV_HEADER_CONNECTIONS_SOSFLAG = 23;


    public static final int CSV_HEADER_SALESINV_ORIGIN = 0;
    public static final int CSV_HEADER_SALESINV_DESTIN = 1;
    public static final int CSV_HEADER_SALESINV_FLIGHT_NO = 2;
    public static final int CSV_HEADER_SALESINV_START_DATE = 3;
    public static final int CSV_HEADER_SALESINV_END_DATE = 4;
    public static final int CSV_HEADER_SALESINV_DOW = 5;
    public static final int CSV_HEADER_SALESINV_CURVE_ID = 6;
    public static final int CSV_HEADER_SALESINV_SALE_FLAG = 7;

    public static final String CSV_HEADER_SECTOR = "sector";
    public static final String CSV_HEADER_MONTH = "month";
    public static final String CSV_HEADER_VALUE = "value";

    public static final String CSV_HEADER_SI_RBD_SEATS = "seats"; public static final String CSV_HEADER_SI_RBD_TYPE = "type"; // RBD columns like W7, W6 will be identified by their position/header name in the CSV

    public static final String CSV_HEADER_FORECAST_ALLOC_GRID = "band,trend,formula,description";
    public static final String CSV_HEADER_FORECAST_ALLOC_BANDS = "band,start,end,tolerance,b2b_factor";
}

