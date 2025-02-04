package com.akasaair.queues.common.constants;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

public class Constants implements CsvHeaderConstants, ErrorMessagesConstants, TableNamesConstants, ColumnNamesConstants,
        DateTimeConstants, PermissionsConstants {
    public static final String CONST_COLAN = ":";
    public static final String PERCENTAGE_SIGN = "%";
    public static final String UNDERSCORE = "_";
    public static final String DOT = ".";
    public static final String SLASH = "/";
    public static final String SPACE = " ";
    public static final String EMPTY = "";
    public static final String COMMA = ",";
    public static final String ZERO = "0";
    public static final String ONE = "1";
    public static final String UPLOAD_QUEUE_STATUS_IN_PROGRESS = "IN-PROGRESS";
    public static final String UPLOAD_QUEUE_STATUS_DONE = "DONE";
    public static final String UPLOAD_QUEUE_STATUS_FAILED = "FAILED";
    public static final String FLAG_0 = "0";
    public static final String FLAG_1 = "1";
    public static final String FLAG_2 = "2";
    public static final String DAY_SPAN_PREVIOUS = "-1";
    public static final String DAY_SPAN_NEXT = "1";
    public static final String DAY_SPAN_CURRENT = "0";
    public static final String OWN_FARE_FLAG_YES = "Y";
    public static final String OWN_FARE_FLAG_NO = "N";
    public static final char DIGIT_1 = '1';
    public static final char DIGIT_9 = '9';
    public static final int OFFSET_START_VALUE = -10;
    public static final int OFFSET_END_VALUE = 10;
    public static final int NUMBER_OF_DECISIONS = 3;
    public static final int NDO_SIZE = 366;
    public static final double LF_VALUE = 1.05;
    public static final double PLFTHRESHOLD_VALUE = 103.0;
    public static final int DAYS_TO_ENTER_INTO_MARKET_LIST = 30;
    public static final String TEMP = "temp";
    public static final String PARAMETER_RBD_VALUES = "RBD_VALUES";
    public static final String PER_TYPE_VALUE = "D";
    public static final String INTERNATIONAL = "International";
    public static final String DOMESTIC = "Domestic";
    public static final String CONNECTIONS = "Connections";
    public static final String INTERNATIONAL_SYMBOL = "I";
    public static final String DOMESTIC_SYMBOL = "D";
    public static Map<String, String> STRATEGY_HEADER_VALUES = new LinkedHashMap<>();
    public final static String S3_FILE_PATH_NAME = System.getenv("FILE_PATH_NAME");
    public final static String S3_BUCKET_NAME = System.getenv("S3_BUCKET_NAME");
    public final static String FILE_NAME = System.getenv("FILE_NAME");

    static {
        STRATEGY_HEADER_VALUES.put("Time", TIME_RANGE_COLUMN_NAME);
        STRATEGY_HEADER_VALUES.put("Criteria", CRITERIA_COLUMN_NAME);
        STRATEGY_HEADER_VALUES.put("Offset", OFFSET_COLUMN_NAME);
        STRATEGY_HEADER_VALUES.put("Strategy", STRATEGY_COLUMN_NAME);
        STRATEGY_HEADER_VALUES.put("NdoBand", NDO_BAND_COLUMN_NAME);
        STRATEGY_HEADER_VALUES.put("DplfBand", DPLF_BAND_COLUMN_NAME);
        STRATEGY_HEADER_VALUES.put("Channel", CHANNEL_COLUMN_NAME);
    }

    public static String generateUUID() {
        return UUID.randomUUID().toString();
    }

}
