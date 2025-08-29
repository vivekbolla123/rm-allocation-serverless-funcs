package com.akasaair.queues.common.constants;

import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

public interface DateTimeConstants {
    DateTimeFormatter DATE_OUTPUT_FORMATTER = DateTimeFormatter.ofPattern("MM/dd/yyyy");
    DateTimeFormatter DATE_TIME_OUTPUT_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    DateTimeFormatter TIME_OUTPUT_FORMATTER = DateTimeFormatter.ofPattern("[HH:mm][H:mm]");
    SimpleDateFormat SIMPLE_DATE_OUTPUT_FORMATTER_2 = new SimpleDateFormat("MM/dd/yyyy");
    DateTimeFormatter DATE_OUTPUT_FORMATTER_3 = DateTimeFormatter.ofPattern("MM-dd-yyyy");
    SimpleDateFormat SIMPLE_DAY_FORMATTER = new SimpleDateFormat("E");
    public static final DateTimeFormatter SALES_DATE_INPUT_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd"); // Existing
    public static final DateTimeFormatter SALES_MONTH_YEAR_INPUT_FORMATTER = DateTimeFormatter.ofPattern("MMMM_yyyy", Locale.ENGLISH);
    public static final DateTimeFormatter SALES_DATE_DB_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");
    public static final String MONTH_DAY_PLACEHOLDER = "00";
}
