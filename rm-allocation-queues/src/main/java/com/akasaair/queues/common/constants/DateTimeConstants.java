package com.akasaair.queues.common.constants;

import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;

public interface DateTimeConstants {
    DateTimeFormatter DATE_OUTPUT_FORMATTER = DateTimeFormatter.ofPattern("MM/dd/yyyy");
    DateTimeFormatter DATE_TIME_OUTPUT_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    DateTimeFormatter TIME_OUTPUT_FORMATTER = DateTimeFormatter.ofPattern("[HH:mm][H:mm]");
    SimpleDateFormat SIMPLE_DATE_OUTPUT_FORMATTER_2 = new SimpleDateFormat("MM/dd/yyyy");
    SimpleDateFormat SIMPLE_DAY_FORMATTER = new SimpleDateFormat("E");
}
