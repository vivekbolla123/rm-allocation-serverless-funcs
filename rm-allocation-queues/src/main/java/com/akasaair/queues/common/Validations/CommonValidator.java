package com.akasaair.queues.common.Validations;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;

import static com.akasaair.queues.common.constants.DateTimeConstants.SALES_DATE_INPUT_FORMATTER;

public class CommonValidator {

    public static boolean isValidIsoDate(String date) {
        try {
            LocalDate.parse(date, SALES_DATE_INPUT_FORMATTER);
            return true;
        } catch (DateTimeParseException e) {
            return false;
        }
    }

}
