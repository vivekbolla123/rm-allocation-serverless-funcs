package com.akasaair.queues.common.Validations;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;

import static com.akasaair.queues.common.constants.Constants.*;
import static com.akasaair.queues.common.constants.DateTimeConstants.DATE_OUTPUT_FORMATTER;
import static com.akasaair.queues.common.constants.DateTimeConstants.TIME_OUTPUT_FORMATTER;

public class MarketListValidator {

    public static boolean isLengthEqualTo(String value, int length) {
        return value.length() == length;
    }

    public static boolean isValidCarrExcl(String value) {
        String[] carrierCodes = value.split("/");
        boolean isValid = true;

        for (String code : carrierCodes) {
            if (code.trim().length() != 2) {
                isValid = false;
                break;
            }
        }
        if (value.endsWith("/")) {
            isValid = false;
        }
        return isValid;
    }

    public static boolean isValidFlightNumber(String flightNumber) {
        if (flightNumber == null) {
            return false;
        }
        return flightNumber.matches("^[A-Z0-9]{2,3}-\\d{2,4}$") || flightNumber.trim().isEmpty();
    }

    public static boolean checkDOWDigits(String value) {
        for (char digit : value.toCharArray()) {
            if (!(digit == DIGIT_1 || digit == DIGIT_9)) {
                return false;
            }
        }
        return true;
    }

    public static boolean checkStrategyFlag(String value) {
        return value.equalsIgnoreCase(FLAG_0) || value.equalsIgnoreCase(FLAG_1);
    }
    public static boolean checkFareAnchor(String value) {
        return value.equalsIgnoreCase("MIN") || value.equalsIgnoreCase("MAX")||value.equalsIgnoreCase("MINO") || value.equalsIgnoreCase("MAXO");
    }
    public static boolean checkPriceStrategy(String value) {
        return value.equalsIgnoreCase(FLAG_2) || value.equalsIgnoreCase(FLAG_1);
    }
    public static boolean checkOwnFareFlag(String value) {
        return value.equalsIgnoreCase(OWN_FARE_FLAG_YES) || value.equalsIgnoreCase(OWN_FARE_FLAG_NO);
    }

    public static boolean checkDaySpan(String value) {
        return value.equalsIgnoreCase(DAY_SPAN_NEXT) || value.equalsIgnoreCase(DAY_SPAN_CURRENT)
                || value.equalsIgnoreCase(DAY_SPAN_PREVIOUS);
    }

    public static boolean checkAutoTimeFlag(String value) {
        return value.equalsIgnoreCase(OWN_FARE_FLAG_YES) || value.equalsIgnoreCase(OWN_FARE_FLAG_NO);
    }

    public static boolean isValidTime(String time) {
        if (time == null) {
            return false;
        }
        try {
            LocalTime.parse(time, TIME_OUTPUT_FORMATTER);
        } catch (DateTimeParseException nfe) {
            return false;
        }
        return true;
    }

    public static boolean isValidDate(String date) {
        if (date == null) {
            return false;
        }
        try {
            LocalDate parseDate = LocalDate.parse(date, DATE_OUTPUT_FORMATTER);
            LocalDate currentDateUtc = LocalDate.now();
            return parseDate.isAfter(currentDateUtc) || parseDate.isEqual(currentDateUtc);
        } catch (DateTimeParseException nfe) {
            return false;
        }
    }

    public static boolean isNumeric(String strNum) {
        if (strNum == null) {
            return false;
        }
        try {
            Integer.parseInt(strNum);
        } catch (NumberFormatException ex) {
            return false;
        }
        return true;
    }

    public static boolean isDouble(String strNum) {
        if (strNum == null) {
            return false;
        }
        try {
            Double.parseDouble(strNum);
        } catch (NumberFormatException ex) {
            return false;
        }
        return true;
    }

    public static boolean isFloat(String strNum) {
        if (strNum == null) {
            return false;
        }
        try {
            Float.parseFloat(strNum);
        } catch (NumberFormatException ex) {
            return false;
        }
        return true;
    }

    public static boolean isPositive(String strNum) {
        return Integer.parseInt(strNum) >= 0;
    }

}