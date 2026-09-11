package com.akasaair.queues.common.Validations;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;

import static com.akasaair.queues.common.constants.DateTimeConstants.SALES_DATE_INPUT_FORMATTER;

/**
 * Static validation helpers for the payload restriction upload
 * (config_payload_restriction).
 *
 * Follows the same "utility class of static booleans" convention as
 * {@link CommonValidator} and {@link MarketListValidator}.
 */
public class PayloadRestrictionValidator {

    /** Inclusive lower bound allowed for lid / adjustedCapacity. */
    public static final int LID_CAPACITY_MIN = 0;
    /** Inclusive upper bound allowed for lid / adjustedCapacity. */
    public static final int LID_CAPACITY_MAX = 200;

    private PayloadRestrictionValidator() {
        // utility class
    }

    /**
     * True when {@code value} is an integer within [min, max] inclusive.
     */
    public static boolean isWithinRange(String value, int min, int max) {
        if (value == null || value.trim().isEmpty()) {
            return false;
        }
        try {
            int parsed = Integer.parseInt(value.trim());
            return parsed >= min && parsed <= max;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /**
     * True when {@code value} is a valid lid / adjustedCapacity, i.e. an
     * integer within [0, 200] inclusive.
     */
    public static boolean isValidLidOrCapacity(String value) {
        return isWithinRange(value, LID_CAPACITY_MIN, LID_CAPACITY_MAX);
    }

    /**
     * True when both dates parse as yyyy-MM-dd and {@code endDate} is not before
     * {@code startDate} (i.e. end &gt;= start). Returns false if either date is
     * unparseable.
     */
    public static boolean isValidDateRange(String startDate, String endDate) {
        if (startDate == null || endDate == null) {
            return false;
        }
        try {
            LocalDate start = LocalDate.parse(startDate.trim(), SALES_DATE_INPUT_FORMATTER);
            LocalDate end = LocalDate.parse(endDate.trim(), SALES_DATE_INPUT_FORMATTER);
            return !end.isBefore(start);
        } catch (DateTimeParseException e) {
            return false;
        }
    }
}
