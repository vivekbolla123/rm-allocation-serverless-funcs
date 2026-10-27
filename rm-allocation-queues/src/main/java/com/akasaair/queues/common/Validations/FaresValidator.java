package com.akasaair.queues.common.Validations;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;

import static com.akasaair.queues.common.constants.Constants.*;
import static com.akasaair.queues.common.constants.ColumnNamesConstants.FARE_START_DATE_COLUMN_NAME;
import static com.akasaair.queues.common.constants.ColumnNamesConstants.FARE_END_DATE_COLUMN_NAME;

public class FaresValidator implements Validator {

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("MM/dd/yyyy");

    @Override
    public boolean isValid(List<Map<String, String>> data, String val, List<String> errors) {
        try {
            List<String> rdbValues = Arrays.asList(val.split(COMMA));

            for (int lineNumber = 0; lineNumber < data.size(); lineNumber++) {
                Map<String, String> singleData = data.get(lineNumber);
                String rbdVal    = singleData.get(RBD_COLUMN_NAME);
                String sector    = singleData.get(SECTOR_COLUMN_NAME);
                String org       = singleData.get("Org");
                String dst       = singleData.get("Dst");
                String route     = singleData.get(ROUTE_COLUMN_NAME);
                String totalStr  = singleData.get("TOTAL");
                String currency  = singleData.get("currency");
                String startDate = singleData.get(FARE_START_DATE_COLUMN_NAME);
                String endDate   = singleData.get(FARE_END_DATE_COLUMN_NAME);

                validateSector(sector, errors, lineNumber);
                validateOrg(org, errors, lineNumber);
                validateDst(dst, errors, lineNumber);
                validateRoute(route, errors, lineNumber);
                validateRBD(rbdVal, errors, lineNumber);
                validateTotal(totalStr, errors, lineNumber);
                validateCurrency(currency, errors, lineNumber);
                validateDateFields(startDate, endDate, errors, lineNumber);

                // Validate RBD values against the master list
                if (!rdbValues.contains(rbdVal)) {
                    errors.add("Line " + (lineNumber + 1) + ": Invalid RBD value: " + rbdVal);
                }
            }

            // We no longer validate overall count because uploads happen at the group level.
            // If there are no row-level validation errors, return true.
            return errors.isEmpty();
        } catch (Exception e) {
            errors.add("Unexpected error during validation: " + e.getMessage());
            return false;
        }
    }

    private void validateSector(String sector, List<String> errors, int lineNumber) {
        if (sector == null || sector.length() != 6) {
            errors.add("Line " + (lineNumber + 1) + ": Invalid Sector: " + sector);
        }
    }

    private void validateOrg(String org, List<String> errors, int lineNumber) {
        if (org == null || org.length() != 3) {
            errors.add("Line " + (lineNumber + 1) + ": Invalid Org: " + org);
        }
    }

    private void validateDst(String dst, List<String> errors, int lineNumber) {
        if (dst == null || dst.length() != 3) {
            errors.add("Line " + (lineNumber + 1) + ": Invalid Dst: " + dst);
        }
    }

    private void validateRoute(String route, List<String> errors, int lineNumber) {
        if (!"L".equals(route) && !"R".equals(route)) {
            errors.add("Line " + (lineNumber + 1) + ": Invalid Route: " + route);
        }
    }

    private void validateRBD(String rbdVal, List<String> errors, int lineNumber) {
        if (rbdVal == null || rbdVal.length() > 2) {
            errors.add("Line " + (lineNumber + 1) + ": Invalid RBD: " + rbdVal);
        }
    }

    private void validateTotal(String totalStr, List<String> errors, int lineNumber) {
        try {
            Float.parseFloat(totalStr);
        } catch (NumberFormatException e) {
            errors.add("Line " + (lineNumber + 1) + ": Invalid Total: " + totalStr);
        }
    }

    private void validateCurrency(String currency, List<String> errors, int lineNumber) {
        if (currency == null || currency.length() != 3) {
            errors.add("Line " + (lineNumber + 1) + ": Invalid Currency: " + currency);
        }
    }

    /**
     * Validates depDateStart and depDateEnd fields.
     * Both are optional (empty = store as null in DB = open-ended bound).
     * If non-empty, must be in MM/dd/yyyy format and depDateStart must not be after depDateEnd.
     */
    private void validateDateFields(String startDate, String endDate, List<String> errors, int lineNumber) {
        LocalDate parsedStart = null;
        LocalDate parsedEnd   = null;

        boolean startEmpty = (startDate == null || startDate.trim().isEmpty());
        boolean endEmpty   = (endDate   == null || endDate.trim().isEmpty());

        if (!startEmpty) {
            try {
                parsedStart = LocalDate.parse(startDate.trim(), DATE_FORMATTER);
            } catch (DateTimeParseException e) {
                errors.add("Line " + (lineNumber + 1) + ": Invalid depDateStart format '" + startDate
                        + "'. Expected MM/dd/yyyy.");
            }
        }

        if (!endEmpty) {
            try {
                parsedEnd = LocalDate.parse(endDate.trim(), DATE_FORMATTER);
            } catch (DateTimeParseException e) {
                errors.add("Line " + (lineNumber + 1) + ": Invalid depDateEnd format '" + endDate
                        + "'. Expected MM/dd/yyyy.");
            }
        }

        // If both parsed successfully, ensure depDateStart <= depDateEnd (inclusive)
        if (parsedStart != null && parsedEnd != null && parsedStart.isAfter(parsedEnd)) {
            errors.add("Line " + (lineNumber + 1) + ": depDateStart (" + startDate
                    + ") must not be after depDateEnd (" + endDate + ").");
        }
    }

}
