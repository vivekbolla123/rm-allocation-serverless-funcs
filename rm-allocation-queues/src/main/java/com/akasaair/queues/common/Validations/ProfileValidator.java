package com.akasaair.queues.common.Validations;

import java.util.*;

import static com.akasaair.queues.common.constants.Constants.*;

public class ProfileValidator implements Validator {

    private static final List<String> VALID_MONTHS = Arrays.asList(
            "January_H1", "January_H2", "February_H1", "February_H2", "March_H1", "March_H2", "April_H1", "April_H2", "May_H1", "May_H2", "June_H1", "June_H2", "July_H1", "July_H2", "August_H1", "August_H2", "September_H1", "September_H2", "October_H1", "October_H2", "November_H1", "November_H2", "December_H1", "December_H2"
    );

    @Override
    public boolean isValid(List<Map<String, String>> data, String val, List<String> errors) {
        try {
            for (int lineNumber = 0; lineNumber < data.size(); lineNumber++) {
                Map<String, String> singleData = data.get(lineNumber);
                String month = singleData.get("month");
                
                validateMonth(month, errors, lineNumber);
            }
            
            return errors.isEmpty();
        } catch (Exception e) {
            errors.add("Unexpected error during validation: " + e.getMessage());
            return false;
        }
    }

    private void validateMonth(String month, List<String> errors, int lineNumber) {
        // Check if month is null or empty
        if (month == null || month.isEmpty()) {
            errors.add("Line " + (lineNumber + 1) + ": Month cannot be empty");
            return;
        }
        
        // Check for spaces in month
        if (month.contains(" ")) {
            errors.add("Line " + (lineNumber + 1) + ": Month cannot contain spaces: " + month);
        }
        
        // Check if month is valid
        if (!VALID_MONTHS.contains(month)) {
            errors.add("Line " + (lineNumber + 1) + ": Invalid month name: " + month);
        }
    }
}