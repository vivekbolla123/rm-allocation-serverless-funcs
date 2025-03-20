package com.akasaair.queues.common.Validations;

import java.util.*;

import static com.akasaair.queues.common.constants.Constants.*;

public class ProfileValidator implements Validator {

    private static final List<String> VALID_MONTHS = Arrays.asList(
            "January", "February", "March", "April", "May", "June", 
            "July", "August", "September", "October", "November", "December"
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