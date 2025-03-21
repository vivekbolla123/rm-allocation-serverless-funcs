package com.akasaair.queues.common.Validations;

import java.util.*;

import static com.akasaair.queues.common.constants.Constants.*;

public class FaresValidator implements Validator {

    @Override
    public boolean isValid(List<Map<String, String>> data, String val, List<String> errors) {
        try {
            List<String> rdbValues = Arrays.asList(val.split(COMMA));
            HashSet<String> rbd = new HashSet<>();
            Map<String, Integer> hashMap = new HashMap<>();

            for (int lineNumber = 0; lineNumber < data.size(); lineNumber++) {
                Map<String, String> singleData = data.get(lineNumber);
                String rbdVal = singleData.get(RBD_COLUMN_NAME);
                String sector = singleData.get(SECTOR_COLUMN_NAME);
                String org = singleData.get("Org");
                String dst = singleData.get("Dst");
                String route = singleData.get(ROUTE_COLUMN_NAME);
                String totalStr = singleData.get("TOTAL");
                String currency = singleData.get("currency");

                validateSector(sector, errors, lineNumber);
                validateOrg(org, errors, lineNumber);
                validateDst(dst, errors, lineNumber);
                validateRoute(route, errors, lineNumber);
                validateRBD(rbdVal, errors, lineNumber);
                validateTotal(totalStr, errors, lineNumber);
                validateCurrency(currency, errors, lineNumber);

                // Track unique sector and route combinations
                String key = singleData.get(SECTOR_COLUMN_NAME) + singleData.get(ROUTE_COLUMN_NAME);
                hashMap.put(key, hashMap.getOrDefault(key, 0) + 1);

                // Validate RBD values
                if (!rdbValues.contains(rbdVal)) {
                    errors.add("Line " + (lineNumber + 1) + ": Invalid RBD value: " + rbdVal);
                }
            }

            // Check element count
            if (!errors.isEmpty()) {
                return false;
            }
            return eachElement(hashMap, rdbValues.size());
        } catch (Exception e) {
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

    public static boolean eachElement(Map<?, Integer> map,int rbdSize) {
        for (Integer value : map.values()) {
            if (value != rbdSize) {
                return false;
            }
        }
        return true;
    }

}
