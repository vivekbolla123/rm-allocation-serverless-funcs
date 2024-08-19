package com.akasaair.queues.common.Validations;

import java.util.*;

import static com.akasaair.queues.common.constants.Constants.*;

public class FaresValidator implements Validator {

    @Override
    public boolean isValid(List<Map<String, String>> data, String val,List<String> errors) {
        try {
            List<String> rdbValues = Arrays.asList(val.split(COMMA));
            HashSet<String> rbd = new HashSet<>();
            Map<String, Integer> hashMap = new HashMap<>();
            for (Map<String, String> singleData : data) {
                String rbdVal = singleData.get(RBD_COLUMN_NAME);
                String sector = (String) singleData.get(SECTOR_COLUMN_NAME);
                String org = (String) singleData.get("Org");
                String dst = (String) singleData.get("Dst");
                String route = (String) singleData.get(ROUTE_COLUMN_NAME);
                String totalStr = singleData.get("TOTAL").toString();
                String currency = (String) singleData.get("currency");
                validateSector(sector, errors);
                validateOrg(org, errors);
                validateDst(dst, errors);
                validateRoute(route, errors);
                validateRBD(rbdVal, errors);
                validateTotal(totalStr, errors);
                validateCurrency(currency, errors);
                String key = singleData.get(SECTOR_COLUMN_NAME) + singleData.get(ROUTE_COLUMN_NAME);
                if (!hashMap.containsKey(key)) {
                    hashMap.put(key, 0);
                }
                hashMap.put(key, hashMap.get(key) + 1);
                if (!rdbValues.contains(rbdVal))
                    errors.add("Invalid RBD value: " + rbdVal);

            }
            if (!errors.isEmpty()) {
                errors.forEach(System.out::println);
                return false;
            }
            return eachElement(hashMap,rdbValues.size());
        } catch (Exception e) {
            return false;
        }
    }

    private void validateSector(String sector, List<String> errors) {
        if (sector == null || sector.length() != 6) {
            errors.add("Invalid Sector: " + sector);
        }
    }

    private void validateOrg(String org, List<String> errors) {
        if (org == null || org.length() != 3) {
            errors.add("Invalid Org: " + org);
        }
    }

    private void validateDst(String dst, List<String> errors) {
        if (dst == null || dst.length() != 3) {
            errors.add("Invalid Dst: " + dst);
        }
    }

    private void validateRoute(String route, List<String> errors) {
        if (!"L".equals(route) && !"R".equals(route)) {
            errors.add("Invalid Route: " + route);
        }
    }

    private void validateRBD(String rbdVal, List<String> errors) {
        if (rbdVal == null || rbdVal.length() > 2) {
            errors.add("Invalid RBD: " + rbdVal);
        }
    }

    private void validateTotal(String totalStr, List<String> errors) {
        try {
            Float.parseFloat(totalStr);
        } catch (NumberFormatException e) {
            errors.add("Invalid Total: " + totalStr);
        }
    }

    private void validateCurrency(String currency, List<String> errors) {
        if (currency == null || currency.length() != 3) {
            errors.add("Invalid Currency: " + currency);
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
