package com.akasaair.queues.common.Validations;

import java.util.*;

import static com.akasaair.queues.common.constants.Constants.*;

public class FaresValidator implements Validator {

    @Override
    public boolean isValid(List<Map<String, String>> data, String val) {
        try {
            List<String> rdbValues = Arrays.asList(val.split(COMMA));
            HashSet<String> rbd = new HashSet<>();
            Map<String, Integer> hashMap = new HashMap<>();
            for (Map<String, String> singleData : data) {
                String rbdVal = singleData.get(RBD_COLUMN_NAME);
                String key = singleData.get(SECTOR_COLUMN_NAME) + singleData.get(ROUTE_COLUMN_NAME);
                if (!hashMap.containsKey(key)) {
                    hashMap.put(key, 0);
                }
                hashMap.put(key, hashMap.get(key) + 1);
                if (!rdbValues.contains(rbdVal))
                    return false;

            }
            return eachElement(hashMap,rdbValues.size());
        } catch (Exception e) {
            return false;
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
