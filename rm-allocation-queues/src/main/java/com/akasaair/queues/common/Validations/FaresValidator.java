package com.akasaair.queues.common.Validations;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static com.akasaair.queues.common.constants.Constants.*;

public class FaresValidator implements Validator {

    @Override
    public boolean isValid(List<Map<String, String>> data, String val) {
        try {
            List<String> rdbValues = Arrays.asList(val.split(COMMA));
            HashSet<String> rbd = new HashSet<>();
            for (Map<String, String> singleData : data) {
                String rbdVal = singleData.get(RBD_COLUMN_NAME);
                rbd.add(rbdVal);
                if (!rdbValues.contains(rbdVal))
                    return false;
            }
            return rbd.size() == rdbValues.size();
        } catch (Exception e) {
            return false;
        }
    }

}
