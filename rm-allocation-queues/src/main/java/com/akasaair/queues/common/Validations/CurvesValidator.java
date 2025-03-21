package com.akasaair.queues.common.Validations;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static com.akasaair.queues.common.constants.Constants.*;

public class CurvesValidator implements Validator {

    @Override
    public boolean isValid(List<Map<String, String>> data, String val,List<String> errors) {
        try {
            HashSet<Integer> ndo = new HashSet<>();
            for (Map<String, String> singleData : data) {
                int ndoVal = Integer.parseInt(singleData.get(NDO_COLUMN_NAME));
                ndo.add(ndoVal);
                double lf = Double.parseDouble(singleData.get(LF_COLUMN_NAME));
                if (lf > LF_VALUE)
                    return false;
                if (ndoVal > NDO_SIZE)
                    return false;
            }
            return ndo.size() == NDO_SIZE;
        } catch (Exception e) {
            return false;
        }
    }

}
