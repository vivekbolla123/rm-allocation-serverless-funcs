package com.akasaair.queues.common.Validations;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.sql.*;
import static com.akasaair.queues.common.constants.QueryConstants.*;

import static com.akasaair.queues.common.constants.Constants.*;

public class CurvesValidator implements Validator {
    private String valueString;
    public CurvesValidator(String valueString) {
            this.valueString = valueString;
    }

    @Override
    public boolean isValid(List<Map<String, String>> data, String val,List<String> errors) {
        try {
            val = this.valueString;
            if (val == null || val.isEmpty() || "None".equals(val)) {
                val = Double.toString(LF_VALUE);
            } 
            HashSet<Integer> ndo = new HashSet<>();
            for (Map<String, String> singleData : data) {
                int ndoVal = Integer.parseInt(singleData.get(NDO_COLUMN_NAME));
                ndo.add(ndoVal);
                double lf = Double.parseDouble(singleData.get(LF_COLUMN_NAME));
                if (lf > Double.parseDouble(val))
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
