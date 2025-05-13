package com.akasaair.queues.common.Validations;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.sql.*;
import static com.akasaair.queues.common.constants.QueryConstants.*;

import static com.akasaair.queues.common.constants.Constants.*;

public class CurvesValidator implements Validator {

    @Override
    public boolean isValid(List<Map<String, String>> data, String val,List<String> errors) {
        try {
            val = searchResults(LF_VALUE_KEY);
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

    public String searchResults(String value) {
        String listObj = null;
        try (PreparedStatement pstmt = connection.prepareStatement(GET_PARAMETER_VALUES())) {
            pstmt.setString(1, value);
            try (ResultSet resultSet = pstmt.executeQuery()) {
                if (resultSet.next()) {
                    listObj = resultSet.getString(PARAMETER_VALUE_COLUMN_NAME);
                }
            }
        } catch (SQLException e) {
            System.out.println(DATABASE_ACCESS_ERROR_MESSAGE);
            e.printStackTrace();
        }
        return listObj;
    }

}
