package com.akasaair.queues.common.Validations;

import java.util.List;
import java.util.Map;

public interface Validator {
    boolean isValid(List<Map<String, String>> data, String value,List<String> errors);
}
