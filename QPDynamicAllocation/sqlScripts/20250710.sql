INSERT INTO `QP_DW_RMALLOC`.`rm_parameter_values` VALUES 
('fare_filter_allfares', '{"excluded_class_prefixes": ["W"], "excluded_classes": ["G9", "G8", "G7"]}'),
('fare_filter_b2bfares', '{"excluded_class_prefixes": ["W", "G"], "included_fare_basis": ["M"]}'),
('fare_filter_b2cfares', '{"excluded_class_prefixes": ["W", "G"], "included_fare_basis": ["R", "C", "V", "Z"]}');


UPDATE `QP_DW_RMALLOC`.`rm_parameter_values` SET `parameterValue` = '3' WHERE (`parameterKey` = 'NAVODS_ALERT_MAX_NDO');