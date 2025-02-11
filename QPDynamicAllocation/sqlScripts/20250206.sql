ALTER TABLE `QP_DW_RMALLOC`.`config_pfl_threshold` 
ADD COLUMN `B2B_threshold` DOUBLE NULL AFTER `W6_threshold`;

UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'NDO,W8_threshold,W6_threshold,B2B_threshold,flightNumber' WHERE (`tableName` = 'config_pfl_threshold');

INSERT INTO `QP_DW_RMALLOC`.`rm_parameter_values` (`parameterKey`, `parameterValue`) VALUES ('B2B_DISCOUNT_MAP', '{ "0": "450", "1": "350", "2": "250", "3": "150" }');


