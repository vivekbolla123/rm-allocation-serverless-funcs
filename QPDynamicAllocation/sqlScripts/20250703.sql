INSERT INTO `QP_DW_RMALLOC`.`rm_parameter_values` (`parameterKey`, `parameterValue`) VALUES ('NAVODS_ALERT_SECTOR_LIST', '[\"BOMDEL1127\", \"BOMDEL1120\", \"DELBOM1719\", \"DELBOM1411\", \"BOMDBR1529\", \"DBRBOM1530\", \"CCUBOM1125\", \"CCUBOM1173\"]');
INSERT INTO `QP_DW_RMALLOC`.`rm_parameter_values` (`parameterKey`, `parameterValue`) VALUES ('NAVODS_ALERT_MAX_NDO', '1');

UPDATE `QP_DW_RMALLOC`.`config_inputs` SET `tableName` = 'd1_d2_strategies', `value` = 'd1_d2_strategies' WHERE (`tableName` = 'strategy') and (`value` = 'strategy');