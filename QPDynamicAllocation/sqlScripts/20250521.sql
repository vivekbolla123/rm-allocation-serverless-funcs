UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'NDO,W1_threshold,W2_threshold,W3_threshold,W4_threshold,W5_threshold,W6_threshold,W7_threshold,W8_threshold,W9_threshold,G8_threshold,P_threshold,L_threshold,B2B_threshold,flightNumber' WHERE (`tableName` = 'config_pfl_threshold');

ALTER TABLE `QP_DW_RMALLOC`.`config_pfl_threshold` ADD COLUMN `G8_threshold` DOUBLE NULL DEFAULT NULL AFTER `W9_threshold`;
