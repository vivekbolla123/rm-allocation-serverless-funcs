UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'NDO,W1_threshold,W2_threshold,W3_threshold,W4_threshold,W5_threshold,W6_threshold,W7_threshold,W8_threshold,W9_threshold,G8_threshold,P_threshold,L_threshold,B2B_threshold,S_threshold,flightNumber' WHERE (`tableName` = 'config_pfl_threshold');
UPDATE `QP_DW_RMALLOC`.`rm_parameter_values` SET `parameterValue` = '118' WHERE (`parameterKey` = 'AU_COLUMN_LENGTH');
INSERT INTO `QP_DW_RMALLOC`.`rm_parameter_values` (`parameterKey`, `parameterValue`) VALUES ('SOFTBLOCK_RBDS', '[\"G8\",\"W9\",\"W8\",\"W7\",\"W6\",\"W5\",\"W4\",\"W3\", \"W2\", \"W1\", \"S\"]');
ALTER TABLE `QP_DW_RMALLOC`.`config_pfl_threshold` 
ADD COLUMN `S_threshold` DOUBLE NULL DEFAULT NULL AFTER `G8_threshold`;
