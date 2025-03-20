ALTER TABLE QP_DW_RMALLOC.config_pfl_threshold ADD COLUMN W9_threshold double;
ALTER TABLE QP_DW_RMALLOC.config_pfl_threshold ADD COLUMN W7_threshold double;
ALTER TABLE QP_DW_RMALLOC.config_pfl_threshold ADD COLUMN W5_threshold double;
ALTER TABLE QP_DW_RMALLOC.config_pfl_threshold ADD COLUMN W4_threshold double;
ALTER TABLE QP_DW_RMALLOC.config_pfl_threshold ADD COLUMN W3_threshold double;
ALTER TABLE QP_DW_RMALLOC.config_pfl_threshold ADD COLUMN W2_threshold double;
ALTER TABLE QP_DW_RMALLOC.config_pfl_threshold ADD COLUMN W1_threshold double;
ALTER TABLE QP_DW_RMALLOC.config_pfl_threshold ADD COLUMN P_threshold double;
ALTER TABLE QP_DW_RMALLOC.config_pfl_threshold ADD COLUMN L_threshold double;

UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'NDO,W1_threshold,W2_threshold,W3_threshold,W4_threshold,W5_threshold,W6_threshold,W7_threshold,W8_threshold,W9_threshold,P_threshold,L_threshold,B2B_threshold,flightNumber' WHERE (`tableName` = 'config_pfl_threshold');

INSERT INTO `QP_DW_RMALLOC`.`rm_parameter_values` (`parameterKey`, `parameterValue`) VALUES ('MFMIN0', '0');
INSERT INTO `QP_DW_RMALLOC`.`rm_parameter_values` (`parameterKey`, `parameterValue`) VALUES ('MFMIN1', '1');
INSERT INTO `QP_DW_RMALLOC`.`rm_parameter_values` (`parameterKey`, `parameterValue`) VALUES ('OVERBOOKING_MANUAL_LF', '100');
INSERT INTO `QP_DW_RMALLOC`.`rm_parameter_values` (`parameterKey`, `parameterValue`) VALUES ('AU_COLUMN_LENGTH', '111');

CREATE TABLE `QP_DW_RMALLOC`.`tbf_discount_grid` (
    Sector VARCHAR(255),
    Month VARCHAR(20),
    TF DECIMAL(10,2),
    TE DECIMAL(10,2),
    TD DECIMAL(10,2),
    TC DECIMAL(10,2),
    TB DECIMAL(10,2),
    TY DECIMAL(10,2)
);

INSERT INTO `QP_DW_RMALLOC`.`config_inputs` (`tableName`, `value`, `title`, `view_permission`, `upload_permission`, `message`, `order`) VALUES ('tbfDiscountGrid', 'tbfDiscountGrid', 'TBF Discount Grid', 'view_tbf_discount_grid', 'update_tbf_discount_grid', 'Will drop the entire table and insert the entire file uploaded.', '26');

INSERT INTO `QP_DW_RMALLOC`.`config_column_names` (`tableName`, `columns`) VALUES ('tbf_discount_grid', 'Sector,Month,TF,TE,TD,TC,TB,TY');

INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`, `Permission`) VALUES ('101', 'view_tbf_discount_grid');
INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`, `Permission`) VALUES ('102', 'update_tbf_discount_grid');

UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"ADMIN\", \"permissions\": [1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 52, 53, 54, 55, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102]}' WHERE (`Role` = 'ADMIN');
UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"INVENTORY ANALYST\", \"permissions\": [1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 20, 22, 23, 24, 25, 26, 27, 28, 29, 33, 34, 36, 41, 47, 53, 54, 55, 59, 60, 61, 62, 63, 66, 72, 74, 75, 76, 77, 78, 79, 80, 84, 85, 88, 89, 90, 91, 92, 93, 94, 95, 98, 99, 100, 101, 102]}' WHERE (`Role` = 'INVENTORYANALYST');
UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"INVENTORY ANALYST\", \"permissions\": [1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 20, 22, 23, 24, 25, 26, 27, 28, 29, 33, 34, 36, 37, 41, 47, 53, 54, 55, 59, 60, 61, 62, 63, 66, 72, 74, 75, 76, 77, 78, 79, 80, 84, 85, 88, 89, 90, 91, 92, 93, 94, 95, 98, 100, 101, 102]}' WHERE (`Role` = 'INVENTORYANALYST2');
UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"INVENTORY PRICING ANALYST\", \"permissions\": [1, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 16, 18, 20, 21, 22, 23, 24, 25, 26, 33, 41, 47, 51, 53, 54, 55, 59, 56, 57, 60, 62, 63, 64, 65, 66, 72, 74, 75, 76, 88, 92, 100, 101, 102]}' WHERE (`Role` = 'INVENTORYPRICINGANALYST');
UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"MANAGER\", \"permissions\": [1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 52, 53, 54, 55, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 98, 100, 101, 102]}' WHERE (`Role` = 'MANAGER');
