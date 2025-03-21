CREATE TABLE `QP_DW_RMALLOC`.`distress_inventory_strategy` (
    ndo_band INT,
    dplf_band INT,
    au_value INT
);

CREATE TABLE `QP_DW_RMALLOC`.`default_distress_inventory` (
    month VARCHAR(20) NOT NULL,
    default_au INT NOT NULL,
    au_cap INT NOT NULL
);

ALTER TABLE `QP_DW_RMALLOC`.`market_list` 
ADD COLUMN `distressInventoryFlag` VARCHAR(5) NULL DEFAULT 0 AFTER `B2BTolerance`;

INSERT INTO `QP_DW_RMALLOC`.`config_column_names` (`tableName`, `columns`) VALUES ('default_distress_inventory', 'month,default_au,au_cap');
INSERT INTO `QP_DW_RMALLOC`.`config_column_names` (`tableName`, `columns`) VALUES ('distress_inventory_strategy', 'ndo_band,dplf_band,au_value');
UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,strategyFlag,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusion,fareAnchor,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BTolerance,B2CTolerance,B2BBackstop,B2CBackstop,B2BFactor,OwnFareFlag,DaySpan,AutoTimeRangeFlag,openingFares,OverBooking,profileFares,rbdPushFlag,distressInventoryFlag,analystName' WHERE (`tableName` = 'market_list');

INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`, `Permission`) VALUES ('84', 'view_distress_strategy');
INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`, `Permission`) VALUES ('85', 'view_default_distress');
INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`, `Permission`) VALUES ('86', 'update_distress_strategy');
INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`, `Permission`) VALUES ('87', 'update_default_distress');

UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"ADMIN\", \"permissions\": [1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 52, 53, 54, 55, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 81, 82, 83, 84, 85, 86, 87]}' WHERE (`Role` = 'ADMIN');
UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"MANAGER\", \"permissions\": [1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 52, 53, 54, 55, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 81, 82, 83, 84, 85, 86, 87]}' WHERE (`Role` = 'MANAGER');
UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"INVENTORY ANALYST\", \"permissions\": [1, 2, 4, 6, 7, 8, 9, 10, 11, 20, 22, 23, 24, 25, 26, 27, 28, 29, 33, 34, 36, 38, 41, 47, 53, 54, 55, 59, 60, 61, 62, 63, 66, 72, 74, 75, 76, 77, 78, 79, 80, 84, 85]}' WHERE (`Role` = 'INVENTORYANALYST');
UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"INVENTORY ANALYST\", \"permissions\": [1, 2, 4, 6, 7, 8, 9, 10, 11, 20, 22, 23, 24, 25, 26, 27, 28, 29, 33, 34, 36, 37, 41, 47, 53, 54, 55, 59, 60, 61, 62, 63, 66, 72, 74, 75, 76, 77, 78, 79, 80, 84, 85]}' WHERE (`Role` = 'INVENTORYANALYST2');