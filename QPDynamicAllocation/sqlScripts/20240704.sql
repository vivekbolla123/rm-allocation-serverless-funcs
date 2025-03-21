ALTER TABLE `QP_DW_RMALLOC`.`market_list` 
ADD COLUMN `seriesBlock` VARCHAR(5) NULL DEFAULT 'N' AFTER `distressInventoryFlag`;

ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_AS` 
ADD COLUMN `seriesBlock` VARCHAR(5) NULL DEFAULT 'N' AFTER `distressInventoryFlag`;

ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_DR` 
ADD COLUMN `seriesBlock` VARCHAR(5) NULL DEFAULT 'N' AFTER `distressInventoryFlag`;

ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_GR` 
ADD COLUMN `seriesBlock` VARCHAR(5) NULL DEFAULT 'N' AFTER `distressInventoryFlag`;

ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_JM` 
ADD COLUMN `seriesBlock` VARCHAR(5) NULL DEFAULT 'N' AFTER `distressInventoryFlag`;

ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_JP` 
ADD COLUMN `seriesBlock` VARCHAR(5) NULL DEFAULT 'N' AFTER `distressInventoryFlag`;

ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_KK` 
ADD COLUMN `seriesBlock` VARCHAR(5) NULL DEFAULT 'N' AFTER `distressInventoryFlag`;

ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_MB` 
ADD COLUMN `seriesBlock` VARCHAR(5) NULL DEFAULT 'N' AFTER `distressInventoryFlag`;

ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_MM` 
ADD COLUMN `seriesBlock` VARCHAR(5) NULL DEFAULT 'N' AFTER `distressInventoryFlag`;

ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_NP` 
ADD COLUMN `seriesBlock` VARCHAR(5) NULL DEFAULT 'N' AFTER `distressInventoryFlag`;

ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_PD` 
ADD COLUMN `seriesBlock` VARCHAR(5) NULL DEFAULT 'N' AFTER `distressInventoryFlag`;

ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_SC` 
ADD COLUMN `seriesBlock` VARCHAR(5) NULL DEFAULT 'N' AFTER `distressInventoryFlag`;

ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_SV` 
ADD COLUMN `seriesBlock` VARCHAR(5) NULL DEFAULT 'N' AFTER `distressInventoryFlag`;

ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_TP` 
ADD COLUMN `seriesBlock` VARCHAR(5) NULL DEFAULT 'N' AFTER `distressInventoryFlag`;

ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_VS` 
ADD COLUMN `seriesBlock` VARCHAR(5) NULL DEFAULT 'N' AFTER `distressInventoryFlag`;

ALTER TABLE `QP_DW_RMALLOC`.`market_list_international` 
ADD COLUMN `seriesBlock` VARCHAR(5) NULL DEFAULT 'N' AFTER `distressInventoryFlag`;

CREATE TABLE `QP_DW_RMALLOC`.`series_blocking` (
    DepartureDate VARCHAR(20),
    OrgCode VARCHAR(20),
    SeatsBlocked VARCHAR(5)
);

INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`, `Permission`) VALUES ('92', 'view_series_blocking');
INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`, `Permission`) VALUES ('93', 'update_series_blocking');

UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"ADMIN\", \"permissions\": [1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 52, 53, 54, 55, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93]}' WHERE (`Role` = 'ADMIN');
UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"INVENTORY ANALYST\", \"permissions\": [1, 2, 4, 6, 7, 8, 9, 10, 11, 20, 22, 23, 24, 25, 26, 27, 28, 29, 33, 34, 36, 38, 41, 47, 53, 54, 55, 59, 60, 61, 62, 63, 66, 72, 74, 75, 76, 77, 78, 79, 80, 84, 85, 88, 89, 90, 91, 92, 93]}' WHERE (`Role` = 'INVENTORYANALYST');
UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"INVENTORY ANALYST\", \"permissions\": [1, 2, 4, 6, 7, 8, 9, 10, 11, 20, 22, 23, 24, 25, 26, 27, 28, 29, 33, 34, 36, 37, 41, 47, 53, 54, 55, 59, 60, 61, 62, 63, 66, 72, 74, 75, 76, 77, 78, 79, 80, 84, 85, 88, 89, 90, 91, 92, 93]}' WHERE (`Role` = 'INVENTORYANALYST2');
UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"MANAGER\", \"permissions\": [1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 52, 53, 54, 55, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93]}' WHERE (`Role` = 'MANAGER');

INSERT INTO `QP_DW_RMALLOC`.`config_inputs` (`tableName`, `value`, `title`, `view_permission`, `upload_permission`, `message`, `order`) VALUES ('seriesBlocking', 'seriesBlocking', 'Series Blocking', 'view_series_blocking', 'update_series_blocking', 'Will drop the entire table and insert the entire file uploaded.', '24');

UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,strategyFlag,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusion,fareAnchor,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BTolerance,B2CTolerance,B2BBackstop,B2CBackstop,B2BFactor,OwnFareFlag,DaySpan,AutoTimeRangeFlag,openingFares,OverBooking,profileFares,rbdPushFlag,distressInventoryFlag,seriesBlock,analystName' WHERE (`tableName` = 'market_list');
UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,strategyFlag,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusion,fareAnchor,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BTolerance,B2CTolerance,B2BBackstop,B2CBackstop,B2BFactor,OwnFareFlag,DaySpan,AutoTimeRangeFlag,openingFares,OverBooking,profileFares,rbdPushFlag,distressInventoryFlag,seriesBlock,analystName' WHERE (`tableName` = 'market_list_international');
INSERT INTO `QP_DW_RMALLOC`.`config_column_names` (`tableName`, `columns`) VALUES ('series_blocking', 'DepartureDate,OrgCode,SeatsBlocked');

