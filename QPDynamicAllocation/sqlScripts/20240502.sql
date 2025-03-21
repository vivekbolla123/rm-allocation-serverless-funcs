ALTER TABLE `QP_DW_RMALLOC`.`allocation_run_audit` 
ADD COLUMN `marketListCount` VARCHAR(45) NULL AFTER `region` ;

ALTER TABLE `QP_RM_REPORTS`.`allocation_summary_report` 
ADD COLUMN `marketListCount` VARCHAR(45) NULL AFTER `region`;

ALTER TABLE `QP_DW_RMALLOC`.`config_user_sector_map` 
ADD COLUMN `user3` VARCHAR(45) NULL AFTER `user2`,
ADD COLUMN `user4` VARCHAR(45) NULL AFTER `user3`;

CREATE TABLE market_list_connections (
   Sector1 TEXT,
   Flight1 BIGINT,
   Sector2 TEXT,
   Flight2 BIGINT,
   Pertype TEXT,
   PerStart TEXT,
   PerEnd TEXT,
   DOW BIGINT,
   Price_Strategy BIGINT,
   Discount_Value BIGINT,
   FirstRBDAlloc BIGINT,
   OtherRBDAlloc BIGINT,
   B2BBackstop BIGINT,
   B2CBackstop BIGINT,
   B2BFactor DOUBLE PRECISION,
   SkippingFactor BIGINT,
   UUID TEXT,
   Outbound_stop INT,
   analystName VARCHAR(45)
);

CREATE TABLE run_summary_connections (
   Id VARCHAR(50) PRIMARY KEY,
   Sector1 VARCHAR(6),
   FltNum1 VARCHAR(4),
   Sector2 VARCHAR(6),
   FltNum2 VARCHAR(4),
   BookedLoad VARCHAR(45),
   DepDate VARCHAR(45),
   anchorFare VARCHAR(15),
   OpenRBD VARCHAR(15),
   Channel VARCHAR(15),
   HowDetermined TEXT,
   RunId VARCHAR(50),
   CreatedAt DATETIME,
   allocationStatus INT,
   Backstop VARCHAR(45),
   SellingFare VARCHAR(45),
   BookedLoad1 VARCHAR(45),
   BookedLoad2 VARCHAR(45),
   SellingFare1 VARCHAR(45),
   SellingFare2 VARCHAR(45),
   OpenRBD1 VARCHAR(45),
   Discount VARCHAR(45),
   OpenRBD2 VARCHAR(45),
   analystName VARCHAR(45)
);

CREATE TABLE inputs_file_upload (
   obj_id VARCHAR(50),
   signed_url VARCHAR(500),
   status VARCHAR(100),
   init_timestamp DATETIME,
   in_queue_timestamp DATETIME,
   inprogress_timestamp DATETIME,
   done_timestamp DATETIME,
   username VARCHAR(255),
   role TEXT,
   allowedPermissions VARCHAR(400),
   tableName VARCHAR(100),
   errors LONGTEXT,
   name VARCHAR(45),
   total_rows INT,
   total_time_taken VARCHAR(45),
   route_for_adhoc VARCHAR(45)
);

INSERT INTO `QP_DW_RMALLOC`.`config_column_names` (`tableName`, `columns`) VALUES ('run_summary_connections', 'anchorFare, Backstop, BookedLoad, BookedLoad1, BookedLoad2, Channel, CreatedAt, DepDate, FltNum1, FltNum2, HowDetermined, Id, OpenRBD, OpenRBD1, OpenRBD2, RunId, Sector1, Sector2, SellingFare,Discount, SellingFare1, SellingFare2,analystName');
INSERT INTO `QP_DW_RMALLOC`.`config_column_names` (`tableName`, `columns`) VALUES ('market_list_connections', 'Sector1,Flight1,Sector2,Flight2,Perstart,PerEnd,DOW,Price_Strategy,Discount_Value,FirstRBDAlloc,OtherRBDAlloc,B2BBackstop,B2CBackstop,B2BFactor,SkippingFactor,Outbound_stop,analystName');
INSERT INTO `QP_DW_RMALLOC`.`config_column_names` (`tableName`, `columns`) VALUES ('config_user_sector_map_output', 'sector,user1 as `Direct under 30`, user2 as `Direct above 30`, user3 as `Connection under 30`, user4 as `Connection above 30`');
UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'sector,user1,user2,user3,user4' WHERE (`tableName` = 'config_user_sector_map');

INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`, `Permission`) VALUES ('74', 'view_market_list_connections');
INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`, `Permission`) VALUES ('75', 'update_market_list_connections');
INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`, `Permission`) VALUES ('76', 'view_output_summary_connections');
INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`, `Permission`) VALUES ('77', 'delete_marketList_connections');
INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`, `Permission`) VALUES ('78', 'view_own_connections_adhoc_market_list');
INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`, `Permission`) VALUES ('79', 'update_own_connections_adhoc_market_list');
INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`, `Permission`) VALUES ('80', 'trigger_own_connections_adhoc_market_list');
INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`, `Permission`) VALUES ('81', 'trigger_all_connections_adhoc_market_list');
INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`, `Permission`) VALUES ('82', 'view_all_connections_adhoc_market_list');
INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`, `Permission`) VALUES ('83', 'update_all_connections_adhoc_market_list');

UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"ADMIN\", \"permissions\": [1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 52, 53, 54, 55, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 81, 82, 83]}' WHERE (`Role` = 'ADMIN');
UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"MANAGER\", \"permissions\": [1, 2, 3, 4, 5, 6, 7, 8, 9, 20, 21, 22, 23, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 52, 53, 54, 55, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 81, 82, 83]}' WHERE (`Role` = 'MANAGER');
UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"INVENTORY ANALYST\", \"permissions\": [1, 2, 4, 6, 7, 8, 9, 10, 11, 20, 22, 23, 24, 25, 26, 27, 28, 29, 33, 34, 36, 38, 41, 47, 53, 54, 55, 59, 60, 61, 62, 63, 66, 72, 74, 75, 76, 77, 78, 79, 80]}' WHERE (`Role` = 'INVENTORYANALYST');
UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"INVENTORY ANALYST\", \"permissions\": [1, 2, 4, 6, 7, 8, 9, 10, 11, 20, 22, 23, 24, 25, 26, 27, 28, 29, 33, 34, 36, 37, 41, 47, 53, 54, 55, 59, 60, 61, 62, 63, 66, 72, 74, 75, 76, 77, 78, 79, 80]}' WHERE (`Role` = 'INVENTORYANALYST2');
