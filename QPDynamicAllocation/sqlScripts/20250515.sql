ALTER TABLE `QP_DW_RMALLOC`.`run_summary` 
ADD COLUMN `mktFile` VARCHAR(60) NULL AFTER `w0Flag`;

ALTER TABLE `QP_DW_RMALLOC`.`run_summary_connections` 
ADD COLUMN `mktFile` VARCHAR(60) NULL AFTER `conversionRate`;

ALTER TABLE `QP_DW_RMALLOC`.`market_fares`  RENAME TO `QP_DW_RMALLOC`.`market_fares_rg`;

ALTER TABLE `QP_DW_RMALLOC`.`market_fares_worker`  RENAME TO `QP_DW_RMALLOC`.`market_fares_rg_worker`;

UPDATE `QP_DW_RMALLOC`.`inputs_status` SET `name` = 'market_fares_rg' WHERE (`name` = 'market_fares');

CREATE TABLE `QP_DW_RMALLOC`.`run_paused` (
  `run_id` varchar(255) NOT NULL,
  `record_id` varchar(255) NOT NULL,
  `createAt` datetime NOT NULL,
  `value` longtext NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `QP_DW_RMALLOC`.`allocation_run_operation_log` (
  `operation_id` bigint NOT NULL AUTO_INCREMENT,
  `operation` varchar(50) NOT NULL,
  `operation_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `user` varchar(100) NOT NULL,
  `run_id` varchar(100) DEFAULT NULL,
  `run_timestamp` timestamp NULL DEFAULT NULL,
  `dtd` varchar(25) DEFAULT NULL,
  `marketlist_name` varchar(50) DEFAULT NULL,
  `region` varchar(25) DEFAULT NULL,
  `type` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`operation_id`)
) ENGINE=InnoDB AUTO_INCREMENT=63 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`, `Permission`) VALUES ('106', 'view_allocation_live');
INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`, `Permission`) VALUES ('107', 'write_allocation_live');
INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`, `Permission`) VALUES ('108', 'view_tbf_segment_mapping');
INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`, `Permission`) VALUES ('109', 'update_tbf_segment_mapping');

UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"ADMIN\", \"permissions\": [1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 52, 53, 54, 55, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109]}' WHERE (`Role` = 'ADMIN');
UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"INVENTORY ANALYST\", \"permissions\": [1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 20, 22, 23, 24, 25, 26, 27, 28, 29, 33, 34, 36, 99, 41, 47, 53, 54, 55, 59, 62, 63, 66, 72, 74, 75, 94, 95, 90, 91, 103, 104, 105, 106, 108, 109]}' WHERE (`Role` = 'INVENTORYANALYST');
UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"INVENTORY ANALYST\", \"permissions\": [1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 20, 22, 23, 24, 25, 26, 27, 28, 29, 33, 34, 36, 37, 41, 47, 53, 54, 55, 59, 62, 63, 66, 72, 74, 75, 103, 104, 105, 106, 108, 109]}' WHERE (`Role` = 'INVENTORYANALYST2');
UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"MANAGER\", \"permissions\": [1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 52, 53, 54, 55, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 98, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109]}' WHERE (`Role` = 'MANAGER');

ALTER TABLE `QP_DW_RMALLOC`.`allocation_run_audit` 
ADD COLUMN `run_status` VARCHAR(45) NULL AFTER `sftp_retry_count`;

INSERT INTO `QP_DW_RMALLOC`.`config_inputs` (`tableName`, `value`, `title`, `view_permission`, `upload_permission`, `message`, `order`) VALUES ('tbfSegmentMapping', 'tbfSegmentMapping', 'TBF Segment map', 'view_tbf_segment_mapping', 'update_tbf_segment_mapping', 'Will drop the entire table and insert the entire file uploaded.', '29');
INSERT INTO `QP_DW_RMALLOC`.`config_column_names` (`tableName`, `columns`) VALUES ('tbf_segment_mapping', 'Sector,Airline_Retention,Level');

