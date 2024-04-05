CREATE TABLE config_user_sector_map (
    sector VARCHAR(50) PRIMARY KEY,
    user1 VARCHAR(50),
    user2 VARCHAR(50)
);



CREATE TABLE `config_schedulers` (
  `schedulerName` varchar(50) NOT NULL,
  `allocationType` varchar(1) DEFAULT NULL,
  `description` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`schedulerName`)
)

INSERT INTO `config_schedulers` VALUES ('rm-allocation-report-gen','S','Report Generation'),('rm-allocation-sftp-error','S','Calculation of SFTP Error'),('rm-cleanup-schedule','S','Cleanup'),('rm-day-schedule-int-rule-1','I','Running for NDO 0-30'),('rm-day-schedule-int-rule-2','I','Running for NDO 31-90'),('rm-day-schedule-int-rule-3','I','Running for NDO 91-120'),('rm-day-schedule-int-rule-4','I','Running for NDO 121-365'),('rm-day-schedule-rule-1','D','Running for NDO 0-7'),('rm-day-schedule-rule-2','D','Running for NDO 8-14'),('rm-day-schedule-rule-3','D','Running for NDO 15-22'),('rm-day-schedule-rule-4','D','Running for NDO 22-90'),('rm-day-schedule-rule-5','D','Running for NDO 91-120'),('rm-day-schedule-rule-6','D','Running for NDO 121-180'),('rm-day-schedule-rule-7','D','Running for NDO 181-240'),('rm-day-schedule-rule-8','D','Running for NDO 241-300'),('rm-day-schedule-rule-9','D','Running for NDO 301-365'),('rm-scheduled-jobs-schedule','S','Scheduled Jobs');

INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`) VALUES ('66');
INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`) VALUES ('67');
INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`) VALUES ('68');
INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`) VALUES ('69');
INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`) VALUES ('70');
INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`) VALUES ('71');

UPDATE `QP_DW_RMALLOC`.`config_permissions` SET `Permission` = 'view_sector_analyst_map' WHERE (`PermissionID` = '66');
UPDATE `QP_DW_RMALLOC`.`config_permissions` SET `Permission` = 'update_sector_analyst_map' WHERE (`PermissionID` = '67');
UPDATE `QP_DW_RMALLOC`.`config_permissions` SET `Permission` = 'delete_marketList_international' WHERE (`PermissionID` = '68');
UPDATE `QP_DW_RMALLOC`.`config_permissions` SET `Permission` = 'delete_curve' WHERE (`PermissionID` = '69');
UPDATE `QP_DW_RMALLOC`.`config_permissions` SET `Permission` = 'delete_strategy' WHERE (`PermissionID` = '70');
UPDATE `QP_DW_RMALLOC`.`config_permissions` SET `Permission` = 'view_delete_page' WHERE (`PermissionID` = '71');

UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"ADMIN\", \"permissions\": [1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 52, 53, 54, 55, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71]}' WHERE (`Role` = 'ADMIN');
UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"INVENTORY ANALYST\", \"permissions\": [1, 2, 4, 6, 7, 8, 9, 10, 11, 20, 22, 23, 24, 25, 26, 27, 28, 29, 33, 34, 36, 38, 41, 47, 53, 54, 55, 58, 59, 60, 61, 62, 63, 66]}' WHERE (`Role` = 'INVENTORYANALYST');
UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"INVENTORY ANALYST\", \"permissions\": [1, 2, 4, 6, 7, 8, 9, 10, 11, 20, 22, 23, 24, 25, 26, 27, 28, 29, 33, 34, 36, 37, 41, 47, 53, 54, 55, 58, 59, 60, 61, 62, 63, 66]}' WHERE (`Role` = 'INVENTORYANALYST2');
UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"MANAGER\", \"permissions\": [1, 2, 3, 4, 5, 6, 7, 8, 9, 20, 21, 22, 23, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 41, 44, 45, 46, 47, 48, 49, 50, 52, 53, 54, 55, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71]}' WHERE (`Role` = 'MANAGER');


