ALTER TABLE `allocation_run_audit` 
ADD COLUMN `actual_end_time` DATETIME NULL AFTER `market_list_name`,
ADD COLUMN `is_connections_required` VARCHAR(3) NULL DEFAULT 0 AFTER `actual_end_time`,
ADD COLUMN `update_navitaire_method` VARCHAR(5) NULL DEFAULT 'api' AFTER `is_connections_required`,
ADD COLUMN `is_sftp_pushed` VARCHAR(5) NULL DEFAULT 0 AFTER `update_navitaire_method`,
ADD COLUMN `is_connections_started` VARCHAR(45) NULL DEFAULT 0 AFTER `is_sftp_pushed`;


CREATE TABLE `run_flight_date_audit` (
  `rowId` VARCHAR(75) NOT NULL,
  `runId` VARCHAR(45) NULL,
  `flightNumber` VARCHAR(45) NULL,
  `flightDate` VARCHAR(45) NULL,
  `status` VARCHAR(45) NULL,
  `result` TEXT NULL,
  PRIMARY KEY (`rowId`));