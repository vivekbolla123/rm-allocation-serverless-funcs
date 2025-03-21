CREATE DATABASE QP_RM_ARCHIVES;

CREATE TABLE `QP_DW_RMALLOC`.`cleanup_config` (
  `schema_name` VARCHAR(50) NULL,
  `table_name` VARCHAR(50) NULL,
  `time_column` VARCHAR(50) NULL,
  `max_days` INT NULL);