CREATE TABLE `QP_DW_RMALLOC`.`scheduled_job_run_audit` (
  `uuid` VARCHAR(45) NOT NULL,
  `module_name` VARCHAR(45) NULL,
  `start_time` DATETIME NULL,
  `end_time` DATETIME NULL,
  PRIMARY KEY (`uuid`));
