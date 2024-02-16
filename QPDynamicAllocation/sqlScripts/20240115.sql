CREATE TABLE `QP_DW_RMALLOC`.`s3_file_upload` (
  `runId` VARCHAR(255) NOT NULL,
  `createdAt` VARCHAR(45) NULL,
  PRIMARY KEY (`runId`));

ALTER TABLE `QP_DW_RMALLOC`.`allocation_run_audit` 
ADD COLUMN `is_s3_pushed` VARCHAR(3) NULL AFTER `is_audit_completed`;
