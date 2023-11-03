ALTER TABLE `QP_DW_RMALLOC`.`allocation_run_audit` 
ADD COLUMN `is_audit_completed` TINYINT NULL DEFAULT 0 AFTER `is_run_completed`;