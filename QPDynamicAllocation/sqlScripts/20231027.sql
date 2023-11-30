ALTER TABLE `QP_DW_RMALLOC`.`run_flight_date_audit` 
ADD COLUMN `b2bstatus` VARCHAR(45) NULL AFTER `result`,
ADD COLUMN `b2cstatus` VARCHAR(45) NULL AFTER `b2bstatus`;
