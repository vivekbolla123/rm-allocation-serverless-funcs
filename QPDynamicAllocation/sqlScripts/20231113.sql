ALTER TABLE `QP_DW_RMALLOC`.`run_flight_date_audit`
ADD COLUMN `createdAt` DATETIME NULL
AFTER `b2cstatus`;