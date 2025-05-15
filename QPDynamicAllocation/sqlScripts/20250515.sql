ALTER TABLE `QP_DW_RMALLOC`.`run_summary` 
ADD COLUMN `mktFile` VARCHAR(60) NULL AFTER `w0Flag`;

ALTER TABLE `QP_DW_RMALLOC`.`run_summary_connections` 
ADD COLUMN `mktFile` VARCHAR(60) NULL AFTER `conversionRate`;

ALTER TABLE `QP_DW_RMALLOC`.`market_fares`  RENAME TO `QP_DW_RMALLOC`.`market_fares_rg`;

ALTER TABLE `QP_DW_RMALLOC`.`market_fares_worker`  RENAME TO `QP_DW_RMALLOC`.`market_fares_rg_worker`;

UPDATE `QP_DW_RMALLOC`.`inputs_status` SET `name` = 'market_fares_rg' WHERE (`name` = 'market_fares');

