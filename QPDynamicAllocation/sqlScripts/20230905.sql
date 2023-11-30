ALTER TABLE `run_summary` 
ADD COLUMN `TimeRange` VARCHAR(45) NULL AFTER `fareOffset`,
ADD COLUMN `dlfBand` VARCHAR(45) NULL AFTER `TimeRange`,
ADD COLUMN `ndoBand` VARCHAR(45) NULL AFTER `dlfBand`;
