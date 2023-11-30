-- Alter Run Summary
ALTER TABLE `navitaire_allocation_audit`
ADD COLUMN `b2cRunId` VARCHAR(45) NULL AFTER `run_id`,
ADD COLUMN `b2bRunId` VARCHAR(45) NULL AFTER `b2cRunId`,
ADD COLUMN `failures` INT NULL AFTER `b2bRunId`,
ADD COLUMN `totalApiCalls` INT NULL AFTER `failures`;