-- Alter Run Summary
ALTER TABLE `run_summary`
ADD COLUMN `recommended_actions` VARCHAR(10) NULL AFTER `overBookingCount`,;