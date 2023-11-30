ALTER TABLE `market_list` 
ADD COLUMN `SkippingFactor` BIGINT NULL AFTER `UUID`;

ALTER TABLE `market_list_adhoc` 
ADD COLUMN `SkippingFactor` BIGINT NULL AFTER `UUID`;

ALTER TABLE `market_list_adhoc_AS` 
ADD COLUMN `SkippingFactor` BIGINT NULL AFTER `UUID`;

ALTER TABLE `market_list_adhoc_DR` 
ADD COLUMN `SkippingFactor` BIGINT NULL AFTER `UUID`;

ALTER TABLE `market_list_adhoc_JP` 
ADD COLUMN `SkippingFactor` BIGINT NULL AFTER `UUID`;

ALTER TABLE `market_list_adhoc_KK` 
ADD COLUMN `SkippingFactor` BIGINT NULL AFTER `UUID`;

ALTER TABLE `market_list_adhoc_MB` 
ADD COLUMN `SkippingFactor` BIGINT NULL AFTER `UUID`;

ALTER TABLE `market_list_adhoc_mb` 
ADD COLUMN `SkippingFactor` BIGINT NULL AFTER `UUID`;

ALTER TABLE `market_list_adhoc_MM` 
ADD COLUMN `SkippingFactor` BIGINT NULL AFTER `UUID`;

ALTER TABLE `market_list_adhoc_NP` 
ADD COLUMN `SkippingFactor` BIGINT NULL AFTER `UUID`;

ALTER TABLE `market_list_adhoc_SC` 
ADD COLUMN `SkippingFactor` BIGINT NULL AFTER `UUID`;

ALTER TABLE `market_list_adhoc_SV` 
ADD COLUMN `SkippingFactor` BIGINT NULL AFTER `UUID`;

ALTER TABLE `market_list_adhoc_Test` 
ADD COLUMN `SkippingFactor` BIGINT NULL AFTER `UUID`;

ALTER TABLE `market_list_adhoc_undefined` 
ADD COLUMN `SkippingFactor` BIGINT NULL AFTER `UUID`;

ALTER TABLE `run_summary` 
ADD COLUMN `criteria` VARCHAR(5) NULL AFTER `ndoBand`,
CHANGE COLUMN `HowDetermined` `HowDetermined` TEXT NULL DEFAULT NULL ;

UPDATE `config_code_version` SET `code_version` = '4.0.0' WHERE (`code_version` = '3.3.1');
