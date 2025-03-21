CREATE TABLE `QP_DW_RMALLOC`.`config_marketFares_rerouting` (
  `sector` VARCHAR(6) NOT NULL,
  `routedSector` VARCHAR(6) NULL,
  PRIMARY KEY (`sector`));

INSERT INTO `QP_DW_RMALLOC`.`config_marketFares_rerouting` (`sector`, `routedSector`) VALUES ('AMDGWL', 'BOMGWL');
INSERT INTO `QP_DW_RMALLOC`.`config_marketFares_rerouting` (`sector`, `routedSector`) VALUES ('GWLAMD', 'GWLBOM');
