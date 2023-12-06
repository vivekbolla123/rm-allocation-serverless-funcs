CREATE TABLE `config_opening_fares` (
  `id` int NOT NULL AUTO_INCREMENT,
  `conditions` text,
  `B2BValue` varchar(45) DEFAULT NULL,
  `B2CValue` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=13 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO `config_opening_fares` VALUES 
(1,'{"start_time":"00:00","end_time":"10:00","start_of_day_dlf_band":"3,4,5,6","end_of_day_dlf_band":"3,4,5,6","ndo_band":"1,2,3,4,5,6,7"}','D','A'),
(2,'{"start_time":"10:01","end_time":"23:59","start_of_day_dlf_band":"3,4,5,6","end_of_day_dlf_band":"3,4,5,6","ndo_band":"1,2,3,4,5,6,7"}','D','A'),
(3,'{"start_time":"00:00","end_time":"10:00","start_of_day_dlf_band":"3,4,5,6","end_of_day_dlf_band":"0,1,2","ndo_band":"1,2,3,4,5,6,7"}','D','A'),
(4,'{"start_time":"10:01","end_time":"23:59","start_of_day_dlf_band":"3,4,5,6","end_of_day_dlf_band":"0,1,2","ndo_band":"1,2,3,4,5,6,7"}','F','C'),
(5,'{"start_time":"00:00","end_time":"10:00","start_of_day_dlf_band":"0,1,2","end_of_day_dlf_band":"3,4,5,6","ndo_band":"1,2,3,4,5,6,7"}','F','C'),
(6,'{"start_time":"10:01","end_time":"23:59","start_of_day_dlf_band":"0,1,2","end_of_day_dlf_band":"3,4,5,6","ndo_band":"1,2,3,4,5,6,7"}','D','A'),
(7,'{"start_time":"00:00","end_time":"10:00","start_of_day_dlf_band":"0,1,2","end_of_day_dlf_band":"0,1,2","ndo_band":"1,2,3,4,5,6,7"}','F','C'),
(8,'{"start_time":"10:01","end_time":"23:59","start_of_day_dlf_band":"0,1,2","end_of_day_dlf_band":"0,1,2","ndo_band":"1,2,3,4,5,6,7"}','B','F'),
(9,'{"departure_start_time":"00:00","departure_end_time":"12:00","min_load_factor":"0","max_load_factor":"95","ndo_band":"0"}','B','B'),
(10,'{"departure_start_time":"00:00","departure_end_time":"12:00","min_load_factor":"96","max_load_factor":"100","ndo_band":"0"}','D','E'),
(11,'{"departure_start_time":"12:01","departure_end_time":"23:59","min_load_factor":"0","max_load_factor":"95","ndo_band":"0"}','B','B'),
(12,'{"departure_start_time":"12:01","departure_end_time":"23:59","min_load_factor":"96","max_load_factor":"100","ndo_band":"0"}','D','E');

CREATE TABLE `config_share_of_booking` (
  `hours` INT NOT NULL,
  `shareOfBooking` VARCHAR(45) NULL,
  PRIMARY KEY (`hours`));

INSERT INTO `config_share_of_booking` (`hours`, `shareOfBooking`) VALUES ('0', '2.50');
INSERT INTO `config_share_of_booking` (`hours`, `shareOfBooking`) VALUES ('1', '1.26');
INSERT INTO `config_share_of_booking` (`hours`, `shareOfBooking`) VALUES ('2', '0.55');
INSERT INTO `config_share_of_booking` (`hours`, `shareOfBooking`) VALUES ('3', '0.30');
INSERT INTO `config_share_of_booking` (`hours`, `shareOfBooking`) VALUES ('4', '0.18');
INSERT INTO `config_share_of_booking` (`hours`, `shareOfBooking`) VALUES ('5', '0.21');
INSERT INTO `config_share_of_booking` (`hours`, `shareOfBooking`) VALUES ('6', '0.48');
INSERT INTO `config_share_of_booking` (`hours`, `shareOfBooking`) VALUES ('7', '1.25');
INSERT INTO `config_share_of_booking` (`hours`, `shareOfBooking`) VALUES ('8', '2.29');
INSERT INTO `config_share_of_booking` (`hours`, `shareOfBooking`) VALUES ('9', '3.73');
INSERT INTO `config_share_of_booking` (`hours`, `shareOfBooking`) VALUES ('10', '5.36');
INSERT INTO `config_share_of_booking` (`hours`, `shareOfBooking`) VALUES ('11', '6.73');
INSERT INTO `config_share_of_booking` (`hours`, `shareOfBooking`) VALUES ('12', '6.92');
INSERT INTO `config_share_of_booking` (`hours`, `shareOfBooking`) VALUES ('13', '6.78');
INSERT INTO `config_share_of_booking` (`hours`, `shareOfBooking`) VALUES ('14', '6.33');
INSERT INTO `config_share_of_booking` (`hours`, `shareOfBooking`) VALUES ('15', '6.29');
INSERT INTO `config_share_of_booking` (`hours`, `shareOfBooking`) VALUES ('16', '6.12');
INSERT INTO `config_share_of_booking` (`hours`, `shareOfBooking`) VALUES ('17', '6.07');
INSERT INTO `config_share_of_booking` (`hours`, `shareOfBooking`) VALUES ('18', '6.27');
INSERT INTO `config_share_of_booking` (`hours`, `shareOfBooking`) VALUES ('19', '6.24');
INSERT INTO `config_share_of_booking` (`hours`, `shareOfBooking`) VALUES ('20', '6.16');
INSERT INTO `config_share_of_booking` (`hours`, `shareOfBooking`) VALUES ('21', '6.32');
INSERT INTO `config_share_of_booking` (`hours`, `shareOfBooking`) VALUES ('22', '6.25');
INSERT INTO `config_share_of_booking` (`hours`, `shareOfBooking`) VALUES ('23', '5.39');

UPDATE `QP_DW_RMALLOC`.`config_code_version` SET `code_version` = '6.0.0' WHERE (`code_version` = '4.0.0');

ALTER TABLE `QP_DW_RMALLOC`.`market_list` 
ADD COLUMN `openingFares` INT NULL AFTER `analystName`;
ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc` 
ADD COLUMN `openingFares` INT NULL AFTER `analystName`;
ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_AS` 
ADD COLUMN `openingFares` INT NULL AFTER `analystName`;
ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_DR` 
ADD COLUMN `openingFares` INT NULL AFTER `analystName`;
ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_JP` 
ADD COLUMN `openingFares` INT NULL AFTER `analystName`;
ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_KK` 
ADD COLUMN `openingFares` INT NULL AFTER `analystName`;
ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_MB` 
ADD COLUMN `openingFares` INT NULL AFTER `analystName`;
ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_mb` 
ADD COLUMN `openingFares` INT NULL AFTER `analystName`;
ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_MM` 
ADD COLUMN `openingFares` INT NULL AFTER `analystName`;
ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_NP` 
ADD COLUMN `openingFares` INT NULL AFTER `analystName`;
ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_SC` 
ADD COLUMN `openingFares` INT NULL AFTER `analystName`;
ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_SR` 
ADD COLUMN `openingFares` INT NULL AFTER `analystName`;
ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_SV` 
ADD COLUMN `openingFares` INT NULL AFTER `analystName`;
ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_Test` 
ADD COLUMN `openingFares` INT NULL AFTER `analystName`;

ALTER TABLE `QP_DW_RMALLOC`.`run_summary` 
ADD COLUMN `endOfDaydlfBand` VARCHAR(45) NULL AFTER `endTime`,
ADD COLUMN `endOfDayLoad` VARCHAR(45) NULL AFTER `endOfDaydlfBand`,
ADD COLUMN `strategyFare` VARCHAR(45) NULL AFTER `endOfDayLoad`,
ADD COLUMN `lastSellingFare` VARCHAR(45) NULL AFTER `strategyFare`,
ADD COLUMN `upsell` VARCHAR(45) NULL AFTER `lastSellingFare`;

UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Id,Origin,Destin,FltNum,DepDate,BookedLoad,endOfDayLoad,TgtLoad,bookedPlf,Variance,strategyFare,lastSellingFare,upsell,anchorFare,OpenRBD,SellingFare,Channel,HowDetermined,RunId,CreatedAt,MktFare_Min,AirlineCode_Min,FlightNumber_Min,MktFare_Max,AirlineCode_Max,FlightNumber_Max,criteria,fareOffset,TimeRange,startTime,endTime,Backstop,StrategyReference,dlfBand,endOfDaydlfBand,ndoBand,analystName' WHERE (`tableName` = 'run_summary');
UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FlightNumber,PerType,PerStart,PerEnd,DOW,strategyFlag,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusion,fareAnchor,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BBackstop,B2CBackstop,B2BFactor,OwnFareFlag,DaySpan,analystName,openingFares' WHERE (`tableName` = 'market_list');
