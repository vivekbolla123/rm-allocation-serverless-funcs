INSERT INTO `QP_DW_RMALLOC`.`rm_parameter_values` (`parameterKey`, `parameterValue`) VALUES ('IMP_FORECAST_DEDUCTION', '2');
INSERT INTO `QP_DW_RMALLOC`.`rm_parameter_values` (`parameterKey`, `parameterValue`) VALUES ('TOLERANCE_MAP', '{\"4\": -1, \"3\": 10, \"2\": 15, \"1\": 25, \"0\": 25}');
INSERT INTO `QP_DW_RMALLOC`.`rm_parameter_values` (`parameterKey`, `parameterValue`) VALUES ('B2B_FACTOR_MAP', '{\"4\": 0, \"3\": 0, \"2\": 0.5, \"1\": 1, \"0\": 1}');


CREATE TABLE `QP_DW_RMALLOC`.`imputed_forecast_bands` (
  `band` int NOT NULL,
  `start` int DEFAULT NULL,
  `end` int DEFAULT NULL,
  PRIMARY KEY (`band`)
);

INSERT INTO `QP_DW_RMALLOC`.`imputed_forecast_bands` (`band`, `start`, `end`) VALUES ('0', '-9999', '79');
INSERT INTO `QP_DW_RMALLOC`.`imputed_forecast_bands` (`band`, `start`, `end`) VALUES ('1', '80', '87');
INSERT INTO `QP_DW_RMALLOC`.`imputed_forecast_bands` (`band`, `start`, `end`) VALUES ('2', '88', '92');
INSERT INTO `QP_DW_RMALLOC`.`imputed_forecast_bands` (`band`, `start`, `end`) VALUES ('3', '93', '100');
INSERT INTO `QP_DW_RMALLOC`.`imputed_forecast_bands` (`band`, `start`, `end`) VALUES ('4', '101', '9999');


INSERT INTO `QP_DW_RMALLOC`.`config_column_names` (`tableName`, `columns`) VALUES ('imputed_forecast_bands', 'band,start,end');

INSERT INTO `QP_DW_RMALLOC`.`config_inputs` (`tableName`, `value`, `title`, `view_permission`, `upload_permission`, `message`, `order`) VALUES ('imputed_forecast_bands', 'imputed_forecast_bands', 'Imputed Forecast Bands', 'view_imp_forecast_bands', 'update_imp_forecast_bands', 'Will drop the entire table and insert the entire file uploaded.', '36');

INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`, `Permission`) VALUES ('128', 'view_imp_forecast_bands');
INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`, `Permission`) VALUES ('129', 'update_imp_forecast_bands');

UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"ADMIN\", \"permissions\": [1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 52, 53, 54, 55, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129]}' WHERE (`Role` = 'ADMIN');
UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"MANAGER\", \"permissions\": [1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 52, 53, 54, 55, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 98, 100, 101, 102, 103, 104, 105, 107, 108, 109, 110, 106, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129]}' WHERE (`Role` = 'MANAGER');



-- Ml Flag Addition Changes
ALTER TABLE QP_DW_RMALLOC.market_list ADD COLUMN forecastAllocFlag VARCHAR(50) DEFAULT 0 AFTER Outbound_Stop;

ALTER TABLE QP_DW_RMALLOC.market_list_international ADD COLUMN forecastAllocFlag VARCHAR(50) DEFAULT 0 AFTER Outbound_Stop;

ALTER TABLE QP_DW_RMALLOC.market_list_thirty ADD COLUMN forecastAllocFlag VARCHAR(50) DEFAULT 0 AFTER Outbound_Stop;

ALTER TABLE QP_DW_RMALLOC.market_list_adhoc_International_TP ADD COLUMN forecastAllocFlag VARCHAR(50) DEFAULT 0 AFTER Outbound_Stop;

ALTER TABLE QP_DW_RMALLOC.market_list_adhoc_International_vijay ADD COLUMN forecastAllocFlag VARCHAR(50) DEFAULT 0 AFTER Outbound_Stop;

ALTER TABLE QP_DW_RMALLOC.market_list_adhoc_International_KM ADD COLUMN forecastAllocFlag VARCHAR(50) DEFAULT 0 AFTER Outbound_Stop;

Alter table QP_DW_RMALLOC.market_list_adhoc_JK add column forecastAllocFlag VARCHAR(50) DEFAULT 0 AFTER Outbound_Stop;

Alter table QP_DW_RMALLOC.market_list_adhoc_DM add column forecastAllocFlag VARCHAR(50) DEFAULT 0 AFTER Outbound_Stop;

Alter table QP_DW_RMALLOC.market_list_adhoc_AD add column forecastAllocFlag VARCHAR(50) DEFAULT 0 AFTER Outbound_Stop;

Alter table QP_DW_RMALLOC.market_list_adhoc_JM add column forecastAllocFlag VARCHAR(50) DEFAULT 0 AFTER Outbound_Stop;

Alter table QP_DW_RMALLOC.market_list_adhoc_KK add column forecastAllocFlag VARCHAR(50) DEFAULT 0 AFTER Outbound_Stop;

Alter table QP_DW_RMALLOC.market_list_adhoc_MM add column forecastAllocFlag VARCHAR(50) DEFAULT 0 AFTER Outbound_Stop;


Alter table QP_DW_RMALLOC.market_list_adhoc_NP add column forecastAllocFlag VARCHAR(50) DEFAULT 0 AFTER Outbound_Stop;

Alter table QP_DW_RMALLOC.market_list_adhoc_PQ add column forecastAllocFlag VARCHAR(50) DEFAULT 0 AFTER Outbound_Stop;

Alter table QP_DW_RMALLOC.market_list_adhoc_PO add column forecastAllocFlag VARCHAR(50) DEFAULT 0 AFTER Outbound_Stop;

Alter table QP_DW_RMALLOC.market_list_adhoc_PB add column forecastAllocFlag VARCHAR(50) DEFAULT 0 AFTER Outbound_Stop;

Alter table QP_DW_RMALLOC.market_list_adhoc_PRIYANKA add column forecastAllocFlag VARCHAR(50) DEFAULT 0 AFTER Outbound_Stop;

Alter table QP_DW_RMALLOC.market_list_adhoc_SC add column forecastAllocFlag VARCHAR(50) DEFAULT 0 AFTER Outbound_Stop;

Alter table QP_DW_RMALLOC.market_list_adhoc_Shantaram add column forecastAllocFlag VARCHAR(50) DEFAULT 0 AFTER Outbound_Stop;

Alter table QP_DW_RMALLOC.market_list_adhoc_SR add column forecastAllocFlag VARCHAR(50) DEFAULT 0 AFTER Outbound_Stop;

Alter table QP_DW_RMALLOC.market_list_adhoc_SS add column forecastAllocFlag VARCHAR(50) DEFAULT 0 AFTER Outbound_Stop;

Alter table QP_DW_RMALLOC.market_list_adhoc_MRUNAL add column forecastAllocFlag VARCHAR(50) DEFAULT 0 AFTER Outbound_Stop;

Alter table QP_DW_RMALLOC.market_list_adhoc_Ovais add column forecastAllocFlag VARCHAR(50) DEFAULT 0 AFTER Outbound_Stop;

Alter table QP_DW_RMALLOC.market_list_adhoc_OVAIS add column forecastAllocFlag VARCHAR(50) DEFAULT 0 AFTER Outbound_Stop;

UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,DATE_FORMAT(STR_TO_DATE(PerStart, \'%m/%d/%Y\'), \'%a\') AS Day,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusionB2C,CarrExlusionB2B,flightExclusionB2C,flightExclusionB2B,fareAnchor,hardAnchor,plfThreshold,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BTolerance,B2CTolerance,B2BBackstop,B2CBackstop,B2BFactor,obSeats,obFare,DaySpan,AutoTimeRangeFlag,openingFares,OverBooking,profileFares,rbdPushFlag,distressInventoryFlag,seriesBlock,autoGroup,autoBackstopFlag,tbfFlag,CugFlag,MktFareFlag,directFlag,TBFEnhFlag,defaultMlFlag,b2bStrategy,Outbound_Stop,Marketfare_Upsell,forecastAllocFlag,analystName' WHERE (`tableName` = 'market_list');
UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,DATE_FORMAT(STR_TO_DATE(PerStart, \'%m/%d/%Y\'), \'%a\') AS Day,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusionB2C,CarrExlusionB2B,flightExclusionB2C,flightExclusionB2B,fareAnchor,hardAnchor,plfThreshold,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BTolerance,B2CTolerance,B2BBackstop,B2CBackstop,B2BFactor,obSeats,obFare,DaySpan,AutoTimeRangeFlag,openingFares,OverBooking,profileFares,rbdPushFlag,distressInventoryFlag,seriesBlock,autoGroup,autoBackstopFlag,tbfFlag,CugFlag,MktFareFlag,directFlag,TBFEnhFlag,defaultMlFlag,b2bStrategy,Outbound_Stop,Marketfare_Upsell,forecastAllocFlag,analystName' WHERE (`tableName` = 'market_list_international');
UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,DATE_FORMAT(STR_TO_DATE(PerStart, \'%m/%d/%Y\'), \'%a\') AS Day,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusionB2C,CarrExlusionB2B,flightExclusionB2C,flightExclusionB2B,fareAnchor,hardAnchor,plfThreshold,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BTolerance,B2CTolerance,B2BBackstop,B2CBackstop,B2BFactor,obSeats,obFare,DaySpan,AutoTimeRangeFlag,openingFares,OverBooking,profileFares,rbdPushFlag,distressInventoryFlag,seriesBlock,autoGroup,autoBackstopFlag,tbfFlag,CugFlag,MktFareFlag,directFlag,TBFEnhFlag,defaultMlFlag,b2bStrategy,Outbound_Stop,Marketfare_Upsell,forecastAllocFlag,analystName' WHERE (`tableName` = 'market_list_thirty');
UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusionB2C,CarrExlusionB2B,flightExclusionB2C,flightExclusionB2B,fareAnchor,hardAnchor,plfThreshold,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BTolerance,B2CTolerance,B2BBackstop,B2CBackstop,B2BFactor,obSeats,obFare,DaySpan,AutoTimeRangeFlag,openingFares,OverBooking,profileFares,rbdPushFlag,distressInventoryFlag,seriesBlock,autoGroup,autoBackstopFlag,tbfFlag,CugFlag,MktFareFlag,directFlag,TBFEnhFlag,defaultMlFlag,b2bStrategy,Outbound_Stop,Marketfare_Upsell,forecastAllocFlag,analystName' WHERE (`tableName` = 'market_list_s3');