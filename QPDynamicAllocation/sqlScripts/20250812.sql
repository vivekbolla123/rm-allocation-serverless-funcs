ALTER TABLE QP_DW_RMALLOC.market_list ADD COLUMN b2bStrategy VARCHAR(50) DEFAULT NULL AFTER defaultMlFlag;

ALTER TABLE QP_DW_RMALLOC.market_list_international ADD COLUMN b2bStrategy VARCHAR(50) DEFAULT NULL AFTER defaultMlFlag;

ALTER TABLE QP_DW_RMALLOC.market_list_thirty ADD COLUMN b2bStrategy VARCHAR(50) DEFAULT NULL AFTER defaultMlFlag;

ALTER TABLE QP_DW_RMALLOC.market_list_adhoc_International_TP ADD COLUMN b2bStrategy VARCHAR(50) DEFAULT NULL AFTER defaultMlFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_JK add column b2bStrategy VARCHAR(50) DEFAULT NULL AFTER defaultMlFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_DM add column b2bStrategy VARCHAR(50) DEFAULT NULL AFTER defaultMlFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_AD add column b2bStrategy VARCHAR(50) DEFAULT NULL AFTER defaultMlFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_JM add column b2bStrategy VARCHAR(50) DEFAULT NULL AFTER defaultMlFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_KK add column b2bStrategy VARCHAR(50) DEFAULT NULL AFTER defaultMlFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_MM add column b2bStrategy VARCHAR(50) DEFAULT NULL AFTER defaultMlFlag;


Alter table QP_DW_RMALLOC.market_list_adhoc_NP add column b2bStrategy VARCHAR(50) DEFAULT NULL AFTER defaultMlFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_PQ add column b2bStrategy VARCHAR(50) DEFAULT NULL AFTER defaultMlFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_PB add column b2bStrategy VARCHAR(50) DEFAULT NULL AFTER defaultMlFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_PRIYANKA add column b2bStrategy VARCHAR(50) DEFAULT NULL AFTER defaultMlFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_SC add column b2bStrategy VARCHAR(50) DEFAULT NULL AFTER defaultMlFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_Shantaram add column b2bStrategy VARCHAR(50) DEFAULT NULL AFTER defaultMlFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_SR add column b2bStrategy VARCHAR(50) DEFAULT NULL AFTER defaultMlFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_SS add column b2bStrategy VARCHAR(50) DEFAULT NULL AFTER defaultMlFlag;


UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,DATE_FORMAT(STR_TO_DATE(PerStart, \'%m/%d/%Y\'), \'%a\') AS Day,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusionB2C,CarrExlusionB2B,flightExclusionB2C,flightExclusionB2B,fareAnchor,hardAnchor,plfThreshold,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BTolerance,B2CTolerance,B2BBackstop,B2CBackstop,B2BFactor,obSeats,obFare,DaySpan,AutoTimeRangeFlag,openingFares,OverBooking,profileFares,rbdPushFlag,distressInventoryFlag,seriesBlock,autoGroup,autoBackstopFlag,tbfFlag,CugFlag,MktFareFlag,directFlag,TBFEnhFlag,defaultMlFlag,b2bStrategy,analystName' WHERE (`tableName` = 'market_list');
UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,DATE_FORMAT(STR_TO_DATE(PerStart, \'%m/%d/%Y\'), \'%a\') AS Day,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusionB2C,CarrExlusionB2B,flightExclusionB2C,flightExclusionB2B,fareAnchor,hardAnchor,plfThreshold,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BTolerance,B2CTolerance,B2BBackstop,B2CBackstop,B2BFactor,obSeats,obFare,DaySpan,AutoTimeRangeFlag,openingFares,OverBooking,profileFares,rbdPushFlag,distressInventoryFlag,seriesBlock,autoGroup,autoBackstopFlag,tbfFlag,CugFlag,MktFareFlag,directFlag,TBFEnhFlag,defaultMlFlag,b2bStrategy,analystName' WHERE (`tableName` = 'market_list_international');
UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,DATE_FORMAT(STR_TO_DATE(PerStart, \'%m/%d/%Y\'), \'%a\') AS Day,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusionB2C,CarrExlusionB2B,flightExclusionB2C,flightExclusionB2B,fareAnchor,hardAnchor,plfThreshold,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BTolerance,B2CTolerance,B2BBackstop,B2CBackstop,B2BFactor,obSeats,obFare,DaySpan,AutoTimeRangeFlag,openingFares,OverBooking,profileFares,rbdPushFlag,distressInventoryFlag,seriesBlock,autoGroup,autoBackstopFlag,tbfFlag,CugFlag,MktFareFlag,directFlag,TBFEnhFlag,defaultMlFlag,b2bStrategy,analystName' WHERE (`tableName` = 'market_list_thirty');
UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusionB2C,CarrExlusionB2B,flightExclusionB2C,flightExclusionB2B,fareAnchor,hardAnchor,plfThreshold,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BTolerance,B2CTolerance,B2BBackstop,B2CBackstop,B2BFactor,obSeats,obFare,DaySpan,AutoTimeRangeFlag,openingFares,OverBooking,profileFares,rbdPushFlag,distressInventoryFlag,seriesBlock,autoGroup,autoBackstopFlag,tbfFlag,CugFlag,MktFareFlag,directFlag,TBFEnhFlag,defaultMlFlag,b2bStrategy,analystName' WHERE (`tableName` = 'market_list_s3');


ALTER TABLE `QP_DW_RMALLOC`.`run_summary` 
ADD COLUMN `B2BFactor` VARCHAR(50) AFTER `OtherRBDAlloc`;


UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FltNum,DepDate,DATE_FORMAT(STR_TO_DATE(DepDate, \'%m/%d/%Y\'), \'%a\') AS Day,BookedLoad,TgtLoad as StartOfDayTgtLoad,bookedPlf,actualbookedPlf,Variance,currentTgtLoad,currentLoadFactor as currentTgtLoadFactor ,endOfDayTgtLoad,bookingToday,bookingInHour,strategyFare,hardAnchorFare,lastSellingFare,profileFare,lastSevenAvgFare as last7AvgFare,upsell,fareAnchor,OpenRBD,SellingFare,Channel,HowDetermined,RunId,CreatedAt,MktFare_Min,FlightNumber_Min,MktFare_Max,FlightNumber_Max,criteria,fareOffset,TimeRange,startTime,endTime,Backstop,autoBackstop,StrategyReference,dlfBand as startOfDayDplfBand,endOfDaydlfBand as currentDplfBand,ndoBand,overBookingCount,recommendedActions,status,w0Flag,analystName,mktFile,forecastPLF,FirstRBDAlloc,OtherRBDAlloc,B2BFactor' WHERE (`tableName` = 'run_summary');


CREATE TABLE `QP_DW_RMALLOC`.`forecast_bands` (
  `forecast_band` int NOT NULL,
  `start` int DEFAULT NULL,
  `end` int DEFAULT NULL,
  PRIMARY KEY (`value`)
);

CREATE TABLE `QP_DW_RMALLOC`.`auto_b2b_factor` (
  `name` varchar(50) NOT NULL,
  `ndo_band` int NOT NULL,
  `forecast_band` int NOT NULL,
  `dplf_band` int NOT NULL,
  `b2b_factor` VARCHAR(25) DEFAULT NULL,
  PRIMARY KEY (`ndo_band`,`dplf_band`,`name`,`forecast_band`)
);

INSERT INTO `QP_DW_RMALLOC`.`config_column_names` (`tableName`, `columns`) VALUES ('auto_b2b_factor', 'name,ndo_band,forecast_band,0,1,2,3,4,5,6');
INSERT INTO `QP_DW_RMALLOC`.`config_column_names` (`tableName`, `columns`) VALUES ('forecast_bands', 'forecast_band,start,end');
INSERT INTO `QP_DW_RMALLOC`.`config_column_names` (`tableName`, `columns`) VALUES ('auto_b2b_input', 'name, ndo_band, forecast_band, dplf_band, b2b_factor');


INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`, `Permission`) VALUES ('123', 'view_auto_b2b_factor');
INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`, `Permission`) VALUES ('124', 'update_auto_b2b_factor');
INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`, `Permission`) VALUES ('125', 'view_forecast_bands');
INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`, `Permission`) VALUES ('126', 'update_forecast_bands');
INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`, `Permission`) VALUES ('127', 'delete_auto_b2b_factor');


INSERT INTO `QP_DW_RMALLOC`.`config_inputs` (`tableName`, `value`, `title`, `view_permission`, `upload_permission`, `message`, `order`) VALUES ('auto_b2b_factor', 'auto_b2b_factor', 'Auto B2B Factor', 'view_auto_b2b_factor', 'update_auto_b2b_factor', 'Will drop and update values based on names uploaded', '34');
INSERT INTO `QP_DW_RMALLOC`.`config_inputs` (`tableName`, `value`, `title`, `view_permission`, `upload_permission`, `message`, `order`) VALUES ('forecast_bands', 'forecast_bands', 'Forecast Bands', 'view_forecast_bands', 'update_forecast_bands', 'Will drop the entire table and insert the entire file uploaded.', '35');


UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"ADMIN\", \"permissions\": [1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 52, 53, 54, 55, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127]}' WHERE (`Role` = 'ADMIN');
UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"INVENTORY ANALYST\", \"permissions\": [1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 20, 22, 23, 24, 25, 26, 27, 28, 29, 33, 34, 36, 99, 41, 47, 53, 54, 55, 59, 62, 63, 66, 72, 74, 75, 94, 95, 90, 91, 103, 104, 105, 107, 109, 110, 118, 119, 123, 124, 125, 126, 127]}' WHERE (`Role` = 'INVENTORYANALYST');
UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"INVENTORY ANALYST\", \"permissions\": [1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 20, 22, 23, 24, 25, 26, 27, 28, 29, 33, 34, 36, 37, 41, 47, 53, 54, 55, 59, 62, 63, 66, 72, 74, 75, 103, 104, 105, 107, 109, 110, 118, 119, 123, 124, 125, 126, 127]}' WHERE (`Role` = 'INVENTORYANALYST2');
UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"MANAGER\", \"permissions\": [1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 52, 53, 54, 55, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 98, 100, 101, 102, 103, 104, 105, 107, 108, 109, 110, 106, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127]}' WHERE (`Role` = 'MANAGER');
