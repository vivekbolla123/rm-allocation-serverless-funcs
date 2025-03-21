ALTER TABLE `QP_DW_RMALLOC`.`run_summary`
ADD COLUMN `marketFaresSummary` VARCHAR(45);
ALTER TABLE `QP_DW_RMALLOC`.`run_summary`
ADD COLUMN `historicOwnFare` VARCHAR(45) NULL
AFTER `expectedEndOfDayLoadFactor`,
    ADD COLUMN `historicFare` VARCHAR(45) NULL
AFTER `historicOwnFare`,
    ADD COLUMN `timeSectorDrop` VARCHAR(45) NULL
AFTER `historicFare`,    
    ADD COLUMN `overBookingCount` VARCHAR(45) NULL
AFTER `timeSectorDrop`,
    ADD COLUMN `gridDrop` VARCHAR(45) NULL
AFTER `overBookingCount`;

UPDATE `QP_DW_RMALLOC`.`config_column_names`
SET `columns` = 'Origin,Destin,FltNum,DepDate,BookedLoad,TgtLoad,bookedPlf,Variance,expectedEndOfDayBooking as EODExpLoad,expectedEndOfDayLoadFactor as EODExpLoadFactor,endOfDayTgtLoad,bookingToday,historicOwnFare,strategyFare,lastSellingFare,upsell,fareAnchor,OpenRBD,SellingFare,Channel,HowDetermined,RunId,CONVERT_TZ(CreatedAt, \'UTC\', \'ASIA/KOLKATA\') as CreatedAt,historicFare,MktFare_Min,AirlineCode_Min,FlightNumber_Min,MktFare_Max,AirlineCode_Max,FlightNumber_Max,criteria,fareOffset,TimeRange,startTime,endTime,Backstop,StrategyReference,dlfBand,endOfDaydlfBand,ndoBand,timeSectorDrop,gridDrop,overBookingCount,marketFaresSummary,analystName'
WHERE (`tableName` = 'run_summary');

ALTER TABLE `QP_DW_RMALLOC`.`market_list`
ADD COLUMN `OverBooking` VARCHAR(2) NULL DEFAULT 'N'
AFTER `openingFares`;

ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_Aparna` 
ADD COLUMN `OverBooking` VARCHAR(2) NULL DEFAULT 'N' AFTER `openingFares`

ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_AS`
ADD COLUMN `OverBooking` VARCHAR(2) NULL DEFAULT 'N'
AFTER `openingFares`

ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_DR` 
ADD COLUMN `OverBooking` VARCHAR(2) NULL DEFAULT 'N' AFTER `openingFares`

ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_JP`
ADD COLUMN `OverBooking` VARCHAR(2) NULL DEFAULT 'N'
AFTER `openingFares`

ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_KK` 
ADD COLUMN `OverBooking` VARCHAR(2) NULL DEFAULT 'N' AFTER `openingFares`

ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_MB`
ADD COLUMN `OverBooking` VARCHAR(2) NULL DEFAULT 'N'
AFTER `openingFares`

ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_mb` 
ADD COLUMN `OverBooking` VARCHAR(2) NULL DEFAULT 'N' AFTER `openingFares`

ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_MM`
ADD COLUMN `OverBooking` VARCHAR(2) NULL DEFAULT 'N'
AFTER `openingFares`

ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_NP` 
ADD COLUMN `OverBooking` VARCHAR(2) NULL DEFAULT 'N' AFTER `openingFares`

ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_SC`
ADD COLUMN `OverBooking` VARCHAR(2) NULL DEFAULT 'N'
AFTER `openingFares`

ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_SR` 
ADD COLUMN `OverBooking` VARCHAR(2) NULL DEFAULT 'N' AFTER `openingFares`

ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_SV`
ADD COLUMN `OverBooking` VARCHAR(2) NULL DEFAULT 'N'
AFTER `openingFares`

ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_SV_Test` 
ADD COLUMN `OverBooking` VARCHAR(2) NULL DEFAULT 'N' AFTER `openingFares`

ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_SV_Test1`
ADD COLUMN `OverBooking` VARCHAR(2) NULL DEFAULT 'N' AFTER `openingFares`

ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_Test` 
ADD COLUMN `OverBooking` VARCHAR(2) NULL DEFAULT 'N' AFTER `openingFares`

UPDATE `QP_DW_RMALLOC`.`config_column_names`
SET `columns` = 'Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,strategyFlag,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusion,fareAnchor,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BBackstop,B2CBackstop,B2BFactor,OwnFareFlag,DaySpan,AutoTimeRangeFlag,openingFares,OverBooking,analystName'
WHERE (`tableName` = 'market_list');