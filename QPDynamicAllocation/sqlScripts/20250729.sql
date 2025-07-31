ALTER TABLE QP_DW_RMALLOC.market_list ADD COLUMN defaultMlFlag VARCHAR(50) DEFAULT 0 AFTER TBFEnhFlag;

ALTER TABLE QP_DW_RMALLOC.market_list_international ADD COLUMN defaultMlFlag VARCHAR(50) DEFAULT 0 AFTER TBFEnhFlag;

ALTER TABLE QP_DW_RMALLOC.market_list_thirty ADD COLUMN defaultMlFlag VARCHAR(50) DEFAULT 0 AFTER TBFEnhFlag;

ALTER TABLE QP_DW_RMALLOC.market_list_adhoc_International_TP ADD COLUMN defaultMlFlag VARCHAR(50) DEFAULT 0 AFTER TBFEnhFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_JK add column defaultMlFlag VARCHAR(50) DEFAULT 0 AFTER TBFEnhFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_DM add column defaultMlFlag VARCHAR(50) DEFAULT 0 AFTER TBFEnhFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_AD add column defaultMlFlag VARCHAR(50) DEFAULT 0 AFTER TBFEnhFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_JM add column defaultMlFlag VARCHAR(50) DEFAULT 0 AFTER TBFEnhFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_KK add column defaultMlFlag VARCHAR(50) DEFAULT 0 AFTER TBFEnhFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_MM add column defaultMlFlag VARCHAR(50) DEFAULT 0 AFTER TBFEnhFlag;


Alter table QP_DW_RMALLOC.market_list_adhoc_NP add column defaultMlFlag VARCHAR(50) DEFAULT 0 AFTER TBFEnhFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_PQ add column defaultMlFlag VARCHAR(50) DEFAULT 0 AFTER TBFEnhFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_PB add column defaultMlFlag VARCHAR(50) DEFAULT 0 AFTER TBFEnhFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_PRIYANKA add column defaultMlFlag VARCHAR(50) DEFAULT 0 AFTER TBFEnhFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_SC add column defaultMlFlag VARCHAR(50) DEFAULT 0 AFTER TBFEnhFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_Shantaram add column defaultMlFlag VARCHAR(50) DEFAULT 0 AFTER TBFEnhFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_SR add column defaultMlFlag VARCHAR(50) DEFAULT 0 AFTER TBFEnhFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_SS add column defaultMlFlag VARCHAR(50) DEFAULT 0 AFTER TBFEnhFlag;


UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,DATE_FORMAT(STR_TO_DATE(PerStart, \'%m/%d/%Y\'), \'%a\') AS Day,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusionB2C,CarrExlusionB2B,flightExclusionB2C,flightExclusionB2B,fareAnchor,hardAnchor,plfThreshold,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BTolerance,B2CTolerance,B2BBackstop,B2CBackstop,B2BFactor,obSeats,obFare,DaySpan,AutoTimeRangeFlag,openingFares,OverBooking,profileFares,rbdPushFlag,distressInventoryFlag,seriesBlock,autoGroup,autoBackstopFlag,tbfFlag,CugFlag,MktFareFlag,directFlag,TBFEnhFlag,defaultMlFlag,analystName' WHERE (`tableName` = 'market_list');
UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,DATE_FORMAT(STR_TO_DATE(PerStart, \'%m/%d/%Y\'), \'%a\') AS Day,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusionB2C,CarrExlusionB2B,flightExclusionB2C,flightExclusionB2B,fareAnchor,hardAnchor,plfThreshold,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BTolerance,B2CTolerance,B2BBackstop,B2CBackstop,B2BFactor,obSeats,obFare,DaySpan,AutoTimeRangeFlag,openingFares,OverBooking,profileFares,rbdPushFlag,distressInventoryFlag,seriesBlock,autoGroup,autoBackstopFlag,tbfFlag,CugFlag,MktFareFlag,directFlag,TBFEnhFlag,defaultMlFlag,analystName' WHERE (`tableName` = 'market_list_international');
UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,DATE_FORMAT(STR_TO_DATE(PerStart, \'%m/%d/%Y\'), \'%a\') AS Day,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusionB2C,CarrExlusionB2B,flightExclusionB2C,flightExclusionB2B,fareAnchor,hardAnchor,plfThreshold,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BTolerance,B2CTolerance,B2BBackstop,B2CBackstop,B2BFactor,obSeats,obFare,DaySpan,AutoTimeRangeFlag,openingFares,OverBooking,profileFares,rbdPushFlag,distressInventoryFlag,seriesBlock,autoGroup,autoBackstopFlag,tbfFlag,CugFlag,MktFareFlag,directFlag,TBFEnhFlag,defaultMlFlag,analystName' WHERE (`tableName` = 'market_list_thirty');
UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusionB2C,CarrExlusionB2B,flightExclusionB2C,flightExclusionB2B,fareAnchor,hardAnchor,plfThreshold,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BTolerance,B2CTolerance,B2BBackstop,B2CBackstop,B2BFactor,obSeats,obFare,DaySpan,AutoTimeRangeFlag,openingFares,OverBooking,profileFares,rbdPushFlag,distressInventoryFlag,seriesBlock,autoGroup,autoBackstopFlag,tbfFlag,CugFlag,MktFareFlag,directFlag,TBFEnhFlag,defaultMlFlag,analystName' WHERE (`tableName` = 'market_list_s3');

INSERT INTO `QP_DW_RMALLOC`.`rm_parameter_values` (`parameterKey`, `parameterValue`) VALUES ('DEFAULT_MARKET_LIST', '{"Origin": "ABC","Destin": "XYZ","PerStart": "mm/dd/yyyy","PerEnd": "mm/dd/yyyy","FlightNumber": "1234","analystName": "System","DOW": "9999999","PerType": "D","TimeWindowStart": "17:00","TimeWindowEnd": "20:00","CurveID": "BOMDEL_C4","CarrExlusionB2C": "**","CarrExlusionB2B": "**","flightExclusionB2C": "","flightExclusionB2B": "","fareAnchor": "METRO_STG_MED1","hardAnchor": "","plfThreshold": "","fareOffset": "","FirstRBDAlloc": "-99","OtherRBDAlloc": "10","SkippingFactor": "5","B2BTolerance": "","B2CTolerance": "","B2BBackstop": "4300","B2CBackstop": "4500","B2BFactor": "0","obSeats": "","obFare": "","DaySpan": "0","AutoTimeRangeFlag": "0","openingFares": "1","OverBooking": "1","profileFares": "1","rbdPushFlag": "0","distressInventoryFlag": "0","seriesBlock": "0","autoGroup": "0","autoBackstopFlag": "1","tbfFlag": "1","CugFlag": "0","MktFareFlag": "1","directFlag": "1","TBFEnhFlag": "0", "UUID": "uuid", "defaultMlFlag": "1"}');

INSERT INTO `QP_DW_RMALLOC`.`rm_parameter_values` (`parameterKey`, `parameterValue`) VALUES ('DEFAULT_MARKET_LIST_DAYS', '30');


ALTER TABLE `QP_DW_RMALLOC`.`run_summary` 
ADD COLUMN `forecastPLF` VARCHAR(45) AFTER `mktFile`,
ADD COLUMN `FirstRBDAlloc` VARCHAR(50) AFTER `forecastPLF`,
ADD COLUMN `OtherRBDAlloc` VARCHAR(50) AFTER `FirstRBDAlloc`;


UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FltNum,DepDate,DATE_FORMAT(STR_TO_DATE(DepDate, \'%m/%d/%Y\'), \'%a\') AS Day,BookedLoad,TgtLoad as StartOfDayTgtLoad,bookedPlf,actualbookedPlf,Variance,currentTgtLoad,currentLoadFactor as currentTgtLoadFactor ,endOfDayTgtLoad,bookingToday,bookingInHour,strategyFare,hardAnchorFare,lastSellingFare,profileFare,lastSevenAvgFare as last7AvgFare,upsell,fareAnchor,OpenRBD,SellingFare,Channel,HowDetermined,RunId,CreatedAt,MktFare_Min,FlightNumber_Min,MktFare_Max,FlightNumber_Max,criteria,fareOffset,TimeRange,startTime,endTime,Backstop,autoBackstop,StrategyReference,dlfBand as startOfDayDplfBand,endOfDaydlfBand as currentDplfBand,ndoBand,overBookingCount,recommendedActions,status,w0Flag,analystName,mktFile,forecastPLF,FirstRBDAlloc,OtherRBDAlloc' WHERE (`tableName` = 'run_summary');