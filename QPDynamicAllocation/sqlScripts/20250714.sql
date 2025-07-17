ALTER TABLE QP_DW_RMALLOC.market_list ADD COLUMN directFlag VARCHAR(50) DEFAULT 1 AFTER MktFareFlag,
ADD COLUMN TBFEnhFlag VARCHAR(50) DEFAULT 0 AFTER directFlag;

ALTER TABLE QP_DW_RMALLOC.market_list_international ADD COLUMN directFlag VARCHAR(50) DEFAULT 1 AFTER MktFareFlag,
ADD COLUMN TBFEnhFlag VARCHAR(50) DEFAULT 0 AFTER directFlag;

ALTER TABLE QP_DW_RMALLOC.market_list_thirty ADD COLUMN directFlag VARCHAR(50) DEFAULT 1 AFTER MktFareFlag,
ADD COLUMN TBFEnhFlag VARCHAR(50) DEFAULT 0 AFTER directFlag;

ALTER TABLE QP_DW_RMALLOC.market_list_adhoc_International_TP ADD COLUMN directFlag VARCHAR(50) DEFAULT 1 AFTER MktFareFlag,
ADD COLUMN TBFEnhFlag VARCHAR(50) DEFAULT 0 AFTER directFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_JK add column directFlag VARCHAR(50) DEFAULT 1 AFTER MktFareFlag,
ADD COLUMN TBFEnhFlag VARCHAR(50) DEFAULT 0 AFTER directFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_DM add column directFlag VARCHAR(50) DEFAULT 1 AFTER MktFareFlag,
ADD COLUMN TBFEnhFlag VARCHAR(50) DEFAULT 0 AFTER directFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_AD add column directFlag VARCHAR(50) DEFAULT 1 AFTER MktFareFlag,
ADD COLUMN TBFEnhFlag VARCHAR(50) DEFAULT 0 AFTER directFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_JM add column directFlag VARCHAR(50) DEFAULT 1 AFTER MktFareFlag,
ADD COLUMN TBFEnhFlag VARCHAR(50) DEFAULT 0 AFTER directFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_KK add column directFlag VARCHAR(50) DEFAULT 1 AFTER MktFareFlag,
ADD COLUMN TBFEnhFlag VARCHAR(50) DEFAULT 0 AFTER directFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_MM add column directFlag VARCHAR(50) DEFAULT 1 AFTER MktFareFlag,
ADD COLUMN TBFEnhFlag VARCHAR(50) DEFAULT 0 AFTER directFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_kushal add column directFlag VARCHAR(50) DEFAULT 1 AFTER MktFareFlag,
ADD COLUMN TBFEnhFlag VARCHAR(50) DEFAULT 0 AFTER directFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_NP add column directFlag VARCHAR(50) DEFAULT 1 AFTER MktFareFlag,
ADD COLUMN TBFEnhFlag VARCHAR(50) DEFAULT 0 AFTER directFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_PQ add column directFlag VARCHAR(50) DEFAULT 1 AFTER MktFareFlag,
ADD COLUMN TBFEnhFlag VARCHAR(50) DEFAULT 0 AFTER directFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_PB add column directFlag VARCHAR(50) DEFAULT 1 AFTER MktFareFlag,
ADD COLUMN TBFEnhFlag VARCHAR(50) DEFAULT 0 AFTER directFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_PRIYANKA add column directFlag VARCHAR(50) DEFAULT 1 AFTER MktFareFlag,
ADD COLUMN TBFEnhFlag VARCHAR(50) DEFAULT 0 AFTER directFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_SC add column directFlag VARCHAR(50) DEFAULT 1 AFTER MktFareFlag,
ADD COLUMN TBFEnhFlag VARCHAR(50) DEFAULT 0 AFTER directFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_Shantaram add column directFlag VARCHAR(50) DEFAULT 1 AFTER MktFareFlag,
ADD COLUMN TBFEnhFlag VARCHAR(50) DEFAULT 0 AFTER directFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_SR add column directFlag VARCHAR(50) DEFAULT 1 AFTER MktFareFlag,
ADD COLUMN TBFEnhFlag VARCHAR(50) DEFAULT 0 AFTER directFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_SS add column directFlag VARCHAR(50) DEFAULT 1 AFTER MktFareFlag,
ADD COLUMN TBFEnhFlag VARCHAR(50) DEFAULT 0 AFTER directFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_SV add column directFlag VARCHAR(50) DEFAULT 1 AFTER MktFareFlag,
ADD COLUMN TBFEnhFlag VARCHAR(50) DEFAULT 0 AFTER directFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_SV_Test add column directFlag VARCHAR(50) DEFAULT 1 AFTER MktFareFlag,
ADD COLUMN TBFEnhFlag VARCHAR(50) DEFAULT 0 AFTER directFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_SV_Test1 add column directFlag VARCHAR(50) DEFAULT 1 AFTER MktFareFlag,
ADD COLUMN TBFEnhFlag VARCHAR(50) DEFAULT 0 AFTER directFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_SV_Test2 add column directFlag VARCHAR(50) DEFAULT 1 AFTER MktFareFlag,
ADD COLUMN TBFEnhFlag VARCHAR(50) DEFAULT 0 AFTER directFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_SV_Test3 add column directFlag VARCHAR(50) DEFAULT 1 AFTER MktFareFlag,
ADD COLUMN TBFEnhFlag VARCHAR(50) DEFAULT 0 AFTER directFlag;

UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,DATE_FORMAT(STR_TO_DATE(PerStart, \'%m/%d/%Y\'), \'%a\') AS Day,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusionB2C,CarrExlusionB2B,flightExclusionB2C,flightExclusionB2B,fareAnchor,hardAnchor,plfThreshold,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BTolerance,B2CTolerance,B2BBackstop,B2CBackstop,B2BFactor,obSeats,obFare,DaySpan,AutoTimeRangeFlag,openingFares,OverBooking,profileFares,rbdPushFlag,distressInventoryFlag,seriesBlock,autoGroup,autoBackstopFlag,tbfFlag,CugFlag,MktFareFlag,directFlag,TBFEnhFlag,analystName' WHERE (`tableName` = 'market_list');
UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,DATE_FORMAT(STR_TO_DATE(PerStart, \'%m/%d/%Y\'), \'%a\') AS Day,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusionB2C,CarrExlusionB2B,flightExclusionB2C,flightExclusionB2B,fareAnchor,hardAnchor,plfThreshold,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BTolerance,B2CTolerance,B2BBackstop,B2CBackstop,B2BFactor,obSeats,obFare,DaySpan,AutoTimeRangeFlag,openingFares,OverBooking,profileFares,rbdPushFlag,distressInventoryFlag,seriesBlock,autoGroup,autoBackstopFlag,tbfFlag,CugFlag,MktFareFlag,directFlag,TBFEnhFlag,analystName' WHERE (`tableName` = 'market_list_international');
UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,DATE_FORMAT(STR_TO_DATE(PerStart, \'%m/%d/%Y\'), \'%a\') AS Day,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusionB2C,CarrExlusionB2B,flightExclusionB2C,flightExclusionB2B,fareAnchor,hardAnchor,plfThreshold,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BTolerance,B2CTolerance,B2BBackstop,B2CBackstop,B2BFactor,obSeats,obFare,DaySpan,AutoTimeRangeFlag,openingFares,OverBooking,profileFares,rbdPushFlag,distressInventoryFlag,seriesBlock,autoGroup,autoBackstopFlag,tbfFlag,CugFlag,MktFareFlag,directFlag,TBFEnhFlag,analystName' WHERE (`tableName` = 'market_list_thirty');
UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusionB2C,CarrExlusionB2B,flightExclusionB2C,flightExclusionB2B,fareAnchor,hardAnchor,plfThreshold,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BTolerance,B2CTolerance,B2BBackstop,B2CBackstop,B2BFactor,obSeats,obFare,DaySpan,AutoTimeRangeFlag,openingFares,OverBooking,profileFares,rbdPushFlag,distressInventoryFlag,seriesBlock,autoGroup,autoBackstopFlag,tbfFlag,CugFlag,MktFareFlag,directFlag,TBFEnhFlag,analystName' WHERE (`tableName` = 'market_list_s3');