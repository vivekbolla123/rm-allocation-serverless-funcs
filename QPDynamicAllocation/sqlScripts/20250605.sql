ALTER TABLE QP_DW_RMALLOC.market_list ADD COLUMN MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

ALTER TABLE QP_DW_RMALLOC.market_list_international ADD COLUMN MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

ALTER TABLE QP_DW_RMALLOC.market_list_thirty ADD COLUMN MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

ALTER TABLE QP_DW_RMALLOC.market_list_adhoc_International_TP ADD COLUMN MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_JK add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_DM add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_AD add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_JM add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_KK add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_MM add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_kushal add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_NP add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_PQ add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_PB add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_PRIYANKA add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_SC add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_Shantaram add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_SR add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_SS add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_SV add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_SV_Test add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_SV_Test1 add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_SV_Test2 add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_SV_Test3 add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,DATE_FORMAT(STR_TO_DATE(PerStart, \'%m/%d/%Y\'), \'%a\') AS Day,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusionB2C,CarrExlusionB2B,flightExclusionB2C,flightExclusionB2B,fareAnchor,hardAnchor,plfThreshold,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BTolerance,B2CTolerance,B2BBackstop,B2CBackstop,B2BFactor,obSeats,obFare,DaySpan,AutoTimeRangeFlag,openingFares,OverBooking,profileFares,rbdPushFlag,distressInventoryFlag,seriesBlock,autoGroup,autoBackstopFlag,tbfFlag,CugFlag,MktFareFlag,analystName' WHERE (`tableName` = 'market_list');
UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,DATE_FORMAT(STR_TO_DATE(PerStart, \'%m/%d/%Y\'), \'%a\') AS Day,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusionB2C,CarrExlusionB2B,flightExclusionB2C,flightExclusionB2B,fareAnchor,hardAnchor,plfThreshold,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BTolerance,B2CTolerance,B2BBackstop,B2CBackstop,B2BFactor,obSeats,obFare,DaySpan,AutoTimeRangeFlag,openingFares,OverBooking,profileFares,rbdPushFlag,distressInventoryFlag,seriesBlock,autoGroup,autoBackstopFlag,tbfFlag,CugFlag,MktFareFlag,analystName' WHERE (`tableName` = 'market_list_international');
UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,DATE_FORMAT(STR_TO_DATE(PerStart, \'%m/%d/%Y\'), \'%a\') AS Day,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusionB2C,CarrExlusionB2B,flightExclusionB2C,flightExclusionB2B,fareAnchor,hardAnchor,plfThreshold,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BTolerance,B2CTolerance,B2BBackstop,B2CBackstop,B2BFactor,obSeats,obFare,DaySpan,AutoTimeRangeFlag,openingFares,OverBooking,profileFares,rbdPushFlag,distressInventoryFlag,seriesBlock,autoGroup,autoBackstopFlag,tbfFlag,CugFlag,MktFareFlag,analystName' WHERE (`tableName` = 'market_list_thirty');
UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusionB2C,CarrExlusionB2B,flightExclusionB2C,flightExclusionB2B,fareAnchor,hardAnchor,plfThreshold,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BTolerance,B2CTolerance,B2BBackstop,B2CBackstop,B2BFactor,obSeats,obFare,DaySpan,AutoTimeRangeFlag,openingFares,OverBooking,profileFares,rbdPushFlag,distressInventoryFlag,seriesBlock,autoGroup,autoBackstopFlag,tbfFlag,CugFlag,MktFareFlag,analystName' WHERE (`tableName` = 'market_list_s3');
