ALTER TABLE QP_DW_RMALLOC.market_list ADD COLUMN obSeats VARCHAR(50) AFTER B2BFactor;
ALTER TABLE QP_DW_RMALLOC.market_list ADD COLUMN obFare VARCHAR(50) AFTER obSeats;

Alter table QP_DW_RMALLOC.market_list_adhoc_JK add column obSeats VARCHAR(50) AFTER B2BFactor;
Alter table QP_DW_RMALLOC.market_list_adhoc_JK add column obFare VARCHAR(50) AFTER obSeats;

Alter table QP_DW_RMALLOC.market_list_adhoc_DM add column obSeats VARCHAR(50) AFTER B2BFactor;
Alter table QP_DW_RMALLOC.market_list_adhoc_DM add column obFare VARCHAR(50) AFTER obSeats;

Alter table QP_DW_RMALLOC.market_list_adhoc_AD add column obSeats VARCHAR(50) AFTER B2BFactor;
Alter table QP_DW_RMALLOC.market_list_adhoc_AD add column obFare VARCHAR(50) AFTER obSeats;

Alter table QP_DW_RMALLOC.market_list_adhoc_JM add column obSeats VARCHAR(50) AFTER B2BFactor;
Alter table QP_DW_RMALLOC.market_list_adhoc_JM add column obFare VARCHAR(50) AFTER obSeats;

Alter table QP_DW_RMALLOC.market_list_adhoc_KK add column obSeats VARCHAR(50) AFTER B2BFactor;
Alter table QP_DW_RMALLOC.market_list_adhoc_KK add column obFare VARCHAR(50) AFTER obSeats;

Alter table QP_DW_RMALLOC.market_list_adhoc_MM add column obSeats VARCHAR(50) AFTER B2BFactor;
Alter table QP_DW_RMALLOC.market_list_adhoc_MM add column obFare VARCHAR(50) AFTER obSeats;

Alter table QP_DW_RMALLOC.market_list_adhoc_kushal add column obSeats VARCHAR(50) AFTER B2BFactor;
Alter table QP_DW_RMALLOC.market_list_adhoc_kushal add column obFare VARCHAR(50) AFTER obSeats;

Alter table QP_DW_RMALLOC.market_list_adhoc_NP add column obSeats VARCHAR(50) AFTER B2BFactor;
Alter table QP_DW_RMALLOC.market_list_adhoc_NP add column obFare VARCHAR(50) AFTER obSeats;

Alter table QP_DW_RMALLOC.market_list_adhoc_PQ add column obSeats VARCHAR(50) AFTER B2BFactor;
Alter table QP_DW_RMALLOC.market_list_adhoc_PQ add column obFare VARCHAR(50) AFTER obSeats;

Alter table QP_DW_RMALLOC.market_list_adhoc_PB add column obSeats VARCHAR(50) AFTER B2BFactor;
Alter table QP_DW_RMALLOC.market_list_adhoc_PB add column obFare VARCHAR(50) AFTER obSeats;

Alter table QP_DW_RMALLOC.market_list_adhoc_PRIYANKA add column obSeats VARCHAR(50) AFTER B2BFactor;
Alter table QP_DW_RMALLOC.market_list_adhoc_PRIYANKA add column obFare VARCHAR(50) AFTER obSeats;

Alter table QP_DW_RMALLOC.market_list_adhoc_SC add column obSeats VARCHAR(50) AFTER B2BFactor;
Alter table QP_DW_RMALLOC.market_list_adhoc_SC add column obFare VARCHAR(50) AFTER obSeats;

Alter table QP_DW_RMALLOC.market_list_adhoc_Shantaram add column obSeats VARCHAR(50) AFTER B2BFactor;
Alter table QP_DW_RMALLOC.market_list_adhoc_Shantaram add column obFare VARCHAR(50) AFTER obSeats;

Alter table QP_DW_RMALLOC.market_list_adhoc_SR add column obSeats VARCHAR(50) AFTER B2BFactor;
Alter table QP_DW_RMALLOC.market_list_adhoc_SR add column obFare VARCHAR(50) AFTER obSeats;

Alter table QP_DW_RMALLOC.market_list_adhoc_SS add column obSeats VARCHAR(50) AFTER B2BFactor;
Alter table QP_DW_RMALLOC.market_list_adhoc_SS add column obFare VARCHAR(50) AFTER obSeats;

Alter table QP_DW_RMALLOC.market_list_adhoc_SV add column obSeats VARCHAR(50) AFTER B2BFactor;
Alter table QP_DW_RMALLOC.market_list_adhoc_SV add column obFare VARCHAR(50) AFTER obSeats;

Alter table QP_DW_RMALLOC.market_list_adhoc_SV_Test add column obSeats VARCHAR(50) AFTER B2BFactor;
Alter table QP_DW_RMALLOC.market_list_adhoc_SV_Test add column obFare VARCHAR(50) AFTER obSeats;

Alter table QP_DW_RMALLOC.market_list_adhoc_SV_Test1 add column obSeats VARCHAR(50) AFTER B2BFactor;
Alter table QP_DW_RMALLOC.market_list_adhoc_SV_Test1 add column obFare VARCHAR(50) AFTER obSeats;

Alter table QP_DW_RMALLOC.market_list_adhoc_SV_Test2 add column obSeats VARCHAR(50) AFTER B2BFactor;
Alter table QP_DW_RMALLOC.market_list_adhoc_SV_Test2 add column obFare VARCHAR(50) AFTER obSeats;

Alter table QP_DW_RMALLOC.market_list_adhoc_SV_Test3 add column obSeats VARCHAR(50) AFTER B2BFactor;
Alter table QP_DW_RMALLOC.market_list_adhoc_SV_Test3 add column obFare VARCHAR(50) AFTER obSeats;

UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,DATE_FORMAT(STR_TO_DATE(PerStart, '%m/%d/%Y'), '%a') AS Day,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusionB2C,CarrExlusionB2B,flightExclusionB2C,flightExclusionB2B,fareAnchor,hardAnchor,plfThreshold,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BTolerance,B2CTolerance,B2BBackstop,B2CBackstop,B2BFactor,obSeats,obFare,DaySpan,AutoTimeRangeFlag,openingFares,OverBooking,profileFares,rbdPushFlag,distressInventoryFlag,seriesBlock,autoGroup,autoBackstopFlag,tbfFlag,analystName' WHERE (`tableName` = 'market_list');
UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,DATE_FORMAT(STR_TO_DATE(PerStart, '%m/%d/%Y'), '%a') AS Day,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusionB2C,CarrExlusionB2B,flightExclusionB2C,flightExclusionB2B,fareAnchor,hardAnchor,plfThreshold,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BTolerance,B2CTolerance,B2BBackstop,B2CBackstop,B2BFactor,obSeats,obFare,DaySpan,AutoTimeRangeFlag,openingFares,OverBooking,profileFares,rbdPushFlag,distressInventoryFlag,seriesBlock,autoGroup,autoBackstopFlag,tbfFlag,analystName' WHERE (`tableName` = 'market_list_s3');
