
Alter table QP_DW_RMALLOC.market_list add column flightExclusionB2C VARCHAR(50) AFTER CarrExlusionB2B;
Alter table QP_DW_RMALLOC.market_list add column flightExclusionB2B VARCHAR(50) AFTER flightExclusionB2C;

Alter table QP_DW_RMALLOC.market_list_adhoc_JK add column flightExclusionB2C VARCHAR(50) AFTER CarrExlusionB2B;
Alter table QP_DW_RMALLOC.market_list_adhoc_JK add column flightExclusionB2B VARCHAR(50) AFTER flightExclusionB2C;

Alter table QP_DW_RMALLOC.market_list_adhoc_JM add column flightExclusionB2C VARCHAR(50) AFTER CarrExlusionB2B;
Alter table QP_DW_RMALLOC.market_list_adhoc_JM add column flightExclusionB2B VARCHAR(50) AFTER flightExclusionB2C;

Alter table QP_DW_RMALLOC.market_list_adhoc_KK add column flightExclusionB2C VARCHAR(50) AFTER CarrExlusionB2B;
Alter table QP_DW_RMALLOC.market_list_adhoc_KK add column flightExclusionB2B VARCHAR(50) AFTER flightExclusionB2C;

Alter table QP_DW_RMALLOC.market_list_adhoc_MM add column flightExclusionB2C VARCHAR(50) AFTER CarrExlusionB2B;
Alter table QP_DW_RMALLOC.market_list_adhoc_MM add column flightExclusionB2B VARCHAR(50) AFTER flightExclusionB2C;

Alter table QP_DW_RMALLOC.market_list_adhoc_Kushal add column flightExclusionB2C VARCHAR(50) AFTER CarrExlusionB2B;
Alter table QP_DW_RMALLOC.market_list_adhoc_Kushal add column flightExclusionB2B VARCHAR(50) AFTER flightExclusionB2C;

Alter table QP_DW_RMALLOC.market_list_adhoc_NP add column flightExclusionB2C VARCHAR(50) AFTER CarrExlusionB2B;
Alter table QP_DW_RMALLOC.market_list_adhoc_NP add column flightExclusionB2B VARCHAR(50) AFTER flightExclusionB2C;

Alter table QP_DW_RMALLOC.market_list_adhoc_PQ add column flightExclusionB2C VARCHAR(50) AFTER CarrExlusionB2B;
Alter table QP_DW_RMALLOC.market_list_adhoc_PQ add column flightExclusionB2B VARCHAR(50) AFTER flightExclusionB2C;

Alter table QP_DW_RMALLOC.market_list_adhoc_PRIYANKA add column flightExclusionB2C VARCHAR(50) AFTER CarrExlusionB2B;
Alter table QP_DW_RMALLOC.market_list_adhoc_PRIYANKA add column flightExclusionB2B VARCHAR(50) AFTER flightExclusionB2C;

Alter table QP_DW_RMALLOC.market_list_adhoc_SC add column flightExclusionB2C VARCHAR(50) AFTER CarrExlusionB2B;
Alter table QP_DW_RMALLOC.market_list_adhoc_SC add column flightExclusionB2B VARCHAR(50) AFTER flightExclusionB2C;

Alter table QP_DW_RMALLOC.market_list_adhoc_Shantaram add column flightExclusionB2C VARCHAR(50) AFTER CarrExlusionB2B;
Alter table QP_DW_RMALLOC.market_list_adhoc_Shantaram add column flightExclusionB2B VARCHAR(50) AFTER flightExclusionB2C;

Alter table QP_DW_RMALLOC.market_list_adhoc_SR add column flightExclusionB2C VARCHAR(50) AFTER CarrExlusionB2B;
Alter table QP_DW_RMALLOC.market_list_adhoc_SR add column flightExclusionB2B VARCHAR(50) AFTER flightExclusionB2C;

Alter table QP_DW_RMALLOC.market_list_adhoc_SS add column flightExclusionB2C VARCHAR(50) AFTER CarrExlusionB2B;
Alter table QP_DW_RMALLOC.market_list_adhoc_SS add column flightExclusionB2B VARCHAR(50) AFTER flightExclusionB2C;

Alter table QP_DW_RMALLOC.market_list_adhoc_SV add column flightExclusionB2C VARCHAR(50) AFTER CarrExlusionB2B;
Alter table QP_DW_RMALLOC.market_list_adhoc_SV add column flightExclusionB2B VARCHAR(50) AFTER flightExclusionB2C;

Alter table QP_DW_RMALLOC.market_list_adhoc_SV_Test add column flightExclusionB2C VARCHAR(50) AFTER CarrExlusionB2B;
Alter table QP_DW_RMALLOC.market_list_adhoc_SV_Test add column flightExclusionB2B VARCHAR(50) AFTER flightExclusionB2C;

Alter table QP_DW_RMALLOC.market_list_adhoc_SV_Test1 add column flightExclusionB2C VARCHAR(50) AFTER CarrExlusionB2B;
Alter table QP_DW_RMALLOC.market_list_adhoc_SV_Test1 add column flightExclusionB2B VARCHAR(50) AFTER flightExclusionB2C;

Alter table QP_DW_RMALLOC.market_list_adhoc_SV_Test2 add column flightExclusionB2C VARCHAR(50) AFTER CarrExlusionB2B;
Alter table QP_DW_RMALLOC.market_list_adhoc_SV_Test2 add column flightExclusionB2B VARCHAR(50) AFTER flightExclusionB2C;

Alter table QP_DW_RMALLOC.market_list_adhoc_SV_Test3 add column flightExclusionB2C VARCHAR(50) AFTER CarrExlusionB2B;
Alter table QP_DW_RMALLOC.market_list_adhoc_SV_Test3 add column flightExclusionB2B VARCHAR(50) AFTER flightExclusionB2C;

Alter table QP_DW_RMALLOC.market_list_adhoc_VI add column flightExclusionB2C VARCHAR(50) AFTER CarrExlusionB2B;
Alter table QP_DW_RMALLOC.market_list_adhoc_VI add column flightExclusionB2B VARCHAR(50) AFTER flightExclusionB2C;

UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,DATE_FORMAT(STR_TO_DATE(PerStart, \'%m/%d/%Y\'), \'%a\') AS Day,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusionB2C,CarrExlusionB2B,flightExclusionB2C,flightExclusionB2B,fareAnchor,hardAnchor,plfThreshold,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BTolerance,B2CTolerance,B2BBackstop,B2CBackstop,B2BFactor,DaySpan,AutoTimeRangeFlag,openingFares,OverBooking,profileFares,rbdPushFlag,distressInventoryFlag,seriesBlock,autoGroup,autoBackstopFlag,tbfFlag,analystName' WHERE (`tableName` = 'market_list');
UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusionB2C,CarrExlusionB2B,flightExclusionB2C,flightExclusionB2B,fareAnchor,hardAnchor,plfThreshold,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BTolerance,B2CTolerance,B2BBackstop,B2CBackstop,B2BFactor,DaySpan,AutoTimeRangeFlag,openingFares,OverBooking,profileFares,rbdPushFlag,distressInventoryFlag,seriesBlock,autoGroup,autoBackstopFlag,tbfFlag,analystName' WHERE (`tableName` = 'market_list_s3');
