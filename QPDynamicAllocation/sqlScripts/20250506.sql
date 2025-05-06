ALTER TABLE `QP_DW_RMALLOC`.`market_list` 
ADD COLUMN `CugFlag` VARCHAR(45) NULL DEFAULT 0 AFTER `autoBackstopFlag`;

ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_AD` 
ADD COLUMN `CugFlag` VARCHAR(45) NULL DEFAULT 0 AFTER `autoBackstopFlag`;
ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_DM` 
ADD COLUMN `CugFlag` VARCHAR(45) NULL DEFAULT 0 AFTER `autoBackstopFlag`;
ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_JK` 
ADD COLUMN `CugFlag` VARCHAR(45) NULL DEFAULT 0 AFTER `autoBackstopFlag`;
ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_JM` 
ADD COLUMN `CugFlag` VARCHAR(45) NULL DEFAULT 0 AFTER `autoBackstopFlag`;
ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_KK` 
ADD COLUMN `CugFlag` VARCHAR(45) NULL DEFAULT 0 AFTER `autoBackstopFlag`;
ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_MM` 
ADD COLUMN `CugFlag` VARCHAR(45) NULL DEFAULT 0 AFTER `autoBackstopFlag`;
ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_mrunal` 
ADD COLUMN `CugFlag` VARCHAR(45) NULL DEFAULT 0 AFTER `autoBackstopFlag`;
ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_NP 
ADD COLUMN `CugFlag` VARCHAR(45) NULL DEFAULT 0 AFTER `autoBackstopFlag`;
ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_OP` 
ADD COLUMN `CugFlag` VARCHAR(45) NULL DEFAULT 0 AFTER `autoBackstopFlag`;
ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_PB` 
ADD COLUMN `CugFlag` VARCHAR(45) NULL DEFAULT 0 AFTER `autoBackstopFlag`;
ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_PQ` 
ADD COLUMN `CugFlag` VARCHAR(45) NULL DEFAULT 0 AFTER `autoBackstopFlag`;
ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_PRIYANKA` 
ADD COLUMN `CugFlag` VARCHAR(45) NULL DEFAULT 0 AFTER `autoBackstopFlag`;
ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_SC` 
ADD COLUMN `CugFlag` VARCHAR(45) NULL DEFAULT 0 AFTER `autoBackstopFlag`;
ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_Shantaram` 
ADD COLUMN `CugFlag` VARCHAR(45) NULL DEFAULT 0 AFTER `autoBackstopFlag`;
ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_SR` 
ADD COLUMN `CugFlag` VARCHAR(45) NULL DEFAULT 0 AFTER `autoBackstopFlag`;
ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_SS` 
ADD COLUMN `CugFlag` VARCHAR(45) NULL DEFAULT 0 AFTER `autoBackstopFlag`;

ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_VC` 
ADD COLUMN `CugFlag` VARCHAR(45) NULL DEFAULT 0 AFTER `autoBackstopFlag`;
ALTER TABLE `QP_DW_RMALLOC`.`market_list_adhoc_VI` 
ADD COLUMN `CugFlag` VARCHAR(45) NULL DEFAULT 0 AFTER `autoBackstopFlag`;
ALTER TABLE `QP_DW_RMALLOC`.`market_list_international` 

UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,DATE_FORMAT(STR_TO_DATE(PerStart, \'%m/%d/%Y\'), \'%a\') AS Day,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusionB2C,CarrExlusionB2B,flightExclusionB2C,flightExclusionB2B,fareAnchor,hardAnchor,plfThreshold,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BTolerance,B2CTolerance,B2BBackstop,B2CBackstop,B2BFactor,obSeats,obFare,DaySpan,AutoTimeRangeFlag,openingFares,OverBooking,profileFares,rbdPushFlag,distressInventoryFlag,seriesBlock,autoGroup,autoBackstopFlag,tbfFlag,CugFlag,analystName' WHERE (`tableName` = 'market_list');
UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,DATE_FORMAT(STR_TO_DATE(PerStart, \'%m/%d/%Y\'), \'%a\') AS Day,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusionB2C,CarrExlusionB2B,flightExclusionB2C,flightExclusionB2B,fareAnchor,hardAnchor,plfThreshold,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BTolerance,B2CTolerance,B2BBackstop,B2CBackstop,B2BFactor,obSeats,obFare,DaySpan,AutoTimeRangeFlag,openingFares,OverBooking,profileFares,rbdPushFlag,distressInventoryFlag,seriesBlock,autoGroup,autoBackstopFlag,tbfFlag,CugFlag,analystName' WHERE (`tableName` = 'market_list_international');
UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusionB2C,CarrExlusionB2B,flightExclusionB2C,flightExclusionB2B,fareAnchor,hardAnchor,plfThreshold,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BTolerance,B2CTolerance,B2BBackstop,B2CBackstop,B2BFactor,obSeats,obFare,DaySpan,AutoTimeRangeFlag,openingFares,OverBooking,profileFares,rbdPushFlag,distressInventoryFlag,seriesBlock,autoGroup,autoBackstopFlag,tbfFlag,CugFlag,analystName' WHERE (`tableName` = 'market_list_s3');

UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,DATE_FORMAT(STR_TO_DATE(PerStart, \'%m/%d/%Y\'), \'%a\') AS Day,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusionB2C,CarrExlusionB2B,flightExclusionB2C,flightExclusionB2B,fareAnchor,hardAnchor,plfThreshold,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BTolerance,B2CTolerance,B2BBackstop,B2CBackstop,B2BFactor,obSeats,obFare,DaySpan,AutoTimeRangeFlag,openingFares,OverBooking,profileFares,rbdPushFlag,distressInventoryFlag,seriesBlock,autoGroup,autoBackstopFlag,tbfFlag,CugFlag,analystName' WHERE (`tableName` = 'market_list');
UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,DATE_FORMAT(STR_TO_DATE(PerStart, \'%m/%d/%Y\'), \'%a\') AS Day,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusionB2C,CarrExlusionB2B,flightExclusionB2C,flightExclusionB2B,fareAnchor,hardAnchor,plfThreshold,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BTolerance,B2CTolerance,B2BBackstop,B2CBackstop,B2BFactor,obSeats,obFare,DaySpan,AutoTimeRangeFlag,openingFares,OverBooking,profileFares,rbdPushFlag,distressInventoryFlag,seriesBlock,autoGroup,autoBackstopFlag,tbfFlag,CugFlag,analystName' WHERE (`tableName` = 'market_list_international');
UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusionB2C,CarrExlusionB2B,flightExclusionB2C,flightExclusionB2B,fareAnchor,hardAnchor,plfThreshold,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BTolerance,B2CTolerance,B2BBackstop,B2CBackstop,B2BFactor,obSeats,obFare,DaySpan,AutoTimeRangeFlag,openingFares,OverBooking,profileFares,rbdPushFlag,distressInventoryFlag,seriesBlock,autoGroup,autoBackstopFlag,tbfFlag,CugFlag,analystName' WHERE (`tableName` = 'market_list_s3');



INSERT INTO `QP_DW_RMALLOC`.`rm_parameter_values` (`parameterKey`, `parameterValue`) VALUES ('CUG_B2C_OPENING_THRESHOLD', '0.3');
INSERT INTO `QP_DW_RMALLOC`.`rm_parameter_values` (`parameterKey`, `parameterValue`) VALUES ('CUG_SEATS_PERCENT', '0.25');
UPDATE `QP_DW_RMALLOC`.`rm_parameter_values` SET `parameterValue` = 'ZJ,ZI,ZH,ZG,ZF,ZE,ZD,ZC,ZB,ZA,Z9,Z8,Z7,Z6,Z5,Z4,Z3,Z2,Z1,Z0,V9,V8,V7,V6,V5,V4,V3,V2,V1,V0,U9,U8,U7,U6,U5,U4,U3,U2,U1,U0,T9,T8,T7,T6,T5,T4,T3,T2,T1,T0,R9,R8,R7,R6,R5,R4,R3,R2,R1,R0,Q9,Q8,Q7,Q6,Q5,Q4,Q3,Q2,Q1,Q0,Q,O,N,M,K,J,I,H,F,E,D,C,B,Y,TF,TE,TD,TC,TB,TY,GJ,GI,GH,GG,GF,GE,GD,GC,GB,GA,G0,G1,G2,G3,G4' WHERE (`parameterKey` = 'RBD_VALUES');
UPDATE `QP_DW_RMALLOC`.`rm_parameter_values` SET `parameterValue` = '116' WHERE (`parameterKey` = 'AU_COLUMN_LENGTH');
