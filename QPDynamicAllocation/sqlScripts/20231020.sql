ALTER TABLE `QP_DW_RMALLOC`.`run_summary` 
ADD COLUMN `bookedPlf` VARCHAR(45) NULL AFTER `CreatedAt`,
ADD COLUMN `analystName` VARCHAR(45) NULL AFTER `bookedPlf`,
CHANGE COLUMN `Dev_BKGCurve` `Variance` VARCHAR(45) NULL DEFAULT NULL ;

UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Id,Origin,Destin,FltNum,DepDate,BookedLoad,TgtLoad,bookedPlf,Variance,fareAnchor,OpenRBD,SellingFare,Channel,HowDetermined,RunId,CreatedAt,MktFare_Min,AirlineCode_Min,FlightNumber_Min,MktFare_Max,AirlineCode_Max,FlightNumber_Max,criteria,fareOffset,TimeRange,backstop,StrategyReference,dlfBand,ndoBand,analystName' WHERE (`tableName` = 'run_summary');
