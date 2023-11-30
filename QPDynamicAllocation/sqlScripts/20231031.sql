ALTER TABLE `QP_DW_RMALLOC`.`run_summary` 
ADD COLUMN `allocationStatus` INT NULL AFTER `analystName`,
ADD COLUMN `startTime` VARCHAR(45) NULL AFTER `allocationStatus`,
ADD COLUMN `endTime` VARCHAR(45) NULL AFTER `startTime`;

UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Id,Origin,Destin,FltNum,DepDate,BookedLoad,TgtLoad,bookedPlf,Variance,fareAnchor,OpenRBD,SellingFare,Channel,HowDetermined,RunId,CreatedAt,MktFare_Min,AirlineCode_Min,FlightNumber_Min,MktFare_Max,AirlineCode_Max,FlightNumber_Max,criteria,fareOffset,TimeRange,startTime,endTime,backstop,StrategyReference,dlfBand,ndoBand,analystName' WHERE (`tableName` = 'run_summary');
