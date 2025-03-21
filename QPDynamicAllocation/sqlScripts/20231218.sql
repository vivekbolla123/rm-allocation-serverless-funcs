ALTER TABLE `QP_DW_RMALLOC`.`run_summary` 
DROP COLUMN `endOfDayLoad`,
ADD COLUMN `endOfDayTgtLoad` VARCHAR(45) NULL AFTER `marketFaresSummary`,
ADD COLUMN `expectedEndOfDayLoadFactor` VARCHAR(45) NULL AFTER `endOfDayTgtLoad`;

UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FltNum,DepDate,BookedLoad,TgtLoad,bookedPlf,Variance,expectedEndOfDayBooking as EODExpLoad,expectedEndOfDayLoadFactor as EODExpLoadFactor,endOfDayTgtLoad,bookingToday as BookingsToday,strategyFare,lastSellingFare,upsell,fareAnchor,OpenRBD,SellingFare,Channel,HowDetermined,RunId,CONVERT_TZ(CreatedAt, \'UTC\', \'ASIA/KOLKATA\') as CreatedAt,MktFare_Min,AirlineCode_Min,FlightNumber_Min,MktFare_Max,AirlineCode_Max,FlightNumber_Max,criteria,fareOffset,TimeRange,startTime,endTime,Backstop,StrategyReference,dlfBand,endOfDaydlfBand,ndoBand,analystName' WHERE (`tableName` = 'run_summary');
