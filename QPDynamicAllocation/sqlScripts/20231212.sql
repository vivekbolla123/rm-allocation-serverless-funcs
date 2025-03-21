ALTER TABLE `QP_DW_RMALLOC`.`run_summary` 
ADD COLUMN `marketFaresSummary` VARCHAR(45) ;

UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FltNum,DepDate,BookedLoad,TgtLoad,bookedPlf,Variance,expectedEndOfDayBooking as EODExpLoad,expectedEndOfDayLoadFactor as EODExpLoadFactor,endOfDayTgtLoad,bookingToday,strategyFare,lastSellingFare,upsell,fareAnchor,OpenRBD,SellingFare,Channel,HowDetermined,RunId,CONVERT_TZ(CreatedAt, \'UTC\', \'ASIA/KOLKATA\') as CreatedAt,MktFare_Min,AirlineCode_Min,FlightNumber_Min,MktFare_Max,AirlineCode_Max,FlightNumber_Max,criteria,fareOffset,TimeRange,startTime,endTime,Backstop,StrategyReference,dlfBand,endOfDaydlfBand,ndoBand,marketFaresSummary,analystName' WHERE (`tableName` = 'run_summary');
