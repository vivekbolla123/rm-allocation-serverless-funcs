ALTER TABLE `QP_DW_RMALLOC`.`run_summary` 
ADD COLUMN `w0Flag` VARCHAR(45) NULL DEFAULT 0 AFTER `hardAnchorFare`;


UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FltNum,DepDate,DATE_FORMAT(STR_TO_DATE(DepDate, \'%m/%d/%Y\'), \'%a\') AS Day,BookedLoad,TgtLoad as StartOfDayTgtLoad,bookedPlf,actualbookedPlf,Variance,currentTgtLoad,currentLoadFactor,endOfDayTgtLoad,bookingToday,bookingInHour,strategyFare,hardAnchorFare,lastSellingFare,profileFare,lastSevenAvgFare as last7AvgFare,upsell,fareAnchor,OpenRBD,SellingFare,Channel,HowDetermined,RunId,CreatedAt,MktFare_Min,FlightNumber_Min,MktFare_Max,FlightNumber_Max,criteria,fareOffset,TimeRange,startTime,endTime,Backstop,autoBackstop,StrategyReference,dlfBand as startOfDayDplfBand,endOfDaydlfBand as currentDplfBand,ndoBand,overBookingCount,recommendedActions,status,w0Flag,analystName' WHERE (`tableName` = 'run_summary');
