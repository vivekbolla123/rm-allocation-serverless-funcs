UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FltNum,DepDate,BookedLoad,TgtLoad as StartOfDayTgtLoad,bookedPlf,Variance,currentTgtLoad,currentLoadFactor,endOfDayTgtLoad,bookingToday,historicOwnFare,strategyFare,lastSellingFare,profileFare,upsell,fareAnchor,OpenRBD,SellingFare,Channel,HowDetermined,RunId,CreatedAt,MktFare_Min,AirlineCode_Min,FlightNumber_Min,MktFare_Max,AirlineCode_Max,FlightNumber_Max,criteria,fareOffset,TimeRange,startTime,endTime,Backstop,StrategyReference,dlfBand as startOfDayDplfBand,endOfDaydlfBand as currentDplfBand,ndoBand,overBookingCount,marketFaresSummary,status,analystName' WHERE (`tableName` = 'run_summary');
UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'NDO,CurveID,LF' WHERE (`tableName` = 'Curves');

ALTER TABLE `QP_DW_RMALLOC`.`Curves` 
DROP COLUMN `B2B_Avg_Fare`,
DROP COLUMN `B2C_Avg_Fare`;
