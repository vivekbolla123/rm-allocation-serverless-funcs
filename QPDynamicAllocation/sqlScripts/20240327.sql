ALTER TABLE `QP_DW_RMALLOC`.`run_summary` 
CHANGE COLUMN `expectedEndOfDayBooking` `currentTgtLoad` VARCHAR(45) NULL DEFAULT NULL ,
CHANGE COLUMN `expectedEndOfDayLoadFactor` `currentLoadFactor` VARCHAR(45) NULL DEFAULT NULL ;
ALTER TABLE `QP_RM_ARCHIVES`.`run_summary` 
CHANGE COLUMN `expectedEndOfDayBooking` `currentTgtLoad` VARCHAR(45) NULL DEFAULT NULL ,
CHANGE COLUMN `expectedEndOfDayLoadFactor` `currentLoadFactor` VARCHAR(45) NULL DEFAULT NULL ;

UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FltNum,DepDate,BookedLoad,TgtLoad as StartOfDayTgtLoad,bookedPlf,Variance,currentTgtLoad,currentLoadFactor,endOfDayTgtLoad,bookingToday,historicOwnFare,strategyFare,lastSellingFare,profileFare,upsell,fareAnchor,OpenRBD,SellingFare,Channel,HowDetermined,RunId,CreatedAt,MktFare_Min,AirlineCode_Min,FlightNumber_Min,MktFare_Max,AirlineCode_Max,FlightNumber_Max,criteria,fareOffset,TimeRange,startTime,endTime,Backstop,StrategyReference,dlfBand as currentDlfBand,endOfDaydlfBand,ndoBand,overBookingCount,marketFaresSummary,status,analystName' WHERE (`tableName` = 'run_summary');

UPDATE `QP_DW_RMALLOC`.`config_share_of_booking` SET `shareOfBooking` = '6.25' WHERE (`hours` = '23');
UPDATE `QP_DW_RMALLOC`.`config_share_of_booking` SET `shareOfBooking` = '6.32' WHERE (`hours` = '22');
UPDATE `QP_DW_RMALLOC`.`config_share_of_booking` SET `shareOfBooking` = '6.16' WHERE (`hours` = '21');
UPDATE `QP_DW_RMALLOC`.`config_share_of_booking` SET `shareOfBooking` = '6.24' WHERE (`hours` = '20');
UPDATE `QP_DW_RMALLOC`.`config_share_of_booking` SET `shareOfBooking` = '6.27' WHERE (`hours` = '19');
UPDATE `QP_DW_RMALLOC`.`config_share_of_booking` SET `shareOfBooking` = '6.07' WHERE (`hours` = '18');
UPDATE `QP_DW_RMALLOC`.`config_share_of_booking` SET `shareOfBooking` = '6.12' WHERE (`hours` = '17');
UPDATE `QP_DW_RMALLOC`.`config_share_of_booking` SET `shareOfBooking` = '6.29' WHERE (`hours` = '16');
UPDATE `QP_DW_RMALLOC`.`config_share_of_booking` SET `shareOfBooking` = '6.33' WHERE (`hours` = '15');
UPDATE `QP_DW_RMALLOC`.`config_share_of_booking` SET `shareOfBooking` = '6.78' WHERE (`hours` = '14');
UPDATE `QP_DW_RMALLOC`.`config_share_of_booking` SET `shareOfBooking` = '6.92' WHERE (`hours` = '13');
UPDATE `QP_DW_RMALLOC`.`config_share_of_booking` SET `shareOfBooking` = '6.73' WHERE (`hours` = '12');
UPDATE `QP_DW_RMALLOC`.`config_share_of_booking` SET `shareOfBooking` = '5.36' WHERE (`hours` = '11');
UPDATE `QP_DW_RMALLOC`.`config_share_of_booking` SET `shareOfBooking` = '3.73' WHERE (`hours` = '10');
UPDATE `QP_DW_RMALLOC`.`config_share_of_booking` SET `shareOfBooking` = '2.29' WHERE (`hours` = '9');
UPDATE `QP_DW_RMALLOC`.`config_share_of_booking` SET `shareOfBooking` = '1.25' WHERE (`hours` = '8');
UPDATE `QP_DW_RMALLOC`.`config_share_of_booking` SET `shareOfBooking` = '0.48' WHERE (`hours` = '7');
UPDATE `QP_DW_RMALLOC`.`config_share_of_booking` SET `shareOfBooking` = '0.21' WHERE (`hours` = '6');
UPDATE `QP_DW_RMALLOC`.`config_share_of_booking` SET `shareOfBooking` = '0.18' WHERE (`hours` = '5');
UPDATE `QP_DW_RMALLOC`.`config_share_of_booking` SET `shareOfBooking` = '0.30' WHERE (`hours` = '4');
UPDATE `QP_DW_RMALLOC`.`config_share_of_booking` SET `shareOfBooking` = '0.55' WHERE (`hours` = '3');
UPDATE `QP_DW_RMALLOC`.`config_share_of_booking` SET `shareOfBooking` = '1.26' WHERE (`hours` = '2');
UPDATE `QP_DW_RMALLOC`.`config_share_of_booking` SET `shareOfBooking` = '2.50' WHERE (`hours` = '1');
UPDATE `QP_DW_RMALLOC`.`config_share_of_booking` SET `shareOfBooking` = '0' WHERE (`hours` = '0');
