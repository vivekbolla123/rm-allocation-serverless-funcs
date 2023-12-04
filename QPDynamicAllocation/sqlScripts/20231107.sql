ALTER TABLE `market_list`
ADD
    COLUMN `OwnFareFlag` VARCHAR(3) NULL AFTER `SkippingFactor`,
ADD
    COLUMN `DaySpan` INT NULL AFTER `OwnFareFlag`;

ALTER TABLE
    `market_list_adhoc_AS`
ADD
    COLUMN `OwnFareFlag` VARCHAR(3) NULL AFTER `SkippingFactor`,
ADD
    COLUMN `DaySpan` INT NULL AFTER `OwnFareFlag`;

ALTER TABLE
    `market_list_adhoc_DR`
ADD
    COLUMN `OwnFareFlag` VARCHAR(3) NULL AFTER `SkippingFactor`,
ADD
    COLUMN `DaySpan` INT NULL AFTER `OwnFareFlag`;

ALTER TABLE
    `market_list_adhoc_JP`
ADD
    COLUMN `OwnFareFlag` VARCHAR(3) NULL AFTER `SkippingFactor`,
ADD
    COLUMN `DaySpan` INT NULL AFTER `OwnFareFlag`;

ALTER TABLE
    `market_list_adhoc_KK`
ADD
    COLUMN `OwnFareFlag` VARCHAR(3) NULL AFTER `SkippingFactor`,
ADD
    COLUMN `DaySpan` INT NULL AFTER `OwnFareFlag`;

ALTER TABLE
    `market_list_adhoc_MB`
ADD
    COLUMN `OwnFareFlag` VARCHAR(3) NULL AFTER `SkippingFactor`,
ADD
    COLUMN `DaySpan` INT NULL AFTER `OwnFareFlag`;

ALTER TABLE
    `market_list_adhoc_mb`
ADD
    COLUMN `OwnFareFlag` VARCHAR(3) NULL AFTER `SkippingFactor`,
ADD
    COLUMN `DaySpan` INT NULL AFTER `OwnFareFlag`;

ALTER TABLE
    `market_list_adhoc_MM`
ADD
    COLUMN `OwnFareFlag` VARCHAR(3) NULL AFTER `SkippingFactor`,
ADD
    COLUMN `DaySpan` INT NULL AFTER `OwnFareFlag`;

ALTER TABLE
    `market_list_adhoc_NP`
ADD
    COLUMN `OwnFareFlag` VARCHAR(3) NULL AFTER `SkippingFactor`,
ADD
    COLUMN `DaySpan` INT NULL AFTER `OwnFareFlag`;

ALTER TABLE
    `market_list_adhoc_SC`
ADD
    COLUMN `OwnFareFlag` VARCHAR(3) NULL AFTER `SkippingFactor`,
ADD
    COLUMN `DaySpan` INT NULL AFTER `OwnFareFlag`;

ALTER TABLE
    `market_list_adhoc_SR`
ADD
    COLUMN `OwnFareFlag` VARCHAR(3) NULL AFTER `SkippingFactor`,
ADD
    COLUMN `DaySpan` INT NULL AFTER `OwnFareFlag`;

ALTER TABLE
    `market_list_adhoc_SV`
ADD
    COLUMN `OwnFareFlag` VARCHAR(3) NULL AFTER `SkippingFactor`,
ADD
    COLUMN `DaySpan` INT NULL AFTER `OwnFareFlag`;

ALTER TABLE
    `market_list_adhoc_Test`
ADD
    COLUMN `OwnFareFlag` VARCHAR(3) NULL AFTER `SkippingFactor`,
ADD
    COLUMN `DaySpan` INT NULL AFTER `OwnFareFlag`;

ALTER TABLE `d1_d2_strategies`
ADD
    COLUMN `channel` varchar(5) NULL AFTER `offset`;

UPDATE
    `QP_DW_RMALLOC`.`config_column_names`
SET
    `columns` = 'Origin,Destin,FlightNumber,PerType,PerStart,PerEnd,DOW,strategyFlag,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusion,fareAnchor,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BBackstop,B2CBackstop,B2BFactor,OwnFareFlag,DaySpan,analystName'
WHERE (`tableName` = 'market_list');

UPDATE
    `QP_DW_RMALLOC`.`config_column_names`
SET
    `columns` = 'strategy,channel,Decision,Band,0,1,2,3,4,5,6'
WHERE (
        `tableName` = 'd1_d2_strategies'
    );

UPDATE
    `QP_DW_RMALLOC`.`config_column_names`
SET
    `columns` = 'strategy,ndo_band,dplf_band,criteria,time_range,offset,channel'
WHERE (`tableName` = 'strategy_input');