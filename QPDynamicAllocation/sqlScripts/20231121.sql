ALTER TABLE `market_list`
ADD
    COLUMN `AutoTimeRangeFlag` int NULL AFTER `DaySpan`;

ALTER TABLE
    `market_list_adhoc_AS`
ADD
    COLUMN `AutoTimeRangeFlag` int NULL AFTER `DaySpan`;

ALTER TABLE
    `market_list_adhoc_DR`
ADD
    COLUMN `AutoTimeRangeFlag` int NULL AFTER `DaySpan`;

ALTER TABLE
    `market_list_adhoc_JP`
ADD
    COLUMN `AutoTimeRangeFlag` int NULL AFTER `DaySpan`;

ALTER TABLE
    `market_list_adhoc_KK`
ADD
    COLUMN `AutoTimeRangeFlag` int NULL AFTER `DaySpan`;

ALTER TABLE
    `market_list_adhoc_MB`
ADD
    COLUMN `AutoTimeRangeFlag` int NULL AFTER `DaySpan`;

ALTER TABLE
    `market_list_adhoc_mb`
ADD
    COLUMN `AutoTimeRangeFlag` int NULL AFTER `DaySpan`;

ALTER TABLE
    `market_list_adhoc_MM`
ADD
    COLUMN `AutoTimeRangeFlag` int NULL AFTER `DaySpan`;

ALTER TABLE
    `market_list_adhoc_NP`
ADD
    COLUMN `AutoTimeRangeFlag` int NULL AFTER `DaySpan`;

ALTER TABLE
    `market_list_adhoc_SC`
ADD
    COLUMN `AutoTimeRangeFlag` int NULL AFTER `DaySpan`;

ALTER TABLE
    `market_list_adhoc_SR`
ADD
    COLUMN `AutoTimeRangeFlag` int NULL AFTER `DaySpan`;

ALTER TABLE
    `market_list_adhoc_SV`
ADD
    COLUMN `AutoTimeRangeFlag` int NULL AFTER `DaySpan`;

ALTER TABLE
    `market_list_adhoc_Test`
ADD
    COLUMN `AutoTimeRangeFlag` int NULL AFTER `DaySpan`;

UPDATE
    `QP_DW_RMALLOC`.`config_column_names`
SET
    `columns` = 'Origin,Destin,FlightNumber,PerType,PerStart,PerEnd,DOW,strategyFlag,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusion,fareAnchor,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BBackstop,B2CBackstop,B2BFactor,OwnFareFlag,DaySpan,AutoTimeRangeFlag,analystName'
WHERE (`tableName` = 'market_list');

CREATE TABLE
    `departure_time_ranges` (
        StartTime VARCHAR(45) NOT NULL,
        EndTime VARCHAR(45) NOT NULL,
        TimeBand VARCHAR(45) NOT NULL
    );

INSERT INTO
    `departure_time_ranges` (StartTime, EndTime, TimeBand)
VALUES ('00:00', '06:00', 1), ('06:01', '09:00', 2), ('09:01', '12:00', 3), ('12:01', '18:00', 4), ('18:01', '21:30', 5), ('21:31', '23:59', 1);