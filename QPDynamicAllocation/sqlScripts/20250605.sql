ALTER TABLE QP_DW_RMALLOC.market_list ADD COLUMN MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

ALTER TABLE QP_DW_RMALLOC.market_list_international ADD COLUMN MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

ALTER TABLE QP_DW_RMALLOC.market_list_thirty ADD COLUMN MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

ALTER TABLE QP_DW_RMALLOC.market_list_adhoc_International_TP ADD COLUMN MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_JK add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_DM add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_AD add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_JM add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_KK add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_MM add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_kushal add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_NP add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_PQ add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_PB add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_PRIYANKA add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_SC add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_Shantaram add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_SR add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_SS add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_SV add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_SV_Test add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_SV_Test1 add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_SV_Test2 add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

Alter table QP_DW_RMALLOC.market_list_adhoc_SV_Test3 add column MktFareFlag VARCHAR(50) DEFAULT 1 AFTER CugFlag;

UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,DATE_FORMAT(STR_TO_DATE(PerStart, \'%m/%d/%Y\'), \'%a\') AS Day,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusionB2C,CarrExlusionB2B,flightExclusionB2C,flightExclusionB2B,fareAnchor,hardAnchor,plfThreshold,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BTolerance,B2CTolerance,B2BBackstop,B2CBackstop,B2BFactor,obSeats,obFare,DaySpan,AutoTimeRangeFlag,openingFares,OverBooking,profileFares,rbdPushFlag,distressInventoryFlag,seriesBlock,autoGroup,autoBackstopFlag,tbfFlag,CugFlag,MktFareFlag,analystName' WHERE (`tableName` = 'market_list');
UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,DATE_FORMAT(STR_TO_DATE(PerStart, \'%m/%d/%Y\'), \'%a\') AS Day,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusionB2C,CarrExlusionB2B,flightExclusionB2C,flightExclusionB2B,fareAnchor,hardAnchor,plfThreshold,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BTolerance,B2CTolerance,B2BBackstop,B2CBackstop,B2BFactor,obSeats,obFare,DaySpan,AutoTimeRangeFlag,openingFares,OverBooking,profileFares,rbdPushFlag,distressInventoryFlag,seriesBlock,autoGroup,autoBackstopFlag,tbfFlag,CugFlag,MktFareFlag,analystName' WHERE (`tableName` = 'market_list_international');
UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,DATE_FORMAT(STR_TO_DATE(PerStart, \'%m/%d/%Y\'), \'%a\') AS Day,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusionB2C,CarrExlusionB2B,flightExclusionB2C,flightExclusionB2B,fareAnchor,hardAnchor,plfThreshold,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BTolerance,B2CTolerance,B2BBackstop,B2CBackstop,B2BFactor,obSeats,obFare,DaySpan,AutoTimeRangeFlag,openingFares,OverBooking,profileFares,rbdPushFlag,distressInventoryFlag,seriesBlock,autoGroup,autoBackstopFlag,tbfFlag,CugFlag,MktFareFlag,analystName' WHERE (`tableName` = 'market_list_thirty');
UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Origin,Destin,FlightNumber,PerStart,PerEnd,DOW,TimeWindowStart,TimeWindowEnd,CurveID,CarrExlusionB2C,CarrExlusionB2B,flightExclusionB2C,flightExclusionB2B,fareAnchor,hardAnchor,plfThreshold,fareOffset,FirstRBDAlloc,OtherRBDAlloc,SkippingFactor,B2BTolerance,B2CTolerance,B2BBackstop,B2CBackstop,B2BFactor,obSeats,obFare,DaySpan,AutoTimeRangeFlag,openingFares,OverBooking,profileFares,rbdPushFlag,distressInventoryFlag,seriesBlock,autoGroup,autoBackstopFlag,tbfFlag,CugFlag,MktFareFlag,analystName' WHERE (`tableName` = 'market_list_s3');


INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`, `Permission`) VALUES ('110', 'view_sales_inv');
UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"ADMIN\", \"permissions\": [1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 52, 53, 54, 55, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110]}' WHERE (`Role` = 'ADMIN');
UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"MANAGER\", \"permissions\": [1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 52, 53, 54, 55, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 98, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110]}' WHERE (`Role` = 'MANAGER');

CREATE TABLE `distress_calendar` (
  `date` varchar(10) DEFAULT NULL,
  `sector` varchar(10) DEFAULT NULL,
  `value` varchar(10) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `sales_inventory` (
  `date` varchar(8) DEFAULT NULL,
  `minvalue` int DEFAULT NULL,
  `maxvalue` int DEFAULT NULL,
  `seatsallocation` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `salesinv_rbd_distribution` (
  `seats` int NOT NULL,
  `type` varchar(50) NOT NULL,
  `rbd` varchar(10) NOT NULL,
  `value` int DEFAULT NULL,
  PRIMARY KEY (`seats`,`type`,`rbd`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO `QP_DW_RMALLOC`.`config_column_names` (`tableName`, `columns`) VALUES ('distress_calendar', 'sector,value,date');
INSERT INTO `QP_DW_RMALLOC`.`config_column_names` (`tableName`, `columns`) VALUES ('sales_inventory', 'date,minvalue,maxvalue,seatsallocation');


DELIMITER $$
CREATE  PROCEDURE `dynamic_pivot_distresscal`(
    IN p_sector VARCHAR(255),   -- Input sector name (NULL for all)
    IN p_start_date DATE,       -- Input start date (NULL for no start limit)
    IN p_end_date DATE          -- Input end date (NULL for no end limit)
)
BEGIN
    SET SESSION group_concat_max_len = 1000000;

    -- Dynamically generate the list of columns for the pivot
    SET @station_list = (
        SELECT GROUP_CONCAT(DISTINCT
            CONCAT('MAX(CASE WHEN cpfsu.sector = ', QUOTE(sector), ' THEN cpfsu.value END) AS `', sector, '`')
            ORDER BY sector
        )
        FROM config_user_sector_map
        WHERE (p_sector IS NULL OR sector = p_sector)
    );

    IF @station_list IS NULL OR @station_list = '' THEN
        SET @station_list = 'NULL AS no_sectors_found';
    END IF;

    -- Construct the base SQL query
    -- Select the requested columns and the pivoted columns
    -- Join condition assumes cde.date is VARCHAR (mm/dd/yyyy) and cpfsu.date is DATE
    -- If cpfsu.date is also VARCHAR (mm/dd/yyyy), change ON clause to: ON cde.date = cpfsu.date
    SET @sql_query = CONCAT('
        SELECT
            cde.date,         -- Original date string format
            cde.mappedMonth,
            cde.event,
            ', @station_list, '
        FROM
            config_date_event cde
        LEFT JOIN
            distress_calendar cpfsu
        ON
            STR_TO_DATE(cde.date, \'%m/%d/%Y\') = STR_TO_DATE(cpfsu.date, \'%m/%d/%Y\')'); -- Convert cde.date for the join

    -- Dynamically build the WHERE clause using STR_TO_DATE for date comparisons
    SET @where_clause = ' WHERE 1=1';

    -- Add sector filter (applied to the joined table)
    IF p_sector IS NOT NULL THEN
        SET @where_clause = CONCAT(@where_clause, ' AND (cpfsu.sector IS NULL OR cpfsu.sector = ', QUOTE(p_sector), ')');
    END IF;

    -- Add start date filter (converting cde.date for comparison)
    IF p_start_date IS NOT NULL THEN
        SET @where_clause = CONCAT(@where_clause, ' AND STR_TO_DATE(cde.date, \'%m/%d/%Y\') >= ', QUOTE(p_start_date));
    END IF;

    -- Add end date filter (converting cde.date for comparison)
    IF p_end_date IS NOT NULL THEN
        SET @where_clause = CONCAT(@where_clause, ' AND STR_TO_DATE(cde.date, \'%m/%d/%Y\') <= ', QUOTE(p_end_date));
    END IF;

    -- Append the WHERE clause and GROUP BY clause
    -- Group by all non-aggregated columns selected
    SET @sql_query = CONCAT(@sql_query,
                           @where_clause,
                           ' GROUP BY cde.date, cde.DOW, cde.mappedMonth, cde.event
                             ORDER BY STR_TO_DATE(cde.date, \'%m/%d/%Y\')'); -- Order by the actual date value for correct sequence

    -- Prepare and execute
    PREPARE stmt FROM @sql_query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

END$$
DELIMITER ;
