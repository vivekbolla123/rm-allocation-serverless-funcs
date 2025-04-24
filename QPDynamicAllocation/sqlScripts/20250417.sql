UPDATE `QP_DW_RMALLOC`.`rm_parameter_values` SET `parameterValue` = '96' WHERE (`parameterKey` = 'OVERBOOKING_FARE_LF');
UPDATE `QP_DW_RMALLOC`.`rm_parameter_values` SET `parameterValue` = '0.1' WHERE (`parameterKey` = 'OVERBOOKING_FARE_PERCENT_VALUE');

CREATE TABLE QP_DW_RMALLOC.market_fares_ag (
    channel VARCHAR(255),
    departure_date DATE,
    destination_airport VARCHAR(255),
    gross_fare DOUBLE,
    outbound_airline_code VARCHAR(255),
    outbound_departuretime VARCHAR(6),
    outbound_flight VARCHAR(255),
    outbound_stop VARCHAR(255),
    site_name VARCHAR(255),
    source_airport VARCHAR(255),
    status VARCHAR(255),
    rec_key VARCHAR(255),
    created_at DATETIME,
    site_identifier VARCHAR(100),
    capture_time DATETIME,
    scrape_file_name VARCHAR(100),
    outbound_duration DOUBLE,
    currency VARCHAR(45)
);

CREATE TABLE QP_DW_RMALLOC.market_fares_ag_worker (
    channel VARCHAR(255),
    departure_date DATE,
    destination_airport VARCHAR(255),
    gross_fare DOUBLE,
    outbound_airline_code VARCHAR(255),
    outbound_departuretime VARCHAR(6),
    outbound_flight VARCHAR(255),
    outbound_stop VARCHAR(255),
    site_name VARCHAR(255),
    source_airport VARCHAR(255),
    status VARCHAR(255),
    rec_key VARCHAR(255),
    created_at DATETIME,
    site_identifier VARCHAR(100),
    capture_time DATETIME,
    scrape_file_name VARCHAR(100),
    outbound_duration DOUBLE,
    currency VARCHAR(45)
);

INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`, `Permission`) VALUES ('104', 'view_report_forecast');

UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"ADMIN\", \"permissions\": [1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 52, 53, 54, 55, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104]}' WHERE (`Role` = 'ADMIN');
UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"INVENTORY ANALYST\", \"permissions\": [1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 20, 22, 23, 24, 25, 26, 27, 28, 29, 33, 34, 36, 41, 47, 53, 54, 55, 59, 60, 61, 62, 63, 66, 72, 74, 75, 76, 77, 78, 79, 80, 84, 85, 88, 89, 90, 91, 92, 93, 94, 95, 98, 99, 100, 101, 102, 103, 104]}' WHERE (`Role` = 'INVENTORYANALYST');
UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"INVENTORY ANALYST\", \"permissions\": [1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 20, 22, 23, 24, 25, 26, 27, 28, 29, 33, 34, 36, 37, 41, 47, 53, 54, 55, 59, 60, 61, 62, 63, 66, 72, 74, 75, 76, 77, 78, 79, 80, 84, 85, 88, 89, 90, 91, 92, 93, 94, 95, 98, 100, 101, 102, 103, 104]}' WHERE (`Role` = 'INVENTORYANALYST2');
UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"INVENTORY PRICING ANALYST\", \"permissions\": [1, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 16, 18, 20, 21, 22, 23, 24, 25, 26, 33, 41, 47, 51, 53, 54, 55, 59, 56, 57, 60, 62, 63, 64, 65, 66, 72, 74, 75, 76, 88, 92, 100, 101, 102, 103, 104]}' WHERE (`Role` = 'INVENTORYPRICINGANALYST');
UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"MANAGER\", \"permissions\": [1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 52, 53, 54, 55, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 98, 100, 101, 102, 103, 104]}' WHERE (`Role` = 'MANAGER');
