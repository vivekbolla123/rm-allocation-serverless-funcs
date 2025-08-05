INSERT INTO `QP_DW_RMALLOC`.`rm_parameter_values` (`parameterKey`, `parameterValue`) VALUES ('PRICING_DUF_SECTORS', '{\"SAG\": \"319\",\"TRV\": \"425\",\"DEL\": \"66\",\"IXE\": \"372\",\"BOM\": \"89\",\"GOX\": \"425\",\"GAU\": \"313\",\"JAI\": \"407\"}');
INSERT INTO `QP_DW_RMALLOC`.`rm_parameter_values` (`parameterKey`, `parameterValue`) VALUES ('PRICING_RCS_SECTORS', '[\"GAU\",\"IXA\",\"IXZ\",\"SXR\"]');
INSERT INTO `QP_DW_RMALLOC`.`rm_parameter_values` (`parameterKey`, `parameterValue`) VALUES ('PRICING_GST_SECTORS', '[\"IXB\",\"GAU\",\"IXA\",\"AJL\"]');
INSERT INTO `QP_DW_RMALLOC`.`rm_parameter_values` (`parameterKey`, `parameterValue`) VALUES ('PRICING_CARR_EXCL', '(\'9I\', \'IC\', \'S9\', \'S5\')');


CREATE TABLE QP_DW_PRIAUT.tax_mast_dom (
    Station VARCHAR(10),
    Code VARCHAR(10),
    ASF DECIMAL(10,2),
    CUTE DECIMAL(10,2),
    PSF DECIMAL(10,2),
    RCS DECIMAL(10,2),
    UDF_DF DECIMAL(10,2),
    GST DECIMAL(5,2)
);

CREATE TABLE QP_DW_PRIAUT.tax_mast_intl (
    Currency VARCHAR(3),
    Sectors TEXT,
    GST DECIMAL(5,2),
    CUTE DECIMAL(10,2),
    ASF DECIMAL(10,2),
    PSF_ADF DECIMAL(10,2),
    UDF DECIMAL(10,2),
    PSC_PZ DECIMAL(10,2),
    RCS_CUTE DECIMAL(10,2),
    RCS DECIMAL(10,2),
    Tax1 DECIMAL(10,2),
    Tax2 DECIMAL(10,2),
    Tax3 DECIMAL(10,2),
    Tax4 DECIMAL(10,2),
    Tax5 DECIMAL(10,2)
);

CREATE TABLE QP_DW_PRIAUT.qp_fares (
    Carrier_Code VARCHAR(10),
    Action_Code VARCHAR(10),
    Origin VARCHAR(10),
    Destination VARCHAR(10),
    Fare_Basis_Code VARCHAR(50),
    OW_RT VARCHAR(10),
    Routing VARCHAR(100),
    Footnote VARCHAR(100),
    Currency VARCHAR(10),
    Fare_Amount DECIMAL(10,2),
    Point_Price DECIMAL(10,2),
    Effective_Date VARCHAR(20),
    Tariff_No VARCHAR(20),
    Rule_Nbr VARCHAR(20),
    Link VARCHAR(20),
    Sequence VARCHAR(20),
    SOS_Charge VARCHAR(20),
    Sort_Price DECIMAL(10,2),
    Fare_Direction VARCHAR(10),
    Trip_Direction VARCHAR(10),
    Inbound_Outbound VARCHAR(20),
    Discontinue_Date VARCHAR(20),
    In_Batch VARCHAR(20),
    OpenJaw VARCHAR(20)
);

CREATE INDEX idx_origin_dest ON QP_DW_PRIAUT.qp_fares (Origin, Destination);
CREATE INDEX idx_fare_basis ON QP_DW_PRIAUT.qp_fares (Fare_Basis_Code);

INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`, `Permission`) VALUES ('113', 'view_pricing_qp_fares');
INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`, `Permission`) VALUES ('114', 'update_pricing_qp_fares');
INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`, `Permission`) VALUES ('115', 'view_pricing_tax_mast_dom');
INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`, `Permission`) VALUES ('116', 'view_pricing_tax_mast_intl');
INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`, `Permission`) VALUES ('117', 'view_action_sectors');
INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`, `Permission`) VALUES ('120', 'view_pricing_summary');
INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`, `Permission`) VALUES ('121', 'update_pricing_tax_mast_dom');
INSERT INTO `QP_DW_RMALLOC`.`config_permissions` (`PermissionID`, `Permission`) VALUES ('122', 'update_pricing_tax_mast_intl');



UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"ADMIN\", \"permissions\": [1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 52, 53, 54, 55, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122]}' WHERE (`Role` = 'ADMIN');
UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"INVENTORY PRICING ANALYST\", \"permissions\": [1, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 16, 18, 20, 21, 22, 23, 24, 25, 26, 33, 41, 47, 51, 53, 54, 55, 59, 56, 57, 60, 62, 63, 64, 65, 66, 72, 74, 75, 76, 88, 92, 103, 104, 105, 107, 109, 110, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122]}' WHERE (`Role` = 'INVENTORYPRICINGANALYST');
UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"MANAGER\", \"permissions\": [1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 52, 53, 54, 55, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 98, 100, 101, 102, 103, 104, 105, 107, 108, 109, 110, 106, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122]}' WHERE (`Role` = 'MANAGER');
UPDATE `QP_DW_RMALLOC`.`config_role_permission` SET `PermissionID` = '{\"role\": \"PRICING ANALYST\", \"permissions\": [1, 2, 4, 5, 6, 8, 10, 20, 22, 23, 25, 26, 27, 28, 29, 33, 34, 35, 36, 41, 47, 53, 54, 55, 59, 60, 61, 62, 63, 103, 107, 113, 114, 115, 116, 117, 120, 121, 122]}' WHERE (`Role` = 'PRICINGANALYST');

INSERT INTO `QP_DW_RMALLOC`.`config_inputs` (`tableName`, `value`, `title`, `view_permission`, `upload_permission`, `message`, `order`) VALUES ('qp_fares', 'qp_fares', 'Pricing QP Fares', 'view_pricing_qp_fares', 'update_pricing_qp_fares', 'Will drop the entire table and insert the entire file uploaded.', '30');
INSERT INTO `QP_DW_RMALLOC`.`config_inputs` (`tableName`, `value`, `title`, `view_permission`, `upload_permission`, `message`, `order`) VALUES ('tax_mast_dom', 'tax_mast_dom', 'Pricing Tax Master Domestic', 'view_pricing_tax_mast_dom', 'update_pricing_tax_mast_dom', 'view only table', '31');
INSERT INTO `QP_DW_RMALLOC`.`config_inputs` (`tableName`, `value`, `title`, `view_permission`, `upload_permission`, `message`, `order`) VALUES ('tax_mast_intl', 'tax_mast_intl', 'Pricing Tax Master International', 'view_pricing_tax_mast_intl', 'update_pricing_tax_mast_intl', 'view only table', '32');
INSERT INTO `QP_DW_RMALLOC`.`config_inputs` (`tableName`, `value`, `title`, `view_permission`, `upload_permission`, `message`, `order`) VALUES ('actionable_sectors_output', 'actionable_sectors_output', 'Actionable Sectors Summary', 'view_action_sectors', 'update_action_sectors', 'view only table', '33');

INSERT INTO `QP_DW_RMALLOC`.`config_column_names` (`tableName`, `columns`) VALUES ('QP_DW_PRIAUT.tax_mast_dom', 'Station, Code, ASF, CUTE, PSF, RCS, UDF_DF, GST');
INSERT INTO `QP_DW_RMALLOC`.`config_column_names` (`tableName`, `columns`) VALUES ('QP_DW_PRIAUT.tax_mast_intl', 'Currency, Sectors, GST, CUTE, ASF, PSF_ADF, UDF, PSC_PZ, RCS_CUTE, RCS, Tax1, Tax2, Tax3, Tax4, Tax5');
INSERT INTO `QP_DW_RMALLOC`.`config_column_names` (`tableName`, `columns`) VALUES ('QP_DW_PRIAUT.qp_fares', 'Carrier_Code, Action_Code, Origin, Destination, Fare_Basis_Code, OW_RT, Routing, Footnote, Currency, Fare_Amount, Point_Price, Effective_Date, Tariff_No, Rule_Nbr, Link, Sequence, SOS_Charge, Sort_Price, Fare_Direction, Trip_Direction, Inbound_Outbound, Discontinue_Date, In_Batch, OpenJaw');
INSERT INTO `QP_DW_RMALLOC`.`config_column_names` (`tableName`, `columns`) VALUES ('actionable_sectors_output', 'Sector, Channel, Carrier, Min_QP, Max_QP, Min_QP_RBD, Max_QP_RBD, Action_RBD, XQP_Fare, Fare_difference, PLF, Instances, Base_XQP_Fare, Distance, New_Fare_Point, Yield, Action_required, Req_Min_Instances, Req_Min_FD, Req_Min_Yield, Req_Discared_Carrier, Req_Forward_PLF, `Constraint`, run_id, created_at, route, analyst');

INSERT INTO `QP_DW_RMALLOC`.`currentVersion` (`tableName`, `curr_version`) VALUES ('QP_DW_PRIAUT.qp_fares', 'c313c26b-ddc7-422a-bd3b-b62ad53588f6');
INSERT INTO `QP_DW_RMALLOC`.`currentVersion` (`tableName`, `curr_version`) VALUES ('QP_DW_PRIAUT.tax_mast_dom', '3204748a-cbe4-4c2e-8629-864b624b5e4e');
INSERT INTO `QP_DW_RMALLOC`.`currentVersion` (`tableName`, `curr_version`) VALUES ('QP_DW_PRIAUT.tax_mast_intl', '4c41a8cf-26bd-4f44-8c60-867f75763142');


CREATE TABLE QP_DW_PRIAUT.pricing_run_audits (
    run_id VARCHAR(36),
    start_time DATETIME,
    end_time DATETIME,
    table_name VARCHAR(50),
    total_actionable_records INT,
    max_actions INT,
    min_actions INT,
    constraints_satisfied INT,
    route VARCHAR(1),
    qp_fares VARCHAR(36),
    tax_master VARCHAR(36),
    created_at DATETIME
);