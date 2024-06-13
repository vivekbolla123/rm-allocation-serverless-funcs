-- Drop the table if it already exists
DROP TABLE IF EXISTS no_show_probabilities;

-- Create the new table
CREATE TABLE no_show_probabilities (
    Sector varchar(10),
    Month varchar(10),
    Cluster int,
    Nint int,
    OBCount int,
    Prob float
);

UPDATE `QP_DW_RMALLOC`.`rm_parameter_values` SET `parameterValue` = '1' WHERE (`parameterKey` = 'OVERBOOKING_END_NDO');
INSERT INTO `QP_DW_RMALLOC`.`rm_parameter_values` (`parameterKey`, `parameterValue`) VALUES ('OVERBOOKING_FARE_ABSOLUTE_VALUE', '1000');
INSERT INTO `QP_DW_RMALLOC`.`rm_parameter_values` (`parameterKey`, `parameterValue`) VALUES ('OVERBOOKING_FARE_LF', '2');
INSERT INTO `QP_DW_RMALLOC`.`rm_parameter_values` (`parameterKey`, `parameterValue`) VALUES ('OVERBOOKING_FARE_PERCENT_VALUE', '1.1');

UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'Sector,Month,Cluster,N,OBCount,Prob' WHERE (`tableName` = 'no_show_probabilities');

