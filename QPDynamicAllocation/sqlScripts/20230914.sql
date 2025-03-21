ALTER TABLE `QP_DW_RMALLOC`.`run_summary` 
ADD COLUMN `AirlineCode_Max` VARCHAR(45) NULL AFTER `MktFare_Min`,
ADD COLUMN `FlightNumber_Max` VARCHAR(45) NULL AFTER `AirlineCode_Max`,
ADD COLUMN `AirlineCode_Min` VARCHAR(45) NULL AFTER `TgtLoad`,
ADD COLUMN `FlightNumber_Min` VARCHAR(45) NULL AFTER `AirlineCode_Min`,
ADD COLUMN `Dev_BKGCurve` VARCHAR(45) NULL AFTER `FlightNumber_Min`,
ADD COLUMN `StrategyReference` VARCHAR(45) NULL AFTER `Dev_BKGCurve`;

ALTER TABLE `QP_DW_RMALLOC`.`run_summary` 
ADD COLUMN `backstop` VARCHAR(45) NULL AFTER `StrategyReference`;

-- Already done
INSERT INTO `QP_DW_RMALLOC`.`rm_parameter_values` (`parameterKey`, `parameterValue`) VALUES ('B2B_RDB_COUNT', '20');
INSERT INTO `QP_DW_RMALLOC`.`rm_parameter_values` (`parameterKey`, `parameterValue`) VALUES ('AU_COLUMN_LENGTH', '86');
UPDATE `QP_DW_RMALLOC`.`rm_parameter_values` SET `parameterValue` = 'ZJ,ZI,ZH,ZG,ZF,ZE,ZD,ZC,ZB,ZA,Z9,Z8,Z7,Z6,Z5,Z4,Z3,V9,V8,Z2,V7,V6,Z1,V5,V4,Z0,V3,V2,V1,V0,U9,U8,U7,U6,U5,U4,U3,U2,U1,U0,T9,T8,T7,T6,T5,T4,T3,T2,T1,T0,R9,R8,R7,R6,R5,R4,R3,R2,R1,R0,Q9,Q8,Q7,Q6,Q5,Q4,Q3,Q2,Q1,Q0,Q,O,N,M,K,J,I,H,F,E,D,C,B,Y' WHERE (`parameterKey` = 'RBD_VALUES');
UPDATE `QP_DW_RMALLOC`.`rm_parameter_values` SET `parameterValue` = '84' WHERE (`parameterKey` = 'RDB_SIZE');