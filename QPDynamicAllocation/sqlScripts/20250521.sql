UPDATE `QP_DW_RMALLOC`.`config_column_names` SET `columns` = 'NDO,W1_threshold,W2_threshold,W3_threshold,W4_threshold,W5_threshold,W6_threshold,W7_threshold,W8_threshold,W9_threshold,G8_threshold,P_threshold,L_threshold,B2B_threshold,flightNumber' WHERE (`tableName` = 'config_pfl_threshold');

ALTER TABLE `QP_DW_RMALLOC`.`config_pfl_threshold` ADD COLUMN `G8_threshold` DOUBLE NULL DEFAULT NULL AFTER `W9_threshold`;

UPDATE `QP_DW_RMALLOC`.`rm_parameter_values` SET `parameterValue` = '117' WHERE (`parameterKey` = 'AU_COLUMN_LENGTH');

UPDATE `QP_DW_RMALLOC`.`rm_parameter_values` SET `parameterValue` = 'ZJ,ZI,ZH,ZG,ZF,ZE,ZD,ZC,ZB,ZA,Z9,Z8,Z7,Z6,Z5,Z4,Z3,Z2,Z1,Z0,V9,V8,V7,V6,V5,V4,V3,V2,V1,V0,U9,U8,U7,U6,U5,U4,U3,U2,U1,U0,T9,T8,T7,T6,T5,T4,T3,T2,T1,T0,R9,R8,R7,R6,R5,R4,R3,R2,R1,R0,Q9,Q8,Q7,Q6,Q5,Q4,Q3,Q2,Q1,Q0,Q,O,N,M,K,J,I,H,F,E,D,C,B,Y,TF,TE,TD,TC,TB,TY,GJ,GI,GH,GG,GF,GE,GD,GC,GB,GA,WA,WB,WC,WD,WE' WHERE (`parameterKey` = 'RBD_VALUES');