CREATE DATABASE  IF NOT EXISTS `qp_dw_rmalloc`
USE `qp_dw_rmalloc`;

CREATE TABLE `rm_parameter_values` (
  `parameterKey` varchar(255) NOT NULL,
  `parameterValue` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`parameterKey`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO `rm_parameter_values` VALUES ('B2B_FARE_PRICE_COMPARISON','0.8'),('LINEAR_JUMP_VALUE','5'),('MIN_D3_D4_VALUE','5'),('PLF_CURVE_VALUE','85');