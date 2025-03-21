CREATE DATABASE  IF NOT EXISTS `QP_DW_RMALLOC`
USE `QP_DW_RMALLOC`;

-- allocation_acceptable_range_d1
CREATE TABLE `allocation_acceptable_range_d1` (
  `airports` varchar(45) NOT NULL,
  `ar_start` varchar(45) DEFAULT NULL,
  `ar_end` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`airports`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- d1_d2_strategies
CREATE TABLE `d1_d2_strategies` (
  `ndo_band` int NOT NULL,
  `dplf_band` int NOT NULL,
  `strategy` varchar(45) NOT NULL,
  `criteria` varchar(45) DEFAULT NULL,
  `time_range` varchar(45) DEFAULT NULL,
  `offset` int DEFAULT NULL,
  PRIMARY KEY (`ndo_band`,`dplf_band`,`strategy`),
  UNIQUE KEY `Group` (`ndo_band`,`dplf_band`,`strategy`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- dplf_bands
CREATE TABLE `dplf_bands` (
  `dplf_band` int NOT NULL,
  `start` int DEFAULT NULL,
  `end` int DEFAULT NULL,
  PRIMARY KEY (`dplf_band`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO `dplf_bands` VALUES (0,-9999,-15),(1,-15,-10),(2,-10,-5),(3,-5,5),(4,5,10),(5,10,15),(6,15,9999);

-- ndo_bands
CREATE TABLE `ndo_bands` (
  `ndo_band` int NOT NULL,
  `start` int DEFAULT NULL,
  `end` int DEFAULT NULL,
  PRIMARY KEY (`ndo_band`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO `ndo_bands` VALUES (0,0,1),(1,1,2),(2,2,3),(3,3,7),(4,7,14),(5,14,30),(6,30,60),(7,60,9999);

-- time_ranges
CREATE TABLE `time_ranges` (
  `time_range` varchar(45) NOT NULL,
  `start` varchar(45) DEFAULT NULL,
  `end` varchar(45) DEFAULT NULL,
  `type` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`time_range`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO `time_ranges` VALUES ('AR',NULL,NULL,'REF'),('ExTR','-120','+120','REL'),('FR','00:00','23:59','ABS'),('TR','0','0','REL');

-- Parameter table
CREATE TABLE `rm_parameter_values` (
  `parameterKey` varchar(255) NOT NULL,
  `parameterValue` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`parameterKey`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO `rm_parameter_values` VALUES ('B2B_FARE_PRICE_COMPARISON','0.8'),('LINEAR_JUMP_VALUE','5'),('MIN_D3_D4_VALUE','2'),('PLF_CURVE_VALUE','85');

-- File Upload Table
CREATE TABLE `file_upload_audit` (
  `tableName` varchar(255) NOT NULL,
  `curr_version` varchar(255) NOT NULL,
  `start_time` varchar(45) DEFAULT NULL,
  `end_time` varchar(45) DEFAULT NULL,
  `userName` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`curr_version`, `tableName`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- current Version Table
CREATE TABLE `currentVersion` (
  `tableName` varchar(45) NOT NULL,
  `curr_version` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`tableName`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO `currentVersion` (`tableName`) VALUES ('allocation_acceptable_range_d1'), ('Curves'),('d1_d2_strategies'), ('dplf_bands'),('Fares'),('market_list'),('market_list_adhoc'),('ndo_bands'),('time_ranges');

-- Update Input status table
INSERT INTO `inputs_status` VALUES ('allocation_acceptable_range_d1',_binary '\0'),('Curves',_binary '\0'),('d1_d2_strategies',_binary '\0'),('dplf_bands',_binary '\0'),('Fares',_binary '\0'),('market_list',_binary '\0'),('market_list_adhoc',_binary '\0'),('ndo_bands',_binary '\0'),('time_ranges',_binary '\0');

-- Alter current tables
ALTER TABLE `market_list` ADD COLUMN `B2BFactor` VARCHAR(45) NULL AFTER `B2CBackstop`;
ALTER TABLE `market_list` CHANGE COLUMN `bookedStatus` `strategyFlag` TEXT NULL DEFAULT NULL ;
ALTER TABLE `Curves` ADD COLUMN `average_fare` DOUBLE NULL AFTER `Pax`;

ALTER TABLE `market_list`
ADD COLUMN `UUID` VARCHAR(45) NULL AFTER `B2BFactor`,
CHANGE COLUMN `fareOffset` `fareOffset` TEXT NULL DEFAULT NULL ,
CHANGE COLUMN `B2BFactor` `B2BFactor` TEXT NULL DEFAULT NULL ;

-- Alter Allocation Run Audit
ALTER TABLE `allocation_run_audit`
ADD COLUMN `dtd_start` int NULL AFTER `user`,
ADD COLUMN `dtd_end` int NULL AFTER `dtd_start`,
ADD COLUMN `Curves` varchar(45) NULL AFTER `dtd_end`,
ADD COLUMN `d1_d2_strategies` varchar(45) NULL AFTER `Curves`,
ADD COLUMN `market_list` varchar(45) NULL AFTER `d1_d2_strategies`,
ADD COLUMN `Fares` varchar(45) NULL AFTER `market_list`,
ADD COLUMN `type` VARCHAR(45) NULL AFTER `Fares`;

-- Alter Run Summary
ALTER TABLE `run_summary`
ADD COLUMN `hiFare` VARCHAR(10) NULL AFTER `RunId`,
ADD COLUMN `fareAnchor` VARCHAR(5) NULL AFTER `hiFare`,
ADD COLUMN `fareOffset` VARCHAR(5) NULL AFTER `fareAnchor`;