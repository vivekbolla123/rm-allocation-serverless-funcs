CREATE DATABASE  IF NOT EXISTS `qp_dw_rmalloc`
USE `qp_dw_rmalloc`;

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