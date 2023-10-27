-- Create table in QP_RM_REPORTS Database.
CREATE TABLE `QP_RM_REPORTS`.`allocation_summary_report` (
  `report_reference` datetime DEFAULT NULL,
  `run_id` varchar(50) DEFAULT NULL,
  `type` varchar(45) DEFAULT NULL,
  `dtd` varchar(45) DEFAULT NULL,
  `start_time` datetime DEFAULT NULL,
  `end_time` datetime DEFAULT NULL,
  `actual_end_time` datetime DEFAULT NULL,
  `failures` int DEFAULT NULL,
  `total` int DEFAULT NULL,
  `user` varchar(100) DEFAULT NULL,
  `runTime` time DEFAULT NULL,
  `HowCount` int DEFAULT NULL,
  `market_list_name` varchar(45) DEFAULT NULL,
  `Id_marketList` varchar(45) DEFAULT NULL,
  `Id_Curves` varchar(45) DEFAULT NULL,
  `Id_Strategy` varchar(45) DEFAULT NULL,
  `Id_QpFares` varchar(45) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci

-- Add column
ALTER TABLE `QP_DW_RMALLOC`.`allocation_run_audit` 
ADD COLUMN `is_run_completed` VARCHAR(3) NULL AFTER `is_connections_started`;
