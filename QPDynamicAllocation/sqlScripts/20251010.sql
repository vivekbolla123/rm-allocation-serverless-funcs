CREATE TABLE QP_DW_PRIAUT.`current_table_version` (
  `table_name` varchar(45) NOT NULL,
  `curr_version` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`table_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE QP_DW_PRIAUT.`file_upload_audit` (
  `table_name` varchar(255) NOT NULL,
  `curr_table_version` varchar(255) NOT NULL,
  `start_time` varchar(45) DEFAULT NULL,
  `end_time` varchar(45) DEFAULT NULL,
  `user_name` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`curr_table_version`,`table_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `inputs_file_upload` (
  `obj_id` varchar(50) NOT NULL,
  `status` varchar(100) DEFAULT NULL,
  `init_timestamp` datetime DEFAULT NULL,
  `in_queue_timestamp` datetime DEFAULT NULL,
  `inprogress_timestamp` datetime DEFAULT NULL,
  `done_timestamp` datetime DEFAULT NULL,
  `user_name` varchar(255) DEFAULT NULL,
  `role` text,
  `table_name` varchar(100) DEFAULT NULL,
  `errors` longtext,
  `total_rows` int DEFAULT NULL,
  `total_time_taken` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`obj_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

UPDATE `QP_DW_PRIAUT`.`config_column_names` SET `table_name` = 'qp_fares' WHERE (`config_id` = '1');
UPDATE `QP_DW_PRIAUT`.`config_column_names` SET `table_name` = 'tax_mast_dom' WHERE (`config_id` = '2');
UPDATE `QP_DW_PRIAUT`.`config_column_names` SET `table_name` = 'tax_mast_intl' WHERE (`config_id` = '3');

CREATE TABLE config_sale_dates (
	  sale_id VARCHAR(55) PRIMARY KEY,
    sector VARCHAR(50) NOT NULL,
    type VARCHAR(50) NOT NULL,
    currency VARCHAR(10),
    sale_start DATE NOT NULL,
    sale_end DATE NOT NULL,
    
    UNIQUE KEY uk_sale_rule (sale_id, sector, sale_start, sale_end)
);

CREATE TABLE config_sale_discounts (
	  sale_id VARCHAR(55) NOT NULL,
	  departure_start DATE NOT NULL,
    departure_end DATE NOT NULL,
    discount VARCHAR(20) NOT NULL,
    
    UNIQUE KEY uk_sale_rule (sale_id, departure_start, departure_end)
);

INSERT INTO `QP_DW_PRIAUT`.`config_column_names` (`config_id`, `table_name`, `column_names`) VALUES ('8', 'config_sale_dates', 'sale_id,sector,type,currency,sale_start,sale_end');
INSERT INTO `QP_DW_PRIAUT`.`config_column_names` (`config_id`, `table_name`, `column_names`) VALUES ('9', 'config_sale_discounts', 'sale_id,departure_start,departure_end,discount');

ALTER TABLE QP_DW_PRIAUT.config_column_names
ADD COLUMN id_column_names TEXT;

UPDATE `QP_DW_PRIAUT`.`config_column_names` SET `id_column_names` = 'sale_id, sector, sale_start, sale_end' WHERE (`config_id` = '8');
UPDATE `QP_DW_PRIAUT`.`config_column_names` SET `id_column_names` = 'sale_id, departure_start, departure_end' WHERE (`config_id` = '9');

