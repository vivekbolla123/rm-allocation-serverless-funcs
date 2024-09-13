import logging
from sqlalchemy import text

class QCProcess:

    def __init__(self, logger, params, overrides, context):
        self.logger = logging.getLogger()
        self.rm_rd_conn = params['wrconn']
    
    def run(self):
        self.create_historical_fare_table()
        self.create_profile_fare_table()
        
    def create_historical_fare_table(self):
        # SQL to create the stored procedure
        create_procedure_sql = """
        CREATE PROCEDURE create_historical_fare_table()
        BEGIN
            DECLARE done INT DEFAULT FALSE;
            DECLARE sector_name VARCHAR(255);
            DECLARE sector_list TEXT DEFAULT '';
            DECLARE create_table_sql TEXT DEFAULT '';
            DECLARE sector_cursor CURSOR FOR SELECT DISTINCT sector FROM QP_DW_RMALLOC.config_ownFares;
            DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

            SET SESSION group_concat_max_len = 1000000;

            OPEN sector_cursor;
            read_loop: LOOP
                FETCH sector_cursor INTO sector_name;
                IF done THEN
                    LEAVE read_loop;
                END IF;
                SET sector_list = CONCAT(sector_list, 
                    IF(sector_list = '', '', ', '),
                    'MAX(CASE WHEN sector = "', sector_name, '" THEN fare END) AS `', sector_name, '`');
                SET create_table_sql = CONCAT(create_table_sql,
                    IF(create_table_sql = '', '', ', '),
                    '`', sector_name, '` INT');
            END LOOP;
            CLOSE sector_cursor;

            SET @create_table = CONCAT('
            CREATE TABLE IF NOT EXISTS QP_DW_RMALLOC.min_max_historical (
                Sector VARCHAR(10) PRIMARY KEY,
                ', create_table_sql, '
            )');
            
            PREPARE stmt FROM @create_table;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;

            SET @sql = CONCAT('
            INSERT INTO QP_DW_RMALLOC.min_max_historical
            SELECT 
                month_type AS Sector,
                ', sector_list, '
            FROM (
                SELECT 
                    sector,
                    CONCAT(SUBSTRING(Month, 1, 3), "_min") AS month_type,
                    ROUND(MIN(LEAST(`0`, `1`, `2`, `3`, `4`, `5`, `6`, `7`)) * 0.9) AS fare
                FROM QP_DW_RMALLOC.config_ownFares
                GROUP BY sector, Month
                UNION ALL
                SELECT 
                    sector,
                    CONCAT(SUBSTRING(Month, 1, 3), "_max") AS month_type,
                    ROUND(MAX(GREATEST(`0`, `1`, `2`, `3`, `4`, `5`, `6`, `7`)) * 1.1) AS fare
                FROM QP_DW_RMALLOC.config_ownFares
                GROUP BY sector, Month
            ) AS unpivoted_data
            GROUP BY month_type
            ORDER BY 
                CASE 
                    WHEN SUBSTRING(month_type, 1, 3) = "Jan" THEN 1
                    WHEN SUBSTRING(month_type, 1, 3) = "Feb" THEN 2
                    WHEN SUBSTRING(month_type, 1, 3) = "Mar" THEN 3
                    WHEN SUBSTRING(month_type, 1, 3) = "Apr" THEN 4
                    WHEN SUBSTRING(month_type, 1, 3) = "May" THEN 5
                    WHEN SUBSTRING(month_type, 1, 3) = "Jun" THEN 6
                    WHEN SUBSTRING(month_type, 1, 3) = "Jul" THEN 7
                    WHEN SUBSTRING(month_type, 1, 3) = "Aug" THEN 8
                    WHEN SUBSTRING(month_type, 1, 3) = "Sep" THEN 9
                    WHEN SUBSTRING(month_type, 1, 3) = "Oct" THEN 10
                    WHEN SUBSTRING(month_type, 1, 3) = "Nov" THEN 11
                    WHEN SUBSTRING(month_type, 1, 3) = "Dec" THEN 12
                END,
                month_type
            ON DUPLICATE KEY UPDATE
            ');

            SET @update_sql = (SELECT GROUP_CONCAT(
                CONCAT('`', sector, '` = VALUES(`', sector, '`)')
                SEPARATOR ', '
            ) FROM (SELECT DISTINCT sector FROM QP_DW_RMALLOC.config_ownFares) AS sectors);

            SET @sql = CONCAT(@sql, @update_sql);

            PREPARE stmt FROM @sql;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;

        END
        """

        # Execute the CREATE PROCEDURE statement
        self.rm_rd_conn.execute(create_procedure_sql)

        # Call the stored procedure
        self.rm_rd_conn.execute("CALL create_historical_fare_table()")        
    
    def create_profile_fare_table(self):
        create_procedure_sql = """
        CREATE PROCEDURE create_profile_fare_table()
        BEGIN
            DECLARE done INT DEFAULT FALSE;
            DECLARE sector_name VARCHAR(255);
            DECLARE sector_list TEXT DEFAULT '';
            DECLARE create_table_sql TEXT DEFAULT '';
            DECLARE sector_cursor CURSOR FOR SELECT DISTINCT sector FROM QP_DW_RMALLOC.config_profile_fares;
            DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

            SET SESSION group_concat_max_len = 1000000;

            OPEN sector_cursor;
            read_loop: LOOP
                FETCH sector_cursor INTO sector_name;
                IF done THEN
                    LEAVE read_loop;
                END IF;
                SET sector_list = CONCAT(sector_list, 
                    IF(sector_list = '', '', ', '),
                    'MAX(CASE WHEN sector = "', sector_name, '" THEN fare END) AS `', sector_name, '`');
                SET create_table_sql = CONCAT(create_table_sql,
                    IF(create_table_sql = '', '', ', '),
                    '`', sector_name, '` INT');
            END LOOP;
            CLOSE sector_cursor;

            SET @create_table = CONCAT('
            CREATE TABLE IF NOT EXISTS QP_DW_RMALLOC.min_max_profile (
                Sector VARCHAR(10) PRIMARY KEY,
                ', create_table_sql, '
            )');
            
            PREPARE stmt FROM @create_table;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;

            SET @sql = CONCAT('
            INSERT INTO QP_DW_RMALLOC.min_max_profile
            SELECT 
                month_type AS Sector,
                ', sector_list, '
            FROM (
                SELECT 
                    sector,
                    CONCAT(SUBSTRING(Month, 1, 3), "_min") AS month_type,
                    ROUND(MIN(LEAST(Q0, Q1, Q2, Q3, Q4, Q5, Q6, Q7, Q8, Q9, Q10, Q11, Q12, Q13, Q14, Q15, Q16, Q17, Q18, Q19, Q20)) * 0.9) AS fare
                FROM QP_DW_RMALLOC.config_profile_fares
                GROUP BY sector, Month
                UNION ALL
                SELECT 
                    sector,
                    CONCAT(SUBSTRING(Month, 1, 3), "_max") AS month_type,
                    ROUND(MAX(GREATEST(Q0, Q1, Q2, Q3, Q4, Q5, Q6, Q7, Q8, Q9, Q10, Q11, Q12, Q13, Q14, Q15, Q16, Q17, Q18, Q19, Q20)) * 1.1) AS fare
                FROM QP_DW_RMALLOC.config_profile_fares
                GROUP BY sector, Month
            ) AS unpivoted_data
            GROUP BY month_type
            ORDER BY 
                CASE 
                    WHEN SUBSTRING(month_type, 1, 3) = "Jan" THEN 1
                    WHEN SUBSTRING(month_type, 1, 3) = "Feb" THEN 2
                    WHEN SUBSTRING(month_type, 1, 3) = "Mar" THEN 3
                    WHEN SUBSTRING(month_type, 1, 3) = "Apr" THEN 4
                    WHEN SUBSTRING(month_type, 1, 3) = "May" THEN 5
                    WHEN SUBSTRING(month_type, 1, 3) = "Jun" THEN 6
                    WHEN SUBSTRING(month_type, 1, 3) = "Jul" THEN 7
                    WHEN SUBSTRING(month_type, 1, 3) = "Aug" THEN 8
                    WHEN SUBSTRING(month_type, 1, 3) = "Sep" THEN 9
                    WHEN SUBSTRING(month_type, 1, 3) = "Oct" THEN 10
                    WHEN SUBSTRING(month_type, 1, 3) = "Nov" THEN 11
                    WHEN SUBSTRING(month_type, 1, 3) = "Dec" THEN 12
                END,
                month_type
            ON DUPLICATE KEY UPDATE
            ');

            SET @update_sql = (SELECT GROUP_CONCAT(
                CONCAT('`', sector, '` = VALUES(`', sector, '`)')
                SEPARATOR ', '
            ) FROM (SELECT DISTINCT sector FROM QP_DW_RMALLOC.config_profile_fares) AS sectors);

            SET @sql = CONCAT(@sql, @update_sql);

            PREPARE stmt FROM @sql;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;

        END
        """

        # Execute the CREATE PROCEDURE statement
        self.rm_rd_conn.execute(create_procedure_sql)

        # Call the stored procedure
        self.rm_rd_conn.execute("CALL create_profile_fare_table()")