ALTER TABLE QP_DW_RMALLOC.cleanup_config ADD COLUMN is_archive_s3 tinyint DEFAULT 0;
ALTER TABLE QP_DW_RMALLOC.cleanup_config RENAME COLUMN is_archive TO is_archive_db;
UPDATE `QP_DW_RMALLOC`.`cleanup_config` SET `is_archive_s3` = '1' WHERE (`id` = '6');
UPDATE `QP_DW_RMALLOC`.`cleanup_config` SET `is_archive_s3` = '1' WHERE (`id` = '8');
UPDATE `QP_DW_RMALLOC`.`cleanup_config` SET `is_archive_db` = '0' WHERE (`id` = '6');
UPDATE `QP_DW_RMALLOC`.`cleanup_config` SET `is_archive_db` = '0' WHERE (`id` = '8');