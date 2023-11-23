class CleanUpConfig:

    def __init__(self, logger, params, overrides):
        self.logger = logger
        self.rm_rd_conn = params['rdconn']
        self.rm_wr_conn = params['wrconn']
    
    def run(self):
        """This method gets configurations from cleanup_config table and performs Delete operation on specifed criteria """
        try:
            data = self.rm_rd_conn.execute("SELECT schema_name, table_name, time_column, max_days, is_archive FROM cleanup_config")
            if data:
                for row in data:
                    archive = False
                    schema, table_name, time_column, max_days, is_archive = row
                   
                    archive = self.archive_before_delete(schema, table_name, time_column, max_days, is_archive)
                    if archive:
                        result = self.rm_wr_conn.execute(f"DELETE FROM {schema}.{table_name} WHERE {time_column} < NOW() - INTERVAL {max_days} DAY")
                        self.logger.info(f"{result.rowcount} records deleted from {schema}.{table_name}!")
            self.logger.info("No table configured for clean up!")
        except Exception as err:
            self.logger.error(err)

    def archive_before_delete(self, schema, table_name, time_column, max_days, is_archive):
        try:
            if not is_archive:
                self.logger.info("Archive Skipped as per configuration!")
                return True
            get_records = self.rm_rd_conn.execute(f"SELECT * FROM {schema}.{table_name} WHERE {time_column} < NOW() - INTERVAL {max_days} DAY").fetchall()
            if not get_records:
                self.logger.info(f"No records found from {schema}.{table_name}")
                return False
            for rec in get_records:
                col_len = len(rec)
                values = ('%s, ' * col_len).rstrip(", ")
                archive_data = f"""INSERT INTO QP_RM_ARCHIVES.{table_name} VALUES ({values})"""
                res = self.rm_wr_conn.execute(archive_data, rec)
                self.logger.info(f"{res.rowcount} records Archived in {schema}.{table_name}")
                return True
        except Exception as error:
            self.logger.error(error)
            return False
