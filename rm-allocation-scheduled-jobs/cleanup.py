from sqlalchemy import text
import configsettings
import pandas as pd
import awswrangler as wr

class CleanUpConfig:

    def __init__(self, logger, params, overrides, context):
        self.logger = logger
        self.rm_rd_conn = params['rdconn']
        self.rm_wr_conn = params['wrconn']

    def run(self):
        """This method gets configurations from cleanup_config table and performs Delete operation on specified criteria """
        try:
            data_query = text("SELECT schema_name, table_name, time_column, max_days, is_archive_db, is_archive_s3 FROM cleanup_config")
            data = self.rm_rd_conn.execute(data_query)
            if data:
                for row in data:
                    archive = False
                    schema, table_name, time_column, max_days, is_archive_db, is_archive_s3 = row

                    archive = self.archive_before_delete(schema, table_name, time_column, max_days, is_archive_db, is_archive_s3)
                    if archive:
                        delete_query = text(f"DELETE FROM {schema}.{table_name} WHERE {time_column} < NOW() - INTERVAL :max_days DAY")
                        result = self.rm_wr_conn.execute(delete_query, {"max_days": max_days})
                        self.logger.info(f"{result.rowcount} records deleted from {schema}.{table_name}!")
            else:
                self.logger.info("No table configured for clean up!")
        except Exception as err:
            self.logger.error(err)

    def archive_before_delete(self, schema, table_name, time_column, max_days, is_archive_db, is_archive_s3):
        try:
            if not is_archive_db and not is_archive_s3:
                self.logger.info("Archive Skipped as per configuration!")
                return True

            if is_archive_s3:
                date_query = f"""
                    SELECT DATE(MIN({time_column})) as min_date,
                        DATE(MAX({time_column})) as max_date
                    FROM {schema}.{table_name}
                    WHERE {time_column} < NOW() - INTERVAL {max_days} DAY
                """
                dates_df = pd.read_sql(date_query, con=self.rm_rd_conn)
                min_date = dates_df.iloc[0]['min_date']
                max_date = dates_df.iloc[0]['max_date']

                if not min_date or not max_date:
                    self.logger.info("Archive skipped since no data to archive")
                    return True

                base_path = f"s3://{configsettings.S3_BUCKET_NAME}/{configsettings.S3_FILE_PATH}rm-admin/archives"

                query = f"""
                    SELECT *, DATE({time_column}) as archive_date
                    FROM {schema}.{table_name}
                    WHERE {time_column} < NOW() - INTERVAL {max_days} DAY
                """
                df = pd.read_sql(sql=query, con=self.rm_rd_conn)

                if min_date == max_date:
                    year, month, day = str(min_date).split('-')
                    s3_path = base_path + f"/table={table_name}/year={year}/month={month}/day={day}/"
    
                    wr.s3.to_parquet(
                        df=df.drop(columns=['archive_date']),
                        path=s3_path,
                        dataset=True,
                        mode='append',
                        compression='snappy',
                        filename_prefix=f"{table_name}_{str(min_date).replace('-', '')}_"
                    )
                    self.logger.info(f"Archived records for {min_date} to S3 at {s3_path}")
                else:
                    grouped = df.groupby('archive_date')
                    for archive_date, group_df in grouped:
                        year, month, day = str(archive_date).split('-')
                        s3_path = base_path + f"/table={table_name}/year={year}/month={month}/day={day}/"
        
                        wr.s3.to_parquet(
                            df=group_df.drop(columns=['archive_date']), 
                            path=s3_path,
                            dataset=True,
                            mode='append',
                            compression='snappy',
                            filename_prefix=f"{table_name}_{str(archive_date).replace('-', '')}_"
                        )
                        self.logger.info(f"Archived records for {archive_date} to S3 at {s3_path}")

                self.logger.info(f"Successfully archived records from {schema}.{table_name} to S3")
            
            if is_archive_db:
                archive_query = text(f"INSERT INTO QP_RM_ARCHIVES.{table_name} SELECT * FROM {schema}.{table_name} WHERE {time_column} < NOW() - INTERVAL :max_days DAY")
                self.rm_rd_conn.execute(archive_query, {"max_days": max_days})

            return True
        except Exception as error:
            self.logger.error(error)
            return False