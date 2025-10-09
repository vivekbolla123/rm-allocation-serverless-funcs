import pandas as pd
from sqlalchemy import create_engine, text
import configsettings
from constants_utils import ConstantsUtils
from datetime import datetime, timedelta
import traceback
import boto3
from uuid import uuid4
from os import getenv
import io

class CugReport:
    def __init__(self, logger, params, overrides, context):
        """Initialize CalendarUpdate with required connections and constants."""
        self.logger = logger
        self.rm_rd_conn = params['wrconn']

    def get_cug_run(self):
        query = """
        WITH parsed_data AS (
            SELECT 
                run_id,
                start_time,
                end_time,
                -- Extract flight information from request JSON and remove quotes
                JSON_UNQUOTE(JSON_EXTRACT(request, '$.flight')) as flight,
                -- Extract departure date from request JSON
                JSON_UNQUOTE(JSON_EXTRACT(request, '$.startDate')) as departure_date,
                -- Extract specific values from result JSON
                JSON_EXTRACT(result, '$.WA') as WA,
                JSON_EXTRACT(result, '$.WB') as WB, 
                JSON_EXTRACT(result, '$.WC') as WC,
                JSON_EXTRACT(result, '$.WD') as WD,
                JSON_EXTRACT(result, '$.WE') as WE
            FROM QP_DW_RMALLOC.navitaire_allocation_audit
            WHERE run_id = (
                SELECT run_id 
                FROM QP_DW_RMALLOC.allocation_run_audit 
                WHERE dtd_start = 10 AND dtd_end = 30 
                ORDER BY start_time DESC 
                LIMIT 1
            )
        )
        SELECT 
            run_id,
            start_time,
            end_time,
            TRIM(flight) as flight,
            departure_date,
            CAST(WA as SIGNED) as WA,
            CAST(WB as SIGNED) as WB,
            CAST(WC as SIGNED) as WC,
            CAST(WD as SIGNED) as WD,
            CAST(WE as SIGNED) as WE
        FROM parsed_data
        WHERE flight IS NOT NULL
        ORDER BY departure_date asc;"""
        try:
            data = pd.read_sql(query, self.rm_rd_conn)
        except Exception as e:
            self.logger.error(f"Error fetching CUG rbds: {str(e)}")
            raise
        return data
    
    def upload_to_s3(self, df, run_id):
        try:
            
            # Create S3 client
            s3_client = boto3.client('s3')
            
            # Define bucket and file path
            bucket_name = configsettings.S3_BUCKET_NAME
            
            # Create the path with the cug_report folder
            s3_folder_path = f"{configsettings.S3_FILE_PATH}rm-admin/cug_report"
            file_name = f"{run_id}_cug_report.csv"
            s3_file_path = f"{s3_folder_path}/{file_name}"
            
            # Convert DataFrame to CSV
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_content = csv_buffer.getvalue()
            
            # Upload to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_file_path,
                Body=csv_content
            )
            
            return True
            
        except Exception as e:
            error_msg = f"Error uploading CUG report data to S3: {str(e)}"
            self.logger.error(error_msg)
            traceback.print_exc()
            return False

    def run(self):
        """Main method to run daily file generation."""
        try:
            # Get the CUG run data (run_id is determined by the subquery in get_cug_run)
            df = self.get_cug_run()
            
            if df.empty:
                self.logger.warning("No data found for CUG report")
                return False
            
            # Extract run_id from the first row
            run_id = df['run_id'].iloc[0] if 'run_id' in df.columns else 'unknown'
            
            # Upload to S3
            upload_success = self.upload_to_s3(df, run_id)
            
            if upload_success:
                self.logger.info(f"CUG report generation completed successfully for run_id: {run_id}")
                return True
            else:
                self.logger.error("Failed to upload CUG report to S3")
                return False
                
        except Exception as e:
            error_msg = f"Error in CUG report generation: {str(e)}"
            self.logger.error(error_msg)
            traceback.print_exc()
            return False