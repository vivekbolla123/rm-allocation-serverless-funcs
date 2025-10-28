import pandas as pd
from pymongo import MongoClient
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
        self.mongo_client = params['mongoconn']  # MongoDB client
        self.db = self.mongo_client['qp_dw_rmalloc']  # Database name

    def get_cug_run(self):
        """
        Fetch CUG run data from MongoDB collections.
        Equivalent to the MySQL query with subquery and JSON extraction.
        """
        try:
            # Step 1: Get the latest run_id from allocation_run_audit
            allocation_run_audit = self.db['allocation_run_audit']
            latest_run = allocation_run_audit.find_one(
                {
                    'dtd_start': 10,
                    'dtd_end': 30
                },
                sort=[('start_time', -1)]
            )
            
            if not latest_run:
                self.logger.warning("No run found in allocation_run_audit")
                return pd.DataFrame()
            
            run_id = latest_run['run_id']
            self.logger.info(f"Found latest run_id: {run_id}")
            
            # Step 2: Query navitaire_allocation_audit with the run_id
            navitaire_audit = self.db['navitaire_allocation_audit']
            
            # Test 1: Check if documents exist
            pipeline = [
    {
        '$match': {
            'run_id': run_id
        }
    },
    {
        '$addFields': {
            'requestObj': {
                '$function': {
                    'body': 'function(str) { try { return JSON.parse(str); } catch(e) { return {}; } }',
                    'args': ['$request'],
                    'lang': 'js'
                }
            },
            'resultObj': {
                '$function': {
                    'body': 'function(str) { try { return JSON.parse(str); } catch(e) { return {}; } }',
                    'args': ['$result'],
                    'lang': 'js'
                }
            }
        }
    },
    {
        '$match': {
            'requestObj.flight': {'$exists': True, '$ne': None}
        }
    },
    {
        '$project': {
            '_id': 0,
            'run_id': 1,
            'start_time': 1,
            'end_time': 1,
            'flight': '$requestObj.flight',
            'departure_date': '$requestObj.startDate',
            'WA': {'$toInt': {'$ifNull': ['$resultObj.WA', 0]}},
            'WB': {'$toInt': {'$ifNull': ['$resultObj.WB', 0]}},
            'WC': {'$toInt': {'$ifNull': ['$resultObj.WC', 0]}},
            'WD': {'$toInt': {'$ifNull': ['$resultObj.WD', 0]}},
            'WE': {'$toInt': {'$ifNull': ['$resultObj.WE', 0]}}
        }
    },
    {
        '$sort': {'departure_date': 1}
    }
]




            
            results = list(navitaire_audit.aggregate(pipeline))
            
            # Convert to DataFrame
            df = pd.DataFrame(results)
            
            # Trim flight field if it exists
            if not df.empty and 'flight' in df.columns:
                df['flight'] = df['flight'].astype(str).str.strip()
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error fetching CUG rbds: {str(e)}")
            traceback.print_exc()
            raise
    
    def upload_to_s3(self, df, run_id):
        """Upload DataFrame to S3 as CSV file."""
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
            
            self.logger.info(f"Successfully uploaded to S3: {s3_file_path}")
            return True
            
        except Exception as e:
            error_msg = f"Error uploading CUG report data to S3: {str(e)}"
            self.logger.error(error_msg)
            traceback.print_exc()
            return False

    def run(self):
        """Main method to run daily file generation."""
        try:
            # Get the CUG run data (run_id is determined by the query in get_cug_run)
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