import boto3
import time
from botocore.exceptions import ClientError, BotoCoreError
from common.config.config_settings import (
    RM_WEBSERVICE_AWS_ACCESS_KEY_NEW_PROD,
    RM_WEBSERVICE_AWS_SECRET_KEY_NEW_PROD,
    S3_BUCKET_NAME,
    S3_FILE_PATH_NAME,
    S3_PREFIX
)


class S3Helper:
    def __init__(self, logger):
        self.logger = logger
        self.bucket_name = S3_BUCKET_NAME
        self._s3_client = None
        self._create_s3_client()

    @property
    def s3_client(self):
        """Lazy-load and reconnect S3 client if needed"""
        if self._s3_client is None:
            self._create_s3_client()
        return self._s3_client

    def _create_s3_client(self):
        """Create S3 client with error handling"""
        try:
            self._s3_client = boto3.client(
                "s3",
                aws_access_key_id=RM_WEBSERVICE_AWS_ACCESS_KEY_NEW_PROD,
                aws_secret_access_key=RM_WEBSERVICE_AWS_SECRET_KEY_NEW_PROD,
                region_name='ap-south-1',
            )
            self.logger.info("S3 client created successfully")
        except Exception as e:
            self.logger.error(f"Failed to create S3 client: {e}")
            raise

    def _reset_s3_client(self):
        """Reset the S3 client on connection issues"""
        self._s3_client = None
        self.logger.info("S3 client reset")

    def generate_path(self, table_name: str, file_name: str) -> str:
        return f"{S3_FILE_PATH_NAME}{table_name}/{file_name}_{table_name}"

    def store_file(self, file_bytes, table_name: str, file_name: str, max_retries: int = 3):
        """Upload file bytes to S3 with automatic retry and reconnection"""
        file_path = self.generate_path(table_name, file_name)
        backoff_seconds = 5

        for attempt in range(max_retries):
            try:
                response = self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=file_path,
                    Body=file_bytes
                )

                # Check if ETag is present → successful upload
                e_tag = response.get("ETag")
                if not e_tag:
                    raise RuntimeError("Failed to upload file to S3: missing ETag")

                return True

            except (BotoCoreError, ClientError) as e:
                self.logger.error(f"AWS S3 connection error (attempt {attempt + 1}/{max_retries}): {e}")
                self._reset_s3_client()

                if attempt < max_retries - 1:
                    self.logger.info(f"Retrying in {backoff_seconds} seconds...")
                    time.sleep(backoff_seconds)
                else:
                    self.logger.error(f"Max retries ({max_retries}) reached for uploading {file_path}")
                    raise

            except Exception as e:
                self.logger.error(f"Failed to upload file to S3: {e}")
                raise

    def list_files(self, max_retries: int = 3):
        """Returns List of S3 files with automatic retry and reconnection."""
        backoff_seconds = 5

        for attempt in range(max_retries):
            try:
                response = self.s3_client.list_objects_v2(
                    Bucket=self.bucket_name,
                    Prefix=S3_PREFIX
                )

                if 'Contents' not in response:
                    return []

                # Filter out directories and sort by last modified
                sorted_files = sorted(
                    [obj for obj in response['Contents'] if not obj['Key'].endswith('/')],
                    key=lambda x: x['LastModified']
                )
                
                return sorted_files

            except (BotoCoreError, ClientError) as e:
                self.logger.error(f"AWS S3 connection error listing files (attempt {attempt + 1}/{max_retries}): {e}")
                self._reset_s3_client()

                if attempt < max_retries - 1:
                    self.logger.info(f"Retrying in {backoff_seconds} seconds...")
                    time.sleep(backoff_seconds)
                else:
                    self.logger.error(f"Max retries ({max_retries}) reached for listing S3 files")
                    raise

            except Exception as e:
                self.logger.error(f"Failed to list S3 files: {e}")
                raise
    
    def get_object(self, key):
        try:
            response = self.s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=key)
            return response['Body']
        except Exception as e:
            self.logger.error(f"Failed to get S3 object: {e}")
            raise
    
    def delete_object(self, key):
        try:
            self.s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=key)
        except Exception as e:
            self.logger.error(f"Error deleting S3 object: {e}")