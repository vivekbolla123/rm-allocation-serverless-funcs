from datetime import datetime
import io
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from threading import Lock, Event, Thread
from queue import Queue
import time
import traceback

from common.utils.logger import logger
from common.constants.constants import *
from common.config.config_settings import S3_FILE_PATH_NAME, S3_PREFIX
from common.utils.common_helper import calculate_duration
from common.utils.s3_helper import S3Helper
from common.utils.db_helper import DbHelper
from common.processors.sale_discount_processor import pivot_sale_fares


class FileUploadWorker:
    """Handles individual file upload - each instance processes one file"""
    
    def __init__(self, file_key):
        self.file_key = file_key
        self.error_occurred = 0
        self.list_of_errors = []
        self.s3_helper = S3Helper(logger)
        self.db_helper = DbHelper()
        self.obj_id = None
        self.num_of_rows = 0

    def insert_file_data(self, data, table_name, user_name):
        uuid =  self.obj_id
        uuid_clean = uuid.replace("-", "_")
        temp_table_name = f"{table_name}_temp_{uuid_clean}"
        
        try:
            columns, id_columns = self.db_helper.get_column_headers(table_name)
            headers = [col.strip() for col in columns.split(COMMA)]
            
            self.db_helper.insert_statement(data, headers, temp_table_name)
            self.db_helper.delete_values(table_name, temp_table_name, id_columns)
            self.db_helper.copy_from_temp_to_main_table(table_name, temp_table_name, data)
            self.db_helper.update_audit_statement(table_name, uuid)
            
            # Delete temp table
            self.db_helper.delete_table(temp_table_name)
            
        except Exception as e:
            raise

    def validate_and_insert(self, table, user_name):
        try:
            self.db_helper.update_status_in_progress(self.obj_id)
            
            file_object = self.s3_helper.get_object(self.file_key)
            file_content = file_object.read().decode('utf-8')
            
            reader = io.StringIO(file_content)

            # Count rows (excluding header)
            lines = reader.readlines()
            row_count = len(lines) - 1 if len(lines) > 0 else 0
            self.num_of_rows = row_count

            reader.seek(0)
            csv_df = pd.read_csv(reader)
            
            if table == SALE_DISCOUNTS_TABLE:
                try:
                    sales_df, discounts_df = pivot_sale_fares(csv_df)
                    self.insert_file_data(sales_df, SALE_DATES_TABLE, user_name)
                    self.insert_file_data(discounts_df, SALE_DISCOUNTS_TABLE, user_name)
                except Exception as e:
                    self.list_of_errors.append("Error Uploading Strategy File to DB. Please check file content")
                    raise RuntimeError(f"Error processing strategy file content: {str(e)}")
            else:
                try:
                    self.insert_file_data(csv_df, table, user_name)
                    self.s3_helper.store_file(file_content.encode("utf-8"), table, self.obj_id)
                except Exception as e:
                    raise RuntimeError(f"Error processing file content: {str(e)}")
                    
        except Exception as e:
            self.list_of_errors.append("Failure During Validation")
            logger.error(f"Error in validate_and_insert: {str(e)}")
            raise

    def process_file(self):
        """Main method to process a single file"""
        final_key = self.file_key
        length_of_key = len(final_key)
        self.obj_id = final_key[len(S3_PREFIX) : length_of_key - 4]

        table = None
        role = None
        user_name = None
        start_time = None

        upload_info, self.list_of_errors, self.error_occurred = self.db_helper.get_upload_info(
            self.obj_id, self.list_of_errors, self.error_occurred
        )
        
        if upload_info:
            table = upload_info["table"]
            role = upload_info["role"]
            user_name = upload_info["user_name"]
            start_time = upload_info["start_time"]

        logger.info(f"[{self.obj_id}] Processing table: {table}")

        try:
            self.validate_and_insert(table, user_name)
        except Exception as e:
            self.error_occurred = 1
            if not self.list_of_errors:
                self.list_of_errors.append("Validation has failed while inserting data in table")
                self.db_helper.add_errors_to_table(self.list_of_errors, self.obj_id)
            logger.error(f"[{self.obj_id}] Error: {e}")

        if self.error_occurred == 0:
            status = UPLOAD_QUEUE_STATUS_DONE
        else:
            status = UPLOAD_QUEUE_STATUS_FAILED

        end_time = datetime.now()
        duration = calculate_duration(start_time, end_time)
        logger.info(f"[{self.obj_id}] Status: {status}")

        self.list_of_errors = self.db_helper.update_final_upload_info(
            status, end_time, self.num_of_rows, duration, self.obj_id, self.list_of_errors
        )

        local_test_dir = "locals-pricing"
        s3_path_len = len(S3_FILE_PATH_NAME)
        if final_key[s3_path_len:s3_path_len+len(local_test_dir)].lower() != local_test_dir:
            self.s3_helper.delete_object(final_key)

        return {
            'obj_id': self.obj_id,
            'status': status,
            'error_occurred': self.error_occurred,
            'errors': self.list_of_errors.copy()
        }


class FileQueue:
    """Thread-safe queue that ensures uniqueness of file keys"""
    
    def __init__(self):
        self.queue = Queue()
        self.processing_keys = set()
        self.lock = Lock()
    
    def add_file(self, file_key):
        """Add file key to queue if not already being processed"""
        with self.lock:
            if file_key not in self.processing_keys:
                self.processing_keys.add(file_key)
                self.queue.put(file_key)
                return True
        return False
    
    def get_file(self, timeout=1):
        """Get next file key from queue (blocking with timeout)"""
        try:
            return self.queue.get(timeout=timeout)
        except:
            return None
    
    def mark_complete(self, file_key):
        """Mark a file as processed and remove from tracking"""
        self.queue.task_done()
        # CRITICAL: Remove from processing_keys so it can be reprocessed if uploaded again
        with self.lock:
            self.processing_keys.discard(file_key)
    
    def size(self):
        """Return current queue size"""
        return self.queue.qsize()


class S3FileScanner:
    """Producer: Continuously scans S3 and adds new files to queue"""
    
    def __init__(self, file_queue, scan_interval=5):
        self.file_queue = file_queue
        self.scan_interval = scan_interval
        self.s3_helper = S3Helper(logger)
        self.db_helper = DbHelper()
        self.running = False
        self.stop_event = Event()
    
    def start(self):
        """Start scanning S3 in a loop - RUNS FOREVER"""
        self.running = True
        logger.info(f"S3 Scanner started (scanning every {self.scan_interval}s)")
        
        while not self.stop_event.is_set():
            try:
                self._scan_and_queue()
            except Exception as e:
                # Log error but continue running
                logger.error(f"Error in S3 scanner (will retry): {e}")
                logger.error(traceback.format_exc())
            
            # Wait for next scan interval
            self.stop_event.wait(self.scan_interval)
        
        logger.info("S3 Scanner stopped")
    
    def _scan_and_queue(self):
        """Scan S3 and add new files to queue"""
        try:
            sorted_files = self.s3_helper.list_files()
            
            if not sorted_files:
                logger.debug("No files found in S3")
                return
            
            new_files_count = 0
            for file in sorted_files:
                file_key = file['Key']
                obj_id = file_key[len(S3_PREFIX) : len(file_key) - 4]
                
                # Check database status to avoid reprocessing
                try:
                    status = self.db_helper.get_upload_status(obj_id)
                    if status in [UPLOAD_QUEUE_STATUS_IN_PROGRESS, UPLOAD_QUEUE_STATUS_DONE]:
                        continue
                except Exception as e:
                    # If status check fails, log and allow queueing
                    logger.debug(f"Could not check status for {obj_id}: {e}")
                
                # Add to queue if unique
                if self.file_queue.add_file(file_key):
                    new_files_count += 1
                    logger.info(f"Queued: {file_key}")
            
            if new_files_count > 0:
                logger.info(f"Added {new_files_count} new files to queue (Queue size: {self.file_queue.size()})")
            else:
                logger.debug(f"No new files to queue (Queue size: {self.file_queue.size()})")
                
        except Exception as e:
            logger.error(f"Error scanning S3: {e}")
            raise
    
    def stop(self):
        """Stop the scanner"""
        self.running = False
        self.stop_event.set()


class FileProcessor:
    """Consumer: Continuously processes files from queue - RUNS FOREVER"""
    
    def __init__(self, file_queue, max_workers=4):
        self.file_queue = file_queue
        self.max_workers = max_workers
        self.running = False
        self.stop_event = Event()
        self.overall_errors = []
        self.overall_error_count = 0
        self.results_lock = Lock()
        self.active_futures = {}
        self.futures_lock = Lock()
    
    def start(self):
        """Start processing files from queue - RUNS FOREVER"""
        self.running = True
        logger.info(f"File Processor started (max workers: {self.max_workers})")
        
        with ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="WORKER") as executor:
            while not self.stop_event.is_set():
                try:
                    # Get next file from queue
                    file_key = self.file_queue.get_file(timeout=1)
                    
                    if file_key is None:
                        continue  # No file available, try again
                    
                    # Submit for processing
                    future = executor.submit(self._process_file_safe, file_key)
                    
                    # Track active futures
                    with self.futures_lock:
                        self.active_futures[future] = file_key
                    
                    # Add callback to handle completion
                    future.add_done_callback(self._handle_completion)
                    
                except Exception as e:
                    # Log error but continue processing
                    logger.error(f"Error in processor loop (will continue): {e}")
                    logger.error(traceback.format_exc())
        
        logger.info("File Processor stopped")
    
    def _process_file_safe(self, file_key):
        """Process a single file with exception handling"""
        file_name = file_key.split('/')[-1][:-4]
        
        try:
            logger.info(f"Processing: {file_name}")
            worker = FileUploadWorker(file_key)
            result = worker.process_file()
            return result
            
        except Exception as e:
            # Catch ANY exception during processing
            logger.error(f"CRITICAL: Unhandled exception processing {file_name}: {e}")
            logger.error(traceback.format_exc())
            
            # Return error result so the file is marked as failed
            return {
                'obj_id': file_name,
                'status': UPLOAD_QUEUE_STATUS_FAILED,
                'error_occurred': 1,
                'errors': [f"Critical error: {str(e)}"]
            }
    
    def _handle_completion(self, future):
        """Handle completed file processing"""
        file_key = None
        
        try:
            # Get file_key from tracking
            with self.futures_lock:
                file_key = self.active_futures.pop(future, None)
            
            if file_key is None:
                logger.error("Could not find file_key for completed future")
                return
            
            file_name = file_key.split('/')[-1][:-4]
            
            try:
                result = future.result()
                
                with self.results_lock:
                    if result['error_occurred']:
                        self.overall_error_count += 1
                        self.overall_errors.extend(result['errors'])
                
            except Exception as e:
                logger.error(f"Error getting result for {file_name}: {e}")
                logger.error(traceback.format_exc())
                with self.results_lock:
                    self.overall_error_count += 1
                    self.overall_errors.append(f"Failed to process {file_name}: {str(e)}")
                    
        except Exception as e:
            logger.error(f"Error in completion handler: {e}")
            logger.error(traceback.format_exc())
            
        finally:
            # ALWAYS mark as complete in queue, even if errors occurred
            if file_key:
                try:
                    self.file_queue.mark_complete(file_key)
                except Exception as e:
                    logger.error(f"Error marking file complete: {e}")
    
    def stop(self):
        """Stop the processor"""
        self.running = False
        self.stop_event.set()


class FileHandler:
    """Orchestrator that manages scanner and processor - RUNS FOREVER"""
    
    def __init__(self, max_workers=4, scan_interval=5):
        self.file_queue = FileQueue()
        self.scanner = S3FileScanner(self.file_queue, scan_interval)
        self.processor = FileProcessor(self.file_queue, max_workers)
        self.scanner_thread = None
    
    def start(self):
        """Start both scanner and processor - RUNS FOREVER"""
        logger.info("=" * 60)
        logger.info("Starting FileHandler (will run forever)")
        logger.info("=" * 60)
        
        # Start scanner in background thread
        self.scanner_thread = Thread(target=self.scanner.start, daemon=True, name="S3Scanner")
        self.scanner_thread.start()
        
        # Start processor in main thread (blocking) - this runs forever
        try:
            self.processor.start()
        except Exception as e:
            logger.error(f"Critical error in processor: {e}")
            logger.error(traceback.format_exc())
            # Even if processor crashes, try to restart it
            logger.info("Attempting to restart processor...")
            time.sleep(5)
            self.start()  # Recursive restart
    
    def stop(self):
        """Stop both scanner and processor"""
        logger.info("Shutting down FileHandler...")
        self.scanner.stop()
        self.processor.stop()
        
        if self.scanner_thread:
            self.scanner_thread.join(timeout=10)


# Usage - This will run forever until manually stopped
if __name__ == "__main__":
    file_handler = FileHandler(max_workers=4, scan_interval=5)
    
    try:
        logger.info("Starting file handler (Press Ctrl+C to stop)...")
        file_handler.start()  # Blocks forever
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
        file_handler.stop()
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}")
        logger.error(traceback.format_exc())
        # Auto-restart on crash
        logger.info("Restarting in 10 seconds...")
        time.sleep(10)
        file_handler = FileHandler(max_workers=4, scan_interval=5)
        file_handler.start()