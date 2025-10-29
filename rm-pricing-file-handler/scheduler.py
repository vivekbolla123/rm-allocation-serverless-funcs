from concurrent.futures import ThreadPoolExecutor
from threading import Lock, Event, Thread
from queue import Queue
import time
import traceback

from common.utils.s3_helper import S3Helper
from common.utils.logger import logger
from common.constants.constants import *
from common.config.config_settings import S3_PREFIX
from main import FileUploadWorker

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
            
            for file in sorted_files:
                file_key = file['Key']
                obj_id = file_key[len(S3_PREFIX) : len(file_key) - 4]
                
                if self.file_queue.add_file(file_key):
                    logger.info(f"Queued: {obj_id}")
                
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
            logger.error(f"CRITICAL: Unhandled exception processing {file_name}: {e}")
            logger.error(traceback.format_exc())
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
            with self.futures_lock:
                file_key = self.active_futures.pop(future, None)
            
            if file_key is None:
                logger.error("Could not find file_key for completed future")
                return
            
        except Exception as e:
            logger.error(f"Error in completion handler: {e}")
            logger.error(traceback.format_exc())
            
        finally:
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
        logger.info("---Starting FileHandler---")
        
        # Start scanner in background thread
        self.scanner_thread = Thread(target=self.scanner.start, daemon=True, name="S3Scanner")
        self.scanner_thread.start()
        
        # Start scanner in background thread if not already started
        if not self.scanner_thread or not self.scanner_thread.is_alive():
            self.scanner_thread = Thread(target=self.scanner.start, daemon=True, name="S3Scanner")
            self.scanner_thread.start()

        # Start processor with automatic restart on failure
        while True:
            try:
                self.processor_thread = Thread(target=self.processor.start, daemon=True, name="FileProcessor")
                self.processor_thread.start()

                # Keep main thread alive until stop requested
                while not self.processor.stop_event.is_set() and not self.scanner.stop_event.is_set():
                    time.sleep(1)

                # Exit loop if stop requested
                if self.processor.stop_event.is_set() or self.scanner.stop_event.is_set():
                    logger.info("Stop event set, exiting FileHandler.")
                    break

            except Exception as e:
                logger.error(f"Critical error in processor: {e}")
                logger.error(traceback.format_exc())
                logger.info("Restarting processor in 5 seconds...")
                time.sleep(5)
    
    def stop(self):
        """Stop both scanner and processor"""
        logger.info("Shutting down FileHandler...")
        self.scanner.stop()
        self.processor.stop()
        
        if hasattr(self, "scanner_thread"):
            self.scanner_thread.join(timeout=10)
        if hasattr(self, "processor_thread"):
            self.processor_thread.join(timeout=10)
        logger.info("All components stopped.")


# Usage - This will run forever until manually stopped
if __name__ == "__main__":
    file_handler = FileHandler(max_workers=4, scan_interval=5)
    
    try:
        file_handler.start()  
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