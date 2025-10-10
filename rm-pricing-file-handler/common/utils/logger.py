import logging
import sys

def create_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(levelname)-8s - %(message)s  ")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    # Suppress APScheduler INFO logs
    logging.getLogger('apscheduler').setLevel(logging.ERROR)
    
    return logger

logger = create_logger()