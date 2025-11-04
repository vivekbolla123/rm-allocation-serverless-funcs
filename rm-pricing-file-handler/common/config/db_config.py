from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from pymongo import MongoClient
from common.config.config_settings import *

RM_DB_CONN_URL = "mysql+pymysql://{user}:{pw}@{host}/{db}".format(
    user=RM_DB_APPUSER_USERNAME, pw=RM_DB_APPUSER_PASSWORD, host=RM_DB_APPUSER_URL, db=RM_DB_SCHEMA_NAME
)

# Create engines with auto-management
write_engine = create_engine(
    RM_DB_CONN_URL,
    pool_pre_ping=True,      # Auto-check if connection is alive
    pool_recycle=3600,       # Auto-recycle stale connections
)

# MongoDB connection
mongoclient = MongoClient(
    host=MONGO_URL,
    port=27017,
    username=MONGO_USERNAME,
    password=MONGO_PASSWORD,
    authSource=MONGO_AUTHSOURCE,
    connectTimeoutMS=30000,
    socketTimeoutMS=30000,
    serverSelectionTimeoutMS=30000
)
mongodb = mongoclient[MONGO_DB]

WriteSession = sessionmaker(bind=write_engine)

@contextmanager
def write_db():
    session = WriteSession()
    try:
        yield session
        session.commit()  # Auto-commit on success
    except:
        session.rollback()  # Auto-rollback on error
        raise
    finally:
        session.close()  # Auto-close and return to pool

@contextmanager
def get_mongo_db():
    """Context manager for MongoDB operations"""
    try:
        yield mongodb
    except Exception as e:
        raise e