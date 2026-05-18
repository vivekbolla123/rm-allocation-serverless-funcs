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
mongo_master_db = mongoclient[MONGO_MASTER_DB]


def _load_master_collections():
    """
    Load the list of collection names that exist in the master db (qp_rm_master).
    Called once at module import. Whatever collections exist in master db are the master collections.
    Falls back to a hardcoded default if the query fails.
    """
    fallback = {
        "config_station_master", "active_sectors",
        "market_fares_ag", "market_fares_rg", "tax_mast_dom", "tax_mast_intl",
        "active_flight_connections", "active_flight_legs", "cleanup_config",
    }
    try:
        collections = mongo_master_db.list_collection_names()
        if collections:
            return set(collections)
    except Exception as e:
        print(f"[WARN] Failed to list collections from master db, using fallback: {e}")
    return fallback


# Loaded once at module import (app startup) — all downstream imports reuse this set.
MASTER_DB_COLLECTIONS = _load_master_collections()


def get_db_for_collection(collection_name):
    """Return the appropriate db context manager for a given collection name."""
    if collection_name in MASTER_DB_COLLECTIONS:
        return get_mongo_master_db
    return get_mongo_db

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

@contextmanager
def get_mongo_master_db():
    """Context manager for MongoDB master db operations (common collections)"""
    try:
        yield mongo_master_db
    except Exception as e:
        raise e