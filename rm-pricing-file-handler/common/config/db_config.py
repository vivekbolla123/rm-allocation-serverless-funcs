from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from common.config.config_settings import RM_DB_APPUSER_URL, RM_DB_APPUSER_PASSWORD, RM_DB_APPUSER_USERNAME, RM_DB_SCHEMA_NAME

RM_DB_CONN_URL = "mysql+pymysql://{user}:{pw}@{host}/{db}".format(
    user=RM_DB_APPUSER_USERNAME, pw=RM_DB_APPUSER_PASSWORD, host=RM_DB_APPUSER_URL, db=RM_DB_SCHEMA_NAME
)

# Create engines with auto-management
write_engine = create_engine(
    RM_DB_CONN_URL,
    pool_pre_ping=True,      # Auto-check if connection is alive
    pool_recycle=3600,       # Auto-recycle stale connections
)

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