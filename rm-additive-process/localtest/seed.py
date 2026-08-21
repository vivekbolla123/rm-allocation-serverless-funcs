"""
localtest/seed.py
=============================================================================
Loads the mock CSVs into the two local stores.

  * MySQL  — creates the schema and the three QP_DL_* tables, then replaces
             their contents. The production SQL in AdditiveEsbHelper then runs
             verbatim against them, which is the point: the queries under test
             are the real ones, only the data is local.
  * Mongo  — drops and rebuilds one collection per CSV.

Seeding is destructive for exactly the collections/tables listed in
localtest/schema.py and nothing else.
"""

import os

import pandas as pd
from sqlalchemy import text

from localtest.schema import DDL, MYSQL_DATASETS, MONGO_DATASETS


CHUNK_SIZE = 5000


# =============================================================================
# CSV reading
# =============================================================================

def read_input_csv(inputs_dir, filename, date_cols=(), datetime_cols=()):
    path = os.path.join(inputs_dir, filename)
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"missing input CSV: {path}\nRun `--generate` first, or drop your own "
            f"file there.")
    df = pd.read_csv(path)
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
    for col in datetime_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    return df


# =============================================================================
# MySQL
# =============================================================================

def seed_mysql(cfg, logger, inputs_dir=None):
    inputs_dir = inputs_dir or cfg.inputs_dir

    server = cfg.mysql_engine(with_schema=False)
    with server.connect() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{cfg.mysql_schema}` "
                          f"DEFAULT CHARACTER SET utf8mb4"))
    server.dispose()
    logger.info(f"[seed:mysql] schema `{cfg.mysql_schema}` ready")

    engine = cfg.mysql_engine()
    counts = {}
    try:
        with engine.begin() as conn:
            for table, ddl in DDL.items():
                conn.execute(text(ddl))
                conn.execute(text(f"TRUNCATE TABLE `{table}`"))
            logger.info(f"[seed:mysql] {len(DDL)} table(s) created and truncated")

        for filename, (table, date_cols, dt_cols) in MYSQL_DATASETS.items():
            df = read_input_csv(inputs_dir, filename, date_cols, dt_cols)
            if df.empty:
                logger.warning(f"[seed:mysql] {filename} is empty — {table} left empty")
                counts[table] = 0
                continue
            df.to_sql(table, engine, if_exists='append', index=False,
                      chunksize=CHUNK_SIZE, method='multi')
            counts[table] = len(df)
            logger.info(f"[seed:mysql] {filename:<24} -> {table:<22} {len(df):>8} rows")
    finally:
        engine.dispose()

    return counts


# =============================================================================
# MongoDB
# =============================================================================

def seed_mongo(cfg, logger, inputs_dir=None):
    inputs_dir = inputs_dir or cfg.inputs_dir

    client = cfg.mongo_client()
    counts = {}
    try:
        targets = {'dw': client[cfg.mongo_db], 'master': client[cfg.mongo_master_db]}

        for filename, (which_db, collection, date_cols) in MONGO_DATASETS.items():
            df = read_input_csv(inputs_dir, filename, date_cols)
            db = targets[which_db]
            db[collection].drop()
            if df.empty:
                logger.warning(f"[seed:mongo] {filename} is empty — "
                               f"{db.name}.{collection} left empty")
                counts[f'{db.name}.{collection}'] = 0
                continue
            docs = df.to_dict('records')
            db[collection].insert_many(docs)
            counts[f'{db.name}.{collection}'] = len(docs)
            logger.info(f"[seed:mongo] {filename:<24} -> "
                        f"{db.name}.{collection:<34} {len(docs):>8} docs")

        # Indexes the model's own reads benefit from.
        client[cfg.mongo_db]['fares'].create_index([('SECTOR', 1), ('RBD', 1)])
        client[cfg.mongo_master_db]['tax_mast_dom'].create_index([('Code', 1)])
        client[cfg.mongo_master_db]['tax_mast_intl'].create_index([('Sectors', 1)])
    finally:
        client.close()

    return counts


def seed_all(cfg, logger, inputs_dir=None, mysql=True, mongo=True):
    result = {}
    if mongo:
        result['mongo'] = seed_mongo(cfg, logger, inputs_dir)
    if mysql:
        result['mysql'] = seed_mysql(cfg, logger, inputs_dir)
    return result
