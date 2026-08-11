"""
additive_dw_loader.py  —  S3 -> DW loader for the Additive Pickup Model output
=============================================================================

ONE RUN PER DAY. Each daily snapshot only ADDS its rows; prior snapshots are
never replaced or deleted (append-only). Idempotent — a snapshot_date already
loaded for a table is skipped, so a re-run cannot duplicate rows. (force
re-loads a single day: deletes only that snapshot_date's rows, then re-appends.)
Every row also carries the run_id column (1:1 with snapshot_date) for traceability.

S3 layout written by the model (_write_partitioned_s3):
    {output_prefix}/{dataset}/snapshotdate=YYYY-MM-DD/sector=XXX/data.parquet
SnapshotDate + Sector are the partition keys (dropped from the parquet body) and
are reconstructed from the object key here. run_id stays in the body.

Entry points:
    lambda_handler(event, context)   — {"snapshot_date": "...", "datasets": [...], "force": false}
    main()                           — local / Glue run
"""

import io
import re
import sys
import json
import logging
from urllib.parse import quote_plus

import boto3
import pandas as pd
from sqlalchemy import create_engine, text, inspect

import configsettings

logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3_BUCKET = configsettings.ADDITIVE_MODEL_BUCKET
OUTPUT_PREFIX = getattr(
    configsettings, 'ADDITIVE_MODEL_OUTPUT_PREFIX',
    f"{configsettings.ADDITIVE_MODEL_FILE_PATH}datasets",
).rstrip('/')
DW_SCHEMA = configsettings.DW_DB_WRITE_SCHEMA

DATASET_TABLE_MAP = {
    'flight_level_normal':    'additive_flight_level_normal',
    'fare_bucket_normal':     'additive_fare_bucket_normal',
    'flight_level_weighted':  'additive_flight_level_weighted',
    'fare_bucket_weighted':   'additive_fare_bucket_weighted',
    'inventory_optimization': 'additive_inventory_optimization',
}

# path segments are {column.lower()} — SnapshotDate -> 'snapshotdate', Sector -> 'sector'
_PARTITION_RE = re.compile(r'snapshotdate=([^/]+)/sector=([^/]+)/')
_LOAD_CHUNKSIZE = 5000


class AdditiveDWLoader:
    """Read the model's S3 datasets for a snapshot and append them into the DW."""

    def __init__(self, s3, engine, bucket, output_prefix, schema, logger,
                 dataset_table_map=None):
        self.s3 = s3
        self.engine = engine
        self.bucket = bucket
        self.output_prefix = output_prefix.rstrip('/')
        self.schema = schema
        self.logger = logger
        self.dataset_table_map = dataset_table_map or DATASET_TABLE_MAP

    def _list_keys(self, prefix):
        keys, token = [], None
        while True:
            kw = {'Bucket': self.bucket, 'Prefix': prefix}
            if token:
                kw['ContinuationToken'] = token
            resp = self.s3.list_objects_v2(**kw)
            for obj in resp.get('Contents', []):
                if obj['Key'].endswith('.parquet'):
                    keys.append(obj['Key'])
            if not resp.get('IsTruncated'):
                break
            token = resp.get('NextContinuationToken')
        return keys

    def discover_latest_snapshot(self, dataset):
        dates = set()
        for key in self._list_keys(f"{self.output_prefix}/{dataset}/"):
            m = _PARTITION_RE.search(key)
            if m:
                dates.add(m.group(1))
        return max(dates) if dates else None

    def _read_partition(self, key):
        m = _PARTITION_RE.search(key)
        if not m:
            self.logger.warning(f"  key without snapshotdate/sector partition, skipping: {key}")
            return None
        snapshot_date, sector = m.group(1), m.group(2)
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        if df.empty:
            return None
        if 'SnapshotDate' not in df.columns:
            df['SnapshotDate'] = snapshot_date
        if 'Sector' not in df.columns:
            df['Sector'] = sector
        return df

    def read_dataset(self, dataset, snapshot_date):
        prefix = f"{self.output_prefix}/{dataset}/snapshotdate={snapshot_date}/"
        keys = self._list_keys(prefix)
        if not keys:
            return pd.DataFrame()
        frames = [self._read_partition(k) for k in keys]
        frames = [f for f in frames if f is not None and not f.empty]
        if not frames:
            return pd.DataFrame()
        out = pd.concat(frames, ignore_index=True)
        out['SnapshotDate'] = pd.to_datetime(out['SnapshotDate']).dt.date
        return out

    def _table_exists(self, table):
        return inspect(self.engine).has_table(table, schema=self.schema)

    def _snapshot_loaded(self, table, snapshot_date):
        if not self._table_exists(table):
            return False
        sql = text(f"SELECT 1 FROM `{self.schema}`.`{table}` "
                   f"WHERE `SnapshotDate` = :d LIMIT 1")
        with self.engine.connect() as conn:
            return conn.execute(sql, {'d': str(snapshot_date)}).first() is not None

    def _delete_snapshot(self, table, snapshot_date):
        if not self._table_exists(table):
            return 0
        sql = text(f"DELETE FROM `{self.schema}`.`{table}` WHERE `SnapshotDate` = :d")
        with self.engine.begin() as conn:
            return conn.execute(sql, {'d': str(snapshot_date)}).rowcount or 0

    def _append(self, df, table):
        df.to_sql(table, self.engine, if_exists='append', index=False,
                  chunksize=_LOAD_CHUNKSIZE, method='multi')

    def load_dataset(self, dataset, snapshot_date, force=False):
        table = self.dataset_table_map.get(dataset)
        if table is None:
            self.logger.warning(f"[{dataset}] no table mapping — skipped.")
            return {'dataset': dataset, 'status': 'no_mapping', 'rows': 0}

        if self._snapshot_loaded(table, snapshot_date):
            if not force:
                self.logger.info(f"[{dataset}] snapshot {snapshot_date} already in "
                                 f"`{table}` — skipped (append-only, no duplicates).")
                return {'dataset': dataset, 'status': 'skipped_exists', 'rows': 0}
            deleted = self._delete_snapshot(table, snapshot_date)
            self.logger.info(f"[{dataset}] force — deleted {deleted} existing row(s) "
                             f"for snapshot {snapshot_date} before re-append.")

        df = self.read_dataset(dataset, snapshot_date)
        if df.empty:
            self.logger.info(f"[{dataset}] no S3 data for snapshot {snapshot_date}.")
            return {'dataset': dataset, 'status': 'no_data', 'rows': 0}

        self._append(df, table)
        self.logger.info(f"[{dataset}] appended {len(df)} row(s) -> `{table}` "
                         f"(snapshot {snapshot_date}).")
        return {'dataset': dataset, 'status': 'appended', 'rows': int(len(df)), 'table': table}

    def run(self, snapshot_date=None, datasets=None, force=False):
        datasets = datasets or list(self.dataset_table_map.keys())
        if not snapshot_date:
            found = [self.discover_latest_snapshot(d) for d in datasets]
            found = [d for d in found if d]
            if not found:
                self.logger.error("No snapshot partitions found in S3 — nothing to load.")
                return {'snapshot_date': None, 'results': []}
            snapshot_date = max(found)
            self.logger.info(f"Auto-detected latest snapshot_date = {snapshot_date}")

        self.logger.info(f"Loading snapshot {snapshot_date} — datasets: {datasets} "
                         f"(force={force})")
        results = [self.load_dataset(d, snapshot_date, force=force) for d in datasets]
        total = sum(r['rows'] for r in results)
        self.logger.info(f"DONE — snapshot {snapshot_date}: {total} row(s) appended across "
                         f"{sum(1 for r in results if r['status'] == 'appended')} dataset(s).")
        return {'snapshot_date': str(snapshot_date), 'total_rows': total, 'results': results}


def _build_engine():
    url = (f"mysql+pymysql://{configsettings.DW_DB_USER}:"
           f"{quote_plus(configsettings.DW_DB_PASSWORD)}@"
           f"{configsettings.DW_DB_HOST}/{configsettings.DW_DB_WRITE_SCHEMA}")
    return create_engine(url, pool_pre_ping=True)


def _make_loader():
    return AdditiveDWLoader(
        s3=boto3.client('s3'), engine=_build_engine(), bucket=S3_BUCKET,
        output_prefix=OUTPUT_PREFIX, schema=DW_SCHEMA, logger=logger)


def lambda_handler(event, context):
    if isinstance(event, dict) and "Records" in event:
        data = json.loads(event["Records"][0]["body"])
    elif isinstance(event, dict) and "body" in event:
        data = json.loads(event["body"])
    else:
        data = event or {}
    data = data.get("overrides", data)

    snapshot_date = data.get("snapshot_date")
    datasets = data.get("datasets")
    force = str(data.get("force", "false")).lower() == "true"
    try:
        result = _make_loader().run(snapshot_date=snapshot_date, datasets=datasets, force=force)
        return {"statusCode": 200, "body": json.dumps(result, default=str)}
    except Exception as e:
        logger.exception("DW load failed")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)-8s - %(message)s")
    args = sys.argv[1:]
    force = '--force' in args
    args = [a for a in args if a != '--force']
    snapshot_date = args[0] if args else None
    result = _make_loader().run(snapshot_date=snapshot_date, force=force)
    logger.info(f"RESULT: {json.dumps(result, default=str)}")


if __name__ == "__main__":
    main()