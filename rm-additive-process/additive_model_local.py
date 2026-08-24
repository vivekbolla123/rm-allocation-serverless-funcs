"""
run_additive_local.py  —  ADHOC local driver for the Additive Pickup Model
=============================================================================

Runs the simulation ENTIRELY on your machine (no Lambda, no Glue compute — the
only AWS you pay for is the one-off Glue historical backfill you run first, plus
trivial S3 GET/PUT for the cache + outputs and the ESB/Mongo reads the model
needs anyway).

Prereqs (do this ONCE, on AWS, before running this script):
    Seed the FULL historical union for every sector into the adhoc S3 root with a
    single Glue run (fetch-only, no dispatch):

        aws glue start-job-run --job-name <rm-additive-fetch> --arguments '{
            "--SECTORS":"<ALL sectors, comma-separated>",
            "--RUN_ID":"adhoc-sim",
            "--FORCE_BACKFILL":"true",
            "--REFRESH_SPEC":"none",
            "--SECTORS_EXPECTED":"0",
            "--MODE":"PROD",
            "--PROCESS_QUEUE_URL":"<unused>",
            "--ENV":"uat",
            "--HIST_START":"2025-06-01",
            "--HIST_END":"2026-08-14",
            "--SKIP_DISPATCH":"true"
        }'

    HIST_START = earliest snapshot's hist floor (01-06-2026 -> 2025-06-01);
    HIST_END   = latest snapshot's yesterday   (15-08-2026 -> 2026-08-14).
    That single window is the union of every snapshot's historical need. The
    process clips it per-snapshot at read time, so no future data leaks.

Then run this locally:
    python run_additive_local.py --env uat --sectors all --workers 8

What it does:
    * Sequential across snapshots (cache/tagging must advance in date order).
    * Parallel across sectors WITHIN a snapshot (independent, sector-partitioned
      writes — safe). --workers 1 = plain sequential.
    * Each run: reads the Glue-seeded hist cache from S3 (clipped to < snapshot,
      so zero further historical fetches), fetches forward bookings as-of the
      snapshot from ESB, forecasts, writes output to
      s3://<bucket>/additive_model_test/datasets/snapshotdate=<date>/sector=<s>/.
"""

import argparse
import logging
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta
from uuid import uuid4

import boto3
import pandas as pd
from sqlalchemy import create_engine
from pymongo import MongoClient

import configsettings
from additive_model import AdditivePickupModelProcessor

# Default snapshot sequence (used only if neither --start/--end nor --snapshots given).
DEFAULT_SNAPSHOTS = [
    '01-06-2026', '15-06-2026', '01-07-2026',
    '15-07-2026', '01-08-2026', '15-08-2026',
]


def _parse_date(s):
    return pd.to_datetime(s, dayfirst=('-' in s and len(s.split('-')[0]) <= 2)).date()


def daily_range(start, end):
    """Inclusive list of 'DD-MM-YYYY' strings from start to end."""
    d0, d1 = _parse_date(start), _parse_date(end)
    out, d = [], d0
    while d <= d1:
        out.append(d.strftime('%d-%m-%Y'))
        d += timedelta(days=1)
    return out

# -----------------------------------------------------------------------------
# LOGGING
# -----------------------------------------------------------------------------

def make_logger(name='additive-local'):
    lg = logging.getLogger(name)
    if not lg.handlers:
        lg.setLevel(logging.INFO)
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(logging.Formatter('%(asctime)s %(levelname)-7s %(message)s'))
        lg.addHandler(h)
    return lg


# -----------------------------------------------------------------------------
# CONNECTIONS  (edit this block if your local creds path differs)
# -----------------------------------------------------------------------------

def _ssm(name, region='ap-south-1'):
    return boto3.client('ssm', region_name=region).get_parameter(
        Name=name, WithDecryption=True)['Parameter']['Value']


def build_esb_engine(env):
    """ESB (Navitaire) MySQL engine — mirrors the Glue job's connection."""
    if env == 'prod':
        host = 'p-esb.cz6mmsw6mnag.ap-south-1.rds.amazonaws.com'
        user = _ssm('/copilot/rm/prod/secrets/Qp-Forecast-DB-User')
        pwd = _ssm('/copilot/rm/prod/secrets/Qp-Forecast-DB-Pass')
    else:
        host = '172.24.87.43'
        user = _ssm('/copilot/rm/uat/secrets/Qp-Forecast-DB-User')
        pwd = _ssm('/copilot/rm/uat/secrets/Qp-Forecast-DB-Pass')
    schema = configsettings.ESB_READ_SCHEMA
    conn = f"mysql+pymysql://{user}:{pwd}@{host}/{schema}"
    # small pool — one engine per worker process
    return create_engine(conn, pool_size=2, max_overflow=4, pool_pre_ping=True)


def build_params(env):
    """Assemble the `params` dict the processor expects."""
    mongo_client = MongoClient(
        host=configsettings.MONGO_URL, port=27017,
        username=configsettings.MONGO_USERNAME,
        password=configsettings.MONGO_PASSWORD,
        authSource=configsettings.MONGO_AUTH_SOURCE)
    return {
        'rdEsbConn': build_esb_engine(env),
        'mongo': mongo_client['qp_dw_rmalloc'],
        'mongo_master': mongo_client[configsettings.MONGO_MASTER_DB],
        's3': boto3.client('s3', region_name='ap-south-1'),
    }


def load_all_sectors(params):
    docs = params['mongo_master'].active_sectors.find({}, {'_id': 0, 'sector': 1})
    return sorted({d['sector'] for d in docs if d.get('sector')})


# -----------------------------------------------------------------------------
# WORKER  (one sector, one snapshot)
# -----------------------------------------------------------------------------

# Per-process globals so a pool worker builds connections once, not per task.
_WORKER = {}


def _init_worker(env):
    _WORKER['params'] = build_params(env)
    _WORKER['logger'] = make_logger(f'worker-{env}')


def _run_one(snapshot, sector, run_id, fares_dir, mode):
    """Run a single (snapshot, sector). Returns (sector, ok, elapsed, err)."""
    params = _WORKER['params']
    logger = _WORKER['logger']
    t0 = time.time()
    try:
        overrides = {
            'simulate_snapshot': snapshot,
            'sectors': [sector],
            'mode': mode,
            'run_id': run_id,
        }
        if fares_dir:
            overrides['fares_dir'] = fares_dir
        proc = AdditivePickupModelProcessor(logger, params, overrides=overrides, context=None)
        proc.run()
        return (sector, True, time.time() - t0, None)
    except Exception as e:
        return (sector, False, time.time() - t0, repr(e))


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description='Local additive-model simulation driver.')
    ap.add_argument('--env', default='uat', choices=['uat', 'prod'])
    ap.add_argument('--sectors', default='all',
                    help="'all' or comma-separated list, e.g. BOMDEL,DELBOM")
    ap.add_argument('--start', default=None, help='daily range start, DD-MM-YYYY')
    ap.add_argument('--end', default=None, help='daily range end (inclusive), DD-MM-YYYY')
    ap.add_argument('--snapshots', default=None,
                    help='explicit comma-separated snapshot list (overrides --start/--end)')
    ap.add_argument('--fares-dir', default=None,
                    help='local dir with <YYYYMM01>_qpfares.csv monthly files')
    ap.add_argument('--mode', default='TEST', choices=['TEST', 'PROD'],
                    help='TEST = local cache+output; PROD = S3 cache+output')
    ap.add_argument('--workers', type=int, default=1,
                    help='parallel sectors within each snapshot (1 = sequential)')
    ap.add_argument('--run-id', default=None)
    args = ap.parse_args()

    logger = make_logger()
    run_id = args.run_id or f'adhoc-{uuid4().hex[:8]}'

    # resolve snapshot list: explicit --snapshots > --start/--end range > default
    if args.snapshots:
        snapshots = [s.strip() for s in args.snapshots.split(',') if s.strip()]
    elif args.start and args.end:
        snapshots = daily_range(args.start, args.end)
    else:
        snapshots = DEFAULT_SNAPSHOTS

    # resolve sector list (needs a Mongo connection up front)
    bootstrap = build_params(args.env)
    if args.sectors.strip().lower() == 'all':
        sectors = load_all_sectors(bootstrap)
    else:
        sectors = [s.strip() for s in args.sectors.split(',') if s.strip()]

    logger.info('=' * 78)
    logger.info(f'LOCAL SIM  run_id={run_id}  env={args.env}  mode={args.mode}  '
                f'workers={args.workers}')
    logger.info(f'snapshots ({len(snapshots)}): {snapshots[0]} .. {snapshots[-1]}'
                if len(snapshots) > 3 else f'snapshots: {snapshots}')
    logger.info(f'fares: {"local " + args.fares_dir if args.fares_dir else "Mongo (qp_dw_rmalloc.fares)"}')
    logger.info(f'sectors   ({len(sectors)}): '
                f'{sectors if len(sectors) <= 12 else sectors[:12] + ["..."]}')
    logger.info('=' * 78)

    grand_t0 = time.time()
    totals = {'ok': 0, 'fail': 0}
    failures = []

    # SEQUENTIAL across snapshots (cache + tagging advance in date order);
    # PARALLEL across sectors within each snapshot.
    for si, snap in enumerate(snapshots, 1):
        logger.info(f'--- [{si}/{len(snapshots)}] snapshot {snap} '
                    f'({len(sectors)} sectors) ---')
        snap_t0 = time.time()
        s_ok = s_fail = 0

        if args.workers <= 1:
            _init_worker(args.env)   # build connections once for this process
            for sector in sectors:
                sec, ok, el, err = _run_one(snap, sector, run_id, args.fares_dir, args.mode)
                if ok:
                    s_ok += 1
                else:
                    s_fail += 1
                    failures.append((snap, sec, err))
                    logger.error(f'  FAIL {sec}: {err}')
                logger.info(f'  {sec:8s} {"ok" if ok else "FAIL"}  {el:5.1f}s')
        else:
            with ProcessPoolExecutor(max_workers=args.workers,
                                     initializer=_init_worker,
                                     initargs=(args.env,)) as ex:
                futs = {ex.submit(_run_one, snap, sec, run_id, args.fares_dir, args.mode): sec
                        for sec in sectors}
                for fut in as_completed(futs):
                    sec, ok, el, err = fut.result()
                    if ok:
                        s_ok += 1
                    else:
                        s_fail += 1
                        failures.append((snap, sec, err))
                        logger.error(f'  FAIL {sec}: {err}')
                    logger.info(f'  {sec:8s} {"ok" if ok else "FAIL"}  {el:5.1f}s')

        totals['ok'] += s_ok
        totals['fail'] += s_fail
        logger.info(f'--- snapshot {snap} done: {s_ok} ok, {s_fail} fail, '
                    f'{time.time() - snap_t0:.1f}s ---')

    logger.info('=' * 78)
    logger.info(f'ALL DONE  {totals["ok"]} ok, {totals["fail"]} fail  '
                f'in {(time.time() - grand_t0) / 60:.1f} min')
    if failures:
        logger.info('Failures:')
        for snap, sec, err in failures:
            logger.info(f'  {snap} {sec}: {err}')
    logger.info(f'Output -> s3://{configsettings.ADDITIVE_MODEL_BUCKET}/'
                f'additive_model_test/datasets/snapshotdate=<date>/sector=<sector>/')
    logger.info('=' * 78)
    return 1 if failures else 0


if __name__ == '__main__':
    sys.exit(main())