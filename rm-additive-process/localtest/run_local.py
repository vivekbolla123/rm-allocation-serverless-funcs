"""
localtest/run_local.py
=============================================================================
Entry point for the local additive-model flow.

    # one-shot: generate mock CSVs, seed local MySQL + Mongo, run the model
    python -m localtest.run_local --generate --seed --run

    # re-run against already-seeded stores
    python -m localtest.run_local --run --sectors BOMDEL

    # no local MySQL available: serve the ESB reads from the CSVs
    python -m localtest.run_local --generate --seed --run --esb csv

Outputs land in localtest/outputs/ as CSV and in the local Mongo database as
add_model_* collections. Those two folders — inputs/ and outputs/ — are the only
files the flow writes: there are no intermediate artefacts and no parquet cache.
"""

import argparse
import logging
import sys
import time
from datetime import date
from uuid import uuid4


def _configure_logging(level):
    logging.basicConfig(
        level=level,
        format='%(asctime)s %(levelname)-7s %(message)s',
        datefmt='%H:%M:%S',
        stream=sys.stdout,
        force=True)
    return logging.getLogger('additive-local')


# =============================================================================
# Readiness
# =============================================================================

def check_stores(cfg, logger, need_mysql):
    """Fail fast with an actionable message rather than deep inside a query."""
    problems = []

    if not cfg.mongo_reachable():
        problems.append(
            f"MongoDB is not reachable at {cfg.mongo_host}:{cfg.mongo_port}.\n"
            f"    Start it (Windows: `net start MongoDB`) or point the flow elsewhere "
            f"with ADDITIVE_LOCAL_MONGO_HOST / _PORT.")

    if need_mysql and not cfg.mysql_reachable():
        problems.append(
            f"MySQL is not reachable at {cfg.mysql_host}:{cfg.mysql_port} as "
            f"'{cfg.mysql_user}'.\n"
            f"    Start your local MySQL, or set ADDITIVE_LOCAL_MYSQL_* to match it,\n"
            f"    or re-run with `--esb csv` to serve the ESB reads from the input CSVs.")

    if problems:
        for p in problems:
            logger.error(p)
        raise SystemExit(2)


def resolve_esb_backend(cfg, requested, logger):
    if requested == 'auto':
        backend = 'mysql' if cfg.mysql_reachable() else 'csv'
        logger.info(f"[local] ESB backend auto-resolved to '{backend}'")
        return backend
    return requested


# =============================================================================
# Run
# =============================================================================

def run_model(cfg, logger, sectors, esb_backend, args):
    from localtest.local_processor import LocalAdditiveProcessor

    client = cfg.mongo_client()
    engine = cfg.mysql_engine() if esb_backend == 'mysql' else None
    try:
        db = client[cfg.mongo_db]
        master_db = client[cfg.mongo_master_db]

        params = {
            'rdEsbConn': engine,
            'mongo': db,
            'mongo_master': master_db,
            's3': None,               # LocalAdditiveCache replaces every S3 call
        }
        overrides = {
            'sectors': sectors,       # empty -> read master_db.active_sectors
            'run_id': args.run_id or f'local-{uuid4().hex[:12]}',
            'analyst': args.analyst,
            'batch_index': 1,
            'batch_total': 1,
            'sectors_expected': len(sectors) if sectors else None,
            # There is no local parquet cache, so every run is a full refetch.
            'force_backfill': True,
        }

        processor = LocalAdditiveProcessor(
            logger, params, overrides, None, cfg,
            esb_backend=esb_backend,
            use_csv_fare_buckets=args.use_csv_fare_buckets,
            use_csv_tagging=args.use_csv_tagging,
            dump_derived=args.dump_derived,
            include_ndo_in_mongo=args.mongo_include_ndo,
            write_full_csv=args.full_csv)

        if args.reset_outputs:
            processor.cache.clear_outputs()

        started = time.perf_counter()
        result = processor.run()
        elapsed = round(time.perf_counter() - started, 1)

        return result, processor.output_summary(), elapsed
    finally:
        if engine is not None:
            engine.dispose()
        client.close()


def report(cfg, logger, result, written, elapsed):
    logger.info('=' * 78)
    logger.info(f"status            : {result.get('status')}")
    logger.info(f"run_id            : {result.get('run_id')}")
    logger.info(f"sectors succeeded : {result.get('sectors_succeeded')}")
    logger.info(f"sectors failed    : {result.get('sectors_failed')}")
    if result.get('failure_reason'):
        logger.info(f"failure reason    : {result['failure_reason']}")
    logger.info(f"elapsed           : {elapsed}s")

    logger.info('-' * 78)
    if written:
        for dataset, count in sorted(written.items()):
            logger.info(f"output {dataset:<26} {count:>8} docs")
    else:
        logger.warning('no output datasets were written')

    client = cfg.mongo_client()
    try:
        from localtest.schema import OUTPUT_COLLECTIONS
        db = client[cfg.mongo_db]
        logger.info('-' * 78)
        for collection in sorted(set(OUTPUT_COLLECTIONS.values())):
            logger.info(f"mongo  {cfg.mongo_db}.{collection:<38} "
                        f"{db[collection].count_documents({}):>8} docs")
    finally:
        client.close()

    logger.info(f"csv    {cfg.outputs_dir}")


# =============================================================================
# main
# =============================================================================

def build_parser():
    ap = argparse.ArgumentParser(
        prog='run_local',
        description='Local, CSV-driven test flow for the additive pickup model.')

    stage = ap.add_argument_group('stages (at least one required)')
    stage.add_argument('--generate', action='store_true',
                       help='(re)generate the mock input CSVs')
    stage.add_argument('--seed', action='store_true',
                       help='load the input CSVs into local MySQL + MongoDB')
    stage.add_argument('--run', action='store_true', help='run the model')

    data = ap.add_argument_group('mock data shape (used with --generate)')
    data.add_argument('--hist-days', type=int, default=400)
    data.add_argument('--future-days', type=int, default=60)
    data.add_argument('--load-factor', type=float, default=None,
                      help='target load factor (default 0.45); drives how much '
                           'pickup is left for the AU ladder to protect')
    data.add_argument('--capacity', type=int, default=None,
                      help='force one seat count on every flight (default: the '
                           'mixed 189 / 174 fleet)')
    data.add_argument('--seed-value', type=int, default=20260821,
                      help='RNG seed for the generator')
    data.add_argument('--anchor', default=None, help='YYYY-MM-DD; defaults to today')

    run = ap.add_argument_group('run options')
    run.add_argument('--sectors', nargs='*', default=None,
                     help='sectors to process; omit to read active_sectors from Mongo')
    run.add_argument('--esb', choices=['auto', 'mysql', 'csv'], default='auto',
                     help="where the ESB reads come from (default auto: mysql if "
                          "reachable, else csv)")
    run.add_argument('--analyst', default='local-test')
    run.add_argument('--run-id', default=None)
    run.add_argument('--reset-outputs', action='store_true',
                     help='drop the add_model_* output collections before running')
    run.add_argument('--mongo-include-ndo', action='store_true',
                     help='keep the 0..180 pickup-curve columns in the Mongo documents')
    run.add_argument('--full-csv', action='store_true',
                     help='also write <dataset>_<sector>_full.csv with the 0..180 '
                          'pickup-curve columns')
    run.add_argument('--use-csv-fare-buckets', action='store_true',
                     help=f'read step 1 from inputs/fare_buckets.csv instead of '
                          f'computing it')
    run.add_argument('--use-csv-tagging', action='store_true',
                     help='read step 4 from inputs/tagging.csv instead of computing it')
    run.add_argument('--dump-derived', action='store_true',
                     help='write the computed fare buckets and tagging back into '
                          'inputs/ so they can be edited and replayed')

    ap.add_argument('--verbose', action='store_true')
    return ap


def main(argv=None):
    from localtest.bootstrap import bootstrap, ensure_localtest_importable
    ensure_localtest_importable()

    args = build_parser().parse_args(argv)
    logger = _configure_logging(logging.DEBUG if args.verbose else logging.INFO)

    if not (args.generate or args.seed or args.run):
        build_parser().print_help()
        raise SystemExit('\nNothing to do: pass --generate, --seed and/or --run.')

    cfg = bootstrap()
    logger.info('additive pickup model — LOCAL flow')
    for line in cfg.describe().splitlines():
        logger.info('  ' + line)

    esb_backend = resolve_esb_backend(cfg, args.esb, logger) if (args.run or args.seed) \
        else 'csv'
    check_stores(cfg, logger, need_mysql=(args.seed or args.run) and esb_backend == 'mysql')

    # --- stage 1: generate -------------------------------------------------
    if args.generate:
        from localtest.generate_inputs import DEFAULT_LOAD_FACTOR, generate
        anchor = date.fromisoformat(args.anchor) if args.anchor else None
        generate(cfg.inputs_dir, hist_days=args.hist_days,
                 future_days=args.future_days,
                 load_factor=args.load_factor or DEFAULT_LOAD_FACTOR,
                 capacity=args.capacity,
                 seed=args.seed_value, anchor=anchor, logger=logger)

    # --- stage 2: seed -----------------------------------------------------
    if args.seed:
        from localtest.seed import seed_all
        seed_all(cfg, logger, mysql=(esb_backend == 'mysql'), mongo=True)
        if esb_backend == 'csv':
            logger.info('[seed] MySQL skipped — the csv ESB backend reads inputs/ directly')

    # --- stage 3: run ------------------------------------------------------
    if args.run:
        sectors = args.sectors or []
        result, written, elapsed = run_model(cfg, logger, sectors, esb_backend, args)
        report(cfg, logger, result, written, elapsed)

        if result.get('status') == 'FAILED':
            raise SystemExit(1)


if __name__ == '__main__':
    main()
