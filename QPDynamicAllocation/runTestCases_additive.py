"""
QPDynamicAllocation/runTestCases_additive.py
=============================================================================
Test Runner for Additive Model & NextGen AU Optimization in QPDynamicAllocation.
Connects to local MongoDB, initializes tables from QPDynamicAllocation/testcases/input/,
executes the optimization pipeline, and writes output files to QPDynamicAllocation/testcases/output/.
"""

import os
import sys
import logging
import pandas as pd
from pymongo import MongoClient

# Setup paths
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(CURRENT_DIR)
ADDITIVE_DIR = os.path.join(REPO_ROOT, 'rm-additive-process')
CONFIG_DIR = os.path.join(REPO_ROOT, 'rm-allocation-configurations')

if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)
if ADDITIVE_DIR not in sys.path:
    sys.path.insert(0, ADDITIVE_DIR)
if CONFIG_DIR not in sys.path:
    sys.path.insert(0, CONFIG_DIR)

from load_tests_db_data import Schema
from additive_model import AdditivePickupModelProcessor

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('RunTestCasesAdditive')


def runTestCasesAdditive():
    inputs_dir = os.path.join(CURRENT_DIR, 'testcasesadditive', 'input')
    output_dir = os.path.join(CURRENT_DIR, 'testcasesadditive', 'output')
    os.makedirs(output_dir, exist_ok=True)

    print("=" * 80)
    print("STARTING TEST RUN: Additive Pickup & AU Optimization")
    print(f"Inputs : {inputs_dir}")
    print(f"Outputs: {output_dir}")
    print("=" * 80)

    # 1. Connect to local MongoDB & initialize tables
    client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=3000)
    db = client['qp_dw_rmalloc']
    master_db = client['qp_rm_master']

    schema = Schema(db, master_db)
    if hasattr(schema, 'initializeTables'):
        schema.initializeTables()
    else:
        schema.initializTables()

    # 2. Initialize processor in TEST mode
    params = {'mongo': db, 'mongo_master': master_db, 'rdEsbConn': None, 's3': None}
    overrides = {'mode': 'TEST', 'sectors': ['BOMDEL', 'DELBOM'], 'output_dir': output_dir}
    processor = AdditivePickupModelProcessor(logger, params, overrides, None)
    processor.mongo_helper.fetch_tax_master_data()

    # 3. Load test flight and fare bucket datasets from CSV
    future_df = pd.read_csv(os.path.join(inputs_dir, 'test_future_flights.csv'))
    future_df['DepartureDate_future'] = pd.to_datetime(future_df['DepartureDate_future'])

    bucket_df = pd.read_csv(os.path.join(inputs_dir, 'test_fare_buckets.csv'))
    bucket_df['DepartureDate_future'] = pd.to_datetime(bucket_df['DepartureDate_future'])

    booked_df = pd.read_csv(os.path.join(inputs_dir, 'test_booked_data.csv'))
    booked_df['DepartureDate'] = pd.to_datetime(booked_df['DepartureDate'])

    flight_level_df = future_df.copy()
    farebasis_df = bucket_df[['Sector', 'FareBasis']].drop_duplicates()

    # 4. Execute Step 15: AU Optimization & Competition Response
    optimized_df, flight_metrics_df = processor._run_inventory_optimization(
        bucket_df, future_df, farebasis_df, booked_df,
        count_col='WTP_pax_count', std_col='predicted_pax_std'
    )

    if flight_metrics_df is not None and not flight_metrics_df.empty:
        merge_keys = ['Sector', 'DepartureDate_future', 'FlightNumber_future']
        flight_level_df = flight_level_df.merge(flight_metrics_df, on=merge_keys, how='left')

    # 5. Save results to testcases/output/
    processor._save_results(
        bucket_df, flight_level_df, optimized_df,
        bucket_df, flight_level_df, sectors=['BOMDEL', 'DELBOM']
    )

    print("\n" + "=" * 80)
    print(f"SUCCESS: Outputs generated in {output_dir}")
    print("=" * 80)
    for f in sorted(os.listdir(output_dir)):
        if f.startswith(('inventory_', 'flight_level_', 'fare_bucket_')):
            size_kb = os.path.getsize(os.path.join(output_dir, f)) / 1024.0
            print(f"  • {f} ({size_kb:.2f} KB)")
    print("=" * 80)


if __name__ == '__main__':
    runTestCasesAdditive()
