import os
import sys
import datetime
import pandas as pd
from pymongo import MongoClient

# Setup paths
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT_DIR = os.path.dirname(_BASE_DIR)
_CONFIG_DIR = os.path.join(_ROOT_DIR, "rm-allocation-configurations")
_PROCESS_DIR = os.path.join(_ROOT_DIR, "rm-allocation-process-inputs")

for p in [_BASE_DIR, _CONFIG_DIR, _PROCESS_DIR, _ROOT_DIR]:
    if p not in sys.path:
        sys.path.insert(0, p)

import Constants
from load_tests_db_data_nextgen import SchemaNextGen
from runAlloc import DynAlloc


def run_nextgen_test():
    print("=" * 70)
    print("Starting NextGen Optimization Test Runner")
    print("=" * 70)

    # Establish MongoDB connections
    client = MongoClient('mongodb://localhost:27017/')
    db = client['rm_database']
    master_db = client['qp_rm_master']

    # Set input and output directories to inputnexgen and outputnexgen
    candidates_in = [
        os.path.join(_BASE_DIR, "testcases", "inputnexgen"),
        os.path.join(_BASE_DIR, "testcases", "inputnextgen"),
    ]
    input_dir = next((c for c in candidates_in if os.path.exists(c)), candidates_in[0])
    candidates_out = [
        os.path.join(_BASE_DIR, "testcases", "outputnexgen", "generated_output"),
        os.path.join(_BASE_DIR, "testcases", "outputnextgen", "generated_output"),
    ]
    output_dir = next((c for c in candidates_out if os.path.exists(c)), candidates_out[0])
    os.makedirs(output_dir, exist_ok=True)

    print(f"Input directory:  {input_dir}")
    print(f"Output directory: {output_dir}")

    # Initialize tables using SchemaNextGen
    print(f"\n[1/3] Initializing MongoDB collections from {os.path.basename(input_dir)}...")
    schema = SchemaNextGen(db, master_db, input_dir=input_dir)
    schema.initializTables()

    inputData = {
        "debugMode": False,
        "isUpdateNavitaire": False,
        "currDate": datetime.date(2023, 8, 1),
        "currDateTime": datetime.datetime(2023, 8, 1, 12, 12, 12),
        "MARKET_LIST_QUEUE_URL": None,
        "RBD_QUEUE_URL": None,
        "NAV_DB_NAME": None,
        "NAV_DB_DW_NAME": None,
        "NAV_DB_WB_NAME": None,
        "NAV_DB_REF_NAME": None,
        "NAV_BASE_API_URL": None,
        "NAV_OCP_SUBSCRIPTION_KEY": None,
        "NAV_USERNAME": None,
        "NAV_PWD": None,
        "NAV_DOMAIN": None,
        Constants.CONST_INPUT_SOURCE: Constants.TESTING_INPUT,
        Constants.CONST_LOGS: Constants.TESTING_LOGS,
        Constants.CONST_OUTPUT: Constants.TESTING_NEXTGEN_OUTPUT,
        "API_KEY": None,
        "API_URL": None,
        "latestCode": False,
        "currentTime": True,
        "output_dir": output_dir
    }

    currParams = {
        "runId": "TestNextGen12345",
        "runSingleMarket": False,
        "NavitaireConnection": None,
        "dbconn": None,
        "wrconn": None,
        "dwconn": None,
        "mongodbclient": db,
        "mongo_master_db": master_db,
        "sqs": None,
        "NavitaireUpdateMethod": Constants.S3,
        "s3": None,
        "runTime": None,
        "cache_client": None,
        "route_type": "direct",
        "timelogger": None,
        "output_dir": output_dir
    }

    print("\n[2/3] Running NextGen Dynamic Allocation...")
    DynAlloc(inputData, currParams)

    print(f"\n[3/3] Validating Generated Outputs in {os.path.basename(os.path.dirname(output_dir))}...")
    summary_path = os.path.join(output_dir, "summary_nextgen.csv")
    inv_opt_path = os.path.join(output_dir, "inventory_optimization.csv")

    assert os.path.exists(summary_path), f"Expected summary_nextgen.csv at {summary_path}"
    assert os.path.exists(inv_opt_path), f"Expected inventory_optimization.csv at {inv_opt_path}"

    df_summary = pd.read_csv(summary_path)
    df_inv = pd.read_csv(inv_opt_path)

    print(f"\n- summary_nextgen.csv rows: {len(df_summary)}, columns: {len(df_summary.columns)}")
    print(f"- inventory_optimization.csv rows: {len(df_inv)}, columns: {len(df_inv.columns)}")

    if not df_inv.empty:
        tb = df_inv['Total_Bookings'].iloc[0]
        pf = df_inv['Predicted_Flown'].iloc[0]
        print(f"\nSourced Bookload verification:")
        print(f"  Total_Bookings = {tb} (sourced directly from weighted forecast file)")
        print(f"  Predicted_Flown = {pf}")

        promo_df = df_inv[df_inv['Promo_Flag'] == 1]
        if not promo_df.empty:
            p_fare = promo_df['fare'].iloc[0]
            p_au = promo_df['AU'].iloc[0]
            p_au2 = promo_df['AU_2'].iloc[0]
            p_aurev = promo_df['AU_Revised'].iloc[0]
            print(f"\nPromo Inventory verification:")
            print(f"  Promo Fare = {p_fare}")
            print(f"  Promo Initial AU = {p_au}")
            print(f"  Promo AU_2 (Competition Response) = {p_au2}")
            print(f"  Promo AU_Revised (Capped by PromoFirstRBD_NG) = {p_aurev}")
            assert p_aurev <= 10, f"Promo AU_Revised {p_aurev} exceeds PromoFirstRBD_NG limit 10"
        else:
            print(f"\nPromo Inventory: PromoFlag_NG is off or no promo row generated.")

    print("\n" + "=" * 70)
    print("NextGen Optimization test completed successfully!")
    print("=" * 70)


if __name__ == "__main__":
    run_nextgen_test()
