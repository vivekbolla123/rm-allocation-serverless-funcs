#!/usr/bin/env python3
"""
rm-additive-process/localtest/run_inventory_ng_test.py
=============================================================================
Standalone test runner for NextGen AU Inventory Optimization.

Default behavior (no parameters required):
- Input:  rm-additive-process/localtest/inputs/add_model_fare_bucket_level_weighted.csv
- Output: rm-additive-process/localtest/outputs/inventory_ng_inputfile.csv

Generates exactly the 50 required columns:
sector,date,DOW,STD,flight,NDO,capacity,Total_Bookings,fare,oal_b2c_min,anchor_fare,
strategy_fare,Promo_Flag,mu_bar_orig,mu,AU,P_eq,mu_bar_2,mu_2,AU_2,AU_Revised,
predicted_pickup,Predicted_Flown,Predicted_pickup_Revised,Predicted_Flown_Revised,
LF_Forecast_Revised,AF_Forecast,RASK_Forecast,Distance,RPKM,ASKM,
Capped_Predicted_Flown_Revised,A_cap,Current_Revenue,Expected_Revenue,
Total_Expected_Revenue,Predicted_Revenue,SnapshotDate,Tagging,FareBucket,sigma,
STD_future,qp_b2c_min,sigma_bar,F_j,protection_limit_yj,ProtectionLimit,sigma_3,
ProtectionLimit_3,sigma_bar_3.
"""

import argparse
import logging
import os
import sys
import time
import pandas as pd
import numpy as np

# Robust import paths whether run from repo root or inside localtest
_CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
_PKG_DIR = os.path.abspath(os.path.join(_CURRENT_DIR, ".."))
_ROOT_DIR = os.path.abspath(os.path.join(_CURRENT_DIR, "..", ".."))

for p in [_PKG_DIR, _ROOT_DIR, os.path.join(_ROOT_DIR, "rm-allocation-configurations")]:
    if p not in sys.path:
        sys.path.insert(0, p)

from helpers.utils.common_utils import (
    INVENTORY_NG_COLUMNS,
    compute_inventory_ng_df,
    compute_flight_inventory_ng
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger("inventory_ng_runner")


# Hardcoded default paths
DEFAULT_INPUT_FILE = os.path.join(_CURRENT_DIR, "inputs", "add_model_fare_bucket_level_weighted.csv")
DEFAULT_OUTPUT_FILE = os.path.join(_CURRENT_DIR, "outputs", "inventory_ng_inputfile.csv")


def resolve_path(path_candidate, is_output=False):
    """Resolves file paths checking inputs/outputs and fallback locations."""
    if os.path.isabs(path_candidate):
        return path_candidate
    if os.path.exists(path_candidate) and not is_output:
        return os.path.abspath(path_candidate)

    candidates = [
        os.path.join(_CURRENT_DIR, "inputs", path_candidate) if not is_output else os.path.join(_CURRENT_DIR, "outputs", path_candidate),
        os.path.join(_CURRENT_DIR, path_candidate),
        os.path.join(_ROOT_DIR, path_candidate),
        os.path.join(_PKG_DIR, path_candidate)
    ]
    for c in candidates:
        if os.path.exists(c):
            return c
    return os.path.abspath(candidates[0] if is_output else path_candidate)


def run_inventory_ng_pipeline(
    input_path=DEFAULT_INPUT_FILE,
    output_path=DEFAULT_OUTPUT_FILE,
    pax_col='Final_pax_count',
    std_col='weighted_pax_std',
    buy_up_factor=0.0,
    sigma_factor=0.25,
    first_rbd=20.0,
    promo_ratio=0.80
):
    """
    Reads the input fare bucket weighted CSV, executes NextGen optimization,
    and returns the resulting 50-column DataFrame.
    """
    resolved_in = resolve_path(input_path) if input_path != DEFAULT_INPUT_FILE else DEFAULT_INPUT_FILE
    if not os.path.exists(resolved_in):
        # Fallback to root farebucketweoghted.csv if input not in inputs/
        fallback_root = os.path.join(_ROOT_DIR, "farebucketweoghted.csv")
        if os.path.exists(fallback_root):
            resolved_in = fallback_root
        else:
            raise FileNotFoundError(f"Input file not found: {resolved_in}")

    resolved_out = resolve_path(output_path, is_output=True) if output_path != DEFAULT_OUTPUT_FILE else DEFAULT_OUTPUT_FILE

    logger.info(f"Input file:  {resolved_in}")
    logger.info(f"Output file: {resolved_out}")

    t0 = time.time()
    input_df = pd.read_csv(resolved_in)
    logger.info(f"Loaded {len(input_df)} rows with columns: {input_df.columns.tolist()} in {time.time()-t0:.2f}s")

    # Group flights by (Sector, DepartureDate, FlightNumber)
    sec_col = 'Sector' if 'Sector' in input_df.columns else 'sector'
    dep_col = 'DepartureDate' if 'DepartureDate' in input_df.columns else ('DepartureDate_future' if 'DepartureDate_future' in input_df.columns else 'date')
    flt_col = 'FlightNumber' if 'FlightNumber' in input_df.columns else ('FlightNumber_future' if 'FlightNumber_future' in input_df.columns else 'flight')

    group_keys = [sec_col, dep_col, flt_col]
    grouped = input_df.groupby(group_keys, sort=False)
    n_flights = len(grouped)
    logger.info(f"Found {n_flights} flights to process across {input_df[sec_col].nunique()} sector(s): {sorted(input_df[sec_col].unique())}")

    logger.info("Executing NextGen Inventory Optimization across flights...")
    t1 = time.time()
    
    out_df = compute_inventory_ng_df(
        input_df,
        pax_col=pax_col,
        std_col=std_col,
        buy_up_factor=buy_up_factor,
        sigma_factor=sigma_factor,
        first_rbd=first_rbd,
        promo_ratio=promo_ratio
    )

    elapsed = time.time() - t1
    logger.info(f"Optimization completed in {elapsed:.2f}s (avg {elapsed/max(1, n_flights)*1000:.1f}ms/flight).")
    logger.info(f"Output shape: {out_df.shape} ({len(out_df)} rows, {len(out_df.columns)} columns)")

    # Verify column exactness
    missing = [c for c in INVENTORY_NG_COLUMNS if c not in out_df.columns]
    extra = [c for c in out_df.columns if c not in INVENTORY_NG_COLUMNS]
    if missing:
        logger.error(f"Missing required columns in output: {missing}")
    if extra:
        logger.warning(f"Extra columns found in output: {extra}")
    assert out_df.columns.tolist() == INVENTORY_NG_COLUMNS, "Output columns do not match INVENTORY_NG_COLUMNS exactly!"
    logger.info("All 50 output columns verified exactly matching the specification.")

    if resolved_out:
        out_dir = os.path.dirname(os.path.abspath(resolved_out))
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
        try:
            out_df.to_csv(resolved_out, index=False)
            logger.info(f"Saved output CSV to: {resolved_out}")
        except PermissionError:
            logger.warning(f"Permission denied writing {resolved_out} (file open in external program).")

    return out_df


def main():
    parser = argparse.ArgumentParser(
        description="Test runner for NextGen inventory optimization (inventory_ng_inputfile.csv)"
    )
    parser.add_argument(
        "--input",
        "-i",
        default=DEFAULT_INPUT_FILE,
        help=f"Path to input fare bucket weighted CSV (default: {DEFAULT_INPUT_FILE})"
    )
    parser.add_argument(
        "--output",
        "-o",
        default=DEFAULT_OUTPUT_FILE,
        help=f"Path to output CSV (default: {DEFAULT_OUTPUT_FILE})"
    )
    parser.add_argument(
        "--pax-col",
        default="Final_pax_count",
        help="Demand column to use (default: Final_pax_count)"
    )
    parser.add_argument(
        "--std-col",
        default="weighted_pax_std",
        help="Sigma column to use (default: weighted_pax_std)"
    )
    parser.add_argument(
        "--buy-up",
        type=float,
        default=0.0,
        help="Buy-up factor alpha (default: 0.0)"
    )
    parser.add_argument(
        "--sigma-factor",
        type=float,
        default=0.25,
        help="Sigma factor (default: 0.25)"
    )
    parser.add_argument(
        "--first-rbd",
        type=float,
        default=20.0,
        help="First RBD allocation seats (default: 20.0)"
    )

    args = parser.parse_args()

    out_df = run_inventory_ng_pipeline(
        input_path=args.input,
        output_path=args.output,
        pax_col=args.pax_col,
        std_col=args.std_col,
        buy_up_factor=args.buy_up,
        sigma_factor=args.sigma_factor,
        first_rbd=args.first_rbd
    )

    print("\n--- SAMPLE INVENTORY_NG OUTPUT (First 5 rows) ---")
    preview_cols = ['sector', 'date', 'flight', 'FareBucket', 'fare', 'mu', 'AU', 'AU_Revised', 'anchor_fare', 'strategy_fare', 'Predicted_Revenue']
    print(out_df[preview_cols].head(5).to_string(index=False))
    print(f"\nCompleted successfully. Output saved to: {args.output}")


if __name__ == "__main__":
    main()
