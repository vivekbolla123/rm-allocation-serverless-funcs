import pytest
import os
import sys
import pandas as pd

sys.path.insert(0, os.path.abspath("rm-allocation-configurations"))
sys.path.insert(0, os.path.abspath("rm-allocation-process-inputs"))

from Constants import CONST_OUTPUT_SUMMARY_NEXTGEN_ORDER, CONST_OUTPUT_INVENTORY_OPTIMIZATION_ORDER
from models.output_summary_nextgen import OutputSummaryNextGen


def test_nextgen_summary_exact_columns_and_order():
    expected_nextgen_summary_cols = [
        'Origin', 'Destin', 'FltNum', 'DepDate', 'Capacity',
        'NG_FirstRBD', 'Predicted_Flown_Revised',
        'MarketFare_Selected', 'MarketFare_Min', 'MarketFare_Max',
        'Grid_Name', 'Rule_Applied', 'nextgen_flag', 'competition_response_flag',
        'buy_up_factor', 'RunId', 'Id',
        'NG_Historical_current_booked', 'NG_LF_Forecast', 'NG_AF_Forecast',
        'NG_Opening_fare', 'NG_Strategy_fare', 'NG_Anchor_fare',
        'analystName', 'status', 'allocationStatus'
    ]
    assert CONST_OUTPUT_SUMMARY_NEXTGEN_ORDER == expected_nextgen_summary_cols
    assert len(CONST_OUTPUT_SUMMARY_NEXTGEN_ORDER) == 26


def test_inventory_optimization_exact_columns_and_order():
    expected_inv_opt_cols = [
        'sector', 'date', 'DOW', 'STD', 'flight', 'NDO', 'capacity', 'Total_Bookings',
        'fare', 'oal_b2c_min', 'anchor_fare', 'strategy_fare', 'opening fare', 'Promo_Flag',
        'mu_bar_orig', 'mu', 'AU', 'P_eq', 'mu_bar_2', 'mu_2', 'AU_2', 'AU_Revised',
        'predicted_pickup', 'Predicted_Flown', 'Predicted_pickup_Revised', 'Predicted_Flown_Revised',
        'LF_Forecast_Revised', 'AF_Forecast', 'Distance', 'ASKM', 'RPKM', 'RASK_Forecast', 'Current_Revenue',
        'Expected_Revenue', 'Total_Expected_Revenue', 'Predicted_Revenue',
        'SnapshotDate', 'Tagging', 'FareBucket', 'RunId'
    ]
    assert CONST_OUTPUT_INVENTORY_OPTIMIZATION_ORDER == expected_inv_opt_cols
    assert len(CONST_OUTPUT_INVENTORY_OPTIMIZATION_ORDER) == 40


def test_output_summary_has_nextgen_columns():
    from Constants import CONST_OUTPUT_SUMMARY_ORDER
    ng_cols = [
        'NG_LF_Forecast', 'NG_AF_Forecast', 'NG_Historical_current_booked',
        'NG_Opening_fare', 'NG_FirstRBD', 'NG_Strategy_fare', 'NG_Anchor_fare'
    ]
    for col in ng_cols:
        assert col in CONST_OUTPUT_SUMMARY_ORDER
    assert CONST_OUTPUT_SUMMARY_ORDER[-7:] == ng_cols


def _resolve_input_path(filename):
    for d in ["inputnexgen", "inputnextgen", "input"]:
        p = os.path.abspath(f"QPDynamicAllocation/testcases/{d}/{filename}")
        if os.path.exists(p):
            return p
    return os.path.abspath(f"QPDynamicAllocation/testcases/inputnexgen/{filename}")


def _resolve_output_path(filename):
    for d in ["outputnexgen/generated_output", "outputnextgen/generated_output", "output/generated_output"]:
        p = os.path.abspath(f"QPDynamicAllocation/testcases/{d}/{filename}")
        if os.path.exists(p):
            return p
    return os.path.abspath(f"QPDynamicAllocation/testcases/outputnexgen/generated_output/{filename}")


def test_comp_fare_recalibration_blrgau_zero():
    csv_path = _resolve_input_path("comp_fare_recalibration.csv")
    assert os.path.exists(csv_path)
    df = pd.read_csv(csv_path)
    blrgau_row = df[df['Sector'] == 'BLRGAU']
    assert not blrgau_row.empty
    row_dict = blrgau_row.iloc[0].to_dict()
    for col in ['IX', '6E', 'SG', 'AI']:
        assert float(row_dict[col]) == 0.0


def test_add_model_fare_bucket_level_weighted_file_dates_and_sectors():
    target_path = _resolve_input_path("add_model_fare_bucket_level_weighted.csv")
    assert os.path.exists(target_path)
    df = pd.read_csv(target_path, nrows=50)
    assert df['SnapshotDate'].iloc[0] in ['2023-08-01 00:00:00', '09/21/2026', '2026-09-21']
    assert pd.to_datetime(df['DepartureDate']).min() >= pd.to_datetime('2023-08-01')

    # Verify key testcase flights are present
    df_all_keys = pd.read_csv(target_path, usecols=['Sector', 'FlightNumber'])
    bomdel_flights = df_all_keys[df_all_keys['Sector'] == 'BOMDEL']['FlightNumber'].unique()
    assert 1127 in bomdel_flights or 1120 in bomdel_flights or 1109 in bomdel_flights

    if 'DELBOM' in df_all_keys['Sector'].values:
        delbom_flights = df_all_keys[df_all_keys['Sector'] == 'DELBOM']['FlightNumber'].unique()
        assert 1128 in delbom_flights

    if 'BLRGAU' in df_all_keys['Sector'].values:
        blrgau_flights = df_all_keys[df_all_keys['Sector'] == 'BLRGAU']['FlightNumber'].unique()
        assert 1323 in blrgau_flights


def test_generated_output_csv_exact_headers():
    summary_ng_path = _resolve_output_path("summary_nextgen.csv")
    inv_opt_path = _resolve_output_path("inventory_optimization.csv")

    if os.path.exists(summary_ng_path):
        df_ng = pd.read_csv(summary_ng_path)
        assert list(df_ng.columns) == CONST_OUTPUT_SUMMARY_NEXTGEN_ORDER

    if os.path.exists(inv_opt_path):
        df_inv = pd.read_csv(inv_opt_path)
        assert list(df_inv.columns) == CONST_OUTPUT_INVENTORY_OPTIMIZATION_ORDER


def test_au_grid_final_aus_rounded_and_inventory_opt_unrounded():
    au_grid_path = _resolve_output_path("AUGrid.csv")
    inv_opt_path = _resolve_output_path("inventory_optimization.csv")

    if os.path.exists(au_grid_path):
        df_au = pd.read_csv(au_grid_path)
        au_cols = [c for c in df_au.columns if 'Class AU' in c]
        for col in au_cols:
            for val in df_au[col].dropna():
                # Values in AUGrid must be integers (rounded off)
                assert float(val).is_integer(), f"AUGrid column {col} has non-integer value {val}"

    if os.path.exists(inv_opt_path):
        df_inv = pd.read_csv(inv_opt_path)
        # In inventory_optimization, AU and AU_Revised remain unrounded floats
        assert pd.api.types.is_float_dtype(df_inv['AU'])
        assert pd.api.types.is_float_dtype(df_inv['AU_Revised'])
        # Verify presence of non-integer floats in unrounded forecast ladder
        has_non_int = any(not float(v).is_integer() for v in df_inv['AU_Revised'].dropna())
        assert has_non_int, "Expected unrounded floats in inventory_optimization AU_Revised"
