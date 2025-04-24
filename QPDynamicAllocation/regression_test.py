import os
import pandas as pd
import pytest
import numpy as np

MASTER_OUTPUT_DIR = "QPDynamicAllocation/testcases/output/expected_output"
CURRENT_OUTPUT_DIR = "QPDynamicAllocation/testcases/output/generated_output"

def get_csv_files(directory):
    return [f for f in os.listdir(directory) if f.endswith(".csv")]

def find_differences(df_master, df_current):
    """Find and report differences between two dataframes."""
    differences = []
    
    # Check if columns match
    if set(df_master.columns) != set(df_current.columns):
        missing_cols = set(df_master.columns) - set(df_current.columns)
        extra_cols = set(df_current.columns) - set(df_master.columns)
        
        if missing_cols:
            differences.append(f"Missing columns in current output: {', '.join(missing_cols)}")
        if extra_cols:
            differences.append(f"Extra columns in current output: {', '.join(extra_cols)}")
    
    # Compare data row by row, column by column
    common_cols = set(df_master.columns).intersection(set(df_current.columns))
    
    for col in common_cols:
        for idx in range(min(len(df_master), len(df_current))):
            master_val = df_master[col].iloc[idx]
            current_val = df_current[col].iloc[idx]
            
            # Handle NaN comparison specially
            if pd.isna(master_val) and pd.isna(current_val):
                continue
                
            # For numeric values, check if they're close enough
            if pd.api.types.is_numeric_dtype(df_master[col]) and pd.api.types.is_numeric_dtype(df_current[col]):
                if pd.isna(master_val) != pd.isna(current_val):
                    differences.append(f"Row {idx+1}, Column '{col}': Master has {'NaN' if pd.isna(master_val) else master_val} but Current has {'NaN' if pd.isna(current_val) else current_val}")
                elif not pd.isna(master_val) and not pd.isna(current_val):
                    if not np.isclose(master_val, current_val, rtol=1e-5, atol=1e-8, equal_nan=True):
                        differences.append(f"Row {idx+1}, Column '{col}': Master={master_val}, Current={current_val}")
            # For non-numeric values, direct comparison
            elif master_val != current_val:
                differences.append(f"Row {idx+1}, Column '{col}': Master='{master_val}', Current='{current_val}'")
    
    # Check if row counts differ
    if len(df_master) != len(df_current):
        differences.append(f"Row count mismatch: Master has {len(df_master)} rows, Current has {len(df_current)} rows")
    
    return differences

@pytest.mark.parametrize("filename", get_csv_files(MASTER_OUTPUT_DIR))
def test_compare_master_with_current(filename):
    master_path = os.path.join(MASTER_OUTPUT_DIR, filename)
    current_path = os.path.join(CURRENT_OUTPUT_DIR, filename)

    assert os.path.exists(current_path), f"Missing current output file: {filename}"

    df_master = pd.read_csv(master_path)
    df_current = pd.read_csv(current_path)

    # Match only the first len(df_master) rows of current
    df_current_subset = df_current.iloc[:len(df_master)]

    try:
        pd.testing.assert_frame_equal(
            df_master.reset_index(drop=True),
            df_current_subset.reset_index(drop=True),
            check_dtype=False,
        )
    except AssertionError:
        # Find specific differences
        differences = find_differences(
            df_master.reset_index(drop=True), 
            df_current_subset.reset_index(drop=True)
        )
        
        # Create a detailed error message
        error_msg = f"Regression test failed for {filename}:\n"
        error_msg += "\n".join(differences[:20])  # Limit to first 20 differences to prevent overwhelming output
        
        if len(differences) > 20:
            error_msg += f"\n...and {len(differences) - 20} more differences."
            
        pytest.fail(error_msg)