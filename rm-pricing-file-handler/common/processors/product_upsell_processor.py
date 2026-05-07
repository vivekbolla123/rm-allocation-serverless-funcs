import pandas as pd
import numpy as np

def convert_product_upsell_data_types(df):
    """
    Convert product upsell data to proper data types for database storage.
    
    Args:
        df: DataFrame containing product upsell data
        
    Returns:
        DataFrame: Data with proper data types
    """
    if df.empty:
        return df
    
    df = df.copy()  # Avoid modifying original dataframe
    df.columns = df.columns.str.lower()
    # CRITICAL: Ensure rule_nbr is always stored as string
    # This prevents issues where rule numbers like 1002, 5002 are stored as integers
    if 'rule_nbr' in df.columns:
        df['rule_nbr'] = df['rule_nbr'].astype(str)
    
    # String columns that should remain as strings
    string_columns = ['sector', 'rbd_start', 'rbd_end', 'flt_nbr', 'flt_tag', 'footnote', 'ow_rt', 'ndo', 'pos', 
                     'type', 'base_all_in', 'base_footnote', 'base_ndo']
    for col in string_columns:
        if col in df.columns:
            # Convert to string, handling NaN values properly
            df[col] = df[col].astype(str).replace('nan', '')
    
    # Numeric columns that should be float/numeric
    numeric_columns = ['value', 'min_value', 'max_value', 'sort_price']
    for col in numeric_columns:
        if col in df.columns:
            # Convert to numeric, handling empty strings and NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df

def validate_product_upsell(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate product upsell data for duplicate keys and ensure proper data types.
    Column normalization is handled upstream by normalize_dataframe_columns.

    Args:
        df: DataFrame with already-normalized column names

    Returns:
        DataFrame: Validated data with proper data types

    Raises:
        ValueError: If duplicate keys are found or required columns are missing
    """
    if df.empty:
        return df

    key_columns = ['rule_nbr', 'sector', 'rbd_start', 'rbd_end', 'footnote', 'ow_rt', 'ndo', 'pos', 'flt_nbr', 'flt_tag']

    missing_columns = [col for col in key_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    # rule_nbr must always be string to prevent 1002 being stored as int
    df['rule_nbr'] = df['rule_nbr'].astype(str)

    string_columns = ['sector', 'rbd_start', 'rbd_end', 'footnote', 'ow_rt', 'ndo', 'pos', 'flt_nbr', 'flt_tag']
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).replace('nan', '')

    numeric_columns = ['value', 'min_value', 'max_value', 'sort_price']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    duplicates = df.duplicated(subset=key_columns, keep=False)
    if duplicates.any():
        duplicate_rows = df[duplicates].copy()
        duplicate_rows['row_number'] = duplicate_rows.index + 2

        duplicate_details = []
        for key_combination, group in duplicate_rows.groupby(key_columns):
            row_numbers = group['row_number'].tolist()
            key_values = [f"{col}={key_combination[i]}" for i, col in enumerate(key_columns)]
            duplicate_details.append(
                f"Key combination ({', '.join(key_values)}) found in rows: {', '.join(map(str, row_numbers))}"
            )

        raise ValueError(
            f"VALIDATION FAILED: Duplicate key combinations found in product_upsell data. "
            f"Each combination of rule_nbr, sector, rbd_start, rbd_end, footnote, ow_rt, ndo, pos must be unique. "
            f"Duplicates found: {'; '.join(duplicate_details)}"
        )

    return df