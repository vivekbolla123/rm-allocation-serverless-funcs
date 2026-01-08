import pandas as pd

def validate_product_upsell(df):
    """
    Validate product upsell data for duplicate keys
    
    Args:
        df: DataFrame containing product upsell data
        
    Returns:
        DataFrame: Validated data
        
    Raises:
        ValueError: If duplicate keys are found
    """
    if df.empty:
        return df
    
    # Define the key columns that should be unique
    key_columns = ['rule_nbr', 'sector', 'rbd_start', 'rbd_end', 'footnote', 'ow_rt', 'ndo', 'pos']
    
    # Check if all key columns exist in the dataframe
    missing_columns = [col for col in key_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    # Check for duplicate combinations of key columns
    duplicates = df.duplicated(subset=key_columns, keep=False)
    
    if duplicates.any():
        duplicate_rows = df[duplicates].copy()
        # Add row numbers (1-based, accounting for header)
        duplicate_rows['row_number'] = duplicate_rows.index + 2  # +2 because index is 0-based and we have header
        
        # Group by the key columns to show which rows have the same keys
        duplicate_details = []
        grouped = duplicate_rows.groupby(key_columns)
        
        for key_combination, group in grouped:
            row_numbers = group['row_number'].tolist()
            key_values = [f"{col}={key_combination[i]}" for i, col in enumerate(key_columns)]
            duplicate_details.append(
                f"Key combination ({', '.join(key_values)}) found in rows: {', '.join(map(str, row_numbers))}"
            )
        
        error_message = (
            f"VALIDATION FAILED: Duplicate key combinations found in product_upsell data. "
            f"Each combination of rule_nbr, sector, rbd_start, rbd_end, footnote, ow_rt, ndo, pos must be unique. "
            f"Duplicates found: {'; '.join(duplicate_details)}"
        )
        
        raise ValueError(error_message)
    
    return df