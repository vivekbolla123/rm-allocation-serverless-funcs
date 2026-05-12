import pandas as pd


def convert_footnote_master_data_types(df):
    """
    Convert footnote master data to proper data types for database storage.

    Args:
        df: DataFrame containing footnote master data

    Returns:
        DataFrame: Data with proper data types
    """
    if df.empty:
        return df

    df = df.copy()
    df.columns = df.columns.str.lower()

    string_columns = ['footnote', 'sales_start', 'sales_end', 'travel_start', 'travel_end']
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).replace('nan', '')

    return df


def validate_footnote_master(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate footnote master data for required columns, date formats, and duplicates.

    Expected columns: footnote, sales_start, sales_end, travel_start, travel_end
    Date format: MM/DD/YYYY (or empty, or 'Open' for end dates)

    Args:
        df: DataFrame with footnote master data

    Returns:
        DataFrame: Validated data with proper data types

    Raises:
        ValueError: If validation fails
    """
    if df.empty:
        return df

    required_columns = ['footnote', 'sales_start', 'sales_end', 'travel_start', 'travel_end']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    # Ensure string types
    string_columns = ['footnote', 'sales_start', 'sales_end', 'travel_start', 'travel_end']
    for col in string_columns:
        df[col] = df[col].astype(str).replace('nan', '')

    # Validate footnote is not empty
    empty_footnotes = df[df['footnote'].str.strip() == '']
    if not empty_footnotes.empty:
        row_numbers = (empty_footnotes.index + 2).tolist()
        raise ValueError(
            f"VALIDATION FAILED: Empty footnote values found in rows: "
            f"{', '.join(map(str, row_numbers[:10]))}"
            + (f" ... and {len(row_numbers) - 10} more" if len(row_numbers) > 10 else "")
        )

    # Validate date format (MM/DD/YYYY) for date columns
    date_columns = ['sales_start', 'sales_end', 'travel_start', 'travel_end']
    invalid_dates = []
    for col in date_columns:
        for idx, val in df[col].items():
            if val and val.strip() != '' and val.strip().lower() != 'open':
                try:
                    pd.to_datetime(val.strip(), format='%m/%d/%Y')
                except (ValueError, TypeError):
                    invalid_dates.append(f"Row {idx + 2}: {col}='{val}'")

    if invalid_dates:
        raise ValueError(
            f"VALIDATION FAILED: Invalid date format found. Expected MM/DD/YYYY or empty or 'Open'. "
            f"Invalid entries: {'; '.join(invalid_dates[:10])}"
            + (f" ... and {len(invalid_dates) - 10} more" if len(invalid_dates) > 10 else "")
        )

    # Validate that at least one date range is provided per row
    no_dates = df[
        (df['sales_start'].str.strip() == '') &
        (df['sales_end'].str.strip() == '') &
        (df['travel_start'].str.strip() == '') &
        (df['travel_end'].str.strip().isin(['', 'open']))
    ]
    if not no_dates.empty:
        row_numbers = (no_dates.index + 2).tolist()
        raise ValueError(
            f"VALIDATION FAILED: Rows with no date range specified (need at least sales or travel dates). "
            f"Rows: {', '.join(map(str, row_numbers[:10]))}"
            + (f" ... and {len(row_numbers) - 10} more" if len(row_numbers) > 10 else "")
        )

    # Validate logical date ordering (start <= end) where both are provided
    date_order_errors = []
    for idx, row in df.iterrows():
        # Sales date ordering
        s_start = row['sales_start'].strip()
        s_end = row['sales_end'].strip()
        if s_start and s_end and s_end.lower() != 'open':
            try:
                start_dt = pd.to_datetime(s_start, format='%m/%d/%Y')
                end_dt = pd.to_datetime(s_end, format='%m/%d/%Y')
                if start_dt > end_dt:
                    date_order_errors.append(f"Row {idx + 2}: sales_start ({s_start}) > sales_end ({s_end})")
            except (ValueError, TypeError):
                pass  # Already caught by format validation above

        # Travel date ordering
        t_start = row['travel_start'].strip()
        t_end = row['travel_end'].strip()
        if t_start and t_end and t_end.lower() != 'open':
            try:
                start_dt = pd.to_datetime(t_start, format='%m/%d/%Y')
                end_dt = pd.to_datetime(t_end, format='%m/%d/%Y')
                if start_dt > end_dt:
                    date_order_errors.append(f"Row {idx + 2}: travel_start ({t_start}) > travel_end ({t_end})")
            except (ValueError, TypeError):
                pass

    if date_order_errors:
        raise ValueError(
            f"VALIDATION FAILED: Start date is after end date. "
            f"Errors: {'; '.join(date_order_errors[:10])}"
            + (f" ... and {len(date_order_errors) - 10} more" if len(date_order_errors) > 10 else "")
        )

    # Check for duplicate footnote codes
    duplicates = df.duplicated(subset=['footnote'], keep=False)
    if duplicates.any():
        duplicate_rows = df[duplicates].copy()
        duplicate_rows['row_number'] = duplicate_rows.index + 2
        dup_details = []
        for footnote_val, group in duplicate_rows.groupby('footnote'):
            row_numbers = group['row_number'].tolist()
            dup_details.append(f"footnote='{footnote_val}' in rows: {', '.join(map(str, row_numbers))}")

        raise ValueError(
            f"VALIDATION FAILED: Duplicate footnote codes found. "
            f"Each footnote must be unique. "
            f"Duplicates: {'; '.join(dup_details[:10])}"
            + (f" ... and {len(dup_details) - 10} more" if len(dup_details) > 10 else "")
        )

    return df
