import pandas as pd

# Canonical static column names for sale fares
SALE_FARE_CANONICAL_COLS = ['key', 'sector', 'type', 'currency', 'sale_start', 'sale_end']


def _normalize_static_col(name):
    """Normalize a column name for matching: lowercase, strip, remove spaces/underscores."""
    return str(name).strip().lower().replace(" ", "").replace("_", "")


def _build_static_col_mapping(raw_col_names):
    """
    Match raw static column names from the CSV to canonical names.
    Uses the same case/whitespace/underscore-insensitive logic as
    normalize_dataframe_columns in main.py.

    Returns:
        list of canonical column names in the order they appear in the CSV.

    Raises:
        ValueError if a canonical column cannot be matched.
    """
    canonical_lookup = {
        _normalize_static_col(col): col for col in SALE_FARE_CANONICAL_COLS
    }

    mapped = []
    unmatched = []
    for raw in raw_col_names:
        key = _normalize_static_col(raw)
        if key in canonical_lookup:
            mapped.append(canonical_lookup[key])
        else:
            unmatched.append(raw)

    missing = [c for c in SALE_FARE_CANONICAL_COLS if c not in mapped]
    if missing:
        raise ValueError(
            f"Sale fare file is missing required static columns: {missing}. "
            f"Unrecognized columns: {unmatched}"
        )
    return mapped


def pivot_sale_fares(df):
    # Number of static columns in CSV (columns before departure dates)
    # Columns: key, sector, type, currency, sale_start, sale_end
    num_static_cols = 6
    
    # CSV has 3 header rows:
    # Row 0 (pd.read_csv header): Title row - "Travel Date Range" label (ignored)
    # df.iloc[0]: Empty static cols + Departure START dates
    # df.iloc[1]: Column names (key, Sector, etc.) + Departure END dates
    # df.iloc[2:]: Actual data rows
    
    # Get departure START dates from row 0 (date columns only)
    dept_start_dates = df.iloc[0, num_static_cols:].tolist()
    
    # Get departure END dates from row 1 (date columns only)
    dept_end_dates = df.iloc[1, num_static_cols:].tolist()

    # Normalize static column names from row 1 (the "real" header row)
    raw_static_names = df.iloc[1, :num_static_cols].tolist()
    normalized_static_cols = _build_static_col_mapping(raw_static_names)
    
    # Extract data rows (skip first 2 header rows)
    data_df = df.iloc[2:].reset_index(drop=True)
    
    # Use position-based indexing to get static columns, assign normalized names
    data_df_numeric = pd.DataFrame(
        data_df.iloc[:, :num_static_cols].values,
        columns=normalized_static_cols
    )
    
    # Create sale_id 
    data_df_numeric['sale_id'] = data_df_numeric.apply(
        lambda row: _generate_sale_id(row['sector'], row['sale_start'], row['sale_end']),
        axis=1
    )
    data_df_numeric.dropna(subset=['sector', 'type', 'sale_start', 'sale_end'], how='all', inplace=True)
    
    # TABLE 1: Sales
    sales_df = data_df_numeric[['sale_id', 'key', 'sector', 'type', 'currency', 'sale_start', 'sale_end']].drop_duplicates().reset_index(drop=True)
    
    # TABLE 2: Discounts
    # Get discount values from positions num_static_cols onwards
    discount_values = data_df.iloc[:, num_static_cols:].values
    
    # Create flat discount records (include key from parent sale row)
    discounts_data = []
    for row_idx, (sale_id, key_val) in enumerate(zip(data_df_numeric['sale_id'], data_df_numeric['key'])):
        for col_idx, dept_start in enumerate(dept_start_dates):
            discounts_data.append({
                'sale_id': sale_id,
                'key': key_val,
                'departure_start': dept_start,
                'departure_end': dept_end_dates[col_idx],
                'discount': discount_values[row_idx, col_idx]
            })
    
    discounts_df = pd.DataFrame(discounts_data)
    
    return sales_df, discounts_df

def _generate_sale_id(sector, sale_start, sale_end):
    """
    Generate unique sale_id from sector, sale_start, and sale_end.
    Format: sector_salestart_saleend (lowercase, no spaces/dashes)
    """
    sale_id = f"{sector}_{sale_start}_{sale_end}".lower().replace(' ', '_').replace('-', '')
    return sale_id