"""
Market List Processor — validates CSV uploads for market_list,
market_list_international, and market_list_connections tables.

Validates headers (case-insensitive match to expected camelCase columns),
data types, date formats, and date range logic before insertion.
"""

import pandas as pd
from datetime import datetime


# Expected column names in camelCase (as stored in MongoDB).
# CSV headers are matched case-insensitively, then renamed to these.
EXPECTED_COLUMNS = [
    "Sector",
    "LadderCode",
    "DateRangeStart",
    "DateRangeEnd",
    "Type",
    "MinThreshold_B2C",
    "MaxThreshold_B2C",
    "MinThreshold_B2B",
    "MaxThreshold_B2B",
    "CarrierInclusion",
    "CarrierExclusion",
    "B2C_Distinct_Fare_Count",
    "CoverageParameter",
    "MarketFlag",
    "UP_B2C",
    "UP_B2B",
    "B2B_Threshold",
    "Yield_Threshold",
]

# Lowercase -> camelCase lookup — keyed with all spaces removed for flexible matching
_HEADER_MAP = {col.lower().replace(" ", ""): col for col in EXPECTED_COLUMNS}

# Columns that must be numeric
_NUMERIC_COLUMNS = [
    "MinThreshold_B2C", "MaxThreshold_B2C",
    "MinThreshold_B2B", "MaxThreshold_B2B",
    "B2C_Distinct_Fare_Count", "CoverageParameter",
    "UP_B2C", "UP_B2B", "B2B_Threshold", "Yield_Threshold",
]

# Columns that must be dates (YYYY-MM-DD)
_DATE_COLUMNS = ["DateRangeStart", "DateRangeEnd"]

# Valid values for Type column
_VALID_TYPES = {"bookings", "market"}

# Valid values for MarketFlag column
_VALID_MARKET_FLAGS = {"y", "n"}


def validate_market_list(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate a market list DataFrame.
    Column normalization and presence checks are handled upstream by normalize_dataframe_columns.

    Steps:
        1. Validate Sector format (6 alpha chars)
        2. Validate LadderCode is not empty
        3. Validate date columns (MM/DD/YYYY format)
        4. Validate DateRangeStart <= DateRangeEnd
        5. Validate date direction for Bookings type
        6. Validate numeric columns are numeric and non-negative
        7. Validate MinThreshold < MaxThreshold for B2C and B2B
        8. Validate Type is 'Bookings' or 'Market'
        9. Validate MarketFlag is 'Y' or 'N'
        10. Check for duplicate LadderCode
        11. Check for exact duplicate rows

    Args:
        df: DataFrame with already-normalized column names

    Returns:
        Validated DataFrame

    Raises:
        ValueError: On any validation failure with descriptive message.
    """
    if df.empty:
        raise ValueError("Uploaded file is empty")

    df = df.copy()
    errors = []

    # --- 1. Sector format ---
    for idx, val in df["Sector"].items():
        s = str(val).strip()
        if len(s) != 6 or not s.isalpha():
            errors.append(f"Row {idx + 2}: invalid Sector '{val}' — expected 6 alpha characters")
    if errors:
        raise ValueError("; ".join(errors))

    df["Sector"] = df["Sector"].astype(str).str.strip().str.upper()

    # --- 2. LadderCode must not be empty ---
    df["LadderCode"] = df["LadderCode"].astype(str).str.strip()
    empty_codes = df[df["LadderCode"].isin(["", "nan", "None"])].index
    if len(empty_codes) > 0:
        details = [f"Row {r + 2}" for r in empty_codes]
        raise ValueError(f"Empty LadderCode in: {', '.join(details)}")

    # --- 3. Date format (MM/DD/YYYY) ---
    for date_col in _DATE_COLUMNS:
        non_empty = df[date_col].notna() & (df[date_col].astype(str).str.strip() != '')
        if non_empty.any():
            parsed = pd.to_datetime(df.loc[non_empty, date_col], format="%m/%d/%Y", errors="coerce")
            bad_rows = parsed[parsed.isna()].index
            if len(bad_rows) > 0:
                bad_vals = [f"Row {r + 2}: '{df.at[r, date_col]}'" for r in bad_rows]
                raise ValueError(
                    f"Invalid date format in {date_col} (expected MM/DD/YYYY): {'; '.join(bad_vals)}"
                )

    # --- 4. DateRangeStart <= DateRangeEnd ---
    both_present = (
        df["DateRangeStart"].notna() & (df["DateRangeStart"].astype(str).str.strip() != '') &
        df["DateRangeEnd"].notna() & (df["DateRangeEnd"].astype(str).str.strip() != '')
    )
    if both_present.any():
        starts = pd.to_datetime(df.loc[both_present, "DateRangeStart"], errors="coerce")
        ends = pd.to_datetime(df.loc[both_present, "DateRangeEnd"], errors="coerce")
        bad = starts[starts > ends].index
        if len(bad) > 0:
            details = [f"Row {r + 2}" for r in bad]
            raise ValueError(f"DateRangeStart is after DateRangeEnd in: {', '.join(details)}")

    # --- 5. Bookings type date direction ---
    today = pd.Timestamp(datetime.now().date())
    for idx, row in df.iterrows():
        row_type = str(row["Type"]).strip().lower()
        start_val = str(row.get("DateRangeStart", "")).strip()
        end_val = str(row.get("DateRangeEnd", "")).strip()

        if row_type == "bookings" and start_val and end_val:
            start = pd.Timestamp(start_val)
            if start > today:
                errors.append(
                    f"Row {idx + 2}: Type is 'Bookings' but DateRangeStart ({start_val}) is in the future"
                )
    if errors:
        raise ValueError("; ".join(errors))

    # --- 6. Numeric columns: non-null and non-negative ---
    for col in _NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        bad_rows = df[df[col].isna()].index
        if len(bad_rows) > 0:
            bad_vals = [f"Row {r + 2}" for r in bad_rows]
            raise ValueError(f"Non-numeric value in {col}: {', '.join(bad_vals)}")
        if (df[col] < 0).any():
            neg_rows = df[df[col] < 0].index
            details = [f"Row {r + 2}" for r in neg_rows]
            raise ValueError(f"Negative value in {col}: {', '.join(details)}")

    # --- 7. Min < Max thresholds ---
    for channel in ["B2C", "B2B"]:
        min_col = f"MinThreshold_{channel}"
        max_col = f"MaxThreshold_{channel}"
        bad = df[df[min_col] >= df[max_col]].index
        if len(bad) > 0:
            details = [f"Row {r + 2}" for r in bad]
            raise ValueError(f"{min_col} >= {max_col} in: {', '.join(details)}")

    # --- 8. Type validation ---
    df["Type"] = df["Type"].astype(str).str.strip()
    invalid_types = df[~df["Type"].str.lower().isin(_VALID_TYPES)].index
    if len(invalid_types) > 0:
        details = [f"Row {r + 2}: '{df.at[r, 'Type']}'" for r in invalid_types]
        raise ValueError(f"Invalid Type (expected 'Bookings' or 'Market'): {'; '.join(details)}")

    # --- 9. MarketFlag validation ---
    df["MarketFlag"] = df["MarketFlag"].astype(str).str.strip()
    invalid_flags = df[~df["MarketFlag"].str.upper().isin({"Y", "N"})].index
    if len(invalid_flags) > 0:
        details = [f"Row {r + 2}: '{df.at[r, 'MarketFlag']}'" for r in invalid_flags]
        raise ValueError(f"Invalid MarketFlag (expected 'Y' or 'N'): {'; '.join(details)}")

    # --- 10. Duplicate LadderCode ---
    dupes = df[df.duplicated(subset=["LadderCode"], keep=False)]
    if not dupes.empty:
        dup_codes = dupes["LadderCode"].unique().tolist()
        raise ValueError(f"Duplicate LadderCode entries found: {dup_codes}")

    # --- 11. Exact duplicate rows ---
    exact_dupes = df[df.duplicated(keep=False)]
    if not exact_dupes.empty:
        dup_rows = [str(r + 2) for r in exact_dupes.index]
        raise ValueError(f"Exact duplicate rows found at rows: {', '.join(dup_rows)}")

    return df