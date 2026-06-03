"""
Tariff Sheet Processor — validates and transforms the uploaded tariff CSV
into the storage format for the live_tariff_sheet MongoDB collection.

The uploaded CSV has display-friendly column names and comma-formatted fare values.
This processor:
1. Validates the CSV structure and data
2. Strips comma formatting from fare values and converts to Int32
3. Transforms into the storage schema with proper field names
"""

import pandas as pd
import numpy as np


def _parse_fare_value(val) -> int | None:
    """
    Parse a single fare cell value.
    - Empty/whitespace/NaN → None
    - Strip commas, convert to int
    - Must be a positive integer after conversion

    Returns:
        int value or None if empty

    Raises:
        ValueError: If value is non-empty but not a valid positive integer
    """
    if pd.isna(val):
        return None
    s = str(val).strip()
    if s == "":
        return None
    # Strip commas (e.g. "1,402" → "1402")
    s = s.replace(",", "")
    try:
        parsed = int(float(s))
    except (ValueError, TypeError):
        raise ValueError(f"Invalid fare value: '{val}'")
    if parsed <= 0:
        raise ValueError(f"Fare value must be positive, got: {parsed}")
    return parsed


def validate_tariff_sheet(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate a tariff sheet DataFrame.

    Validations:
    1. Required columns present: Market, Stops, Fuel Charge (YQ) in INR, at least Fare_Level_1
    2. Market column is non-empty and follows "City - City" format (contains " - ")
    3. Stops column contains only 0 or 1
    4. YQ column is a non-negative integer
    5. Fare level columns contain valid numeric values (after stripping commas) or are empty
    6. Fare levels are in ascending order (non-empty values must be strictly increasing)
    7. No duplicate Market+Stops combinations
    8. At least one fare level per row

    Args:
        df: Raw DataFrame from CSV upload (columns as-is from CSV header)

    Returns:
        Validated DataFrame (still with original column names)

    Raises:
        ValueError: On any validation failure with descriptive message
    """
    if df.empty:
        raise ValueError("Uploaded tariff sheet is empty")

    df = df.copy()
    errors = []

    # --- 1. Required columns ---
    required_cols = ["Market", "Stops", "Fuel Charge (YQ) in INR"]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Detect fare level columns — accept any number (Fare_Level_1, Fare_Level_2, ... up to N)
    # The number of fare levels varies per tariff sheet based on calculation thresholds
    fare_level_cols = [col for col in df.columns if col.startswith("Fare_Level_")]
    if not fare_level_cols:
        raise ValueError(
            "No Fare_Level columns found. Expected at least one column matching 'Fare_Level_N' pattern "
            "(e.g., Fare_Level_1, Fare_Level_2, ...). The number of fare levels can vary."
        )

    # Sort fare level columns by their numeric suffix for ordered processing
    fare_level_cols = sorted(
        fare_level_cols,
        key=lambda c: int(c.replace("Fare_Level_", ""))
    )

    # --- 2. Market format ---
    for idx, val in df["Market"].items():
        s = str(val).strip()
        if not s or s.lower() in ("nan", "none", ""):
            errors.append(f"Row {idx + 2}: Market is empty")
        elif " - " not in s:
            errors.append(f"Row {idx + 2}: Market '{s}' must contain ' - ' (City - City format)")
    if errors:
        raise ValueError("; ".join(errors))

    df["Market"] = df["Market"].astype(str).str.strip()

    # --- 3. Stops values (0 or 1) ---
    df["Stops"] = pd.to_numeric(df["Stops"], errors="coerce")
    bad_stops = df[~df["Stops"].isin([0, 1])].index
    if len(bad_stops) > 0:
        details = [f"Row {r + 2}: '{df.at[r, 'Stops']}'" for r in bad_stops]
        raise ValueError(f"Stops must be 0 or 1: {'; '.join(details)}")
    df["Stops"] = df["Stops"].astype(int)

    # --- 4. YQ values (non-negative integer) ---
    yq_col = "Fuel Charge (YQ) in INR"
    for idx, val in df[yq_col].items():
        s = str(val).strip().replace(",", "")
        try:
            yq_val = int(float(s))
        except (ValueError, TypeError):
            errors.append(f"Row {idx + 2}: Invalid YQ value '{val}'")
            continue
        if yq_val < 0:
            errors.append(f"Row {idx + 2}: YQ must be non-negative, got {yq_val}")
    if errors:
        raise ValueError("; ".join(errors))

    # --- 5 & 6 & 8. Fare level validation ---
    for idx, row in df.iterrows():
        row_fares = []
        has_at_least_one = False

        for col in fare_level_cols:
            try:
                parsed = _parse_fare_value(row[col])
            except ValueError as e:
                errors.append(f"Row {idx + 2}, {col}: {str(e)}")
                continue

            if parsed is not None:
                has_at_least_one = True
                row_fares.append((col, parsed))

        # --- 8. At least one fare level per row ---
        if not has_at_least_one:
            errors.append(f"Row {idx + 2}: Must have at least one fare level value")

        # --- 6. Ascending order check ---
        if len(row_fares) >= 2:
            for i in range(1, len(row_fares)):
                prev_col, prev_val = row_fares[i - 1]
                curr_col, curr_val = row_fares[i]
                if curr_val <= prev_val:
                    errors.append(
                        f"Row {idx + 2}: Fare levels not strictly ascending — "
                        f"{prev_col}={prev_val} >= {curr_col}={curr_val}"
                    )
                    break  # One error per row is enough

    if errors:
        raise ValueError("; ".join(errors))

    # --- 7. No duplicate Market+Stops ---
    dupes = df[df.duplicated(subset=["Market", "Stops"], keep=False)]
    if not dupes.empty:
        dup_details = dupes[["Market", "Stops"]].drop_duplicates().values.tolist()
        details = [f"'{m}' (Stops={int(s)})" for m, s in dup_details]
        raise ValueError(f"Duplicate Market+Stops combinations: {', '.join(details)}")

    return df


def format_tariff_sheet(df: pd.DataFrame, station_master=None) -> dict:
    """
    Transform validated tariff sheet DataFrame into MongoDB storage format.

    Input columns: Market, Stops, Fuel Charge (YQ) in INR, Fare_Level_1..Fare_Level_77

    Output: dict suitable for storing in live_tariff_sheet collection:
    {
        "fare_data": [
            {
                "sector": "IXAGAU",
                "market": "Agartala - Guwahati",
                "stops": 0,
                "yq": 199,
                "fare_levels": [1402, 1502, 1550, ...]  # Int32 values, no commas, no empty padding
            },
            ...
        ]
    }

    Fare values transformation:
    - Strip commas: "1,402" → "1402"
    - Convert to int: "1402" → 1402
    - Empty/whitespace values are excluded from the fare_levels array

    Sector derivation:
    - Market "Agartala - Guwahati" → look up "Agartala" in station_master → IXA
    - Look up "Guwahati" → GAU → sector = "IXAGAU"

    Args:
        df: Validated DataFrame with original CSV column names
        station_master: list of dicts from config_station_master with keys 'station', 'name'

    Returns:
        dict with 'fare_data' key containing list of sector dicts
    """
    yq_col = "Fuel Charge (YQ) in INR"

    # Build reverse lookup: city name (lowercase) → station code
    name_to_code = {}
    if station_master:
        for entry in station_master:
            name = entry.get('name', '')
            code = entry.get('station', '')
            if name and code:
                name_to_code[name.strip().lower()] = code.strip()

    # Identify fare level columns in order
    fare_level_cols = sorted(
        [col for col in df.columns if col.startswith("Fare_Level_")],
        key=lambda c: int(c.replace("Fare_Level_", ""))
    )

    fare_data = []

    for _, row in df.iterrows():
        # Parse YQ
        yq_val = int(float(str(row[yq_col]).strip().replace(",", "")))

        # Parse fare levels — only include non-empty values
        fare_levels = []
        for col in fare_level_cols:
            parsed = _parse_fare_value(row[col])
            if parsed is not None:
                fare_levels.append(parsed)

        # Derive sector code from market name using station master
        market = str(row["Market"]).strip()
        sector = ""
        if " - " in market and name_to_code:
            parts = market.split(" - ", 1)
            origin_name = parts[0].strip().lower()
            dest_name = parts[1].strip().lower()
            origin_code = name_to_code.get(origin_name, "")
            dest_code = name_to_code.get(dest_name, "")
            if origin_code and dest_code:
                sector = origin_code + dest_code

        record = {
            "sector": sector,
            "market": market,
            "stops": int(row["Stops"]),
            "yq": yq_val,
            "fare_levels": fare_levels,
        }
        fare_data.append(record)

    return {"fare_data": fare_data}
