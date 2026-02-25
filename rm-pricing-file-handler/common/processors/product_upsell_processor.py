import pandas as pd
import numpy as np
import re
from common.config.db_config import get_mongo_db
from common.utils.db_helper import DbHelper

def _resolve_ndo_character(ndostart, ndo_master):
    if pd.isna(ndostart) or str(ndostart).strip() in ['', 'nan']:
        return 'X'
    
    ndostart_str = str(ndostart).strip()
    numeric_matches = re.findall(r'\d+', ndostart_str)
    if not numeric_matches:
        return 'X'
    
    input_ndo = int(numeric_matches[0])
    best_char = 'X'
    best_start = -1
    
    for row in ndo_master:
        master_start_str = str(row.get('NDOStart', '0'))
        master_numeric = re.findall(r'\d+', master_start_str)
        if not master_numeric:
            continue
        master_ndo = int(master_numeric[0])
        
        if master_ndo <= input_ndo and master_ndo > best_start:
            best_start = master_ndo
            best_char = str(row.get('Character', 'X'))
            
    return best_char

def _resolve_footnote(row, footnote_master):
    # Match SaleStart, SaleEnd, TravelStart, TravelEnd
    sale_start = str(row.get('SaleStart', '')).strip()
    sale_end = str(row.get('SaleEnd', '')).strip()
    travel_start = str(row.get('TravelStart', '')).strip()
    travel_end = str(row.get('TravelEnd', '')).strip()
    
    if sale_start == 'nan': sale_start = ''
    if sale_end == 'nan': sale_end = ''
    if travel_start == 'nan': travel_start = ''
    if travel_end == 'nan': travel_end = ''
    
    # If no dates provided, we can't map footnote based on dates
    if not sale_start and not sale_end and not travel_start and not travel_end:
        return ''
        
    for f_row in footnote_master:
        match = True
        if sale_start and f_row.get('SaleStart') and str(f_row.get('SaleStart')) != sale_start:
            match = False
        if sale_end and f_row.get('SaleEnd') and str(f_row.get('SaleEnd')) != sale_end:
            match = False
        if travel_start and f_row.get('TravelStart') and str(f_row.get('TravelStart')) != travel_start:
            match = False
        if travel_end and f_row.get('TravelEnd') and str(f_row.get('TravelEnd')) != travel_end:
            match = False
            
        if match:
            return str(f_row.get('Footnote', ''))
            
    return ''

def enrich_upsell_data(df):
    if df.empty:
        return df
        
    df = df.copy()
    
    # standardise column names to what code downstream expects where they differ in case
    # we'll map 'Footnote' to 'footnote' and 'ndostart' if needed, but we should preserve
    # the original or ensure both exist
    
    # Defaulting optional required columns if missing from new format CSV
    if 'ow_rt' not in df.columns:
        df['ow_rt'] = 'O'
    if 'pos' not in df.columns:
        df['pos'] = ''
        
    try:
        with get_mongo_db() as db:
            rule_number_master = list(db['config_rule_number_master'].find({}))
            footnote_master = list(db['config_footnote_master'].find({}))
            ndo_master = list(db['config_ndo_master'].find({}))
    except Exception as e:
        import logging
        logging.error(f"Failed to fetch master data from MongoDB: {str(e)}")
        rule_number_master = []
        footnote_master = []
        ndo_master = []

    rule_map = {str(r.get('Product', '')).strip().upper(): str(r.get('Rule', '')).strip() for r in rule_number_master if r.get('Product')}
    
    # Enrich rule_nbr if not present or missing values
    if 'rule_nbr' not in df.columns:
        df['rule_nbr'] = ''
        
    if 'Product' in df.columns:
        mask = df['rule_nbr'].isna() | (df['rule_nbr'].astype(str).str.strip() == '')
        if mask.any():
            df.loc[mask, 'rule_nbr'] = df[mask]['Product'].apply(
                lambda x: rule_map.get(str(x).strip().upper(), '') if pd.notna(x) else ''
            )
            
    # Enrich footnote
    # The existing codebase expects lowercase 'footnote' for downstream usage
    footnote_col = 'footnote' if 'footnote' in df.columns else 'Footnote' if 'Footnote' in df.columns else 'footnote'
    if footnote_col not in df.columns:
        df[footnote_col] = ''
        
    mask = df[footnote_col].isna() | (df[footnote_col].astype(str).str.strip() == '')
    if mask.any():
        df.loc[mask, footnote_col] = df[mask].apply(lambda row: _resolve_footnote(row, footnote_master), axis=1)
        
    # Ensure lowercase 'footnote' exists for validation and DB insertion
    if 'footnote' not in df.columns and 'Footnote' in df.columns:
        df['footnote'] = df['Footnote']

    # Keep both so expected DB headers that rely on capitalizing don't break either
    if 'Footnote' not in df.columns and 'footnote' in df.columns:
        df['Footnote'] = df['footnote']

    # Enrich ndo (downstream validation and usage expects 'ndo')
    if 'ndo' not in df.columns:
        df['ndo'] = ''
        
    if 'ndostart' in df.columns:
        mask = df['ndo'].isna() | (df['ndo'].astype(str).str.strip() == '')
        if mask.any():
            df.loc[mask, 'ndo'] = df[mask]['ndostart'].apply(lambda x: _resolve_ndo_character(x, ndo_master))
    else:
        # Default 'ndo' to 'X' if empty and no ndostart mapping is possible
        df['ndo'] = df['ndo'].replace('', 'X').fillna('X')
        
    return df

def convert_product_upsell_data_types(df):
    """
    Convert product upsell data to proper data types for database storage.
    """
    if df.empty:
        return df
    
    df = df.copy()
    
    # CRITICAL: Ensure rule_nbr is always stored as string
    if 'rule_nbr' in df.columns:
        df['rule_nbr'] = df['rule_nbr'].astype(str)
    
    string_columns = ['sector', 'rbd_start', 'rbd_end', 'footnote', 'ow_rt', 'ndo', 'pos', 
                     'type', 'base_all_in', 'base_footnote', 'base_ndo', 'Product', 'SaleStart',
                     'SaleEnd', 'TravelStart', 'TravelEnd', 'ndostart', 'ndoend']
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).replace('nan', '')
    
    numeric_columns = ['value', 'min_value', 'max_value', 'sort_price']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df

def validate_product_upsell(df, table_name="product_upsell"):
    """
    Validate product upsell data for duplicate keys and ensure proper data types
    """
    if df.empty:
        return df
        
    # First, enrich the dataframe with master mappings (rule_nbr, footnote, ndo)
    df = enrich_upsell_data(df)
    
    df = convert_product_upsell_data_types(df)
    
    # Define the key columns that should be unique
    key_columns = ['rule_nbr', 'sector', 'rbd_start', 'rbd_end', 'footnote', 'ow_rt', 'ndo', 'pos']
    
    # Check if all key columns exist in the dataframe
    missing_columns = [col for col in key_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required derived columns: {missing_columns}")
    
    # Check for duplicate combinations of key columns
    duplicates = df.duplicated(subset=key_columns, keep=False)
    
    if duplicates.any():
        duplicate_rows = df[duplicates].copy()
        duplicate_rows['row_number'] = duplicate_rows.index + 2
        
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
        
    # CRITICAL: Reindex the dataframe to strictly map to the required database table schema.
    # This prevents 'Mismatch between CSV columns (15) and expected headers (16)' errors, 
    # and removes input-only columns like 'Product', 'SaleStart', etc.
    try:
        db_helper = DbHelper()
        columns_str, _ = db_helper.get_column_headers(table_name)
        if columns_str:
            expected_headers = [col.strip() for col in columns_str.split(',')]
            # Temporary fallback logic in case DB holds old headers, we want to ensure new fields are included
            if 'Product' not in expected_headers:
                expected_headers = ['rule_nbr'] + [h for h in expected_headers if h != 'rule_nbr'] + ['Product', 'SaleStart', 'SaleEnd', 'TravelStart', 'TravelEnd', 'ndostart', 'ndoend']
        else:
            expected_headers = ['rule_nbr', 'Product', 'sector', 'rbd_start', 'rbd_end', 'SaleStart', 'SaleEnd', 'TravelStart', 'TravelEnd', 'footnote', 'type', 'value', 'ndostart', 'ndoend', 'base_all_in', 'min_value', 'max_value', 'base_footnote', 'base_ndo', 'ow_rt', 'ndo', 'pos']
    except Exception as e:
        expected_headers = ['rule_nbr', 'Product', 'sector', 'rbd_start', 'rbd_end', 'SaleStart', 'SaleEnd', 'TravelStart', 'TravelEnd', 'footnote', 'type', 'value', 'ndostart', 'ndoend', 'base_all_in', 'min_value', 'max_value', 'base_footnote', 'base_ndo', 'ow_rt', 'ndo', 'pos']
        
    # Ensure all expected headers exist in our dataframe
    for header in expected_headers:
        if header not in df.columns:
            df[header] = np.nan
            
    # Filter and order exactly as expected
    df = df[expected_headers]
    
    return df