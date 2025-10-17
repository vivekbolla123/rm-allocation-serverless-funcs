import pandas as pd

def pivot_sale_fares(df):
    # Number of static columns in CSV (columns before departure dates)
    num_static_cols = 5
    
    # Extract the header rows (departure dates)
    dept_start_dates = df.columns[num_static_cols:].tolist()
    dept_end_dates = df.iloc[0, num_static_cols:].tolist()
    
    # Extract data rows (skip first 1 header rows)
    data_df = df.iloc[1:].reset_index(drop=True)
    
    # Use position-based indexing to get static columns
    # Positions: 0=Sector, 1=Type, 2=Currency, 3=SaleStart, 4=SaleEnd
    data_df_numeric = pd.DataFrame(
        data_df.iloc[:, :num_static_cols].values,
        columns=['sector', 'type', 'currency', 'sale_start', 'sale_end']
    )
    
    # Create sale_id 
    data_df_numeric['sale_id'] = data_df_numeric.apply(
        lambda row: _generate_sale_id(row['sector'], row['sale_start'], row['sale_end']),
        axis=1
    )
    data_df_numeric.dropna(subset=['sector', 'type', 'sale_start', 'sale_end'], how='all', inplace=True)
    
    # TABLE 1: Sales
    sales_df = data_df_numeric[['sale_id', 'sector', 'type', 'currency', 'sale_start', 'sale_end']].drop_duplicates().reset_index(drop=True)
    
    # TABLE 2: Discounts
    # Get discount values from positions num_static_cols onwards
    discount_values = data_df.iloc[:, num_static_cols:].values
    
    # Create flat discount records
    discounts_data = []
    for row_idx, sale_id in enumerate(data_df_numeric['sale_id']):
        for col_idx, dept_start in enumerate(dept_start_dates):
            discounts_data.append({
                'sale_id': sale_id,
                'departure_start': dept_start,
                'departure_end': dept_end_dates[col_idx],
                'discount': discount_values[row_idx, col_idx].rstrip('%')
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