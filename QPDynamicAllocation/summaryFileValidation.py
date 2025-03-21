import sys
import pandas as pd
import numpy as np

def compare_csv_files():
    # Load the CSV files into DataFrames
    df1 = pd.read_csv(f"{sys.path[0]}/output/summary.csv")
    df2 = pd.read_csv(f"{sys.path[0]}/output/archive/summary.csv")
    df3 = df2
    df1 = df1.drop(columns=['Id'])
    df2 = df2.drop(columns=['Id'])
    df2 = df2.drop(columns=['marketFaresSummary'])
    df2 = df2.drop(columns=['bookingsToday'])
    
    column_names=df1.columns
    df2 = df2[column_names]
    
    # Compare row and column counts
    # if df1.shape != df2.shape:
    #     print("Row and/or column count mismatch:")
    #     print(f"File 1: {df1.shape}, File 2: {df2.shape}")
    #     return

    # Compare each value in the DataFrames
    mismatches = []
    for row_index, (row1, row2) in enumerate(zip(df1.values, df2.values), start=1):
        for col_index, (val1, val2) in enumerate(zip(row1, row2), start=1):
            if pd.isna(val1) or pd.isna(val2):
                val1="nan"
                val2="nan"

            if isinstance(val1, str) and isinstance(val2, str):
                if val1 != val2:
                    column_name = column_names[col_index - 1]  
                    mismatches.append((row_index, column_name, val1, val2))
            # Handling floating-point numbers
            elif isinstance(val1, (float, np.floating)) and isinstance(val2, (float, np.floating)):
                if round(val1)!=round(val2):
                    column_name = column_names[col_index - 1]  
                    mismatches.append((row_index, column_name, val1, val2))

    print(row_index)
    mismatches=set(mismatches)
    df5 = pd.DataFrame()
    # Print any mismatches found
    if mismatches:
        print("Value mismatches:")
        for row, col, val1, val2 in mismatches:
            print(f"Row {row}, Column '{col}': {val1} != {val2}")
            k=df3.iloc[row].to_dict()
            df4=pd.DataFrame([k])
            df5=pd.concat([df5,df4],ignore_index=True)
    else:
        print("No mismatches found.")
    
    # print(df5)  
    df5.to_csv(f"{sys.path[0]}/test/test.csv", index=False)
    print("Converted")
compare_csv_files()