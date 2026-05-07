import pandas as pd

single_rbd_map = {
    "Q": "QA", "O": "O0", "N": "N0", "M": "M0",
    "K": "K0", "J": "J0", "I": "I0", "H": "H0",
    "F": "F0", "E": "E0", "D": "D0", "C": "C0",
    "B": "B0", "Y": "Y0"
}

def format_saver_file(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if 'rbd' not in df.columns:
        raise KeyError("Column 'rbd' not found in DataFrame.")

    isna = df['rbd'].isna()
    s = df['rbd'].astype(str)

    mask_single = s.str.len() == 1
    mapped = s.where(~mask_single, s.map(single_rbd_map)).fillna(s)

    mapped = mapped.mask(isna)

    df['rbd'] = mapped
    
    return df