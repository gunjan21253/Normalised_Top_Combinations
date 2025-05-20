


import pandas as pd

def clean_text_cell(x):
    if pd.isna(x):
        return x
    return str(x).lower().replace("-", " ").strip()

def clean_non_math_columns(df, math_columns):
    cleaned_df = df.copy()
    for col in cleaned_df.columns:
        if col not in math_columns:
            cleaned_df[col] = cleaned_df[col].apply(clean_text_cell)
    return cleaned_df
