


import pandas as pd
import re

# Regex to match values like "2m", "5 kg", "3.5cm", "10inch"
pattern = re.compile(r'^\s*\d+(\.\d+)?\s*([a-zA-Z/^\d]+)\s*$')

def column_is_mostly_math(df, col, threshold=0.6):
    series = df[col].dropna().astype(str)
    if len(series) == 0:
        return False
    match_count = series.apply(lambda x: bool(pattern.fullmatch(x.strip()))).sum()
    proportion = match_count / len(series)
    return proportion >= threshold

def identify_math_columns(df, threshold=0.6):
    math_columns = [col for col in df.columns if column_is_mostly_math(df, col, threshold)]
    print("Likely mathematical columns:", math_columns)
    return math_columns
