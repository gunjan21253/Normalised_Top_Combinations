
# normalize_units_quantulum.py

import pandas as pd
from quantulum3 import parser
from collections import Counter
import re 
from quantulum3.load import add_custom_unit

# custom_units = [ 
#     {name: "square millimetre", 
#      surfaces:["sqmm", "sq mm", "sq.mm", "sq.mm.", "Sqmm", "Sqmm.", "Sq. mm", "SQMM"], 
#      entity:"area"} , 
#     {name: "metre" , 
#      surface: ["Mtrs." , "Mtrs" , "mtr" , "mtrs"] , 
#      entity: 'length'}
#     ]

# for unit in custom_units:
#     add_custom_unit(**unit)

# from quantulum3.units import add_custom_unit

# List of custom units
custom_units = [
    {
        "name": "square millimetre",
        "surfaces": ["sqmm", "sq mm", "sq.mm", "sq.mm.", "Sqmm", "Sqmm.", "Sq. mm", "SQMM"],
        "entity": "area",
    },
    {
        "name": "meter",
        "surfaces": ["Mtrs." , "Mtrs" , "mtr" , "mtrs"],
        "entity": "length"
    }
  
]

# Add custom units to quantulum3
for unit in custom_units:
    add_custom_unit(
        name=unit["name"],
        surfaces=unit["surfaces"],
        entity=unit["entity"]
    )


# add_custom_unit(
#     name="square millimetre",
#     surfaces=["sqmm", "sq mm", "sq.mm", "sq.mm.", "Sqmm", "Sqmm.", "Sq. mm", "SQMM"],
#     entity="area",
# )

def normalize_rate_1(text):
    if pd.isnull(text):
        return None
    try:
        quantities = parser.parse(str(text))
        if len(quantities) == 2:
            num, denom = quantities[0], quantities[1]
            if denom.value == 0:
                return None
            rate = num.value / denom.value
            return f"{rate:.4f} {num.unit.name}/{denom.unit.name}"
        elif len(quantities) == 1:
            q = quantities[0]
            return f"{q.value} {q.unit.name}"
        else:
            return None
    except Exception as e:
        print(f"Error parsing '{text}': {e}")
        return None



# def normalize_unit(text):
#     """
#     Normalize the unit part of a measurement string (e.g., "1.5 Sq.mm" or "1.5sqmm" → "1.5 sqmm").
#     Preserves the number, cleans the unit.
#     """
#     text = text.strip()
    
#     # Match a number (int or float), optionally followed by space, and then the unit
#     match = re.match(r'^([+-]?[\d.,]+)\s*([a-zA-Z°μΩ/\^.\s\d]+)$', text)
    
#     if match:
#         number, unit = match.groups()
#         cleaned_unit = re.sub(r'[\s\.]', '', unit).lower()
#         return f"{number} {cleaned_unit}"
    
#     # Try matching merged number+unit like "1.5sqmm"
#     match = re.match(r'^([+-]?[\d.,]+)([a-zA-Z°μΩ/\^.\s\d]+)$', text)
    
#     if match:
#         number, unit = match.groups()
#         cleaned_unit = re.sub(r'[\s\.]', '', unit).lower()
#         return f"{number} {cleaned_unit}"
    
#     return text  # if no match, return unchanged

def normalize_math_columns(df, math_columns, output_csv_path):
    for col in math_columns:
        # df[f"{col}_normalized"] = df[col].apply(normalize_unit)
        df[f"{col}_normalized"] = df[col].apply(normalize_rate_1)
    df = df.drop(columns = math_columns)

    
    df.to_csv(output_csv_path, index=False)
    print(f"Normalization complete. Saved to '{output_csv_path}'")
    return df



def normalize_and_fill_units(input_csv_path, output_csv_path, original_math_columns):
    df = pd.read_csv(input_csv_path)
    math_columns = [col + '_normalized' for col in original_math_columns]

    for col in math_columns:
        if col not in df.columns:
            continue

        units = df[col].dropna().astype(str).apply(lambda x: x.split()[-1] if ' ' in x else 'dimensionless')
        non_dimensionless_units = [u for u in units if u != 'dimensionless']
        if not non_dimensionless_units:
            continue

        most_common_unit = Counter(non_dimensionless_units).most_common(1)[0][0]

        def replace_if_dimensionless(cell):
            if pd.isna(cell):
                return cell
            parts = str(cell).split()
            if len(parts) == 2 and parts[1] == 'dimensionless':
                return f"{parts[0]} {most_common_unit}"
            return cell

        df[col] = df[col].apply(replace_if_dimensionless)

    df.to_csv(output_csv_path, index=False)
    print(f"Filled dimensionless values. Saved to '{output_csv_path}'")
    return math_columns
















# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "quantulum3[classifier]",
#     "setuptools",
# ]
# ///

# Add custom unit definition for square millimetre


# INPUTS = [
#     # Original
#     "9 sqmm",
#     "1.93 sq mm",
#     "1.93 sq.mm",
#     "1.93 sq.mm.",
#     # From user data & variations
#     "1.5 sqmm",
#     "2.5 sqmm",
#     "4 sqmm",
#     "0.75 sqmm",
#     "0.5 sqmm",
#     "6 sqmm",
#     "0.1sqmm",
#     "10sqmm",
#     "1.5 sq mm",
#     "0.75 sq mm",
#     "4 Sq mm",
#     "1 Sq. mm",
#     "2.5Sqmm",
#     "0.5SQMM",
#     "1SQMM",
#     "1 Sqmm.",
#     "1.0 sq. mm",
#     "1.0 Sq. mm",
#     "0.75 - 6.0 sqmm",
#     "text with 2.5 sqmm inside",
#     "approx 5 sq.mm.",
# ]

# for input_text in INPUTS:
#     # Print the input and the result
#     print(input_text, parser.parse(input_text))