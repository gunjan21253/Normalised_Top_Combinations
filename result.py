import itertools
import os
import re
import pandas as pd
import numpy as np
from pprint import pprint
import json
import os
import re
import pandas as pd

def extract_top_values(input_dir, output_csv_path = 'top_values_per_spec.csv'):
    top_values_dict = {}
    # To build a dataframe for CSV output: keys = filenames, rows = top value + count per column
    csv_data = {}

    for filename in os.listdir(input_dir):
        if not filename.endswith('_clusters.csv'):
            continue
        filepath = os.path.join(input_dir, filename)
        df = pd.read_csv(filepath)

        top_values = []
        top_values_with_counts = []

        for col in df.columns:
            # Get the mode (most frequent value) with count:
            mode_value = df[col].mode().iloc[0]

            # Extract count from the string like 'automatic (293)'
            match = re.search(r'\((\d+)\)', mode_value)
            count = int(match.group(1)) if match else None

            # Clean top value by removing the count part
            top_value_clean = re.sub(r'\s*\(\d+\)', '', mode_value)

            top_values.append(top_value_clean)

            # Save "value (count)" string for CSV, if count is missing just value
            if count is not None:
                top_values_with_counts.append(f"{top_value_clean} ({count})")
            else:
                top_values_with_counts.append(top_value_clean)

        top_values_dict[filename] = top_values
        csv_data[filename] = top_values_with_counts

   
    max_rows = max(len(v) for v in csv_data.values())
    for filename in csv_data:
       
        csv_data[filename] += [''] * (max_rows - len(csv_data[filename]))

    csv_df = pd.DataFrame(csv_data)
    csv_df.to_csv(output_csv_path, index=False)

   
    return dict(sorted(top_values_dict.items(), key=lambda item: len(item[1]), reverse=True))

def get_keys_with_few_values(data_dict, max_length=2):
    """
    Returns a list of keys from the dictionary whose value lists
    have length less than or equal to max_length.

    Parameters:
    - data_dict (dict): Dictionary with keys and list-type values.
    - max_length (int): Maximum allowed length of the value list.

    Returns:
    - List of keys meeting the condition.
    """
    return [key for key, value in data_dict.items() if len(value) <= max_length]

def remove_keys_from_dict(data_dict, keys_to_remove):
    for key in keys_to_remove:
        data_dict.pop(key, None)  


def get_column_to_values_map(sorted_top_values_dict, reference_df):
    column_to_values = {}
    for key, values in sorted_top_values_dict.items():
        col_name = key.replace('_clusters.csv', '')
        if col_name in reference_df.columns:
            column_to_values[col_name] = values
        else:
            print(f" Column '{col_name}' not found in DataFrame. Skipping.")
    return column_to_values



# def count_combinations(df, column_to_values, combo_length):
#     results = []
#     feature_combos = list(itertools.combinations(column_to_values.keys(), combo_length))
#     for features in feature_combos:
#         value_lists = [column_to_values[f] for f in features]
#         value_combos = itertools.product(*value_lists)
#         for values in value_combos:
#             condition = (df[features[0]] == values[0])
#             for f, v in zip(features[1:], values[1:]):
#                 condition &= (df[f] == v)
#             count = condition.sum()
#             if count > 0:
#                 results.append({'features': features, 'values': values, 'count': count})
#     return results


def count_combinations(df, column_to_values, combo_length):
    results = []
    feature_combos = list(itertools.combinations(column_to_values.keys(), combo_length))
    for features in feature_combos:
        value_lists = [column_to_values[f] for f in features]
        value_combos = itertools.product(*value_lists)
        for values in value_combos:
            condition = (df[features[0]] == values[0])
            for f, v in zip(features[1:], values[1:]):
                condition &= (df[f] == v)
            count = condition.sum()
            if count > 0:
                results.append({'features': features, 'values': values, 'count': count})
    return results


def process_top_combinations(reference_df, column_to_values, min_len=2, max_len=4, top_n=20):
    all_top_combinations = []
    print("\n Counting combinations from length 3 to 4...")

    for k in range(min_len, max_len + 1):
        print(f"\n Processing {k}-feature combinations...")
        result_k = count_combinations(reference_df, column_to_values, combo_length=k)

        if not result_k:
            print(f" No combinations found for length {k}.")
            continue

        top_k = sorted(result_k, key=lambda x: x['count'], reverse=True)[:top_n]

        print(f"\n Top {top_n} {k}-feature combinations with highest counts:")
        for r in top_k:
            print(f"Features: {r['features']}")
            print(f"Values: {r['values']}")
            print(f"Count: {r['count']}")
            print("-" * 40)

            all_top_combinations.append({
                'features': list(r['features']),
                'values': list(r['values']),
                'count': r['count'],
                'length': k
            })
    return all_top_combinations

def save_combinations_to_txt(combinations, output_path):
    with open(output_path, 'w') as f:
        for combo in combinations:
            features = ', '.join(combo['features'])
            values = ', '.join(combo['values'])
            count = int(combo['count'])  # convert to native int
            f.write(f"Features: {features}\n")
            f.write(f"Values: {values}\n")
            f.write(f"Count: {count}\n")
            f.write("-" * 40 + "\n")
    print(f"\n Top combinations saved to: {output_path}")



