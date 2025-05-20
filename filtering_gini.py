import os
import pandas as pd
import numpy as np
import re

def extract_counts(series):
    counts = []
    for val in series.dropna().astype(str):
        matches = re.findall(r'\((\d+)\)', val)
        counts.extend([int(m) for m in matches])
    return counts


def gini_from_counts(counts):
    total = sum(counts)
    if total == 0:
        return 0
    probs = np.array(counts) / total
    return 1.0 - np.sum(probs ** 2)


def filter_by_gini(input_dir, math_columns=None, math_thresh=0.4, nonmath_thresh=0.1):
    gini_results = {}
    for fn in os.listdir(input_dir):
        if not fn.endswith('.csv'):
            continue
        path = os.path.join(input_dir, fn)
        df = pd.read_csv(path)

        total_gini = 0
        col_count = 0
        for col in df.columns:
            counts = extract_counts(df[col])
            gini = gini_from_counts(counts)
            total_gini += gini
            col_count += 1
        avg_gini = total_gini / col_count if col_count else 0
        key = fn.replace('_clusters.csv', '')
        gini_results[key] = avg_gini

    new_list = []
    for key, gini_val in gini_results.items():
        if math_columns and key in math_columns:
            if gini_val < math_thresh:
                new_list.append(key)
        else:
            if gini_val < nonmath_thresh:
                new_list.append(key)

    return new_list



def delete_unqualified_files(input_dir, valid_list):
    for fn in os.listdir(input_dir):
        if not fn.endswith('.csv'):
            continue
        key = fn.replace('_clusters.csv', '')
        if key not in valid_list:
            path_to_remove = os.path.join(input_dir, fn)
            os.remove(path_to_remove)
            print(f"→ Deleted: {path_to_remove}")