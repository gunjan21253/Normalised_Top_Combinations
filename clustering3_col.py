
import os
import re
import pandas as pd
from sklearn.cluster import AgglomerativeClustering
from sentence_transformers import SentenceTransformer

DIST_THRESHOLD = 0.8

def total_count(col_series: pd.Series) -> int:
    nums = (
        col_series
        .dropna()
        .astype(str)
        .str.extractall(r'\((\d+)\)')[0]
        .astype(int)
    )
    return nums.sum()

def cluster_text_columns(df, output_dir="cluster_outputs"):
    os.makedirs(output_dir, exist_ok=True)
    model = SentenceTransformer('thenlper/gte-base')
    cluster_results = {}

    for col in df.columns:
        print(f"\n=== Processing column: {col} ===")
        values = df[col].dropna().astype(str).values
        if len(values) == 0:
            print("No non-null values to cluster.")
            continue

        embeddings = model.encode(values)
        clustering = AgglomerativeClustering(
            n_clusters=None,
            distance_threshold=DIST_THRESHOLD,
            linkage='ward'
        )
        labels = clustering.fit_predict(embeddings)

        df_clustered = pd.DataFrame({'Value': values, 'Cluster': labels})
        cluster_results[col] = df_clustered

        # Output cluster CSV
        cluster_dict = {}
        for cluster_id in sorted(df_clustered['Cluster'].unique()):
            cluster_data = df_clustered[df_clustered['Cluster'] == cluster_id]
            value_counts = cluster_data['Value'].value_counts()
            formatted_values = [f"{val} ({count})" for val, count in value_counts.items()]
            cluster_dict[f'Cluster {cluster_id}'] = formatted_values

        df_out = pd.DataFrame(dict([(k, pd.Series(v)) for k, v in cluster_dict.items()]))
        df_out.to_csv(f"{output_dir}/{col}_clusters.csv", index=False)

        print(f"→ Saved: {output_dir}/{col}_clusters.csv")

    return cluster_results

def sort_clusters_by_total(input_dir, output_dir='cluster_outputs_sorted_by_total'):
    os.makedirs(output_dir, exist_ok=True)

    for fn in os.listdir(input_dir):
        if not fn.endswith('_clusters.csv'):
            continue

        path_in = os.path.join(input_dir, fn)
        path_out = os.path.join(output_dir, fn)
        df = pd.read_csv(path_in)

        totals = {col: total_count(df[col]) for col in df.columns}
        sorted_cols = sorted(totals, key=lambda c: totals[c], reverse=True)

        df[sorted_cols].to_csv(path_out, index=False)
        print(f"→ Sorted saved: {path_out}")

def retain_top_80_percent_clusters(input_dir, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    for fn in os.listdir(input_dir):
        if not fn.endswith('_clusters.csv'):
            continue

        path_in = os.path.join(input_dir, fn)
        path_out = os.path.join(output_dir, fn)
        df = pd.read_csv(path_in)

        totals = {col: total_count(df[col]) for col in df.columns}
        sorted_cols = sorted(totals, key=lambda c: totals[c], reverse=True)
        total_sum = sum(totals.values())

        cumulative = 0
        filtered_cols = []
        for col in sorted_cols:
            cumulative += totals[col]
            filtered_cols.append(col)
            if cumulative >= 0.8 * total_sum:
                break

        df_filtered = df[filtered_cols]
        df_filtered.to_csv(path_out, index=False)

        print(f"\n→ {fn}:")
        print(f"   Total Clusters: {len(df.columns)} | Retained: {len(filtered_cols)} | Saved: {path_out}")
