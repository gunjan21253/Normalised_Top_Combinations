from collections import Counter
from collections import defaultdict
import re 

def rename_first_level(df, cluster_col='cluster', output_col='renamed_spec_master_desc', group_col='fk_im_spec_master_desc'):
    cluster_names = (
        df.groupby(cluster_col)[group_col]
        .agg(lambda x: x.mode().iloc[0])
        .to_dict()
    )
    df[output_col] = df[cluster_col].map(cluster_names)
    return df




def sanitize_name(name):
    """Remove unsafe characters from a name."""
    return re.sub(r'[\\/&*()#@]', '', name)

from collections import defaultdict, Counter

from collections import defaultdict, Counter
import pandas as pd
import os

def sanitize_name(name):
    # Simple placeholder sanitization logic
    return name.strip()

def rename_second_level(
    df,
    cluster_labels,
    original_names,
    input_col='renamed_spec_master_desc',
    output_col='final_df',
    cluster_log_path='second_level_clusters.txt'
):
    cluster_to_specs = defaultdict(list)

    # Group original spec names by their cluster label
    for label, spec in zip(cluster_labels, original_names):
        cluster_to_specs[label].append(spec)

    # Count frequency of each spec across the full DataFrame
    global_counts = Counter(df[input_col])

    representative_map = {}
    with open(cluster_log_path, "w", encoding="utf-8") as f:
        f.write("Clusters and their Representative Names (Based on Global Frequency):\n\n")

        for cluster_id, specs in cluster_to_specs.items():
            # Pick representative with highest global count
            rep = max(specs, key=lambda s: global_counts[s])
            sanitized_rep = sanitize_name(rep)

            for spec in specs:
                representative_map[spec] = sanitized_rep

            f.write(f"Cluster {cluster_id} → Representative: {sanitized_rep}\n")
            for spec in sorted(set(specs)):
                f.write(f"  - {spec} (Global Count: {global_counts[spec]})\n")
            f.write("\n")

    # Map renamed values
    df[output_col] = df[input_col].map(representative_map)
    df.to_csv('2nd_cluster.csv', index=False)

    return df, representative_map
