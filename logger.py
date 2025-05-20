
import pandas as pd

def save_clusters_before_rename(df: pd.DataFrame, filename: str):
    with open(filename, "w", encoding="utf-8") as f:
        f.write(" Clusters BEFORE renaming (Spec Name → Values):\n\n")
        clustered_rows = df.groupby('cluster')[['fk_im_spec_master_desc', 'fk_im_spec_options_desc']]
        for cluster_id, group in clustered_rows:
            f.write(f"\n Cluster {cluster_id}:\n")
            for _, row in group.iterrows():
                f.write(f" - {row['fk_im_spec_master_desc']} → {row['fk_im_spec_options_desc']}\n")

def save_clusters_after_rename(df: pd.DataFrame, filename: str):
    with open(filename, "a", encoding="utf-8") as f:  # append mode
        f.write("\n Clusters AFTER renaming (new column used):\n\n")
        cluster_groups_after = df.groupby('cluster')[['renamed_spec_master_desc', 'fk_im_spec_options_desc']]
        for cluster_id, group in cluster_groups_after:
            f.write(f"\n Cluster {cluster_id}:\n")
            for _, row in group.iterrows():
                f.write(f" - {row['renamed_spec_master_desc']} → {row['fk_im_spec_options_desc']}\n")
