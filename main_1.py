
from pathlib import Path
import datetime
import pandas as pd
from pprint import pprint
from config import *
from dataloader import load_and_clean_data
from emb_clustering import generate_embeddings , perform_clustering

from renaming import rename_first_level, rename_second_level
from transform_top12 import pivot_and_save , sort_columns_by_non_nan_count , select_top_n_columns
from logger import save_clusters_before_rename , save_clusters_after_rename
from identification_numeric import identify_math_columns
from numeric_cols import normalize_math_columns , normalize_and_fill_units
from non_numeric import clean_non_math_columns
from clustering3_col import (
    cluster_text_columns,
    sort_clusters_by_total,
    retain_top_80_percent_clusters
)

from filtering_gini import filter_by_gini , delete_unqualified_files
from result import extract_top_values , get_column_to_values_map , count_combinations , process_top_combinations , save_combinations_to_txt
from pprint import pprint
import pandas as pd
import os
import datetime

def log_step(message):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


from pathlib import Path
import datetime

def create_run_output_dir(raw_path, base_dir="run_outputs"):
    # Extract CategoryName from file name
    log_step("Pipeline started.")
    category = Path(raw_path).stem  # gets filename without extension

    # ISO formatted date (YYYY-MM-DDTHH-MM-SS)
    timestamp = datetime.datetime.now().isoformat(timespec="seconds").replace(":", "-")

    folder_name = f"{category}_{timestamp}"

    output_path = Path(base_dir) / folder_name
    output_path.mkdir(parents=True, exist_ok=True)

    print(f"Created output directory: {output_path}")
    return output_path


def main_1():
    log_step("Pipeline started.")
    
    output_dir_run = Path(create_run_output_dir(RAW_DATA_PATH))
    log_step(f"Created output directory: {output_dir_run}")
    print("heelo")
    CLUSTER_LOG_BEFORE = output_dir_run / "cluster_log_before.txt"
    CLUSTER_LOG_AFTER = output_dir_run / "cluster_log_after.txt"
    OUTPUT_TRANSFORMED_CSV = output_dir_run / "transformed_final_output.csv"

    log_step("Loading and cleaning raw data.")
    df = load_and_clean_data(RAW_DATA_PATH)
    log_step(f"Loaded data with shape: {df.shape}")

    log_step("Generating embeddings for first-level clustering.")
    embeddings_1 = generate_embeddings(df['semantic_text'].tolist(), MODEL_1)

    log_step("Performing first-level clustering.")
    cluster_col = 'cluster'
    renamed_col = 'renamed_spec_master_desc'
    df[cluster_col] = perform_clustering(embeddings_1, CLUSTER_THRESHOLD_1)
    log_step(f"First-level clustering completed with {df[cluster_col].nunique()} clusters.")

    save_clusters_before_rename(df, CLUSTER_LOG_BEFORE)
    log_step(f"Saved first-level clusters before renaming to {CLUSTER_LOG_BEFORE}.")

    df = rename_first_level(df, cluster_col=cluster_col, output_col=renamed_col)
    log_step("First-level clusters renamed.")

    save_clusters_after_rename(df, CLUSTER_LOG_AFTER)
    log_step(f"Saved first-level clusters after renaming to {CLUSTER_LOG_AFTER}.")

    log_step("Generating embeddings for second-level clustering.")
    unique_names = df[renamed_col].unique().tolist()
    embeddings_2 = generate_embeddings(unique_names, MODEL_2)

    log_step("Performing second-level clustering.")
    labels_2 = perform_clustering(embeddings_2, CLUSTER_THRESHOLD_2)
    final_col = 'final_df'

    df, mapping = rename_second_level(df, labels_2, unique_names, input_col=renamed_col, output_col=final_col)
    log_step("Second-level clusters renamed and mapping created.")

    log_step("Pivoting data and saving to CSV.")
    pivot_and_save(df, OUTPUT_TRANSFORMED_CSV)
    log_step(f"Pivoted data saved to {OUTPUT_TRANSFORMED_CSV}.")

    log_step("Loading pivoted CSV.")
    pivoted_df = pd.read_csv(OUTPUT_TRANSFORMED_CSV)
    log_step(f"Pivoted CSV loaded with shape: {pivoted_df.shape}")

    log_step("Sorting columns by non-NaN count.")
    sorted_df = sort_columns_by_non_nan_count(pivoted_df)

    log_step("Selecting top 12 columns.")
    top_12_df = select_top_n_columns(pivoted_df, n=12)

    top_12_csv_path = output_dir_run / 'transformed-m-sorted-first12.csv'
    top_12_df.to_csv(top_12_csv_path, index=False)
    log_step(f"Top 12 columns saved to {top_12_csv_path}.")

    input_path = top_12_csv_path
    normalized_path = output_dir_run / 'normalized_output_2.csv'
    filled_path = output_dir_run / 'normalized_output_2_filled.csv'

    log_step(f"Reading top 12 columns CSV for math column identification: {input_path}")
    df = pd.read_csv(input_path)
    if 'pc_item_id' in df.columns:
        df = df.drop(columns=['pc_item_id'])
        log_step("Dropped 'pc_item_id' column.")

    log_step("Identifying math columns.")
    math_columns = identify_math_columns(df)
    log_step(f"Math columns identified: {math_columns}")

    log_step("Cleaning non-math columns.")
    df = clean_non_math_columns(df, math_columns)

    log_step("Normalizing math columns.")
    normalized_df = normalize_math_columns(df.copy(), math_columns, normalized_path)
    log_step(f"Normalized data saved to {normalized_path}.")

    log_step("Normalizing and filling units for math columns.")
    math_columns_normalised = normalize_and_fill_units(normalized_path, filled_path, math_columns)
    log_step(f"Normalization and unit filling completed, saved to {filled_path}")

    df = pd.read_csv(filled_path)

    cluster_output_dir = output_dir_run / "cluster_outputs"
    log_step(f"Starting clustering of text columns, output directory: {cluster_output_dir}")

    cluster_results = cluster_text_columns(df, output_dir=cluster_output_dir)
    log_step("Clustering of text columns completed.")

    output_dir_sorted = output_dir_run / "cluster_outputs_sorted_by_total"
    log_step(f"Sorting clusters by total frequency, output directory: {output_dir_sorted}")
    sort_clusters_by_total(input_dir=cluster_output_dir, output_dir=output_dir_sorted)
    log_step("Clusters sorted by total frequency.")

    output_dir_top80 = output_dir_sorted / "cluster_outputs_top_80_percent"
    log_step(f"Retaining top 80% clusters by frequency, output directory: {output_dir_top80}")
    retain_top_80_percent_clusters(output_dir_sorted , output_dir_top80)
    log_step("Top 80% clusters retained.")

    log_step("Running Gini impurity filter.")
    retained_list = filter_by_gini(output_dir_top80, math_columns_normalised)
    log_step(f"Gini-based non-retained columns: {retained_list}")

    delete_unqualified_files(output_dir_top80, retained_list)
    log_step("Deleted unqualified cluster files.")

    output_csv_path = output_dir_run / 'top_values_per_spec.csv'
    sorted_top_values_dict = extract_top_values(output_dir_top80, output_csv_path)
    log_step(f"Extracted top values saved to {output_csv_path}")

    print("\n Sorted Top Values Dictionary:")
    pprint(sorted_top_values_dict)

    log_step("Mapping cluster columns to original dataframe.")
    reference_df = pd.read_csv(filled_path)

    column_to_values = get_column_to_values_map(sorted_top_values_dict, reference_df)
    log_step(f"Columns to be used for combination checking: {list(column_to_values.keys())}")

    log_step("Processing top feature combinations.")
    top_combinations = process_top_combinations(reference_df, column_to_values)

    top_combinations_path = output_dir_run / 'top_feature_combinations.txt'
    save_combinations_to_txt(top_combinations, top_combinations_path)
    log_step(f"Top feature combinations saved to {top_combinations_path}")

    log_step("Pipeline completed.")

if __name__ == '__main__':
    main_1()
