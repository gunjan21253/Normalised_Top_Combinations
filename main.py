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
from result import extract_top_values , get_column_to_values_map , count_combinations , process_top_combinations , save_combinations_to_txt,get_keys_with_few_values , remove_keys_from_dict
from pprint import pprint
import pandas as pd
import os
import datetime


import time

step_timings = {}
def log_step(message):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Detect start or end
    tokens = message.strip().lower().split()
    if tokens[-1] == "start":
        step_key = " ".join(tokens[:-1])
        step_timings[step_key] = time.time()
        print(f"[{timestamp}] {message}")
    elif tokens[-1] == "end":
        step_key = " ".join(tokens[:-1])
        start_time = step_timings.get(step_key, None)
        if start_time is not None:
            duration = time.time() - start_time
            print(f"[{timestamp}] {message} (Duration: {duration:.2f} seconds)")
            del step_timings[step_key]
        else:
            print(f"[{timestamp}] {message} (No start time recorded)")
    else:
        print(f"[{timestamp}] {message}")



def create_run_output_dir(raw_path, base_dir="run_outputs"):
    # Extract CategoryName from file name
    log_step("Pipeline started.")
    category = os.path.splitext(os.path.basename(raw_path))[0]

    # ISO formatted date (YYYY-MM-DDTHH-MM-SS)
    timestamp = datetime.datetime.now().isoformat(timespec="seconds").replace(":", "-")

    folder_name = f"{category}_{timestamp}"

    output_path = os.path.join(base_dir, folder_name)

    os.makedirs(output_path, exist_ok=True)

    print(f"Created output directory: {output_path}")
    return output_path



def main():
    output_dir_run = create_run_output_dir(RAW_DATA_PATH)
    CLUSTER_LOG_BEFORE = os.path.join(output_dir_run, "cluster_log_before.txt")
    CLUSTER_LOG_AFTER = os.path.join(output_dir_run, "cluster_log_after.txt")
    OUTPUT_TRANSFORMED_CSV = os.path.join(output_dir_run, "transformed_final_output.csv")


    #  Loading
    log_step("Raw_data loading start.")
    df = load_and_clean_data(RAW_DATA_PATH)
    log_step("Raw_data loading end.")




    #  First level clustering (spec name + value)
    log_step("Embedding_1 (master+option) start")
    embeddings_1 = generate_embeddings(df['semantic_text'].tolist(), MODEL_1)
    log_step("Embedding_1 (master+option) end")
    print(f"Embedding 1 Shape: {embeddings_1.shape}")
    cluster_col = 'cluster'
    renamed_col = 'renamed_spec_master_desc'
    log_step("Clustering_1 (master+option) start")
    df[cluster_col] = perform_clustering(embeddings_1, CLUSTER_THRESHOLD_1)
    log_step("Clustering_1 (master+option) end")


    save_clusters_before_rename(df, CLUSTER_LOG_BEFORE)

    df = rename_first_level(df, cluster_col=cluster_col, output_col=renamed_col)

    save_clusters_after_rename(df, CLUSTER_LOG_AFTER)

    # Second level clustering (merged similar spec names)
    unique_names = df[renamed_col].unique().tolist()
    log_step("Embedding_2 (master) start")
    embeddings_2 = generate_embeddings(unique_names, MODEL_2)
    log_step("Embedding_2 (master) end")
    log_step("Clustering_2 (master) start")
    labels_2 = perform_clustering(embeddings_2, CLUSTER_THRESHOLD_2)
    log_step("Clustering_2 (master) end")
    final_col = 'final_df'
    df, mapping = rename_second_level(df, labels_2, unique_names, input_col=renamed_col, output_col=final_col)
    log_step("Renaming & mapping done")
   
    
    pivot_and_save(df, OUTPUT_TRANSFORMED_CSV)
    log_step("transformed_csv1 saved.")

    pivoted_df = pd.read_csv(OUTPUT_TRANSFORMED_CSV)

    # Sort columns by non-NaN count
    
    sorted_df = sort_columns_by_non_nan_count(pivoted_df)
    log_step("sort_columns_by_non_nan_count")
    # Select top 12 columns
    top_12_df = select_top_n_columns(pivoted_df, n=12)
    log_step("selcted top 12 columns")

 
    top_12_df.to_csv(os.path.join(output_dir_run, 'transformed-m-sorted-first12.csv'), index=False)

    input_path = os.path.join(output_dir_run, 'transformed-m-sorted-first12.csv')
    normalized_path = os.path.join(output_dir_run,'normalized_output_2.csv')
    filled_path = os.path.join(output_dir_run,'normalized_output_2_filled.csv')

    # identifying math columns
    df = pd.read_csv(input_path)
    df = df.drop(columns=['pc_item_id']) 


    log_step("Maths columns identification start")
    math_columns = identify_math_columns(df)
    log_step("Maths columns identification end")

    log_step("Cleaning non-numeric col start")
    df = clean_non_math_columns(df, math_columns)
    log_step("Cleaning non-numeric col end")

    log_step("Maths columns normalise start")
    normalized_df = normalize_math_columns(df.copy(), math_columns, normalized_path)
    log_step("Maths columns normalise end")
    #  dimensionless units
    log_step("Maths columns normalise for dimensionless start")
    math_columns_normalised = normalize_and_fill_units(normalized_path, filled_path, math_columns)
    log_step("Maths columns normalise for dimensionless end")

    df = pd.read_csv(filled_path)
    log_step("clustering 3 start")
    cluster_output_dir = os.path.join(output_dir_run, "cluster_outputs")

    cluster_results = cluster_text_columns(df, output_dir=cluster_output_dir)
    log_step("clustering 3 end")

    log_step("sorting inside cluster start")
    output_dir_sorted = os.path.join(output_dir_run, "cluster_outputs_sorted_by_total")

    sort_clusters_by_total(input_dir=cluster_output_dir, output_dir=output_dir_sorted)
    log_step("sorting inside cluster end")


    output_dir_top80 = os.path.join(output_dir_run,"cluster_outputs_top_80_percent" )

    # Retaining top 80% by frequency
    log_step("top 80 per spec start")
    retain_top_80_percent_clusters(output_dir_sorted , output_dir_top80)
    log_step("top 80 per spec end")


    log_step("gini start")
    print("\n Running Gini impurity filter...")
    retained_list = filter_by_gini(output_dir_top80, math_columns_normalised)
    print(f"\n Gini-based non-retained columns: {retained_list}")
    log_step("gini end")

    delete_unqualified_files(output_dir_top80, retained_list)
    log_step("extracting top values start")
    output_csv_path = os.path.join(output_dir_run, 'top_values_per_spec.csv')
    sorted_top_values_dict = extract_top_values(output_dir_top80, output_csv_path)
    log_step("extracting top values end")

    print("\n Sorted Top Values Dictionary:")
    pprint(sorted_top_values_dict)

    log_step("finding assumable and removing start")
    assumable_specs = get_keys_with_few_values(sorted_top_values_dict, max_length=2)
    print(assumable_specs)
    remove_keys_from_dict(sorted_top_values_dict, assumable_specs)

    log_step("finding assumable and removing end")

    print("\n Mapping cluster columns to original dataframe...")
    reference_df = pd.read_csv(filled_path)


    log_step("Mapping start")
    column_to_values = get_column_to_values_map(sorted_top_values_dict, reference_df)
    log_step("Mapping end")
    print(" Columns to be used for combination checking:", list(column_to_values.keys()))
    log_step("top combinations start")
    top_combinations = process_top_combinations(reference_df, column_to_values)
    top_combinations_path = os.path.join(output_dir_run, 'top_feature_combinations.txt')
    save_combinations_to_txt(top_combinations ,top_combinations_path )
    log_step("top combinations end")

if __name__ == '__main__':
    main()
