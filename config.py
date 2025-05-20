
# config.py
RAW_DATA_PATH = 'sample_datapoints_polycab.csv'
OUTPUT_TRANSFORMED_CSV = 'transformed_final_output.csv'

# INPUT_DIR = os.path.join(output_dir_sorted,"cluster_outputs_top_80_percent" )
# NORMALIZED_CSV_PATH = 'normalized_output_2_filled.csv'
GINI_THRESHOLDS = {'math': 0.4, 'other': 0.1}

MODEL_1 = 'all-MiniLM-L6-v2'
MODEL_2 = 'thenlper/gte-base'

CLUSTER_THRESHOLD_1 = 1.4
CLUSTER_THRESHOLD_2 = 0.55

# CLUSTER_LOG_BEFORE = "cluster_log_before.txt"
# CLUSTER_LOG_AFTER = "cluster_log_after.txt"