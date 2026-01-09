# Normalised Top Combinations

A small Python project to generate **top feature/specification combinations** from a dataset which contains unstructured data and apply **normalization** steps (e.g., renaming, numeric vs non-numeric handling) before exporting results.

> Repo includes sample inputs (CSV) and pre-generated combination outputs (`4k/10k/20k_top_feature_combinations.txt`).

---

## What’s inside

Key files/scripts (high level):

- `main.py`, `main_1.py` — main entry scripts to run the pipeline
- `config.py` — configuration (input path, output path, run parameters, etc.)
- `dataloader.py` — loading + basic preprocessing for CSV
- `renaming.py` — normalization/renaming utilities
- `numeric_cols.py`, `identification_numeric.py` — numeric column detection/handling
- `non_numeric.py` — handling non-numeric/text columns
- `filtering_gini.py` — optional filtering/scoring step (name suggests Gini-based filtering)
- `emb_clustering.py`, `clustering3_col.py` — optional clustering utilities (name suggests embedding-based + 3-column clustering)
- `transform_top12.py` — transforms/exports for “top-N” outputs (name suggests top-12 formatting)
- `result.py` — result writing/aggregation

Sample data / outputs:
- `new2.csv`
- `sample_datapoints_polycab.csv`
- `4k_top_feature_combinations.txt`
- `10k_top_feature_combinations.txt`
- `20k_top_feature_combinations.txt`

---

## Setup

### 1) Create environment
This repo includes `requirements.txt` and also `pyproject.toml` + `uv.lock`.

**Option A — pip**
```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# Mac/Linux: source .venv/bin/activate

pip install -r requirements.txt
