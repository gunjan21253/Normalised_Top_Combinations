# Normalised Top Combinations

A small Python project that reads a CSV dataset which conatin unstructured data , performs normalization/cleanup, and generates **top feature/specification combinations** as text outputs (e.g., `4k_top_feature_combinations.txt`, `10k_top_feature_combinations.txt`, `20k_top_feature_combinations.txt`).

---

## Repository contents (high level)

Commonly used files:
- `main.py` / `main_1.py` — entry points to run the pipeline
- `config.py` — configuration for input/output + run limits
- `dataloader.py` — CSV load + preprocessing
- `renaming.py` — renaming / normalization helpers
- `numeric_cols.py`, `identification_numeric.py` — numeric detection/handling
- `non_numeric.py` — non-numeric/text handling
- `filtering_gini.py` — optional filtering/scoring stage
- `emb_clustering.py`, `clustering3_col.py` — optional clustering utilities
- `transform_top12.py` — formatting/export helpers
- `result.py` — result writing/aggregation

Sample inputs / example outputs:
- `sample_datapoints_polycab.csv`, `new2.csv`
- `4k_top_feature_combinations.txt`
- `10k_top_feature_combinations.txt`
- `20k_top_feature_combinations.txt`

---

## Setup

This repo includes `requirements.txt` and also `pyproject.toml` + `uv.lock`.

### Option A — pip
```bash
python -m venv .venv
# Windows:
#   .venv\Scripts\activate
# Mac/Linux:
#   source .venv/bin/activate

pip install -r requirements.txt
```

### Option B — uv
```bash
uv sync
```

---

## Configure input/output

Open `config.py` and set/update:
- **Input CSV path**
- **Output directory / output filenames**
- Any run parameters (examples: top-K limits, thresholds, filters, etc.)

---

## Run

Typical run:
```bash
python main.py
```

If your workflow uses the alternate entry:
```bash
python main_1.py
```

---

## Expected outputs

Depending on your configuration and the entry script, the pipeline will write one or more output files similar to:

- `4k_top_feature_combinations.txt`
- `10k_top_feature_combinations.txt`
- `20k_top_feature_combinations.txt`

(Example output files are already present in the repo.)

---

## Notes / Tips

- Start with `sample_datapoints_polycab.csv` to validate the setup end-to-end.
- If your CSV schema differs, update the loader/renaming logic accordingly.
- If embedding/clustering modules are enabled, ensure the required dependencies are installed.

---

## Contributing

PRs welcome. Helpful improvements:
- Add a CLI (`argparse` / `typer`) so users don’t edit `config.py`
- Add unit tests
- Add a clear input schema section + example output format

---

## License

No LICENSE file is included yet.
