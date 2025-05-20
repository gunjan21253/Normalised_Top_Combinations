
import pandas as pd

def load_and_clean_data(path):
    df = pd.read_csv(path)
    df = df.dropna(subset=['fk_im_spec_master_desc', 'fk_im_spec_options_desc', 'pc_item_name'])
    df['semantic_text'] = (
        df['fk_im_spec_master_desc'].str.lower().str.strip() + ": " +
        df['fk_im_spec_options_desc'].str.lower().str.strip()
    )
    return df
