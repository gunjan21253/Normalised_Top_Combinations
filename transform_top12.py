
def pivot_and_save(
    df,
    output_path,
    drop_columns=None,
    index_col='pc_item_id',
    columns_col='final_df',
    values_col='fk_im_spec_options_desc',
    extra_drop_columns=None
):
    # Determine generic columns to keep
    required_columns = {index_col, columns_col, values_col}
    inferred_drop_columns = [col for col in df.columns if col not in required_columns]

    # Combine inferred with any user-specified extra drops
    if extra_drop_columns:
        inferred_drop_columns += extra_drop_columns
        inferred_drop_columns = list(set(inferred_drop_columns))  # ensure no duplicates

    df = df.drop(columns=inferred_drop_columns)

    pivoted_df = df.pivot_table(
        index=[index_col],
        columns=columns_col,
        values=values_col,
        aggfunc='first'
    ).reset_index()

    pivoted_df.columns.name = None
    pivoted_df.columns = [str(col) for col in pivoted_df.columns]
    pivoted_df.to_csv(output_path, index=False)




def sort_columns_by_non_nan_count(df, index_col='pc_item_id'):
    """
    Returns a DataFrame with columns sorted (excluding index_col)
    by descending count of non-NaN values.
    """
    non_index_cols = [col for col in df.columns if col != index_col]
    sorted_cols = sorted(non_index_cols, key=lambda c: df[c].notna().sum(), reverse=True)
    return df[[index_col] + sorted_cols]


def select_top_n_columns(df, n=12, index_col='pc_item_id'):
    """
    Returns a DataFrame with index_col + top-n non-NaN count columns.
    """
    non_index_cols = [col for col in df.columns if col != index_col]
    top_cols = sorted(non_index_cols, key=lambda c: df[c].notna().sum(), reverse=True)[:n]
    return df[[index_col] + top_cols]

