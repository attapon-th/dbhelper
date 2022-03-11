import pandas as pd


def convert_dtypes(df: pd.DataFrame):
    df = df.convert_dtypes()
    for col in df.columns:
        d = df[col].dtype
        sd = str(d.name).lower()
        if sd == "int64" or sd == "boolean" or 'date' in sd:
            continue
        elif "int" in sd:
            df[col] = df[col].astype(pd.Int64Dtype)
        else:
            df[col] = df[col].astype(pd.StringDtype(storage='pyarrow'))
    return df


def select_column(df: pd.DataFrame, columns: list, raise_not_exists: bool = False) -> pd.DataFrame:
    match_all = True
    df_columns = df.columns.tolist()
    cols = []
    for c in columns:
        if c in df_columns:
            cols.append(c)
        elif raise_not_exists:
            raise Exception(f"Column[{c}] is not exists.")
        else:
            match_all = False
    if match_all:
        return df
    return df[cols]
