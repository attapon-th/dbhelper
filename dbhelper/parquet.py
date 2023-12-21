import os
import time
import shutil
import pandas as pd
import pyarrow as pa
from pyarrow.parquet import ParquetWriter
from typing import Literal, Optional, Union, Iterator, List


def to_parquet(
    engine,
    sql_query: str,
    file_output: str,
    chunksize: int = 10000,
    compression: Literal["snappy", "gzip", "brotli"] = "snappy",
    echo: bool = False,
) -> int:
    """
    Generates a Parquet file from a SQL query and saves it to the specified file path.

    Args:
        engine (Any): The database engine to execute the SQL query.
        sql_query (str): The SQL query to execute.
        file_output (str): The file path to save the Parquet file to.
        chunksize (int, optional): The number of rows to process at a time. Defaults to 10000.
        echo (bool, optional): Whether to display print statements. Defaults to False.

    Returns:
        int: The total number of rows processed.

    Raises:
        Exception: If there was an error executing the SQL query or saving the Parquet file.
    """

    def func_print(*v, **k):
        if echo is not True:
            pass
        print(*v, **k)

    total_count = -1
    dir_temp: str = "./temp_" + str(time.time()).replace(".", "")
    temp_file = os.path.join(dir_temp, str(time.time()))
    os.makedirs(dir_temp, exist_ok=True)
    func_print("Start Query SqlStantement")
    try:
        idf = pd.read_sql(
            sql_query,
            engine,
            coerce_float=True,
            chunksize=chunksize,
            dtype_backend="pyarrow",
        )
        func_print("End Query SqlStantement")
        func_print("Start Parquet Write")
        total_count = 0
        df = next(idf)
        if not isinstance(df, pd.DataFrame) or df.empty:
            func_print("Empty Data.")
            return total_count

        table = pa.Table.form_pandas(df, preserve_index=False)
        pqWriter = ParquetWriter(
            temp_file, schema=table.schema, compression=compression
        )
        pqWriter.write_table(table)
        for df in idf:
            if isinstance(df, pd.DataFrame):
                total_count = df.shape[0]
            if df.empty:
                continue
            pqWriter.write_table(pa.Table.from_pandas(df))

        func_print("End Parquet Write")

        func_print("Move file.")
        # ยายไฟล์จาก temfile
        os.rename(temp_file, file_output)
        func_print("End.")
    except Exception as ex:
        raise ex
    finally:
        if os.path.exists(dir_temp):
            shutil.rmtree(dir_temp, ignore_errors=True)
    return total_count


def read_parquet(
    filename: str,
    columns: Optional[List[str]] = None,
    chunksize: Optional[int] = None,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """
    Reads a Parquet file and returns its contents as a pandas DataFrame or an iterator of DataFrames.

    Parameters:
        filename (str): The path to the Parquet file.
        columns (Optional[List[str]], optional): The columns to read from the Parquet file. Defaults to None (read all columns).
        chunksize (Optional[int], optional): The number of rows to read at a time when reading the Parquet file in chunks. Defaults to None (read the entire file).

    Returns:
        Union[pd.DataFrame, Iterator[pd.DataFrame]]: The contents of the Parquet file as a pandas DataFrame or an iterator of DataFrames.
    """
    if chunksize is not None and chunksize > 0:
        return pd.read_parquet(
            filename,
            columns=columns,
            chunksize=chunksize,
            engine="pyarrow",
            dtype_backend="pyarrow",
        )
    else:
        return pd.read_parquet(
            filename,
            columns=columns,
            engine="pyarrow",
            dtype_backend="pyarrow",
        )
