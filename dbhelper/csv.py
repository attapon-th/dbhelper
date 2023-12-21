import os
from typing import Iterator, Union, Literal, Optional
import time
import shutil
import pandas as pd
import pyarrow as pa


def to_csv(
    engine,
    sql_query: str,
    file_output: str,
    chunksize: int = 10000,
    echo: bool = False,
) -> int:
    """
    Generates a CSV file from a SQL query and saves it to the specified file path.

    Args:
        engine (Any): The database engine to execute the SQL query.
        sql_query (str): The SQL query to execute.
        file_output (str): The file path to save the CSV file to.
        chunksize (int, optional): The number of rows to process at a time. Defaults to 10000.
        echo (bool, optional): Whether to display print statements. Defaults to False.

    Returns:
        int: The total number of rows processed.

    Raises:
        Exception: If there was an error executing the SQL query or saving the CSV file.
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
            dtype=pd.ArrowDtype(pa.string()),
            dtype_backend="pyarrow",
        )
        func_print("End Query SqlStantement")
        func_print("Start CSV Write")
        total_count = 0
        is_header = True
        for df in idf:
            if isinstance(df, pd.DataFrame):
                total_count = df.shape[0]
            if df.empty:
                continue
            df.to_csv(
                temp_file,
                mode="a",
                header=is_header,
                index=False,
                errors="ignore",
            )
            is_header = False

        func_print("End CSV Write")

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


def read_csv(
    filename: str,
    delimiter: str = ",",
    dtype: Union[dict, pd.ArrowDtype, None] = pd.ArrowDtype(pa.string()),
    engine: Literal["c", "python", "pyarrow", "python-fwf"] = "pyarrow",
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = "pyarrow",
    on_bad_lines: Literal["error", "warn", "skip"] = "warn",
    nrows: Optional[int] = None,
    chunksize: Optional[int] = None,
    low_memory: bool = False,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """
    Reads a CSV file and returns its contents as a pandas DataFrame or an iterator of DataFrames.

    Parameters:
        filename (str): The path to the CSV file.
        delimiter (str, optional): The delimiter used in the CSV file. Defaults to ",".
        dtype (Union[dict, pd.ArrowDtype, None], optional): The data type of the columns in the DataFrame. Defaults to pd.ArrowDtype(pa.string()).
        engine (Literal["c", "python", "pyarrow", "python-fwf"], optional): The engine used to read the CSV file. Defaults to "pyarrow".
        dtype_backend (Literal["numpy_nullable", "pyarrow"], optional): The backend library used for dtype conversion. Defaults to "pyarrow".
        on_bad_lines (Literal["error", "warn", "skip"], optional): The action to take when encountering bad lines in the CSV file. Defaults to "warn".
        nrows (Optional[int], optional): The maximum number of rows to read from the CSV file. Defaults to None.
        chunksize (Optional[int], optional): The number of rows to read at a time when reading the CSV file in chunks. Defaults to None.
        low_memory (bool, optional): Whether to allocate the DataFrame memory incrementally. Defaults to False.

    Returns:
        Union[pd.DataFrame, Iterator[pd.DataFrame]]: The contents of the CSV file as a pandas DataFrame or an iterator of DataFrames.
    """
    if chunksize is not None and chunksize > 0:
        return pd.read_csv(
            filename,
            delimiter=delimiter,
            dtype=dtype,
            engine=engine,
            dtype_backend=dtype_backend,
            on_bad_lines=on_bad_lines,
            chunksize=chunksize,
            low_memory=low_memory,
        )
    else:
        return pd.read_csv(
            filename,
            delimiter=delimiter,
            dtype=dtype,
            engine=engine,
            dtype_backend=dtype_backend,
            on_bad_lines=on_bad_lines,
            nrows=nrows,
            low_memory=low_memory,
        )
