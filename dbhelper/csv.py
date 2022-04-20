import pandas as pd
from . import dataframe as dh
import os
import sys
from typing import List, Dict, Callable, Iterator
import time
import gzip
import shutil
import csv
import zipfile
import io

CSV_COMPRESSION_NONE = "plain"
CSV_COMPRESSION_GZIP = "gzip"
CSV_COMPRESSION_ZIP = "zip"


def to_csv(engine, sql_query: str, file_name: os.PathLike, compression=CSV_COMPRESSION_GZIP,  func_print: Callable = print) -> int:
    """SQL Query Statemet to CSV format file and compression data.

    Args:
        engine (Connection): Connection Database And SQLAlchemy.Engine
        sql_query (str): SQL Query Statement (SELECT Only)
        file_name (os.PathLike): save with filename and extention file (Example: `./mycsv.csv.gz`)
        compression (str, optional): Compression file type (`plain|gzip|zip`). Defaults to CSV_COMPRESSION_GZIP.
        func_print (Callable, optional): Callback Print Massage function . Defaults to print.

    Raises:
        ex: Errror Handler

    Returns:
        int: Total count record data. 
    """
    if func_print is None:
        def func_print(*v, **k):
            pass
    try:
        dir_temp = "./temp_" + str(time.time()).replace(".", "")
        temp_file = os.path.join(dir_temp, str(time.time()))
        os.makedirs(dir_temp, exist_ok=True)
        func_print("Start Query SqlStantement")
        iterator = pd.read_sql_query(
            sql_query,
            engine,
            coerce_float=True,
            chunksize=10000,
            # dtype=pd.StringDtype(storage="pyarrow"),
        )
        func_print("End Query SqlStantement")

        total_count = 0
        if compression == CSV_COMPRESSION_GZIP:
            fa = gzip.open(temp_file, mode='w', compresslevel=5,
                           encoding=None, errors=None, newline=None)
        elif compression == CSV_COMPRESSION_ZIP:
            fa = zipfile.ZipFile(temp_file, "a", 5)
        else:
            fa = open(temp_file, "a", buffering=io.DEFAULT_BUFFER_SIZE)

        for i, df in enumerate(iterator):
            func_print("Convert To CSV chunk: ", (i+1))
            if df is not None:
                func_print("Rows Count: ", len(df.index))

            df = dh.convert_dtypes(df)
            func_print("Pandas to CSV file.")
            is_header = i == 0
            df.to_csv(fa, header=is_header, index=False, mode='a',
                      quoting=csv.QUOTE_NONNUMERIC, chunksize=1000)
            total_count += len(df.index)

        if fa:
            fa.close()

        func_print("Move file.")
        # ยายไฟล์จาก temfile
        os.rename(temp_file, file_name)
        func_print("End.")
    except Exception as ex:
        raise ex
    finally:
        if os.path.exists(dir_temp):
            shutil.rmtree(dir_temp, ignore_errors=True)
    return total_count


def read_csv(filename: os.PathLike, **pandas_option) -> pd.DataFrame:
    """Read Csv file 

    pandas option: [https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html](https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html)

    Args:
        filename (os.PathLike): Any valid string path is acceptable. The string could be a URL. Valid URL schemes include http, ftp, s3, gs, and file. For file URLs, a host is expected. A local file could be: file://localhost/path/to/table.csv.

If you want to pass in a path object, pandas accepts any os.PathLike.

By file-like object, we refer to objects with a read() method, such as a file handle (e.g. via builtin open function) or StringIO.

    Returns:
        pd.DataFrame: pandas DataFrame
    """
    pandas_option = __default_pandas_option(**pandas_option)
    df = pd.read_csv(filename, **pandas_option)
    df = dh.convert_dtypes(df)
    return df


def head_csv(filename: os.PathLike, nrows: int = 10, **pandas_option) -> pd.DataFrame:
    """Read Head record in csv file

    Args:
        filename (os.PathLike): filename
        nrows (int, optional): number rows. Defaults to 10.

    Returns:
        pd.DataFrame: pandas DataFrame
    """
    pandas_option = __default_pandas_option(**pandas_option)

    df = pd.read_csv(filename, nrows=nrows, **pandas_option)
    df = dh.convert_dtypes(df)
    return df


def batch_csv(filename: os.PathLike, batch_size: int = 10000,  **pandas_option) -> Iterator[pd.DataFrame]:
    """Read CSV file for iteration object

    Args:
        filename (os.PathLike): filename
        batch_size (int, optional): batch_size or chunksize row number. Defaults to 10000.

    Yields:
        Iterator[pd.DataFrame]: Return  Iterator[pd.DataFrame]
    """
    pandas_option = __default_pandas_option(**pandas_option)

    df = pd.read_csv(filename, chunksize=batch_size, **pandas_option)
    df = dh.convert_dtypes(df)
    return df


def __default_pandas_option(**pandas_option):
    if "on_bad_lines" in pandas_option:
        pandas_option['on_bad_lines'] = "skip"
    return pandas_option
