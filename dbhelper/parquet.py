# import re
import os
import shutil
import time
import typing as tp
from typing import Callable

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


# {NONE, SNAPPY, GZIP, BROTLI, LZ4, }.
PARQUET_COMPRESSION_NONE = "NONE"
PARQUET_COMPRESSION_SNAPPY = "SNAPPY"
PARQUET_COMPRESSION_GZIP = "GZIP"
PARQUET_COMPRESSION_BROTLI = "BROTLI"
PARQUET_COMPRESSION_LZ4 = "LZ4"
PARQUET_COMPRESSION_ZSTD = "ZSTD"


def to_parquet(
    engine,
    sql_query: str,
    file_name: os.PathLike,
    compression=PARQUET_COMPRESSION_SNAPPY,
    func_print: Callable = print,
) -> int:
    """SQL Query Statemet to Parquet format file.

    Args:
        engine (_type_): Connection Database And SQLAlchemy.Engine
        sql_query (str): SQL Query Statement (SELECT Only)
        file_name (os.PathLike): save with filename and extention file (Example: `./myparquet.parquet`)
        compression (_type_, optional): _description_. Compression file type to PARQUET_COMPRESSION_SNAPPY.
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
        for i, df in enumerate(iterator):
            func_print("Convert To Parquet chunk: ", (i + 1))
            if df is not None:
                func_print("Rows Count: ", len(df.index))

            df = dfh.convert_dtypes(df)

            func_print("Pandas to parquet file.")
            table = pa.Table.from_pandas(df, preserve_index=False)
            # print(table)
            # for the first chunk of records
            if i == 0:
                # print(df.dtypes)
                # create a parquet write object giving it an output file
                pqwriter = pq.ParquetWriter(
                    temp_file, table.schema, compression=compression
                )
                pqwriter.write_table(table)
            # subsequent chunks can be written to the same file
            else:
                pqwriter.write_table(table)
            total_count += len(df.index)

        # close the parquet writer
        if pqwriter:
            pqwriter.close()

        # engine.close()

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


def read_parquet(filename: os.PathLike) -> pd.DataFrame:
    """Read Parquet file into pandas DataFrame

    Args:
        filename (os.PathLike): file name `os.PathLike`

    Returns:
        pd.DataFrame: pandas DataFrame
    """
    return pq.read_pandas(filename).to_pandas()


def head_parquet(filename: os.PathLike, batch_size: int = 10) -> pd.DataFrame:
    """Read Head record in Parquet file

    Args:
        filename (os.PathLike): filename
        nrows (int, optional): number rows. Defaults to 10.

    Returns:
        pd.DataFrame: pandas DataFrame
    """
    pf = pq.ParquetFile(filename)
    head_rows = next(pf.iter_batches(batch_size=batch_size))
    df = pa.Table.from_batches([head_rows]).to_pandas()
    return df


def batch_parquet(
    filename: os.PathLike, batch_size: int = 10000
) -> tp.Iterator[pd.DataFrame]:
    """Read Parquet file into iteration pandas dataframe object

    Args:
        filename (os.PathLike): filename
        batch_size (int, optional): batch_size or chunksize row number. Defaults to 10000.

    Yields:
        Iterator[pd.DataFrame]: Return  Iterator[pd.DataFrame]
    """
    pf = pq.ParquetFile(filename)
    for df in pf.iter_batches(batch_size=batch_size):
        yield pa.Table.from_batches([df]).to_pandas()
