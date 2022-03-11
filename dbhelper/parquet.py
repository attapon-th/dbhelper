# import re
from errno import EMFILE
import os
import time
import typing as tp

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Callable
from . import dataframe as dfh


# {NONE, SNAPPY, GZIP, BROTLI, LZ4, }.
PARQUET_COMPRESSION_NONE = 'NONE'
PARQUET_COMPRESSION_SNAPPY = 'SNAPPY'
PARQUET_COMPRESSION_GZIP = 'GZIP'
PARQUET_COMPRESSION_BROTLI = 'BROTLI'
PARQUET_COMPRESSION_LZ4 = 'LZ4'
PARQUET_COMPRESSION_ZSTD = 'ZSTD'


def create_parquet_file(engine, sql_query: str, file_name: os.PathLike, compression=PARQUET_COMPRESSION_SNAPPY, callback_print: Callable = None):
    if callback_print is None:
        def callback_print(*v, **k):
            pass
    try:
        dir_temp = "./temp_" + str(time.time())
        temp_file = os.path.join(dir_temp, str(time.time()))
        os.makedirs(dir_temp, exist_ok=True)
        callback_print("Start Query SqlStantement")
        iterator = pd.read_sql_query(
            sql_query,
            engine,
            coerce_float=True,
            chunksize=10000,
            # dtype=pd.StringDtype(storage="pyarrow"),
        )
        callback_print("End Query SqlStantement")

        total_count = 0
        for i, df in enumerate(iterator):
            callback_print("Convert To Parquet chunk: ", (i+1))
            if df is not None:
                callback_print("Rows Count: ", len(df.index))

            df = dfh.convert_dtypes(df)

            callback_print("Pandas to parquet file.")
            table = pa.Table.from_pandas(df, preserve_index=False)
            # print(table)
            # for the first chunk of records
            if i == 0:
                # print(df.dtypes)
                # create a parquet write object giving it an output file
                pqwriter = pq.ParquetWriter(
                    temp_file, table.schema, compression=compression)
                pqwriter.write_table(table)
            # subsequent chunks can be written to the same file
            else:
                pqwriter.write_table(table)
            total_count += len(df.index)

        # close the parquet writer
        if pqwriter:
            pqwriter.close()

        engine.close()

        callback_print("Move file.")
        # ยายไฟล์จาก temfile
        os.rename(temp_file, file_name)
        callback_print("End.")
    except Exception as ex:
        raise ex
    finally:
        if os.path.exists(dir_temp):
            os.unlink(dir_temp)


def read_parquet(filename: os.PathLike) -> pd.DataFrame:
    return pq.read_pandas(filename).to_pandas()


def head_parquet(filename: os.PathLike, batch_size: int = 10) -> pd.DataFrame:
    pf = pq.ParquetFile(filename)
    head_rows = next(pf.iter_batches(batch_size=batch_size))
    df = pa.Table.from_batches([head_rows]).to_pandas()
    return df


def batch_parquet(filename: os.PathLike, batch_size: int = 10000) -> tp.Iterator[pd.DataFrame]:
    pf = pq.ParquetFile(filename)
    for df in pf.iter_batches(batch_size=batch_size):
        yield pa.Table.from_batches([df]).to_pandas()
