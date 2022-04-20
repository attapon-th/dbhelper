# Table of Contents

* [dbhelper](https://attapon-th.github.io/dbhelper)
* [parquet](#parquet)
  * [to\_parquet](#parquet.to_parquet)
  * [read\_parquet](#parquet.read_parquet)
  * [head\_parquet](#parquet.head_parquet)
  * [batch\_parquet](#parquet.batch_parquet)

<a id="parquet"></a>

# parquet

<a id="parquet.to_parquet"></a>

#### to\_parquet

```python
def to_parquet(engine,
               sql_query: str,
               file_name: os.PathLike,
               compression=PARQUET_COMPRESSION_SNAPPY,
               func_print: Callable = print) -> int
```

SQL Query Statemet to Parquet format file.

**Arguments**:

- `engine` __type__ - Connection Database And SQLAlchemy.Engine
- `sql_query` _str_ - SQL Query Statement (SELECT Only)
- `file_name` _os.PathLike_ - save with filename and extention file (Example: `./myparquet.parquet`)
- `compression` __type_, optional_ - _description_. Compression file type to PARQUET_COMPRESSION_SNAPPY.
- `func_print` _Callable, optional_ - Callback Print Massage function . Defaults to print.
  

**Raises**:

- `ex` - Errror Handler
  

**Returns**:

- `int` - Total count record data.

<a id="parquet.read_parquet"></a>

#### read\_parquet

```python
def read_parquet(filename: os.PathLike) -> pd.DataFrame
```

Read Parquet file into pandas DataFrame

**Arguments**:

- `filename` _os.PathLike_ - file name `os.PathLike`
  

**Returns**:

- `pd.DataFrame` - pandas DataFrame

<a id="parquet.head_parquet"></a>

#### head\_parquet

```python
def head_parquet(filename: os.PathLike, batch_size: int = 10) -> pd.DataFrame
```

Read Head record in Parquet file

**Arguments**:

- `filename` _os.PathLike_ - filename
- `nrows` _int, optional_ - number rows. Defaults to 10.
  

**Returns**:

- `pd.DataFrame` - pandas DataFrame

<a id="parquet.batch_parquet"></a>

#### batch\_parquet

```python
def batch_parquet(filename: os.PathLike,
                  batch_size: int = 10000) -> tp.Iterator[pd.DataFrame]
```

Read Parquet file into iteration pandas dataframe object

**Arguments**:

- `filename` _os.PathLike_ - filename
- `batch_size` _int, optional_ - batch_size or chunksize row number. Defaults to 10000.
  

**Yields**:

- `Iterator[pd.DataFrame]` - Return  Iterator[pd.DataFrame]

