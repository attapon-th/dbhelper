# Table of Contents

* [csv](#csv)
  * [to\_csv](#csv.to_csv)
  * [read\_csv](#csv.read_csv)
  * [head\_csv](#csv.head_csv)
  * [batch\_csv](#csv.batch_csv)

<a id="csv"></a>

# csv

<a id="csv.to_csv"></a>

#### to\_csv

```python
def to_csv(engine,
           sql_query: str,
           file_name: os.PathLike,
           compression=CSV_COMPRESSION_GZIP,
           func_print: Callable = print) -> int
```

SQL Query Statemet to CSV format file and compression data.

**Arguments**:

- `engine` _Connection_ - Connection Database And SQLAlchemy.Engine
- `sql_query` _str_ - SQL Query Statement (SELECT Only)
- `file_name` _os.PathLike_ - save with filename and extention file (Example: `./mycsv.csv.gz`)
- `compression` _str, optional_ - Compression file type (`plain|gzip|zip`). Defaults to CSV_COMPRESSION_GZIP.
- `func_print` _Callable, optional_ - Callback Print Massage function . Defaults to print.
  

**Raises**:

- `ex` - Errror Handler
  

**Returns**:

- `int` - Total count record data.

<a id="csv.read_csv"></a>

#### read\_csv

```python
def read_csv(filename: os.PathLike, **pandas_option) -> pd.DataFrame
```

Read Csv file

pandas option: [https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html](https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html)

**Arguments**:

- `filename` _os.PathLike_ - file name `os.PathLike`
  

**Returns**:

- `pd.DataFrame` - pandas DataFrame

<a id="csv.head_csv"></a>

#### head\_csv

```python
def head_csv(filename: os.PathLike,
             nrows: int = 10,
             **pandas_option) -> pd.DataFrame
```

Read Head record in csv file

**Arguments**:

- `filename` _os.PathLike_ - filename
- `nrows` _int, optional_ - number rows. Defaults to 10.
  

**Returns**:

- `pd.DataFrame` - pandas DataFrame

<a id="csv.batch_csv"></a>

#### batch\_csv

```python
def batch_csv(filename: os.PathLike,
              batch_size: int = 10000,
              **pandas_option) -> Iterator[pd.DataFrame]
```

Read CSV file for iteration object

**Arguments**:

- `filename` _os.PathLike_ - filename
- `batch_size` _int, optional_ - batch_size or chunksize row number. Defaults to 10000.
  

**Yields**:

- `Iterator[pd.DataFrame]` - Return  Iterator[pd.DataFrame]

