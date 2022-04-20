# DB Helper API Documentation

# Table of Contents

* [Index](https://attapon-th.github.io/dbhelper/)
* [connection](#connection)
  * [create\_connection](#connection.create_connection)
* [dataframe](#dataframe)
  * [convert\_dtypes](#dataframe.convert_dtypes)
  * [select\_column](#dataframe.select_column)
* [parquet](#parquet)
* [vertica](#vertica)
  * [create\_table\_with\_query](#vertica.create_table_with_query)
  * [create\_table\_from](#vertica.create_table_from)
  * [create\_table\_local\_temp](#vertica.create_table_local_temp)
  * [get\_ddl](#vertica.get_ddl)
  * [copy\_to\_vertica](#vertica.copy_to_vertica)
  * [merge\_to\_table](#vertica.merge_to_table)
  * [table\_check](#vertica.table_check)
  * [drop\_table](#vertica.drop_table)
* [csv](#csv)
  * [to\_csv](#csv.to_csv)
  * [read\_csv](#csv.read_csv)
  * [head\_csv](#csv.head_csv)
  * [batch\_csv](#csv.batch_csv)

<a id="connection"></a>

# connection

<a id="connection.create_connection"></a>

#### create\_connection

```python
def create_connection(dsn: str, password: str = None)
```

Create Database Connection

MySQL: `mysql://{user}@{host}:{port}/{db_name}`

Vertica: `vertica://{user}@{host}:{port}/{db_name}`

OtherDB: `{dialect}+{driver}://{user}@{host}:{port}/{db_name}`

**Arguments**:

- `dsn` _str_ - dsn string connection Example: `mysql://root@127.0.0.1:3306/mydb`
- `password` _str, optional_ - Password Database. Defaults to None.
  

**Returns**:

- `DBConnection` - Connection Database if success.

<a id="dataframe"></a>

# dataframe

<a id="dataframe.convert_dtypes"></a>

#### convert\_dtypes

```python
def convert_dtypes(df: pd.DataFrame) -> pd.DataFrame
```

Convert Type in Pandas dataframe to pandas type

**Arguments**:

- `df` _pd.DataFrame_ - source PandasDataFrame to convert type
  

**Returns**:

- `pd.DataFrame` - result after convert to dtype

<a id="dataframe.select_column"></a>

#### select\_column

```python
def select_column(df: pd.DataFrame,
                  columns: list,
                  raise_not_exists: bool = False) -> pd.DataFrame
```

select column in Pandas DataFrame

**Arguments**:

- `df` _pd.DataFrame_ - source PandasDataFrame to convert type
- `columns` _list_ - list column name for check
- `raise_not_exists` _bool, optional_ - raise error if `column name if not exist`. Defaults to False.
  

**Raises**:

- `Exception` - Error column if not exists.
  

**Returns**:

- `pd.DataFrame` - result pandas DataFrame

<a id="parquet"></a>

# parquet

<a id="vertica"></a>

# vertica

<a id="vertica.create_table_with_query"></a>

#### create\_table\_with\_query

```python
def create_table_with_query(vertica_connection: VerticaConnection,
                            query: str,
                            to_table: str,
                            is_temp: bool = False) -> str
```

Create Table in Vertica with SQL Query

**Arguments**:

- `vertica_connection` _VerticaConnection_ - Vertica Connection
- `query` _str_ - SQL Query (SELECT Only)
- `to_table` _str_ - Table name ([schema.tablename|tablename])
- `is_temp_table` _bool, optional_ - create temp table delete auto when Vertica Session Connect closed. Defaults to False.
  

**Raises**:

- `Exception` - Create Table Error

<a id="vertica.create_table_from"></a>

#### create\_table\_from

```python
def create_table_from(vertica_connection: VerticaConnection, from_table: str,
                      to_table: str)
```

Create Table from another table in vertica database

**Arguments**:

- `vertica_connection` _VerticaConnection_ - Vertica Connection
- `from_table` _str_ - Source table copy DDL.
- `to_table` _str_ - Target table name
  

**Raises**:

- `Exception` - Execute Create Table Error

<a id="vertica.create_table_local_temp"></a>

#### create\_table\_local\_temp

```python
def create_table_local_temp(vertica_connection: VerticaConnection, query: str,
                            to_tablename: str)
```

Create local temp table in vertica

**Arguments**:

- `vertica_connection` _VerticaConnection_ - Vertica Connection
- `query` _str_ - SQL Statement (SELECT Only).
- `to_tablename` _str_ - Target table name only (No Schema name)
  

**Raises**:

- `Exception` - Execute Create Table Error

<a id="vertica.get_ddl"></a>

#### get\_ddl

```python
def get_ddl(vertica_connection: VerticaConnection, query: str,
            to_table: str) -> str
```

Get SQL Create Table Statement With Query

**Arguments**:

- `vertica_connection` _VerticaConnection_ - Vertica Connection
- `query` _str_ - query (str): SQL Statement (SELECT Only).
- `to_table` _str_ - Target table name (Full call: 'schema.table' )
  

**Returns**:

- `str` - SQL Create table

<a id="vertica.copy_to_vertica"></a>

#### copy\_to\_vertica

```python
def copy_to_vertica(vertica_connection: VerticaConnection,
                    fs: Union[os.PathLike, io.BytesIO, io.StringIO, Any],
                    table: str,
                    columns: List[str],
                    comprassion: str = "",
                    reject_table: str = None)
```

_summary_

**Arguments**:

- `vertica_connection` _VerticaConnection_ - Vertica Connection .
- `fs` _Union[os.PathLike, io.BytesIO, io.StringIO]_ - file path or file open. Example: open("/tmp/file.csv", "rb")
- `table` _str_ - Target table name.
- `comprassion` _str_ - Specifies the input format. [UNCOMPRESSED (default), BZIP,GZIP,LZO,ZSTD]
- `reject_table` _str, optional_ - Reject Data to table name. Defaults to None.
- `check_column` _bool, optional_ - Check column if exists. Defaults to True.
  

**Raises**:

- `Exception` - Target table copy is not exist.
- `Exception` - Copy data error.
  

**Returns**:

- `str` - SQL COPY

<a id="vertica.merge_to_table"></a>

#### merge\_to\_table

```python
def merge_to_table(
        vertica_connection: VerticaConnection,
        from_table: str,
        to_table: str,
        merge_on_columns: List[str],
        *,
        no_execute: bool = False,
        add_field_insert: Dict[str, AnyStr] = None,
        add_field_update: Dict[str, AnyStr] = None) -> Union[AnyStr, int]
```

Vertica Merge Data between table and table

**Arguments**:

- `vertica_connection` _VerticaConnection_ - Vertica Connection.
- `from_table` _str_ - Source table name.
- `to_table` _str_ - Target table name.
- `merge_on_columns` _List[str]_ - Check columns match is `UPDATE` and not match is `INSERT`
- `no_execute` _bool, optional_ - If `True` Return SQL Statement Only. Defaults to False.
- `add_field_insert` _Dict[str, AnyStr], optional_ - Add field insret more. Defaults to None.
- `add_field_update` _Dict[str, AnyStr], optional_ - Add field update more. Defaults to None.
  

**Raises**:

- `Exception` - Error Merge is not success.
  

**Returns**:

  Union[AnyStr, int]: Return  `if no_execute == True` Return SQL Statement  `else` Return `merge_total` count total data merge into table target.

<a id="vertica.table_check"></a>

#### table\_check

```python
def table_check(vertica_connection: VerticaConnection,
                table: str) -> pd.DataFrame
```

_summary_

**Arguments**:

- `vertica_connection` _VerticaConnection_ - Vertica Connection.
- `table` _str_ - Full Table name.
  

**Returns**:

- `pd.DataFrame` - table if exsit return Padas DataFrame.

<a id="vertica.drop_table"></a>

#### drop\_table

```python
def drop_table(vertica_connection: VerticaConnection, table: str) -> bool
```

Drop Table if exists.

**Arguments**:

- `vertica_connection` _VerticaConnection_ - Vertica Connection.
- `table` _str_ - Full Table name.
  

**Returns**:

- `bool` - is success.

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

- `filename` _os.PathLike_ - Any valid string path is acceptable. The string could be a URL. Valid URL schemes include http, ftp, s3, gs, and file. For file URLs, a host is expected. A local file could be: file://localhost/path/to/table.csv.
  
  If you want to pass in a path object, pandas accepts any os.PathLike.
  
  By file-like object, we refer to objects with a read() method, such as a file handle (e.g. via builtin open function) or StringIO.
  

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

