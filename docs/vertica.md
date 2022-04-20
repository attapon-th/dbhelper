# Table of Contents

* [vertica](#vertica)
  * [create\_table\_with\_query](#vertica.create_table_with_query)
  * [create\_table\_from](#vertica.create_table_from)
  * [create\_table\_local\_temp](#vertica.create_table_local_temp)
  * [get\_ddl](#vertica.get_ddl)
  * [copy\_to\_vertica](#vertica.copy_to_vertica)
  * [merge\_to\_table](#vertica.merge_to_table)
  * [table\_check](#vertica.table_check)
  * [drop\_table](#vertica.drop_table)

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

Check table if exists

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

