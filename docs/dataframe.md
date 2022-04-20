# Table of Contents


* [dbhelper](https://attapon-th.github.io/dbhelper)
* [dataframe](#dataframe)
  * [convert\_dtypes](#dataframe.convert_dtypes)
  * [select\_column](#dataframe.select_column)

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

