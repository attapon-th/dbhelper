# Database Helper (Dump, Backup, Restore)

[![GitHub version](https://badge.fury.io/gh/attapon-th%2Fdbhelper.svg)](https://badge.fury.io/gh/attapon-th%2Fdbhelper)
[![GitHub release version](https://img.shields.io/github/v/release/attapon-th/dbhelper?include_prereleases)](https://github.com/attapon-th/dbhelper)



## API Documentation

* [dbhelper](https://attapon-th.github.io/dbhelper/)
  * [connection](https://attapon-th.github.io/dbhelper/connection)
  * [csv](https://attapon-th.github.io/dbhelper/csv)
  * [parquet](https://attapon-th.github.io/dbhelper/parquet)
  * [vertica](https://attapon-th.github.io/dbhelper/vertica)
  * [dataframe](https://attapon-th.github.io/dbhelper/dataframe)



## TODO:  

- [x] - Connection Database (Vertica, MySQL or SQLAlchemy Engine connect)
- [x] - Dump SQL Query to **Parquet**.
- [x] - Dump SQL QUery to **CSV**  ** Only GZip or plain text
- [x] - Restore to **Vertica** from **Parquet**
- [x] - Restore to **Vertica** from **CSV**
- [ ] - ETC...


## Installation

To install `dbhelper` with pip:

```shell
pip install git+https://github.com/attapon-th/dbhelper.git@main
```

To install `dbhelper` from source, run the following command from the root directory:

```
python setup.py install
```

Usage:

### Create Connection
```python
from dbhelper import connection as dh_conn


# DSN - MySQL
dsn = f'mysql://{user}@{host}:{port}/{db_name}'

# DSN - Vertica
dsn = f'vertica://{user}@{host}:{port}/{db_name}'

# DSN - The Engine is the starting point for any SQLAlchemy application
# https://docs.sqlalchemy.org/en/14/core/engines.html#engine-configuration
dsn = f'{dialect}+{driver}://{user}@{host}:{port}/{db_name}'

conn = dh_conn.create_connection(dsn, password)

```

### Dump Parquet file by SQL Query

```python
from dbhelper import parquet as pq

sql = "SELET * FROM some_table Where id > 100;"
output = "./some_table_id100.parquet"
total_count = pq.create_parquet_file(conn, sql, output, compression='SNAPPY', func_print=print)
print("record dump total: ", total_count)
```


## Usage Cli

- `sql_parquet`  
- `sql_csv`

```shell
python -m sql_parquet --help

#--- OUTPUT ---#
Usage: python -m sql_parquet [OPTIONS] COMMAND [ARGS]...

  Dump SQL Statement to Parquet file.

Options:
  --help  Show this message and exit.

Commands:
  dump
  vmerge
```

### Example `sql_parquet`

Dump `SQL Query` to `Parquet` file.

```shell
python -m sql_parquet \
dump \
-s "SELET * FROM some_table Where id > 100;"
-d "mysql://user@localhost:3306/test_db"
-p "password" 
-o "test.parquet"
```


Restore `Parquet` file to `Vertica`.

```shell
python -m sql_parquet \
vmerge \
-d "vertica://user@localhost:3306/test_db"
-p "password" 
-f "test.parquet"
-m "primary_key1, primary_key2"
-t "schema_name.table_name"
```

### Example `sql_csv`


Dump `SQL Query` to `csv` file.

> Default compression: `gzip`

```shell
python -m sql_csv \
dump \
-s "SELET * FROM some_table Where id > 100;"
-d "mysql://user@localhost:3306/test_db"
-p "password" 
-o "test.csv.gz"
```


Restore `CSV` file to `Vertica`.

```shell
python -m sql_csv \
vmerge \
-d "vertica://user@localhost:5433/test_db"
-p "password" 
-f "test.csv.gz"
-m "primary_key1, primary_key2"
-t "schema_name.table_name"
```
