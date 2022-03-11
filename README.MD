# Database Helper (Dump, Backup, Restore)
> 

## TODO:  
- [x] - Connection Database (Vertica, MySQL or SQLAlchemy Engine connect)
- [x] - Dump SQL Query to **Parquet**.
- [ ] - Dump SQL QUery to **CSV**
- [x] - Restore to **Vertica** from **Parquet**
- [ ] - Restore to **Vertica** from **CSV**

    

## Installation

To install `dbhelper` with pip:
```shell
# Latest commit on main branch
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


Usage  `sql_parquet` Cli:

Cli help:
```
python -m sql_parquet dump --help

#--- OUTPUT ---#
Usage: python -m sql_parquet dump [OPTIONS]

Options:
  -o, --output TEXT    Dump file output name  [required]
  -d, --dsn TEXT       Database connection string  [required]
  -s, --sql TEXT       SQL Query Statement[file|str]  [required]
  -p, --password TEXT  Password database
  --help               Show this message and exit.
```

Example:
```
python -m sql_parquet \
-s "SELET * FROM some_table Where id > 100;"
-d "mysql://user@localhost:3306/test_db"
-p "password" 
-o "test.parquet"
```

### Parquet to Vertica
