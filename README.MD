# Database Helper (Dump, Backup, Restore)
> 

## TODO:  
- [x] - Connection Database
    

## Installation

To install vertica-python with pip:
```shell
# Latest commit on main branch
pip install git+https://github.com/attapon-th/dbhelper.git@main
```

To install vertica-python from source, run the following command from the root directory:
```
python setup.py install
```


Usage:

### Create Connection
```python
from dbhelper import connection as dh_conn
from dbhelper import connection as dh_connection

# DSN - MySQL
dsn = 'mysql://{user}@{host}:{port}/{db_name}'

# DSN - Vertica
dsn = 'vertica://{user}@{host}:{port}/{db_name}'

# DSN - The Engine is the starting point for any SQLAlchemy application
# https://docs.sqlalchemy.org/en/14/core/engines.html#engine-configuration
dsn = '{dialect}+{driver}://{user}@{host}:{port}/{db_name}'

conn = dh_conn.create_connection(dsn, password)

```

### Dump Parquet file by SQL Query
