# Database Helper (Dump, Backup, Restore)
> 

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

```python
from dbhelper import connection as dh_conn
from dbhelper import connection as dh_connection

conn = dh_conn.create_connection(dsn, password)

```