# Table of Contents

* [dbhelper](https://attapon-th.github.io/dbhelper)
* [connection](#connection)
  * [create\_connection](#connection.create_connection)

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

