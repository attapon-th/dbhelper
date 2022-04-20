import mysql.connector
import vertica_python
import dsnparse
from sqlalchemy.engine import create_engine

def create_connection(dsn: str, password: str = None):
    """Create Database Connection  

       MySQL: `mysql://{user}@{host}:{port}/{db_name}`  

       Vertica: `vertica://{user}@{host}:{port}/{db_name}`  

       OtherDB: `{dialect}+{driver}://{user}@{host}:{port}/{db_name}`

    Args:
        dsn (str): dsn string connection Example: `mysql://root@127.0.0.1:3306/mydb`
        password (str, optional): Password Database. Defaults to None.

    Returns:
        DBConnection: Connection Database if success.
    """
    o = dsnparse.parse(dsn)
    # print(o)
    scheme = o.scheme.lower()
    if password != None or password != "":
        o.password = password

    if "mysql" == scheme:
        # print(o.password)
        cfg = dict(user=o.username,
                   password=o.password,
                   host=o.hostname,
                   database=o.path.lstrip("/"),
                   port="3306",
                   )
        if type(o.port) == int:
            try:
                cfg["port"] = str(o.port)
            except:
                pass
        # print(cfg)
        # os._exit(0)
        cnx = mysql.connector.connect(**cfg)
        return cnx
    elif "vertica" == scheme:
        return __conn_vertica(dsn, password=password)
    else:
        return create_engine(o.geturl())


def __conn_vertica(dsn: str, password: str):
    print("Connect DSN: ", dsn)
    conn_info = vertica_python.parse_dsn(dsn)
    if password:
        conn_info['password'] = password
    return vertica_python.connect(**conn_info)
