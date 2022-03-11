import mysql.connector
import vertica_python
import dsnparse
from sqlalchemy.engine import create_engine
import os


def create_connection_database(dsn: str, password: str = None):
    o = dsnparse.parse(dsn)
    # print(o)
    scheme = o.scheme.lower()
    if password != None or password != "":
        o.password = password
    # print(o.hostname, o.port, o.username, o.password,)
    # print(type(o.port), type(o.port) == int)
    # os._exit(0)
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
        return conn_vertica(dsn, password=password)
    else:
        return create_engine(o.geturl())


def conn_vertica(dsn: str, password: str):
    print("Connect DSN: ", dsn)
    conn_info = vertica_python.parse_dsn(dsn)
    if password:
        conn_info['password'] = password
    return vertica_python.connect(**conn_info)
