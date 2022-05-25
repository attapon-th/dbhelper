from __future__ import annotations
import dsnparse
from sqlalchemy.engine import create_engine, Engine
try:
    from urllib import unquote, quote
except ImportError:
    from urllib.parse import unquote, quote


def create_connection(dsn: str, password: str = None) -> Engine:
    """Create Database Connection  

       MySQL: `mysql://{user}@{host}:{port}/{db_name}`  

       Vertica: `vertica://{user}@{host}:{port}/{db_name}`  

       OtherDB: `{dialect}+{driver}://{user}@{host}:{port}/{db_name}`

    Args:
        dsn (str): dsn string connection Example: `mysql://root@127.0.0.1:3306/mydb`
        password (str, optional): Password Database. Defaults to None.

    Returns:
        Engine: Engine SQLAlchemy Connection Database
    """
    o = dsnparse.parse(dsn)
    # print(o)
    scheme = o.scheme.lower()
    if password != None or password != "":
        o.password = quote(password)

    if "mysql" == scheme:
        o.scheme = 'mysql+mysqlconnector'
        if len(o.query) == 0:
            o.query_str = 'auth_plugin=mysql_native_password'
        elif not 'auth_plugin' in o.query:
            o.query_str += '&auth_plugin=mysql_native_password'
        cnx = create_engine(
            o.geturl(),
            connect_args={'auth_plugin': 'mysql_native_password'})
        return cnx
    elif "vertica" == scheme:
        o.scheme = 'vertica+vertica_python'
        return create_engine(o.geturl())
    else:
        return create_engine(o.geturl())


def test_connection_engine(conn: Engine) -> tuple[bool, str]:
    """Test Connection Database

    Args:
        conn (Engine): SQLAlchemy Engine

    Returns:
        bool: Connection Database if success.
    """
    try:
        with conn.connect() as connection:
            pass
            return True, ""
    except Exception as ex:
        return False, str(ex)
