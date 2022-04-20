from random import random
from typing import Callable, List
import pandas as pd
from vertica_python.vertica.cursor import Cursor
from vertica_python import errors, Connection as VerticaConnection
from typing import List, Dict, AnyStr, Any, Union
from . import dataframe as dfh
import os
import io
import random

random.seed(10)


def create_table_with_query(vertica_connection: VerticaConnection, query: str, to_table: str, is_temp: bool = False) -> str:
    """Create Table in Vertica with SQL Query

    Args:
        vertica_connection (VerticaConnection): Vertica Connection
        query (str): SQL Query (SELECT Only)
        to_table (str): Table name ([schema.tablename|tablename])
        is_temp_table (bool, optional): create temp table delete auto when Vertica Session Connect closed. Defaults to False.

    Raises:
        Exception: Create Table Error
    """
    if is_temp == True:
        vsql = f'CREATE TEMP TABLE IF NOT  EXISTS {to_table} ON COMMIT PRESERVE ROWS AS {query}'
    else:
        vsql = f'CREATE TABLE IF NOT EXISTS {to_table} AS {query}'
    if not "limit" in query.lower():
        vsql += " LIMIT 0"
    vsql += ";"
    try:
        with vertica_connection.cursor() as vertica_cursor:
            vertica_cursor.execute(vsql)
    except Exception as er:
        raise Exception("Create Table Error: %s" % er.__str__())


def create_table_from(vertica_connection: VerticaConnection,  from_table: str, to_table: str,):
    """Create Table from another table in vertica database

    Args:
        vertica_connection (VerticaConnection): Vertica Connection
        from_table (str): Source table copy DDL.
        to_table (str): Target table name

    Raises:
        Exception: Execute Create Table Error
    """
    # if vertica_cursor.closed():
    #     raise errors.InterfaceError('Cursor is closed')
    vsql = f'CREATE TABLE IF NOT EXISTS {to_table} LIKE {from_table}'
    try:
        with vertica_connection.cursor() as vertica_cursor:
            vertica_cursor.execute(vsql)
    except Exception as er:
        raise Exception("Create Table Error: %s" % er.__str__())


def create_table_local_temp(vertica_connection: VerticaConnection,  query: str, to_tablename: str):
    """Create local temp table in vertica

    Args:
        vertica_connection (VerticaConnection): Vertica Connection
        query (str): SQL Statement (SELECT Only).
        to_tablename (str): Target table name only (No Schema name)

    Raises:
        Exception:  Execute Create Table Error
    """
    vsql = f'CREATE LOCAL TEMP TABLE IF NOT EXISTS {to_tablename} ON COMMIT PRESERVE ROWS AS '
    if not "limit" in query.lower():
        vsql += f"\nSELECT * FROM ({query}) a LIMIT 0;"
    try:
        with vertica_connection.cursor() as vertica_cursor:
            vertica_cursor.execute(vsql)
    except Exception as er:
        raise Exception("Create Table Error: %s" % er.__str__())


def get_ddl(vertica_connection: VerticaConnection,  query: str, to_table: str) -> str:
    """Get SQL Create Table Statement With Query

    Args:
        vertica_connection (VerticaConnection): Vertica Connection
        query (str): query (str): SQL Statement (SELECT Only).
        to_table (str):  Target table name (Full call: 'schema.table' )

    Returns:
        str: SQL Create table
    """
    table = to_table
    rnd = int(random.random()*100000)
    to_table = f"{to_table}_{rnd}".replace(".", "_")
    create_table_local_temp(vertica_connection, query, to_table)
    with vertica_connection.cursor() as cur:
        cur.execute(f"SELECT export_objects('', '{to_table}', FALSE);")
        dll = cur.fetchone().pop()
        cur.execute(f"DROP TABLE IF EXISTS {to_table}")
        dll = dll.split(";").pop(1)
        dll = dll.split("(", 1).pop()
        dll = dll.rsplit(")", 1).pop(0)
    ddl = f"CREATE TABLE IF NOT EXISTS {table}({dll});"
    return ddl


def copy_to_vertica(
        vertica_connection: VerticaConnection,
        fs: Union[os.PathLike, io.BytesIO, io.StringIO, Any],
        table: str,
        columns: List[str],
        comprassion: str = "",
        reject_table: str = None
):
    """_summary_

    Args:
        vertica_connection (VerticaConnection): Vertica Connection .
        fs (Union[os.PathLike, io.BytesIO, io.StringIO]): file path or file open. Example: open("/tmp/file.csv", "rb")
        table (str): Target table name.
        comprassion (str): Specifies the input format. [UNCOMPRESSED (default), BZIP,GZIP,LZO,ZSTD]
        reject_table (str, optional): Reject Data to table name. Defaults to None.
        check_column (bool, optional): Check column if exists. Defaults to True.

    Raises:
        Exception: Target table copy is not exist.
        Exception: Copy data error.

    Returns:
        str: SQL COPY
    """
    # if vertica_cursor.closed():
    #     raise errors.InterfaceError('Cursor is closed')
    if type(comprassion) == str:
        comprassion = comprassion.upper()
    else:
        comprassion = ""
    cols = ",".join(columns)
    vsql = f"COPY {table} ({cols}) FROM stdin {comprassion} PARSER public.fcsvparser() "

    if not reject_table is None:
        vsql += f"REJECTED DATA AS TABLE {reject_table};"
    vsql += ";"
    try:
        with vertica_connection.cursor() as vertica_cursor:
            vertica_cursor.copy(vsql, fs, buffer_size=65536)
    except Exception as er:
        raise Exception("Copy Data Error: %s" % er.__str__())
    return vsql


def merge_to_table(vertica_connection: VerticaConnection,
                   from_table: str,
                   to_table: str,
                   merge_on_columns: List[str],
                   *,
                   no_execute: bool = False,
                   add_field_insert: Dict[str, AnyStr] = None,
                   add_field_update: Dict[str, AnyStr] = None) -> Union[AnyStr, int]:
    """Vertica Merge Data between table and table

    Args:
        vertica_connection (VerticaConnection): Vertica Connection.
        from_table (str): Source table name.
        to_table (str): Target table name.
        merge_on_columns (List[str]): Check columns match is `UPDATE` and not match is `INSERT`
        no_execute (bool, optional): If `True` Return SQL Statement Only. Defaults to False.
        add_field_insert (Dict[str, AnyStr], optional): Add field insret more. Defaults to None.
        add_field_update (Dict[str, AnyStr], optional):  Add field update more. Defaults to None.

    Raises:
        Exception: Error Merge is not success.

    Returns:
        Union[AnyStr, int]: Return  `if no_execute == True` Return SQL Statement  `else` Return `merge_total` count total data merge into table target.
    """
    # if vertica_cursor.closed():
    #     raise errors.InterfaceError('Cursor is closed')
    # * check table if exists
    from_df = table_check(vertica_connection, from_table)
    to_df = table_check(vertica_connection, to_table)
    if from_df is None:
        raise Exception(f"Table[{from_table}] is not exist.")
    if to_df is None:
        raise Exception(f"Table[{to_table}] is not exist.")

    # * check column if exists in `merge_on_columns`
    cols_from = from_df.columns.tolist()
    cols_to = to_df.columns.tolist()
    cols_from_merge_on_not_exists = [
        a for a in merge_on_columns if not a in cols_from]
    cols_to_merge_on_not_exists = [
        a for a in merge_on_columns if not a in cols_to]
    if len(cols_from_merge_on_not_exists) > 0:
        raise Exception(
            f"Error columns[{cols_from_merge_on_not_exists}] not in `merge_on_columns`")
    if len(cols_to_merge_on_not_exists) > 0:
        raise Exception(
            f"Error columns[{cols_to_merge_on_not_exists}] not in `merge_on_columns`")

    # * map columns `cols_from` contain `cols_to`
    cols_for_marge = []
    cols_for_merge_onpk = []
    for c in cols_to:
        if c in cols_from:
            cols_for_marge.append(c)
            if not c in merge_on_columns:
                cols_for_merge_onpk.append(c)

    target_table = to_table
    source_table = from_table
    merge_on = " AND ".join([a.format(merge_on_columns[i])
                            for i, a in enumerate(["t.{0}=s.{0}"]*len(merge_on_columns))])

    updates = ", ".join([a.format(cols_for_merge_onpk[i])
                         for i, a in enumerate(["{0}=s.{0}"]*len(cols_for_merge_onpk))])
    if len(cols_for_merge_onpk) > 0 and add_field_update:
        for k, v in add_field_update.items():
            updates += f", {k}='{v}'"

    columns = ", ".join(cols_for_marge)
    inserts = ", ".join([a.format(cols_for_marge[i])
                         for i, a in enumerate(["s.{0}"]*len(cols_for_marge))])

    if len(cols_for_marge) > 0 and add_field_insert:
        for k, v in add_field_update.items():
            columns += f", {k}"
            inserts += f", '{v}'"

    vsql = f"""MERGE INTO {target_table} t USING {source_table} s ON {merge_on} """
    if len(cols_for_merge_onpk) > 0:
        vsql += f"\nWHEN MATCHED THEN UPDATE SET {updates} "
    vsql += f"\nWHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({inserts}) "
    vsql += ";"
    if no_execute:
        return vsql
    try:
        merge_count = 0
        # print(vsql)
        with vertica_connection.cursor() as vertica_cursor:
            vertica_cursor.execute(vsql)
            if not vertica_cursor is None:
                a = vertica_cursor.fetchone()
                if isinstance(a, dict):
                    if "OUTPUT" in a:
                        merge_count = a["OUTPUT"]
                elif isinstance(a, list) and len(a) > 0:
                    merge_count = a[0]
                else:
                    merge_count = -1
        return merge_count
    except Exception as ex:
        raise Exception(
            f"Error merge data from[{from_table}] to [{to_table}]. {ex}")


def table_check(vertica_connection: VerticaConnection, table: str) -> pd.DataFrame:
    """Check table if exists

    Args:
        vertica_connection (VerticaConnection): Vertica Connection.
        table (str): Full Table name.

    Returns:
        pd.DataFrame: table if exsit return Padas DataFrame. 
    """
    try:
        df = pd.read_sql_query(
            f"SELECT * FROM {table} LIMIT 0;", vertica_connection)
        return df
    except:
        return None


def drop_table(vertica_connection: VerticaConnection, table: str) -> bool:
    """Drop Table if exists.

    Args:
        vertica_connection (VerticaConnection): Vertica Connection.
        table (str): Full Table name.

    Returns:
        bool: is success.
    """
    try:
        with vertica_connection.cursor() as vertica_cursor:
            vertica_cursor.execute(f"DROP TABLE IF EXISTS {table};")
        return not vertica_cursor is None
    except:
        return False
