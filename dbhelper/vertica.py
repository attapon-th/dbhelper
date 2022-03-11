from typing import List
import pandas as pd
from vertica_python.vertica.cursor import Cursor
from typing import List, Dict, AnyStr, Any, Union
from . import dataframe as dfh


def create_table_ddl(vertica_cursor: Cursor, sql_create_table: str):
    """Create Table with sql create table

    Args:
        vertica_cursor (Cursor): Cursor from vertica connection 
        sql_create_table (str): SQL Create Table.

    Raises:
        Exception: Execute Create Table Error
    """
    try:
        vertica_cursor.execute(sql_create_table)
    except Exception as er:
        raise Exception("Create Table Error: %s" % er.__str__())


def create_table_from(vertica_cursor: Cursor,  from_table: str, to_table: str,):
    """Create Table from another table in vertica database

    Args:
        vertica_cursor (Cursor): Cursor from vertica connection 
        from_table (str): Source table copy DDL.
        to_table (str): Target table name

    Raises:
        Exception: Execute Create Table Error
    """
    vsql = f'CREATE TABLE IF NOT EXISTS {to_table} LIKE {from_table}'
    try:
        vertica_cursor.execute(vsql)
    except Exception as er:
        raise Exception("Create Table Error: %s" % er.__str__())


def copy_to_vertica(vertica_cursor: Cursor, df: pd.DataFrame, table: str, *, reject_table: str = None, check_column: bool = True):
    """_summary_

    Args:
        vertica_cursor (Cursor): Cursor from vertica connection .
        df (pd.DataFrame): Pandas DataFrame.
        table (str): Target table name.
        reject_table (str, optional): Reject Data to table name. Defaults to None.
        check_column (bool, optional): Check column if exists. Defaults to True.

    Raises:
        Exception: Target table copy is not exist.
        Exception: Copy data error.
    """
    if check_column:
        df_col = table_check(vertica_cursor, table)
        if df_col is None:
            raise Exception(f"Copy to Table[{table}] is not exists.")
        df = dfh.select_column(df, df_col.columns.tolist())

    cols = ",".join(df.columns.tolist())
    vsql = f"COPY {table} ({cols}) FROM stdin PARSER public.fcsvparser()"

    if not reject_table is None:
        vsql += f"REJECTED DATA AS TABLE {reject_table};"
    vsql += ";"
    try:
        vertica_cursor.execute(vsql)
    except Exception as er:
        raise Exception("Copy Data Error: %s" % er.__str__())


def build_sql_no_duplicate(table: str, column_check: str, order_by: str) -> str:
    """Build SQL For SELECT data no duplicate by `column_check` and select first row by `order_by`

    Args:
        table (str): full table name
        column_check (str): columns check is not duplicate. Example: "c1,c2,c3"
        order_by (str): order by for select row first

    Returns:
        str: SQL SELECT is no duplicate data
    """
    return "SELECT no_dup.* FROM (SELECT *, ROW_NUMBER() OVER(PARTITION BY {column_check} ORDER BY {order_by}) as __rn FROM {table}) no_dup WHERE no_dup.__rn=1"


def merge_to_table(vertica_cursor: Cursor,
                   from_table: str,
                   to_table: str,
                   merge_on_columns: List[str],
                   *,
                   no_execute: bool = False,
                   add_field_insert: Dict[str, AnyStr] = None,
                   add_field_update: Dict[str, AnyStr] = None) -> Union[AnyStr, int]:
    """Vertica Merge Data between table and table

    Args:
        vertica_cursor (Cursor): Cursor from vertica connection.
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
    # * check table if exists
    from_df = table_check(vertica_cursor, from_table)
    to_df = table_check(vertica_cursor, to_table)
    if from_df is None:
        raise Exception(f"Table[{from_table}] is not exist.")
    if to_df is None:
        raise Exception(f"Table[{to_table}] is not exist.")

    # * check column if exists in `merge_on_columns`
    cols_from = from_df.columns.tolist()
    cols_to = to_df.columns.tolist()
    cols_from_merge_on_not_exists = [
        a for a in cols_from if a in merge_on_columns]
    cols_to_merge_on_not_exists = [a for a in cols_to if a in merge_on_columns]
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
        vsql = f"\nWHEN MATCHED THEN UPDATE SET {updates} "
    vsql = "\nWHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({inserts}) "
    vsql = ";"
    if no_execute:
        return vsql
    try:
        merge_count = 0
        vertica_cursor.execute(vsql)
        if vertica_cursor:
            a = vertica_cursor.fetchone()
            if isinstance(a, dict):
                if "OUTPUT" in a:
                    merge_count = a["OUTPUT"]
            elif len(a) > 0:
                merge_count = a[0]
            else:
                merge_count = -1
        return merge_count
    except Exception as ex:
        raise Exception(
            f"Error merge data from[{from_table}] to[{to_table}]. {ex}")


def table_check(vertica_cursor: Cursor, table: str) -> pd.DataFrame:
    """_summary_

    Args:
        vertica_cursor (Cursor): Cursor from vertica connection.
        table (str): Full Table name.

    Returns:
        pd.DataFrame: table if exsit return Padas DataFrame. 
    """
    try:
        df = pd.read_sql_query(
            f"SELECT * FROM {table} LIMIT 0;", vertica_cursor.connection)
        return df
    except:
        return None


def drop_table(vertica_cursor: Cursor, table: str) -> bool:
    """Drop Table if exists.

    Args:
        vertica_cursor (Cursor): Cursor from vertica connection.
        table (str): Full Table name.

    Returns:
        bool: is success.
    """
    try:
        vertica_cursor.execute(f"DROP TABLE IF EXISTS {table};")
        return True
    except:
        return False
