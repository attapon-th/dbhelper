#!/usr/bin/env python3
from time import time

from sqlalchemy import column
from dbhelper import connection as cc
from dbhelper import vertica as vc
from dbhelper import csv as cv
from dbhelper import dataframe as dh
import pandas as pd

import click
import os
import sys
import time


@click.group()
def main():
    """
    Dump SQL Statement to Parquet file.
    """
    pass


@main.command()
@click.option('--output', '-o', type=str, required=True,  default="out.csv.gz", help="Dump file output name")
@click.option('--dsn', '-d', type=str, required=True, help="Database connection string")
@click.option('--sql', '-s', required=True,  type=str, help="SQL Query Statement[file|str]")
@click.option('--password', '-p',  type=str, help="Password database")
@click.option('--compression',  type=str, default="gzip", help="Compression CSV")
def dump(output, dsn, sql="", password=None, compression="gzip"):
    conn = cc.create_connection(dsn, password)

    if os.path.exists(sql):
        with open(sql, "r") as f:
            sql_str = f.read()
    else:
        sql_str = sql
    sql_str = sql_str.strip().rstrip(";")
    sql_count = f"SELECT count(1) c FROM ({sql_str}) q"
    df = pd.read_sql_query(sql_count, conn)
    if df:
        print("SQL Count data from SQL: ", df["c"][0])
    total = cv.to_csv(conn, sql_str, output, compression, func_print=print)
    print("total count: ", total)


@main.command()
@click.option('--file', '-f', type=str, required=True, help="file import name")
@click.option('--dsn', '-d', type=str, required=True, help="Vertica connection string")
@click.option('--table', '-t', required=True,  type=str, help="table import data")
@click.option('--merge-on', '-m',   type=str, help="columns merge on Example: col1,col2,col3")
@click.option('--reject',  type=str, help="reject table")
@click.option('--password', '-p',  type=str, help="Password database")
@click.option('--compression',  type=str, default="gzip", help="Compression CSV")
def vmerge(file, dsn, table, merge_on, reject, password=None, compression="gzip"):
    conn = cc.create_connection(dsn, password)

    if not os.path.exists(file):
        print("Error: file[%s] is not exist." % file)
        sys.exit(9)

    # cur = conn.cursor()

    try:
        col_on = [a.strip() for a in merge_on.split(",")]
        print("check table")
        df_ck = vc.table_check(conn, table)
        if df_ck is None:
            print(f"Table[{table}] is not exits.")
            sys.exit(9)
        print("check table --> [PASS]")
        print("check file.")
        df_head = cv.head_csv(file)
        df_head = dh.select_column(df_head, df_ck.columns.tolist())
        if df_head.columns.__len__() == 0:
            sys.exit(9)
        columns: list = df_head.columns.tolist()

        dh.select_column(df_ck, col_on, True)
        print("check file --> [PASS]")

        print("create table temp")
        table_temp = "%s_%s" % (table, str(time.time()).replace(".", ""))
        # table_temp = table_temp
        cols_str = ",".join(columns)
        vc.create_table_with_query(conn, f"SELECT {cols_str} FROM {table}", table_temp, True)
        print(f"table[{table_temp}] created -- [PASS]")

        print("Copy data file to Vertica")
        with open(file, "rb") as fs:
            print("copy to vertica")
            vc.copy_to_vertica(conn, fs, table_temp, columns, compression, reject_table=reject)
            print("copy to vertica: ", "[PASS]")
        print("Start Merge to main table.")
        total = vc.merge_to_table(conn, table_temp, table, col_on)
        print("Merge data count: ", total)

        # print(f"drop temp table[{table_temp}")
        # vc.drop_table(conn, table_temp)

    except Exception as ex:
        print(ex)
        sys.exit(99)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
