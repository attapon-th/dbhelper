from dbhelper import parquet as pq
from dbhelper import connection as cc
import click
import os


@click.group()
def main():
    """
    Dump SQL Statement to Parquet file.
    """
    pass


@main.command()
@click.option('--output', '-o', type=str, required=True,  default="out.parquet", help="Dump file output name")
@click.option('--dsn', '-d', type=str, required=True, help="Database connection string")
@click.option('--sql', '-s', required=True,  type=str, help="SQL Query Statement[file|str]")
@click.option('--password', '-p',  type=str, help="Password database")
def dump(output, dsn, sql="", password=None):
    conn = cc.create_connection(dsn, password)

    if os.path.exists(sql):
        with open(sql, "r") as f:
            sql_str = f.read()
    else:
        sql_str = sql

    total = pq.create_parquet(conn, sql_str, output, func_print=print)
    print("total count: ", total)


if __name__ == "__main__":
    main()
