import click
from ..csv import to_csv
from ..connection import create_connection
from .dsn_helper import get_dsn
from pandas import DataFrame
import pandas as pd
import pyarrow as pa


@click.command(
    help="""Dump SQL Statement to CSV file  

Example: 

With Stdout (limit show 100 records):  

    dbper csv "SELET * FROM some_table Where id > 100;"

With GZIP:  

    dbper csv -o ./test.csv.gz

Without compression:  

    dbper csv -o ./test.csv  "SELET * FROM some_table Where id > 100;"


"""
)
@click.argument("sql", required=True, type=str, nargs=1)
@click.option("-o", "--output", default="stdout", type=click.Path(), help="Output file name (default: stdout)")
@click.option("-c", "--config", default="config.ini", help="Config file (default: config.ini)")
@click.option("-k", "--keys", default="vertica.dsn", help="Config Keys (default: vertica.dsn)")
@click.option("-d", "--dsn", default="DB_DSN", help="SQLAlchemy connection string or OS Environment (default: DB_DSN )")
def csv(sql: str, config, keys, output, dsn):
    dsn = get_dsn(dsn, config, keys)
    engine = create_connection(dsn)
    if output.lower() == "stdout":
        df: DataFrame = pd.read_sql_query(
            sql,
            engine,
            coerce_float=True,
            dtype_backend="pyarrow",
            dtype=pd.ArrowDtype(pa.string()),
        )
        click.echo(df.head(100).to_csv(index=False))
        # click.echo(f"total count: {df.shape[0]:,}")
        return
    total = to_csv(engine, sql, output, echo=True)
    click.echo(f"output file: {output}")
    click.echo(f"total count: {total:,}")
