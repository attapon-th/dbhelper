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
@click.option("-d", "--dsn", required=True, default="DB_DSN", help="SQLAlchemy connection string (default: environment variable name: DB_DSN )")
@click.option("--output", "-o", type=click.Path(), help="Output file name (default: stdout)")
def csv(sql: str, output: str | None = "stdout", dsn: str = "DB_DSN"):
    dsn = get_dsn(dsn)
    engine = create_connection(dsn)
    if output is None:
        df: DataFrame = pd.read_sql_query(
            sql,
            engine,
            coerce_float=True,
            dtype_backend="pyarrow",
            dtype=pd.ArrowDtype(pa.string()),
        )
        click.echo(df.head(100).to_csv(output))
        click.echo(f"total count: {df.shape[0]:,}")
        return
    total = to_csv(engine, sql, output, echo=True)
    click.echo(f"output file: {output}")
    click.echo(f"total count: {total:,}")
