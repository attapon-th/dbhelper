import click
from ..parquet import to_parquet
from ..connection import create_connection
from .dsn_helper import get_dsn


@click.command(help="""Dump SQL Statement to Parquet file""")
@click.argument("sql", required=True, type=str, nargs=1)
@click.option("--output", "-o", default="output.parquet", help="Output file name (default: stdout)")
@click.option("-d", "--dsn", required=True, default="DB_DSN", help="SQLAlchemy connection string (default: environment variable name: DB_DSN )")
@click.option("--compression", "-c", type=click.Choice(["snappy", "gzip", "brotli"]), default="snappy", help="Compression type")
def parquet(output, dsn, sql, compression="snappy"):
    dsn = get_dsn(dsn)
    engine = create_connection(dsn)
    total = to_parquet(engine, sql, compression, output, echo=True)
    click.echo(f"output file: {output}")
    click.echo(f"total count: {total:,}")
