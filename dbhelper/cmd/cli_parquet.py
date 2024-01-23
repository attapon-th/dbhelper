import click
from ..parquet import to_parquet
from ..connection import create_connection
from .dsn_helper import get_dsn


@click.command(help="""Dump SQL Statement to Parquet file""")
@click.argument("sql", required=True, type=str, nargs=1)
@click.option("-o", "--output", default="output.parquet", help="Output file name (default: stdout)")
@click.option("-c", "--compression", type=click.Choice(["snappy", "gzip", "brotli"]), default="snappy", help="Compression type")
@click.option("-d", "--dsn", default="DB_DSN", help="SQLAlchemy connection string or OS Environment(default: DB_DSN)")
@click.option("-c", "--config", default="config.ini", help="Config file (default: config.ini)")
@click.option("-k", "--keys", default="vertica.dsn", help="Config Keys (default: vertica.dsn)")
def parquet(output, dsn, sql, config, keys, compression="snappy"):
    dsn = get_dsn(dsn, config, keys)
    engine = create_connection(dsn)
    total = to_parquet(engine, sql, compression, output, echo=True)
    click.echo(f"output file: {output}")
    click.echo(f"total count: {total:,}")
