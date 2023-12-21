#!/usr/bin/env python
import click
import logging
import dbhelper as dh
from urllib.parse import quote_plus


@click.group()
@click.option(
    "--log-level",
    default="INFO",
    show_default=True,
    help="the level is one of the predefined levels (CRITICAL, ERROR, WARNING, INFO, DEBUG)",
)
def cli(log_level: str):
    logging.basicConfig(
        level=logging.getLevelName(log_level),
        datefmt=r"%Y-%m-%dT%H:%M:%S",
        format="%(asctime)s.%(msecs)03d %(levelname)s  %(message)s",
    )
    pass


# -----------------------------------------------------------------------------


@click.command(help="Process sql file")
@click.argument("file", required=True, type=click.Path(exists=True), nargs=1)
@click.option("-d", "--dsn", required=True, help="SQLAlchemy connection string")
def process(file: str, dsn: str):
    try:
        if dsn is None:
            raise Exception("DSN is required")
        ok = dh.process_sql(file, dsn)
        if not ok:
            raise Exception("Process error")
    except Exception as ex:
        click.echo("Error: %s" % str(ex), err=True)


# -----------------------------------------------------------------------------


@click.command(help="quote password(encode password) string")
@click.option("--password", prompt="Password", hide_input=True)
def password(password: str):
    click.echo(quote_plus(password))


# -----------------------------------------------------------------------------


@click.command(
    help="""Dump SQL Statement to CSV file  

OUTPUT: `./test.csv.gz` is compressed with GZIP

OR 

OUTPUT: `./test.csv` is not compressed 
"""
)
@click.argument(
    "output",
    required=True,
    type=click.Path(exists=True),
    nargs=1,
)
@click.option("--dsn", "-d", type=str, required=True, help="Database connection string")
@click.option(
    "--sql", "-s", required=True, type=str, help="SQL Query Statement[file|str]"
)
def csv(output, dsn, sql):
    engine = dh.create_connection(dsn)
    total = dh.to_csv(engine, sql, output, echo=True)
    click.echo(f"output file: {output}")
    click.echo(f"total count: {total:,}")


# -----------------------------------------------------------------------------


@click.command(help="""Dump SQL Statement to Parquet file""")
@click.argument(
    "output",
    required=True,
    type=click.Path(exists=True),
    nargs=1,
)
@click.option("--dsn", "-d", type=str, required=True, help="Database connection string")
@click.option(
    "--sql", "-s", required=True, type=str, help="SQL Query Statement[file|str]"
)
@click.option(
    "--compression",
    "-c",
    type=click.Choice(["snappy", "gzip", "brotli"]),
    default="snappy",
    help="Compression type",
)
def parquet(output, dsn, sql, compression="snappy"):
    engine = dh.create_connection(dsn)
    total = dh.to_parquet(engine, sql, compression, output, echo=True)
    click.echo(f"output file: {output}")
    click.echo(f"total count: {total:,}")


# -----------------------------------------------------------------------------


cli.add_command(process)
cli.add_command(password)
cli.add_command(csv)
cli.add_command(parquet)


def main():
    cli()


if __name__ == "__main__":
    main()
