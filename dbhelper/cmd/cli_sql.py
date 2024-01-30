import click
from ..process_sql import process_sql
from .dsn_helper import get_dsn


@click.command(help="Process SQL file or SQL Query")
@click.argument("file_or_sql", required=True, nargs=1)
@click.option("-d", "--dsn", help="SQLAlchemy connection string or OS Environment (default: DB_DSN)")
@click.option("-c", "--config", default="config.ini", help="Config file (default: config.ini)")
@click.option("-k", "--keys", default="vertica.dsn", help="Config Keys (default: vertica.dsn)")
def process(
    file_or_sql,
    dsn,
    config: str = "config.ini",
    keys: str = "vertica.dsn",
):
    try:
        dsn = get_dsn(dsn, config, keys)
        ok = process_sql(file_or_sql, dsn)
        if not ok:
            raise Exception("Process error")
    except Exception as ex:
        click.echo("Error: %s" % str(ex), err=True)
        exit(99)
