import click
from ..process_sql import process_sql
from .dsn_helper import get_dsn


@click.command(help="Process sql file")
@click.argument("file", required=True, type=click.Path(exists=True), nargs=1)
@click.option("-d", "--dsn", required=True, default="DB_DSN", help="SQLAlchemy connection string (default: DB_DSN is environment variable)")
def process(file, dsn: str = "DB_DSN"):
    try:
        dsn = get_dsn(dsn)
        ok = process_sql(file, dsn)
        if not ok:
            raise Exception("Process error")
    except Exception as ex:
        click.echo("Error: %s" % str(ex), err=True)
