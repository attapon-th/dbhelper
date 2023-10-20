#!/usr/bin/env python
import click
from dbhelper.sqlprocess import process_sql, process_utils as ut
import logging


@click.group()
@click.option('--log-level', default="INFO", show_default=True, help="the level is one of the predefined levels (CRITICAL, ERROR, WARNING, INFO, DEBUG)")
def cli(log_level: str):
    logging.basicConfig(
        level=logging.getLevelName(log_level),
        datefmt=r"%Y-%m-%dT%H:%M:%S",
        format="%(asctime)s.%(msecs)03d %(levelname)s  %(message)s"
    )
    pass


@click.command(help="Process sql file")
@click.argument('file', required=True)
@click.option('-c', '--config', default="config.ini", show_default=True, help="config file")
def process(file: str, config: str):
    try:
        process_sql.process(file, config)
    except Exception as ex:
        click.echo("Error: %s" % str(ex), err=True)


@click.command(help="quote password string")
@click.option('--password', prompt='Password', hide_input=True)
def password(password: str):
    click.echo(ut.quote_password(password))


cli.add_command(process)
cli.add_command(password)


def main():
    cli()


if __name__ == '__main__':
    main()
