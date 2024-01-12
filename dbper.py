#!/usr/bin/env python3
import click
import logging
from dbhelper import cmd
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


@click.command(help="quote password(encode password) string")
@click.option("--password", prompt="Password", hide_input=True)
def password(password: str):
    click.echo(quote_plus(password))


cli.add_command(password)
cli.add_command(cmd.cli_sql.process)
cli.add_command(cmd.cli_parquet.parquet)
cli.add_command(cmd.cli_csv.csv)


def main():
    cli()


if __name__ == "__main__":
    main()
