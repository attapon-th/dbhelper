
import os
from configparser import ConfigParser
from typing import Union
from sqlalchemy.engine import Engine, URL, create_engine
import urllib.parse

_url = URL
create_url: _url = URL.create


def connection_vertica(dsn: Union[str, _url], echo: bool = False, **kwargs) -> Engine:
    kwargs['echo'] = echo
    return create_engine(dsn, **kwargs)


def quote_password(str: str) -> str:
    return urllib.parse.quote(str)


def read_config(file: str) -> ConfigParser:
    if not os.path.exists(file):
        raise FileNotFoundError(f"file not found: {file}")
    config = ConfigParser()
    config.read(file)
    return config
