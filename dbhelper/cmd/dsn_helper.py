import os
from typing import Any
from dacutil import get_config
from sqlalchemy.engine import make_url, URL


def get_dsn(dsn, config_file, keys) -> str:
    if config_file is not None:
        conf = get_config(config_file)
        dsn = config_keys(conf, keys)

    if not check_dsn(dsn):
        dsn = get_dsn_string(dsn)

    return dsn


def check_dsn(dsn: str):
    if dsn is None:
        return False
    try:
        u: URL = make_url(dsn)
        return u is not None
    except:
        return False


def get_dsn_string(dsn: str | None) -> str:
    if dsn is not None:
        return dsn

    if dsn == "DB_DSN":
        dsn = os.getenv("DB_DSN", "")
        return dsn

    if dsn == "":
        raise Exception("DSN is required")
    raise Exception("Get DSN Error")


def config_keys(conf, keys: str) -> Any:
    sp = keys.split(".")

    key = sp[0]
    if key not in conf:
        return None
    conf = conf.get(key)
    if len(sp) > 1:
        return config_keys(conf, ".".join(sp[1:]))
    return conf
