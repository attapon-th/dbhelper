import os


def get_dsn(dsn: str | None) -> str:
    if dsn is not None:
        return dsn

    if dsn == "DB_DSN":
        dsn = os.getenv("DB_DSN", "")
        return dsn

    if dsn == "":
        raise Exception("DSN is required")
    raise Exception("Get DSN Error")
