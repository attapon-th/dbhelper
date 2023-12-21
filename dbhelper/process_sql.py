import os
import logging
from datetime import datetime
from sqlalchemy.engine import Engine, create_engine, make_url, URL
from typing import List, Dict

import warnings

warnings.filterwarnings("ignore")


log: logging.Logger = logging.getLogger()


def file_sql(file: str) -> List[Dict[str, List[str]]]:
    if not file.endswith(".sql") and os.path.exists(file):
        log.error(f"error file: {file}")
        return []
    lines: List[str] = open(file, "r", encoding="utf-8").readlines()
    if len(lines) == 0:
        log.error(f"error empty file: {file}")
        return []

    lines = [l.rstrip("\n") for l in lines if l.strip() != ""]
    sqls: List[Dict[str, List[str]]] = [{"name": [], "sql": []}]
    i = 0
    isSQL = False
    for line in lines:
        if len(sqls) <= i:
            sqls.append({"name": [], "sql": []})
        if line.startswith("--"):
            if isSQL is False:
                line = line.strip("-").strip()
                if len(line) > 0:
                    sqls[i]["name"].append(line)
            continue
        elif line.endswith(";"):
            sqls[i]["sql"].append(line)
            i += 1
            isSQL = False
            continue
        else:
            isSQL = True
            sqls[i]["sql"].append(line)

    return sqls


def process_sql(file: str, dsn: str) -> bool:
    u: URL = make_url(dsn)
    engine: Engine = create_engine(u.render_as_string(hide_password=False))

    conn = engine.raw_connection()
    cur = conn.cursor()

    sqls = file_sql(file)
    if len(sqls) == 0:
        return False

    log.info(f"Start process: {file}")
    try:
        for sql in sqls:
            dt = datetime.now()
            name = "  ".join(sql["name"])
            sql = " \n".join(sql["sql"])
            log.info(f"Start execute: {name}")
            log.debug(f"sql: {sql}")
            cur.execute(sql)
            log.info(f"Ended execute: {name}, Duration: {datetime.now() - dt}")
    except Exception as e:
        log.error(e)
        return False
    finally:
        cur.close()
        conn.close()
        engine.dispose()
    return True
