import os
import logging
import logging
from configparser import ConfigParser
from datetime import datetime
from sqlalchemy import Engine
from dbhelper.sqlprocess import process_utils as ut
import warnings
warnings.filterwarnings('ignore')


log: logging.Logger = logging.getLogger()

support_vars = [
    dict(name='crontab', required=False, help="Crontab string (default: None is not process automatically)"),
    dict(name='name', required=False, help="Name of process (default: Name of file)"),
    dict(name='output', required=False, help="output type (default: none )"),

]


def process(file: str, dsn: str) -> bool:
    if not file.endswith(".sql") and os.path.exists(file):
        log.error(f"error file: {file}.")
        return False
    log.debug("connect vertica")
    engine: Engine = ut.connection_vertica(dsn=dsn)
    with engine.raw_connection().cursor() as cur:
        log.info(f"Start process: {file}")
        data = get_sql(file)
        log.info("Jobname: %s", data['vars']['name'])
        for datasql in data['sqls']:
            sql = datasql['sql']
            if len(sql) < 5:
                log.warn(f"Skill {datasql['name']}, sql: {datasql['sql']}")
                continue
            dt = datetime.now()
            log.info(f"Start execute: {datasql['name']}")
            log.debug(f"sql: {sql}")
            cur.execute(sql)
            log.info(f"Ended execute: {datasql['name']}, Duration: {datetime.now() - dt}")
    # click.echo(json.dumps(, indent=2), color=True)
    engine.dispose()
    return True


def check_format(file: str):
    if not file.endswith(".sql") and os.path.exists(file):
        log.error(f"error file: {file}.")
        return False

    with open(file, 'r', encoding='utf-8') as f:
        for line in f:
            if not line.startswith("--") and line.endswith(";"):
                return True
    return False


def get_sql(file: str) -> dict:
    if not file.endswith(".sql") and os.path.exists(file):
        log.error(f"error file: {file}.")
        return False
    vars = dict(
        version="1",
        crontab=None,
        name=os.path.basename(file).rstrip(".sql"),
        output=None
    )

    with open(file, 'r', encoding='utf-8') as f:
        s: str = ""
        for line in f:
            line = line.strip("\n \t")
            if len(line) == 0:
                continue
            s += line + "\n"
        sqlfile: list[str] = s.split("---\n")

    if len(sqlfile) > 1:
        isVar = False
        tmps = sqlfile
        for i, tmp in enumerate(sqlfile):
            for s in tmp.split("\n"):
                s = s.strip('-').strip()
                if s.startswith("VAR_"):
                    isVar = True
                    c = s.removeprefix("VAR_").split(":")
                    if len(c) == 2:
                        c[0] = c[0].strip().rstrip(":").lower()
                        c[1] = c[1].strip()
                        vars[c[0]] = c[1]
                        continue
            tmps.pop(0)
            if isVar:
                break
        sqlfile = tmps
    sqls: list[dict[str, str]] = []
    for i, s in enumerate(sqlfile):
        sqls.append(dict(name=f"No.{(i+1):3}", sql=""))
        for line in s.split("\n"):
            if line.startswith("--"):
                if sqls[i]["name"].startswith("No.") and len(line.strip('-')) > 1:
                    sqls[i]['name'] += " - " + line.strip('-')
                continue
            sqls[i]["sql"] += line + "\n"

    return dict(vars=vars, sqls=sqls)
