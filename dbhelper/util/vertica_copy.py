from sqlalchemy.engine import Engine
from pandas import DataFrame
from typing import Literal
from .dbuitl import DBUtil


class VerticaCopy(DBUtil):
    def __init__(self, engine: Engine, schema: str) -> None:
        super().__init__(engine, schema)

        self._from_input = "STDIN"
        self._compression: Literal["BZIP", "GZIP", ""] = ""
        self._columns: list[str] = []

    def input(self, filepath: str):
        if filepath.upper() != "STDIN":
            filepath = "'" + filepath + "'"
        return self

    def commpression(self, compression: Literal["BZIP", "GZIP", ""]):
        self._compression = compression
        return self

    def columns(self, columns: list[str]):
        self._columns = columns
        return self

    def get_sql(self, table: str, reject_table: str | None = "") -> str:
        self.check_connect()
        schema: str = self.schema

        colStr = ""
        if len(self._columns) > 0:
            colStr = f"({', '.join(self._columns)})"
        sql: str = f"COPY {schema}.{table}{colStr} FROM {self._from_input} {self._compression} PARSER fcsvparser()"
        if reject_table is not None:
            sql += f" REJECTED DATA AS TABLE {schema}.__REJECT_{table}"
        return sql

    def copy(self, df: DataFrame, table: str) -> bool:
        self.check_connect()
        conn = self.engine.raw_connection().driver_connection
        if conn is None:
            return False
        try:
            src_columns: list[str] = df.columns.to_list()
            tgt_column: list[str] = self.get_column_names(table)

            # columns is src_columns intersect tgt_column
            columns: list[str] = [a for a in src_columns if a in tgt_column]

            sql: str = self.columns(columns).get_sql(table)

            if not hasattr(conn, "cursor"):
                print("cursor not support")
                return False

            cur = conn.cursor()
            if not hasattr(cur, "copy"):
                print("copy not support")
                return False
            cur.copy(sql, df.to_csv(index=False, escapechar='"', quotechar='"'))

        except Exception as err:
            conn.rollback()
            print(err)

        return False
