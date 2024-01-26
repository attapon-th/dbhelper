from sqlalchemy.engine import Engine
from typing import Literal
from .dbuitl import DBUtil


class VerticaCopy(DBUtil):
    def __init__(self, engine: Engine, schema: str, table: str) -> None:
        super().__init__(engine, schema)
        self.table = table
        self._from_input = "STDIN"
        self._compression: Literal["BZIP", "GZIP", ""] = ""
        self._columns: list[str] = []

    def input(self, filepath_or_stdin: str = "STDIN"):
        """
        Sets the input file path for the COPY operation.

        Args:
            filepath_or_stdin (str, optional): The path to the input file. Defaults to "STDIN".

        Returns:
            self

        """
        if filepath_or_stdin.upper() != "STDIN":
            filepath_or_stdin = "'" + filepath_or_stdin + "'"
        return self

    def commpression(self, compression: Literal["BZIP", "GZIP", ""] = ""):
        """
        Sets the compression type for the COPY operation.

        Args:
            compression (Literal["BZIP", "GZIP", ""]): The compression type. Defaults to "" is no compression.

        Returns:
            self
        """
        self._compression = compression
        return self

    def columns(self, columns: list[str]):
        """
        Sets the csv columns for the COPY operation.

        Args:
            columns (list[str]): The list of csv columns to include in the COPY operation.

        Returns:
            self

        """
        self._columns = columns
        return self

    def get_sql(self) -> str:
        """
        Returns the SQL for the COPY operation.

        Args:
            reject_table (str | None, optional): The name of the rejected table. Defaults to None is {schema}.REJECT_{table}.

        Returns:
            str: The SQL for the COPY operation.
        """
        self.check_connect()
        schema: str = self.schema
        table: str = self.table
        colStr = ""
        if len(self._columns) > 0:
            colStr = f"({', '.join(self._columns)})"
        sql: str = f"COPY {schema}.{table}{colStr} FROM {self._from_input} {self._compression} PARSER fcsvparser() \n"
        sql += f"REJECTED DATA AS TABLE {schema}.__REJECT_{table}"
        sql += ";"
        return sql

    def copy(self, csv) -> bool:
        """
        Executes the COPY operation.

        Args:
            csv (str | bytes | StringIO | BytesIO | TextIOWrapper | file): The csv data to copy.
                It can be any object that implements a read() method

        Returns:
            bool | None: True if the COPY operation was successful, False otherwise.
        """
        self.check_connect()
        table: str = self.table
        conn = self.engine.raw_connection().driver_connection
        if conn is None:
            return False
        try:
            src_columns: list[str] = self._columns
            tgt_column: list[str] = self.get_column_names(table)

            # columns is src_columns intersect tgt_column
            columns: list[str] = [a for a in src_columns if a in tgt_column]

            sql: str = self.columns(columns).get_sql()

            if not hasattr(conn, "cursor"):
                print("cursor not support")
                return False

            cur = conn.cursor()
            if not hasattr(cur, "copy"):
                print("copy not support")
                return False
            cur.copy(sql, csv, buffer_size=65536)

        except Exception as err:
            conn.rollback()
            print(err)

        return False
