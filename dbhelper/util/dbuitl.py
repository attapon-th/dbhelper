import sqlalchemy as sa
from sqlalchemy.engine.interfaces import ReflectedColumn, ReflectedPrimaryKeyConstraint
from sqlalchemy import Engine
from typing import List, Any


class DBUtil:
    def __init__(self, engine: Engine, schema_or_dbname: str) -> None:
        """
        Initializes a new instance of the class.

        Args:
            engine (Engine): The engine object used to connect to the database.
            schema_or_dbname (str): The name of the schema or database.

        Returns:
            None
        """
        self.engine: Engine = engine
        self.schema: str = schema_or_dbname
        self.conn = engine.connect()
        self.closed: bool = True

    def execute(self, sql: str, parameters: Any = None):
        """
        Execute the given SQL query with optional parameters.

        Args:
            sql (str): The SQL query to be executed.
            parameters (Any, optional): Optional parameters for the SQL query. Defaults to None.

        Returns:
            The result of executing the SQL query.
        """
        self.check_connect()
        return self.conn.execute(sa.text(sql), parameters)

    def close(self):
        """
        Closes the connection and disposes of the engine.
        """
        self.closed = True
        self.conn.close()
        self.engine.dispose()

    def check_connect(self):
        """
        This function checks the connection status and opens a new connection if it is closed.
        No parameters are required, and no return type is specified.
        """
        if self.closed:
            self.conn = self.engine.connect()
            self.closed = False

    def has_tables(self, table: str) -> bool:
        """
        Checks if the given table exists in the database.

        Args:
            table (str): The name of the table to check.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        self.check_connect()
        return self.conn.dialect.has_table(self.conn, table, schema=self.schema)

    def list_tables(self) -> List[str]:
        """
        Lists all tables in the database.

        Returns:
            List[str]: A list of table names.
        """
        self.check_connect()
        return self.conn.dialect.get_table_names(self.conn, self.schema)

    def get_columns(self, table: str) -> list[ReflectedColumn]:
        """
        Returns a list of columns in the given table.

        Args:
            table (str): The name of the table to retrieve columns from.

        Returns:
            list[ReflectedColumn]: A list of ReflectedColumn objects representing the columns in the table.
        """
        self.check_connect()
        return self.conn.dialect.get_columns(self.conn, table, self.schema)

    def get_column_names(self, table: str) -> list[str]:
        """
        Returns a list of column names in the given table.

        Args:
            table (str): The name of the table to retrieve column names from.

        Returns:
            list[str]: A list of column names in the table.
        """
        self.check_connect()
        return [c["name"] for c in self.get_columns(table)]

    def get_primarykeys(self, table: str) -> list[str]:
        """
        Returns a list of primary keys in the given table.

        Args:
            table (str): The name of the table to retrieve primary keys from.

        Returns:
            list[str]: A list of primary keys in the table.
        """
        self.check_connect()
        pk_cons: ReflectedPrimaryKeyConstraint = self.conn.dialect.get_pk_constraint(self.conn, table, self.schema)
        has_pk_constraint: bool = isinstance(pk_cons, dict) and "constrained_columns" in pk_cons and len(pk_cons["constrained_columns"]) > 0
        if has_pk_constraint:
            return pk_cons["constrained_columns"]

        columns: list[ReflectedColumn] = self.conn.dialect.get_columns(self.conn, table, schema=self.schema)
        return [c["name"] for c in columns if "primary_key" in c and c["primary_key"] is True]
