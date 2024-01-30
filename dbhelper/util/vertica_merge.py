from sqlalchemy.engine import Engine
from typing import List, Any, Dict, Optional
from .dbuitl import DBUtil


class VerticaMerge(DBUtil):
    def __init__(self, engine: Engine, source_schema: str, source_table: str, target_schema: str, target_table: str) -> None:
        """
        Initializes the DataMover object with the provided engine, source schema, and source table.

        Args:
            engine (Engine): The database engine to use.
            source_schema (str): The source schema to use for data extraction.
            source_table (str): The source table to use for data extraction.
            target_schema (str): The target schema to use for data insertion.
            target_table (str): The target table to use for data insertion.

        Returns:
            None
        """
        super().__init__(engine, source_schema)
        self._targetdb: DBUtil

        self._source_schema: str = source_schema
        self._source_table: str = source_table

        self._target_schema: str = target_schema
        self._target_table: str = target_table
        self.target(target_schema, target_table)

        self._on_columns: list[str] = []
        self._insert_columns: list[str] = []
        self._update_columns: list[str] = []

    def source(self, schema: str, table: str):
        """
        Sets the source schema and table for the merge operation.

        Args:
            schema (str): The name of the source schema.
            table (str): The name of the source table.

        Returns:
            self
        """
        self._source_schema = schema
        self._source_table = table
        return self

    def target(self, schema: str, table: str):
        """
        Sets the target schema and table for the merge operation.

        Args:
            schema (str): The name of the target schema.
            table (str): The name of the target table.

        Returns:
            self
        """
        self._target_schema = schema
        self._target_table = table
        self._targetdb = DBUtil(self.engine, schema)
        return self

    def on(self, on_columns: list[str] | None = None):
        """
        set vertica sql Merge  `Merge {target_table} Using {source_table} ON {on_columns}`

        Args:
            on_columns: Optional list of strings representing the columns to join on. (default: all primary keys in target table)
        Returns:
            The modified object.
        """
        if on_columns is not None:
            self._on_columns = on_columns

        if self._targetdb is None or self._target_table is None:
            raise Exception("target table not set")
        self._on_columns: list[str] = self._targetdb.get_primarykeys(self._target_table)
        return self

    def insert(self, insert_columns: Optional[List[str]] = None):
        """
        Set vertica sql Merge `WHEN NOT MATCHED THEN INSERT () VALUES ()`

        Args:
            insert_columns: Optional list of strings representing the columns to insert. (default: all source columns)

        Returns:
            self
        """
        if insert_columns is not None:
            self._insert_columns = insert_columns

        if self._targetdb is None or self._target_table is None:
            raise Exception("target table not set")
        tgt_cols: list[str] = self._targetdb.get_column_names(self._target_table)
        src_cols: list[str] = self.get_column_names(self._source_table)
        cols: list[str] = [c for c in src_cols if c in tgt_cols]
        self._insert_columns = cols
        return self

    def update(self, update_columns: Optional[List[str]] = None):
        """
        Set vertica sql Merge `WHEN MATCHED THEN UPDATE SET ()`

        Args:
            update_columns: Optional list of strings representing the columns to update. (default: all source columns)

        Returns:
            self
        """
        if update_columns is not None:
            self._update_columns = update_columns

        if len(self._on_columns) == 0:
            self.on()
        if self._targetdb is None or self._target_table is None:
            raise Exception("target table not set")
        tgt_cols: list[str] = self._targetdb.get_column_names(self._target_table)
        src_cols: list[str] = self.get_column_names(self._source_table)
        cols: list[str] = [c for c in src_cols if c in tgt_cols and c not in self._on_columns]
        self._update_columns = cols
        return self

    def get_sql(self, more_insert: Optional[Dict[str, str]] = None, more_update: Optional[Dict[str, str]] = None) -> str:
        """
        Get vertica sql Merge

        Args:
            more_insert: Optional dict of strings including columns and values
            more_update: Optional dict of strings including columns and values

        Returns:
            str vertica sql
        """
        if self._targetdb is None or self._target_table is None:
            raise Exception("target table not set")
        if len(self._on_columns) == 0:
            self.on()
        if len(self._insert_columns) == 0:
            self.insert()
        if len(self._update_columns) == 0:
            self.update()
        # target table alias name t
        sql: str = f"MERGE INTO {self._target_schema}.{self._target_table} t \n"

        # source table alias name s
        sql += f"USING {self._source_schema}.{self._source_table} s ON \n"
        merge_on = " AND ".join([f"t.{a} = s.{a}" for a in self._on_columns])
        sql += f"{merge_on} \n"

        # UPDATE
        update_set: str = ", ".join([f"{a} = s.{a}" for a in self._update_columns])
        if more_update is not None:
            update_set = update_set + ", " + ", ".join([f"{k} = {v}" for k, v in more_update.items()])
        sql += f"WHEN MATCHED THEN UPDATE SET {update_set} \n"

        # INSERT cols
        insert_cols = ",".join(self._insert_columns)
        insert_values: str = ",".join([f"s.{a}" for a in self._insert_columns])
        if more_insert is not None:
            insert_cols: str = insert_cols + "," + ",".join(more_insert.keys())
            insert_values = insert_values + "," + ",".join(more_insert.values())
        sql += f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_values}) \n"
        sql += ";"
        return sql

    def merge(self, more_insert: Optional[Dict[str, str]] = None, more_update: Optional[Dict[str, str]] = None) -> Any:
        """
        Execute vertica sql Merge

        Args:
            more_insert: Optional dict of strings including columns and values
            more_update: Optional dict of strings including columns and values

        Returns:
            Any
        """
        self.check_connect()
        sql: str = self.get_sql(more_insert, more_update)
        cur = self._targetdb.execute(sql)
        return cur.fetchone()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        """
        Closes the connection and disposes of the engine.
        """
        self._targetdb.close()
        super().close()
        self.engine.dispose()
