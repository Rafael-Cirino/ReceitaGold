from typing import Self

import duckdb
import polars as pl

from config.settings import MAX_MEMORY


class SQLiteReader:
    def __init__(self, db_path: str, max_memory: str = MAX_MEMORY) -> None:
        self.db_path = db_path.replace("sqlite:///", "")
        self.max_memory = max_memory
        self._con: duckdb.DuckDBPyConnection | None = None

    def _connect(self) -> duckdb.DuckDBPyConnection:
        if self._con is None:
            self._con = duckdb.connect()
            self._con.execute(f"SET max_memory = '{self.max_memory}'")
            self._con.execute(f"ATTACH '{self.db_path}' AS sqlite_db (TYPE SQLITE)")
            self._con.execute("USE sqlite_db")
        return self._con

    def query(self, sql: str) -> pl.DataFrame:
        return self._connect().sql(sql).pl()

    def get_tables(self) -> list[str]:
        return self.query("SELECT name FROM sqlite_master WHERE type = 'table'")[
            "name"
        ].to_list()

    def read_table(self, table: str) -> pl.DataFrame:
        return self.query(f'SELECT * FROM "{table}"')

    def close(self) -> None:
        if self._con is not None:
            self._con.close()
            self._con = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
