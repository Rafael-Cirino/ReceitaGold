from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any, ClassVar, Self

import polars as pl
from loguru import logger
from sqlalchemy import Connection, CursorResult, Engine, create_engine, text
from tqdm import tqdm

from config.settings import settings
from pipeline.silver.schema import (
    FILE_COLUMNS,
    RECEITA_BRONZE_PATH,
    UNZIP_TMP_PATH,
    FileColumnConfig,
)
from pipeline.silver.utils import convert_to_utf8
from pipeline.sql_reader import SQLiteReader
from pipeline.utils import create_sql_index, split_name_number, unzip_file_with_progress


def write_db_by_batches(df: pl.LazyFrame, table_name: str, db_url: str):
    length = df.select(pl.len()).collect(engine="streaming").item()
    batches = 500000
    total_batches = (length + 100 - 1) // batches
    for offset in tqdm(
        range(0, length, batches), total=total_batches, desc=f"Loading {table_name}"
    ):
        batch_df = df.slice(offset, batches).collect(engine="streaming")
        batch_df.write_database(
            table_name=table_name,
            connection=db_url,
            if_table_exists="append",
        )


class ReceitaLoader:
    def __init__(self, receita_path: str | Path = RECEITA_BRONZE_PATH) -> None:
        """
        Initialize the loader and pre-load the set of food-related CNAE codes.
        """
        self.receita_path = Path(receita_path)

        df_public_company = pl.read_csv(
            self.receita_path / "naturezas_nao_empresas.csv",
            schema_overrides={"natureza_code": pl.UInt64()},
            separator=";",
        )

        self.public_company: set[int] = set(
            df_public_company["natureza_code"].to_list()
        )

    def _load_csv_from_zip(
        self, filename: str, columns_type: FileColumnConfig | None = None
    ) -> pl.LazyFrame | None:
        """
        Unzip a Receita Federal archive, convert to UTF-8, and return a LazyFrame.
        """
        zip_path = self.receita_path / f"{filename}.zip"
        if not zip_path.exists():
            return None

        extracted_files = unzip_file_with_progress(zip_path, UNZIP_TMP_PATH)
        utf8_path = convert_to_utf8(UNZIP_TMP_PATH, extracted_files[0])

        if columns_type is None:
            return pl.scan_csv(utf8_path, separator=";")

        cols_types = columns_type["cols"]
        use_cols = columns_type.get("use_cols")

        df = pl.scan_csv(
            utf8_path,
            separator=";",
            new_columns=list(cols_types.keys()),
            decimal_comma=True,
            schema_overrides=cols_types,
        )

        if use_cols is None:
            return df
        return df.drop(use_cols)

    def load_empresas(self, fname: str) -> pl.LazyFrame | None:
        """
        Load and filter the empresas dataset.
        """
        name, _ = split_name_number(fname)
        data = self._load_csv_from_zip(fname, FILE_COLUMNS[name])
        if data is None:
            return None

        data = data.filter(~pl.col("natureza_code").is_in(self.public_company))

        return data

    def load_generic(self, fname: str) -> pl.LazyFrame | None:
        """
        Load a generic dataset.
        """
        name, _ = split_name_number(fname)
        return self._load_csv_from_zip(fname, FILE_COLUMNS[name])

    def load(self, fname: str) -> pl.LazyFrame | None:
        """
        Dispatch to the appropriate loader based on the dataset name.
        """
        name, _ = split_name_number(fname)

        method = getattr(self, f"load_{name}", None)
        if method is None:
            return self.load_generic(fname)

        return method(fname)

    def cleanup(self) -> None:
        """Remove the temporary extraction directory if it exists."""
        shutil.rmtree(UNZIP_TMP_PATH, ignore_errors=True)


class SilverLayerDatabase:
    """

    Manages the Silver Layer SQLite database for structured Receita data.

    """

    # Fix: Map directly to SQL strings to avoid stringifying SQLAlchemy classes

    TYPE_MAP: ClassVar[dict[type, str]] = {
        pl.String: "TEXT",
        pl.Int64: "INTEGER",
        pl.Int32: "INTEGER",
        pl.Int16: "INTEGER",
        pl.UInt64: "INTEGER",
        pl.UInt32: "INTEGER",
        pl.UInt16: "INTEGER",
        pl.Float64: "REAL",
        pl.Boolean: "INTEGER",
        pl.Date: "TEXT",
        pl.Datetime: "TEXT",
    }

    def __init__(self, db_url: str = "sqlite:///data/silver.db") -> None:
        self.db_url = db_url
        self.engine: Engine | None = None
        self.conn: Connection | None = None

    def __enter__(self) -> Self:
        if self.db_url.startswith("sqlite:///"):
            db_path = self.db_url[len("sqlite:///") :]

            if db_path != ":memory:":
                Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        self.engine = create_engine(self.db_url)
        self.conn = self.engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        )

        self.execute("PRAGMA journal_mode=DELETE")

        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.conn:
            self.conn.close()

        if self.engine:
            self.engine.dispose()

    def _polars_type_to_sql(
        self, dtype: pl.PolarsDataType | type[pl.PolarsDataType]
    ) -> str:
        """
        Convert a Polars data type to SQLite column type safely.
        """

        if hasattr(dtype, "inner"):
            dtype = dtype.inner

        # Ensure we always get the class type (handles both pl.String and pl.String())
        dtype_class = type(dtype) if not isinstance(dtype, type) else dtype

        return self.TYPE_MAP.get(dtype_class, "TEXT")

    def execute(
        self, sql: str, params: dict | list[dict] | None = None
    ) -> CursorResult[Any]:
        if self.conn is None:
            raise RuntimeError("Database not connected. Use context manager.")

        return self.conn.execute(text(sql), params or {})

    def create_table(
        self,
        table_name: str,
        schema: dict[str, pl.PolarsDataType],
        index_columns: list[str | list[str]] | None = None,
    ) -> None:
        if not table_name.isidentifier():
            raise ValueError(f"Invalid table name: {table_name}")

        if self.conn is None:
            raise RuntimeError("Database not connected. Use context manager.")

        columns = []

        for col_name, col_type in schema.items():
            sql_type = self._polars_type_to_sql(col_type)

            columns.append(f'"{col_name}" {sql_type}')

        create_sql = f"""
            CREATE TABLE IF NOT EXISTS "{table_name}" (
                {", ".join(columns)}
            ) STRICT
        """

        self.execute(create_sql)

        # --- CÓDIGO NOVO PARA CRIAR OS ÍNDICES ---
        if index_columns:
            for index_sql in create_sql_index(index_columns, table_name):
                self.execute(index_sql)

    def insert_data(self, table_name: str, records: list[dict]) -> int:
        if not records:
            return 0

        if self.conn is None:
            raise RuntimeError("Database not connected. Use context manager.")

        if not table_name.isidentifier():
            raise ValueError(f"Invalid table name: {table_name}")

        columns = list(records[0].keys())
        placeholders = ", ".join([f":{col}" for col in columns])
        quoted_cols = ", ".join([f'"{col}"' for col in columns])
        insert_sql = f"""
            INSERT INTO "{table_name}" ({quoted_cols})
            VALUES ({placeholders})
        """

        self.execute(insert_sql, records)
        return len(records)

    def query_distinct(self, table_name: str, column: str) -> set:
        if self.conn is None:
            raise RuntimeError("Database not connected. Use context manager.")

        if not table_name.isidentifier() or not column.isidentifier():
            raise ValueError("Invalid table or column name")

        query = f'SELECT DISTINCT "{column}" FROM "{table_name}"'
        cursor = self.execute(query)

        return {row[0] for row in cursor.fetchall() if row[0] is not None}

    def get_row_count(self, table_name: str) -> int:
        if self.conn is None:
            raise RuntimeError("Database not connected. Use context manager.")

        if not table_name.isidentifier():
            raise ValueError(f"Invalid table name: {table_name}")

        query = f'SELECT COUNT(*) FROM "{table_name}"'
        cursor = self.execute(query)
        return cursor.fetchone()[0] or 0

    def table_exists(self, table_name: str) -> bool:
        if self.conn is None:
            raise RuntimeError("Database not connected. Use context manager.")

        cursor = self.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=:name",
            {"name": table_name},
        )

        return cursor.fetchone() is not None


class SilverPipeline:
    """
    Orchestrates the Silver Layer ingestion pipeline for Receita Federal data.
    """

    SINGLE_FILE_DATASETS: ClassVar[list[str]] = [
        "Motivos",
        "Municipios",
        "Naturezas",
        "Paises",
        "Qualificacoes",
        "Cnaes",
        "Simples",
    ]

    def __init__(
        self,
        receita_path: str | Path = RECEITA_BRONZE_PATH,
        db_url: str | None = None,
    ) -> None:
        self.receita_path = Path(receita_path)
        if db_url is None:
            db_url = f"sqlite:///{settings.data_silver_path / 'silver.db'}"

        self.db_url = db_url
        self.loader = ReceitaLoader(receita_path)

    def _load_cnae_food(self) -> None:
        logger.info("Processing cnae_alimenticio (single-file)...")

        table_name = "cnae_alimenticio"
        self._create_dataset_table(table_name)

        df = pl.scan_csv(
            self.receita_path / f"{table_name}.csv",
            separator=";",
            schema_overrides={"cnaes_code": pl.String()},
        )
        if df is not None:
            write_db_by_batches(df, table_name, self.db_url)
        else:
            logger.warning("No data loaded for {}.", table_name)

    def _create_dataset_table(self, table_name: str) -> None:
        schema = FILE_COLUMNS[table_name]["cols"]
        index = FILE_COLUMNS[table_name].get("index")

        with SilverLayerDatabase(self.db_url) as db:
            db.create_table(table_name, schema, index_columns=index)

    def _load_matching_files(self, table_name: str, pattern: str) -> None:
        self._create_dataset_table(table_name)

        for fpath in self.receita_path.glob(pattern):
            fname = fpath.stem
            logger.info("Loading {}...", fname)
            df = self.loader.load(fname)
            if df is not None:
                write_db_by_batches(df, table_name, self.db_url)
                logger.success("Inserted records.")

    def _load_single_file_datasets(self) -> None:
        for fname in self.SINGLE_FILE_DATASETS:
            table_name = fname.lower()
            logger.info("Processing {}...", table_name)
            self._create_dataset_table(table_name)

            df = self.loader.load(fname)
            if df is not None:
                write_db_by_batches(df, table_name, self.db_url)
            else:
                logger.warning("No data loaded for {}.", table_name)

    def _load_estabelecimentos(self) -> None:
        logger.info("Processing estabelecimentos (multi-file)...")
        self._load_matching_files("estabelecimentos", "Estabelecimentos*.zip")

    def _load_empresas(self) -> None:
        logger.info("Processing empresas (multi-file)...")
        self._load_matching_files("empresas", "Empresas*.zip")

    def _load_socios(self) -> None:
        logger.info("Processing socios (multi-file, filtered)...")

        with SQLiteReader(self.db_url) as db:
            query = """
                SELECT cnpj_base FROM estabelecimentos
                UNION
                SELECT cnpj_base FROM empresas
            """
            # db.execute(query) deve retornar um cursor ou lista de tuplas
            existing_cnpj_bases = db.query(query).lazy()

        logger.info(
            "Found {} unique cnpj_base values in estabelecimentos.",
            existing_cnpj_bases.collect().height,
        )

        schema = FILE_COLUMNS["socios"]["cols"]
        index = FILE_COLUMNS["socios"].get("index", None)
        with SilverLayerDatabase(self.db_url) as db:
            db.create_table("socios", schema, index_columns=index)

        for fpath in self.receita_path.glob("Socios*.zip"):
            fname = fpath.stem
            logger.info("Loading {}...", fname)
            df = self.loader.load(fname)
            if df is not None:
                df = df.join(existing_cnpj_bases, on="cnpj_base", how="semi")
                write_db_by_batches(df, "socios", self.db_url)
                logger.success("Inserted filtered socios records.")

        logger.success("Socios loading complete.")

    def run(self) -> None:
        self._load_cnae_food()
        # self._load_single_file_datasets()
        # self._load_estabelecimentos()
        # self._load_empresas()
        # self._load_socios()

        # Cleanup temporary files
        self.loader.cleanup()
        logger.success("Pipeline complete. Temporary files cleaned up.")


def run_silver_pipeline(
    receita_path: str | Path = RECEITA_BRONZE_PATH,
    db_url: str | None = None,
) -> None:
    pipeline = SilverPipeline(receita_path=receita_path, db_url=db_url)
    pipeline.run()


def main() -> None:
    pipeline = SilverPipeline()
    pipeline.run()


if __name__ == "__main__":
    main()
