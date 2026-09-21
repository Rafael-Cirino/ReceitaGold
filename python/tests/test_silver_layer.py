"""
Test suite for the Silver Layer ingestion pipeline.

Tests cover:
    - SilverLayerDatabase operations (table creation, insert, query)
    - Sequential loading execution order
    - Socios filtering by existing estabelecimentos cnpj_base
    - Multi-file streaming / memory-efficient chunking
"""

from __future__ import annotations

import zipfile
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from pipeline.silver.loader import (
    ReceitaLoader,
    SilverLayerDatabase,
    run_silver_pipeline,
    write_db_by_batches,
)
from pipeline.silver.schema import FILE_COLUMNS

# ── Helpers / Fixtures ─────────────────────────────────────────────────────────


@pytest.fixture()
def db() -> SilverLayerDatabase:
    """Create an in-memory SilverLayerDatabase for testing (using context manager)."""
    with SilverLayerDatabase("sqlite:///:memory:") as db:
        yield db


@pytest.fixture()
def sample_paises_schema() -> dict[str, Any]:
    return FILE_COLUMNS["paises"]["cols"]


@pytest.fixture()
def sample_paises_records() -> list[dict[str, Any]]:
    return [
        {"pais_code": 105, "pais_name": "Brasil"},
        {"pais_code": 230, "pais_name": "Estados Unidos"},
    ]


@pytest.fixture()
def temp_receita_dir(tmp_path: Path) -> Path:
    """Create a temporary directory mimicking the receita structure."""
    receita_dir = tmp_path / "receita"
    receita_dir.mkdir()
    return receita_dir


def _create_mock_zip(
    dest_dir: Path,
    base_name: str,
    csv_content: str,
    sep: str = ";",
) -> Path:
    """
    Create a minimal ZIP file containing a CSV in the destination directory.

    Parameters
    ----------
    dest_dir : Path
        Directory where the ZIP will be created.
    base_name : str
        Base filename (without extension). The ZIP will be named ``{base_name}.zip``.
    csv_content : str
        Raw CSV content to pack inside the ZIP.
    sep : str, optional
        Separator used in the CSV (default ``";"``).

    Returns
    -------
    Path
        Path to the created ZIP file.
    """
    zip_path = dest_dir / f"{base_name}.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"{base_name}.csv", csv_content)
    return zip_path


# ── Tests: SilverLayerDatabase ─────────────────────────────────────────────────


class TestSilverLayerDatabase:
    """Unit tests for SilverLayerDatabase class."""

    def test_create_table(
        self, db: SilverLayerDatabase, sample_paises_schema: dict
    ) -> None:
        """Test that a table is created with correct columns and types."""
        db.create_table("paises", sample_paises_schema)
        assert db.table_exists("paises")

    def test_create_table_invalid_name(
        self, db: SilverLayerDatabase, sample_paises_schema: dict
    ) -> None:
        """Test that invalid table names raise ValueError."""
        with pytest.raises(ValueError, match="Invalid table name"):
            db.create_table("pais-code", sample_paises_schema)

    def test_insert_data(
        self,
        db: SilverLayerDatabase,
        sample_paises_schema: dict,
        sample_paises_records: list[dict],
    ) -> None:
        """Test that records can be inserted and row count is correct."""
        db.create_table("paises", sample_paises_schema)
        count = db.insert_data("paises", sample_paises_records)
        assert count == 2
        assert db.get_row_count("paises") == 2

    def test_insert_data_empty(self, db: SilverLayerDatabase) -> None:
        """Test that inserting an empty list returns 0."""
        count = db.insert_data("paises", [])
        assert count == 0

    def test_insert_data_invalid_table(self, db: SilverLayerDatabase) -> None:
        """Test that inserting into an invalid table name raises ValueError."""
        with pytest.raises(ValueError, match="Invalid table name"):
            db.insert_data("pais-code", [{"pais_code": 1}])

    def test_query_distinct(
        self,
        db: SilverLayerDatabase,
        sample_paises_schema: dict,
        sample_paises_records: list[dict],
    ) -> None:
        """Test query_distinct returns the expected set of values."""
        db.create_table("paises", sample_paises_schema)
        db.insert_data("paises", sample_paises_records)
        codes = db.query_distinct("paises", "pais_code")
        assert codes == {105, 230}

    def test_get_row_count(
        self, db: SilverLayerDatabase, sample_paises_schema: dict
    ) -> None:
        """Test get_row_count returns 0 for empty table."""
        db.create_table("paises", sample_paises_schema)
        assert db.get_row_count("paises") == 0

    def test_table_exists(
        self, db: SilverLayerDatabase, sample_paises_schema: dict
    ) -> None:
        """Test table_exists reflection after creation."""
        assert not db.table_exists("paises")
        db.create_table("paises", sample_paises_schema)
        assert db.table_exists("paises")

    def test_idempotent_create_table(
        self,
        db: SilverLayerDatabase,
        sample_paises_schema: dict,
        sample_paises_records: list[dict],
    ) -> None:
        """Test that creating a table twice does not raise or duplicate data."""
        db.create_table("paises", sample_paises_schema)
        db.create_table(
            "paises", sample_paises_schema
        )  # second call should be harmless
        db.insert_data("paises", sample_paises_records)
        assert db.get_row_count("paises") == 2


# ── Tests: Polars type to SQL type mapping ─────────────────────────────────────


class TestTypeMapping:
    def test_string_maps_to_text(self) -> None:
        db = SilverLayerDatabase("sqlite:///:memory:")
        assert db._polars_type_to_sql(pl.String()) == "TEXT"

    def test_int64_maps_to_integer(self) -> None:
        db = SilverLayerDatabase("sqlite:///:memory:")
        assert db._polars_type_to_sql(pl.Int64()) == "INTEGER"

    def test_float64_maps_to_real(self) -> None:
        db = SilverLayerDatabase("sqlite:///:memory:")
        assert db._polars_type_to_sql(pl.Float64()) == "REAL"

    def test_nullable_type(self) -> None:
        db = SilverLayerDatabase("sqlite:///:memory:")
        # Nullable types should resolve to their inner type
        assert db._polars_type_to_sql(pl.String()) == "TEXT"
        assert db._polars_type_to_sql(pl.Int64()) == "INTEGER"


# ── Tests: Pipeline execution order ────────────────────────────────────────────


class TestPipelineOrder:
    """Tests that the pipeline processes datasets in the exact specified order."""

    @patch("pipeline.silver.loader.SQLiteReader")
    @patch("pipeline.silver.loader.ReceitaLoader")
    @patch("pipeline.silver.loader.SilverLayerDatabase")
    def test_sequential_order(
        self,
        MockDB: MagicMock,
        MockLoader: MagicMock,
        MockSQLiteReader: MagicMock,
        temp_receita_dir: Path,
    ) -> None:
        """
        Verify that datasets are processed in the strict order:
        motivos → municipios → naturezas → paises → qualificacoes → cnaes → simples
        → estabelecimentos → empresas → socios
        """
        loader_instance = MockLoader.return_value
        loader_instance.load.return_value = None
        loader_instance.cleanup.return_value = None

        db_instance = MockDB.return_value.__enter__.return_value

        # SQLiteReader is used to read existing cnpj_base values during socios
        # loading. Return an empty LazyFrame so the pipeline can proceed.
        MockSQLiteReader.return_value.__enter__.return_value.query.return_value.lazy.return_value = pl.LazyFrame()

        run_silver_pipeline(receita_path=temp_receita_dir, db_url="sqlite:///:memory:")

        table_calls = []
        for entry in db_instance.method_calls:
            if (
                entry.args
                and isinstance(entry.args[0], str)
                and entry.args[0].isidentifier()
            ):
                table_calls.append(entry.args[0])

        single_tables = [
            "motivos",
            "municipios",
            "naturezas",
            "paises",
            "qualificacoes",
            "cnaes",
            "simples",
        ]
        multi_tables = ["estabelecimentos", "empresas", "socios"]

        assert all(table in table_calls for table in single_tables), (
            f"Missing tables in {table_calls}"
        )

        for multi in multi_tables:
            multi_idx = table_calls.index(multi)
            for single in single_tables:
                single_idx = table_calls.index(single)
                assert single_idx < multi_idx, f"{single} should come before {multi}"

    def test_pipeline_skips_missing_single_files(self, tmp_path: Path) -> None:
        """
        Missing single-file datasets are reported and skipped; the pipeline
        still loads the multi-file datasets and finishes cleanly.
        """
        receita_dir = tmp_path / "receita"
        receita_dir.mkdir()

        (receita_dir / "cnaes_food.csv").write_text("cnaes_code\n5611201\n")
        (receita_dir / "naturezas_nao_empresas.csv").write_text("natureza_code\n3999\n")

        # Only provide estabelecimentos (needed by the socios filtering step).
        # None of the single-file datasets (Motivos, Municipios, ...) are present.
        _create_mock_zip(
            receita_dir,
            "Estabelecimentos0",
            "cnpj_base;cnpj_ordem;cnpj_dv;identificador_matriz_filial;nome_fantasia;"
            "situacao_cadastral;data_situacao_cadastral;motivo_situacao_cadastral;"
            "nome_cidade_exterior;pais_code;data_inicio_atividade;cnae_principal_code;"
            "cnae_secundario_code;tipo_logradouro;logradouro;numero;complemento;bairro;"
            "cep;uf;municipio_code;ddd_1;telefone_1;ddd_2;telefone_2;ddd_fax;fax;"
            "correio_eletronico;situacao_especial;situacao_especial_data\n"
            "12345678;1;0;1;Loja A;2;20200101;0;;105;20200101;5611201;;;;;;;;;;;;;\n",
        )

        db_file = receita_dir / "skips_silver.db"
        run_silver_pipeline(receita_path=receita_dir, db_url=f"sqlite:///{db_file}")


# ── Tests: Multi-file streaming ────────────────────────────────────────────────


class TestMultiFileStreaming:
    """
    Test memory-efficient chunked processing for multi-file datasets.
    """

    @patch("pipeline.silver.loader.SQLiteReader")
    @patch("pipeline.silver.loader.write_db_by_batches")
    @patch("pipeline.silver.loader.ReceitaLoader")
    def test_estabelecimentos_streaming(
        self,
        MockLoader: MagicMock,
        MockWrite: MagicMock,
        MockSQLiteReader: MagicMock,
        temp_receita_dir: Path,
    ) -> None:
        """
        Ensure multi-file estabelecimentos are processed one archive at a time
        (one write_db_by_batches call per file).
        """
        for idx in range(2):
            _create_mock_zip(
                temp_receita_dir,
                f"Estabelecimentos{idx}",
                "cnpj_base;cnpj_ordem\n123;1\n456;2\n",
            )

        (temp_receita_dir / "cnaes_food.csv").write_text("cnaes_code\n1234\n")

        loader_instance = MockLoader.return_value
        loader_instance.load.side_effect = lambda fname: (
            pl.LazyFrame() if fname.lower().startswith("estabelecimento") else None
        )
        loader_instance.cleanup.return_value = None

        # Return an empty LazyFrame so the socios pre-query is harmless.
        MockSQLiteReader.return_value.__enter__.return_value.query.return_value.lazy.return_value = pl.LazyFrame()

        run_silver_pipeline(receita_path=temp_receita_dir, db_url="sqlite:///:memory:")

        estabelecimento_calls = [
            entry
            for entry in MockWrite.call_args_list
            if entry.args[1] == "estabelecimentos"
        ]
        assert len(estabelecimento_calls) == 2
        assert loader_instance.load.call_count >= 2


# ── Tests: Socios filtering ────────────────────────────────────────────────────


class TestSociosFiltering:
    """
    Test that socios records are filtered by existing cnpj_base in estabelecimentos.
    """

    def test_socios_filtered_by_cnpj_base(self, tmp_path: Path) -> None:
        """
        Socios records should only be inserted if their cnpj_base
        exists in the estabelecimentos table.
        """
        receita_dir = tmp_path / "receita"
        receita_dir.mkdir()
        (receita_dir / "cnaes_food.csv").write_text("cnaes_code\n5611201\n")
        (receita_dir / "naturezas_nao_empresas.csv").write_text("natureza_code\n3999\n")

        _create_mock_zip(
            receita_dir,
            "Estabelecimentos0",
            "cnpj_base;cnpj_ordem;cnpj_dv;identificador_matriz_filial;nome_fantasia;"
            "situacao_cadastral;data_situacao_cadastral;motivo_situacao_cadastral;"
            "nome_cidade_exterior;pais_code;data_inicio_atividade;cnae_principal_code;"
            "cnae_secundario_code;tipo_logradouro;logradouro;numero;complemento;bairro;"
            "cep;uf;municipio_code;ddd_1;telefone_1;ddd_2;telefone_2;ddd_fax;fax;"
            "correio_eletronico;situacao_especial;situacao_especial_data\n"
            "12345678;1;0;1;Loja A;2;20200101;0;;105;20200101;5611201;;;;;;;;;;;;;\n"
            "87654321;1;0;1;Loja B;2;20200101;0;;105;20200101;5611201;;;;;;;;;;;;;\n",
        )

        _create_mock_zip(
            receita_dir,
            "Socios0",
            "cnpj_base;identificador_de_socio;nome_socio;cpf_cnpj_socio;"
            "codigo_qualificacao_socio;data_entrada_sociedade;pais_code;"
            "representante_legal_cpf;representante_legal_nome;"
            "representante_legal_qualificacao;faixa_etaria\n"
            "12345678;1;Joao;111;10;20200101;105;;;;30\n"
            "99999999;2;Maria;222;10;20200101;105;;;;25\n",
        )

        db_file = receita_dir / "test_silver.db"
        db_url = f"sqlite:///{db_file}"

        # Run the full pipeline
        run_silver_pipeline(receita_path=receita_dir, db_url=db_url)

        # Verify using native sqlite3 to avoid any connection state issues
        import sqlite3

        conn = sqlite3.connect(str(db_file))
        conn.execute("PRAGMA journal_mode=DELETE")
        try:
            cursor = conn.cursor()

            # Verify estabelecimentos data exists
            cursor.execute("SELECT cnpj_base FROM estabelecimentos")
            est_cnpjs = {row[0] for row in cursor.fetchall()}
            assert len(est_cnpjs) > 0, (
                f"No estabelecimentos data found, got: {est_cnpjs}"
            )
            assert "12345678" in est_cnpjs

            # Verify socios were properly filtered
            cursor.execute("SELECT cnpj_base FROM socios")
            inserted_cnpjs = {row[0] for row in cursor.fetchall()}
            assert "12345678" in inserted_cnpjs
            assert "99999999" not in inserted_cnpjs
        finally:
            conn.close()


# ── Tests: Integration with real SQLite ────────────────────────────────────────


class TestIntegration:
    """End-to-end integration tests using real in-memory SQLite and mock CSV data."""

    def test_full_small_pipeline(self, tmp_path: Path) -> None:
        """
        Run the full pipeline end-to-end on a small mock dataset.

        The Silver pipeline always loads estabelecimentos and socios, so these
        archives must be present for the socios filtering query to succeed.
        """
        receita_dir = tmp_path / "receita"
        receita_dir.mkdir()

        (receita_dir / "cnaes_food.csv").write_text("cnaes_code\n5611201\n")
        (receita_dir / "naturezas_nao_empresas.csv").write_text("natureza_code\n3999\n")

        _create_mock_zip(
            receita_dir,
            "Estabelecimentos0",
            "cnpj_base;cnpj_ordem;cnpj_dv;identificador_matriz_filial;nome_fantasia;"
            "situacao_cadastral;data_situacao_cadastral;motivo_situacao_cadastral;"
            "nome_cidade_exterior;pais_code;data_inicio_atividade;cnae_principal_code;"
            "cnae_secundario_code;tipo_logradouro;logradouro;numero;complemento;bairro;"
            "cep;uf;municipio_code;ddd_1;telefone_1;ddd_2;telefone_2;ddd_fax;fax;"
            "correio_eletronico;situacao_especial;situacao_especial_data\n"
            "12345678;1;0;1;Loja A;2;20200101;0;;105;20200101;5611201;;;;;;;;;;;;;\n",
        )

        _create_mock_zip(
            receita_dir,
            "Socios0",
            "cnpj_base;identificador_de_socio;nome_socio;cpf_cnpj_socio;"
            "codigo_qualificacao_socio;data_entrada_sociedade;pais_code;"
            "representante_legal_cpf;representante_legal_nome;"
            "representante_legal_qualificacao;faixa_etaria\n"
            "12345678;1;Joao;111;10;20200101;105;;;;30\n",
        )

        db_file = receita_dir / "full_silver.db"
        run_silver_pipeline(receita_path=receita_dir, db_url=f"sqlite:///{db_file}")


# ── Test: Typer CLI entry point ────────────────────────────────────────────────


class TestCLI:
    """Tests for the Typer CLI command."""

    @patch("main.run_silver_pipeline")
    def test_ingest_silver_command(self, mock_pipeline: MagicMock) -> None:
        """Test that ingest-silver CLI command invokes the pipeline."""
        from typer.testing import CliRunner

        from main import app

        runner = CliRunner()
        result = runner.invoke(app, ["ingest-silver"])

        assert result.exit_code == 0
        mock_pipeline.assert_called_once()


# ── Tests: Polars DataFrame round-trip ─────────────────────────────────────────


class TestDataFrameRoundTrip:
    """Verify that data can be loaded, collected, and inserted without errors."""

    def test_single_file_roundtrip(self, tmp_path: Path) -> None:
        """
        Create a mock ZIP for 'Cnaes', load it, and insert into SQLite.
        """
        from pipeline.silver.loader import ReceitaLoader

        receita_dir = tmp_path / "receita"
        receita_dir.mkdir()
        (receita_dir / "cnaes_food.csv").write_text("cnaes_code\n1111\n")
        (receita_dir / "naturezas_nao_empresas.csv").write_text("natureza_code\n3999\n")

        _create_mock_zip(
            receita_dir,
            "Cnaes0",
            "cnaes_code;description\n1111;Test Code\n2222;Another Code\n",
        )

        loader = ReceitaLoader(receita_dir)
        df = loader.load("Cnaes0")
        assert df is not None

        with SilverLayerDatabase("sqlite:///:memory:") as db:
            schema = FILE_COLUMNS["cnaes"]["cols"]
            db.create_table("cnaes", schema)
            pdf = df.collect().to_pandas()
            records = pdf.to_dict(orient="records")
            count = db.insert_data("cnaes", records)
            assert count == 2


# ── Tests: SilverLayerDatabase error handling & indexes ────────────────────────


class TestSilverLayerDatabaseGuards:
    """Tests for connection guards, validation, and index creation."""

    def test_methods_raise_without_connection(self, sample_paises_schema: dict) -> None:
        """All DB operations require the context manager connection."""
        db = SilverLayerDatabase("sqlite:///:memory:")

        with pytest.raises(RuntimeError, match="Database not connected"):
            db.create_table("paises", sample_paises_schema)
        with pytest.raises(RuntimeError, match="Database not connected"):
            db.insert_data("paises", [{"pais_code": 1}])
        with pytest.raises(RuntimeError, match="Database not connected"):
            db.query_distinct("paises", "pais_code")
        with pytest.raises(RuntimeError, match="Database not connected"):
            db.get_row_count("paises")
        with pytest.raises(RuntimeError, match="Database not connected"):
            db.table_exists("paises")
        with pytest.raises(RuntimeError, match="Database not connected"):
            db.execute("SELECT 1")

    def test_query_distinct_invalid_column(self, db: SilverLayerDatabase) -> None:
        """query_distinct rejects invalid column identifiers."""
        with pytest.raises(ValueError, match="Invalid table or column name"):
            db.query_distinct("paises", "pais-code")
        with pytest.raises(ValueError, match="Invalid table or column name"):
            db.query_distinct("pais-code", "pais_code")

    def test_get_row_count_invalid_table(self, db: SilverLayerDatabase) -> None:
        """get_row_count rejects invalid table identifiers."""
        with pytest.raises(ValueError, match="Invalid table name"):
            db.get_row_count("pais-code")

    def test_create_table_with_indexes(self, db: SilverLayerDatabase) -> None:
        """create_table creates the declared indexes on the table."""
        schema = FILE_COLUMNS["empresas"]["cols"]
        db.create_table(
            "empresas", schema, index_columns=FILE_COLUMNS["empresas"]["index"]
        )

        result = db.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='empresas'"
        )
        index_names = {row[0] for row in result.fetchall()}
        assert "ix_empresas_cnpj_base" in index_names

    def test_insert_data_with_none_values(self, db: SilverLayerDatabase) -> None:
        """Records containing None values are inserted without errors."""
        schema = FILE_COLUMNS["empresas"]["cols"]
        db.create_table("empresas", schema)
        inserted = db.insert_data(
            "empresas",
            [{"cnpj_base": "12345678", "razao_social": None, "natureza_code": 2062}],
        )
        assert inserted == 1


# ── Tests: write_db_by_batches ─────────────────────────────────────────────────


class TestWriteDbByBatches:
    """Tests for the chunked database writer."""

    def test_write_db_by_batches(self, tmp_path: Path) -> None:
        """LazyFrames are written to an existing table in batches."""
        db_file = tmp_path / "batch.db"
        db_url = f"sqlite:///{db_file}"

        schema = FILE_COLUMNS["paises"]["cols"]
        with SilverLayerDatabase(db_url) as db:
            db.create_table("paises", schema)

        df = pl.LazyFrame(
            {"pais_code": [1, 2, 3], "pais_name": ["A", "B", "C"]},
            schema=schema,
        )

        write_db_by_batches(df, "paises", db_url)

        with SilverLayerDatabase(db_url) as db:
            assert db.get_row_count("paises") == 3

    def test_write_db_by_batches_empty(self, tmp_path: Path) -> None:
        """An empty LazyFrame writes nothing and does not raise."""
        db_file = tmp_path / "empty.db"
        db_url = f"sqlite:///{db_file}"

        schema = FILE_COLUMNS["paises"]["cols"]
        with SilverLayerDatabase(db_url) as db:
            db.create_table("paises", schema)

        df = pl.LazyFrame(schema=schema)

        write_db_by_batches(df, "paises", db_url)

        with SilverLayerDatabase(db_url) as db:
            assert db.get_row_count("paises") == 0


# ── Tests: ReceitaLoader dispatch ──────────────────────────────────────────────


class TestReceitaLoaderDispatch:
    """Tests for ReceitaLoader.load() dispatch behavior."""

    @pytest.fixture()
    def loader(self, tmp_path: Path) -> ReceitaLoader:
        from pipeline.silver.loader import ReceitaLoader

        receita_dir = tmp_path / "receita"
        receita_dir.mkdir()
        (receita_dir / "cnaes_food.csv").write_text("cnaes_code\n5611201\n")
        (receita_dir / "naturezas_nao_empresas.csv").write_text("natureza_code\n3999\n")
        return ReceitaLoader(receita_dir)

    def test_load_missing_zip_returns_none(self, loader: ReceitaLoader) -> None:
        """load() returns None when the ZIP file does not exist."""
        assert loader.load("Cnaes0") is None
        assert loader.load("Empresas0") is None

    def test_load_filters_empresas_by_public_company(
        self, loader: ReceitaLoader, tmp_path: Path
    ) -> None:
        """load_empresas excludes rows whose natureza_code is a public company."""
        _create_mock_zip(
            tmp_path / "receita",
            "Empresas0",
            "cnpj_base;razao_social;natureza_code;qualificacao_code;"
            "capital_social;porte_empresa;ente_federativo_responsavel\n"
            "11111111;Privada LTDA;2062;49;1000.00;5;\n"
            "22222222;Publica SA;3999;49;2000.00;5;\n",
        )

        df = loader.load("Empresas0")
        assert df is not None
        collected = df.collect()

        # natureza 3999 is listed in naturezas_nao_empresas.csv → filtered out
        assert collected.height == 1
        assert collected["cnpj_base"][0] == "11111111"

    def test_load_generic_keeps_all_rows(
        self, loader: ReceitaLoader, tmp_path: Path
    ) -> None:
        """load() falls back to load_generic for datasets without a custom loader."""
        _create_mock_zip(
            tmp_path / "receita",
            "Paises0",
            "pais_code;pais_name\n105;Brasil\n230;Estados Unidos\n",
        )

        df = loader.load("Paises0")
        assert df is not None
        assert df.collect().height == 2


# ── Tests: FILE_COLUMNS schema sanity ──────────────────────────────────────────


class TestFileColumnsSchema:
    """Sanity checks on the FILE_COLUMNS schema definition."""

    def test_all_datasets_have_required_keys(self) -> None:
        """Every dataset defines at least the 'cols' key with non-empty types."""
        for name, config in FILE_COLUMNS.items():
            assert "cols" in config, f"{name} is missing 'cols'"
            assert config["cols"], f"{name} has empty 'cols'"

    def test_all_datasets_have_index(self) -> None:
        """Every dataset declares an index list."""
        for name, config in FILE_COLUMNS.items():
            assert "index" in config, f"{name} is missing 'index'"
            assert config["index"], f"{name} has empty 'index'"

    def test_expected_datasets_present(self) -> None:
        expected = {
            "cnaes",
            "empresas",
            "estabelecimentos",
            "paises",
            "socios",
            "naturezas",
            "simples",
            "municipios",
            "qualificacoes",
            "motivos",
        }
        assert expected.issubset(FILE_COLUMNS.keys())

    def test_all_columns_map_to_sql_types(self) -> None:
        """Every column type is covered by the SQL TYPE_MAP."""
        db = SilverLayerDatabase("sqlite:///:memory:")
        for name, config in FILE_COLUMNS.items():
            for col, dtype in config["cols"].items():
                sql_type = db._polars_type_to_sql(dtype)
                assert sql_type in {"TEXT", "INTEGER", "REAL"}, (
                    f"{name}.{col} ({dtype}) mapped to unexpected type {sql_type}"
                )
