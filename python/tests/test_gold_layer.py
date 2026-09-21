"""
Test suite for the Gold Layer pipeline (GoldLayerBuilder).

Tests cover:
    - create_sql_index helper
    - GoldLayerBuilder gold empresas build (dedup, full join, filters)
    - gold socios semi-join against gold.empresas
    - gold cnaes copy
    - gold municipios UF enrichment
    - gold simples semi-join
    - Index creation on the gold database
    - End-to-end run_pipeline integration over a real Silver SQLite DB
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from config.constants import ACTIVE_SITUACAO_CADASTRAL, BRAZIL_PAIS_CODE
from pipeline.gold.receita_builder import GoldLayerBuilder
from pipeline.gold.receita_schema import GOLD_SCHEMA
from pipeline.silver.loader import SilverLayerDatabase
from pipeline.silver.schema import FILE_COLUMNS
from pipeline.utils import create_sql_index

# ── Helpers / Fixtures ─────────────────────────────────────────────────────────

EMPRESAS_SCHEMA = FILE_COLUMNS["empresas"]["cols"]
ESTABELECIMENTOS_SCHEMA = FILE_COLUMNS["estabelecimentos"]["cols"]
SOCIOS_SCHEMA = FILE_COLUMNS["socios"]["cols"]
CNAES_SCHEMA = FILE_COLUMNS["cnaes"]["cols"]
MUNICIPIOS_SCHEMA = FILE_COLUMNS["municipios"]["cols"]
SIMPLES_SCHEMA = FILE_COLUMNS["simples"]["cols"]
NATUREZAS_SCHEMA = FILE_COLUMNS["naturezas"]["cols"]
QUALIFICACOES_SCHEMA = FILE_COLUMNS["qualificacoes"]["cols"]


@pytest.fixture()
def silver_db_path(tmp_path: Path) -> Path:
    """Create a silver SQLite database pre-populated with minimal test data."""
    db_path = tmp_path / "silver" / "silver.db"

    empresas = [
        {
            "cnpj_base": "11111111",
            "razao_social": "Restaurante A LTDA",
            "natureza_code": 2062,
            "qualificacao_code": 49,
            "capital_social": 1000.0,
            "porte_empresa": 5,
            "ente_federativo_responsavel": "",
        },
        {
            "cnpj_base": "22222222",
            "razao_social": "Banco B SA",
            "natureza_code": 2062,
            "qualificacao_code": 49,
            "capital_social": 500000.0,
            "porte_empresa": 5,
            "ente_federativo_responsavel": "",
        },
    ]
    estabelecimentos = [
        {
            "cnpj_base": "11111111",
            "cnpj_ordem": "0001",
            "cnpj_dv": "00",
            "identificador_matriz_filial": 1,
            "nome_fantasia": "Restaurante A",
            "situacao_cadastral": 2,
            "data_situacao_cadastral": 20200101,
            "motivo_situacao_cadastral": 0,
            "nome_cidade_exterior": "",
            "pais_code": 105,
            "data_inicio_atividade": 20200101,
            "cnae_principal_code": "5611201",
            "cnae_secundario_code": "",
            "tipo_logradouro": "RUA",
            "logradouro": "Das Flores",
            "numero": "10",
            "complemento": "",
            "bairro": "Centro",
            "cep": "01001000",
            "uf": "SP",
            "municipio_code": 7107,
            "ddd_1": "11",
            "telefone_1": "999999999",
            "ddd_2": "",
            "telefone_2": "",
            "ddd_fax": "",
            "fax": "",
            "correio_eletronico": "",
            "situacao_especial": "",
            "situacao_especial_data": 0,
        },
        {
            "cnpj_base": "22222222",
            "cnpj_ordem": "0001",
            "cnpj_dv": "00",
            "identificador_matriz_filial": 1,
            "nome_fantasia": "Banco B",
            "situacao_cadastral": 3,
            "data_situacao_cadastral": 20200101,
            "motivo_situacao_cadastral": 0,
            "nome_cidade_exterior": "",
            "pais_code": 105,
            "data_inicio_atividade": 20200101,
            "cnae_principal_code": "6410700",
            "cnae_secundario_code": "",
            "tipo_logradouro": "AV",
            "logradouro": "Paulista",
            "numero": "1000",
            "complemento": "",
            "bairro": "Centro",
            "cep": "01310100",
            "uf": "SP",
            "municipio_code": 7107,
            "ddd_1": "11",
            "telefone_1": "888888888",
            "ddd_2": "",
            "telefone_2": "",
            "ddd_fax": "",
            "fax": "",
            "correio_eletronico": "",
            "situacao_especial": "",
            "situacao_especial_data": 0,
        },
    ]
    socios = [
        {
            "cnpj_base": "11111111",
            "identificador_de_socio": 1,
            "nome_socio": "Joao",
            "cpf_cnpj_socio": "111",
            "codigo_qualificacao_socio": 10,
            "data_entrada_sociedade": 20200101,
            "pais_code": 105,
            "representante_legal_cpf": "",
            "representante_legal_nome": "",
            "representante_legal_qualificacao": 0,
            "faixa_etaria": 30,
        },
        {
            "cnpj_base": "22222222",
            "identificador_de_socio": 1,
            "nome_socio": "Maria",
            "cpf_cnpj_socio": "222",
            "codigo_qualificacao_socio": 10,
            "data_entrada_sociedade": 20200101,
            "pais_code": 105,
            "representante_legal_cpf": "",
            "representante_legal_nome": "",
            "representante_legal_qualificacao": 0,
            "faixa_etaria": 40,
        },
    ]
    cnaes = [
        {"cnaes_code": "5611201", "description": "Restaurantes"},
        {"cnaes_code": "6410700", "description": "Bancos"},
    ]
    municipios = [{"municipio_code": 7107, "municipio_nome": "SAO PAULO"}]
    naturezas = [
        {
            "natureza_code": 2062,
            "natureza_juridica_descricao": "Sociedade Empresaria Limitada",
        },
    ]
    qualificacoes = [
        {"qualificacao_code": 49, "qualificacao_descricao": "Socio-Administrador"},
    ]
    simples = [
        {
            "cnpj_base": "11111111",
            "opcao_pelo_simples": "S",
            "data_opcao_pelo_simples": 20200101,
            "data_exclusao_do_simples": 0,
            "opcao_pelo_mei": "N",
            "data_opcao_pelo_mei": 0,
            "data_exclusao_do_mei": 0,
        },
        {
            "cnpj_base": "22222222",
            "opcao_pelo_simples": "N",
            "data_opcao_pelo_simples": 0,
            "data_exclusao_do_simples": 0,
            "opcao_pelo_mei": "N",
            "data_opcao_pelo_mei": 0,
            "data_exclusao_do_mei": 0,
        },
    ]

    db_url = f"sqlite:///{db_path}"
    with SilverLayerDatabase(db_url) as db:
        db.create_table(
            "empresas", EMPRESAS_SCHEMA, index_columns=FILE_COLUMNS["empresas"]["index"]
        )
        db.create_table(
            "estabelecimentos",
            ESTABELECIMENTOS_SCHEMA,
            index_columns=FILE_COLUMNS["estabelecimentos"]["index"],
        )
        db.create_table(
            "socios", SOCIOS_SCHEMA, index_columns=FILE_COLUMNS["socios"]["index"]
        )
        db.create_table(
            "cnaes", CNAES_SCHEMA, index_columns=FILE_COLUMNS["cnaes"]["index"]
        )
        db.create_table(
            "municipios",
            MUNICIPIOS_SCHEMA,
            index_columns=FILE_COLUMNS["municipios"]["index"],
        )
        db.create_table(
            "simples", SIMPLES_SCHEMA, index_columns=FILE_COLUMNS["simples"]["index"]
        )
        db.create_table(
            "naturezas",
            NATUREZAS_SCHEMA,
            index_columns=FILE_COLUMNS["naturezas"]["index"],
        )
        db.create_table(
            "qualificacoes",
            QUALIFICACOES_SCHEMA,
            index_columns=FILE_COLUMNS["qualificacoes"]["index"],
        )

        db.insert_data("empresas", empresas)
        db.insert_data("estabelecimentos", estabelecimentos)
        db.insert_data("socios", socios)
        db.insert_data("cnaes", cnaes)
        db.insert_data("municipios", municipios)
        db.insert_data("simples", simples)
        db.insert_data("naturezas", naturezas)
        db.insert_data("qualificacoes", qualificacoes)

    return db_path.parent


@pytest.fixture()
def gold_db_path(tmp_path: Path) -> Path:
    gold_dir = tmp_path / "gold"
    gold_dir.mkdir()
    return gold_dir


@pytest.fixture()
def builder(silver_db_path: Path, gold_db_path: Path) -> GoldLayerBuilder:
    return GoldLayerBuilder(silver_db_path=silver_db_path, gold_db_path=gold_db_path)


def _fetch_all(db_path: Path, query: str) -> list[tuple]:
    with sqlite3.connect(db_path) as conn:
        return conn.execute(query).fetchall()


# ── Tests: create_sql_index helper ─────────────────────────────────────────────


class TestCreateSqlIndex:
    def test_generates_index_statements(self) -> None:
        statements = create_sql_index(["cnpj_base", "uf"], "empresas")
        assert statements == [
            "CREATE INDEX IF NOT EXISTS ix_empresas_cnpj_base ON empresas (cnpj_base);",
            "CREATE INDEX IF NOT EXISTS ix_empresas_uf ON empresas (uf);",
        ]

    def test_strips_schema_prefix(self) -> None:
        statements = create_sql_index(["cnpj_base"], "gold.empresas")
        assert "ON empresas" in statements[0]
        assert "ix_empresas_cnpj_base" in statements[0]

    def test_empty_list_returns_empty(self) -> None:
        assert create_sql_index([], "empresas") == []

    def test_mixed_single_and_composite_columns(self) -> None:
        statements = create_sql_index(
            [["cnpj_base", "cnpj_ordem", "cnpj_dv"], "uf", "municipio_code"],
            "estabelecimentos",
        )
        assert statements == [
            (
                "CREATE INDEX IF NOT EXISTS"
                " ix_estabelecimentos_cnpj_base_cnpj_ordem_cnpj_dv"
                " ON estabelecimentos (cnpj_base, cnpj_ordem, cnpj_dv);"
            ),
            (
                "CREATE INDEX IF NOT EXISTS ix_estabelecimentos_uf"
                " ON estabelecimentos (uf);"
            ),
            (
                "CREATE INDEX IF NOT EXISTS ix_estabelecimentos_municipio_code"
                " ON estabelecimentos (municipio_code);"
            ),
        ]

    def test_composite_index_strips_schema_prefix(self) -> None:
        statements = create_sql_index([["cnpj_base", "uf"]], "gold.empresas")
        assert statements == [
            (
                "CREATE INDEX IF NOT EXISTS ix_empresas_cnpj_base_uf"
                " ON empresas (cnpj_base, uf);"
            ),
        ]

    def test_empty_composite_entry_raises(self) -> None:
        with pytest.raises(ValueError, match="Empty index definition"):
            create_sql_index(["uf", []], "empresas")


# ── Tests: run_pipeline integration ───────────────────────────────────────────


@pytest.mark.integration
class TestGoldPipelineIntegration:
    """End-to-end tests running GoldLayerBuilder against a real Silver DB."""

    def test_run_pipeline_creates_all_gold_tables(
        self, builder: GoldLayerBuilder, gold_db_path: Path
    ) -> None:
        builder.run_pipeline()

        tables = {
            row[0]
            for row in _fetch_all(
                gold_db_path / "gold.db",
                "SELECT name FROM sqlite_master WHERE type='table'",
            )
        }
        expected = set(GOLD_SCHEMA)
        assert expected.issubset(tables)

    def test_gold_empresas_filters_inactive_and_foreign(
        self, builder: GoldLayerBuilder, gold_db_path: Path
    ) -> None:
        """
        gold.empresas keeps only rows with active situacao_cadastral and
        Brazilian (or NULL) pais_code, and includes estabelecimentos that
        have no matching empresas row (FULL OUTER JOIN).
        """
        builder.run_pipeline()

        rows = _fetch_all(
            gold_db_path / "gold.db",
            "SELECT cnpj_base, situacao_cadastral, pais_code FROM empresas",
        )
        cnpj_bases = {row[0] for row in rows}

        # 11111111: active + Brasil → kept (via join with empresas)
        # 22222222: situacao_cadastral = 3 → filtered out
        assert "11111111" in cnpj_bases
        assert "22222222" not in cnpj_bases

        for _, situacao, pais in rows:
            assert situacao in (*ACTIVE_SITUACAO_CADASTRAL, None)
            assert pais in (*BRAZIL_PAIS_CODE, None)

    def test_gold_socios_semi_join(
        self, builder: GoldLayerBuilder, gold_db_path: Path
    ) -> None:
        """gold.socios only contains socios of CNPJs present in gold.empresas."""
        builder.run_pipeline()

        rows = _fetch_all(
            gold_db_path / "gold.db", "SELECT cnpj_base, nome_socio FROM socios"
        )
        cnpj_bases = {row[0] for row in rows}

        assert cnpj_bases == {"11111111"}

    def test_gold_cnaes_copied_from_silver(
        self, builder: GoldLayerBuilder, gold_db_path: Path
    ) -> None:
        """gold.cnaes contains all silver cnaes rows."""
        builder.run_pipeline()

        rows = _fetch_all(
            gold_db_path / "gold.db",
            "SELECT cnaes_code, description FROM cnaes ORDER BY cnaes_code",
        )
        assert rows == [
            ("5611201", "Restaurantes"),
            ("6410700", "Bancos"),
        ]

    def test_gold_municipios_enriched_with_uf(
        self, builder: GoldLayerBuilder, gold_db_path: Path
    ) -> None:
        """gold.municipios carries the UF derived from gold.empresas addresses."""
        builder.run_pipeline()

        rows = _fetch_all(
            gold_db_path / "gold.db",
            "SELECT municipio_code, municipio_nome, uf FROM municipios",
        )
        assert rows == [(7107, "SAO PAULO", "SP")]

    def test_gold_simples_semi_join(
        self, builder: GoldLayerBuilder, gold_db_path: Path
    ) -> None:
        """gold.simples only contains rows whose cnpj_base is in gold.empresas."""
        builder.run_pipeline()

        rows = _fetch_all(
            gold_db_path / "gold.db",
            "SELECT cnpj_base, opcao_pelo_simples FROM simples",
        )
        assert rows == [("11111111", "S")]

    def test_gold_naturezas_copied_from_silver(
        self, builder: GoldLayerBuilder, gold_db_path: Path
    ) -> None:
        """gold.naturezas contains all silver naturezas rows."""
        builder.run_pipeline()

        rows = _fetch_all(
            gold_db_path / "gold.db",
            "SELECT natureza_code, natureza_juridica_descricao FROM naturezas",
        )
        assert rows == [(2062, "Sociedade Empresaria Limitada")]

    def test_gold_qualificacoes_copied_from_silver(
        self, builder: GoldLayerBuilder, gold_db_path: Path
    ) -> None:
        """gold.qualificacoes contains all silver qualificacoes rows."""
        builder.run_pipeline()

        rows = _fetch_all(
            gold_db_path / "gold.db",
            "SELECT qualificacao_code, qualificacao_descricao FROM qualificacoes",
        )
        assert rows == [(49, "Socio-Administrador")]

    def test_gold_indexes_created(
        self, builder: GoldLayerBuilder, gold_db_path: Path
    ) -> None:
        """The declared GOLD_SCHEMA indexes exist in the gold database."""
        builder.run_pipeline()

        index_names = {
            row[0]
            for row in _fetch_all(
                gold_db_path / "gold.db",
                "SELECT name FROM sqlite_master WHERE type='index'",
            )
        }

        for table, table_schema in GOLD_SCHEMA.items():
            for col in table_schema["index"] or []:
                cols = [col] if isinstance(col, str) else list(col)
                expected = "ix_" + "_".join([table, *cols])
                assert expected in index_names, f"Missing index {expected}"

    def test_run_pipeline_is_rerunnable(
        self, builder: GoldLayerBuilder, gold_db_path: Path
    ) -> None:
        """Running the pipeline twice does not duplicate data or raise."""
        builder.run_pipeline()
        builder.run_pipeline()

        assert (
            _fetch_all(gold_db_path / "gold.db", "SELECT COUNT(*) FROM empresas")[0][0]
            == 1
        )
        assert (
            _fetch_all(gold_db_path / "gold.db", "SELECT COUNT(*) FROM socios")[0][0]
            == 1
        )
        assert (
            _fetch_all(gold_db_path / "gold.db", "SELECT COUNT(*) FROM cnaes")[0][0]
            == 2
        )


# ── Tests: GoldLayerBuilder construction ───────────────────────────────────────


class TestGoldLayerBuilderInit:
    def test_paths_resolve_to_gold_dot_db(self, tmp_path: Path) -> None:
        builder = GoldLayerBuilder(
            silver_db_path=tmp_path / "silver", gold_db_path=tmp_path / "gold"
        )
        assert builder.silver_db_path == tmp_path / "silver" / "silver.db"
        assert builder.gold_db_path == tmp_path / "gold" / "gold.db"

    def test_safe_drop_table_is_idempotent(self, builder: GoldLayerBuilder) -> None:
        import duckdb

        con = duckdb.connect()
        try:
            con.execute("CREATE TABLE t AS SELECT 1 AS x;")
            builder._safe_drop_table(con, "t")
            builder._safe_drop_table(con, "t")  # second drop must not raise
        finally:
            con.close()
