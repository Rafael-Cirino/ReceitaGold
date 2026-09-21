from pathlib import Path

import duckdb
from loguru import logger
from sqlalchemy import create_engine, text

from config.constants import ACTIVE_SITUACAO_CADASTRAL, BRAZIL_PAIS_CODE
from config.settings import MAX_MEMORY, settings
from pipeline.gold.receita_schema import GOLD_SCHEMA, GoldSchema
from pipeline.utils import create_sql_index


class GoldLayerBuilder:
    def __init__(
        self,
        silver_db_path: Path | None = None,
        gold_db_path: Path | None = None,
    ):
        self.silver_db_path = (
            silver_db_path or settings.data_silver_path
        ) / "silver.db"
        self.gold_db_path = (gold_db_path or settings.data_gold_path) / "gold.db"

        self.temp_db_path = Path("/tmp/temp.duckdb")
        self.temp_db_folder = Path("/tmp/duckdb_temp")

    def _create_indexes_sqlalchemy(self, table_index, table_name):
        if not table_index:
            return

        logger.info("Criando índices do banco final...")

        db_url = f"sqlite:///{self.gold_db_path}"
        engine = create_engine(db_url)

        with engine.begin() as conn:
            conn.execute(text("PRAGMA synchronous = OFF;"))
            conn.execute(text("PRAGMA journal_mode = MEMORY;"))

            for index_sql in create_sql_index(table_index, table_name):
                conn.execute(text(index_sql))

        engine.dispose()
        logger.success("Índices criados.")

    @staticmethod
    def _safe_drop_table(con: duckdb.DuckDBPyConnection, table_name: str) -> None:
        con.execute(f"DROP TABLE IF EXISTS {table_name};")

    def _create_index_for_table(self, schema: GoldSchema, table_name: str) -> None:
        table_index = schema.get("index", [])
        if table_index:
            self._create_indexes_sqlalchemy(table_index, table_name)

    def _build_copy_table(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        schema: GoldSchema,
        source_name: str,
        source_sql: str,
        success_message: str | None = None,
    ) -> None:
        target_name = f"gold.{table_name}"
        replace_sql = self._get_nullif_replace_expr(con, source_name)

        self._safe_drop_table(con, target_name)
        con.execute(
            f"""
            CREATE TABLE {target_name} AS
            SELECT * {replace_sql}
            FROM {source_sql};
            """
        )

        if success_message:
            logger.success(success_message)
        self._create_index_for_table(schema, target_name)

    def _get_nullif_replace_expr(
        self, con: duckdb.DuckDBPyConnection, table_or_query: str
    ) -> str:
        """
        Descobre colunas VARCHAR e gera a string `REPLACE (NULLIF(col, '') AS col)`
        """
        # Se for uma query complexa (tem espaços), empacota num LIMIT 0 para o DESCRIBE ser instantâneo
        if " " in table_or_query:
            describe_target = f"SELECT * FROM ({table_or_query}) LIMIT 0"
        else:
            describe_target = table_or_query

        col_info = con.execute(f"DESCRIBE {describe_target}").fetchall()
        text_cols = [row[0] for row in col_info if row[1] == "VARCHAR"]

        if not text_cols:
            return ""

        replace_expr = ", ".join([f"NULLIF({col}, '') AS {col}" for col in text_cols])
        return f"REPLACE ({replace_expr})"

    def _build_gold_empresas(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        schema: GoldSchema,
        pais_codes: list[int],
        situacoes_cadastrais: list[int],
    ):
        """Faz o FULL JOIN das 3 tabelas e aplica os filtros."""
        logger.info("Processando empresas.")

        table_name = f"gold.{table_name}"
        pais_filter = ", ".join(str(p) for p in pais_codes)
        sit_filter = ", ".join(str(s) for s in situacoes_cadastrais)

        logger.info("Etapa 1/2: padronizando empresas.")
        con.execute("DROP TABLE IF EXISTS emp_temp;")
        con.execute(
            """
            CREATE TEMP TABLE emp_temp AS
            SELECT DISTINCT ON (cnpj_base) *
            FROM silver.empresas;
            """
        )

        logger.info("Etapa 2/2: gravando dados finais.")

        # Gera o REPLACE dinâmico
        replace_sql = self._get_nullif_replace_expr(
            con,
            "SELECT * FROM emp_temp FULL OUTER JOIN silver.estabelecimentos USING (cnpj_base)",
        )

        self._safe_drop_table(con, table_name)
        con.execute(
            f"""
            CREATE TABLE {table_name} AS
            SELECT * {replace_sql}
            FROM emp_temp
            FULL OUTER JOIN silver.estabelecimentos USING (cnpj_base)
            WHERE (pais_code IN ({pais_filter}) OR pais_code IS NULL)
              AND (situacao_cadastral IN ({sit_filter}) OR situacao_cadastral IS NULL);
            """
        )

        logger.success("Empresas processadas.")
        self._create_index_for_table(schema, table_name)

    def _build_gold_socios(
        self, con: duckdb.DuckDBPyConnection, table_name: str, schema: GoldSchema
    ):
        """Filtra a tabela de sócios com base nos CNPJs que sobreviveram na empresas_gold."""
        logger.info("Processando sócios.")
        self._build_copy_table(
            con,
            table_name,
            schema,
            "silver.socios",
            "silver.socios s SEMI JOIN gold.empresas g ON s.cnpj_base = g.cnpj_base",
        )

    def _build_gold_cnaes(
        self, con: duckdb.DuckDBPyConnection, table_name: str, schema: GoldSchema
    ):
        """Carrega a tabela de CNAEs na camada gold."""
        logger.info("Processando CNAEs.")
        self._build_copy_table(con, table_name, schema, "silver.cnaes", "silver.cnaes")

    def _build_gold_naturezas(
        self, con: duckdb.DuckDBPyConnection, table_name: str, schema: GoldSchema
    ):
        """Carrega a tabela de Naturezas Jurídicas na camada gold."""
        logger.info("Processando Naturezas Jurídicas.")
        self._build_copy_table(
            con, table_name, schema, "silver.naturezas", "silver.naturezas"
        )

    def _build_gold_qualificacoes(
        self, con: duckdb.DuckDBPyConnection, table_name: str, schema: GoldSchema
    ):
        """Carrega a tabela de Qualificações na camada gold."""
        logger.info("Processando Qualificações.")
        self._build_copy_table(
            con, table_name, schema, "silver.qualificacoes", "silver.qualificacoes"
        )

    def _build_gold_municipios(
        self, con: duckdb.DuckDBPyConnection, table_name: str, schema: GoldSchema
    ):
        """Enriquece a base com dados de município e UF."""
        logger.info("Processando municípios.")
        table_name = f"gold.{table_name}"

        # Inspeciona as colunas de origem do municipio + o UF gerado no join
        target_query = """
            SELECT m.*, u.uf 
            FROM silver.municipios m 
            LEFT JOIN (SELECT municipio_code, 'XX' AS uf FROM gold.empresas) u 
            USING (municipio_code)
        """
        replace_sql = self._get_nullif_replace_expr(con, target_query)

        query = f"""
            CREATE TABLE {table_name} AS
            WITH uf_map AS (
                SELECT municipio_code, MAX(uf) AS uf
                FROM gold.empresas
                WHERE uf IS NOT NULL
                GROUP BY municipio_code
            )
            SELECT * {replace_sql}
            FROM silver.municipios m
            LEFT JOIN uf_map u USING (municipio_code);
        """

        self._safe_drop_table(con, table_name)
        con.execute(query)
        self._create_index_for_table(schema, table_name)

    def _add_simples(
        self, con: duckdb.DuckDBPyConnection, table_name: str, schema: GoldSchema
    ):
        """Incopora os dados do Simples ao conjunto gold."""
        logger.info("Processando Simples.")
        self._build_copy_table(
            con,
            table_name,
            schema,
            "silver.simples",
            "silver.simples s SEMI JOIN gold.empresas e USING (cnpj_base)",
            "Simples integrado.",
        )

    def _build_cnae_alimenticio(
        self, con: duckdb.DuckDBPyConnection, table_name: str, schema: GoldSchema
    ):
        """Incopora os dados do CNAE Alimentício ao conjunto gold."""
        logger.info("Processando CNAE Alimentício.")
        self._build_copy_table(
            con,
            table_name,
            schema,
            "silver.cnae_alimenticio",
            "silver.cnae_alimenticio",
            "CNAE Alimentício integrado.",
        )

    def run_pipeline(
        self,
        gold_schema: dict[str, GoldSchema] = GOLD_SCHEMA,
        pais_codes: list[int] = BRAZIL_PAIS_CODE,
        situacoes_cadastrais: list[int] = ACTIVE_SITUACAO_CADASTRAL,
    ):
        """Executa a pipeline completa da camada Gold."""
        logger.info("Iniciando processamento da camada gold.")

        con = duckdb.connect(self.temp_db_path)

        try:
            con.execute(f"SET max_memory = '{MAX_MEMORY}'")
            con.execute("SET preserve_insertion_order=false")
            con.execute(f"PRAGMA temp_directory = '{self.temp_db_folder}';")

            logger.info("Conectando aos bancos de origem e destino.")
            con.execute(
                f"ATTACH '{self.silver_db_path}' AS silver (TYPE SQLITE, READ_ONLY)"
            )
            con.execute(f"ATTACH '{self.gold_db_path}' AS gold (TYPE SQLITE)")

            self._build_gold_empresas(
                con,
                "empresas",
                gold_schema["empresas"],
                pais_codes,
                situacoes_cadastrais,
            )
            self._build_gold_socios(con, "socios", gold_schema["socios"])
            self._build_gold_municipios(con, "municipios", gold_schema["municipios"])
            self._build_gold_cnaes(con, "cnaes", gold_schema["cnaes"])
            self._add_simples(con, "simples", gold_schema["simples"])
            self._build_gold_naturezas(con, "naturezas", gold_schema["naturezas"])
            self._build_gold_qualificacoes(
                con, "qualificacoes", gold_schema["qualificacoes"]
            )
            self._build_cnae_alimenticio(
                con, "cnae_alimenticio", gold_schema["cnae_alimenticio"]
            )

            logger.success("Camada gold finalizada.")

        except Exception:
            logger.exception("Erro na camada gold.")
            raise
        finally:
            con.close()


if __name__ == "__main__":
    builder = GoldLayerBuilder()
    builder.run_pipeline()
