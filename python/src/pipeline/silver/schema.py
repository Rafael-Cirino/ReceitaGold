from __future__ import annotations

from pathlib import Path
from typing import Final, TypedDict

import polars as pl

from config.settings import settings

# ── Paths ──────────────────────────────────────────────────────────────────────
BRONZE_PATH: Path = settings.data_dir_path / "bronze"
RECEITA_BRONZE_PATH: Path = BRONZE_PATH / "receita"
UNZIP_TMP_PATH: Path = Path("/tmp/receita_extracted")

# ── Column schema types ────────────────────────────────────────────────────────
class FileColumnConfig(TypedDict):
    """Schema definition for a Receita Federal dataset file."""
    cols: dict[str, pl.PolarsDataType]
    use_cols: list[str] | None
    index: list[str] | None
    drop: list[str] | None


# ── Column schemas for each Receita Federal dataset ────────────────────────────
FILE_COLUMNS: Final[dict[str, FileColumnConfig]] = {
    "cnaes": {
        "cols": {
            "cnaes_code": pl.String(),
            "description": pl.String(),
        },
        "index": ["cnaes_code"]
    },
    "empresas": {
        "cols": {
            "cnpj_base": pl.String(),
            "razao_social": pl.String(),
            "natureza_code": pl.UInt64(),
            "qualificacao_code": pl.UInt64(),
            "capital_social": pl.Float64(),
            "porte_empresa": pl.UInt16(),
            "ente_federativo_responsavel": pl.String(),
        },
        "index": ["cnpj_base"]
    },
    "estabelecimentos": {
        "cols": {
            "cnpj_base": pl.String(),
            "cnpj_ordem": pl.String(),
            "cnpj_dv": pl.String(),
            "identificador_matriz_filial": pl.UInt64(),
            "nome_fantasia": pl.String(),
            "situacao_cadastral": pl.UInt16(),
            "data_situacao_cadastral": pl.UInt32(),
            "motivo_situacao_cadastral": pl.UInt16(),
            "nome_cidade_exterior": pl.String(),
            "pais_code": pl.UInt64(),
            "data_inicio_atividade": pl.UInt32(),
            "cnae_principal_code": pl.String(),
            "cnae_secundario_code": pl.String(),
            "tipo_logradouro": pl.String(),
            "logradouro": pl.String(),
            "numero": pl.String(),
            "complemento": pl.String(),
            "bairro": pl.String(),
            "cep": pl.String(),
            "uf": pl.String(),
            "municipio_code": pl.UInt64(),
            "ddd_1": pl.String(),
            "telefone_1": pl.String(),
            "ddd_2": pl.String(),
            "telefone_2": pl.String(),
            "ddd_fax": pl.String(),
            "fax": pl.String(),
            "correio_eletronico": pl.String(),
            "situacao_especial": pl.String(),
            "situacao_especial_data": pl.UInt32(),
        },
        "drop": ["ddd_fax", "fax"],
        "index": ["cnpj_base", "cnpj_ordem", "cnpj_dv", "uf", "municipio_code"]
    },
    "paises": {
        "cols": {
            "pais_code": pl.UInt64(),
            "pais_name": pl.String(),
        },
        "index": ["pais_code"]
    },
    "socios": {
        "cols": {
            "cnpj_base": pl.String(),
            "identificador_de_socio": pl.UInt16(),
            "nome_socio": pl.String(),
            "cpf_cnpj_socio": pl.String(),
            "codigo_qualificacao_socio": pl.UInt64(),
            "data_entrada_sociedade": pl.UInt32(),
            "pais_code": pl.UInt64(),
            "representante_legal_cpf": pl.String(),
            "representante_legal_nome": pl.String(),
            "representante_legal_qualificacao": pl.UInt64(),
            "faixa_etaria": pl.Int64(),
        },
        "index": ["cnpj_base"]
    },
    "naturezas": {
        "cols": {
            "natureza_code": pl.UInt64(),
            "natureza_juridica_descricao": pl.String(),
        },
        "index": ["natureza_code"]
    },
    "simples": {
        "cols": {
            "cnpj_base": pl.String(),
            "opcao_pelo_simples": pl.String(),
            "data_opcao_pelo_simples": pl.UInt32(),
            "data_exclusao_do_simples": pl.UInt32(),
            "opcao_pelo_mei": pl.String(),
            "data_opcao_pelo_mei": pl.UInt32(),
            "data_exclusao_do_mei": pl.UInt32(),
        },
        "index": ["cnpj_base"]
    },
    "municipios": {
        "cols": {
            "municipio_code": pl.UInt64(),
            "municipio_nome": pl.String(),
        },
        "index": ["municipio_code"]
    },
    "qualificacoes": {
        "cols": {
            "qualificacao_code": pl.UInt64(),
            "qualificacao_descricao": pl.String(),
        },
        "index": ["qualificacao_code"]
    },
    "motivos": {
        "cols": {
            "motivo_code": pl.UInt16(),
            "motivo_descricao": pl.String(),
        },
        "index": ["motivo_code"]
    },
    "cnae_alimenticio": {
        "cols": {
            "cnaes_code": pl.String(),
            "descricao": pl.String(),
            "categoria": pl.String()
        },
        "index": ["cnaes_code"]
    }
}
