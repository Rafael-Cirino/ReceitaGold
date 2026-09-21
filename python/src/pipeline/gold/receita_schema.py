from typing import Final, TypedDict


# ── Column schema types ────────────────────────────────────────────────────────
class GoldSchema(TypedDict):
    """Index definition for a gold layer table."""

    index: list[str | list[str]] | None


GOLD_SCHEMA: Final[dict[str, GoldSchema]] = {
    "empresas": {
        "index": [
            "cnpj_base",
            ["municipio_code", "cnpj_base"],
            ["uf", "municipio_code"],
            ["cnpj_base", "identificador_matriz_filial", "cnpj_ordem"],
            ["municipio_code", "cnae_principal_code"],
            ["municipio_code", "cnae_secundario_code"],
            ["municipio_code", "nome_fantasia"],
        ],
    },
    "socios": {
        "index": ["cnpj_base"],
    },
    "cnaes": {
        "index": ["cnaes_code"],
    },
    "simples": {
        "index": ["cnpj_base"],
    },
    "municipios": {
        "index": ["municipio_code", ["uf", "municipio_code"]],
    },
    "naturezas": {
        "index": ["natureza_code"]
    },
    "qualificacoes": {
        "index": ["qualificacao_code"],
    },
    "cnae_alimenticio": {
        "index": ["cnaes_code"],
    }
}
