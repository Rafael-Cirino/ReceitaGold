from enum import IntEnum
from typing import Final


# ── Receita ───────────────────────────────────────────────────────────
# ── Domain constants ───────────────────────────────────────────────────────────
class SituacaoCadastral(IntEnum):
    """Situação cadastral codes used by Receita Federal."""

    ATIVA = 2


class PaisCode(IntEnum):
    """Country codes used by Receita Federal."""

    BRASIL = 105
    BRASIL_AFRETAMENTO = 106
    NOT_DECLARED = 997
    NOT_DECLARED_PRE = 998
    NOT_DECLARED_B = 999


ACTIVE_SITUACAO_CADASTRAL: Final[list] = [SituacaoCadastral.ATIVA]
BRAZIL_PAIS_CODE: Final[list] = [
    PaisCode.BRASIL,
    PaisCode.BRASIL_AFRETAMENTO,
    PaisCode.NOT_DECLARED,
    PaisCode.NOT_DECLARED_PRE,
    PaisCode.NOT_DECLARED_B,
]
