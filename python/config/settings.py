"""
Application settings loaded from environment variables using pydantic-settings.
"""

from __future__ import annotations

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

MAX_MEMORY: str = "4GB"  # Maximum memory for DuckDB operations

# Project root (the folder containing the `python/` package), derived from this
# file's location so paths are stable regardless of the current working directory.
PROJECT_ROOT: Path = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    """
    Central configuration for the ReceitaGold pipeline.

    Values are loaded from a .env file (if present) and/or environment variables.
    """

    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / ".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Data directory path (relative to project root)
    DATA_DIR: Path = PROJECT_ROOT / "data"

    # --- Execution ---
    LOG_LEVEL: str = "INFO"

    def _resolve_data_dir(self) -> Path:
        """Resolve DATA_DIR to an absolute Path, anchoring relative values to PROJECT_ROOT."""
        data_dir = Path(self.DATA_DIR)
        if not data_dir.is_absolute():
            data_dir = PROJECT_ROOT / data_dir
        return data_dir.resolve()

    @property
    def data_dir_path(self) -> Path:
        """Resolve the data directory as an absolute Path."""
        return self._resolve_data_dir()

    @property
    def data_silver_path(self) -> Path:
        """Resolve the silver layer directory as an absolute Path."""
        silver_path = self._resolve_data_dir() / "silver"
        silver_path.mkdir(exist_ok=True)
        return silver_path

    @property
    def data_gold_path(self) -> Path:
        """Resolve the gold layer directory as an absolute Path."""
        gold_path = self._resolve_data_dir() / "gold"
        gold_path.mkdir(exist_ok=True)
        return gold_path


# Singleton instance
settings = Settings()
