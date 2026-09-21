from pathlib import Path
from typing import Annotated

import typer

from config.settings import settings
from pipeline.gold.receita_builder import GoldLayerBuilder
from pipeline.silver.loader import run_silver_pipeline

app = typer.Typer(
    help="ReceitaGold - Receita Federal data pipeline (Bronze -> Silver -> Gold)"
)


@app.command()
def ingest_silver(
    db_path: Annotated[
        Path | None,
        typer.Option(
            "--db-path",
            help="Path to the SQLite Silver database (default: data/silver/silver.db)",
        ),
    ] = None,
    data_dir: Annotated[
        Path | None,
        typer.Option(
            "--data-dir",
            help="Directory containing Receita zip files (default: data/bronze/receita)",
        ),
    ] = None,
) -> None:
    """Run the Silver Layer ingestion pipeline."""
    if db_path is None:
        db_path = settings.data_silver_path / "silver.db"

    if data_dir is None:
        data_dir = settings.data_dir_path / "bronze" / "receita"

    db_url = f"sqlite:///{db_path}"

    typer.echo("Starting Silver Layer ingestion pipeline...")
    typer.echo(f"  Source: {data_dir}")
    typer.echo(f"  Target: {db_path}")

    try:
        run_silver_pipeline(receita_path=data_dir, db_url=db_url)
        typer.echo("\n✓ Pipeline executed successfully!")
    except FileNotFoundError as error:
        typer.echo(f"\n✗ File not found: {error}", err=True)
        raise typer.Exit(code=1)
    except Exception as error:  # noqa: BLE001 - top-level CLI guard
        typer.echo(f"\n✗ Pipeline failed: {error}", err=True)
        raise typer.Exit(code=1)


@app.command()
def build_gold(
    silver_db_path: Annotated[
        Path | None,
        typer.Option(
            "--silver-db-path",
            help="Directory containing the Silver SQLite database (default: data/silver)",
        ),
    ] = None,
    gold_db_path: Annotated[
        Path | None,
        typer.Option(
            "--gold-db-path",
            help="Directory to write the Gold SQLite database into (default: data/gold)",
        ),
    ] = None,
) -> None:
    """Run the Gold Layer build pipeline."""
    builder = GoldLayerBuilder(silver_db_path=silver_db_path, gold_db_path=gold_db_path)

    typer.echo("Starting Gold Layer build pipeline...")
    typer.echo(f"  Source: {builder.silver_db_path}")
    typer.echo(f"  Target: {builder.gold_db_path}")

    try:
        builder.run_pipeline()
        typer.echo("\n✓ Pipeline executed successfully!")
    except Exception as error:  # noqa: BLE001 - top-level CLI guard
        typer.echo(f"\n✗ Pipeline failed: {error}", err=True)
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
