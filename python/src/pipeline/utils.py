from __future__ import annotations

import re
import sys
import zipfile
from pathlib import Path

from tqdm import tqdm


def split_name_number(name: str) -> tuple[str, str | None]:
    """
    Split a string into its alphabetic prefix and trailing numeric suffix.

    The function finds the last contiguous block of digits at the end of the
    string and separates it from the rest. If there are no trailing digits,
    the number part is None.

    Examples:
        split_name_number("empresas0")          -> ("empresas", "0")
        split_name_number("empresas01")         -> ("empresas", "01")
        split_name_number("socios0")            -> ("socios", "0")
        split_name_number("estabelecimento2")   -> ("estabelecimento", "2")
        split_name_number("cnaes")              -> ("cnaes", None)
        split_name_number("123")                -> ("", "123")
        split_name_number("")                   -> ("", None)
    """
    name = name.lower()
    match = re.search(r"(\d+)$", name)
    if match:
        number_part = match.group(1)
        name_part = name[: match.start()]
        return name_part, number_part
    return name, None


def unzip_file_with_progress(zip_path: Path, dst_folder: Path) -> list[str]:
    dst_folder.mkdir(exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        # Extract all contents into the specified directory
        extrated_files = zip_ref.namelist()

        # Get total size for progress bar
        total_size = sum(zinfo.file_size for zinfo in zip_ref.infolist())

        # Extract with progress bar
        with tqdm(
            total=total_size,
            unit="B",
            unit_scale=True,
            desc="Extracting",
            mininterval=0.1,
            miniters=1,
        ) as pbar:
            for zinfo in zip_ref.infolist():
                dst_path = dst_folder / zinfo.filename

                # Handle directories
                if zinfo.is_dir():
                    dst_path.mkdir(parents=True, exist_ok=True)
                    continue

                # Create parent directories for files
                dst_path.parent.mkdir(parents=True, exist_ok=True)

                # Extract file with chunked progress updates
                with zip_ref.open(zinfo) as src, open(dst_path, "wb") as dst:
                    while True:
                        chunk = src.read(64 * 1024)  # 64KB chunks
                        if not chunk:
                            break
                        dst.write(chunk)
                        pbar.update(len(chunk))
                        sys.stderr.flush()

    return extrated_files


def create_sql_index(
    index_columns: list[str | list[str]], table_name: str
) -> list[str]:
    """Build ``CREATE INDEX IF NOT EXISTS`` statements for a table.

    Each entry in ``index_columns`` is either a single column name (``str``)
    or a list of column names, which produces one composite index:

        create_sql_index(["uf", ["cnpj_base", "cnpj_ordem"]], "gold.empresas")
        # [
        #   'CREATE INDEX IF NOT EXISTS ix_empresas_uf ON empresas (uf);',
        #   'CREATE INDEX IF NOT EXISTS ix_empresas_cnpj_base_cnpj_ordem '
        #   'ON empresas (cnpj_base, cnpj_ordem);',
        # ]
    """
    tname = table_name.split(".")[-1]  # Remove schema prefix if present

    statements: list[str] = []
    for entry in index_columns:
        cols = [entry] if isinstance(entry, str) else list(entry)
        if not cols:
            raise ValueError(f"Empty index definition for table {table_name!r}")

        index_name = "_".join([tname, *cols])
        joined_cols = ", ".join(cols)
        statements.append(
            f"CREATE INDEX IF NOT EXISTS ix_{index_name} ON {tname} ({joined_cols});"
        )
    return statements
