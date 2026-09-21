from pathlib import Path


def convert_to_utf8(dst_folder: Path, fname: str) -> Path:
    """
    Convert a file from Latin-1 encoding to UTF-8 encoding.
    """
    utf8_path = dst_folder / "utf8_converted.csv"

    # 1. Convert the file line-by-line (uses almost zero RAM)
    with (
        open(dst_folder / fname, mode="r", encoding="latin-1") as f_in,
        open(utf8_path, mode="w", encoding="utf-8") as f_out,
    ):
        f_out.writelines(f_in)

    return utf8_path