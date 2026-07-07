"""Load SQL files bundled in ``fasm_pipeline/sql/``.

Resolving relative to this module (rather than the process CWD) means the
queries load correctly whether the pipeline runs from a repo checkout, an
installed package, or a Docker image.
"""
from pathlib import Path

SQL_DIR = Path(__file__).parent / "sql"


def read_sql(name: str) -> str:
    """Return the contents of ``fasm_pipeline/sql/<name>``."""
    return (SQL_DIR / name).read_text()
