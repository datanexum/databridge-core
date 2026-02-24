"""CSV and JSON loading utilities."""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


def load_csv(
    file_path: str,
    preview_rows: int = 5,
    max_preview_rows: int = 10,
) -> Dict[str, Any]:
    """Load a CSV file and return a preview with schema information.

    Args:
        file_path: Path to the CSV file.
        preview_rows: Number of rows to preview.
        max_preview_rows: Hard limit on preview rows.

    Returns:
        Dict with file info, schema, preview, and null counts.

    Raises:
        FileNotFoundError: If file doesn't exist.
    """
    path = Path(file_path)
    if not path.exists():
        # Check current working directory
        cwd_path = Path.cwd() / path.name
        if cwd_path.exists():
            file_path = str(cwd_path)
        else:
            raise FileNotFoundError(f"File not found: {file_path}")

    df = pd.read_csv(file_path)
    preview_rows = min(preview_rows, max_preview_rows)

    return {
        "file": file_path,
        "rows": len(df),
        "columns": list(df.columns),
        "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
        "preview": df.head(preview_rows).to_dict(orient="records"),
        "null_counts": df.isnull().sum().to_dict(),
    }


def load_json(
    file_path: str,
    preview_rows: int = 5,
    max_preview_rows: int = 10,
) -> Dict[str, Any]:
    """Load a JSON file (array or object) and return a preview.

    Args:
        file_path: Path to the JSON file.
        preview_rows: Number of rows to preview.
        max_preview_rows: Hard limit on preview rows.

    Returns:
        Dict with file info, columns, and preview data.
    """
    with open(file_path, "r") as f:
        data = json.load(f)

    if isinstance(data, list):
        df = pd.DataFrame(data)
    elif isinstance(data, dict):
        if all(isinstance(v, list) for v in data.values()):
            df = pd.DataFrame(data)
        else:
            df = pd.DataFrame([data])
    else:
        raise ValueError("Unsupported JSON structure")

    preview_rows = min(preview_rows, max_preview_rows)

    return {
        "file": file_path,
        "rows": len(df),
        "columns": list(df.columns),
        "preview": df.head(preview_rows).to_dict(orient="records"),
    }


def query_database(
    connection_string: str,
    query: str,
    preview_rows: int = 10,
    max_preview_rows: int = 10,
) -> Dict[str, Any]:
    """Execute a SQL SELECT query and return results.

    Args:
        connection_string: SQLAlchemy connection string.
        query: SQL SELECT query to execute.
        preview_rows: Maximum rows to return.
        max_preview_rows: Hard limit on preview rows.

    Returns:
        Dict with query, row count, columns, and preview.

    Raises:
        ImportError: If SQLAlchemy is not installed.
        ValueError: If query is not a SELECT statement.
    """
    try:
        from sqlalchemy import create_engine
    except ImportError:
        raise ImportError(
            "SQLAlchemy not installed. Run: pip install 'databridge-core[sql]'"
        )

    if not query.strip().upper().startswith("SELECT"):
        raise ValueError("Only SELECT queries are allowed")

    engine = create_engine(connection_string)
    df = pd.read_sql(query, engine)
    preview_rows = min(preview_rows, max_preview_rows)

    return {
        "query": query,
        "rows_returned": len(df),
        "columns": list(df.columns),
        "preview": df.head(preview_rows).to_dict(orient="records"),
        "truncated": len(df) > preview_rows,
    }
