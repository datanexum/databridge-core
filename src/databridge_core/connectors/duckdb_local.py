"""Local DuckDB SQL engine for querying CSV, Parquet, JSON, and Excel files.

Standalone implementation — requires ``duckdb`` package.
Install via: ``pip install databridge-core[duckdb]``
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False

# Module-level connection (lazy singleton)
_conn: Optional["duckdb.DuckDBPyConnection"] = None


def _get_conn() -> "duckdb.DuckDBPyConnection":
    """Get or create the module-level DuckDB connection."""
    global _conn
    if not DUCKDB_AVAILABLE:
        raise ImportError(
            "DuckDB is not installed. Run: pip install duckdb  "
            "or: pip install databridge-core[duckdb]"
        )
    if _conn is None:
        _conn = duckdb.connect(":memory:")
    return _conn


def _register_file(conn: "duckdb.DuckDBPyConnection", file_path: str, table_name: str) -> None:
    """Register a file as a named view in DuckDB."""
    path = Path(file_path).resolve()
    ext = path.suffix.lower()
    path_escaped = str(path).replace("'", "''")

    if ext == ".csv":
        conn.execute(
            f'CREATE OR REPLACE VIEW "{table_name}" AS '
            f"SELECT * FROM read_csv_auto('{path_escaped}')"
        )
    elif ext == ".parquet":
        conn.execute(
            f'CREATE OR REPLACE VIEW "{table_name}" AS '
            f"SELECT * FROM read_parquet('{path_escaped}')"
        )
    elif ext == ".json":
        conn.execute(
            f'CREATE OR REPLACE VIEW "{table_name}" AS '
            f"SELECT * FROM read_json_auto('{path_escaped}')"
        )
    elif ext in (".xlsx", ".xls", ".xlsb"):
        engine = "pyxlsb" if ext == ".xlsb" else None
        df = pd.read_excel(file_path, engine=engine)
        df.columns = [str(c) for c in df.columns]
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].astype(str).replace("nan", None)
        tmp = f"_tmp_{table_name}"
        conn.register(tmp, df)
        conn.execute(
            f'CREATE OR REPLACE VIEW "{table_name}" AS SELECT * FROM "{tmp}"'
        )
    else:
        raise ValueError(f"Unsupported file format: {ext}")


def query_local(
    sql: str,
    register_files: Optional[Dict[str, str]] = None,
    max_preview_rows: int = 10,
) -> Dict[str, Any]:
    """Execute SQL against local files using DuckDB.

    Args:
        sql: SQL query (DuckDB syntax). Can use read_csv_auto(), read_parquet(), etc.
        register_files: Optional dict mapping table names to file paths to register
            before executing the query.
        max_preview_rows: Maximum rows in the preview output.

    Returns:
        Dict with columns, dtypes, row count, and preview data.
    """
    conn = _get_conn()

    if register_files:
        for name, path in register_files.items():
            _register_file(conn, path, name)

    result_df = conn.execute(sql).fetchdf()
    preview_rows = min(len(result_df), max_preview_rows)

    return {
        "rows_returned": len(result_df),
        "columns": list(result_df.columns),
        "dtypes": {col: str(dtype) for col, dtype in result_df.dtypes.items()},
        "preview": result_df.head(preview_rows).to_dict(orient="records"),
        "truncated": len(result_df) > preview_rows,
        "sql": sql,
    }


def register_table(file_path: str, table_name: str) -> Dict[str, Any]:
    """Register a local file as a named table in DuckDB.

    Args:
        file_path: Path to CSV, Parquet, JSON, or Excel file.
        table_name: SQL table name to register.

    Returns:
        Dict with table_name, row_count, columns, and schema.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    conn = _get_conn()
    _register_file(conn, file_path, table_name)

    # Get info
    count_row = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()
    row_count = count_row[0] if count_row else 0

    schema_df = conn.execute(f'DESCRIBE "{table_name}"').fetchdf()
    schema = [
        {"column": r["column_name"], "type": r["column_type"]}
        for _, r in schema_df.iterrows()
    ]

    return {
        "table_name": table_name,
        "file_path": str(path),
        "row_count": row_count,
        "columns": [s["column"] for s in schema],
        "schema": schema,
    }


def list_tables() -> Dict[str, Any]:
    """List all tables and views registered in the DuckDB session.

    Returns:
        Dict with table_count and list of table info dicts.
    """
    conn = _get_conn()

    tables_df = conn.execute(
        "SELECT table_name, table_type FROM information_schema.tables "
        "WHERE table_schema = 'main'"
    ).fetchdf()

    tables = []
    for _, row in tables_df.iterrows():
        name = row["table_name"]
        if name.startswith("_tmp_"):
            continue
        try:
            count_row = conn.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()
            row_count = count_row[0] if count_row else 0
        except Exception:
            row_count = -1
        tables.append({
            "table_name": name,
            "table_type": row["table_type"],
            "row_count": row_count,
        })

    return {
        "table_count": len(tables),
        "tables": tables,
    }


def export_to_parquet(sql_or_table: str, output_path: str) -> Dict[str, Any]:
    """Export a query result or table to Parquet format.

    Args:
        sql_or_table: A SQL query string or a registered table name.
        output_path: Path for the output .parquet file.

    Returns:
        Dict with output_path, row_count, and file_size_bytes.
    """
    conn = _get_conn()
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    # Determine if input is a table name or SQL
    sql = sql_or_table
    if not sql_or_table.strip().upper().startswith("SELECT"):
        sql = f'SELECT * FROM "{sql_or_table}"'

    path_escaped = str(out.resolve()).replace("'", "''")
    conn.execute(f"COPY ({sql}) TO '{path_escaped}' (FORMAT PARQUET)")

    file_size = out.stat().st_size if out.exists() else 0
    # Get row count
    count_row = conn.execute(f"SELECT COUNT(*) FROM ({sql})").fetchone()
    row_count = count_row[0] if count_row else 0

    return {
        "output_path": str(out),
        "row_count": row_count,
        "file_size_bytes": file_size,
        "file_size_mb": round(file_size / (1024 * 1024), 2),
    }
