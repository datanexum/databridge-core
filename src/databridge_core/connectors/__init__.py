"""DataBridge Connectors -- Local DuckDB SQL engine and Parquet export.

Requires optional dependency: ``pip install databridge-core[sql]``

Public API:
    query_local        — Execute SQL against local files using DuckDB
    register_table     — Register a file as a named DuckDB table
    list_tables        — List registered tables
    export_to_parquet  — Export query results to Parquet
"""

from .duckdb_local import query_local, register_table, list_tables, export_to_parquet

__all__ = [
    "query_local",
    "register_table",
    "list_tables",
    "export_to_parquet",
]
