"""Table parsing from raw text."""

import re
from typing import Any, Dict, List, Optional


def parse_table_from_text(
    text: str,
    delimiter: str = "auto",
    max_preview_rows: int = 10,
) -> Dict[str, Any]:
    """Attempt to parse tabular data from extracted text.

    Args:
        text: Raw text containing tabular data.
        delimiter: Column delimiter ('auto', 'tab', 'space', 'pipe', or custom).
        max_preview_rows: Maximum rows in preview.

    Returns:
        Dict with columns, row count, and preview data.

    Raises:
        ValueError: If no text content to parse.
    """
    lines = [line.strip() for line in text.strip().split("\n") if line.strip()]

    if not lines:
        raise ValueError("No text content to parse")

    # Auto-detect delimiter
    if delimiter == "auto":
        first_line = lines[0]
        if "\t" in first_line:
            delimiter = "\t"
        elif "|" in first_line:
            delimiter = "|"
        elif "  " in first_line:
            delimiter = r"\s{2,}"
        else:
            delimiter = r"\s+"
    elif delimiter == "tab":
        delimiter = "\t"
    elif delimiter == "space":
        delimiter = r"\s+"
    elif delimiter == "pipe":
        delimiter = "|"

    # Parse rows
    rows: List[List[str]] = []
    for line in lines:
        if delimiter in ["\t", "|"]:
            cells = [c.strip() for c in line.split(delimiter)]
        else:
            cells = [c.strip() for c in re.split(delimiter, line)]
        rows.append(cells)

    # Assume first row is header
    if len(rows) > 1:
        headers = rows[0]
        data = rows[1:]

        records = []
        for row in data[:max_preview_rows]:
            record = {}
            for i, val in enumerate(row):
                col_name = headers[i] if i < len(headers) else f"col_{i}"
                record[col_name] = val
            records.append(record)

        return {
            "columns": headers,
            "row_count": len(data),
            "preview": records,
        }
    else:
        return {"raw_row": rows[0]}
