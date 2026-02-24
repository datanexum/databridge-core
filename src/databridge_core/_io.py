"""Shared I/O helpers."""

import pandas as pd


def read_csv(file_path: str) -> pd.DataFrame:
    """Read a CSV file into a DataFrame."""
    return pd.read_csv(file_path)
