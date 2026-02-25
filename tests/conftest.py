"""Shared test fixtures for databridge-core."""

import csv
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def tmp_dir(tmp_path):
    """Return a temporary directory."""
    return tmp_path


@pytest.fixture
def customers_a(tmp_path):
    """Create a 10-row 'source of truth' CSV."""
    path = tmp_path / "customers_a.csv"
    rows = [
        {"id": "1", "name": "Alice Johnson", "email": "alice@example.com", "city": "New York", "balance": "1500.00"},
        {"id": "2", "name": "Bob Smith", "email": "bob@example.com", "city": "Chicago", "balance": "2300.50"},
        {"id": "3", "name": "Charlie Brown", "email": "charlie@example.com", "city": "Houston", "balance": "850.75"},
        {"id": "4", "name": "Diana Prince", "email": "diana@example.com", "city": "Phoenix", "balance": "3200.00"},
        {"id": "5", "name": "Eve Williams", "email": "eve@example.com", "city": "San Antonio", "balance": "1100.25"},
        {"id": "6", "name": "Frank Castle", "email": "frank@example.com", "city": "Dallas", "balance": "4500.00"},
        {"id": "7", "name": "Grace Hopper", "email": "grace@example.com", "city": "San Jose", "balance": "2750.30"},
        {"id": "8", "name": "Hank Pym", "email": "hank@example.com", "city": "Austin", "balance": "990.00"},
        {"id": "9", "name": "Ivy League", "email": "ivy@example.com", "city": "Columbus", "balance": "1800.60"},
        {"id": "10", "name": "Jack Ryan", "email": "jack@example.com", "city": "Charlotte", "balance": "3100.45"},
    ]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    return str(path)


@pytest.fixture
def customers_b(tmp_path):
    """Create a 9-row 'target' CSV with conflicts and orphans."""
    path = tmp_path / "customers_b.csv"
    rows = [
        {"id": "1", "name": "Alice Johnson", "email": "alice@example.com", "city": "New York", "balance": "1500.00"},
        {"id": "2", "name": "Bob Smith", "email": "bob@example.com", "city": "Chicago", "balance": "2400.50"},  # conflict: balance
        {"id": "3", "name": "Charles Brown", "email": "charlie@example.com", "city": "Houston", "balance": "850.75"},  # conflict: name
        {"id": "4", "name": "Diana Prince", "email": "diana@example.com", "city": "Scottsdale", "balance": "3200.00"},  # conflict: city
        {"id": "5", "name": "Eve Williams", "email": "eve@example.com", "city": "San Antonio", "balance": "1100.25"},
        {"id": "6", "name": "Frank Castle", "email": "frank@example.com", "city": "Dallas", "balance": "4500.00"},
        {"id": "7", "name": "Grace Hopper", "email": "grace@example.com", "city": "San Jose", "balance": "2750.30"},
        {"id": "8", "name": "Hank Pym", "email": "hank@example.com", "city": "Austin", "balance": "990.00"},
        # id 9 missing (orphan in A), id 10 missing (orphan in A)
        {"id": "11", "name": "Kate Bishop", "email": "kate@example.com", "city": "Denver", "balance": "2100.00"},  # orphan in B
    ]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    return str(path)
