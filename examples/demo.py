"""DataBridge Core -- Quick demo.

Run: python examples/demo.py
"""

from pathlib import Path

# Resolve example file paths
examples_dir = Path(__file__).parent
file_a = str(examples_dir / "customers_a.csv")
file_b = str(examples_dir / "customers_b.csv")


def main():
    from databridge_core import compare_hashes, profile_data, load_csv

    # 1. Profile the source file
    print("=" * 60)
    print("1. PROFILE SOURCE DATA")
    print("=" * 60)
    profile = profile_data(file_a)
    print(f"  File: {profile['file']}")
    print(f"  Rows: {profile['rows']}, Columns: {profile['columns']}")
    print(f"  Type: {profile['structure_type']}")
    print(f"  Potential keys: {profile['potential_key_columns']}")
    print()

    # 2. Compare two sources
    print("=" * 60)
    print("2. COMPARE SOURCES")
    print("=" * 60)
    result = compare_hashes(file_a, file_b, key_columns="id")
    stats = result["statistics"]
    print(f"  Source A: {result['source_a']['total_rows']} rows")
    print(f"  Source B: {result['source_b']['total_rows']} rows")
    print(f"  Exact matches: {stats['exact_matches']}")
    print(f"  Conflicts: {stats['conflicts']}")
    print(f"  Orphans in A: {stats['orphans_only_in_source_a']}")
    print(f"  Orphans in B: {stats['orphans_only_in_source_b']}")
    print(f"  Match rate: {stats['match_rate_percent']}%")
    print()

    # 3. Load and preview
    print("=" * 60)
    print("3. LOAD & PREVIEW")
    print("=" * 60)
    loaded = load_csv(file_a, preview_rows=3)
    print(f"  Columns: {loaded['columns']}")
    print(f"  Preview (first 3 rows):")
    for row in loaded["preview"]:
        print(f"    {row}")
    print()

    print("Done! Try the CLI: databridge profile examples/customers_a.csv")


if __name__ == "__main__":
    main()
