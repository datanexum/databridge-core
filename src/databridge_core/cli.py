"""DataBridge Core CLI -- Rich-formatted data reconciliation from the terminal."""

import json
import sys
from pathlib import Path

import click


@click.group()
@click.version_option(package_name="databridge-core")
def cli():
    """DataBridge Core -- Data reconciliation, profiling, and ingestion toolkit."""
    pass


@cli.command()
@click.argument("file", type=click.Path(exists=True))
def profile(file):
    """Profile a CSV file: structure, quality, cardinality."""
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel

    from .profiler import profile_data

    console = Console()

    with console.status("Profiling..."):
        result = profile_data(file)

    console.print(Panel(
        f"[bold]{result['file']}[/bold]\n"
        f"Rows: {result['rows']:,}  |  Columns: {result['columns']}  |  "
        f"Type: {result['structure_type']}",
        title="Profile Summary",
    ))

    # Column types table
    table = Table(title="Columns")
    table.add_column("Column", style="cyan")
    table.add_column("Type", style="green")
    table.add_column("Nulls %", style="yellow")
    table.add_column("Cardinality", style="magenta")

    null_pct = result["data_quality"]["null_percentage"]
    for col, dtype in result["column_types"].items():
        card = "KEY" if col in result["potential_key_columns"] else (
            "HIGH" if col in result["high_cardinality_cols"] else (
                "LOW" if col in result["low_cardinality_cols"] else "-"
            )
        )
        table.add_row(col, dtype, f"{null_pct.get(col, 0):.1f}%", card)

    console.print(table)

    dq = result["data_quality"]
    console.print(f"\nDuplicate rows: {dq['duplicate_rows']} ({dq['duplicate_percentage']}%)")

    if result["potential_key_columns"]:
        console.print(
            f"Potential key columns: [bold cyan]{', '.join(result['potential_key_columns'])}[/bold cyan]"
        )


@cli.command()
@click.argument("source_a", type=click.Path(exists=True))
@click.argument("source_b", type=click.Path(exists=True))
@click.option("--keys", required=True, help="Comma-separated key columns.")
@click.option("--compare", default="", help="Comma-separated compare columns (default: all non-key).")
def compare(source_a, source_b, keys, compare):
    """Compare two CSV files by hashing rows."""
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table

    from .reconciler import compare_hashes

    console = Console()

    with console.status("Comparing..."):
        result = compare_hashes(source_a, source_b, keys, compare)

    stats = result["statistics"]

    console.print(Panel(
        f"Source A: {result['source_a']['total_rows']:,} rows  |  "
        f"Source B: {result['source_b']['total_rows']:,} rows\n"
        f"Keys: {', '.join(result['key_columns'])}  |  "
        f"Compare: {len(result['compare_columns'])} columns",
        title="Comparison",
    ))

    table = Table(title="Results")
    table.add_column("Metric", style="cyan")
    table.add_column("Count", style="bold", justify="right")

    match_style = "green" if stats["match_rate_percent"] >= 90 else (
        "yellow" if stats["match_rate_percent"] >= 70 else "red"
    )

    table.add_row("Exact matches", str(stats["exact_matches"]))
    table.add_row("Conflicts", f"[red]{stats['conflicts']}[/red]" if stats["conflicts"] else "0")
    table.add_row("Orphans in A only", str(stats["orphans_only_in_source_a"]))
    table.add_row("Orphans in B only", str(stats["orphans_only_in_source_b"]))
    table.add_row(
        "Match rate",
        f"[{match_style}]{stats['match_rate_percent']}%[/{match_style}]",
    )

    console.print(table)


@cli.command()
@click.argument("source_a", type=click.Path(exists=True))
@click.argument("source_b", type=click.Path(exists=True))
@click.option("--column", "-c", required=True, help="Column name in source A.")
@click.option("--column-b", default="", help="Column name in source B (default: same as --column).")
@click.option("--threshold", "-t", default=80, help="Minimum similarity (0-100).")
@click.option("--limit", "-n", default=10, help="Maximum matches to show.")
def fuzzy(source_a, source_b, column, column_b, threshold, limit):
    """Find fuzzy matches between two CSV columns."""
    from rich.console import Console
    from rich.table import Table

    from .reconciler import fuzzy_match_columns

    console = Console()
    col_b = column_b or column

    with console.status("Fuzzy matching..."):
        result = fuzzy_match_columns(source_a, source_b, column, col_b, threshold, limit)

    console.print(f"\n[bold]Found {result['total_matches']} matches[/bold] "
                  f"(threshold: {threshold}%)\n")

    table = Table(title="Top Matches")
    table.add_column("Value A", style="cyan")
    table.add_column("Value B", style="green")
    table.add_column("Score", justify="right", style="bold")

    for m in result["top_matches"]:
        score = m["similarity"]
        style = "green" if score >= 90 else ("yellow" if score >= 80 else "red")
        table.add_row(m["value_a"], m["value_b"], f"[{style}]{score:.0f}%[/{style}]")

    console.print(table)


@cli.command()
@click.argument("file_a", type=click.Path(exists=True))
@click.argument("file_b", type=click.Path(exists=True))
def diff(file_a, file_b):
    """Show text diff between two files."""
    from rich.console import Console
    from rich.syntax import Syntax

    from .reconciler import unified_diff

    console = Console()

    text_a = Path(file_a).read_text()
    text_b = Path(file_b).read_text()

    result = unified_diff(text_a, text_b, from_label=file_a, to_label=file_b)

    if result:
        console.print(Syntax(result, "diff", theme="monokai"))
    else:
        console.print("[green]Files are identical.[/green]")


@cli.command()
@click.argument("old_file", type=click.Path(exists=True))
@click.argument("new_file", type=click.Path(exists=True))
def drift(old_file, new_file):
    """Detect schema drift between two CSV files."""
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table

    from .profiler import detect_schema_drift

    console = Console()

    with console.status("Detecting drift..."):
        result = detect_schema_drift(old_file, new_file)

    if not result["has_drift"]:
        console.print("[green]No schema drift detected.[/green]")
        return

    console.print(Panel("[bold red]Schema drift detected[/bold red]", title="Drift Report"))

    if result["columns_added"]:
        console.print(f"[green]+ Added:[/green] {', '.join(result['columns_added'])}")
    if result["columns_removed"]:
        console.print(f"[red]- Removed:[/red] {', '.join(result['columns_removed'])}")

    if result["type_changes"]:
        table = Table(title="Type Changes")
        table.add_column("Column", style="cyan")
        table.add_column("From", style="red")
        table.add_column("To", style="green")
        table.add_column("Safe?", justify="center")

        for col, info in result["type_changes"].items():
            safe = info.get("safe_conversion")
            safe_str = "[green]Yes[/green]" if safe else (
                "[red]No[/red]" if safe is False else "-"
            )
            table.add_row(col, info["from"], info["to"], safe_str)

        console.print(table)


@cli.command()
@click.argument("file", type=click.Path(exists=True))
@click.option("--column", "-c", required=True, help="Column to transform.")
@click.option("--op", required=True, type=click.Choice(
    ["upper", "lower", "strip", "trim_spaces", "remove_special"]
), help="Transformation operation.")
@click.option("--output", "-o", default="", help="Output file path (default: preview only).")
def transform(file, column, op, output):
    """Apply a string transformation to a CSV column."""
    from rich.console import Console
    from rich.table import Table

    from .reconciler import transform_column

    console = Console()

    result = transform_column(file, column, op, output)

    table = Table(title=f"Transform: {op}({column})")
    table.add_column("Before", style="red")
    table.add_column("After", style="green")

    for before, after in zip(result["preview"]["before"], result["preview"]["after"]):
        table.add_row(str(before), str(after))

    console.print(table)

    if "saved_to" in result:
        console.print(f"\nSaved to: [bold]{result['saved_to']}[/bold]")
    else:
        console.print(f"\n[dim]{result.get('note', '')}[/dim]")


@cli.command()
@click.argument("source_a", type=click.Path(exists=True))
@click.argument("source_b", type=click.Path(exists=True))
@click.option("--keys", required=True, help="Comma-separated key columns.")
@click.option("--type", "merge_type", default="inner",
              type=click.Choice(["inner", "left", "right", "outer"]),
              help="Merge type (default: inner).")
@click.option("--output", "-o", default="", help="Output file path.")
def merge(source_a, source_b, keys, merge_type, output):
    """Merge two CSV files on key columns."""
    from rich.console import Console
    from rich.table import Table

    from .reconciler import merge_sources

    console = Console()

    with console.status("Merging..."):
        result = merge_sources(source_a, source_b, keys, merge_type, output)

    console.print(
        f"\n[bold]Merged:[/bold] {result['source_a_rows']:,} + {result['source_b_rows']:,} "
        f"-> {result['merged_rows']:,} rows ({merge_type})"
    )

    if result["preview"]:
        table = Table(title="Preview (first rows)")
        for col in result["columns"][:10]:
            table.add_column(col, style="cyan", overflow="fold")

        for row in result["preview"][:5]:
            table.add_row(*[str(row.get(c, "")) for c in result["columns"][:10]])

        console.print(table)

    if "saved_to" in result:
        console.print(f"\nSaved to: [bold]{result['saved_to']}[/bold]")


@cli.command()
@click.argument("pattern", default="*.csv")
@click.option("--name", "-n", default="", help="Filename substring filter.")
def find(pattern, name):
    """Find files matching a glob pattern."""
    from rich.console import Console
    from rich.table import Table

    from .files import find_files

    console = Console()

    with console.status("Searching..."):
        result = find_files(pattern, name)

    console.print(f"\n[bold]Found {result['files_found']} files[/bold]\n")

    if result["files"]:
        table = Table()
        table.add_column("Name", style="cyan")
        table.add_column("Size", justify="right")
        table.add_column("Modified", style="dim")
        table.add_column("Directory", style="dim", overflow="fold")

        for f in result["files"]:
            table.add_row(f["name"], f"{f['size_kb']:.1f} KB", f["modified"][:16], f["directory"])

        console.print(table)


@cli.command()
@click.argument("directory", type=click.Path(exists=True))
@click.option("--output", "-o", default="data/triage", help="Output directory for reports.")
@click.option("--workers", "-w", default=4, help="Number of parallel workers.")
def triage(directory, output, workers):
    """Scan a directory of Excel files and classify by archetype."""
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel

    console = Console()

    try:
        from .triage import scan_and_classify
    except ImportError:
        console.print("[red]Triage requires openpyxl. Install with:[/red]")
        console.print("  pip install 'databridge-core[triage]'")
        raise SystemExit(1)

    with console.status("Scanning Excel files..."):
        result = scan_and_classify(
            directory=directory,
            output_dir=output,
            max_workers=workers,
        )

    summary = result["summary"]
    console.print(Panel(
        f"[bold]Scanned {summary['total_files']} files[/bold] in {summary['duration_seconds']:.1f}s "
        f"({summary['files_per_second']:.1f} files/sec)\n"
        f"OK: {summary['scanned']}  |  Errors: {summary['errors']}  |  Skipped: {summary['skipped']}",
        title="Triage Summary",
    ))

    if summary.get("archetype_counts"):
        table = Table(title="Archetype Distribution")
        table.add_column("Archetype", style="cyan")
        table.add_column("Count", justify="right", style="bold")

        for archetype, count in sorted(summary["archetype_counts"].items(), key=lambda x: -x[1]):
            table.add_row(archetype, str(count))

        console.print(table)

    console.print(f"\nReports saved to: [bold]{output}[/bold]")


@cli.command()
@click.argument("text", required=False)
@click.option("--file", "-f", type=click.Path(exists=True), help="Read text from file.")
@click.option("--delimiter", "-d", default="auto", help="Column delimiter.")
def parse(text, file, delimiter):
    """Parse tabular data from text or a file."""
    from rich.console import Console
    from rich.table import Table

    from .ingestion import parse_table_from_text

    console = Console()

    if file:
        text = Path(file).read_text()
    elif not text:
        text = click.get_text_stream("stdin").read()

    if not text:
        console.print("[red]No input text provided.[/red]")
        raise SystemExit(1)

    result = parse_table_from_text(text, delimiter)

    if "raw_row" in result and result["raw_row"]:
        console.print(f"Single row: {result['raw_row']}")
        return

    console.print(f"\n[bold]Parsed {result['row_count']} rows[/bold]\n")

    table = Table(title="Parsed Table")
    for col in result["columns"]:
        table.add_column(col, style="cyan")

    for row in result["preview"]:
        table.add_row(*[str(row.get(c, "")) for c in result["columns"]])

    console.print(table)
