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


# ---------------------------------------------------------------------------
# Detection commands
# ---------------------------------------------------------------------------


@cli.command("erp-detect")
@click.argument("file_or_dir", type=click.Path(exists=True))
@click.option("--pattern", "-p", default="*.csv", help="Glob pattern for batch mode.")
@click.option("--limit", "-n", default=0, help="Max files for batch mode (0 = unlimited).")
@click.option("--all-scores", is_flag=True, help="Show scores for all ERP systems.")
def erp_detect(file_or_dir, pattern, limit, all_scores):
    """Detect source ERP system from COA file fingerprints."""
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table

    from .erp_detect import detect_erp, detect_erp_batch

    console = Console()
    target = Path(file_or_dir)

    if target.is_file():
        with console.status("Analyzing file..."):
            result = detect_erp(str(target), return_all_scores=all_scores)

        if result.get("error"):
            console.print(f"[red]Error: {result['error']}[/red]")
            raise SystemExit(1)

        style = "green" if result["confidence"] >= 0.5 else (
            "yellow" if result["confidence"] >= 0.2 else "red"
        )
        console.print(Panel(
            f"[bold]ERP:[/bold] [{style}]{result['detected_erp']}[/{style}]  |  "
            f"Confidence: [{style}]{result['confidence']:.0%}[/{style}]  |  "
            f"Columns: {result['columns_found']}  |  "
            f"Ambiguous: {'Yes' if result.get('ambiguous') else 'No'}",
            title=f"ERP Detection: {result['file']}",
        ))

        if result.get("signals"):
            for signal in result["signals"]:
                console.print(f"  [dim]{signal}[/dim]")

        if all_scores and result.get("all_scores"):
            table = Table(title="All Scores")
            table.add_column("ERP", style="cyan")
            table.add_column("Score", justify="right")
            table.add_column("Confidence", justify="right")
            table.add_column("Strong Matches", justify="right")

            for s in result["all_scores"]:
                table.add_row(s["erp"], f"{s['score']:.1f}", f"{s['confidence']:.0%}", str(s["strong_matches"]))

            console.print(table)
    else:
        with console.status("Scanning directory..."):
            result = detect_erp_batch(str(target), pattern=pattern, limit=limit)

        if result.get("error"):
            console.print(f"[red]Error: {result['error']}[/red]")
            raise SystemExit(1)

        console.print(Panel(
            f"[bold]Scanned {result['total_files']} files[/bold]  |  "
            f"Errors: {result['errors']}",
            title="ERP Detection: Batch",
        ))

        if result.get("erp_distribution"):
            table = Table(title="ERP Distribution")
            table.add_column("ERP", style="cyan")
            table.add_column("Count", justify="right", style="bold")
            table.add_column("Avg Confidence", justify="right")

            for erp, count in sorted(result["erp_distribution"].items(), key=lambda x: -x[1]):
                avg = result["avg_confidence"].get(erp, 0)
                table.add_row(erp, str(count), f"{avg:.0%}")

            console.print(table)


@cli.command("fraud-detect")
@click.argument("file_or_dir", type=click.Path(exists=True))
@click.option("--checks", "-c", default="", help="Comma-separated checks (default: all 6).")
@click.option("--limit", "-n", default=0, help="Max files for batch mode (0 = unlimited).")
def fraud_detect(file_or_dir, checks, limit):
    """Scan transaction data for fraud indicators (6 pattern types)."""
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table

    from .fraud_detect import detect_fraud, detect_fraud_batch

    console = Console()
    target = Path(file_or_dir)
    check_list = [c.strip() for c in checks.split(",") if c.strip()] or None

    if target.is_file():
        with console.status("Scanning for fraud indicators..."):
            result = detect_fraud(str(target), checks=check_list)

        if result.get("error"):
            console.print(f"[red]Error: {result['error']}[/red]")
            raise SystemExit(1)

        risk = result["risk_score"]
        style = "red" if risk >= 50 else ("yellow" if risk >= 20 else "green")

        console.print(Panel(
            f"[bold]File:[/bold] {result['file']}  |  "
            f"Rows: {result['rows_analyzed']:,}  |  "
            f"Risk Score: [{style}]{risk}/100[/{style}]  |  "
            f"Findings: {result['findings_count']}",
            title="Fraud Detection",
        ))

        if result["findings"]:
            table = Table(title="Findings")
            table.add_column("Type", style="cyan")
            table.add_column("Severity")
            table.add_column("Evidence", overflow="fold")

            for f in result["findings"]:
                sev = f.get("severity", "")
                sev_style = "red" if sev == "CRITICAL" else ("yellow" if sev == "HIGH" else "dim")
                table.add_row(
                    f.get("type", ""),
                    f"[{sev_style}]{sev}[/{sev_style}]",
                    f.get("evidence", ""),
                )

            console.print(table)
        else:
            console.print("[green]No fraud indicators detected.[/green]")
    else:
        with console.status("Scanning directory..."):
            result = detect_fraud_batch(str(target), limit=limit)

        if result.get("error"):
            console.print(f"[red]Error: {result['error']}[/red]")
            raise SystemExit(1)

        console.print(Panel(
            f"[bold]Scanned {result['total_files']} files[/bold]  |  "
            f"Total findings: {result['total_findings']}  |  "
            f"Avg per file: {result['avg_findings_per_file']}",
            title="Fraud Detection: Batch",
        ))

        if result.get("by_type"):
            table = Table(title="Findings by Type")
            table.add_column("Type", style="cyan")
            table.add_column("Count", justify="right", style="bold")

            for ftype, count in sorted(result["by_type"].items(), key=lambda x: -x[1]):
                table.add_row(ftype, str(count))

            console.print(table)

        if result.get("high_risk_files"):
            table = Table(title="High-Risk Files (score >= 50)")
            table.add_column("File", style="red")
            table.add_column("Risk Score", justify="right")
            table.add_column("Findings", justify="right")

            for f in result["high_risk_files"]:
                table.add_row(f["file"], str(f["risk_score"]), str(f["findings"]))

            console.print(table)


@cli.command("fx-validate")
@click.argument("file_or_dir", type=click.Path(exists=True))
@click.option("--limit", "-n", default=0, help="Max files for batch mode (0 = unlimited).")
def fx_validate(file_or_dir, limit):
    """Validate FX translation rates in multi-currency trial balances."""
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table

    from .fx_validate import validate_fx, validate_fx_batch

    console = Console()
    target = Path(file_or_dir)

    if target.is_file():
        with console.status("Validating FX rates..."):
            result = validate_fx(str(target))

        if result.get("error"):
            console.print(f"[red]Error: {result['error']}[/red]")
            raise SystemExit(1)

        risk = result.get("risk_score", 0)
        style = "red" if risk >= 50 else ("yellow" if risk >= 20 else "green")

        console.print(Panel(
            f"[bold]File:[/bold] {result['file']}  |  "
            f"Currencies: {result.get('functional_currency', '?')} -> {result.get('reporting_currency', '?')}  |  "
            f"Accounts: {result['accounts_checked']}  |  "
            f"Risk: [{style}]{risk}/100[/{style}]  |  "
            f"Findings: {result['findings_count']}",
            title="FX Validation",
        ))

        if result["findings"]:
            table = Table(title="Findings")
            table.add_column("Type", style="cyan")
            table.add_column("Severity")
            table.add_column("Account")
            table.add_column("Evidence", overflow="fold")

            for f in result["findings"]:
                sev = f.get("severity", "")
                sev_style = "red" if sev == "CRITICAL" else ("yellow" if sev == "HIGH" else "dim")
                table.add_row(
                    f.get("type", ""),
                    f"[{sev_style}]{sev}[/{sev_style}]",
                    f.get("account", ""),
                    f.get("evidence", ""),
                )

            console.print(table)
        else:
            console.print("[green]No FX issues detected.[/green]")
    else:
        with console.status("Scanning directory..."):
            result = validate_fx_batch(str(target), limit=limit)

        if result.get("error"):
            console.print(f"[red]Error: {result['error']}[/red]")
            raise SystemExit(1)

        console.print(Panel(
            f"[bold]Scanned {result['total_files']} files[/bold]  |  "
            f"Files with issues: {result['files_with_issues']}  |  "
            f"Total findings: {result['total_findings']}",
            title="FX Validation: Batch",
        ))

        if result.get("by_type"):
            table = Table(title="Findings by Type")
            table.add_column("Type", style="cyan")
            table.add_column("Count", justify="right", style="bold")

            for ftype, count in sorted(result["by_type"].items(), key=lambda x: -x[1]):
                table.add_row(ftype, str(count))

            console.print(table)

        if result.get("problem_files"):
            table = Table(title="Problem Files")
            table.add_column("File", style="red")
            table.add_column("Currencies")
            table.add_column("Findings", justify="right")
            table.add_column("Risk", justify="right")

            for f in result["problem_files"]:
                table.add_row(f["file"], f.get("currencies", ""), str(f["findings"]), str(f["risk_score"]))

            console.print(table)


@cli.command("standards-check")
@click.argument("file_or_dir", type=click.Path(exists=True))
@click.option("--standard", "-s", default=None,
              type=click.Choice(["US_GAAP", "IFRS", "JGAAP", "DUAL"], case_sensitive=False),
              help="Override standard (default: auto-detect).")
@click.option("--limit", "-n", default=0, help="Max files for batch mode (0 = unlimited).")
def standards_check(file_or_dir, standard, limit):
    """Check COA files for GAAP/IFRS/J-GAAP compliance violations."""
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table

    from .standards_check import check_standards, check_standards_batch

    console = Console()
    target = Path(file_or_dir)

    if target.is_file():
        with console.status("Checking standards compliance..."):
            result = check_standards(str(target), target_standard=standard)

        if result.get("error"):
            console.print(f"[red]Error: {result['error']}[/red]")
            raise SystemExit(1)

        score = result.get("compliance_score", 0)
        style = "green" if score >= 80 else ("yellow" if score >= 50 else "red")

        console.print(Panel(
            f"[bold]File:[/bold] {result['file']}  |  "
            f"Standard: {result['detected_standard']}  |  "
            f"Accounts: {result['accounts_checked']}  |  "
            f"Compliance: [{style}]{score}/100[/{style}]  |  "
            f"Findings: {result['findings_count']}",
            title="Standards Compliance",
        ))

        if result["findings"]:
            table = Table(title="Findings")
            table.add_column("Type", style="cyan")
            table.add_column("Severity")
            table.add_column("Account")
            table.add_column("Issue", overflow="fold")

            for f in result["findings"]:
                sev = f.get("severity", "")
                sev_style = "red" if sev == "CRITICAL" else (
                    "yellow" if sev == "HIGH" else "dim"
                )
                table.add_row(
                    f.get("type", ""),
                    f"[{sev_style}]{sev}[/{sev_style}]",
                    f.get("account", ""),
                    f.get("issue", ""),
                )

            console.print(table)
        else:
            console.print("[green]Fully compliant. No issues found.[/green]")
    else:
        with console.status("Scanning directory..."):
            result = check_standards_batch(str(target), target_standard=standard, limit=limit)

        if result.get("error"):
            console.print(f"[red]Error: {result['error']}[/red]")
            raise SystemExit(1)

        avg = result.get("avg_compliance_score", 0)
        style = "green" if avg >= 80 else ("yellow" if avg >= 50 else "red")

        console.print(Panel(
            f"[bold]Scanned {result['total_files']} files[/bold]  |  "
            f"Non-compliant: {result['non_compliant_files']}  |  "
            f"Total findings: {result['total_findings']}  |  "
            f"Avg score: [{style}]{avg}[/{style}]",
            title="Standards Compliance: Batch",
        ))

        if result.get("by_standard"):
            table = Table(title="Files by Standard")
            table.add_column("Standard", style="cyan")
            table.add_column("Count", justify="right", style="bold")

            for std, count in sorted(result["by_standard"].items(), key=lambda x: -x[1]):
                table.add_row(std, str(count))

            console.print(table)

        if result.get("non_compliant_files_detail"):
            table = Table(title="Non-Compliant Files")
            table.add_column("File", style="red")
            table.add_column("Standard")
            table.add_column("Findings", justify="right")
            table.add_column("Score", justify="right")

            for f in result["non_compliant_files_detail"]:
                table.add_row(
                    f["file"], f.get("standard", ""), str(f["findings"]),
                    str(f.get("compliance_score", 0)),
                )

            console.print(table)


@cli.command("link-entities")
@click.argument("logic_dna_dir", type=click.Path(exists=True))
@click.option("--output", "-o", default="data/linker", help="Output directory for entity map")
@click.option("--threshold", "-t", default=0.65, type=float, help="Link threshold (default 0.65)")
def link_entities_cmd(logic_dna_dir, output, threshold):
    """Resolve entities across Logic DNA files."""
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel

    from .linker import link_entities

    console = Console()

    with console.status("Linking entities..."):
        result = link_entities(logic_dna_dir, output_dir=output, threshold=threshold)

    if result.get("error"):
        console.print(f"[red]Error: {result['error']}[/red]")
        raise SystemExit(1)

    summary = result.get("summary", {})
    console.print(Panel(
        f"[bold]Clusters: {summary.get('total_clusters', 0)}[/bold]  |  "
        f"Mentions: {summary.get('total_mentions', 0)}  |  "
        f"Links: {summary.get('total_links', 0)}  |  "
        f"Conflicts: {summary.get('total_conflicts', 0)}  |  "
        f"Files: {summary.get('files_processed', 0)}",
        title="Entity Linking",
    ))

    sample = result.get("sample_clusters", [])
    if sample:
        table = Table(title=f"Top {len(sample)} Clusters")
        table.add_column("Name", style="cyan")
        table.add_column("Domain")
        table.add_column("Mentions", justify="right")
        table.add_column("Files", justify="right")
        table.add_column("Confidence", justify="right")

        for c in sample:
            table.add_row(
                c["canonical_name"][:40],
                c.get("domain", ""),
                str(c.get("mention_count", 0)),
                str(c.get("file_count", 0)),
                f"{c.get('avg_confidence', 0):.2f}",
            )

        console.print(table)


@cli.command("expect")
@click.argument("file", type=click.Path(exists=True))
@click.option("--name", "-n", default=None, help="Suite name (defaults to filename)")
@click.option("--output", "-o", default="data/expectations", help="Output directory")
def expect_cmd(file, name, output):
    """Generate data quality expectations from a file."""
    from rich.console import Console
    from rich.panel import Panel

    from .profiler import generate_expectation_suite

    console = Console()

    with console.status("Generating expectations..."):
        result = generate_expectation_suite(file, name=name, output_dir=output)

    console.print(Panel(
        f"[bold]Suite: {result['suite_name']}[/bold]  |  "
        f"Expectations: {result['expectations_count']}  |  "
        f"Output: {result['output_file']}",
        title="Expectation Suite Generated",
    ))


@cli.command("validate")
@click.argument("file", type=click.Path(exists=True))
@click.option("--suite", "-s", required=True, help="Suite name or path to suite JSON")
@click.option("--suite-dir", default="data/expectations", help="Suite directory")
def validate_cmd(file, suite, suite_dir):
    """Validate a data file against an expectation suite."""
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel

    from .profiler import validate

    console = Console()

    suite_path = suite if suite.endswith(".json") else None
    suite_name = None if suite_path else suite

    with console.status("Validating..."):
        result = validate(file, suite_path=suite_path, suite_name=suite_name, suite_dir=suite_dir)

    status = result["status"]
    style = "green" if status == "passed" else "red"

    console.print(Panel(
        f"[bold]Status: [{style}]{status.upper()}[/{style}][/bold]  |  "
        f"Passed: {result['passed']}/{result['total_expectations']}  |  "
        f"Success: {result['success_percent']}%  |  "
        f"Duration: {result['duration_seconds']}s",
        title=f"Validation: {result['suite_name']}",
    ))

    if result["failures"]:
        table = Table(title="Failures")
        table.add_column("Expectation", style="cyan")
        table.add_column("Column")
        table.add_column("Expected")
        table.add_column("Observed", style="red")

        for f in result["failures"]:
            table.add_row(
                f.get("expectation", ""),
                f.get("column", ""),
                str(f.get("expected", "")),
                str(f.get("observed", f.get("detail", ""))),
            )

        console.print(table)


@cli.command("query")
@click.argument("sql")
@click.option("--register", "-r", multiple=True, help="Register file as table: name=path")
@click.option("--limit", "-l", default=10, type=int, help="Max preview rows")
def query_cmd(sql, register, limit):
    """Execute SQL against local files using DuckDB."""
    from rich.console import Console
    from rich.table import Table

    from .connectors import query_local

    console = Console()

    reg_files = {}
    for r in register:
        if "=" in r:
            name, path = r.split("=", 1)
            reg_files[name] = path

    with console.status("Querying..."):
        result = query_local(sql, register_files=reg_files or None, max_preview_rows=limit)

    console.print(f"[bold]{result['rows_returned']} rows returned[/bold]")

    if result.get("preview"):
        table = Table()
        cols = result.get("columns", [])
        for c in cols:
            table.add_column(c)

        for row in result["preview"]:
            table.add_row(*[str(row.get(c, "")) for c in cols])

        console.print(table)

        if result.get("truncated"):
            console.print(f"[dim]... showing {limit} of {result['rows_returned']} rows[/dim]")
