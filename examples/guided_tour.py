import sys
import time
import random
import json
from pathlib import Path
from datetime import datetime

import pandas as pd
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.markdown import Markdown
from rich.prompt import Confirm, Prompt
from rich.align import Align
from rich.text import Text
from rich.theme import Theme
from rich import box

try:
    from databridge_core import (
        fuzzy_match_columns,
        compare_hashes,
        get_orphan_details,
        get_conflict_details
    )
except ImportError:
    # Fallback for running directly from examples/ without pip install
    _core_src = Path(__file__).resolve().parent.parent / "src"
    sys.path.insert(0, str(_core_src))
    from databridge_core import (
        fuzzy_match_columns,
        compare_hashes,
        get_orphan_details,
        get_conflict_details
    )

# ---------------------------------------------------------------------------
# Themes
# ---------------------------------------------------------------------------
MODERN_THEME = Theme({
    "info": "cyan",
    "warning": "yellow",
    "error": "red",
    "success": "green",
    "header": "bold magenta",
    "prompt": "bold yellow"
})

RETRO_THEME = Theme({
    "info": "green",
    "warning": "green",
    "error": "bold green",
    "success": "bold green",
    "header": "bold green",
    "prompt": "bold green",
    "text": "green"
})

TRON_THEME = Theme({
    "info": "bright_cyan",
    "warning": "bright_blue",
    "error": "bright_red",
    "success": "bright_cyan",
    "header": "bold bright_cyan",
    "prompt": "bold bright_blue",
    "text": "cyan",
    "grid": "dim cyan",
})

# ---------------------------------------------------------------------------
# Retro 80s Easter Eggs
# ---------------------------------------------------------------------------
RETRO_QUOTES = [
    '"I feel the need... the need for clean data." -- Top Gun, 1986',
    '"Life moves pretty fast. If you don\'t reconcile, you could miss it." -- Ferris Bueller, 1986',
    '"I\'ll be back... with the audit results." -- The Terminator, 1984',
    '"Roads? Where we\'re going, we don\'t need spreadsheets." -- Back to the Future, 1985',
    '"Here\'s looking at you, dataset." -- Casablanca (re-run on VHS, 1985)',
    '"Game over, man! Game over!" -- Aliens, 1986 (when data doesn\'t match)',
    '"E.T. phone home... for the reconciliation report." -- E.T., 1982',
    '"Wax on, data off." -- The Karate Kid, 1984',
]


# ---------------------------------------------------------------------------
# Helpers — Shared
# ---------------------------------------------------------------------------
def slow_type(console, text, version, delay=0.02):
    """Simulates typing effect."""
    if version == "TRON":
        for i, char in enumerate(text):
            style = "bold bright_white" if i == len(text) - 1 else "bright_cyan"
            console.print(char, end="", style=style)
            time.sleep(delay)
        console.print(style="bright_cyan")
    else:
        style = "green" if version == "1985" else "dim"
        for char in text:
            console.print(char, end="", style=style)
            time.sleep(delay)
        console.print()


def show_data_sample(console, df, title, version, rows=5):
    """Renders a beautiful table of the data."""
    if version == "TRON":
        style, box_type = "bright_cyan", box.DOUBLE
    elif version == "1985":
        style, box_type = "green", box.ASCII
    else:
        style, box_type = "bold magenta", box.ROUNDED

    table = Table(title=title, show_header=True, header_style=style, box=box_type)

    if df.empty:
        if version == "TRON":
            console.print("[bright_cyan]GRID ERROR: NO DATA FRAGMENTS FOUND[/bright_cyan]")
        elif version == "Modern":
            console.print("[red]No data to display.[/red]")
        else:
            console.print("[green]ERROR: NO DATA[/green]")
        return

    col_style = "dim cyan" if version == "TRON" else None
    for col in df.columns[:6]:
        table.add_column(str(col), style=col_style)

    for _, row in df.head(rows).iterrows():
        table.add_row(*[str(val) for val in row.values[:6]])

    console.print(table)


def thinking_animation(console, label, version, duration=2):
    """Simulates AI agent analysis."""
    if version == "1985":
        retro_pacman_progress(console, f">>> {label.upper()}")
        return
    if version == "TRON":
        tron_disc_animation(console, label)
        return

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        transient=True,
    ) as progress:
        task = progress.add_task(f"[cyan]{label}...", total=100)
        while not progress.finished:
            progress.update(task, advance=random.randint(5, 15))
            time.sleep(duration / 10)


# ---------------------------------------------------------------------------
# Retro 80s Helpers
# ---------------------------------------------------------------------------
def get_retro_header():
    return """
************************************************************
*                                                          *
*   DATABRIDGE OS v1.0 (c) 1985 DATANEXUM SYSTEMS         *
*   640K RAM DETECTED. READY.                              *
*                                                          *
*   "Where we're going, we don't need spreadsheets."       *
*                                                          *
************************************************************
    """


def retro_boot_sequence(console):
    """80s-style boot sequence with period-appropriate flair."""
    boot_lines = [
        "BIOS v1.0 ... OK",
        "CHECKING EXTENDED MEMORY ... 640K FOUND",
        "LOADING COMMAND.COM ...",
        "HIMEM.SYS ... LOADED",
        "MOUSE.COM ... NOT FOUND (WHO NEEDS ONE?)",
        "DATABRIDGE.EXE ... LOADED",
        'SYNTH MODULE ... * "Take On Me" *',
        "",
        "C:\\DATABRIDGE> _",
    ]
    for line in boot_lines:
        console.print(f"[green]{line}[/green]")
        time.sleep(0.3)
    console.print()


def retro_wargames_prompt(console, question):
    """WarGames-style 'SHALL WE PLAY A GAME?' prompt."""
    console.print("[green]+------------------------------------------+[/green]")
    console.print(f"[green]|  {question:<38s}  |[/green]")
    console.print("[green]+------------------------------------------+[/green]")


def retro_pacman_progress(console, label, width=30):
    """Pac-Man style progress bar."""
    dots = "." * width
    console.print(f"[green]{label}[/green]")
    for i in range(width + 1):
        eaten = "=" * i
        pac = "C" if i < width else "O"
        remaining = dots[i + 1:] if i < width else ""
        bar = f"  [{eaten}{pac}{remaining}]"
        console.print(f"[green]{bar}[/green]", end="\r")
        time.sleep(0.06)
    console.print()


# ---------------------------------------------------------------------------
# TRON Helpers
# ---------------------------------------------------------------------------
def get_tron_header():
    return """[bright_cyan]
+--------------------------------------------------------------+
|  +--+ +--+ +--+ +--+  +-+ +--+--++--+ +--+ +--+              |
|   ||| +--+ ||| +--+  +--+ +--+ ||| ||| +--+ +--+             |
|  +--+ +--+ +--+ +--+  +--+ +--+ +--+ +--+ +--+               |
|                                                              |
|             B Y   D A T A N E X U M                          |
|                                                              |
|        D A T A B R I D G E   G R I D   v 4 . 4              |
|           -----------------------------                      |
|                 << ENTERING THE GRID >>                      |
+--------------------------------------------------------------+
[/bright_cyan]"""


def tron_grid_animation(console, duration=1.5):
    """Animated grid scan effect."""
    grid_width = 60
    frames = [
        "| . . . | . . . | . . . | . . . | . . . | . . . | . . . |",
        "| - - . | . . . | . . . | . . . | . . . | . . . | . . . |",
        "| - - - - - . . | . . . | . . . | . . . | . . . | . . . |",
        "| . . . | - - - - - . . | . . . | . . . | . . . | . . . |",
        "| . . . | . . . | - - - - - . . | . . . | . . . | . . . |",
        "| . . . | . . . | . . . | - - - - - . . | . . . | . . . |",
        "| . . . | . . . | . . . | . . . | - - - - - . . | . . . |",
        "| . . . | . . . | . . . | . . . | . . . | - - - - - . . |",
        "| . . . | . . . | . . . | . . . | . . . | . . . | - - - -",
    ]
    delay = duration / len(frames)
    for frame in frames:
        console.print(f"[dim cyan]{frame}[/dim cyan]", end="\r")
        time.sleep(delay)
    console.print(f"[bright_cyan]{'-' * grid_width}[/bright_cyan]")


def tron_disc_animation(console, label):
    """Identity disc throw animation with spinning disc frames."""
    phases = ["SCANNING", "MATCHING", "ENCODING", "RESOLVED"]
    markers = ["*", ".", "*", "."]
    console.print(f"\n[bright_cyan]  DISC ENGAGED: {label.upper()}[/bright_cyan]")
    for phase, marker in zip(phases, markers):
        disc = (
            f"    /--------\\\n"
            f"   /  {marker}    {marker}  \\\n"
            f"  | {phase:^10s} |\n"
            f"   \\  {marker}    {marker}  /\n"
            f"    \\--------/"
        )
        console.print(f"[bright_cyan]{disc}[/bright_cyan]")
        time.sleep(0.4)
    console.print()


def get_tron_end_of_line():
    return """[bright_cyan]
+--------------------------------------------------------------+
|                                                              |
|   +--+ +-+--+  +--+ +--+  +  +--+--+                         |
|   |--+ | | |  |--+ |--+   |  | | |--+                        |
|   +--+ +-+--+  +--+ +     +--+ + +--+                        |
|                                                              |
|            << E N D   O F   L I N E >>                       |
|                                                              |
+--------------------------------------------------------------+
[/bright_cyan]"""


# ---------------------------------------------------------------------------
# Main Tour
# ---------------------------------------------------------------------------
def run_tour():
    version = "Modern"
    console = Console(theme=MODERN_THEME)
    console.clear()
    kb_results = {"scenarios": {}}

    # --- VERSION SELECTION ---
    console.print(Panel(
        "Select your interface version:\n[1] Modern Experience\n[2] 1985 Retro Mode\n[3] The Grid",
        title="Boot Menu"
    ))
    choice = Prompt.ask("Choose version", choices=["1", "2", "3"], default="1")

    if choice == "2":
        version = "1985"
        console = Console(theme=RETRO_THEME, color_system="standard")
        console.clear()
        console.print(get_retro_header())
        retro_boot_sequence(console)
        time.sleep(0.5)
    elif choice == "3":
        version = "TRON"
        console = Console(theme=TRON_THEME)
        console.clear()
        console.print(get_tron_header())
        tron_grid_animation(console)
        time.sleep(0.5)

    kb_results["tour_version"] = version

    # --- WELCOME ---
    if version == "TRON":
        slow_type(console, "\nGREETINGS, PROGRAM.", version)
        slow_type(console, "YOU HAVE ENTERED THE DATABRIDGE GRID.", version)
        slow_type(console, "YOUR MISSION: RECTIFY CORRUPTED DATA ACROSS 4 GRID SECTORS.", version)
        slow_type(console, "THE MCP AWAITS YOUR COMMANDS.", version)
    elif version == "1985":
        slow_type(console, "\nWELCOME TO THE DATABRIDGE FINANCIAL EXPERT SYSTEM.", version)
        slow_type(console, "LOADING KNOWLEDGE BASE...", version)
        slow_type(console, "OBJECTIVE: AUTOMATED DATA RECONCILIATION.", version)
        console.print(f"\n[green]  {random.choice(RETRO_QUOTES)}[/green]\n")
    elif version == "Modern":
        welcome_msg = """
# DataBridge AI: The Invisible Architect
## Interactive Guided Tour (Large Dataset Edition)

Welcome! You are about to see how DataBridge Core automates the most
tedious manual data tasks in Finance and Operations.

**Objective:** Reduce Human API time from hours to seconds.
        """
        console.print(Align.center(Panel(Markdown(welcome_msg), border_style="bold green", expand=False)))

    time.sleep(1)

    if version == "TRON":
        ready_prompt = "[bright_cyan]INITIALIZE GRID SEQUENCE? (Y/N)[/bright_cyan]"
    elif version == "Modern":
        ready_prompt = "[bold yellow]Ready to start the simulation?[/bold yellow]"
    else:
        retro_wargames_prompt(console, "SHALL WE PLAY A GAME?")
        ready_prompt = "[green]INITIATE SIMULATION? (Y/N)[/green]"

    if not Confirm.ask(ready_prompt):
        if version == "TRON":
            console.print("[bright_cyan]PROGRAM DEREZZED. RETURNING TO PORTAL.[/bright_cyan]")
        elif version == "Modern":
            console.print("[red]Tour cancelled.[/red]")
        else:
            console.print('[green]"Game over, man!" -- Hudson, Aliens (1986)[/green]')
        return

    data_dir = Path(__file__).resolve().parent / "tour_data"
    if not data_dir.exists():
        console.print(f"[bold red]Error: tour_data directory not found at {data_dir}[/bold red]")
        console.print("Install with: [bold]pip install databridge-core[/bold]")
        return

    # ===================================================================
    # SCENARIO 1: THE MESSY EXPORT
    # ===================================================================
    if version == "TRON":
        title = "[bright_cyan]+--- GRID SECTOR 1: DATA CORRUPTION DETECTED ---+[/bright_cyan]"
    elif version == "Modern":
        title = "[bold cyan]SCENARIO 1: THE MESSY EXPORT[/bold cyan]"
    else:
        title = "[green]*** PHASE 1: DATA INGESTION ***[/green]"
    console.rule(title)

    if version == "TRON":
        tron_grid_animation(console, duration=1.0)
        msg = "\nGRID ANOMALY DETECTED: DEREZZED DATA FRAGMENTS IN SAP SECTOR. METADATA CORRUPTION PRESENT."
    elif version == "1985":
        msg = "\nPROBLEM: A FINANCE USER EXPORTED A GLOBAL SAP COA. IT HAS 5 LINES OF HUMAN-ADDED 'GARBAGE' METADATA AT THE TOP."
    else:
        msg = "\nPROBLEM: A finance user exported a global SAP COA. It has 5 lines of human-added 'garbage' metadata at the top."
    slow_type(console, msg, version)

    coa_path = data_dir / "sap_coa_messy.csv"

    try:
        raw_lines = coa_path.read_text().splitlines()[:8]
        if version == "TRON":
            view_title, raw_box = "CORRUPTED GRID DATA", box.DOUBLE
        elif version == "1985":
            view_title, raw_box = "BUFFER PREVIEW", box.ASCII
        else:
            view_title, raw_box = "Raw File Content (First 8 Lines)", box.ROUNDED
        raw_view_table = Table(title=view_title, show_header=False, box=raw_box)
        for line in raw_lines:
            raw_view_table.add_row(line)
        console.print(raw_view_table)
    except Exception:
        if version == "TRON":
            console.print("[bright_red]GRID READ FAILURE. DATA SECTOR UNREACHABLE.[/bright_red]")
        elif version == "Modern":
            console.print("[red]Error reading raw file.[/red]")
        else:
            console.print("[green]READ ERROR.[/green]")

    if version == "TRON":
        clean_prompt = "[bright_cyan]INITIATE RECTIFICATION SEQUENCE? (Y/N)[/bright_cyan]"
    elif version == "Modern":
        clean_prompt = "[yellow]Can you see the '#' comment lines and inconsistent fields? Ready to clean?[/yellow]"
    else:
        clean_prompt = "[green]CLEAN DATA BUFFER? (Y/N)[/green]"

    if Confirm.ask(clean_prompt):
        thinking_animation(console, "Detecting Anchor Cell & Skipping Metadata", version)
        df_clean = pd.read_csv(coa_path, comment='#')

        if version == "TRON":
            success_msg = f"RECTIFIED: [bright_cyan]{len(df_clean)}[/bright_cyan] PROGRAMS RECOVERED FROM CORRUPTION."
            panel_title, border = "GRID RECTIFICATION COMPLETE", "bright_cyan"
        elif version == "1985":
            success_msg = f"OK. {len(df_clean)} RECORDS IDENTIFIED. NOISE REMOVED."
            panel_title, border = "SYSTEM STATUS", "green"
        else:
            success_msg = f"SUCCESS: Identified [bold green]{len(df_clean)}[/bold green] valid accounts. Metadata stripped."
            panel_title, border = "Invisible Architect Action", "green"
        console.print(Panel(success_msg, title=panel_title, border_style=border))
        show_data_sample(
            console, df_clean,
            "RECTIFIED GRID DATA" if version == "TRON" else (
                "Cleaned & Parsed Dataset" if version == "Modern" else "NORMALIZED DATA"),
            version
        )
        kb_results["scenarios"]["messy_export"] = {"accounts_cleaned": len(df_clean)}
        time.sleep(1)

    console.print("\n")
    if version == "1985":
        console.print(f"[green]  {random.choice(RETRO_QUOTES)}[/green]\n")

    # ===================================================================
    # SCENARIO 2: FUZZY VENDOR MAPPING
    # ===================================================================
    if version == "TRON":
        title = "[bright_cyan]+--- GRID SECTOR 2: IDENTITY DISC MATCHING ---+[/bright_cyan]"
    elif version == "Modern":
        title = "[bold cyan]SCENARIO 2: FUZZY VENDOR MAPPING[/bold cyan]"
    else:
        title = "[green]*** PHASE 2: ENTITY MAPPING ***[/green]"
    console.rule(title)

    if version == "TRON":
        msg = "\nPROGRAMS REQUESTING IDENTITY VERIFICATION. 600 ENTITIES REQUIRE DISC ANALYSIS."
    elif version == "1985":
        msg = "\nPROBLEM: WE NEED TO MAP 600 VENDORS FROM A LEGACY SYSTEM TO NEW TARGETS. NAMES DON'T MATCH EXACTLY."
    else:
        msg = "\nPROBLEM: We need to map 600 vendors from a legacy system to new targets. Names don't match exactly."
    slow_type(console, msg, version)

    legacy_df = pd.read_csv(data_dir / "legacy_vendors.csv")
    new_erp_df = pd.read_csv(data_dir / "new_erp_vendors.csv")

    if version == "TRON":
        leg_title, erp_title = "SECTOR A PROGRAMS", "SECTOR B PROGRAMS"
    elif version == "Modern":
        leg_title, erp_title = "Legacy List", "ERP Target"
    else:
        leg_title, erp_title = "DISK A", "DISK B"

    leg_table = Table(title=leg_title, box=None)
    leg_table.add_column("Vendor Name", style="dim cyan" if version == "TRON" else None)
    for v in legacy_df['vendor_name'].head(3):
        leg_table.add_row(v)

    erp_table = Table(title=erp_title, box=None)
    erp_table.add_column("Legal Name", style="dim cyan" if version == "TRON" else None)
    for v in new_erp_df['legal_name'].head(3):
        erp_table.add_row(v)

    console.print(leg_table, justify="center")
    if version == "TRON":
        console.print(Align.center("[bright_cyan]--- DISC COMPARE ---[/bright_cyan]"))
    elif version == "Modern":
        console.print(Align.center("[bold]VS[/bold]"))
    else:
        console.print(Align.center("[green]--COMPARE--[/green]"))
    console.print(erp_table, justify="center")

    if version == "TRON":
        fuzzy_prompt = "\n[bright_cyan]DISC ANALYSIS IN PROGRESS. INITIATE IDENTITY MATCH? (Y/N)[/bright_cyan]"
    elif version == "Modern":
        fuzzy_prompt = "\n[yellow]Notice the 'Inc.' and 'Corp.' noise. Start fuzzy matching?[/yellow]"
    else:
        fuzzy_prompt = "[green]INITIATE FUZZY SEARCH? (Y/N)[/green]"

    if Confirm.ask(fuzzy_prompt):
        thinking_animation(console, "Calculating Levenshtein Distance & Token Ratios", version)

        try:
            mapping = fuzzy_match_columns(
                str(data_dir / "legacy_vendors.csv"),
                str(data_dir / "new_erp_vendors.csv"),
                column_a="vendor_name",
                column_b="legal_name",
                threshold=70
            )
        except ImportError:
            console.print("[bold red]rapidfuzz is required for fuzzy matching.[/bold red]")
            console.print("Install with: [bold]pip install 'databridge-core[fuzzy]'[/bold]")
            mapping = None
        except Exception as e:
            console.print(f"[bold red]Fuzzy matching failed: {e}[/bold red]")
            mapping = None

        if mapping:
            if version == "TRON":
                res_title, res_box = "IDENTITY DISC CROSS-REFERENCE", box.DOUBLE
            elif version == "1985":
                res_title, res_box = "MAPPING RESULTS", box.ASCII
            else:
                res_title, res_box = "AI-Generated Cross-Walk", box.ROUNDED
            results_table = Table(title=res_title, box=res_box)
            if version == "TRON":
                results_table.add_column("Source Program", style="dim cyan")
                results_table.add_column("Target Program", style="bright_cyan")
                results_table.add_column("Disc Match", justify="right", style="bright_blue")
            else:
                results_table.add_column("Legacy Name", style="red" if version == "Modern" else "green")
                results_table.add_column("ERP Target", style="green")
                results_table.add_column("Confidence", justify="right")

            for m in mapping['top_matches'][:5]:
                results_table.add_row(m['value_a'], m['value_b'], f"{m['similarity']:.1f}%")

            console.print(results_table)
            if version == "TRON":
                console.print(f"[bright_cyan]{mapping['total_matches']} PROGRAMS IDENTIFIED AND LINKED.[/bright_cyan]")
            elif version == "Modern":
                console.print(f"[dim]Note: Automatically linked {mapping['total_matches']} vendors.[/dim]")
            else:
                console.print(f"[green]{mapping['total_matches']} MATCHES FOUND.[/green]")
            kb_results["scenarios"]["fuzzy_mapping"] = {
                "total_matches": mapping['total_matches'],
                "top_matches": mapping['top_matches'][:5]
            }
        time.sleep(1)

    console.print("\n")
    if version == "1985":
        console.print(f"[green]  {random.choice(RETRO_QUOTES)}[/green]\n")

    # ===================================================================
    # SCENARIO 3: TRANSACTION AUDIT
    # ===================================================================
    if version == "TRON":
        title = "[bright_cyan]+--- GRID SECTOR 3: LIGHTCYCLE TRACE AUDIT ---+[/bright_cyan]"
    elif version == "Modern":
        title = "[bold cyan]SCENARIO 3: TRANSACTION AUDIT[/bold cyan]"
    else:
        title = "[green]*** PHASE 3: RECONCILIATION ***[/green]"
    console.rule(title)

    if version == "TRON":
        msg = "\nTRACING PROGRAM PATHS ON THE GRID. 1,200 CYCLES VS BANK SECTOR. ANOMALOUS CYCLES DETECTED."
    elif version == "1985":
        msg = "\nPROBLEM: 1,200 GENERAL LEDGER ENTRIES VS BANK STATEMENT. FIND THE NEEDLES IN THE HAYSTACK."
    else:
        msg = "\nPROBLEM: 1,200 General Ledger entries vs Bank Statement. Find the needles in the haystack."
    slow_type(console, msg, version)

    gl_path = data_dir / "gl_extract.csv"
    bank_path = data_dir / "bank_aligned.csv"

    if version == "TRON":
        hash_prompt = "\n[bright_cyan]ENGAGE LIGHTCYCLE TRACE HASHING? (Y/N)[/bright_cyan]"
    elif version == "Modern":
        hash_prompt = "\n[yellow]Manually checking 1,200 rows takes 4 hours. Use hashing?[/yellow]"
    else:
        hash_prompt = "[green]COMPUTE HASH SUMS? (Y/N)[/green]"

    if Confirm.ask(hash_prompt):
        thinking_animation(console, "Generating Row-Level Digital Fingerprints", version)

        recon = compare_hashes(str(gl_path), str(bank_path), key_columns="amount", compare_columns="amount")

        if recon['statistics']['orphans_only_in_source_a'] > 0:
            orphans = get_orphan_details(str(gl_path), str(bank_path), key_columns="amount", orphan_source="a", limit=3)

            if version == "TRON":
                alert_msg = "\n+--- GRID BREACH: MISSING LIGHTCYCLE TRACES ---+"
                alert_style = "bold bright_red"
            elif version == "Modern":
                alert_msg = "\n!! AUDIT ALERT: MISSING TRANSACTIONS DETECTED !!"
                alert_style = "bold red"
            else:
                alert_msg = "\n!!! CRITICAL ERROR: DATA MISMATCH !!!"
                alert_style = "bold green"
            alert_text = Text(alert_msg, style=alert_style)
            console.print(Align.center(alert_text))

            orphan_table = Table(box=None)
            if version == "TRON":
                orphan_table.add_column("Derezzed Cycle", style="bright_cyan")
                orphan_table.add_column("Energy", style="bright_blue")
            else:
                orphan_table.add_column("Transaction", style="bold")
                orphan_table.add_column("Amount", style="yellow" if version == "Modern" else "green")

            for item in orphans['orphans_in_a']['sample']:
                desc = str(item.get('description', item.get('key', 'N/A')))
                amount = item.get('amount', 'N/A')
                orphan_table.add_row(desc, f"${amount}")

            console.print(Align.center(Panel(
                orphan_table,
                border_style="bright_cyan" if version == "TRON" else (
                    "red" if version == "Modern" else "green")
            )))

        kb_results["scenarios"]["transaction_audit"] = {
            "statistics": recon['statistics']
        }
        time.sleep(1)

    console.print("\n")
    if version == "1985":
        console.print(f"[green]  {random.choice(RETRO_QUOTES)}[/green]\n")

    # ===================================================================
    # SCENARIO 4: REVENUE INTEGRITY
    # ===================================================================
    if version == "TRON":
        title = "[bright_cyan]+--- GRID SECTOR 4: MCP INTEGRITY SCAN ---+[/bright_cyan]"
    elif version == "Modern":
        title = "[bold cyan]SCENARIO 4: REVENUE INTEGRITY[/bold cyan]"
    else:
        title = "[green]*** PHASE 4: AUDIT LOG ***[/green]"
    console.rule(title)

    if version == "TRON":
        msg = "\nMCP VARIANCE PROTOCOL ENGAGED. SCANNING FOR DEREZZED VALUES ACROSS 1,500 GRID NODES."
    elif version == "1985":
        msg = "\nPROBLEM: DETECT HIDDEN DISCOUNTS WHERE CRM PRICE != ERP BILLED AMOUNT ACROSS 1,500 RECORDS."
    else:
        msg = "\nPROBLEM: Detect hidden discounts where CRM Price != ERP Billed Amount across 1,500 records."
    slow_type(console, msg, version)

    crm_path = data_dir / "crm_sales.csv"
    erp_path = data_dir / "erp_aligned.csv"

    if version == "TRON":
        audit_prompt = "\n[bright_cyan]INITIATE MCP INTEGRITY SCAN? (Y/N)[/bright_cyan]"
    elif version == "Modern":
        audit_prompt = "\n[yellow]Ready to audit price discrepancies?[/yellow]"
    else:
        audit_prompt = "[green]RUN VARIANCE ANALYSIS? (Y/N)[/green]"

    if Confirm.ask(audit_prompt):
        thinking_animation(console, "Scanning for Value Conflicts", version)

        conflicts = get_conflict_details(
            str(crm_path), str(erp_path),
            key_columns="opportunity_id",
            compare_columns="amount",
            limit=3
        )

        if version == "TRON":
            conflict_title = f"GRID INTEGRITY BREACH: [bright_red]{conflicts['total_conflicts']}[/bright_red] DEREZZED VALUES FOUND."
            panel_border = "bright_cyan"
        elif version == "Modern":
            conflict_title = f"FOUND [bold red]{conflicts['total_conflicts']}[/bold red] PRICE DISCREPANCIES."
            panel_border = "yellow"
        else:
            conflict_title = f"{conflicts['total_conflicts']} CONFLICTS LOCATED."
            panel_border = "green"
        console.print(Panel(conflict_title, border_style=panel_border))

        for c in conflicts['conflicts']:
            if version == "TRON":
                details = (
                    f"[dim cyan]NODE:[/dim cyan] [bright_cyan]{c['key']['opportunity_id']}[/bright_cyan]"
                    f" [dim cyan]|[/dim cyan] CRM: [bright_cyan]${c['differences'][0]['value_a']}[/bright_cyan]"
                    f" [dim cyan]vs[/dim cyan] ERP: [bright_red]${c['differences'][0]['value_b']}[/bright_red]"
                )
            elif version == "1985":
                details = (
                    f"ID: {c['key']['opportunity_id']}"
                    f" - VAR: {c['differences'][0]['value_a']} / {c['differences'][0]['value_b']}"
                )
            else:
                details = (
                    f"Opp: {c['key']['opportunity_id']}"
                    f" | CRM: [green]${c['differences'][0]['value_a']}[/green]"
                    f" vs ERP: [red]${c['differences'][0]['value_b']}[/red]"
                )
            console.print(details)

        kb_results["scenarios"]["revenue_integrity"] = {
            "total_conflicts": conflicts['total_conflicts'],
            "conflicts": [
                {
                    "opportunity_id": c['key']['opportunity_id'],
                    "crm_amount": c['differences'][0]['value_a'],
                    "erp_amount": c['differences'][0]['value_b']
                }
                for c in conflicts['conflicts']
            ]
        }
        time.sleep(1)

    console.print("\n")

    # ===================================================================
    # CONCLUSION
    # ===================================================================
    if version == "TRON":
        console.print(get_tron_end_of_line())
        tron_grid_animation(console, duration=2.0)
        console.print()
        scenarios_done = len(kb_results.get("scenarios", {}))
        console.print(Panel(
            f"[bright_cyan]GRID SECTORS CLEARED: {scenarios_done}/4\n"
            f"CYCLE TIME: ~45 SECONDS\n"
            f"HUMAN CYCLES SAVED: ~8 HOURS\n\n"
            f"THE GRID RECOGNIZES YOU, PROGRAM.\n"
            f"DATABRIDGE IS NOW YOUR WEAPON.[/bright_cyan]",
            title="[bold bright_cyan]GRID STATUS: ALL SECTORS RECTIFIED[/bold bright_cyan]",
            border_style="bright_cyan",
            box=box.DOUBLE,
        ))
    elif version == "Modern":
        finish_msg = """
# TOUR COMPLETE
## Total Time: ~45 Seconds
## Human Effort Saved: ~8 Hours

DataBridge Core is now ready to be your **Invisible Architect**.
        """
        console.print("\n")
        console.print(Align.center(Panel(Markdown(finish_msg), border_style="bold green", expand=False)))
    else:
        retro_end = (
            "\n************************************************************\n"
            "*                                                          *\n"
            "*           M I S S I O N   C O M P L E T E               *\n"
            "*                                                          *\n"
            '*   "Shall we play a game?"  -- WOPR, WarGames (1983)     *\n'
            "*                                                          *\n"
            "*   ANALYSIS COMPLETE. ALL PHASES PASSED.                  *\n"
            "*   TOTAL TIME: ~45 SEC  |  HUMAN HOURS SAVED: ~8         *\n"
            "*                                                          *\n"
            "*   THE INVISIBLE ARCHITECT IS READY.                      *\n"
            "*   INSERT NEXT DISK OR PRESS ANY KEY TO EXIT.             *\n"
            "*                                                          *\n"
            "************************************************************\n"
        )
        console.print(f"[green]{retro_end}[/green]")
        retro_pacman_progress(console, "SAVING TO SECTOR 7")
        console.print(f"\n[green]  {random.choice(RETRO_QUOTES)}[/green]\n")

    if version == "TRON":
        kb_prompt = "\n[bright_cyan]EXPORT GRID DATA TO KNOWLEDGE BASE? (Y/N)[/bright_cyan]"
    elif version == "Modern":
        kb_prompt = "\n[bold white]Would you like to export these results to a JSON Knowledge Base?[/bold white]"
    else:
        kb_prompt = "[green]EXPORT TO TAPE? (Y/N)[/green]"

    if Confirm.ask(kb_prompt):
        thinking_animation(console, "Generating Graph-Ready Knowledge JSON", version)
        kb_results["exported_at"] = datetime.now().isoformat()
        output_path = Path.cwd() / "knowledge_export.json"
        output_path.write_text(json.dumps(kb_results, indent=2, default=str))
        if version == "TRON":
            console.print(f"[bright_cyan]GRID DATA EXPORTED TO {output_path.name}. END OF LINE.[/bright_cyan]")
        elif version == "Modern":
            console.print(f"[bold green]Success! {output_path.name} created in {output_path.parent}[/bold green]")
        else:
            console.print(f"[green]TAPE WRITTEN TO {output_path}. GOODBYE.[/green]")


if __name__ == "__main__":
    run_tour()
