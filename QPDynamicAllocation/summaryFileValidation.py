import sys
import time
from pathlib import Path
from collections import defaultdict

import pandas as pd
import numpy as np
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich import box
from rich.columns import Columns
from rich.rule import Rule
from rich.padding import Padding

console = Console()

COMPOSITE_KEY = ['Origin', 'Destin', 'FltNum', 'DepDate', 'Channel']

BASE_DIR = Path(__file__).resolve().parent

CURRENT_FILE = BASE_DIR / "testcases" / "output" / "generated_output" / "summary.csv"
ARCHIVE_FILE = BASE_DIR / "testcases" / "output" / "expected_output" / "summary.csv"
OUTPUT_FILE = BASE_DIR / "testcases" / "test" / "mismatched.csv"


# ─── Helpers ────────────────────────────────────────────────────────────────

def fmt_key(key):
    vals = key if isinstance(key, tuple) else (key,)
    return "  ".join(
        f"[dim]{k}=[/dim][bold cyan]{v}[/bold cyan]"
        for k, v in zip(COMPOSITE_KEY, vals)
    )


def to_numeric_safe(v):
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def vals_match(v1, v2) -> bool:
    if pd.isna(v1) and pd.isna(v2):
        return True
    if pd.isna(v1) or pd.isna(v2):
        return False

    n1, n2 = to_numeric_safe(v1), to_numeric_safe(v2)
    if n1 is not None and n2 is not None:
        return round(n1, 2) == round(n2, 2)

    return str(v1).strip() == str(v2).strip()


# ─── Main ───────────────────────────────────────────────────────────────────

def compare_csv_files():
    start = time.perf_counter()

    console.print()
    console.print(Panel.fit(
        "[bold white]Summary Version Comparator[/bold white]\n",
        border_style="bright_blue",
        padding=(0, 4),
    ))
    console.print()

    # ── Load files ──────────────────────────────────────────────────────────
    for path in (CURRENT_FILE, ARCHIVE_FILE):
        if not path.exists():
            console.print(Panel(
                f"[bold red]File not found:[/bold red]\n{path}",
                border_style="red", padding=(0, 3),
            ))
            return

    with Progress(
        SpinnerColumn(spinner_name="dots", style="cyan"),
        TextColumn("[progress.description]{task.description}"),
        TimeElapsedColumn(),
        console=console,
        transient=True,
    ) as progress:
        t = progress.add_task("Loading files...", total=None)

        df1 = pd.read_csv(CURRENT_FILE)
        df2 = pd.read_csv(ARCHIVE_FILE)
        df2_original = df2.copy()

        df1 = df1.drop(columns=['Id'], errors='ignore')
        df2 = df2.drop(columns=['Id', 'marketFaresSummary', 'bookingsToday'], errors='ignore')

        # align column order
        df2 = df2.reindex(columns=df1.columns)

        progress.update(t, description="Files loaded")
        time.sleep(0.15)

    # ── File summary cards ───────────────────────────────────────────────────
    def stat_card(label, df, color):
        return Panel(
            f"[bold {color}]{label}[/bold {color}]\n\n"
            f"[dim]Rows  [/dim][bold]{len(df):,}[/bold]\n"
            f"[dim]Cols  [/dim][bold]{len(df.columns):,}[/bold]",
            border_style=color,
            padding=(0, 3),
            expand=True,
        )

    console.print(Columns([
        stat_card("NEW Summary", df1, "bright_green"),
        stat_card("OLD Summary", df2, "bright_yellow"),
    ]))
    console.print()

    # ── Validate composite keys ──────────────────────────────────────────────
    missing = [c for c in COMPOSITE_KEY
               if c not in df1.columns or c not in df2.columns]
    if missing:
        console.print(Panel(
            f"[bold red]Missing composite key column(s):[/bold red] {', '.join(missing)}",
            border_style="red", padding=(0, 3),
        ))
        return

    # ── Indexing ─────────────────────────────────────────────────────────────
    df1 = df1.set_index(COMPOSITE_KEY)
    df2 = df2.set_index(COMPOSITE_KEY)

    # ── Exclusive records ────────────────────────────────────────────────────
    only_in_current = df1.index.difference(df2.index)
    only_in_archive = df2.index.difference(df1.index)

    def print_exclusive_keys(keys, label, color):
        if keys.empty:
            return

        console.print(Rule(
            f"[bold {color}]{label}  ({len(keys):,} record(s))[/bold {color}]",
            style=color,
        ))

        tbl = Table(box=box.SIMPLE_HEAD, show_header=True,
                    header_style=f"bold {color}", show_edge=False, padding=(0, 1))

        for col in COMPOSITE_KEY:
            tbl.add_column(col, style="dim")

        for key in keys:
            vals = key if isinstance(key, tuple) else (key,)
            tbl.add_row(*[str(v) for v in vals])

        console.print(tbl)
        console.print()

    print_exclusive_keys(
        only_in_current,
        "NEW in NEW summary  (not in OLD summary)",
        "bright_green"
    )

    print_exclusive_keys(
        only_in_archive,
        "MISSING from NEW summary  (only in OLD summary)",
        "yellow"
    )

    # ── VALUE COMPARISON (FIXED ALIGNMENT) ───────────────────────────────────
    common_index = df1.index.intersection(df2.index).sort_values()

    df1_c = df1.loc[common_index]
    df2_c = df2.loc[common_index]

    # guarantee identical ordering
    df1_c = df1_c.sort_index()
    df2_c = df2_c.sort_index()

    arr1 = df1_c.to_numpy(dtype=object)
    arr2 = df2_c.to_numpy(dtype=object)
    col_names = df1_c.columns.tolist()

    row_has_diff = np.zeros(len(df1_c), dtype=bool)

    for ci in range(len(col_names)):
        for ri in range(len(df1_c)):
            if not vals_match(arr1[ri, ci], arr2[ri, ci]):
                row_has_diff[ri] = True

    mismatches = []

    for ri in np.where(row_has_diff)[0]:
        key = df1_c.index[ri]

        for ci, col in enumerate(col_names):
            v1, v2 = arr1[ri, ci], arr2[ri, ci]
            if not vals_match(v1, v2):
                mismatches.append((key, col, v2, v1))

    elapsed = time.perf_counter() - start

    # ── PRINT RESULTS ────────────────────────────────────────────────────────
    if not mismatches:
        console.print(Panel(
            "[bold bright_green]No value mismatches found[/bold bright_green]\n"
            f"[dim]Compared {len(common_index):,} common records across "
            f"{len(col_names):,} columns[/dim]",
            border_style="bright_green",
            padding=(0, 3),
        ))
    else:
        by_key = defaultdict(list)
        for key, col, old_val, new_val in mismatches:
            by_key[key].append((col, old_val, new_val))

        console.print(Rule(
            f"[bold red]VALUE MISMATCHES  .  {len(mismatches):,} field(s)  in  "
            f"{len(by_key):,} record(s)[/bold red]",
            style="red",
        ))
        console.print()

        for key, changes in by_key.items():
            console.print(f"  [bold white]>[/bold white]  {fmt_key(key)}")

            tbl = Table(
                box=box.SIMPLE,
                show_header=True,
                header_style="bold dim",
                show_edge=False,
                padding=(0, 2),
            )

            tbl.add_column("Column", style="bold cyan", no_wrap=True)
            tbl.add_column("OLD", style="yellow")
            tbl.add_column("-> NEW", style="bright_green")

            for col, old_val, new_val in changes:
                tbl.add_row(col, str(old_val), str(new_val))

            console.print(Padding(tbl, (0, 0, 1, 6)))

    # ── Summary footer ───────────────────────────────────────────────────────
    console.print(Rule(style="dim"))

    summary_parts = [
        f"[dim]Compared:[/dim] [bold]{len(common_index):,}[/bold]",
        f"[dim]New:[/dim] [bold bright_green]{len(only_in_current):,}[/bold bright_green]",
        f"[dim]Missing:[/dim] [bold yellow]{len(only_in_archive):,}[/bold yellow]",
        f"[dim]Mismatched fields:[/dim] [bold red]{len(mismatches):,}[/bold red]",
        f"[dim]Time:[/dim] [bold]{elapsed:.2f}s[/bold]",
    ]

    console.print("  " + "   [dim]|[/dim]   ".join(summary_parts))
    console.print()

    # ── Export mismatched rows ───────────────────────────────────────────────
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    if mismatches:
        mismatch_keys = list({m[0] for m in mismatches})
        df2_original = df2_original.set_index(COMPOSITE_KEY)
        export_df = df2_original.loc[mismatch_keys].reset_index()
        export_df.to_csv(OUTPUT_FILE, index=False)

        console.print(Panel(
            f"[bold]Exported[/bold] {len(export_df):,} mismatched row(s) ->\n"
            f"[underline cyan]{OUTPUT_FILE}[/underline cyan]",
            border_style="cyan",
            padding=(0, 3),
        ))
    else:
        pd.DataFrame().to_csv(OUTPUT_FILE, index=False)
        console.print(f"[dim]Empty result written -> {OUTPUT_FILE}[/dim]")

    console.print()


compare_csv_files()