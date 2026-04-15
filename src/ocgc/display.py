"""Rich formatting for terminal output."""

import time

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from ocgc.db import DBInfo, FilesystemStats, PartTypeStats, SessionRow

console = Console()

# Color palette
C_HEADER = "bold cyan"
C_VALUE = "bold white"
C_DIM = "dim"
C_WARN = "bold yellow"
C_DANGER = "bold red"
C_SUCCESS = "bold green"
C_ROOT = "green"
C_SUB = "yellow"

PART_TYPE_COLORS = {
    "reasoning": "red",
    "tool": "blue",
    "text": "green",
    "step-start": "cyan",
    "step-finish": "magenta",
    "patch": "yellow",
    "file": "white",
    "compaction": "dim white",
}


def format_bytes(n: int) -> str:
    if n >= 1_073_741_824:
        return f"{n / 1_073_741_824:.1f} GB"
    if n >= 1_048_576:
        return f"{n / 1_048_576:.1f} MB"
    if n >= 1024:
        return f"{n / 1024:.1f} KB"
    return f"{n} B"


def format_age(ms_epoch: int) -> str:
    now = time.time() * 1000
    delta_s = (now - ms_epoch) / 1000
    if delta_s < 0:
        return "just now"
    if delta_s < 60:
        return "<1m ago"
    if delta_s < 3600:
        return f"{int(delta_s / 60)}m ago"
    if delta_s < 86400:
        return f"{delta_s / 3600:.1f}h ago"
    days = delta_s / 86400
    if days < 30:
        return f"{int(days)}d ago"
    return f"{int(days / 30)}mo ago"


def warn_opencode_running() -> None:
    console.print(
        Panel(
            "[bold yellow]opencode appears to be running.[/]\n"
            "Writing to the DB while opencode is active may cause issues.\n"
            "Consider stopping opencode first.",
            title="[bold yellow]Warning[/]",
            border_style="yellow",
        )
    )


def warn_if_opencode_running() -> None:
    """Check if opencode is running and print a warning if so."""
    from ocgc.db import check_opencode_running

    if check_opencode_running():
        warn_opencode_running()


def print_status(
    db_info: DBInfo,
    root_count: int,
    sub_count: int,
    part_stats: list[PartTypeStats],
    age_dist: dict[str, int],
    fs_stats: FilesystemStats,
) -> None:
    # --- Header panel ---
    header = Table.grid(padding=(0, 2))
    header.add_column(style=C_DIM, justify="right")
    header.add_column(style=C_VALUE)
    header.add_row("Database", str(db_info.path))
    header.add_row("DB size", format_bytes(db_info.db_size))
    header.add_row("WAL size", format_bytes(db_info.wal_size))
    header.add_row("Total DB", f"[bold]{format_bytes(db_info.total_size)}[/]")
    header.add_row("", "")
    diff_info = f"{format_bytes(fs_stats.session_diff_size)}  ({fs_stats.session_diff_count} files)"
    header.add_row("Session diffs", diff_info)
    header.add_row("Snapshots", f"{format_bytes(fs_stats.snapshot_size)}  ({fs_stats.snapshot_count} projects)")
    header.add_row("Tool output", format_bytes(fs_stats.tool_output_size))
    grand_total = db_info.total_size + fs_stats.total_size
    header.add_row("Total on disk", f"[bold]{format_bytes(grand_total)}[/]")
    header.add_row("", "")
    header.add_row("Sessions", f"[bold]{root_count + sub_count}[/]")
    header.add_row("  Root", f"[{C_ROOT}]{root_count}[/]")
    header.add_row("  Subagent", f"[{C_SUB}]{sub_count}[/]")

    console.print(Panel(header, title="[bold cyan]ocgc status[/]", border_style="cyan"))

    # --- Storage breakdown bar chart ---
    total_bytes = sum(s.size_bytes for s in part_stats)
    if total_bytes == 0:
        console.print("[dim]No part data found.[/]")
        return

    storage_table = Table(
        title="Storage by Part Type",
        show_header=True,
        header_style="bold",
        border_style="dim",
        padding=(0, 1),
    )
    storage_table.add_column("Type", style="bold", min_width=12)
    storage_table.add_column("Size", justify="right", min_width=10)
    storage_table.add_column("Count", justify="right", min_width=8)
    storage_table.add_column("%", justify="right", min_width=6)
    storage_table.add_column("Bar", min_width=30)

    max_bar = 30
    for s in part_stats:
        pct = s.size_bytes / total_bytes * 100
        bar_len = int(pct / 100 * max_bar)
        color = PART_TYPE_COLORS.get(s.type_name, "white")
        bar_text = Text("█" * bar_len + "░" * (max_bar - bar_len))
        bar_text.stylize(color, 0, bar_len)
        bar_text.stylize("dim", bar_len)
        storage_table.add_row(
            f"[{color}]{s.type_name}[/]",
            format_bytes(s.size_bytes),
            f"{s.count:,}",
            f"{pct:.1f}%",
            bar_text,
        )

    storage_table.add_section()
    storage_table.add_row(
        "[bold]Total[/]",
        f"[bold]{format_bytes(total_bytes)}[/]",
        f"[bold]{sum(s.count for s in part_stats):,}[/]",
        "100%",
        "",
    )
    console.print(storage_table)

    # --- Age distribution ---
    age_table = Table(
        title="Session Age Distribution",
        show_header=True,
        header_style="bold",
        border_style="dim",
        padding=(0, 1),
    )
    age_table.add_column("Period", style="bold", min_width=16)
    age_table.add_column("Sessions", justify="right", min_width=8)
    age_table.add_column("Bar", min_width=20)

    max_count = max(age_dist.values()) if age_dist else 1
    bar_width = 20
    for label, count in age_dist.items():
        bar_len = int(count / max_count * bar_width) if max_count > 0 else 0
        bar_text = Text("█" * bar_len + "░" * (bar_width - bar_len))
        bar_text.stylize("cyan", 0, bar_len)
        bar_text.stylize("dim", bar_len)
        age_table.add_row(label, str(count), bar_text)

    console.print(age_table)


def print_sessions(sessions: list[SessionRow]) -> None:
    table = Table(
        title="Sessions",
        show_header=True,
        header_style="bold",
        border_style="dim",
        padding=(0, 1),
    )
    table.add_column("ID", style="dim", max_width=12)
    table.add_column("Directory", min_width=12, max_width=20)
    table.add_column("Title", min_width=15, max_width=40, no_wrap=True, overflow="ellipsis")
    table.add_column("Size", justify="right", style="bold")
    table.add_column("Age", justify="right")
    table.add_column("Type", justify="center")
    table.add_column("Msgs", justify="right")

    for s in sessions:
        dir_name = s.directory.rstrip("/").rsplit("/", 1)[-1] if s.directory else ""
        title_str = s.title or "(untitled)"
        title = title_str[:37] + "..." if len(title_str) > 40 else title_str
        type_label = f"[{C_SUB}]sub[/]" if s.is_subagent else f"[{C_ROOT}]root[/]"
        sid = s.id[:12]
        table.add_row(
            sid,
            dir_name,
            title,
            format_bytes(s.size_bytes),
            format_age(s.time_created),
            type_label,
            str(s.message_count),
        )

    console.print(table)


def print_analysis(
    top_sessions: list[SessionRow],
    avg_size: float,
    growth_rate: float | None,
    root_stats: list[PartTypeStats],
    sub_stats: list[PartTypeStats],
    total_sessions: int,
    fs_stats: FilesystemStats,
    orphan_count: int,
    orphan_bytes: int,
) -> None:
    # --- Top sessions ---
    top_table = Table(
        title="Top 10 Sessions by Size",
        show_header=True,
        header_style="bold",
        border_style="dim",
        padding=(0, 1),
    )
    top_table.add_column("#", style="dim", width=3)
    top_table.add_column("Title", min_width=20, max_width=40)
    top_table.add_column("Directory", max_width=18)
    top_table.add_column("Size", justify="right", style="bold")
    top_table.add_column("Type", justify="center")
    top_table.add_column("Msgs", justify="right")

    for i, s in enumerate(top_sessions, 1):
        dir_name = s.directory.rstrip("/").rsplit("/", 1)[-1] if s.directory else ""
        title_str = s.title or "(untitled)"
        title = title_str[:37] + "..." if len(title_str) > 40 else title_str
        type_label = f"[{C_SUB}]sub[/]" if s.is_subagent else f"[{C_ROOT}]root[/]"
        top_table.add_row(str(i), title, dir_name, format_bytes(s.size_bytes), type_label, str(s.message_count))

    console.print(top_table)

    # --- Summary stats panel ---
    summary = Table.grid(padding=(0, 2))
    summary.add_column(style=C_DIM, justify="right")
    summary.add_column(style=C_VALUE)
    summary.add_row("Total sessions", str(total_sessions))
    total_part_bytes = sum(s.size_bytes for s in root_stats) + sum(s.size_bytes for s in sub_stats)
    summary.add_row("Total part data", format_bytes(total_part_bytes))
    summary.add_row("Avg session size", format_bytes(int(avg_size)))
    if growth_rate is not None:
        rate_color = C_DANGER if growth_rate > 100 else C_WARN if growth_rate > 20 else C_SUCCESS
        summary.add_row("Growth rate", f"[{rate_color}]{growth_rate:.1f} MB/active day[/]")
    summary.add_row("", "")
    diff_info = f"{format_bytes(fs_stats.session_diff_size)}  ({fs_stats.session_diff_count} files)"
    summary.add_row("Session diffs", diff_info)
    summary.add_row("Snapshots", f"{format_bytes(fs_stats.snapshot_size)}  ({fs_stats.snapshot_count} projects)")
    summary.add_row("Tool output", format_bytes(fs_stats.tool_output_size))
    if orphan_count > 0:
        summary.add_row("Orphan diffs", f"[{C_WARN}]{orphan_count} files ({format_bytes(orphan_bytes)})[/]")

    console.print(Panel(summary, title="[bold cyan]Summary[/]", border_style="cyan"))

    # --- Root vs Subagent comparison ---
    comp_table = Table(
        title="Root vs Subagent Storage",
        show_header=True,
        header_style="bold",
        border_style="dim",
        padding=(0, 1),
    )
    comp_table.add_column("Category", style="bold")
    comp_table.add_column("Parts", justify="right")
    comp_table.add_column("Size", justify="right")

    root_total = sum(s.size_bytes for s in root_stats)
    sub_total = sum(s.size_bytes for s in sub_stats)
    root_parts = sum(s.count for s in root_stats)
    sub_parts = sum(s.count for s in sub_stats)

    comp_table.add_row(f"[{C_ROOT}]Root sessions[/]", f"{root_parts:,}", format_bytes(root_total))
    comp_table.add_row(f"[{C_SUB}]Subagent sessions[/]", f"{sub_parts:,}", format_bytes(sub_total))
    comp_table.add_section()
    total_size = format_bytes(root_total + sub_total)
    comp_table.add_row("[bold]Total[/]", f"[bold]{root_parts + sub_parts:,}[/]", f"[bold]{total_size}[/]")

    console.print(comp_table)


def print_purge_summary(
    summary: dict[str, int], dry_run: bool = False,
    diff_files: int = 0, diff_bytes: int = 0,
) -> None:
    label = "[bold yellow]Dry Run — Nothing will be deleted[/]" if dry_run else "[bold red]Purge Summary[/]"
    border = "yellow" if dry_run else "red"

    grid = Table.grid(padding=(0, 2))
    grid.add_column(style=C_DIM, justify="right")
    grid.add_column(style=C_VALUE)
    grid.add_row("Sessions", str(summary["session_count"]))
    grid.add_row("Messages", f"{summary['message_count']:,}")
    grid.add_row("Parts", f"{summary['part_count']:,}")
    grid.add_row("Data size", format_bytes(summary["total_bytes"]))
    if diff_files > 0:
        grid.add_row("Session diffs", f"{diff_files} file(s) ({format_bytes(diff_bytes)})")

    console.print(Panel(grid, title=label, border_style=border))


def print_reasoning_summary(summary: dict[str, int], dry_run: bool = False) -> None:
    label = "[bold yellow]Dry Run — Reasoning parts to strip[/]" if dry_run else "[bold red]Strip Reasoning[/]"
    border = "yellow" if dry_run else "red"

    grid = Table.grid(padding=(0, 2))
    grid.add_column(style=C_DIM, justify="right")
    grid.add_column(style=C_VALUE)
    grid.add_row("Reasoning parts", f"{summary['part_count']:,}")
    grid.add_row("Data size", format_bytes(summary["total_bytes"]))

    console.print(Panel(grid, title=label, border_style=border))


def print_vacuum_result(before: int, after: int) -> None:
    saved = before - after
    grid = Table.grid(padding=(0, 2))
    grid.add_column(style=C_DIM, justify="right")
    grid.add_column(style=C_VALUE)
    grid.add_row("Before", format_bytes(before))
    grid.add_row("After", format_bytes(after))
    grid.add_row("Saved", f"[{C_SUCCESS}]{format_bytes(saved)}[/]" if saved > 0 else format_bytes(saved))

    console.print(Panel(grid, title="[bold cyan]Vacuum Complete[/]", border_style="cyan"))
