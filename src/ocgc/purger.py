"""Purge and vacuum logic."""

import re
import sqlite3
import time

import click
from rich.panel import Panel
from rich.table import Table

from ocgc import db
from ocgc.display import (
    C_DIM,
    C_VALUE,
    console,
    format_bytes,
    print_purge_summary,
    print_reasoning_summary,
    print_vacuum_result,
    warn_if_opencode_running,
    warn_opencode_running,
)


def parse_duration(s: str) -> int:
    """Parse duration string (e.g., 7d, 2w, 30d, 1h) to milliseconds."""
    m = re.match(r"^(\d+)\s*(m|h|d|w|mo)$", s.strip().lower())
    if not m:
        raise click.BadParameter(f"Invalid duration: {s!r}. Use e.g. 7d, 2w, 30d, 1h, 15m, 3mo")
    value = int(m.group(1))
    unit = m.group(2)
    multipliers = {"m": 60_000, "h": 3600_000, "d": 86400_000, "w": 604800_000, "mo": 2592000_000}
    return value * multipliers[unit]


def parse_size(s: str) -> int:
    """Parse size string (e.g., 50M, 1G, 500K) to bytes."""
    m = re.match(r"^(\d+(?:\.\d+)?)\s*(k|m|g|kb|mb|gb)$", s.strip().lower())
    if not m:
        raise click.BadParameter(f"Invalid size: {s!r}. Use e.g. 50M, 1G, 500K")
    value = float(m.group(1))
    unit = m.group(2).rstrip("b")
    multipliers = {"k": 1024, "m": 1048576, "g": 1073741824}
    return int(value * multipliers[unit])


def run_purge(
    older_than: str | None,
    subagents: bool,
    larger_than: str | None,
    strip_reasoning: bool,
    session_ids: tuple[str, ...],
    keep_latest: int | None,
    dry_run: bool,
    force: bool,
) -> None:
    if db.check_opencode_running():
        warn_opencode_running()
        if not dry_run and not force and not click.confirm("opencode is running. Continue anyway?"):
            return

    has_filter = older_than or subagents or larger_than or session_ids or keep_latest is not None
    if not has_filter and not strip_reasoning:
        console.print("[red]Error:[/] At least one purge flag is required.")
        console.print("Use --older-than, --subagents, --larger-than, --session, --keep-latest, or --strip-reasoning")
        raise SystemExit(1)

    older_than_ms = parse_duration(older_than) if older_than else None
    larger_than_bytes = parse_size(larger_than) if larger_than else None
    now_ms = int(time.time() * 1000)

    if strip_reasoning and not has_filter:
        # Strip reasoning from ALL sessions
        try:
            conn = db.connect(readonly=True)
        except FileNotFoundError as e:
            click.echo(f"Error: {e}", err=True)
            raise SystemExit(1) from None
        try:
            summary = db.get_reasoning_summary(conn, session_ids=None)
        finally:
            conn.close()

        if summary["part_count"] == 0:
            console.print("[dim]No reasoning parts found.[/]")
            return

        print_reasoning_summary(summary, dry_run=dry_run)

        if dry_run:
            return

        if not force and not click.confirm("Strip all reasoning parts?"):
            return

        conn = db.connect(readonly=False)
        try:
            count = db.strip_reasoning(conn, session_ids=None)
            console.print(f"[green]Deleted {count:,} reasoning parts.[/]")
        finally:
            conn.close()
        return

    # Get matching session IDs
    try:
        conn = db.connect(readonly=True)
    except FileNotFoundError as e:
        click.echo(f"Error: {e}", err=True)
        raise SystemExit(1) from None
    try:
        matched_ids = db.get_session_ids_for_purge(
            conn,
            older_than_ms=older_than_ms,
            subagents_only=subagents,
            larger_than_bytes=larger_than_bytes,
            session_ids=list(session_ids) if session_ids else None,
            keep_latest=keep_latest,
            now_ms=now_ms,
        )

        if not matched_ids:
            console.print("[dim]No sessions match the given criteria.[/]")
            return

        if strip_reasoning:
            summary = db.get_reasoning_summary(conn, matched_ids)
            if summary["part_count"] == 0:
                console.print("[dim]No reasoning parts found in matching sessions.[/]")
                return
            print_reasoning_summary(summary, dry_run=dry_run)
        else:
            summary = db.get_purge_summary(conn, matched_ids)
            # Count session diff files that would be cleaned
            diff_dir = db.get_storage_dir() / "storage" / "session_diff"
            diff_files = 0
            diff_bytes = 0
            if diff_dir.is_dir():
                for sid in matched_ids:
                    p = diff_dir / f"{sid}.json"
                    if p.exists():
                        diff_files += 1
                        diff_bytes += p.stat().st_size
            print_purge_summary(summary, dry_run=dry_run, diff_files=diff_files, diff_bytes=diff_bytes)
    finally:
        conn.close()

    if dry_run:
        return

    if not force:
        action = "Strip reasoning from" if strip_reasoning else "Delete"
        if not click.confirm(f"{action} {len(matched_ids)} session(s)?"):
            return

    try:
        conn = db.connect(readonly=False)
    except FileNotFoundError as e:
        click.echo(f"Error: {e}", err=True)
        raise SystemExit(1) from None
    try:
        if strip_reasoning:
            count = db.strip_reasoning(conn, matched_ids)
            console.print(f"[green]Deleted {count:,} reasoning parts from {len(matched_ids)} sessions.[/]")
        else:
            files_result = db.purge_sessions(conn, matched_ids)
            freed = format_bytes(summary["total_bytes"])
            msg = f"[green]Deleted {summary['session_count']:,} sessions, freed ~{freed}."
            if files_result.files_deleted:
                freed_bytes = format_bytes(files_result.bytes_freed)
                msg += f" Removed {files_result.files_deleted} session diff file(s) ({freed_bytes})."
            msg += "[/]"
            console.print(msg)
    finally:
        conn.close()

    console.print("[dim]Run 'ocgc vacuum' to reclaim disk space.[/]")


def run_vacuum(force: bool = False) -> None:
    warn_if_opencode_running()

    db_info = db.get_db_info()
    if db_info.db_size == 0:
        path = db.get_db_path()
        if not path.exists():
            click.echo(f"Error: Database not found at {path}", err=True)
            raise SystemExit(1)
    console.print(f"[dim]Current DB size: {db_info.db_size / 1048576:.1f} MB[/]")
    console.print("[yellow]Warning:[/] VACUUM temporarily doubles disk usage.")
    console.print("[dim]Note: VACUUM will fail if opencode is running (DB locked) or if disk space is insufficient.[/]")

    if not force and not click.confirm("Proceed with VACUUM?"):
        return

    with console.status("[bold cyan]Running VACUUM...[/]"):
        try:
            before, after = db.vacuum_db()
        except sqlite3.OperationalError as e:
            err_msg = str(e).lower()
            if "locked" in err_msg or "busy" in err_msg:
                console.print("[red]Error:[/] Database is locked. Is opencode still running? Close it and try again.")
            elif "full" in err_msg or "no space" in err_msg or "disk" in err_msg:
                console.print("[red]Error:[/] Insufficient disk space. VACUUM needs roughly the DB size in free space.")
            else:
                console.print(f"[red]Error:[/] VACUUM failed: {e}")
            raise SystemExit(1) from None

    print_vacuum_result(before, after)


def run_clean_snapshots(dry_run: bool, force: bool) -> None:
    projects = db.get_snapshot_projects()
    if not projects:
        console.print("[dim]No snapshot directories found.[/]")
        return

    total_bytes = sum(size for _, size in projects)

    grid = Table.grid(padding=(0, 2))
    grid.add_column(style=C_DIM, justify="right")
    grid.add_column(style=C_VALUE)
    grid.add_row("Snapshot dirs", str(len(projects)))
    grid.add_row("Total size", format_bytes(total_bytes))

    label = "[bold yellow]Dry Run — Snapshots to delete[/]" if dry_run else "[bold red]Clean Snapshots[/]"
    border = "yellow" if dry_run else "red"
    console.print(Panel(grid, title=label, border_style=border))

    if dry_run:
        return

    if not force:
        console.print("[yellow]Warning:[/] This deletes git snapshot data for all projects.")
        console.print("[dim]Snapshots will be recreated by opencode as needed.[/]")
        if not click.confirm("Delete all snapshot directories?"):
            return

    result = db.purge_snapshots()
    freed = format_bytes(result.bytes_freed)
    console.print(f"[green]Deleted {result.files_deleted} snapshot dir(s), freed {freed}.[/]")


def run_clean_orphans(dry_run: bool, force: bool) -> None:
    try:
        conn = db.connect(readonly=True)
    except FileNotFoundError as e:
        click.echo(f"Error: {e}", err=True)
        raise SystemExit(1) from None
    try:
        orphans = db.get_orphan_session_diffs(conn)
    finally:
        conn.close()

    if not orphans:
        console.print("[dim]No orphan session diff files found.[/]")
        return

    total_bytes = sum(o.size for o in orphans)

    grid = Table.grid(padding=(0, 2))
    grid.add_column(style=C_DIM, justify="right")
    grid.add_column(style=C_VALUE)
    grid.add_row("Orphan files", str(len(orphans)))
    grid.add_row("Total size", format_bytes(total_bytes))

    label = "[bold yellow]Dry Run — Orphan diffs to delete[/]" if dry_run else "[bold red]Clean Orphans[/]"
    border = "yellow" if dry_run else "red"
    console.print(Panel(grid, title=label, border_style=border))

    if dry_run:
        return

    if not force and not click.confirm(f"Delete {len(orphans)} orphan session diff file(s)?"):
        return

    result = db.purge_orphan_diffs(orphans)
    console.print(f"[green]Deleted {result.files_deleted} orphan file(s), freed {format_bytes(result.bytes_freed)}.[/]")
