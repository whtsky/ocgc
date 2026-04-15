"""Analysis logic."""

import time

from ocgc import db


def run_status() -> None:
    from ocgc.display import print_status, warn_if_opencode_running

    warn_if_opencode_running()

    db_info = db.get_db_info()
    try:
        conn = db.connect(readonly=True)
    except FileNotFoundError as e:
        import click

        click.echo(f"Error: {e}", err=True)
        raise SystemExit(1) from None
    try:
        root_count, sub_count = db.get_session_count(conn)
        part_stats = db.get_part_type_stats(conn)
        now_ms = int(time.time() * 1000)
        age_dist = db.get_age_distribution(conn, now_ms)
        fs_stats = db.get_filesystem_stats()
        print_status(db_info, root_count, sub_count, part_stats, age_dist, fs_stats)
    finally:
        conn.close()


def run_sessions(sort_by: str = "size", limit: int | None = None) -> None:
    from ocgc.display import print_sessions, warn_if_opencode_running

    warn_if_opencode_running()

    try:
        conn = db.connect(readonly=True)
    except FileNotFoundError as e:
        import click

        click.echo(f"Error: {e}", err=True)
        raise SystemExit(1) from None
    try:
        sessions = db.get_sessions(conn, sort_by=sort_by, limit=limit)
        if not sessions:
            from ocgc.display import console

            console.print("[dim]No sessions found.[/]")
            return
        print_sessions(sessions)
    finally:
        conn.close()


def run_analyze() -> None:
    from ocgc.display import print_analysis, warn_if_opencode_running

    warn_if_opencode_running()

    try:
        conn = db.connect(readonly=True)
    except FileNotFoundError as e:
        import click

        click.echo(f"Error: {e}", err=True)
        raise SystemExit(1) from None
    try:
        top_sessions = db.get_sessions(conn, sort_by="size", limit=10)
        if not top_sessions:
            from ocgc.display import console

            console.print("[dim]No sessions found.[/]")
            return
        root_count, sub_count = db.get_session_count(conn)
        total_sessions = root_count + sub_count
        root_stats, sub_stats = db.get_part_type_stats_by_session_type(conn)
        total_bytes = sum(s.size_bytes for s in root_stats) + sum(s.size_bytes for s in sub_stats)
        avg_size = total_bytes / total_sessions if total_sessions else 0
        growth_rate = db.get_growth_rate(conn)
        fs_stats = db.get_filesystem_stats()
        orphans = db.get_orphan_session_diffs(conn)
        print_analysis(
            top_sessions=top_sessions,
            avg_size=avg_size,
            growth_rate=growth_rate,
            root_stats=root_stats,
            sub_stats=sub_stats,
            total_sessions=total_sessions,
            fs_stats=fs_stats,
            orphan_count=len(orphans),
            orphan_bytes=sum(o.size for o in orphans),
        )
    finally:
        conn.close()
