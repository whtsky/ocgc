"""Click CLI entry point."""

import click

ENV_VARS = [
    ("OCGC_DB_PATH", "Path to the OpenCode database (highest priority)"),
    ("OPENCODE_DB", "opencode's own DB override; relative values resolve against the data dir"),
    ("XDG_DATA_HOME", "Base of the default data dir ($XDG_DATA_HOME/opencode)"),
    ("OCGC_SCHEMA", "Default for --schema (auto, v1, v2)"),
    ("OCGC_SKIP_RUNNING_CHECK", 'Skip the "is opencode running?" check'),
]


class OcgcGroup(click.Group):
    """Group whose --help also lists every subcommand's options and the env vars."""

    def format_epilog(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        for name in self.list_commands(ctx):
            cmd = self.get_command(ctx, name)
            if cmd is None or cmd.hidden:
                continue
            sub_ctx = click.Context(cmd, info_name=name, parent=ctx)
            rows = [rv for p in cmd.get_params(sub_ctx) if (rv := p.get_help_record(sub_ctx)) is not None]
            rows = [r for r in rows if not r[0].startswith("--help")]
            if not rows:
                continue
            with formatter.section(f"{name.capitalize()} options (ocgc {name} ...)"):
                formatter.write_dl(rows)
        with formatter.section("Environment variables"):
            formatter.write_dl(ENV_VARS)
        super().format_epilog(ctx, formatter)


@click.group(cls=OcgcGroup, epilog="Run 'ocgc COMMAND --help' for details on a single command.")
@click.version_option(package_name="ocgc", prog_name="ocgc")
@click.option(
    "--schema",
    type=click.Choice(["auto", "v1", "v2"]),
    default=None,
    help="Force the DB schema: v1 (opencode 1.x), v2 (opencode 2.x) or auto-detect (default; env OCGC_SCHEMA).",
)
def cli(schema: str | None) -> None:
    """ocgc - OpenCode Garbage Collector.

    Analyze and reclaim storage used by OpenCode sessions, diffs, and snapshots.
    Works with both opencode 1.x (v1) and 2.x (v2) databases.
    """
    from ocgc import db

    db.set_schema_override(schema)


@cli.command()
def status() -> None:
    """Dashboard: DB size, session count, storage breakdown."""
    from ocgc.analyzer import run_status

    run_status()


@cli.command()
@click.option("--sort", "sort_by", type=click.Choice(["size", "age", "name"]), default="size", help="Sort sessions by")
@click.option("--limit", "-l", type=int, default=None, help="Limit number of sessions shown")
def sessions(sort_by: str, limit: int | None) -> None:
    """List sessions with sizes, ages, and types."""
    from ocgc.analyzer import run_sessions

    run_sessions(sort_by=sort_by, limit=limit)


@cli.command(short_help="Deep analysis: biggest sessions, part types, growth rate.")
def analyze() -> None:
    """Deep analysis: biggest sessions, part type breakdown, growth rate."""
    from ocgc.analyzer import run_analyze

    run_analyze()


@cli.command(
    epilog="Filters combine with AND. --strip-* flags remove only those parts from matching sessions (all "
    "sessions if no filter is given) instead of deleting sessions. --clean-* flags run independently before "
    "any session purge."
)
@click.option("--older-than", default=None, help="Delete sessions older than duration (e.g., 15m, 1h, 7d, 2w, 3mo)")
@click.option("--subagents", is_flag=True, default=False, help="Delete subagent sessions (parent_id IS NOT NULL)")
@click.option("--larger-than", default=None, help="Delete sessions larger than size (e.g., 50M, 1G)")
@click.option("--strip-reasoning", is_flag=True, default=False, help="Remove reasoning parts only (keeps sessions)")
@click.option("--strip-tools", is_flag=True, default=False, help="Remove tool-call parts only (keeps sessions)")
@click.option("--strip-events", is_flag=True, default=False, help="Remove event-log rows only (keeps sessions)")
@click.option(
    "--strip-diffs",
    is_flag=True,
    default=False,
    help="Delete session diff files (storage/session_diff/*.json) only (keeps sessions)",
)
@click.option(
    "--legacy",
    is_flag=True,
    default=False,
    help="Only target v1 (opencode 1.x) sessions in the v1 tables; on a migrated v2 DB these are the stale "
    "pre-migration copies. Combines with the other filters and --strip-* (alone: all v1 sessions).",
)
@click.option("--session", "session_ids", multiple=True, help="Delete specific session by ID (repeatable)")
@click.option("--keep-latest", type=int, default=None, help="Keep N most recent sessions, delete the rest")
@click.option("--clean-snapshots", is_flag=True, default=False, help="Delete all snapshot directories")
@click.option(
    "--clean-orphans",
    is_flag=True,
    default=False,
    help="Delete orphan session diff files (no matching session)",
)
@click.option(
    "--clean-orphan-events",
    is_flag=True,
    default=False,
    help="Delete event rows whose session no longer exists",
)
@click.option(
    "--clean-legacy",
    is_flag=True,
    default=False,
    help="Delete stale v1 session/message/part rows left behind after opencode 2.x's v1->v2 migration",
)
@click.option("--dry-run", "-n", is_flag=True, default=False, help="Show what would be deleted without doing it")
@click.option("--force", "-f", is_flag=True, default=False, help="Skip confirmation prompt")
def purge(
    older_than: str | None,
    subagents: bool,
    larger_than: str | None,
    strip_reasoning: bool,
    strip_tools: bool,
    strip_events: bool,
    strip_diffs: bool,
    legacy: bool,
    session_ids: tuple[str, ...],
    keep_latest: int | None,
    clean_snapshots: bool,
    clean_orphans: bool,
    clean_orphan_events: bool,
    clean_legacy: bool,
    dry_run: bool,
    force: bool,
) -> None:
    """Delete sessions by age, type, size, or ID."""
    if keep_latest is not None and keep_latest < 0:
        raise click.BadParameter("must be a non-negative integer", param_hint="'--keep-latest'")
    from ocgc.purger import (
        run_clean_legacy,
        run_clean_orphan_events,
        run_clean_orphans,
        run_clean_snapshots,
        run_purge,
    )

    ran_cleaner = False
    if clean_snapshots:
        run_clean_snapshots(dry_run=dry_run, force=force)
        ran_cleaner = True
    if clean_orphans:
        run_clean_orphans(dry_run=dry_run, force=force)
        ran_cleaner = True
    if clean_orphan_events:
        run_clean_orphan_events(dry_run=dry_run, force=force)
        ran_cleaner = True
    if clean_legacy:
        run_clean_legacy(dry_run=dry_run, force=force)
        ran_cleaner = True

    wants_purge = bool(
        older_than
        or subagents
        or larger_than
        or session_ids
        or keep_latest is not None
        or strip_reasoning
        or strip_tools
        or strip_events
        or strip_diffs
        or legacy
    )
    if ran_cleaner and not wants_purge:
        return

    run_purge(
        older_than=older_than,
        subagents=subagents,
        larger_than=larger_than,
        strip_reasoning=strip_reasoning,
        strip_tools=strip_tools,
        strip_events=strip_events,
        strip_diffs=strip_diffs,
        session_ids=session_ids,
        keep_latest=keep_latest,
        dry_run=dry_run,
        force=force,
        legacy=legacy,
    )


@cli.command()
@click.option("--force", "-f", is_flag=True, default=False, help="Skip confirmation prompt")
def vacuum(force: bool) -> None:
    """Run VACUUM to reclaim disk space after purge."""
    from ocgc.purger import run_vacuum

    run_vacuum(force=force)
