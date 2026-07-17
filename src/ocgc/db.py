"""SQLite query layer for OpenCode's database."""

import os
import sqlite3
import subprocess
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

DEFAULT_DB_PATH = Path.home() / ".local" / "share" / "opencode" / "opencode.db"


@dataclass
class DBInfo:
    path: Path
    db_size: int
    wal_size: int

    @property
    def total_size(self) -> int:
        return self.db_size + self.wal_size


@dataclass
class SessionRow:
    id: str
    parent_id: str | None
    directory: str
    title: str | None
    time_created: int  # ms epoch
    time_updated: int  # ms epoch
    part_bytes: int
    event_bytes: int
    message_count: int

    @property
    def size_bytes(self) -> int:
        """Total stored bytes for this session: part data + event-log data."""
        return self.part_bytes + self.event_bytes

    @property
    def is_subagent(self) -> bool:
        return self.parent_id is not None


@dataclass
class PartTypeStats:
    type_name: str
    count: int
    size_bytes: int


@dataclass
class EventTypeStats:
    """Byte usage of the append-only `event` table, grouped by event type.

    The event log (message.updated.1 snapshots etc.) is invisible to part-based
    accounting yet is frequently the single largest consumer of DB space.
    """

    type_name: str
    count: int
    size_bytes: int


@dataclass
class DBHealth:
    """Physical health of the SQLite file, independent of logical row data."""

    reclaimable_bytes: int  # freelist_count * page_size — freed by VACUUM
    auto_vacuum: str  # "NONE" | "FULL" | "INCREMENTAL" | raw value


@dataclass
class FilesystemStats:
    session_diff_size: int
    snapshot_size: int
    tool_output_size: int
    session_diff_count: int
    snapshot_count: int

    @property
    def total_size(self) -> int:
        return self.session_diff_size + self.snapshot_size + self.tool_output_size


@dataclass
class PurgeFilesResult:
    files_deleted: int = 0
    bytes_freed: int = 0


@dataclass
class OrphanDiff:
    session_id: str
    path: Path
    size: int


def get_db_path() -> Path:
    return Path(os.environ.get("OCGC_DB_PATH", str(DEFAULT_DB_PATH)))


def check_opencode_running() -> bool:
    if os.environ.get("OCGC_SKIP_RUNNING_CHECK"):
        return False
    try:
        result = subprocess.run(
            ["pgrep", "-x", "opencode"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def connect(readonly: bool = True) -> sqlite3.Connection:
    path = get_db_path()
    if not path.exists():
        raise FileNotFoundError(f"OpenCode database not found at {path}")
    uri = f"file:{quote(str(path))}{'?mode=ro' if readonly else ''}"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    if not readonly:
        conn.execute("PRAGMA journal_mode=WAL")
    return conn


def get_db_info() -> DBInfo:
    path = get_db_path()
    wal_path = Path(str(path) + "-wal")
    return DBInfo(
        path=path,
        db_size=path.stat().st_size if path.exists() else 0,
        wal_size=wal_path.stat().st_size if wal_path.exists() else 0,
    )


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    """True if a table exists — OpenCode's schema varies across versions."""
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (name,),
    ).fetchone()
    return row is not None


def event_table_exists(conn: sqlite3.Connection) -> bool:
    """The `event` table (event-sourcing log) only exists on newer schemas."""
    return _table_exists(conn, "event")


_AUTO_VACUUM_MODES = {0: "NONE", 1: "FULL", 2: "INCREMENTAL"}


def get_db_health(conn: sqlite3.Connection) -> DBHealth:
    """Report reclaimable dead space and the auto_vacuum mode.

    ``reclaimable_bytes`` is free-but-unreleased space that a VACUUM would return
    to the OS. When ``auto_vacuum`` is NONE, deletes never shrink the file on
    their own, so periodic VACUUM is required.
    """
    page_size = conn.execute("PRAGMA page_size").fetchone()[0]
    freelist = conn.execute("PRAGMA freelist_count").fetchone()[0]
    auto_vacuum = conn.execute("PRAGMA auto_vacuum").fetchone()[0]
    return DBHealth(
        reclaimable_bytes=int(page_size) * int(freelist),
        auto_vacuum=_AUTO_VACUUM_MODES.get(int(auto_vacuum), str(auto_vacuum)),
    )


def get_session_count(conn: sqlite3.Connection) -> tuple[int, int]:
    """Return (root_count, subagent_count)."""
    row = conn.execute("""
        SELECT
            SUM(CASE WHEN parent_id IS NULL THEN 1 ELSE 0 END),
            SUM(CASE WHEN parent_id IS NOT NULL THEN 1 ELSE 0 END)
        FROM session
    """).fetchone()
    return (row[0] or 0, row[1] or 0)


def get_part_type_stats(conn: sqlite3.Connection) -> list[PartTypeStats]:
    rows = conn.execute("""
        SELECT
            json_extract(data, '$.type') AS t,
            COUNT(*) AS cnt,
            SUM(LENGTH(data)) AS sz
        FROM part
        GROUP BY t
        ORDER BY sz DESC
    """).fetchall()
    return [PartTypeStats(r["t"] or "unknown", r["cnt"], r["sz"] or 0) for r in rows]


def get_event_type_stats(conn: sqlite3.Connection) -> list[EventTypeStats]:
    """Byte usage of the `event` table grouped by event type (largest first)."""
    if not event_table_exists(conn):
        return []
    rows = conn.execute("""
        SELECT
            type AS t,
            COUNT(*) AS cnt,
            SUM(LENGTH(data)) AS sz
        FROM event
        GROUP BY type
        ORDER BY sz DESC
    """).fetchall()
    return [EventTypeStats(r["t"] or "unknown", r["cnt"], r["sz"] or 0) for r in rows]


def get_event_total(conn: sqlite3.Connection) -> tuple[int, int]:
    """Return (row_count, total_bytes) for the whole event table."""
    if not event_table_exists(conn):
        return (0, 0)
    row = conn.execute("SELECT COUNT(*), SUM(LENGTH(data)) FROM event").fetchone()
    return (row[0] or 0, row[1] or 0)


def get_message_total(conn: sqlite3.Connection) -> tuple[int, int]:
    """Return (row_count, total_bytes) for the message table."""
    row = conn.execute("SELECT COUNT(*), SUM(LENGTH(data)) FROM message").fetchone()
    return (row[0] or 0, row[1] or 0)


def get_orphan_event_stats(conn: sqlite3.Connection) -> tuple[int, int]:
    """Return (row_count, total_bytes) for events whose session no longer exists.

    OpenCode's `event` table has no foreign key to `session` (only to
    `event_sequence`), so deleting a session leaves its events behind. These
    orphans are always safe to remove.
    """
    if not event_table_exists(conn):
        return (0, 0)
    row = conn.execute(
        "SELECT COUNT(*), SUM(LENGTH(data)) FROM event "
        "WHERE aggregate_id NOT IN (SELECT id FROM session)"
    ).fetchone()
    return (row[0] or 0, row[1] or 0)


def get_age_distribution(conn: sqlite3.Connection, now_ms: int) -> dict[str, int]:
    """Return session counts bucketed by age."""
    buckets = {
        "last 24h": 24 * 3600 * 1000,
        "1-7 days": 7 * 24 * 3600 * 1000,
        "7-14 days": 14 * 24 * 3600 * 1000,
        "14-30 days": 30 * 24 * 3600 * 1000,
    }
    result: dict[str, int] = {}
    prev_cutoff = now_ms
    for label, age_ms in buckets.items():
        cutoff = now_ms - age_ms
        count = conn.execute(
            "SELECT COUNT(*) FROM session WHERE time_created < ? AND time_created >= ?",
            (prev_cutoff, cutoff),
        ).fetchone()[0]
        result[label] = count
        prev_cutoff = cutoff

    older = conn.execute(
        "SELECT COUNT(*) FROM session WHERE time_created < ?",
        (prev_cutoff,),
    ).fetchone()[0]
    result["older than 30d"] = older
    return result


def get_sessions(
    conn: sqlite3.Connection,
    sort_by: str = "size",
    limit: int | None = None,
) -> list[SessionRow]:
    order_clause = {
        "size": "size_bytes DESC",
        "age": "s.time_created ASC",
        "name": "s.title ASC",
    }.get(sort_by, "size_bytes DESC")

    limit_clause = "LIMIT ?" if limit else ""
    params: list[int] = []
    if limit:
        params.append(limit)

    # event rows are keyed by aggregate_id, which holds the session id.
    has_events = event_table_exists(conn)
    if has_events:
        event_join = """
        LEFT JOIN (
            SELECT aggregate_id, SUM(LENGTH(data)) AS event_bytes
            FROM event
            GROUP BY aggregate_id
        ) es ON es.aggregate_id = s.id
        """
        event_bytes_expr = "COALESCE(es.event_bytes, 0)"
    else:
        event_join = ""
        event_bytes_expr = "0"

    rows = conn.execute(
        f"""
        SELECT
            s.id,
            s.parent_id,
            s.directory,
            s.title,
            s.time_created,
            s.time_updated,
            COALESCE(ps.size_bytes, 0) AS part_bytes,
            {event_bytes_expr} AS event_bytes,
            COALESCE(ps.size_bytes, 0) + {event_bytes_expr} AS size_bytes,
            COALESCE(mc.msg_count, 0) AS message_count
        FROM session s
        LEFT JOIN (
            SELECT session_id, SUM(LENGTH(data)) AS size_bytes
            FROM part
            GROUP BY session_id
        ) ps ON ps.session_id = s.id
        {event_join}
        LEFT JOIN (
            SELECT session_id, COUNT(*) AS msg_count
            FROM message
            GROUP BY session_id
        ) mc ON mc.session_id = s.id
        ORDER BY {order_clause}
        {limit_clause}
    """,
        params,
    ).fetchall()

    return [
        SessionRow(
            id=r["id"],
            parent_id=r["parent_id"],
            directory=r["directory"],
            title=r["title"],
            time_created=r["time_created"],
            time_updated=r["time_updated"],
            part_bytes=r["part_bytes"],
            event_bytes=r["event_bytes"],
            message_count=r["message_count"],
        )
        for r in rows
    ]


def get_part_type_stats_by_session_type(
    conn: sqlite3.Connection,
) -> tuple[list[PartTypeStats], list[PartTypeStats]]:
    """Return (root_stats, subagent_stats)."""

    def _query(is_subagent: bool) -> list[PartTypeStats]:
        condition = "s.parent_id IS NOT NULL" if is_subagent else "s.parent_id IS NULL"
        rows = conn.execute(f"""
            SELECT
                json_extract(p.data, '$.type') AS t,
                COUNT(*) AS cnt,
                SUM(LENGTH(p.data)) AS sz
            FROM part p
            JOIN session s ON s.id = p.session_id
            WHERE {condition}
            GROUP BY t
            ORDER BY sz DESC
        """).fetchall()
        return [PartTypeStats(r["t"] or "unknown", r["cnt"], r["sz"] or 0) for r in rows]

    return _query(False), _query(True)


def get_growth_rate(conn: sqlite3.Connection) -> float | None:
    """Estimate MB per active day of usage."""
    row = conn.execute("""
        SELECT
            COUNT(DISTINCT date(s.time_created / 1000, 'unixepoch')) AS active_days,
            SUM(LENGTH(p.data)) AS total_bytes
        FROM part p
        JOIN session s ON s.id = p.session_id
    """).fetchone()
    if not row or not row["active_days"] or row["active_days"] < 1:
        return None
    active_days: int = row["active_days"]
    total_bytes = int(row["total_bytes"])
    return (total_bytes / 1048576) / active_days


def get_session_ids_for_purge(
    conn: sqlite3.Connection,
    older_than_ms: int | None = None,
    subagents_only: bool = False,
    larger_than_bytes: int | None = None,
    session_ids: list[str] | None = None,
    keep_latest: int | None = None,
    now_ms: int | None = None,
) -> list[str]:
    """Return session IDs matching purge criteria. Filters combine with AND."""
    conditions = []
    params: list[str | int] = []

    if session_ids:
        placeholders = ",".join("?" for _ in session_ids)
        conditions.append(f"s.id IN ({placeholders})")
        params.extend(session_ids)

    if subagents_only:
        conditions.append("s.parent_id IS NOT NULL")

    if older_than_ms is not None and now_ms is not None:
        cutoff = now_ms - older_than_ms
        conditions.append("s.time_created < ?")
        params.append(cutoff)

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    if larger_than_bytes is not None:
        if event_table_exists(conn):
            event_join = """
            LEFT JOIN (
                SELECT aggregate_id, SUM(LENGTH(data)) AS event_bytes
                FROM event GROUP BY aggregate_id
            ) es ON es.aggregate_id = s.id
            """
            size_expr = "COALESCE(ps.size_bytes, 0) + COALESCE(es.event_bytes, 0)"
        else:
            event_join = ""
            size_expr = "COALESCE(ps.size_bytes, 0)"
        query = f"""
            SELECT s.id FROM session s
            LEFT JOIN (
                SELECT session_id, SUM(LENGTH(data)) AS size_bytes
                FROM part GROUP BY session_id
            ) ps ON ps.session_id = s.id
            {event_join}
            {where}
            {"AND" if conditions else "WHERE"} {size_expr} >= ?
        """
        params.append(larger_than_bytes)
    else:
        query = f"SELECT s.id FROM session s {where}"

    ids = {r[0] for r in conn.execute(query, params)}

    if keep_latest is not None:
        if not conditions and larger_than_bytes is None and not session_ids:
            # keep_latest alone: delete everything except the latest N
            latest_ids = {
                r[0]
                for r in conn.execute(
                    "SELECT id FROM session ORDER BY time_created DESC LIMIT ?",
                    (keep_latest,),
                )
            }
            all_ids = {r[0] for r in conn.execute("SELECT id FROM session")}
            ids = all_ids - latest_ids
        else:
            # With other filters: compute latest N from the already-filtered set
            if ids:
                placeholders = ",".join("?" for _ in ids)
                id_list = list(ids)
                latest_ids = {
                    r[0]
                    for r in conn.execute(
                        f"SELECT id FROM session WHERE id IN ({placeholders}) ORDER BY time_created DESC LIMIT ?",
                        [*id_list, keep_latest],
                    )
                }
                ids -= latest_ids

    return sorted(ids)


def get_purge_summary(conn: sqlite3.Connection, session_ids: list[str]) -> dict[str, int]:
    """Return summary stats for sessions about to be purged."""
    if not session_ids:
        return {
            "session_count": 0,
            "part_count": 0,
            "message_count": 0,
            "part_bytes": 0,
            "event_count": 0,
            "event_bytes": 0,
            "total_bytes": 0,
        }
    placeholders = ",".join("?" for _ in session_ids)
    part_row = conn.execute(
        f"SELECT COUNT(*), SUM(LENGTH(data)) FROM part WHERE session_id IN ({placeholders})",
        session_ids,
    ).fetchone()
    msg_count = conn.execute(
        f"SELECT COUNT(*) FROM message WHERE session_id IN ({placeholders})",
        session_ids,
    ).fetchone()[0]
    event_count = 0
    event_bytes = 0
    if event_table_exists(conn):
        event_row = conn.execute(
            f"SELECT COUNT(*), SUM(LENGTH(data)) FROM event WHERE aggregate_id IN ({placeholders})",
            session_ids,
        ).fetchone()
        event_count = event_row[0] or 0
        event_bytes = event_row[1] or 0
    part_bytes = part_row[1] or 0
    return {
        "session_count": len(session_ids),
        "part_count": part_row[0] or 0,
        "message_count": msg_count or 0,
        "part_bytes": part_bytes,
        "event_count": event_count,
        "event_bytes": event_bytes,
        "total_bytes": part_bytes + event_bytes,
    }


def get_reasoning_summary(conn: sqlite3.Connection, session_ids: list[str] | None) -> dict[str, int]:
    """Return summary stats for reasoning parts in given sessions (or all if None)."""
    if session_ids is not None:
        if not session_ids:
            return {"part_count": 0, "total_bytes": 0}
        placeholders = ",".join("?" for _ in session_ids)
        row = conn.execute(
            f"""SELECT COUNT(*), SUM(LENGTH(data)) FROM part
                WHERE json_extract(data, '$.type') = 'reasoning'
                AND session_id IN ({placeholders})""",
            session_ids,
        ).fetchone()
    else:
        row = conn.execute(
            """SELECT COUNT(*), SUM(LENGTH(data)) FROM part
               WHERE json_extract(data, '$.type') = 'reasoning'"""
        ).fetchone()
    return {"part_count": row[0] or 0, "total_bytes": row[1] or 0}


def get_event_summary(conn: sqlite3.Connection, session_ids: list[str] | None) -> dict[str, int]:
    """Return event-log stats for given sessions (or the whole table if None)."""
    if not event_table_exists(conn):
        return {"event_count": 0, "total_bytes": 0}
    if session_ids is not None:
        if not session_ids:
            return {"event_count": 0, "total_bytes": 0}
        placeholders = ",".join("?" for _ in session_ids)
        row = conn.execute(
            f"SELECT COUNT(*), SUM(LENGTH(data)) FROM event WHERE aggregate_id IN ({placeholders})",
            session_ids,
        ).fetchone()
    else:
        row = conn.execute("SELECT COUNT(*), SUM(LENGTH(data)) FROM event").fetchone()
    return {"event_count": row[0] or 0, "total_bytes": row[1] or 0}


def purge_sessions(conn: sqlite3.Connection, session_ids: list[str]) -> PurgeFilesResult:
    """Delete sessions and cascade (parts, messages, todos, shares). Also removes session_diff files."""
    if not session_ids:
        return PurgeFilesResult()
    placeholders = ",".join("?" for _ in session_ids)
    # Due to FK cascades on message and part, deleting session should cascade.
    # But let's be explicit to ensure cleanup.
    conn.execute(f"DELETE FROM part WHERE session_id IN ({placeholders})", session_ids)
    conn.execute(f"DELETE FROM message WHERE session_id IN ({placeholders})", session_ids)
    # Tables that may or may not exist depending on OpenCode version
    for table in ("todo", "session_share"):
        try:
            conn.execute(
                f"DELETE FROM {table} WHERE session_id IN ({placeholders})",
                session_ids,
            )
        except sqlite3.OperationalError as e:
            if "no such table" not in str(e).lower():
                raise
    # event/event_sequence key on aggregate_id with no FK to session, so a session
    # delete never cascades — remove explicitly or they orphan and waste space.
    for table in ("event", "event_sequence"):
        try:
            conn.execute(
                f"DELETE FROM {table} WHERE aggregate_id IN ({placeholders})",
                session_ids,
            )
        except sqlite3.OperationalError as e:
            if "no such table" not in str(e).lower():
                raise
    conn.execute(f"DELETE FROM session WHERE id IN ({placeholders})", session_ids)
    conn.commit()

    # Best-effort cleanup of session_diff files
    return purge_session_diffs(session_ids)


def strip_reasoning(conn: sqlite3.Connection, session_ids: list[str] | None = None) -> int:
    """Delete reasoning parts from given sessions (or all). Return count deleted."""
    if session_ids is not None:
        if not session_ids:
            return 0
        placeholders = ",".join("?" for _ in session_ids)
        cur = conn.execute(
            f"""DELETE FROM part
                WHERE json_extract(data, '$.type') = 'reasoning'
                AND session_id IN ({placeholders})""",
            session_ids,
        )
    else:
        cur = conn.execute("DELETE FROM part WHERE json_extract(data, '$.type') = 'reasoning'")
    conn.commit()
    return cur.rowcount


def strip_events(conn: sqlite3.Connection, session_ids: list[str] | None = None) -> int:
    """Delete event-log rows for given sessions (or all). Return count deleted.

    Leaves ``event_sequence`` intact so a live session keeps its seq counter and
    OpenCode simply appends from where it left off. The message/part tables hold
    the canonical state, so dropping the event log does not lose history.
    """
    if not event_table_exists(conn):
        return 0
    if session_ids is not None:
        if not session_ids:
            return 0
        placeholders = ",".join("?" for _ in session_ids)
        cur = conn.execute(
            f"DELETE FROM event WHERE aggregate_id IN ({placeholders})",
            session_ids,
        )
    else:
        cur = conn.execute("DELETE FROM event")
    conn.commit()
    return cur.rowcount


def purge_orphan_events(conn: sqlite3.Connection) -> dict[str, int]:
    """Delete events (and event_sequence rows) whose session no longer exists."""
    count, freed = get_orphan_event_stats(conn)
    if count == 0:
        return {"event_count": 0, "bytes_freed": 0}
    conn.execute("DELETE FROM event WHERE aggregate_id NOT IN (SELECT id FROM session)")
    try:
        conn.execute("DELETE FROM event_sequence WHERE aggregate_id NOT IN (SELECT id FROM session)")
    except sqlite3.OperationalError as e:
        if "no such table" not in str(e).lower():
            raise
    conn.commit()
    return {"event_count": count, "bytes_freed": freed}


def _total_db_size(path: Path) -> int:
    """Return combined size of db + WAL + SHM files."""
    total = path.stat().st_size if path.exists() else 0
    for suffix in ("-wal", "-shm"):
        p = Path(str(path) + suffix)
        if p.exists():
            total += p.stat().st_size
    return total


def get_storage_dir() -> Path:
    """Return the opencode storage base dir (parent of the DB file)."""
    return get_db_path().parent


def _dir_size(path: Path) -> int:
    """Return total size of all files in a directory tree."""
    if not path.is_dir():
        return 0
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


def get_filesystem_stats() -> FilesystemStats:
    """Return sizes of session_diff/, snapshot/, and tool-output/ directories."""
    base = get_storage_dir()
    diff_dir = base / "storage" / "session_diff"
    snap_dir = base / "snapshot"
    tool_dir = base / "tool-output"

    diff_count = len(list(diff_dir.glob("*.json"))) if diff_dir.is_dir() else 0
    snap_count = len([d for d in snap_dir.iterdir() if d.is_dir()]) if snap_dir.is_dir() else 0

    return FilesystemStats(
        session_diff_size=_dir_size(diff_dir),
        snapshot_size=_dir_size(snap_dir),
        tool_output_size=_dir_size(tool_dir),
        session_diff_count=diff_count,
        snapshot_count=snap_count,
    )


def purge_session_diffs(session_ids: list[str]) -> PurgeFilesResult:
    """Delete session_diff JSON files for given session IDs. Best-effort."""
    diff_dir = get_storage_dir() / "storage" / "session_diff"
    result = PurgeFilesResult()
    if not diff_dir.is_dir():
        return result
    for sid in session_ids:
        path = diff_dir / f"{sid}.json"
        try:
            size = path.stat().st_size
            path.unlink()
            result.files_deleted += 1
            result.bytes_freed += size
        except FileNotFoundError:
            pass
    return result


def get_orphan_session_diffs(conn: sqlite3.Connection) -> list[OrphanDiff]:
    """Find session_diff files that have no matching session in DB."""
    diff_dir = get_storage_dir() / "storage" / "session_diff"
    if not diff_dir.is_dir():
        return []

    db_ids = {r[0] for r in conn.execute("SELECT id FROM session")}
    orphans = []
    for path in diff_dir.glob("*.json"):
        sid = path.stem
        if sid not in db_ids:
            orphans.append(OrphanDiff(session_id=sid, path=path, size=path.stat().st_size))
    return sorted(orphans, key=lambda o: o.size, reverse=True)


def purge_orphan_diffs(orphans: list[OrphanDiff]) -> PurgeFilesResult:
    """Delete orphan session_diff files. Best-effort."""
    result = PurgeFilesResult()
    for o in orphans:
        try:
            o.path.unlink()
            result.files_deleted += 1
            result.bytes_freed += o.size
        except FileNotFoundError:
            pass
    return result


def get_snapshot_projects() -> list[tuple[str, int]]:
    """Return (dirname, size_bytes) for each snapshot project directory."""
    snap_dir = get_storage_dir() / "snapshot"
    if not snap_dir.is_dir():
        return []
    projects = []
    for d in sorted(snap_dir.iterdir()):
        if d.is_dir():
            projects.append((d.name, _dir_size(d)))
    return projects


def purge_snapshots(project: str | None = None) -> PurgeFilesResult:
    """Delete snapshot directories. If project given, delete only that one."""
    import shutil

    snap_dir = get_storage_dir() / "snapshot"
    result = PurgeFilesResult()
    if not snap_dir.is_dir():
        return result

    if project:
        target = snap_dir / project
        if target.is_dir():
            result.bytes_freed = _dir_size(target)
            shutil.rmtree(target)
            result.files_deleted = 1
    else:
        for d in snap_dir.iterdir():
            if d.is_dir():
                result.bytes_freed += _dir_size(d)
                shutil.rmtree(d)
                result.files_deleted += 1

    return result


def vacuum_db() -> tuple[int, int]:
    """Run VACUUM. Returns (before_total, after_total) including WAL+SHM."""
    path = get_db_path()
    before = _total_db_size(path)
    conn = connect(readonly=False)
    conn.execute("VACUUM")
    conn.close()
    after = _total_db_size(path)
    return before, after
