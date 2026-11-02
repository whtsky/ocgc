"""SQLite query layer for OpenCode's database."""

import json
import os
import sqlite3
import subprocess
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

DATA_DIR = Path(os.environ.get("XDG_DATA_HOME") or Path.home() / ".local" / "share") / "opencode"
DEFAULT_DB_PATH = DATA_DIR / "opencode.db"

SCHEMA_CHOICES = ("auto", "v1", "v2")
_schema_override: str | None = None


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
    content_bytes: int  # v1: part data; v2: session_message data + referenced blobs
    event_bytes: int
    message_count: int
    schema: str = "v1"  # which table set the row was read from: "v1" | "v2"

    @property
    def size_bytes(self) -> int:
        """Total stored bytes for this session: content data + event-log data."""
        return self.content_bytes + self.event_bytes

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


class SchemaError(ValueError):
    """The database's schema could not be determined or does not match --schema."""


@dataclass(frozen=True)
class SchemaInfo:
    """Which OpenCode table set(s) hold the sessions.

    v1 (opencode 1.x): ``session`` / ``message`` / ``part``.
    v2 (opencode 2.x): ``session_v2`` / ``session_message`` (each message row
    carries its own content; assistant rows hold a ``content`` array of
    text/reasoning/tool items), plus ``session_inbox``, ``instruction_*`` and
    content-addressed ``blob`` / ``blob_ref``.

    opencode 2.x migrates v1 sessions once and records ``{"phase":"completed"}``
    under kv key ``migration.v1-v2``. After that the v1 tables are stale copies
    that v2 never reads again.
    """

    mode: str  # "v1" | "v2" | "mixed" (v2 sessions + v1 sessions not yet migrated)
    has_v1: bool
    has_v2: bool
    migrated: bool

    @property
    def reads_v1(self) -> bool:
        return self.mode in ("v1", "mixed")

    @property
    def reads_v2(self) -> bool:
        return self.mode in ("v2", "mixed")

    @property
    def legacy_stale(self) -> bool:
        """v1 tables still exist but the v1->v2 migration has completed."""
        return self.has_v1 and self.has_v2 and self.migrated

    @property
    def label(self) -> str:
        return {
            "v1": "v1 (opencode 1.x)",
            "v2": "v2 (opencode 2.x)",
            "mixed": "mixed (v2 + unmigrated v1)",
        }[self.mode]


def set_schema_override(schema: str | None) -> None:
    """Force schema detection to "v1" or "v2" ("auto"/None restores detection)."""
    global _schema_override
    _schema_override = None if schema in (None, "auto") else schema


def get_schema_override() -> str | None:
    return _schema_override


def get_db_path() -> Path:
    """Resolve the DB path: $OCGC_DB_PATH, else $OPENCODE_DB (relative values are
    resolved against the opencode data dir, as opencode does), else the default."""
    if os.environ.get("OCGC_DB_PATH"):
        return Path(os.environ["OCGC_DB_PATH"])
    if os.environ.get("OPENCODE_DB"):
        p = Path(os.environ["OPENCODE_DB"])
        return p if p.is_absolute() else DATA_DIR / p
    return DEFAULT_DB_PATH


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
    try:
        get_schema(conn)  # fail fast on a non-OpenCode DB or a --schema mismatch
        if not readonly:
            conn.execute("PRAGMA journal_mode=WAL")
    except BaseException:
        conn.close()
        raise
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


def get_schema(conn: sqlite3.Connection) -> SchemaInfo:
    """Detect the schema the same way opencode-sessions/opencode-messages do.

    auto: v2 if only v2 tables exist or the v1->v2 migration completed; v1 if
    only v1 tables exist; otherwise mixed. ``OCGC_SCHEMA`` or
    :func:`set_schema_override` forces v1 or v2.
    """
    names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    has_v1 = {"session", "message", "part"} <= names
    has_v2 = {"session_v2", "session_message"} <= names
    migrated = False
    if has_v2 and "kv" in names:
        row = conn.execute("SELECT value FROM kv WHERE key = 'migration.v1-v2'").fetchone()
        if row is not None:
            try:
                migrated = json.loads(row[0]).get("phase") == "completed"
            except (ValueError, AttributeError):
                migrated = False

    forced = _schema_override or os.environ.get("OCGC_SCHEMA") or "auto"
    if forced not in SCHEMA_CHOICES:
        raise SchemaError(f"schema must be one of {', '.join(SCHEMA_CHOICES)}, got {forced!r}")
    if forced == "v1":
        if not has_v1:
            raise SchemaError("schema v1 forced but the database has no v1 session/message/part tables")
        mode = "v1"
    elif forced == "v2":
        if not has_v2:
            raise SchemaError("schema v2 forced but the database has no v2 session_v2/session_message tables")
        mode = "v2"
    elif has_v2 and (migrated or not has_v1):
        mode = "v2"
    elif has_v2:
        mode = "mixed"
    elif has_v1:
        mode = "v1"
    else:
        raise SchemaError("database does not look like an OpenCode database (no session tables)")
    return SchemaInfo(mode=mode, has_v1=has_v1, has_v2=has_v2, migrated=migrated)


def _session_source(schema: SchemaInfo) -> str:
    """SELECT yielding every visible session with a `schema` column ('v1'/'v2').

    In mixed mode a v1 session already copied into session_v2 is read from v2 only.
    """
    cols = "id, parent_id, directory, title, time_created, time_updated"
    v1 = f"SELECT {cols}, 'v1' AS schema FROM session"
    v2 = f"SELECT {cols}, 'v2' AS schema FROM session_v2"
    if schema.mode == "v1":
        return v1
    if schema.mode == "v2":
        return v2
    return f"{v2} UNION ALL {v1} WHERE id NOT IN (SELECT id FROM session_v2)"


def _content_size_joins(conn: sqlite3.Connection, schema: SchemaInfo) -> tuple[str, str]:
    """Return (LEFT JOIN clauses, size expression) for per-session content bytes of `s`.

    v1 content is the part table; v2 content is the session_message rows plus any
    blobs the session references (blobs are shared by hash, so a blob referenced
    by several sessions counts toward each).
    """
    joins: list[str] = []
    terms: list[str] = []
    if schema.reads_v1:
        joins.append("""
        LEFT JOIN (
            SELECT session_id, SUM(LENGTH(data)) AS b FROM part GROUP BY session_id
        ) cv1 ON cv1.session_id = s.id AND s.schema = 'v1'""")
        terms.append("COALESCE(cv1.b, 0)")
    if schema.reads_v2:
        joins.append("""
        LEFT JOIN (
            SELECT session_id, SUM(LENGTH(data)) AS b FROM session_message GROUP BY session_id
        ) cv2 ON cv2.session_id = s.id AND s.schema = 'v2'""")
        terms.append("COALESCE(cv2.b, 0)")
        if _table_exists(conn, "blob_ref") and _table_exists(conn, "blob"):
            joins.append("""
        LEFT JOIN (
            SELECT r.session_id, SUM(b.bytes) AS b
            FROM (SELECT DISTINCT session_id, hash FROM blob_ref) r
            JOIN blob b ON b.hash = r.hash
            GROUP BY r.session_id
        ) bv2 ON bv2.session_id = s.id AND s.schema = 'v2'""")
            terms.append("COALESCE(bv2.b, 0)")
    return "".join(joins), " + ".join(terms) or "0"


def _event_size_join(conn: sqlite3.Connection) -> tuple[str, str]:
    """Return (LEFT JOIN clause, size expression) for per-session event-log bytes of `s`."""
    if not event_table_exists(conn):
        return "", "0"
    # event rows are keyed by aggregate_id, which holds the session id.
    join = """
        LEFT JOIN (
            SELECT aggregate_id, SUM(LENGTH(data)) AS event_bytes
            FROM event
            GROUP BY aggregate_id
        ) es ON es.aggregate_id = s.id"""
    return join, "COALESCE(es.event_bytes, 0)"


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


def _placeholders(ids: list[str]) -> str:
    return ",".join("?" for _ in ids)


def get_session_count(conn: sqlite3.Connection) -> tuple[int, int]:
    """Return (root_count, subagent_count)."""
    row = conn.execute(f"""
        SELECT
            SUM(CASE WHEN parent_id IS NULL THEN 1 ELSE 0 END),
            SUM(CASE WHEN parent_id IS NOT NULL THEN 1 ELSE 0 END)
        FROM ({_session_source(get_schema(conn))})
    """).fetchone()
    return (row[0] or 0, row[1] or 0)


V2_ASSISTANT_META = "assistant-meta"


def _content_type_stats(conn: sqlite3.Connection, by_parent: bool) -> dict[bool, dict[str, list[int]]]:
    """Content bytes grouped by type, optionally split by subagent (True) vs root (False).

    v1 groups part rows by ``$.type``. v2 has no part table: non-assistant
    session_message rows are grouped by their ``type`` (user, synthetic,
    system, compaction, ...), and assistant rows are broken down by the type of
    each ``content`` item (text, reasoning, tool); whatever an assistant row
    holds outside its content array is reported as ``assistant-meta``.
    Without ``by_parent`` everything lands under the False key.
    """
    schema = get_schema(conn)
    out: dict[bool, dict[str, list[int]]] = {False: {}, True: {}}

    def add(sub: object, name: str | None, count: int, size: int | None) -> None:
        slot = out[bool(sub)].setdefault(name or "unknown", [0, 0])
        slot[0] += count
        slot[1] += size or 0

    if schema.reads_v1:
        sub_col = "s.parent_id IS NOT NULL" if by_parent else "0"
        join = "JOIN session s ON s.id = p.session_id" if by_parent else ""
        # mixed: v1 sessions already copied into session_v2 are counted from v2
        where = "WHERE p.session_id NOT IN (SELECT id FROM session_v2)" if schema.mode == "mixed" else ""
        for r in conn.execute(f"""
            SELECT {sub_col} AS sub, json_extract(p.data, '$.type') AS t, COUNT(*), SUM(LENGTH(p.data))
            FROM part p {join} {where}
            GROUP BY sub, t
        """):
            add(r[0], r[1], r[2], r[3])

    if schema.reads_v2:
        sub_col = "s.parent_id IS NOT NULL" if by_parent else "0"
        join = "JOIN session_v2 s ON s.id = m.session_id" if by_parent else ""
        assistant: dict[bool, list[int]] = {}
        for r in conn.execute(f"""
            SELECT {sub_col} AS sub, m.type AS t, COUNT(*), SUM(LENGTH(m.data))
            FROM session_message m {join}
            GROUP BY sub, t
        """):
            if r[1] == "assistant":
                assistant[bool(r[0])] = [r[2], r[3] or 0]
            else:
                add(r[0], r[1], r[2], r[3])
        content_bytes: dict[bool, int] = {}
        for r in conn.execute(f"""
            SELECT {sub_col} AS sub, json_extract(j.value, '$.type') AS t, COUNT(*), SUM(LENGTH(j.value))
            FROM session_message m {join}, json_each(m.data, '$.content') j
            WHERE m.type = 'assistant'
            GROUP BY sub, t
        """):
            add(r[0], r[1], r[2], r[3])
            content_bytes[bool(r[0])] = content_bytes.get(bool(r[0]), 0) + (r[3] or 0)
        for sub, (count, size) in assistant.items():
            add(sub, V2_ASSISTANT_META, count, max(size - content_bytes.get(sub, 0), 0))

    return out


def _to_stats(group: dict[str, list[int]]) -> list[PartTypeStats]:
    stats = [PartTypeStats(name, c, b) for name, (c, b) in group.items()]
    return sorted(stats, key=lambda s: s.size_bytes, reverse=True)


def get_part_type_stats(conn: sqlite3.Connection) -> list[PartTypeStats]:
    """Content storage by type (v1 part types; v2 message/content-item types)."""
    return _to_stats(_content_type_stats(conn, by_parent=False)[False])


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
    """Return (row_count, total_bytes) of v1 message-table metadata for visible v1 sessions.

    v2 message rows ARE the content and are already covered by
    :func:`get_part_type_stats`, so they are not counted here.
    """
    schema = get_schema(conn)
    if not schema.reads_v1:
        return (0, 0)
    where = "WHERE session_id NOT IN (SELECT id FROM session_v2)" if schema.mode == "mixed" else ""
    row = conn.execute(f"SELECT COUNT(*), SUM(LENGTH(data)) FROM message {where}").fetchone()
    return (row[0] or 0, row[1] or 0)


def get_blob_total(conn: sqlite3.Connection) -> tuple[int, int]:
    """Return (blob_count, total_bytes) of v2's content-addressed attachment store."""
    if not _table_exists(conn, "blob"):
        return (0, 0)
    row = conn.execute("SELECT COUNT(*), SUM(bytes) FROM blob").fetchone()
    return (row[0] or 0, row[1] or 0)


def get_legacy_stats(conn: sqlite3.Connection) -> dict[str, int] | None:
    """Stats for stale v1 tables left behind after the v1->v2 migration, else None."""
    schema = get_schema(conn)
    if not schema.legacy_stale:
        return None
    sessions = conn.execute("SELECT COUNT(*) FROM session").fetchone()[0]
    msg = conn.execute("SELECT COUNT(*), SUM(LENGTH(data)) FROM message").fetchone()
    part = conn.execute("SELECT COUNT(*), SUM(LENGTH(data)) FROM part").fetchone()
    return {
        "session_count": sessions or 0,
        "message_count": msg[0] or 0,
        "part_count": part[0] or 0,
        "total_bytes": (msg[1] or 0) + (part[1] or 0),
    }


# Only session aggregates can be orphaned; event_sequence also tracks non-session
# aggregates (e.g. "global", project ids) that must never be deleted.
_SESSION_AGGREGATE = "aggregate_id LIKE 'ses\\_%' ESCAPE '\\'"


def _orphan_event_where(conn: sqlite3.Connection) -> str:
    return f"{_SESSION_AGGREGATE} AND aggregate_id NOT IN (SELECT id FROM ({_session_source(get_schema(conn))}))"


def get_orphan_event_stats(conn: sqlite3.Connection) -> tuple[int, int]:
    """Return (row_count, total_bytes) for events whose session no longer exists.

    OpenCode's `event` table has no foreign key to `session` (only to
    `event_sequence`), so deleting a session leaves its events behind. These
    orphans are always safe to remove.
    """
    if not event_table_exists(conn):
        return (0, 0)
    row = conn.execute(f"SELECT COUNT(*), SUM(LENGTH(data)) FROM event WHERE {_orphan_event_where(conn)}").fetchone()
    return (row[0] or 0, row[1] or 0)


def get_age_distribution(conn: sqlite3.Connection, now_ms: int) -> dict[str, int]:
    """Return session counts bucketed by age."""
    src = _session_source(get_schema(conn))
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
            f"SELECT COUNT(*) FROM ({src}) WHERE time_created < ? AND time_created >= ?",
            (prev_cutoff, cutoff),
        ).fetchone()[0]
        result[label] = count
        prev_cutoff = cutoff

    older = conn.execute(
        f"SELECT COUNT(*) FROM ({src}) WHERE time_created < ?",
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

    schema = get_schema(conn)
    content_joins, content_expr = _content_size_joins(conn, schema)
    event_join, event_bytes_expr = _event_size_join(conn)
    # v2 also stores synthetic/system/compaction/idle/switch rows; only user and
    # assistant rows are counted as messages.
    count_joins: list[str] = []
    count_terms: list[str] = []
    if schema.reads_v1:
        count_joins.append("""
        LEFT JOIN (
            SELECT session_id, COUNT(*) AS n FROM message GROUP BY session_id
        ) mc1 ON mc1.session_id = s.id AND s.schema = 'v1'""")
        count_terms.append("COALESCE(mc1.n, 0)")
    if schema.reads_v2:
        count_joins.append("""
        LEFT JOIN (
            SELECT session_id, COUNT(*) AS n FROM session_message
            WHERE type IN ('user', 'assistant') GROUP BY session_id
        ) mc2 ON mc2.session_id = s.id AND s.schema = 'v2'""")
        count_terms.append("COALESCE(mc2.n, 0)")

    rows = conn.execute(
        f"""
        SELECT
            s.id,
            s.parent_id,
            s.directory,
            s.title,
            s.time_created,
            s.time_updated,
            s.schema,
            {content_expr} AS content_bytes,
            {event_bytes_expr} AS event_bytes,
            {content_expr} + {event_bytes_expr} AS size_bytes,
            {" + ".join(count_terms)} AS message_count
        FROM ({_session_source(schema)}) s
        {content_joins}
        {event_join}
        {"".join(count_joins)}
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
            content_bytes=r["content_bytes"],
            event_bytes=r["event_bytes"],
            message_count=r["message_count"],
            schema=r["schema"],
        )
        for r in rows
    ]


def get_part_type_stats_by_session_type(
    conn: sqlite3.Connection,
) -> tuple[list[PartTypeStats], list[PartTypeStats]]:
    """Return (root_stats, subagent_stats)."""
    stats = _content_type_stats(conn, by_parent=True)
    return _to_stats(stats[False]), _to_stats(stats[True])


def get_growth_rate(conn: sqlite3.Connection) -> float | None:
    """Estimate MB per active day of usage."""
    schema = get_schema(conn)
    content_joins, content_expr = _content_size_joins(conn, schema)
    row = conn.execute(f"""
        SELECT
            COUNT(DISTINCT date(s.time_created / 1000, 'unixepoch')) AS active_days,
            SUM({content_expr}) AS total_bytes
        FROM ({_session_source(schema)}) s
        {content_joins}
    """).fetchone()
    if not row or not row["active_days"] or row["active_days"] < 1:
        return None
    active_days: int = row["active_days"]
    total_bytes = int(row["total_bytes"] or 0)
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
    schema = get_schema(conn)
    src = _session_source(schema)
    conditions = []
    params: list[str | int] = []

    if session_ids:
        conditions.append(f"s.id IN ({_placeholders(session_ids)})")
        params.extend(session_ids)

    if subagents_only:
        conditions.append("s.parent_id IS NOT NULL")

    if older_than_ms is not None and now_ms is not None:
        cutoff = now_ms - older_than_ms
        conditions.append("s.time_created < ?")
        params.append(cutoff)

    if larger_than_bytes is not None:
        content_joins, content_expr = _content_size_joins(conn, schema)
        event_join, event_expr = _event_size_join(conn)
        conditions.append(f"{content_expr} + {event_expr} >= ?")
        params.append(larger_than_bytes)
        joins = content_joins + event_join
    else:
        joins = ""

    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    query = f"SELECT s.id FROM ({src}) s {joins} {where}"

    ids = {r[0] for r in conn.execute(query, params)}

    if keep_latest is not None:
        if not conditions:
            # keep_latest alone: delete everything except the latest N
            latest_ids = {
                r[0]
                for r in conn.execute(
                    f"SELECT id FROM ({src}) ORDER BY time_created DESC LIMIT ?",
                    (keep_latest,),
                )
            }
            ids -= latest_ids
        elif ids:
            # With other filters: compute latest N from the already-filtered set
            id_list = list(ids)
            latest_ids = {
                r[0]
                for r in conn.execute(
                    f"SELECT id FROM ({src}) WHERE id IN ({_placeholders(id_list)}) ORDER BY time_created DESC LIMIT ?",
                    [*id_list, keep_latest],
                )
            }
            ids -= latest_ids

    return sorted(ids)


def get_purge_summary(conn: sqlite3.Connection, session_ids: list[str]) -> dict[str, int]:
    """Return summary stats for sessions about to be purged.

    Counts every row the purge removes, from whichever table set holds it
    (including stale v1 copies of v2 sessions).
    """
    summary = {
        "session_count": len(session_ids),
        "part_count": 0,
        "message_count": 0,
        "part_bytes": 0,
        "event_count": 0,
        "event_bytes": 0,
        "blob_bytes": 0,
        "total_bytes": 0,
    }
    if not session_ids:
        return summary
    ph = _placeholders(session_ids)
    tables = _purge_tables(conn)
    if "part" in tables:
        row = conn.execute(
            f"SELECT COUNT(*), SUM(LENGTH(data)) FROM part WHERE session_id IN ({ph})", session_ids
        ).fetchone()
        summary["part_count"] = row[0] or 0
        summary["part_bytes"] = row[1] or 0
    if "message" in tables:
        summary["message_count"] += conn.execute(
            f"SELECT COUNT(*) FROM message WHERE session_id IN ({ph})", session_ids
        ).fetchone()[0]
    if "session_message" in tables:
        # v2 message rows hold the content itself, so their bytes count as content.
        row = conn.execute(
            f"SELECT COUNT(*), SUM(LENGTH(data)) FROM session_message WHERE session_id IN ({ph})", session_ids
        ).fetchone()
        summary["message_count"] += row[0] or 0
        summary["part_bytes"] += row[1] or 0
    if "blob_ref" in tables and _table_exists(conn, "blob"):
        # only blobs no other session still references are freed
        summary["blob_bytes"] = (
            conn.execute(
                f"""SELECT SUM(b.bytes) FROM blob b
                    WHERE b.hash IN (SELECT hash FROM blob_ref WHERE session_id IN ({ph}))
                    AND NOT EXISTS (
                        SELECT 1 FROM blob_ref r WHERE r.hash = b.hash AND r.session_id NOT IN ({ph})
                    )""",
                [*session_ids, *session_ids],
            ).fetchone()[0]
            or 0
        )
    shared_ids = get_shared_purge_ids(conn, session_ids)
    if "event" in tables and shared_ids:
        row = conn.execute(
            f"SELECT COUNT(*), SUM(LENGTH(data)) FROM event WHERE aggregate_id IN ({_placeholders(shared_ids)})",
            shared_ids,
        ).fetchone()
        summary["event_count"] = row[0] or 0
        summary["event_bytes"] = row[1] or 0
    summary["total_bytes"] = summary["part_bytes"] + summary["event_bytes"] + summary["blob_bytes"]
    return summary


def _v1_scope(schema: SchemaInfo, scope: str) -> str:
    """Mixed mode with no explicit ids: only touch v1 sessions not yet copied into v2."""
    if not scope and schema.mode == "mixed":
        return "AND session_id NOT IN (SELECT id FROM session_v2)"
    return scope


def _content_item_summary(conn: sqlite3.Connection, item_type: str, session_ids: list[str] | None) -> dict[str, int]:
    """Count/bytes of v1 parts or v2 assistant content items of the given type."""
    if session_ids is not None and not session_ids:
        return {"part_count": 0, "total_bytes": 0}
    schema = get_schema(conn)
    count = 0
    size = 0
    scope = f"AND session_id IN ({_placeholders(session_ids)})" if session_ids is not None else ""
    scope_m = scope.replace("session_id", "m.session_id")
    ids = session_ids or []
    if schema.reads_v1:
        row = conn.execute(
            "SELECT COUNT(*), SUM(LENGTH(data)) FROM part"
            f" WHERE json_extract(data, '$.type') = ? {_v1_scope(schema, scope)}",
            [item_type, *ids],
        ).fetchone()
        count += row[0] or 0
        size += row[1] or 0
    if schema.reads_v2:
        row = conn.execute(
            f"""SELECT COUNT(*), SUM(LENGTH(j.value))
                FROM session_message m, json_each(m.data, '$.content') j
                WHERE m.type = 'assistant' AND json_extract(j.value, '$.type') = ? {scope_m}""",
            [item_type, *ids],
        ).fetchone()
        count += row[0] or 0
        size += row[1] or 0
    return {"part_count": count, "total_bytes": size}


def _dumps_sqlite_text(data: object) -> str:
    r"""Serialize JSON to a string sqlite3 can always bind.

    Stored JSON can contain lone-surrogate escapes (JS writes ``\udXXX`` for
    invalid UTF-8); ``json.loads`` materializes them, and a compact
    ``ensure_ascii=False`` re-dump keeps them as raw characters that the sqlite3
    driver cannot UTF-8-encode. Fall back to full ASCII escaping, which stores
    them as escape sequences again (valid JSON, round-trips via ``json.loads``).
    """
    text = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    try:
        text.encode("utf-8")
    except UnicodeEncodeError:
        text = json.dumps(data, ensure_ascii=True, separators=(",", ":"))
    return text


def _strip_content_items(conn: sqlite3.Connection, item_type: str, session_ids: list[str] | None) -> int:
    """Remove v1 parts / v2 assistant content items of the given type. Return count removed."""
    if session_ids is not None and not session_ids:
        return 0
    schema = get_schema(conn)
    ids = session_ids or []
    scope = f"AND session_id IN ({_placeholders(ids)})" if session_ids is not None else ""
    removed = 0
    if schema.reads_v1:
        cur = conn.execute(
            f"DELETE FROM part WHERE json_extract(data, '$.type') = ? {_v1_scope(schema, scope)}", [item_type, *ids]
        )
        removed += cur.rowcount
    if schema.reads_v2:
        # v2 keeps parts inline in the assistant row's `content` array, so rows are
        # rewritten rather than deleted. Select ids first: the data can be huge.
        msg_ids = [
            r[0]
            for r in conn.execute(
                f"""SELECT id FROM session_message
                    WHERE type = 'assistant' {scope}
                    AND EXISTS (
                        SELECT 1 FROM json_each(data, '$.content') j WHERE json_extract(j.value, '$.type') = ?
                    )""",
                [*ids, item_type],
            )
        ]
        for mid in msg_ids:
            row = conn.execute("SELECT data FROM session_message WHERE id = ?", (mid,)).fetchone()
            data = json.loads(row[0])
            content = data.get("content")
            if not isinstance(content, list):
                continue
            kept = [c for c in content if not (isinstance(c, dict) and c.get("type") == item_type)]
            removed += len(content) - len(kept)
            data["content"] = kept
            conn.execute(
                "UPDATE session_message SET data = ? WHERE id = ?",
                (_dumps_sqlite_text(data), mid),
            )
    conn.commit()
    return removed


def get_reasoning_summary(conn: sqlite3.Connection, session_ids: list[str] | None) -> dict[str, int]:
    """Return summary stats for reasoning parts in given sessions (or all if None)."""
    return _content_item_summary(conn, "reasoning", session_ids)


def get_tools_summary(conn: sqlite3.Connection, session_ids: list[str] | None) -> dict[str, int]:
    """Return summary stats for tool parts in given sessions (or all if None)."""
    return _content_item_summary(conn, "tool", session_ids)


def get_event_summary(conn: sqlite3.Connection, session_ids: list[str] | None) -> dict[str, int]:
    """Return event-log stats for given sessions (or the whole table if None)."""
    if not event_table_exists(conn):
        return {"event_count": 0, "total_bytes": 0}
    if session_ids is not None:
        if not session_ids:
            return {"event_count": 0, "total_bytes": 0}
        row = conn.execute(
            f"SELECT COUNT(*), SUM(LENGTH(data)) FROM event WHERE aggregate_id IN ({_placeholders(session_ids)})",
            session_ids,
        ).fetchone()
    else:
        row = conn.execute("SELECT COUNT(*), SUM(LENGTH(data)) FROM event").fetchone()
    return {"event_count": row[0] or 0, "total_bytes": row[1] or 0}


# Child tables keyed by session id, v1 and v2. Missing tables are skipped.
_SESSION_CHILD_TABLES = (
    ("part", "session_id"),
    ("message", "session_id"),
    ("todo", "session_id"),
    ("session_share", "session_id"),
    ("session_message", "session_id"),
    ("session_inbox", "session_id"),
    ("instruction_entry", "session_id"),
    ("instruction_state", "session_id"),
    # event/event_sequence key on aggregate_id with no FK to session, so a session
    # delete never cascades — remove explicitly or they orphan and waste space.
    ("event", "aggregate_id"),
    ("event_sequence", "aggregate_id"),
)


_V1_SESSION_TABLES = frozenset({"part", "message", "todo", "session_share", "session"})


def _v1_only(schema: SchemaInfo) -> bool:
    """v1 was forced (``--legacy`` / ``--schema v1``) on a DB that also has v2 tables.

    The v1 rows are then often stale copies of live v2 sessions with the same id,
    so a purge must leave every v2 table (and anything shared by id) alone.
    """
    return schema.mode == "v1" and schema.has_v2


def get_shared_purge_ids(conn: sqlite3.Connection, session_ids: list[str]) -> list[str]:
    """The subset of ``session_ids`` whose id-keyed shared data (event log, session_diff
    files) a purge removes: all of them, except in v1-only mode where ids still alive
    in session_v2 keep theirs."""
    if not session_ids or not _v1_only(get_schema(conn)):
        return session_ids
    live = {
        r[0]
        for r in conn.execute(
            f"SELECT id FROM session_v2 WHERE id IN ({_placeholders(session_ids)})",
            session_ids,
        )
    }
    return [sid for sid in session_ids if sid not in live]


def _purge_tables(conn: sqlite3.Connection) -> set[str]:
    """Existing tables a session purge clears in the current schema mode."""
    names = {t for t, _ in _SESSION_CHILD_TABLES} | {"session", "session_v2", "blob_ref"}
    if _v1_only(get_schema(conn)):
        # id-keyed event rows are still cleared, but only for ids not live in v2
        names &= _V1_SESSION_TABLES | {"event", "event_sequence"}
    return {t for t in names if _table_exists(conn, t)}


def purge_sessions(conn: sqlite3.Connection, session_ids: list[str]) -> PurgeFilesResult:
    """Delete sessions and all their rows (v1 and v2 tables). Also removes session_diff files.

    FK cascades are not relied on (SQLite leaves them off per connection), so every
    child table is cleared explicitly, including stale v1 copies of v2 sessions.
    In v1-only mode (``--legacy``) only the v1 tables are touched, and the event
    log / diff files only for ids that are not live v2 sessions.
    """
    if not session_ids:
        return PurgeFilesResult()
    tables = _purge_tables(conn)
    shared_ids = get_shared_purge_ids(conn, session_ids)
    ph = _placeholders(session_ids)
    for table, column in _SESSION_CHILD_TABLES:
        if table not in tables:
            continue
        ids = shared_ids if column == "aggregate_id" else session_ids
        if ids:
            conn.execute(f"DELETE FROM {table} WHERE {column} IN ({_placeholders(ids)})", ids)
    if "blob_ref" in tables:
        # Release this session's blob refs, then drop the blobs nobody references anymore
        # (mirrors opencode's SessionBlob.releaseSession).
        released = conn.execute(f"SELECT DISTINCT hash FROM blob_ref WHERE session_id IN ({ph})", session_ids)
        hashes = [r[0] for r in released]
        conn.execute(f"DELETE FROM blob_ref WHERE session_id IN ({ph})", session_ids)
        for i in range(0, len(hashes), 500):
            batch = hashes[i : i + 500]
            conn.execute(
                f"""DELETE FROM blob WHERE hash IN ({_placeholders(batch)})
                    AND NOT EXISTS (SELECT 1 FROM blob_ref r WHERE r.hash = blob.hash)""",
                batch,
            )
    for table in ("session_v2", "session"):
        if table in tables:
            conn.execute(f"DELETE FROM {table} WHERE id IN ({ph})", session_ids)
    conn.commit()

    # Best-effort cleanup of session_diff files
    return purge_session_diffs(shared_ids)


def strip_reasoning(conn: sqlite3.Connection, session_ids: list[str] | None = None) -> int:
    """Delete reasoning parts from given sessions (or all). Return count deleted."""
    return _strip_content_items(conn, "reasoning", session_ids)


def strip_tools(conn: sqlite3.Connection, session_ids: list[str] | None = None) -> int:
    """Delete tool parts from given sessions (or all). Return count deleted."""
    return _strip_content_items(conn, "tool", session_ids)


def strip_events(conn: sqlite3.Connection, session_ids: list[str] | None = None) -> int:
    """Delete event-log rows for given sessions (or all). Return count deleted.

    Leaves ``event_sequence`` intact so a live session keeps its seq counter and
    OpenCode simply appends from where it left off. The message/part (v1) and
    session_message (v2) tables hold the canonical state, so dropping the event
    log does not lose history; opencode 2.x does not even persist events by default.
    """
    if not event_table_exists(conn):
        return 0
    if session_ids is not None:
        if not session_ids:
            return 0
        cur = conn.execute(
            f"DELETE FROM event WHERE aggregate_id IN ({_placeholders(session_ids)})",
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
    where = _orphan_event_where(conn)
    conn.execute(f"DELETE FROM event WHERE {where}")
    if _table_exists(conn, "event_sequence"):
        conn.execute(f"DELETE FROM event_sequence WHERE {where}")
    conn.commit()
    return {"event_count": count, "bytes_freed": freed}


def purge_legacy(conn: sqlite3.Connection) -> dict[str, int]:
    """Empty the stale v1 tables (session/message/part/todo/session_share).

    Only allowed once opencode's v1->v2 migration has completed: from then on
    opencode 2.x reads sessions exclusively from session_v2/session_message.
    The tables themselves are kept (emptied, not dropped) so opencode's own
    schema migrations still find them.
    """
    stats = get_legacy_stats(conn)
    if stats is None:
        raise SchemaError("no stale v1 data: the v1->v2 migration has not completed (or there are no v1 tables)")
    for table in ("part", "message", "todo", "session_share", "session"):
        if _table_exists(conn, table):
            conn.execute(f"DELETE FROM {table}")
    conn.commit()
    return stats


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


def _session_diff_paths(conn: sqlite3.Connection, session_ids: list[str] | None) -> list[Path]:
    """Existing session_diff files for the given sessions (or every file if None).

    Ids still live in session_v2 are skipped in v1-only mode (see get_shared_purge_ids).
    """
    diff_dir = get_storage_dir() / "storage" / "session_diff"
    if not diff_dir.is_dir():
        return []
    if session_ids is None:
        return list(diff_dir.glob("*.json"))
    paths = (diff_dir / f"{sid}.json" for sid in get_shared_purge_ids(conn, session_ids))
    return [p for p in paths if p.exists()]


def get_diff_summary(conn: sqlite3.Connection, session_ids: list[str] | None = None) -> dict[str, int]:
    """Count and size of session_diff files for given sessions (or all)."""
    paths = _session_diff_paths(conn, session_ids)
    return {"file_count": len(paths), "total_bytes": sum(p.stat().st_size for p in paths)}


def strip_diffs(conn: sqlite3.Connection, session_ids: list[str] | None = None) -> PurgeFilesResult:
    """Delete session_diff files for given sessions (or all), keeping the sessions. Best-effort.

    These hold the per-session file diffs opencode shows in its review panel; the
    conversation itself lives in the DB, so dropping them only loses that view.
    """
    result = PurgeFilesResult()
    for path in _session_diff_paths(conn, session_ids):
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

    db_ids = {r[0] for r in conn.execute(f"SELECT id FROM ({_session_source(get_schema(conn))})")}
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
