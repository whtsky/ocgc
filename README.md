# ocgc: OpenCode Garbage Collector

Analyze and reclaim storage used by [OpenCode](https://github.com/anomalyco/opencode) sessions, diffs, and snapshots.

OpenCode stores sessions in a SQLite file that grows without limit and has [no built-in cleanup](https://github.com/anomalyco/opencode/issues/4980). It also writes session diffs and git snapshots to disk. `ocgc` shows where the space goes and reclaims it.

Most of that space is usually **not** what you'd expect. The append-only `event` table — full `message.updated` snapshots written on every streaming update — is frequently the single largest consumer, yet it's easy to miss because it isn't part of any session's visible part data. `ocgc` measures it, folds it into each session's real size, and can strip it or clean up the orphaned events that deleted sessions leave behind.

Both OpenCode database generations are supported and detected automatically: **v1** (opencode 1.x — `session` / `message` / `part`) and **v2** (opencode 2.x — `session_v2` / `session_message`, plus `session_inbox`, `instruction_*` and the `blob` / `blob_ref` attachment store). See [OpenCode 1.x and 2.x databases](#opencode-1x-and-2x-databases).

## Screenshots

`ocgc status`

![status](screenshots/status.png)

`ocgc analyze`

![analyze](screenshots/analyze.png)

`ocgc purge --clean-orphans --clean-snapshots --older-than 14d --dry-run`

![purge](screenshots/purge.png)

## Quick start

Copy this to your AI agent:

```
Read https://raw.githubusercontent.com/whtsky/ocgc/refs/heads/main/README.md and help me garbage collect OpenCode storage.
```

## Install

```bash
uv tool install ocgc
```

Or with pip:

```bash
pip install ocgc
```

## Usage

```bash
# Dashboard: DB size, session breakdown, storage by part type
ocgc status

# List sessions sorted by size
ocgc sessions --sort size --limit 20

# Deep analysis: top sessions, growth rate, root vs subagent
ocgc analyze

# Preview what a purge would do
ocgc purge --older-than 14d --dry-run

# Delete subagent sessions older than a week
ocgc purge --subagents --older-than 7d

# Strip reasoning parts only (keeps sessions)
ocgc purge --strip-reasoning

# Strip tool-call parts only (keeps sessions, frees space)
ocgc purge --strip-tools

# Strip the event log only — often the single biggest win
# (message.updated snapshots, redundant with the message/part tables)
ocgc purge --strip-events

# Strip events from just the old sessions
ocgc purge --strip-events --older-than 30d

# Delete session diff files (storage/session_diff/*.json) only — keeps sessions,
# frees disk immediately (no vacuum needed)
ocgc purge --strip-diffs
ocgc purge --strip-diffs --older-than 30d

# Other purge options
ocgc purge --larger-than 50M
ocgc purge --keep-latest 50
ocgc purge --session ses_abc123

# Clean up orphan session diff files (no matching session in DB)
ocgc purge --clean-orphans

# Clean up orphan event rows left behind by previously-deleted sessions
ocgc purge --clean-orphan-events

# opencode 2.x: delete the stale v1 copies left behind by the v1->v2 migration
# (often most of the file — see below)
ocgc purge --clean-legacy --dry-run
ocgc purge --clean-legacy

# Restrict any purge/strip to the v1 (legacy) sessions only
ocgc purge --legacy --strip-events          # strip events of v1 sessions only
ocgc purge --legacy --strip-tools --older-than 30d
ocgc purge --legacy --subagents             # delete v1 subagent rows only

# Force the schema instead of auto-detecting it (global option)
ocgc --schema v1 sessions

# Delete all snapshot directories
ocgc purge --clean-snapshots

# Reclaim disk space after purging
ocgc vacuum
```

### Purge options

| Option | Effect |
| --- | --- |
| `--older-than DUR` | Sessions created more than `DUR` ago (`15m`, `1h`, `7d`, `2w`, `3mo`) |
| `--subagents` | Subagent sessions only (`parent_id IS NOT NULL`) |
| `--larger-than SIZE` | Sessions whose content + event data is at least `SIZE` (`500K`, `50M`, `1G`) |
| `--session ID` | A specific session (repeatable) |
| `--keep-latest N` | Keep the N most recent (matching) sessions, delete the rest |
| `--legacy` | Only v1 (opencode 1.x) sessions, read from and purged in the v1 tables only. On a migrated v2 DB these are the stale pre-migration copies; live `session_v2` data is never touched. Combines with every other filter and `--strip-*`; on its own it selects all v1 sessions |
| `--strip-reasoning` | Remove reasoning parts (v1 `part` rows / v2 assistant `content` items), keep sessions |
| `--strip-tools` | Remove tool-call parts (v1 `part` rows / v2 assistant `content` items), keep sessions |
| `--strip-events` | Remove event-log rows, keep sessions |
| `--strip-diffs` | Delete session diff files (`storage/session_diff/<id>.json`), keep sessions. With no filter, deletes every diff file (orphans included) |
| `--clean-orphans` | Delete session diff files with no matching session |
| `--clean-orphan-events` | Delete event rows whose session no longer exists |
| `--clean-snapshots` | Delete all snapshot directories |
| `--clean-legacy` | opencode 2.x only: empty the stale v1 `session` / `message` / `part` / `todo` / `session_share` tables once the v1->v2 migration has completed |
| `--dry-run`, `-n` | Show what would be deleted without doing it |
| `--force`, `-f` | Skip the confirmation prompt |

Multiple purge flags combine with AND. `--strip-reasoning`, `--strip-tools`, `--strip-events`, and `--strip-diffs` change the action from deleting sessions to removing just those rows (or diff files) (from matching sessions, or all sessions if no other filters are given), and can be combined. `--clean-orphans`, `--clean-orphan-events`, `--clean-snapshots`, and `--clean-legacy` run independently before any session purge. `--dry-run` previews without touching anything. `--force` skips the confirmation prompt.

Stripping the event log leaves `event_sequence` intact, so OpenCode keeps appending cleanly to sessions you continue using. Deleting a session (session purge) now also removes its event rows, which upstream ocgc left orphaned — run `--clean-orphan-events` once to sweep up any left over from earlier purges.

Orphan-event cleanup only considers session aggregates (`ses_…`): `event_sequence` also tracks non-session aggregates such as `global` and project ids, which are always kept.

## OpenCode 1.x and 2.x databases

opencode 2.x moved sessions to new tables and, on first start, migrates every v1 session across, recording `{"phase":"completed"}` under the `migration.v1-v2` key of the `kv` table. `ocgc` detects the layout the same way opencode does:

- **v1**: only v1 tables exist, so sessions come from `session` / `message` / `part`.
- **v2**: v2 tables exist and either the migration has completed or there are no v1 tables. Sessions come from `session_v2` / `session_message`.
- **mixed**: v2 tables exist, but the migration has not completed. `ocgc` shows every v2 session plus the v1 sessions not yet copied.

`ocgc status` shows the detected schema. Override it with the global `--schema auto|v1|v2` option or the `OCGC_SCHEMA` environment variable.

What changes with v2:

- **Message counts** include only `user` and `assistant` rows. Synthetic, system, compaction, idle and switch rows are not counted.
- **Session size** is the session's `session_message` rows, plus any blobs it references, plus its event-log rows.
- **Storage breakdown** in `status`/`analyze` groups by message type (`user`, `synthetic`, `system`, `compaction`, …). Assistant rows are split by content item (`tool`, `reasoning`, `text`). Whatever an assistant row holds outside its content array is reported as `assistant-meta`.
- **`--strip-reasoning` / `--strip-tools`** rewrite assistant rows and drop the matching `content` items, because v2 has no separate part rows.
- **Session purge** also clears `session_message`, `session_inbox`, `instruction_entry`, `instruction_state` and `blob_ref`. Blobs are deleted once no other session references them. The session's stale v1 copy is deleted too.
- **Stale v1 data.** After the migration, the old v1 tables stay in the file but opencode 2.x never reads them again, and sessions deleted in v2 still linger there. This is frequently the biggest consumer of space. `status` and `analyze` report it as "Stale v1 data". `ocgc purge --clean-legacy` empties those tables (it keeps the tables themselves, so opencode's own schema migrations keep working). It refuses while the migration is still incomplete. After cleaning, downgrading to opencode 1.x shows no history.
- **`--legacy` filter.** Use it to clean only part of the v1 data, e.g. strip just the events or tool output of old v1 sessions. On a v2 DB it only deletes event rows and diff files for ids that are no longer live v2 sessions; `--strip-events` combined with it strips events for every selected id; `--strip-diffs` combined with it still skips diff files of ids that are live v2 sessions.

## Filesystem storage

Beyond the SQLite database, OpenCode stores data on the filesystem:

- Session diffs (`~/.local/share/opencode/storage/session_diff/`): one JSON file per session containing full file contents. Cleaned automatically when sessions are purged. Use `--strip-diffs` to delete them while keeping the sessions (opencode only loses the per-session file-changes view; the conversation is untouched), or `--clean-orphans` to delete only those with no matching session.
- Snapshots (`~/.local/share/opencode/snapshot/`): git object packs for each project. Use `--clean-snapshots` to delete.
- Tool output (`~/.local/share/opencode/tool-output/`): small files from tool invocations. Shown in stats but no cleanup needed.

`ocgc status` and `ocgc analyze` show sizes for all three directories. `analyze` also reports orphan session diff files (files with no matching session in the DB).

## Safety

`status`, `sessions`, and `analyze` are read-only. `purge` asks for confirmation before deleting. `ocgc` warns if OpenCode is running.

## Custom database path

```bash
export OCGC_DB_PATH=/path/to/opencode.db
ocgc status
```

Resolution order:

1. `OCGC_DB_PATH`
2. `OPENCODE_DB` — the variable opencode itself uses. A relative value is resolved against the opencode data dir, as opencode does, e.g. `OPENCODE_DB=opencode-next.db` for a non-release channel.
3. `$XDG_DATA_HOME/opencode/opencode.db`, i.e. `~/.local/share/opencode/opencode.db`

## Environment variables

| Variable | Purpose |
| --- | --- |
| `OCGC_DB_PATH` | Path to the OpenCode database (highest priority) |
| `OPENCODE_DB` | opencode's own DB override; relative values resolve against the data dir |
| `XDG_DATA_HOME` | Base of the default data dir (`$XDG_DATA_HOME/opencode`) |
| `OCGC_SCHEMA` | Default for `--schema` (`auto`, `v1`, `v2`) |
| `OCGC_SKIP_RUNNING_CHECK` | Skip the "is opencode running?" check |

## License

MIT
