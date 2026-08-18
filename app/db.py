"""SQLite connection and schema management.

The service deliberately opens a new connection for every public database
operation.  This mirrors the independent connections used by real worker
processes and keeps transaction ownership explicit.
"""

import os
import sqlite3
from pathlib import Path
from typing import Optional, Union


PathLike = Union[str, os.PathLike]
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DATABASE_PATH = PROJECT_ROOT / "data" / "taskboard.db"


SCHEMA = """
CREATE TABLE IF NOT EXISTS groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    override_parameters TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    group_id INTEGER REFERENCES groups(id) ON DELETE RESTRICT,
    base_parameters TEXT NOT NULL DEFAULT '{}',
    group_parameters_snapshot TEXT,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'claimed', 'running', 'done', 'failed')),
    claimed_by TEXT,
    claim_token TEXT,
    claimed_at TEXT,
    lease_expires_at TEXT,
    started_at TEXT,
    completed_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK (
        (status = 'pending' AND claimed_by IS NULL AND claim_token IS NULL)
        OR (status <> 'pending' AND claimed_by IS NOT NULL AND claim_token IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS steps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    sequence INTEGER NOT NULL CHECK (sequence > 0),
    name TEXT NOT NULL,
    override_parameters TEXT NOT NULL DEFAULT '{}',
    resolved_parameters TEXT,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'running', 'done', 'failed')),
    started_at TEXT,
    completed_at TEXT,
    UNIQUE (task_id, sequence)
);

CREATE TABLE IF NOT EXISTS execution_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id INTEGER NOT NULL,
    step_sequence INTEGER NOT NULL,
    success INTEGER NOT NULL CHECK (success IN (0, 1)),
    completed_at TEXT NOT NULL,
    UNIQUE (task_id, step_sequence),
    FOREIGN KEY (task_id, step_sequence)
        REFERENCES steps(task_id, sequence) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_tasks_pending
    ON tasks(created_at, id) WHERE status = 'pending';

CREATE TABLE IF NOT EXISTS operation_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    at TEXT NOT NULL,
    level TEXT NOT NULL DEFAULT 'info' CHECK (level IN ('info', 'warning')),
    event TEXT NOT NULL,
    task_id INTEGER,
    step_sequence INTEGER,
    worker_id TEXT,
    message TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_operation_logs_id ON operation_logs(id);
"""


def database_path(path: Optional[PathLike] = None) -> str:
    """Resolve an explicit path, then the environment, then the local default."""

    raw_path = (
        os.fspath(path)
        if path is not None
        else os.environ.get("TASKBOARD_DB_PATH", os.fspath(DEFAULT_DATABASE_PATH))
    )
    if raw_path == ":memory:":
        raise ValueError(
            "':memory:' cannot be shared by this service's independent connections; "
            "use a temporary database file"
        )
    return os.fspath(Path(raw_path).expanduser().resolve())


def connect(path: Optional[PathLike] = None) -> sqlite3.Connection:
    """Return a fully configured, independent SQLite connection."""

    resolved = database_path(path)
    Path(resolved).parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(
        resolved,
        timeout=30.0,
        isolation_level=None,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout = 30000")
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA journal_mode = WAL")
    return connection


def initialize_database(path: Optional[PathLike] = None) -> str:
    """Create the schema if necessary and return the resolved database path."""

    resolved = database_path(path)
    connection = connect(resolved)
    try:
        connection.executescript(SCHEMA)
        # Single additive migration so databases created before the lease
        # feature keep working; a full migration framework stays out of scope.
        columns = {
            row["name"]
            for row in connection.execute("PRAGMA table_info(tasks)").fetchall()
        }
        if "lease_expires_at" not in columns:
            connection.execute("ALTER TABLE tasks ADD COLUMN lease_expires_at TEXT")
        connection.execute("PRAGMA optimize")
    finally:
        connection.close()
    return resolved
