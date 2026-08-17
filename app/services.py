"""Domain operations for groups, tasks, workers, and execution logs."""

import copy
import json
import secrets
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Union

from .db import connect


DatabasePath = Union[str, Path]
JsonObject = Dict[str, Any]


class ServiceError(Exception):
    """Base class for errors that can be translated into an API response."""


class NotFoundError(ServiceError):
    pass


class ConflictError(ServiceError):
    pass


class ValidationError(ServiceError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def _json_dump(value: Mapping[str, Any]) -> str:
    return json.dumps(
        value, ensure_ascii=False, allow_nan=False, separators=(",", ":")
    )


def _json_load(value: Optional[str]) -> Optional[JsonObject]:
    if value is None:
        return None
    loaded = json.loads(value)
    if not isinstance(loaded, dict):
        raise ValueError("stored parameter data must be a JSON object")
    return loaded


def _require_object(value: Mapping[str, Any], label: str) -> JsonObject:
    if not isinstance(value, Mapping):
        raise ValidationError("{} must be a JSON object".format(label))
    # JSON round-tripping validates supported JSON types and creates a deep copy.
    try:
        copied = json.loads(
            json.dumps(dict(value), ensure_ascii=False, allow_nan=False)
        )
    except (TypeError, ValueError) as exc:
        raise ValidationError("{} must contain valid JSON values".format(label)) from exc
    return copied


def resolve_parameter_chain(
    base_parameters: Mapping[str, Any],
    group_parameters: Mapping[str, Any],
    step_overrides: Sequence[Mapping[str, Any]],
) -> List[JsonObject]:
    """Resolve and snapshot L1 -> L2 -> sticky L3 parameters.

    L2 values (including an empty string) are applied literally.  Each non-empty
    L3 value mutates the effective state for the current and following steps;
    an exact empty string at L3 is skipped and therefore keeps the currently
    effective value.  Every returned snapshot is an independent deep copy.
    """

    effective = _require_object(base_parameters, "base_parameters")
    effective.update(_require_object(group_parameters, "group_parameters"))
    snapshots: List[JsonObject] = []

    for index, raw_override in enumerate(step_overrides, start=1):
        override = _require_object(raw_override, "step {} overrides".format(index))
        for key, value in override.items():
            if isinstance(value, str) and value == "":
                continue
            effective[key] = copy.deepcopy(value)
        snapshots.append(copy.deepcopy(effective))

    return snapshots


def _group_from_row(row: sqlite3.Row) -> JsonObject:
    return {
        "id": row["id"],
        "name": row["name"],
        "overrides": _json_load(row["override_parameters"]),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _log_from_row(row: sqlite3.Row) -> JsonObject:
    return {
        "id": row["id"],
        "task_id": row["task_id"],
        "step_sequence": row["step_sequence"],
        "success": bool(row["success"]),
        "completed_at": row["completed_at"],
    }


def _task_from_connection(connection: sqlite3.Connection, task_id: int) -> JsonObject:
    task = connection.execute(
        """
        SELECT t.*, g.name AS group_name
        FROM tasks AS t
        LEFT JOIN groups AS g ON g.id = t.group_id
        WHERE t.id = ?
        """,
        (task_id,),
    ).fetchone()
    if task is None:
        raise NotFoundError("task {} was not found".format(task_id))

    step_rows = connection.execute(
        "SELECT * FROM steps WHERE task_id = ? ORDER BY sequence",
        (task_id,),
    ).fetchall()
    log_rows = connection.execute(
        "SELECT * FROM execution_logs WHERE task_id = ? ORDER BY step_sequence",
        (task_id,),
    ).fetchall()
    logs = [_log_from_row(row) for row in log_rows]
    log_by_sequence = {row["step_sequence"]: row for row in logs}

    steps = []
    current_step = None
    for row in step_rows:
        step = {
            "id": row["id"],
            "task_id": row["task_id"],
            "sequence": row["sequence"],
            "name": row["name"],
            "overrides": _json_load(row["override_parameters"]),
            "resolved_parameters": _json_load(row["resolved_parameters"]),
            "status": row["status"],
            "started_at": row["started_at"],
            "completed_at": row["completed_at"],
            "execution_log": log_by_sequence.get(row["sequence"]),
        }
        steps.append(step)
        if row["status"] == "running":
            current_step = step

    resolved_parameters = None
    if current_step is not None:
        resolved_parameters = current_step["resolved_parameters"]
    else:
        for step in reversed(steps):
            if step["resolved_parameters"] is not None:
                resolved_parameters = step["resolved_parameters"]
                break

    group_snapshot = _json_load(task["group_parameters_snapshot"])

    return {
        "id": task["id"],
        "name": task["name"],
        "group_id": task["group_id"],
        "group_name": task["group_name"],
        "base_parameters": _json_load(task["base_parameters"]),
        "group_parameters_snapshot": group_snapshot,
        "resolved_parameters": resolved_parameters,
        "status": task["status"],
        "claimed_by": task["claimed_by"],
        "claimed_at": task["claimed_at"],
        "started_at": task["started_at"],
        "completed_at": task["completed_at"],
        "created_at": task["created_at"],
        "updated_at": task["updated_at"],
        "steps": steps,
        "current_step": current_step,
        "execution_logs": logs,
    }


def create_group(
    db_path: DatabasePath, name: str, overrides: Mapping[str, Any]
) -> JsonObject:
    name = name.strip()
    if not name:
        raise ValidationError("group name must not be blank")
    parameters = _require_object(overrides, "overrides")
    now = utc_now()
    connection = connect(db_path)
    try:
        try:
            cursor = connection.execute(
                """
                INSERT INTO groups(name, override_parameters, created_at, updated_at)
                VALUES (?, ?, ?, ?)
                """,
                (name, _json_dump(parameters), now, now),
            )
        except sqlite3.IntegrityError as exc:
            raise ConflictError("a group named {!r} already exists".format(name)) from exc
        row = connection.execute(
            "SELECT * FROM groups WHERE id = ?", (cursor.lastrowid,)
        ).fetchone()
        return _group_from_row(row)
    finally:
        connection.close()


def list_groups(db_path: DatabasePath) -> List[JsonObject]:
    connection = connect(db_path)
    try:
        rows = connection.execute("SELECT * FROM groups ORDER BY id").fetchall()
        return [_group_from_row(row) for row in rows]
    finally:
        connection.close()


def update_group(
    db_path: DatabasePath,
    group_id: int,
    name: Optional[str] = None,
    overrides: Optional[Mapping[str, Any]] = None,
) -> JsonObject:
    if name is None and overrides is None:
        raise ValidationError("at least one group field must be supplied")
    if name is not None:
        name = name.strip()
        if not name:
            raise ValidationError("group name must not be blank")
    parameters = (
        _require_object(overrides, "overrides") if overrides is not None else None
    )
    connection = connect(db_path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        current = connection.execute(
            "SELECT * FROM groups WHERE id = ?", (group_id,)
        ).fetchone()
        if current is None:
            raise NotFoundError("group {} was not found".format(group_id))
        try:
            connection.execute(
                """
                UPDATE groups
                SET name = ?, override_parameters = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    name if name is not None else current["name"],
                    _json_dump(parameters)
                    if parameters is not None
                    else current["override_parameters"],
                    utc_now(),
                    group_id,
                ),
            )
        except sqlite3.IntegrityError as exc:
            raise ConflictError("a group named {!r} already exists".format(name)) from exc
        row = connection.execute(
            "SELECT * FROM groups WHERE id = ?", (group_id,)
        ).fetchone()
        connection.commit()
        return _group_from_row(row)
    except Exception:
        if connection.in_transaction:
            connection.rollback()
        raise
    finally:
        connection.close()


def create_task(
    db_path: DatabasePath,
    name: str,
    group_id: Optional[int],
    base_parameters: Mapping[str, Any],
    steps: Sequence[Mapping[str, Any]],
) -> JsonObject:
    name = name.strip()
    if not name:
        raise ValidationError("task name must not be blank")
    base = _require_object(base_parameters, "base_parameters")
    if not steps:
        raise ValidationError("a task must contain at least one step")

    normalized_steps = []
    for sequence, raw_step in enumerate(steps, start=1):
        if not isinstance(raw_step, Mapping):
            raise ValidationError("step {} must be an object".format(sequence))
        step_name = str(raw_step.get("name", "Step {}".format(sequence))).strip()
        if not step_name:
            raise ValidationError("step {} name must not be blank".format(sequence))
        raw_overrides = raw_step.get("overrides", {})
        normalized_steps.append(
            (step_name, _require_object(raw_overrides, "step overrides"))
        )

    connection = connect(db_path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        if group_id is not None:
            group = connection.execute(
                "SELECT id FROM groups WHERE id = ?", (group_id,)
            ).fetchone()
            if group is None:
                raise NotFoundError("group {} was not found".format(group_id))
        now = utc_now()
        cursor = connection.execute(
            """
            INSERT INTO tasks(
                name, group_id, base_parameters, status, created_at, updated_at
            ) VALUES (?, ?, ?, 'pending', ?, ?)
            """,
            (name, group_id, _json_dump(base), now, now),
        )
        task_id = cursor.lastrowid
        connection.executemany(
            """
            INSERT INTO steps(
                task_id, sequence, name, override_parameters, status
            ) VALUES (?, ?, ?, ?, 'pending')
            """,
            [
                (task_id, sequence, step_name, _json_dump(overrides))
                for sequence, (step_name, overrides) in enumerate(
                    normalized_steps, start=1
                )
            ],
        )
        task = _task_from_connection(connection, task_id)
        connection.commit()
        return task
    except Exception:
        if connection.in_transaction:
            connection.rollback()
        raise
    finally:
        connection.close()


def list_tasks(db_path: DatabasePath) -> List[JsonObject]:
    connection = connect(db_path)
    try:
        connection.execute("BEGIN")
        ids = connection.execute(
            "SELECT id FROM tasks ORDER BY created_at DESC, id DESC"
        ).fetchall()
        tasks = [_task_from_connection(connection, row["id"]) for row in ids]
        connection.commit()
        return tasks
    except Exception:
        if connection.in_transaction:
            connection.rollback()
        raise
    finally:
        connection.close()


def get_task(db_path: DatabasePath, task_id: int) -> JsonObject:
    connection = connect(db_path)
    try:
        connection.execute("BEGIN")
        task = _task_from_connection(connection, task_id)
        connection.commit()
        return task
    except Exception:
        if connection.in_transaction:
            connection.rollback()
        raise
    finally:
        connection.close()


def claim_next_task(
    db_path: DatabasePath,
    worker_id: str,
    *,
    connection: Optional[sqlite3.Connection] = None,
) -> Optional[JsonObject]:
    """Atomically claim the oldest task and return its server-issued token."""

    worker_id = worker_id.strip()
    if not worker_id:
        raise ValidationError("worker_id must not be blank")
    owns_connection = connection is None
    connection = connection or connect(db_path)
    try:
        # SQLite permits only one writer.  Acquiring that right before the
        # SELECT removes the otherwise unsafe read/update race between workers.
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute(
            """
            SELECT id FROM tasks
            WHERE status = 'pending' AND claimed_by IS NULL AND claim_token IS NULL
            ORDER BY created_at, id
            LIMIT 1
            """
        ).fetchone()
        if row is None:
            connection.commit()
            return None

        now = utc_now()
        claim_token = secrets.token_urlsafe(32)
        cursor = connection.execute(
            """
            UPDATE tasks
            SET status = 'claimed', claimed_by = ?, claim_token = ?,
                claimed_at = ?, updated_at = ?
            WHERE id = ? AND status = 'pending'
                AND claimed_by IS NULL AND claim_token IS NULL
            """,
            (worker_id, claim_token, now, now, row["id"]),
        )
        if cursor.rowcount != 1:
            # The condition is a second invariant even though BEGIN IMMEDIATE
            # already serializes SQLite writers.
            raise ConflictError("the selected task was claimed concurrently")
        task = _task_from_connection(connection, row["id"])
        connection.commit()
        return {"task": task, "claim_token": claim_token}
    except Exception:
        if connection.in_transaction:
            connection.rollback()
        raise
    finally:
        if owns_connection:
            connection.close()


def start_task(
    db_path: DatabasePath,
    task_id: int,
    worker_id: str,
    claim_token: str,
) -> JsonObject:
    """Snapshot the current L2 override and all resolved step parameters once."""

    worker_id = worker_id.strip()
    claim_token = claim_token.strip()
    if not worker_id:
        raise ValidationError("worker_id must not be blank")
    if not claim_token:
        raise ValidationError("claim_token must not be blank")
    connection = connect(db_path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        task = connection.execute(
            "SELECT * FROM tasks WHERE id = ?", (task_id,)
        ).fetchone()
        if task is None:
            raise NotFoundError("task {} was not found".format(task_id))
        token_matches = secrets.compare_digest(task["claim_token"] or "", claim_token)
        if task["claimed_by"] != worker_id or not token_matches:
            raise ConflictError("claim credentials do not match task {}".format(task_id))
        if task["status"] == "running":
            # A retry by the owning worker must not re-read a changed group.
            result = _task_from_connection(connection, task_id)
            connection.commit()
            return result
        if task["status"] != "claimed":
            raise ConflictError(
                "task {} cannot start from status {}".format(task_id, task["status"])
            )

        if task["group_id"] is None:
            group_parameters: JsonObject = {}
        else:
            group = connection.execute(
                "SELECT override_parameters FROM groups WHERE id = ?",
                (task["group_id"],),
            ).fetchone()
            if group is None:
                raise ConflictError("the task's group no longer exists")
            group_parameters = _json_load(group["override_parameters"]) or {}

        step_rows = connection.execute(
            "SELECT sequence, override_parameters FROM steps "
            "WHERE task_id = ? ORDER BY sequence",
            (task_id,),
        ).fetchall()
        if not step_rows:
            raise ConflictError("task {} has no steps".format(task_id))
        snapshots = resolve_parameter_chain(
            _json_load(task["base_parameters"]) or {},
            group_parameters,
            [_json_load(row["override_parameters"]) or {} for row in step_rows],
        )
        now = utc_now()
        for row, snapshot in zip(step_rows, snapshots):
            connection.execute(
                """
                UPDATE steps SET resolved_parameters = ?
                WHERE task_id = ? AND sequence = ?
                """,
                (_json_dump(snapshot), task_id, row["sequence"]),
            )
        first_sequence = step_rows[0]["sequence"]
        connection.execute(
            """
            UPDATE steps SET status = 'running', started_at = ?
            WHERE task_id = ? AND sequence = ? AND status = 'pending'
            """,
            (now, task_id, first_sequence),
        )
        connection.execute(
            """
            UPDATE tasks
            SET status = 'running', group_parameters_snapshot = ?,
                started_at = ?, updated_at = ?
            WHERE id = ? AND status = 'claimed'
                AND claimed_by = ? AND claim_token = ?
            """,
            (
                _json_dump(group_parameters),
                now,
                now,
                task_id,
                worker_id,
                claim_token,
            ),
        )
        result = _task_from_connection(connection, task_id)
        connection.commit()
        return result
    except Exception:
        if connection.in_transaction:
            connection.rollback()
        raise
    finally:
        connection.close()


def complete_step(
    db_path: DatabasePath,
    task_id: int,
    sequence: int,
    worker_id: str,
    claim_token: str,
    success: bool,
    *,
    connection: Optional[sqlite3.Connection] = None,
) -> JsonObject:
    """Record a step result with first-write-wins idempotency."""

    worker_id = worker_id.strip()
    claim_token = claim_token.strip()
    if not worker_id:
        raise ValidationError("worker_id must not be blank")
    if not claim_token:
        raise ValidationError("claim_token must not be blank")
    owns_connection = connection is None
    connection = connection or connect(db_path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        task = connection.execute(
            "SELECT * FROM tasks WHERE id = ?", (task_id,)
        ).fetchone()
        if task is None:
            raise NotFoundError("task {} was not found".format(task_id))
        token_matches = secrets.compare_digest(task["claim_token"] or "", claim_token)
        if task["claimed_by"] != worker_id or not token_matches:
            raise ConflictError("claim credentials do not match task {}".format(task_id))
        step = connection.execute(
            "SELECT * FROM steps WHERE task_id = ? AND sequence = ?",
            (task_id, sequence),
        ).fetchone()
        if step is None:
            raise NotFoundError(
                "step {} was not found on task {}".format(sequence, task_id)
            )

        existing_log = connection.execute(
            """
            SELECT * FROM execution_logs
            WHERE task_id = ? AND step_sequence = ?
            """,
            (task_id, sequence),
        ).fetchone()
        if existing_log is not None:
            result = {
                "inserted": False,
                "duplicate": True,
                "log": _log_from_row(existing_log),
                "task": _task_from_connection(connection, task_id),
            }
            connection.commit()
            return result

        if task["status"] != "running" or step["status"] != "running":
            raise ConflictError(
                "step {} is not the running step for task {}".format(
                    sequence, task_id
                )
            )

        now = utc_now()
        cursor = connection.execute(
            """
            INSERT INTO execution_logs(task_id, step_sequence, success, completed_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(task_id, step_sequence) DO NOTHING
            """,
            (task_id, sequence, 1 if success else 0, now),
        )
        if cursor.rowcount == 0:
            # Kept as a defensive branch should the transaction strategy ever
            # change; state transitions below are driven only by this insert.
            existing_log = connection.execute(
                """
                SELECT * FROM execution_logs
                WHERE task_id = ? AND step_sequence = ?
                """,
                (task_id, sequence),
            ).fetchone()
            result = {
                "inserted": False,
                "duplicate": True,
                "log": _log_from_row(existing_log),
                "task": _task_from_connection(connection, task_id),
            }
            connection.commit()
            return result

        connection.execute(
            """
            UPDATE steps SET status = ?, completed_at = ?
            WHERE task_id = ? AND sequence = ?
            """,
            ("done" if success else "failed", now, task_id, sequence),
        )
        if not success:
            connection.execute(
                """
                UPDATE tasks SET status = 'failed', completed_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (now, now, task_id),
            )
        else:
            next_step = connection.execute(
                """
                SELECT sequence FROM steps
                WHERE task_id = ? AND sequence > ?
                ORDER BY sequence LIMIT 1
                """,
                (task_id, sequence),
            ).fetchone()
            if next_step is None:
                connection.execute(
                    """
                    UPDATE tasks SET status = 'done', completed_at = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (now, now, task_id),
                )
            else:
                connection.execute(
                    """
                    UPDATE steps SET status = 'running', started_at = ?
                    WHERE task_id = ? AND sequence = ? AND status = 'pending'
                    """,
                    (now, task_id, next_step["sequence"]),
                )
                connection.execute(
                    "UPDATE tasks SET updated_at = ? WHERE id = ?", (now, task_id)
                )

        log = connection.execute(
            """
            SELECT * FROM execution_logs
            WHERE task_id = ? AND step_sequence = ?
            """,
            (task_id, sequence),
        ).fetchone()
        result = {
            "inserted": True,
            "duplicate": False,
            "log": _log_from_row(log),
            "task": _task_from_connection(connection, task_id),
        }
        connection.commit()
        return result
    except Exception:
        if connection.in_transaction:
            connection.rollback()
        raise
    finally:
        if owns_connection:
            connection.close()


def seed_demo(db_path: DatabasePath) -> JsonObject:
    """Create a small, immediately demonstrable task set."""

    connection = connect(db_path)
    try:
        group = connection.execute(
            "SELECT * FROM groups WHERE name = 'Demo Customers'"
        ).fetchone()
    finally:
        connection.close()
    if group is None:
        demo_group = create_group(
            db_path,
            "Demo Customers",
            {"channel": "email", "sender": "demo@example.com", "subject": ""},
        )
    else:
        demo_group = _group_from_row(group)

    suffix = datetime.now(timezone.utc).strftime("%H%M%S%f")
    running = create_task(
        db_path,
        "Demo running {}".format(suffix),
        demo_group["id"],
        {"channel": "sms", "retries": 2, "locale": "zh-CN"},
        [
            {"name": "Prepare recipients", "overrides": {"batch": 100}},
            {
                "name": "Send messages",
                "overrides": {"channel": "push", "batch": ""},
            },
            {"name": "Collect receipts", "overrides": {"retries": 3}},
        ],
    )
    pending = create_task(
        db_path,
        "Demo pending {}".format(suffix),
        demo_group["id"],
        {"channel": "sms", "retries": 1},
        [{"name": "Send", "overrides": {}}],
    )
    # Demo creation must remain reliable even if the queue already contains
    # older user-created tasks.  Claim this newly-created task by id with the
    # same atomic transition and ownership invariant as claim-next.
    connection = connect(db_path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        now = utc_now()
        claim_token = secrets.token_urlsafe(32)
        claimed = connection.execute(
            """
            UPDATE tasks
            SET status = 'claimed', claimed_by = 'demo-worker', claim_token = ?,
                claimed_at = ?, updated_at = ?
            WHERE id = ? AND status = 'pending'
                AND claimed_by IS NULL AND claim_token IS NULL
            """,
            (claim_token, now, now, running["id"]),
        )
        if claimed.rowcount != 1:
            raise ConflictError("demo seed could not claim its running task")
        connection.commit()
    except Exception:
        if connection.in_transaction:
            connection.rollback()
        raise
    finally:
        connection.close()
    running = start_task(db_path, running["id"], "demo-worker", claim_token)
    return {
        "group": demo_group,
        "running_task": running,
        "pending_task": pending,
        "claim_token": claim_token,
    }
