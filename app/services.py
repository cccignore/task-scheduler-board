"""Domain operations for groups, tasks, workers, and execution logs."""

import copy
import json
import logging
import os
import secrets
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Union

from .db import connect


DatabasePath = Union[str, Path]
JsonObject = Dict[str, Any]

logger = logging.getLogger("taskboard")

# A claim is a lease, not permanent ownership: a worker that dies without
# reporting loses the task once the lease expires and claim-next reclaims it.
DEFAULT_LEASE_SECONDS = 900.0


class ServiceError(Exception):
    """Base class for errors that can be translated into an API response."""


class NotFoundError(ServiceError):
    pass


class ConflictError(ServiceError):
    pass


class ValidationError(ServiceError):
    pass


def _utc_iso(moment: datetime) -> str:
    return moment.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def utc_now() -> str:
    return _utc_iso(datetime.now(timezone.utc))


def resolve_lease_seconds(lease_seconds: Optional[float] = None) -> float:
    """Resolve an explicit lease, then the environment, then the default."""

    raw = (
        lease_seconds
        if lease_seconds is not None
        else os.environ.get("TASKBOARD_LEASE_SECONDS", DEFAULT_LEASE_SECONDS)
    )
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise ValidationError("lease seconds must be a number") from exc
    if value < 0:
        raise ValidationError("lease seconds must not be negative")
    return value


def _lease_expiry(lease_seconds: Optional[float] = None) -> str:
    seconds = resolve_lease_seconds(lease_seconds)
    return _utc_iso(datetime.now(timezone.utc) + timedelta(seconds=seconds))


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


def _record_operation(
    connection: sqlite3.Connection,
    event: str,
    message: str,
    *,
    level: str = "info",
    task_id: Optional[int] = None,
    step_sequence: Optional[int] = None,
    worker_id: Optional[str] = None,
) -> None:
    """Append one audit row inside the caller's open transaction.

    Every process that mutates scheduler state (API server, dashboard-managed
    workers, CLI workers, proof scripts) shares this table, so the dashboard
    can show a single cross-process operations ledger.
    """

    connection.execute(
        """
        INSERT INTO operation_logs(
            at, level, event, task_id, step_sequence, worker_id, message
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (utc_now(), level, event, task_id, step_sequence, worker_id, message),
    )


def list_operation_logs(
    db_path: DatabasePath,
    after_id: Optional[int] = None,
    limit: int = 200,
) -> List[JsonObject]:
    """Return audit rows: the newest ``limit`` rows, or rows after a cursor."""

    limit = max(1, min(int(limit), 500))
    connection = connect(db_path)
    try:
        if after_id is None:
            rows = connection.execute(
                "SELECT * FROM operation_logs ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        else:
            rows = connection.execute(
                """
                SELECT * FROM operation_logs
                WHERE id > ? ORDER BY id DESC LIMIT ?
                """,
                (int(after_id), limit),
            ).fetchall()
        return [
            {
                "id": row["id"],
                "at": row["at"],
                "level": row["level"],
                "event": row["event"],
                "task_id": row["task_id"],
                "step_sequence": row["step_sequence"],
                "worker_id": row["worker_id"],
                "message": row["message"],
            }
            for row in rows
        ]
    finally:
        connection.close()


def _log_duplicate_report(
    connection: sqlite3.Connection,
    existing_log: sqlite3.Row,
    reported_success: bool,
    worker_id: str,
) -> None:
    """First-write-wins keeps duplicates silent for callers; operators still
    deserve a trace, especially when the late report contradicts the record."""

    canonical_success = bool(existing_log["success"])
    if canonical_success != reported_success:
        _record_operation(
            connection,
            "duplicate_conflict",
            "任务 #{} · Step {} 收到矛盾的重复上报：账本记录为{}，本次却报{}——首写生效，本次被忽略".format(
                existing_log["task_id"],
                existing_log["step_sequence"],
                "成功" if canonical_success else "失败",
                "成功" if reported_success else "失败",
            ),
            level="warning",
            task_id=existing_log["task_id"],
            step_sequence=existing_log["step_sequence"],
            worker_id=worker_id,
        )
        logger.warning(
            "contradictory duplicate report ignored: task %s step %s is %s, "
            "%s reported %s",
            existing_log["task_id"],
            existing_log["step_sequence"],
            "success" if canonical_success else "failure",
            worker_id,
            "success" if reported_success else "failure",
        )
    else:
        _record_operation(
            connection,
            "duplicate_report",
            "任务 #{} · Step {} 的重复完成上报被忽略：该 Step 已有唯一日志（幂等 no-op）".format(
                existing_log["task_id"], existing_log["step_sequence"]
            ),
            task_id=existing_log["task_id"],
            step_sequence=existing_log["step_sequence"],
            worker_id=worker_id,
        )
        logger.info(
            "duplicate report ignored: task %s step %s already recorded by "
            "first writer",
            existing_log["task_id"],
            existing_log["step_sequence"],
        )


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
            if (
                step["status"] in ("done", "failed")
                and step["resolved_parameters"] is not None
            ):
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
        "lease_expires_at": task["lease_expires_at"],
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
        _record_operation(
            connection,
            "task_created",
            "任务 #{}「{}」创建（{} 个 Step），进入待认领队列".format(
                task_id, name, len(normalized_steps)
            ),
            task_id=task_id,
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


def _reclaim_expired_leases(connection: sqlite3.Connection, now: str) -> List[int]:
    """Inside an open write transaction, push lease-expired tasks back to pending.

    Completed steps keep their statuses and execution logs, so the next owner
    resumes from the first still-pending step instead of redoing finished work.
    """

    expired = connection.execute(
        """
        SELECT id FROM tasks
        WHERE status IN ('claimed', 'running')
            AND lease_expires_at IS NOT NULL AND lease_expires_at < ?
        ORDER BY id
        """,
        (now,),
    ).fetchall()
    task_ids = [row["id"] for row in expired]
    if not task_ids:
        return task_ids

    previous_owners = {
        row["id"]: row["claimed_by"]
        for row in connection.execute(
            "SELECT id, claimed_by FROM tasks WHERE id IN ({})".format(
                ",".join("?" for _ in task_ids)
            ),
            task_ids,
        ).fetchall()
    }
    placeholders = ",".join("?" for _ in task_ids)
    connection.execute(
        """
        UPDATE steps SET status = 'pending', started_at = NULL
        WHERE status = 'running' AND task_id IN ({})
        """.format(placeholders),
        task_ids,
    )
    connection.execute(
        """
        UPDATE tasks
        SET status = 'pending', claimed_by = NULL, claim_token = NULL,
            claimed_at = NULL, lease_expires_at = NULL, updated_at = ?
        WHERE id IN ({})
        """.format(placeholders),
        [now] + task_ids,
    )
    for task_id in task_ids:
        _record_operation(
            connection,
            "lease_reclaim",
            "任务 #{} 租约过期被回收，重新排队；原持有者 {} 的凭证已作废".format(
                task_id, previous_owners.get(task_id) or "未知"
            ),
            level="warning",
            task_id=task_id,
            worker_id=previous_owners.get(task_id),
        )
    logger.info("reclaimed %d expired lease(s): tasks %s", len(task_ids), task_ids)
    return task_ids


def claim_next_task(
    db_path: DatabasePath,
    worker_id: str,
    *,
    connection: Optional[sqlite3.Connection] = None,
    lease_seconds: Optional[float] = None,
) -> Optional[JsonObject]:
    """Atomically claim the oldest task and return its server-issued token.

    Claiming grants a lease rather than permanent ownership: before selecting,
    the same write transaction returns tasks whose lease has expired to the
    pending queue with their credentials cleared, so a crashed worker's task
    becomes claimable again and its rotated-out token can no longer act.
    """

    worker_id = worker_id.strip()
    if not worker_id:
        raise ValidationError("worker_id must not be blank")
    lease_expiry = _lease_expiry(lease_seconds)
    owns_connection = connection is None
    connection = connection or connect(db_path)
    try:
        # SQLite permits only one writer.  Acquiring that right before the
        # SELECT removes the otherwise unsafe read/update race between workers.
        connection.execute("BEGIN IMMEDIATE")
        now = utc_now()
        _reclaim_expired_leases(connection, now)
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

        claim_token = secrets.token_urlsafe(32)
        cursor = connection.execute(
            """
            UPDATE tasks
            SET status = 'claimed', claimed_by = ?, claim_token = ?,
                claimed_at = ?, lease_expires_at = ?, updated_at = ?
            WHERE id = ? AND status = 'pending'
                AND claimed_by IS NULL AND claim_token IS NULL
            """,
            (worker_id, claim_token, now, lease_expiry, now, row["id"]),
        )
        if cursor.rowcount != 1:
            # The condition is a second invariant even though BEGIN IMMEDIATE
            # already serializes SQLite writers.
            raise ConflictError("the selected task was claimed concurrently")
        _record_operation(
            connection,
            "claim",
            "{} 认领任务 #{}：事务保证唯一持有，发放新凭证与租约".format(
                worker_id, row["id"]
            ),
            task_id=row["id"],
            worker_id=worker_id,
        )
        # Commit before assembling the response payload: ownership is already
        # decided, so the multi-query read happens outside the write lock.
        connection.commit()
        logger.info("task %s claimed by %s", row["id"], worker_id)

        connection.execute("BEGIN")
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

        stored_snapshot = _json_load(task["group_parameters_snapshot"])
        if stored_snapshot is not None:
            # A task re-claimed after a lease expiry keeps its original L2
            # snapshot: the group override takes effect exactly once, at the
            # first start, and must not drift when a new worker resumes.
            group_parameters: JsonObject = stored_snapshot
        elif task["group_id"] is None:
            group_parameters = {}
        else:
            group = connection.execute(
                "SELECT override_parameters FROM groups WHERE id = ?",
                (task["group_id"],),
            ).fetchone()
            if group is None:
                raise ConflictError("the task's group no longer exists")
            group_parameters = _json_load(group["override_parameters"]) or {}

        step_rows = connection.execute(
            "SELECT sequence, override_parameters, status FROM steps "
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
        # Recomputing every snapshot is idempotent on resume because both
        # inputs (base and the frozen L2 snapshot) are unchanged.
        for row, snapshot in zip(step_rows, snapshots):
            connection.execute(
                """
                UPDATE steps SET resolved_parameters = ?
                WHERE task_id = ? AND sequence = ?
                """,
                (_json_dump(snapshot), task_id, row["sequence"]),
            )
        next_sequence = next(
            (row["sequence"] for row in step_rows if row["status"] == "pending"),
            None,
        )
        if next_sequence is None:
            raise ConflictError(
                "task {} has no pending step left to run".format(task_id)
            )
        connection.execute(
            """
            UPDATE steps SET status = 'running', started_at = ?
            WHERE task_id = ? AND sequence = ? AND status = 'pending'
            """,
            (now, task_id, next_sequence),
        )
        connection.execute(
            """
            UPDATE tasks
            SET status = 'running', group_parameters_snapshot = ?,
                started_at = ?, lease_expires_at = ?, updated_at = ?
            WHERE id = ? AND status = 'claimed'
                AND claimed_by = ? AND claim_token = ?
            """,
            (
                _json_dump(group_parameters),
                task["started_at"] or now,
                _lease_expiry(),
                now,
                task_id,
                worker_id,
                claim_token,
            ),
        )
        if stored_snapshot is not None:
            start_message = (
                "{} 恢复执行任务 #{}：复用首次冻结的组参数快照，从 Step {} 续跑".format(
                    worker_id, task_id, next_sequence
                )
            )
        else:
            start_message = (
                "{} 启动任务 #{}：冻结组参数快照，解析全部 Step 生效参数，Step {} 开始执行".format(
                    worker_id, task_id, next_sequence
                )
            )
        _record_operation(
            connection,
            "start",
            start_message,
            task_id=task_id,
            step_sequence=next_sequence,
            worker_id=worker_id,
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
            _log_duplicate_report(connection, existing_log, success, worker_id)
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
            _log_duplicate_report(connection, existing_log, success, worker_id)
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
                UPDATE tasks
                SET status = 'failed', completed_at = ?, lease_expires_at = NULL,
                    updated_at = ?
                WHERE id = ?
                """,
                (now, now, task_id),
            )
            _record_operation(
                connection,
                "step_report",
                "任务 #{} · Step {} 上报失败，唯一日志已写入 → 任务终止为 failed".format(
                    task_id, sequence
                ),
                level="warning",
                task_id=task_id,
                step_sequence=sequence,
                worker_id=worker_id,
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
                    UPDATE tasks
                    SET status = 'done', completed_at = ?, lease_expires_at = NULL,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (now, now, task_id),
                )
                _record_operation(
                    connection,
                    "step_report",
                    "任务 #{} · Step {} 上报成功，唯一日志已写入 → 全部 Step 完成，任务 done".format(
                        task_id, sequence
                    ),
                    task_id=task_id,
                    step_sequence=sequence,
                    worker_id=worker_id,
                )
            else:
                connection.execute(
                    """
                    UPDATE steps SET status = 'running', started_at = ?
                    WHERE task_id = ? AND sequence = ? AND status = 'pending'
                    """,
                    (now, task_id, next_step["sequence"]),
                )
                # Every successful report also renews the lease, so a live
                # worker on a long task is never reclaimed mid-flight.
                connection.execute(
                    """
                    UPDATE tasks SET lease_expires_at = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (_lease_expiry(), now, task_id),
                )
                _record_operation(
                    connection,
                    "step_report",
                    "任务 #{} · Step {} 上报成功，唯一日志已写入 → Step {} 进入执行，租约已续期".format(
                        task_id, sequence, next_step["sequence"]
                    ),
                    task_id=task_id,
                    step_sequence=sequence,
                    worker_id=worker_id,
                )
        logger.info(
            "task %s step %s reported %s by %s",
            task_id,
            sequence,
            "success" if success else "failure",
            worker_id,
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


def reset_all(db_path: DatabasePath) -> None:
    """Wipe every demo table so the dashboard can restart from a clean slate."""

    connection = connect(db_path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        for table in ("execution_logs", "steps", "tasks", "groups", "operation_logs"):
            connection.execute("DELETE FROM {}".format(table))
        _record_operation(
            connection,
            "reset",
            "看板已清空：任务、Step、执行日志、组与历史台账全部删除",
            level="warning",
        )
        connection.commit()
    except Exception:
        if connection.in_transaction:
            connection.rollback()
        raise
    finally:
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
                claimed_at = ?, lease_expires_at = ?, updated_at = ?
            WHERE id = ? AND status = 'pending'
                AND claimed_by IS NULL AND claim_token IS NULL
            """,
            (claim_token, now, _lease_expiry(), now, running["id"]),
        )
        if claimed.rowcount != 1:
            raise ConflictError("demo seed could not claim its running task")
        _record_operation(
            connection,
            "claim",
            "demo-worker 认领演示任务 #{}：与 claim-next 相同的原子事务与凭证机制".format(
                running["id"]
            ),
            task_id=running["id"],
            worker_id="demo-worker",
        )
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
