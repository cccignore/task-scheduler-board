#!/usr/bin/env python3
"""Prove first-write-wins logging with five pre-connected spawn processes."""

import argparse
import multiprocessing
import os
import queue
import sqlite3
import sys
import tempfile
import time
from pathlib import Path
from threading import BrokenBarrierError
from typing import Any, Dict, List, Optional


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from scripts.run_concurrency_proof import (  # noqa: E402
    GLOBAL_TIMEOUT_SECONDS,
    RESULT_TIMEOUT_SECONDS,
    STARTUP_TIMEOUT_SECONDS,
    _abort_barriers,
    _error_text,
    _get_from_queue,
    _shutdown_started_processes,
    _wait_at_barrier,
)


def _completion_worker(
    db_path: str,
    task_id: int,
    worker_id: str,
    claim_token: str,
    ready_barrier: Any,
    ready_queue: Any,
    result_queue: Any,
) -> None:
    pid = os.getpid()
    connection = None
    complete = None
    ready_error = None
    database_file = None
    connection_id = None
    try:
        try:
            from app.db import connect
            from app.services import complete_step

            complete = complete_step
            connection = connect(db_path)
            connection_id = id(connection)
            if connection.execute("SELECT 1").fetchone()[0] != 1:
                raise RuntimeError("connection validation query returned the wrong value")
            database_row = connection.execute("PRAGMA database_list").fetchone()
            database_file = str(Path(database_row[2]).resolve())
            if database_file != str(Path(db_path).resolve()):
                raise RuntimeError(
                    "connection opened {!r}, expected {!r}".format(
                        database_file, str(Path(db_path).resolve())
                    )
                )
        except BaseException as exc:
            ready_error = _error_text(exc)

        ready_queue.put(
            {
                "pid": pid,
                "connection_id": connection_id,
                "database_file": database_file,
                "error": ready_error,
            }
        )
        ready_barrier.wait(timeout=STARTUP_TIMEOUT_SECONDS)

        result: Dict[str, Any] = {
            "pid": pid,
            "connection_id": connection_id,
            "attempted": False,
            "inserted": None,
            "duplicate": None,
            "error": ready_error,
        }
        if ready_error is None:
            try:
                result["attempted"] = True
                completion = complete(
                    db_path,
                    task_id,
                    1,
                    worker_id,
                    claim_token,
                    True,
                    connection=connection,
                )
                result["inserted"] = completion["inserted"]
                result["duplicate"] = completion["duplicate"]
            except BaseException as exc:
                result["error"] = _error_text(exc)
        result_queue.put(result)
    except BrokenBarrierError:
        result_queue.put(
            {
                "kind": "worker_error",
                "pid": pid,
                "error": "multiprocessing barrier broke",
            }
        )
    except BaseException as exc:
        result_queue.put(
            {"kind": "worker_error", "pid": pid, "error": _error_text(exc)}
        )
    finally:
        if connection is not None:
            try:
                connection.close()
            except BaseException:
                pass


def _read_log_rows(db_path: str, task_id: int) -> List[Any]:
    connection = sqlite3.connect(db_path)
    try:
        return connection.execute(
            "SELECT id, task_id, step_sequence, success, completed_at "
            "FROM execution_logs WHERE task_id = ? ORDER BY step_sequence",
            (task_id,),
        ).fetchall()
    finally:
        connection.close()


def _format_stats(stats: Dict[str, Any]) -> str:
    outcome = "PASS" if stats["passed"] else "FAIL"
    return "\n".join(
        [
            "REAL MULTIPROCESS IDEMPOTENCY PROOF: {}".format(outcome),
            "  start method                  : {}".format(stats["start_method"]),
            "  worker processes              : {}".format(stats["processes"]),
            "  unique ready process ids      : {}".format(stats["ready_processes"]),
            "  verified pre-open connections : {}".format(
                stats["ready_connections"]
            ),
            "  ready pid/connection records  : {}".format(
                ", ".join(stats["ready_connection_records"])
            ),
            "  observed completion calls     : {}".format(stats["completion_calls"]),
            "  observed result records       : {}".format(stats["observed_results"]),
            "  exact inserted responses      : {}".format(
                stats["inserted_responses"]
            ),
            "  exact duplicate/no-op responses: {}".format(
                stats["duplicate_responses"]
            ),
            "  invalid response pairs        : {}".format(
                stats["invalid_response_pairs"]
            ),
            "  log rows after five reports   : {}".format(stats["log_rows"]),
            "  stored result is success      : {}".format(stats["stored_success"]),
            "  later failed retry is no-op   : {}".format(
                stats["late_failure_noop"]
            ),
            "  original log row unchanged    : {}".format(stats["row_unchanged"]),
            "  failure-first retry is no-op  : {}".format(
                stats["failure_first_noop"]
            ),
            "  final task status             : {}".format(stats["task_status"]),
            "  child/cleanup errors          : {}".format(len(stats["errors"])),
        ]
    )


def _run_idempotency_proof(
    db_path: str, *, processes: int, quiet: bool
) -> Dict[str, Any]:
    from app.db import initialize_database
    from app.services import (
        claim_next_task,
        complete_step,
        create_task,
        get_task,
        start_task,
    )

    initialize_database(db_path)
    task = create_task(
        db_path,
        name="five-way-idempotency-race",
        group_id=None,
        base_parameters={"proof": True},
        steps=[{"name": "only-step", "overrides": {}}],
    )
    claim_result = claim_next_task(db_path, "proof-worker")
    if claim_result is None or claim_result["task"]["id"] != task["id"]:
        raise AssertionError("proof setup did not claim the expected task")
    claim_token = claim_result["claim_token"]
    if not isinstance(claim_token, str) or not claim_token:
        raise AssertionError("proof setup did not receive a claim token")
    start_task(db_path, task["id"], "proof-worker", claim_token)

    context = multiprocessing.get_context("spawn")
    ready_barrier = context.Barrier(processes + 1)
    ready_queue = context.Queue()
    result_queue = context.Queue()
    children = [
        context.Process(
            target=_completion_worker,
            name="idempotency-proof-{}".format(index),
            args=(
                db_path,
                task["id"],
                "proof-worker",
                claim_token,
                ready_barrier,
                ready_queue,
                result_queue,
            ),
        )
        for index in range(processes)
    ]

    deadline = time.monotonic() + GLOBAL_TIMEOUT_SECONDS
    started_children: List[Any] = []
    ready_records: List[Dict[str, Any]] = []
    results: List[Dict[str, Any]] = []
    cleanup_errors: List[str] = []
    try:
        for child in children:
            child.start()
            # Never join a Process unless start() returned successfully.
            started_children.append(child)

        for index in range(processes):
            ready_records.append(
                _get_from_queue(
                    ready_queue,
                    deadline,
                    STARTUP_TIMEOUT_SECONDS,
                    "ready connection {}/{}".format(index + 1, processes),
                )
            )
        ready_errors = [record["error"] for record in ready_records if record["error"]]
        if ready_errors:
            raise AssertionError(
                "worker connection readiness failed: {}".format("; ".join(ready_errors))
            )
        _wait_at_barrier(
            ready_barrier, deadline, STARTUP_TIMEOUT_SECONDS, "connection readiness"
        )

        for result_index in range(processes):
            results.append(
                _get_from_queue(
                    result_queue,
                    deadline,
                    RESULT_TIMEOUT_SECONDS,
                    "completion result {}/{}".format(result_index + 1, processes),
                )
            )
    finally:
        _abort_barriers((ready_barrier,))
        cleanup_errors = _shutdown_started_processes(started_children)

    extra_messages: List[Dict[str, Any]] = []
    while True:
        try:
            extra_messages.append(result_queue.get_nowait())
        except queue.Empty:
            break
    worker_messages = [
        message
        for message in extra_messages
        if message.get("kind") == "worker_error"
    ]
    extra_results = [
        message
        for message in extra_messages
        if message.get("kind") != "worker_error"
    ]
    results.extend(extra_results)

    exit_errors = [
        "{} exited with {}".format(child.name, child.exitcode)
        for child in started_children
        if child.exitcode != 0
    ]
    errors = (
        [record["error"] for record in ready_records if record.get("error")]
        + [record["error"] for record in results if record.get("error")]
        + [message["error"] for message in worker_messages]
        + exit_errors
        + cleanup_errors
    )

    expected_database = str(Path(db_path).resolve())
    ready_pids = {record["pid"] for record in ready_records}
    ready_connection_keys = {
        (record["pid"], record["connection_id"]) for record in ready_records
    }
    ready_connection_records = [
        "{}/{}".format(record["pid"], record["connection_id"])
        for record in sorted(ready_records, key=lambda item: item["pid"])
    ]
    ready_connections_verified = (
        len(ready_records) == processes
        and len(ready_pids) == processes
        and len(ready_connection_keys) == processes
        and all(record["database_file"] == expected_database for record in ready_records)
        and not any(record.get("error") for record in ready_records)
    )

    exact_inserted_responses = sum(
        record.get("inserted") is True and record.get("duplicate") is False
        for record in results
    )
    exact_duplicate_responses = sum(
        record.get("inserted") is False and record.get("duplicate") is True
        for record in results
    )
    invalid_response_pairs = (
        len(results) - exact_inserted_responses - exact_duplicate_responses
    )
    response_pattern_exact = (
        len(results) == processes
        and exact_inserted_responses == 1
        and exact_duplicate_responses == processes - 1
        and invalid_response_pairs == 0
    )
    completion_calls = sum(record.get("attempted") is True for record in results)

    before_late_failure = _read_log_rows(db_path, task["id"])
    late_result = complete_step(
        db_path,
        task["id"],
        sequence=1,
        worker_id="proof-worker",
        claim_token=claim_token,
        success=False,
    )
    after_late_failure = _read_log_rows(db_path, task["id"])
    final_task = get_task(db_path, task["id"])

    # Also prove the other first-write-wins direction: a later success cannot
    # replace a failure row or resurrect the failed task.
    failure_task = create_task(
        db_path,
        name="failure-first-idempotency",
        group_id=None,
        base_parameters={"proof": True},
        steps=[{"name": "only-step", "overrides": {}}],
    )
    failure_claim = claim_next_task(db_path, "failure-proof-worker")
    if failure_claim is None or failure_claim["task"]["id"] != failure_task["id"]:
        raise AssertionError("failure-first setup did not claim the expected task")
    failure_token = failure_claim["claim_token"]
    start_task(
        db_path,
        failure_task["id"],
        "failure-proof-worker",
        failure_token,
    )
    first_failure = complete_step(
        db_path,
        failure_task["id"],
        sequence=1,
        worker_id="failure-proof-worker",
        claim_token=failure_token,
        success=False,
    )
    before_late_success = _read_log_rows(db_path, failure_task["id"])
    late_success = complete_step(
        db_path,
        failure_task["id"],
        sequence=1,
        worker_id="failure-proof-worker",
        claim_token=failure_token,
        success=True,
    )
    after_late_success = _read_log_rows(db_path, failure_task["id"])
    final_failure_task = get_task(db_path, failure_task["id"])
    failure_first_noop = (
        first_failure["inserted"] is True
        and first_failure["duplicate"] is False
        and late_success["inserted"] is False
        and late_success["duplicate"] is True
        and before_late_success == after_late_success
        and len(after_late_success) == 1
        and after_late_success[0][3] == 0
        and final_failure_task["status"] == "failed"
    )

    stats: Dict[str, Any] = {
        "start_method": context.get_start_method(),
        "processes": processes,
        "ready_processes": len(ready_pids),
        "ready_connections": len(ready_connection_keys),
        "ready_connection_records": ready_connection_records,
        "ready_connections_verified": ready_connections_verified,
        "completion_calls": completion_calls,
        "observed_results": len(results),
        "inserted_responses": exact_inserted_responses,
        "duplicate_responses": exact_duplicate_responses,
        "invalid_response_pairs": invalid_response_pairs,
        "response_pattern_exact": response_pattern_exact,
        "log_rows": len(after_late_failure),
        "stored_success": (
            len(after_late_failure) == 1 and after_late_failure[0][3] == 1
        ),
        "late_failure_noop": (
            late_result["inserted"] is False
            and late_result["duplicate"] is True
        ),
        "row_unchanged": before_late_failure == after_late_failure,
        "failure_first_noop": failure_first_noop,
        "task_status": final_task["status"],
        "errors": errors,
    }
    stats["passed"] = (
        stats["start_method"] == "spawn"
        and ready_connections_verified
        and not errors
        and completion_calls == processes
        and response_pattern_exact
        and stats["log_rows"] == 1
        and stats["stored_success"]
        and stats["late_failure_noop"]
        and stats["row_unchanged"]
        and stats["failure_first_noop"]
        and stats["task_status"] == "done"
    )

    if not quiet or not stats["passed"]:
        print(_format_stats(stats))
        for error in errors:
            print("  ERROR: {}".format(error))
    if not stats["passed"]:
        raise AssertionError("multiprocess idempotency proof failed")
    return stats


def run_idempotency_proof(
    database_path: Optional[os.PathLike] = None,
    *,
    processes: int = 5,
    quiet: bool = False,
) -> Dict[str, Any]:
    if processes != 5:
        raise ValueError("this acceptance proof intentionally requires exactly 5 processes")

    temporary_directory = None
    if database_path is None:
        temporary_directory = tempfile.TemporaryDirectory(prefix="taskboard-log-")
        db_path = str(Path(temporary_directory.name) / "proof.db")
    else:
        db_path = os.fspath(database_path)
        if Path(db_path).exists():
            raise ValueError("proof database must not already exist: {}".format(db_path))

    previous_database = os.environ.get("TASKBOARD_DB_PATH")
    had_database_environment = "TASKBOARD_DB_PATH" in os.environ
    os.environ["TASKBOARD_DB_PATH"] = db_path
    try:
        return _run_idempotency_proof(
            db_path, processes=processes, quiet=quiet
        )
    finally:
        if had_database_environment:
            os.environ["TASKBOARD_DB_PATH"] = previous_database or ""
        else:
            os.environ.pop("TASKBOARD_DB_PATH", None)
        if temporary_directory is not None:
            temporary_directory.cleanup()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    try:
        run_idempotency_proof()
    except BaseException as exc:
        print("idempotency proof terminated: {}".format(_error_text(exc)))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
