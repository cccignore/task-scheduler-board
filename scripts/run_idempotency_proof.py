#!/usr/bin/env python3
"""Prove first-write-wins logging with five real spawn processes."""

import argparse
import multiprocessing
import os
import queue
import sqlite3
import sys
import tempfile
from pathlib import Path
from threading import BrokenBarrierError
from typing import Any, Dict, List, Optional


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _completion_worker(
    db_path: str,
    task_id: int,
    worker_id: str,
    start_barrier: Any,
    result_queue: Any,
) -> None:
    complete = None
    import_error = None
    try:
        from app.services import complete_step

        complete = complete_step
    except BaseException as exc:
        import_error = "{}: {}".format(type(exc).__name__, exc)

    try:
        start_barrier.wait(timeout=60)
        if import_error is not None:
            result_queue.put((None, None, import_error))
            return
        result = complete(db_path, task_id, 1, worker_id, True)
        result_queue.put((result["inserted"], result["duplicate"], None))
    except BrokenBarrierError:
        result_queue.put((None, None, "multiprocessing barrier broke"))
    except BaseException as exc:
        result_queue.put(
            (None, None, "{}: {}".format(type(exc).__name__, exc))
        )


def _read_log_rows(db_path: str, task_id: int) -> List[Any]:
    connection = sqlite3.connect(db_path)
    try:
        return connection.execute(
            "SELECT task_id, step_sequence, success, completed_at "
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
            "  simultaneous processes       : {}".format(stats["processes"]),
            "  inserted responses            : {}".format(stats["inserted_responses"]),
            "  duplicate/no-op responses     : {}".format(stats["duplicate_responses"]),
            "  log rows after five reports   : {}".format(stats["log_rows"]),
            "  stored result is success      : {}".format(stats["stored_success"]),
            "  later failed retry is no-op   : {}".format(stats["late_failure_noop"]),
            "  original log remained bytewise: {}".format(stats["log_unchanged"]),
            "  final task status             : {}".format(stats["task_status"]),
            "  child errors                  : {}".format(len(stats["errors"])),
        ]
    )


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

    os.environ["TASKBOARD_DB_PATH"] = db_path
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
    claimed = claim_next_task(db_path, "proof-worker")
    if claimed is None or claimed["id"] != task["id"]:
        raise AssertionError("proof setup did not claim the expected task")
    start_task(db_path, task["id"], "proof-worker")

    context = multiprocessing.get_context("spawn")
    start_barrier = context.Barrier(processes + 1)
    result_queue = context.Queue()
    children = [
        context.Process(
            target=_completion_worker,
            name="idempotency-proof-{}".format(index),
            args=(
                db_path,
                task["id"],
                "proof-worker",
                start_barrier,
                result_queue,
            ),
        )
        for index in range(processes)
    ]
    results: List[Any] = []
    try:
        for child in children:
            child.start()
        start_barrier.wait(timeout=60)
        for _ in range(processes):
            results.append(result_queue.get(timeout=60))
    finally:
        for child in children:
            child.join(timeout=10)
        for child in children:
            if child.is_alive():
                child.terminate()
                child.join(timeout=5)

    while True:
        try:
            results.append(result_queue.get_nowait())
        except queue.Empty:
            break

    exit_errors = [
        "{} exited with {}".format(child.name, child.exitcode)
        for child in children
        if child.exitcode != 0
    ]
    errors = [result[2] for result in results if result[2] is not None] + exit_errors
    inserted_responses = sum(result[0] is True for result in results)
    duplicate_responses = sum(result[1] is True for result in results)
    before_late_failure = _read_log_rows(db_path, task["id"])

    late_result = complete_step(
        db_path,
        task["id"],
        sequence=1,
        worker_id="proof-worker",
        success=False,
    )
    after_late_failure = _read_log_rows(db_path, task["id"])
    final_task = get_task(db_path, task["id"])

    stats: Dict[str, Any] = {
        "start_method": context.get_start_method(),
        "processes": processes,
        "inserted_responses": inserted_responses,
        "duplicate_responses": duplicate_responses,
        "log_rows": len(after_late_failure),
        "stored_success": (
            len(after_late_failure) == 1 and after_late_failure[0][2] == 1
        ),
        "late_failure_noop": (
            late_result["inserted"] is False and late_result["duplicate"] is True
        ),
        "log_unchanged": before_late_failure == after_late_failure,
        "task_status": final_task["status"],
        "errors": errors,
    }
    stats["passed"] = (
        stats["start_method"] == "spawn"
        and not errors
        and len(results) == processes
        and inserted_responses == 1
        and duplicate_responses == processes - 1
        and stats["log_rows"] == 1
        and stats["stored_success"]
        and stats["late_failure_noop"]
        and stats["log_unchanged"]
        and stats["task_status"] == "done"
    )

    if not quiet or not stats["passed"]:
        print(_format_stats(stats))
        for error in errors:
            print("  ERROR: {}".format(error))
    if temporary_directory is not None:
        temporary_directory.cleanup()
    if not stats["passed"]:
        raise AssertionError("multiprocess idempotency proof failed")
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    try:
        run_idempotency_proof()
    except BaseException as exc:
        print("idempotency proof terminated: {}: {}".format(type(exc).__name__, exc))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
