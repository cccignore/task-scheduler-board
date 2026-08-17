#!/usr/bin/env python3
"""Attack claim-next with synchronized spawn processes and pre-open DB links."""

import argparse
import multiprocessing
import os
import queue
import sqlite3
import sys
import tempfile
import time
from collections import Counter
from pathlib import Path
from threading import BrokenBarrierError
from typing import Any, Dict, List, Optional, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


STARTUP_TIMEOUT_SECONDS = 30.0
ROUND_TIMEOUT_SECONDS = 20.0
RESULT_TIMEOUT_SECONDS = 15.0
GLOBAL_TIMEOUT_SECONDS = 120.0
GRACEFUL_SHUTDOWN_SECONDS = 5.0
TERMINATE_TIMEOUT_SECONDS = 5.0


def _error_text(exc: BaseException) -> str:
    return "{}: {}".format(type(exc).__name__, exc)


def _remaining_timeout(deadline: float, phase_limit: float, phase: str) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("global timeout exceeded during {}".format(phase))
    return min(phase_limit, remaining)


def _wait_at_barrier(
    barrier: Any, deadline: float, phase_limit: float, phase: str
) -> None:
    try:
        barrier.wait(timeout=_remaining_timeout(deadline, phase_limit, phase))
    except BrokenBarrierError as exc:
        raise TimeoutError("{} barrier broke or timed out".format(phase)) from exc


def _get_from_queue(
    source_queue: Any, deadline: float, phase_limit: float, phase: str
) -> Any:
    try:
        return source_queue.get(
            timeout=_remaining_timeout(deadline, phase_limit, phase)
        )
    except queue.Empty as exc:
        raise TimeoutError("timed out waiting for {}".format(phase)) from exc


def _abort_barriers(barriers: Sequence[Any]) -> None:
    for barrier in barriers:
        try:
            barrier.abort()
        except BaseException:
            # Cleanup must never replace the failure that caused it.
            pass


def _shutdown_started_processes(processes: Sequence[Any]) -> List[str]:
    """Bound cleanup by phase deadlines and never raise over the primary error."""

    cleanup_errors: List[str] = []
    graceful_deadline = time.monotonic() + GRACEFUL_SHUTDOWN_SECONDS
    for process in processes:
        try:
            process.join(timeout=max(0.0, graceful_deadline - time.monotonic()))
        except BaseException as exc:
            cleanup_errors.append(
                "{} graceful join failed: {}".format(process.name, _error_text(exc))
            )

    for process in processes:
        try:
            if process.is_alive():
                process.terminate()
        except BaseException as exc:
            cleanup_errors.append(
                "{} terminate failed: {}".format(process.name, _error_text(exc))
            )

    terminate_deadline = time.monotonic() + TERMINATE_TIMEOUT_SECONDS
    for process in processes:
        try:
            if process.is_alive():
                process.join(timeout=max(0.0, terminate_deadline - time.monotonic()))
            if process.is_alive():
                cleanup_errors.append("{} did not terminate".format(process.name))
        except BaseException as exc:
            cleanup_errors.append(
                "{} final join failed: {}".format(process.name, _error_text(exc))
            )
    return cleanup_errors


def _claim_worker(
    db_path: str,
    worker_id: str,
    rounds: int,
    ready_barrier: Any,
    start_barrier: Any,
    finished_barrier: Any,
    ready_queue: Any,
    result_queue: Any,
) -> None:
    """Hold one verified connection open while participating in every race."""

    pid = os.getpid()
    connection = None
    claim = None
    ready_error = None
    database_file = None
    connection_id = None
    try:
        try:
            from app.db import connect
            from app.services import claim_next_task

            claim = claim_next_task
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
                "worker_id": worker_id,
                "pid": pid,
                "connection_id": connection_id,
                "database_file": database_file,
                "error": ready_error,
            }
        )
        ready_barrier.wait(timeout=STARTUP_TIMEOUT_SECONDS)

        for round_index in range(rounds):
            start_barrier.wait(timeout=ROUND_TIMEOUT_SECONDS)
            result: Dict[str, Any] = {
                "round": round_index,
                "worker_id": worker_id,
                "pid": pid,
                "connection_id": connection_id,
                "attempted": False,
                "task_id": None,
                "task_status": None,
                "task_claimed_by": None,
                "claim_token": None,
                "error": ready_error,
            }
            if ready_error is None:
                try:
                    result["attempted"] = True
                    claim_result = claim(db_path, worker_id, connection=connection)
                    if claim_result is not None:
                        task = claim_result["task"]
                        result.update(
                            {
                                "task_id": task["id"],
                                "task_status": task["status"],
                                "task_claimed_by": task["claimed_by"],
                                "claim_token": claim_result["claim_token"],
                            }
                        )
                except BaseException as exc:
                    result["error"] = _error_text(exc)
            result_queue.put(result)
            finished_barrier.wait(timeout=ROUND_TIMEOUT_SECONDS)
    except BrokenBarrierError:
        result_queue.put(
            {
                "kind": "worker_error",
                "worker_id": worker_id,
                "pid": pid,
                "error": "multiprocessing barrier broke",
            }
        )
    except BaseException as exc:
        result_queue.put(
            {
                "kind": "worker_error",
                "worker_id": worker_id,
                "pid": pid,
                "error": _error_text(exc),
            }
        )
    finally:
        if connection is not None:
            try:
                connection.close()
            except BaseException:
                pass


def _format_stats(stats: Dict[str, Any]) -> str:
    outcome = "PASS" if stats["passed"] else "FAIL"
    return "\n".join(
        [
            "REAL MULTIPROCESS CLAIM PROOF: {}".format(outcome),
            "  start method                 : {}".format(stats["start_method"]),
            "  worker processes             : {}".format(stats["workers"]),
            "  unique ready process ids     : {}".format(stats["ready_processes"]),
            "  verified pre-open connections: {}".format(
                stats["ready_connections"]
            ),
            "  ready pid/connection records : {}".format(
                ", ".join(stats["ready_connection_records"])
            ),
            "  one-task race rounds         : {}".format(stats["rounds"]),
            "  observed claim calls         : {}".format(stats["claim_attempts"]),
            "  observed result records      : {}".format(stats["observed_results"]),
            "  result pair anomalies        : {}".format(
                stats["result_pair_anomalies"]
            ),
            "  expected unique winners      : {}".format(stats["rounds"]),
            "  actual unique winners        : {}".format(stats["unique_winners"]),
            "  duplicate claims             : {}".format(stats["duplicate_claims"]),
            "  missing/extra winner rounds  : {}".format(stats["anomalous_rounds"]),
            "  claim metadata anomalies     : {}".format(
                stats["claim_metadata_anomalies"]
            ),
            "  child/cleanup errors         : {}".format(len(stats["errors"])),
            "  pending tasks after proof    : {}".format(stats["pending_tasks"]),
        ]
    )


def _run_claim_proof(
    db_path: str, *, rounds: int, workers: int, quiet: bool
) -> Dict[str, Any]:
    from app.db import initialize_database
    from app.services import create_task

    initialize_database(db_path)
    context = multiprocessing.get_context("spawn")
    ready_barrier = context.Barrier(workers + 1)
    start_barrier = context.Barrier(workers + 1)
    finished_barrier = context.Barrier(workers + 1)
    ready_queue = context.Queue()
    result_queue = context.Queue()
    processes = [
        context.Process(
            target=_claim_worker,
            name="claim-proof-{}".format(index),
            args=(
                db_path,
                "worker-{}".format(index),
                rounds,
                ready_barrier,
                start_barrier,
                finished_barrier,
                ready_queue,
                result_queue,
            ),
        )
        for index in range(workers)
    ]

    deadline = time.monotonic() + GLOBAL_TIMEOUT_SECONDS
    started_processes: List[Any] = []
    ready_records: List[Dict[str, Any]] = []
    results: List[Dict[str, Any]] = []
    cleanup_errors: List[str] = []
    expected_ids: List[int] = []
    try:
        for process in processes:
            process.start()
            # Only successfully started processes may ever be joined or terminated.
            started_processes.append(process)

        for index in range(workers):
            ready_records.append(
                _get_from_queue(
                    ready_queue,
                    deadline,
                    STARTUP_TIMEOUT_SECONDS,
                    "ready connection {}/{}".format(index + 1, workers),
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

        for round_index in range(rounds):
            task = create_task(
                db_path,
                name="claim-race-{:03d}".format(round_index + 1),
                group_id=None,
                base_parameters={"round": round_index + 1},
                steps=[{"name": "only-step", "overrides": {}}],
            )
            expected_ids.append(task["id"])
            _wait_at_barrier(
                start_barrier,
                deadline,
                ROUND_TIMEOUT_SECONDS,
                "round {} start".format(round_index + 1),
            )
            _wait_at_barrier(
                finished_barrier,
                deadline,
                ROUND_TIMEOUT_SECONDS,
                "round {} finish".format(round_index + 1),
            )
            for result_index in range(workers):
                results.append(
                    _get_from_queue(
                        result_queue,
                        deadline,
                        RESULT_TIMEOUT_SECONDS,
                        "round {} result {}/{}".format(
                            round_index + 1, result_index + 1, workers
                        ),
                    )
                )
    finally:
        _abort_barriers((ready_barrier, start_barrier, finished_barrier))
        cleanup_errors = _shutdown_started_processes(started_processes)

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
        "{} exited with {}".format(process.name, process.exitcode)
        for process in started_processes
        if process.exitcode != 0
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
        len(ready_records) == workers
        and len(ready_pids) == workers
        and len(ready_connection_keys) == workers
        and all(record["database_file"] == expected_database for record in ready_records)
        and not any(record.get("error") for record in ready_records)
    )

    expected_pairs = {
        (round_index, "worker-{}".format(worker_index))
        for round_index in range(rounds)
        for worker_index in range(workers)
    }
    pair_counts = Counter(
        (record.get("round"), record.get("worker_id")) for record in results
    )
    missing_pairs = sum(1 for pair in expected_pairs if pair_counts[pair] == 0)
    duplicate_pairs = sum(
        count - 1
        for pair, count in pair_counts.items()
        if pair in expected_pairs and count > 1
    )
    unexpected_pairs = sum(
        count for pair, count in pair_counts.items() if pair not in expected_pairs
    )
    result_pair_anomalies = missing_pairs + duplicate_pairs + unexpected_pairs

    winners_by_round: Dict[int, List[Dict[str, Any]]] = {
        index: [] for index in range(rounds)
    }
    for record in results:
        round_index = record.get("round")
        if (
            isinstance(round_index, int)
            and 0 <= round_index < rounds
            and record.get("task_id") is not None
            and record.get("error") is None
        ):
            winners_by_round[round_index].append(record)

    winner_ids = [
        record["task_id"]
        for winner_list in winners_by_round.values()
        for record in winner_list
    ]
    winner_counts = Counter(winner_ids)
    duplicate_claims = sum(
        count - 1 for count in winner_counts.values() if count > 1
    )
    anomalous_rounds = sum(
        1
        for index, expected_id in enumerate(expected_ids)
        if [record["task_id"] for record in winners_by_round[index]] != [expected_id]
    )

    connection = sqlite3.connect(db_path)
    try:
        database_rows = connection.execute(
            "SELECT id, status, claimed_by, claim_token FROM tasks ORDER BY id"
        ).fetchall()
    finally:
        connection.close()
    database_by_id = {row[0]: row for row in database_rows}

    claim_metadata_anomalies = 0
    observed_tokens: List[str] = []
    for winner_list in winners_by_round.values():
        for record in winner_list:
            token = record.get("claim_token")
            database_row = database_by_id.get(record["task_id"])
            metadata_valid = (
                record.get("task_status") == "claimed"
                and record.get("task_claimed_by") == record.get("worker_id")
                and isinstance(token, str)
                and bool(token)
                and database_row is not None
                and database_row[1] == "claimed"
                and database_row[2] == record.get("worker_id")
                and database_row[3] == token
            )
            if not metadata_valid:
                claim_metadata_anomalies += 1
            if isinstance(token, str) and token:
                observed_tokens.append(token)
    claim_metadata_anomalies += len(observed_tokens) - len(set(observed_tokens))

    attempted_calls = sum(record.get("attempted") is True for record in results)
    pending_tasks = sum(row[1] == "pending" for row in database_rows)
    claimed_tasks = sum(row[1] == "claimed" for row in database_rows)
    stats: Dict[str, Any] = {
        "start_method": context.get_start_method(),
        "workers": workers,
        "ready_processes": len(ready_pids),
        "ready_connections": len(ready_connection_keys),
        "ready_connection_records": ready_connection_records,
        "ready_connections_verified": ready_connections_verified,
        "rounds": rounds,
        "claim_attempts": attempted_calls,
        "observed_results": len(results),
        "result_pair_anomalies": result_pair_anomalies,
        "unique_winners": len(winner_counts),
        "duplicate_claims": duplicate_claims,
        "anomalous_rounds": anomalous_rounds,
        "claim_metadata_anomalies": claim_metadata_anomalies,
        "errors": errors,
        "total_tasks": len(database_rows),
        "claimed_tasks": claimed_tasks,
        "pending_tasks": pending_tasks,
    }
    expected_results = rounds * workers
    stats["passed"] = (
        stats["start_method"] == "spawn"
        and ready_connections_verified
        and not errors
        and stats["observed_results"] == expected_results
        and stats["claim_attempts"] == expected_results
        and result_pair_anomalies == 0
        and stats["unique_winners"] == rounds
        and duplicate_claims == 0
        and anomalous_rounds == 0
        and claim_metadata_anomalies == 0
        and stats["total_tasks"] == rounds
        and stats["claimed_tasks"] == rounds
        and stats["pending_tasks"] == 0
    )

    if not quiet or not stats["passed"]:
        print(_format_stats(stats))
        for error in errors:
            print("  ERROR: {}".format(error))
    if not stats["passed"]:
        raise AssertionError("multiprocess claim proof failed")
    return stats


def run_claim_proof(
    database_path: Optional[os.PathLike] = None,
    *,
    rounds: int = 40,
    workers: int = 8,
    quiet: bool = False,
) -> Dict[str, Any]:
    """Run synchronized one-task races and return machine-checkable statistics."""

    if rounds < 1:
        raise ValueError("rounds must be positive")
    if workers < 2 or workers > 10:
        raise ValueError("workers must be between 2 and the stated maximum of 10")

    temporary_directory = None
    if database_path is None:
        temporary_directory = tempfile.TemporaryDirectory(prefix="taskboard-claim-")
        db_path = str(Path(temporary_directory.name) / "proof.db")
    else:
        db_path = os.fspath(database_path)
        if Path(db_path).exists():
            raise ValueError("proof database must not already exist: {}".format(db_path))

    previous_database = os.environ.get("TASKBOARD_DB_PATH")
    had_database_environment = "TASKBOARD_DB_PATH" in os.environ
    os.environ["TASKBOARD_DB_PATH"] = db_path
    try:
        return _run_claim_proof(
            db_path, rounds=rounds, workers=workers, quiet=quiet
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
    parser.add_argument("--rounds", type=int, default=40)
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()
    try:
        run_claim_proof(rounds=args.rounds, workers=args.workers)
    except BaseException as exc:
        print("claim proof terminated: {}".format(_error_text(exc)))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
