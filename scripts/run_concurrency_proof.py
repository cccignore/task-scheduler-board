#!/usr/bin/env python3
"""Attack claim-next using real spawn processes and fresh DB connections."""

import argparse
import multiprocessing
import os
import queue
import sqlite3
import sys
import tempfile
from collections import Counter
from pathlib import Path
from threading import BrokenBarrierError
from typing import Any, Dict, List, Optional


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _claim_worker(
    db_path: str,
    worker_id: str,
    rounds: int,
    ready_barrier: Any,
    start_barrier: Any,
    finished_barrier: Any,
    result_queue: Any,
) -> None:
    """Stay alive across rounds; every claim call still opens its own connection."""

    claim = None
    import_error = None
    try:
        from app.services import claim_next_task

        claim = claim_next_task
    except BaseException as exc:  # reported to the parent, including import failures
        import_error = "{}: {}".format(type(exc).__name__, exc)

    try:
        ready_barrier.wait(timeout=60)
        for round_index in range(rounds):
            start_barrier.wait(timeout=60)
            task_id = None
            error = import_error
            if error is None:
                try:
                    task = claim(db_path, worker_id)
                    task_id = None if task is None else task["id"]
                except BaseException as exc:
                    error = "{}: {}".format(type(exc).__name__, exc)
            result_queue.put((round_index, worker_id, task_id, error))
            finished_barrier.wait(timeout=60)
    except BrokenBarrierError:
        result_queue.put((-1, worker_id, None, "multiprocessing barrier broke"))


def _format_stats(stats: Dict[str, Any]) -> str:
    outcome = "PASS" if stats["passed"] else "FAIL"
    return "\n".join(
        [
            "REAL MULTIPROCESS CLAIM PROOF: {}".format(outcome),
            "  start method              : {}".format(stats["start_method"]),
            "  worker processes          : {}".format(stats["workers"]),
            "  one-task race rounds      : {}".format(stats["rounds"]),
            "  independent claim calls   : {}".format(stats["claim_attempts"]),
            "  expected unique winners   : {}".format(stats["rounds"]),
            "  actual unique winners     : {}".format(stats["unique_winners"]),
            "  duplicate claims          : {}".format(stats["duplicate_claims"]),
            "  missing/extra winner rounds: {}".format(stats["anomalous_rounds"]),
            "  child errors              : {}".format(len(stats["errors"])),
            "  pending tasks after proof : {}".format(stats["pending_tasks"]),
        ]
    )


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

    # Prevent app.main's module-level app from opening a different database in
    # spawned interpreters. Domain calls below still receive the explicit path.
    os.environ["TASKBOARD_DB_PATH"] = db_path
    from app.db import initialize_database
    from app.services import create_task

    initialize_database(db_path)
    context = multiprocessing.get_context("spawn")
    ready_barrier = context.Barrier(workers + 1)
    start_barrier = context.Barrier(workers + 1)
    finished_barrier = context.Barrier(workers + 1)
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
                result_queue,
            ),
        )
        for index in range(workers)
    ]

    results: List[Any] = []
    expected_ids: List[int] = []
    try:
        for process in processes:
            process.start()
        ready_barrier.wait(timeout=60)

        for round_index in range(rounds):
            task = create_task(
                db_path,
                name="claim-race-{:03d}".format(round_index + 1),
                group_id=None,
                base_parameters={"round": round_index + 1},
                steps=[{"name": "only-step", "overrides": {}}],
            )
            expected_ids.append(task["id"])
            start_barrier.wait(timeout=60)
            finished_barrier.wait(timeout=60)
            for _ in range(workers):
                results.append(result_queue.get(timeout=30))
    finally:
        for process in processes:
            process.join(timeout=10)
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)

    while True:
        try:
            results.append(result_queue.get_nowait())
        except queue.Empty:
            break

    exit_errors = [
        "{} exited with {}".format(process.name, process.exitcode)
        for process in processes
        if process.exitcode != 0
    ]
    errors = [item[3] for item in results if item[3] is not None] + exit_errors
    winners_by_round: Dict[int, List[int]] = {index: [] for index in range(rounds)}
    for round_index, _worker_id, task_id, error in results:
        if 0 <= round_index < rounds and task_id is not None and error is None:
            winners_by_round[round_index].append(task_id)

    winner_ids = [
        task_id for winner_list in winners_by_round.values() for task_id in winner_list
    ]
    counts = Counter(winner_ids)
    duplicate_claims = sum(count - 1 for count in counts.values() if count > 1)
    anomalous_rounds = sum(
        1
        for index, expected_id in enumerate(expected_ids)
        if winners_by_round[index] != [expected_id]
    )

    connection = sqlite3.connect(db_path)
    try:
        task_counts = connection.execute(
            "SELECT COUNT(*), SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) "
            "FROM tasks"
        ).fetchone()
    finally:
        connection.close()

    stats: Dict[str, Any] = {
        "start_method": context.get_start_method(),
        "workers": workers,
        "rounds": rounds,
        "claim_attempts": rounds * workers,
        "unique_winners": len(counts),
        "duplicate_claims": duplicate_claims,
        "anomalous_rounds": anomalous_rounds,
        "errors": errors,
        "total_tasks": task_counts[0],
        "pending_tasks": task_counts[1] or 0,
    }
    stats["passed"] = (
        stats["start_method"] == "spawn"
        and not errors
        and stats["unique_winners"] == rounds
        and duplicate_claims == 0
        and anomalous_rounds == 0
        and stats["total_tasks"] == rounds
        and stats["pending_tasks"] == 0
    )

    if not quiet or not stats["passed"]:
        print(_format_stats(stats))
        for error in errors:
            print("  ERROR: {}".format(error))
    if temporary_directory is not None:
        temporary_directory.cleanup()
    if not stats["passed"]:
        raise AssertionError("multiprocess claim proof failed")
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rounds", type=int, default=40)
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()
    try:
        run_claim_proof(rounds=args.rounds, workers=args.workers)
    except BaseException as exc:
        print("claim proof terminated: {}: {}".format(type(exc).__name__, exc))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
