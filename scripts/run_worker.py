#!/usr/bin/env python3
"""Run one or more real worker processes from the command line.

The same loop can be started from the dashboard's worker panel; this CLI stays
for terminal-driven demos.  Each worker is an independent OS process with its
own database connections, exactly like the deployment model in the assignment.
"""

import argparse
import multiprocessing
import sys
from pathlib import Path
from typing import Any, Dict


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _spawned_worker(db_path: str, worker_id: str, options: Dict[str, Any]) -> None:
    from app.worker import run_worker_loop

    run_worker_loop(db_path, worker_id, **options)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workers", type=int, default=2, help="worker processes")
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    parser.add_argument("--step-seconds", type=float, default=1.2)
    parser.add_argument(
        "--fail-rate",
        type=float,
        default=0.0,
        help="probability that a step reports failure (0..1)",
    )
    parser.add_argument(
        "--max-tasks",
        type=int,
        default=None,
        help="stop each worker after N tasks (default: run until Ctrl+C)",
    )
    parser.add_argument("--lease-seconds", type=float, default=None)
    parser.add_argument("--db", default=None, help="database path override")
    args = parser.parse_args()
    if args.workers < 1:
        raise SystemExit("--workers must be at least 1")
    if not 0.0 <= args.fail_rate <= 1.0:
        raise SystemExit("--fail-rate must be between 0 and 1")

    from app.db import database_path, initialize_database
    from app.worker import run_worker_loop

    db_path = initialize_database(database_path(args.db))
    options = {
        "poll_seconds": args.poll_seconds,
        "step_seconds": args.step_seconds,
        "fail_rate": args.fail_rate,
        "max_tasks": args.max_tasks,
        "lease_seconds": args.lease_seconds,
    }
    print("workers -> {}".format(db_path), flush=True)
    print("press Ctrl+C to stop", flush=True)

    if args.workers == 1:
        return run_worker_loop(db_path, "worker-1", **options)

    context = multiprocessing.get_context("spawn")
    processes = [
        context.Process(
            target=_spawned_worker,
            name="demo-worker-{}".format(index + 1),
            args=(db_path, "worker-{}".format(index + 1), dict(options, seed=index)),
        )
        for index in range(args.workers)
    ]
    for process in processes:
        process.start()
    try:
        for process in processes:
            process.join()
    except KeyboardInterrupt:
        for process in processes:
            process.terminate()
        for process in processes:
            process.join(timeout=5)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
