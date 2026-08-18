"""A real worker process loop: claim, start, and execute tasks step by step.

The dashboard's worker manager and ``scripts/run_worker.py`` both spawn this
loop in independent OS processes, mirroring the deployment model in the
assignment: every worker owns its own database connections and competes for
tasks only through transactions.
"""

import os
import random
import time
from typing import Any, Dict, Optional

from .services import ConflictError, claim_next_task, complete_step, start_task


def _log(worker_id: str, message: str) -> None:
    print(
        "[{}] {} {}".format(time.strftime("%H:%M:%S"), worker_id, message),
        flush=True,
    )


def _execute_task(
    db_path: str,
    worker_id: str,
    claim: Dict[str, Any],
    step_seconds: float,
    fail_rate: float,
    rng: random.Random,
) -> None:
    task = claim["task"]
    token = claim["claim_token"]
    _log(worker_id, "claimed task #{} ({!r})".format(task["id"], task["name"]))
    task = start_task(db_path, task["id"], worker_id, token)

    while task["status"] == "running" and task["current_step"] is not None:
        step = task["current_step"]
        time.sleep(step_seconds)
        success = rng.random() >= fail_rate
        report = complete_step(
            db_path,
            task["id"],
            step["sequence"],
            worker_id,
            token,
            success,
        )
        _log(
            worker_id,
            "task #{} step {} ({!r}) -> {}".format(
                task["id"],
                step["sequence"],
                step["name"],
                "success" if success else "FAILURE",
            ),
        )
        task = report["task"]
    _log(worker_id, "task #{} finished as {}".format(task["id"], task["status"]))


def run_worker_loop(
    db_path: str,
    worker_id: str,
    *,
    poll_seconds: float = 1.0,
    step_seconds: float = 1.2,
    fail_rate: float = 0.0,
    max_tasks: Optional[int] = None,
    lease_seconds: Optional[float] = None,
    seed: Optional[int] = None,
) -> int:
    """Claim and execute tasks until stopped or ``max_tasks`` is reached."""

    rng = random.Random(seed)
    completed = 0
    _log(worker_id, "online (pid {}), polling every {:.1f}s".format(
        os.getpid(), poll_seconds
    ))
    try:
        while max_tasks is None or completed < max_tasks:
            claim = claim_next_task(
                db_path, worker_id, lease_seconds=lease_seconds
            )
            if claim is None:
                time.sleep(poll_seconds)
                continue
            try:
                _execute_task(
                    db_path, worker_id, claim, step_seconds, fail_rate, rng
                )
            except ConflictError as exc:
                # Losing a lease mid-task is expected behaviour, not a crash.
                _log(worker_id, "lost task ownership: {}".format(exc))
            completed += 1
    except KeyboardInterrupt:
        pass
    _log(worker_id, "offline after {} task(s)".format(completed))
    return 0
