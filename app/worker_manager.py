"""Spawn and supervise real worker OS processes from the dashboard.

The manager only ever starts genuine ``multiprocessing`` spawn processes that
run :func:`app.worker.run_worker_loop`; each one opens its own database
connections and competes for tasks purely through transactions, so the
dashboard demo exercises exactly the same code path as terminal workers.
"""

import multiprocessing
import threading
from typing import Any, Dict, List

from .services import ValidationError, utc_now


MAX_MANAGED_WORKERS = 10  # The assignment's stated scale ceiling.


def _managed_worker(
    db_path: str, worker_id: str, step_seconds: float, fail_rate: float, seed: int
) -> None:
    from app.worker import run_worker_loop

    run_worker_loop(
        db_path,
        worker_id,
        step_seconds=step_seconds,
        fail_rate=fail_rate,
        seed=seed,
    )


class WorkerManager:
    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._context = multiprocessing.get_context("spawn")
        self._lock = threading.Lock()
        self._workers: Dict[str, Dict[str, Any]] = {}
        self._counter = 0

    def _prune_dead_locked(self) -> None:
        for name in [
            name
            for name, item in self._workers.items()
            if not item["process"].is_alive()
        ]:
            self._workers.pop(name)

    def spawn(
        self, count: int = 1, step_seconds: float = 1.2, fail_rate: float = 0.0
    ) -> List[Dict[str, Any]]:
        if not 1 <= count <= MAX_MANAGED_WORKERS:
            raise ValidationError(
                "count must be between 1 and {}".format(MAX_MANAGED_WORKERS)
            )
        if not 0.2 <= step_seconds <= 10.0:
            raise ValidationError("step_seconds must be between 0.2 and 10")
        if not 0.0 <= fail_rate <= 1.0:
            raise ValidationError("fail_rate must be between 0 and 1")

        with self._lock:
            self._prune_dead_locked()
            if len(self._workers) + count > MAX_MANAGED_WORKERS:
                raise ValidationError(
                    "at most {} workers may run at once; {} already running".format(
                        MAX_MANAGED_WORKERS, len(self._workers)
                    )
                )
            for _ in range(count):
                self._counter += 1
                name = "worker-{}".format(self._counter)
                process = self._context.Process(
                    target=_managed_worker,
                    name="managed-{}".format(name),
                    args=(self._db_path, name, step_seconds, fail_rate, self._counter),
                    daemon=True,
                )
                process.start()
                self._workers[name] = {
                    "process": process,
                    "started_at": utc_now(),
                    "step_seconds": step_seconds,
                    "fail_rate": fail_rate,
                }
        return self.describe()

    def describe(self) -> List[Dict[str, Any]]:
        with self._lock:
            self._prune_dead_locked()
            return [
                {
                    "worker_id": name,
                    "pid": item["process"].pid,
                    "alive": item["process"].is_alive(),
                    "started_at": item["started_at"],
                    "step_seconds": item["step_seconds"],
                    "fail_rate": item["fail_rate"],
                }
                for name, item in sorted(self._workers.items())
            ]

    def stop_all(self) -> int:
        with self._lock:
            stopped = 0
            for item in self._workers.values():
                process = item["process"]
                if process.is_alive():
                    process.terminate()
                    stopped += 1
            for item in self._workers.values():
                item["process"].join(timeout=5)
            self._workers.clear()
        return stopped
