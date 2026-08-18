"""FastAPI entry point for the task scheduling dashboard."""

import logging
import sqlite3
import threading
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, Optional, Union

from fastapi import FastAPI, Request, status
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from .db import database_path as resolve_database_path
from .db import initialize_database
from .schemas import (
    ClaimCredentials,
    CompleteStepRequest,
    GroupCreate,
    GroupUpdate,
    ProofRequest,
    TaskCreate,
    WorkerRequest,
    WorkerSpawnRequest,
)
from .services import (
    ConflictError,
    NotFoundError,
    ServiceError,
    ValidationError,
    claim_next_task,
    complete_step,
    create_group,
    create_task,
    get_task,
    list_groups,
    list_operation_logs,
    list_tasks,
    reset_all,
    seed_demo,
    start_task,
    update_group,
)
from .worker_manager import MAX_MANAGED_WORKERS, WorkerManager


# The multiprocess proofs are CPU-heavy and briefly mutate process-wide state
# (a temporary TASKBOARD_DB_PATH), so at most one may run at a time.
_proof_lock = threading.Lock()


PathLike = Union[str, Path]


def _model_dict(model: Any) -> Dict[str, Any]:
    """Support the Pydantic versions paired with supported FastAPI releases."""

    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()


def create_app(database_path: Optional[PathLike] = None) -> FastAPI:
    """Build an isolated application, optionally bound to an explicit DB file."""

    # Surface the service layer's claim/report/reclaim audit trail on the
    # console without overriding a host application's logging setup.
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
        )

    selected_database = resolve_database_path(database_path)
    initialize_database(selected_database)
    worker_manager = WorkerManager(selected_database)

    @asynccontextmanager
    async def lifespan(_application: FastAPI):
        try:
            yield
        finally:
            worker_manager.stop_all()

    application = FastAPI(
        title="Task Scheduling Dashboard",
        version="1.0.0",
        description="Concurrent-safe task claiming and idempotent step reporting.",
        lifespan=lifespan,
    )
    application.state.database_path = selected_database
    application.state.worker_manager = worker_manager

    @application.exception_handler(NotFoundError)
    async def handle_not_found(_request: Request, exc: NotFoundError) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @application.exception_handler(ConflictError)
    async def handle_conflict(_request: Request, exc: ConflictError) -> JSONResponse:
        return JSONResponse(status_code=409, content={"detail": str(exc)})

    @application.exception_handler(ValidationError)
    async def handle_validation(
        _request: Request, exc: ValidationError
    ) -> JSONResponse:
        return JSONResponse(status_code=422, content={"detail": str(exc)})

    @application.exception_handler(ServiceError)
    async def handle_service_error(
        _request: Request, exc: ServiceError
    ) -> JSONResponse:
        return JSONResponse(status_code=400, content={"detail": str(exc)})

    @application.exception_handler(sqlite3.OperationalError)
    async def handle_database_busy(
        _request: Request, exc: sqlite3.OperationalError
    ) -> JSONResponse:
        # Write-lock contention beyond busy_timeout is retryable back-pressure,
        # not an internal error; anything else stays a genuine 500.
        message = str(exc).lower()
        if "locked" not in message and "busy" not in message:
            raise exc
        return JSONResponse(
            status_code=503,
            content={"detail": "database is busy, please retry"},
            headers={"Retry-After": "1"},
        )

    @application.get("/api/health")
    def health() -> Dict[str, str]:
        return {"status": "ok"}

    @application.get("/api/groups")
    def groups_index() -> Dict[str, Any]:
        return {"groups": list_groups(selected_database)}

    @application.post("/api/groups", status_code=status.HTTP_201_CREATED)
    def groups_create(body: GroupCreate) -> Dict[str, Any]:
        return {
            "group": create_group(
                selected_database,
                name=body.name,
                overrides=body.overrides,
            )
        }

    @application.patch("/api/groups/{group_id}")
    def groups_update(group_id: int, body: GroupUpdate) -> Dict[str, Any]:
        return {
            "group": update_group(
                selected_database,
                group_id=group_id,
                name=body.name,
                overrides=body.overrides,
            )
        }

    @application.get("/api/tasks")
    def tasks_index() -> Dict[str, Any]:
        # Returning full details keeps the polling dashboard stateless; SQLite
        # remains the sole source of business state.
        return {"tasks": list_tasks(selected_database)}

    @application.post("/api/tasks", status_code=status.HTTP_201_CREATED)
    def tasks_create(body: TaskCreate) -> Dict[str, Any]:
        return {
            "task": create_task(
                selected_database,
                name=body.name,
                group_id=body.group_id,
                base_parameters=body.base_parameters,
                steps=[_model_dict(step) for step in body.steps],
            )
        }

    @application.get("/api/tasks/{task_id}")
    def tasks_detail(task_id: int) -> Dict[str, Any]:
        return {"task": get_task(selected_database, task_id)}

    @application.post("/api/workers/claim-next")
    def workers_claim_next(body: WorkerRequest) -> Dict[str, Any]:
        claim = claim_next_task(selected_database, body.worker_id)
        return claim or {"task": None, "claim_token": None}

    @application.post("/api/tasks/{task_id}/start")
    def tasks_start(task_id: int, body: ClaimCredentials) -> Dict[str, Any]:
        return {
            "task": start_task(
                selected_database,
                task_id,
                worker_id=body.worker_id,
                claim_token=body.claim_token,
            )
        }

    @application.post("/api/tasks/{task_id}/steps/{sequence}/complete")
    def steps_complete(
        task_id: int, sequence: int, body: CompleteStepRequest
    ) -> Dict[str, Any]:
        # This route is intentionally synchronous: FastAPI runs simultaneous
        # requests in its worker threadpool, each with an independent DB link.
        return complete_step(
            selected_database,
            task_id=task_id,
            sequence=sequence,
            worker_id=body.worker_id,
            claim_token=body.claim_token,
            success=body.success,
        )

    @application.post("/api/demo", status_code=status.HTTP_201_CREATED)
    def demo_seed() -> Dict[str, Any]:
        result = seed_demo(selected_database)
        return dict(result, task=result["running_task"])

    @application.get("/api/logs")
    def logs_index(
        after_id: Optional[int] = None, limit: int = 200
    ) -> Dict[str, Any]:
        return {"logs": list_operation_logs(selected_database, after_id, limit)}

    @application.get("/api/workers/managed")
    def workers_index() -> Dict[str, Any]:
        return {
            "workers": worker_manager.describe(),
            "max_workers": MAX_MANAGED_WORKERS,
        }

    @application.post("/api/workers/managed", status_code=status.HTTP_201_CREATED)
    def workers_spawn(body: WorkerSpawnRequest) -> Dict[str, Any]:
        workers = worker_manager.spawn(
            count=body.count,
            step_seconds=body.step_seconds,
            fail_rate=body.fail_rate,
        )
        return {"workers": workers, "max_workers": MAX_MANAGED_WORKERS}

    @application.post("/api/workers/managed/stop")
    def workers_stop() -> Dict[str, Any]:
        stopped = worker_manager.stop_all()
        return {"stopped": stopped, "workers": []}

    @application.post("/api/proofs/claim")
    def proofs_claim(body: ProofRequest) -> Dict[str, Any]:
        # Runs in FastAPI's threadpool (sync route); the proof spawns real OS
        # processes racing over a temporary database, never the live board.
        if not _proof_lock.acquire(blocking=False):
            raise ConflictError("another proof is already running")
        try:
            from scripts.run_concurrency_proof import run_claim_proof

            stats = run_claim_proof(
                rounds=body.rounds, workers=body.workers, quiet=True
            )
            return {"kind": "claim", "stats": stats}
        except (AssertionError, ValueError) as exc:
            return JSONResponse(
                status_code=500,
                content={"detail": "并发认领证明未通过：{}".format(exc)},
            )
        finally:
            _proof_lock.release()

    @application.post("/api/proofs/idempotency")
    def proofs_idempotency() -> Dict[str, Any]:
        if not _proof_lock.acquire(blocking=False):
            raise ConflictError("another proof is already running")
        try:
            from scripts.run_idempotency_proof import run_idempotency_proof

            stats = run_idempotency_proof(quiet=True)
            return {"kind": "idempotency", "stats": stats}
        except (AssertionError, ValueError) as exc:
            return JSONResponse(
                status_code=500,
                content={"detail": "幂等写入证明未通过：{}".format(exc)},
            )
        finally:
            _proof_lock.release()

    @application.post("/api/reset")
    def reset_board() -> Dict[str, Any]:
        stopped = worker_manager.stop_all()
        reset_all(selected_database)
        return {"ok": True, "stopped_workers": stopped}

    static_directory = Path(__file__).resolve().parent / "static"
    if static_directory.is_dir():
        @application.get("/", include_in_schema=False)
        def dashboard() -> FileResponse:
            return FileResponse(str(static_directory / "index.html"))

        application.mount(
            "/static", StaticFiles(directory=str(static_directory)), name="static"
        )

    return application


app = create_app()
