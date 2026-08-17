"""FastAPI entry point for the task scheduling dashboard."""

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
    TaskCreate,
    WorkerRequest,
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
    list_tasks,
    seed_demo,
    start_task,
    update_group,
)


PathLike = Union[str, Path]


def _model_dict(model: Any) -> Dict[str, Any]:
    """Support the Pydantic versions paired with supported FastAPI releases."""

    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()


def create_app(database_path: Optional[PathLike] = None) -> FastAPI:
    """Build an isolated application, optionally bound to an explicit DB file."""

    selected_database = resolve_database_path(database_path)
    initialize_database(selected_database)
    application = FastAPI(
        title="Task Scheduling Dashboard",
        version="1.0.0",
        description="Concurrent-safe task claiming and idempotent step reporting.",
    )
    application.state.database_path = selected_database

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
