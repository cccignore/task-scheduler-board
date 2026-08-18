"""Request models for the HTTP API."""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class GroupCreate(BaseModel):
    name: str
    overrides: Dict[str, Any] = Field(default_factory=dict)


class GroupUpdate(BaseModel):
    name: Optional[str] = None
    overrides: Optional[Dict[str, Any]] = None


class StepCreate(BaseModel):
    name: str
    overrides: Dict[str, Any] = Field(default_factory=dict)


class TaskCreate(BaseModel):
    name: str
    group_id: Optional[int] = None
    base_parameters: Dict[str, Any] = Field(default_factory=dict)
    steps: List[StepCreate]


class WorkerRequest(BaseModel):
    worker_id: str


class ClaimCredentials(WorkerRequest):
    claim_token: str


class CompleteStepRequest(ClaimCredentials):
    success: bool


class WorkerSpawnRequest(BaseModel):
    count: int = Field(default=1, ge=1, le=10)
    step_seconds: float = Field(default=1.2, ge=0.2, le=10.0)
    fail_rate: float = Field(default=0.0, ge=0.0, le=1.0)


class ProofRequest(BaseModel):
    rounds: int = Field(default=12, ge=1, le=40)
    workers: int = Field(default=6, ge=2, le=10)
