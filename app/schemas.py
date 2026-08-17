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
