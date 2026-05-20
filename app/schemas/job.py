import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator


class JobCreate(BaseModel):
    """Request body for POST /jobs."""
    payload: dict[str, Any]
    priority: str = "normal"

    @field_validator("priority")
    @classmethod
    def priority_must_be_valid(cls, v: str) -> str:
        allowed = {"critical", "high", "normal"}
        if v not in allowed:
            raise ValueError(f"priority must be one of {allowed}")
        return v


class JobResponse(BaseModel):
    """Response body for job endpoints."""
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    status: str
    priority: str
    payload: dict[str, Any]
    result: dict[str, Any] | None
    retry_count: int
    error: str | None
    created_at: datetime
    updated_at: datetime


class QueueDepthResponse(BaseModel):
    """Response body for GET /queues/metrics."""
    depths: dict[str, int]
    total: int
    high_watermark: int
    low_watermark: int
    accepting_jobs: bool
