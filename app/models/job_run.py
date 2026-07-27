import enum
import uuid
from datetime import datetime, timezone
from typing import List, TYPE_CHECKING

from sqlmodel import TEXT, Column, Field, Relationship, SQLModel
from sqlalchemy import TIMESTAMP

if TYPE_CHECKING:
    from app.models.sub_task import SubTask


class RunStatus(str, enum.Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class JobRun(SQLModel, table=True):
    __tablename__ = "job_runs"

    run_id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True, max_length=36)
    case_id: str = Field(nullable=False, index=True)

    status: RunStatus = Field(default=RunStatus.QUEUED, sa_column=Column(TEXT, nullable=False))
    created_at: datetime = Field(default_factory=_utcnow, sa_column=Column(TIMESTAMP, nullable=False))
    updated_at: datetime = Field(
        default_factory=_utcnow,
        sa_column=Column(TIMESTAMP, nullable=False, onupdate=_utcnow),
    )

    sub_tasks: List["SubTask"] = Relationship(back_populates="job_run")
