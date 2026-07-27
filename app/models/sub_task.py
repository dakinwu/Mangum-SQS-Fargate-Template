import enum
import uuid
from datetime import datetime, timezone
from typing import Optional, TYPE_CHECKING

from sqlalchemy import TIMESTAMP, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import TEXT, Column, Field, Relationship, SQLModel

if TYPE_CHECKING:
    from app.models.job_run import JobRun
    from app.models.sub_task_type import SubTaskType


class TaskStatus(str, enum.Enum):
    PENDING = "Pending"
    PROCESSING = "Processing"
    COMPLETED = "Completed"
    FAILED = "Failed"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class SubTask(SQLModel, table=True):
    __tablename__ = "sub_tasks"

    task_id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True, max_length=36)
    sub_task_type_id: int = Field(
        sa_column=Column(ForeignKey("sub_task_types.id", ondelete="RESTRICT"), nullable=False, index=True)
    )
    job_run_id: str = Field(
        sa_column=Column(ForeignKey("job_runs.run_id", ondelete="CASCADE"), nullable=False, index=True)
    )

    context: Optional[dict] = Field(default=None, sa_column=Column(JSONB, nullable=True))
    result: Optional[dict] = Field(default=None, sa_column=Column(JSONB, nullable=True))

    status: TaskStatus = Field(default=TaskStatus.PENDING, sa_column=Column(TEXT, nullable=False))
    created_at: datetime = Field(default_factory=_utcnow, sa_column=Column(TIMESTAMP, nullable=False))
    updated_at: datetime = Field(
        default_factory=_utcnow,
        sa_column=Column(TIMESTAMP, nullable=False, onupdate=_utcnow),
    )

    job_run: Optional["JobRun"] = Relationship(back_populates="sub_tasks")
    sub_task_type: Optional["SubTaskType"] = Relationship(back_populates="sub_tasks")
