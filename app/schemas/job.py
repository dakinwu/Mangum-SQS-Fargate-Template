from datetime import datetime
from typing import Optional

from pydantic import BaseModel

from app.models.job_run import RunStatus
from app.models.sub_task import TaskStatus


class CreateJobRunRequest(BaseModel):
    case_id: str
    sub_task_type_names: list[str]


class SubTaskResponse(BaseModel):
    task_id: str
    job_run_id: str
    sub_task_type_id: int
    status: TaskStatus
    context: Optional[dict] = None
    result: Optional[dict] = None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class JobRunResponse(BaseModel):
    run_id: str
    case_id: str
    status: RunStatus
    created_at: datetime
    updated_at: datetime
    sub_tasks: list[SubTaskResponse] = []

    model_config = {"from_attributes": True}


class SubTaskTypeResponse(BaseModel):
    id: int
    name: str
    display_name: str
    description: str
    is_enabled: bool

    model_config = {"from_attributes": True}
