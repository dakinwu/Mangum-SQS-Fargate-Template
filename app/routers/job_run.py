import json

from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session, select

from app.base.utils.logging import log_event, log_warning
from app.config import get_settings
from app.core.db import get_db
from app.models.job_run import JobRun
from app.models.sub_task import SubTask
from app.models.sub_task_type import SubTaskType
from app.schemas.job import CreateJobRunRequest, JobRunResponse
from app.worker.queue_backend import get_queue_backend

router = APIRouter(prefix="/job-run", tags=["job-run"])


@router.post("/", response_model=JobRunResponse)
def create_job_run(request: CreateJobRunRequest, db: Session = Depends(get_db)):
    types = db.exec(
        select(SubTaskType).where(SubTaskType.name.in_(request.sub_task_type_names), SubTaskType.is_enabled == True)  # noqa: E712
    ).all()
    found_names = {t.name for t in types}
    missing = set(request.sub_task_type_names) - found_names
    if missing:
        raise HTTPException(status_code=400, detail=f"Unknown or disabled sub_task_type_names: {sorted(missing)}")

    run = JobRun(case_id=request.case_id)
    db.add(run)
    db.commit()
    db.refresh(run)

    tasks = [SubTask(job_run_id=run.run_id, sub_task_type_id=t.id) for t in types]
    db.add_all(tasks)
    db.commit()
    for task in tasks:
        db.refresh(task)

    settings = get_settings()
    if not settings.sqs_queue_url:
        log_warning("SQS_QUEUE_URL not configured - tasks created but not enqueued", module="JobRun", run_id=run.run_id)
    else:
        backend = get_queue_backend()
        name_by_type_id = {t.id: t.name for t in types}
        for task in tasks:
            body = json.dumps(
                {
                    "task_id": task.task_id,
                    "run_id": run.run_id,
                    "sub_task_type_name": name_by_type_id[task.sub_task_type_id],
                }
            )
            message_id = backend.send(body)
            log_event("TASK_ENQUEUED", module="JobRun", data={"task_id": task.task_id, "message_id": message_id})

    db.refresh(run)
    return run
