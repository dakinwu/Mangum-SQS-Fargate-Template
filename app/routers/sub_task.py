from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session

from app.core.db import get_db
from app.models.sub_task import SubTask
from app.schemas.job import SubTaskResponse

router = APIRouter(prefix="/sub-task", tags=["sub-task"])


@router.get("/{task_id}", response_model=SubTaskResponse)
def get_sub_task(task_id: str, db: Session = Depends(get_db)):
    task = db.get(SubTask, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="SubTask not found")
    return task
