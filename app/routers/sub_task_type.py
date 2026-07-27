from fastapi import APIRouter, Depends
from sqlmodel import Session, select

from app.core.db import get_db
from app.models.sub_task_type import SubTaskType
from app.schemas.job import SubTaskTypeResponse

router = APIRouter(prefix="/sub-task-type", tags=["sub-task-type"])


@router.get("/", response_model=list[SubTaskTypeResponse])
def list_sub_task_types(db: Session = Depends(get_db)):
    return db.exec(select(SubTaskType)).all()
