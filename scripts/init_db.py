"""One-shot local DB bootstrap: `python scripts/init_db.py`.

Uses SQLModel.metadata.create_all() rather than migrations - this is a
template, not a project with a schema history to manage. If you outgrow
this, Alembic is the natural next step (autogenerate a first revision from
these same models).
"""

from sqlmodel import SQLModel

from app.core.db import engine
from app.models.job_run import JobRun  # noqa: F401
from app.models.sub_task import SubTask  # noqa: F401
from app.models.sub_task_type import SubTaskType  # noqa: F401

if __name__ == "__main__":
    SQLModel.metadata.create_all(engine)
    print("Tables created.")
