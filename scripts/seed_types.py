"""Seeds the two placeholder SubTaskType rows this template ships with:
`python scripts/seed_types.py`.
"""

from sqlmodel import Session, select

from app.core.db import engine

# SubTask/JobRun aren't used directly here, but SQLModel's mapper needs every
# model imported at least once so it can resolve the string-based forward
# refs inside SubTaskType.sub_tasks / SubTask.job_run / SubTask.sub_task_type.
from app.models.job_run import JobRun  # noqa: F401
from app.models.sub_task import SubTask  # noqa: F401
from app.models.sub_task_type import SubTaskType

SEED_TYPES = [
    {"name": "quick", "display_name": "Quick Job", "description": "Placeholder job that always succeeds after a short delay."},
    {"name": "flaky", "display_name": "Flaky Job", "description": "Placeholder job that sometimes fails (terminal or transient) to exercise both error paths."},
]

if __name__ == "__main__":
    with Session(engine) as session:
        for seed in SEED_TYPES:
            existing = session.exec(select(SubTaskType).where(SubTaskType.name == seed["name"])).first()
            if existing:
                print(f"Skipping existing type: {seed['name']}")
                continue
            session.add(SubTaskType(**seed))
        session.commit()
    print("Seed complete.")
