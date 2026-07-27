"""Importing this package registers all models with SQLModel's mapper
registry. Relationships reference each other by string (e.g. SubTask.job_run
-> "JobRun"), so SQLAlchemy needs every model class imported at least once
before it configures mappers - otherwise whichever model happens to get
queried first raises InvalidRequestError trying to resolve a sibling it
hasn't seen yet. Importing `app.models` (or any of its submodules, since
Python runs the package's __init__.py first) makes that a non-issue.
"""

from app.models.job_run import JobRun  # noqa: F401
from app.models.sub_task import SubTask  # noqa: F401
from app.models.sub_task_type import SubTaskType  # noqa: F401
