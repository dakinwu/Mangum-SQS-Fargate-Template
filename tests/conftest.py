"""Test fixtures.

Requires a real Postgres reachable via the settings in .env / the
environment (docker compose up -d db is enough - see README.md). Tests do
NOT use SQLite: SubTask.context/result are Postgres JSONB columns, and the
sync-vs-async session split under test is itself part of what these tests
demonstrate.

Isolation strategy: each test gets fresh rows, and a function-scoped
autouse fixture truncates the three tables after every test. This is
simpler and more robust here than a rolled-back transaction, because the
code under test (dispatch_task) opens and commits its own sessions
internally - a single wrapping transaction wouldn't see those commits.
"""

import asyncio

import pytest
from fastapi.testclient import TestClient
from sqlmodel import SQLModel, Session, delete

from app.core.db import engine
from app.main import app
from app.models.job_run import JobRun
from app.models.sub_task import SubTask
from app.models.sub_task_type import SubTaskType
from app.worker.queue_backend import InMemoryQueueBackend, set_queue_backend


@pytest.fixture(scope="session", autouse=True)
def _create_tables():
    SQLModel.metadata.create_all(engine)
    yield


@pytest.fixture(autouse=True)
def _clean_tables():
    yield
    with Session(engine) as session:
        session.exec(delete(SubTask))
        session.exec(delete(JobRun))
        session.exec(delete(SubTaskType))
        session.commit()


@pytest.fixture
def db_session():
    with Session(engine) as session:
        yield session


@pytest.fixture
def memory_queue():
    backend = InMemoryQueueBackend()
    set_queue_backend(backend)
    yield backend


@pytest.fixture
def client(memory_queue):
    return TestClient(app)


@pytest.fixture
def sub_task_type(db_session):
    t = SubTaskType(name="quick", display_name="Quick Job")
    db_session.add(t)
    db_session.commit()
    db_session.refresh(t)
    return t


@pytest.fixture
def job_run(db_session):
    run = JobRun(case_id="test-case")
    db_session.add(run)
    db_session.commit()
    db_session.refresh(run)
    return run


@pytest.fixture
def sub_task(db_session, job_run, sub_task_type):
    task = SubTask(job_run_id=job_run.run_id, sub_task_type_id=sub_task_type.id)
    db_session.add(task)
    db_session.commit()
    db_session.refresh(task)
    return task


@pytest.fixture(scope="session")
def event_loop():
    """Session-scoped so every async test shares one event loop, matching
    the module-level async_engine created once in app/core/db.py at import
    time - asyncpg connections are bound to the loop they were created on,
    so a fresh per-test loop (pytest-asyncio's default) would break the
    pooled connections after the first test. This overrides a fixture
    pytest-asyncio provides itself, which newer versions deprecate in favor
    of `asyncio_default_fixture_loop_scope` - that ini option only affects
    async *fixtures*, not test functions themselves, so it doesn't fix this;
    overriding `event_loop` directly is still the working option as of
    pytest-asyncio 0.25.
    """
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


