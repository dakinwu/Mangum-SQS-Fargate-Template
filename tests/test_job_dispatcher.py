"""Requires Postgres (docker compose up -d db) - see tests/conftest.py."""

import pytest
from sqlmodel import Session

from app.core.db import engine
from app.models.job_run import JobRun, RunStatus
from app.models.sub_task import SubTask, TaskStatus
from app.services.job_dispatcher import dispatcher


@pytest.mark.asyncio
async def test_claim_skips_task_already_completed(monkeypatch, db_session, sub_task):
    """The claim's WHERE excludes terminal states, so a dispatch that lands
    on an already-Completed task (e.g. a very late redelivery) must not
    re-run the processor or disturb the recorded result."""
    sub_task.status = TaskStatus.COMPLETED
    sub_task.result = {"outcome": "ok"}
    db_session.add(sub_task)
    db_session.commit()

    call_count = 0

    async def fake_processor(*, task_id, run_id):
        nonlocal call_count
        call_count += 1
        return {"outcome": "should not run"}

    monkeypatch.setitem(dispatcher.PROCESS_JOBS_MAP, "quick", fake_processor)

    await dispatcher.dispatch_task(task_id=sub_task.task_id, run_id=sub_task.job_run_id, sub_task_type_name="quick")

    assert call_count == 0
    with Session(engine) as session:
        refreshed = session.get(SubTask, sub_task.task_id)
        assert refreshed.result == {"outcome": "ok"}


@pytest.mark.asyncio
async def test_claim_allows_reclaiming_stale_processing_task(monkeypatch, db_session, sub_task):
    """The claim's WHERE deliberately also matches Processing - not just
    Pending - so a task whose previous attempt crashed mid-flight (message
    redelivered after visibility expired) can be picked up again instead of
    being stuck forever. This is the recovery case the permissive WHERE
    exists for (see docs/architecture-notes.md #7)."""
    sub_task.status = TaskStatus.PROCESSING
    db_session.add(sub_task)
    db_session.commit()

    call_count = 0

    async def fake_processor(*, task_id, run_id):
        nonlocal call_count
        call_count += 1
        return {"outcome": "recovered"}

    monkeypatch.setitem(dispatcher.PROCESS_JOBS_MAP, "quick", fake_processor)

    await dispatcher.dispatch_task(task_id=sub_task.task_id, run_id=sub_task.job_run_id, sub_task_type_name="quick")

    assert call_count == 1
    with Session(engine) as session:
        refreshed = session.get(SubTask, sub_task.task_id)
        assert refreshed.status == TaskStatus.COMPLETED


@pytest.mark.asyncio
async def test_terminal_failure_marks_task_and_run_failed(monkeypatch, sub_task):
    async def failing_processor(*, task_id, run_id):
        raise RuntimeError("deterministic bug")

    monkeypatch.setitem(dispatcher.PROCESS_JOBS_MAP, "quick", failing_processor)

    with pytest.raises(RuntimeError):
        await dispatcher.dispatch_task(task_id=sub_task.task_id, run_id=sub_task.job_run_id, sub_task_type_name="quick")

    with Session(engine) as session:
        task = session.get(SubTask, sub_task.task_id)
        run = session.get(JobRun, sub_task.job_run_id)
        assert task.status == TaskStatus.FAILED
        assert run.status == RunStatus.FAILED


@pytest.mark.asyncio
async def test_transient_error_leaves_task_reclaimable(monkeypatch, sub_task):
    from app.services.job_dispatcher.dispatcher import TransientDispatchError

    async def flaky_processor(*, task_id, run_id):
        raise ConnectionError("simulated network blip")

    monkeypatch.setitem(dispatcher.PROCESS_JOBS_MAP, "quick", flaky_processor)

    with pytest.raises(TransientDispatchError):
        await dispatcher.dispatch_task(task_id=sub_task.task_id, run_id=sub_task.job_run_id, sub_task_type_name="quick")

    with Session(engine) as session:
        task = session.get(SubTask, sub_task.task_id)
        # Left PROCESSING (claimed but not completed), not FAILED - it's
        # still re-claimable by the next redelivery.
        assert task.status == TaskStatus.PROCESSING
