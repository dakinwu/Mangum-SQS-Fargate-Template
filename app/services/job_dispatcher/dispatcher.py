"""Job dispatcher - resolves the correct processor for a sub-task type and
executes it with idempotent claiming and transient/terminal error handling.

Shared by the SQS consumer and (in principle) any other trigger mechanism.
"""

import traceback
from datetime import datetime, timezone

from sqlalchemy import update
from sqlalchemy.exc import InterfaceError, OperationalError
from sqlmodel import select

from app.base.utils.logging import log_error, log_warning
from app.core.db import get_async_db
from app.models.job_run import JobRun, RunStatus
from app.models.sub_task import SubTask, TaskStatus
from app.services.job_dispatcher.processors.example_flaky_job import ExampleFlakyJob
from app.services.job_dispatcher.processors.example_quick_job import ExampleQuickJob


class TransientDispatchError(Exception):
    """Dispatch failed on transient infrastructure (DB/network blip), not on
    the job itself. The task is NOT marked FAILED - the caller (the SQS
    consumer) must keep the message so it gets redelivered and retried."""


_TRANSIENT_INFRA_ERRORS = (TimeoutError, ConnectionError, OperationalError, InterfaceError)


def _is_transient_infra_error(exc: BaseException | None) -> bool:
    """Walk the explicit __cause__ chain (NOT __context__) looking for a
    transient infra error. Only __cause__ ("raise ... from e") is followed -
    __context__ would misclassify a deterministic error that merely occurred
    while an unrelated transient one was being handled, which would cause a
    poisoned message to redeliver forever."""
    seen: set[int] = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        if isinstance(exc, _TRANSIENT_INFRA_ERRORS):
            return True
        exc = exc.__cause__
    return False


PROCESS_JOBS_MAP = {
    "quick": ExampleQuickJob.process_job,
    "flaky": ExampleFlakyJob.process_job,
}


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


async def _rollup_job_run_status(db, run_id: str) -> None:
    """Recompute JobRun.status from its SubTask children. Called after every
    dispatch so the parent run reflects the latest child status without a
    client having to poll each sub-task individually."""
    result = await db.execute(select(SubTask).where(SubTask.job_run_id == run_id))
    tasks = result.scalars().all()
    if not tasks:
        return

    statuses = {t.status for t in tasks}
    if statuses <= {TaskStatus.COMPLETED}:
        new_status = RunStatus.COMPLETED
    elif TaskStatus.FAILED in statuses and statuses <= {TaskStatus.COMPLETED, TaskStatus.FAILED}:
        new_status = RunStatus.FAILED
    elif statuses & {TaskStatus.PENDING, TaskStatus.PROCESSING}:
        new_status = RunStatus.PROCESSING
    else:
        new_status = RunStatus.COMPLETED

    run = (await db.execute(select(JobRun).where(JobRun.run_id == run_id))).scalars().first()
    if run and run.status != new_status:
        run.status = new_status
        run.updated_at = _now()
        db.add(run)
        await db.commit()


async def dispatch_task(*, task_id: str, run_id: str, sub_task_type_name: str) -> None:
    """Execute the placeholder pipeline for a single SubTask.

    Creates a fresh async DB session, atomically claims the task, transitions
    the parent run QUEUED -> PROCESSING on first pickup, delegates to the
    processor, then rolls up status. On deterministic failure, marks task
    and run FAILED and re-raises. On transient infra failure, raises
    TransientDispatchError instead (task is NOT marked FAILED).
    """
    process_job_func = PROCESS_JOBS_MAP.get(sub_task_type_name)
    if not process_job_func:
        log_error("No processor for sub_task_type", module="Dispatcher", sub_task_type=sub_task_type_name, task_id=task_id)
        return

    try:
        async with get_async_db() as db:
            # Idempotency guard: atomically claim the task unless it's
            # already terminal (COMPLETED/FAILED). This is a conditional
            # UPDATE, not a SELECT-then-UPDATE, because a read-then-write
            # claim would have a TOCTOU race between the check and the
            # write; the atomic UPDATE's rowcount tells us definitively
            # whether we won the claim, no race window.
            #
            # Note this deliberately still matches an already-PROCESSING
            # task (re-claim), not just PENDING. Preventing two *truly
            # simultaneous* deliveries of the same message from both
            # processing it is SQS's job (VisibilityTimeout hides a message
            # from other consumers for the duration of this call) - this
            # claim's job is different: letting a *later, separate* delivery
            # (visibility expired because the previous attempt crashed
            # mid-flight) safely resume a task stuck in PROCESSING instead
            # of being locked out of it forever.
            claim_result = await db.execute(
                update(SubTask)
                .where(SubTask.task_id == task_id, SubTask.status.in_([TaskStatus.PENDING, TaskStatus.PROCESSING]))
                .values(status=TaskStatus.PROCESSING, updated_at=_now())
            )
            await db.commit()
            if claim_result.rowcount == 0:
                log_warning("Task already in a terminal state - skipping duplicate dispatch", module="Dispatcher", task_id=task_id)
                return

            run = (await db.execute(select(JobRun).where(JobRun.run_id == run_id))).scalars().first()
            if run and run.status == RunStatus.QUEUED:
                run.status = RunStatus.PROCESSING
                run.updated_at = _now()
                db.add(run)
                await db.commit()

            result = await process_job_func(task_id=task_id, run_id=run_id)

            task = (await db.execute(select(SubTask).where(SubTask.task_id == task_id))).scalars().first()
            if task:
                task.status = TaskStatus.COMPLETED
                task.result = result
                task.updated_at = _now()
                db.add(task)
                await db.commit()

            await _rollup_job_run_status(db, run_id)

    except Exception as e:
        if _is_transient_infra_error(e):
            # Task stays PENDING/PROCESSING (claim failed, or job raised
            # before completing) - both are re-claimable on redelivery, so
            # this is not treated as a terminal task failure.
            log_warning(
                "Transient infrastructure error dispatching task - leaving for SQS redelivery",
                module="Dispatcher", task_id=task_id, run_id=run_id, error=repr(e),
            )
            raise TransientDispatchError(repr(e)) from e

        log_error(
            "Error dispatching task", module="Dispatcher", task_id=task_id, run_id=run_id,
            error=repr(e), traceback=traceback.format_exc(),
        )
        try:
            async with get_async_db() as db:
                task = (await db.execute(select(SubTask).where(SubTask.task_id == task_id))).scalars().first()
                if task:
                    task.status = TaskStatus.FAILED
                    task.result = {"error": str(e) or repr(e)}
                    task.updated_at = _now()
                    db.add(task)
                    await db.commit()
                await _rollup_job_run_status(db, run_id)
        except Exception as update_err:
            log_error("Error updating status after failure", module="Dispatcher", task_id=task_id, error=str(update_err))
        raise
