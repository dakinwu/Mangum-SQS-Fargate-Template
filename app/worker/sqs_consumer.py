"""SQS consumer - the "real worker" half of this template.

Polls the queue and dispatches messages to the job dispatcher with bounded
concurrency (MAX_CONCURRENT_TASKS).

Back-pressure:
    The receive loop never pulls more messages than there are free
    concurrency slots. Pulling messages it has no room to process would let
    their VisibilityTimeout tick down while they sit queued in memory,
    causing SQS to redeliver (and double-process) them before we even start.

Visibility heartbeat:
    Each in-flight message runs a dedicated daemon THREAD (not a coroutine)
    that periodically extends VisibilityTimeout. This is thread-based on
    purpose: a job handler that blocks the event loop for a stretch (CPU-bound
    work, a slow blocking call) would delay a coroutine-based heartbeat past
    VisibilityTimeout and trigger redelivery + double-processing. A real OS
    thread keeps ticking regardless of what the event loop is doing.

Message lifecycle:
    On success or deterministic (terminal) failure -> delete the message
    (the dispatcher has already recorded the task's final status in the DB;
    retrying a deterministic failure wastes the visibility window). On
    TransientDispatchError -> keep the message, shorten its visibility so it
    redelivers promptly instead of waiting out the full timeout. On a crash
    mid-dispatch -> the message just naturally reappears once
    VisibilityTimeout expires (SQS's own at-least-once delivery).
"""

import asyncio
import json
import threading
import traceback
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Set

from sqlalchemy import update
from sqlmodel import select

from app.base.utils.logging import log_error, log_event, log_warning
from app.config import get_settings
from app.core.db import get_async_db
from app.models.job_run import JobRun
from app.models.sub_task import SubTask, TaskStatus
from app.models.sub_task_type import SubTaskType
from app.services.job_dispatcher.dispatcher import TransientDispatchError, dispatch_task
from app.worker.queue_backend import QueueBackend, get_queue_backend

MAX_CONCURRENT_TASKS = get_settings().max_concurrent_tasks

SQS_MAX_BATCH = 10
VISIBILITY_TIMEOUT_SECONDS = 3600
HEARTBEAT_INTERVAL_SECONDS = 600
RECEIVE_WAIT_SECONDS = 20
TRANSIENT_RETRY_DELAY_SECONDS = 300
WORKER_STALL_THRESHOLD_SECONDS = 300

_PENDING_MIN_AGE = timedelta(minutes=5)
_PENDING_MAX_AGE = timedelta(minutes=30)
_PROCESSING_MIN_AGE = timedelta(hours=2)
_PROCESSING_MAX_AGE = timedelta(hours=3)
_RECOVERY_ENQUEUE_LIMIT = 100

_health_state: Dict[str, Optional[datetime]] = {"started_at": None, "last_receive_at": None}


def _now_utc_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def get_worker_health() -> Dict[str, Any]:
    started_at = _health_state["started_at"]
    last_receive_at = _health_state["last_receive_at"]

    if started_at is None:
        return {"status": "disabled"}

    now = _now_utc_naive()
    if last_receive_at is None:
        return {"status": "starting", "started_at": started_at.isoformat(), "last_receive_at": None}

    seconds_since_receive = (now - last_receive_at).total_seconds()
    status = "stalled" if seconds_since_receive > WORKER_STALL_THRESHOLD_SECONDS else "healthy"
    return {
        "status": status,
        "started_at": started_at.isoformat(),
        "last_receive_at": last_receive_at.isoformat(),
        "seconds_since_last_receive": seconds_since_receive,
        "stall_threshold_seconds": WORKER_STALL_THRESHOLD_SECONDS,
    }


class _VisibilityHeartbeat:
    """Thread-driven VisibilityTimeout extender for one in-flight message."""

    def __init__(self, backend: QueueBackend, receipt_handle: str, task_id: str):
        self._backend = backend
        self._receipt_handle = receipt_handle
        self._task_id = task_id
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name=f"heartbeat-{task_id}", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        """Signal the thread to stop and wait for it to exit (call off-loop)."""
        self._stop.set()
        self._thread.join(timeout=HEARTBEAT_INTERVAL_SECONDS)

    def _run(self) -> None:
        while not self._stop.wait(HEARTBEAT_INTERVAL_SECONDS):
            try:
                self._backend.change_visibility(self._receipt_handle, VISIBILITY_TIMEOUT_SECONDS)
                log_event("HEARTBEAT_EXTENDED", module="SQS", data={"task_id": self._task_id})
            except Exception as exc:
                log_warning("Heartbeat failed, stopping", module="SQS", task_id=self._task_id, error=str(exc))
                return


async def _handle_message(message: dict, backend: QueueBackend) -> None:
    receipt_handle = message["ReceiptHandle"]
    payload: dict = {}
    heartbeat: Optional[_VisibilityHeartbeat] = None
    should_delete = True
    try:
        payload = json.loads(message["Body"])
        task_id = payload.get("task_id")
        log_event("TASK_RECEIVED", module="SQS", data={"task_id": task_id})

        heartbeat = _VisibilityHeartbeat(backend, receipt_handle, str(task_id))
        heartbeat.start()

        await dispatch_task(
            task_id=payload["task_id"],
            run_id=payload["run_id"],
            sub_task_type_name=payload["sub_task_type_name"],
        )
    except TransientDispatchError as exc:
        should_delete = False
        log_warning(
            "Transient infrastructure error - keeping message for redelivery",
            module="SQS", task_id=payload.get("task_id"), error=repr(exc),
            retry_delay_s=TRANSIENT_RETRY_DELAY_SECONDS,
        )
    except Exception as exc:
        log_error(
            "Failed to process message", module="SQS", task_id=payload.get("task_id"),
            error=repr(exc), traceback=traceback.format_exc(),
        )
    finally:
        if heartbeat is not None:
            # Stop before delete/visibility-change so it can't extend a
            # deleted message's visibility. Runs off-loop via to_thread since
            # .stop() joins the heartbeat thread.
            await asyncio.to_thread(heartbeat.stop)
        if should_delete:
            try:
                await asyncio.to_thread(backend.delete, receipt_handle)
            except Exception as exc:
                log_error("Failed to delete message", module="SQS", task_id=payload.get("task_id"), error=str(exc))
        else:
            try:
                await asyncio.to_thread(backend.change_visibility, receipt_handle, TRANSIENT_RETRY_DELAY_SECONDS)
            except Exception as exc:
                log_warning("Failed to shorten visibility for retry", module="SQS", task_id=payload.get("task_id"), error=str(exc))


async def _abandon_ancient_processing_tasks() -> None:
    """Mark PROCESSING tasks older than _PROCESSING_MAX_AGE as FAILED - the
    process that claimed them is presumed dead."""
    cutoff = _now_utc_naive() - _PROCESSING_MAX_AGE
    try:
        async with get_async_db() as db:
            result = await db.execute(
                update(SubTask)
                .where(SubTask.status == TaskStatus.PROCESSING, SubTask.updated_at < cutoff)
                .values(status=TaskStatus.FAILED, result={"error": f"Abandoned: stuck in PROCESSING longer than {_PROCESSING_MAX_AGE}"})
            )
            await db.commit()
            if result.rowcount:
                log_event("TASKS_ABANDONED", module="SQS", level="WARNING", data={"count": result.rowcount})
    except Exception as exc:
        log_error("Error during ancient-task abandonment", module="SQS", error=str(exc))


async def _recover_stuck_tasks(backend: QueueBackend) -> None:
    """Re-enqueue orphaned PENDING (5-30 min old) and stuck PROCESSING (2-3h
    old) tasks on worker startup - covers the case where the process itself
    died (crash/deploy/OOM) and left rows that SQS's own visibility-timeout
    expiry won't re-surface (e.g. the message was already deleted under a
    since-abandoned assumption, or is simply older than any reasonable
    timeout)."""
    now = _now_utc_naive()
    try:
        async with get_async_db() as db:
            pending_rows = (
                await db.execute(
                    select(SubTask, SubTaskType.name)
                    .join(SubTaskType, SubTask.sub_task_type_id == SubTaskType.id)
                    .where(
                        SubTask.status == TaskStatus.PENDING,
                        SubTask.created_at < now - _PENDING_MIN_AGE,
                        SubTask.created_at > now - _PENDING_MAX_AGE,
                    )
                    .limit(_RECOVERY_ENQUEUE_LIMIT)
                )
            ).all()
            processing_rows = (
                await db.execute(
                    select(SubTask, SubTaskType.name)
                    .join(SubTaskType, SubTask.sub_task_type_id == SubTaskType.id)
                    .where(
                        SubTask.status == TaskStatus.PROCESSING,
                        SubTask.updated_at < now - _PROCESSING_MIN_AGE,
                        SubTask.updated_at > now - _PROCESSING_MAX_AGE,
                    )
                    .limit(_RECOVERY_ENQUEUE_LIMIT)
                )
            ).all()

        for task, type_name in [*pending_rows, *processing_rows]:
            log_event("TASK_RECOVERED", module="SQS", level="WARNING", data={"task_id": task.task_id})
            backend.send(json.dumps({"task_id": task.task_id, "run_id": task.job_run_id, "sub_task_type_name": type_name}))

        if pending_rows or processing_rows:
            log_event(
                "RECOVERY_COMPLETED", module="SQS",
                data={"pending_recovered": len(pending_rows), "processing_recovered": len(processing_rows)},
            )
    except Exception as exc:
        log_error("Error during stuck task recovery", module="SQS", error=str(exc))


async def poll_loop() -> None:
    """Long-running coroutine - call once via asyncio.create_task()."""
    settings = get_settings()
    if not settings.sqs_queue_url:
        log_warning("SQS_QUEUE_URL not configured - poll_loop will not start", module="SQS")
        return

    backend = get_queue_backend()

    await _abandon_ancient_processing_tasks()
    await _recover_stuck_tasks(backend)

    in_flight: Set[asyncio.Task] = set()
    receipt_by_task: Dict[asyncio.Task, str] = {}

    _health_state["started_at"] = _now_utc_naive()
    log_event("POLL_LOOP_STARTED", module="SQS", data={"max_concurrent_tasks": MAX_CONCURRENT_TASKS})

    while True:
        try:
            done = {t for t in in_flight if t.done()}
            for t in done:
                receipt_by_task.pop(t, None)
            in_flight -= done

            if len(in_flight) >= MAX_CONCURRENT_TASKS:
                finished, pending = await asyncio.wait(in_flight, return_when=asyncio.FIRST_COMPLETED)
                for t in finished:
                    receipt_by_task.pop(t, None)
                in_flight = pending

            capacity = MAX_CONCURRENT_TASKS - len(in_flight)
            batch_size = min(capacity, SQS_MAX_BATCH)

            messages = await asyncio.to_thread(backend.receive, batch_size, RECEIVE_WAIT_SECONDS)
            _health_state["last_receive_at"] = _now_utc_naive()

            for message in messages:
                task = asyncio.create_task(_handle_message(message, backend))
                in_flight.add(task)
                receipt_by_task[task] = message["ReceiptHandle"]
        except asyncio.CancelledError:
            await _release_in_flight_messages(backend, in_flight, receipt_by_task)
            log_event("POLL_LOOP_CANCELLED", module="SQS", data={"in_flight_released": len(in_flight)})
            raise
        except Exception as exc:
            log_error("Unexpected error in poll_loop", module="SQS", error=str(exc))
            await asyncio.sleep(1.0)


async def _release_in_flight_messages(
    backend: QueueBackend, in_flight: Set[asyncio.Task], receipt_by_task: Dict[asyncio.Task, str]
) -> None:
    """On shutdown, force-release every still-running message's visibility to
    0 so another worker (or this one after restart) can grab it immediately
    instead of waiting out the full timeout. Safe because dispatch_task's
    atomic claim means a redelivered-and-already-claimed task is a no-op for
    whoever picks it up next."""
    releases = []
    for t in in_flight:
        if t.done():
            continue
        rh = receipt_by_task.get(t)
        if rh:
            releases.append(asyncio.to_thread(backend.change_visibility, rh, 0))
    if releases:
        results = await asyncio.gather(*releases, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                log_warning("visibility reset on cancel failed", module="SQS", error=str(result))
