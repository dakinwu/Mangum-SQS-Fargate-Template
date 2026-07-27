"""Unit tests for the consumer mechanics, using InMemoryQueueBackend so no
Docker/network is required for this file specifically."""

import asyncio
import json

import pytest

from app.services.job_dispatcher.dispatcher import TransientDispatchError
from app.worker import sqs_consumer
from app.worker.queue_backend import InMemoryQueueBackend
from app.worker.sqs_consumer import _handle_message, _VisibilityHeartbeat


@pytest.mark.asyncio
async def test_heartbeat_extends_visibility(monkeypatch):
    monkeypatch.setattr(sqs_consumer, "HEARTBEAT_INTERVAL_SECONDS", 0.02)
    backend = InMemoryQueueBackend()
    backend.send(json.dumps({"task_id": "t1"}))
    [message] = backend.receive(1, 1)
    initial_visible_at = backend._in_flight[message["ReceiptHandle"]]["visible_at"]

    heartbeat = _VisibilityHeartbeat(backend, message["ReceiptHandle"], "t1")
    heartbeat.start()
    await asyncio.sleep(0.08)
    await asyncio.to_thread(heartbeat.stop)

    entry = backend._in_flight[message["ReceiptHandle"]]
    # Each tick recomputes visible_at as now + timeout, so at least one tick
    # having fired means it moved strictly further into the future.
    assert entry["visible_at"] > initial_visible_at


@pytest.mark.asyncio
async def test_handle_message_success_deletes_message(monkeypatch):
    async def fake_dispatch(**kwargs):
        return None

    monkeypatch.setattr(sqs_consumer, "dispatch_task", fake_dispatch)

    backend = InMemoryQueueBackend()
    backend.send(json.dumps({"task_id": "t1", "run_id": "r1", "sub_task_type_name": "quick"}))
    [message] = backend.receive(1, 1)

    await _handle_message(message, backend)

    assert message["ReceiptHandle"] not in backend._in_flight


@pytest.mark.asyncio
async def test_handle_message_transient_error_keeps_message(monkeypatch):
    async def fake_dispatch(**kwargs):
        raise TransientDispatchError("db blip")

    monkeypatch.setattr(sqs_consumer, "dispatch_task", fake_dispatch)

    backend = InMemoryQueueBackend()
    backend.send(json.dumps({"task_id": "t1", "run_id": "r1", "sub_task_type_name": "quick"}))
    [message] = backend.receive(1, 1)

    await _handle_message(message, backend)

    # Message was NOT deleted - it's still tracked as in-flight, just with a
    # shortened visibility timeout for prompt redelivery.
    entry = backend._in_flight.get(message["ReceiptHandle"])
    assert entry is not None


@pytest.mark.asyncio
async def test_handle_message_terminal_error_still_deletes(monkeypatch):
    async def fake_dispatch(**kwargs):
        raise RuntimeError("deterministic bug")

    monkeypatch.setattr(sqs_consumer, "dispatch_task", fake_dispatch)

    backend = InMemoryQueueBackend()
    backend.send(json.dumps({"task_id": "t1", "run_id": "r1", "sub_task_type_name": "quick"}))
    [message] = backend.receive(1, 1)

    await _handle_message(message, backend)

    assert message["ReceiptHandle"] not in backend._in_flight


@pytest.mark.asyncio
async def test_poll_loop_never_exceeds_max_concurrent_tasks(monkeypatch):
    """Back-pressure: poll_loop must never have more in-flight handlers than
    MAX_CONCURRENT_TASKS, even when the queue has more messages available."""
    monkeypatch.setattr(sqs_consumer, "MAX_CONCURRENT_TASKS", 2)
    monkeypatch.setattr(sqs_consumer, "RECEIVE_WAIT_SECONDS", 0)

    max_in_flight_seen = 0
    in_flight_now = 0

    async def fake_dispatch(**kwargs):
        nonlocal max_in_flight_seen, in_flight_now
        in_flight_now += 1
        max_in_flight_seen = max(max_in_flight_seen, in_flight_now)
        await asyncio.sleep(0.05)
        in_flight_now -= 1

    monkeypatch.setattr(sqs_consumer, "dispatch_task", fake_dispatch)

    backend = InMemoryQueueBackend()
    for i in range(6):
        backend.send(json.dumps({"task_id": f"t{i}", "run_id": "r1", "sub_task_type_name": "quick"}))
    monkeypatch.setattr(sqs_consumer, "get_queue_backend", lambda: backend)
    monkeypatch.setattr(sqs_consumer.get_settings(), "sqs_queue_url", "http://fake")

    task = asyncio.create_task(sqs_consumer.poll_loop())
    await asyncio.sleep(0.4)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert max_in_flight_seen <= 2
