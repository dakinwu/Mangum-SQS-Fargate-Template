"""Queue backend abstraction.

Two implementations behind one interface so `poll_loop` (app/worker/
sqs_consumer.py) doesn't fork logic per backend:

- SQSQueueBackend: real boto3 SQS client. Points at LocalStack locally (via
  AWS_ENDPOINT_URL) or real AWS in dev/prod - same code path either way,
  since LocalStack speaks the real SQS API.
- InMemoryQueueBackend: a deque-based fake for dependency-free unit tests of
  the consumer's back-pressure/heartbeat/idempotency mechanics, without
  needing Docker or network I/O.

Only the bootstrap/config differs between backends - not the poll_loop code.
"""

import time
import uuid
from collections import deque
from typing import Protocol

import boto3

from app.config import get_settings


class QueueBackend(Protocol):
    def receive(self, max_messages: int, wait_seconds: int) -> list[dict]: ...
    def delete(self, receipt_handle: str) -> None: ...
    def change_visibility(self, receipt_handle: str, visibility_timeout: int) -> None: ...
    def send(self, body: str) -> str: ...


class SQSQueueBackend:
    def __init__(self, queue_url: str | None = None):
        settings = get_settings()
        self.queue_url = queue_url or settings.sqs_queue_url
        client_kwargs = {"region_name": settings.aws_region}
        if settings.aws_endpoint_url:
            client_kwargs["endpoint_url"] = settings.aws_endpoint_url
        self._client = boto3.client("sqs", **client_kwargs)

    def receive(self, max_messages: int, wait_seconds: int) -> list[dict]:
        response = self._client.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_seconds,
            VisibilityTimeout=3600,
        )
        return response.get("Messages", [])

    def delete(self, receipt_handle: str) -> None:
        self._client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=receipt_handle)

    def change_visibility(self, receipt_handle: str, visibility_timeout: int) -> None:
        self._client.change_message_visibility(
            QueueUrl=self.queue_url, ReceiptHandle=receipt_handle, VisibilityTimeout=visibility_timeout
        )

    def send(self, body: str) -> str:
        response = self._client.send_message(QueueUrl=self.queue_url, MessageBody=body)
        return response["MessageId"]


class InMemoryQueueBackend:
    """Simulates SQS visibility semantics well enough to unit-test poll_loop:
    a received message is hidden from subsequent `receive()` calls until its
    visibility timeout elapses (or it's deleted / re-hidden explicitly)."""

    def __init__(self):
        self._queue: deque[dict] = deque()
        self._in_flight: dict[str, dict] = {}  # receipt_handle -> {"body", "visible_at"}

    def send(self, body: str) -> str:
        message_id = str(uuid.uuid4())
        self._queue.append({"MessageId": message_id, "ReceiptHandle": None, "Body": body})
        return message_id

    def receive(self, max_messages: int, wait_seconds: int) -> list[dict]:
        now = time.monotonic()
        # Re-surface anything whose visibility timeout has elapsed.
        for handle, entry in list(self._in_flight.items()):
            if entry["visible_at"] <= now:
                self._queue.append({"MessageId": entry["message_id"], "ReceiptHandle": None, "Body": entry["body"]})
                del self._in_flight[handle]

        messages = []
        for _ in range(min(max_messages, len(self._queue))):
            item = self._queue.popleft()
            receipt_handle = str(uuid.uuid4())
            self._in_flight[receipt_handle] = {
                "message_id": item["MessageId"],
                "body": item["Body"],
                "visible_at": now + 3600,
            }
            messages.append({"MessageId": item["MessageId"], "ReceiptHandle": receipt_handle, "Body": item["Body"]})
        return messages

    def delete(self, receipt_handle: str) -> None:
        self._in_flight.pop(receipt_handle, None)

    def change_visibility(self, receipt_handle: str, visibility_timeout: int) -> None:
        entry = self._in_flight.get(receipt_handle)
        if entry is not None:
            entry["visible_at"] = time.monotonic() + visibility_timeout


_backend: QueueBackend | None = None


def get_queue_backend() -> QueueBackend:
    global _backend
    if _backend is None:
        _backend = SQSQueueBackend()
    return _backend


def set_queue_backend(backend: QueueBackend) -> None:
    """Test hook: swap in an InMemoryQueueBackend."""
    global _backend
    _backend = backend
